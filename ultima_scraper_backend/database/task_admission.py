"""Admit ordinary executable tasks to the existing durable definition dispatcher."""

from __future__ import annotations

import json
from contextlib import closing, nullcontext
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
from uuid import uuid4

from ultima_scraper_db_archive.global_db import CentralJobModel, MPSPlanModel

from .mps_manifest import MPSManifestStore
from .write_queue import DatabaseWriteClient, _positive_int_env
from ..job_system.models import Task
from ..job_system.storage import TaskPayloadBudgetExceeded


def prepare_task_plan(
    *,
    select_backend: Callable[[dict[str, Any]], str] | None = None,
    batch_size: int | None = None,
    max_bytes: int | None = None,
    **submission: Any,
) -> dict[str, Any]:
    """Own preparation and optional routing selection in one SQL transaction.

    A routing callback reads backend availability; it must not publish work.
    Its assignment commits with the definitions, before recovery can see them.
    """
    from ultima_scraper_db_archive.global_db import global_session
    from ..job_system.resources import task_insert_max_bytes
    from ..job_system.runtime_admission import validate_durable_configuration
    from ..job_system.storage import JobStorage

    with global_session() as session:
        config = create_task_plan(
            session,
            batch_size=_positive_int_env(
                "ULTIMA_JOB_TASK_INSERT_BATCH_SIZE", JobStorage.DEFAULT_WRITE_BATCH_SIZE,
                value=batch_size,
            ),
            max_bytes=task_insert_max_bytes(max_bytes),
            **submission,
        )
        if select_backend is not None:
            config["metadata"]["dispatch_backend_id"] = select_backend(config)
            validate_durable_configuration(submission["job_id"], config)
            session.get(MPSPlanModel, submission["job_id"]).job_config_json = json.dumps(config)
            from ..job_system.ledger import owns, revise, ExecutionLedger
            if owns(config):
                ledger = ExecutionLedger()
                run = ledger._locked_run(session, submission["job_id"])
                plan = ledger.manifests._snapshot(session.get(MPSPlanModel, submission["job_id"]))
                config = revise(session, run, plan)
        session.commit()
        return config


def create_task_plan(
    session: Any,
    *,
    job_id: str,
    job_type: str,
    site: str,
    job_config: dict[str, Any],
    args_payload: dict[str, Any],
    tasks: Iterable[Task],
    batch_size: int,
    max_bytes: int,
    build_tasks: bool = False,
) -> dict[str, Any]:
    """Commit with the caller's transaction; expose no partial runnable plan.

    Ordinary jobs use explicit task definitions in the existing manifest tables.
    The historic MPS metadata names identify that dispatch protocol; the separate
    job kind selects ordinary resource controls and retains the public job type.
    """
    if job_config.get("metadata", {}).get("admission_idempotent"):
        from sqlalchemy import text
        # Short transaction lock: every API process joins the same durable job.
        # No connection is held while the worker performs upstream I/O.
        session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
                        {"key": f"ultima:api-admission:{job_id}"})
        existing = session.get(MPSPlanModel, job_id)
        if existing is not None:
            return json.loads(existing.job_config_json)
    if session.get(CentralJobModel, job_id) is not None:
        raise ValueError(f"Job {job_id} already exists")
    config = {**job_config, "task_ids": []}
    config["metadata"] = {
        **job_config.get("metadata", {}),
        "mps_manifest": True,
        "durable_job_kind": "ordinary",
        "job_resource_scope": "ordinary",
    }
    session.add(
        CentralJobModel(
            job_id=job_id,
            job_type=job_type,
            site=site,
            status="pending",
            args_json=json.dumps(
                {key: value for key, value in args_payload.items() if key != "tasks"}
            ),
        )
    )
    session.add(
        MPSPlanModel(
            job_id=job_id,
            site=site,
            status="staging",
            total=0,
            job_config_json=json.dumps(config),
            proxy_pool_json="[]",
            summary_json="{}",
            selection_json="{}",
        )
    )
    session.flush()
    store = MPSManifestStore()
    definitions = store.definitions
    manifest_id = f"definition-{uuid4().hex}"
    definitions.begin(job_id, manifest_id, db_session=session)
    input_max_bytes = _positive_int_env(
        "ULTIMA_JOB_TASK_INPUT_MAX_BYTES", 0
    ) or _positive_int_env(
        "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
    )
    batch: list[dict[str, Any]] = []
    occupied = offset = 0

    def flush() -> None:
        nonlocal offset, batch, occupied
        if batch:
            definitions.append(
                manifest_id, offset=offset, tasks=batch, db_session=session
            )
            offset += len(batch)
            batch, occupied = [], 0

    from ..job_system.job_builder import iter_expanded_tasks

    # The request retains its staged input file until this owned SQL call drains.
    # Explicitly close expansion on a later insert failure as well as success.
    source = iter_expanded_tasks(tasks) if build_tasks else iter(tasks)
    with closing(source) if hasattr(source, "close") else nullcontext(source):
        for task in source:
            if not site:
                site = str(task.site_name).lower()
                session.get(CentralJobModel, job_id).site = site
                session.get(MPSPlanModel, job_id).site = site
            payload = task.to_dict(include_result=False)
            payload.update(
                job_id=job_id,
                mps_task_manifest=manifest_id,
                state="pending",
                worker_id=None,
                started_at=None,
                completed_at=None,
                result=None,
                attempts=0,
                deferred_dedup_key=None,
            )
            input_bytes = len(json.dumps(payload).encode("utf-8"))
            if input_bytes > input_max_bytes:
                raise TaskPayloadBudgetExceeded(
                    task.task_id, input_bytes, input_max_bytes, phase="SQL input read"
                )
            # Include the definition's identifiers, dependency array and row fields,
            # rather than measuring only the handler's payload.
            size = sum(
                len(str(value).encode("utf-8"))
                for value in (
                    manifest_id,
                    offset + len(batch),
                    task.task_id,
                    json.dumps(payload),
                    json.dumps(task.depends_on),
                    "pending",
                )
            )
            if size > max_bytes:
                raise TaskPayloadBudgetExceeded(
                    task.task_id, size, max_bytes, phase="SQL input insert"
                )
            if batch and (len(batch) >= batch_size or occupied + size > max_bytes):
                flush()
            batch.append(payload)
            occupied += size
    flush()
    if not offset:
        raise ValueError("No valid tasks provided")
    if build_tasks:
        from .task_building import normalize_built_tasks

        normalize_built_tasks(session, manifest_id, input_max_bytes=input_max_bytes, max_bytes=max_bytes)
    plan = definitions.commit(job_id, manifest_id, total=offset, db_session=session)
    required = job_config.get("metadata", {}).get("required_capabilities")
    if required:
        from ..job_system.runtime_admission import validate_durable_configuration

        config = plan["job_config"]
        config["metadata"]["required_capabilities"] = sorted(
            set(config["metadata"].get("required_capabilities") or []) | set(required)
        )
        validate_durable_configuration(job_id, config)
        session.get(MPSPlanModel, job_id).job_config_json = json.dumps(config)
    # The manifest owns active configuration; the central row is a job summary.
    row = session.get(CentralJobModel, job_id)
    row.last_event_at = datetime.now(timezone.utc)
    from ..job_system.ledger import admit

    return admit(session, plan)["job_config"]
