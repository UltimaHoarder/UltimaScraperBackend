"""Bounded durable job headers and task history."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from sqlalchemy import (
    BigInteger,
    String,
    Text,
    and_,
    case,
    cast,
    column,
    func,
    inspect,
    select,
    text,
    values,
)
from sqlalchemy.orm import aliased
from ultima_scraper_db_archive.global_db import CentralTaskModel, MPSTaskDefinitionModel

from ..job_system.storage import TaskPayloadBudgetExceeded
from .write_queue import DatabaseWriteClient, _positive_int_env


def history_byte_limit() -> int:
    return _positive_int_env("ULTIMA_JOB_HISTORY_MAX_BYTES", 0) or _positive_int_env(
        "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
    )


def headers(session_factory, job_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Requested plans and retired ordinary headers; strip arrays before transfer."""
    if not job_ids:
        return {}
    budget = history_byte_limit()
    with session_factory() as session:
        rows = session.execute(
            text("""
            WITH plans AS (
                SELECT job_id, total, completed, failed, cursor, status,
                       job_config_json::jsonb - 'tasks' - 'task_ids' AS config
                FROM management.mps_plans
                WHERE job_id=ANY(:job_ids) AND parent_job_id IS NULL
            ), retired AS (
                SELECT job_id, job_config_json::jsonb - 'tasks' - 'task_ids' AS config
                FROM management.jobs
                WHERE job_id=ANY(:job_ids)
                    AND job_config_json::jsonb->>'state' IN ('completed', 'failed', 'cancelled')
                    AND NOT EXISTS (SELECT 1 FROM plans WHERE plans.job_id=jobs.job_id)
            ), selected AS (
                SELECT * FROM plans
                UNION ALL
                SELECT job_id,
                    coalesce((config->>'total_tasks')::bigint, 0),
                    coalesce((config->>'completed_tasks')::bigint, 0),
                    coalesce((config->>'failed_tasks')::bigint, 0),
                    0, config->>'state', config
                FROM retired
            )
            SELECT job_id, total, completed, failed, cursor, status,
                   CASE WHEN sum(octet_length(config::text)) OVER () <= :budget
                        THEN config END AS config
            FROM selected
        """),
            {"job_ids": job_ids, "budget": budget},
        )
        output = {}
        for row in rows:
            if row.config is None:
                raise TaskPayloadBudgetExceeded(
                    row.job_id, budget + 1, budget, phase="job history headers"
                )
            output[row.job_id] = dict(
                config=row.config,
                total=row.total,
                completed=row.completed,
                failed=row.failed,
                cursor=row.cursor,
                status=row.status,
            )
        return output


def archive_runtime_header(session_factory, snapshot, *, max_bytes: int) -> None:
    """Writer-only final header merge, before acknowledging Redis retirement.

    Definition/revision locks preserve canonical SQL controls. Only execution
    counters, timestamps and runtime metadata are copied from the captured Redis
    header. Each encoded header is bounded independently of total task count.
    """
    from datetime import datetime, timezone
    from ..job_system import mps_attempt, ordinary_edit, retention
    from ..job_system.storage import _iso_to_datetime

    raw = snapshot.config
    if raw is None:
        raise RuntimeError(f"Cannot archive missing job header {snapshot.job_id}")
    size = len(raw if isinstance(raw, bytes) else raw.encode("utf-8"))
    if size > max_bytes:
        raise TaskPayloadBudgetExceeded(
            snapshot.job_id, size, max_bytes, phase="runtime job header"
        )
    runtime_config = json.loads(raw)
    if not isinstance(runtime_config, dict) or not isinstance(
        runtime_config.get("metadata", {}), dict
    ):
        raise RuntimeError(f"Invalid runtime job header {snapshot.job_id}")
    runtime_metadata = runtime_config.get("metadata", {})

    with session_factory() as session:
        mps_attempt.sql_assert_current(session, snapshot.job_id)
        retention.sql_assert_current(session, snapshot.job_id)

        def read_config(table: str):
            # Table names are internal literals. Strip any historical task-ID
            # arrays on the server before admitting a header to the process.
            row = session.execute(
                text(f"""
                SELECT CASE WHEN octet_length((coalesce(job_config_json::jsonb, '{{}}'::jsonb)
                    - 'tasks' - 'task_ids')::text) <= :max_bytes
                    THEN coalesce(job_config_json::jsonb, '{{}}'::jsonb) - 'tasks' - 'task_ids'
                    END AS config
                FROM management.{table} WHERE job_id=:job_id FOR UPDATE
            """),
                {"job_id": snapshot.job_id, "max_bytes": max_bytes},
            ).one_or_none()
            if row is None:
                raise retention.StaleRetirement(
                    f"Durable job header {snapshot.job_id} was deleted"
                )
            if row.config is None:
                raise TaskPayloadBudgetExceeded(
                    snapshot.job_id,
                    max_bytes + 1,
                    max_bytes,
                    phase="durable job header",
                )
            return row.config

        durable = snapshot.manifest_flag == "1"
        config = read_config("mps_plans" if durable else "jobs")
        if durable:
            if int(config.get("metadata", {}).get("mps_config_revision", 0)) != int(
                runtime_metadata.get("mps_config_revision", 0)
            ):
                raise retention.StaleRetirement(
                    f"Durable job configuration {snapshot.job_id} changed"
                )
            read_config("jobs")  # Lock in the same plan-then-job order as edits.
        elif config and ordinary_edit.revision(
            snapshot.job_id, config
        ) != ordinary_edit.revision(snapshot.job_id, runtime_config):
            raise retention.StaleRetirement(
                f"Ordinary job configuration {snapshot.job_id} changed"
            )
        if not config:
            config = {
                key: value
                for key, value in runtime_config.items()
                if key not in {"tasks", "task_ids"}
            }
        metadata = config.setdefault("metadata", {})
        for key in ("run_count", "last_ran_at", "last_error", "result_message"):
            if key in runtime_metadata:
                metadata[key] = runtime_metadata[key]
        config["state"] = snapshot.status
        for key, value in snapshot.runtime_state.items():
            if value is None:
                continue
            if key in {"total_tasks", "completed_tasks", "failed_tasks"}:
                value = int(value)
                if value < 0:
                    raise ValueError(f"Invalid terminal {key} for {snapshot.job_id}")
            config[key] = value
        encoded = json.dumps(config)
        size = len(encoded.encode("utf-8"))
        if size > max_bytes:
            raise TaskPayloadBudgetExceeded(
                snapshot.job_id, size, max_bytes, phase="archived job header"
            )
        event_time = max(
            (
                time
                for key in ("updated_at", "completed_at")
                if (time := _iso_to_datetime(snapshot.runtime_state.get(key)))
                is not None
            ),
            default=_iso_to_datetime(snapshot.generation) or datetime.now(timezone.utc),
        )
        row = session.execute(
            text(
                "SELECT status, last_event_at FROM management.jobs WHERE job_id=:job_id"
            ),
            {"job_id": snapshot.job_id},
        ).one()
        if (
            row.status != snapshot.status
            and row.last_event_at is not None
            and row.last_event_at > event_time
        ):
            raise retention.StaleRetirement(
                f"Job {snapshot.job_id} has a newer durable status"
            )
        session.execute(
            text("""
            UPDATE management.jobs SET job_config_json=:config, status=:status,
                started_at=coalesce(:started_at, started_at),
                completed_at=coalesce(:completed_at, completed_at),
                last_event_at=greatest(last_event_at, :event_time)
            WHERE job_id=:job_id
        """),
            {
                "job_id": snapshot.job_id,
                "config": encoded,
                "status": snapshot.status,
                "started_at": _iso_to_datetime(
                    snapshot.runtime_state.get("started_at")
                ),
                "completed_at": _iso_to_datetime(
                    snapshot.runtime_state.get("completed_at")
                ),
                "event_time": event_time,
            },
        )
        if durable:
            session.execute(
                text(
                    "UPDATE management.mps_plans SET job_config_json=:config WHERE job_id=:job_id"
                ),
                {"job_id": snapshot.job_id, "config": encoded},
            )
        session.commit()


@dataclass
class TaskRecord:
    task_id: str
    read_bytes: int
    archive: Any = None
    definition: str | None = None


def task_records(
    session_factory,
    job_id: str,
    task_ids: list[str],
    *,
    definition_id: str | None = None,
    byte_limit: int | None = None,
) -> list[TaskRecord]:
    """Read a fitting ordered prefix, sizing inputs and results before transfer.

    The size query and conditional payload joins share a single SQL snapshot.
    Missing IDs consume no bytes. A missing runtime record is read here only after
    Redis, so an archive-before-purge transition cannot erase the task from a page.
    """
    if not task_ids:
        return []
    budget = history_byte_limit() if byte_limit is None else byte_limit
    requested = (
        values(
            column("position", BigInteger),
            column("requested_id", String),
            name="requested",
        )
        .data(list(enumerate(task_ids)))
        .cte("requested")
    )
    archived, definition = aliased(CentralTaskModel), aliased(MPSTaskDefinitionModel)
    # Deferred canonical inputs are sized separately. Every normally loaded
    # column, including error/message/result text, contributes to the allowance.
    archive_size = sum(
        func.coalesce(func.octet_length(cast(getattr(archived, attr.key), Text)), 0)
        for attr in inspect(CentralTaskModel).column_attrs
        if not attr.deferred
    )
    input_json = func.coalesce(definition.task_json, archived.definition_json)
    selected = (
        select(
            requested.c.position,
            requested.c.requested_id,
            (
                case((archived.task_id.is_not(None), archive_size), else_=0)
                + func.coalesce(func.octet_length(input_json), 0)
            ).label("read_bytes"),
        )
        .select_from(requested)
        .outerjoin(
            archived,
            and_(
                archived.job_id == job_id,
                archived.task_id == requested.c.requested_id,
            ),
        )
        .outerjoin(
            definition,
            and_(
                definition.manifest_id == definition_id,
                definition.task_id == requested.c.requested_id,
            ),
        )
        .cte("selected")
    )
    sized = select(
        selected,
        func.sum(selected.c.read_bytes)
        .over(order_by=selected.c.position, rows=(None, 0))
        .label("bytes_through_row"),
    ).cte("sized")
    payload_archive, payload_definition = (
        aliased(CentralTaskModel),
        aliased(MPSTaskDefinitionModel),
    )
    fits = sized.c.bytes_through_row <= budget
    query = (
        select(
            sized.c.requested_id,
            sized.c.read_bytes,
            fits,
            payload_archive,
            func.coalesce(
                payload_definition.task_json, payload_archive.definition_json
            ),
        )
        .select_from(sized)
        .outerjoin(
            payload_archive,
            and_(
                fits,
                payload_archive.job_id == job_id,
                payload_archive.task_id == sized.c.requested_id,
            ),
        )
        .outerjoin(
            payload_definition,
            and_(
                fits,
                payload_definition.manifest_id == definition_id,
                payload_definition.task_id == sized.c.requested_id,
            ),
        )
        .order_by(sized.c.position)
    )
    with session_factory() as session:
        result = []
        for task_id, size, fits, archive, definition_json in session.execute(query):
            if not fits:
                if not result:
                    raise TaskPayloadBudgetExceeded(
                        task_id, int(size), budget, phase="SQL task history"
                    )
                break
            result.append(TaskRecord(task_id, int(size), archive, definition_json))
        return result


def definition_ids(
    session_factory, manifest_id: str, *, offset: int, limit: int, reverse: bool = False
) -> list[str]:
    model = MPSTaskDefinitionModel
    with session_factory() as session:
        query = select(model.task_id).where(model.manifest_id == manifest_id)
        if reverse:
            query = query.where(model.ordinal <= offset).order_by(model.ordinal.desc())
        else:
            query = query.where(model.ordinal >= offset).order_by(model.ordinal)
        return list(session.execute(query.limit(limit)).scalars())
