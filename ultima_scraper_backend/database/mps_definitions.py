"""Durable, paged task definitions for an explicit replacement of an MPS job.

Uploads use hidden staging plans. Only a complete, validated definition can
replace the job's active selection. All methods accept the writer transaction
so its receipt commits with the corresponding upload/checkpoint mutation.
"""

from __future__ import annotations

import json
from collections import Counter, deque
from collections.abc import Sequence
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from ultima_scraper_db_archive.global_db import (
    CentralJobModel,
    CentralTaskModel,
    MPSCandidateRangeModel,
    MPSPlanModel,
    MPSTaskDefinitionModel,
)

if TYPE_CHECKING:
    from .mps_manifest import MPSManifestStore


class MPSDefinitionBudgetExceeded(ValueError):
    """The first ready definition cannot fit this transfer's byte budget."""

    def __init__(self, task_id: str, required_bytes: int, byte_limit: int):
        self.task_id = task_id
        self.required_bytes = required_bytes
        super().__init__(
            f"MPS definition {task_id} requires {required_bytes} bytes; read budget is {byte_limit}"
        )


def definition_routing(session: Any, manifest_id: str) -> dict[str, Any]:
    from ..job_system.runtime_admission import policy
    from ..job_system.storage import TaskPayloadBudgetExceeded

    limits = policy()
    byte_limit = min(limits.per_header_bytes, limits.bytes)
    capabilities_query = """
        SELECT DISTINCT capability
        FROM management.mps_task_definitions node
        CROSS JOIN LATERAL jsonb_array_elements_text(node.task_json::jsonb->'capabilities') AS capability
        WHERE manifest_id=:manifest
    """
    # Check the aggregate on the server before transferring any capability
    # strings. A million distinct task inputs must not become a Python list
    # merely to discover that their header cannot fit its configured budget.
    size = session.execute(text(f"""
        SELECT coalesce(sum(octet_length(to_json(capability)::text)+2), 0)
        FROM ({capabilities_query}) capabilities
    """), {"manifest": manifest_id}).scalar_one()
    if size > byte_limit:
        raise TaskPayloadBudgetExceeded(manifest_id, int(size), byte_limit, phase="routing capabilities")
    required_capabilities = list(session.execute(
        text(capabilities_query + " ORDER BY capability"), {"manifest": manifest_id}
    ).scalars())
    first_ip_lock = session.execute(text("""
        SELECT CASE WHEN octet_length(value)<=:limit THEN value END, octet_length(value)
        FROM (
            SELECT task_json::jsonb->>'ip_lock_key' AS value
            FROM management.mps_task_definitions
            WHERE manifest_id=:manifest AND coalesce(task_json::jsonb->>'ip_lock_key','')<>''
            ORDER BY ordinal LIMIT 1
        ) first_lock
    """), {"manifest": manifest_id, "limit": byte_limit}).first()
    if first_ip_lock and first_ip_lock[1] > byte_limit:
        raise TaskPayloadBudgetExceeded(manifest_id, first_ip_lock[1], byte_limit, phase="routing IP lock")
    first_ip_lock = first_ip_lock[0] if first_ip_lock else None
    return {"required_capabilities": required_capabilities, "ip_lock_key": first_ip_lock}


class MPSTaskDefinitions:
    def __init__(self, manifests: MPSManifestStore) -> None:
        self.manifests = manifests

    def _session(self, db_session: Any | None):
        return (
            nullcontext(db_session)
            if db_session is not None
            else self.manifests.session_factory()
        )

    @staticmethod
    def _locked_plan(session: Any, job_id: str) -> MPSPlanModel:
        plan = session.execute(
            select(MPSPlanModel)
            .where(MPSPlanModel.job_id == job_id)
            .with_for_update()
        ).scalar_one_or_none()
        if plan is None:
            raise ValueError(f"MPS plan {job_id} not found")
        return plan

    def begin(
        self, job_id: str, manifest_id: str, *, name: str | None = None,
        schedule: dict[str, Any] | None = None, db_session: Any | None = None,
    ) -> dict[str, Any]:
        from ..job_system.models import JobSchedule

        if manifest_id == job_id:
            raise ValueError("Replacement definition must have its own manifest ID")
        if name is not None and not name.strip():
            raise ValueError("Job name cannot be empty")
        patch: dict[str, Any] = {}
        if name is not None:
            patch["name"] = name.strip()
        if schedule is not None:
            patch["schedule"] = JobSchedule.from_dict(schedule).to_dict()
            if patch["schedule"]["scheduled_for"]:
                datetime.fromisoformat(patch["schedule"]["scheduled_for"])
        with self._session(db_session) as session:
            source = self._locked_plan(session, job_id)
            if source.status == "preparing":
                raise ValueError("MPS retry preparation is still in progress")
            if source.parent_job_id is not None:
                raise ValueError("A task definition is not an editable job")
            existing = session.get(MPSPlanModel, manifest_id)
            if existing is not None:
                if (
                    json.loads(existing.selection_json).get("source_job_id") != job_id
                    or json.loads(existing.job_config_json) != patch
                ):
                    raise ValueError("Replacement manifest ID already has another definition")
                return {"manifest_id": manifest_id, "uploaded": existing.total}
            source_selection = json.loads(source.selection_json)
            session.add(MPSPlanModel(
                job_id=manifest_id, parent_job_id=job_id,
                site=source.site, status="staging", total=0,
                job_config_json=json.dumps(patch), proxy_pool_json="[]", summary_json="{}",
                selection_json=json.dumps({
                    "source_job_id": job_id,
                    "expected_manifest_id": source_selection.get("task_manifest_id"),
                }),
            ))
            session.flush()
            if db_session is None:
                session.commit()
        return {"manifest_id": manifest_id, "uploaded": 0}

    def append(
        self, manifest_id: str, *, offset: int, tasks: list[dict[str, Any]],
        db_session: Any | None = None,
    ) -> dict[str, Any]:
        from ..job_system.models import Task, TaskState

        if not tasks:
            raise ValueError("Replacement upload batch cannot be empty")
        with self._session(db_session) as session:
            stage = self._locked_plan(session, manifest_id)
            if stage.status != "staging" or stage.total != offset:
                raise ValueError("Replacement upload does not match its durable checkpoint")
            job_id = json.loads(stage.selection_json)["source_job_id"]
            rows = []
            for ordinal, payload in enumerate(tasks, start=offset):
                task = Task.from_dict(payload)
                task.job_id = job_id
                task.mps_task_manifest = manifest_id
                task.state = TaskState.PENDING
                task.worker_id = task.started_at = task.completed_at = None
                task.result = None
                task.attempts = 0
                task.deferred_dedup_key = None
                rows.append({
                    "manifest_id": manifest_id, "ordinal": ordinal,
                    "task_id": task.task_id, "task_json": json.dumps(task.to_dict()),
                    "dependency_ids": task.depends_on, "status": "pending",
                })
            session.execute(insert(MPSTaskDefinitionModel), rows)
            stage.total += len(rows)
            stage.updated_at = datetime.now(timezone.utc)
            uploaded = stage.total
            if db_session is None:
                session.commit()
        return {"manifest_id": manifest_id, "uploaded": uploaded}

    def append_spawned(self, plan: MPSPlanModel, rows: list[dict[str, Any]], *, session: Any) -> int:
        """Persist one bounded window's children inside its archive transaction.

        A compressed selection keeps its original ranges; only discovered
        children need explicit rows, appended after that immutable prefix.
        """
        from ..job_system.models import Task, TaskState
        from .mps_retry import reconcile_children

        reconcile_children(plan, rows, session)
        children = []
        for row in rows:
            if row["status"] != "completed":
                continue  # Failed handlers did not admit children previously.
            result = json.loads(row.get("result_json") or "{}")
            children.extend(Task.from_dict(payload) for payload in result.get("new_tasks", []))
        if not children:
            return 0
        by_id = {task.task_id: task for task in children}
        if len(by_id) != len(children):
            raise ValueError("Spawned tasks must have distinct task IDs")
        # Only new nodes can introduce a cycle: existing definitions already
        # rejected references to then-missing task IDs. This graph is bounded
        # by the current window's result payload, not the total job size.
        remaining = {task.task_id: len(set(task.depends_on) & by_id.keys()) for task in children}
        dependents: dict[str, list[str]] = {}
        for task in children:
            for dependency in set(task.depends_on) & by_id.keys():
                dependents.setdefault(dependency, []).append(task.task_id)
        ready = deque(task_id for task_id, count in remaining.items() if count == 0)
        visited = 0
        while ready:
            visited += 1
            for dependent in dependents.get(ready.popleft(), ()):
                remaining[dependent] -= 1
                if remaining[dependent] == 0:
                    ready.append(dependent)
        if visited != len(children):
            raise ValueError("Spawned task dependencies contain a cycle")

        selection = json.loads(plan.selection_json)
        manifest_id = selection.get("task_manifest_id")
        if manifest_id is None:
            selection.setdefault("candidate_total", plan.total)
            selection["spawned_manifest_id"] = plan.job_id
            definition_id = plan.job_id
            offset = plan.total - selection["candidate_total"]
        else:
            definition_id, offset = manifest_id, plan.total
        model = MPSTaskDefinitionModel
        existing = session.execute(select(model.task_id).where(
            model.manifest_id == definition_id, model.task_id.in_(by_id),
        ).limit(1)).first()
        from .job_partitions import task_identity_model

        identity = task_identity_model(session)
        archived = session.execute(select(identity.task_id).where(
            identity.task_id.in_(by_id),
        ).limit(1)).first()
        if existing or archived:
            raise ValueError("Spawned task ID already belongs to another task")
        definitions = []
        for ordinal, task in enumerate(children, start=offset):
            task.job_id = plan.job_id
            task.mps_task_manifest = manifest_id or ""
            task.state = TaskState.PENDING
            task.worker_id = task.started_at = task.completed_at = None
            task.result = None
            task.attempts = 0
            task.deferred_dedup_key = None
            definitions.append({"manifest_id": definition_id, "ordinal": ordinal,
                "task_id": task.task_id, "task_json": json.dumps(task.to_dict()),
                "dependency_ids": task.depends_on, "status": "pending"})
        session.execute(insert(model), definitions)
        missing = session.execute(text("""
            SELECT node.task_id, dependency.id
            FROM management.mps_task_definitions AS node
            CROSS JOIN LATERAL unnest(node.dependency_ids) AS dependency(id)
            LEFT JOIN management.mps_task_definitions AS target
              ON target.manifest_id=node.manifest_id AND target.task_id=dependency.id
            LEFT JOIN management.job_tasks AS history
              ON history.job_id=:job_id AND history.task_id=dependency.id
            WHERE node.manifest_id=:manifest AND node.task_id=ANY(:task_ids)
              AND target.task_id IS NULL AND history.task_id IS NULL
            LIMIT 1
        """), {"manifest": definition_id, "job_id": plan.job_id, "task_ids": list(by_id)}).first()
        if missing:
            raise ValueError(f"Task {missing[0]} depends on missing task {missing[1]}")

        config = json.loads(plan.job_config_json)
        metadata = config.setdefault("metadata", {})
        counts = Counter(metadata.get("mps_task_type_counts") or {"scrape": plan.total})
        counts.update(task.task_type.value for task in children)
        plan.total += len(children)
        metadata.update(mps_config_revision=int(metadata.get("mps_config_revision", 0)) + 1,
                        mps_task_type_counts=dict(counts), total_tasks_count_override=plan.total)
        config["total_tasks"] = plan.total
        plan.job_config_json = json.dumps(config)
        plan.selection_json = json.dumps(selection)
        summary = json.loads(plan.summary_json)
        summary["spawned_tasks"] = int(summary.get("spawned_tasks", 0)) + len(children)
        summary["task_count"] = plan.total
        plan.summary_json = json.dumps(summary)
        if manifest_id is not None:
            stage = self._locked_plan(session, manifest_id)
            stage.total = plan.total
            stage.updated_at = datetime.now(timezone.utc)
        session.execute(text("""
            UPDATE management.jobs SET args_json =
              (args_json::jsonb || CAST(:changes AS jsonb))::text WHERE job_id=:job_id
        """), {"job_id": plan.job_id,
               "changes": json.dumps({"task_count": plan.total, "task_types": list(counts)})})
        return len(children)

    @staticmethod
    def _validate_dependency_graph(session: Any, manifest_id: str) -> None:
        """Validate references and cycles without retaining the graph in Python.

        Kahn's algorithm stores remaining counts and edges in transaction-local
        tables. Only one bounded ready frontier is materialized at a time; it
        does not enumerate paths or keep a recursive visited set in memory.
        """
        from ..job_system.storage import JobStorage

        if session.execute(text("""
            SELECT 1 FROM management.mps_task_definitions
            WHERE manifest_id=:manifest AND cardinality(dependency_ids)>0 LIMIT 1
        """), {"manifest": manifest_id}).first() is None:
            return  # Independent tasks have neither missing references nor cycles.
        # A fresh manifest can be severely underestimated by table statistics.
        # Keep this indexed per-reference probe correlated: an anti-join plan
        # may otherwise repeatedly scan a materialized copy of the definition.
        missing = session.execute(text("""
            SELECT node.task_id, dependency.id
            FROM management.mps_task_definitions node
            CROSS JOIN LATERAL unnest(node.dependency_ids) dependency(id)
            WHERE node.manifest_id=:manifest AND NOT EXISTS (
                SELECT 1 FROM management.mps_task_definitions target
                WHERE target.manifest_id=:manifest AND target.task_id=dependency.id
                OFFSET 0
            ) LIMIT 1
        """), {"manifest": manifest_id}).first()
        if missing:
            raise ValueError(f"Task {missing[0]} depends on missing task {missing[1]}")

        batch_size = JobStorage.DEFAULT_WRITE_BATCH_SIZE
        session.execute(text("""
            CREATE TEMP TABLE usr_mps_validation_nodes (
                task_id text PRIMARY KEY, remaining bigint NOT NULL
            ) ON COMMIT DROP
        """))
        session.execute(text("""
            INSERT INTO usr_mps_validation_nodes
            SELECT task_id, cardinality(dependency_ids)
            FROM management.mps_task_definitions WHERE manifest_id=:manifest
        """), {"manifest": manifest_id})
        session.execute(text("""
            CREATE TEMP TABLE usr_mps_validation_edges ON COMMIT DROP AS
            SELECT dependency.id AS prerequisite, node.task_id AS dependent
            FROM management.mps_task_definitions node
            CROSS JOIN LATERAL unnest(node.dependency_ids) dependency(id)
            WHERE node.manifest_id=:manifest
        """), {"manifest": manifest_id})
        session.execute(text("""
            CREATE INDEX usr_mps_validation_edges_prerequisite
            ON usr_mps_validation_edges(prerequisite)
        """))
        session.execute(text("""
            CREATE TEMP TABLE usr_mps_validation_ready (
                ordinal bigserial PRIMARY KEY, task_id text NOT NULL
            ) ON COMMIT DROP
        """))
        session.execute(text("""
            INSERT INTO usr_mps_validation_ready(task_id)
            SELECT task_id FROM usr_mps_validation_nodes WHERE remaining=0
        """))
        session.execute(text("ANALYZE usr_mps_validation_nodes"))
        session.execute(text("ANALYZE usr_mps_validation_edges"))
        # A monotonic SQL queue avoids rescanning already-consumed ready
        # entries. The loop also avoids a client round trip for every graph
        # level. batch_size is a validated positive integer.
        session.execute(text(f"""
            DO $validation$
            DECLARE frontier text[]; after_ordinal bigint := 0; through_ordinal bigint;
            BEGIN
                LOOP
                    SELECT array_agg(task_id), max(ordinal)
                    INTO frontier, through_ordinal FROM (
                        SELECT ordinal, task_id FROM usr_mps_validation_ready
                        WHERE ordinal>after_ordinal ORDER BY ordinal LIMIT {batch_size}
                    ) ready;
                    EXIT WHEN frontier IS NULL;
                    WITH released AS (
                        SELECT dependent, count(*) AS count
                        FROM usr_mps_validation_edges
                        WHERE prerequisite=ANY(frontier) GROUP BY dependent
                    )
                    , updated AS (
                        UPDATE usr_mps_validation_nodes node
                        SET remaining=node.remaining-released.count
                        FROM released WHERE node.task_id=released.dependent
                        RETURNING node.task_id, node.remaining
                    )
                    INSERT INTO usr_mps_validation_ready(task_id)
                    SELECT task_id FROM updated WHERE remaining=0;
                    after_ordinal := through_ordinal;
                END LOOP;
            END $validation$
        """))
        cyclic = session.execute(text(
            "SELECT task_id FROM usr_mps_validation_nodes WHERE remaining>0 LIMIT 1"
        )).first()
        # Several commits may share a writer receipt transaction. Rollback
        # drops the tables on an SQL failure; normal returns release them here.
        session.execute(text("""
            DROP TABLE usr_mps_validation_ready, usr_mps_validation_edges, usr_mps_validation_nodes
        """))
        if cyclic is not None:
            raise ValueError("Task dependencies contain a cycle")

    def commit(
        self, job_id: str, manifest_id: str, *, total: int,
        db_session: Any | None = None,
    ) -> dict[str, Any]:
        """Atomically replace the selection and reset the same job's history.

        Resetting task history matches the existing explicit-replacement API.
        It happens only after every upload batch and dependency has been checked.
        Metadata edits committed during upload are retained unless this request
        explicitly changes the same field.
        """
        from ..job_system.models import JobSchedule

        with self._session(db_session) as session:
            source = self._locked_plan(session, job_id)
            stage = self._locked_plan(session, manifest_id)
            staged_selection = json.loads(stage.selection_json)
            selection = json.loads(source.selection_json)
            if staged_selection.get("source_job_id") != job_id:
                raise ValueError("Replacement belongs to another job")
            if selection.get("task_manifest_id") == manifest_id:
                return self.manifests._snapshot(source)
            if selection.get("task_manifest_id") != staged_selection["expected_manifest_id"]:
                raise ValueError("Job task selection changed during replacement upload")
            if stage.status != "staging" or total <= 0 or stage.total != total:
                raise ValueError("Replacement upload is incomplete")
            self._validate_dependency_graph(session, manifest_id)
            type_counts = dict(session.execute(text("""
                SELECT task_json::json->>'task_type', count(*)
                FROM management.mps_task_definitions WHERE manifest_id = :manifest
                GROUP BY task_json::json->>'task_type'
            """), {"manifest": manifest_id}).all())
            routing = definition_routing(session, manifest_id)
            config = json.loads(source.job_config_json)
            config.update(json.loads(stage.job_config_json))
            config.update(
                state="pending", started_at=None, completed_at=None,
                tasks=[], task_ids=[], total_tasks=total, completed_tasks=0, failed_tasks=0,
            )
            metadata = config.setdefault("metadata", {})
            metadata.update(
                mps_config_revision=int(metadata.get("mps_config_revision", 0)) + 1,
                mps_task_manifest=manifest_id, mps_task_type_counts=type_counts,
                total_tasks_count_override=total,
                required_capabilities=routing["required_capabilities"],
                ip_lock_key=routing["ip_lock_key"] or metadata.get("ip_lock_key"),
            )
            schedule = JobSchedule.from_dict(config.get("schedule")).to_dict()
            pending = any(schedule.values())
            metadata.update(mps_schedule_pending=pending, source="scheduled" if pending else "manual")
            from ..job_system.runtime_admission import validate_durable_configuration

            validate_durable_configuration(job_id, config)
            source.job_config_json = json.dumps(config)
            source.selection_json = json.dumps({"task_manifest_id": manifest_id})
            source.summary_json = json.dumps({"candidate_count": total, "explicit_tasks": True})
            source.total, source.cursor, source.completed, source.failed = total, 0, 0, 0
            source.status = "ready"
            source.updated_at = datetime.now(timezone.utc)
            stage.status = "definition"
            stage.updated_at = source.updated_at
            session.execute(delete(MPSCandidateRangeModel).where(MPSCandidateRangeModel.job_id == job_id))
            # Compressed jobs keep their discovered child definitions directly
            # under the source plan; retire that suffix with the old ranges.
            session.execute(delete(MPSTaskDefinitionModel).where(MPSTaskDefinitionModel.manifest_id == job_id))
            session.execute(delete(CentralTaskModel).where(CentralTaskModel.job_id == job_id))
            session.execute(update(CentralJobModel).where(CentralJobModel.job_id == job_id).values(
                status="pending", started_at=None, completed_at=None, worker_id=None,
                result_success=None, result_message=None, result_error=None,
                result_data_json=None, result_completed_at=None,
            ))
            session.execute(text("""
                UPDATE management.jobs SET args_json =
                  ((args_json::jsonb - 'tasks') || CAST(:changes AS jsonb))::text
                WHERE job_id = :job_id
            """), {"job_id": job_id, "changes": json.dumps({
                "job_name": config["name"], "schedule": schedule, "task_count": total,
                "task_types": list(type_counts), "mps_manifest": True,
                "mps_task_manifest": manifest_id,
            })})
            # Only the committed definition remains owned by the job. Older
            # uploads cannot later activate, and deletion of the job cascades
            # through all remaining definition rows via the ownership FK.
            session.execute(delete(MPSPlanModel).where(
                MPSPlanModel.parent_job_id == job_id,
                MPSPlanModel.job_id != manifest_id,
            ))
            snapshot = self.manifests._snapshot(source)
            if db_session is None:
                session.commit()
            return snapshot

    def page(
        self, manifest_id: str, *, offset: int, limit: int,
        db_session: Any | None = None,
    ) -> list[dict[str, Any]]:
        from .mps_manifest import _positive_int

        _positive_int(limit, "limit")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        model = MPSTaskDefinitionModel
        with self._session(db_session) as session:
            rows = session.execute(select(
                model.ordinal, model.task_id, model.task_json, model.status,
            ).where(model.manifest_id == manifest_id, model.ordinal >= offset)
                .order_by(model.ordinal).limit(limit))
            return [dict(ordinal=row.ordinal, task_id=row.task_id,
                         task=json.loads(row.task_json), status=row.status) for row in rows]

    def ready_page(
        self, manifest_id: str, *, limit: int, byte_limit: int | None = None,
        db_session: Any | None = None,
        exclude_task_ids: Sequence[str] = (),
        only_task_ids: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Read an ordered prefix of ready tasks within count and byte bounds.

        Metadata and payload lookups share one SQL snapshot. Only fitting rows
        execute the payload subquery; rejected JSON never enters the client.
        Dependencies may cross windows or appear later in the submitted list.
        """
        from .mps_manifest import _positive_int

        _positive_int(limit, "limit")
        if byte_limit is not None:
            _positive_int(byte_limit, "byte_limit")
        with self._session(db_session) as session:
            # Resolve the immutable history owner before planning the page.
            # A join-derived partition key makes PostgreSQL lock all history
            # leaves even when execution only needs this job's dependencies.
            history_job_id = session.scalar(select(func.coalesce(
                MPSPlanModel.parent_job_id, MPSPlanModel.job_id,
            )).where(MPSPlanModel.job_id == manifest_id))
            if history_job_id is None:
                return []
            rows = session.execute(text("""
                WITH ready AS MATERIALIZED (
                    SELECT node.ordinal, node.task_id,
                      octet_length(node.task_json) AS task_bytes,
                      EXISTS (
                        SELECT 1 FROM unnest(node.dependency_ids) AS dependency(id)
                        LEFT JOIN management.mps_task_definitions AS target
                          ON target.manifest_id=node.manifest_id AND target.task_id=dependency.id
                        LEFT JOIN management.job_tasks AS history
                          ON history.job_id=:history_job_id
                            AND history.task_id=dependency.id
                        WHERE COALESCE(target.status, history.status, 'pending')='failed'
                      ) AS dependency_failed
                    FROM management.mps_task_definitions AS node
                    WHERE node.manifest_id=:manifest AND node.status='pending'
                      AND node.task_id <> ALL(CAST(:excluded AS text[]))
                      AND (CAST(:selected AS text[]) IS NULL
                        OR node.task_id = ANY(CAST(:selected AS text[])))
                      AND NOT EXISTS (
                        SELECT 1 FROM unnest(node.dependency_ids) AS dependency(id)
                        LEFT JOIN management.mps_task_definitions AS target
                          ON target.manifest_id=node.manifest_id AND target.task_id=dependency.id
                        LEFT JOIN management.job_tasks AS history
                          ON history.job_id=:history_job_id
                            AND history.task_id=dependency.id
                        WHERE COALESCE(target.status, history.status, 'pending') NOT IN ('completed', 'failed')
                      )
                    ORDER BY node.ordinal LIMIT :limit
                ), sized AS (
                    SELECT *, sum(task_bytes) OVER (
                        ORDER BY ordinal ROWS UNBOUNDED PRECEDING
                    ) AS bytes_through_row FROM ready
                )
                SELECT ordinal, task_id, task_bytes, dependency_failed,
                    CASE WHEN CAST(:byte_limit AS bigint) IS NULL
                        OR bytes_through_row <= CAST(:byte_limit AS bigint)
                    THEN (
                        SELECT payload.task_json
                        FROM management.mps_task_definitions AS payload
                        WHERE payload.manifest_id=:manifest AND payload.task_id=sized.task_id
                    ) END AS task_json
                FROM sized ORDER BY ordinal
            """), {"manifest": manifest_id, "limit": limit, "byte_limit": byte_limit,
                   "history_job_id": history_job_id,
                   "excluded": list(exclude_task_ids),
                   "selected": None if only_task_ids is None else list(only_task_ids)})
            result = []
            for row in rows:
                if row.task_json is None:
                    if not result:
                        assert byte_limit is not None
                        raise MPSDefinitionBudgetExceeded(row.task_id, int(row.task_bytes), byte_limit)
                    break
                task = json.loads(row.task_json)
                retry_missing_child = "_mps_retry_waiting_for" in task
                result.append(dict(
                    ordinal=row.ordinal, task_id=row.task_id, task=task,
                    dependency_failed=row.dependency_failed or retry_missing_child,
                    retry_missing_child=retry_missing_child,
                ))
            return result
