"""Synchronous SQL access to central job and task rows."""

from __future__ import annotations

from typing import TYPE_CHECKING
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
import json
from typing import Any, Mapping, Sequence, cast
from ...job_system import JobStatus, Task, TaskType
from ...job_system.payloads import ScrapeTaskPayload
from ...job_system.resources import task_insert_max_bytes as _central_task_insert_max_bytes
from .job_schemas import TaskDetail
from .job_views import ACTIVE_LIVE_JOB_STATUSES, ACTIVE_MATERIALIZATION_STATUSES, GHOST_JOB_ERROR, _central_task_row_to_detail, _enum_value, _optional_enum_value
from .job_template_requests import SupportedSiteName, _normalize_site_name

if TYPE_CHECKING:
    pass


def _mark_central_jobs_failed_sync(
    job_ids: Sequence[str],
    *,
    error_message: str = GHOST_JOB_ERROR,
) -> None:
    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        global_session,
    )

    unique_ids = list(dict.fromkeys(job_ids))
    if not unique_ids:
        return

    failure_time = datetime.now(timezone.utc)
    with global_session() as session:
        from sqlmodel import select as sql_select

        rows = session.exec(
            sql_select(CentralJobModel).where(CentralJobModel.job_id.in_(unique_ids))
        ).all()

        updated = False
        for row in rows:
            if (
                row.status == JobStatus.FAILED.value
                and row.result_error == error_message
            ):
                continue
            row.status = JobStatus.FAILED.value
            row.result_success = False
            row.result_error = error_message
            row.completed_at = row.completed_at or failure_time
            row.result_completed_at = failure_time
            row.last_event_at = failure_time
            updated = True
        if updated:
            session.commit()


def _restore_central_ghost_jobs_sync(job_statuses: Mapping[str, str]) -> None:
    """Clear a stale ghost verdict once live Redis proves the job is real."""

    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        global_session,
    )

    normalized_statuses = {
        job_id: _enum_value(status).lower()
        for job_id, status in job_statuses.items()
        if _enum_value(status).lower() in ACTIVE_LIVE_JOB_STATUSES
    }
    if not normalized_statuses:
        return

    with global_session() as session:
        from sqlmodel import select as sql_select

        rows = session.exec(
            sql_select(CentralJobModel).where(
                CentralJobModel.job_id.in_(list(normalized_statuses))
            )
        ).all()

        updated = False
        for row in rows:
            if row.result_error != GHOST_JOB_ERROR:
                continue
            row.status = normalized_statuses[row.job_id]
            row.result_success = None
            row.result_error = None
            row.completed_at = None
            row.result_completed_at = None
            row.last_event_at = datetime.now(timezone.utc)
            updated = True
        if updated:
            session.commit()


def _normalize_supported_site_name(site: object) -> SupportedSiteName:
    return cast(SupportedSiteName, _normalize_site_name(str(site)))


CENTRAL_TASK_INSERT_BATCH_SIZE = 1000


def _task_to_central_row(job_id: str, task: Task) -> dict[str, Any]:
    task_type_value = _enum_value(task.task_type)
    if (
        task.task_type == TaskType.SCRAPE
        and isinstance(task.payload, ScrapeTaskPayload)
        and task.payload.paid_content_feed
    ):
        task_type_value = "scrape_paid_content"
    return {
        "task_id": task.task_id,
        "job_id": job_id,
        "task_type": task_type_value,
        "status": _enum_value(task.state),
        "site": task.site_name,
        "username": task.user_username,
        "content_type": _optional_enum_value(task.content_type),
        "auth_id": task.auth_id,
        "definition_json": (json.dumps(task.to_dict(include_result=False))
                            if task.mps_task_manifest is None else None),
    }


def _central_task_insert_batch_size(value: int | None = None) -> int:
    from ...database.write_queue import _positive_int_env

    return _positive_int_env(
        "ULTIMA_JOB_TASK_INSERT_BATCH_SIZE", CENTRAL_TASK_INSERT_BATCH_SIZE, value=value
    )


def _central_task_row_batches(
    *, job_id: str, tasks: Iterable[Task], batch_size: int,
    max_bytes: int | None = None,
) -> Iterator[list[dict[str, Any]]]:
    from ...job_system.storage import TaskPayloadBudgetExceeded

    limit = _central_task_insert_max_bytes(max_bytes)
    rows: list[dict[str, Any]] = []
    used = 0
    for task in tasks:
        row = _task_to_central_row(job_id, task)
        size = sum(len(str(value).encode("utf-8")) for value in row.values() if value is not None)
        if size > limit:
            raise TaskPayloadBudgetExceeded(task.task_id, size, limit, phase="SQL input insert")
        if rows and (len(rows) >= batch_size or used + size > limit):
            yield rows
            rows, used = [], 0
        rows.append(row)
        used += size
    if rows:
        yield rows


def _fetch_central_task_details_sync(
    job_id: str,
    *,
    offset: int = 0,
    limit: int | None = None,
) -> list[TaskDetail]:
    from sqlmodel import select as sql_select

    from ultima_scraper_db_archive.global_db import (
        CentralTaskModel,
        global_session,
    )

    with global_session() as session:
        query = (
            sql_select(CentralTaskModel)
            .where(CentralTaskModel.job_id == job_id)
            .order_by(CentralTaskModel.created_at.asc(), CentralTaskModel.task_id.asc())
            .offset(offset)
        )
        if limit is not None:
            query = query.limit(limit)

        rows = list(session.exec(query).all())

    return [_central_task_row_to_detail(row) for row in rows]


def _fetch_central_task_detail_sync(
    job_id: str,
    task_id: str,
) -> TaskDetail | None:
    """Load one archived task without scanning the job's task history."""

    from ultima_scraper_db_archive.global_db import global_session
    from ...database.job_history import task_records
    from .mps_history import record_detail

    records = task_records(global_session, job_id, [task_id])
    return record_detail(records[0]) if records else None


def _job_requires_cancellation_before_delete(
    job: object | None,
    *,
    mps_plan_status: str | None = None,
) -> bool:
    """Return whether deletion must first stop live or staging work."""
    terminal_states = {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
    }
    if job is not None:
        state = _enum_value(getattr(job, "state", None)).lower()
        if state not in terminal_states:
            return True
        metadata = getattr(job, "metadata", {}) or {}
        # Cancellation is terminal for the job control plane.  A materializer
        # may leave its progress field active briefly, but calling cancel_job
        # again would raise "Job is already cancelled" and prevent deletion.
        if (
            state != JobStatus.CANCELLED.value
            and _enum_value(metadata.get("materialization_status")).lower()
            in ACTIVE_MATERIALIZATION_STATUSES
        ):
            return True
    if mps_plan_status is not None:
        return mps_plan_status.lower() not in {"complete", "cancelled", "failed"}
    return False


def _find_job_ids_older_than_sync(cutoff: datetime) -> list[str]:
    from sqlmodel import select as sql_select
    from ultima_scraper_db_archive.global_db import CentralJobModel, global_session

    with global_session() as session:
        rows = session.exec(
            sql_select(CentralJobModel.job_id)
            .where(CentralJobModel.created_at < cutoff)
            .order_by(CentralJobModel.created_at.asc(), CentralJobModel.job_id.asc())
        ).all()
    return [str(job_id) for job_id in rows]


def _purge_job_tasks_sync(job_id: str, batch_size: int = 25000) -> int:
    """Purge tasks in bounded batches with intermediate commits to avoid locking."""
    import time
    from sqlalchemy import delete as sql_delete, select as sql_select, text
    from ultima_scraper_db_archive.global_db import CentralTaskModel, global_session

    total_deleted = 0
    query = text("""
        WITH batch AS (
            SELECT tableoid,ctid FROM management.job_tasks
            WHERE job_id = :job_id
            LIMIT :limit
        )
        DELETE FROM management.job_tasks
        WHERE job_id=:job_id AND (tableoid,ctid) IN (SELECT tableoid,ctid FROM batch)
    """)
    while True:
        with global_session() as session:
            from ...database.job_partitions import drop_history_partition, purge_identity_batch
            drop_history_partition(session, job_id, "job_tasks", allow_orphan=True)
            try:
                with session.begin_nested():
                    count = session.execute(query, {"job_id": job_id, "limit": batch_size}).rowcount
            except Exception:
                subq = session.execute(
                    sql_select(CentralTaskModel.task_id).where(CentralTaskModel.job_id == job_id).limit(batch_size)
                ).scalars().all()
                count = session.execute(
                    sql_delete(CentralTaskModel).where(CentralTaskModel.job_id == job_id, CentralTaskModel.task_id.in_(subq))
                ).rowcount
            if count < batch_size:
                identities, remaining = purge_identity_batch(session, "job_tasks", job_id, batch_size)
                count += identities
            else:
                remaining = True
            session.commit()
            total_deleted += count
            if count < batch_size and not remaining:
                break
            time.sleep(0.01)
    return total_deleted
