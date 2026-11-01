"""Job management API routes.

Provides CRUD operations for jobs:
- Create new jobs
- List jobs with filtering
- Get job details
- Update job status
- Delete jobs
"""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
from collections.abc import AsyncIterable, Iterable
from contextlib import aclosing, asynccontextmanager
from datetime import datetime, timedelta, timezone
from itertools import batched
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, cast

from ...database.threads import run_database_call
from ..job_update_body import read_job_update, request_body_schema
from .job_views import (
    ACTIVE_LIVE_JOB_STATUSES,
    ACTIVE_MATERIALIZATION_STATUSES,
    GHOST_JOB_ERROR,
    _build_error_stats_from_task_details,
    _build_task_page_response,
    _build_task_stats_model,
    _central_task_row_to_detail,
    _coerce_iso_datetime,
    _durable_task_stats,
    _empty_task_page,
    _enum_value,
    _event_task_sort_key,
    _event_task_state_to_detail,
    _extract_int,
    _first_int,
    _is_stale_incomplete_active_job,
    _job_to_response,
    _merge_event_task_metadata,
    _normalize_error_group,
    _normalize_task_type_filter,
    _optional_enum_value,
    _resolve_task_download_file,
    _task_detail_matches_filters,
    _task_result_output,
    _ui_task_type,
)
from .job_template_requests import (
    ALL_SCRAPABLE_TYPES,
    CONTENT_TYPE_MAP,
    _build_schedule_from_request,
    _coerce_json_mapping,
    _normalize_bool,
    _normalize_content_type,
    _normalize_max_items,
    _normalize_nonnegative_int,
    _normalize_paid_feed_content_types,
    _parameter_definitions_from_payloads,
    _payload_to_template,
    _payload_to_template_payload,
    _template_to_response,
)
from .job_central_store import (
    _central_task_insert_batch_size,
    _central_task_row_batches,
    _fetch_central_task_detail_sync,
    _fetch_central_task_details_sync,
    _find_job_ids_older_than_sync,
    _job_requires_cancellation_before_delete,
    _mark_central_jobs_failed_sync,
    _normalize_supported_site_name,
    _purge_job_tasks_sync,
    _restore_central_ghost_jobs_sync,
)
from .job_schemas import (
    JsonValue,
    TaskPayload,
    CreateJobRequest,
    CreateJobTemplateRequest,
    UpdateJobRequest,
    RetryJobRequest,
    DeleteJobsRequest,
    DeleteJobFailure,
    DeleteJobsResponse,
    TaskStats,
    JobErrorStats,
    JobResponse,
    TaskDetail,
    ActiveTaskDetail,
    ActiveTaskListResponse,
    JobRuntimeSamplesPage,
    JobDetailResponse,
    TaskPageResponse,
    JobTemplateResponse,
    JobTemplateCreateBase,
    CreateJobTemplateFromTasksRequest,
    CreateJobTemplateFromJobRequest,
    TemplateEntry,
    InstantiateTemplateRequest,
    JobListResponse,
)

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import insert, or_
from sqlalchemy.exc import SQLAlchemyError
from ultima_scraper_api import ContentType

from ...config import UltimaScraperCollectionConfig
from ...database.write_queue import DatabaseWriteOperation, stable_command_id
from ...helpers import resolve_user_identifier
from ...helpers.background_tasks import spawn_background_task
from ...job_system import (
    Job,
    JobManager,
    JobStatus,
    JobStorage,
    Task,
    TaskState,
    TaskType,
    build_job,
    build_task,
    get_subscribed_auths,
)
from ...job_system import mps_state
from ...job_system.event_log import EventLog, EventType
from ...job_system.models import JobConfig, JobSource
from ...job_system.payloads import DownloadTaskPayload, ScrapeTaskPayload
from ...job_system.storage import TaskLoadLimitExceeded
from ...job_system.mps_attempt import StaleTaskDefinition
from ...job_system.templates import (
    JobTemplate,
    JobTemplateStore,
    JobTemplateTaskDefinition,
)
from ...manager import BackendManager
from ...models.backend import BackendInstance
from ..dependencies import get_backend_instance, get_backend_manager, get_config
from ...redis_events import publish_ws_event

if TYPE_CHECKING:

    from ...job_system.models import Task as JobSystemTask
else:
    JobSystemTask = Task

logger = logging.getLogger(__name__)


def _raise_archived_terminal_job_payload_error(action: str) -> None:
    raise HTTPException(
        status_code=409,
        detail=(
            "Completed/cancelled job tasks were archived from Redis; "
            f"{action} requires live Redis task payloads"
        ),
    )


router = APIRouter(prefix="/jobs", tags=["jobs"])
MAX_INLINE_TASK_LOAD = JobStorage.DEFAULT_MAX_INCLUDE_TASKS

# ── Shared constants ────────────────────────────────────────────────────


def _redis_zrange_has_members(raw_members: object) -> bool:
    return isinstance(raw_members, list) and len(raw_members) > 0


def _fetch_central_error_stats_sync(job_id: str) -> JobErrorStats:
    from sqlalchemy import func as sa_func
    from sqlmodel import select as sql_select

    from ultima_scraper_db_archive.global_db import (
        CentralTaskModel,
        global_session,
    )

    with global_session() as session:
        rows = cast(
            list[tuple[str | None, int]],
            session.exec(
                sql_select(CentralTaskModel.error, sa_func.count())
                .where(
                    CentralTaskModel.job_id == job_id,
                    CentralTaskModel.error.is_not(None),
                )
                .group_by(CentralTaskModel.error)
            ).all(),
        )

    groups: dict[str, int] = {}
    total = 0
    for error, count in rows:
        normalized = _normalize_error_group(error)
        if normalized is None:
            continue
        groups[normalized] = groups.get(normalized, 0) + int(count)
        total += int(count)
    return JobErrorStats(total=total, groups=groups)


async def _mark_central_jobs_failed(
    job_ids: Sequence[str],
    *,
    error_message: str = GHOST_JOB_ERROR,
) -> None:
    if not job_ids:
        return
    try:
        await asyncio.to_thread(
            _mark_central_jobs_failed_sync,
            job_ids,
            error_message=error_message,
        )
    except Exception as exc:
        logger.warning("Failed to mark ghost jobs as failed in central DB: %s", exc)


async def _restore_central_ghost_jobs(job_statuses: Mapping[str, str]) -> None:
    if not job_statuses:
        return
    try:
        await asyncio.to_thread(_restore_central_ghost_jobs_sync, job_statuses)
    except Exception as exc:
        logger.warning("Failed to restore recovered ghost jobs in central DB: %s", exc)


async def _redis_zrevrange(
    redis_manager: object, name: str, start: int, end: int
) -> list[str | tuple[str, float]]:
    method = getattr(redis_manager, "zrevrange", None)
    if callable(method):
        result = cast(Any, method)(name, start, end)
        return cast(list[str | tuple[str, float]], await result)
    client = getattr(redis_manager, "client", None)
    if client is None:
        return []
    return cast(list[str | tuple[str, float]], await client.zrevrange(name, start, end))


async def _redis_pipeline_execute(
    redis_manager: object,
    *,
    hgetall_keys: Sequence[str] = (),
    get_keys: Sequence[str] = (),
    zrange_specs: Sequence[tuple[str, int, int]] = (),
) -> list[object]:
    client = getattr(redis_manager, "client", None)
    if client is None:
        return []
    pipe = client.pipeline()
    for key in hgetall_keys:
        pipe.hgetall(key)
    for key in get_keys:
        pipe.get(key)
    for key, start, end in zrange_specs:
        pipe.zrange(key, start, end)
    return cast(list[object], await pipe.execute())


from ...job_system.resources import task_insert_max_bytes as _central_task_insert_max_bytes


def _persist_central_job_submission_sync(
    *,
    job_id: str,
    job_type: str,
    status: str,
    site: str,
    args_payload: dict[str, Any],
    tasks: Iterable[Task],
    job_config: dict[str, Any] | None = None,
    batch_size: int | None = None,
    max_bytes: int | None = None,
    windowed: bool = False,
    build_tasks: bool = False,
) -> dict[str, Any] | None:
    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        CentralTaskModel,
        global_session,
    )

    effective_batch_size = _central_task_insert_batch_size(batch_size)
    effective_max_bytes = _central_task_insert_max_bytes(max_bytes)
    if windowed:
        from ...database.task_admission import prepare_task_plan

        if job_config is None:
            raise ValueError("Durable task admission requires the job configuration")
        return prepare_task_plan(
            job_id=job_id, job_type=job_type, site=site,
            job_config=job_config, args_payload=args_payload, tasks=tasks,
            batch_size=effective_batch_size, max_bytes=effective_max_bytes,
            build_tasks=build_tasks,
        )
    with global_session() as session:
        try:
            existing = session.get(CentralJobModel, job_id)
            args_json = json.dumps(args_payload)
            if existing is None:
                existing = CentralJobModel(
                    job_id=job_id,
                    job_type=job_type,
                    status=status,
                    args_json=args_json,
                    site=site,
                )
            else:
                existing.job_type = job_type
                existing.status = status
                existing.args_json = args_json
                existing.site = site
            if job_config is not None:
                existing.job_config_json = json.dumps(job_config)
            session.add(existing)
            for rows in _central_task_row_batches(
                job_id=job_id, tasks=tasks, batch_size=effective_batch_size, max_bytes=effective_max_bytes
            ):
                session.execute(insert(CentralTaskModel), rows)
            # Recovery measures inactivity from publication, even when building
            # the bounded batches took longer than its normal grace period.
            existing.last_event_at = datetime.now(timezone.utc)
            session.commit()
        except Exception:
            session.rollback()
            raise


async def _persist_central_job_submission(
    *,
    job_id: str,
    job_type: str,
    site: str,
    args_payload: dict[str, Any],
    tasks: Iterable[Task],
    job_config: dict[str, Any] | None = None,
    windowed: bool = False,
    build_tasks: bool = False,
) -> dict[str, Any] | None:
    return await run_database_call(
        _persist_central_job_submission_sync,
        job_id=job_id,
        job_type=job_type,
        status=JobStatus.PENDING.value,
        site=site,
        args_payload=args_payload,
        tasks=tasks,
        job_config=job_config,
        windowed=windowed,
        build_tasks=build_tasks,
    )


def _task_progress(task: Task | Any) -> tuple[float, int, int | None]:
    """Calculate task progress from result metrics/output.

    Works with both domain.Task and job_system.models.Task types.
    """
    items_processed = 0
    items_total: int | None = None

    metrics: dict[str, Any] = {}
    output: dict[str, Any] = {}
    if task.result:
        metrics = dict(task.result.metrics or {})
        output = _task_result_output(task)

    processed_candidate = _first_int(
        metrics,
        [
            "items_processed",
            "items_scraped",
            "subscriptions_processed",
            "items_saved",
            "items_downloaded",
            "downloads_completed",
        ],
    )
    if processed_candidate is None:
        processed_candidate = _first_int(
            output,
            [
                "items_processed",
                "items_scraped",
                "subscriptions_processed",
                "items_saved",
                "items_downloaded",
                "downloads_completed",
            ],
        )

    total_candidate = _first_int(
        metrics,
        [
            "items_total",
            "total_items",
            "subscriptions_total",
            "media_count",
            "total_media",
        ],
    )
    if total_candidate is None:
        total_candidate = _first_int(
            output,
            [
                "items_total",
                "total_items",
                "subscriptions_total",
                "media_count",
                "total_media",
            ],
        )

    if processed_candidate is not None:
        items_processed = processed_candidate
    items_total = total_candidate

    if items_total is not None and items_processed > items_total:
        items_total = items_processed

    if items_total is None and items_processed > 0 and task.result:
        if getattr(task.result, "success", False):
            items_total = items_processed

    progress = 0.0
    if items_total and items_total > 0:
        progress = min(items_processed / items_total, 1.0)

    return progress, items_processed, items_total


def _task_to_detail(task: Task) -> TaskDetail:
    """Convert a domain Task object to an API TaskDetail response model."""
    content_type = _optional_enum_value(task.content_type)
    progress, items_processed, items_total = _task_progress(task)

    error_message = None
    if task.result and task.result.errors:
        error_message = task.result.errors[0]
    elif (
        task.result
        and task.result.message
        and task.state in (TaskState.FAILED, TaskState.CANCELLED)
    ):
        error_message = task.result.message

    # Build payload for UI editing - include essential fields
    task_payload: dict[str, JsonValue] = {
        "site": str(task.site_name) if task.site_name else None,
        "username": task.user_username,
    }
    if task.auth_id is not None:
        task_payload["auth_id"] = task.auth_id
    if content_type:
        task_payload["content_types"] = [content_type]

    # Merge in original payload if it exists
    if task.payload:
        try:
            if hasattr(task.payload, "model_dump"):
                original = task.payload.model_dump()
            elif hasattr(task.payload, "dict"):
                original = task.payload.dict()
            else:
                original = dict(task.payload)  # type: ignore

            for key, value in original.items():
                if key not in task_payload:
                    task_payload[key] = value
        except Exception:
            pass

    is_paid_content_feed = (
        isinstance(task.payload, ScrapeTaskPayload) and task.payload.paid_content_feed
    )
    if is_paid_content_feed:
        performer_filter = task_payload.pop("paid_content_performer", None)
        paid_content_types = task_payload.pop("paid_content_types", None)
        task_payload.pop("paid_content_feed", None)
        if performer_filter is not None:
            task_payload["performer"] = performer_filter
        if paid_content_types is not None:
            task_payload["content_types"] = paid_content_types

    # Map stored TaskType back to UI task type
    ui_task_type = _ui_task_type(
        task.task_type.value,
        content_type=content_type,
        is_paid_content_feed=is_paid_content_feed,
    )

    # Resolve proxy routing and used hop
    proxy_url = None
    proxy_routes: list[str | None] | None = None
    proxy_hop_used: int | None = None

    result_dict = task.result.to_dict() if task.result is not None else None
    result_metrics = (result_dict or {}).get("metrics") or {}
    result_output = (result_dict or {}).get("output") or {}

    if isinstance(result_metrics, dict) and "proxy_url" in result_metrics:
        proxy_url = result_metrics.get("proxy_url")
    elif isinstance(result_output, dict) and "proxy_url" in result_output:
        proxy_url = result_output.get("proxy_url")
    elif isinstance(result_output, dict) and "resolved_route_url" in result_output:
        proxy_url = result_output.get("resolved_route_url")
    elif isinstance(result_metrics, dict) and "resolved_route_url" in result_metrics:
        proxy_url = result_metrics.get("resolved_route_url")
    elif "guest_proxy_url" in task_payload:
        proxy_url = cast(Optional[str], task_payload.get("guest_proxy_url"))
    elif "proxy_url" in task_payload:
        proxy_url = cast(Optional[str], task_payload.get("proxy_url"))

    if isinstance(result_output, dict) and "proxy_routes" in result_output:
        proxy_routes = cast(Optional[list[Optional[str]]], result_output.get("proxy_routes"))
    elif isinstance(result_metrics, dict) and "proxy_routes" in result_metrics:
        proxy_routes = cast(Optional[list[Optional[str]]], result_metrics.get("proxy_routes"))
    elif "guest_proxy_url" in task_payload or "guest_fallback_proxy_urls" in task_payload:
        from ...job_system.handlers.scrape.handler import _build_guest_proxy_routes

        g_proxy = task_payload.get("guest_proxy_url")
        g_fallbacks = task_payload.get("guest_fallback_proxy_urls") or []
        g_direct_idx = task_payload.get("guest_direct_route_index")
        proxy_routes = _build_guest_proxy_routes(
            guest_proxy_url=str(g_proxy) if g_proxy else None,
            guest_fallback_proxy_urls=[str(f) for f in g_fallbacks if f],
            guest_direct_route_index=(
                int(g_direct_idx) if g_direct_idx is not None else None
            ),
        )

    if isinstance(result_metrics, dict) and "proxy_hop_used" in result_metrics:
        proxy_hop_used = result_metrics.get("proxy_hop_used")
    elif isinstance(result_output, dict) and "proxy_hop_used" in result_output:
        proxy_hop_used = result_output.get("proxy_hop_used")
    elif isinstance(result_output, dict) and "resolved_route_index" in result_output:
        proxy_hop_used = result_output.get("resolved_route_index")
    elif isinstance(result_metrics, dict) and "resolved_route_index" in result_metrics:
        proxy_hop_used = result_metrics.get("resolved_route_index")

    if proxy_hop_used is not None and proxy_routes and 0 <= int(proxy_hop_used) < len(proxy_routes):
        proxy_url = proxy_routes[int(proxy_hop_used)]
    elif proxy_routes and proxy_url is not None and proxy_hop_used is None:
        try:
            proxy_hop_used = proxy_routes.index(proxy_url)
        except ValueError:
            proxy_hop_used = None

    return TaskDetail(
        task_id=task.task_id,
        task_type=ui_task_type,
        status=task.state.value,
        site=(str(task.site_name) if task.site_name else None),
        username=task.user_username,
        content_type=content_type,
        payload=task_payload,
        worker_id=task.worker_id,
        started_at=task.started_at,
        completed_at=task.completed_at,
        progress=progress,
        items_processed=items_processed,
        items_total=items_total,
        error=error_message,
        result=result_dict,
        proxy_url=str(proxy_url) if proxy_url is not None else None,
        proxy_routes=proxy_routes,
        proxy_hop_used=int(proxy_hop_used) if proxy_hop_used is not None else None,
    )


def _fetch_durable_headers_sync(job_ids: list[str]) -> dict[str, dict[str, Any]]:
    from contextlib import nullcontext

    from ultima_scraper_db_archive.global_db import global_session
    from ...database.job_history import headers
    from ...job_system.ledger import ExecutionLedger, owns

    if not job_ids:
        return {}
    ledger = ExecutionLedger()
    # Keep one bounded request-scoped checkout even with NullPool/session
    # pooling. Do not hold it while doing Redis, network or response work.
    with global_session() as session:
        output = headers(lambda: nullcontext(session), job_ids)
        owned = [job_id for job_id, header in output.items() if owns(header["config"])]
        for offset in range(0, len(owned), 256):
            batch = owned[offset:offset + 256]
            statistics = ledger.stats_many(batch, db_session=session)
            configs = ledger.jobs_many(batch, db_session=session, statistics=statistics)
            for job_id, config in configs.items():
                stats, header = statistics[job_id], output[job_id]
                header.update(
                    config=config,
                    status="deleting" if config.get("metadata", {}).get("deletion_pending") else config["state"],
                    total=stats["total"], completed=stats["completed"],
                    failed=stats["failed"], execution_stats=stats,
                )
    return output


def _read_task_plan_sync(job_id: str) -> dict[str, Any] | None:
    from ...database.mps_manifest import MPSManifestStore

    return MPSManifestStore().get(job_id)


async def _fetch_job_task_detail(
    job_id: str,
    task_id: str,
    backend: BackendInstance,
) -> TaskDetail | None:
    """Resolve a task from live Redis state or its archived database row."""

    plan = await run_database_call(_read_task_plan_sync, job_id)
    if plan is not None:
        from .mps_history import task_detail

        return await task_detail(JobStorage(backend.redis) if backend.redis is not None else None, plan, task_id)
    if backend.redis is not None:
        storage = JobStorage(cast(Any, backend.redis))
        from ...database.job_history import history_byte_limit

        try:
            tasks, _ = await storage._fetch_task_prefix_by_ids(job_id, [task_id], max_bytes=history_byte_limit())
        except TaskLoadLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        if tasks and tasks[0] is not None:
            return _task_to_detail(tasks[0])
    try:
        return await run_database_call(_fetch_central_task_detail_sync, job_id, task_id)
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc


def _fetch_central_task_page_sync(
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
    state: str | None = None,
    task_type: str | None = None,
    error_only: bool = False,
) -> TaskPageResponse:
    from sqlmodel import select as sql_select

    from ultima_scraper_db_archive.global_db import (
        CentralTaskModel,
        global_session,
    )

    normalized_state = state.strip().lower() if state else None
    normalized_task_type = _normalize_task_type_filter(task_type)

    with global_session() as session:
        query = sql_select(CentralTaskModel).where(CentralTaskModel.job_id == job_id)
        if normalized_state:
            query = query.where(CentralTaskModel.status == normalized_state)
        if normalized_task_type:
            query = query.where(CentralTaskModel.task_type == normalized_task_type)
        if error_only:
            query = query.where(
                or_(
                    CentralTaskModel.error.is_not(None),
                    CentralTaskModel.status == TaskState.FAILED.value,
                    CentralTaskModel.status == TaskState.CANCELLED.value,
                )
            )
        query = (
            query.order_by(
                CentralTaskModel.created_at.asc(), CentralTaskModel.task_id.asc()
            )
            .offset(offset)
            .limit(limit + 1)
        )
        rows = list(session.exec(query).all())

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    next_cursor = str(offset + len(page_rows)) if has_more else None
    return TaskPageResponse(
        tasks=[_central_task_row_to_detail(row) for row in page_rows],
        next_cursor=next_cursor,
        has_more=has_more,
        scanned_count=offset + len(page_rows),
        offset=offset,
        limit=limit,
    )


async def _fetch_event_log_task_page(
    redis_manager: object,
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
    state: str | None = None,
    task_type: str | None = None,
    error_only: bool = False,
) -> TaskPageResponse:
    event_log = EventLog(cast(Any, redis_manager))
    events = await event_log.read_events(job_id)
    if not events:
        return _empty_task_page(offset=offset, limit=limit)

    task_states: dict[str, dict[str, Any]] = {}
    for event in events:
        task_id = event.task_id
        if not task_id:
            continue

        task_state = task_states.setdefault(
            task_id,
            {
                "status": TaskState.PENDING.value,
                "task_type": None,
                "site": None,
                "username": None,
                "content_type": None,
                "auth_id": None,
                "first_event_at": event.timestamp,
                "claimed_at": None,
                "started_at": None,
                "completed_at": None,
                "error": None,
                "worker_id": event.worker_id,
            },
        )
        task_state["worker_id"] = task_state.get("worker_id") or event.worker_id
        _merge_event_task_metadata(task_state, event.data)

        if event.event_type == EventType.TASK_CLAIMED:
            task_state["status"] = "claimed"
            task_state["claimed_at"] = event.timestamp
        elif event.event_type == EventType.TASK_STARTED:
            task_state["status"] = TaskState.RUNNING.value
            task_state["started_at"] = event.timestamp
        elif event.event_type == EventType.TASK_SAVING_DB:
            task_state["status"] = "saving_db"
        elif event.event_type == EventType.TASK_COMPLETED:
            task_state["status"] = TaskState.COMPLETED.value
            task_state["completed_at"] = event.timestamp
            task_state["error"] = None
        elif event.event_type == EventType.TASK_FAILED:
            task_state["status"] = TaskState.FAILED.value
            task_state["completed_at"] = event.timestamp
            task_state["error"] = event.data.get("error")
        elif event.event_type == EventType.TASK_CANCELLED:
            task_state["status"] = TaskState.CANCELLED.value
            task_state["completed_at"] = event.timestamp
            task_state["error"] = event.data.get("error")
        elif event.event_type == EventType.TASK_ORPHANED:
            task_state["status"] = "orphaned"
        elif event.event_type == EventType.TASK_RECOVERED:
            task_state["status"] = TaskState.PENDING.value

    filtered_tasks = [
        _event_task_state_to_detail(task_id, task_state)
        for task_id, task_state in sorted(task_states.items(), key=_event_task_sort_key)
        if _task_detail_matches_filters(
            _event_task_state_to_detail(task_id, task_state),
            state=state,
            task_type=task_type,
            error_only=error_only,
        )
    ]
    return _build_task_page_response(filtered_tasks, offset=offset, limit=limit)


def _tasks_to_template_definitions(
    tasks: Sequence["Task | JobSystemTask"],
) -> list["JobTemplateTaskDefinition"]:
    from ...job_system.models import TaskType as JobSystemTaskType
    from ...job_system.templates import JobTemplateTaskDefinition

    alias_by_task_id = {
        task.task_id: f"task_{index}" for index, task in enumerate(tasks, start=1)
    }

    definitions: list[JobTemplateTaskDefinition] = []
    for task in tasks:
        task_type = JobSystemTaskType(task.task_type.value)
        content_type = _optional_enum_value(task.content_type) or ""
        depends_on = [
            alias_by_task_id[task_id]
            for task_id in task.depends_on
            if task_id in alias_by_task_id
        ]
        missing = [
            task_id for task_id in task.depends_on if task_id not in alias_by_task_id
        ]
        if missing:
            logger.warning(
                "Template creation skipped missing dependencies for task %s: %s",
                task.task_id,
                ", ".join(missing),
            )

        definitions.append(
            JobTemplateTaskDefinition(
                name=alias_by_task_id.get(task.task_id),
                task_type=task_type,
                site_name_template=str(task.site_name),
                user_id_template=str(task.user_id),
                content_type_template=content_type,
                payload_template=_payload_to_template_payload(task.payload),
                capabilities=list(task.capabilities),
                depends_on=depends_on,
                ip_lock_key_template=task.ip_lock_key,
                max_attempts=task.max_attempts,
                dedupe_override=False,
            )
        )

    return definitions


def _build_template_from_request(
    request: JobTemplateCreateBase,
    tasks: Sequence["Task | JobSystemTask"],
    *,
    metadata: dict[str, JsonValue],
) -> "JobTemplate":
    from ...job_system.templates import BackendStrategy, JobTemplate, RetryPolicy

    retry_policy = (
        RetryPolicy(
            attempts=request.retry_policy.attempts,
            backoff_seconds=request.retry_policy.backoff_seconds,
            max_backoff_seconds=request.retry_policy.max_backoff_seconds,
        )
        if request.retry_policy
        else RetryPolicy()
    )
    backend_strategy = (
        BackendStrategy(
            mode=request.backend_strategy.mode,
            backend_id=request.backend_strategy.backend_id,
        )
        if request.backend_strategy
        else BackendStrategy()
    )

    return JobTemplate(
        name=request.template_name,
        description=request.description,
        created_by=request.created_by,
        tasks=_tasks_to_template_definitions(tasks),
        parameters=_parameter_definitions_from_payloads(request.parameters),
        default_priority=request.default_priority,
        retry_policy=retry_policy,
        backend_strategy=backend_strategy,
        job_name_template=request.job_name_template,
        default_schedule_at=request.default_schedule_at,
        metadata=metadata,
    )


def _build_template_tasks(tasks: list[Task]) -> list[JobTemplateTaskDefinition]:
    alias_by_id = {
        task.task_id: f"task_{index + 1}" for index, task in enumerate(tasks)
    }
    template_tasks: list[JobTemplateTaskDefinition] = []

    for task in tasks:
        depends_on: list[str] = []
        for dependency in task.depends_on:
            alias = alias_by_id.get(dependency)
            if alias:
                depends_on.append(alias)
            else:
                logger.warning(
                    "Missing task dependency %s while building template", dependency
                )

        content_type_template = _optional_enum_value(task.content_type) or ""

        template_tasks.append(
            JobTemplateTaskDefinition(
                name=alias_by_id.get(task.task_id),
                task_type=task.task_type,
                site_name_template=str(task.site_name),
                user_id_template=str(task.user_id),
                user_username_template=task.user_username,
                auth_id_template=(
                    str(task.auth_id) if task.auth_id is not None else None
                ),
                content_type_template=content_type_template,
                payload_template=_payload_to_template(task.payload),
                capabilities=list(task.capabilities or []),
                depends_on=depends_on,
                ip_lock_key_template=task.ip_lock_key,
                max_attempts=task.max_attempts,
                dedupe_override=False,
            )
        )

    return template_tasks


def _build_job_template(
    *,
    name: str,
    description: str | None,
    created_by: str,
    tasks: list[Task],
    job_name_template: str | None,
) -> JobTemplate:
    return JobTemplate(
        name=name,
        description=description or "",
        created_by=created_by,
        tasks=_build_template_tasks(tasks),
        job_name_template=job_name_template or name,
    )


async def _resolve_auth_from_subscription(
    site_name: str, user_id: int, backend_manager: BackendManager
) -> int | None:
    """
    Resolve auth_id from active subscriptions for a user.

    Looks up which auth accounts are subscribed to the given user and returns
    the first active one. This enables automatic auth selection based on
    subscription relationships.

    Args:
        site_name: Site name (e.g., "onlyfans")
        user_id: The performer's user ID
        backend_manager: Backend manager for database access

    Returns:
        The auth_id of an active subscription, or None if no subscription found
    """
    try:

        def _lookup_subscriptions() -> Sequence[Any]:
            db_manager = backend_manager.multi_site_db_manager
            site_db = db_manager.get_or_create_manager(site_name.lower())
            if not site_db:
                return []
            with site_db.get_session() as session:
                return get_subscribed_auths(session, user_id, include_expired=False)

        # Run sync DB query in thread to avoid blocking the event loop
        subscriptions = await asyncio.to_thread(_lookup_subscriptions)

        if not subscriptions:
            logger.debug(
                f"No active subscriptions found for user {user_id} on {site_name}"
            )
            return None

        # Return first active subscription's auth_id
        # TODO: When multiple auths are subscribed, add UI to let user choose
        first_sub = subscriptions[0]
        logger.info(
            f"Auto-resolved auth_id {first_sub.auth_id} ({first_sub.auth_username}) "
            f"for user {user_id} from subscription"
        )
        return first_sub.auth_id

    except Exception as e:
        logger.error(f"Error resolving auth from subscription: {e}", exc_info=True)
        return None


async def _resolve_auth_from_message_history(
    site_name: str,
    user_id: int,
    backend_manager: BackendManager,
) -> int | None:
    """Resolve auth_id from local message history for a chat user.

    Message scrapes can still be valid for users we are not actively subscribed to
    when one of our auth accounts has an existing conversation with them. In that
    case, select the auth with the most recent message activity.
    """
    try:
        from sqlalchemy import case
        from sqlalchemy import func as sa_func
        from sqlmodel import select as sql_select

        from ultima_scraper_db_archive import MessageModel, UserAuthModel

        def _lookup_message_history() -> list[int]:
            db_manager = backend_manager.multi_site_db_manager
            site_db = db_manager.get_or_create_manager(site_name.lower())
            if not site_db:
                return []

            with site_db.get_session() as session:
                active_auth_ids = [
                    int(auth_user_id)
                    for auth_user_id in session.exec(
                        sql_select(UserAuthModel.user_id).where(
                            UserAuthModel.site == site_name.lower(),
                            UserAuthModel.active.is_(True),
                        )
                    ).all()
                    if auth_user_id is not None
                ]
                if not active_auth_ids:
                    return []

                auth_col = case(
                    (MessageModel.user_id.in_(active_auth_ids), MessageModel.user_id),
                    else_=MessageModel.receiver_id,
                ).label("auth_id")

                rows = session.exec(
                    sql_select(auth_col)
                    .where(
                        or_(
                            (
                                MessageModel.user_id.in_(active_auth_ids)
                                & (MessageModel.receiver_id == user_id)
                            ),
                            (
                                (MessageModel.user_id == user_id)
                                & MessageModel.receiver_id.in_(active_auth_ids)
                            ),
                        )
                    )
                    .group_by(auth_col)
                    .order_by(
                        sa_func.max(MessageModel.created_at).desc(),
                        sa_func.max(MessageModel.id).desc(),
                    )
                ).all()

                return [
                    int(auth_user_id)
                    for auth_user_id in rows
                    if auth_user_id is not None
                ]

        auth_ids = await asyncio.to_thread(_lookup_message_history)
        if not auth_ids:
            logger.debug(
                "No message-history auth found for user %s on %s",
                user_id,
                site_name,
            )
            return None

        selected_auth_id = auth_ids[0]
        logger.info(
            "Auto-resolved auth_id %s for user %s from message history (%d candidate%s)",
            selected_auth_id,
            user_id,
            len(auth_ids),
            "" if len(auth_ids) == 1 else "s",
        )
        return selected_auth_id
    except Exception as exc:
        logger.error(
            "Error resolving auth from message history: %s",
            exc,
            exc_info=True,
        )
        return None


def _is_message_only_request(content_types: Sequence[str]) -> bool:
    normalized = {
        str(content_type).strip().lower()
        for content_type in content_types
        if str(content_type).strip()
    }
    return bool(normalized) and normalized <= {"messages"}


async def _resolve_default_scrape_auth_id(
    *,
    site_name: str,
    user_id: int,
    content_types: Sequence[str],
    backend_manager: BackendManager,
    use_guest_auth: bool = False,
) -> int | None:
    """Resolve the default auth for a scrape task.

    Order of precedence:
    1. Active subscription auth
    2. Existing chat/message history for message-only requests
    """
    if user_id == 0 or use_guest_auth:
        return None

    auth_id = await _resolve_auth_from_subscription(
        site_name,
        user_id,
        backend_manager,
    )
    if auth_id is not None:
        return auth_id

    if _is_message_only_request(content_types):
        return await _resolve_auth_from_message_history(
            site_name,
            user_id,
            backend_manager,
        )

    return None


async def _normalize_auth_id_for_site(
    site_name: str,
    raw_auth_id: object,
    backend_manager: BackendManager,
) -> int | None:
    """Normalize auth identifier to the platform account ID (``user_id``).

    Accepts either ``UserAuthModel.user_id`` or ``UserAuthModel.id`` and returns
    ``user_id`` for task routing.
    """
    parsed_auth_id = _extract_int(raw_auth_id)
    if parsed_auth_id is None:
        logger.warning("Invalid auth_id value: %s", raw_auth_id)
        return None

    account: Any | None = None
    try:
        from sqlmodel import select as sql_select

        from ultima_scraper_db_archive import UserAuthModel

        site_db = backend_manager.multi_site_db_manager.get_or_create_manager(
            site_name.lower()
        )
        if site_db is not None:

            def _resolve_account_exact_first() -> Any | None:
                session = site_db.get_session()
                try:
                    exact_matches = session.exec(
                        sql_select(UserAuthModel).where(
                            UserAuthModel.site == site_name.lower(),
                            UserAuthModel.id == parsed_auth_id,
                        )
                    ).all()
                    if exact_matches:
                        return exact_matches[0]

                    user_matches = session.exec(
                        sql_select(UserAuthModel).where(
                            UserAuthModel.site == site_name.lower(),
                            UserAuthModel.user_id == parsed_auth_id,
                        )
                    ).all()
                    if user_matches:
                        return user_matches[0]
                    return None
                finally:
                    session.close()

            account = await asyncio.to_thread(_resolve_account_exact_first)

        if account is None:
            manager = backend_manager.get_site_db_manager(site_name)
            account = await asyncio.to_thread(manager.get_user_auth, parsed_auth_id)
    except Exception as exc:
        logger.warning(
            "Failed to resolve auth_id %s for site %s: %s",
            parsed_auth_id,
            site_name,
            exc,
        )
        return parsed_auth_id

    if account and account.user_id:
        normalized_auth_id = int(account.user_id)
        if normalized_auth_id != parsed_auth_id:
            logger.info(
                "Normalized auth_id %s -> %s for site %s",
                parsed_auth_id,
                normalized_auth_id,
                site_name,
            )
        return normalized_auth_id

    return parsed_auth_id


async def _build_task_from_payload(
    payload: TaskPayload,
    backend_manager: BackendManager,
    *,
    ignore_space_requirements: bool = False,
) -> Task | list[Task] | AsyncIterable[Task] | None:
    """Convert one input into tasks; account-backed inputs yield bounded pages.

    Args:
        payload: Task payload from API request
        backend_manager: BackendManager instance for user resolution

    Returns:
        Task, bounded task list, async task source, or None if invalid
    """
    params = payload.params
    task_type_str = payload.task_type
    task_ignore_space_requirements = _normalize_bool(
        params.get("ignore_space_requirements"),
        default=ignore_space_requirements,
    )

    # Map task type string to TaskType enum
    if task_type_str == "scrape_and_download":
        # Create both scrape and download tasks
        site_name = _normalize_supported_site_name(params.get("site", "OnlyFans"))
        # Resolve user
        user_id_raw = params.get("user_id")
        username = params.get("username")
        user_id: int | None = None

        if user_id_raw is not None:
            try:
                user_id = int(user_id_raw)
            except (ValueError, TypeError):
                logger.warning(f"Invalid user_id value in params: {user_id_raw}")

        if user_id is None and username:
            # Resolve username to user_id (fallback to 0 if not in database)
            user_id = await resolve_user_identifier(username, backend_manager)
            if user_id is None:
                logger.warning(
                    f"Username '{username}' not found in database. "
                    f"Will create task with user_id=0 to fetch from API first."
                )
                user_id = 0
        elif user_id is None:
            logger.warning("scrape_and_download task missing both user_id and username")
            return None

        raw_content_types = params.get("content_types", ["Posts"])
        content_types: list[str] = []
        if isinstance(raw_content_types, list):
            raw_content_types_list = cast(list[object], raw_content_types)
            for content_type in raw_content_types_list:
                content_types.append(_normalize_content_type(content_type))
        else:
            content_types = [_normalize_content_type(raw_content_types)]

        # Expand 'all' into all scrapable content types
        if "all" in content_types:
            content_types = ALL_SCRAPABLE_TYPES

        max_items = _normalize_max_items(params.get("max_items"))
        rescrape_existing = _normalize_bool(
            params.get("rescrape_existing"), default=True
        )

        # Extract and normalize auth_id from params
        raw_auth_id = params.get("auth_id")
        auth_id: int | None = None
        if raw_auth_id is not None:
            auth_id = await _normalize_auth_id_for_site(
                site_name, raw_auth_id, backend_manager
            )

        if auth_id is None:
            auth_id = await _resolve_default_scrape_auth_id(
                site_name=site_name,
                user_id=user_id,
                content_types=content_types,
                backend_manager=backend_manager,
            )

        # Create tasks for ALL content types in the list
        # Each content type gets a scrape task + download task pair
        all_tasks: list[Task] = []

        for content_type_str in content_types:
            content_type = CONTENT_TYPE_MAP.get(content_type_str, ContentType.POSTS)

            # Create scrape task for this content type
            scrape_task = build_task(
                TaskType.SCRAPE,
                site_name=site_name,
                user_id=user_id,
                user_username=username,
                content_type=content_type,
                download_media=False,  # Don't download during scrape
                max_items=max_items,
                rescrape_existing=rescrape_existing,
                auth_id=auth_id,
                ignore_space_requirements=task_ignore_space_requirements,
            )
            all_tasks.append(scrape_task)

            # Create download task that depends ONLY on its corresponding scrape task
            # This allows downloads to start as soon as their scrape completes
            download_task = build_task(
                TaskType.DOWNLOAD,
                site_name=site_name,
                user_id=user_id,
                user_username=username,
                content_type=content_type,  # Same content type as scrape
                auth_id=auth_id,
                ignore_space_requirements=task_ignore_space_requirements,
            )
            download_task.depends_on = [
                scrape_task.task_id
            ]  # Only depends on its scrape
            all_tasks.append(download_task)

        return all_tasks

    elif task_type_str == "scrape_user":
        site_name = _normalize_supported_site_name(params.get("site", "OnlyFans"))
        user_id_raw = params.get("user_id")
        username = params.get("username")
        user_id: int | None = None

        if user_id_raw is not None:
            try:
                user_id = int(user_id_raw)
            except (ValueError, TypeError):
                logger.warning(f"Invalid user_id value in params: {user_id_raw}")

        if user_id is None and username:
            # Resolve username to user_id (fallback to 0 if not in database)
            user_id = await resolve_user_identifier(username, backend_manager)
            if user_id is None:
                logger.warning(
                    f"Username '{username}' not found in database. "
                    f"Will create task with user_id=0 to fetch from API first."
                )
                user_id = 0
        elif user_id is None:
            logger.warning("scrape_user task missing both user_id and username")
            return None

        # Get content types
        raw_content_types = params.get("content_types", ["Posts"])
        content_types: list[str] = []
        if isinstance(raw_content_types, list):
            raw_content_types_list = cast(list[object], raw_content_types)
            for content_type in raw_content_types_list:
                content_types.append(_normalize_content_type(content_type))
        else:
            content_types = [_normalize_content_type(raw_content_types)]
        download_media = params.get("download_media", True)
        max_items = _normalize_max_items(params.get("max_items"))
        use_guest_auth = _normalize_bool(params.get("use_guest_auth"), default=False)
        guest_proxy_url_raw = params.get("guest_proxy_url")
        guest_proxy_url = (
            str(guest_proxy_url_raw).strip()
            if isinstance(guest_proxy_url_raw, str) and guest_proxy_url_raw.strip()
            else None
        )
        guest_fallback_proxy_urls_raw = params.get("guest_fallback_proxy_urls")
        guest_fallback_proxy_urls: list[str] = []
        if isinstance(guest_fallback_proxy_urls_raw, list):
            for raw_fallback in guest_fallback_proxy_urls_raw:
                if isinstance(raw_fallback, str) and raw_fallback.strip():
                    normalized_fallback = raw_fallback.strip()
                    if (
                        normalized_fallback != guest_proxy_url
                        and normalized_fallback not in guest_fallback_proxy_urls
                    ):
                        guest_fallback_proxy_urls.append(normalized_fallback)
        guest_direct_route_index = _normalize_nonnegative_int(
            params.get("guest_direct_route_index")
        )
        guest_proxy_alternatives: dict[str, list[str]] = {}
        raw_alternatives = params.get("guest_proxy_alternatives")
        if isinstance(raw_alternatives, dict):
            for raw_route, raw_reserves in raw_alternatives.items():
                if not isinstance(raw_route, str) or not raw_route.strip():
                    continue
                if not isinstance(raw_reserves, list):
                    continue
                reserves = list(
                    dict.fromkeys(
                        reserve.strip() for reserve in raw_reserves
                        if isinstance(reserve, str) and reserve.strip()
                    )
                )
                if reserves:
                    guest_proxy_alternatives[raw_route.strip()] = reserves
        track_invalid_user = _normalize_bool(
            params.get("track_invalid_user"), default=False
        )
        conclusive_miss_threshold = _normalize_nonnegative_int(
            params.get("conclusive_miss_threshold")
        )
        max_route_hops = _normalize_nonnegative_int(
            params.get("max_route_hops")
        )

        # Expand 'all' into all scrapable content types
        if "all" in content_types:
            content_types = ALL_SCRAPABLE_TYPES

        # Extract and normalize auth_id from params
        raw_auth_id = params.get("auth_id")
        auth_id: int | None = None
        if raw_auth_id is not None:
            auth_id = await _normalize_auth_id_for_site(
                site_name, raw_auth_id, backend_manager
            )

        if auth_id is None:
            auth_id = await _resolve_default_scrape_auth_id(
                site_name=site_name,
                user_id=user_id,
                content_types=content_types,
                backend_manager=backend_manager,
                use_guest_auth=use_guest_auth,
            )

        # Create tasks for all content types
        all_tasks: list[Task] = []
        for content_type_str in content_types:
            content_type = CONTENT_TYPE_MAP.get(content_type_str, ContentType.POSTS)
            task = build_task(
                TaskType.SCRAPE,
                site_name=site_name,
                user_id=user_id,
                user_username=username,
                content_type=content_type,
                download_media=download_media,
                max_items=max_items,
                auth_id=auth_id,
                use_guest_auth=use_guest_auth,
                guest_proxy_url=guest_proxy_url,
                guest_fallback_proxy_urls=guest_fallback_proxy_urls,
                guest_proxy_alternatives=guest_proxy_alternatives,
                guest_direct_route_index=guest_direct_route_index,
                track_invalid_user=track_invalid_user,
                conclusive_miss_threshold=conclusive_miss_threshold,
                max_route_hops=max_route_hops,
                ignore_space_requirements=task_ignore_space_requirements,
            )
            all_tasks.append(task)

        # Return single task or list of tasks
        return all_tasks[0] if len(all_tasks) == 1 else all_tasks

    elif task_type_str == "scrape_paid_content":
        site_name = _normalize_supported_site_name(params.get("site", "OnlyFans"))
        raw_auth_id = params.get("auth_id")
        if raw_auth_id is None:
            logger.warning("scrape_paid_content task requires auth_id")
            return None

        auth_id = await _normalize_auth_id_for_site(
            site_name, raw_auth_id, backend_manager
        )
        if auth_id is None:
            logger.warning("scrape_paid_content task received invalid auth_id")
            return None

        performer_lookup: str | None = None
        raw_performer = params.get("performer")
        if raw_performer is not None:
            performer_lookup = str(raw_performer).strip() or None

        content_types = _normalize_paid_feed_content_types(params.get("content_types"))
        download_media = _normalize_bool(params.get("download_media"), default=True)
        max_items = _normalize_max_items(params.get("max_items"))

        task = build_task(
            TaskType.SCRAPE,
            site_name=site_name,
            user_id=auth_id,
            user_username=performer_lookup,
            content_type=None,
            auth_id=auth_id,
            download_media=download_media,
            max_items=max_items,
            paid_content_feed=True,
            paid_content_performer=performer_lookup,
            paid_content_types=content_types,
            ignore_space_requirements=task_ignore_space_requirements,
        )
        return task

    elif task_type_str == "download_media":
        site_name = _normalize_supported_site_name(params.get("site", "OnlyFans"))
        user_id_raw = params.get("user_id")
        username = params.get("username")
        user_id: int | None = None

        if user_id_raw is not None:
            try:
                user_id = int(user_id_raw)
            except (ValueError, TypeError):
                logger.warning(f"Invalid user_id value in params: {user_id_raw}")

        if user_id is None and username:
            # Resolve username to user_id
            user_id = await resolve_user_identifier(username, backend_manager)
            if user_id is None:
                logger.error(
                    f"Username '{username}' not found in database. "
                    f"Cannot download media without scraping user first."
                )
                return None
        elif user_id is None:
            logger.warning("download_media task missing both user_id and username")
            return None

        # Get content types (mirrors scrape_user handling)
        raw_content_types = params.get("content_types", ["Posts"])
        content_types: list[str] = []
        if isinstance(raw_content_types, list):
            raw_content_types_list = cast(list[object], raw_content_types)
            for content_type in raw_content_types_list:
                content_types.append(_normalize_content_type(content_type))
        else:
            content_types = [_normalize_content_type(raw_content_types)]

        if "all" in content_types:
            content_types = ALL_SCRAPABLE_TYPES

        # Extract and normalize auth_id from params
        raw_auth_id = params.get("auth_id")
        auth_id: int | None = None
        if raw_auth_id is not None:
            auth_id = await _normalize_auth_id_for_site(
                site_name, raw_auth_id, backend_manager
            )

        # Auto-resolve auth_id from subscriptions if not provided
        if auth_id is None:
            auth_id = await _resolve_auth_from_subscription(
                site_name, user_id, backend_manager
            )
            if auth_id is None:
                logger.error(
                    f"No active subscription found for user {username} ({user_id}). "
                    f"Cannot download without subscription."
                )
                return None

        all_tasks: list[Task] = []
        for content_type_str in content_types:
            content_type = CONTENT_TYPE_MAP.get(content_type_str, ContentType.POSTS)
            all_tasks.append(
                build_task(
                    TaskType.DOWNLOAD,
                    site_name=site_name,
                    user_id=user_id,
                    user_username=username,
                    content_type=content_type,
                    auth_id=auth_id,
                    ignore_space_requirements=task_ignore_space_requirements,
                )
            )

        return all_tasks[0] if len(all_tasks) == 1 else all_tasks

    elif task_type_str == "refresh_subscriptions":
        site_name = _normalize_supported_site_name(params.get("site", "OnlyFans"))
        requested_auth_id: int | None = None
        raw_requested_auth_id = params.get("auth_id")
        if raw_requested_auth_id is not None:
            try:
                requested_auth_id = int(raw_requested_auth_id)
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid auth_id value for refresh_subscriptions: %s",
                    raw_requested_auth_id,
                )
                requested_auth_id = None

        from ...job_system.auth_expansion import iter_subscription_refresh_tasks

        return iter_subscription_refresh_tasks(
            multi_site_db_manager=backend_manager.multi_site_db_manager,
            site_name=site_name, requested_auth_id=requested_auth_id,
        )

    else:
        logger.warning(f"Unknown task type: {task_type_str}")
        return None


async def _built_tasks(result):
    """Drain one factory result and close a suspended account source on error."""
    if isinstance(result, AsyncIterable):
        async with aclosing(result):
            async for task in result:
                yield task
    elif isinstance(result, list):
        for task in result:
            yield task
    elif result is not None:
        yield result


def _apply_ignore_space_requirements(tasks: Sequence[Task], *, enabled: bool) -> None:
    if not enabled:
        return
    for task in tasks:
        payload = task.payload
        if isinstance(payload, (DownloadTaskPayload, ScrapeTaskPayload)):
            payload.ignore_space_requirements = True


@asynccontextmanager
async def _prepare_created_job(request, backend, backend_manager, payloads=None):
    from ...database.write_queue import DatabaseWriteClient, _positive_int_env
    from ...job_system.task_inputs import _tasks, stage_task_inputs

    task_types: set[str] = set()
    refresh_requested = False

    async def build_inputs():
        nonlocal refresh_requested
        source = payloads if payloads is not None else request.tasks
        async with aclosing(_tasks(source)) as incoming:
            async for payload in incoming:
                refresh_requested |= payload.task_type == "refresh_subscriptions"
                result = await _build_task_from_payload(
                    payload, backend_manager,
                    ignore_space_requirements=request.ignore_space_requirements,
                )
                async with aclosing(_built_tasks(result)) as built:
                    async for task in built:
                        task_types.add(payload.task_type)
                        yield task

    def no_tasks():
        if refresh_requested:
            return HTTPException(400, "No active auth accounts found for the selected site. Add and activate an account first.")
        return HTTPException(400, "No valid tasks provided")

    if backend.redis and not request.save_as_template:
        job = Job(name=request.job_name, created_by=request.created_by, tasks=[])
        input_max_bytes = _positive_int_env("ULTIMA_JOB_TASK_INPUT_MAX_BYTES", 0) or _positive_int_env(
            "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
        )
        try:
            async with stage_task_inputs(build_inputs(), job_id=job.job_id, input_max_bytes=input_max_bytes) as staged:
                yield job, staged.tasks_by_site(), staged.sites[0], staged.count, task_types, True
        except ValueError as exc:
            if str(exc) == "No valid tasks provided":
                raise no_tasks() from exc
            raise
    else:
        tasks = [task async for task in build_inputs()]
        if not tasks:
            raise no_tasks()
        if request.save_as_template:
            if backend.redis is None or not backend.redis.is_connected:
                raise HTTPException(
                    status_code=400,
                    detail="Saving templates requires Redis to be enabled and connected",
                )

            template_name_value = request.template_name or request.job_name
            template_name = template_name_value.strip()
            if not template_name:
                raise HTTPException(
                    status_code=400,
                    detail="Template name is required when saving as template",
                )

            template = _build_job_template(
                name=template_name,
                description=request.template_description,
                created_by=request.created_by,
                tasks=tasks,
                job_name_template=request.job_name,
            )

            store = JobTemplateStore(cast(Any, backend.redis))
            try:
                await store.create_template(template)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        result = build_job(tasks=tasks, job_name=request.job_name, created_by=request.created_by)
        yield result.job, result.job.tasks, str(tasks[0].site_name).lower(), len(tasks), task_types, False


@router.post("", response_model=JobResponse, openapi_extra={"requestBody": request_body_schema(CreateJobRequest)})
async def create_job(
    request: Request,
    backend: BackendInstance = Depends(get_backend_instance),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> JobResponse:
    """Read a submission incrementally, retaining its public JSON request schema."""
    if backend.redis is None or not backend.redis.is_connected:
        raise HTTPException(503, "Job execution requires connected Redis")
    if isinstance(request, CreateJobRequest):
        return await _create_job(request, backend, backend_manager)
    from ...database.write_queue import DatabaseWriteClient, _positive_int_env

    writes = DatabaseWriteClient(backend.redis)
    command_bytes, queue_bytes = writes.max_command_bytes, writes.max_queue_bytes
    async with read_job_update(
        request, byte_limit=min(command_bytes, queue_bytes), default_inflight_bytes=queue_bytes,
        batch_limit=_positive_int_env("ULTIMA_JOB_TASK_INPUT_BATCH_SIZE", JobStorage.DEFAULT_WRITE_BATCH_SIZE),
        resource_prefix="ULTIMA_JOB_UPLOAD", request_model=CreateJobRequest,
        task_model=TaskPayload, required_tasks=True,
    ) as (model, payloads):
        return await _create_job(model, backend, backend_manager, payloads=payloads)


async def _create_job(
    request: CreateJobRequest,
    backend: BackendInstance,
    backend_manager: BackendManager,
    *, payloads=None,
) -> JobResponse:
    """Publish complete ordinary inputs in SQL before making a job runnable."""
    if backend.redis is None or not backend.redis.is_connected:
        raise HTTPException(503, "Job execution requires connected Redis")
    try:
        async with _prepare_created_job(request, backend, backend_manager, payloads) as prepared:
            job, tasks, site_name, input_count, task_types, build_tasks = prepared
            if request.backend_id:
                job.metadata["dispatch_backend_id"] = request.backend_id
            elif backend.redis and not backend.distributed_mode:
                job.metadata["dispatch_backend_id"] = backend.instance_id

            if request.schedule:
                job.schedule = _build_schedule_from_request(request.schedule)
                if (
                    job.schedule.scheduled_for
                    or job.schedule.interval_seconds
                    or job.schedule.cron
                ):
                    job.metadata["source"] = JobSource.SCHEDULED.value

            is_scheduled_job = job.is_scheduled
            if is_scheduled_job and (not backend.redis or not backend.redis.is_connected):
                raise HTTPException(
                    status_code=400,
                    detail="Scheduling requires Redis to be enabled and connected",
                )

            # Persist the header and bounded task-row batches before activation.
            # SQL admission and cleanup remain owned by this request.
            try:
                schedule_payload = (
                    request.schedule.model_dump() if request.schedule else None
                )
                args_payload = {
                    "job_name": request.job_name,
                    "created_by": request.created_by,
                    "task_count": input_count,
                    "task_types": sorted(task_types),
                    "schedule": schedule_payload,
                }
                saved_config = await _persist_central_job_submission(
                    job_id=job.job_id,
                    job_type=request.job_name or "scrape_and_download",
                    site=site_name,
                    args_payload=args_payload,
                    tasks=tasks,
                    job_config=job.to_config(include_task_ids=False).to_dict(),
                    windowed=True,
                    build_tasks=build_tasks,
                )
                if saved_config is not None:
                    job = Job.from_dict(saved_config)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except TaskLoadLimitExceeded as exc:
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except Exception as e:
                logger.warning("Failed to persist job to database: %s", e)
                raise HTTPException(
                    status_code=503, detail="Failed to persist job to database"
                ) from e

            logger.info("Job %s admitted to SQL execution ledger (%d tasks)", job.job_id, job.total_tasks)

            # Access attributes safely
            job_name = getattr(job, "job_name", request.job_name)
            status = getattr(job, "status", JobStatus.PENDING)
            created_at = getattr(job, "created_at", None)
            updated_at = getattr(job, "updated_at", None)

            created_at_str = _coerce_iso_datetime(created_at) or ""
            updated_at_str = _coerce_iso_datetime(updated_at)

            await publish_ws_event(
                backend.redis,
                {
                    "type": "webui_ack",
                    "event": "job_created",
                    "job_id": job.job_id,
                    "job_name": job_name,
                    "status": str(status),
                    "task_count": job.total_tasks,
                    "scheduled": bool(job.is_scheduled),
                },
            )

            response = _job_to_response(job)
            response.job_name = job_name
            response.status = status
            response.created_at = created_at_str
            response.updated_at = updated_at_str
            response.task_count = job.total_tasks
            response.task_stats.total = job.total_tasks
            return response

    except (HTTPException, RequestValidationError):
        raise
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except Exception as exc:
        logger.exception("Failed to create job")
        raise HTTPException(500, str(exc)) from exc


@router.post("/templates/from-tasks", response_model=JobTemplateResponse)
async def create_job_template_from_tasks(
    request: CreateJobTemplateFromTasksRequest,
    backend: BackendInstance = Depends(get_backend_instance),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> JobTemplateResponse:
    """Create a job template from task payloads."""
    if not backend.redis or not backend.redis.is_connected:
        raise HTTPException(
            status_code=400,
            detail="Job template creation requires Redis to be enabled and connected",
        )

    redis = backend.redis

    tasks: list[Task] = []
    for task_payload in request.tasks:
        result = await _build_task_from_payload(task_payload, backend_manager)
        async with aclosing(_built_tasks(result)) as built:
            async for task in built:
                tasks.append(task)

    if not tasks:
        raise HTTPException(status_code=400, detail="No valid tasks provided")

    metadata = _coerce_json_mapping(request.metadata)
    metadata.setdefault("source", "tasks")
    metadata.setdefault("task_count", len(tasks))

    template = _build_template_from_request(request, tasks, metadata=metadata)

    from ...job_system.templates import JobTemplateStore

    store = JobTemplateStore(redis)
    try:
        saved = await store.create_template(template)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Failed to create job template: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500, detail="Failed to create job template"
        ) from exc

    return _template_to_response(saved)


@router.post("/templates/from-job", response_model=JobTemplateResponse)
async def create_job_template_from_job(
    request: CreateJobTemplateFromJobRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobTemplateResponse:
    """Create a job template from an existing job."""
    if not backend.redis or not backend.redis.is_connected:
        raise HTTPException(
            status_code=400,
            detail="Job template creation requires Redis to be enabled and connected",
        )

    from ...job_system.storage import JobStorage
    from ...job_system.templates import JobTemplateStore

    redis = backend.redis

    storage = JobStorage(redis)
    try:
        job = await storage.fetch_job_with_tasks(
            request.job_id,
            include_tasks=True,
            max_include_tasks=MAX_INLINE_TASK_LOAD,
        )
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    if not job:
        raise HTTPException(status_code=404, detail="Job not found in Redis")

    if not job.tasks:
        if job.state in {JobStatus.COMPLETED, JobStatus.CANCELLED}:
            _raise_archived_terminal_job_payload_error("template creation")
        raise HTTPException(status_code=400, detail="Job has no tasks")

    metadata = _coerce_json_mapping(request.metadata)
    metadata.setdefault("source", "job")
    metadata.setdefault("source_job_id", request.job_id)
    metadata.setdefault("task_count", len(job.tasks))

    template = _build_template_from_request(request, job.tasks, metadata=metadata)

    store = JobTemplateStore(redis)
    try:
        saved = await store.create_template(template)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Failed to create job template: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500, detail="Failed to create job template"
        ) from exc

    return _template_to_response(saved)


@router.get("/templates", response_model=list[JobTemplateResponse])
async def list_templates(
    backend: BackendInstance = Depends(get_backend_instance),
) -> list[JobTemplateResponse]:
    """List all available job templates."""
    if not backend.redis or not backend.redis.is_connected:
        return []
    store = JobTemplateStore(cast(Any, backend.redis))
    templates = await store.list_templates()
    return [_template_to_response(t) for t in templates]


@router.get("/templates/{template_id}", response_model=JobTemplateResponse)
async def get_template(
    template_id: str,
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobTemplateResponse:
    """Get a specific job template by ID."""
    if not backend.redis or not backend.redis.is_connected:
        raise HTTPException(status_code=400, detail="Templates require Redis")
    store = JobTemplateStore(cast(Any, backend.redis))
    template = await store.get_template(template_id)
    if not template:
        raise HTTPException(
            status_code=404, detail=f"Template '{template_id}' not found"
        )
    return _template_to_response(template)


@router.post("/templates/{template_id}/instantiate", response_model=JobResponse)
async def instantiate_template(
    template_id: str,
    request: InstantiateTemplateRequest,
    backend: BackendInstance = Depends(get_backend_instance),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> JobResponse:
    """Instantiate a job template with the given parameter values and enqueue the job."""
    if not backend.redis or not backend.redis.is_connected:
        raise HTTPException(
            status_code=400, detail="Instantiating templates requires Redis"
        )

    store = JobTemplateStore(cast(Any, backend.redis))
    template = await store.get_template(template_id)
    if not template:
        raise HTTPException(
            status_code=404, detail=f"Template '{template_id}' not found"
        )

    base_params = dict(request.parameter_values)
    site: str = str(base_params.get("site", "OnlyFans"))

    # Build list of (username, auth_id) entries.
    # Prefer batch ``entries``; fall back to legacy single fields.
    entries: list[TemplateEntry]
    if request.entries:
        entries = request.entries
    else:
        username_val = (
            base_params.get("username")
            if isinstance(base_params.get("username"), str)
            else None
        )
        if not username_val:
            raise HTTPException(
                status_code=400,
                detail="Either 'entries' or 'username' in parameter_values is required.",
            )
        entries = [TemplateEntry(username=username_val, auth_id=request.auth_id or 0)]

    logger.info(
        "instantiate_template: template_id=%s, entries=%s",
        template_id,
        [(e.username, e.auth_id) for e in entries],
    )

    # Resolve each entry and instantiate tasks
    all_tasks: list[Task] = []
    errors: list[str] = []
    first_instantiation = None

    for entry in entries:
        param_values = {**base_params, "username": entry.username}

        # Resolve username → user_id
        user_id = await resolve_user_identifier(entry.username, backend_manager, site)
        if user_id is None:
            errors.append(f"Could not resolve username '{entry.username}' on {site}.")
            continue
        param_values["user_id"] = user_id

        try:
            instantiation = template.instantiate(
                param_values, created_by=request.created_by
            )
        except (ValueError, KeyError) as exc:
            logger.error(
                "instantiate_template failed for %s: %s",
                entry.username,
                exc,
                exc_info=True,
            )
            errors.append(f"{entry.username}: {exc}")
            continue

        if first_instantiation is None:
            first_instantiation = instantiation

        # Stamp auth_id and user_username on every task from this entry
        for task in instantiation.tasks:
            if entry.auth_id:
                task.auth_id = entry.auth_id
            if not task.user_username:
                task.user_username = entry.username
        _apply_ignore_space_requirements(
            instantiation.tasks,
            enabled=request.ignore_space_requirements,
        )

        all_tasks.extend(instantiation.tasks)

    if not all_tasks:
        detail = "No tasks could be created."
        if errors:
            detail += " Errors: " + "; ".join(errors)
        raise HTTPException(status_code=400, detail=detail)

    job_name = request.job_name or (
        first_instantiation.job_name if first_instantiation else template.name
    )
    result = build_job(
        tasks=all_tasks,
        job_name=job_name,
        created_by=request.created_by,
    )
    job = result.job
    if backend.redis and not backend.distributed_mode:
        job.metadata["dispatch_backend_id"] = backend.instance_id
    if first_instantiation:
        job.metadata.update(first_instantiation.metadata)

    if request.schedule:
        job.schedule = _build_schedule_from_request(request.schedule)

    # Persist to central DB with owned SQL admission before activation.
    try:
        site_name_lower = (
            str(all_tasks[0].site_name).lower() if all_tasks else "onlyfans"
        )
        args_payload = {
            "template_id": template_id,
            "template_name": template.name,
            "task_count": len(all_tasks),
            "parameter_values": base_params,
            "entry_count": len(entries),
            "entries_preview": [
                {"username": e.username, "auth_id": e.auth_id} for e in entries[:25]
            ],
        }
        saved_config = await _persist_central_job_submission(
            job_id=job.job_id,
            job_type=job_name or "template_job",
            site=site_name_lower,
            args_payload=args_payload,
            tasks=job.tasks,
            job_config=job.to_config(include_task_ids=False).to_dict(),
            windowed=True,
        )
        if saved_config is not None:
            job = Job.from_dict(saved_config)
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning("Failed to persist template job to database: %s", exc)
        raise HTTPException(
            status_code=503, detail="Failed to persist template job to database"
        ) from exc

    is_scheduled = job.is_scheduled
    logger.info("Template job %s admitted to SQL execution ledger", job.job_id)

    await publish_ws_event(
        backend.redis,
        {
            "type": "webui_ack",
            "event": "job_created",
            "job_id": job.job_id,
            "job_name": job_name,
            "status": job.state.value,
            "task_count": job.total_tasks,
            "scheduled": bool(is_scheduled),
        },
    )

    response = _job_to_response(job)
    response.job_name = job_name or ""
    response.created_by = request.created_by
    response.task_count = job.total_tasks
    response.task_stats.total = job.total_tasks
    return response


# A replay further behind than this cannot catch up with a busy job, and each
# backlog page costs a scan across every run's events. Send the client to the
# head instead; its pages refetch their own state on resync.
EXECUTION_REPLAY_MAX_LAG = 5000


@router.get("/execution-events")
async def execution_events(
    after: int | None = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    """Replay committed lifecycle events; an omitted cursor starts at the head."""
    from ...job_system.ledger import ExecutionLedger
    ledger = ExecutionLedger()
    head = await run_database_call(ledger.event_head)
    if after is None:
        return {"enabled": True, "events": [], "next_cursor": head, "has_more": False}
    if head - after > EXECUTION_REPLAY_MAX_LAG:
        return {"enabled": True, "events": [], "next_cursor": head, "has_more": False, "resync": True}
    events = await run_database_call(ledger.events, after=after, limit=limit, through=head)
    cursor = events[-1]["sequence"] if len(events) == limit else head
    return {"enabled": True, "events": events, "next_cursor": cursor, "has_more": len(events) == limit}


@router.get("/{job_id}/attempts")
async def execution_attempts(
    job_id: str,
    after_task: str = Query(""),
    after_attempt: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    from ...job_system.ledger import ExecutionLedger
    ledger = ExecutionLedger()
    if await run_database_call(ledger.job, job_id) is None:
        raise HTTPException(404, "Execution ledger run not found")
    rows = await run_database_call(ledger.attempts, job_id, after_task=after_task,
        after_attempt=after_attempt, limit=limit)
    return {"attempts": rows, "next_cursor": {"after_task": rows[-1]["task_id"],
        "after_attempt": rows[-1]["attempt"]} if len(rows) == limit else None}


async def _execution_control(job_id: str, action: str):
    from ...job_system.ledger import ExecutionLedger
    try:
        config = await run_database_call(ExecutionLedger().control, job_id, action)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    if config is None:
        raise HTTPException(404, "Execution ledger run not found")
    return _job_to_response(Job.from_dict(config))


@router.post("/{job_id}/pause", response_model=JobResponse)
async def pause_execution(job_id: str):
    return await _execution_control(job_id, "pause")


@router.post("/{job_id}/resume", response_model=JobResponse)
async def resume_execution(job_id: str):
    return await _execution_control(job_id, "resume")


@router.get("", response_model=JobListResponse)
async def list_jobs(
    status: Optional[JobStatus] = Query(None, description="Filter by status"),
    search: str | None = Query(
        None, description="Search by job ID, name, type, or task username"
    ),
    include_tasks: bool = Query(
        False,
        description="Compatibility flag. Task arrays are no longer included in list responses.",
    ),
    include_api_commands: bool = Query(True, description="Include internal API command jobs"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
    job_type: str | None = Query(None, description="Filter by job type", alias="type"),
    owner: str | None = Query(None, description="Filter by job creator / owner"),
    schedule: str | None = Query(None, description="Filter by schedule ('scheduled' or 'manual')"),
    site: str | None = Query(None, description="Filter by site"),
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobListResponse:
    """List jobs with optional filtering.

    Args:
        status: Optional status filter
        page: Page number (1-indexed)
        page_size: Items per page
        backend: Backend instance (injected)

    Returns:
        Paginated job list

    Performance:
        Optimized to batch Redis calls using pipelines instead of N+1 queries.
        Task summaries are only fetched for the paginated subset of jobs.
    """
    import json

    from sqlmodel import select as sql_select

    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        CentralTaskModel,
        global_session,
    )

    from ...job_system.storage import JobStorage

    try:
        trimmed_search = search.strip() if search and isinstance(search, str) else ""
        search_pattern = f"%{trimmed_search}%" if trimmed_search else None
        raw_job_type = job_type.strip() if job_type and isinstance(job_type, str) else None
        raw_owner = owner.strip() if owner and isinstance(owner, str) else None
        raw_schedule = schedule.strip().lower() if schedule and isinstance(schedule, str) else None
        raw_site = site.strip() if site and isinstance(site, str) else None

        # Step 1: Fetch paginated jobs from central database using SQL
        # LIMIT/OFFSET instead of loading every row into Python.
        resolved_page = page if isinstance(page, int) else 1
        resolved_page_size = page_size if isinstance(page_size, int) else 50
        start = (resolved_page - 1) * resolved_page_size

        def _apply_search_filter(query_obj: Any) -> Any:
            # Internal API command jobs use the api- namespace assigned by
            # BackendManager; apply this before both counting and pagination.
            if include_api_commands is False:
                query_obj = query_obj.where(~CentralJobModel.job_id.like("api-%"))
            if raw_job_type and raw_job_type.lower() != "all":
                norm_type = raw_job_type.lower().replace(" ", "_")
                query_obj = query_obj.where(
                    or_(
                        CentralJobModel.job_type.ilike(raw_job_type),
                        CentralJobModel.job_type.ilike(norm_type),
                    )
                )
            if raw_owner and raw_owner.lower() != "all":
                query_obj = query_obj.where(
                    CentralJobModel.args_json.ilike(f'%"created_by": "{raw_owner}"%')
                )
            if raw_site and raw_site.lower() != "all":
                query_obj = query_obj.where(CentralJobModel.site == raw_site)
            if raw_schedule and raw_schedule != "all":
                is_sched_condition = or_(
                    CentralJobModel.args_json.ilike('%"interval_seconds":%'),
                    CentralJobModel.args_json.ilike('%"cron":%'),
                    CentralJobModel.args_json.ilike('%"scheduled_for":%'),
                    CentralJobModel.args_json.ilike('%"is_scheduled": true%'),
                )
                if raw_schedule == "scheduled":
                    query_obj = query_obj.where(is_sched_condition)
                elif raw_schedule == "manual":
                    query_obj = query_obj.where(~is_sched_condition)
            if not search_pattern:
                return query_obj
            task_username_match = (
                sql_select(CentralTaskModel.job_id)
                .where(
                    CentralTaskModel.job_id == CentralJobModel.job_id,
                    CentralTaskModel.username.is_not(None),
                    CentralTaskModel.username.ilike(search_pattern),
                )
                .exists()
            )
            return query_obj.where(
                or_(
                    CentralJobModel.job_id.ilike(search_pattern),
                    CentralJobModel.job_type.ilike(search_pattern),
                    CentralJobModel.args_json.ilike(search_pattern),
                    task_username_match,
                )
            )

        def _fetch_paginated_db_jobs() -> tuple[list[CentralJobModel], int, list[str]]:
            with global_session() as session:
                # Count query
                from sqlalchemy import func as sa_func
                from sqlalchemy import text as sa_text

                if search_pattern:
                    # The browser can abandon a search while its SQL keeps
                    # scanning retained task history. Bound both statements
                    # on this transaction so searches cannot consume database
                    # I/O for hours alongside active scraping.
                    session.execute(
                        sa_text("SELECT set_config('statement_timeout', '15000', true)")
                    )

                count_q = sql_select(sa_func.count()).select_from(CentralJobModel)
                if status is not None:
                    count_q = count_q.where(CentralJobModel.status == status.value)
                count_q = _apply_search_filter(count_q)
                total = int(cast(Any, session.exec(count_q)).one())

                # Data query with LIMIT/OFFSET
                query = sql_select(CentralJobModel)
                if status is not None:
                    query = query.where(CentralJobModel.status == status.value)
                query = _apply_search_filter(query)
                query = (
                    query.order_by(CentralJobModel.created_at.desc())
                    .offset(start)
                    .limit(resolved_page_size)
                )
                rows = cast(
                    list[CentralJobModel],
                    cast(Any, session.exec(query)).all(),
                )
                raw_types = sorted({job.job_type for job in rows if job.job_type})
                return rows, total, raw_types

        paginated_jobs, total, available_types = await run_database_call(_fetch_paginated_db_jobs)

        # Step 2: Batch fetch Redis states for paginated jobs only
        job_ids = [job.job_id for job in paginated_jobs]
        redis_states: dict[str, dict[str, str]] = {}
        redis_configs: dict[str, JobConfig] = {}
        redis_config_present: dict[str, bool] = {}
        redis_has_task_records: dict[str, bool] = {}

        if backend.redis is not None and job_ids:
            results = await _redis_pipeline_execute(
                backend.redis,
                hgetall_keys=[
                    JobStorage.JOB_STATE_KEY.format(job_id=job_id) for job_id in job_ids
                ],
                get_keys=[
                    JobStorage.JOB_CONFIG_KEY.format(job_id=job_id)
                    for job_id in job_ids
                ],
                zrange_specs=[
                    (JobStorage.TASK_INDEX_KEY.format(job_id=job_id), 0, 0)
                    for job_id in job_ids
                ],
            )
            state_results = results[: len(job_ids)]
            config_results = results[len(job_ids) : len(job_ids) * 2]
            task_index_results = results[len(job_ids) * 2 :]

            # Parse results
            for job_id, raw_state in zip(job_ids, state_results, strict=True):
                if raw_state:
                    redis_states[job_id] = dict(cast(dict[str, str], raw_state))

            for job_id, raw_config in zip(job_ids, config_results, strict=True):
                redis_config_present[job_id] = bool(raw_config)
                if not raw_config:
                    continue
                try:
                    if not isinstance(raw_config, (str, bytes, bytearray)):
                        continue
                    config_blob = json.loads(raw_config)
                    redis_configs[job_id] = JobConfig.from_dict(config_blob)
                except Exception:
                    continue

            for job_id, raw_task_index in zip(job_ids, task_index_results, strict=True):
                redis_has_task_records[job_id] = _redis_zrange_has_members(
                    raw_task_index
                )

        durable_header_job_ids = [
            job.job_id
            for job in paginated_jobs
            if job.job_id not in redis_states
            or job.job_id not in redis_configs
            or job.job_type == "mass_profile_scrape"
            or (job.args_json and '"mps_manifest"' in job.args_json)
            or (job.args_json and '"execution_engine"' in job.args_json)
            or (getattr(job, "job_config_json", None) and '"execution_engine"' in job.job_config_json)
        ]
        durable_headers = await run_database_call(
            _fetch_durable_headers_sync, durable_header_job_ids
        )
        for job_id, durable in durable_headers.items():
            redis_configs.setdefault(job_id, JobConfig.from_dict(durable["config"]))

        # Step 3: Build responses using cached states
        all_jobs: list[JobResponse] = []
        ghost_job_ids: list[str] = []
        recovered_ghost_jobs: dict[str, str] = {}

        for job in paginated_jobs:
            redis_state = redis_states.get(job.job_id)
            durable_header = durable_headers.get(job.job_id)
            args_data: dict[str, Any] | None = None
            if job.args_json:
                try:
                    parsed_args = json.loads(job.args_json)
                    if isinstance(parsed_args, dict):
                        args_data = cast(dict[str, Any], parsed_args)
                except json.JSONDecodeError:
                    args_data = None

            # Update status from Redis if different (don't sync to DB in list - too slow)
            effective_status = job.status
            if redis_state is not None:
                redis_status = redis_state.get("status")
                if redis_status:
                    effective_status = redis_status
            if durable_header is not None and durable_header.get("execution_stats") is not None:
                # SQL owns ledger status; a Redis projection can lag while
                # cancellation is committed and history is being purged.
                effective_status = str(durable_header["status"])

            # Parse job name from args_json
            job_name = job.job_type
            if args_data:
                job_name = str(args_data.get("job_name") or job.job_type)

            config = redis_configs.get(job.job_id)
            schedule = config.schedule if config else None
            metadata = config.metadata if config else None
            is_scheduled = bool(
                schedule
                and (
                    schedule.scheduled_for or schedule.interval_seconds or schedule.cron
                )
            )

            run_count_value = (
                _extract_int(metadata.get("run_count")) if metadata else None
            )
            run_count = run_count_value if run_count_value is not None else 0
            last_ran_at = (
                str(metadata.get("last_ran_at"))
                if metadata and metadata.get("last_ran_at") is not None
                else None
            )
            if last_ran_at is None and redis_state is not None:
                last_ran_at = redis_state.get("completed_at") or redis_state.get(
                    "started_at"
                )
            last_activity_at = None
            if redis_state is not None:
                last_activity_at = (
                    redis_state.get("updated_at")
                    or redis_state.get("completed_at")
                    or redis_state.get("started_at")
                    or redis_state.get("created_at")
                )
            if last_activity_at is None:
                last_activity_at = _coerce_iso_datetime(
                    getattr(job, "last_event_at", None)
                    or job.completed_at
                    or job.started_at
                )

            # Get task count and completion counters
            task_count = (
                _extract_int(args_data.get("task_count")) if args_data else None
            )
            if task_count is None and metadata:
                task_count = _extract_int(metadata.get("total_tasks_count_override"))
            if task_count is None and config and getattr(config, "total_tasks", 0) > 0:
                task_count = getattr(config, "total_tasks", 0)
            if task_count is None:
                task_count = 1
            completed_tasks = 0
            failed_tasks = 0

            is_ledger_or_durable = bool(
                (durable_header and durable_header.get("execution_stats"))
                or (metadata and metadata.get("execution_engine") == "ledger")
                or (args_data and args_data.get("mps_manifest"))
                or getattr(job, "job_type", None) == "mass_profile_scrape"
            )

            if durable_header is not None and is_ledger_or_durable:
                task_stats = _durable_task_stats(durable_header)
                task_count, completed_tasks, failed_tasks = (
                    task_stats.total,
                    task_stats.completed,
                    task_stats.failed,
                )
            elif redis_state is not None:
                task_count_str = redis_state.get("task_count")
                if task_count_str:
                    try:
                        task_count = int(task_count_str)
                    except (ValueError, TypeError):
                        pass
                # Also check total_tasks which might be more accurate
                total_tasks_str = redis_state.get("total_tasks")
                if total_tasks_str:
                    try:
                        task_count = int(total_tasks_str)
                    except (ValueError, TypeError):
                        pass
                try:
                    completed_tasks = int(redis_state.get("completed_tasks", "0"))
                except (ValueError, TypeError):
                    pass
                try:
                    failed_tasks = int(redis_state.get("failed_tasks", "0"))
                except (ValueError, TypeError):
                    pass
                task_stats = _build_task_stats_model(
                    {
                        "total": redis_state.get("total_tasks", task_count),
                        "ready": redis_state.get("ready_tasks", 0),
                        "blocked": redis_state.get("blocked_tasks", 0),
                        "inflight": redis_state.get("inflight_tasks", 0),
                        "deferred": redis_state.get("deferred_tasks", 0),
                        "completed": redis_state.get(
                            "completed_tasks", completed_tasks
                        ),
                        "failed": redis_state.get("failed_tasks", failed_tasks),
                        "task_type_counts": (
                            json.loads(redis_state.get("task_type_counts", "{}"))
                            if redis_state.get("task_type_counts")
                            else {}
                        ),
                    }
                )
            elif durable_header is not None:
                task_stats = _durable_task_stats(durable_header)
                task_count, completed_tasks, failed_tasks = (
                    task_stats.total,
                    task_stats.completed,
                    task_stats.failed,
                )
            else:
                task_stats = TaskStats(
                    total=task_count, completed=completed_tasks, failed=failed_tasks
                )

            # Resolve created_by from config or args
            created_by: str | None = None
            if config:
                created_by = config.created_by
            if not created_by and args_data:
                created_by = cast(str | None, args_data.get("created_by"))

            configured_priority = (
                _extract_int(metadata.get("priority")) if metadata else None
            )
            effective_priority = (
                configured_priority
                if configured_priority is not None
                else (_extract_int(getattr(job, "priority", 0)) or 0)
            )

            materialization_status = (
                redis_state.get("materialization_status")
                if redis_state is not None
                else None
            ) or (
                str(metadata.get("materialization_status"))
                if metadata and metadata.get("materialization_status") is not None
                else None
            )
            materialized_tasks = _extract_int(
                redis_state.get("materialized_tasks")
                if redis_state is not None
                else None
            )
            if materialized_tasks is None and metadata:
                materialized_tasks = _extract_int(metadata.get("materialized_tasks"))
            materialization_total = _extract_int(
                redis_state.get("materialization_total")
                if redis_state is not None
                else None
            )
            if materialization_total is None and metadata:
                materialization_total = _extract_int(
                    metadata.get("materialization_total")
                )
            materialization_error = (
                redis_state.get("materialization_error")
                if redis_state is not None
                else None
            ) or (
                str(metadata.get("materialization_error"))
                if metadata and metadata.get("materialization_error") is not None
                else None
            )

            has_redis_artifact = (
                redis_state is not None
                or redis_config_present.get(job.job_id, False)
                or redis_has_task_records.get(job.job_id, False)
            )
            has_canonical_artifacts = redis_has_task_records.get(
                job.job_id, False
            ) and (
                redis_config_present.get(job.job_id, False)
                or redis_state is not None
            )
            is_stale_incomplete = durable_header is None and _is_stale_incomplete_active_job(
                status=effective_status,
                created_at=(
                    redis_state.get("created_at")
                    if redis_state is not None
                    else job.created_at
                ),
                has_redis_artifact=has_redis_artifact,
                has_canonical_artifacts=has_canonical_artifacts,
                expected_task_count=task_count,
                materialization_status=materialization_status,
                last_activity_at=job.last_event_at,
            )
            if is_stale_incomplete:
                logger.warning(
                    "Reclassifying incomplete active job %s as failed; canonical Redis artifacts are missing",
                    job.job_id,
                )
                ghost_job_ids.append(job.job_id)
                if status in {
                    JobStatus.PENDING,
                    JobStatus.RUNNING,
                }:
                    total = max(total - 1, 0)
                    continue
                effective_status = JobStatus.FAILED.value
                materialization_status = "failed"
                materialization_error = GHOST_JOB_ERROR
            elif (
                getattr(job, "result_error", None) == GHOST_JOB_ERROR
                and _enum_value(effective_status).lower() in ACTIVE_LIVE_JOB_STATUSES
                and (
                    has_canonical_artifacts
                    or _enum_value(materialization_status).lower()
                    in ACTIVE_MATERIALIZATION_STATUSES
                )
            ):
                recovered_ghost_jobs[job.job_id] = _enum_value(
                    effective_status
                ).lower()

            all_jobs.append(
                JobResponse(
                    job_id=job.job_id,
                    deletion_progress=durable_header["config"].get("metadata", {}).get("deletion_progress", {}) if durable_header and durable_header["config"].get("metadata", {}).get("deletion_pending") else None,
                    execution_engine=metadata.get("execution_engine") if metadata else None,
                    job_name=job_name,
                    job_type=str(getattr(job, "job_type", "") or "") or None,
                    site=str(getattr(job, "site", "") or "") or None,
                    status=(
                        JobStatus(effective_status)
                        if effective_status in [s.value for s in JobStatus]
                        else JobStatus.PENDING
                    ),
                    priority=effective_priority,
                    worker_id=(str(getattr(job, "worker_id", "") or "") or None),
                    created_at=(job.created_at.isoformat() if job.created_at else ""),
                    updated_at=(job.started_at.isoformat() if job.started_at else None),
                    started_at=(
                        job.started_at.isoformat()
                        if job.started_at
                        else (redis_state.get("started_at") if redis_state else None)
                    ),
                    completed_at=(
                        (job.completed_at.isoformat() if job.completed_at else None)
                        or (redis_state.get("completed_at") if redis_state else None)
                        or (
                            last_ran_at
                            if effective_status in ("completed", "failed", "cancelled")
                            else None
                        )
                    ),
                    task_count=task_count,
                    completed_tasks=completed_tasks,
                    failed_tasks=failed_tasks,
                    tasks=[],
                    task_stats=task_stats,
                    error_stats=JobErrorStats(total=failed_tasks, groups={}),
                    created_by=created_by,
                    is_scheduled=is_scheduled,
                    scheduled_for=schedule.scheduled_for if schedule else None,
                    interval_seconds=schedule.interval_seconds if schedule else None,
                    cron=schedule.cron if schedule else None,
                    run_count=run_count,
                    last_ran_at=last_ran_at,
                    last_activity_at=last_activity_at,
                    materialization_status=materialization_status,
                    materialized_tasks=materialized_tasks,
                    materialization_total=materialization_total,
                    materialization_error=materialization_error,
                )
            )

        if ghost_job_ids:
            await _mark_central_jobs_failed(ghost_job_ids)
        if recovered_ghost_jobs:
            await _restore_central_ghost_jobs(recovered_ghost_jobs)

        # ── Redis fallback: surface active jobs missing from central DB ──
        # If a job is in Redis (active) but not in the central DB (e.g. the
        # persist failed silently), the dashboard would never show it under
        # "Active Jobs" and its tasks would appear as standalone orphans.
        # We scan Redis for active jobs not already in the response and
        # synthesize JobResponse entries, then backfill the central DB.
        if backend.redis is not None and page == 1:
            try:
                known_ids = {j.job_id for j in all_jobs}
                all_redis_ids_raw = await _redis_zrevrange(
                    backend.redis, JobStorage.JOB_INDEX_KEY, 0, 499
                )
                all_redis_ids = [
                    str(rid[0]) if isinstance(rid, tuple) else str(rid)
                    for rid in all_redis_ids_raw
                ]
                # Filter to IDs not already in response and not internal
                missing_ids = [
                    rid
                    for rid in all_redis_ids
                    if rid not in known_ids
                    and not rid.lower().startswith("api-")
                    and not rid.lower().startswith("test-")
                    and rid.lower() != "api"
                ]
                if missing_ids:
                    m_results = await _redis_pipeline_execute(
                        backend.redis,
                        hgetall_keys=[
                            JobStorage.JOB_STATE_KEY.format(job_id=mid)
                            for mid in missing_ids
                        ],
                        get_keys=[
                            JobStorage.JOB_CONFIG_KEY.format(job_id=mid)
                            for mid in missing_ids
                        ],
                        zrange_specs=[
                            (JobStorage.TASK_INDEX_KEY.format(job_id=mid), 0, 0)
                            for mid in missing_ids
                        ],
                    )
                    m_state_results = m_results[: len(missing_ids)]
                    m_config_results = m_results[
                        len(missing_ids) : len(missing_ids) * 2
                    ]
                    m_task_index_results = m_results[len(missing_ids) * 2 :]

                    active_statuses = set(ACTIVE_LIVE_JOB_STATUSES)
                    backfill_rows: list[
                        tuple[str, str, str, str]
                    ] = []  # (job_id, name, status, site)

                    for mid, raw_st, raw_cfg, raw_task_index in zip(
                        missing_ids,
                        m_state_results,
                        m_config_results,
                        m_task_index_results,
                        strict=True,
                    ):
                        if not raw_st:
                            continue
                        st = dict(cast(dict[str, str], raw_st))
                        redis_status = st.get("status", "")
                        # Only apply status filter if one was requested
                        if status is not None and redis_status != status.value:
                            continue
                        # Only include active jobs (or honour the requested filter)
                        if status is None and redis_status not in active_statuses:
                            continue

                        materialization_status = st.get("materialization_status")
                        materialization_active = (
                            materialization_status in ACTIVE_MATERIALIZATION_STATUSES
                        )
                        if not raw_cfg or (
                            not _redis_zrange_has_members(raw_task_index)
                            and not materialization_active
                        ):
                            logger.debug(
                                "Skipping Redis fallback for incomplete job %s; missing config or task index",
                                mid,
                            )
                            continue

                        cfg_obj: JobConfig | None = None
                        try:
                            if isinstance(raw_cfg, (str, bytes, bytearray)):
                                cfg_obj = JobConfig.from_dict(json.loads(raw_cfg))
                        except Exception:
                            cfg_obj = None
                        if cfg_obj is None:
                            continue

                        job_name_val = cfg_obj.name if cfg_obj else mid
                        task_count_val = 1
                        try:
                            task_count_val = int(st.get("task_count", "1"))
                        except (ValueError, TypeError):
                            pass
                        total_tasks_str = st.get("total_tasks")
                        if total_tasks_str:
                            try:
                                task_count_val = int(total_tasks_str)
                            except (ValueError, TypeError):
                                pass
                        completed_val, failed_val = 0, 0
                        try:
                            completed_val = int(st.get("completed_tasks", "0"))
                        except (ValueError, TypeError):
                            pass
                        try:
                            failed_val = int(st.get("failed_tasks", "0"))
                        except (ValueError, TypeError):
                            pass

                        created_by_val = cfg_obj.created_by if cfg_obj else None
                        schedule_val = cfg_obj.schedule if cfg_obj else None
                        is_sched = bool(
                            schedule_val
                            and (
                                schedule_val.scheduled_for
                                or schedule_val.interval_seconds
                                or schedule_val.cron
                            )
                        )
                        created_at_val = st.get("created_at", "")
                        site_val = "onlyfans"
                        if cfg_obj:
                            # Site determination logic can be improved later
                            pass

                        all_jobs.append(
                            JobResponse(
                                job_id=mid,
                                job_name=job_name_val,
                                job_type=job_name_val,
                                site=site_val,
                                status=(
                                    JobStatus(redis_status)
                                    if redis_status in [s.value for s in JobStatus]
                                    else JobStatus.PENDING
                                ),
                                priority=(
                                    _extract_int(cfg_obj.metadata.get("priority"))
                                    if cfg_obj and cfg_obj.metadata
                                    else 0
                                )
                                or 0,
                                worker_id=st.get("worker_id") or None,
                                created_at=created_at_val,
                                updated_at=None,
                                started_at=st.get("started_at") or None,
                                completed_at=st.get("completed_at") or None,
                                task_count=task_count_val,
                                completed_tasks=completed_val,
                                failed_tasks=failed_val,
                                tasks=[],
                                task_stats=TaskStats(
                                    total=task_count_val,
                                    completed=completed_val,
                                    failed=failed_val,
                                ),
                                error_stats=JobErrorStats(total=failed_val, groups={}),
                                created_by=created_by_val,
                                is_scheduled=is_sched,
                                scheduled_for=(
                                    schedule_val.scheduled_for if schedule_val else None
                                ),
                                interval_seconds=(
                                    schedule_val.interval_seconds
                                    if schedule_val
                                    else None
                                ),
                                cron=schedule_val.cron if schedule_val else None,
                                run_count=0,
                                last_ran_at=None,
                                last_activity_at=st.get("updated_at")
                                or st.get("completed_at")
                                or st.get("started_at")
                                or st.get("created_at"),
                                materialization_status=materialization_status,
                                materialized_tasks=_extract_int(
                                    st.get("materialized_tasks")
                                ),
                                materialization_total=_extract_int(
                                    st.get("materialization_total")
                                ),
                                materialization_error=st.get("materialization_error"),
                            )
                        )
                        total += 1
                        backfill_rows.append(
                            (mid, job_name_val, redis_status, site_val)
                        )

                    # Backfill missing jobs into central DB (best-effort)
                    if backfill_rows:

                        def _backfill_central_db() -> None:
                            try:
                                with global_session() as sess:
                                    for b_id, b_name, b_status, b_site in backfill_rows:
                                        existing = sess.get(CentralJobModel, b_id)
                                        if existing is None:
                                            sess.add(
                                                CentralJobModel(
                                                    job_id=b_id,
                                                    job_type=b_name,
                                                    status=b_status,
                                                    site=b_site,
                                                    args_json=json.dumps(
                                                        {"job_name": b_name}
                                                    ),
                                                )
                                            )
                                    sess.commit()
                            except Exception as bf_err:
                                logger.debug(
                                    "Backfill of Redis jobs to central DB failed: %s",
                                    bf_err,
                                )

                        asyncio.get_event_loop().run_in_executor(
                            None, _backfill_central_db
                        )

            except Exception as rf_err:
                logger.debug("Redis fallback scan for missing jobs failed: %s", rf_err)

        return JobListResponse(
            jobs=all_jobs,
            total=total,
            page=page,
            page_size=page_size,
            available_types=available_types,
        )

    except TaskLoadLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except SQLAlchemyError as exc:
        if getattr(getattr(exc, "orig", None), "sqlstate", None) == "57014":
            raise HTTPException(
                504, "Job search exceeded its database time limit. Narrow your search."
            ) from exc
        logger.exception("Failed to list jobs")
        return JobListResponse(jobs=[], total=0, page=page, page_size=page_size)
    except Exception:
        logger.exception("Failed to list jobs")
        return JobListResponse(
            jobs=[],
            total=0,
            page=page,
            page_size=page_size,
        )


@router.get("/active-tasks", response_model=ActiveTaskListResponse)
async def list_active_tasks(
    limit_jobs: int = Query(120, ge=1, le=1000),
    limit_tasks: int = Query(250, ge=1, le=100000),
    backend: BackendInstance = Depends(get_backend_instance),
) -> ActiveTaskListResponse:
    from ...job_system.ledger import ExecutionLedger
    tasks = []
    maximum = min(limit_tasks if isinstance(limit_tasks, int) else 250, 250)
    limit_jobs = limit_jobs if isinstance(limit_jobs, int) else 120
    rows = await run_database_call(ExecutionLedger().active, limit=maximum)
    for row in rows:
        task = Task.from_dict(row.pop("task"))
        progress, processed, total = _task_progress(task)
        tasks.append(ActiveTaskDetail(**row, task_id=task.task_id, task_type=task.task_type.value,
            status=task.state.value, site=task.site_name, username=task.user_username,
            worker_id=task.worker_id, started_at=task.started_at, completed_at=task.completed_at,
            progress=progress, items_processed=processed, items_total=total))
    return ActiveTaskListResponse(tasks=tasks, total=len(tasks))


@router.get(
    "/{job_id}/runtime-samples", response_model=JobRuntimeSamplesPage
)
async def get_job_runtime_samples(
    job_id: str,
    after_id: str | None = Query(None, pattern=r"^\d+-\d+$"),
    limit: int = Query(10_000, ge=1, le=10_000),
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobRuntimeSamplesPage:
    if backend.redis is None or not backend.redis.is_connected:
        raise HTTPException(status_code=503, detail="Redis is unavailable")
    storage = JobStorage(backend.redis)
    page = await storage.get_job_runtime_samples(
        job_id, after_id=after_id, limit=limit
    )
    return JobRuntimeSamplesPage(**page)


@router.get("/{job_id}", response_model=JobDetailResponse)
async def get_job(
    job_id: str,
    include_tasks: bool = Query(
        False,
        description="Compatibility flag. Use /jobs/{job_id}/tasks for paged task drilldown.",
    ),
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobDetailResponse:
    """Get detailed job information by ID.

    Args:
        job_id: Job ID
        backend: Backend instance (injected)

    Returns:
        Detailed job information including tasks
    """
    import json

    from sqlmodel import select as sql_select

    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        CentralTaskModel,
        global_session,
    )

    try:
        # Run sync SQLite lookup in thread to avoid blocking the event loop
        def _find_job_in_db() -> CentralJobModel | None:
            with global_session() as session:
                return session.exec(
                    sql_select(CentralJobModel).where(CentralJobModel.job_id == job_id)
                ).first()

        db_result = await run_database_call(_find_job_in_db)
        if db_result is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        job = db_result
        task_plan = await run_database_call(_read_task_plan_sync, job_id) if include_tasks else None

        redis_job = None
        storage: JobStorage | None = None
        redis_tasks: list[TaskDetail] = []
        redis_state: dict[str, str] = {}
        redis_config_present = False
        redis_has_task_records = False
        effective_status = job.status
        result_success = job.result_success
        result_error = job.result_error
        result_message = job.result_message
        task_stats = TaskStats()
        task_stats_observed_at_ms: int | None = None
        mps_refill_trace: list[dict[str, Any]] = []
        error_stats = JobErrorStats()
        last_activity_at = _coerce_iso_datetime(
            getattr(job, "last_event_at", None) or job.completed_at or job.started_at
        )
        completed_at_value = _coerce_iso_datetime(job.completed_at)
        runtime_telemetry: dict[str, Any] | None = None

        if backend.redis is not None:
            try:
                artifact_results = await _redis_pipeline_execute(
                    backend.redis,
                    hgetall_keys=[JobStorage.JOB_STATE_KEY.format(job_id=job_id)],
                    get_keys=[JobStorage.JOB_CONFIG_KEY.format(job_id=job_id)],
                    zrange_specs=[
                        (JobStorage.TASK_INDEX_KEY.format(job_id=job_id), 0, 0)
                    ],
                )
                raw_state = artifact_results[0] if len(artifact_results) > 0 else {}
                raw_config = artifact_results[1] if len(artifact_results) > 1 else None
                raw_task_index = (
                    artifact_results[2] if len(artifact_results) > 2 else []
                )
                if raw_state:
                    redis_state = dict(cast(dict[str, str], raw_state))
                    redis_status = redis_state.get("status")
                    if redis_status:
                        effective_status = redis_status
                redis_config_present = bool(raw_config)
                redis_has_task_records = _redis_zrange_has_members(raw_task_index)

                storage = JobStorage(cast(Any, backend.redis))
                runtime_telemetry = await storage.get_job_runtime_telemetry(job_id)
                redis_job = await storage.fetch_job_with_tasks(
                    job_id,
                    include_tasks=include_tasks and task_plan is None,
                    max_include_tasks=MAX_INLINE_TASK_LOAD,
                )
                task_stats = _build_task_stats_model(
                    await storage.get_job_task_stats(job_id)
                )
                task_stats_observed_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                last_activity_at = (
                    redis_job.updated_at
                    or redis_job.completed_at
                    or redis_job.started_at
                    or redis_job.created_at
                    or last_activity_at
                )
            except TaskLoadLimitExceeded as limit_error:
                raise HTTPException(
                    status_code=413, detail=str(limit_error)
                ) from limit_error
            except Exception as redis_error:
                logger.debug(
                    "Failed to load Redis job %s: %s",
                    job_id,
                    redis_error,
                )

        durable_header = None
        if redis_job is None or redis_job.metadata.get("execution_engine") == "ledger":
            durable_header = (await run_database_call(_fetch_durable_headers_sync, [job_id])).get(job_id)
            if durable_header is not None:
                redis_job = Job.from_dict(durable_header["config"])
                task_stats = _durable_task_stats(durable_header)
                effective_status = redis_job.state.value

        if runtime_telemetry is None and redis_job is not None:
            stored_telemetry = redis_job.metadata.get("runtime_telemetry")
            if isinstance(stored_telemetry, dict):
                runtime_telemetry = stored_telemetry

        if storage is not None and (
            redis_state.get("mps_manifest") == "1"
            or (redis_job is not None and redis_job.metadata.get("execution_engine") == "ledger")
        ):
            try:
                mps_refill_trace = await mps_state.get_refill_trace(
                    storage._client(), job_id, limit=64
                )
            except Exception as trace_error:
                logger.debug("Failed to load MPS refill trace for job %s: %s", job_id, trace_error)

        def _fetch_db_tasks() -> list[TaskDetail]:
            from sqlalchemy import func as sa_func

            with global_session() as session:
                task_total: int = session.exec(
                    sql_select(sa_func.count())
                    .select_from(CentralTaskModel)
                    .where(CentralTaskModel.job_id == job_id)
                ).one()  # type: ignore[arg-type, assignment]
                if task_total > MAX_INLINE_TASK_LOAD:
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            f"Refusing to load {task_total} tasks for job {job_id} "
                            f"(max allowed: {MAX_INLINE_TASK_LOAD}). "
                            "Use /jobs/{job_id}/tasks pagination or /tasks/stream."
                        ),
                    )
            return _fetch_central_task_details_sync(job_id)

        if include_tasks:
            plan = task_plan
            if redis_job is not None and redis_job.metadata.get("execution_engine") == "ledger":
                from ...job_system.ledger import ExecutionLedger
                if redis_job.total_tasks > MAX_INLINE_TASK_LOAD:
                    raise HTTPException(413, "Use task pagination for this job")
                page = await run_database_call(ExecutionLedger().task_page, job_id, limit=MAX_INLINE_TASK_LOAD)
                if page["has_more"]:
                    raise HTTPException(413, "Inline task history exceeds the byte allowance; use task pagination")
                redis_tasks = [_task_to_detail(Task.from_dict(payload)) for payload in page["tasks"]]
            elif plan is not None:
                from .mps_history import task_page

                if plan["total"] > MAX_INLINE_TASK_LOAD:
                    raise HTTPException(413, "Use /jobs/{job_id}/tasks pagination for this job's task history")
                history = await task_page(storage, job_id, offset=0, limit=max(1, plan["total"]), plan=plan)
                if history.has_more:
                    raise HTTPException(413, "Inline task history exceeds the history byte allowance; use task pagination")
                redis_tasks = history.tasks
            else:
                if redis_job:
                    redis_tasks = [_task_to_detail(task) for task in redis_job.tasks or []]
                if not redis_tasks:
                    redis_tasks = await run_database_call(_fetch_db_tasks)

            if result_error is None:
                error_messages = [
                    detail.error for detail in redis_tasks if detail.error
                ]
                if error_messages:
                    result_error = error_messages[0]
                elif redis_job is not None and redis_job.metadata.get("last_error"):
                    result_error = str(redis_job.metadata.get("last_error"))

            if (
                result_message is None
                and redis_job is not None
                and redis_job.metadata.get("result_message")
            ):
                result_message = str(redis_job.metadata.get("result_message"))

        if redis_tasks:
            error_stats = _build_error_stats_from_task_details(redis_tasks)
        else:
            try:
                error_stats = await asyncio.to_thread(
                    _fetch_central_error_stats_sync, job_id
                )
            except Exception:
                error_stats = JobErrorStats(total=0, groups={})

        # Parse args JSON
        args_data: dict[str, Any] | None = None
        if job.args_json:
            try:
                args_data = json.loads(job.args_json)
            except json.JSONDecodeError:
                pass

        run_count_value = None
        last_ran_at_value: str | None = None
        if redis_job is not None:
            run_count_value = _extract_int(redis_job.metadata.get("run_count"))
            last_ran_at_value = (
                str(redis_job.metadata.get("last_ran_at"))
                if redis_job.metadata.get("last_ran_at") is not None
                else None
            )
            if task_stats.total == 0:
                task_stats = TaskStats(
                    total=redis_job.total_tasks,
                    completed=redis_job.completed_tasks,
                    failed=redis_job.failed_tasks,
                )

        materialization_status = redis_state.get("materialization_status") or (
            str(redis_job.metadata.get("materialization_status"))
            if redis_job is not None
            and redis_job.metadata.get("materialization_status") is not None
            else None
        )
        materialized_tasks = _extract_int(redis_state.get("materialized_tasks"))
        if materialized_tasks is None and redis_job is not None:
            materialized_tasks = _extract_int(
                redis_job.metadata.get("materialized_tasks")
            )
        materialization_total = _extract_int(redis_state.get("materialization_total"))
        if materialization_total is None and redis_job is not None:
            materialization_total = _extract_int(
                redis_job.metadata.get("materialization_total")
            )
        materialization_error = redis_state.get("materialization_error") or (
            str(redis_job.metadata.get("materialization_error"))
            if redis_job is not None
            and redis_job.metadata.get("materialization_error") is not None
            else None
        )

        expected_task_count = task_stats.total
        if expected_task_count <= 0 and args_data:
            expected_task_count = _extract_int(args_data.get("task_count")) or 0

        has_redis_artifact = (
            bool(redis_state) or redis_config_present or redis_has_task_records
        )
        has_canonical_artifacts = redis_has_task_records and (
            redis_config_present or bool(redis_state)
        )
        is_stale_incomplete = durable_header is None and _is_stale_incomplete_active_job(
            status=effective_status,
            created_at=redis_state.get("created_at") or job.created_at,
            has_redis_artifact=has_redis_artifact,
            has_canonical_artifacts=has_canonical_artifacts,
            expected_task_count=expected_task_count,
            materialization_status=materialization_status,
            last_activity_at=job.last_event_at,
        )
        if is_stale_incomplete:
            logger.warning(
                "Reclassifying job %s detail view as failed; canonical Redis artifacts are missing",
                job_id,
            )
            effective_status = JobStatus.FAILED.value
            materialization_status = "failed"
            materialization_error = result_error or GHOST_JOB_ERROR
            result_success = False
            result_error = result_error or GHOST_JOB_ERROR
            if task_stats.total == 0 and expected_task_count > 0:
                task_stats.total = expected_task_count
            ghost_completed_at = datetime.now(timezone.utc).isoformat()
            completed_at_value = completed_at_value or ghost_completed_at
            last_activity_at = ghost_completed_at
            await _mark_central_jobs_failed([job_id], error_message=result_error)
        elif (
            result_error == GHOST_JOB_ERROR
            and _enum_value(effective_status).lower() in ACTIVE_LIVE_JOB_STATUSES
            and (
                has_canonical_artifacts
                or _enum_value(materialization_status).lower()
                in ACTIVE_MATERIALIZATION_STATUSES
            )
        ):
            # A deferred MPS job can be mistaken for a ghost while its task
            # index is still being built. Once Redis proves the job is live,
            # discard that stale central verdict in both this response and the
            # retained row used by subsequent list/detail requests.
            result_success = None
            result_error = None
            completed_at_value = None
            await _restore_central_ghost_jobs(
                {job_id: _enum_value(effective_status).lower()}
            )

        # Build response
        return JobDetailResponse(
            job_id=job.job_id,
            execution_engine=redis_job.metadata.get("execution_engine") if redis_job else None,
            job_name=args_data.get("job_name") if args_data else None,
            job_type=job.job_type,
            status=effective_status,
            site=job.site,
            priority=job.priority,
            worker_id=job.worker_id,
            retries=job.retries,
            max_retries=job.max_retries,
            created_at=(job.created_at.isoformat() if job.created_at else None),
            started_at=(
                redis_state.get("started_at")
                or (redis_job.started_at if redis_job is not None else None)
                or (job.started_at.isoformat() if job.started_at else None)
            ),
            completed_at=completed_at_value,
            result_success=result_success,
            result_message=result_message,
            result_error=result_error,
            args=args_data,
            tasks=redis_tasks,
            task_stats=task_stats,
            task_stats_observed_at_ms=task_stats_observed_at_ms,
            mps_refill_trace=mps_refill_trace,
            error_stats=error_stats,
            run_count=run_count_value if run_count_value is not None else 0,
            last_ran_at=last_ran_at_value,
            last_activity_at=last_activity_at,
            materialization_status=materialization_status,
            materialized_tasks=materialized_tasks,
            materialization_total=materialization_total,
            materialization_error=materialization_error,
            runtime_telemetry=runtime_telemetry,
        )

    except HTTPException:
        raise
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except Exception as e:
        logger.exception(f"Failed to get job {job_id}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{job_id}/tasks", response_model=TaskPageResponse)
async def get_job_tasks(
    job_id: str,
    cursor: str | None = Query(
        None, description="Cursor returned by the previous task page"
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    state: str | None = Query(None, description="Optional task-state filter"),
    task_type: str | None = Query(None, description="Optional task-type filter"),
    error_only: bool = Query(False, description="Only return tasks with errors"),
    backend: BackendInstance = Depends(get_backend_instance),
) -> TaskPageResponse:
    """Get paginated tasks for a job.

    Args:
        job_id: Job ID
        offset: Start index (0-based)
        limit: Maximum number of tasks to return (max 1000)
        backend: Backend instance (injected)
    """
    effective_offset = offset if isinstance(offset, int) else 0
    effective_limit = limit if isinstance(limit, int) else 100
    effective_state = state if isinstance(state, str) else None
    effective_task_type = task_type if isinstance(task_type, str) else None
    effective_error_only = error_only if isinstance(error_only, bool) else False

    from ...job_system.ledger import ExecutionLedger
    ledger = ExecutionLedger()
    if await run_database_call(ledger.job, job_id) is not None:
        ledger_cursor = cursor if isinstance(cursor, str) else ""
        after = ledger_cursor.removeprefix("ledger:") if ledger_cursor.startswith("ledger:") else ""
        page = await run_database_call(ledger.task_page, job_id, after=after,
            offset=0 if after else effective_offset, limit=effective_limit)
        details = [_task_to_detail(Task.from_dict(payload)) for payload in page["tasks"]]
        details = [detail for detail in details if _task_detail_matches_filters(
            detail, state=effective_state, task_type=effective_task_type, error_only=effective_error_only)]
        return TaskPageResponse(**{**page, "tasks": details}, offset=effective_offset, limit=effective_limit)

    raw_cursor = cursor if isinstance(cursor, str) else None
    effective_cursor = raw_cursor if raw_cursor is not None else str(effective_offset)
    if raw_cursor is not None:
        try:
            effective_offset = max(0, int(raw_cursor))
        except ValueError:
            effective_offset = offset if isinstance(offset, int) else 0

    plan = await run_database_call(_read_task_plan_sync, job_id)
    if plan is not None:
        from .mps_history import task_page

        return await task_page(JobStorage(backend.redis) if backend.redis is not None else None,
            job_id, offset=effective_offset, limit=effective_limit, state=effective_state,
            task_type=effective_task_type, error_only=effective_error_only, plan=plan)
    if backend.redis is not None:
        storage = JobStorage(cast(Any, backend.redis))
        page = await storage.fetch_task_page(
            job_id,
            cursor=effective_cursor,
            limit=effective_limit,
            state=effective_state,
            task_type=_normalize_task_type_filter(effective_task_type),
            error_only=effective_error_only,
        )
        if page.tasks or page.has_more or page.next_cursor is not None:
            return TaskPageResponse(
                tasks=[_task_to_detail(task) for task in page.tasks],
                next_cursor=page.next_cursor,
                has_more=page.has_more,
                scanned_count=page.scanned_count,
                offset=int(effective_cursor or "0"),
                limit=effective_limit,
            )

    central_page = await run_database_call(
        _fetch_central_task_page_sync,
        job_id,
        offset=effective_offset,
        limit=effective_limit,
        state=effective_state,
        task_type=effective_task_type,
        error_only=effective_error_only,
    )
    if (
        central_page.tasks
        or central_page.has_more
        or central_page.next_cursor is not None
    ):
        return central_page

    if backend.redis is None:
        return central_page

    return await _fetch_event_log_task_page(
        cast(Any, backend.redis),
        job_id,
        offset=effective_offset,
        limit=effective_limit,
        state=effective_state,
        task_type=effective_task_type,
        error_only=effective_error_only,
    )


@router.get("/{job_id}/tasks/{task_id}/evidence/{selection_id}")
async def get_task_media_evidence(
    job_id: str, task_id: str, selection_id: str,
    kind: str = Query("download_results"), after: int = Query(-1, ge=-1),
    limit: int = Query(100, ge=1),
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, Any]:
    """Return one task-owned evidence page without loading its full report."""
    from ...database.media_selection import MediaSelectionStore, page_byte_budget
    task = await _fetch_job_task_detail(job_id, task_id, backend)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    store = MediaSelectionStore()
    try:
        await asyncio.to_thread(store.scope, selection_id, job_id=job_id, task_id=task_id)
        page = await asyncio.to_thread(store.evidence_page, selection_id, kind=kind,
            after=after, limit=limit, max_bytes=page_byte_budget())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return page


@router.get(
    "/{job_id}/tasks/{task_id}/files/{result_index}",
    response_model=None,
)
async def stream_job_task_download_file(
    job_id: str,
    task_id: str,
    result_index: int,
    backend: BackendInstance = Depends(get_backend_instance),
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> FileResponse:
    """Stream the exact local file retained in a download task result.

    The result index is resolved entirely from backend-owned task state. The
    retained path must also resolve beneath a configured download directory,
    preventing this route from becoming an arbitrary filesystem reader.
    """

    if result_index < 0:
        raise HTTPException(status_code=404, detail="Download result not found")

    task = await _fetch_job_task_detail(job_id, task_id, backend)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    output = (task.result or {}).get("output")
    reference = output.get("media_evidence") if isinstance(output, dict) else None
    if isinstance(reference, dict):
        from ...database.media_selection import MediaSelectionStore, page_byte_budget
        store = MediaSelectionStore()
        try:
            await asyncio.to_thread(store.scope, reference["selection_id"], job_id=job_id, task_id=task_id)
            page = await asyncio.to_thread(store.evidence_page, reference["selection_id"],
                kind="download_results", after=result_index - 1, limit=1, max_bytes=page_byte_budget())
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=404, detail="Download result not found") from exc
        if not page["items"] or page["ordinals"][0] != result_index:
            raise HTTPException(status_code=404, detail="Download result not found")
        task = task.model_copy(update={"result": {"output": {"download_results": page["items"]}}})
        result_index = 0

    file_path = await asyncio.to_thread(
        _resolve_task_download_file,
        task,
        result_index,
        config,
    )
    media_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(
        path=file_path,
        media_type=media_type or "application/octet-stream",
        filename=file_path.name,
        headers={"Cache-Control": "private, max-age=86400"},
    )


@router.get("/{job_id}/tasks/stream")
async def stream_job_tasks(
    job_id: str,
    cursor: str | None = Query(None, description="Optional starting cursor"),
    batch_size: int = Query(100, ge=1, le=1000),
    state: str | None = Query(None, description="Optional task-state filter"),
    task_type: str | None = Query(None, description="Optional task-type filter"),
    error_only: bool = Query(False, description="Only stream tasks with errors"),
    backend: BackendInstance = Depends(get_backend_instance),
):
    """Stream all tasks for a job using Server-Sent Events (SSE).

    This allows the client to consume millions of tasks without
    the backend or browser running out of RAM.

    Args:
        job_id: Job ID
        batch_size: Number of tasks to fetch per chunk
        backend: Backend instance (injected)
    """

    effective_cursor = cursor if isinstance(cursor, str) else None
    effective_batch_size = batch_size if isinstance(batch_size, int) else 100
    effective_state = state if isinstance(state, str) else None
    effective_task_type = task_type if isinstance(task_type, str) else None
    effective_error_only = error_only if isinstance(error_only, bool) else False

    async def event_generator():
        try:
            emitted = False

            if backend.redis is not None:
                storage = JobStorage(cast(Any, backend.redis))
                if await storage._client().hget(storage.JOB_STATE_KEY.format(job_id=job_id), 'mps_manifest') in (b'1', '1'):
                    from .mps_history import task_page

                    page_cursor = max(int(effective_cursor or '0'), 0)
                    while True:
                        page = await task_page(storage, job_id, offset=page_cursor, limit=effective_batch_size,
                            state=effective_state, task_type=effective_task_type, error_only=effective_error_only)
                        for detail in page.tasks:
                            yield f"data: {detail.model_dump_json()}\n\n"
                        if page.next_cursor is None:
                            break
                        page_cursor = int(page.next_cursor)
                    yield "data: [DONE]\n\n"
                    return
                async for task in storage.fetch_tasks_stream(
                    job_id,
                    effective_batch_size,
                    cursor=effective_cursor,
                    state=effective_state,
                    task_type=_normalize_task_type_filter(effective_task_type),
                    error_only=effective_error_only,
                ):
                    emitted = True
                    detail = _task_to_detail(task)
                    yield f"data: {detail.model_dump_json()}\n\n"

            if not emitted:
                page_cursor = int(effective_cursor or "0") if effective_cursor else 0
                while True:
                    batch = await asyncio.to_thread(
                        _fetch_central_task_page_sync,
                        job_id,
                        offset=page_cursor,
                        limit=effective_batch_size,
                        state=effective_state,
                        task_type=effective_task_type,
                        error_only=effective_error_only,
                    )
                    if not batch.tasks:
                        break
                    emitted = True
                    for detail in batch.tasks:
                        yield f"data: {detail.model_dump_json()}\n\n"
                    if not batch.next_cursor:
                        break
                    page_cursor = int(batch.next_cursor)

            yield "data: [DONE]\n\n"
        except Exception as e:
            logger.error(f"Error streaming tasks for job {job_id}: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/{job_id}/template", response_model=JobTemplateResponse)
async def create_job_template_from_job_id(
    job_id: str,
    request: CreateJobTemplateRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobTemplateResponse:
    """Create a job template from an existing job."""
    if backend.redis is None or not backend.redis.is_connected:
        raise HTTPException(
            status_code=400,
            detail="Saving templates requires Redis to be enabled and connected",
        )

    storage = JobStorage(backend.redis)
    try:
        job = await storage.fetch_job_with_tasks(
            job_id,
            include_tasks=True,
            max_include_tasks=MAX_INLINE_TASK_LOAD,
        )
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if not job.tasks:
        if job.state in {JobStatus.COMPLETED, JobStatus.CANCELLED}:
            _raise_archived_terminal_job_payload_error("template creation")
        raise HTTPException(status_code=400, detail="Job has no tasks to template")

    template = _build_job_template(
        name=request.name.strip(),
        description=request.description,
        created_by=request.created_by,
        tasks=job.tasks,
        job_name_template=job.name,
    )

    if not template.name:
        raise HTTPException(status_code=400, detail="Template name is required")

    store = JobTemplateStore(cast(Any, backend.redis))
    try:
        created = await store.create_template(template)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _template_to_response(created)


@router.patch(
    "/{job_id}", response_model=JobResponse,
    openapi_extra={"requestBody": request_body_schema(UpdateJobRequest)},
)
async def update_job(
    job_id: str,
    request: Request,
    backend: BackendInstance = Depends(get_backend_instance),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> JobResponse:
    """Update metadata or stream task replacement inputs into durable storage."""
    # Direct Python callers already have a validated model.
    if isinstance(request, UpdateJobRequest):
        return await _update_job(job_id, request, backend, backend_manager)
    if backend.redis is None or not backend.redis.is_connected:
        raise HTTPException(400, "Updating jobs requires Redis to be enabled and connected")
    manager = JobManager(backend.redis)
    try:
        job = await manager.fetch_job_for_update(job_id)
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Could not read the retained job for editing") from exc
    if job is None:
        raise HTTPException(404, f"Job {job_id} not found")
    from ...database.write_queue import _positive_int_env
    from ...job_system.resources import window_policy

    writes = manager.database_writes
    is_mps = bool(job.metadata.get("mps_manifest")) and job.metadata.get("durable_job_kind") != "ordinary"
    async with read_job_update(
        request,
        byte_limit=min(writes.max_command_bytes, writes.max_queue_bytes),
        default_inflight_bytes=writes.max_queue_bytes,
        batch_limit=(
            window_policy(job.metadata, JobStorage).replacement_rows
            if is_mps else _positive_int_env(
                "ULTIMA_JOB_TASK_INPUT_BATCH_SIZE", JobStorage.DEFAULT_WRITE_BATCH_SIZE
            )
        ),
        resource_prefix="ULTIMA_MPS_UPLOAD" if is_mps else "ULTIMA_JOB_UPLOAD",
        request_model=UpdateJobRequest, task_model=TaskPayload,
    ) as (model, payloads):
        async def build_tasks():
            async for payload in payloads:
                task = await _build_task_from_payload(payload, backend_manager)
                async with aclosing(_built_tasks(task)) as built:
                    async for item in built:
                        yield item

        return await _update_job(
            job_id, model, backend, backend_manager,
            streamed_tasks=build_tasks() if payloads is not None else None,
        )


async def _update_job(
    job_id: str,
    request: UpdateJobRequest,
    backend: BackendInstance = Depends(get_backend_instance),
    backend_manager: BackendManager = Depends(get_backend_manager),
    *,
    streamed_tasks: AsyncIterable[Task] | None = None,
) -> JobResponse:
    """Update a job's name or schedule."""
    if backend.redis is None or not backend.redis.is_connected:
        raise HTTPException(
            status_code=400,
            detail="Updating jobs requires Redis to be enabled and connected",
        )

    from ...job_system.ordinary_edit import EditInProgress, EditPersistenceError

    request_payload = request.model_dump(exclude={"tasks"})
    updated_name = request.job_name.strip() if request.job_name is not None else None

    async def build_requested_tasks():
        for task_payload in request.tasks or ():
            result = await _build_task_from_payload(task_payload, backend_manager)
            async with aclosing(_built_tasks(result)) as built:
                async for task in built:
                    yield task

    schedule = (
        _build_schedule_from_request(request.schedule)
        if request.schedule is not None
        else None
    )

    job_manager = JobManager(backend.redis)
    try:
        job = await job_manager.update_job_by_id(
            job_id,
            name=updated_name if request.job_name is not None else None,
            schedule=schedule,
            tasks=(
                streamed_tasks if streamed_tasks is not None
                else build_requested_tasks() if request.tasks is not None else None
            ),
            route_immediate_job=backend.distributed_mode,
            args_patch={key: value for key, value in request_payload.items() if value is not None},
        )
    except (HTTPException, RequestValidationError):
        raise
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except EditInProgress as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except EditPersistenceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(status_code=404, detail=message) from exc
        raise HTTPException(status_code=400, detail=message) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Job edit %s could not finish; pending publication can recover", job_id)
        raise HTTPException(status_code=503, detail="Job edit could not finish; any committed inputs are retained for recovery") from exc

    return _job_to_response(job)


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: str,
    request: RetryJobRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> JobResponse:
    """Retry a job by cloning failed tasks and overriding the auth ID.

    Args:
        job_id: Job ID to retry
        request: Retry request payload
        backend: Backend instance (injected)

    Returns:
        Newly created retry job
    """
    if backend.redis is None:
        raise HTTPException(
            status_code=400,
            detail="Retry requires Redis-backed job storage to load tasks",
        )

    from sqlmodel import select as sql_select

    from ultima_scraper_db_archive import UserAuthModel
    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        CentralTaskModel,
        global_session,
    )

    from ...database.factory import MultiSiteDatabaseManager

    redis_manager = backend.redis
    storage = JobStorage(redis_manager)
    job_manager = JobManager(redis_manager)
    header = await storage.fetch_job(job_id, include_tasks=False)
    if header is not None and header.metadata.get("execution_engine") == "ledger":
        from ...job_system.ledger import ExecutionLedger
        try:
            retried = await run_database_call(ExecutionLedger().retry, job_id, auth_id=request.auth_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if retried is None:
            raise HTTPException(status_code=404, detail="Job was deleted before retry")
        return _job_to_response(Job.from_dict(retried))
    if header is None or header.metadata.get("mps_manifest"):
        from ...database.mps_manifest import MPSManifestStore
        from . import mps_durable

        manifest = await run_database_call(MPSManifestStore().get, job_id)
        if manifest is not None:
            return await mps_durable.retry_job(
                manifest, request=request, backend=backend
            )
        if header is not None:
            raise HTTPException(
                status_code=409, detail="The durable MPS manifest is missing"
            )

    from ...database.write_queue import _positive_int_env

    insert_batch_size = _central_task_insert_batch_size()
    auth_batch_size = _positive_int_env(
        "ULTIMA_JOB_AUTH_VALIDATION_BATCH_SIZE", insert_batch_size
    )

    try:
        original_job, retry_job = await job_manager.prepare_retry_job(
            job_id,
            auth_id=request.auth_id,
            max_include_tasks=MAX_INLINE_TASK_LOAD,
        )
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Job {job_id} not found in Redis task store. "
                "This job can no longer be retried; create a new job instead."
            ),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        site_name = "onlyfans"
        if retry_job.tasks:
            site_name = str(retry_job.tasks[0].site_name).lower()

        def _persist_retry_to_db() -> None:
            # Validate only identifier columns in bounded task groups. Do not
            # hydrate credentials or retain a job-wide set of auth identities.
            site_db_manager = MultiSiteDatabaseManager()
            validated_auths: dict[str, set[int]] = {}
            for task_batch in batched(retry_job.tasks, auth_batch_size):
                auths_by_site: dict[str, set[int]] = {}
                for task in task_batch:
                    aid = request.auth_id if request.auth_id is not None else task.auth_id
                    if aid is not None:
                        auths_by_site.setdefault(str(task.site_name).lower(), set()).add(aid)
                for auth_site, auth_ids in auths_by_site.items():
                    unchecked_ids = auth_ids - validated_auths.get(auth_site, set())
                    if not unchecked_ids:
                        continue
                    site_db = site_db_manager.get_or_create_manager(auth_site)
                    if site_db is None:
                        continue
                    site_session = site_db.get_session()
                    try:
                        active_ids = set(site_session.exec(
                            sql_select(UserAuthModel.user_id).where(
                                UserAuthModel.user_id.in_(unchecked_ids),
                                UserAuthModel.site == auth_site,
                                UserAuthModel.active.is_(True),
                            ).distinct()
                        ).all())
                    finally:
                        site_session.close()
                    missing = unchecked_ids - active_ids
                    if missing:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Auth {min(missing)} is not active for {auth_site}",
                        )
                # Reuse adjacent groups without accumulating every job identity.
                validated_auths = auths_by_site

            # Persist to central DB
            with global_session() as session:
                try:
                    now_dt = datetime.now(timezone.utc)
                    original_db_job = session.exec(
                        sql_select(CentralJobModel).where(
                            CentralJobModel.job_id == original_job.job_id
                        )
                    ).first()
                    if original_db_job:
                        original_db_job.status = JobStatus.CANCELLED.value
                        original_db_job.result_message = (
                            f"Superseded by retry job {retry_job.job_id}"
                        )
                        original_db_job.result_error = None
                        original_db_job.completed_at = now_dt
                        original_db_job.last_event_at = now_dt
                        session.add(original_db_job)

                    db_job = CentralJobModel(
                        job_id=retry_job.job_id,
                        job_type="retry",
                        job_config_json=json.dumps(retry_job.to_config(include_task_ids=False).to_dict()),
                        status=JobStatus.PENDING.value,
                        args_json=json.dumps(
                            {
                                "job_name": retry_job.name,
                                "created_by": retry_job.created_by,
                                "task_count": len(retry_job.tasks),
                                "retry_of": original_job.job_id,
                                "retry_auth_id": request.auth_id,
                            }
                        ),
                        site=site_name,
                    )
                    session.add(db_job)
                    for rows in _central_task_row_batches(
                        job_id=retry_job.job_id,
                        tasks=retry_job.tasks,
                        batch_size=insert_batch_size,
                    ):
                        session.execute(insert(CentralTaskModel), rows)
                    db_job.last_event_at = datetime.now(timezone.utc)
                    session.commit()
                except Exception:
                    session.rollback()
                    raise

        await run_database_call(_persist_retry_to_db)
    except HTTPException:
        raise
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning("Failed to persist retry job to database: %s", exc)
        raise HTTPException(
            status_code=503, detail="Failed to persist retry job to database"
        ) from exc

    try:
        _assigned_backend_id, deferred_tasks = await job_manager.activate_retry_job(
            original_job=original_job,
            retry_job=retry_job,
            cancel_reason=f"superseded by retry job {retry_job.job_id}",
            distributed_mode=backend.distributed_mode,
            preferred_backend_id=(
                None if backend.distributed_mode else backend.instance_id
            ),
        )
    except Exception as activation_error:
        logger.warning(
            "Failed to activate retry job %s for original job %s: %s",
            retry_job.job_id,
            original_job.job_id,
            activation_error,
        )
        raise HTTPException(
            status_code=500, detail="Failed to activate retry job"
        ) from activation_error

    await publish_ws_event(
        backend.redis,
        {
            "type": "webui_ack",
            "event": "job_retry",
            "job_id": retry_job.job_id,
            "job_name": retry_job.name,
            "status": JobStatus.PENDING.value,
            "task_count": len(retry_job.tasks),
            "deferred_tasks": len(deferred_tasks),
            "retry_of": original_job.job_id,
            "retry_auth_id": request.auth_id,
        },
    )

    response = _job_to_response(retry_job)
    response.task_count = len(retry_job.tasks)
    response.task_stats = _build_task_stats_model(
        await storage.get_job_task_stats(retry_job.job_id)
    )
    response.error_stats = JobErrorStats(total=0, groups={})
    return response


@router.post("/{job_id}/prioritize")
async def prioritize_job(
    job_id: str,
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, str | int]:
    """Boost a running job so its queued tasks are claimed before others.

    Re-scores the job's tasks in the backend queue with ``priority = -1``
    which produces lower scores than the default ``0``, causing ZPOPMIN
    to claim them first.

    Args:
        job_id: Job ID to prioritize
        backend: Backend instance (injected)

    Returns:
        Confirmation with the number of re-scored tasks.
    """
    if backend.redis is None:
        raise HTTPException(
            status_code=400,
            detail="Prioritize requires Redis-backed job storage",
        )

    job_manager = JobManager(backend.redis)
    try:
        job, rescored = await job_manager.prioritize_job_by_id(job_id, priority=-1)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await publish_ws_event(
        backend.redis,
        {
            "type": "webui_ack",
            "event": "job_prioritized",
            "job_id": job_id,
            "job_name": job.name,
            "rescored": rescored,
        },
    )

    return {"job_id": job_id, "rescored": rescored, "priority": -1}


async def _run_cancel_cleanup(redis_manager: Any, job_id: str) -> None:
    try:
        await JobManager(redis_manager).finalize_job_cancellation(job_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Deferred cancel cleanup failed for %s: %s", job_id, exc)


def _schedule_cancel_cleanup(redis_manager: Any, job_id: str) -> None:
    spawn_background_task(
        _run_cancel_cleanup(redis_manager, job_id),
        name=f"cancel-job-cleanup-{job_id}",
    )


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: str,
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, str]:
    """Cancel a pending or running job immediately.

    The request path performs the fast control-plane work synchronously:
    mark the job cancelled, clear live queue routing, and notify the UI.
    Expensive dedupe cleanup is deferred so very large jobs do not time out
    in the browser while cancellation is already taking effect.

    Args:
        job_id: Job ID to cancel
        backend: Backend instance (injected)

    Returns:
        Confirmation dict with ``job_id`` and ``status``.
    """
    if backend.redis is None:
        raise HTTPException(
            status_code=400,
            detail="Cancel requires Redis-backed job storage",
        )

    job_manager = JobManager(backend.redis)
    if getattr(backend, "writes", None) is not None:
        job_manager.database_writes = backend.writes
    try:
        job = await job_manager.cancel_job_by_id(
            job_id,
            reason="cancelled by user",
            wait_for_resource_cleanup=False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except StaleTaskDefinition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    cleanup_ready = True
    if job.metadata.get("execution_engine") == "ledger":
        return {"job_id": job_id, "status": job.state.value}
    stale_definition = False
    completed_at = datetime.now(timezone.utc).isoformat()
    try:
        status_result = await job_manager.database_writes.submit(
            DatabaseWriteOperation.SYNC_JOB_STATUS,
            {
                **({"mps_task_manifest": job.metadata.get("mps_task_manifest") or ""} if job.metadata.get("mps_manifest") else {}),
                "job_id": job_id,
                "status": JobStatus.CANCELLED.value,
                "error": None,
                "completed_at": completed_at,
                "job_snapshot": {
                    "name": job.name,
                    "created_by": job.created_by,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                    "metadata": dict(job.metadata),
                },
            },
            command_id=stable_command_id(
                "job-status-v2",
                job_id,
                JobStatus.CANCELLED.value,
                completed_at,
            ),
        )
        stale_definition = bool(status_result.get("stale"))
        cleanup_ready = bool(status_result.get("synced")) and not stale_definition
    except Exception as exc:  # noqa: BLE001
        cleanup_ready = False
        logger.warning(
            "Cancelled job %s in Redis but skipped task-blob purge because central DB sync failed: %s",
            job_id,
            exc,
        )

    if stale_definition:
        raise HTTPException(status_code=409, detail="The job definition changed during cancellation; the current definition was retained")

    if cleanup_ready:
        _schedule_cancel_cleanup(backend.redis, job_id)

    await publish_ws_event(
        backend.redis,
        {
            "type": "webui_ack",
            "event": "job_cancelled",
            "job_id": job_id,
            "job_name": job.name,
            "status": "cancelled",
        },
    )

    return {"job_id": job_id, "status": "cancelled"}


async def _purge_remaining_tasks_background(job_id: str, storage: Any = None) -> None:
    """Asynchronously purge remaining task rows in bounded background batches."""
    try:
        total = await asyncio.to_thread(_purge_job_tasks_sync, job_id)
        if total:
            logger.info("Background task purge removed %d task(s) for deleted job %s", total, job_id)
        if storage is not None and getattr(storage, "redis", None) is not None:
            try:
                client = getattr(storage, "_client", lambda: storage.redis.client)()
                await client.srem(storage.PENDING_TASK_PURGE_KEY, job_id)
            except Exception:
                pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("Background task purge failed for job %s: %s", job_id, exc)


@router.post("/bulk-delete", response_model=DeleteJobsResponse)
async def bulk_delete_jobs(
    request: DeleteJobsRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> DeleteJobsResponse:
    return await _bulk_delete_jobs(request, backend)


async def _bulk_delete_jobs(request, backend, progress=None) -> DeleteJobsResponse:
    """Delete selected jobs or every job older than the requested cutoff."""
    job_ids = list(dict.fromkeys(job_id.strip() for job_id in request.job_ids if job_id.strip()))
    if bool(job_ids) == (request.older_than_hours is not None):
        raise HTTPException(
            status_code=422,
            detail="Provide either job_ids or older_than_hours, but not both",
        )

    if request.older_than_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=request.older_than_hours)
        try:
            job_ids = await run_database_call(_find_job_ids_older_than_sync, cutoff)
        except SQLAlchemyError as exc:
            raise HTTPException(status_code=503, detail="Could not find jobs for deletion") from exc

    failure_map: dict[str, str] = {}
    completed = 0
    queued = 0
    if progress:
        progress({"type": "progress", "completed": 0, "total": len(job_ids)})
    semaphore = asyncio.Semaphore(2)

    async def _delete_single(jid: str) -> None:
        nonlocal completed, queued
        async with semaphore:
            try:
                if progress:
                    loop = asyncio.get_running_loop()
                    def cleanup_progress(table, rows):
                        event = ({"type": "state", "job_id": jid, "status": table}
                                 if table in {"cancelled", "deleting"} else
                                 {"type": "cleanup", "job_id": jid, "table": table, "rows": rows})
                        loop.call_soon_threadsafe(progress, event)
                    result = await _delete_job_impl(jid, backend, cleanup_progress)
                else:
                    result = await delete_job(jid, backend=backend)
                if result and result.get("status") == "deleting":
                    queued += 1
            except HTTPException as exc:
                detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
                failure_map[jid] = detail
            except Exception as exc:  # noqa: BLE001
                logger.exception("Bulk deletion failed for job %s", jid)
                failure_map[jid] = str(exc)
            finally:
                completed += 1
                if progress:
                    progress({"type": "progress", "completed": completed,
                              "total": len(job_ids), "job_id": jid,
                              "failed": len(failure_map)})

    await asyncio.gather(*(_delete_single(jid) for jid in job_ids))
    failures = [
        DeleteJobFailure(job_id=jid, error=failure_map[jid])
        for jid in job_ids
        if jid in failure_map
    ]

    return DeleteJobsResponse(
        requested=len(job_ids),
        deleted=len(job_ids) - len(failures) - queued,
        queued=queued,
        failed=len(failures),
        failures=failures,
    )


@router.post("/bulk-delete-stream")
async def bulk_delete_jobs_stream(
    request: DeleteJobsRequest,
    backend: BackendInstance = Depends(get_backend_instance),
):
    """Stream actual job completion and heartbeats during large SQL purges."""
    # Validate before opening a successful response.
    if bool([jid for jid in request.job_ids if jid.strip()]) == (request.older_than_hours is not None):
        raise HTTPException(status_code=422, detail="Provide either job_ids or older_than_hours, but not both")
    queue = asyncio.Queue()

    async def run():
        try:
            result = await _bulk_delete_jobs(request, backend, queue.put_nowait)
            queue.put_nowait({"type": "result", **result.model_dump()})
        except Exception as exc:
            queue.put_nowait({"type": "error", "error": str(getattr(exc, "detail", exc))})

    # Keep deletion running if the browser disconnects halfway through cleanup.
    task = spawn_background_task(run())

    async def events():
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=5)
            except asyncio.TimeoutError:
                event = {"type": "heartbeat"}
            yield json.dumps(event) + "\n"
            if event["type"] in {"result", "error"}:
                await task
                break

    return StreamingResponse(events(), media_type="application/x-ndjson",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.delete("/{job_id}")
async def delete_job(
    job_id: str,
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, str]:
    return await _delete_job_impl(job_id, backend)


async def _delete_job_impl(job_id, backend, progress=None):
    """Delete durable work before releasing its bounded runtime window."""
    from contextlib import AsyncExitStack

    from sqlalchemy import delete as sql_delete, select as sql_select
    from sqlalchemy.exc import SQLAlchemyError
    from ultima_scraper_db_archive.global_db import (
        CentralJobModel, CentralTaskModel, MPSPlanModel, global_session,
    )

    from ...job_system import mps_attempt, mps_state
    from ...job_system.storage import JobStorage
    from ...job_system import durable as mps_durable

    storage = JobStorage(backend.redis) if backend.redis is not None else None
    stored_job = await storage.fetch_job(job_id, include_tasks=False) if storage else None
    from ...job_system.ledger import ExecutionLedger
    # SQL owns execution; Redis may be absent or have an expired projection.
    ledger = ExecutionLedger(global_session)
    if await run_database_call(ledger.begin_delete, job_id):
        if progress:
            progress("deleting", 0)
        return {"job_id": job_id, "status": "deleting"}
    def read_definition() -> tuple[str | None, str | None]:
        with global_session() as session:
            plan = session.execute(
                sql_select(MPSPlanModel.selection_json, MPSPlanModel.status)
                .where(MPSPlanModel.job_id == job_id)
            ).first()
            if plan is None:
                return None, None
            selection, status = plan
            return (
                (json.loads(selection).get("task_manifest_id") or "")
                if selection is not None
                else None,
                str(status) if status is not None else None,
            )

    try:
        manifest_id, mps_plan_status = await asyncio.to_thread(read_definition)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Could not read the job for deletion") from exc

    if storage is not None and (
        manifest_id is not None
        or (stored_job is not None and stored_job.metadata.get("mps_manifest"))
    ):
        lock_val = await storage._client().get(mps_attempt.lock_key(job_id))
        lock_str = (
            lock_val.decode() if isinstance(lock_val, bytes) else str(lock_val or "")
        )
        if lock_str.startswith("mps-edit:"):
            raise HTTPException(
                status_code=409, detail="MPS job is changing; retry deletion"
            )

    if storage is not None and _job_requires_cancellation_before_delete(
        stored_job,
        mps_plan_status=mps_plan_status if manifest_id is not None else None,
    ):
        # Cancellation owns the control-plane transition and waits for any
        # SQL status admission before this route removes durable history.
        await cancel_job(job_id, backend=backend)
        refreshed_job = await storage.fetch_job(job_id, include_tasks=False)
        if refreshed_job is not None:
            stored_job = refreshed_job

    identity = None
    if stored_job is not None and stored_job.metadata.get("mps_manifest"):
        identity = mps_attempt.Identity(job_id, "", stored_job.metadata.get("mps_task_manifest") or "")
        if manifest_id is not None and identity.manifest_id != manifest_id:
            raise HTTPException(status_code=409, detail="The job definition changed; retry deletion")

    def delete_from_db() -> tuple[bool, bool]:
        with global_session() as session:
            plan = session.execute(
                sql_select(MPSPlanModel).where(MPSPlanModel.job_id == job_id).with_for_update()
            ).scalar_one_or_none()
            if plan is not None:
                current = json.loads(plan.selection_json).get("task_manifest_id") or ""
                if current != manifest_id:
                    raise mps_attempt.StaleTaskDefinition("Job changed before deletion")
                # Existing foreign keys cascade to ranges, uploaded definitions,
                # and their staging plans. No profile/task collection is loaded.
                session.execute(sql_delete(MPSPlanModel).where(MPSPlanModel.job_id == job_id))

            has_tasks = bool(session.execute(
                sql_select(CentralTaskModel.task_id).where(CentralTaskModel.job_id == job_id).limit(1)
            ).first())

            deleted_initial = 0
            if has_tasks:
                from sqlalchemy import text
                try:
                    deleted_initial = session.execute(
                        text("""
                            WITH batch AS (
                                SELECT tableoid,ctid FROM management.job_tasks
                                WHERE job_id = :job_id
                                LIMIT 5000
                            )
                            DELETE FROM management.job_tasks
                            WHERE job_id=:job_id AND (tableoid,ctid) IN (SELECT tableoid,ctid FROM batch)
                        """),
                        {"job_id": job_id},
                    ).rowcount
                except Exception:
                    subq = session.execute(
                        sql_select(CentralTaskModel.task_id).where(CentralTaskModel.job_id == job_id).limit(5000)
                    ).scalars().all()
                    if subq:
                        deleted_initial = session.execute(
                            sql_delete(CentralTaskModel).where(CentralTaskModel.job_id == job_id, CentralTaskModel.task_id.in_(subq))
                        ).rowcount

            jobs_removed = session.execute(sql_delete(CentralJobModel).where(CentralJobModel.job_id == job_id))
            found = plan is not None or has_tasks or bool(jobs_removed.rowcount)
            session.commit()

            more_tasks_remain = False
            if has_tasks and deleted_initial >= 5000:
                more_tasks_remain = bool(session.execute(
                    sql_select(CentralTaskModel.task_id).where(CentralTaskModel.job_id == job_id).limit(1)
                ).first())
            return found, more_tasks_remain

    try:
        async with AsyncExitStack() as stack:
            if manifest_id is not None or identity is not None:
                if storage is None:
                    raise HTTPException(status_code=503, detail="Redis is required to serialize MPS deletion")
                key, owner = await stack.enter_async_context(mps_durable._schedule_edit_lock(storage, job_id))
                await mps_durable._check_owner(storage._client(), key, owner)
            found_in_db, more_tasks_remain = await asyncio.to_thread(delete_from_db)
            if identity is not None:
                # Stop new worker claims before releasing the publication lease,
                # even if the following Redis cleanup needs a request retry.
                async with storage._client().pipeline(transaction=True) as pipe:
                    state_key = storage.JOB_STATE_KEY.format(job_id=job_id)
                    await pipe.watch(key, state_key)
                    await mps_durable._check_owner(pipe, key, owner)
                    current = await pipe.hget(state_key, "mps_task_manifest")
                    current = current.decode() if isinstance(current, bytes) else current
                    if (current or "") != identity.manifest_id:
                        raise mps_attempt.StaleTaskDefinition("Job changed during deletion")
                    if await pipe.exists(state_key):
                        pipe.multi()
                        pipe.hset(state_key, "status", "cancelled")
                        await pipe.execute()
        # Definition guards wait for edits. Release this request's edit lease
        # before runtime cleanup; SQL no longer admits work for the deleted plan.
        redis_removed = False
        if storage is not None:
            redis_removed = stored_job is not None or bool(await storage._client().exists(
                storage.TASK_INDEX_KEY.format(job_id=job_id),
                mps_state.PLAN_KEY.format(job_id=job_id),
            ))
            if found_in_db or redis_removed:
                with mps_attempt.scope(identity):
                    await storage.delete_job(job_id)
        if more_tasks_remain:
            if storage is not None:
                try:
                    await storage._client().sadd(storage.PENDING_TASK_PURGE_KEY, job_id)
                except Exception:
                    pass
            spawn_background_task(_purge_remaining_tasks_background(job_id, storage=storage))
    except mps_attempt.StaleTaskDefinition as exc:
        raise HTTPException(status_code=409, detail="The job definition changed during deletion; the current definition was retained") from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Could not delete the job from the database; runtime state was retained") from exc
    except RuntimeError as exc:
        if "is materializing" in str(exc):
            raise HTTPException(status_code=409, detail="MPS job is changing; retry deletion") from exc
        raise

    if not found_in_db and not redis_removed:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    await publish_ws_event(backend.redis, {
        "type": "webui_ack", "event": "job_deleted", "job_id": job_id,
        "deleted_from_db": found_in_db, "deleted_from_redis": redis_removed,
    })
    return {"status": "deleted", "job_id": job_id}
