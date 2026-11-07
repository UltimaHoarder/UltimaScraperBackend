"""Present jobs and tasks as API response models and task pages."""

from __future__ import annotations

from typing import TYPE_CHECKING
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
import json
from pathlib import Path
from typing import Any, Mapping, Sequence, cast
from ...config import UltimaScraperCollectionConfig
from ...job_system import Job, JobStatus, Task, TaskState
from .job_schemas import JobErrorStats, JobResponse, TaskDetail, TaskPageResponse, TaskStats

if TYPE_CHECKING:
    from ultima_scraper_db_archive.global_db import CentralTaskModel


GHOST_JOB_GRACE_PERIOD = timedelta(seconds=30)


GHOST_JOB_ERROR = (
    "Job initialization failed; canonical Redis job was never fully created"
)


ACTIVE_LIVE_JOB_STATUSES = frozenset(
    {
        JobStatus.PENDING.value,
        JobStatus.RUNNING.value,
        "claimed",
    }
)


ACTIVE_MATERIALIZATION_STATUSES = frozenset(
    {
        "queued",
        "materializing",
        "finalizing",
    }
)


def _extract_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _first_int(mapping: dict[str, Any], keys: list[str]) -> int | None:
    for key in keys:
        if key in mapping:
            candidate = _extract_int(mapping.get(key))
            if candidate is not None:
                return candidate
    return None


def _stringify_mapping(value: Mapping[Any, Any] | None) -> dict[str, object]:
    if not value:
        return {}
    return {str(key): item for key, item in value.items()}


def _enum_value(value: object) -> str:
    raw_value = getattr(value, "value", value)
    return raw_value if isinstance(raw_value, str) else str(raw_value)


def _optional_enum_value(value: object | None) -> str | None:
    if value is None:
        return None
    return _enum_value(value)


def _task_result_output(task: Task | Any) -> dict[str, object]:
    if not task.result:
        return {}
    raw_output: object = task.result.output
    if isinstance(raw_output, Mapping):
        return _stringify_mapping(cast(Mapping[Any, Any], raw_output))
    return {}


def _coerce_iso_datetime(value: object) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(value)


def _coerce_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return _coerce_datetime(isoformat())
    return None


def _is_stale_incomplete_active_job(
    *,
    status: object,
    created_at: object,
    has_redis_artifact: bool,
    has_canonical_artifacts: bool,
    expected_task_count: int | None,
    materialization_status: object = None,
    last_activity_at: object = None,
) -> bool:
    normalized_status = _enum_value(status).lower()
    if normalized_status not in ACTIVE_LIVE_JOB_STATUSES:
        return False
    if _enum_value(materialization_status).lower() in ACTIVE_MATERIALIZATION_STATUSES:
        return False
    if has_canonical_artifacts:
        return False
    if expected_task_count is not None and expected_task_count <= 0:
        return False

    created_at_dt = _coerce_datetime(created_at)
    if created_at_dt is None:
        return False
    last_activity_dt = _coerce_datetime(last_activity_at)
    if last_activity_dt is not None:
        created_at_dt = max(created_at_dt, last_activity_dt)

    return datetime.now(timezone.utc) - created_at_dt >= GHOST_JOB_GRACE_PERIOD


def _build_task_stats_model(raw_stats: Mapping[str, object] | None) -> TaskStats:
    stats = dict(raw_stats or {})
    type_counts = stats.get("task_type_counts")
    normalized_type_counts = (
        {
            str(key): int(value)
            for key, value in cast(dict[str, object], type_counts).items()
        }
        if isinstance(type_counts, Mapping)
        else {}
    )
    return TaskStats(
        total=_extract_int(stats.get("total")) or 0,
        ready=_extract_int(stats.get("ready")) or 0,
        blocked=_extract_int(stats.get("blocked")) or 0,
        inflight=_extract_int(stats.get("inflight")) or 0,
        deferred=_extract_int(stats.get("deferred")) or 0,
        completed=_extract_int(stats.get("completed")) or 0,
        failed=_extract_int(stats.get("failed")) or 0,
        task_type_counts=normalized_type_counts,
    )


def _normalize_error_group(error: object) -> str | None:
    if error is None:
        return None
    text = str(error).strip()
    if not text:
        return None
    normalized = " ".join(text.split())
    return normalized[:120]


def _build_error_stats_from_task_details(tasks: Sequence[TaskDetail]) -> JobErrorStats:
    groups: dict[str, int] = {}
    total = 0
    for task in tasks:
        error_group = _normalize_error_group(task.error)
        if error_group is None:
            continue
        groups[error_group] = groups.get(error_group, 0) + 1
        total += 1
    return JobErrorStats(total=total, groups=groups)


def _job_to_response(job: Job) -> JobResponse:
    task_stats = TaskStats(
        total=job.total_tasks,
        completed=job.completed_tasks,
        failed=job.failed_tasks,
    )
    return JobResponse(
        job_id=job.job_id,
        execution_engine=job.metadata.get("execution_engine"),
        job_name=job.name,
        status=JobStatus.DELETING if job.metadata.get("deletion_pending") else cast(JobStatus, job.state),
        deletion_progress=job.metadata.get("deletion_progress", {}) if job.metadata.get("deletion_pending") else None,
        priority=_extract_int(job.metadata.get("priority")) or 0,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        task_count=job.total_tasks,
        completed_tasks=job.completed_tasks,
        failed_tasks=job.failed_tasks,
        tasks=[],
        task_stats=task_stats,
        error_stats=JobErrorStats(total=job.failed_tasks, groups={}),
        created_by=job.created_by,
        is_scheduled=job.is_scheduled,
        scheduled_for=job.schedule.scheduled_for if job.schedule else None,
        interval_seconds=job.schedule.interval_seconds if job.schedule else None,
        cron=job.schedule.cron if job.schedule else None,
        run_count=_extract_int(job.metadata.get("run_count")) or 0,
        last_ran_at=(
            str(job.metadata.get("last_ran_at"))
            if job.metadata.get("last_ran_at") is not None
            else None
        ),
        last_activity_at=job.updated_at
        or job.completed_at
        or job.started_at
        or job.created_at,
        materialization_status=(
            str(job.metadata.get("materialization_status"))
            if job.metadata.get("materialization_status") is not None
            else None
        ),
        materialized_tasks=_extract_int(job.metadata.get("materialized_tasks")),
        materialization_total=_extract_int(job.metadata.get("materialization_total")),
        materialization_error=(
            str(job.metadata.get("materialization_error"))
            if job.metadata.get("materialization_error") is not None
            else None
        ),
    )


def _central_task_row_to_detail(
    row: "CentralTaskModel",
    definition_json: str | None = None,
) -> TaskDetail:
    items_total = row.items_total
    items_processed = row.items_processed
    progress = 0.0
    if items_total and items_total > 0:
        progress = min(items_processed / items_total, 1.0)

    ui_task_type = _ui_task_type(row.task_type, content_type=row.content_type)

    row_dict = getattr(row, "__dict__", None)

    archived_result: dict[str, Any] | None = None
    raw_result_json = None
    if isinstance(row_dict, dict) and "result_json" in row_dict:
        raw_result_json = row_dict.get("result_json")
    else:
        try:
            raw_result_json = getattr(row, "result_json", None)
        except Exception:
            raw_result_json = None

    if isinstance(raw_result_json, str) and raw_result_json:
        try:
            parsed_result = json.loads(raw_result_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed_result = None
        if isinstance(parsed_result, dict):
            archived_result = cast(dict[str, Any], parsed_result)

    # Fall back to the lightweight `message` column when the full result
    # payload is unavailable (older archived rows) so the Pipeline tab still
    # shows the per-task summary line after Redis cleanup.
    archived_message = None
    if isinstance(row_dict, dict) and "message" in row_dict:
        archived_message = row_dict.get("message")
    else:
        try:
            archived_message = getattr(row, "message", None)
        except Exception:
            archived_message = None

    if isinstance(archived_message, str) and archived_message:
        if archived_result is None:
            archived_result = {"message": archived_message}
        elif not archived_result.get("message"):
            archived_result["message"] = archived_message

    archived_payload: dict[str, Any] | None = None
    raw_definition_json = definition_json
    if raw_definition_json is None:
        if isinstance(row, dict):
            raw_definition_json = row.get("definition_json")
        elif isinstance(row_dict, dict) and "definition_json" in row_dict:
            raw_definition_json = row_dict.get("definition_json")
        else:
            try:
                raw_definition_json = getattr(row, "definition_json", None)
            except Exception:
                raw_definition_json = None

    if isinstance(raw_definition_json, str) and raw_definition_json:
        try:
            parsed_def = json.loads(raw_definition_json)
            if isinstance(parsed_def, dict):
                archived_payload = parsed_def.get("payload")
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    proxy_url = None
    proxy_routes: list[str | None] | None = None
    proxy_hop_used: int | None = None
    if archived_result:
        metrics = archived_result.get("metrics") or {}
        output = archived_result.get("output") or {}
        if isinstance(metrics, dict) and "proxy_url" in metrics:
            proxy_url = metrics.get("proxy_url")
        elif isinstance(output, dict) and "proxy_url" in output:
            proxy_url = output.get("proxy_url")
        elif isinstance(output, dict) and "resolved_route_url" in output:
            proxy_url = output.get("resolved_route_url")
        elif isinstance(metrics, dict) and "resolved_route_url" in metrics:
            proxy_url = metrics.get("resolved_route_url")
        if isinstance(output, dict) and "proxy_routes" in output:
            proxy_routes = output.get("proxy_routes")
        elif isinstance(metrics, dict) and "proxy_routes" in metrics:
            proxy_routes = metrics.get("proxy_routes")
        if isinstance(metrics, dict) and "proxy_hop_used" in metrics:
            proxy_hop_used = metrics.get("proxy_hop_used")
        elif isinstance(output, dict) and "proxy_hop_used" in output:
            proxy_hop_used = output.get("proxy_hop_used")
        elif isinstance(output, dict) and "resolved_route_index" in output:
            proxy_hop_used = output.get("resolved_route_index")
        elif isinstance(metrics, dict) and "resolved_route_index" in metrics:
            proxy_hop_used = metrics.get("resolved_route_index")

    if not proxy_routes and archived_payload and isinstance(archived_payload, dict):
        # Only a guest task's routes are in its payload; a signed-in one uses its
        # account's proxy, chosen when a worker claims it, so none is known yet.
        if archived_payload.get("use_guest_auth") and (
            "guest_proxy_url" in archived_payload
            or "guest_fallback_proxy_urls" in archived_payload
        ):
            from ...job_system.handlers.scrape.handler import _build_guest_proxy_routes

            g_proxy = archived_payload.get("guest_proxy_url")
            g_fallbacks = archived_payload.get("guest_fallback_proxy_urls") or []
            g_direct_idx = archived_payload.get("guest_direct_route_index")
            proxy_routes = _build_guest_proxy_routes(
                guest_proxy_url=str(g_proxy) if g_proxy else None,
                guest_fallback_proxy_urls=[str(f) for f in g_fallbacks if f],
                guest_direct_route_index=(
                    int(g_direct_idx) if g_direct_idx is not None else None
                ),
            )
        if not proxy_url and proxy_hop_used is None:
            proxy_url = archived_payload.get("guest_proxy_url") or archived_payload.get(
                "proxy_url"
            )

    if proxy_hop_used is not None and proxy_routes and 0 <= int(proxy_hop_used) < len(proxy_routes):
        proxy_url = proxy_routes[int(proxy_hop_used)]
    elif proxy_routes and proxy_url is not None and proxy_hop_used is None:
        try:
            proxy_hop_used = proxy_routes.index(proxy_url)
        except ValueError:
            proxy_hop_used = None

    archived_worker_id = None
    if isinstance(row_dict, dict) and "worker_id" in row_dict:
        archived_worker_id = row_dict.get("worker_id")
    else:
        try:
            archived_worker_id = getattr(row, "worker_id", None)
        except Exception:
            archived_worker_id = None

    return TaskDetail(
        task_id=row.task_id,
        task_type=ui_task_type,
        status=row.status,
        site=row.site,
        username=row.username,
        content_type=row.content_type,
        payload=archived_payload,
        worker_id=archived_worker_id,
        started_at=row.started_at.isoformat() if row.started_at else None,
        completed_at=row.completed_at.isoformat() if row.completed_at else None,
        progress=progress,
        items_processed=items_processed,
        items_total=items_total,
        error=row.error,
        result=archived_result,
        proxy_url=str(proxy_url) if proxy_url is not None else None,
        proxy_routes=proxy_routes,
        proxy_hop_used=int(proxy_hop_used) if proxy_hop_used is not None else None,
    )


def _durable_task_stats(header: dict[str, Any]) -> TaskStats:
    if header.get("execution_stats"):
        return TaskStats(**header["execution_stats"])
    return TaskStats(total=header["total"], completed=header["completed"], failed=header["failed"],
        task_type_counts=header["config"].get("metadata", {}).get("mps_task_type_counts") or {})


def _task_download_results(task: TaskDetail) -> list[dict[str, Any]]:
    result = task.result if isinstance(task.result, dict) else None
    output = result.get("output") if result else None
    if not isinstance(output, dict):
        return []
    download_results = output.get("download_results")
    if not isinstance(download_results, list):
        return []
    return [item for item in download_results if isinstance(item, dict)]


def _resolve_task_download_file(
    task: TaskDetail,
    result_index: int,
    config: UltimaScraperCollectionConfig,
) -> Path:
    """Resolve one task-owned result path beneath a configured download root."""

    results = _task_download_results(task)
    if result_index >= len(results):
        raise HTTPException(status_code=404, detail="Download result not found")

    raw_path = results[result_index].get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise HTTPException(status_code=404, detail="Downloaded file not found")

    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        raise HTTPException(status_code=404, detail="Downloaded file not found")

    try:
        resolved_candidate = candidate.resolve(strict=True)
    except (OSError, RuntimeError):
        raise HTTPException(
            status_code=404, detail="Downloaded file not found"
        ) from None

    if not resolved_candidate.is_file():
        raise HTTPException(status_code=404, detail="Downloaded file not found")

    configured_roots: list[Path] = []
    download_setup = getattr(getattr(config, "settings", None), "download_setup", None)
    for drive in getattr(download_setup, "drives", []):
        for directory in getattr(drive, "directories", []):
            try:
                configured_roots.append(Path(directory).expanduser().resolve())
            except (OSError, RuntimeError, TypeError):
                continue

    if not any(resolved_candidate.is_relative_to(root) for root in configured_roots):
        raise HTTPException(status_code=404, detail="Downloaded file not found")

    return resolved_candidate


def _normalize_task_type_filter(task_type: str | None) -> str | None:
    if task_type is None:
        return None
    normalized = task_type.strip().lower()
    if not normalized:
        return None
    alias_map = {
        "scrape_user": "scrape",
        "download_media": "download",
        "refresh_subscriptions": "api_command",
    }
    return alias_map.get(normalized, normalized)


def _ui_task_type(
    task_type: str,
    *,
    content_type: str | None = None,
    is_paid_content_feed: bool = False,
) -> str:
    if task_type == "scrape":
        return "scrape_paid_content" if is_paid_content_feed else "scrape_user"
    if task_type == "download":
        return "download_media"
    if task_type == "api_command" and content_type == "subscriptions":
        return "refresh_subscriptions"
    return task_type


def _empty_task_page(*, offset: int, limit: int) -> TaskPageResponse:
    return TaskPageResponse(
        tasks=[],
        next_cursor=None,
        has_more=False,
        scanned_count=offset,
        offset=offset,
        limit=limit,
    )


def _task_detail_matches_filters(
    task: TaskDetail,
    *,
    state: str | None,
    task_type: str | None,
    error_only: bool,
) -> bool:
    normalized_state = state.strip().lower() if state else None
    normalized_task_type = _normalize_task_type_filter(task_type)

    if normalized_state and task.status.strip().lower() != normalized_state:
        return False
    if (
        normalized_task_type
        and _normalize_task_type_filter(task.task_type) != normalized_task_type
    ):
        return False
    if error_only and not (
        task.error or task.status in {TaskState.FAILED.value, TaskState.CANCELLED.value}
    ):
        return False
    return True


def _build_task_page_response(
    tasks: Sequence[TaskDetail],
    *,
    offset: int,
    limit: int,
) -> TaskPageResponse:
    page_tasks = list(tasks[offset : offset + limit + 1])
    has_more = len(page_tasks) > limit
    visible_tasks = page_tasks[:limit]
    return TaskPageResponse(
        tasks=visible_tasks,
        next_cursor=str(offset + len(visible_tasks)) if has_more else None,
        has_more=has_more,
        scanned_count=offset + len(visible_tasks),
        offset=offset,
        limit=limit,
    )


def _merge_event_task_metadata(
    task_state: dict[str, Any],
    event_data: Mapping[str, Any],
) -> None:
    task_type = event_data.get("task_type")
    if task_type:
        task_state["task_type"] = str(task_type)

    site = event_data.get("site") or event_data.get("site_name")
    if site:
        task_state["site"] = str(site)

    username = event_data.get("username") or event_data.get("user_username")
    if username:
        task_state["username"] = str(username)

    content_type = _optional_enum_value(event_data.get("content_type"))
    if content_type:
        task_state["content_type"] = content_type

    auth_id = _extract_int(event_data.get("auth_id"))
    if auth_id is None:
        auth_id = _extract_int(event_data.get("account_id"))
    if auth_id is not None:
        task_state["auth_id"] = auth_id


def _event_task_sort_key(item: tuple[str, Mapping[str, Any]]) -> tuple[datetime, str]:
    task_id, task_state = item
    sort_at = (
        _coerce_datetime(task_state.get("first_event_at"))
        or _coerce_datetime(task_state.get("claimed_at"))
        or _coerce_datetime(task_state.get("started_at"))
        or _coerce_datetime(task_state.get("completed_at"))
        or datetime.max.replace(tzinfo=timezone.utc)
    )
    return sort_at, task_id


def _event_task_state_to_detail(
    task_id: str,
    task_state: Mapping[str, Any],
) -> TaskDetail:
    status = str(task_state.get("status") or TaskState.PENDING.value)
    content_type = _optional_enum_value(task_state.get("content_type"))
    raw_task_type = str(task_state.get("task_type") or "unknown")

    return TaskDetail(
        task_id=task_id,
        task_type=_ui_task_type(raw_task_type, content_type=content_type),
        status=status,
        site=(str(task_state["site"]) if task_state.get("site") else None),
        username=(str(task_state["username"]) if task_state.get("username") else None),
        content_type=content_type,
        payload=None,
        worker_id=(
            str(task_state["worker_id"]) if task_state.get("worker_id") else None
        ),
        started_at=_coerce_iso_datetime(task_state.get("started_at")),
        completed_at=_coerce_iso_datetime(task_state.get("completed_at")),
        progress=1.0 if status == TaskState.COMPLETED.value else 0.0,
        items_processed=0,
        items_total=None,
        error=(str(task_state["error"]) if task_state.get("error") else None),
        result=None,
    )
