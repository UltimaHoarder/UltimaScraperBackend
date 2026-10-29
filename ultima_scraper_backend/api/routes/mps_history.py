"""Bounded drilldown across durable definitions, archived results and live windows."""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException

from ...database import job_history
from ...database.mps_manifest import MPSManifestStore, mps_task_id
from ...database.write_queue import _positive_int_env
from ...job_system import JobStorage, Task
from ...job_system.storage import TaskLoadLimitExceeded
from ...database.threads import run_database_call


def record_detail(record):
    from .job_views import _central_task_row_to_detail
    from .jobs import _task_to_detail

    detail = (
        _task_to_detail(Task.from_dict(json.loads(record.definition)))
        if record.definition
        else None
    )
    if record.archive is not None:
        archived = _central_task_row_to_detail(
            record.archive,
            definition_json=record.definition if record.definition else None,
        )
        if detail is None:
            detail = archived
        else:
            archived_updates = archived.model_dump(exclude={"payload"})
            if archived.proxy_routes is not None:
                archived_updates["proxy_routes"] = archived.proxy_routes
            elif detail.proxy_routes is not None:
                archived_updates["proxy_routes"] = detail.proxy_routes

            if archived.proxy_hop_used is not None:
                archived_updates["proxy_hop_used"] = archived.proxy_hop_used
                if archived.proxy_routes and 0 <= int(archived.proxy_hop_used) < len(archived.proxy_routes):
                    archived_updates["proxy_url"] = archived.proxy_routes[int(archived.proxy_hop_used)]
                else:
                    archived_updates["proxy_url"] = archived.proxy_url
            elif detail.proxy_hop_used is not None:
                archived_updates["proxy_hop_used"] = detail.proxy_hop_used
                archived_updates["proxy_url"] = detail.proxy_url
            elif archived.proxy_url is not None:
                archived_updates["proxy_url"] = archived.proxy_url
            elif detail.proxy_url is not None:
                archived_updates["proxy_url"] = detail.proxy_url

            detail = detail.model_copy(update=archived_updates)
    return detail



async def _assert_definition(store, plan):
    current = await run_database_call(store.get, plan["job_id"])
    if current is None:
        raise HTTPException(404, f"Job {plan['job_id']} not found")
    if current["selection"].get("task_manifest_id") != plan["selection"].get(
        "task_manifest_id"
    ):
        raise HTTPException(
            409, "The job definition changed while reading history; reload the page"
        )


async def task_detail(storage: JobStorage | None, plan, task_id: str):
    from .jobs import _task_to_detail

    from ...job_system.ledger import ExecutionLedger, owns
    if owns(plan["job_config"]):
        payload = await run_database_call(ExecutionLedger().task, plan["job_id"], task_id)
        return _task_to_detail(Task.from_dict(payload)) if payload is not None else None

    store = MPSManifestStore()
    budget = job_history.history_byte_limit()
    try:
        raw, _ = (
            await storage._read_task_prefix_by_ids(
                plan["job_id"], [task_id], max_bytes=budget
            )
            if storage
            else ([None], 0)
        )
        if raw[0]:
            task = Task.from_dict(json.loads(raw[0]))
            if (task.mps_task_manifest or "") != (
                plan["selection"].get("task_manifest_id") or ""
            ):
                raise HTTPException(
                    409,
                    "The job definition changed while reading history; reload the page",
                )
            detail = _task_to_detail(task)
        else:
            rows = await run_database_call(
                job_history.task_records,
                store.session_factory,
                plan["job_id"],
                [task_id],
                definition_id=plan["selection"].get("task_manifest_id")
                or plan["selection"].get("spawned_manifest_id"),
                byte_limit=budget,
            )
            detail = record_detail(rows[0]) if rows else None
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    await _assert_definition(store, plan)
    return detail


async def task_page(
    storage: JobStorage | None,
    job_id: str,
    *,
    offset: int,
    limit: int,
    state: str | None = None,
    task_type: str | None = None,
    error_only: bool = False,
    plan: dict[str, Any] | None = None,
) -> Any:
    from .job_schemas import TaskPageResponse
    from .job_views import _normalize_task_type_filter
    from .jobs import _task_to_detail

    store = MPSManifestStore()
    if plan is None:
        plan = await run_database_call(store.get, job_id)
    if plan is None:
        raise HTTPException(404, f"Job {job_id} not found")
    materialized = (
        await storage._client().hget(
            storage.JOB_STATE_KEY.format(job_id=job_id), "materialized_tasks"
        )
        if storage
        else None
    )
    manifest_id = plan["selection"].get("task_manifest_id")
    definition_id = manifest_id or plan["selection"].get("spawned_manifest_id")
    candidate_total = int(plan["selection"].get("candidate_total", plan["total"]))
    prefix_visible = (
        0
        if manifest_id
        else min(candidate_total, max(
            plan["cursor"], int(materialized or 0),
            plan["selection"].get("candidate_checkpoint", {}).get("next", 0),
        ))
    )
    definition_count = plan["total"] if manifest_id else plan["total"] - candidate_total
    visible = prefix_visible + definition_count
    normalized_state = state.strip().lower() if state else None
    normalized_type = _normalize_task_type_filter(task_type)
    if offset >= visible or (
        not definition_id and normalized_type not in (None, "scrape")
    ):
        return TaskPageResponse(
            tasks=[], offset=offset, limit=limit, scanned_count=visible
        )

    has_filters = bool(normalized_state or normalized_type or error_only)
    resource = (
        "ULTIMA_JOB"
        if plan["job_config"].get("metadata", {}).get("durable_job_kind") == "ordinary"
        else "ULTIMA_MPS"
    )
    scan_budget = (
        _positive_int_env(
            f"{resource}_TASK_PAGE_SCAN_MAX_TASKS",
            limit * JobStorage.TASK_PAGE_SCAN_MULTIPLIER,
        )
        if has_filters
        else limit
    )
    count = min(scan_budget, visible - offset)
    reverse = error_only or normalized_state in {
        "running",
        "completed",
        "failed",
        "cancelled",
    }
    task_ids = []
    if reverse:
        definition_rows = min(count, max(0, definition_count - offset))
        if definition_id and definition_rows:
            task_ids = await run_database_call(
                job_history.definition_ids,
                store.session_factory,
                definition_id,
                offset=definition_count - offset - 1,
                limit=definition_rows,
                reverse=True,
            )
        prefix_count = count - definition_rows
        if prefix_count:
            end = prefix_visible - max(0, offset - definition_count)
            first = end - prefix_count
            ids = await run_database_call(
                store.candidate_page, job_id, offset=first, limit=prefix_count
            )
            task_ids.extend(
                reversed(
                    [
                        mps_task_id(job_id, first + i, user_id)
                        for i, user_id in enumerate(ids)
                    ]
                )
            )
    else:
        prefix_count = max(0, min(count, prefix_visible - offset))
        if prefix_count:
            ids = await run_database_call(
                store.candidate_page, job_id, offset=offset, limit=prefix_count
            )
            task_ids.extend(
                mps_task_id(job_id, offset + i, user_id)
                for i, user_id in enumerate(ids)
            )
        if definition_id and count > prefix_count:
            task_ids.extend(
                await run_database_call(
                    job_history.definition_ids,
                    store.session_factory,
                    definition_id,
                    offset=max(0, offset - prefix_visible),
                    limit=count - prefix_count,
                )
            )

    budget = job_history.history_byte_limit()
    try:
        raw_tasks, _ = (
            await storage._read_task_prefix_by_ids(job_id, task_ids, max_bytes=budget)
            if storage
            else ([None] * len(task_ids), 0)
        )
        task_ids = task_ids[: len(raw_tasks)]
        missing = [
            task_id for task_id, raw in zip(task_ids, raw_tasks, strict=True) if not raw
        ]
        records = await run_database_call(
            job_history.task_records,
            store.session_factory,
            job_id,
            missing,
            definition_id=definition_id,
            byte_limit=budget,
        )
    except TaskLoadLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    by_id = {record.task_id: record for record in records}
    matched = []
    consumed = occupied = 0
    for task_id, raw in zip(task_ids, raw_tasks, strict=True):
        if raw:
            size = len(raw.encode() if isinstance(raw, str) else raw)
            task = Task.from_dict(json.loads(raw))
            if (task.mps_task_manifest or "") != (manifest_id or ""):
                raise HTTPException(
                    409,
                    "The job definition changed while reading history; reload the page",
                )
            detail = _task_to_detail(task)
        else:
            record = by_id.get(task_id)
            if record is None:
                break  # SQL reached its byte allowance before this identifier.
            size, detail = record.read_bytes, record_detail(record)
        if occupied + size > budget:
            break
        occupied += size
        consumed += 1
        if detail is None:
            continue
        if (
            normalized_type
            and _normalize_task_type_filter(detail.task_type) != normalized_type
        ):
            continue
        if normalized_state and detail.status.lower() != normalized_state:
            continue
        if error_only and not (
            detail.error or detail.status in {"failed", "cancelled"}
        ):
            continue
        matched.append(detail)
        if len(matched) == limit:
            break
    await _assert_definition(store, plan)
    cursor = offset + consumed
    has_more = consumed > 0 and cursor < visible
    return TaskPageResponse(
        tasks=matched,
        next_cursor=str(cursor) if has_more else None,
        has_more=has_more,
        scanned_count=cursor,
        offset=offset,
        limit=limit,
    )
