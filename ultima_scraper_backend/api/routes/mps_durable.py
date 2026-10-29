"""MPS submission and retry HTTP presentation over the shared durable engine."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from typing import Any
from fastapi import HTTPException
from ...database.write_queue import DatabaseWriteClient, DatabaseWriteOperation, stable_command_id
from ...job_system import Job, JobStorage, TaskState, durable

async def _prepare_retry_job(plan: dict[str, Any], *, request: Any, backend: Any) -> dict[str, Any]:
    """Clone under the source edit lease using SQL history and its live window."""

    storage = JobStorage(backend.redis)
    original = await storage.fetch_job(plan["job_id"], include_tasks=False)
    if original is None:
        original = Job.from_dict(plan["job_config"])
    live_failed = []
    live_tasks = []
    async for task in storage.fetch_tasks_stream(plan["job_id"]):
        if task.state == TaskState.FAILED:
            live_tasks.append({"task_id": task.task_id, "user_id": task.user_id})
            if not (plan["selection"].get("task_manifest_id") or plan["selection"].get("spawned_manifest_id")):
                live_failed.append(int(task.user_id))
    name = original.name
    while name.startswith("Retry: "):
        name = name.removeprefix("Retry: ").lstrip()
    retry = Job.create(
        name=f"Retry: {name}",
        created_by=original.created_by or "webui",
        tasks=[],
        metadata={
            "source": "manual",
            "site": plan["site"],
            "mps_manifest": True,
            "retry_of": plan["job_id"],
            "original_job_name": original.name,
            "retry_auth_id": request.auth_id,
            "include_direct_connection": bool(
                plan.get("selection", {}).get(
                    "include_direct_connection",
                    original.metadata.get("include_direct_connection", False),
                )
            ),
            **({
                "durable_job_kind": "ordinary",
                "dispatch_backend_id": None if backend.distributed_mode else backend.instance_id,
            } if plan["job_config"].get("metadata", {}).get("durable_job_kind") == "ordinary" else {}),
        },
    )
    from ...database.write_queue import DatabaseWriteError

    try:
        prepared = await DatabaseWriteClient(
            backend.redis, producer_id="mps-retry"
        ).submit(
            DatabaseWriteOperation.RETRY_MPS_JOB,
            {
                "source_job_id": plan["job_id"],
                "job_id": retry.job_id,
                "job_config": retry.to_dict(),
                "live_failed_user_ids": live_failed,
                "live_failed_tasks": live_tasks,
                "expected_manifest_id": plan["selection"].get("task_manifest_id"),
                "auth_id": request.auth_id,
            },
            site=plan["site"],
            command_id=stable_command_id("mps-retry-v1", retry.job_id),
        )
    except DatabaseWriteError as exc:
        raise HTTPException(
            status_code=400, detail=f"Could not prepare MPS retry: {exc}"
        ) from exc
    return prepared


async def _publish_retry_job(prepared: dict[str, Any], *, backend: Any) -> Any:
    """Propagate cancellation and expose the retry after releasing the source lease."""
    from . import jobs

    storage = JobStorage(backend.redis)
    retry = Job.from_dict(prepared["job_config"])
    job = await durable.publish(storage, prepared)
    if job is None:
        raise HTTPException(status_code=409, detail="MPS retry was deleted before publication")
    await jobs.publish_ws_event(
        backend.redis,
        {
            "type": "webui_ack",
            "event": "job_retry",
            "job_id": retry.job_id,
            "job_name": retry.name,
            "status": job.state.value,
            "task_count": prepared["total"],
            "retry_of": retry.metadata["retry_of"],
            "retry_auth_id": retry.metadata.get("retry_auth_id"),
        },
    )
    response = jobs._job_to_response(job)
    response.task_count = prepared["total"]
    return response


async def retry_job(plan: dict[str, Any], *, request: Any, backend: Any) -> Any:
    storage = JobStorage(backend.redis)
    try:
        async with durable._schedule_edit_lock(storage, plan["job_id"]):
            prepared = await _prepare_retry_job(plan, request=request, backend=backend)
    except RuntimeError as exc:
        if "is materializing" in str(exc):
            raise HTTPException(status_code=409, detail="MPS job is changing; retry the request") from exc
        raise
    # Retirement uses the normal definition guards, which wait for edits. Do
    # not make it wait on this request's own source edit lease.
    return await _publish_retry_job(prepared, backend=backend)


async def create_job(
    request: Any, backend: Any, backend_manager: Any, config: Any
) -> Any:
    from . import jobs, mps

    scope_started = time.monotonic()
    scope = await mps._resolve_analysis_scope(request, backend_manager)
    logging.getLogger(__name__).info("MPS create scope_ms=%.1f", (time.monotonic() - scope_started) * 1000)
    proxies, proxy_capacity = await asyncio.to_thread(
        lambda: mps._resolve_mps_proxies(
            request=request, config=config, backend_manager=backend_manager
        )
    )
    default_job_name = (
        f"MPS {scope.site_display} "
        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}"
    )
    job = Job.create(
        name=(
            request.job_name.strip()
            if request.job_name and request.job_name.strip()
            else default_job_name
        ),
        created_by=request.created_by or "webui_mps",
        tasks=[],
        metadata={
            "source": "manual",
            "site": scope.site_key,
            "mps_manifest": True,
            "job_resource_scope": "mps",
            "proxy_capacity": proxy_capacity,
            "include_direct_connection": request.include_direct_connection,
        },
    )
    selection = request.model_dump(
        exclude={
            "proxy_urls", "proxy_mode", "use_proxies", "job_name", "created_by",
            "candidate_count_hint",
        }
    )
    selection.update(start_id=scope.start_id, end_id=scope.end_id)
    candidate_hint = request.candidate_count_hint
    if candidate_hint is None:
        cached_users = mps._USER_COUNT_CACHE.get(("users", scope.start_id, scope.end_id))
        if cached_users is not None and not request.ignore_missing_ids:
            candidate_hint = max(0, scope.end_id - scope.start_id + 1 - cached_users[1])
    prepare_started = time.monotonic()
    plan = await DatabaseWriteClient(
        backend.redis, producer_id="mps-submission"
    ).submit(
        DatabaseWriteOperation.PREPARE_MPS_JOB,
        {
            "job_id": job.job_id,
            "job_config": job.to_dict(),
            "selection": selection,
            "proxy_pool": proxies,
            "candidate_count_hint": candidate_hint,
        },
        site=scope.site_key,
        command_id=stable_command_id("mps-manifest-v1", job.job_id),
    )
    logging.getLogger(__name__).info("MPS create job=%s writer_prepare_wait_ms=%.1f", job.job_id, (time.monotonic() - prepare_started) * 1000)
    if plan.get("empty"):
        raise HTTPException(
            status_code=400,
            detail="No profiles selected for the requested candidate offset and filters.",
        )
    storage = JobStorage(backend.redis)
    persisted_job = await durable.publish(storage, plan)
    if persisted_job is None:
        raise HTTPException(status_code=409, detail="MPS job was deleted before publication")
    summary = plan["summary"]
    batch_plan = mps._build_batch_plan(
        candidate_count=summary["candidate_count"],
        candidate_offset=request.candidate_offset,
        requested_batch_size=request.max_profiles,
        range_size=scope.end_id - scope.start_id + 1,
        invalid_table_ready=True,
        include_invalid=request.include_invalid,
        invalid_last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
        include_existing_users=request.include_existing_users,
        existing_last_checked_older_than_hours=request.existing_last_checked_older_than_hours,
        missing_count=summary.get("missing_count", 0),
        include_missing=not request.ignore_missing_ids,
        routes=max(1, len(set(proxies)) + int((not bool(proxies)) or request.include_direct_connection)),
    )
    await jobs.publish_ws_event(
        backend.redis,
        {
            "type": "webui_ack",
            "event": "job_created",
            "job_id": job.job_id,
            "job_name": job.name,
            "status": persisted_job.state.value,
            "task_count": plan["total"],
            "materialization_status": "queued",
        },
    )
    return mps.MPSCreateJobResponse(
        job=jobs._job_to_response(persisted_job),
        candidate_offset=request.candidate_offset,
        selected_profiles=plan["total"],
        total_missing_in_range=summary["missing_count"],
        total_invalid_eligible_in_range=summary["invalid_eligible_count"],
        total_existing_eligible_in_range=summary["existing_eligible_count"],
        total_candidates_in_range=summary["candidate_count"],
        remaining_candidate_count=batch_plan.remaining_candidate_count,
        next_candidate_offset=batch_plan.next_candidate_offset,
        has_next_batch=batch_plan.has_next_batch,
        recommended_batch_size=batch_plan.recommended_batch_size,
        adaptive_batch_size=batch_plan.adaptive_batch_size,
        next_batch_size=batch_plan.requested_batch_size
        if batch_plan.has_next_batch
        else None,
        batch_recommendations=batch_plan.batch_recommendations,
        warnings=batch_plan.warnings,
        proxies_configured=len(getattr(config.settings.network, "proxies", []) or []),
        proxies_used=len(proxies),
        direct_connection_included=(not bool(proxies)) or request.include_direct_connection,
        proxy_mode=request.proxy_mode,
        proxy_urls_preview=proxies[:200],
    )
