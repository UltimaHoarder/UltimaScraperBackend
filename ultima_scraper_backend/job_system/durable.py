"""Shared durable plan publication, bounded execution windows and recovery."""
from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
from redis.exceptions import WatchError
from ..redis_channels import KEY_PREFIX
from ..database.mps_manifest import MPSManifestStore
from ..database.write_queue import (
    DatabaseWriteClient,
    DatabaseWriteOperation,
    stable_command_id,
    _positive_int_env,
)
from . import Job, JobManager, JobSchedule, JobStorage, JobState, Task, TaskState, mps_attempt, mps_state
from .payloads import ScrapeTaskPayload
from .resources import terminal_job_state, window_policy
from .task_sources import ready_tasks
from . import runtime_admission

logger = logging.getLogger(__name__)


async def _retire_retry_source(storage: JobStorage, plan: dict[str, Any]) -> None:
    """Replay the source cancellation before exposing a durable retry's work."""
    source_id = plan["job_config"].get("metadata", {}).get("retry_of")
    if not source_id or source_id == plan["job_id"]:
        return
    # The clone already cancelled its source in the same SQL transaction.
    # Recovery propagates that decision; it must not issue another cancellation
    # against a later definition that now uses the source's job ID.
    source_plan = await asyncio.to_thread(MPSManifestStore().get, source_id)
    if source_plan is None or source_plan["status"] != "cancelled":
        return
    identity = mps_attempt.Identity(
        source_id, "", source_plan["selection"].get("task_manifest_id") or ""
    )
    try:
        with mps_attempt.scope(identity):
            source = await storage.fetch_job(source_id, include_tasks=False)
            if source is None:
                return
            manager = JobManager(storage.redis)
            backend_id = await manager._resolve_backend_assignment(source_id)
            if source.state.value != "cancelled":
                await storage.mark_job_cancelled(
                    source_id, reason=f"superseded by retry job {plan['job_id']}"
                )
            await manager.router.clear_job_routing(job_id=source_id, backend_id=backend_id)
            if await storage._client().exists(storage.TASK_INDEX_KEY.format(job_id=source_id)):
                await manager.finalize_job_cancellation(source_id)
    except mps_attempt.StaleTaskDefinition:
        pass  # A replacement won after the durable cancellation was read.


def schedule_pending(job: Job) -> bool:
    return bool(job.metadata.get("mps_schedule_pending", job.is_scheduled))


async def _terminate_runtime_mps_job(
    redis: Any,
    storage: JobStorage,
    job: Job,
    manifest_id: str | None,
) -> None:
    """Close a durable SQL plan after Redis already recorded a terminal job."""

    await DatabaseWriteClient(
        redis, producer_id="mps-materializer"
    ).submit(
        DatabaseWriteOperation.TERMINATE_MPS_JOB,
        {
            "job_id": job.job_id,
            "status": job.state.value,
            "mps_task_manifest": manifest_id or "",
        },
        command_id=stable_command_id(
            "mps-terminate-v2", job.job_id, manifest_id, job.state.value
        ),
    )
    manager = JobManager(storage.redis)
    await manager.router.clear_job_routing(
        job_id=job.job_id,
        backend_id=await manager._resolve_backend_assignment(job.job_id),
        mps_task_manifest=manifest_id or "",
    )
    if await storage._client().exists(storage.TASK_INDEX_KEY.format(job_id=job.job_id)):
        await manager._finalize_terminal_job_cleanup(
            job.job_id,
            terminal_state=JobState(job.state.value),
        )


class JobPublicationBusy(RuntimeError):
    """Another publisher owns the job's renewable runtime lease."""


@asynccontextmanager
async def _schedule_edit_lock(storage: JobStorage, job_id: str):
    """Serialize a schedule/routing transition with window publication."""
    from . import materializer as mps

    client = storage._client()
    key = mps._mps_materialization_lock_key(job_id)
    owner = f"mps-edit:{uuid4().hex}"
    if not await client.set(
        key, owner, nx=True, ex=mps.MPS_MATERIALIZATION_LOCK_TTL_SECONDS
    ):
        raise JobPublicationBusy("MPS job is materializing; retry the schedule update")
    heartbeat = asyncio.create_task(
        mps._refresh_mps_materialization_lock(client, lock_key=key, owner=owner)
    )
    try:
        yield key, owner
    finally:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        await mps._release_mps_materialization_lock(client, lock_key=key, owner=owner)


async def update_job_config(
    manager: JobManager,
    job: Job,
    *,
    name: str | None = None,
    schedule: JobSchedule | None = None,
) -> Job:
    """Commit an edit before publishing its compact runtime configuration."""
    if name is not None and not name.strip():
        raise ValueError("Job name cannot be empty")

    async def persist(lock_key: str | None = None, lock_owner: str | None = None) -> Job:
        await manager.database_writes.submit(
            DatabaseWriteOperation.UPDATE_MPS_CONFIG,
            {
                "job_id": job.job_id,
                "name": name,
                "schedule": schedule.to_dict() if schedule is not None else None,
            },
        )
        # A later concurrent rename may have committed before this reply.
        plan = await asyncio.to_thread(MPSManifestStore().get, job.job_id)
        assert plan is not None
        try:
            updated = await ensure_job(manager.storage, plan, lock_key=lock_key, lock_owner=lock_owner)
        except runtime_admission.RuntimeAdmissionDeferred:
            return job_from_plan(plan)
        if updated.metadata.get("mps_config_pending"):
            updated.name = plan["job_config"].get("name", updated.name)
            updated.schedule = JobSchedule.from_dict(plan["job_config"].get("schedule"))
        return updated

    if schedule is None:
        return await persist() if name is not None else job
    async with _schedule_edit_lock(manager.storage, job.job_id) as (key, owner):
        updated = await persist(key, owner)
        await _check_owner(manager.storage._client(), key, owner)
        await manager.router.clear_job_routing(
            job_id=job.job_id,
            backend_id=await manager._resolve_backend_assignment(job.job_id),
        )
        if await manager.storage._client().exists(manager.storage.JOB_CONFIG_KEY.format(job_id=job.job_id)):
            await mps_state.schedule(manager.storage._client(), job.job_id)
        return updated


async def start_scheduled_job(manager: JobManager, job: Job) -> Job:
    """Release the due schedule without rebuilding or resetting its live batch."""
    async with _schedule_edit_lock(manager.storage, job.job_id) as (key, owner):
        await manager.database_writes.submit(
            DatabaseWriteOperation.UPDATE_MPS_CONFIG,
            {
                "job_id": job.job_id,
                "start_schedule_revision": int(job.metadata.get("mps_config_revision", 0)),
            },
        )
        plan = await asyncio.to_thread(MPSManifestStore().get, job.job_id)
        assert plan is not None
        try:
            updated = await ensure_job(manager.storage, plan, lock_key=key, lock_owner=owner)
        except runtime_admission.RuntimeAdmissionDeferred:
            return job_from_plan(plan)
        await mps_state.schedule(manager.storage._client(), job.job_id)
        return updated


async def cancel_waiting_job(manager: JobManager, job_id: str, *, reason: str) -> Job | None:
    """Cancel SQL-owned waiting work without spending a cache slot.

    Publication uses this same lease. If admission already won, return to the
    manager's normal runtime cancellation path after releasing the lease.
    """
    async with _schedule_edit_lock(manager.storage, job_id) as (key, owner):
        if await manager.storage._client().exists(manager.storage.JOB_CONFIG_KEY.format(job_id=job_id)):
            return None
        plan = await asyncio.to_thread(MPSManifestStore().get, job_id)
        if plan is None:
            raise ValueError(f"Job {job_id} not found")
        job = job_from_plan(plan)
        if job.state in {JobState.COMPLETED, JobState.CANCELLED}:
            raise RuntimeError(f"Job is already {job.state.value}")
        completed_at = datetime.now(timezone.utc).isoformat()
        result = await manager.database_writes.submit(
            DatabaseWriteOperation.SYNC_JOB_STATUS,
            {"job_id": job_id, "status": "cancelled", "completed_at": completed_at,
             "mps_task_manifest": plan["selection"].get("task_manifest_id") or ""},
            command_id=stable_command_id("cancel-waiting-job-v1", job_id, completed_at),
        )
        if result.get("stale"):
            raise mps_attempt.StaleTaskDefinition(f"Job {job_id} changed during cancellation")
        if not result.get("synced"):
            raise RuntimeError(f"Job {job_id} cancellation did not commit")
        await _check_owner(manager.storage._client(), key, owner)
        job.state = JobState.CANCELLED
        job.completed_at = job.updated_at = completed_at
        job.metadata["cancel_reason"] = reason
        return job


async def _sync_job_config(storage: JobStorage, plan: dict[str, Any]) -> Job | None:
    """Publish a newer SQL definition without overwriting live state/counters."""
    job_id = plan["job_id"]
    saved = Job.from_dict(plan["job_config"])
    revision = int(saved.metadata.get("mps_config_revision", 0))
    client = storage._client()
    key = storage.JOB_CONFIG_KEY.format(job_id=job_id)
    plan_key = mps_state.PLAN_KEY.format(job_id=job_id)
    async with client.pipeline(transaction=True) as pipe:
        while True:
            try:
                await pipe.watch(key, plan_key)
                raw = await pipe.get(key)
                if raw is None:
                    return None
                config = json.loads(raw)
                metadata = config.setdefault("metadata", {})
                if metadata.get("mps_task_manifest") != saved.metadata.get("mps_task_manifest"):
                    # A concurrent edit interrupted window retirement. Never
                    # promote its revision without replacing its task source.
                    break
                if int(metadata.get("mps_config_revision", 0)) >= revision:
                    break
                changed_schedule = (
                    config.get("schedule") != saved.schedule.to_dict()
                    or metadata.get("mps_schedule_pending") != schedule_pending(saved)
                )
                config["name"] = saved.name
                config["schedule"] = saved.schedule.to_dict()
                for field in ("source", "mps_config_revision", "mps_schedule_pending", "mps_task_type_counts", "total_tasks_count_override"):
                    if field in saved.metadata:
                        metadata[field] = saved.metadata[field]
                has_plan = await pipe.exists(plan_key)
                payload = runtime_admission.configuration_payload(job_id, config)
                try:
                    size = await runtime_admission.check(pipe, storage, job_id, payload)
                except runtime_admission.RuntimeAdmissionDeferred:
                    # Preserve current window progress while a larger cached
                    # configuration waits. Schedule guards must still take
                    # effect immediately after their SQL transaction committed.
                    pipe.multi()
                    pipe.hset(storage.JOB_STATE_KEY.format(job_id=job_id), mapping={
                        "mps_config_pending": revision,
                        "mps_schedule_pending": int(schedule_pending(saved)),
                        # Only the canonical timestamp is needed to hold/trigger
                        # claims. Other schedule fields remain in the SQL config;
                        # copying a large cron string here would bypass admission.
                        "mps_pending_scheduled_for": datetime.fromisoformat(saved.schedule.scheduled_for).isoformat() if saved.schedule.scheduled_for else "",
                    })
                    if schedule_pending(saved) and saved.schedule.scheduled_for:
                        pipe.zadd(storage.SCHEDULED_KEY, {job_id: datetime.fromisoformat(saved.schedule.scheduled_for).timestamp()})
                    else:
                        pipe.zrem(storage.SCHEDULED_KEY, job_id)
                    if changed_schedule and has_plan:
                        pipe.hset(plan_key, "activated", 0)
                    await pipe.execute()
                    break
                pipe.multi()
                runtime_admission.record(pipe, job_id, size)
                pipe.set(key, payload)
                pipe.hdel(storage.JOB_STATE_KEY.format(job_id=job_id), "mps_config_pending", "mps_pending_scheduled_for")
                pipe.hset(
                    storage.JOB_STATE_KEY.format(job_id=job_id),
                    mapping={"mps_schedule_pending": int(schedule_pending(saved)),
                             "total_tasks": plan["total"],
                             "materialization_total": plan["total"],
                             "mps_task_type_counts": json.dumps(metadata.get("mps_task_type_counts") or {"scrape": plan["total"]})},
                )
                if has_plan:
                    pipe.hset(plan_key, "total", plan["total"])
                if schedule_pending(saved) and saved.schedule.scheduled_for:
                    pipe.zadd(storage.SCHEDULED_KEY, {
                        job_id: datetime.fromisoformat(saved.schedule.scheduled_for).timestamp()
                    })
                else:
                    pipe.zrem(storage.SCHEDULED_KEY, job_id)
                if changed_schedule and has_plan:
                    pipe.hset(plan_key, "activated", 0)
                await pipe.execute()
                break
            except WatchError:
                continue
    return await storage.fetch_job(job_id, include_tasks=False)


async def ensure_job(
    storage: JobStorage, plan: dict[str, Any], *,
    lock_key: str | None = None, lock_owner: str | None = None,
) -> Job:
    """Restore compact runtime metadata without overwriting a running job."""
    from .replacement import reconcile_definition

    if lock_key is None:
        async with _schedule_edit_lock(storage, plan["job_id"]) as (key, owner):
            current = await asyncio.to_thread(MPSManifestStore().get, plan["job_id"])
            if current is None:
                raise mps_attempt.StaleTaskDefinition("MPS job was deleted before publication")
            return await ensure_job(storage, current, lock_key=key, lock_owner=owner)
    await _check_owner(storage._client(), lock_key, lock_owner)
    await reconcile_definition(storage, plan, lock_key=lock_key, lock_owner=lock_owner)
    job_id = plan["job_id"]
    existing = await _sync_job_config(storage, plan)
    if existing is not None:
        return existing
    job = job_from_plan(plan)
    if schedule_pending(job) and job.schedule.scheduled_for:
        due = datetime.fromisoformat(job.schedule.scheduled_for)
        if due.tzinfo is None:
            due = due.replace(tzinfo=timezone.utc)
        if due > datetime.now(timezone.utc):
            raise runtime_admission.RuntimeAdmissionDeferred(f"Job {job_id} is waiting for its scheduled time")
    config = job.to_config(include_task_ids=False).to_dict()
    state = {
        key: value
        for key, value in job.to_state().to_dict().items()
        if value is not None
    }
    state.update(
        mps_manifest=1,
        mps_task_manifest=plan["selection"].get("task_manifest_id", ""),
        mps_task_type_counts=json.dumps(job.metadata.get("mps_task_type_counts") or {"scrape": plan["total"]}),
        mps_schedule_pending=int(schedule_pending(job)),
        total_tasks=plan["total"],
        mps_archived_completed=plan["completed"],
        mps_archived_failed=plan["failed"],
        completed_tasks=plan["completed"],
        failed_tasks=plan["failed"],
        mps_window_end=plan["cursor"],
        materialization_status="queued",
    )
    client = storage._client()
    config_key = storage.JOB_CONFIG_KEY.format(job_id=job_id)
    payload = runtime_admission.configuration_payload(job_id, config)
    async with client.pipeline(transaction=True) as pipe:
        while True:
            try:
                await pipe.watch(config_key, lock_key)
                await _check_owner(pipe, lock_key, lock_owner)
                if await pipe.exists(config_key):
                    break
                size = await runtime_admission.check(pipe, storage, job_id, payload)
                pipe.multi()
                runtime_admission.record(pipe, job_id, size)
                pipe.set(config_key, payload)
                pipe.hset(storage.JOB_STATE_KEY.format(job_id=job_id), mapping=state)
                pipe.zadd(
                    storage.JOB_INDEX_KEY,
                    {job_id: datetime.fromisoformat(job.created_at).timestamp()},
                )
                if schedule_pending(job) and job.schedule.scheduled_for:
                    pipe.zadd(storage.SCHEDULED_KEY, {
                        job_id: datetime.fromisoformat(job.schedule.scheduled_for).timestamp()
                    })
                await pipe.execute()
                break
            except WatchError:
                continue
    result = await storage.fetch_job(job_id, include_tasks=False)
    assert result is not None
    return result


def job_from_plan(plan: dict[str, Any]) -> Job:
    """The same compact response is available before runtime admission."""
    job = Job.from_dict(plan["job_config"])
    terminal = {
        "complete": terminal_job_state(plan["job_config"], plan["completed"], plan["failed"]),
        "cancelled": "cancelled",
        "failed": "failed",
    }.get(plan["status"])
    if terminal:
        job.state = JobState(terminal)
        job.completed_at = job.completed_at or datetime.now().astimezone().isoformat()
    job.metadata.update(
        mps_manifest=True,
        total_tasks_count_override=plan["total"],
        completed_tasks_count_override=plan["completed"],
        failed_tasks_count_override=plan["failed"],
        materialization_status="queued",
        materialized_tasks=plan["cursor"],
        materialization_total=plan["total"],
    )
    if "include_direct_connection" not in job.metadata and "selection" in plan:
        job.metadata["include_direct_connection"] = bool(
            plan["selection"].get("include_direct_connection", False)
        )
    return job


async def publish(storage: JobStorage, plan: dict[str, Any]) -> Job | None:
    """Return committed work even when its optional runtime header must wait."""
    from .ledger import owns

    if owns(plan["job_config"]):
        return job_from_plan(plan)
    try:
        if not await ensure_plan(storage, plan):
            return None
    except runtime_admission.RuntimeAdmissionDeferred:
        return job_from_plan(plan)
    await mps_state.schedule(storage._client(), plan["job_id"])
    return await storage.fetch_job(plan["job_id"], include_tasks=False)


async def ensure_plan(
    storage: JobStorage, plan: dict[str, Any], *,
    lock_key: str | None = None, lock_owner: str | None = None,
) -> bool:
    """Publish only a current SQL plan, serialized with edits and deletion."""
    from .ledger import owns

    if owns(plan["job_config"]):
        return False  # SQL ledger work never enters the legacy Redis dispatcher.
    if lock_key is None:
        async with _schedule_edit_lock(storage, plan["job_id"]) as (key, owner):
            current = await asyncio.to_thread(MPSManifestStore().get, plan["job_id"])
            if current is None:
                return False
            return await ensure_plan(storage, current, lock_key=key, lock_owner=owner)
    if plan["status"] != "preparing":
        await _retire_retry_source(storage, plan)
    await ensure_job(storage, plan, lock_key=lock_key, lock_owner=lock_owner)
    policy = window_policy(plan["job_config"].get("metadata", {}), storage)
    client = storage._client()
    key = mps_state.PLAN_KEY.format(job_id=plan["job_id"])
    async with client.pipeline(transaction=True) as pipe:
        while True:
            try:
                await pipe.watch(lock_key, key)
                await _check_owner(pipe, lock_key, lock_owner)
                cursor, end, reserved, window_class = await pipe.hmget(
                    key, "cursor", "window_end", "reserved_tasks", "window_class"
                )
                active_window = int(end or 0) > int(cursor or 0) or int(reserved or 0) > 0
                window_keys = {
                    "window_budget_key": policy.budget_key,
                    "window_waiting_key": policy.waiting_key,
                    "window_waiting_set_key": policy.waiting_set_key,
                }
                if active_window and window_class not in (b"ordinary", "ordinary"):
                    # Pre-scope MPS windows did not record their reservation keys.
                    # Preserve their old accounting until the window is archived.
                    window_keys = {
                        "window_budget_key": mps_state.LEGACY_MPS_BUDGET_KEY,
                        "window_waiting_key": mps_state.LEGACY_MPS_WAITING_KEY,
                        "window_waiting_set_key": mps_state.LEGACY_MPS_WAITING_SET_KEY,
                    }
                pipe.multi()
                if not active_window:
                    if cursor is not None:
                        pipe.srem(mps_state.LEGACY_MPS_WAITING_SET_KEY, plan["job_id"])
                        pipe.lrem(mps_state.LEGACY_MPS_WAITING_KEY, 0, plan["job_id"])
                    pipe.hset(key, mapping=window_keys)
                for field, value in {
                    "version": mps_state.PLAN_VERSION,
                    "window_class": policy.scope,
                    **window_keys,
                    "task_manifest_id": plan["selection"].get("task_manifest_id", ""),
                    "cursor": plan["cursor"],
                    "window_end": plan["cursor"],
                    "total": plan["total"],
                    "site": plan["site"],
                    "status": "queued",
                }.items():
                    pipe.hsetnx(key, field, value)
                # Nonterminal plans must survive an outage longer than a cache TTL.
                pipe.persist(key)
                await pipe.execute()
                break
            except WatchError:
                # Workers and lease renewal can change watched keys. Re-read
                # window accounting and verify ownership before retrying.
                await asyncio.sleep(0)
    return True


async def _check_owner(pipe: Any, lock_key: str | None, lock_owner: str | None) -> None:
    if lock_key is None:
        return
    from . import materializer as mps

    current = await pipe.get(lock_key)
    if current is None or mps._decode_redis_value(current) != lock_owner:
        raise mps._MPSMaterializationLockLost("MPS materialization ownership changed")


async def _purge_archived_window(
    storage: JobStorage,
    plan: dict[str, Any],
    raw_plan: dict[str, str],
    *,
    lock_key: str | None = None,
    lock_owner: str | None = None,
) -> bool:
    """Release only the generation whose evidence already committed."""
    job_id = plan["job_id"]
    window_end = int(raw_plan["window_end"])
    old_cursor = int(raw_plan["cursor"])
    rolling = raw_plan.get("rolling_window") == "1"
    if not rolling and window_end > plan["cursor"]:
        raise RuntimeError("Refusing to release an unarchived MPS window")
    client = storage._client()
    plan_key = mps_state.PLAN_KEY.format(job_id=job_id)
    index_key = storage.TASK_INDEX_KEY.format(job_id=job_id)
    reservation_key = mps_state.RESERVATIONS_KEY.format(job_id=job_id)
    async with client.pipeline(transaction=True) as pipe:
        await pipe.watch(
            plan_key,
            index_key,
            reservation_key,
            *([lock_key] if lock_key else []),
        )
        await _check_owner(pipe, lock_key, lock_owner)
        current = await pipe.hget(plan_key, "cursor")
        generation = await pipe.hget(plan_key, "task_manifest_id")
        generation = generation.decode() if isinstance(generation, bytes) else generation
        if generation and generation != plan["selection"].get("task_manifest_id"):
            return False
        if current is None or int(current) != old_cursor:
            # A replacement already released this generation. Never delete
            # tasks belonging to its next window.
            return False
        raw_ids = await pipe.zrange(index_key, 0, window_end - old_cursor)
        if len(raw_ids) > window_end - old_cursor:
            raise RuntimeError(
                "MPS task index contains work outside the archived window"
            )
        ids = [
            value.decode() if isinstance(value, bytes) else str(value)
            for value in raw_ids
        ]
        if rolling:
            # Reservations are the durable source for the resident window. The
            # sorted task index can be incomplete in legacy recovery state, so
            # merge its IDs with reservation fields before checking which tasks
            # SQL has already archived.
            raw_reservation_fields = await pipe.hkeys(reservation_key)
            reserved_ids = []
            for raw_field in raw_reservation_fields:
                field = (
                    raw_field.decode()
                    if isinstance(raw_field, bytes)
                    else str(raw_field)
                )
                if field.startswith("task:"):
                    reserved_ids.append(field[5:])
            ids = list(dict.fromkeys([*ids, *reserved_ids]))
            if not ids:
                raise RuntimeError(
                    "Rolling MPS archive advanced without indexed or reserved task IDs"
                )
            if len(ids) > window_end - old_cursor:
                raise RuntimeError(
                    "MPS resident reservations exceed the archived window"
                )
            records = await pipe.hmget(
                reservation_key, ["task:" + task_id for task_id in ids]
            )
            if any(value is None for value in records):
                raise RuntimeError("Resident task is missing its window reservation")
            candidates = {task_id: value["candidate"]
                          for task_id, raw in zip(ids, records, strict=True)
                          if (value := json.loads(raw)).get("candidate") is not None}
            archived = await asyncio.to_thread(
                MPSManifestStore().archived_task_ids, job_id, ids,
                plan["selection"].get("task_manifest_id"), candidates,
            )
            if len(archived) != plan["cursor"] - old_cursor or not archived:
                return False
            ids = [task_id for task_id in ids if task_id in archived]
        pipe.multi()
        for task_id in ids:
            pipe.delete(
                storage.TASK_KEY.format(job_id=job_id, task_id=task_id),
                storage.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id=task_id),
                f"{KEY_PREFIX}:view:task:{task_id}:state",
            )
        state_sets = [
            storage.TASKS_READY_KEY.format(job_id=job_id),
            storage.TASKS_BLOCKED_KEY.format(job_id=job_id),
            storage.TASKS_INFLIGHT_KEY.format(job_id=job_id),
            storage.TASKS_COMPLETED_KEY.format(job_id=job_id),
            mps_state.FAILED_KEY.format(job_id=job_id),
        ]
        if rolling:
            pipe.zrem(index_key, *ids)
            for key in state_sets:
                pipe.srem(key, *ids)
        else:
            pipe.delete(index_key, *state_sets, storage.TASK_OUTPUTS_KEY.format(job_id=job_id))
        await mps_state.release_budget(
            client, job_id, pipe=pipe, task_ids=ids if rolling else None
        )
        pipe.hset(
            plan_key,
            mapping={
                "cursor": plan["cursor"],
                "window_end": window_end if rolling else plan["cursor"],
                "total": plan["total"],
                "activated": raw_plan.get("activated", "0") if rolling and window_end > plan["cursor"] else 0,
                "refill_pending": int(rolling),
            },
        )
        if not rolling or window_end == plan["cursor"]:
            pipe.hset(plan_key, mapping={
                "window_budget_key": mps_state.BUDGET_KEY,
                "window_waiting_key": mps_state.WAITING_KEY,
                "window_waiting_set_key": mps_state.WAITING_SET_KEY,
            })
        pipe.hset(
            storage.JOB_STATE_KEY.format(job_id=job_id),
            mapping={
                "mps_runtime_released": 0,
                "total_tasks": plan["total"],
                "mps_task_type_counts": json.dumps(plan["job_config"].get("metadata", {}).get("mps_task_type_counts") or {"scrape": plan["total"]}),
                "mps_archived_completed": plan["completed"],
                "mps_archived_failed": plan["failed"],
                "completed_tasks": plan["completed"],
                "failed_tasks": plan["failed"],
                "mps_window_end": window_end if rolling else plan["cursor"],
                "ready_tasks": 0,
                "blocked_tasks": 0,
                "inflight_tasks": 0,
                **({"deferred_tasks": 0} if not rolling else {}),
            },
        )
        if rolling:
            pipe.eval("""
                redis.call('HSET', KEYS[1],
                    'ready_tasks', redis.call('SCARD', KEYS[2]),
                    'blocked_tasks', redis.call('SCARD', KEYS[3]),
                    'inflight_tasks', redis.call('SCARD', KEYS[4]),
                    'completed_tasks', tonumber(ARGV[1]) + redis.call('SCARD', KEYS[5]),
                    'failed_tasks', tonumber(ARGV[2]) + redis.call('SCARD', KEYS[6]))
                return 1
            """, 6, storage.JOB_STATE_KEY.format(job_id=job_id), *state_sets,
                plan["completed"], plan["failed"])
        await pipe.execute()
    return True


async def _yield_archived_header(storage: JobStorage, plan: dict[str, Any], *, lock_key: str | None, lock_owner: str | None) -> bool:
    """Yield cache capacity only under the live materializer lease and SQL proof."""
    from .event_log import EventLog
    if lock_key is None or not lock_owner or lock_owner.startswith("mps-edit:"):
        return False
    client = storage._client()
    if not await client.exists(runtime_admission.PRESSURE_KEY):
        return False
    job_id = plan["job_id"]
    manifest_id = plan["selection"].get("task_manifest_id") or ""
    manager = JobManager(storage.redis)
    backend_id = await manager._resolve_backend_assignment(job_id)
    config_key = storage.JOB_CONFIG_KEY.format(job_id=job_id)
    state_key = storage.JOB_STATE_KEY.format(job_id=job_id)
    plan_key = mps_state.PLAN_KEY.format(job_id=job_id)
    index_key = storage.TASK_INDEX_KEY.format(job_id=job_id)
    async with client.pipeline(transaction=True) as pipe:
        await pipe.watch(lock_key, config_key, state_key, plan_key, index_key)
        await _check_owner(pipe, lock_key, lock_owner)
        current_manifest, window_end = await pipe.hmget(plan_key, "task_manifest_id", "window_end")
        current_manifest = current_manifest.decode() if isinstance(current_manifest, bytes) else current_manifest
        if current_manifest != manifest_id or int(window_end or 0) > plan["cursor"] or await pipe.zcard(index_key):
            return False
        # Prove retirement eligibility before removing even a queue reference.
        # Recheck it on each bounded batch: an expired owner must not touch a
        # replacement or its newly admitted window.
        if backend_id:
            queue_key = manager.router._queue_key(backend_id)
            cursor = 0
            while True:
                cursor, items = await client.zscan(queue_key, cursor=cursor, match=f"{job_id}:*", count=storage.DEFAULT_WRITE_BATCH_SIZE)
                members = [value for value, _ in items if (value.decode() if isinstance(value, bytes) else str(value)).startswith(f"{job_id}:")]
                if members:
                    result = await client.eval("""
                        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
                        if redis.call('HGET', KEYS[3], 'task_manifest_id') ~= ARGV[2]
                          or tonumber(redis.call('HGET', KEYS[3], 'window_end') or '0') > tonumber(ARGV[3])
                          or redis.call('ZCARD', KEYS[4]) ~= 0 then return -2 end
                        local removed = 0
                        for i = 4, #ARGV do removed = removed + redis.call('ZREM', KEYS[2], ARGV[i]) end
                        return removed
                    """, 4, lock_key, queue_key, plan_key, index_key, lock_owner, manifest_id, plan["cursor"], *members)
                    if result == -1:
                        await _check_owner(client, lock_key, lock_owner)
                    if result < 0:
                        return False
                if cursor == 0:
                    break
        pipe.multi()
        await mps_state.release_budget(client, job_id, pipe=pipe)
        runtime_admission.release(pipe, job_id)
        pipe.zrem(storage.JOB_INDEX_KEY, job_id)
        pipe.zrem(storage.SCHEDULED_KEY, job_id)
        pipe.srem(mps_state.SCHEDULED_KEY, job_id)
        pipe.lrem(mps_state.QUEUE_KEY, 0, job_id)
        pipe.lrem(manager.router.UNCLAIMED_JOBS_KEY, 0, job_id)
        if backend_id:
            pipe.srem(manager.router._backend_jobs_key(backend_id), job_id)
            pipe.lrem(manager.router._claimed_jobs_key(backend_id), 0, job_id)
        pipe.delete(config_key, state_key, plan_key, index_key,
                    storage.JOB_RUNTIME_TELEMETRY_KEY.format(job_id=job_id),
                    storage.JOB_RUNTIME_WORKERS_KEY.format(job_id=job_id),
                    manager.router._job_envelope_key(job_id),
                    storage.TASKS_READY_KEY.format(job_id=job_id),
                    storage.TASKS_BLOCKED_KEY.format(job_id=job_id),
                    storage.TASKS_INFLIGHT_KEY.format(job_id=job_id),
                    storage.TASKS_COMPLETED_KEY.format(job_id=job_id),
                    storage.TASK_OUTPUTS_KEY.format(job_id=job_id),
                    storage.RECENT_TASK_FAILURES_KEY.format(job_id=job_id),
                    storage.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id),
                    storage.JOB_STATE_LOCK_KEY.format(job_id=job_id),
                    storage.JOB_ROUTING_LOCK_KEY.format(job_id=job_id),
                    mps_state.FAILED_KEY.format(job_id=job_id),
                    # SQL owns the committed task history. A retired header
                    # must not leave a per-job event cache growing with jobs.
                    EventLog.STREAM_KEY_FORMAT.format(job_id=job_id))
        await pipe.execute()
    return True


async def _dispatch_window(
    backend, storage, job_id, window, *, activate, ready_task_ids=None,
    archive_remaining: bool = False,
):
    await storage._client().hdel(mps_state.PLAN_KEY.format(job_id=job_id), "refill_pending")
    await mps_state.notify_complete(storage, job_id)
    if activate and window.get("activated") != "1":
        if not await storage._client().zcard(storage.TASK_INDEX_KEY.format(job_id=job_id)):
            return False
        job = await storage.fetch_job(job_id, include_tasks=False)
        if job is not None and job.state.value in {"pending", "running"}:
            await JobManager(backend.redis).activate_materialized_job(
                job,
                distributed_mode=False if job.metadata.get("dispatch_backend_id") else backend.distributed_mode,
                backend_id=job.metadata.get("dispatch_backend_id") or (None if backend.distributed_mode else backend.instance_id),
                **({"ready_task_ids": ready_task_ids} if ready_task_ids is not None else {}),
            )
            await storage._client().hset(mps_state.PLAN_KEY.format(job_id=job_id), "activated", 1)
            await mps_state.record_refill_event(storage, job_id, "tasks_activated")
    return archive_remaining


async def advance(
    backend: Any,
    job_id: str,
    *,
    lock_key: str | None = None,
    lock_owner: str | None = None,
    activate: bool = True,
    refill_after_archive: bool = False,
) -> bool:
    """Perform one bounded pass. True requests another pass immediately."""
    from . import materializer as mps

    storage = JobStorage(backend.redis)
    client = storage._client()
    store = MPSManifestStore()
    plan = await asyncio.to_thread(store.get, job_id)
    if plan is None:
        raise RuntimeError(f"Durable MPS manifest {job_id} is missing")
    from .ledger import owns
    if owns(plan["job_config"]):
        return False
    if plan["status"] in {"cancelled", "failed"}:
        # Durable cancellation must survive loss of the Redis job header and a
        # stale materializer notification. Never reconstruct pending work here.
        identity = mps_attempt.Identity(
            job_id, "", plan["selection"].get("task_manifest_id") or ""
        )
        try:
            with mps_attempt.scope(identity):
                runtime = await storage.fetch_job(job_id, include_tasks=False)
                if runtime is not None:
                    if plan["status"] == "cancelled":
                        await storage.mark_job_cancelled(
                            job_id, reason="durable MPS cancellation"
                        )
                    else:
                        await storage.mark_job_failed(job_id, reason="durable MPS failure")
                    manager = JobManager(storage.redis)
                    await manager.router.clear_job_routing(
                        job_id=job_id,
                        backend_id=await manager._resolve_backend_assignment(job_id),
                    )
                    if await client.exists(storage.TASK_INDEX_KEY.format(job_id=job_id)):
                        await manager._finalize_terminal_job_cleanup(
                            job_id, terminal_state=JobState(plan["status"])
                        )
        except mps_attempt.StaleTaskDefinition:
            pass  # A replacement made this terminal SQL snapshot obsolete.
        return False
    try:
        await ensure_plan(storage, plan, lock_key=lock_key, lock_owner=lock_owner)
    except runtime_admission.RuntimeAdmissionDeferred:
        return False
    job = await storage.fetch_job(job_id, include_tasks=False)
    assert job is not None
    manifest_id = plan["selection"].get("task_manifest_id")
    if plan["status"] == "preparing":
        if job.state in {JobState.CANCELLED, JobState.FAILED}:
            await _terminate_runtime_mps_job(backend.redis, storage, job, manifest_id)
            return False
        after = plan["selection"]["retry_preparation"]["after"]
        from ..database.mps_retry import retry_batch_limits

        # A failed byte admission may be retried under an explicitly larger
        # budget without replaying the old rejection receipt.
        prepared = await DatabaseWriteClient(backend.redis, producer_id="mps-materializer").submit(
            DatabaseWriteOperation.ADVANCE_MPS_RETRY,
            {"job_id": job_id, "after": after},
            command_id=stable_command_id(
                "mps-retry-page-v1", job_id, after, *retry_batch_limits({"metadata": job.metadata})
            ),
        )
        return not prepared.get("deleted") and prepared["status"] in {"preparing", "ready"}

    if job.metadata.get("mps_task_manifest") != manifest_id:
        return False  # A newer edit won while this SQL snapshot was being read.
    if (
        job.state in {JobState.CANCELLED, JobState.FAILED}
        and plan["cursor"] < plan["total"]
        and not await client.exists(storage.TASK_INDEX_KEY.format(job_id=job_id))
    ):
        # Terminal cleanup already archived/released the live task index. Its
        # retained counters cannot prove a window that no longer exists. Close
        # the SQL plan via the guarded terminal reconciliation path instead.
        await _terminate_runtime_mps_job(backend.redis, storage, job, manifest_id)
        return False
    raw = await client.hgetall(mps_state.PLAN_KEY.format(job_id=job_id))
    window = {
        mps._decode_redis_value(key): mps._decode_redis_value(value)
        for key, value in raw.items()
    }
    start, end = int(window["cursor"]), int(window["window_end"])
    rolling = window.get("rolling_window") == "1"
    if start < plan["cursor"]:
        if not await _purge_archived_window(
            storage, plan, window, lock_key=lock_key, lock_owner=lock_owner
        ):
            return False
        start = plan["cursor"]
        if not rolling:
            end = start
        raw = await client.hgetall(mps_state.PLAN_KEY.format(job_id=job_id))
        window = {mps._decode_redis_value(key): mps._decode_redis_value(value)
                  for key, value in raw.items()}
        if await _yield_archived_header(storage, plan, lock_key=lock_key, lock_owner=lock_owner):
            return False
    policy = window_policy(job.metadata, storage)
    archive_batch_tasks = _positive_int_env("ULTIMA_JOB_ARCHIVE_BATCH_SIZE", 256)
    budget = await client.hmget(policy.budget_key, "tasks", "bytes")
    legacy_budget = await client.hmget(mps_state.LEGACY_MPS_BUDGET_KEY, "tasks", "bytes")
    threshold = await client.hget(mps_state.RUNTIME_SETTINGS_KEY, "refill_ready_threshold")
    proxy_slots = None
    try:
        from ..config import UltimaScraperCollectionConfig
        configured_proxies = UltimaScraperCollectionConfig.load_or_create_default_config().settings.network.proxies
        proxy_slots = sum(max(1, proxy.max_connections or 8)
                          for proxy in configured_proxies if proxy.enabled)
    except Exception:
        logger.debug("Could not resolve connection slots for refill diagnostics", exc_info=True)
    await mps_state.record_refill_event(storage, job_id, "runtime_controls", details={
        "controls": {
            "materializer_version": "coalesced-refill-v7-atomic-finish-acquire",
            "same_pass_refill": refill_after_archive,
            "proxy_connection_slots": proxy_slots,
            "refill_ready_threshold": int(threshold or 0),
            "window_tasks": policy.tasks, "window_bytes": policy.bytes,
            "global_tasks": policy.global_tasks, "global_bytes": policy.global_bytes,
            "input_rows": policy.input_rows, "input_bytes": policy.input_bytes,
            "archive_batch_tasks": archive_batch_tasks,
            "resident_tasks": end - start,
            "resident_bytes": int(window.get("reserved_bytes", "0")),
            "global_used_tasks": int(budget[0] or 0) + int(legacy_budget[0] or 0),
            "global_used_bytes": int(budget[1] or 0) + int(legacy_budget[1] or 0),
        },
    })
    raw_reservations = await client.hgetall(mps_state.RESERVATIONS_KEY.format(job_id=job_id))
    reservations = {mps._decode_redis_value(key): json.loads(value)
                    for key, value in raw_reservations.items()}
    archive_remaining = False
    if end > start:
        stats = await storage.get_job_task_stats(job_id)
        terminal = stats["completed"] + stats["failed"]
        # A released batch gets a refill turn even when more workers finished
        # while this job yielded capacity to other producers.
        refill_pending = (
            rolling and window.get("refill_pending") == "1" and end < plan["total"]
            and job.state in {JobState.PENDING, JobState.RUNNING}
            and not schedule_pending(job) and not job.metadata.get("mps_config_pending")
        )
        if refill_pending:
            # Recovery can resume between a chunk's SQL commit, cleanup and
            # refill. Keep its remaining published completions moving even if
            # the newly ready backlog would suppress a completion notification.
            archive_remaining = bool(
                await client.scard(storage.TASKS_COMPLETED_KEY.format(job_id=job_id))
                or await client.scard(mps_state.FAILED_KEY.format(job_id=job_id))
            )
        if not refill_pending and (
            (rolling and terminal > plan["completed"] + plan["failed"]) or terminal >= end
        ):
            if rolling and job.state in {JobState.PENDING, JobState.RUNNING, JobState.PAUSED}:
                async with client.pipeline(transaction=False) as pipe:
                    pipe.smembers(storage.TASKS_COMPLETED_KEY.format(job_id=job_id))
                    pipe.smembers(mps_state.FAILED_KEY.format(job_id=job_id))
                    completed_ids, failed_ids = await pipe.execute()
                task_ids = sorted(mps._decode_redis_value(value)
                                  for value in completed_ids | failed_ids)
                missing = [task_id for task_id in task_ids
                           if "task:" + task_id not in reservations]
                if missing or len(task_ids) > end - start:
                    raise RuntimeError(
                        "MPS window reservation invariant broken: "
                        f"terminal={len(task_ids)} live_window={end - start} "
                        f"(cursor={start} window_end={end} rolling={rolling} "
                        f"sql_cursor={plan['cursor']}) "
                        f"reservations={len(reservations)} "
                        f"missing={missing[:5]}"
                        + (" (window overflow)" if len(task_ids) > end - start else "")
                    )
                if ((end < plan["total"] or len(task_ids) < end - start)
                        and job.state in {JobState.PENDING, JobState.RUNNING}
                        and not schedule_pending(job)
                        and len(task_ids) > archive_batch_tasks):
                    # Bound the exclusive retirement pass while execution
                    # remains, even after the final candidate was admitted.
                    # Otherwise a large final window blocks worker finishing
                    # for the whole SQL archive transaction and cleanup.
                    # Candidate order keeps the durable checkpoint compact;
                    # explicit graph tasks retain deterministic ID ordering.
                    task_ids.sort(key=lambda task_id: (
                        reservations["task:" + task_id].get("candidate") is None,
                        reservations["task:" + task_id].get("candidate") or 0,
                        task_id,
                    ))
                    task_ids = task_ids[:archive_batch_tasks]
                    archive_remaining = True
                fetched = await storage.fetch_tasks_by_ids(job_id, task_ids)
                if any(task is None or task.state not in {TaskState.COMPLETED, TaskState.FAILED}
                       for task in fetched):
                    raise RuntimeError("MPS completion counters do not match terminal window evidence")
                tasks = [task for task in fetched if task is not None]
            else:
                # Terminal cleanup can release the live state sets before the
                # remaining task evidence commits. Only that drain scans blobs.
                tasks = [task async for task in storage.fetch_tasks_stream(job_id, batch_size=end - start)]
                if rolling:
                    tasks = [task for task in tasks if task.state in {TaskState.COMPLETED, TaskState.FAILED}]
                    if any("task:" + task.task_id not in reservations for task in tasks):
                        raise RuntimeError("Completed task is missing its window reservation")
            if not tasks or (not rolling and (len(tasks) != end - start or any(
                task.state not in {TaskState.COMPLETED, TaskState.FAILED}
                for task in tasks
            ))):
                raise RuntimeError(
                    "MPS completion counters do not match terminal window evidence"
                )
            runtime_telemetry = await storage.get_job_runtime_telemetry(job_id)
            if runtime_telemetry is not None:
                expected_telemetry_tasks = plan["completed"] + plan["failed"] + len(tasks)
                for _ in range(10):
                    if runtime_telemetry.get("task_count", 0) >= expected_telemetry_tasks:
                        break
                    await asyncio.sleep(0.02)
                    runtime_telemetry = await storage.get_job_runtime_telemetry(job_id)
                    if runtime_telemetry is None:
                        break
            runtime_metadata = {
                key: value for key, value in json.loads(runtime_admission.configuration_payload(
                    job_id, job.to_config(include_task_ids=False).to_dict()
                ))["metadata"].items()
                if key in {"run_count", "last_ran_at", "last_error", "result_message"}
            }
            if runtime_telemetry is not None:
                runtime_metadata["runtime_telemetry"] = runtime_telemetry

            await mps_state.record_refill_event(
                storage,
                job_id,
                "archive_started",
                details={
                    "task_count": len(tasks),
                    "window_start": start,
                    "window_end": end,
                },
            )
            plan = await DatabaseWriteClient(
                backend.redis, producer_id="mps-materializer"
            ).submit(
                DatabaseWriteOperation.ARCHIVE_MPS_WINDOW,
                {
                    "job_id": job_id,
                    "start": start,
                    "end": start + len(tasks) if rolling else end,
                    "tasks": [task.to_dict() for task in tasks],
                    "manifest_id": manifest_id,
                    **({"candidate_ordinals": {
                        task.task_id: reservations["task:" + task.task_id]["candidate"]
                        for task in tasks
                        if reservations["task:" + task.task_id].get("candidate") is not None
                    }} if rolling else {}),
                    "runtime": {
                        "state": job.state.value,
                        "created_at": job.created_at,
                        "updated_at": job.updated_at,
                        "started_at": job.started_at,
                        "metadata": runtime_metadata,
                    },
                },
                command_id=(stable_command_id("job-terminal-batch-v1", job_id, manifest_id,
                                               *sorted(task.task_id for task in tasks))
                            if rolling else stable_command_id("mps-window-v2", job_id, manifest_id, start, end)),
            )
            await mps_state.record_refill_event(
                storage,
                job_id,
                "archive_finished",
                details={"task_count": len(tasks), "window_start": start, "window_end": end},
            )
            await mps_state.record_refill_event(storage, job_id, "cleanup_started")
            waiting_before_release = await client.llen(
                window_policy(job.metadata, storage).waiting_key
            )
            if not await _purge_archived_window(
                storage, plan, window, lock_key=lock_key, lock_owner=lock_owner
            ):
                return False
            await mps_state.record_refill_event(storage, job_id, "cleanup_finished")
            start = plan["cursor"]
            if not rolling:
                end = start
            window["activated"] = "0"
            if plan["cursor"] < plan["total"] and job.state.value in {
                "pending",
                "running",
            }:
                if await _yield_archived_header(storage, plan, lock_key=lock_key, lock_owner=lock_owner):
                    return False
                # Preserve fairness under shared-budget pressure. Otherwise
                # use the capacity just released without another queue/lease
                # round trip. Read the post-purge reservations, not the archived
                # snapshot: completed tasks no longer consume the live window.
                policy = window_policy(job.metadata, storage)
                if (not refill_after_archive or waiting_before_release
                        or await client.llen(policy.waiting_key)):
                    await mps_state.record_refill_event(storage, job_id, "refill_stopped",
                        details={"reason": "competing_job_waiting" if refill_after_archive else "same_pass_disabled"})
                    return True
                raw = await client.hgetall(mps_state.PLAN_KEY.format(job_id=job_id))
                window = {mps._decode_redis_value(key): mps._decode_redis_value(value)
                          for key, value in raw.items()}
                raw_reservations = await client.hgetall(
                    mps_state.RESERVATIONS_KEY.format(job_id=job_id)
                )
                reservations = {mps._decode_redis_value(key): json.loads(value)
                                for key, value in raw_reservations.items()}
    if job.state.value in {"cancelled", "failed"} and plan["cursor"] < plan["total"]:
        await _terminate_runtime_mps_job(backend.redis, storage, job, manifest_id)
        return False
    if plan["cursor"] >= plan["total"]:
        # The SQL window transaction owns terminal completion. A worker must
        # not clear the last window before its evidence/checkpoint commits.
        # MPS sweeps stay COMPLETED when some tasks exhaust their retry policy;
        # ordinary jobs fail on any failed task (see terminal_job_state).
        is_success = terminal_job_state(
            plan["job_config"], plan["completed"], plan["failed"]
        ) == "completed"
        job.state = JobState.COMPLETED if is_success else JobState.FAILED
        job.name = plan["job_config"].get("name", job.name)
        job.completed_at = plan["job_config"].get("completed_at") or datetime.now().astimezone().isoformat()
        job.updated_at = job.completed_at
        await storage.persist_job(job)
        manager = JobManager(storage.redis)
        backend_id = await manager._resolve_backend_assignment(job_id)
        if backend_id:
            await manager.router.mark_job_complete(
                backend_id=backend_id, job=job, success=is_success, message="MPS execution complete",
            )
        else:
            await manager.router.clear_job_routing(job_id=job_id, mps_task_manifest=manifest_id or "")
        if await _yield_archived_header(storage, plan, lock_key=lock_key, lock_owner=lock_owner):
            return False
        state_key = storage.JOB_STATE_KEY.format(job_id=job_id)
        async with client.pipeline(transaction=True) as pipe:
            await pipe.watch(state_key, *([lock_key] if lock_key else []))
            await _check_owner(pipe, lock_key, lock_owner)
            current_manifest = await pipe.hget(state_key, "mps_task_manifest")
            if mps._decode_redis_value(current_manifest or "") != (manifest_id or ""):
                return False
            pipe.multi()
            pipe.hset(state_key, mapping={"materialization_status": "complete",
                "materialized_tasks": plan["total"], "materialization_total": plan["total"]})
            pipe.delete(mps_state.PLAN_KEY.format(job_id=job_id))
            await pipe.execute()
        return False
    if job.state.value in {"paused", "completed"}:
        return False
    if schedule_pending(job):
        return False
    ready_task_ids = None
    if start == end or rolling:
        if job.metadata.get("mps_config_pending"):
            await _yield_archived_header(storage, plan, lock_key=lock_key, lock_owner=lock_owner)
            return False
        policy = window_policy(job.metadata, storage)
        limit, byte_limit = policy.tasks, policy.bytes
        resident_tasks = end - start
        resident_bytes = int(window.get("reserved_bytes", "0"))
        if resident_tasks and window.get("window_budget_key") != policy.budget_key:
            return await _dispatch_window(
                backend, storage, job_id, window, activate=activate,
                archive_remaining=archive_remaining,
            )
        global_tasks, global_bytes = policy.global_tasks, policy.global_bytes
        budget_key, waiting_set_key = policy.budget_key, policy.waiting_set_key
        used_tasks, used_bytes = await client.hmget(
            budget_key, "tasks", "bytes"
        )
        legacy_tasks, legacy_bytes = await client.hmget(
            mps_state.LEGACY_MPS_BUDGET_KEY, "tasks", "bytes"
        )
        available_tasks = min(limit - resident_tasks,
                              global_tasks - int(used_tasks or 0) - int(legacy_tasks or 0))
        available_bytes = min(byte_limit - resident_bytes,
                              global_bytes - int(used_bytes or 0) - int(legacy_bytes or 0))
        if available_tasks <= 0 or available_bytes <= 0:
            await mps_state.record_refill_event(storage, job_id, "refill_stopped",
                details={"reason": ("resident_task_limit" if resident_tasks >= limit else
                    "resident_byte_limit" if resident_bytes >= byte_limit else
                    "global_task_budget" if available_tasks <= 0 else "global_byte_budget")})
            if resident_tasks < limit and resident_bytes < byte_limit:
                await mps_state.wait_for_budget(client, job_id)
            return await _dispatch_window(
                backend, storage, job_id, window, activate=activate,
                archive_remaining=archive_remaining,
            )
        page_limit = min(limit, available_tasks)
        read_byte_limit = min(byte_limit, available_bytes)
        maximum_read_bytes = min(byte_limit, global_bytes)
        page_limit = min(page_limit, policy.input_rows)
        read_byte_limit = min(read_byte_limit, policy.input_bytes)
        maximum_read_bytes = min(maximum_read_bytes, policy.input_bytes)
        active_ids = [key[5:] for key in reservations if key.startswith("task:")]
        active_candidates = [value["candidate"] for key, value in reservations.items()
                             if key.startswith("task:") and value.get("candidate") is not None]
        candidate_cursor = int(window.get("candidate_cursor", plan["selection"].get(
            "candidate_checkpoint", {}).get("next", min(start, plan["selection"].get("candidate_total", plan["total"])))))
        from ..database.mps_definitions import MPSDefinitionBudgetExceeded

        await mps_state.record_refill_event(storage, job_id, "manifest_read_started")
        try:
            source = await ready_tasks(
                store, plan, offset=start,
                limit=page_limit, byte_limit=read_byte_limit,
                exclude_task_ids=active_ids, active_candidate_ordinals=active_candidates,
                candidate_cursor=candidate_cursor,
            )
        except MPSDefinitionBudgetExceeded as exc:
            await mps_state.record_refill_event(storage, job_id, "refill_stopped",
                details={"reason": "manifest_input_byte_limit"})
            if exc.required_bytes > maximum_read_bytes:
                raise ValueError("One task exceeds the configured input/window byte budget") from exc
            await mps_state.wait_for_budget(client, job_id)
            return await _dispatch_window(
                backend, storage, job_id, window, activate=activate,
                archive_remaining=archive_remaining,
            )
        await mps_state.record_refill_event(storage, job_id, "manifest_read_finished")
        tasks: list[Task] = []
        task_sizes: list[int] = []
        payloads: dict[str, str] = {}
        occupied = 0
        for task in source.tasks:
            task.mps_task_manifest = manifest_id or ""
            retry_auth_id = plan["selection"].get("retry_auth_id")
            if retry_auth_id is not None:
                task.auth_id = int(retry_auth_id)
                if isinstance(task.payload, ScrapeTaskPayload):
                    task.payload.use_guest_auth = False
            payload = json.dumps(task.to_dict())
            deferred_payload = json.dumps(
                {
                    **task.to_dict(),
                    "state": "deferred",
                    "deferred_dedup_key": task.dedup_key,
                }
            )
            size = max(
                len(payload.encode("utf-8")), len(deferred_payload.encode("utf-8"))
            )
            if size > min(byte_limit, global_bytes):
                raise ValueError(
                    "One MPS task exceeds the configured MPS window byte budget"
                )
            if occupied + size > min(byte_limit, available_bytes):
                if not tasks:
                    await mps_state.wait_for_budget(client, job_id)
                    return await _dispatch_window(
                        backend, storage, job_id, window, activate=activate,
                        archive_remaining=archive_remaining,
                    )
                break
            occupied += size
            tasks.append(task)
            task_sizes.append(size)
            payloads[storage.TASK_KEY.format(job_id=job_id, task_id=task.task_id)] = (
                payload
            )
        if not tasks:
            if resident_tasks:
                return await _dispatch_window(
                    backend, storage, job_id, window, activate=activate,
                    archive_remaining=archive_remaining,
                )
            raise RuntimeError("MPS manifest has no ready tasks before its durable total")
        dependency_outputs: dict[str, str] = {}
        existing_outputs = {key[7:]: value for key, value in reservations.items() if key.startswith("output:")}
        references = Counter()
        output_sizes: dict[str, int] = {}
        if any(task.depends_on for task in tasks):
            references = Counter(
                dependency
                for task in tasks if task.state != TaskState.FAILED
                for dependency in task.depends_on
            )
            dependency_ids = list(references.keys() - existing_outputs.keys())
            for offset in range(0, len(dependency_ids), storage.DEFAULT_WRITE_BATCH_SIZE):
                sizes = await asyncio.to_thread(
                    store.archived_output_sizes, job_id,
                    dependency_ids[offset:offset + storage.DEFAULT_WRITE_BATCH_SIZE],
                )
                output_sizes.update({
                    task_id: len(task_id.encode()) + size
                    for task_id, size in sizes.items()
                })
            if not references.keys() <= output_sizes.keys() | existing_outputs.keys():
                raise RuntimeError("A ready MPS definition has missing dependency evidence")
            dependency_bytes = sum(output_sizes.values())
            while occupied + dependency_bytes > min(byte_limit, available_bytes):
                if len(tasks) == 1:
                    if occupied + dependency_bytes > min(byte_limit, global_bytes):
                        raise ValueError(
                            "One MPS task and its dependency output exceed the configured window byte budget"
                        )
                    await mps_state.wait_for_budget(client, job_id)
                    return await _dispatch_window(
                        backend, storage, job_id, window, activate=activate,
                        archive_remaining=archive_remaining,
                    )
                removed = tasks.pop()
                occupied -= task_sizes.pop()
                payloads.pop(storage.TASK_KEY.format(job_id=job_id, task_id=removed.task_id))
                if removed.state != TaskState.FAILED:
                    for dependency in removed.depends_on:
                        references[dependency] -= 1
                        if references[dependency] == 0:
                            dependency_bytes -= output_sizes.pop(dependency, 0)
            # Only hydrate outputs retained by the accepted task prefix. The
            # database checks each page's aggregate bytes again before transfer.
            retained_ids = list(output_sizes)
            for offset in range(0, len(retained_ids), storage.DEFAULT_WRITE_BATCH_SIZE):
                batch = retained_ids[offset:offset + storage.DEFAULT_WRITE_BATCH_SIZE]
                outputs = await asyncio.to_thread(
                    store.archived_outputs, job_id, batch,
                    byte_limit=min(byte_limit, available_bytes) - occupied,
                )
                if outputs.keys() != set(batch):
                    raise RuntimeError("A ready MPS definition has missing dependency evidence")
                occupied += sum(len(task_id.encode()) + len(output.encode()) for task_id, output in outputs.items())
                dependency_outputs.update(outputs)
        previous_end = end
        end += len(tasks)
        state_key = storage.JOB_STATE_KEY.format(job_id=job_id)
        reserved: set[str] = set()
        published = False
        await mps_state.record_refill_event(storage, job_id, "publication_started")
        try:
            async with client.pipeline(transaction=True) as pipe:
                plan_key = mps_state.PLAN_KEY.format(job_id=job_id)
                await pipe.watch(
                    state_key,
                    budget_key,
                    mps_state.LEGACY_MPS_BUDGET_KEY,
                    mps_state.RESERVATIONS_KEY.format(job_id=job_id),
                    plan_key,
                    *([lock_key] if lock_key else []),
                )
                await _check_owner(pipe, lock_key, lock_owner)
                published_end = await pipe.hget(plan_key, "window_end")
                if published_end is None or int(published_end) != previous_end:
                    return False
                status = await pipe.hget(state_key, "status")
                if status and mps._decode_redis_value(status) in {
                    "completed",
                    "failed",
                    "cancelled",
                    "paused",
                }:
                    return False
                current_tasks, current_bytes = await pipe.hmget(
                    budget_key, "tasks", "bytes"
                )
                legacy_tasks, legacy_bytes = await pipe.hmget(
                    mps_state.LEGACY_MPS_BUDGET_KEY, "tasks", "bytes"
                )
                if (
                    int(current_tasks or 0) + int(legacy_tasks or 0) + len(tasks) > global_tasks
                    or int(current_bytes or 0) + int(legacy_bytes or 0) + occupied > global_bytes
                ):
                    await mps_state.wait_for_budget(client, job_id)
                    return await _dispatch_window(
                        backend, storage, job_id, window, activate=activate,
                        archive_remaining=archive_remaining,
                    )
                reserved = set(await storage.reserve_dedup_many([task for task in tasks if task.state != TaskState.FAILED]))
                for task in tasks:
                    if task.state != TaskState.FAILED and task.task_id not in reserved:
                        task.state = TaskState.DEFERRED
                        task.deferred_dedup_key = task.dedup_key
                        payloads[
                            storage.TASK_KEY.format(job_id=job_id, task_id=task.task_id)
                        ] = json.dumps(task.to_dict())
                pipe.multi()
                pipe.srem(waiting_set_key, job_id)
                pipe.lrem(policy.waiting_key, 0, job_id)
                pipe.hincrby(budget_key, "tasks", len(tasks))
                pipe.hincrby(budget_key, "bytes", occupied)
                pipe.mset(payloads)
                if dependency_outputs:
                    pipe.hset(storage.TASK_OUTPUTS_KEY.format(job_id=job_id), mapping=dependency_outputs)
                entries = {
                    "task:" + task.task_id: json.dumps({
                        "bytes": size,
                        "candidate": source.candidate_ordinals.get(task.task_id),
                        "dependencies": task.depends_on if task.state != TaskState.FAILED else [],
                    }) for task, size in zip(tasks, task_sizes, strict=True)
                }
                for dependency, count in references.items():
                    if count:
                        previous = existing_outputs.get(dependency, {})
                        entries["output:" + dependency] = json.dumps({
                            "bytes": previous.get("bytes", output_sizes.get(dependency)),
                            "refs": previous.get("refs", 0) + count,
                        })
                pipe.hset(mps_state.RESERVATIONS_KEY.format(job_id=job_id), mapping=entries)
                pipe.zadd(
                    storage.TASK_INDEX_KEY.format(job_id=job_id),
                    {task.task_id: previous_end + index for index, task in enumerate(tasks)},
                )
                ready = [
                    task.task_id for task in tasks if task.state == TaskState.PENDING
                ]
                if ready:
                    pipe.sadd(storage.TASKS_READY_KEY.format(job_id=job_id), *ready)
                failed = [task.task_id for task in tasks if task.state == TaskState.FAILED]
                if failed:
                    pipe.sadd(mps_state.FAILED_KEY.format(job_id=job_id), *failed)
                # Deferred lookup/output state is part of window publication.
                # A crash cannot leave visible tasks without their wakeup index.
                await storage.register_deferred_tasks(job_id, tasks, pipeline=pipe)
                pipe.hset(
                    mps_state.PLAN_KEY.format(job_id=job_id),
                    mapping={
                        "window_end": end,
                        "status": "materializing",
                        "activated": 0,
                        "rolling_window": 1,
                        "candidate_cursor": max(candidate_cursor, max(
                            (source.candidate_ordinals[task.task_id] + 1 for task in tasks
                             if task.task_id in source.candidate_ordinals), default=0)),
                        "reserved_tasks": resident_tasks + len(tasks),
                        "reserved_bytes": resident_bytes + occupied,
                    },
                )
                pipe.hset(
                    state_key,
                    mapping={
                        "mps_window_end": end,
                        "mps_runtime_released": 0,
                        "rolling_window": 1,
                        "materialization_status": "materializing",
                        "materialized_tasks": end,
                    },
                )
                pipe.hincrby(state_key, "deferred_tasks", len(tasks) - len(ready) - len(failed))
                pipe.eval("""
                    redis.call('HSET', KEYS[1],
                        'ready_tasks', redis.call('SCARD', KEYS[2]),
                        'failed_tasks', tonumber(ARGV[1]) + redis.call('SCARD', KEYS[3]))
                    return 1
                """, 3, state_key, storage.TASKS_READY_KEY.format(job_id=job_id),
                    mps_state.FAILED_KEY.format(job_id=job_id), plan["failed"])
                await pipe.execute()
                published = True
            await mps_state.record_refill_event(
                storage,
                job_id,
                "tasks_published",
                details={
                    "task_count": len(tasks),
                    "ready_added": len(ready),
                    "window_start": previous_end,
                    "window_end": end,
                },
            )
        except WatchError:
            await mps_state.record_refill_event(storage, job_id, "refill_stopped",
                details={"reason": "publication_watch_retry"})
            # A large window can take long enough to publish that workers or
            # completion bookkeeping change a watched key before EXEC. The
            # durable plan is still intact; release the unpublished dedupe
            # reservations and ask the materializer to retry this pass.
            logger.debug(
                "MPS window publication raced with task progress for %s; retrying",
                job_id,
            )
            return True
        finally:
            if reserved and not published:
                await mps_state.release_unpublished(
                    storage,
                    job_id,
                    [task for task in tasks if task.task_id in reserved],
                    lock_key=lock_key,
                    lock_owner=lock_owner,
                )
        if window.get("activated") == "1":
            ready_task_ids = ready
        window["activated"] = "0"
    return await _dispatch_window(
        backend, storage, job_id, window, activate=activate,
        ready_task_ids=ready_task_ids, archive_remaining=archive_remaining,
    )
