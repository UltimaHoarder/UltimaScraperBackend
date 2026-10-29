"""Shared durable job materialization, leases and reliable queue recovery.

Historical MPS keys and legacy plan versions remain wire-compatible.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, cast
from uuid import uuid4
from redis.exceptions import WatchError
from ..models.backend import BackendInstance
from ..redis_channels import KEY_PREFIX
from ..redis_events import publish_ws_event
from . import Job, JobManager, JobStorage, TaskState, TaskType, mps_state
from .mps_tasks import _build_mps_task_batch
from .resources import recovery_page_size

logger = logging.getLogger(__name__)

MPS_MATERIALIZATION_BATCH_SIZE = 1000
MPS_MATERIALIZATION_PLAN_VERSION = 2
MPS_MATERIALIZATION_PLAN_TTL_SECONDS = 7 * 24 * 60 * 60
MPS_MATERIALIZATION_LOCK_TTL_SECONDS = 90
MPS_MATERIALIZATION_QUEUE_KEY = f"{KEY_PREFIX}:mps:materialization:queue"
MPS_MATERIALIZATION_PROCESSING_KEY = f"{KEY_PREFIX}:mps:materialization:processing"
MPS_MATERIALIZATION_PLAN_KEY = f"{KEY_PREFIX}:mps:materialization:{{job_id}}"
MPS_MATERIALIZATION_LOCK_KEY = f"{KEY_PREFIX}:mps:materialization:{{job_id}}:lock"


class _MPSMaterializationBusy(RuntimeError):
    """Another control process currently owns this materialization plan."""


class _MPSMaterializationLockLost(RuntimeError):
    """The current owner lost its lock before another owner appeared."""


class _MPSMaterializationTerminated(RuntimeError):
    """The user or another runtime transition made the plan terminal."""


def _decode_redis_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _mps_materialization_plan_key(job_id: str) -> str:
    return MPS_MATERIALIZATION_PLAN_KEY.format(job_id=job_id)


def _mps_materialization_lock_key(job_id: str) -> str:
    return MPS_MATERIALIZATION_LOCK_KEY.format(job_id=job_id)


async def _set_mps_materialization_state(
    storage: JobStorage,
    job_id: str,
    *,
    status: str,
    completed: int,
    total: int,
    error: str | None = None,
) -> None:
    state_key = storage.JOB_STATE_KEY.format(job_id=job_id)
    updates: dict[str, Any] = {
        "materialization_status": status,
        "materialized_tasks": completed,
        "materialization_total": total,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        updates["materialization_error"] = error[:1000]
    await storage._client().hset(state_key, mapping=updates)


async def _ensure_mps_job_is_active(storage: JobStorage, job_id: str) -> Job:
    job = await storage.fetch_job(job_id, include_tasks=False)
    if job is None:
        raise _MPSMaterializationTerminated(
            f"MPS materialization job {job_id} no longer exists"
        )
    if job.state.value in {"completed", "failed", "cancelled"}:
        raise _MPSMaterializationTerminated(job.state.value)
    return job


async def _refresh_mps_materialization_lock(
    client: Any,
    *,
    lock_key: str,
    owner: str,
) -> None:
    compare_and_expire = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('expire', KEYS[1], ARGV[2])
    end
    return 0
    """
    while True:
        await asyncio.sleep(MPS_MATERIALIZATION_LOCK_TTL_SECONDS / 3)
        try:
            refreshed = await client.eval(compare_and_expire, 1, lock_key, owner,
                                           MPS_MATERIALIZATION_LOCK_TTL_SECONDS)
            if not refreshed:
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning('Could not refresh MPS materialization lock %s', lock_key, exc_info=True)


async def _ensure_mps_materialization_lock(
    client: Any,
    *,
    lock_key: str,
    owner: str,
) -> None:
    current_owner = await client.get(lock_key)
    if current_owner is None:
        raise _MPSMaterializationLockLost("MPS materialization lock expired")
    if _decode_redis_value(current_owner) != owner:
        raise _MPSMaterializationBusy("MPS materialization lock ownership was lost")


async def _acquire_mps_materialization_lock(
    client: Any,
    *,
    lock_key: str,
    owner: str,
    wait_seconds: float = 2.0,
) -> bool:
    """Alternate bounded refill turns with workers blocked by the last pass.

    New finishers otherwise keep renewing the shared lease throughout a
    completion wave, starving refill until the resident window is empty.
    An exclusive owner still fails immediately; other jobs retain their turns.
    """
    waiting_key = f"{lock_key}:waiting"
    acquire = """
        local intent = redis.call('GET', KEYS[2])
        if ARGV[4] == '1' and intent ~= ARGV[1] then return -1 end
        if intent and intent ~= ARGV[1] then return -1 end
        local holder = redis.call('GET', KEYS[1])
        if holder and string.sub(holder, 1, 11) ~= 'mps-finish:' then return -1 end
        if not intent then
            -- Previous archive chunks must first let their blocked workers
            -- join the shared lease. Do not block those workers with an intent.
            local now = tonumber(redis.call('TIME')[1])
            redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
            if redis.call('ZCARD', KEYS[3]) > 0 then return 0 end
            if holder then
                -- Once that wave has joined, reserve refill's turn before
                -- new arrivals. Workers blocked after this point yield it.
                redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[3])
                return 2
            end
        elseif holder then
            return 2
        end
        redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
        if intent then redis.call('DEL', KEYS[2]) end
        return 1
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_seconds
    delay = 0.005
    owns_lock = False
    intent_requested = False
    try:
        while True:
            acquired = await client.eval(
                acquire, 3, lock_key, waiting_key, f"{lock_key}:finish_waiters", owner,
                MPS_MATERIALIZATION_LOCK_TTL_SECONDS,
                max(1, int(wait_seconds) + 1), int(intent_requested),
            )
            if acquired in (-1, 1):
                owns_lock = acquired == 1
                return owns_lock
            if acquired == 2:
                intent_requested = True
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 0.05)
    finally:
        # Successful acquisition already removed the intent atomically. Avoid
        # an await between acquiring the lease and starting its owner heartbeat.
        if not owns_lock:
            await _release_mps_materialization_lock(client, lock_key=waiting_key, owner=owner)
            # Cancellation can interrupt an acquisition reply after Lua took
            # the lease. Ownership checks preserve another consumer's lease.
            await _release_mps_materialization_lock(client, lock_key=lock_key, owner=owner)


async def _release_mps_materialization_lock(
    client: Any,
    *,
    lock_key: str,
    owner: str,
) -> None:
    compare_and_delete = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """
    try:
        await client.eval(compare_and_delete, 1, lock_key, owner)
        return
    except Exception:
        # Lightweight Redis implementations may not support Lua. Preserve the
        # same atomic ownership check with WATCH/MULTI instead.
        pass

    while True:
        pipe = client.pipeline(transaction=True)
        try:
            await pipe.watch(lock_key)
            current_owner = await pipe.get(lock_key)
            if current_owner is None or _decode_redis_value(current_owner) != owner:
                return
            pipe.multi()
            pipe.delete(lock_key)
            await pipe.execute()
            return
        except WatchError:
            continue
        finally:
            await pipe.reset()


async def _materialize_mps_job(backend: BackendInstance, job_id: str) -> bool | None:
    if backend.redis is None:
        raise RuntimeError("Redis became unavailable during MPS materialization")

    storage = JobStorage(backend.redis)
    manager = JobManager(backend.redis)
    client = storage._client()
    plan_key = _mps_materialization_plan_key(job_id)
    lock_key = _mps_materialization_lock_key(job_id)
    lock_owner = f"{backend.instance_id}:{uuid4().hex}"
    acquired = await _acquire_mps_materialization_lock(
        client,
        lock_key=lock_key,
        owner=lock_owner,
    )
    if not acquired:
        raise _MPSMaterializationBusy(job_id)

    lock_task = asyncio.create_task(
        _refresh_mps_materialization_lock(
            client,
            lock_key=lock_key,
            owner=lock_owner,
        )
    )
    try:
        if (_decode_redis_value(await client.hget(plan_key, 'version')) == str(mps_state.PLAN_VERSION)
                or await client.hget(storage.JOB_STATE_KEY.format(job_id=job_id), 'mps_manifest') in (b'1', '1')):
            from .durable import advance

            await mps_state.record_refill_event(storage, job_id, "materializer_started")
            # The reliable consumer releases its processing slot before it
            # offers this next pass. Requeueing here stalls a one-slot budget
            # until periodic SQL recovery, despite already committed progress.
            return await advance(
                backend, job_id, lock_key=lock_key, lock_owner=lock_owner,
                refill_after_archive=True,
            )
        raw_plan = await client.hgetall(plan_key)
        if not raw_plan:
            state = await client.hgetall(storage.JOB_STATE_KEY.format(job_id=job_id))
            decoded_state = {
                _decode_redis_value(key): _decode_redis_value(value)
                for key, value in state.items()
            }
            if decoded_state.get("materialization_status") == "complete":
                raise _MPSMaterializationTerminated("complete")
            raise RuntimeError(f"MPS materialization plan for {job_id} is missing")
        plan = {
            _decode_redis_value(key): _decode_redis_value(value)
            for key, value in raw_plan.items()
        }
        target_user_ids = [
            int(value) for value in cast(list[Any], json.loads(plan["target_user_ids"]))
        ]
        proxy_pool = [
            str(value)
            for value in cast(list[Any], json.loads(plan.get("proxy_pool", "[]")))
        ]
        site_display = plan["site"]
        include_direct = str(plan.get("include_direct", "")).strip() in {"1", "true", "yes"}
        try:
            max_route_hops = int(plan.get("max_route_hops", "3"))
        except (TypeError, ValueError):
            max_route_hops = 3
        try:
            conclusive_miss_threshold = (
                int(plan["conclusive_miss_threshold"])
                if "conclusive_miss_threshold" in plan
                else 3
            )
        except (TypeError, ValueError):
            conclusive_miss_threshold = 3
        total = len(target_user_ids)
        cursor = min(max(int(plan.get("cursor", "0")), 0), total)
        try:
            plan_version = int(plan.get("version", "1"))
            ready_count = max(int(plan.get("ready_count", "0")), 0)
            deferred_count = max(int(plan.get("deferred_count", "0")), 0)
        except (TypeError, ValueError):
            plan_version = 1
            ready_count = 0
            deferred_count = 0
        inline_state_index = (
            plan_version >= MPS_MATERIALIZATION_PLAN_VERSION
            and ready_count + deferred_count == cursor
        )

        job = await _ensure_mps_job_is_active(storage, job_id)
        job.metadata.update(
            {
                "materialization_status": "materializing",
                "materialized_tasks": cursor,
                "materialization_total": total,
                "total_tasks_count_override": total,
                "include_direct_connection": include_direct,
            }
        )
        await storage.persist_job(job, persist_tasks=False)
        await _set_mps_materialization_state(
            storage,
            job_id,
            status="materializing",
            completed=cursor,
            total=total,
        )
        await client.hset(plan_key, mapping={"status": "materializing"})

        while cursor < total:
            await _ensure_mps_materialization_lock(
                client,
                lock_key=lock_key,
                owner=lock_owner,
            )
            await _ensure_mps_job_is_active(storage, job_id)
            batch_end = min(cursor + MPS_MATERIALIZATION_BATCH_SIZE, total)
            tasks = _build_mps_task_batch(
                job_id=job_id,
                site_display=site_display,
                target_user_ids=target_user_ids[cursor:batch_end],
                proxy_pool=proxy_pool,
                candidate_offset=cursor,
                include_direct=include_direct,
                max_route_hops=max_route_hops,
                conclusive_miss_threshold=conclusive_miss_threshold,
            )
            reserved_task_ids = set(await storage.reserve_dedup_many(tasks))
            for task in tasks:
                if task.task_id in reserved_task_ids:
                    continue
                task.state = TaskState.DEFERRED
                task.deferred_dedup_key = task.dedup_key

            await storage.persist_tasks(job_id, tasks)
            await storage.register_deferred_tasks(job_id, tasks)
            if inline_state_index:
                ready_task_ids = [
                    task.task_id for task in tasks if task.state != TaskState.DEFERRED
                ]
                if ready_task_ids:
                    ready_key = storage.TASKS_READY_KEY.format(job_id=job_id)
                    await client.sadd(ready_key, *ready_task_ids)
                    await client.expire(ready_key, storage.task_state_ttl)
                ready_count += len(ready_task_ids)
                deferred_count += len(tasks) - len(ready_task_ids)
            cursor = batch_end
            plan_updates: dict[str, Any] = {
                "cursor": cursor,
                "status": "materializing",
            }
            if inline_state_index:
                plan_updates.update(
                    ready_count=ready_count,
                    deferred_count=deferred_count,
                )
            await client.hset(plan_key, mapping=plan_updates)
            await client.expire(plan_key, MPS_MATERIALIZATION_PLAN_TTL_SECONDS)
            await _set_mps_materialization_state(
                storage,
                job_id,
                status="materializing",
                completed=cursor,
                total=total,
            )
            if inline_state_index:
                await storage._write_task_stats(
                    job_id,
                    total=total,
                    ready=ready_count,
                    blocked=0,
                    inflight=0,
                    deferred=deferred_count,
                    completed=0,
                    failed=0,
                    task_type_counts={TaskType.SCRAPE.value: total},
                )
            await asyncio.sleep(0)

        await _ensure_mps_materialization_lock(
            client,
            lock_key=lock_key,
            owner=lock_owner,
        )
        job = await _ensure_mps_job_is_active(storage, job_id)
        job.metadata.update(
            {
                "materialization_status": "finalizing",
                "materialized_tasks": total,
                "materialization_total": total,
                "total_tasks_count_override": total,
            }
        )
        await storage.persist_job(job, persist_tasks=False)
        await _set_mps_materialization_state(
            storage,
            job_id,
            status="finalizing",
            completed=total,
            total=total,
        )
        await client.hset(plan_key, mapping={"status": "finalizing"})

        # Read the physical index here rather than the advertised total in the
        # job-state hash. The placeholder publishes the intended total before
        # task blobs exist, so get_total_task_count() cannot prove that every
        # task actually made it into Redis.
        materialized_count = int(
            await client.zcard(storage.TASK_INDEX_KEY.format(job_id=job_id))
        )
        actual_ready_count = await storage.get_ready_task_count(job_id)
        state_index_is_complete = (
            inline_state_index
            and materialized_count == total
            and actual_ready_count == ready_count
            and ready_count + deferred_count == total
        )
        if state_index_is_complete:
            await storage._write_task_stats(
                job_id,
                total=total,
                ready=ready_count,
                blocked=0,
                inflight=0,
                deferred=deferred_count,
                completed=0,
                failed=0,
                task_type_counts={TaskType.SCRAPE.value: total},
            )
        else:
            # Plans created by older releases, or a plan whose expiring state
            # sets disappeared during a long outage, still need the recovery
            # scan. New versioned plans take the single-pass path above.
            logger.info(
                "Rebuilding task state for legacy or incomplete MPS plan %s",
                job_id,
            )
            await storage.restore_task_state(job, return_ready_task_ids=False)
        await _ensure_mps_materialization_lock(
            client,
            lock_key=lock_key,
            owner=lock_owner,
        )
        job = await _ensure_mps_job_is_active(storage, job_id)
        assigned_backend_id = await manager.activate_materialized_job(
            job,
            distributed_mode=backend.distributed_mode,
            backend_id=None if backend.distributed_mode else backend.instance_id,
        )

        job.metadata.update(
            {
                "materialization_status": "complete",
                "materialized_tasks": total,
                "materialization_total": total,
            }
        )
        if assigned_backend_id:
            job.metadata["backend_id"] = assigned_backend_id
        await storage.persist_job(job, persist_tasks=False)
        await _set_mps_materialization_state(
            storage,
            job_id,
            status="complete",
            completed=total,
            total=total,
        )
        await publish_ws_event(
            backend.redis,
            {
                "type": "webui_ack",
                "event": "job_materialized",
                "job_id": job_id,
                "job_name": job.name,
                "status": job.state.value,
                "task_count": total,
                "materialization_status": "complete",
            },
        )
        await client.delete(plan_key)
    finally:
        lock_task.cancel()
        try:
            await lock_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning(
                "MPS materialization lock heartbeat stopped with an error",
                exc_info=True,
            )
        try:
            await _release_mps_materialization_lock(
                client,
                lock_key=lock_key,
                owner=lock_owner,
            )
        except Exception:
            logger.warning(
                "Could not release MPS materialization lock %s",
                lock_key,
                exc_info=True,
            )


async def _recover_stale_mps_materializations(
    backend: BackendInstance, *, after: int = 0,
) -> int:
    if backend.redis is None:
        return 0
    client = cast(Any, backend.redis.client)
    limit = recovery_page_size(JobStorage.DEFAULT_WRITE_BATCH_SIZE)
    raw_job_ids = await client.lrange(MPS_MATERIALIZATION_PROCESSING_KEY, after, after + limit - 1)
    removed_count = 0
    for raw_job_id in raw_job_ids:
        job_id = _decode_redis_value(raw_job_id)
        if await client.exists(_mps_materialization_lock_key(job_id)):
            continue
        # A reliable-processing entry already occupies notification capacity.
        # Transfer it atomically: interruption between LREM and LPUSH used to
        # lose the hint, and a concurrent publisher could fill the freed slot.
        removed = await client.eval("""
            if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
            local removed = redis.call('LREM', KEYS[2], 0, ARGV[1])
            if removed > 0 and redis.call('SADD', KEYS[4], ARGV[1]) == 1 then
                redis.call('LPUSH', KEYS[3], ARGV[1])
            end
            return removed
        """, 4, _mps_materialization_lock_key(job_id),
            MPS_MATERIALIZATION_PROCESSING_KEY, MPS_MATERIALIZATION_QUEUE_KEY,
            mps_state.SCHEDULED_KEY, job_id)
        if removed:
            removed_count += 1
    # Account for this page's removals. Concurrent changes may move an item
    # across a page boundary; wrapping the cursor revisits it on a later pass.
    return max(0, after + len(raw_job_ids) - removed_count) if len(raw_job_ids) == limit else 0
