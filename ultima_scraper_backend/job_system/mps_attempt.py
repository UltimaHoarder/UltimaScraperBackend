"""Ownership of a worker attempt within an immutable MPS task definition."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..redis_channels import KEY_PREFIX
from . import retention

logger = logging.getLogger(__name__)
MPS_FINISH_WAITER_TTL_SECONDS = 3


class StaleTaskDefinition(RuntimeError):
    """The attempt no longer belongs to the job's current task definition."""


class DefinitionEditInProgress(RuntimeError):
    """A writer must retry after an edit, freeing its lane for that edit's SQL."""


@dataclass(frozen=True)
class Identity:
    job_id: str
    task_id: str
    manifest_id: str


_attempt: ContextVar[Identity | None] = ContextVar("mps_attempt", default=None)


def current(job_id: str | None = None) -> Identity | None:
    value = _attempt.get()
    return (
        value
        if value is not None and (job_id is None or value.job_id == job_id)
        else None
    )


@contextmanager
def bind(job: Any, task: Any):
    from .ordinary_edit import task_scope

    value = None
    if job.metadata.get("mps_manifest"):
        value = Identity(job.job_id, task.task_id, task.mps_task_manifest or "")
    with scope(value), task_scope(job):
        yield value


@contextmanager
def scope(value: Identity | None):
    token = _attempt.set(value)
    try:
        yield value
    finally:
        _attempt.reset(token)


def lock_key(job_id: str) -> str:
    return f"{KEY_PREFIX}:mps:materialization:{job_id}:lock"


_GUARD = """
if redis.call('HGET', KEYS[1], 'mps_manifest') ~= '1' then return -1 end
if (redis.call('HGET', KEYS[1], 'mps_task_manifest') or '') ~= ARGV[1] then return -1 end
local editor = redis.call('GET', KEYS[2])
if editor and string.sub(editor, 1, 9) == 'mps-edit:' then return -2 end
"""

_OPERATION_PROJECTION = """
local operation_keys = {}
for i = 3, #KEYS do operation_keys[i - 2] = KEYS[i] end
local operation_args = {}
for i = 2, #ARGV do operation_args[i - 1] = ARGV[i] end
local KEYS, ARGV = operation_keys, operation_args
"""


async def mutate(
    storage: Any,
    job_id: str,
    manifest_id: str,
    script: str,
    keys: list[str],
    args: list[Any],
    *,
    wait_for_edit: bool = True,
) -> Any:
    """Atomically check the definition and mutate Redis; an edit may abort.

    Wait for an in-progress edit instead of losing a result if validation fails
    and the original definition remains authoritative. Cancellation stays possible.
    """
    while True:
        result = await retention.evaluate(
            storage,
            job_id,
            _GUARD + script,
            [storage.JOB_STATE_KEY.format(job_id=job_id), lock_key(job_id), *keys],
            [manifest_id, *args],
        )
        if result == -1:
            raise StaleTaskDefinition(f"MPS task definition for {job_id} changed")
        if result != -2:
            return result
        if not wait_for_edit:
            raise DefinitionEditInProgress(f"MPS task definition for {job_id} is being edited")
        await asyncio.sleep(0.05)


async def control_mutation(
    storage: Any, job_id: str, script: str, keys: list[str], args: list[Any],
    *, wait_for_edit: bool = True,
) -> Any:
    """Apply a control operation with its original Lua key/argument indices."""
    identity = current(job_id)
    if identity is None:
        from .ordinary_edit import control_mutation as ordinary_mutation

        return await ordinary_mutation(storage, job_id, script, keys, args)
    # mutate checks its leading state/lease keys before this local projection.
    # Copy only this bounded command's references, without Lua unpack limits.
    return await mutate(
        storage, job_id, identity.manifest_id, _OPERATION_PROJECTION + script, keys, args,
        wait_for_edit=wait_for_edit,
    )


async def assert_current(
    storage: Any, job_id: str, manifest_id: str, *, wait_for_edit: bool = True
) -> None:
    await mutate(
        storage, job_id, manifest_id, "return 1", [], [],
        wait_for_edit=wait_for_edit,
    )


async def persist_job(storage: Any, job: Any) -> None:
    """Update runtime fields without overwriting the SQL-owned configuration."""
    from redis.exceptions import WatchError
    from . import runtime_admission

    identity = current(job.job_id)
    manifest_id = (
        identity.manifest_id
        if identity
        else job.metadata.get("mps_task_manifest") or ""
    )
    state_key = storage.JOB_STATE_KEY.format(job_id=job.job_id)
    config_key = storage.JOB_CONFIG_KEY.format(job_id=job.job_id)
    state = {
        key: value
        for key, value in job.to_state().to_dict().items()
        if value is not None
        and key in {"status", "created_at", "updated_at", "started_at", "completed_at"}
    }
    client = storage._client()
    while True:
        await assert_current(storage, job.job_id, manifest_id)
        async with client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(state_key, config_key, lock_key(job.job_id))
                await assert_current(storage, job.job_id, manifest_id)
                current_status = await pipe.hget(state_key, "status")
                if isinstance(current_status, bytes):
                    current_status = current_status.decode()
                desired_status = state.get("status")
                if (
                    current_status in {"completed", "failed", "cancelled"}
                    and desired_status not in {"completed", "failed", "cancelled"}
                ):
                    raise StaleTaskDefinition(
                        f"Cannot rewrite terminal MPS job {job.job_id} "
                        f"as {desired_status}"
                    )
                raw = await pipe.get(config_key)
                if raw is None:
                    raise StaleTaskDefinition("MPS definition header is missing")
                config = json.loads(raw)
                config.setdefault("metadata", {}).update(
                    {
                        key: value
                        for key, value in job.metadata.items()
                        if key in {"worker_id", "backend_id", "failure_reason"}
                    }
                )
                payload = runtime_admission.configuration_payload(job.job_id, config)
                size = await runtime_admission.check(pipe, storage, job.job_id, payload)
                pipe.multi()
                runtime_admission.record(pipe, job.job_id, size)
                pipe.set(config_key, payload, xx=True, keepttl=True)
                pipe.hset(state_key, mapping=state)
                await pipe.execute()
                return
            except WatchError:
                continue


_ACQUIRE_SHARED_FINISH = """
-- A waiting refill must be able to drain existing finishers. Their renewals
-- and releases remain independent; only new finishers yield this turn.
local now = tonumber(redis.call('TIME')[1])
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', now)
local current_lock = redis.call('GET', KEYS[1])
if redis.call('EXISTS', KEYS[3]) == 1 or
   (current_lock and string.sub(current_lock, 1, 11) ~= 'mps-finish:') then
    -- Remember workers blocked by this archive so the next chunk cannot
    -- immediately overtake them again. Crashed requests expire promptly.
    redis.call('ZADD', KEYS[4], now + tonumber(ARGV[3]), ARGV[1])
    redis.call('EXPIRE', KEYS[4], ARGV[3])
    return 0
end
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if not current_lock then
    redis.call('SET', KEYS[1], 'mps-finish:active', 'EX', ARGV[2])
end
local ttl = tonumber(ARGV[2])
redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return 1
"""

_RELEASE_SHARED_FINISH = """
local now = tonumber(redis.call('TIME')[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local remaining = redis.call('ZCARD', KEYS[2])
if remaining == 0 then
    local current_lock = redis.call('GET', KEYS[1])
    if current_lock and string.sub(current_lock, 1, 11) == 'mps-finish:' then
        redis.call('DEL', KEYS[1])
    end
    redis.call('DEL', KEYS[2])
    return 0
else
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    redis.call('EXPIRE', KEYS[2], ARGV[2])
    return remaining
end
"""

_ABANDON_SHARED_FINISH = """
redis.call('ZREM', KEYS[3], ARGV[1])
-- Cancellation can race the acquisition reply. Release only this request's
-- active membership if Lua acquired it before the caller was interrupted.
if not redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 0 end
""" + _RELEASE_SHARED_FINISH

_REFRESH_SHARED_FINISH = """
local now = tonumber(redis.call('TIME')[1])
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
    local current_lock = redis.call('GET', KEYS[1])
    if current_lock and string.sub(current_lock, 1, 11) == 'mps-finish:' then
        local ttl = tonumber(ARGV[2])
        redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        redis.call('EXPIRE', KEYS[2], ARGV[2])
        return 1
    end
end
return 0
"""


async def _refresh_shared_finish_lock(
    client: Any,
    *,
    lock_key: str,
    finishers_key: str,
    finish_id: str,
    ttl: int,
) -> None:
    while True:
        await asyncio.sleep(ttl / 3)
        try:
            refreshed = await client.eval(
                _REFRESH_SHARED_FINISH,
                2,
                lock_key,
                finishers_key,
                finish_id,
                ttl,
            )
            if not refreshed:
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Could not refresh shared MPS finish lock %s", lock_key, exc_info=True)


@asynccontextmanager
async def finish_lock(storage: Any, job: Any, task: Any):
    """Keep post-handler routing/cleanup within the definition it completed.

    Handlers and finishing bookkeeping run concurrently across workers without
    blocking each other. This shared finish lease only serializes bookkeeping
    with definition replacement and archive/purge, using the materialization lease.
    """
    if not job.metadata.get("mps_manifest"):
        from .ordinary_edit import finish

        async with finish(storage, job):
            yield
        return
    from . import materializer as mps

    client = storage._client()
    key = lock_key(job.job_id)
    finishers_key = f"{key}:finishers"
    finish_waiters_key = f"{key}:finish_waiters"
    finish_id = f"finish:{uuid4().hex}"
    manifest_id = task.mps_task_manifest or ""
    ttl = mps.MPS_MATERIALIZATION_LOCK_TTL_SECONDS
    # Bookkeeping usually holds this lease for only a few milliseconds.
    # Retry promptly, then back off to the existing rate if an exclusive writer holds the lock.
    delay = 0.005
    try:
        while True:
            # Check the definition and acquire in the same Redis command.
            # Separate guards amplify Redis traffic on every blocked retry.
            if await mutate(
                storage, job.job_id, manifest_id,
                _OPERATION_PROJECTION + _ACQUIRE_SHARED_FINISH,
                [key, finishers_key, f"{key}:waiting", finish_waiters_key],
                [finish_id, ttl, MPS_FINISH_WAITER_TTL_SECONDS],
            ):
                break
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 0.05)
    except BaseException:
        try:
            await client.eval(
                _ABANDON_SHARED_FINISH, 3, key, finishers_key,
                finish_waiters_key, finish_id, ttl,
            )
        except Exception:
            logger.warning("Could not remove MPS finish request %s", finish_id, exc_info=True)
        raise
    heartbeat = asyncio.create_task(
        _refresh_shared_finish_lock(
            client,
            lock_key=key,
            finishers_key=finishers_key,
            finish_id=finish_id,
            ttl=ttl,
        )
    )
    try:
        # The atomic acquisition verified the definition; this held shared
        # lease prevents an editor from replacing it before bookkeeping.
        yield
    finally:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        remaining = await client.eval(
            _RELEASE_SHARED_FINISH,
            2,
            key,
            finishers_key,
            finish_id,
            ttl,
        )
        # When the last active finisher completes, notify the consumer so
        # that archive/purge can acquire the exclusive materialization lease.
        if remaining == 0:
            from .mps_state import notify_complete

            await notify_complete(storage, job.job_id)


def sql_definitions(session: Any, payloads: Any) -> dict[str, str]:
    """Lock definition headers once per bounded writer batch."""
    from sqlalchemy import select
    from ultima_scraper_db_archive.global_db import MPSPlanModel

    ids = sorted({str(payload["job_id"]) for payload in payloads})
    if not ids:
        return {}
    rows = session.execute(
        select(MPSPlanModel.job_id, MPSPlanModel.selection_json)
        .where(
            MPSPlanModel.job_id.in_(ids),
        )
        .order_by(MPSPlanModel.job_id)
        .with_for_update()
    )
    return {
        job_id: json.loads(selection).get("task_manifest_id") or ""
        for job_id, selection in rows
    }


def sql_assert_current(session: Any, job_id: str) -> None:
    """Fence one archive batch in the same transaction that writes its rows."""
    identity = current(job_id)
    if identity is None:
        return
    payload = {"job_id": job_id, "mps_task_manifest": identity.manifest_id}
    if not sql_is_current(payload, sql_definitions(session, [payload])):
        raise StaleTaskDefinition(f"MPS task definition for {job_id} changed")


def sql_is_current(payload: dict[str, Any], definitions: dict[str, str]) -> bool:
    """Fence delayed SQL telemetry in the transaction that applies it."""
    manifest_id = definitions.get(str(payload["job_id"]))
    if manifest_id is None:
        return "mps_task_manifest" not in payload
    return manifest_id == (payload.get("mps_task_manifest") or "")
