"""Bound the job configuration cache; durable SQL plans own waiting work.

The ledger is one hash, so Redis loss cannot retain its per-job charges while
evicting only its total. Publication and release update it in the same transaction
as the header. Byte limits cover encoded configurations; Redis container costs
and the fixed-field state/plan hashes are additional and bounded by header count.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from ..database.write_queue import DatabaseWriteClient, _positive_int_env
from ..redis_channels import KEY_PREFIX

LEDGER_KEY = f"{KEY_PREFIX}:jobs:headers:budget"
PRESSURE_KEY = f"{KEY_PREFIX}:jobs:headers:pressure"
CURSOR_KEY = f"{KEY_PREFIX}:jobs:headers:recovery_cursor"
RUNTIME_FIELDS = frozenset({
    "worker_id", "backend_id", "failure_reason", "cancel_reason",
    "total_tasks_count_override", "completed_tasks_count_override", "failed_tasks_count_override",
    "materialization_status", "materialized_tasks", "materialization_total",
    "mps_task_type_counts", "mps_config_revision", "mps_schedule_pending",
    "run_count", "last_ran_at", "last_error", "result_message",
})
DIAGNOSTIC_FIELDS = ("failure_reason", "cancel_reason", "last_error", "result_message")


class RuntimeAdmissionDeferred(RuntimeError):
    """A committed job is waiting for runtime cache capacity."""


@dataclass(frozen=True, slots=True)
class HeaderPolicy:
    count: int
    bytes: int
    per_header_bytes: int
    metadata_bytes: int


def policy() -> HeaderPolicy:
    total_bytes = _positive_int_env(
        "ULTIMA_JOB_RUNTIME_MAX_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
    )
    header_bytes = _positive_int_env(
        "ULTIMA_JOB_RUNTIME_HEADER_MAX_BYTES",
        min(total_bytes, _positive_int_env(
            "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
        )),
    )
    return HeaderPolicy(
        _positive_int_env("ULTIMA_JOB_RUNTIME_MAX_HEADERS", 1000),
        total_bytes,
        header_bytes,
        _positive_int_env("ULTIMA_JOB_RUNTIME_METADATA_MAX_BYTES", max(1, min(4096, header_bytes // 2))),
    )


def validate_payload(job_id: str, payload: str, limits: HeaderPolicy | None = None, *, reserve_metadata: bool = False) -> int:
    from .storage import TaskPayloadBudgetExceeded

    limits = limits or policy()
    if reserve_metadata:
        config = json.loads(payload)
        config["metadata"] = {key: value for key, value in config.get("metadata", {}).items() if key not in RUNTIME_FIELDS}
        size = len(json.dumps(config).encode("utf-8")) + limits.metadata_bytes
    else:
        size = len(payload.encode("utf-8"))
    ceiling = min(limits.per_header_bytes, limits.bytes)
    if size > ceiling:
        raise TaskPayloadBudgetExceeded(job_id, size, ceiling, phase="runtime header")
    return size


async def defer(client: Any, job_id: str, reason: str) -> None:
    # One shared hint, with no growing collection of waiting IDs. Recovery
    # refreshes it while demand exceeds capacity; SQL retains every waiter.
    await client.set(PRESSURE_KEY, "1", ex=30)
    raise RuntimeAdmissionDeferred(f"Job {job_id} is waiting for runtime capacity: {reason}")


async def check(pipe: Any, storage: Any, job_id: str, payload: str) -> int:
    """Check before MULTI; callers retry WatchError and queue record() with SET."""
    limits = policy()
    size = validate_payload(job_id, payload, limits, reserve_metadata=True)
    config_key = storage.JOB_CONFIG_KEY.format(job_id=job_id)
    await pipe.watch(LEDGER_KEY, storage.JOB_INDEX_KEY, config_key)
    old, count, occupied = await pipe.hmget(LEDGER_KEY, f"j:{job_id}", "count", "bytes")
    count, occupied = int(count or 0), int(occupied or 0)
    indexed_count = int(await pipe.zcard(storage.JOB_INDEX_KEY))
    indexed = await pipe.zscore(storage.JOB_INDEX_KEY, job_id)
    old_size = int(old or 0)
    existing_size = int(await pipe.strlen(config_key))
    if not existing_size:
        if indexed is None and max(count, indexed_count) >= limits.count:
            await defer(storage._client(), job_id, "header count")
        # A bounded reconciliation pass charges headers from older releases or
        # a lost ledger before new admissions can spend their unknown bytes.
        if old is None and indexed is None and count < indexed_count:
            await defer(storage._client(), job_id, "existing header accounting")
    total = occupied + size - old_size
    if total > limits.bytes and size > (old_size if old is not None else existing_size + limits.metadata_bytes):
        await defer(storage._client(), job_id, "configuration bytes")
    return size


def configuration_payload(job_id: str, config: dict[str, Any]) -> str:
    """Keep cache diagnostics within reserved metadata space; preserve identities.

    The caller's job and SQL error/result remain intact. Only header diagnostic
    previews are shortened, with an ellipsis, so a failure can finish even when
    other headers occupy all unreserved capacity.
    """
    from .storage import TaskPayloadBudgetExceeded

    limits = policy()
    metadata = dict(config.get("metadata") or {})
    runtime = {key: metadata[key] for key in RUNTIME_FIELDS if key in metadata}
    maximum = limits.metadata_bytes - 128
    identities = {key: value for key, value in runtime.items() if key not in DIAGNOSTIC_FIELDS}
    identity_bytes = len(json.dumps(identities).encode())
    if identity_bytes > maximum:
        raise TaskPayloadBudgetExceeded(job_id, identity_bytes + 128, limits.metadata_bytes, phase="runtime identity metadata")
    for key in DIAGNOSTIC_FIELDS:
        if key not in runtime:
            continue
        value = str(runtime[key])
        trial = {**identities, key: value}
        if len(json.dumps(trial).encode()) <= maximum:
            identities = trial
            continue
        lo, hi = 0, len(value)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if len(json.dumps({**identities, key: value[:mid] + "…"}).encode()) <= maximum:
                lo = mid
            else:
                hi = mid - 1
        preview = {**identities, key: value[:lo] + "…"}
        if len(json.dumps(preview).encode()) <= maximum:
            identities = preview
        else:
            metadata.pop(key, None)
    metadata.update(identities)
    return json.dumps({**config, "metadata": metadata})


def validate_durable_configuration(job_id: str, config: dict[str, Any]) -> None:
    """Reject an individually impossible header before its SQL transaction commits."""
    from .models import JobConfig

    header = JobConfig(
        job_id=job_id, name=config.get("name", ""),
        created_by=config.get("created_by", "system"),
        metadata={**config.get("metadata", {}), "mps_manifest": True},
    ).to_dict()
    header["schedule"] = config.get("schedule") or header["schedule"]
    validate_payload(job_id, configuration_payload(job_id, header), reserve_metadata=True)


_RECORD = """
local field = 'j:' .. ARGV[1]
local old = redis.call('HGET', KEYS[1], field)
if old == ARGV[2] then return 0 end
if not old then redis.call('HINCRBY', KEYS[1], 'count', 1) end
redis.call('HINCRBY', KEYS[1], 'bytes', tonumber(ARGV[2]) - tonumber(old or '0'))
redis.call('HSET', KEYS[1], field, ARGV[2])
return 1
"""


def record(pipe: Any, job_id: str, size: int) -> None:
    pipe.eval(_RECORD, 1, LEDGER_KEY, job_id, size)


ADMIT_LUA = f"""
local function admit_header(config_key, index_key, job_id, reservation, max_count, max_bytes, headroom)
    local field = 'j:' .. job_id
    local old = redis.call('HGET', '{LEDGER_KEY}', field)
    local count = tonumber(redis.call('HGET', '{LEDGER_KEY}', 'count') or '0')
    local bytes = tonumber(redis.call('HGET', '{LEDGER_KEY}', 'bytes') or '0')
    local indexed_count = redis.call('ZCARD', index_key)
    local indexed = redis.call('ZSCORE', index_key, job_id)
    local existing = redis.call('STRLEN', config_key)
    local size = tonumber(reservation)
    if existing == 0 and not indexed then
        if math.max(count, indexed_count) >= tonumber(max_count) or (not old and count < indexed_count) then
            redis.call('SET', '{PRESSURE_KEY}', '1', 'EX', 30)
            return -10
        end
    end
    local previous = tonumber(old or '0')
    if bytes + size - previous > tonumber(max_bytes) and size > (old and previous or existing + tonumber(headroom)) then
        redis.call('SET', '{PRESSURE_KEY}', '1', 'EX', 30)
        return -10
    end
    if not old then redis.call('HINCRBY', '{LEDGER_KEY}', 'count', 1) end
    if not old or size ~= previous then
        redis.call('HINCRBY', '{LEDGER_KEY}', 'bytes', size - previous)
        redis.call('HSET', '{LEDGER_KEY}', field, size)
    end
    return 1
end
"""


RELEASE_LUA = f"""
local function release_header(job_id)
    local field = 'j:' .. job_id
    local old = redis.call('HGET', '{LEDGER_KEY}', field)
    if old then
        redis.call('HDEL', '{LEDGER_KEY}', field)
        redis.call('HINCRBY', '{LEDGER_KEY}', 'count', -1)
        redis.call('HINCRBY', '{LEDGER_KEY}', 'bytes', -tonumber(old))
    end
end
"""


def release(pipe: Any, job_id: str) -> None:
    pipe.eval(RELEASE_LUA + "release_header(ARGV[1]); return 1", 0, job_id)


async def reconcile(storage: Any, *, limit: int) -> None:
    """Charge one index page without reading configuration payloads into Python."""
    client = storage._client()
    after = max(0, int(await client.get(CURSOR_KEY) or 0))
    ids = await client.zrange(storage.JOB_INDEX_KEY, after, after + limit - 1)
    for raw_id in ids:
        job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        # Existing headers may already exceed a newly configured budget. Account
        # for them and prevent additional admissions; do not delete live work.
        await client.eval("""
            if not redis.call('ZSCORE', KEYS[3], ARGV[1]) then return 0 end
            local actual = redis.call('STRLEN', KEYS[2])
            local field = 'j:' .. ARGV[1]
            local old = redis.call('HGET', KEYS[1], field)
            if old and actual <= tonumber(old) then return 0 end
            local size = actual + tonumber(ARGV[2])
            if not old then redis.call('HINCRBY', KEYS[1], 'count', 1) end
            redis.call('HINCRBY', KEYS[1], 'bytes', size - tonumber(old or '0'))
            redis.call('HSET', KEYS[1], field, size)
            return 1
        """, 3, LEDGER_KEY, storage.JOB_CONFIG_KEY.format(job_id=job_id),
            storage.JOB_INDEX_KEY, job_id, policy().metadata_bytes)
    await client.set(CURSOR_KEY, after + len(ids) if len(ids) == limit else 0)
