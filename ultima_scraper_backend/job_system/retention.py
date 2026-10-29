"""Guard retirement mutations against a concurrently replaced job header."""

from __future__ import annotations

import json
from hashlib import sha256
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from redis.exceptions import ResponseError


class StaleRetirement(RuntimeError):
    """The archived job header changed before runtime cleanup finished."""


@dataclass(frozen=True)
class Snapshot:
    job_id: str
    status: str
    generation_field: str
    generation: str
    config: bytes | str | None
    index_score: float
    manifest_flag: str = ""
    task_manifest: str = ""
    backend_id: str = ""
    preserve_header: bool = False
    runtime_state: dict[str, str | None] = field(default_factory=dict)


def terminal_runtime(state: dict[str, Any]) -> dict[str, str | None]:
    """Final fields retained in SQL and fenced throughout runtime removal."""
    return {name: state.get(name) for name in (
        "created_at", "updated_at", "started_at", "completed_at",
        "total_tasks", "completed_tasks", "failed_tasks",
    )}


def header_byte_limit(storage: Any, *, value: int | None = None) -> int:
    from ..database.write_queue import _positive_int_env

    return _positive_int_env("ULTIMA_JOB_HEADER_ARCHIVE_MAX_BYTES", storage.database_writes.max_command_bytes, value=value)


async def read_header(storage: Any, job_id: str, *, max_bytes: int) -> tuple[dict[str, Any], Any, Any]:
    """Size the compact header in Redis before transferring it to the writer."""
    from .storage import TaskPayloadBudgetExceeded

    fields = ["status", "edit_pending", "mps_manifest", "mps_task_manifest", "backend_id", *terminal_runtime({})]
    values, config, score, size = await storage._client().eval("""
        -- retirement_header_read
        local fields = cjson.decode(ARGV[2])
        local bytes = redis.call('STRLEN', KEYS[2])
        for _, field in ipairs(fields) do bytes = bytes + redis.call('HSTRLEN', KEYS[1], field) end
        if bytes > tonumber(ARGV[3]) then return {false, false, false, bytes} end
        return {redis.call('HMGET', KEYS[1], unpack(fields)), redis.call('GET', KEYS[2]),
            redis.call('ZSCORE', KEYS[3], ARGV[1]), bytes}
    """, 3, storage.JOB_STATE_KEY.format(job_id=job_id), storage.JOB_CONFIG_KEY.format(job_id=job_id),
        storage.JOB_INDEX_KEY, job_id, json.dumps(fields), max_bytes)
    if int(size) > max_bytes:
        raise TaskPayloadBudgetExceeded(job_id, int(size), max_bytes, phase="retirement header")
    state = {key: value.decode() if isinstance(value, bytes) else value
             for key, value in zip(fields, values, strict=True) if value is not None}
    return state, config, float(score) if score is not None else None


async def capture_terminal(
    storage: Any, job_id: str, status: str | None = None,
    *, allow_updated_at: bool = False, max_bytes: int | None = None,
) -> Snapshot | None:
    """Read a complete terminal header atomically for guarded task cleanup."""
    if max_bytes is None:
        pipe = storage._client().pipeline(transaction=True)
        pipe.hgetall(storage.JOB_STATE_KEY.format(job_id=job_id))
        pipe.get(storage.JOB_CONFIG_KEY.format(job_id=job_id))
        pipe.zscore(storage.JOB_INDEX_KEY, job_id)
        raw_state, config, score = await pipe.execute()
    else:
        raw_state, config, score = await read_header(storage, job_id, max_bytes=max_bytes)
    state = {
        (key.decode() if isinstance(key, bytes) else key):
        (value.decode() if isinstance(value, bytes) else value)
        for key, value in raw_state.items()
    }
    status = state.get("status") if status is None else status
    generation_field = (
        "updated_at" if allow_updated_at and not state.get("completed_at")
        else "completed_at"
    )
    if (status not in {"completed", "cancelled", "failed"}
            or state.get("edit_pending")
            or state.get("status") != status or not state.get(generation_field)
            or config is None or score is None):
        return None
    return Snapshot(
        job_id, status, generation_field, state[generation_field], config, float(score),
        manifest_flag=state.get("mps_manifest") or "",
        task_manifest=state.get("mps_task_manifest") or "",
        backend_id=state.get("backend_id") or "", preserve_header=True,
        runtime_state=terminal_runtime(state) if max_bytes is not None else {},
    )


def definition_id(job_id: str, config: dict[str, Any] | bytes | str | None) -> str:
    """Older ordinary jobs use their unique job ID as the first definition."""
    if isinstance(config, (bytes, str)):
        config = json.loads(config)
    if config is None:
        return job_id
    if not isinstance(config, dict) or not isinstance(config.get("metadata", {}), dict):
        raise RuntimeError(f"Invalid stored job configuration for {job_id}")
    value = config.get("metadata", {}).get("task_definition_id", job_id)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"Invalid task definition identity for {job_id}")
    return value


def archive_signature(snapshot: Snapshot) -> str:
    """Give a changed cleanup header a fresh archive receipt, even on rename."""
    digest = sha256()
    values = (
        snapshot.job_id, snapshot.status, snapshot.generation_field,
        snapshot.generation, snapshot.config, snapshot.index_score,
        snapshot.manifest_flag, snapshot.task_manifest, snapshot.backend_id,
    )
    if snapshot.runtime_state:
        values += (json.dumps(snapshot.runtime_state, sort_keys=True),)
    for value in values:
        data = value if isinstance(value, bytes) else str(value).encode("utf-8")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def sql_assert_current(session: Any, job_id: str) -> None:
    """Serialize an ordinary archive batch with task replacement in PostgreSQL.

    The same row lock is held by the edit transaction until its new task rows
    and configuration commit. Each bounded archive transaction checks again.
    """
    snapshot = current(job_id)
    if snapshot is None or snapshot.manifest_flag == "1":
        return
    from sqlalchemy import select
    from ultima_scraper_db_archive.global_db import CentralJobModel
    from .ordinary_edit import sql_definition_expression

    row = session.execute(
        select(sql_definition_expression())
        .where(CentralJobModel.job_id == job_id)
        .with_for_update()
    ).one_or_none()
    if row is None:
        raise RuntimeError(f"Cannot archive job {job_id}: durable job header is missing")
    if row[0] != definition_id(job_id, snapshot.config):
        raise StaleRetirement(f"Archived task definition for {job_id} changed in PostgreSQL")


_snapshot: ContextVar[Snapshot | None] = ContextVar("job_retention", default=None)


def current(job_id: str | None = None) -> Snapshot | None:
    value = _snapshot.get()
    return (
        value
        if value is not None and (job_id is None or value.job_id == job_id)
        else None
    )


@contextmanager
def scope(value: Snapshot):
    token = _snapshot.set(value)
    try:
        yield value
    finally:
        _snapshot.reset(token)


_GUARD = """
local config = redis.call('GET', KEYS[2])
local score = redis.call('ZSCORE', KEYS[3], ARGV[1])
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[2]
    or redis.call('HGET', KEYS[1], 'edit_pending')
    or redis.call('HGET', KEYS[1], ARGV[3]) ~= ARGV[4]
    or (ARGV[5] == '1' and config ~= ARGV[6])
    or (ARGV[5] == '0' and config)
    or not score or tonumber(score) ~= tonumber(ARGV[7])
    or (redis.call('HGET', KEYS[1], 'mps_manifest') or '') ~= ARGV[8]
    or (redis.call('HGET', KEYS[1], 'mps_task_manifest') or '') ~= ARGV[9]
    or (redis.call('HGET', KEYS[1], 'backend_id') or '') ~= ARGV[10] then
    return redis.error_reply('USR_STALE_RETIREMENT')
end
local runtime = cjson.decode(ARGV[11])
for field, expected in pairs(runtime) do
    local actual = redis.call('HGET', KEYS[1], field)
    if (expected == cjson.null and actual)
        or (expected ~= cjson.null and actual ~= expected) then
        return redis.error_reply('USR_STALE_RETIREMENT')
    end
end
local operation_keys = {}
for i = 4, #KEYS do operation_keys[i - 3] = KEYS[i] end
local operation_args = {}
for i = 12, #ARGV do operation_args[i - 11] = ARGV[i] end
local KEYS, ARGV = operation_keys, operation_args
"""


async def evaluate(
    storage: Any,
    job_id: str,
    script: str,
    keys: list[str],
    args: list[Any],
) -> Any:
    """Check and mutate in one command, including within an MPS definition guard."""
    snapshot = current(job_id)
    if snapshot is None:
        return await storage._client().eval(script, len(keys), *keys, *args)
    try:
        return await storage._client().eval(
            _GUARD + script,
            3 + len(keys),
            storage.JOB_STATE_KEY.format(job_id=job_id),
            storage.JOB_CONFIG_KEY.format(job_id=job_id),
            storage.JOB_INDEX_KEY,
            *keys,
            job_id,
            snapshot.status,
            snapshot.generation_field,
            snapshot.generation,
            int(snapshot.config is not None),
            snapshot.config if snapshot.config is not None else "",
            snapshot.index_score,
            snapshot.manifest_flag,
            snapshot.task_manifest,
            snapshot.backend_id,
            json.dumps(snapshot.runtime_state),
            *args,
        )
    except ResponseError as error:
        if str(error).startswith("USR_STALE_RETIREMENT"):
            raise StaleRetirement(
                f"Archived job header for {job_id} changed"
            ) from error
        raise
