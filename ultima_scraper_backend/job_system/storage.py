"""Redis-backed persistence layer for the job system."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Iterable, Optional, Sequence, cast
from uuid import uuid4

from ultima_scraper_core import runtime_env_value

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ultima_scraper_backend.redis_channels import KEY_PREFIX

from ..database.write_queue import (
    DatabaseWriteClient,
    DatabaseWriteOperation,
    DatabaseWriteRejected,
    stable_command_id,
    _positive_int_env,
)
from ..redis.client import RedisManager
from .mps_state import retain_task_state, retain_deferred, remove_deferred, DEFERRED_RETENTION_MEMBER
from . import mps_attempt, ordinary_edit, recovery_scripts, retention
from .models import Job, JobConfig, JobRuntimeState, JobState, Task, TaskState, TaskType
from .runtime_telemetry import RuntimeTelemetryMixin

logger = logging.getLogger(__name__)
SUPPRESS_AUTOMATIC_RECOVERY_ENV = "ULTIMA_SUPPRESS_AUTOMATIC_RECOVERY"


def _utc_timestamp() -> float:
    return datetime.now(timezone.utc).timestamp()


def _decode_redis_text(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _env_flag(name: str) -> bool:
    raw = os.getenv(name, "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _coerce_optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return None
    return None


def _normalize_site_key(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _job_snapshot_job_type(job_snapshot: dict[str, Any] | None) -> str:
    snapshot = job_snapshot if isinstance(job_snapshot, dict) else {}
    for candidate in (
        snapshot.get("name"),
        snapshot.get("job_name"),
        snapshot.get("job_type"),
    ):
        normalized = str(candidate).strip() if candidate is not None else ""
        if normalized:
            return normalized
    return "job"


def _job_snapshot_site(job_snapshot: dict[str, Any] | None) -> str:
    snapshot = job_snapshot if isinstance(job_snapshot, dict) else {}
    metadata = snapshot.get("metadata")
    metadata_map: dict[str, Any] = (
        cast(dict[str, Any], metadata) if isinstance(metadata, dict) else {}
    )

    for candidate in (
        snapshot.get("site"),
        snapshot.get("site_name"),
        metadata_map.get("site"),
        metadata_map.get("site_name"),
    ):
        normalized = _normalize_site_key(candidate)
        if normalized:
            return normalized
    return "unknown"


def _build_terminal_job_args_payload(
    job_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    snapshot = job_snapshot if isinstance(job_snapshot, dict) else {}
    payload: dict[str, Any] = {
        "job_name": _job_snapshot_job_type(snapshot),
    }

    created_by = snapshot.get("created_by")
    if isinstance(created_by, str) and created_by.strip():
        payload["created_by"] = created_by.strip()

    task_count = _coerce_optional_int(snapshot.get("total_tasks"))
    if task_count is None:
        task_count = _coerce_optional_int(snapshot.get("task_count"))
    if task_count is not None:
        payload["task_count"] = task_count

    metadata = snapshot.get("metadata")
    if isinstance(metadata, dict):
        metadata_map = cast(dict[str, Any], metadata)
        for key in ("source", "source_job_id", "template_id", "template_name"):
            value = metadata_map.get(key)
            if isinstance(value, str) and value.strip():
                payload[key] = value.strip()

    return payload


_TASK_PROGRESS_KEYS: tuple[str, ...] = (
    "items_processed",
    "items_scraped",
    "subscriptions_processed",
    "items_saved",
    "items_downloaded",
    "downloads_completed",
)
_TASK_TOTAL_KEYS: tuple[str, ...] = (
    "items_total",
    "total_items",
    "subscriptions_total",
    "media_count",
    "total_media",
)


def _iso_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _first_int_from_mapping(
    mapping: dict[str, Any], keys: tuple[str, ...]
) -> int | None:
    for key in keys:
        if key not in mapping:
            continue
        candidate = mapping[key]
        if isinstance(candidate, bool):
            continue
        try:
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def _task_progress_values(task: Task) -> tuple[int, int | None, str | None]:
    """Derive (items_processed, items_total, error_message) for archival."""

    items_processed = 0
    items_total: int | None = None
    error_message: str | None = None

    result = task.result
    if result is not None:
        metrics: dict[str, Any] = dict(result.metrics or {})
        output_raw = result.output
        output: dict[str, Any] = (
            cast(dict[str, Any], output_raw) if isinstance(output_raw, dict) else {}
        )

        processed = _first_int_from_mapping(metrics, _TASK_PROGRESS_KEYS)
        if processed is None:
            processed = _first_int_from_mapping(output, _TASK_PROGRESS_KEYS)
        total = _first_int_from_mapping(metrics, _TASK_TOTAL_KEYS)
        if total is None:
            total = _first_int_from_mapping(output, _TASK_TOTAL_KEYS)

        if processed is not None:
            items_processed = processed
        items_total = total
        if items_total is not None and items_processed > items_total:
            items_total = items_processed
        if items_total is None and items_processed > 0 and result.success:
            items_total = items_processed

        if result.errors:
            error_message = result.errors[0]
        elif result.message and task.state in (TaskState.FAILED, TaskState.CANCELLED):
            error_message = result.message

    return items_processed, items_total, error_message


def _task_to_central_row(task: Task, job_id: str) -> dict[str, Any]:
    """Map a Redis-backed Task dataclass onto a CentralTaskModel row dict."""

    return {
        **_task_history_columns(task, job_id),
        "definition_json": (json.dumps(task.to_dict(include_result=False))
                            if task.mps_task_manifest is None else None),
    }


def _task_history_columns(task: Task, job_id: str) -> dict[str, Any]:
    """The CentralTaskModel columns other than ``definition_json``.

    None of them read ``task.payload``, so the ledger archive can build them
    from a task decoded without its payload.
    """

    items_processed, items_total, error_message = _task_progress_values(task)
    started_at = _iso_to_datetime(task.started_at)
    completed_at = _iso_to_datetime(task.completed_at)
    last_event_at = _iso_to_datetime(task.updated_at) or completed_at
    created_at = _iso_to_datetime(task.created_at) or datetime.now(timezone.utc)

    # Tasks can use synthetic string identities (for example API commands),
    # while the archive's user_id column only stores numeric profile IDs.
    # The original identity remains in definition_json for task restoration.
    try:
        user_id = int(task.user_id) if task.user_id is not None else None
    except (TypeError, ValueError):
        user_id = None

    message: str | None = None
    result_json: str | None = None
    if task.result is not None:
        if task.result.message:
            message = task.result.message
        try:
            result_json = json.dumps(task.result.to_dict(), default=str)
        except (TypeError, ValueError) as exc:  # noqa: BLE001
            logger.debug(
                "Failed to serialize task result for archive (job=%s task=%s): %s",
                job_id,
                task.task_id,
                exc,
            )
            result_json = None

    return {
        "task_id": task.task_id,
        "job_id": job_id,
        "task_type": task.task_type.value,
        "status": task.state.value,
        "site": (task.site_name or None),
        "user_id": user_id,
        "username": task.user_username,
        "content_type": task.content_type,
        "auth_id": task.auth_id,
        "items_processed": items_processed,
        "items_total": items_total,
        "created_at": created_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "last_event_at": last_event_at,
        "error": error_message,
        "message": message,
        "result_json": result_json,
    }


class TaskLoadLimitExceeded(RuntimeError):
    """Raised when a caller requests too many inline tasks for one job."""


class ArchivedTaskInputsUnavailable(RuntimeError):
    """The archive predates input preservation or contains invalid inputs."""


class TaskPayloadBudgetExceeded(TaskLoadLimitExceeded):
    """One task cannot fit the requested transfer or archive byte allowance."""

    def __init__(
        self,
        task_id: str,
        required_bytes: int,
        byte_limit: int,
        *,
        phase: str = "Redis read",
    ):
        self.task_id = task_id
        self.required_bytes = required_bytes
        self.byte_limit = byte_limit
        super().__init__(
            f"Task {task_id} requires {required_bytes} bytes for {phase}; budget is {byte_limit}"
        )


_TASK_PREFIX_WITHIN_BYTES = """
local values, used = {}, 0
local allowance = tonumber(ARGV[1])
for i, key in ipairs(KEYS) do
    local size = redis.call('STRLEN', key)
    if used + size > allowance then return {used, size, values} end
    values[#values + 1] = redis.call('GET', key) or false
    used = used + size
end
return {used, 0, values}
"""


@dataclass(slots=True)
class TaskPageResult:
    """A bounded page of tasks plus the next cursor."""

    tasks: list[Task]
    next_cursor: str | None
    has_more: bool
    scanned_count: int


class JobStorage(RuntimeTelemetryMixin):
    """Persist jobs and tasks in Redis using the project key conventions."""

    JOB_CONFIG_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:config"
    JOB_STATE_KEY = f"{KEY_PREFIX}:job:{{job_id}}:state"
    JOB_RUNTIME_TELEMETRY_KEY = f"{KEY_PREFIX}:job:{{job_id}}:runtime_telemetry"
    JOB_RUNTIME_SAMPLES_KEY = f"{KEY_PREFIX}:job:{{job_id}}:runtime_samples"
    JOB_RUNTIME_WORKERS_KEY = f"{KEY_PREFIX}:job:{{job_id}}:runtime_workers"
    JOB_RUNTIME_SAMPLES_MAXLEN = 18_000
    TASK_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:task:{{task_id}}"
    JOB_INDEX_KEY = f"{KEY_PREFIX}:jobs:index"
    RETENTION_CURSOR_KEY = f"{KEY_PREFIX}:jobs:retention:cursor"
    TASK_INDEX_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:tasks"
    DEDUPE_KEY = f"{KEY_PREFIX}:tasks:dedupe:{{dedup}}"
    SCHEDULED_KEY = f"{KEY_PREFIX}:jobs:scheduled"

    TASKS_READY_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:tasks:ready"
    TASKS_BLOCKED_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:tasks:blocked"
    TASKS_COMPLETED_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:tasks:completed"
    TASKS_INFLIGHT_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:tasks:inflight"
    RECENT_TASK_FAILURES_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:tasks:recent_failures"
    TASK_DEPENDENTS_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:task:{{task_id}}:dependents"
    TASK_OUTPUTS_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:task_outputs"
    DEFERRED_TASKS_KEY = f"{KEY_PREFIX}:deferred:{{dedup_key}}"
    DEFERRED_REQUEUE_LOCK_KEY = f"{KEY_PREFIX}:deferred:{{dedup_key}}:requeue_lock"
    TASK_EXEC_LOCK_KEY = f"{KEY_PREFIX}:task:{{task_id}}:exec_lock"
    JOB_STATE_LOCK_KEY = f"{KEY_PREFIX}:job:{{job_id}}:state_lock"
    JOB_ROUTING_LOCK_KEY = f"{KEY_PREFIX}:job:{{job_id}}:routing_lock"
    TASK_ARCHIVE_MARKER_KEY = f"{KEY_PREFIX}:jobs:{{job_id}}:task_archive"
    RECOVERY_CYCLE_LOCK_KEY = f"{KEY_PREFIX}:recovery:backend:{{backend_id}}"
    PENDING_TASK_PURGE_KEY = f"{KEY_PREFIX}:jobs:pending-task-purge"

    TASK_EXEC_LOCK_TTL = 5 * 60
    JOB_ROUTING_LOCK_TTL_SECONDS = 30 * 60
    DEFAULT_DEDUP_TTL_SECONDS = 3600
    DEFAULT_TASK_STATE_TTL = 24 * 60 * 60
    DEFAULT_COMPLETED_JOB_RETENTION_SECONDS = 24 * 60 * 60
    DEFAULT_MAX_INCLUDE_TASKS = 500
    DEFAULT_WRITE_BATCH_SIZE = 1000
    TASK_PAGE_SCAN_MULTIPLIER = 5
    RECOVERY_CYCLE_LOCK_TTL_SECONDS = 45

    def __init__(self, redis_manager: RedisManager) -> None:
        self.redis = redis_manager
        self.database_writes = DatabaseWriteClient(
            redis_manager,
            producer_id="job-storage",
        )
        self.task_state_ttl = self.DEFAULT_TASK_STATE_TTL
        self.completed_job_retention_seconds = (
            self.DEFAULT_COMPLETED_JOB_RETENTION_SECONDS
        )

    def _client(self) -> Any:
        """Return the underlying Redis client cast for dynamic attribute use."""

        return cast(Any, self.redis.client)

    @staticmethod
    def _recovery_scan_count(fallback: int) -> int:
        raw = runtime_env_value("ULTIMA_RECOVERY_SCAN_COUNT")
        if raw is None or not raw.strip():
            return fallback
        try:
            count = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "ULTIMA_RECOVERY_SCAN_COUNT must be a positive integer"
            ) from exc
        if count <= 0:
            raise ValueError("ULTIMA_RECOVERY_SCAN_COUNT must be a positive integer")
        return count

    async def _iter_set_members(
        self,
        key: str,
        *,
        batch_size: int = 1000,
    ) -> AsyncIterator[list[str]]:
        """Yield set members in bounded batches."""

        cursor = 0
        client = self._client()
        while True:
            cursor, members = await client.sscan(key, cursor=cursor, count=batch_size)
            normalized = [
                member.decode() if isinstance(member, bytes) else str(member)
                for member in cast(Sequence[Any], members)
            ]
            if normalized:
                yield normalized
            if cursor == 0:
                break

    async def _iter_job_tasks(
        self,
        job_id: str,
        *,
        batch_size: int = 1000,
        preloaded_tasks: Sequence[Task] | None = None,
    ) -> AsyncIterator[Task]:
        """Yield job tasks from memory when available, otherwise stream from Redis."""

        if preloaded_tasks is not None:
            for task in preloaded_tasks:
                yield task
            return

        async for task in self.fetch_tasks_stream(job_id, batch_size=batch_size):
            yield task

    async def _scan_dependency_index_keys(self, job_id: str) -> AsyncIterator[str]:
        pattern = self.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id="*")
        async for raw_key in self._client().scan_iter(match=pattern, count=100):
            yield raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)

    async def _clear_dependency_index(self, job_id: str) -> None:
        async for key in self._scan_dependency_index_keys(job_id):
            if mps_attempt.current(job_id) is not None or retention.current(job_id) is not None or ordinary_edit.current(job_id) is not None:
                await mps_attempt.control_mutation(
                    self, job_id, "return redis.call('DEL', KEYS[1])", [key], []
                )
            else:
                await self.redis.delete(key)

    async def _clear_job_deferred_members(self, job_id: str) -> None:
        """Remove deferred markers belonging to a single job."""

        member_prefix = f"{job_id}:"
        pattern = self.DEFERRED_TASKS_KEY.format(dedup_key="*")
        client = self._client()
        async for raw_key in client.scan_iter(match=pattern, count=100):
            deferred_key = (
                raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            )
            if await client.type(deferred_key) not in (b"set", "set"):
                # Requeue lease keys share this prefix but are strings.
                continue
            async for members in self._iter_set_members(deferred_key, batch_size=500):
                to_remove = [
                    member for member in members if member.startswith(member_prefix)
                ]
                if to_remove:
                    await remove_deferred(self, deferred_key, *to_remove)

    async def _index_task_dependencies(self, job_id: str, task: Task) -> None:
        """Index reverse dependencies so completion only checks affected tasks."""

        if not task.depends_on:
            return
        client = self._client()
        for dependency_id in task.depends_on:
            dependents_key = self.TASK_DEPENDENTS_KEY.format(
                job_id=job_id,
                task_id=dependency_id,
            )
            if ordinary_edit.current(job_id) is not None:
                await ordinary_edit.control_mutation(self, job_id, "return redis.call('SADD',KEYS[1],ARGV[1])", [dependents_key], [task.task_id])
            else:
                await client.sadd(dependents_key, task.task_id)
            await retain_task_state(self, job_id, dependents_key)

    async def _apply_task_stat_deltas(
        self,
        job_id: str,
        *,
        total_delta: int = 0,
        ready_delta: int = 0,
        blocked_delta: int = 0,
        inflight_delta: int = 0,
        deferred_delta: int = 0,
        completed_delta: int = 0,
        failed_delta: int = 0,
        task_type_deltas: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """Atomically apply counter deltas to the cached job task stats.

        Redis ``HINCRBY`` operations are atomic across worker processes. Keeping
        every numeric transition in one transaction avoids the lost updates that
        occur when several workers read and rewrite the whole state hash.
        """

        task_type_counts: dict[str, int] | None = None

        if task_type_deltas:
            stats = await self.get_job_task_stats(job_id)
            task_type_counts = dict(
                cast(dict[str, int], stats.get("task_type_counts", {}))
            )
            for task_type, delta in task_type_deltas.items():
                next_value = task_type_counts.get(task_type, 0) + int(delta)
                if next_value > 0:
                    task_type_counts[task_type] = next_value
                else:
                    task_type_counts.pop(task_type, None)

        field_deltas = {
            "total_tasks": total_delta,
            "ready_tasks": ready_delta,
            "blocked_tasks": blocked_delta,
            "inflight_tasks": inflight_delta,
            "deferred_tasks": deferred_delta,
            "completed_tasks": completed_delta,
            "failed_tasks": failed_delta,
        }
        state_key = self.JOB_STATE_KEY.format(job_id=job_id)
        pipe = self._client().pipeline(transaction=True)
        for field_name, delta in field_deltas.items():
            if delta:
                pipe.hincrby(state_key, field_name, int(delta))

        state_updates: dict[str, Any] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if task_type_counts is not None:
            state_updates["task_type_counts"] = json.dumps(
                task_type_counts,
                sort_keys=True,
            )
        pipe.hset(state_key, mapping=state_updates)
        if ordinary_edit.current(job_id) is not None:
            await ordinary_edit.control_mutation(self, job_id, """
local n = tonumber(ARGV[1])
for i=2,n*2+1,2 do redis.call('HINCRBY',KEYS[1],ARGV[i],ARGV[i+1]) end
for i=n*2+2,#ARGV,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
return 1
""", [state_key], [len(field_deltas), *[part for pair in field_deltas.items() for part in pair], *[part for pair in state_updates.items() for part in pair]])
        else:
            await pipe.execute()

        return await self.get_job_task_stats(job_id)

    async def persist_job(
        self, job: Job, *, persist_tasks: bool = False, runtime_only: bool = False,
        config_only: bool = False, reset_state: bool = False,
    ) -> None:
        """Publish a complete header without crossing an ordinary edit boundary."""
        if job.metadata.get("mps_manifest"):
            from .mps_attempt import persist_job

            await persist_job(self, job)
            if persist_tasks:
                for task in job.tasks:
                    await self.persist_task(job.job_id, task)
            return
        from . import runtime_admission

        config = job.to_config(include_task_ids=False).to_dict()
        payload = runtime_admission.configuration_payload(job.job_id, config)
        header_limits = runtime_admission.policy()
        header_reservation = runtime_admission.validate_payload(job.job_id, payload, header_limits, reserve_metadata=True)
        state = {key: value for key, value in job.to_state().to_dict().items() if value is not None}
        if runtime_only:
            state = {key: value for key, value in state.items() if key in {
                "status", "created_at", "updated_at", "started_at", "completed_at",
            }}
            state.update({key: job.metadata[key] for key in (
                "worker_id", "backend_id", "failure_reason",
            ) if job.metadata.get(key) is not None})
        scheduled_score = ""
        if job.is_scheduled and job.schedule.scheduled_for:
            try:
                scheduled_score = str(datetime.fromisoformat(job.schedule.scheduled_for).timestamp())
            except ValueError:
                logger.warning("Invalid scheduled_for for job %s", job.job_id)
        result = await ordinary_edit.control_mutation(self, job.job_id, runtime_admission.ADMIT_LUA + """
local raw = redis.call('GET',KEYS[1])
if raw then
    local existing = cjson.decode(raw)
    local metadata = existing.metadata or {}
    if ARGV[9] == '' then
        if ARGV[6] == '1' and (metadata.task_definition_id or ARGV[1]) ~= ARGV[2] then return -1 end
        if ARGV[6] ~= '1' and (metadata.config_revision or ARGV[1]) ~= ARGV[3] then return -1 end
    end
end
if ARGV[6] ~= '1' or not raw then
    local admitted = admit_header(KEYS[1], KEYS[3], ARGV[1], ARGV[15], ARGV[12], ARGV[13], ARGV[14])
    if admitted ~= 1 then return admitted end
    redis.call('SET',KEYS[1],ARGV[4])
end
if ARGV[8] == '1' then redis.call('HDEL',KEYS[2],'started_at','completed_at','failure_reason','worker_id') end
if ARGV[7] ~= '1' then
    for key,value in pairs(cjson.decode(ARGV[5])) do redis.call('HSET',KEYS[2],key,value) end
end
redis.call('ZADD',KEYS[3],ARGV[10],ARGV[1])
if ARGV[6] ~= '1' then
    if ARGV[11] ~= '' then redis.call('ZADD',KEYS[4],ARGV[11],ARGV[1])
    else redis.call('ZREM',KEYS[4],ARGV[1]) end
end
return 1
""", [self.JOB_CONFIG_KEY.format(job_id=job.job_id), self.JOB_STATE_KEY.format(job_id=job.job_id), self.JOB_INDEX_KEY, self.SCHEDULED_KEY], [
            job.job_id, retention.definition_id(job.job_id, config), ordinary_edit.revision(job.job_id, config),
            payload, json.dumps(state), int(runtime_only), int(config_only), int(reset_state),
            ordinary_edit.current(job.job_id).token if ordinary_edit.current(job.job_id) is not None else "",
            _utc_timestamp(), scheduled_score, header_limits.count, header_limits.bytes, header_limits.metadata_bytes, header_reservation,
        ])
        if result == -1:
            raise mps_attempt.StaleTaskDefinition(f"Ordinary configuration for {job.job_id} changed")
        if result == -10:
            raise runtime_admission.RuntimeAdmissionDeferred(f"Job {job.job_id} is waiting for runtime capacity")
        if persist_tasks:
            await self.persist_tasks(job.job_id, job.tasks)

    async def persist_task(self, job_id: str, task: Task) -> None:
        """Store task metadata under the job namespace."""

        payload = json.dumps(task.to_dict())
        task_key = self.TASK_KEY.format(job_id=job_id, task_id=task.task_id)
        if task.mps_task_manifest is not None:
            from .mps_attempt import mutate

            await mutate(self, job_id, task.mps_task_manifest, """
                if not redis.call('ZSCORE', KEYS[3], ARGV[2]) then return -1 end
                if redis.call('EXISTS', KEYS[4]) == 0 then return -1 end
                redis.call('SET', KEYS[4], ARGV[3], 'XX', 'KEEPTTL')
                return 1
            """, [self.TASK_INDEX_KEY.format(job_id=job_id), task_key], [task.task_id, payload])
            return
        await ordinary_edit.control_mutation(self, job_id, """
redis.call('SET',KEYS[1],ARGV[1])
redis.call('ZADD',KEYS[2],ARGV[2],ARGV[3])
return 1
""", [task_key, self.TASK_INDEX_KEY.format(job_id=job_id)], [payload, _utc_timestamp(), task.task_id])

    async def persist_tasks(
        self,
        job_id: str,
        tasks: Sequence[Task],
        *,
        batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
    ) -> None:
        """Persist task blobs and index membership in bounded Redis pipelines.

        Large jobs previously paid two network round trips for every task
        (``SET`` followed by ``ZADD``).  Keeping the same split-key layout but
        sending one pipeline per bounded batch makes submission scale with the
        number of batches instead of the number of tasks.
        """

        if not tasks:
            return

        effective_batch_size = max(1, int(batch_size))
        client = self._client()
        task_index_key = self.TASK_INDEX_KEY.format(job_id=job_id)
        for offset in range(0, len(tasks), effective_batch_size):
            batch = tasks[offset : offset + effective_batch_size]
            base_score = _utc_timestamp()
            pipe = client.pipeline(transaction=False)
            task_payloads: dict[str, str] = {}
            index_members: dict[str, float] = {}
            for index, task in enumerate(batch):
                task.job_id = job_id
                task_payloads[
                    self.TASK_KEY.format(job_id=job_id, task_id=task.task_id)
                ] = json.dumps(task.to_dict())
                # Preserve deterministic insertion order inside the batch while
                # retaining the timestamp-based ordering used by existing jobs.
                index_members[task.task_id] = base_score + (index / 1_000_000)
            if ordinary_edit.current(job_id) is not None:
                await ordinary_edit.control_mutation(self, job_id, """
local count = #KEYS - 1
for i=1,count do
    redis.call('SET',KEYS[i+1],ARGV[i])
    redis.call('ZADD',KEYS[1],ARGV[count+i],ARGV[count*2+i])
end
return count
""", [task_index_key, *task_payloads], [*task_payloads.values(), *index_members.values(), *index_members])
            else:
                pipe.mset(task_payloads)
                pipe.zadd(task_index_key, index_members)
                await pipe.execute()

    async def fetch_job(
        self,
        job_id: str,
        *,
        include_tasks: bool = True,
        max_include_tasks: int | None = None,
    ) -> Optional[Job]:
        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        ledger = ExecutionLedger()
        config = await run_database_call(ledger.job, job_id)
        if config is not None:
            stats = await run_database_call(ledger.stats, job_id)
            config["metadata"].update(completed_tasks_count_override=stats["completed"],
                                      failed_tasks_count_override=stats["failed"])
            # Large input graphs stay paged even for legacy inline callers.
            if include_tasks:
                maximum = max_include_tasks or self.DEFAULT_MAX_INCLUDE_TASKS
                if stats["total"] > maximum:
                    raise TaskLoadLimitExceeded(f"Job {job_id} requires paged task reads")
                page = await run_database_call(ledger.task_page, job_id, limit=maximum)
                if page["has_more"]:
                    raise TaskLoadLimitExceeded(f"Job {job_id} exceeds the inline byte budget")
                config["tasks"] = page["tasks"]
            return Job.from_dict(config)
        config_key = self.JOB_CONFIG_KEY.format(job_id=job_id)
        state_key = self.JOB_STATE_KEY.format(job_id=job_id)
        config_data, state_data = (
            await self.redis.get(config_key),
            await self._client().hgetall(state_key),
        )
        if not config_data:
            return None
        try:
            config_blob = json.loads(config_data)
        except json.JSONDecodeError:
            logger.error("Failed to decode job config for %s", job_id)
            return None
        try:
            config = JobConfig.from_dict(
                {
                    "job_id": config_blob.get("job_id", job_id),
                    "name": config_blob.get(
                        "name", config_blob.get("job_type", job_id)
                    ),
                    "created_by": config_blob.get("created_by", "system"),
                    "schedule": config_blob.get("schedule"),
                    "metadata": dict(config_blob.get("metadata") or {}),
                    "task_ids": list(config_blob.get("task_ids") or []),
                }
            )
            decoded_state = {
                (key.decode() if isinstance(key, bytes) else str(key)): (
                    value.decode() if isinstance(value, bytes) else value
                )
                for key, value in state_data.items()
            }
            config.metadata.update({key: decoded_state[key] for key in (
                "worker_id", "backend_id", "failure_reason", "mps_config_pending",
            ) if decoded_state.get(key) is not None})
            if decoded_state.get("mps_config_pending"):
                config.metadata["mps_config_revision"] = int(decoded_state["mps_config_pending"])
                config.metadata["mps_schedule_pending"] = str(decoded_state.get("mps_schedule_pending")) == "1"
                config.schedule.scheduled_for = decoded_state.get("mps_pending_scheduled_for") or None
            state = JobRuntimeState.from_dict(decoded_state)
            job = Job.from_split_data(config, state)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load job %s: %s", job_id, exc)
            return None

        if include_tasks:
            total_tasks = await self.get_total_task_count(job_id)
            limit = max_include_tasks or self.DEFAULT_MAX_INCLUDE_TASKS
            if total_tasks > limit:
                raise TaskLoadLimitExceeded(
                    f"Job {job_id} has {total_tasks} tasks; requested inline limit is {limit}"
                )
            page = await self.fetch_task_page(job_id, cursor="0", limit=limit)
            job.tasks = page.tasks
        return job

    async def fetch_job_with_tasks(
        self,
        job_id: str,
        *,
        include_tasks: bool = True,
        max_include_tasks: int | None = None,
    ) -> Optional[Job]:
        job = await self.fetch_job(
            job_id,
            include_tasks=include_tasks,
            max_include_tasks=max_include_tasks,
        )
        if job is None:
            return None
        if include_tasks:
            limit = max_include_tasks or self.DEFAULT_MAX_INCLUDE_TASKS
            if len(job.tasks) > limit:
                raise TaskLoadLimitExceeded(
                    f"Job {job_id} has {len(job.tasks)} tasks; requested inline limit is {limit}"
                )
            return job

        return Job(
            name=job.name,
            created_by=job.created_by,
            tasks=[],
            job_id=job.job_id,
            state=job.state,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            schedule=job.schedule,
            metadata=dict(job.metadata),
        )

    def task_input_limits(
        self, *, batch_size: int | None = None, max_bytes: int | None = None,
    ) -> tuple[int, int]:
        return (
            _positive_int_env("ULTIMA_JOB_TASK_INPUT_BATCH_SIZE", self.DEFAULT_WRITE_BATCH_SIZE, value=batch_size),
            _positive_int_env("ULTIMA_JOB_TASK_INPUT_MAX_BYTES", self.database_writes.max_command_bytes, value=max_bytes),
        )

    async def fetch_archived_task_page(
        self,
        job_id: str,
        *,
        after_task_id: str | None = None,
        batch_size: int | None = None,
        max_bytes: int | None = None,
    ) -> TaskPageResult:
        """Read stored inputs without loading result history or the whole job.

        This low-level page does not establish a consistent job generation;
        retry publication must verify that ownership separately.
        """
        from ..database.threads import run_database_call

        rows, allowance = self.task_input_limits(batch_size=batch_size, max_bytes=max_bytes)
        return await run_database_call(
            self._fetch_archived_task_page_sync,
            job_id, after_task_id=after_task_id, batch_size=rows, max_bytes=allowance,
        )

    @staticmethod
    def _fetch_archived_task_page_sync(
        job_id: str, *, after_task_id: str | None, batch_size: int, max_bytes: int,
    ) -> TaskPageResult:
        from sqlalchemy import case, func, select
        from ultima_scraper_db_archive.global_db import CentralTaskModel, global_session

        tasks = CentralTaskModel.__table__
        query = (
            select(tasks.c.task_id, tasks.c.status,
                   func.octet_length(tasks.c.definition_json).label("size"))
            .where(tasks.c.job_id == job_id)
            .order_by(tasks.c.task_id)
            .limit(batch_size)
        )
        if after_task_id is not None:
            query = query.where(tasks.c.task_id > after_task_id)
        page = query.cte("task_input_page")
        metered = select(
            page,
            func.sum(func.coalesce(page.c.size, 0)).over(
                order_by=page.c.task_id, rows=(None, 0)
            ).label("read_bytes"),
        ).cte("metered_task_inputs")
        statement = (
            select(metered.c.task_id, metered.c.status, metered.c.size,
                   metered.c.read_bytes,
                   case((metered.c.read_bytes <= max_bytes, tasks.c.definition_json)))
            .join(tasks, tasks.c.task_id == metered.c.task_id)
            .order_by(metered.c.task_id)
        )
        with global_session() as session:
            records = session.execute(statement).all()
        output: list[Task] = []
        for task_id, status, size, read_bytes, raw in records:
            if size is None:
                raise ArchivedTaskInputsUnavailable(f"Archived inputs are unavailable for task {task_id}")
            if read_bytes > max_bytes:
                if not output:
                    raise TaskPayloadBudgetExceeded(task_id, int(size), max_bytes, phase="SQL input read")
                break
            try:
                definition = json.loads(raw)
                if not isinstance(definition, dict) or definition.get("task_id") != task_id or definition.get("job_id") not in (None, job_id):
                    raise ValueError("Stored task identity does not match its archive row")
                task = Task.from_dict(definition)
                task.job_id = job_id
                task.state = TaskState(status)
            except (KeyError, TypeError, ValueError) as exc:
                raise ArchivedTaskInputsUnavailable(f"Archived inputs are invalid for task {task_id}") from exc
            output.append(task)
        has_more = len(output) < len(records) or len(records) == batch_size
        return TaskPageResult(
            tasks=output,
            next_cursor=output[-1].task_id if has_more and output else None,
            has_more=has_more,
            scanned_count=len(records),
        )

    async def fetch_task(self, job_id: str, task_id: str) -> Optional[Task]:
        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        payload = await run_database_call(ExecutionLedger().task, job_id, task_id)
        if payload is not None:
            return Task.from_dict(payload)
        task_key = self.TASK_KEY.format(job_id=job_id, task_id=task_id)
        data = await self.redis.get(task_key)
        return self._decode_task(job_id, task_id, data)

    def _decode_task(
        self,
        job_id: str,
        task_id: str,
        data: Any,
    ) -> Optional[Task]:
        if not data:
            return None
        try:
            blob = json.loads(data)
        except json.JSONDecodeError:
            logger.error("Failed to decode task payload for %s/%s", job_id, task_id)
            return None
        return Task.from_dict(blob)

    async def fetch_tasks_by_ids(
        self,
        job_id: str,
        task_ids: Sequence[str],
    ) -> list[Optional[Task]]:
        """Fetch an ordered task batch in one Redis round trip."""

        if not task_ids:
            return []
        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        ledger = ExecutionLedger()
        if await run_database_call(ledger.job, job_id) is not None:
            output = []
            for task_id in task_ids:
                payload = await run_database_call(ledger.task, job_id, task_id)
                output.append(Task.from_dict(payload) if payload else None)
            return output
        task_keys = [
            self.TASK_KEY.format(job_id=job_id, task_id=task_id) for task_id in task_ids
        ]
        raw_tasks = cast(Sequence[Any], await self._client().mget(task_keys))
        return [
            self._decode_task(job_id, task_id, raw_task)
            for task_id, raw_task in zip(task_ids, raw_tasks, strict=True)
        ]

    async def _read_task_prefix_by_ids(
        self,
        job_id: str,
        task_ids: Sequence[str],
        *,
        max_bytes: int,
    ) -> tuple[list[Any], int]:
        """Read only a fitting prefix, checking byte lengths atomically with GET."""
        keys = [
            self.TASK_KEY.format(job_id=job_id, task_id=task_id) for task_id in task_ids
        ]
        if not keys:
            return [], 0
        size, overflow, raw_tasks = await self._client().eval(
            _TASK_PREFIX_WITHIN_BYTES,
            len(keys),
            *keys,
            max_bytes,
        )
        if not raw_tasks and overflow:
            raise TaskPayloadBudgetExceeded(task_ids[0], int(overflow), max_bytes)
        return raw_tasks, int(size)

    async def _fetch_task_prefix_by_ids(
        self, job_id: str, task_ids: Sequence[str], *, max_bytes: int,
    ) -> tuple[list[Task | None], int]:
        raw_tasks, size = await self._read_task_prefix_by_ids(
            job_id, task_ids, max_bytes=max_bytes
        )
        return [
            self._decode_task(job_id, task_id, raw_task)
            for task_id, raw_task in zip(
                task_ids[: len(raw_tasks)], raw_tasks, strict=True
            )
        ], int(size)

    async def reserve_dedup(self, task: Task, ttl_seconds: int = 3600) -> bool:
        """Reserve a dedupe key for a task.

        Returns ``True`` when the reservation succeeds. ``False`` indicates that a
        conflicting task reservation already exists.
        """

        key = self.DEDUPE_KEY.format(dedup=task.dedup_key)
        if task.mps_task_manifest is not None:
            from .mps_attempt import mutate

            return bool(await mutate(self, task.job_id, task.mps_task_manifest, """
                if not redis.call('ZSCORE', KEYS[4], ARGV[2]) then return -1 end
                local owner = redis.call('GET', KEYS[3])
                if owner and owner ~= ARGV[2] then return 0 end
                redis.call('SET', KEYS[3], ARGV[2], 'EX', ARGV[3])
                return 1
            """, [key, self.TASK_INDEX_KEY.format(job_id=task.job_id)], [task.task_id, ttl_seconds]))
        if await self.redis.set(key, task.task_id, nx=True, ex=ttl_seconds):
            return True
        # Materialization reserves tasks before a worker claims them. Renew
        # only this task's reservation; a different owner remains a collision.
        return bool(await self._client().eval(
            recovery_scripts.COMPARE_EXPIRE, 1, key, task.task_id, ttl_seconds
        ))

    async def release_dedup(self, task: Task) -> None:
        key = self.DEDUPE_KEY.format(dedup=task.dedup_key)
        if task.mps_task_manifest is not None:
            # A replaced task can share both task ID and dedup key. The owner
            # comparison alone is insufficient after that definition changes.
            await self._client().eval("""
                if redis.call('HGET', KEYS[1], 'mps_manifest') ~= '1' then return 0 end
                if (redis.call('HGET', KEYS[1], 'mps_task_manifest') or '') ~= ARGV[1] then return 0 end
                if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
                return redis.call('DEL', KEYS[2])
            """, 2, self.JOB_STATE_KEY.format(job_id=task.job_id), key,
                task.mps_task_manifest, task.task_id)
            return
        await ordinary_edit.release_reservation(self, key, task.task_id)

    async def release_dedup_for_job(self, job: Job) -> None:
        if job.tasks:
            for task in job.tasks:
                await self.release_dedup(task)
                dedup_name = getattr(task, "dedup_key", None)
                if dedup_name:
                    try:
                        await self.requeue_deferred_tasks_for_dedup(
                            dedup_name, default_backend_id=getattr(job, "backend_id", None)
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.debug(
                            "Failed to requeue deferred tasks for %s: %s", dedup_name, exc
                        )
            return
        await self.release_dedup_for_job_id(
            job.job_id, default_backend_id=getattr(job, "backend_id", None)
        )

    async def release_dedup_for_job_id(
        self,
        job_id: str,
        *,
        batch_size: int | None = None,
        max_bytes: int | None = None,
        default_backend_id: str | None = None,
    ) -> None:
        """Release dedupe reservations for every task in a job in batches and wake deferred tasks."""

        client = self._client()
        if default_backend_id is None:
            backend_raw = await client.hget(
                self.JOB_STATE_KEY.format(job_id=job_id), "backend_id"
            )
            default_backend_id = _decode_redis_text(backend_raw)
        offset = 0
        batch_size, max_bytes = self.task_archive_limits(batch_size=batch_size, max_bytes=max_bytes)

        while True:
            task_ids = await self.list_task_ids_window(
                job_id,
                offset=offset,
                limit=batch_size,
            )
            if not task_ids:
                break

            raw_tasks, _ = await self._read_task_prefix_by_ids(
                job_id, task_ids, max_bytes=max_bytes
            )
            consumed = len(raw_tasks)
            dedup_key_to_task_ids: dict[str, set[str]] = {}

            for task_id, raw_task in zip(task_ids, raw_tasks):
                raw_payload = _decode_redis_text(raw_task)
                if not raw_payload:
                    continue
                try:
                    task_blob = cast(dict[str, Any], json.loads(raw_payload))
                except json.JSONDecodeError:
                    logger.debug(
                        "Failed to decode task payload while releasing dedup for %s/%s",
                        job_id,
                        task_id,
                    )
                    continue

                dedup_name = self._dedup_name_from_task_blob(task_blob)
                if not dedup_name:
                    continue

                dedup_key = self.DEDUPE_KEY.format(dedup=dedup_name)
                dedup_key_to_task_ids.setdefault(dedup_key, set()).add(task_id)

            if dedup_key_to_task_ids:
                dedup_keys = list(dedup_key_to_task_ids)
                owners = cast(Sequence[Any], await client.mget(dedup_keys))
                owned = [
                    (dedup_key, owner_text)
                    for dedup_key, owner in zip(dedup_keys, owners)
                    if (owner_text := _decode_redis_text(owner))
                    and owner_text in dedup_key_to_task_ids[dedup_key]
                ]
                if owned:
                    await mps_attempt.control_mutation(
                        self, job_id,
                        """
                        local removed = 0
                        for i = 1, #KEYS do
                            if redis.call('GET', KEYS[i]) == ARGV[i] then
                                removed = removed + redis.call('DEL', KEYS[i])
                            end
                        end
                        return removed
                        """,
                        [key for key, _ in owned], [owner for _, owner in owned],
                    )
                    for key, _ in owned:
                        dedup_name = key.removeprefix(self.DEDUPE_KEY.format(dedup=""))
                        try:
                            await self.requeue_deferred_tasks_for_dedup(
                                dedup_name,
                                default_backend_id=default_backend_id,
                            )
                        except Exception as exc:  # noqa: BLE001
                            logger.debug(
                                "Failed to requeue deferred tasks for %s: %s",
                                dedup_name,
                                exc,
                            )

            if len(task_ids) < batch_size and consumed == len(task_ids):
                break
            offset += consumed

    async def reserve_dedup_many(
        self,
        tasks: Sequence[Task],
        ttl_seconds: int = 3600,
        *,
        batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
    ) -> list[str]:
        """Reserve dedupe keys in pipelines and return successful task IDs.

        A reservation already owned by the same deterministic task ID is also
        successful.  That makes interrupted background materialization safe to
        resume after its last durable cursor without manufacturing a collision
        against its own earlier write.
        """

        if not tasks:
            return []

        effective_batch_size = max(1, int(batch_size))
        client = self._client()
        reserved: list[str] = []
        for offset in range(0, len(tasks), effective_batch_size):
            batch = tasks[offset : offset + effective_batch_size]
            keys = [self.DEDUPE_KEY.format(dedup=task.dedup_key) for task in batch]
            pipe = client.pipeline(transaction=False)
            for key, task in zip(keys, batch, strict=True):
                pipe.set(key, task.task_id, nx=True, ex=ttl_seconds)
            results = cast(Sequence[Any], await pipe.execute())

            unresolved_indices: list[int] = []
            for index, (task, result) in enumerate(zip(batch, results, strict=True)):
                if result:
                    reserved.append(task.task_id)
                else:
                    unresolved_indices.append(index)

            if not unresolved_indices:
                continue

            owners = cast(
                Sequence[Any],
                await client.mget([keys[index] for index in unresolved_indices]),
            )
            for index, owner in zip(unresolved_indices, owners, strict=True):
                if _decode_redis_text(owner) == batch[index].task_id:
                    reserved.append(batch[index].task_id)

        return reserved

    async def register_deferred_tasks(
        self,
        job_id: str,
        tasks: Sequence[Task],
        *,
        batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
        pipeline: Any | None = None,
    ) -> None:
        """Register already-persisted dedupe collisions in bounded pipelines."""

        deferred_tasks = [
            task
            for task in tasks
            if task.state == TaskState.DEFERRED and task.deferred_dedup_key
        ]
        if not deferred_tasks:
            return

        effective_batch_size = max(1, int(batch_size))
        client = self._client()
        outputs_key = self.TASK_OUTPUTS_KEY.format(job_id=job_id)
        for offset in range(0, len(deferred_tasks), effective_batch_size):
            batch = deferred_tasks[offset : offset + effective_batch_size]
            pipe = pipeline if pipeline is not None else client.pipeline(transaction=False)
            for task in batch:
                dedup_key = str(task.deferred_dedup_key)
                deferred_key = self.DEFERRED_TASKS_KEY.format(dedup_key=dedup_key)
                pipe.sadd(deferred_key, f"{job_id}:{task.task_id}")
                await retain_deferred(self, job_id, deferred_key, pipe=pipe)
                pipe.hset(
                    outputs_key,
                    task.task_id,
                    json.dumps(
                        {
                            "state": "deferred",
                            "reason": "dedup_conflict",
                            "dedup_key": dedup_key,
                        }
                    ),
                )
            await retain_task_state(self, job_id, outputs_key, pipe=pipe)
            if pipeline is None:
                await pipe.execute()

    async def list_job_ids(self) -> list[str]:
        raw_ids = await self._client().zrange(self.JOB_INDEX_KEY, 0, -1)
        ids = cast(Sequence[Any], raw_ids)
        normalized_ids: list[str] = []
        for item in ids:
            if isinstance(item, bytes):
                normalized_ids.append(item.decode())
            else:
                normalized_ids.append(str(item))
        return normalized_ids

    async def list_task_ids_window(
        self,
        job_id: str,
        *,
        offset: int = 0,
        limit: int = 100,
        reverse: bool = False,
    ) -> list[str]:
        """Return a bounded slice of task IDs for a job."""

        if limit <= 0:
            return []
        index_key = self.TASK_INDEX_KEY.format(job_id=job_id)
        if reverse:
            raw_tasks = await self._client().zrevrange(
                index_key,
                offset,
                offset + limit - 1,
            )
        else:
            raw_tasks = await self._client().zrange(
                index_key,
                offset,
                offset + limit - 1,
            )
        task_ids = cast(Sequence[Any], raw_tasks)
        normalized: list[str] = []
        for item in task_ids:
            if isinstance(item, bytes):
                normalized.append(item.decode())
            else:
                normalized.append(str(item))
        return normalized

    def _parse_task_cursor(self, cursor: str | None) -> int:
        if not cursor:
            return 0
        try:
            parsed = int(cursor)
        except (TypeError, ValueError):
            return 0
        return max(parsed, 0)

    def _task_matches_filters(
        self,
        task: Task,
        *,
        state: str | None = None,
        task_type: str | None = None,
        error_only: bool = False,
    ) -> bool:
        normalized_state = state.lower() if state else None
        if normalized_state and task.state.value.lower() != normalized_state:
            return False

        normalized_task_type = task_type.lower() if task_type else None
        if (
            normalized_task_type
            and task.task_type.value.lower() != normalized_task_type
        ):
            return False

        if error_only:
            if task.state == TaskState.FAILED:
                return True
            if task.result and task.result.errors:
                return True
            if (
                task.result
                and task.result.message
                and task.state == TaskState.CANCELLED
            ):
                return True
            return False

        return True

    async def fetch_task_page(
        self,
        job_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
        state: str | None = None,
        task_type: str | None = None,
        error_only: bool = False,
        max_bytes: int | None = None,
    ) -> TaskPageResult:
        """Fetch a bounded page of tasks using the task index as the cursor source."""

        if limit <= 0:
            return TaskPageResult(
                tasks=[], next_cursor=None, has_more=False, scanned_count=0
            )

        if max_bytes is not None:
            max_bytes = _positive_int_env("Task page max_bytes", 1, value=max_bytes)
        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        ledger = ExecutionLedger()
        if await run_database_call(ledger.job, job_id) is not None:
            after = cursor.removeprefix("ledger:") if cursor and cursor.startswith("ledger:") else ""
            page = await run_database_call(ledger.task_page, job_id, after=after,
                offset=0 if after else self._parse_task_cursor(cursor), limit=limit, max_bytes=max_bytes)
            tasks = [Task.from_dict(payload) for payload in page["tasks"]]
            return TaskPageResult(**{**page, "tasks": [task for task in tasks if self._task_matches_filters(
                task, state=state, task_type=task_type, error_only=error_only)]})
        read_bytes = 0
        total_tasks = await self.get_total_task_count(job_id)
        scan_offset = self._parse_task_cursor(cursor)
        if scan_offset >= total_tasks:
            return TaskPageResult(
                tasks=[], next_cursor=None, has_more=False, scanned_count=scan_offset
            )

        matched: list[Task] = []
        index_exhausted = False
        normalized_state = state.strip().lower() if state else None
        normalized_task_type = task_type.strip().lower() if task_type else None
        has_filters = bool(normalized_state or normalized_task_type or error_only)
        scan_limit = (
            max(limit, limit * self.TASK_PAGE_SCAN_MULTIPLIER) if has_filters else limit
        )
        configured_scan_budget = runtime_env_value("ULTIMA_TASK_PAGE_SCAN_ENTRIES")
        if configured_scan_budget is None or not configured_scan_budget.strip():
            scan_budget = scan_limit
        else:
            try:
                scan_budget = int(configured_scan_budget)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "ULTIMA_TASK_PAGE_SCAN_ENTRIES must be a positive integer"
                ) from exc
            if scan_budget <= 0:
                raise ValueError(
                    "ULTIMA_TASK_PAGE_SCAN_ENTRIES must be a positive integer"
                )
        initial_offset = scan_offset
        # Running and terminal tasks are re-persisted with a fresh task-index
        # score as they execute, so they cluster at the recent edge. Sparse
        # filters must start there instead of loading every untouched task in a
        # large job before reaching the first match.
        reverse_scan = error_only or normalized_state in {
            TaskState.RUNNING.value,
            TaskState.COMPLETED.value,
            TaskState.FAILED.value,
            TaskState.CANCELLED.value,
        }

        while (
            len(matched) < limit
            and scan_offset < total_tasks
            and scan_offset - initial_offset < scan_budget
        ):
            read_limit = min(scan_limit, scan_budget - (scan_offset - initial_offset))
            batch_ids = await self.list_task_ids_window(
                job_id,
                offset=scan_offset,
                limit=read_limit,
                reverse=reverse_scan,
            )
            if not batch_ids:
                # Task index is empty or truncated (e.g. Redis task hashes were
                # purged while the state hash still claims total_tasks > 0).
                # Signal exhaustion so callers can fall through to DB/EventLog
                # fallbacks instead of paging forever against an empty index.
                index_exhausted = True
                break

            if max_bytes is None:
                fetched_tasks = await self.fetch_tasks_by_ids(job_id, batch_ids)
            else:
                try:
                    fetched_tasks, transferred = await self._fetch_task_prefix_by_ids(
                        job_id,
                        batch_ids,
                        max_bytes=max_bytes - read_bytes,
                    )
                except TaskPayloadBudgetExceeded:
                    if read_bytes:
                        break  # The next page gets a fresh allowance.
                    raise
                read_bytes += transferred
            consumed = 0
            for task in fetched_tasks:
                consumed += 1
                if task is None:
                    continue
                if self._task_matches_filters(
                    task,
                    state=state,
                    task_type=task_type,
                    error_only=error_only,
                ):
                    matched.append(task)
                    if len(matched) >= limit:
                        break

            scan_offset += consumed
            if max_bytes is not None and (
                read_bytes >= max_bytes or len(fetched_tasks) < len(batch_ids)
            ):
                break
            if len(batch_ids) < read_limit and consumed >= len(batch_ids):
                break

        has_more = not index_exhausted and scan_offset < total_tasks
        next_cursor = str(scan_offset) if has_more else None
        return TaskPageResult(
            tasks=matched,
            next_cursor=next_cursor,
            has_more=has_more,
            scanned_count=scan_offset,
        )

    async def fetch_tasks_stream(
        self,
        job_id: str,
        batch_size: int = 100,
        *,
        cursor: str | None = None,
        state: str | None = None,
        task_type: str | None = None,
        error_only: bool = False,
        max_bytes: int | None = None,
    ) -> AsyncIterator[Task]:
        """Stream tasks for a job in batches."""

        next_cursor: str | None = cursor or "0"
        while True:
            page = await self.fetch_task_page(
                job_id,
                cursor=next_cursor,
                limit=batch_size,
                state=state,
                task_type=task_type,
                error_only=error_only,
                max_bytes=max_bytes,
            )
            for task in page.tasks:
                yield task
            if not page.next_cursor:
                break
            next_cursor = page.next_cursor

    async def get_total_task_count(self, job_id: str) -> int:
        """Return the persisted total task count for a job."""

        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        stats = await run_database_call(ExecutionLedger().stats, job_id)
        if stats is not None:
            return stats["total"]
        state_key = self.JOB_STATE_KEY.format(job_id=job_id)
        indexed_count = 0
        try:
            indexed_count = int(
                await self._client().zcard(self.TASK_INDEX_KEY.format(job_id=job_id))
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Failed to read indexed task count for job %s: %s", job_id, exc
            )

        total_raw = await self._client().hget(state_key, "total_tasks")
        if total_raw is not None:
            try:
                return max(int(total_raw), indexed_count)
            except (TypeError, ValueError):
                logger.debug(
                    "Invalid total_tasks value for job %s: %r", job_id, total_raw
                )

        return indexed_count

    async def get_ready_task_count(self, job_id: str) -> int:
        """Return the number of ready tasks currently queued for a job."""

        ready_key = self.TASKS_READY_KEY.format(job_id=job_id)
        return int(await self._client().scard(ready_key))

    async def get_job_task_stats(self, job_id: str) -> dict[str, Any]:
        """Return task-state counters, using live state sets for active jobs."""

        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        stats = await run_database_call(ExecutionLedger().stats, job_id)
        if stats is not None:
            return stats

        state_key = self.JOB_STATE_KEY.format(job_id=job_id)
        raw_state = await self._client().hgetall(state_key)
        decoded_state = {
            (key.decode() if isinstance(key, bytes) else str(key)): (
                value.decode() if isinstance(value, bytes) else str(value)
            )
            for key, value in raw_state.items()
        }

        def _as_int(key: str) -> int:
            raw_value = decoded_state.get(key)
            if raw_value is None:
                return 0
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return 0

        if decoded_state.get('mps_manifest') == '1':
            from .mps_state import task_stats

            return {**await task_stats(self, job_id), 'mps_manifest': True}

        type_counts_raw = decoded_state.get("task_type_counts")
        task_type_counts: dict[str, int] = {}
        if type_counts_raw:
            try:
                parsed = cast(dict[str, Any], json.loads(type_counts_raw))
            except json.JSONDecodeError:
                parsed = {}
            task_type_counts = {
                str(key): int(value)
                for key, value in parsed.items()
                if isinstance(value, (int, str))
            }

        stats: dict[str, Any] = {
            "total": _as_int("total_tasks"),
            "ready": _as_int("ready_tasks"),
            "blocked": _as_int("blocked_tasks"),
            "inflight": _as_int("inflight_tasks"),
            "deferred": _as_int("deferred_tasks"),
            "completed": _as_int("completed_tasks"),
            "failed": _as_int("failed_tasks"),
            "task_type_counts": task_type_counts,
        }

        if decoded_state.get("status") in {
            JobState.PENDING.value,
            JobState.RUNNING.value,
        }:
            client = self._client()
            pipe = client.pipeline(transaction=False)
            pipe.exists(self.TASK_INDEX_KEY.format(job_id=job_id))
            pipe.zcard(self.TASK_INDEX_KEY.format(job_id=job_id))
            pipe.scard(self.TASKS_READY_KEY.format(job_id=job_id))
            pipe.scard(self.TASKS_BLOCKED_KEY.format(job_id=job_id))
            pipe.scard(self.TASKS_INFLIGHT_KEY.format(job_id=job_id))
            pipe.scard(self.TASKS_COMPLETED_KEY.format(job_id=job_id))
            (
                task_index_exists,
                total,
                ready,
                blocked,
                inflight,
                completed,
            ) = await pipe.execute()
            stats.update(
                total=int(total) if task_index_exists else int(stats["total"]),
                ready=int(ready),
                blocked=int(blocked),
                inflight=int(inflight),
                completed=int(completed),
            )

        return stats

    async def _write_task_stats(
        self,
        job_id: str,
        *,
        total: int | None = None,
        ready: int | None = None,
        blocked: int | None = None,
        inflight: int | None = None,
        deferred: int | None = None,
        completed: int | None = None,
        failed: int | None = None,
        task_type_counts: dict[str, int] | None = None,
    ) -> None:
        """Persist task-state counters in the job state hash."""

        mapping: dict[str, Any] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if total is not None:
            mapping["total_tasks"] = total
        if ready is not None:
            mapping["ready_tasks"] = ready
        if blocked is not None:
            mapping["blocked_tasks"] = blocked
        if inflight is not None:
            mapping["inflight_tasks"] = inflight
        if deferred is not None:
            mapping["deferred_tasks"] = deferred
        if completed is not None:
            mapping["completed_tasks"] = completed
        if failed is not None:
            mapping["failed_tasks"] = failed
        if task_type_counts is not None:
            mapping["task_type_counts"] = json.dumps(task_type_counts, sort_keys=True)
        if mps_attempt.current(job_id) is not None or retention.current(job_id) is not None or ordinary_edit.current(job_id) is not None:
            await mps_attempt.control_mutation(
                self, job_id,
                "for i = 1, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end return 1",
                [self.JOB_STATE_KEY.format(job_id=job_id)],
                [item for pair in mapping.items() for item in pair],
            )
        else:
            await self._client().hset(
                self.JOB_STATE_KEY.format(job_id=job_id), mapping=mapping
            )

    async def get_job_priority(self, job_id: str) -> int:
        """Return the persisted priority hint for a job."""

        raw_config = await self.redis.get(self.JOB_CONFIG_KEY.format(job_id=job_id))
        if not raw_config:
            return 0
        try:
            config_blob = cast(dict[str, Any], json.loads(raw_config))
        except json.JSONDecodeError:
            logger.debug("Invalid config payload while reading priority for %s", job_id)
            return 0

        metadata = config_blob.get("metadata")
        if not isinstance(metadata, dict):
            return 0
        typed_metadata = cast(dict[str, Any], metadata)

        priority = typed_metadata.get("priority", 0)
        try:
            return int(priority)
        except (TypeError, ValueError):
            return 0

    async def iter_ready_task_ids(
        self,
        job_id: str,
        *,
        batch_size: int = 1000,
    ) -> AsyncIterator[list[str]]:
        """Yield ready task IDs for a job in bounded batches."""

        ready_key = self.TASKS_READY_KEY.format(job_id=job_id)
        cursor = 0
        while True:
            cursor, members = await self._client().sscan(
                ready_key,
                cursor=cursor,
                count=batch_size,
            )
            normalized = [
                member.decode() if isinstance(member, bytes) else str(member)
                for member in cast(Sequence[Any], members)
            ]
            if normalized:
                yield sorted(normalized)
            if cursor == 0:
                break

    async def delete_job(self, job_id: str) -> None:
        """Remove runtime artifacts in bounded batches for the observed definition.

        Durable deletion belongs to the API transaction. Retention may use this
        operation after archiving while keeping the SQL history intact.
        """
        identity = mps_attempt.current(job_id)
        if identity is None:
            state = await self._client().hmget(
                self.JOB_STATE_KEY.format(job_id=job_id),
                "mps_manifest", "mps_task_manifest",
            )
            if _decode_redis_text(state[0]) == "1":
                identity = mps_attempt.Identity(
                    job_id, "", _decode_redis_text(state[1]) or ""
                )
        with mps_attempt.scope(identity):
            await self._delete_job_runtime(job_id)

    async def _delete_job_runtime(self, job_id: str) -> None:
        from .event_log import EventLog
        from . import mps_state
        from . import runtime_admission
        from .routing import BackendRouter

        client = self._client()
        state_key = self.JOB_STATE_KEY.format(job_id=job_id)
        backend_id = _decode_redis_text(await client.hget(state_key, "backend_id"))
        router = BackendRouter(self.redis)
        await router.clear_job_routing(job_id=job_id, backend_id=backend_id)
        # Repair stale assignments for known backends; avoid scanning the entire keyspace.
        known_backends: set[str] = set()
        if backend_id:
            known_backends.add(backend_id)
        try:
            raw_backends = await client.zrange(router.LOAD_KEY, 0, -1)
            for b in raw_backends:
                b_str = _decode_redis_text(b)
                if b_str:
                    known_backends.add(b_str)
        except Exception:
            pass

        if known_backends:
            for b in known_backends:
                claimed_key = f"{router.CLAIMED_JOBS_PREFIX}{b}"
                active_key = f"{router.BACKEND_ACTIVE_JOBS_PREFIX}{b}"
                await mps_attempt.control_mutation(
                    self, job_id,
                    """
                    redis.call('LREM', KEYS[1], 0, ARGV[1])
                    redis.call('SREM', KEYS[2], ARGV[1])
                    return 1
                    """,
                    [claimed_key, active_key],
                    [job_id],
                )
        else:
            # Fall back to bounded scan only when backend identities cannot be resolved
            for pattern, operation in (
                (f"{router.CLAIMED_JOBS_PREFIX}*", "LREM"),
                (f"{router.BACKEND_ACTIVE_JOBS_PREFIX}*", "SREM"),
            ):
                async for key in client.scan_iter(match=pattern, count=self._recovery_scan_count(100)):
                    script = (
                        "return redis.call('LREM', KEYS[1], 0, ARGV[1])"
                        if operation == "LREM"
                        else "return redis.call('SREM', KEYS[1], ARGV[1])"
                    )
                    await mps_attempt.control_mutation(self, job_id, script, [key], [job_id])
        await self.release_dedup_for_job_id(job_id)
        await self.clear_task_state(job_id, reset_stats=False)
        task_index = self.TASK_INDEX_KEY.format(job_id=job_id)
        batch_size = self._recovery_scan_count(self.DEFAULT_WRITE_BATCH_SIZE)
        while True:
            task_ids = await self.list_task_ids_window(job_id, offset=0, limit=batch_size)
            if not task_ids:
                break
            keys = [task_index]
            for task_id in task_ids:
                keys.extend((
                    self.TASK_KEY.format(job_id=job_id, task_id=task_id),
                    self.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id=task_id),
                    self.TASK_EXEC_LOCK_KEY.format(task_id=task_id),
                    f"{KEY_PREFIX}:view:task:{task_id}:state",
                ))
            await mps_attempt.control_mutation(
                self, job_id,
                """
                for i = 2, #KEYS do redis.call('DEL', KEYS[i]) end
                for i = 1, #ARGV do redis.call('ZREM', KEYS[1], ARGV[i]) end
                return #ARGV
                """, keys, task_ids,
            )
        await mps_state.release_budget(client, job_id, storage=self)
        await mps_attempt.control_mutation(
            self, job_id,
            runtime_admission.RELEASE_LUA + """
            redis.call('ZREM', KEYS[1], ARGV[1])
            redis.call('ZREM', KEYS[2], ARGV[1])
            redis.call('SREM', KEYS[3], ARGV[1])
            redis.call('SREM', KEYS[4], ARGV[1])
            for i = 5, 7 do redis.call('LREM', KEYS[i], 0, ARGV[1]) end
            for i = 8, #KEYS do redis.call('DEL', KEYS[i]) end
            release_header(ARGV[1])
            return 1
            """,
            [self.JOB_INDEX_KEY, self.SCHEDULED_KEY,
             mps_state.SCHEDULED_KEY, mps_state.WAITING_SET_KEY,
             mps_state.QUEUE_KEY, mps_state.PROCESSING_KEY, mps_state.WAITING_KEY,
             task_index, self.JOB_CONFIG_KEY.format(job_id=job_id), state_key,
             self.JOB_RUNTIME_TELEMETRY_KEY.format(job_id=job_id),
             self.JOB_RUNTIME_WORKERS_KEY.format(job_id=job_id),
             self.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id),
             self.JOB_STATE_LOCK_KEY.format(job_id=job_id),
             self.JOB_ROUTING_LOCK_KEY.format(job_id=job_id),
             self.RECENT_TASK_FAILURES_KEY.format(job_id=job_id),
             mps_state.PLAN_KEY.format(job_id=job_id),
             mps_state.FAILED_KEY.format(job_id=job_id),
             EventLog.STREAM_KEY_FORMAT.format(job_id=job_id)],
            [job_id],
        )

    async def remove_scheduled(self, job_id: str) -> None:
        if mps_attempt.current(job_id) is not None:
            await mps_attempt.control_mutation(
                self, job_id, "return redis.call('ZREM', KEYS[1], ARGV[1])",
                [self.SCHEDULED_KEY], [job_id],
            )
        else:
            await self.redis.client.zrem(self.SCHEDULED_KEY, job_id)

    # ------------------------------------------------------------------
    # Task readiness state helpers (ready → in-flight → completed)
    # ------------------------------------------------------------------

    async def initialize_task_state_from_storage(
        self,
        job_id: str,
        *,
        return_ready_task_ids: bool = True,
        batch_size: int = 1000,
        max_bytes: int | None = None,
    ) -> list[str]:
        """Initialize ready/blocked sets by streaming tasks from Redis."""

        ready_key = self.TASKS_READY_KEY.format(job_id=job_id)
        blocked_key = self.TASKS_BLOCKED_KEY.format(job_id=job_id)
        completed_key = self.TASKS_COMPLETED_KEY.format(job_id=job_id)
        inflight_key = self.TASKS_INFLIGHT_KEY.format(job_id=job_id)
        outputs_key = self.TASK_OUTPUTS_KEY.format(job_id=job_id)

        await mps_attempt.control_mutation(self, job_id,
            "for i=1,#KEYS do redis.call('DEL',KEYS[i]) end return #KEYS",
            [ready_key, blocked_key, completed_key, inflight_key, outputs_key], [])

        async def add(key: str, members: list[str]) -> None:
            await mps_attempt.control_mutation(self, job_id,
                "for i=1,#ARGV do redis.call('SADD',KEYS[1],ARGV[i]) end return #ARGV",
                [key], members)
        await self._clear_dependency_index(job_id)

        ready_tasks: list[str] = [] if return_ready_task_ids else []
        ready_batch: list[str] = []
        blocked_batch: list[str] = []
        ready_count = 0
        blocked_count = 0
        total_count = 0
        task_type_counts: dict[str, int] = {}

        async for task in self.fetch_tasks_stream(job_id, batch_size=batch_size, max_bytes=max_bytes):
            total_count += 1
            await self._index_task_dependencies(job_id, task)
            task_type_counts[task.task_type.value] = (
                task_type_counts.get(task.task_type.value, 0) + 1
            )
            if task.depends_on:
                blocked_count += 1
                blocked_batch.append(task.task_id)
                if len(blocked_batch) >= batch_size:
                    await add(blocked_key, blocked_batch)
                    blocked_batch.clear()
            else:
                ready_count += 1
                ready_batch.append(task.task_id)
                if return_ready_task_ids:
                    ready_tasks.append(task.task_id)
                if len(ready_batch) >= batch_size:
                    await add(ready_key, ready_batch)
                    ready_batch.clear()

        if ready_batch:
            await add(ready_key, ready_batch)
        if blocked_batch:
            await add(blocked_key, blocked_batch)

        for key in (ready_key, blocked_key, completed_key, inflight_key, outputs_key):
            await retain_task_state(self, job_id, key)

        await self._write_task_stats(
            job_id,
            total=total_count,
            ready=ready_count,
            blocked=blocked_count,
            inflight=0,
            deferred=0,
            completed=0,
            failed=0,
            task_type_counts=task_type_counts,
        )
        return ready_tasks

    async def get_task_output(self, job_id: str, task_id: str) -> Any:
        outputs_key = self.TASK_OUTPUTS_KEY.format(job_id=job_id)
        client = self._client()
        data = await client.hget(outputs_key, task_id)
        if data is None:
            return None
        raw = data.decode() if isinstance(data, bytes) else data
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    async def clear_task_state(self, job_id: str, *, reset_stats: bool = True) -> None:
        identity = mps_attempt.current(job_id)
        if identity is not None:
            await mps_attempt.assert_current(self, job_id, identity.manifest_id)
        client = self._client()
        if await client.hget(self.JOB_STATE_KEY.format(job_id=job_id), 'mps_manifest') in (b'1', '1'):
            from .mps_state import clear_runtime

            await clear_runtime(self, job_id, reset_stats=reset_stats)
            await self._clear_dependency_index(job_id)
            await self._clear_job_deferred_members(job_id)
            return
        preserved_stats: dict[str, Any] | None = None
        if not reset_stats:
            cached_stats = await self.get_job_task_stats(job_id)
            completed_key = self.TASKS_COMPLETED_KEY.format(job_id=job_id)
            state_key = self.JOB_STATE_KEY.format(job_id=job_id)
            snapshot_pipe = client.pipeline(transaction=False)
            snapshot_pipe.hget(state_key, "status")
            snapshot_pipe.exists(self.TASK_INDEX_KEY.format(job_id=job_id))
            snapshot_pipe.zcard(self.TASK_INDEX_KEY.format(job_id=job_id))
            snapshot_pipe.exists(completed_key)
            snapshot_pipe.scard(completed_key)
            (
                raw_status,
                task_index_exists,
                total,
                completed_exists,
                completed,
            ) = await snapshot_pipe.execute()
            status = _decode_redis_text(raw_status)
            if status in {
                JobState.COMPLETED.value,
                JobState.FAILED.value,
                JobState.CANCELLED.value,
            }:
                preserved_stats = {
                    **cached_stats,
                    "total": (
                        int(total) if task_index_exists else int(cached_stats["total"])
                    ),
                    "ready": 0,
                    "blocked": 0,
                    "inflight": 0,
                    "deferred": 0,
                    "completed": (
                        int(completed)
                        if completed_exists
                        else int(cached_stats["completed"])
                    ),
                }
            else:
                preserved_stats = cached_stats

        state_keys = [
            self.TASKS_READY_KEY.format(job_id=job_id),
            self.TASKS_BLOCKED_KEY.format(job_id=job_id),
            self.TASKS_COMPLETED_KEY.format(job_id=job_id),
            self.TASKS_INFLIGHT_KEY.format(job_id=job_id),
            self.RECENT_TASK_FAILURES_KEY.format(job_id=job_id),
            self.TASK_OUTPUTS_KEY.format(job_id=job_id),
        ]
        await mps_attempt.control_mutation(
            self, job_id,
            "for i = 1, #KEYS do redis.call('DEL', KEYS[i]) end return 1",
            state_keys, [],
        )
        await self._clear_dependency_index(job_id)
        await self._clear_job_deferred_members(job_id)
        snapshot = retention.current(job_id)
        if snapshot is not None and not snapshot.preserve_header:
            # The archived header is removed at the end of retirement. Updating
            # its counters here would invalidate our own timestamp snapshot.
            return
        if reset_stats:
            await self._write_task_stats(
                job_id,
                ready=0,
                blocked=0,
                inflight=0,
                deferred=0,
                completed=0,
                failed=0,
            )
        elif preserved_stats is not None:
            await self._write_task_stats(
                job_id,
                total=int(preserved_stats["total"]),
                ready=int(preserved_stats["ready"]),
                blocked=int(preserved_stats["blocked"]),
                inflight=int(preserved_stats["inflight"]),
                deferred=int(preserved_stats["deferred"]),
                completed=int(preserved_stats["completed"]),
                failed=int(preserved_stats["failed"]),
                task_type_counts=cast(
                    dict[str, int],
                    preserved_stats["task_type_counts"],
                ),
            )

    @staticmethod
    def _dedup_name_from_task_blob(task_blob: dict[str, Any]) -> str | None:
        task_type = str(task_blob.get("task_type") or "").strip()
        if not task_type:
            return None

        return (
            f"task:{task_type}:{task_blob.get('site_name', '')}:"
            f"{task_blob.get('user_id')}:{task_blob.get('content_type') or ''}:"
            f"{task_blob.get('auth_id') or 'any'}"
        )

    async def _mark_job_terminal(
        self,
        job_id: str,
        *,
        state: JobState,
        reason_key: str,
        reason: str,
    ) -> Job | None:
        """Persist a terminal state and clear live task sets without losing counts."""

        job = await self.fetch_job(job_id, include_tasks=False)
        if job is None:
            return None

        identity = mps_attempt.current(job_id)
        if identity is None and job.metadata.get("mps_manifest"):
            identity = mps_attempt.Identity(
                job_id, "", job.metadata.get("mps_task_manifest") or ""
            )
        with mps_attempt.scope(identity):
            return await self._persist_terminal_state(
                job, state=state, reason_key=reason_key, reason=reason
            )

    async def _persist_terminal_state(
        self, job: Job, *, state: JobState, reason_key: str, reason: str
    ) -> Job:
        job_id = job.job_id
        task_stats = await self.get_job_task_stats(job_id)
        total_tasks = max(
            int(task_stats["total"]), await self.get_total_task_count(job_id)
        )
        completed_at = job.completed_at or datetime.now(timezone.utc).isoformat()
        previous_state = job.state

        job.state = state
        job.completed_at = completed_at
        job.updated_at = completed_at
        if previous_state != state or not job.metadata.get(reason_key):
            job.metadata[reason_key] = reason
        await self.persist_job(job, persist_tasks=False)

        await self.clear_task_state(job_id, reset_stats=False)
        await self.remove_scheduled(job_id)
        await self._write_task_stats(
            job_id,
            total=total_tasks,
            ready=0,
            blocked=0,
            inflight=0,
            deferred=0,
            completed=int(task_stats["completed"]),
            failed=int(task_stats["failed"]),
            task_type_counts=cast(dict[str, int], task_stats["task_type_counts"]),
        )
        return job

    async def mark_job_cancelled(self, job_id: str, reason: str = "") -> Job | None:
        """Mark a job cancelled and clear live task-state sets without scanning all tasks."""

        return await self._mark_job_terminal(
            job_id,
            state=JobState.CANCELLED,
            reason_key="cancel_reason",
            reason=reason,
        )

    async def mark_job_failed(self, job_id: str, reason: str = "") -> Job | None:
        """Mark a job failed while preserving task counts and failure semantics."""

        return await self._mark_job_terminal(
            job_id,
            state=JobState.FAILED,
            reason_key="failure_reason",
            reason=reason,
        )

    async def _set_task_state(
        self, job_id: str, task_id: str, state: str, **updates: Any
    ) -> None:
        task_key = self.TASK_KEY.format(job_id=job_id, task_id=task_id)
        raw_task = await self.redis.get(task_key)
        if raw_task is None:
            return
        try:
            task_blob = json.loads(raw_task)
        except json.JSONDecodeError:
            return
        task_blob["state"] = state
        task_blob["updated_at"] = datetime.now(timezone.utc).isoformat()
        for key, value in updates.items():
            task_blob[key] = value
        from .mps_attempt import current, mutate

        attempt = current(job_id)
        if attempt is not None:
            await mutate(self, job_id, attempt.manifest_id, """
                if not redis.call('ZSCORE', KEYS[3], ARGV[2]) then return -1 end
                redis.call('SET', KEYS[4], ARGV[3], 'XX', 'KEEPTTL')
                return 1
            """, [self.TASK_INDEX_KEY.format(job_id=job_id), task_key], [task_id, json.dumps(task_blob)])
            return
        # A materializer can purge the task between GET and SET.
        await ordinary_edit.control_mutation(self, job_id, "return redis.call('SET',KEYS[1],ARGV[1],'XX','KEEPTTL')", [task_key], [json.dumps(task_blob)])

    async def requeue_deferred_tasks_for_dedup(
        self,
        dedup_key: str,
        *,
        default_backend_id: str | None = None,
    ) -> int:
        """Stream deferred members; atomically validate, transition and enqueue each."""
        from .routing import BackendRouter

        client = self._client()
        lock_key = self.DEFERRED_REQUEUE_LOCK_KEY.format(dedup_key=dedup_key)
        owner = uuid4().hex
        if not await client.set(lock_key, owner, nx=True, ex=30):
            return 0
        router = BackendRouter(self.redis)
        deferred_key = self.DEFERRED_TASKS_KEY.format(dedup_key=dedup_key)
        requeued = 0
        try:
            async for members in self._iter_set_members(
                deferred_key,
                batch_size=self.DEFAULT_WRITE_BATCH_SIZE,
            ):
                if not await client.eval(
                    recovery_scripts.COMPARE_EXPIRE, 1, lock_key, owner, 30
                ):
                    break
                for member in members:
                    job_id, sep, task_id = member.partition(":")
                    if not sep or not job_id or not task_id:
                        continue
                    task_key = self.TASK_KEY.format(job_id=job_id, task_id=task_id)
                    raw_task = await client.get(task_key)
                    try:
                        task_blob = json.loads(raw_task) if raw_task is not None else None
                    except (TypeError, ValueError):
                        # Preserve malformed evidence for inspection; never invent a task.
                        continue
                    if (
                        not isinstance(task_blob, dict)
                        or task_blob.get("task_id") != task_id
                        or task_blob.get("state") != TaskState.DEFERRED.value
                        or task_blob.get("deferred_dedup_key") != dedup_key
                    ):
                        dropped = await client.eval(
                            recovery_scripts.DROP_STALE_DEFERRED,
                            3,
                            lock_key,
                            task_key,
                            deferred_key,
                            owner,
                            raw_task or "",
                            member,
                            DEFERRED_RETENTION_MEMBER,
                        )
                        if dropped == -2:
                            return requeued
                        continue

                    state_key = self.JOB_STATE_KEY.format(job_id=job_id)
                    backend_id = (
                        _decode_redis_text(await client.hget(state_key, "backend_id"))
                        or default_backend_id
                    )
                    if not backend_id:
                        continue
                    updated_at = datetime.now(timezone.utc).isoformat()
                    task_blob.update(
                        state=TaskState.PENDING.value,
                        deferred_dedup_key=None,
                        started_at=None,
                        completed_at=None,
                        updated_at=updated_at,
                    )
                    priority = await self.get_job_priority(job_id)
                    result = int(
                        await client.eval(
                            recovery_scripts.REQUEUE_DEFERRED,
                            9,
                            lock_key,
                            task_key,
                            state_key,
                            self.TASKS_READY_KEY.format(job_id=job_id),
                            deferred_key,
                            self.TASK_EXEC_LOCK_KEY.format(task_id=task_id),
                            self.DEDUPE_KEY.format(dedup=dedup_key),
                            router._queue_key(backend_id),
                            router._ready_sequence_key(backend_id),
                            owner,
                            raw_task,
                            json.dumps(task_blob),
                            task_id,
                            member,
                            DEFERRED_RETENTION_MEMBER,
                            backend_id,
                            default_backend_id or "",
                            updated_at,
                            self.task_state_ttl,
                            priority * router.READY_SCORE_SCALE,
                            30,
                        )
                    )
                    if result == -2:
                        return requeued
                    requeued += result
            return requeued
        finally:
            await client.eval(recovery_scripts.COMPARE_DELETE, 1, lock_key, owner)

    async def cleanup_completed_jobs(
        self, max_age_seconds: int, *, batch_size: int | None = None, max_deleted: int | None = None
    ) -> int:
        """Retire one bounded page, resuming the index scan on the next call.

        The cursor advances past old active jobs and archive failures. Concurrent
        index changes may defer an entry until the next pass; they cannot make a
        page exceed the configured count or authorize deleting a changed header.
        This helper does not itself enable an automatic retention policy.
        """
        if isinstance(max_age_seconds, bool) or not isinstance(max_age_seconds, int) or max_age_seconds < 0:
            raise ValueError("Retention max_age_seconds must be a non-negative integer")
        if max_deleted is not None and (isinstance(max_deleted, bool) or not isinstance(max_deleted, int) or max_deleted < 1):
            raise ValueError("Retention max_deleted must be a positive integer")
        cutoff = _utc_timestamp() - max_age_seconds
        if batch_size is None:
            batch_size = self._recovery_scan_count(self.DEFAULT_WRITE_BATCH_SIZE)
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise ValueError("Retention batch_size must be a positive integer")
        client = self._client()
        try:
            offset = max(0, int(await client.get(self.RETENTION_CURSOR_KEY) or 0))
        except (TypeError, ValueError):
            offset = 0
        page = await client.zrange(
            self.JOB_INDEX_KEY, offset, offset + batch_size - 1, withscores=True
        )
        if not page and offset:
            offset = 0
            page = await client.zrange(
                self.JOB_INDEX_KEY, 0, batch_size - 1, withscores=True
            )
        # Rank pagination seeks into the index without a growing score-range
        # OFFSET traversal when many old active jobs must be retained.
        job_ids = [
            _decode_redis_text(item) for item, score in page if 0 <= score <= cutoff
        ]
        deleted = 0
        for job_id in job_ids:
            assert job_id is not None
            state_key = self.JOB_STATE_KEY.format(job_id=job_id)
            config_key = self.JOB_CONFIG_KEY.format(job_id=job_id)
            archive_batch_size, archive_max_bytes = self.task_archive_limits()
            header_max_bytes = retention.header_byte_limit(self)
            try:
                raw_state, raw_config, index_score = await retention.read_header(self, job_id, max_bytes=header_max_bytes)
            except TaskLoadLimitExceeded as exc:
                logger.warning("Skipping retention cleanup for job %s: %s", job_id, exc)
                continue
            if index_score is None or index_score > cutoff:
                continue
            if not raw_state:
                # An ordinary publisher uses a transaction. Preserve a partial
                # header from older writers and any concurrently repaired entry.
                from .runtime_admission import RELEASE_LUA

                removed = int(
                    await client.eval(
                        RELEASE_LUA + """
                    local score = redis.call('ZSCORE', KEYS[3], ARGV[1])
                    if redis.call('EXISTS', KEYS[1], KEYS[2]) == 0
                        and score and tonumber(score) == tonumber(ARGV[2]) then
                        release_header(ARGV[1])
                        return redis.call('ZREM', KEYS[3], ARGV[1])
                    end
                    return 0
                    """,
                        3,
                        state_key,
                        config_key,
                        self.JOB_INDEX_KEY,
                        job_id,
                        index_score,
                    )
                )
                deleted += removed
                if max_deleted is not None and deleted >= max_deleted:
                    await client.set(self.RETENTION_CURSOR_KEY, 0)
                    return deleted
                continue
            state = {
                (key.decode() if isinstance(key, bytes) else str(key)): (
                    value.decode() if isinstance(value, bytes) else str(value)
                )
                for key, value in raw_state.items()
            }
            if state.get("edit_pending") or state.get("status") not in {"completed", "failed", "cancelled"}:
                continue
            generation_field = (
                "completed_at" if state.get("completed_at") else "updated_at"
            )
            archive_generation = state.get(generation_field)
            completed_at = _iso_to_datetime(archive_generation)
            if completed_at is None:
                logger.warning(
                    "Skipping retention cleanup for terminal job %s without a completion timestamp",
                    job_id,
                )
                continue
            if completed_at.timestamp() > cutoff:
                continue
            snapshot = retention.Snapshot(
                job_id,
                state["status"],
                generation_field,
                archive_generation,
                raw_config,
                index_score,
                manifest_flag=state.get("mps_manifest") or "",
                task_manifest=state.get("mps_task_manifest") or "",
                backend_id=state.get("backend_id") or "",
                runtime_state=retention.terminal_runtime(state),
            )
            try:
                archive_definition = (
                    None if state.get("mps_manifest") == "1"
                    else retention.definition_id(job_id, raw_config)
                )
                archive_result = await self.database_writes.submit(
                    DatabaseWriteOperation.ARCHIVE_JOB_TASKS,
                    {
                        "job_id": job_id,
                        "generation": archive_generation,
                        "batch_size": archive_batch_size,
                        "max_bytes": archive_max_bytes,
                        "archive_header": True,
                        "header_max_bytes": header_max_bytes,
                        "archive_signature": retention.archive_signature(snapshot),
                        **(
                            {"mps_task_manifest": state.get("mps_task_manifest") or ""}
                            if state.get("mps_manifest") == "1"
                            else {
                                "task_definition_id": archive_definition,
                                "archive_signature": retention.archive_signature(snapshot),
                            }
                        ),
                    },
                    command_id=stable_command_id(
                        "archive-job-header-v2" if archive_definition is not None else "archive-job-header-v1",
                        job_id,
                        archive_generation,
                        archive_batch_size,
                        archive_max_bytes,
                        header_max_bytes,
                        archive_definition, retention.archive_signature(snapshot),
                    ),
                )
                if archive_result.get("stale"):
                    continue
                archived = int(archive_result.get("archived_tasks") or 0)
            except Exception as archive_error:  # noqa: BLE001
                logger.warning(
                    "Skipping retention cleanup for job %s: "
                    "failed to archive tasks to central DB (%s)",
                    job_id,
                    archive_error,
                )
                continue
            if archived:
                logger.info(
                    "Archived %d task(s) for job %s before Redis cleanup",
                    archived,
                    job_id,
                )
            identity = (
                mps_attempt.Identity(job_id, "", state.get("mps_task_manifest") or "")
                if state.get("mps_manifest") == "1"
                else None
            )
            try:
                with retention.scope(snapshot), mps_attempt.scope(identity):
                    await self.delete_job(job_id)
            except (mps_attempt.StaleTaskDefinition, retention.StaleRetirement):
                continue  # The archived job changed before a cleanup mutation.
            deleted += 1
            if max_deleted is not None and deleted >= max_deleted:
                # Pressure cleanup only needs one slot. Restart the bounded
                # scan later rather than advancing past entries not inspected.
                await client.set(self.RETENTION_CURSOR_KEY, 0)
                return deleted
        # Account for removed entries without moving past the next old member.
        # A fresh JobStorage instance resumes this bounded scan after a restart.
        if len(page) == batch_size and page[-1][1] <= cutoff:
            pipe = client.pipeline(transaction=False)
            for job_id, _score in page:
                pipe.zscore(self.JOB_INDEX_KEY, job_id)
            scores = await pipe.execute()
            next_offset = offset + sum(
                score is not None and score <= cutoff for score in scores
            )
        else:
            next_offset = 0
        await client.set(self.RETENTION_CURSOR_KEY, next_offset)
        return deleted

    def task_archive_limits(
        self,
        *,
        batch_size: int | None = None,
        max_bytes: int | None = None,
    ) -> tuple[int, int]:
        if batch_size is None:
            try:
                from .resources import _persisted_job_execution_setting

                batch_size = _persisted_job_execution_setting("task_archive_batch_size")
            except Exception:
                pass
        return (
            _positive_int_env(
                "ULTIMA_JOB_TASK_ARCHIVE_BATCH_SIZE", 500, value=batch_size
            ),
            _positive_int_env(
                "ULTIMA_JOB_TASK_ARCHIVE_MAX_BYTES",
                self.database_writes.max_command_bytes,
                value=max_bytes,
            ),
        )

    async def _archive_job_tasks_to_db(
        self,
        job_id: str,
        *,
        batch_size: int | None = None,
        max_bytes: int | None = None,
    ) -> int:
        """Writer-only implementation that archives Redis task state.

        Called before retention removes Redis keys so the UI's DB fallback
        can render the Pipeline tab for old terminal jobs indefinitely.

        A durable Redis marker is written only after every indexed task is in
        PostgreSQL and the row count is verified. If a process crashes during
        the later Redis purge, retries validate that marker and resume purging
        without deleting the already-complete archive.
        """

        batch_size, max_bytes = self.task_archive_limits(
            batch_size=batch_size, max_bytes=max_bytes
        )
        identity = mps_attempt.current(job_id)
        snapshot = retention.current(job_id)
        ordinary_snapshot = (
            snapshot if snapshot is not None and snapshot.manifest_flag != "1"
            else None
        )
        marker_identity = (
            {
                "generation": ordinary_snapshot.generation,
                "task_definition_id": retention.definition_id(job_id, ordinary_snapshot.config),
            }
            if ordinary_snapshot is not None else {}
        )
        if identity is not None:
            await mps_attempt.assert_current(
                self, job_id, identity.manifest_id, wait_for_edit=False
            )
        marker_key = self.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id)
        raw_marker = await self.redis.get(marker_key)
        if raw_marker:
            try:
                marker = json.loads(
                    raw_marker.decode() if isinstance(raw_marker, bytes) else raw_marker
                )
                archived_count = int(marker["task_count"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"Invalid task archive marker for job {job_id}"
                ) from exc
            if all(marker.get(key) == value for key, value in marker_identity.items()):
                central_count = await asyncio.to_thread(
                    self._count_central_tasks_sync, job_id
                )
                if (central_count != archived_count if ordinary_snapshot is not None
                        else central_count < archived_count):
                    raise RuntimeError(
                        f"Task archive for job {job_id} is incomplete: "
                        f"marker={archived_count}, database={central_count}"
                    )
                return archived_count

        # Use the physical index, not the advertised total in JOB_STATE. A
        # cancelled materializer may legitimately have created only part of
        # its planned task set; every task that actually exists must still be
        # archived before that partial working set can be released.
        task_index_key = self.TASK_INDEX_KEY.format(job_id=job_id)
        task_index_exists, expected_count = await asyncio.gather(
            self._client().exists(task_index_key),
            self._client().zcard(task_index_key),
        )
        expected_count = int(expected_count)
        if not task_index_exists:
            if ordinary_snapshot is not None:
                # Older terminal cleanup removed the marker with the task index.
                # Recover only from the exact successful task-archive command;
                # a row count by itself cannot certify this job execution.
                from ..database.write_receipts import lookup

                receipt_id = stable_command_id(
                    "archive-job-tasks-v4",
                    job_id,
                    ordinary_snapshot.generation,
                    batch_size,
                    max_bytes,
                    marker_identity["task_definition_id"],
                    retention.archive_signature(
                        replace(ordinary_snapshot, runtime_state={})
                    ),
                )
                receipt = (await asyncio.to_thread(lookup, [receipt_id])).get(receipt_id)
                if receipt is not None and receipt.success and not receipt.payload.get("stale"):
                    count = receipt.payload.get("archived_tasks")
                    if type(count) is int and count >= 0:
                        central_count = await asyncio.to_thread(
                            self._count_central_tasks_sync, job_id
                        )
                        if central_count != count:
                            raise RuntimeError(
                                f"Task archive for job {job_id} is incomplete: "
                                f"receipt={count}, database={central_count}"
                            )
                        await self._publish_task_archive_marker(job_id, count)
                        return count
                raise DatabaseWriteRejected(
                    f"Cannot verify task archive for job {job_id}: "
                    "no matching completion marker or live task index, and no "
                    "matching successful archive receipt; retaining job and stored tasks"
                )
            # A delayed duplicate can arrive after a successful archive and
            # Redis purge. Preserve the canonical database rows instead of
            # replacing them from an already-released working set.
            archived_count = await asyncio.to_thread(
                self._count_central_tasks_sync, job_id
            )
            await self._publish_task_archive_marker(job_id, archived_count)
            return archived_count

        is_mps = identity is not None or await self._client().hget(
            self.JOB_STATE_KEY.format(job_id=job_id), "mps_manifest"
        ) in (b"1", "1")
        if not is_mps:
            await asyncio.to_thread(self._delete_central_tasks_sync, job_id)

        batch: list[dict[str, Any]] = []
        archived = 0
        batch_bytes = 0

        async def _flush() -> None:
            nonlocal batch, archived, batch_bytes
            if not batch:
                return
            pending = batch
            batch = []
            batch_bytes = 0
            if is_mps:
                written = await asyncio.to_thread(
                    self._upsert_central_tasks_sync, pending
                )
            else:
                written = await asyncio.to_thread(
                    self._insert_central_tasks_sync, pending
                )
            archived += written

        async for task in self.fetch_tasks_stream(
            job_id, batch_size, max_bytes=max_bytes
        ):
            row = _task_to_central_row(task, job_id)
            # These rows contain scalar SQL parameters. Count their encoded
            # field values separately from the Redis JSON representation;
            # serialization can expand Unicode in result_json.
            size = sum(
                len(str(value).encode("utf-8"))
                for value in row.values()
                if value is not None
            )
            if size > max_bytes:
                raise TaskPayloadBudgetExceeded(
                    task.task_id, size, max_bytes, phase="SQL archive"
                )
            if batch and batch_bytes + size > max_bytes:
                await _flush()
            batch.append(row)
            batch_bytes += size
            if len(batch) >= batch_size:
                await _flush()
        await _flush()
        if archived != expected_count:
            raise RuntimeError(
                f"Refusing to purge job {job_id}: archived {archived} of "
                f"{expected_count} indexed task(s)"
            )
        central_count = await asyncio.to_thread(self._count_central_tasks_sync, job_id)
        if is_mps:
            expected_count = central_count
        elif central_count != expected_count:
            raise RuntimeError(
                f"Task archive for job {job_id} changed: "
                f"indexed={expected_count}, database={central_count}"
            )
        await self._publish_task_archive_marker(job_id, expected_count)
        return archived

    async def _publish_task_archive_marker(self, job_id: str, count: int) -> None:
        key = self.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id)
        snapshot = retention.current(job_id)
        payload = json.dumps({
            "task_count": count,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            **(
                {
                    "generation": snapshot.generation,
                    "task_definition_id": retention.definition_id(job_id, snapshot.config),
                }
                if snapshot is not None and snapshot.manifest_flag != "1" else {}
            ),
        })
        if mps_attempt.current(job_id) is not None or snapshot is not None:
            await mps_attempt.control_mutation(
                self, job_id, "redis.call('SET', KEYS[1], ARGV[1]); return 1",
                [key], [payload], wait_for_edit=False,
            )
        else:
            await self.redis.set(key, payload)

    @staticmethod
    def _count_central_tasks_sync(job_id: str) -> int:
        from sqlalchemy import func, select as sql_select

        from ultima_scraper_db_archive.global_db import (
            CentralTaskModel,
            global_session,
        )

        with global_session() as session:
            mps_attempt.sql_assert_current(session, job_id)
            retention.sql_assert_current(session, job_id)
            result = session.execute(
                sql_select(func.count())
                .select_from(CentralTaskModel)
                .where(CentralTaskModel.job_id == job_id)
            )
            return int(result.scalar_one())

    @staticmethod
    def _delete_central_tasks_sync(job_id: str) -> int:
        from sqlalchemy import delete as sql_delete

        from ultima_scraper_db_archive.global_db import (
            CentralTaskModel,
            global_session,
        )

        with global_session() as session:
            try:
                retention.sql_assert_current(session, job_id)
                result = cast(
                    Any,
                    session.execute(
                        sql_delete(CentralTaskModel).where(
                            CentralTaskModel.job_id == job_id
                        )
                    ),
                )
                session.commit()
                return int(result.rowcount or 0)
            except Exception:
                session.rollback()
                raise

    @staticmethod
    def _insert_central_tasks_sync(rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        from sqlalchemy import insert as sql_insert

        from ultima_scraper_db_archive.global_db import (
            CentralTaskModel,
            global_session,
        )

        with global_session() as session:
            try:
                retention.sql_assert_current(session, str(rows[0]["job_id"]))
                session.execute(sql_insert(CentralTaskModel), rows)
                session.commit()
                return len(rows)
            except Exception:
                session.rollback()
                raise

    @staticmethod
    def _upsert_central_tasks_sync(rows: list[dict[str, Any]]) -> int:
        from sqlalchemy.dialects.postgresql import insert
        from ultima_scraper_db_archive.global_db import CentralTaskModel, global_session

        if not rows:
            return 0
        from ..database.job_partitions import task_conflict_columns
        statement = insert(CentralTaskModel)
        with global_session() as session:
            mps_attempt.sql_assert_current(session, str(rows[0]["job_id"]))
            session.execute(statement.on_conflict_do_update(
                index_elements=task_conflict_columns(session),
                set_={column.name: getattr(statement.excluded, column.name)
                      for column in CentralTaskModel.__table__.columns if column.name != 'task_id'},
            ), rows)
            session.commit()
        return len(rows)

    @staticmethod
    def reconciliation_batch_size(value: int | None = None) -> int:
        from ..database.write_queue import _positive_int_env

        return _positive_int_env(
            "ULTIMA_JOB_RECONCILE_BATCH_SIZE",
            JobStorage.DEFAULT_WRITE_BATCH_SIZE,
            value=value,
        )

    @staticmethod
    def _reconcile_database_jobs_sync(
        *,
        batch_size: int | None = None,
        after_task_id: str | None = None,
        db_session: Any | None = None,
    ) -> dict[str, Any]:
        """Repair one task-ID page; only counts and a cursor leave PostgreSQL."""
        from sqlalchemy import func, select, update
        from ultima_scraper_db_archive.global_db import (
            CentralJobModel,
            CentralTaskModel,
            global_session,
        )

        limit = JobStorage.reconciliation_batch_size(batch_size)
        if after_task_id is not None and (
            not isinstance(after_task_id, str) or len(after_task_id) > 64
        ):
            raise ValueError("after_task_id must be a task identifier")
        jobs = CentralJobModel.__table__
        tasks = CentralTaskModel.__table__
        terminal = ("completed", "failed", "cancelled")
        query = (
            select(tasks.c.task_id)
            .join(jobs, jobs.c.job_id == tasks.c.job_id)
            .where(jobs.c.status.in_(terminal), tasks.c.status.notin_(terminal))
            .order_by(tasks.c.task_id)
            .limit(limit)
        )
        if after_task_id is not None:
            query = query.where(tasks.c.task_id > after_task_id)
        candidates = query.cte("reconcile_candidates").prefix_with("MATERIALIZED")
        timestamp = func.coalesce(
            jobs.c.result_completed_at,
            jobs.c.completed_at,
            jobs.c.last_event_at,
            func.now(),
        )
        repaired = (
            update(tasks)
            .where(
                tasks.c.task_id == candidates.c.task_id,
                tasks.c.job_id == jobs.c.job_id,
                jobs.c.status.in_(terminal),
                tasks.c.status.notin_(terminal),
            )
            .values(
                status=jobs.c.status,
                last_event_at=timestamp,
                completed_at=timestamp,
                error=jobs.c.result_error,
            )
            .returning(tasks.c.task_id)
            .cte("reconciled_tasks")
        )
        statement = select(
            select(func.count()).select_from(repaired).scalar_subquery(),
            select(func.count()).select_from(candidates).scalar_subquery(),
            select(func.max(candidates.c.task_id)).scalar_subquery(),
        )
        with (nullcontext(db_session) if db_session is not None else global_session()) as session:
            try:
                count, selected, cursor = session.execute(statement).one()
                if db_session is None:
                    session.commit()
                return {
                    "repaired_tasks": int(count),
                    "next_task_id": cursor if selected == limit else None,
                }
            except Exception:
                session.rollback()
                raise

    @staticmethod
    def _update_job_status_database_sync(
        job_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        db_session: Any | None = None,
    ) -> bool:
        """Writer-only terminal job status mutation."""

        from sqlalchemy import select, update
        from sqlalchemy.orm import defer

        from ultima_scraper_db_archive.global_db import (
            CentralJobModel,
            CentralTaskModel,
            global_session,
        )

        def _coerce_datetime(value: str | datetime | None) -> datetime | None:
            if value is None:
                return None
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    return value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc)
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

        normalized_status = status.strip().lower()
        event_time = _coerce_datetime(completed_at) or datetime.now(timezone.utc)

        with (nullcontext(db_session) if db_session is not None else global_session()) as session:
            try:
                selected = session.execute(
                    select(CentralJobModel, ordinary_edit.sql_definition_expression())
                    .options(defer(CentralJobModel.args_json))
                    .where(CentralJobModel.job_id == job_id).with_for_update()
                ).first()
                row = selected[0] if selected is not None else None
                snapshot = job_snapshot or {}
                config = snapshot.get("job_config")
                if not isinstance(config, dict):
                    config = {"metadata": snapshot.get("metadata") or {}}
                if selected is not None and selected[1] != retention.definition_id(job_id, config):
                    return False
                if row is not None and row.last_event_at is not None:
                    previous_event_at = _coerce_datetime(row.last_event_at)
                    if (
                        previous_event_at is not None
                        and previous_event_at > event_time
                    ):
                        logger.info(
                            "Ignored stale %s status for job %s (event=%s, current=%s)",
                            normalized_status,
                            job_id,
                            event_time.isoformat(),
                            previous_event_at.isoformat(),
                        )
                        return False
                if row is None:
                    row = CentralJobModel(
                        job_id=job_id,
                        job_type=_job_snapshot_job_type(job_snapshot),
                        site=_job_snapshot_site(job_snapshot),
                        status=normalized_status,
                        args_json=json.dumps(
                            _build_terminal_job_args_payload(job_snapshot)
                        ),
                        job_config_json=(
                            json.dumps(job_snapshot["job_config"])
                            if job_snapshot and isinstance(job_snapshot.get("job_config"), dict)
                            else None
                        ),
                        created_at=(
                            _coerce_datetime((job_snapshot or {}).get("created_at"))
                            or event_time
                        ),
                        started_at=_coerce_datetime(
                            (job_snapshot or {}).get("started_at")
                        ),
                    )
                    logger.info(
                        "Central DB row missing for job %s; created terminal row during %s sync",
                        job_id,
                        normalized_status,
                    )

                row.status = normalized_status
                row.last_event_at = event_time

                if normalized_status == "completed":
                    row.completed_at = event_time
                    row.result_success = True
                    row.result_error = None
                    row.result_completed_at = event_time
                elif normalized_status == "failed":
                    row.completed_at = event_time
                    row.result_success = False
                    row.result_error = error
                    row.result_completed_at = event_time
                elif normalized_status == "cancelled":
                    row.completed_at = event_time
                    row.result_success = False
                    row.result_error = None
                    row.result_completed_at = event_time

                if normalized_status in {"completed", "failed", "cancelled"}:
                    task_values: dict[str, Any] = {
                        "status": normalized_status,
                        "last_event_at": event_time,
                        "completed_at": event_time,
                    }
                    if normalized_status == "completed":
                        task_values["error"] = None
                    elif normalized_status == "failed":
                        task_values["error"] = error
                    elif normalized_status == "cancelled":
                        task_values["error"] = None

                    cast(Any, session).execute(
                        update(CentralTaskModel)
                        .where(CentralTaskModel.job_id == job_id)
                        .where(
                            CentralTaskModel.status.notin_(
                                ("completed", "failed", "cancelled")
                            )
                        )
                        .values(**task_values)
                    )

                session.add(row)
                if db_session is None:
                    session.commit()
                return True
            except Exception:
                session.rollback()
                raise

    async def purge_job_task_blobs(
        self,
        job_id: str,
        *,
        batch_size: int = 500,
        batch_pause: float = 0.0,
        max_retries_per_batch: int = 6,
    ) -> int:
        """Delete per-task Redis artifacts for a job while keeping job metadata.

        Removes the TASK_KEY JSON blobs, TASK_DEPENDENTS_KEY sets, TASK_EXEC_LOCK_KEY
        locks, and the TASK_INDEX_KEY itself. Leaves JOB_STATE_KEY / JOB_CONFIG_KEY /
        JOB_INDEX_KEY intact so the job remains visible in the UI as a historical
        record. Safe to call on already-purged jobs (no-op) and safe to resume
        if a previous run crashed part-way through — the task index drives the
        loop, so the next invocation picks up where the last one stopped.

        Uses a single pipelined DEL+ZREM per batch (1 round-trip / batch_size
        tasks). Each batch is retried with exponential backoff on transient
        connection errors so a Redis restart during a multi-hour purge
        resumes instead of aborting.

        Args:
            job_id: Job whose task blobs should be removed.
            batch_size: Task IDs pipelined per round-trip. Lower = gentler on
                Redis (recommended when the server is memory-pressured).
            batch_pause: Optional sleep (seconds) between batches. Use a small
                value (e.g. 0.05) if Redis is struggling.
            max_retries_per_batch: How many times to retry a failing batch
                before raising. Backoff is 0.5s, 1s, 2s, 4s, 8s, 16s.

        Returns:
            Number of task blobs deleted.
        """

        marker_key = self.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id)
        if not await self.redis.get(marker_key):
            raise RuntimeError(
                f"Refusing to purge job {job_id} without a verified task archive"
            )

        await self.clear_task_state(job_id, reset_stats=False)
        task_index_key = self.TASK_INDEX_KEY.format(job_id=job_id)
        deleted = 0
        client = self._client()
        while True:
            task_ids = await self.list_task_ids_window(
                job_id,
                offset=0,
                limit=batch_size,
            )
            if not task_ids:
                break

            # Build the pipeline once per batch; retry the execute with backoff
            # so a transient Redis outage doesn't nuke the whole purge run.
            for attempt in range(max_retries_per_batch):
                pipe = client.pipeline(transaction=False)
                for task_id in task_ids:
                    pipe.delete(
                        self.TASK_KEY.format(job_id=job_id, task_id=task_id),
                        self.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id=task_id),
                        self.TASK_EXEC_LOCK_KEY.format(task_id=task_id),
                    )
                pipe.zrem(task_index_key, *task_ids)
                try:
                    if mps_attempt.current(job_id) is not None or retention.current(job_id) is not None or ordinary_edit.current(job_id) is not None:
                        await mps_attempt.control_mutation(
                            self, job_id,
                            """
                            for i = 2, #KEYS do redis.call('DEL', KEYS[i]) end
                            for i = 1, #ARGV do redis.call('ZREM', KEYS[1], ARGV[i]) end
                            return #ARGV
                            """,
                            [task_index_key, *[
                                key for task_id in task_ids for key in (
                                    self.TASK_KEY.format(job_id=job_id, task_id=task_id),
                                    self.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id=task_id),
                                    self.TASK_EXEC_LOCK_KEY.format(task_id=task_id),
                                )
                            ]], task_ids,
                        )
                    else:
                        await pipe.execute()
                    break
                except (RedisConnectionError, RedisTimeoutError) as exc:
                    if attempt >= max_retries_per_batch - 1:
                        logger.error(
                            "purge_job_task_blobs: giving up on batch for %s "
                            "after %d retries: %s",
                            job_id,
                            max_retries_per_batch,
                            exc,
                        )
                        raise
                    backoff = min(16.0, 0.5 * (2**attempt))
                    logger.warning(
                        "purge_job_task_blobs: batch failed for %s (attempt "
                        "%d/%d), retrying in %.1fs: %s",
                        job_id,
                        attempt + 1,
                        max_retries_per_batch,
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)

            deleted += len(task_ids)
            if batch_pause > 0:
                await asyncio.sleep(batch_pause)

        # The ordinary header is retained for history and later retirement.
        # Keep its generation-bound archive proof until _delete_job_runtime
        # atomically removes both header and marker after the header is durable.
        snapshot = retention.current(job_id)
        keep_marker = snapshot is not None and snapshot.manifest_flag != "1"
        if mps_attempt.current(job_id) is not None or snapshot is not None or ordinary_edit.current(job_id) is not None:
            await mps_attempt.control_mutation(
                self, job_id, "return redis.call('DEL', unpack(KEYS))",
                [task_index_key] if keep_marker else [task_index_key, marker_key], [],
            )
        else:
            await self.redis.delete(task_index_key)
            if not keep_marker:
                await self.redis.delete(marker_key)
        if await client.hget(self.JOB_STATE_KEY.format(job_id=job_id), 'mps_manifest') in (b'1', '1'):
            from .mps_state import PLAN_KEY, release_budget

            await release_budget(client, job_id, storage=self)
            await mps_attempt.control_mutation(
                self, job_id, "return redis.call('EXPIRE', KEYS[1], ARGV[1])",
                [PLAN_KEY.format(job_id=job_id)], [self.completed_job_retention_seconds],
            )
        return deleted

    async def cancel_job_fully(self, job_id: str, reason: str = "") -> None:
        """Cancel a job and release any remaining dedupe reservations."""

        job = await self.mark_job_cancelled(job_id, reason=reason)
        if job is None:
            return
        await self.release_dedup_for_job_id(job_id)

    async def set_job_priority(self, job_id: str, priority: int) -> None:
        """Persist a priority hint on the job metadata."""

        job = await self.fetch_job(job_id, include_tasks=False)
        if job is None:
            return
        job.metadata["priority"] = priority
        job.updated_at = datetime.now(timezone.utc).isoformat()
        await self.persist_job(job, persist_tasks=False)

    async def _publish_terminal_members(
        self, job_id: str, set_key: str, task_ids: Iterable[str]
    ) -> int:
        """Re-publish terminal set members only while their window is still live.

        An MPS window purge releases the index entry, the window reservation
        and the terminal-set membership in one transaction, and it can only
        ever sweep ids it can still see in the index. A snapshot read before
        that purge must therefore not put those ids back: the window invariant
        asserts that every terminal set member holds a reservation, and a
        resurrected member would outlive its reservation, its payload and its
        index entry with nothing left to clear it again.

        The membership test and the SADD share one Lua script so Redis runs
        them atomically against a concurrent purge. Either the purge commits
        first and the id is skipped, or this commits first and the purge's own
        SREM clears it -- no interleaving leaves a ghost behind. Dispatch
        publishes the index entry and the reservation together, so index
        membership is the same residency test `mps_state.terminal_task` uses.
        """

        pending = sorted(task_ids)
        index_key = self.TASK_INDEX_KEY.format(job_id=job_id)
        added = 0
        for start in range(0, len(pending), self.DEFAULT_WRITE_BATCH_SIZE):
            added += int(
                await self._client().eval(
                    """
                    local added = 0
                    for i = 1, #ARGV do
                        if redis.call('ZSCORE', KEYS[1], ARGV[i]) then
                            added = added + redis.call('SADD', KEYS[2], ARGV[i])
                        end
                    end
                    return added
                    """,
                    2,
                    index_key,
                    set_key,
                    *pending[start : start + self.DEFAULT_WRITE_BATCH_SIZE],
                )
            )
        return added

    async def restore_task_state(
        self,
        job: Job,
        *,
        return_ready_task_ids: bool = True,
    ) -> list[str]:
        """Restore task state sets from existing Redis data or job definition.

        This method is used when resuming a job after restart or pause.
        It checks which tasks have already been completed and rebuilds the
        ready/blocked/inflight sets accordingly.

        Returns:
            List of task IDs that are ready to be executed.
        """
        ready_key = self.TASKS_READY_KEY.format(job_id=job.job_id)
        blocked_key = self.TASKS_BLOCKED_KEY.format(job_id=job.job_id)
        completed_key = self.TASKS_COMPLETED_KEY.format(job_id=job.job_id)
        inflight_key = self.TASKS_INFLIGHT_KEY.format(job_id=job.job_id)
        outputs_key = self.TASK_OUTPUTS_KEY.format(job_id=job.job_id)

        client = self._client()
        mps_state = await client.hmget(self.JOB_STATE_KEY.format(job_id=job.job_id), 'mps_manifest', 'mps_archived_completed', 'mps_archived_failed')
        is_mps = mps_state[0] in (b'1', '1')
        archived_completed = int(mps_state[1] or 0) if is_mps else 0
        archived_failed = int(mps_state[2] or 0) if is_mps else 0

        # Get already completed tasks from Redis
        completed_members = await client.smembers(completed_key)
        completed_task_ids = {
            member.decode() if isinstance(member, bytes) else str(member)
            for member in completed_members
        }
        await self._clear_dependency_index(job.job_id)

        # Get inflight tasks (these were being processed when we stopped)
        # Treat them as not started - they'll need to be re-executed
        inflight_members = await client.smembers(inflight_key)
        inflight_task_ids = {
            member.decode() if isinstance(member, bytes) else str(member)
            for member in inflight_members
        }

        # Clear existing state to rebuild (except completed - we want to preserve that)
        await client.delete(ready_key, blocked_key, inflight_key)

        ready_tasks: list[str] = [] if return_ready_task_ids else []
        ready_batch: list[str] = []
        blocked_batch: list[str] = []
        ready_count = 0
        blocked_count = 0
        deferred_count = 0
        failed_count = 0
        task_type_counts: dict[str, int] = {}
        tasks_to_reset: list[str] = []
        failed_task_ids: list[str] = []

        # Rebuild state based on job definition and completed tasks
        async for task in self._iter_job_tasks(
            job.job_id,
            batch_size=1000,
            preloaded_tasks=job.tasks or None,
        ):
            await self._index_task_dependencies(job.job_id, task)
            task_type_counts[task.task_type.value] = (
                task_type_counts.get(task.task_type.value, 0) + 1
            )
            if task.state == TaskState.COMPLETED:
                completed_task_ids.add(task.task_id)
                continue
            if task.state == TaskState.FAILED:
                failed_count += 1
                if is_mps:
                    failed_task_ids.append(task.task_id)
                    continue
            if task.state == TaskState.DEFERRED:
                deferred_count += 1
                continue

            # Skip already completed tasks
            if task.task_id in completed_task_ids:
                continue

            task_state = (
                task.state.value if hasattr(task.state, "value") else str(task.state)
            )
            if task_state in {
                TaskState.RUNNING.value,
                TaskState.CLAIMED.value,
                TaskState.SAVING_DB.value,
            }:
                tasks_to_reset.append(task.task_id)

            # Check if dependencies are satisfied
            if task.depends_on:
                # Check if all dependencies are in the completed set
                deps_satisfied = all(
                    dep_id in completed_task_ids for dep_id in task.depends_on
                )
                if deps_satisfied:
                    ready_count += 1
                    ready_batch.append(task.task_id)
                    if return_ready_task_ids:
                        ready_tasks.append(task.task_id)
                    if len(ready_batch) >= 1000:
                        await client.sadd(ready_key, *ready_batch)
                        ready_batch.clear()
                else:
                    blocked_count += 1
                    blocked_batch.append(task.task_id)
                    if len(blocked_batch) >= 1000:
                        await client.sadd(blocked_key, *blocked_batch)
                        blocked_batch.clear()
            else:
                # No dependencies - task is ready
                ready_count += 1
                ready_batch.append(task.task_id)
                if return_ready_task_ids:
                    ready_tasks.append(task.task_id)
                if len(ready_batch) >= 1000:
                    await client.sadd(ready_key, *ready_batch)
                    ready_batch.clear()

        # Update Redis with rebuilt state
        if ready_batch:
            await client.sadd(ready_key, *ready_batch)
        if blocked_batch:
            await client.sadd(blocked_key, *blocked_batch)

        for task_id in tasks_to_reset:
            await self._set_task_state(
                job.job_id,
                task_id,
                TaskState.PENDING.value,
                started_at=None,
            )

        if is_mps:
            # Publish after the scan, not during it: the scan is slow enough
            # for a concurrent window purge to release ids it read above, and
            # only residency in the index may re-admit them.
            if failed_task_ids:
                from .mps_state import FAILED_KEY

                await self._publish_terminal_members(
                    job.job_id, FAILED_KEY.format(job_id=job.job_id), failed_task_ids
                )
            if completed_task_ids:
                await self._publish_terminal_members(
                    job.job_id, completed_key, completed_task_ids
                )
            task_type_counts = {TaskType.SCRAPE.value: job.total_tasks}
            await client.hset(self.JOB_STATE_KEY.format(job_id=job.job_id), 'mps_runtime_released', 0)

        # Refresh TTLs on all state keys
        for key in (ready_key, blocked_key, completed_key, inflight_key, outputs_key):
            await retain_task_state(self, job.job_id, key)

        await self._write_task_stats(
            job.job_id,
            total=await self.get_total_task_count(job.job_id),
            ready=ready_count,
            blocked=blocked_count,
            inflight=0,
            deferred=deferred_count,
            completed=archived_completed + len(completed_task_ids),
            failed=archived_failed + failed_count,
            task_type_counts=task_type_counts,
        )

        logger.info(
            f"Restored task state for job {job.job_id}: "
            f"{len(completed_task_ids)} completed, "
            f"{ready_count} ready, "
            f"{blocked_count} blocked, "
            f"{len(inflight_task_ids)} inflight (reset to not started)"
        )

        return ready_tasks
