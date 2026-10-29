"""Record and aggregate per-job runtime telemetry kept in Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

logger = logging.getLogger(__name__)

# Seconds between flushes of in-process telemetry batches; unset or <= 0 keeps
# one Redis script per task.
BATCH_SECONDS_ENV = "ULTIMA_RUNTIME_TELEMETRY_BATCH_SECONDS"


def telemetry_batch_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get(BATCH_SECONDS_ENV) or 0))
    except ValueError:
        return 0.0


# (keyword, hash field) pairs summed with HINCRBY / HINCRBYFLOAT, mirroring the
# per-task script below.
_INT_SUMS = (
    ("route_attempts", "route_attempts"),
    ("writer_timed_submits", "writer_timed_submits"),
    ("writer_timing_coverage", "writer_timing_coverage"),
    ("handler_guest_init_count", "handler_guest_init_count"),
    ("handler_initial_lookup_count", "handler_initial_lookup_count"),
    ("handler_refresh_api_count", "handler_refresh_api_count"),
    ("handler_fallback_retry_count", "handler_fallback_retry_count"),
    ("handler_serialization_count", "handler_serialization_count"),
    ("writer_result_delivery_split_count", "writer_result_delivery_split_count"),
    ("writer_result_cache_hits", "writer_result_cache_hits"),
    ("writer_receipt_fallbacks", "writer_receipt_fallbacks"),
)
_FLOAT_SUMS = tuple(
    (name, name[:-3] + "_sum_ms" if name.endswith("_ms") else name)
    for name in (
        "queue_wait_ms",
        "setup_ms",
        "handler_ms",
        "writer_ms",
        "post_ms",
        "release_ms",
        "route_active_ms",
        "claim_wait_ms",
        "writer_admission_ms",
        "writer_queue_ms",
        "writer_execution_ms",
        "writer_commit_ms",
        "writer_overhead_ms",
        "writer_delivery_ms",
        "writer_connection_checkout_ms",
        "writer_receipt_lookup_ms",
        "writer_receipt_finalize_ms",
        "writer_payload_ms",
        "writer_user_row_ms",
        "writer_alias_ms",
        "writer_authority_ms",
        "writer_profile_ms",
        "writer_social_ms",
        "writer_flush_ms",
        "writer_queue_commands_sum",
        "writer_queue_bytes_sum",
        "writer_active_lanes_sum",
        "writer_batch_commands_sum",
        "handler_guest_init_ms",
        "handler_initial_lookup_ms",
        "handler_refresh_api_ms",
        "handler_fallback_retry_ms",
        "handler_serialization_ms",
        "writer_admission_serialization_ms",
        "writer_admission_capacity_wait_ms",
        "writer_admission_redis_ms",
        "writer_admission_overhead_detail_ms",
        "writer_batch_fill_ratio_sum",
        "writer_lane_idle_ms",
        "writer_lane_predecessor_wait_ms",
        "writer_refill_wait_ms",
        "writer_result_publication_ms",
        "writer_result_observation_ms",
        "writer_result_delivery_residual_ms",
        "writer_batch_peer_work_ms",
        "writer_authority_rows_examined",
        "writer_authority_rows_skipped",
        "writer_authority_rows_upserted",
    )
)
_MAXES = (
    "writer_queue_commands_max",
    "writer_queue_bytes_max",
    "writer_active_lanes_max",
    "writer_configured_concurrency",
    "writer_configured_batch_size",
    "writer_queue_max_commands",
    "writer_queue_max_bytes",
    "writer_refill_min_commands",
    "writer_window_commands",
    "writer_refill_min_commands_per_lane",
    "expected_workers",
)
_METRIC_MAPS = ("writer_operation_stats_json", "writer_domain_stats_json")
_METRIC_MAP_FIELDS = ("count", "batch_commands_sum", "batch_wall_ms_sum")
_RECEIPT_PHASE_FIELDS = (
    "receipt_phases_count",
    "receipt_lock_ms_sum",
    "receipt_read_ms_sum",
    "write_fence_ms_sum",
)
_SETUP_MAPS = (
    "setup_subphases_json",
    "guest_auth_stats_json",
    "terminal_stats_json",
    "ledger_stats_json",
)
_ROUTE_FIELDS = (
    "primary_selected_count",
    "hedge_selected_count",
    "fallback_selected_count",
    "deferred_selected_count",
    "healthy_selected_count",
    "retry_selected_count",
    "route_attempts",
    "lookup_observations",
    "route_active_ms",
    "guest_init_ms",
    "lookup_ms",
    "lookup_found_count",
    "lookup_not_found_count",
    "lookup_error_count",
    "lookup_timeout_count",
    "lookup_cancelled_count",
    "guest_init_failure_count",
    "guest_init_cancelled_count",
)


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _json_map(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value or "{}")
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


class _TaskTelemetryBatch:
    """One job and worker's summed task samples between two flushes."""

    def __init__(self) -> None:
        self.ints = dict.fromkeys((field for _, field in _INT_SUMS), 0)
        self.ints["succeeded_tasks"] = 0
        self.ints["failed_tasks"] = 0
        self.floats = dict.fromkeys((field for _, field in _FLOAT_SUMS), 0.0)
        self.floats["task_duration_sum_ms"] = 0.0
        self.maxes = dict.fromkeys(_MAXES, 0)
        self.first_started = 0
        self.last_finished = 0
        # Flat (finished, duration, success, completed, total) per task, in
        # record order: the steady window and completion gaps depend on it.
        self.records: list[int | float] = []
        self.worker_limit = 0
        self.metric_maps: dict[str, dict[str, dict[str, float]]] = {
            name: {} for name in _METRIC_MAPS
        }
        self.counter_map: dict[str, float] = {}
        self.setup_maps: dict[str, dict[str, dict[str, float]]] = {
            name: {} for name in _SETUP_MAPS
        }
        self.routes: dict[str, dict[str, float]] = {}

    def add(self, fields: dict[str, Any]) -> None:
        started = int(fields["started_ms"])
        finished = int(fields["finished_ms"])
        success = bool(fields["success"])
        ints, floats, maxes = self.ints, self.floats, self.maxes
        for name, field in _INT_SUMS:
            ints[field] += max(0, int(fields.get(name) or 0))
        ints["succeeded_tasks" if success else "failed_tasks"] += 1
        for name, field in _FLOAT_SUMS:
            floats[field] += max(0.0, float(fields.get(name) or 0.0))
        duration = max(0.0, float(finished - started))
        floats["task_duration_sum_ms"] += duration
        for name in _MAXES:
            value = int(fields.get(name) or 0)
            if value > maxes[name]:
                maxes[name] = value
        if self.first_started == 0 or started < self.first_started:
            self.first_started = started
        if finished > self.last_finished:
            self.last_finished = finished
        completed = fields.get("completed_count", 0)
        self.records.extend(
            (
                finished,
                duration,
                1 if success else 0,
                -1 if completed is None else max(0, int(completed)),
                max(0, int(fields.get("total_tasks") or 0)),
            )
        )
        self.worker_limit = max(0, int(fields.get("effective_worker_concurrency") or 0))
        for name in _METRIC_MAPS:
            target_map = self.metric_maps[name]
            for key, metrics in _json_map(fields.get(name)).items():
                if not isinstance(metrics, dict):
                    metrics = {}
                target = target_map.setdefault(
                    key, dict.fromkeys(_METRIC_MAP_FIELDS, 0.0)
                )
                for metric in _METRIC_MAP_FIELDS:
                    target[metric] += _number(metrics.get(metric))
                if _number(metrics.get("receipt_phases_count")) > 0:
                    for metric in _RECEIPT_PHASE_FIELDS:
                        target[metric] = target.get(metric, 0.0) + _number(
                            metrics.get(metric)
                        )
        for key, count in _json_map(fields.get("writer_split_missing_reasons_json")).items():
            self.counter_map[key] = self.counter_map.get(key, 0.0) + _number(count)
        for name in _SETUP_MAPS:
            target_map = self.setup_maps[name]
            for key, metrics in _json_map(fields.get(name)).items():
                if not isinstance(metrics, dict):
                    metrics = {}
                target = target_map.setdefault(key, {"count": 0.0, "ms": 0.0})
                target["count"] += _number(metrics.get("count"))
                target["ms"] += _number(metrics.get("ms"))
        for route_id, metrics in _json_map(fields.get("route_lookup_stats_json")).items():
            if not isinstance(metrics, dict):
                continue
            target = self.routes.setdefault(
                route_id, dict.fromkeys(_ROUTE_FIELDS, 0.0)
            )
            for metric in _ROUTE_FIELDS:
                target[metric] += max(0.0, _number(metrics.get(metric)))

    def payload(self, ttl_seconds: int, worker_id: str) -> str:
        return json.dumps(
            {
                "n": len(self.records) // 5,
                "ttl": ttl_seconds,
                "first_started": self.first_started,
                "last_finished": self.last_finished,
                "ints": self.ints,
                # Strings keep full precision through Lua (numbers would be
                # formatted with 14 significant digits).
                "floats": {key: repr(value) for key, value in self.floats.items()},
                "maxes": {key: value for key, value in self.maxes.items() if value > 0},
                "records": self.records,
                "worker_id": worker_id,
                "worker_limit": self.worker_limit,
                "metric_maps": self.metric_maps,
                "counter_map": self.counter_map,
                "setup_maps": self.setup_maps,
                "routes": self.routes,
            },
            separators=(",", ":"),
        )


# Applies one _TaskTelemetryBatch exactly as the per-task script would apply
# its tasks one by one, except that the ready depth is read once per batch.
_BATCH_SCRIPT = """
local p = cjson.decode(ARGV[1])
local tkey, skey, wkey = KEYS[1], KEYS[2], KEYS[3]
local n = tonumber(p.n)
local function hmax(key, field, value)
  local current = tonumber(redis.call('HGET', key, field) or '0')
  if value > current then redis.call('HSET', key, field, value) end
end
local current_first = tonumber(redis.call('HGET', tkey, 'first_started_ms') or '0')
if current_first == 0 or p.first_started < current_first then
  redis.call('HSET', tkey, 'first_started_ms', p.first_started)
end
hmax(tkey, 'last_finished_ms', p.last_finished)
local progress_seq = tonumber(redis.call('HGET', tkey, 'task_count') or '0')
local succeeded_seq = tonumber(redis.call('HGET', tkey, 'succeeded_tasks') or '0')
redis.call('HINCRBY', tkey, 'task_count', n)
for field, value in pairs(p.ints) do
  if value > 0 or (field ~= 'succeeded_tasks' and field ~= 'failed_tasks') then
    redis.call('HINCRBY', tkey, field, value)
  end
end
for field, value in pairs(p.floats) do
  redis.call('HINCRBYFLOAT', tkey, field, value)
end
local ready_depth = tonumber(redis.call('HGET', skey, 'ready_tasks') or '0')
redis.call('HINCRBY', tkey, 'ready_samples', n)
redis.call('HINCRBYFLOAT', tkey, 'ready_depth_sum', ready_depth * n)
hmax(tkey, 'ready_depth_max', ready_depth)
if ready_depth > 0 then
  redis.call('HINCRBY', tkey, 'ready_nonempty_samples', n)
end
local steady_first = tonumber(redis.call('HGET', tkey, 'steady_first_finished_ms') or '0')
local steady_last = tonumber(redis.call('HGET', tkey, 'steady_last_finished_ms') or '0')
local first_counts, last_counts = nil, nil
local steady_count, steady_sum, terminal_basis = 0, 0, false
local worker_id = p.worker_id
local last_field = worker_id .. ':last'
local worker_last = 0
if worker_id ~= '' then
  worker_last = tonumber(redis.call('HGET', wkey, last_field) or '0')
end
local worker_last_changed, max_gap = false, 0
local records = p.records
for i = 1, #records, 5 do
  local finished, duration = records[i], records[i + 1]
  local completed, total = records[i + 3], records[i + 4]
  progress_seq = progress_seq + 1
  if records[i + 2] == 1 then succeeded_seq = succeeded_seq + 1 end
  local progress = completed
  if completed < 0 then
    completed = succeeded_seq
    progress = progress_seq
    terminal_basis = true
  end
  if total > 0 and progress * 10 >= total and progress * 10 <= total * 9 then
    steady_count = steady_count + 1
    steady_sum = steady_sum + duration
    if steady_first == 0 or finished < steady_first then
      steady_first = finished
      first_counts = {completed, progress}
    end
    if finished > steady_last then
      steady_last = finished
      last_counts = {completed, progress}
    end
  end
  if worker_id ~= '' and finished > worker_last then
    if worker_last > 0 and finished - worker_last > max_gap then
      max_gap = finished - worker_last
    end
    worker_last = finished
    worker_last_changed = true
  end
end
if terminal_basis then
  redis.call('HSET', tkey, 'steady_window_basis', 'terminal_tasks')
end
if steady_count > 0 then
  redis.call('HINCRBY', tkey, 'steady_task_count', steady_count)
  redis.call('HINCRBYFLOAT', tkey, 'steady_task_duration_sum_ms', steady_sum)
end
if first_counts then
  redis.call('HSET', tkey, 'steady_first_finished_ms', steady_first,
    'steady_first_completed_count', first_counts[1],
    'steady_first_progress_count', first_counts[2])
end
if last_counts then
  redis.call('HSET', tkey, 'steady_last_finished_ms', steady_last,
    'steady_last_completed_count', last_counts[1],
    'steady_last_progress_count', last_counts[2])
end
for field, value in pairs(p.maxes) do
  hmax(tkey, field, value)
end
if worker_id ~= '' then
  local first_field = worker_id .. ':first'
  local worker_first = tonumber(redis.call('HGET', wkey, first_field) or '0')
  if worker_first == 0 or p.first_started < worker_first then
    redis.call('HSET', wkey, first_field, p.first_started)
  end
  if max_gap > 0 then hmax(tkey, 'max_worker_completion_gap_ms', max_gap) end
  if worker_last_changed then redis.call('HSET', wkey, last_field, worker_last) end
  redis.call('HSET', wkey, worker_id .. ':limit', p.worker_limit)
  redis.call('EXPIRE', wkey, p.ttl)
end
local function decode_field(field)
  return cjson.decode(redis.call('HGET', tkey, field) or '{}')
end
for field, incoming in pairs(p.metric_maps) do
  local current = decode_field(field)
  for name, metrics in pairs(incoming) do
    local target = current[name] or {count = 0, batch_commands_sum = 0, batch_wall_ms_sum = 0}
    target.count = (tonumber(target.count) or 0) + metrics.count
    target.batch_commands_sum = (tonumber(target.batch_commands_sum) or 0) + metrics.batch_commands_sum
    target.batch_wall_ms_sum = (tonumber(target.batch_wall_ms_sum) or 0) + metrics.batch_wall_ms_sum
    if (tonumber(metrics.receipt_phases_count) or 0) > 0 then
      for _, phase in ipairs({'receipt_phases_count','receipt_lock_ms_sum','receipt_read_ms_sum','write_fence_ms_sum'}) do
        target[phase] = (tonumber(target[phase]) or 0) + (tonumber(metrics[phase]) or 0)
      end
    end
    current[name] = target
  end
  redis.call('HSET', tkey, field, cjson.encode(current))
end
local reasons = decode_field('writer_split_missing_reasons_json')
for name, count in pairs(p.counter_map) do
  reasons[name] = (tonumber(reasons[name]) or 0) + count
end
redis.call('HSET', tkey, 'writer_split_missing_reasons_json', cjson.encode(reasons))
for field, incoming in pairs(p.setup_maps) do
  local current = decode_field(field)
  for name, metrics in pairs(incoming) do
    local target = current[name] or {count = 0, ms = 0}
    target.count = (tonumber(target.count) or 0) + metrics.count
    target.ms = (tonumber(target.ms) or 0) + metrics.ms
    current[name] = target
  end
  redis.call('HSET', tkey, field, cjson.encode(current))
end
for route_id, metrics in pairs(p.routes) do
  local valid_route_id = type(route_id) == 'string' and (
    route_id == 'direct' or (
      string.len(route_id) == 22 and string.match(route_id, '^route%-%x+$')
    )
  )
  if valid_route_id then
    local prefix = 'route_lookup:' .. route_id .. ':'
    for _, name in ipairs({'primary_selected_count', 'hedge_selected_count', 'fallback_selected_count', 'deferred_selected_count', 'healthy_selected_count', 'retry_selected_count', 'route_attempts', 'lookup_observations', 'lookup_found_count', 'lookup_not_found_count', 'lookup_error_count', 'lookup_timeout_count', 'lookup_cancelled_count', 'guest_init_failure_count', 'guest_init_cancelled_count'}) do
      redis.call('HINCRBY', tkey, prefix .. name, metrics[name])
    end
    redis.call('HINCRBYFLOAT', tkey, prefix .. 'route_active_ms_sum', metrics.route_active_ms)
    redis.call('HINCRBYFLOAT', tkey, prefix .. 'guest_init_ms_sum', metrics.guest_init_ms)
    redis.call('HINCRBYFLOAT', tkey, prefix .. 'lookup_ms_sum', metrics.lookup_ms)
  end
end
redis.call('EXPIRE', tkey, p.ttl)
return n
"""


class RuntimeTelemetryBatcher:
    """Sum task samples in-process and apply them with one script per flush."""

    def __init__(self, storage: Any, interval_seconds: float) -> None:
        self._storage = storage
        self._interval = interval_seconds
        self._pending: dict[tuple[str, str], _TaskTelemetryBatch] = {}
        self._lock = asyncio.Lock()
        self._script: Any = None
        self._flusher: asyncio.Task[None] | None = None

    def add(self, job_id: str, fields: dict[str, Any]) -> None:
        key = (job_id, str(fields.get("worker_id") or ""))
        batch = self._pending.get(key)
        if batch is None:
            batch = self._pending[key] = _TaskTelemetryBatch()
        batch.add(fields)
        if self._flusher is None or self._flusher.done():
            self._flusher = asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        while self._pending:
            await asyncio.sleep(self._interval)
            try:
                await self.flush()
            except Exception:
                logger.debug("Could not flush runtime telemetry", exc_info=True)

    async def flush(self) -> None:
        async with self._lock:
            pending, self._pending = self._pending, {}
            if not pending:
                return
            storage = self._storage
            client = storage._client()
            if self._script is None:
                self._script = client.register_script(_BATCH_SCRIPT)
            ttl_seconds = max(storage.completed_job_retention_seconds, 24 * 60 * 60)
            for (job_id, worker_id), batch in pending.items():
                await self._script(
                    keys=[
                        storage.JOB_RUNTIME_TELEMETRY_KEY.format(job_id=job_id),
                        storage.JOB_STATE_KEY.format(job_id=job_id),
                        storage.JOB_RUNTIME_WORKERS_KEY.format(job_id=job_id),
                    ],
                    args=[batch.payload(ttl_seconds, worker_id)],
                    client=client,
                )


def _is_stale_stream_id_error(error: Exception) -> bool:
    """Whether XADD refused an explicit ID at or below the stream top."""

    return "equal or smaller" in str(error)


def _merge_setup_subphases(raw_json: str | None, task_count: int) -> dict[str, Any]:
    """Decode the accumulated setup sub-phase map into per-task means.

    Worker Setup spans are summed across every task, so each entry is divided
    by task_count to stay comparable with the other mean_* telemetry fields.
    Sub-phases that fire rarely (JOB_STARTED, persist_job) would round to zero
    on that basis, so mean_ms_per_observation reports their individual cost.
    """

    try:
        payload = json.loads(raw_json or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict) or task_count <= 0:
        return {}
    merged: dict[str, Any] = {}
    for name, metrics in payload.items():
        if not isinstance(metrics, dict):
            continue
        try:
            total_ms = max(0.0, float(metrics.get("ms") or 0.0))
            count = max(0, int(metrics.get("count") or 0))
        except (TypeError, ValueError):
            continue
        merged[str(name)] = {
            "mean_ms": round(total_ms / task_count, 3),
            "count": count,
            "mean_ms_per_observation": (
                round(total_ms / count, 3) if count > 0 else 0.0
            ),
        }
    return merged


class RuntimeTelemetryMixin:
    """Per-task runtime samples and their job-level aggregates for JobStorage."""

    if TYPE_CHECKING:

        def _client(self) -> Any: ...

    def _runtime_telemetry_batcher(self) -> RuntimeTelemetryBatcher | None:
        batcher = getattr(self, "_telemetry_batcher", False)
        if batcher is False:
            interval = telemetry_batch_seconds()
            batcher = RuntimeTelemetryBatcher(self, interval) if interval > 0 else None
            self._telemetry_batcher = batcher
        return batcher

    async def flush_runtime_telemetry(self) -> None:
        """Apply any batched task samples now (no-op on the per-task path)."""

        batcher = getattr(self, "_telemetry_batcher", None)
        if batcher:
            await batcher.flush()

    async def record_task_runtime_telemetry(
        self,
        job_id: str,
        *,
        started_ms: int,
        finished_ms: int,
        queue_wait_ms: float,
        setup_ms: float,
        handler_ms: float,
        writer_ms: float,
        post_ms: float,
        release_ms: float,
        route_active_ms: float,
        route_attempts: int,
        success: bool,
        completed_count: int | None = 0,
        total_tasks: int = 0,
        claim_wait_ms: float = 0.0,
        worker_id: str = "",
        expected_workers: int = 0,
        effective_worker_concurrency: int = 0,
        writer_admission_ms: float = 0.0,
        writer_queue_ms: float = 0.0,
        writer_execution_ms: float = 0.0,
        writer_commit_ms: float = 0.0,
        writer_overhead_ms: float = 0.0,
        writer_delivery_ms: float = 0.0,
        writer_timed_submits: int = 0,
        writer_timing_coverage: int = 0,
        writer_connection_checkout_ms: float = 0.0,
        writer_receipt_lookup_ms: float = 0.0,
        writer_receipt_finalize_ms: float = 0.0,
        writer_payload_ms: float = 0.0,
        writer_user_row_ms: float = 0.0,
        writer_alias_ms: float = 0.0,
        writer_authority_ms: float = 0.0,
        writer_profile_ms: float = 0.0,
        writer_social_ms: float = 0.0,
        writer_flush_ms: float = 0.0,
        writer_queue_commands_sum: float = 0.0,
        writer_queue_commands_max: int = 0,
        writer_queue_bytes_sum: float = 0.0,
        writer_queue_bytes_max: int = 0,
        writer_active_lanes_sum: float = 0.0,
        writer_active_lanes_max: int = 0,
        writer_batch_commands_sum: float = 0.0,
        writer_configured_concurrency: int = 0,
        writer_configured_batch_size: int = 0,
        writer_queue_max_commands: int = 0,
        writer_queue_max_bytes: int = 0,
        writer_refill_min_commands: int = 0,
        handler_guest_init_ms: float = 0.0,
        handler_guest_init_count: int = 0,
        handler_initial_lookup_ms: float = 0.0,
        handler_initial_lookup_count: int = 0,
        handler_refresh_api_ms: float = 0.0,
        handler_refresh_api_count: int = 0,
        handler_fallback_retry_ms: float = 0.0,
        handler_fallback_retry_count: int = 0,
        handler_serialization_ms: float = 0.0,
        handler_serialization_count: int = 0,
        setup_subphases_json: str = "{}",
        writer_admission_serialization_ms: float = 0.0,
        writer_admission_capacity_wait_ms: float = 0.0,
        writer_admission_redis_ms: float = 0.0,
        writer_admission_overhead_detail_ms: float = 0.0,
        writer_batch_fill_ratio_sum: float = 0.0,
        writer_lane_idle_ms: float = 0.0,
        writer_lane_predecessor_wait_ms: float = 0.0,
        writer_refill_wait_ms: float = 0.0,
        writer_window_commands: int = 0,
        writer_refill_min_commands_per_lane: int = 0,
        writer_result_publication_ms: float = 0.0,
        writer_result_observation_ms: float = 0.0,
        writer_result_delivery_residual_ms: float = 0.0,
        writer_result_delivery_split_count: int = 0,
        writer_batch_peer_work_ms: float = 0.0,
        writer_operation_stats_json: str = "{}",
        writer_domain_stats_json: str = "{}",
        writer_split_missing_reasons_json: str = "{}",
        writer_result_cache_hits: int = 0,
        writer_receipt_fallbacks: int = 0,
        writer_authority_rows_examined: float = 0.0,
        writer_authority_rows_skipped: float = 0.0,
        writer_authority_rows_upserted: float = 0.0,
        route_lookup_stats_json: str = "{}",
        guest_auth_stats_json: str = "{}",
        terminal_stats_json: str = "{}",
        ledger_stats_json: str = "{}",
    ) -> None:
        """Atomically retain bounded full-job timing and occupancy integrals.

        The sum of task wall times is the area under the in-flight concurrency
        curve. Route-attempt wall time is the equivalent area for active guest
        routes. Task volume only changes the aggregate counters; route timing
        fields scale with the distinct routes used by the job, not task count.
        This keeps the telemetry compact while remaining correct across workers.
        With ULTIMA_RUNTIME_TELEMETRY_BATCH_SECONDS set, samples are summed
        in-process and flushed once per interval instead.
        """

        fields = dict(locals())
        batcher = self._runtime_telemetry_batcher()
        if batcher is not None:
            del fields["self"], fields["job_id"]
            batcher.add(job_id, fields)
            return
        key = self.JOB_RUNTIME_TELEMETRY_KEY.format(job_id=job_id)
        ttl_seconds = max(self.completed_job_retention_seconds, 24 * 60 * 60)
        total_ms = max(0.0, float(finished_ms - started_ms))
        script = """
local current_first = tonumber(redis.call('HGET', KEYS[1], 'first_started_ms') or '0')
local current_last = tonumber(redis.call('HGET', KEYS[1], 'last_finished_ms') or '0')
local started = tonumber(ARGV[1])
local finished = tonumber(ARGV[2])
if current_first == 0 or started < current_first then
  redis.call('HSET', KEYS[1], 'first_started_ms', started)
end
if finished > current_last then
  redis.call('HSET', KEYS[1], 'last_finished_ms', finished)
end
redis.call('HINCRBY', KEYS[1], 'task_count', 1)
redis.call('HINCRBY', KEYS[1], ARGV[12], 1)
for i = 3, 10 do
  redis.call('HINCRBYFLOAT', KEYS[1], ARGV[i], ARGV[i + 10])
end
redis.call('HINCRBY', KEYS[1], 'route_attempts', ARGV[11])
redis.call('HINCRBYFLOAT', KEYS[1], 'claim_wait_sum_ms', ARGV[22])
local ready_depth = tonumber(redis.call('HGET', KEYS[2], 'ready_tasks') or '0')
redis.call('HINCRBY', KEYS[1], 'ready_samples', 1)
redis.call('HINCRBYFLOAT', KEYS[1], 'ready_depth_sum', ready_depth)
local ready_max = tonumber(redis.call('HGET', KEYS[1], 'ready_depth_max') or '0')
if ready_depth > ready_max then
  redis.call('HSET', KEYS[1], 'ready_depth_max', ready_depth)
end
if ready_depth > 0 then
  redis.call('HINCRBY', KEYS[1], 'ready_nonempty_samples', 1)
end
local completed_count = tonumber(ARGV[23])
local progress_count = completed_count
if completed_count < 0 then
  completed_count = tonumber(redis.call('HGET', KEYS[1], 'succeeded_tasks') or '0')
  progress_count = tonumber(redis.call('HGET', KEYS[1], 'task_count') or '0')
  redis.call('HSET', KEYS[1], 'steady_window_basis', 'terminal_tasks')
end
local total_tasks = tonumber(ARGV[24])
if total_tasks > 0 and progress_count * 10 >= total_tasks and progress_count * 10 <= total_tasks * 9 then
  redis.call('HINCRBY', KEYS[1], 'steady_task_count', 1)
  redis.call('HINCRBYFLOAT', KEYS[1], 'steady_task_duration_sum_ms', ARGV[13])
  local steady_first = tonumber(redis.call('HGET', KEYS[1], 'steady_first_finished_ms') or '0')
  local steady_last = tonumber(redis.call('HGET', KEYS[1], 'steady_last_finished_ms') or '0')
  if steady_first == 0 or finished < steady_first then
    redis.call('HSET', KEYS[1], 'steady_first_finished_ms', finished)
    redis.call('HSET', KEYS[1], 'steady_first_completed_count', completed_count)
    redis.call('HSET', KEYS[1], 'steady_first_progress_count', progress_count)
  end
  if finished > steady_last then
    redis.call('HSET', KEYS[1], 'steady_last_finished_ms', finished)
    redis.call('HSET', KEYS[1], 'steady_last_completed_count', completed_count)
    redis.call('HSET', KEYS[1], 'steady_last_progress_count', progress_count)
  end
end
local expected_workers = tonumber(ARGV[26])
local current_expected = tonumber(redis.call('HGET', KEYS[1], 'expected_workers') or '0')
if expected_workers > current_expected then
  redis.call('HSET', KEYS[1], 'expected_workers', expected_workers)
end
local worker_id = ARGV[25]
if worker_id ~= '' then
  local first_field = worker_id .. ':first'
  local last_field = worker_id .. ':last'
  local limit_field = worker_id .. ':limit'
  local worker_first = tonumber(redis.call('HGET', KEYS[3], first_field) or '0')
  local worker_last = tonumber(redis.call('HGET', KEYS[3], last_field) or '0')
  if worker_first == 0 or started < worker_first then
    redis.call('HSET', KEYS[3], first_field, started)
  end
  if finished > worker_last then
    if worker_last > 0 then
      local worker_gap = finished - worker_last
      local max_gap = tonumber(redis.call('HGET', KEYS[1], 'max_worker_completion_gap_ms') or '0')
      if worker_gap > max_gap then
        redis.call('HSET', KEYS[1], 'max_worker_completion_gap_ms', worker_gap)
      end
    end
    redis.call('HSET', KEYS[3], last_field, finished)
  end
  redis.call('HSET', KEYS[3], limit_field, ARGV[27])
  redis.call('EXPIRE', KEYS[3], ARGV[21])
end
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_admission_sum_ms', ARGV[28])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_queue_sum_ms', ARGV[29])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_execution_sum_ms', ARGV[30])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_commit_sum_ms', ARGV[31])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_overhead_sum_ms', ARGV[32])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_delivery_sum_ms', ARGV[33])
redis.call('HINCRBY', KEYS[1], 'writer_timed_submits', ARGV[34])
redis.call('HINCRBY', KEYS[1], 'writer_timing_coverage', ARGV[35])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_connection_checkout_sum_ms', ARGV[36])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_receipt_lookup_sum_ms', ARGV[37])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_receipt_finalize_sum_ms', ARGV[38])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_payload_sum_ms', ARGV[39])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_user_row_sum_ms', ARGV[40])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_alias_sum_ms', ARGV[41])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_authority_sum_ms', ARGV[42])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_profile_sum_ms', ARGV[43])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_social_sum_ms', ARGV[44])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_flush_sum_ms', ARGV[45])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_queue_commands_sum', ARGV[46])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_queue_bytes_sum', ARGV[48])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_active_lanes_sum', ARGV[50])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_batch_commands_sum', ARGV[52])
local max_fields = {'writer_queue_commands_max', 'writer_queue_bytes_max', 'writer_active_lanes_max', 'writer_configured_concurrency', 'writer_configured_batch_size', 'writer_queue_max_commands', 'writer_queue_max_bytes', 'writer_refill_min_commands'}
local max_args = {47, 49, 51, 53, 54, 55, 56, 57}
for i = 1, #max_fields do
  local candidate = tonumber(ARGV[max_args[i]])
  local current = tonumber(redis.call('HGET', KEYS[1], max_fields[i]) or '0')
  if candidate > current then redis.call('HSET', KEYS[1], max_fields[i], candidate) end
end
redis.call('HINCRBYFLOAT', KEYS[1], 'handler_guest_init_sum_ms', ARGV[58])
redis.call('HINCRBY', KEYS[1], 'handler_guest_init_count', ARGV[59])
redis.call('HINCRBYFLOAT', KEYS[1], 'handler_initial_lookup_sum_ms', ARGV[60])
redis.call('HINCRBY', KEYS[1], 'handler_initial_lookup_count', ARGV[61])
redis.call('HINCRBYFLOAT', KEYS[1], 'handler_refresh_api_sum_ms', ARGV[62])
redis.call('HINCRBY', KEYS[1], 'handler_refresh_api_count', ARGV[63])
redis.call('HINCRBYFLOAT', KEYS[1], 'handler_fallback_retry_sum_ms', ARGV[64])
redis.call('HINCRBY', KEYS[1], 'handler_fallback_retry_count', ARGV[65])
redis.call('HINCRBYFLOAT', KEYS[1], 'handler_serialization_sum_ms', ARGV[66])
redis.call('HINCRBY', KEYS[1], 'handler_serialization_count', ARGV[67])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_admission_serialization_sum_ms', ARGV[68])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_admission_capacity_wait_sum_ms', ARGV[69])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_admission_redis_sum_ms', ARGV[70])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_admission_overhead_detail_sum_ms', ARGV[71])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_batch_fill_ratio_sum', ARGV[72])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_lane_idle_sum_ms', ARGV[73])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_lane_predecessor_wait_sum_ms', ARGV[74])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_refill_wait_sum_ms', ARGV[75])
local scheduler_max_fields = {'writer_window_commands', 'writer_refill_min_commands_per_lane'}
local scheduler_max_args = {76, 77}
for i = 1, #scheduler_max_fields do
  local candidate = tonumber(ARGV[scheduler_max_args[i]])
  local current = tonumber(redis.call('HGET', KEYS[1], scheduler_max_fields[i]) or '0')
  if candidate > current then redis.call('HSET', KEYS[1], scheduler_max_fields[i], candidate) end
end
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_result_publication_sum_ms', ARGV[78])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_result_observation_sum_ms', ARGV[79])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_result_delivery_residual_sum_ms', ARGV[80])
redis.call('HINCRBY', KEYS[1], 'writer_result_delivery_split_count', ARGV[81])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_batch_peer_work_sum_ms', ARGV[82])
local function merge_metric_map(field, arg_index)
  local current = cjson.decode(redis.call('HGET', KEYS[1], field) or '{}')
  local incoming = cjson.decode(ARGV[arg_index] or '{}')
  for name, metrics in pairs(incoming) do
    local target = current[name] or {count = 0, batch_commands_sum = 0, batch_wall_ms_sum = 0}
    target.count = (tonumber(target.count) or 0) + (tonumber(metrics.count) or 0)
    target.batch_commands_sum = (tonumber(target.batch_commands_sum) or 0) + (tonumber(metrics.batch_commands_sum) or 0)
    target.batch_wall_ms_sum = (tonumber(target.batch_wall_ms_sum) or 0) + (tonumber(metrics.batch_wall_ms_sum) or 0)
    if (tonumber(metrics.receipt_phases_count) or 0) > 0 then
      for _, phase in ipairs({'receipt_phases_count','receipt_lock_ms_sum','receipt_read_ms_sum','write_fence_ms_sum'}) do
        target[phase] = (tonumber(target[phase]) or 0) + (tonumber(metrics[phase]) or 0)
      end
    end
    current[name] = target
  end
  redis.call('HSET', KEYS[1], field, cjson.encode(current))
end
local function merge_counter_map(field, arg_index)
  local current = cjson.decode(redis.call('HGET', KEYS[1], field) or '{}')
  local incoming = cjson.decode(ARGV[arg_index] or '{}')
  for name, count in pairs(incoming) do
    current[name] = (tonumber(current[name]) or 0) + (tonumber(count) or 0)
  end
  redis.call('HSET', KEYS[1], field, cjson.encode(current))
end
merge_metric_map('writer_operation_stats_json', 83)
merge_metric_map('writer_domain_stats_json', 84)
merge_counter_map('writer_split_missing_reasons_json', 85)
redis.call('HINCRBY', KEYS[1], 'writer_result_cache_hits', ARGV[86])
redis.call('HINCRBY', KEYS[1], 'writer_receipt_fallbacks', ARGV[87])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_authority_rows_examined', ARGV[88])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_authority_rows_skipped', ARGV[89])
redis.call('HINCRBYFLOAT', KEYS[1], 'writer_authority_rows_upserted', ARGV[90])
local function merge_setup_map(field, arg_index)
  local current = cjson.decode(redis.call('HGET', KEYS[1], field) or '{}')
  local incoming = cjson.decode(ARGV[arg_index] or '{}')
  for name, metrics in pairs(incoming) do
    local target = current[name] or {count = 0, ms = 0}
    target.count = (tonumber(target.count) or 0) + (tonumber(metrics.count) or 0)
    target.ms = (tonumber(target.ms) or 0) + (tonumber(metrics.ms) or 0)
    current[name] = target
  end
  redis.call('HSET', KEYS[1], field, cjson.encode(current))
end
merge_setup_map('setup_subphases_json', 91)
merge_setup_map('guest_auth_stats_json', 93)
merge_setup_map('terminal_stats_json', 94)
merge_setup_map('ledger_stats_json', 95)
local route_stats = cjson.decode(ARGV[92] or '{}')
for route_id, metrics in pairs(route_stats) do
  local valid_route_id = type(route_id) == 'string' and (
    route_id == 'direct' or (
      string.len(route_id) == 22 and string.match(route_id, '^route%-%x+$')
    )
  )
  if valid_route_id and type(metrics) == 'table' then
    local field_prefix = 'route_lookup:' .. route_id .. ':'
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'primary_selected_count', math.max(0, tonumber(metrics.primary_selected_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'hedge_selected_count', math.max(0, tonumber(metrics.hedge_selected_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'fallback_selected_count', math.max(0, tonumber(metrics.fallback_selected_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'deferred_selected_count', math.max(0, tonumber(metrics.deferred_selected_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'healthy_selected_count', math.max(0, tonumber(metrics.healthy_selected_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'retry_selected_count', math.max(0, tonumber(metrics.retry_selected_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'route_attempts', math.max(0, tonumber(metrics.route_attempts) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'lookup_observations', math.max(0, tonumber(metrics.lookup_observations) or 0))
    redis.call('HINCRBYFLOAT', KEYS[1], field_prefix .. 'route_active_ms_sum', math.max(0, tonumber(metrics.route_active_ms) or 0))
    redis.call('HINCRBYFLOAT', KEYS[1], field_prefix .. 'guest_init_ms_sum', math.max(0, tonumber(metrics.guest_init_ms) or 0))
    redis.call('HINCRBYFLOAT', KEYS[1], field_prefix .. 'lookup_ms_sum', math.max(0, tonumber(metrics.lookup_ms) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'lookup_found_count', math.max(0, tonumber(metrics.lookup_found_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'lookup_not_found_count', math.max(0, tonumber(metrics.lookup_not_found_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'lookup_error_count', math.max(0, tonumber(metrics.lookup_error_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'lookup_timeout_count', math.max(0, tonumber(metrics.lookup_timeout_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'lookup_cancelled_count', math.max(0, tonumber(metrics.lookup_cancelled_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'guest_init_failure_count', math.max(0, tonumber(metrics.guest_init_failure_count) or 0))
    redis.call('HINCRBY', KEYS[1], field_prefix .. 'guest_init_cancelled_count', math.max(0, tonumber(metrics.guest_init_cancelled_count) or 0))
  end
end
redis.call('EXPIRE', KEYS[1], ARGV[21])
return 1
"""
        await self._client().eval(
            script,
            3,
            key,
            self.JOB_STATE_KEY.format(job_id=job_id),
            self.JOB_RUNTIME_WORKERS_KEY.format(job_id=job_id),
            int(started_ms),
            int(finished_ms),
            "task_duration_sum_ms",
            "queue_wait_sum_ms",
            "setup_sum_ms",
            "handler_sum_ms",
            "writer_sum_ms",
            "post_sum_ms",
            "release_sum_ms",
            "route_active_sum_ms",
            max(0, int(route_attempts)),
            "succeeded_tasks" if success else "failed_tasks",
            total_ms,
            max(0.0, queue_wait_ms),
            max(0.0, setup_ms),
            max(0.0, handler_ms),
            max(0.0, writer_ms),
            max(0.0, post_ms),
            max(0.0, release_ms),
            max(0.0, route_active_ms),
            ttl_seconds,
            max(0.0, claim_wait_ms),
            -1 if completed_count is None else max(0, int(completed_count)),
            max(0, int(total_tasks)),
            worker_id,
            max(0, int(expected_workers)),
            max(0, int(effective_worker_concurrency)),
            max(0.0, writer_admission_ms),
            max(0.0, writer_queue_ms),
            max(0.0, writer_execution_ms),
            max(0.0, writer_commit_ms),
            max(0.0, writer_overhead_ms),
            max(0.0, writer_delivery_ms),
            max(0, int(writer_timed_submits)),
            max(0, int(writer_timing_coverage)),
            max(0.0, writer_connection_checkout_ms),
            max(0.0, writer_receipt_lookup_ms),
            max(0.0, writer_receipt_finalize_ms),
            max(0.0, writer_payload_ms),
            max(0.0, writer_user_row_ms),
            max(0.0, writer_alias_ms),
            max(0.0, writer_authority_ms),
            max(0.0, writer_profile_ms),
            max(0.0, writer_social_ms),
            max(0.0, writer_flush_ms),
            max(0.0, writer_queue_commands_sum),
            max(0, int(writer_queue_commands_max)),
            max(0.0, writer_queue_bytes_sum),
            max(0, int(writer_queue_bytes_max)),
            max(0.0, writer_active_lanes_sum),
            max(0, int(writer_active_lanes_max)),
            max(0.0, writer_batch_commands_sum),
            max(0, int(writer_configured_concurrency)),
            max(0, int(writer_configured_batch_size)),
            max(0, int(writer_queue_max_commands)),
            max(0, int(writer_queue_max_bytes)),
            max(0, int(writer_refill_min_commands)),
            max(0.0, handler_guest_init_ms),
            max(0, int(handler_guest_init_count)),
            max(0.0, handler_initial_lookup_ms),
            max(0, int(handler_initial_lookup_count)),
            max(0.0, handler_refresh_api_ms),
            max(0, int(handler_refresh_api_count)),
            max(0.0, handler_fallback_retry_ms),
            max(0, int(handler_fallback_retry_count)),
            max(0.0, handler_serialization_ms),
            max(0, int(handler_serialization_count)),
            max(0.0, writer_admission_serialization_ms),
            max(0.0, writer_admission_capacity_wait_ms),
            max(0.0, writer_admission_redis_ms),
            max(0.0, writer_admission_overhead_detail_ms),
            max(0.0, writer_batch_fill_ratio_sum),
            max(0.0, writer_lane_idle_ms),
            max(0.0, writer_lane_predecessor_wait_ms),
            max(0.0, writer_refill_wait_ms),
            max(0, int(writer_window_commands)),
            max(0, int(writer_refill_min_commands_per_lane)),
            max(0.0, writer_result_publication_ms),
            max(0.0, writer_result_observation_ms),
            max(0.0, writer_result_delivery_residual_ms),
            max(0, int(writer_result_delivery_split_count)),
            max(0.0, writer_batch_peer_work_ms),
            writer_operation_stats_json,
            writer_domain_stats_json,
            writer_split_missing_reasons_json,
            max(0, int(writer_result_cache_hits)),
            max(0, int(writer_receipt_fallbacks)),
            max(0.0, writer_authority_rows_examined),
            max(0.0, writer_authority_rows_skipped),
            max(0.0, writer_authority_rows_upserted),
            setup_subphases_json,
            route_lookup_stats_json,
            guest_auth_stats_json,
            terminal_stats_json,
            ledger_stats_json,
        )

    async def get_job_runtime_telemetry(self, job_id: str) -> dict[str, Any] | None:
        """Return derived full-job runtime metrics from the bounded aggregate."""

        raw = await self._client().hgetall(
            self.JOB_RUNTIME_TELEMETRY_KEY.format(job_id=job_id)
        )
        if not raw:
            return None
        decoded = {
            (key.decode() if isinstance(key, bytes) else str(key)): (
                value.decode() if isinstance(value, bytes) else str(value)
            )
            for key, value in raw.items()
        }

        def number(name: str) -> float:
            try:
                return float(decoded.get(name, 0))
            except (TypeError, ValueError):
                return 0.0

        def json_object(name: str) -> dict[str, Any]:
            try:
                value = json.loads(decoded.get(name, "{}") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
            return value if isinstance(value, dict) else {}

        def batch_stats(name: str) -> dict[str, dict[str, float | int]]:
            result: dict[str, dict[str, float | int]] = {}
            for key, raw_metrics in json_object(name).items():
                if not isinstance(raw_metrics, dict):
                    continue
                count = max(0, int(float(raw_metrics.get("count") or 0)))
                if count <= 0:
                    continue
                batch_commands_sum = max(
                    0.0, float(raw_metrics.get("batch_commands_sum") or 0.0)
                )
                batch_wall_ms_sum = max(
                    0.0, float(raw_metrics.get("batch_wall_ms_sum") or 0.0)
                )
                result[str(key)] = {
                    "count": count,
                    "mean_batch_commands": round(batch_commands_sum / count, 3),
                    "mean_batch_wall_ms": round(batch_wall_ms_sum / count, 3),
                }
                receipt_count = max(0, int(float(raw_metrics.get('receipt_phases_count') or 0)))
                if receipt_count:
                    result[str(key)]['receipt_phases_count'] = receipt_count
                    for phase in ('receipt_lock_ms', 'receipt_read_ms', 'write_fence_ms'):
                        result[str(key)]['mean_'+phase] = round(max(0.0,float(raw_metrics.get(phase+'_sum') or 0.0))/receipt_count, 3)
            return result

        task_count = int(number("task_count"))
        first_started_ms = int(number("first_started_ms"))
        last_finished_ms = int(number("last_finished_ms"))
        active_window_ms = max(0, last_finished_ms - first_started_ms)
        task_duration_sum_ms = number("task_duration_sum_ms")
        route_active_sum_ms = number("route_active_sum_ms")
        handler_sum_ms = number("handler_sum_ms")
        writer_sum_ms = number("writer_sum_ms")
        steady_first_finished_ms = int(number("steady_first_finished_ms"))
        steady_last_finished_ms = int(number("steady_last_finished_ms"))
        steady_window_ms = max(0, steady_last_finished_ms - steady_first_finished_ms)
        steady_completed_delta = max(
            0,
            int(number("steady_last_completed_count"))
            - int(number("steady_first_completed_count")),
        )
        steady_task_count = int(number("steady_task_count"))
        steady_progress_delta = max(
            0,
            int(number("steady_last_progress_count"))
            - int(number("steady_first_progress_count")),
        )
        steady_task_duration_sum_ms = number("steady_task_duration_sum_ms")
        ready_samples = int(number("ready_samples"))

        raw_workers = await self._client().hgetall(
            self.JOB_RUNTIME_WORKERS_KEY.format(job_id=job_id)
        )
        worker_fields = {
            (key.decode() if isinstance(key, bytes) else str(key)): float(
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in raw_workers.items()
        }
        worker_ids = {field.rsplit(":", 1)[0] for field in worker_fields}
        worker_time_area_ms = 0.0
        worker_capacity_area_ms = 0.0
        for worker_id in worker_ids:
            worker_first = worker_fields.get(f"{worker_id}:first", 0.0)
            worker_last = worker_fields.get(f"{worker_id}:last", 0.0)
            worker_limit = worker_fields.get(f"{worker_id}:limit", 0.0)
            participating_ms = max(
                0.0,
                min(worker_last, float(last_finished_ms))
                - max(worker_first, float(first_started_ms)),
            )
            worker_time_area_ms += participating_ms
            worker_capacity_area_ms += participating_ms * worker_limit

        def mean(name: str) -> float | None:
            return round(number(name) / task_count, 3) if task_count > 0 else None

        writer_timed_submits = number("writer_timed_submits")

        def writer_mean(name: str) -> float | None:
            return (
                round(number(name) / writer_timed_submits, 3)
                if writer_timed_submits > 0 else None
            )

        route_lookup_stats: dict[str, dict[str, float | int | None]] = {}
        route_outcome_fields = (
            "lookup_found_count",
            "lookup_not_found_count",
            "lookup_error_count",
            "lookup_timeout_count",
            "lookup_cancelled_count",
            "guest_init_failure_count",
            "guest_init_cancelled_count",
        )
        route_metric_prefix = "route_lookup:"
        route_ids = {
            field[len(route_metric_prefix) :].partition(":")[0]
            for field in decoded
            if field.startswith(route_metric_prefix)
        }
        for route_id in route_ids:
            is_safe_route_id = route_id == "direct" or (
                len(route_id) == 22
                and route_id.startswith("route-")
                and all(character in "0123456789abcdef" for character in route_id[6:])
            )
            if not is_safe_route_id:
                continue
            field_prefix = f"{route_metric_prefix}{route_id}:"
            route_attempts = max(0, int(number(f"{field_prefix}route_attempts")))
            if route_attempts == 0:
                continue
            lookup_observations = max(
                0, int(number(f"{field_prefix}lookup_observations"))
            )
            has_route_outcomes = all(
                f"{field_prefix}{outcome_field}" in decoded
                for outcome_field in route_outcome_fields
            )
            route_outcome_counts = {
                outcome_field: max(
                    0, int(number(f"{field_prefix}{outcome_field}"))
                )
                for outcome_field in route_outcome_fields
            }
            route_outcomes_consistent = (
                has_route_outcomes
                and sum(route_outcome_counts.values()) == route_attempts
                and sum(
                    route_outcome_counts[outcome_field]
                    for outcome_field in route_outcome_fields[:5]
                ) == lookup_observations
            )
            route_lookup_stats[route_id] = {
                "primary_selected_count": int(number(f"{field_prefix}primary_selected_count")) if f"{field_prefix}primary_selected_count" in decoded else None,
                "hedge_selected_count": int(number(f"{field_prefix}hedge_selected_count")) if f"{field_prefix}hedge_selected_count" in decoded else None,
                "fallback_selected_count": int(number(f"{field_prefix}fallback_selected_count")) if f"{field_prefix}fallback_selected_count" in decoded else None,
                "deferred_selected_count": int(number(f"{field_prefix}deferred_selected_count")) if f"{field_prefix}deferred_selected_count" in decoded else None,
                "healthy_selected_count": int(number(f"{field_prefix}healthy_selected_count")) if f"{field_prefix}healthy_selected_count" in decoded else None,
                "retry_selected_count": int(number(f"{field_prefix}retry_selected_count")) if f"{field_prefix}retry_selected_count" in decoded else None,
                "route_attempts": route_attempts,
                "lookup_observations": lookup_observations,
                **{
                    outcome_field: (
                        route_outcome_counts[outcome_field]
                        if route_outcomes_consistent
                        else None
                    )
                    for outcome_field in route_outcome_fields
                },
                "mean_route_active_ms": round(
                    number(f"{field_prefix}route_active_ms_sum") / route_attempts,
                    3,
                ),
                "mean_guest_init_ms": round(
                    number(f"{field_prefix}guest_init_ms_sum") / route_attempts,
                    3,
                ),
                "mean_lookup_ms": (
                    round(
                        number(f"{field_prefix}lookup_ms_sum") / lookup_observations,
                        3,
                    )
                    if lookup_observations > 0
                    else None
                ),
            }

        return {
            "version": 14,
            "scope": "full_job_aggregate",
            "task_count": task_count,
            "succeeded_tasks": int(number("succeeded_tasks")),
            "failed_tasks": int(number("failed_tasks")),
            "first_started_at": (
                datetime.fromtimestamp(first_started_ms / 1000, timezone.utc).isoformat()
                if first_started_ms > 0 else None
            ),
            "last_finished_at": (
                datetime.fromtimestamp(last_finished_ms / 1000, timezone.utc).isoformat()
                if last_finished_ms > 0 else None
            ),
            "active_window_ms": active_window_ms,
            "task_duration_sum_ms": round(task_duration_sum_ms, 3),
            "mean_task_duration_ms": mean("task_duration_sum_ms"),
            "mean_queue_wait_ms": mean("queue_wait_sum_ms"),
            "mean_setup_ms": mean("setup_sum_ms"),
            "setup_subphases": _merge_setup_subphases(
                decoded.get("setup_subphases_json"), task_count
            ),
            "mean_handler_ms": mean("handler_sum_ms"),
            "mean_handler_exclusive_ms": (
                round(max(0.0, handler_sum_ms - writer_sum_ms) / task_count, 3)
                if task_count > 0 else None
            ),
            "guest_auth_stats": json.loads(decoded.get("guest_auth_stats_json", "{}")),
            "terminal_stats": json_object("terminal_stats_json"),
            "ledger_stats": json_object("ledger_stats_json"),
            "mean_handler_guest_init_ms": mean("handler_guest_init_sum_ms"),
            "handler_guest_init_count": int(number("handler_guest_init_count")),
            "mean_handler_initial_lookup_ms": mean("handler_initial_lookup_sum_ms"),
            "handler_initial_lookup_count": int(number("handler_initial_lookup_count")),
            "mean_handler_refresh_api_ms": mean("handler_refresh_api_sum_ms"),
            "handler_refresh_api_count": int(number("handler_refresh_api_count")),
            "mean_handler_fallback_retry_ms": mean("handler_fallback_retry_sum_ms"),
            "handler_fallback_retry_count": int(number("handler_fallback_retry_count")),
            "mean_handler_serialization_ms": mean("handler_serialization_sum_ms"),
            "handler_serialization_count": int(number("handler_serialization_count")),
            "mean_writer_ms": mean("writer_sum_ms"),
            "mean_post_ms": mean("post_sum_ms"),
            "mean_release_ms": mean("release_sum_ms"),
            "mean_claim_wait_ms": mean("claim_wait_sum_ms"),
            "mean_writer_admission_ms": mean("writer_admission_sum_ms"),
            "mean_writer_admission_serialization_ms": mean("writer_admission_serialization_sum_ms"),
            "mean_writer_admission_capacity_wait_ms": mean("writer_admission_capacity_wait_sum_ms"),
            "mean_writer_admission_redis_ms": mean("writer_admission_redis_sum_ms"),
            "mean_writer_admission_overhead_detail_ms": mean("writer_admission_overhead_detail_sum_ms"),
            "mean_writer_queue_ms": mean("writer_queue_sum_ms"),
            "mean_writer_execution_ms": mean("writer_execution_sum_ms"),
            "mean_writer_commit_ms": mean("writer_commit_sum_ms"),
            "mean_writer_overhead_ms": mean("writer_overhead_sum_ms"),
            "mean_writer_batch_peer_work_ms": mean("writer_batch_peer_work_sum_ms"),
            "mean_writer_delivery_ms": mean("writer_delivery_sum_ms"),
            "mean_writer_result_publication_ms": mean("writer_result_publication_sum_ms"),
            "mean_writer_result_observation_ms": mean("writer_result_observation_sum_ms"),
            "mean_writer_result_delivery_residual_ms": mean("writer_result_delivery_residual_sum_ms"),
            "writer_result_delivery_split_coverage_ratio": (
                round(
                    number("writer_result_delivery_split_count")
                    / number("writer_timed_submits"),
                    4,
                )
                if number("writer_timed_submits") > 0 else None
            ),
            "writer_result_cache_hits": int(number("writer_result_cache_hits")),
            "writer_receipt_fallbacks": int(number("writer_receipt_fallbacks")),
            "writer_result_cache_hit_ratio": (
                round(
                    number("writer_result_cache_hits")
                    / (
                        number("writer_result_cache_hits")
                        + number("writer_receipt_fallbacks")
                    ),
                    4,
                )
                if number("writer_result_cache_hits")
                + number("writer_receipt_fallbacks") > 0 else None
            ),
            "writer_split_missing_reasons": {
                str(key): max(0, int(float(value or 0)))
                for key, value in json_object(
                    "writer_split_missing_reasons_json"
                ).items()
            },
            "writer_operation_stats": batch_stats("writer_operation_stats_json"),
            "writer_domain_stats": batch_stats("writer_domain_stats_json"),
            "writer_timed_submits": int(number("writer_timed_submits")),
            "writer_timing_coverage_ratio": (
                round(
                    number("writer_timing_coverage")
                    / number("writer_timed_submits"),
                    4,
                )
                if number("writer_timed_submits") > 0 else None
            ),
            "mean_writer_connection_checkout_ms": mean("writer_connection_checkout_sum_ms"),
            "mean_writer_receipt_lookup_ms": mean("writer_receipt_lookup_sum_ms"),
            "mean_writer_receipt_finalize_ms": mean("writer_receipt_finalize_sum_ms"),
            "mean_writer_payload_ms": mean("writer_payload_sum_ms"),
            "mean_writer_user_row_ms": mean("writer_user_row_sum_ms"),
            "mean_writer_alias_ms": mean("writer_alias_sum_ms"),
            "mean_writer_authority_ms": mean("writer_authority_sum_ms"),
            "writer_authority_rows_examined": int(
                round(number("writer_authority_rows_examined"))
            ),
            "writer_authority_rows_skipped": int(
                round(number("writer_authority_rows_skipped"))
            ),
            "writer_authority_rows_upserted": int(
                round(number("writer_authority_rows_upserted"))
            ),
            "mean_writer_profile_ms": mean("writer_profile_sum_ms"),
            "mean_writer_social_ms": mean("writer_social_sum_ms"),
            "mean_writer_flush_ms": mean("writer_flush_sum_ms"),
            "mean_writer_queue_commands_at_start": writer_mean("writer_queue_commands_sum"),
            "max_writer_queue_commands_at_start": int(number("writer_queue_commands_max")),
            "mean_writer_queue_bytes_at_start": writer_mean("writer_queue_bytes_sum"),
            "max_writer_queue_bytes_at_start": int(number("writer_queue_bytes_max")),
            "mean_writer_active_lanes": writer_mean("writer_active_lanes_sum"),
            "max_writer_active_lanes": int(number("writer_active_lanes_max")),
            "mean_writer_batch_commands": writer_mean("writer_batch_commands_sum"),
            "mean_writer_batch_fill_ratio": writer_mean("writer_batch_fill_ratio_sum"),
            "mean_writer_lane_idle_ms": writer_mean("writer_lane_idle_sum_ms"),
            "mean_writer_lane_predecessor_wait_ms": writer_mean("writer_lane_predecessor_wait_sum_ms"),
            "mean_writer_refill_wait_ms": writer_mean("writer_refill_wait_sum_ms"),
            "writer_configured_concurrency": int(number("writer_configured_concurrency")),
            "writer_configured_batch_size": int(number("writer_configured_batch_size")),
            "writer_window_commands": int(number("writer_window_commands")),
            "writer_queue_max_commands": int(number("writer_queue_max_commands")),
            "writer_queue_max_bytes": int(number("writer_queue_max_bytes")),
            "writer_refill_min_commands": int(number("writer_refill_min_commands")),
            "writer_refill_min_commands_per_lane": int(number("writer_refill_min_commands_per_lane")),
            "max_worker_completion_gap_ms": round(
                number("max_worker_completion_gap_ms"), 3
            ),
            "ready_queue_recovery_count": int(number("ready_queue_recovery_count")),
            "ready_queue_recovered_tasks": int(number("ready_queue_recovered_tasks")),
            "time_weighted_inflight": (
                round(task_duration_sum_ms / active_window_ms, 3)
                if active_window_ms > 0 else None
            ),
            "route_attempts": int(number("route_attempts")),
            "route_active_sum_ms": round(route_active_sum_ms, 3),
            "route_lookup_stats": route_lookup_stats,
            "time_weighted_active_routes": (
                round(route_active_sum_ms / active_window_ms, 3)
                if active_window_ms > 0 else None
            ),
            "steady_task_count": steady_task_count,
            "steady_window_basis": decoded.get("steady_window_basis", "successful_tasks"),
            "steady_terminal_throughput": (
                round(steady_progress_delta / (steady_window_ms / 1000.0), 3)
                if steady_window_ms > 0
                and decoded.get("steady_window_basis") == "terminal_tasks"
                else None
            ),
            "steady_window_ms": steady_window_ms,
            "steady_throughput": (
                round(steady_completed_delta / (steady_window_ms / 1000.0), 3)
                if steady_window_ms > 0 else None
            ),
            "steady_mean_task_duration_ms": (
                round(steady_task_duration_sum_ms / steady_task_count, 3)
                if steady_task_count > 0 else None
            ),
            "ramp_window_ms": (
                max(0, steady_first_finished_ms - first_started_ms)
                if first_started_ms <= steady_first_finished_ms <= last_finished_ms else None
            ),
            "drain_window_ms": (
                max(0, last_finished_ms - steady_last_finished_ms)
                if steady_first_finished_ms > 0
                and steady_first_finished_ms <= steady_last_finished_ms <= last_finished_ms
                else None
            ),
            "ready_nonempty_ratio": (
                round(number("ready_nonempty_samples") / ready_samples, 4)
                if ready_samples > 0 else None
            ),
            "ready_nonempty_samples": int(number("ready_nonempty_samples")),
            "ready_sample_count": ready_samples,
            "mean_ready_depth": (
                round(number("ready_depth_sum") / ready_samples, 3)
                if ready_samples > 0 else None
            ),
            "max_ready_depth": int(number("ready_depth_max")),
            "workers_seen": len(worker_ids),
            "expected_workers": int(number("expected_workers")),
            "time_weighted_participating_workers": (
                round(worker_time_area_ms / active_window_ms, 3)
                if active_window_ms > 0 else None
            ),
            "time_weighted_effective_worker_capacity": (
                round(worker_capacity_area_ms / active_window_ms, 3)
                if active_window_ms > 0 else None
            ),
        }

    async def append_job_runtime_samples(
        self,
        samples: Iterable[tuple[str, dict[str, int]]],
        *,
        tick_ms: int | None = None,
    ) -> None:
        """Append bounded, timestamped queue/concurrency observations.

        With ``tick_ms`` the stream entry ID is the sampler tick, so stream order
        is tick order even when API workers sampling adjacent ticks finish out of
        order. Redis rejects an ID at or below the stream top, which drops a late
        sample for an older tick instead of appending it after a newer one.
        """

        sample_batch = list(samples)
        if not sample_batch:
            return

        client = self._client()
        pipe = client.pipeline(transaction=False)
        retention = max(self.completed_job_retention_seconds, 24 * 60 * 60)
        entry_id = f"{int(tick_ms)}-0" if tick_ms is not None else "*"
        for job_id, sample in sample_batch:
            key = self.JOB_RUNTIME_SAMPLES_KEY.format(job_id=job_id)
            fields = {name: str(int(value)) for name, value in sample.items()}
            pipe.xadd(
                key,
                fields,
                id=entry_id,
                maxlen=self.JOB_RUNTIME_SAMPLES_MAXLEN,
                approximate=True,
            )
            pipe.expire(key, retention)
        results = await pipe.execute(raise_on_error=False)
        for result in results:
            if isinstance(result, Exception) and not _is_stale_stream_id_error(result):
                raise result

    async def get_job_runtime_samples(
        self,
        job_id: str,
        *,
        after_id: str | None = None,
        limit: int = 10_000,
    ) -> dict[str, Any]:
        """Read a bounded page of timestamped runtime samples."""

        page_size = max(1, min(int(limit), self.JOB_RUNTIME_SAMPLES_MAXLEN))
        minimum = f"({after_id}" if after_id else "-"
        raw_entries = await self._client().xrange(
            self.JOB_RUNTIME_SAMPLES_KEY.format(job_id=job_id),
            min=minimum,
            max="+",
            count=page_size + 1,
        )
        has_more = len(raw_entries) > page_size
        samples: list[dict[str, Any]] = []
        for raw_id, raw_fields in raw_entries[:page_size]:
            cursor = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            fields = {
                (key.decode() if isinstance(key, bytes) else str(key)): (
                    value.decode() if isinstance(value, bytes) else str(value)
                )
                for key, value in raw_fields.items()
            }
            try:
                sample = {
                    "cursor": cursor,
                    "timestamp_ms": int(fields.get("timestamp_ms", "0")),
                    "total": int(fields.get("total", "0")),
                    "ready": int(fields.get("ready", "0")),
                    "inflight": int(fields.get("inflight", "0")),
                    "completed": int(fields.get("completed", "0")),
                    "failed": int(fields.get("failed", "0")),
                }
            except (TypeError, ValueError):
                continue
            if sample["timestamp_ms"] > 0:
                samples.append(sample)

        next_after_id = samples[-1]["cursor"] if samples else after_id
        return {
            "samples": samples,
            "next_after_id": next_after_id,
            "has_more": has_more,
        }
