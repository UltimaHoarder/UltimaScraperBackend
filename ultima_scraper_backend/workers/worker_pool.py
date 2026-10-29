"""
Worker Pool System with Persistent USAPI Instances

This module implements a pool of worker processes that:
1. Start up with the system and stay alive
2. Each maintains a persistent USAPI instance initialized with guest auth
3. Pull jobs from a Redis queue
4. Add job-specific auths to their USAPI instance when executing jobs
5. Send results/progress back to backend via Redis pub/sub
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import multiprocessing
import os
import queue
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional, Protocol, cast
from uuid import uuid4

import redis as sync_redis
import psutil
import uvloop
import ultima_scraper_api
from redis.asyncio import Redis
from ultima_scraper_api import (
    SUPPORTED_SITES,
    UltimaScraperAPI,
    api_types,
)
from ultima_scraper_api.managers.redis.log_handler import RedisLogHandler

from ultima_scraper_core.typings.guards import has_find_auth
from ultima_scraper_backend.ultima_control.redis_service import RedisService

from ..redis_events import JsonValue, publish_ws_event

# Import config type
from ..config import UltimaScraperCollectionConfig
from ..config_settings_sync import get_canonical_storage_config_snapshot
from ..database import create_worker_database_manager
from ..database.write_queue import (
    DatabaseWriteClient,
    DatabaseWriteTimeout,
    WRITE_TIMING_KEY,
)
from ..job_system import BackendRouter, JobManager
from ..job_system import JobStorage as TaskJobStorage

# Event-sourced system imports
from ..job_system.event_log import (
    EventLog,
)
from ..job_system.models import (
    Job,
    Task,
)
from ..logging_utils import (
    RuntimeLogFormatter,
    configure_runtime_logging,
    create_daily_rotating_file_handler,
)
from ..redis.client import RedisManager
from ..redis.worker_registry import WorkerRegistry, WorkerStatus
from ..redis_channels import (
    AUTH_EVENTS_CHANNEL,
    CONFIG_EVENTS_CHANNEL,
    HOOKS_CHANNEL,
    WORKER_KEY_PREFIX,
)
from ..user_manager import UserManager


logger = logging.getLogger(__name__)
CONTINUABLE_TASK_FAILURE_MINIMUM = 3
CONTINUABLE_TASK_FAILURE_MAXIMUM = 25
CONTINUABLE_TASK_FAILURE_PERCENT_DIVISOR = 20  # Five percent of the batch.
CONTINUABLE_TASK_FAILURE_WINDOW_SECONDS = 60
DEFAULT_CLAIM_BATCH_SIZE = 8
CLAIM_BATCH_SIZE_ENV = "ULTIMA_CLAIM_BATCH_SIZE"


class APIProtocol(Protocol):
    async def login(self, guest: bool = False, **kwargs: Any) -> Any: ...

    def find_auth(self, account_id: int) -> Any: ...


class _TimedDatabaseWrites:
    """Per-task writer view that attributes producer-visible wait by stage."""

    def __init__(self, delegate: DatabaseWriteClient) -> None:
        self._delegate = delegate
        self.elapsed_ms = 0.0
        self.admission_ms = 0.0
        self.admission_serialization_ms = 0.0
        self.admission_capacity_wait_ms = 0.0
        self.admission_redis_ms = 0.0
        self.admission_overhead_ms = 0.0
        self.queue_ms = 0.0
        self.execution_ms = 0.0
        self.commit_ms = 0.0
        self.writer_overhead_ms = 0.0
        self.batch_peer_work_ms = 0.0
        self.delivery_ms = 0.0
        self.result_publication_ms = 0.0
        self.result_observation_ms = 0.0
        self.result_delivery_residual_ms = 0.0
        self.result_delivery_split_count = 0
        self.result_cache_hits = 0
        self.receipt_fallbacks = 0
        self.result_split_missing_reasons: dict[str, int] = {}
        self.operation_stats: dict[str, dict[str, float]] = {}
        self.domain_stats: dict[str, dict[str, float]] = {}
        self.connection_checkout_ms = 0.0
        self.receipt_lookup_ms = 0.0
        self.receipt_finalize_ms = 0.0
        self.receipt_lock_ms = self.receipt_read_ms = self.write_fence_ms = 0.0
        self.payload_ms = 0.0
        self.user_row_ms = 0.0
        self.alias_ms = 0.0
        self.authority_ms = 0.0
        self.authority_rows_examined = 0.0
        self.authority_rows_skipped = 0.0
        self.authority_rows_upserted = 0.0
        self.profile_ms = 0.0
        self.social_ms = 0.0
        self.flush_ms = 0.0
        self.queue_commands_sum = 0.0
        self.queue_commands_max = 0
        self.queue_bytes_sum = 0.0
        self.queue_bytes_max = 0
        self.active_lanes_sum = 0.0
        self.active_lanes_max = 0
        self.batch_commands_sum = 0.0
        self.batch_fill_ratio_sum = 0.0
        self.lane_idle_ms = 0.0
        self.lane_predecessor_wait_ms = 0.0
        self.refill_wait_ms = 0.0
        self.configured_concurrency = 0
        self.configured_batch_size = 0
        self.window_commands = 0
        self.refill_min_commands = 0
        self.refill_min_commands_per_lane = 0
        self.queue_max_commands = 0
        self.queue_max_bytes = 0
        self.timed_submit_count = 0
        self.timing_coverage_count = 0

    async def submit(self, *args: Any, **kwargs: Any) -> Any:
        started = time.monotonic()
        explicit_timeout = kwargs.get("timeout_seconds")
        deadline = (
            started + float(explicit_timeout)
            if explicit_timeout is not None
            else None
        )
        try:
            admission_started = time.monotonic()
            enqueue_result = await self._delegate.enqueue(
                *args,
                **kwargs,
                _include_timing=True,
            )
            admission_ms = (time.monotonic() - admission_started) * 1000.0
            command_id, admission_timing = enqueue_result
            admitted_at_ms = time.time() * 1000.0
            self.admission_ms += admission_ms
            self.admission_serialization_ms += max(
                0.0, float(admission_timing.get("producer_serialization_ms") or 0.0)
            )
            self.admission_capacity_wait_ms += max(
                0.0, float(admission_timing.get("producer_capacity_wait_ms") or 0.0)
            )
            self.admission_redis_ms += max(
                0.0, float(admission_timing.get("producer_redis_enqueue_ms") or 0.0)
            )
            self.admission_overhead_ms += max(
                0.0, float(admission_timing.get("producer_admission_overhead_ms") or 0.0)
            )
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise DatabaseWriteTimeout(
                    f"Timed out waiting for database write {command_id}; "
                    "the accepted command remains queued"
                )
            result = await self._delegate.wait_for_result(
                command_id,
                timeout_seconds=remaining,
                include_timing=True,
            )
            self.timed_submit_count += 1
            timing = result.pop(WRITE_TIMING_KEY, None)
            if isinstance(timing, dict) and timing.get("writer_timing_unavailable"):
                # A receipt fallback without stored timing: count the delivery
                # source, but leave the writer stages unmeasured, not zero.
                self.receipt_fallbacks += 1
                reason = "receipt_timing_unavailable"
                self.result_split_missing_reasons[reason] = (
                    self.result_split_missing_reasons.get(reason, 0) + 1
                )
            elif isinstance(timing, dict):
                writer_started_at_ms = float(
                    timing.get("writer_started_at_ms") or 0.0
                )
                writer_finished_at_ms = float(
                    timing.get("writer_finished_at_ms") or 0.0
                )
                queue_ms = (
                    max(0.0, writer_started_at_ms - admitted_at_ms)
                    if writer_started_at_ms > 0
                    else max(0.0, float(timing.get("writer_queue_ms") or 0.0))
                )
                execution_ms = max(0.0, float(timing.get("execution_ms") or 0.0))
                commit_ms = max(0.0, float(timing.get("commit_ms") or 0.0))
                writer_total_ms = max(
                    execution_ms + commit_ms,
                    float(timing.get("writer_total_ms") or 0.0),
                )
                observed_at_ms = time.time() * 1000.0
                result_publish_started_at_ms = float(
                    timing.get("writer_result_publish_started_at_ms") or 0.0
                )
                result_available_at_ms = float(
                    timing.get("writer_result_available_at_ms") or 0.0
                )
                result_observed_at_ms = float(
                    timing.get("writer_result_observed_at_ms") or 0.0
                )
                result_source = str(timing.get("writer_result_source") or "unknown")
                if result_source == "cache":
                    self.result_cache_hits += 1
                elif result_source == "receipt":
                    self.receipt_fallbacks += 1
                self.queue_ms += queue_ms
                self.execution_ms += execution_ms
                self.commit_ms += commit_ms
                self.writer_overhead_ms += max(
                    0.0,
                    float(timing.get("writer_batch_overhead_ms") or 0.0),
                )
                self.batch_peer_work_ms += max(
                    0.0,
                    float(timing.get("writer_batch_peer_work_ms") or 0.0),
                )
                if (
                    0 < result_publish_started_at_ms <= result_available_at_ms
                    <= result_observed_at_ms
                ):
                    publication_ms = (
                        result_available_at_ms - result_publish_started_at_ms
                    )
                    observation_ms = result_observed_at_ms - result_available_at_ms
                    delivery_ms = publication_ms + observation_ms
                    self.result_publication_ms += publication_ms
                    self.result_observation_ms += observation_ms
                    self.result_delivery_split_count += 1
                else:
                    delivery_ms = (
                        max(0.0, observed_at_ms - writer_finished_at_ms)
                        if writer_finished_at_ms > 0
                        else max(
                            0.0,
                            (time.monotonic() - started) * 1000.0
                            - admission_ms - queue_ms - writer_total_ms,
                        )
                    )
                    self.result_delivery_residual_ms += delivery_ms
                    missing_reason = str(
                        timing.get("writer_result_split_missing_reason")
                        or "clock_order_invalid"
                    )
                    self.result_split_missing_reasons[missing_reason] = (
                        self.result_split_missing_reasons.get(missing_reason, 0) + 1
                    )
                self.delivery_ms += delivery_ms
                for stats, key in (
                    (self.operation_stats, str(timing.get("writer_operation") or "unknown")),
                    (self.domain_stats, str(timing.get("writer_lane_domain") or "unknown")),
                ):
                    aggregate = stats.setdefault(
                        key,
                        {
                            "count": 0.0,
                            "batch_commands_sum": 0.0,
                            "batch_wall_ms_sum": 0.0,
                        },
                    )
                    aggregate["count"] += 1.0
                    aggregate["batch_commands_sum"] += max(
                        0.0, float(timing.get("writer_batch_commands") or 0.0)
                    )
                    aggregate["batch_wall_ms_sum"] += writer_total_ms
                    if timing.get('receipt_phases_measured') is True:
                        aggregate['receipt_phases_count'] = aggregate.get('receipt_phases_count', 0.0)+1
                        for name in ('receipt_lock_ms', 'receipt_read_ms', 'write_fence_ms'):
                            aggregate[name+'_sum'] = aggregate.get(name+'_sum', 0.0)+max(0.0,float(timing.get(name) or 0.0))
                self.connection_checkout_ms += max(
                    0.0, float(timing.get("connection_checkout_ms") or 0.0)
                )
                self.receipt_lookup_ms += max(
                    0.0, float(timing.get("receipt_lookup_ms") or 0.0)
                )
                self.receipt_finalize_ms += max(
                    0.0, float(timing.get("receipt_finalize_ms") or 0.0)
                )
                for name in (
                    "payload_ms",
                    "user_row_ms",
                    "alias_ms",
                    "authority_ms",
                    "profile_ms",
                    "social_ms",
                    "flush_ms",
                ):
                    setattr(
                        self,
                        name,
                        getattr(self, name)
                        + max(0.0, float(timing.get(name) or 0.0)),
                    )
                if timing.get('receipt_phases_measured') is True:
                    for name in ('receipt_lock_ms','receipt_read_ms','write_fence_ms'):
                        setattr(self,name,getattr(self,name)+max(0.0,float(timing.get(name) or 0.0)))
                for name in (
                    "authority_rows_examined",
                    "authority_rows_skipped",
                    "authority_rows_upserted",
                ):
                    setattr(
                        self,
                        name,
                        getattr(self, name)
                        + max(0.0, float(timing.get(name) or 0.0)),
                    )
                queue_commands = max(
                    0, int(timing.get("writer_queue_commands_at_start") or 0)
                )
                queue_bytes = max(
                    0, int(timing.get("writer_queue_bytes_at_start") or 0)
                )
                active_lanes = max(
                    0, int(timing.get("writer_active_lanes") or 0)
                )
                batch_commands = max(
                    0, int(timing.get("writer_batch_commands") or 0)
                )
                self.queue_commands_sum += queue_commands
                self.queue_commands_max = max(self.queue_commands_max, queue_commands)
                self.queue_bytes_sum += queue_bytes
                self.queue_bytes_max = max(self.queue_bytes_max, queue_bytes)
                self.active_lanes_sum += active_lanes
                self.active_lanes_max = max(self.active_lanes_max, active_lanes)
                self.batch_commands_sum += batch_commands
                self.batch_fill_ratio_sum += max(
                    0.0, float(timing.get("writer_batch_fill_ratio") or 0.0)
                )
                self.lane_idle_ms += max(
                    0.0, float(timing.get("writer_lane_idle_ms") or 0.0)
                )
                self.lane_predecessor_wait_ms += max(
                    0.0,
                    float(timing.get("writer_lane_predecessor_wait_ms") or 0.0),
                )
                self.refill_wait_ms += max(
                    0.0, float(timing.get("writer_refill_wait_ms") or 0.0)
                )
                self.configured_concurrency = max(
                    self.configured_concurrency,
                    int(timing.get("writer_configured_concurrency") or 0),
                )
                self.configured_batch_size = max(
                    self.configured_batch_size,
                    int(timing.get("writer_configured_batch_size") or 0),
                )
                self.window_commands = max(
                    self.window_commands,
                    int(timing.get("writer_window_commands") or 0),
                )
                self.refill_min_commands = max(
                    self.refill_min_commands,
                    int(timing.get("writer_refill_min_commands") or 0),
                )
                self.refill_min_commands_per_lane = max(
                    self.refill_min_commands_per_lane,
                    int(timing.get("writer_refill_min_commands_per_lane") or 0),
                )
                self.queue_max_commands = max(
                    self.queue_max_commands,
                    int(timing.get("writer_queue_max_commands") or 0),
                )
                self.queue_max_bytes = max(
                    self.queue_max_bytes,
                    int(timing.get("writer_queue_max_bytes") or 0),
                )
                if (
                    timing.get("execution_measured") is True
                    and timing.get("commit_measured") is True
                    and writer_started_at_ms > 0
                    and writer_finished_at_ms > 0
                ):
                    self.timing_coverage_count += 1
            return result
        finally:
            self.elapsed_ms += (time.monotonic() - started) * 1000.0

    async def enqueue(self, *args: Any, **kwargs: Any) -> Any:
        started = time.monotonic()
        try:
            return await self._delegate.enqueue(*args, **kwargs)
        finally:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            self.admission_ms += elapsed_ms
            self.elapsed_ms += elapsed_ms

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def profile_telemetry_kwargs(self) -> dict[str, float | int | str]:
        """Return the extended writer profile without duplicating call sites."""

        return {
            "writer_connection_checkout_ms": self.connection_checkout_ms,
            "writer_result_publication_ms": self.result_publication_ms,
            "writer_result_observation_ms": self.result_observation_ms,
            "writer_result_delivery_residual_ms": self.result_delivery_residual_ms,
            "writer_result_delivery_split_count": self.result_delivery_split_count,
            "writer_batch_peer_work_ms": self.batch_peer_work_ms,
            "writer_operation_stats_json": json.dumps(
                self.operation_stats,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "writer_domain_stats_json": json.dumps(
                self.domain_stats,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "writer_split_missing_reasons_json": json.dumps(
                self.result_split_missing_reasons,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "writer_result_cache_hits": self.result_cache_hits,
            "writer_receipt_fallbacks": self.receipt_fallbacks,
            "writer_admission_serialization_ms": self.admission_serialization_ms,
            "writer_admission_capacity_wait_ms": self.admission_capacity_wait_ms,
            "writer_admission_redis_ms": self.admission_redis_ms,
            "writer_admission_overhead_detail_ms": self.admission_overhead_ms,
            "writer_receipt_lookup_ms": self.receipt_lookup_ms,
            "writer_receipt_finalize_ms": self.receipt_finalize_ms,
            "writer_payload_ms": self.payload_ms,
            "writer_user_row_ms": self.user_row_ms,
            "writer_alias_ms": self.alias_ms,
            "writer_authority_ms": self.authority_ms,
            "writer_authority_rows_examined": self.authority_rows_examined,
            "writer_authority_rows_skipped": self.authority_rows_skipped,
            "writer_authority_rows_upserted": self.authority_rows_upserted,
            "writer_profile_ms": self.profile_ms,
            "writer_social_ms": self.social_ms,
            "writer_flush_ms": self.flush_ms,
            "writer_queue_commands_sum": self.queue_commands_sum,
            "writer_queue_commands_max": self.queue_commands_max,
            "writer_queue_bytes_sum": self.queue_bytes_sum,
            "writer_queue_bytes_max": self.queue_bytes_max,
            "writer_active_lanes_sum": self.active_lanes_sum,
            "writer_active_lanes_max": self.active_lanes_max,
            "writer_batch_commands_sum": self.batch_commands_sum,
            "writer_batch_fill_ratio_sum": self.batch_fill_ratio_sum,
            "writer_lane_idle_ms": self.lane_idle_ms,
            "writer_lane_predecessor_wait_ms": self.lane_predecessor_wait_ms,
            "writer_refill_wait_ms": self.refill_wait_ms,
            "writer_configured_concurrency": self.configured_concurrency,
            "writer_configured_batch_size": self.configured_batch_size,
            "writer_window_commands": self.window_commands,
            "writer_refill_min_commands": self.refill_min_commands,
            "writer_refill_min_commands_per_lane": self.refill_min_commands_per_lane,
            "writer_queue_max_commands": self.queue_max_commands,
            "writer_queue_max_bytes": self.queue_max_bytes,
        }


def _coerce_int(value: object) -> int | None:
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


def _worker_loop_factory() -> Callable[[], asyncio.AbstractEventLoop]:
    """Return the uvloop event loop factory."""
    return uvloop.new_event_loop


def _normalize_site_key(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _build_job_db_snapshot(job: Job, *, site: str | None = None) -> dict[str, Any]:
    metadata = dict(job.metadata) if isinstance(job.metadata, dict) else {}
    normalized_site = (
        _normalize_site_key(site)
        or _normalize_site_key(metadata.get("site"))
        or _normalize_site_key(metadata.get("site_name"))
    )

    snapshot: dict[str, Any] = {
        "name": job.name,
        "created_by": job.created_by,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "total_tasks": job.total_tasks,
    }
    if not metadata.get("mps_manifest"):
        snapshot["job_config"] = job.to_config(include_task_ids=False).to_dict()
    if normalized_site:
        snapshot["site"] = normalized_site
    if metadata:
        snapshot["metadata"] = metadata
    return snapshot


def _decode_pubsub_payload(raw: object) -> dict[str, Any] | None:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode()
        except UnicodeDecodeError:
            return None
    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return cast(dict[str, Any], parsed)


def _configure_worker_logging(worker_id: str, redis_url: str) -> None:
    """Configure worker logs for durable and live viewing.

    All logs are routed to:
    1. File (logs/worker_pool/debug.log) for debugging
    2. Redis (for display in the WebUI Logs panel)
    """

    root_logger = logging.getLogger()

    # Remove ALL existing handlers (console, stream, etc.)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    # Also prevent basicConfig from adding default handlers
    root_logger.handlers = []

    root_logger.setLevel(logging.INFO)
    configure_runtime_logging(f"worker-{worker_id}", console=False, file_level=logging.WARNING)

    formatter = RuntimeLogFormatter(
        f"[{worker_id}] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler for debugging when needed
    file_handler = create_daily_rotating_file_handler(
        log_type="worker_pool",
        filename="debug.log",
        level=logging.INFO,
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Redis handler publishes logs to the WebUI Logs panel.
    redis_handler = RedisLogHandler(process_name=worker_id)
    redis_handler.setFormatter(formatter)
    root_logger.addHandler(redis_handler)

    # Suppress noisy external library logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("ultima_scraper_api.managers.redis.connection").setLevel(
        logging.WARNING
    )

    # Keep direct writes from bypassing the configured log sinks.
    sys.stdout = open(os.devnull, "w")
    sys.stderr = open(os.devnull, "w")


def get_db_manager_for_worker(site: str, base_directory: Optional[Path] = None) -> Any:
    """Get a short-lived, read-only database manager for worker queries.

    Worker mutations use the durable writer stream. Worker-owned engines are
    limited to reads and never run schema creation.

    Args:
        site: The site name (e.g., "onlyfans", "fansly")
        base_directory: Base directory for user data (default: __user_data__)

    Returns:
        Read-only database manager with no retained connection pool.

    Raises:
        FileNotFoundError: If database doesn't exist (backend must create it first)
        RuntimeError: If database manager module is not available

    """
    if create_worker_database_manager is None:
        raise RuntimeError(
            "Database manager module not available. "
            "Cannot create worker database managers."
        )

    try:
        manager = create_worker_database_manager(
            site=site,
            base_directory=base_directory,
        )
        logger.info(
            f"Created database manager for worker (site: {site}, "
            "read-only, unpooled)"
        )
        return manager
    except FileNotFoundError:
        # Database doesn't exist - backend needs to create it first
        logger.error(
            f"Database for site '{site}' does not exist. "
            "Backend must initialize databases before starting workers."
        )
        raise
    except Exception as e:
        logger.error(f"Failed to create database manager for site '{site}': {e}")
        raise


class Worker:
    """
    Individual worker process that maintains a persistent USAPI instance.

    Architecture:
    - Initializes USAPI with guest auth on startup
    - Pulls jobs from Redis queue
    - Adds job-specific auths to USAPI for execution
    - Keeps USAPI instance alive between jobs
    - Publishes progress/results to Redis
    """

    def __init__(
        self,
        worker_id: str,
        redis_url: str,
        supported_sites: list[str],
        config: UltimaScraperCollectionConfig,
        backend_id: str = "default",
        worker_index: int = 0,
        worker_count: int = 1,
        reload_events_channel: str = HOOKS_CHANNEL,
    ) -> None:
        """
        Initialize worker with identity and connection details.

        Args:
            worker_id: Unique worker identifier
            redis_url: Redis connection URL
            supported_sites: List of sites this worker can handle (e.g., ["onlyfans", "fansly"])
            config: UltimaScraperCollectionConfig instance
            backend_id: Backend identifier this worker belongs to
            worker_index: Worker index (0-based) for shard calculation
            worker_count: Total number of workers for shard calculation
        """
        self.worker_id = worker_id
        self.redis_url = redis_url
        self.supported_sites = [site.lower() for site in supported_sites]
        self.backend_id = backend_id
        self.reload_events_channel = reload_events_channel
        self.running = False
        self._shutdown_event = asyncio.Event()
        self._shutdown_complete = False

        # Shard parameters for distributed auth loading
        self.worker_index = worker_index
        self.worker_count = worker_count

        # Database managers (new system) - one per supported site, read-only mode
        # NOTE: Initialized in async initialize() method, not here!
        # This ensures backend has created databases before workers try to connect
        self.db_managers: dict[str, Any] = {}

        # Store config reference
        self.config = config

        # UltimaScraperAPI instance (initialized in async initialize() method)
        self.ultima_scraper_api: Optional[UltimaScraperAPI] = None

        # Persistent USAPI site API instances (one per site)
        self.apis: dict[str, api_types] = {}
        self.redis: Optional[RedisManager] = None
        self.database_writes: DatabaseWriteClient | None = None
        self.worker_registry: Optional[WorkerRegistry] = None
        self._worker_registered = False
        self._worker_heartbeat_started = False
        self.backend_router: Optional[BackendRouter] = None
        self.task_job_storage: Optional[TaskJobStorage] = None

        # Event-sourced system components (initialized in async initialize())
        self.event_log: Optional[EventLog] = None

        # User manager - initialize now with config
        self.user_manager: UserManager = UserManager(config)
        self._config_reload_lock = asyncio.Lock()
        self._settings_reload_tasks: dict[str, asyncio.Task[None]] = {}
        self._settings_reload_latest_events: dict[str, dict[str, Any]] = {}
        self._settings_last_applied_versions: dict[str, int] = {}
        self._settings_last_applied_storage_revisions: dict[str, str] = {}
        self._storage_config_reload_tasks: set[asyncio.Task[None]] = set()
        # Long-lived pub/sub listeners; cancelled during shutdown.
        self._event_listener_tasks: list[asyncio.Task[None]] = []
        self._claim_runtime_context: dict[tuple[str, str], tuple[float, int]] = {}


    @staticmethod
    def _safe_fd_concurrency_ceiling() -> int:
        """Derive a safe upper concurrency ceiling based on the process file-descriptor limit.

        Respects explicit environment configuration via ULTIMA_WORKER_CONCURRENCY_CEILING
        or ULTIMA_WORKER_MAX_CONCURRENCY. If unset, queries resource.RLIMIT_NOFILE to protect
        against EMFILE exhaustion while allowing scaling past hardcoded clamps (e.g. 58 concurrency
        for 8 workers across 464 proxy routes).
        """
        env_cap = (
            os.environ.get("ULTIMA_WORKER_CONCURRENCY_CEILING")
            or os.environ.get("ULTIMA_WORKER_MAX_CONCURRENCY")
        )
        if env_cap and env_cap.strip().isdigit() and int(env_cap) > 0:
            return int(env_cap)
        try:
            import resource

            soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            # Reserve 128 file descriptors for SQLite, Redis, logging, stdio, and internal pipes.
            # In async HTTP scraping, each task typically consumes 1-2 descriptors.
            # Allocate at most (soft_limit - 128) // 4, clamped between 64 and 512.
            available = max(64, soft_limit - 128)
            return max(64, min(512, available // 4))
        except Exception:
            return 256

    def _worker_pool_setting(self, name: str) -> Any:
        """Read a ``worker_pool`` setting from the live config, if present."""
        settings = getattr(getattr(self, "config", None), "settings", None)
        return getattr(getattr(settings, "worker_pool", None), name, None)

    def compute_dynamic_concurrency(self, max_ceiling: int | None = None) -> int:
        """Derive worker async concurrency dynamically from the declarative proxy configuration.

        Computes:
            total_proxy_slots = sum(p.max_connections for p in enabled_proxies)
            local_concurrency = ceil(total_proxy_slots / worker_count)
        Clamped to [1, effective_ceiling] where effective_ceiling is parameterized
        and defaults to a safe file-descriptor boundary (protecting against EMFILE).
        """
        from ..deprecated_env import deprecated_env_int

        configured = deprecated_env_int(
            "ULTIMA_WORKER_CONCURRENCY"
        ) or self._worker_pool_setting("task_concurrency")
        if configured:
            return int(configured)

        configured_proxies = (
            getattr(self.config.settings.network, "proxies", []) or []
            if hasattr(self.config, "settings")
            and hasattr(self.config.settings, "network")
            else []
        )
        enabled_proxies = [
            p for p in configured_proxies if getattr(p, "enabled", True)
        ]
        if not enabled_proxies:
            return 4

        total_slots = sum(
            max(1, getattr(p, "max_connections", 8) or 8)
            for p in enabled_proxies
        )
        worker_count = max(1, getattr(self, "worker_count", 1) or 1)
        target = (total_slots + worker_count - 1) // worker_count
        effective_ceiling = (
            max_ceiling
            if max_ceiling is not None and max_ceiling > 0
            else self._safe_fd_concurrency_ceiling()
        )
        return max(1, min(effective_ceiling, target))

    def compute_claim_batch_size(self, available_slots: int) -> int:
        """Derive batch claim count for Redis ZPOPMIN.

        Defaults to 8 (claiming 5–10 tasks in a single Redis roundtrip),
        clamped to available local concurrency slots so we don't oversubscribe.
        Configurable via ULTIMA_CLAIM_BATCH_SIZE env var.
        """
        env_raw = os.environ.get(CLAIM_BATCH_SIZE_ENV)
        if env_raw and env_raw.strip().isdigit() and int(env_raw) > 0:
            target_batch = int(env_raw)
        else:
            target_batch = DEFAULT_CLAIM_BATCH_SIZE
        return max(1, min(target_batch, available_slots))


    def _init_db_managers(self) -> None:
        """Initialize database managers for all supported sites.

        Workers own read-only, non-persistent PgBouncer clients. All mutations
        are issued through ``DatabaseWriteClient`` and executed by the
        dedicated writer process.
        """
        if create_worker_database_manager is None:
            raise RuntimeError(
                f"[{self.worker_id}] Worker database manager is unavailable"
            )

        for site in self.supported_sites:
            try:
                manager = get_db_manager_for_worker(site)
                self.db_managers[site] = manager
                logger.info(
                    f"[{self.worker_id}] Initialized database manager for '{site}' "
                    "(read-only, unpooled)"
                )
            except Exception as e:
                raise RuntimeError(
                    f"[{self.worker_id}] Failed to initialize read-only database "
                    f"manager for site '{site}': {e}"
                ) from e

    def get_redis(self) -> Optional[Any]:
        """Get Redis connection for HasRedisManager protocol compatibility.

        Worker uses unified RedisManager with is_connected property.

        Returns:
            Redis client from RedisManager, or None if not initialized/connected
        """
        if self.redis is not None and self.redis.is_connected:
            return self.redis.client
        return None

    async def initialize(self) -> bool:
        """
        Initialize worker with persistent USAPI instances for all supported sites and Redis connection.

        Returns:
            True if initialization successful
        """
        try:
            worker_config = self.config
            # Reload config from database so newly added drives are visible even
            # if the config was updated after the WorkerPool serialized its
            # copy to this process.
            try:
                from ..config import UltimaScraperCollectionConfig as _Cfg

                fresh_config = _Cfg.load_or_create_default_config()
                self.config = fresh_config
                worker_config = fresh_config
                self.user_manager = UserManager(fresh_config)
                logger.debug(
                    f"[{self.worker_id}] Refreshed config and UserManager from database"
                )
            except Exception as exc:
                logger.warning(
                    f"[{self.worker_id}] Could not refresh config from database: {exc}. "
                    "Using config passed at construction time."
                )

            # Initialize database managers FIRST (after backend has created databases)
            # This is done here (not in __init__) to avoid race condition with backend
            self._init_db_managers()

            # Initialize Redis using config (more robust than parsing URL)
            redis_config = self.config.settings.redis
            if self.redis is None:
                self.redis = RedisManager(config=redis_config)
            if not self.redis.is_connected and not await self.redis.connect():
                raise RuntimeError("Worker could not connect to Redis")
            from ultima_scraper_api.managers.redis import attach_redis

            attach_redis(self.redis)
            RedisService.get_instance().attach_manager(self.redis)
            self.database_writes = DatabaseWriteClient(
                self.redis,
                producer_id=self.worker_id,
            )
            logger.info(
                f"[{self.worker_id}] Connected to Redis at {redis_config.host}:{redis_config.port}"
            )

            # Initialize backend router and storage (new unified system)
            self.backend_router = BackendRouter(self.redis)
            self.task_job_storage = TaskJobStorage(self.redis)
            self.task_job_storage.completed_job_retention_seconds = max(
                60,
                int(
                    getattr(
                        redis_config,
                        "completed_job_retention_seconds",
                        self.task_job_storage.DEFAULT_COMPLETED_JOB_RETENTION_SECONDS,
                    )
                ),
            )

            # Initialize event-sourced system components
            self.event_log = EventLog(self.redis)
            logger.info(
                f"[{self.worker_id}] Initialized event log for event-sourced execution"
            )

            # Initialize worker registry for backend-aware tracking
            self.worker_registry = WorkerRegistry(
                redis_manager=self.redis,
                backend_id=self.backend_id,
                worker_id=self.worker_id,
                worker_type="scraper",
                capabilities=[*self.supported_sites, "api_commands"],
                metadata={
                    "sites": self.supported_sites,
                    "capabilities": [*self.supported_sites, "api_commands"],
                },
            )

            # Register as soon as Redis connectivity is established so the
            # control plane can see workers during the long auth/bootstrap phase.
            await self._register_worker()

            # Initialize USAPI using the new centralized UltimaScraperAPI
            # This creates all site APIs with shared WebSocket manager
            config = worker_config
            usapi_config = (
                config.to_usapi_config()
                if hasattr(config, "to_usapi_config")
                else config
            )

            logger.debug(
                f"[{self.worker_id}] Initializing UltimaScraperAPI with centralized WebSocket manager"
            )
            self.ultima_scraper_api = ultima_scraper_api.UltimaScraperAPI(usapi_config)

            # Initialize (guest login) for supported sites only
            await self.ultima_scraper_api.init(ignore_sites=["Fansly", "LoyalFans"])

            # Store API instances from the centralized manager
            for site in self.supported_sites:
                site_name = ultima_scraper_api.get_site_title(site)
                api_instance = getattr(
                    self.ultima_scraper_api.api_instances, site_name, None
                )

                if api_instance:
                    self.apis[site] = api_instance
                    from ..job_system.guest_sessions import (
                        configure_guest_route_rate_limit,
                        configure_guest_session_pool,
                        register_guest_site_api,
                    )

                    register_guest_site_api(site_name, api_instance)
                    if site_name == "OnlyFans":

                        configure_guest_session_pool(
                            api_instance, config.settings.network.proxies,
                            enabled=config.settings.network.guest_session_pool,
                            worker_index=self.worker_index,
                            worker_count=self.worker_count,
                        )
                        configure_guest_route_rate_limit(
                            config.settings.network.guest_route_rate_limit,
                            worker_count=self.worker_count,
                            worker_index=self.worker_index,
                        )
                        from ..job_system.guest_route_permits import (
                            configure_route_permits,
                        )

                        # Every worker uses every proxy; Redis permits keep each
                        # proxy's max_connections a total across all workers.
                        configure_route_permits(
                            self.redis.client if self.redis is not None else None,
                            config.settings.network.proxies,
                        )
                        from ..job_system.guest_route_rate_pool import (
                            configure_route_rate_pool,
                        )

                        # ULTIMA_GUEST_ROUTE_RATE_POOL=1: one per-route budget
                        # for all workers instead of a fixed share each.
                        configure_route_rate_pool(
                            self.redis.client if self.redis is not None else None,
                            config.settings.network.guest_route_rate_limit,
                            worker_count=self.worker_count,
                            worker_index=self.worker_index,
                        )
                    logger.debug(f"[{self.worker_id}] Registered {site} API instance")
                else:
                    logger.error(
                        f"[{self.worker_id}] Failed to get API instance for {site}"
                    )
                logger.debug(
                    f"[{self.worker_id}] Initialized {site} API with guest auth"
                )

            # Load all available authentications from database for ALL supported sites
            # Skip API refresh during initialization to avoid throttling
            try:
                from ultima_scraper_backend.websocket_actions import (
                    create_websocket_action_handler,
                )

                from ..usapi_initializer import (
                    load_all_accounts_to_api,
                )

                # Create WebSocket action handler for this worker
                ws_handler = await create_websocket_action_handler(self)
                logger.debug(f"[{self.worker_id}] Created WebSocket action handler")

                # Load accounts for each supported site
                all_authed_users: list[Any] = []
                for site in self.supported_sites:
                    api_instance = self.apis.get(site)
                    if not api_instance:
                        logger.warning(
                            f"[{self.worker_id}] No API instance for site: {site}"
                        )
                        continue

                    # Get the database manager for this site
                    db_manager = self.db_managers.get(site)
                    if not db_manager:
                        logger.warning(
                            f"[{self.worker_id}] No database manager for site: {site}"
                        )
                        continue
                    db_engine = getattr(db_manager, "engine", None)
                    if db_engine is None:
                        logger.warning(
                            f"[{self.worker_id}] Database manager for site {site} has no engine"
                        )
                        continue

                    authed_users = cast(
                        list[Any],
                        await load_all_accounts_to_api(
                            api_instance=api_instance,  # type: ignore  # API types validated at runtime
                            engine=db_engine,
                            site=site,
                            skip_api_refresh=True,  # Skip API calls during init
                            ws_action_handler=ws_handler,  # Pass handler to register callbacks
                            worker_index=self.worker_index,  # Shard parameters
                            worker_count=self.worker_count,  # Shard parameters
                        ),
                    )
                    if authed_users:
                        all_authed_users.extend(authed_users)
                        logger.info(
                            f"[{self.worker_id}] Loaded {len(authed_users)} {site} account(s)"
                        )

                if all_authed_users:
                    total_authed_users = len(all_authed_users)
                    logger.info(
                        f"[{self.worker_id}] Loaded total {total_authed_users} authenticated account(s) from database"
                    )
                # Don't log warning here - WorkerPool will log a summary after all workers initialize
            except Exception as exc:
                logger.warning(
                    f"[{self.worker_id}] Failed to load authenticated accounts: {exc}. "
                    "Will continue with guest auth only."
                )

            # Populate UserManager with users from database BEFORE registering worker
            # This ensures users are loaded before any tasks can be claimed
            logger.info(
                f"[{self.worker_id}] Populating UserManager before registering worker..."
            )
            await self._populate_user_manager()
            logger.info(
                f"[{self.worker_id}] UserManager populated, worker ready to process tasks"
            )

            # Register worker in Redis and start heartbeats
            await self._register_worker()

            # Listen for user reloads, auth management (add/remove/update) and
            # config changes (new drives, formats, etc.).
            self._event_listener_tasks = [
                asyncio.create_task(self._listen_for_reload_events()),
                asyncio.create_task(self._listen_for_auth_events()),
                asyncio.create_task(self._listen_for_config_reload_events()),
            ]

            # Publish worker online event to hooks channel (not jobs channel)
            if self.redis:
                try:
                    from ..job_system.events import WorkerStatusEvent

                    # Create typed event
                    event = WorkerStatusEvent(
                        event="worker_online",
                        worker_id=self.worker_id,
                        sites=self.supported_sites,
                    )

                    await self.redis.publish(
                        HOOKS_CHANNEL,
                        json.dumps(event.model_dump(mode="json")),
                    )
                except Exception as exc:
                    logger.warning(
                        f"[{self.worker_id}] Failed to publish worker_online event: {exc}"
                    )

            return True

        except Exception as exc:
            if self.worker_registry is not None:
                with contextlib.suppress(Exception):
                    if self._worker_heartbeat_started:
                        await self.worker_registry.stop_heartbeat()
                    if self._worker_registered:
                        await self.worker_registry.unregister()
                self._worker_heartbeat_started = False
                self._worker_registered = False
            logger.exception(f"[{self.worker_id}] Failed to initialize: {exc}")
            return False

    async def _populate_user_manager(
        self,
        user_manager: UserManager | None = None,
        *,
        worker_count: int | None = None,
    ) -> bool:
        """Populate UserManager with users from the database (sharded by user_id)."""
        target_user_manager = user_manager or self.user_manager
        if not target_user_manager:
            logger.warning(f"[{self.worker_id}] UserManager not initialized")
            return False

        try:
            from sqlmodel import select

            from ultima_scraper_db_archive import UserAuthModel, UserModel

            # Iterate through each site's database and only load this worker's shard.
            # Users without auth rows are loaded lazily by task handlers when needed.
            user_count = 0
            auth_row_count = 0
            site_count = 0
            population_failed = False
            effective_worker_count = max(
                1,
                int(self.worker_count if worker_count is None else worker_count),
            )

            for site, db_manager in self.db_managers.items():
                site_key = site.lower()
                if site_key not in self.supported_sites:
                    continue

                try:
                    with db_manager.get_session() as session:
                        site_count += 1
                        # SQL-level sharding avoids loading entire tables into each worker.
                        statement = (
                            select(UserModel, UserAuthModel)
                            .join(UserAuthModel, UserAuthModel.user_id == UserModel.id)
                            .where(
                                UserAuthModel.site == site_key,
                                UserAuthModel.active.is_(True),
                                (UserAuthModel.user_id % effective_worker_count)
                                == self.worker_index,
                            )
                        )
                        shard_rows = list(session.exec(statement).all())
                        auth_row_count += len(shard_rows)

                        for user, auth in shard_rows:
                            user_id = user.id
                            if not user_id:
                                continue

                            usernames: list[str] = []
                            if user.username:
                                usernames.append(user.username)
                            if auth.username and auth.username not in usernames:
                                usernames.append(auth.username)
                            if not usernames:
                                continue

                            try:
                                target_user_manager.add_user(
                                    user_id=user_id,
                                    usernames=usernames,
                                    site_name=site_key,
                                )
                                user_count += 1
                            except ValueError as e:
                                logger.debug(
                                    f"[{self.worker_id}] Skipped user {user_id} for site {site_key}: {e}"
                                )
                except Exception as exc:
                    population_failed = True
                    logger.warning(
                        f"[{self.worker_id}] Failed to query sharded users from {site_key} database: {exc}"
                    )

            logger.info(
                f"[{self.worker_id}] Populated UserManager with {user_count} user entries "
                f"from {auth_row_count} sharded auth row(s) across {site_count} site DB(s) "
                f"(shard {self.worker_index}/{effective_worker_count - 1})"
            )
            return not population_failed

        except Exception as exc:
            logger.warning(f"[{self.worker_id}] Failed to populate UserManager: {exc}")
            return False

    async def _ensure_auth_loaded_for_task(self, task: Task, site: str) -> None:
        """
        Lazy-load authentication if needed.

        Option B (Hybrid Architecture):
        - Shard auths are pre-loaded during worker initialization
        - Non-shard auths are lazy-loaded here using cookie auth (no API call)
        - This allows workers to process tasks for any auth when needed
        """
        # Prefer routing fields on Task (new model). Fallback to payload for legacy callers.
        # Note: APICommandTaskPayload may use 'account_id'.
        raw_auth_id = task.auth_id
        if raw_auth_id is None and task.payload is not None:
            raw_auth_id = getattr(task.payload, "auth_id", None)
            if raw_auth_id is None:
                raw_auth_id = getattr(task.payload, "account_id", None)

        if raw_auth_id is None:
            return

        try:
            auth_id = int(raw_auth_id)
        except (ValueError, TypeError):
            logger.warning(f"[{self.worker_id}] Invalid auth_id on task: {raw_auth_id}")
            return

        site_key = site.lower()

        # Get the API instance for this site
        api = self.apis.get(site_key)
        if not api:
            logger.error(f"[{self.worker_id}] No API instance for site: {site}")
            return

        # Check if auth is already loaded using type guard
        if has_find_auth(api):
            existing_auth = api.find_auth(auth_id)
            if existing_auth:
                logger.debug(
                    f"[{self.worker_id}] Auth {auth_id} already loaded for task {task.task_id}"
                )
                return

        # Auth not loaded - need to lazy load it
        from ..usapi_initializer import (
            USAPIInitializer,
            is_auth_in_shard,
        )

        # Check if this auth should be in our shard (debugging)
        if is_auth_in_shard(auth_id, self.worker_index, self.worker_count):
            logger.warning(
                f"[{self.worker_id}] Auth {auth_id} belongs to our shard but wasn't loaded. "
                "This indicates a problem with shard initialization."
            )
        else:
            target_shard = auth_id % self.worker_count
            logger.info(
                f"[{self.worker_id}] Auth {auth_id} not in our shard "
                f"(belongs to worker {target_shard}). "
                "Lazy-loading with cookies (no API call)..."
            )

        # Lazy load using cookie-based auth (no login endpoint call, no WebSocket)
        try:
            # Get the database manager for this site
            db_manager = self.db_managers.get(site_key)
            if not db_manager:
                logger.error(
                    f"[{self.worker_id}] No database manager for site: {site}. "
                    "Cannot lazy-load auth."
                )
                return

            logger.debug(
                f"[{self.worker_id}] Attempting to lazy-load auth {auth_id} "
                f"from database for task {task.task_id}"
            )

            db_engine = getattr(db_manager, "engine", None)
            if db_engine is None:
                logger.error(
                    f"[{self.worker_id}] Database manager for site: {site} has no engine. "
                    "Cannot lazy-load auth."
                )
                return

            initializer = USAPIInitializer(db_engine)
            authed = await initializer.create_cookie_auth(
                site=site,
                account_id=auth_id,
                api_instance=api,  # type: ignore  # API types validated at runtime
            )

            if authed:
                # Double-check that auth is actually findable in the API
                verify_auth = api.find_auth(auth_id)
                if not verify_auth:
                    logger.error(
                        f"[{self.worker_id}] Auth {auth_id} created but not findable in API! "
                        f"create_cookie_auth returned auth but find_auth returns None. "
                        f"This is a critical bug."
                    )
                    return

                logger.info(
                    f"[{self.worker_id}] ✅ Successfully lazy-loaded auth {auth_id} "
                    f"for task {task.task_id} (cookie-based, verified in API)"
                )

                # Optional: attempt a lightweight login/refresh if the auth model supports it.
                # Cookie auth is often sufficient, but this helps ensure session readiness.
                login_fn = getattr(authed, "login", None)
                if callable(login_fn):
                    try:
                        import inspect

                        maybe_awaitable = login_fn()
                        if inspect.isawaitable(maybe_awaitable):
                            await maybe_awaitable
                            logger.debug(
                                f"[{self.worker_id}] Refreshed login for auth {auth_id}"
                            )
                    except Exception as login_exc:
                        logger.debug(
                            f"[{self.worker_id}] Auth {auth_id} login refresh failed: {login_exc}"
                        )
            else:
                logger.error(
                    f"[{self.worker_id}] ❌ Failed to lazy-load auth {auth_id} for task {task.task_id}. "
                    f"create_cookie_auth returned None. Check database for auth credentials."
                )
        except Exception as exc:
            logger.error(
                f"[{self.worker_id}] Error lazy-loading auth {auth_id}: {exc}",
                exc_info=True,
            )


    async def run(self) -> None:
        """Fill worker capacity exclusively with SQL-owned attempts."""
        from collections import deque

        if not self.apis or not self.task_job_storage:
            logger.error("[%s] Worker not initialized", self.worker_id)
            return
        from ..job_system.ledger_runtime import (
            LedgerRouterAdapter,
            ledger_claim_page_size,
            wait_for_worker_capacity,
            worker_claim_capacity,
        )

        if redis_url := getattr(self, "redis_url", None):
            from ..job_system import live_events

            live_events.install_redis(url=redis_url)
        router = LedgerRouterAdapter(self)
        self.running = True
        jobs_completed_count = 0
        last_heartbeat = time.monotonic()
        active_tasks: set[asyncio.Task[None]] = set()
        buffered_claims = deque()
        self._ledger_stage_changed = asyncio.Event()
        from ..job_system.idle import IdleDelay

        claim_idle = IdleDelay(initial=0.05)
        self._ledger_work_ready = asyncio.Event()

        async def run_claimed(job_id, task_id, claim_wait_ms, concurrency):
            nonlocal jobs_completed_count
            try:
                _, completed, _ = await self._execute_ready_task(
                    job_id,
                    task_id,
                    claim_wait_ms=claim_wait_ms,
                    effective_worker_concurrency=concurrency,
                )
                jobs_completed_count += int(completed)
            except Exception:
                # SQL leases recover attempts whose runner failed before finish.
                logger.exception(
                    "[%s] Task runner failed for %s/%s", self.worker_id, job_id, task_id
                )
            finally:
                self._claim_runtime_context.pop((job_id, task_id), None)
                if not buffered_claims and not any(
                    not item.done() and item is not asyncio.current_task()
                    for item in active_tasks
                ):
                    await self._update_worker_status(
                        status=WorkerStatus.IDLE.value,
                        jobs_completed=jobs_completed_count,
                    )

        logger.info("[%s] Worker started with SQL execution ledger", self.worker_id)
        try:
            while self.running and not self._shutdown_event.is_set():
                try:
                    active_tasks = {item for item in active_tasks if not item.done()}
                    concurrency = self.compute_dynamic_concurrency()
                    if time.monotonic() - last_heartbeat > 30:
                        await self._update_worker_status(
                            status=WorkerStatus.BUSY.value
                            if active_tasks
                            else WorkerStatus.IDLE.value,
                            jobs_completed=jobs_completed_count,
                        )
                        last_heartbeat = time.monotonic()
                    self._ledger_stage_changed.clear()
                    available, total_limit = worker_claim_capacity(
                        self, len(active_tasks), concurrency, router.ledger.limits.batch
                    )
                    if available == 0:
                        await wait_for_worker_capacity(
                            active_tasks, self._ledger_stage_changed
                        )
                        continue
                    if not buffered_claims:
                        # Reserve one bounded page, rather than opening another
                        # SQL transaction for each newly available fetch slot.
                        count = ledger_claim_page_size(
                            router.ledger.limits.batch,
                            self._worker_pool_setting("claim_batch_size"),
                        )
                        started = time.monotonic()
                        claims = await router.claim_ready_tasks(
                            self.backend_id, count=count
                        )
                        claim_wait_ms = (
                            (time.monotonic() - started) * 1000 / max(1, len(claims))
                        )
                        received = time.monotonic()
                        buffered_claims.extend(
                            (job_id, task_id, claim_wait_ms, received)
                            for job_id, task_id in claims
                        )
                        if not buffered_claims:
                            await claim_idle.wait(self._ledger_work_ready, idle=True)
                            continue
                        claim_idle.reset()
                    selected = [
                        buffered_claims.popleft()
                        for _ in range(min(available, len(buffered_claims)))
                    ]
                    old = [
                        (job_id, task_id)
                        for job_id, task_id, _, received in selected
                        if time.monotonic() - received >= router.ledger.limits.lease_seconds / 3
                    ]
                    try:
                        renewed = await router.renew_buffered(old) if old else set()
                    except Exception:
                        buffered_claims.extendleft(reversed(selected))
                        raise
                    for job_id, task_id, claim_wait_ms, received in selected:
                        if (job_id, task_id) in old and (job_id, task_id) not in renewed:
                            continue
                        active_tasks.add(
                            asyncio.create_task(
                                run_claimed(job_id, task_id, claim_wait_ms, total_limit)
                            )
                        )
                    await self._update_worker_status(
                        status=WorkerStatus.BUSY.value,
                        current_job=selected[0][0],
                        jobs_completed=jobs_completed_count,
                    )
                except asyncio.CancelledError:
                    break
                except Exception:
                    logger.exception("[%s] Worker loop failed", self.worker_id)
                    await asyncio.sleep(1)
        finally:
            self.running = False
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)
            batcher = getattr(self, "_ledger_lifecycle_batcher", None)
            if batcher is not None:
                await batcher.close()
            await self._update_worker_status(
                status=WorkerStatus.IDLE.value,
                jobs_completed=jobs_completed_count,
            )
            logger.info("[%s] Worker stopped", self.worker_id)


    async def _execute_ready_task(
        self,
        job_id: str,
        task_id: str,
        *,
        claim_wait_ms: float = 0.0,
        effective_worker_concurrency: int = 0,
    ) -> tuple[bool, bool, Optional[bool]]:
        """Execute only an attempt obtained through the SQL claim transaction."""
        claim_context = self._claim_runtime_context.pop((job_id, task_id), None)
        if claim_context is not None:
            claim_wait_ms, effective_worker_concurrency = claim_context
        attempt = getattr(self, "_ledger_claims", {}).pop((job_id, task_id), None)
        if attempt is None:
            logger.warning(
                "[%s] Ignoring task without a SQL claim: %s/%s",
                self.worker_id,
                job_id,
                task_id,
            )
            return False, False, None
        from ..job_system.ledger_runtime import execute

        return await execute(
            self,
            *attempt,
            claim_wait_ms=claim_wait_ms,
            effective_worker_concurrency=effective_worker_concurrency,
        )


    async def _install_runtime_storage_config(
        self,
        fresh_config: UltimaScraperCollectionConfig,
        *,
        worker_count: int | None = None,
    ) -> None:
        """Atomically replace config objects used by subsequently started tasks."""

        fresh_user_manager = UserManager(fresh_config)
        if not await self._populate_user_manager(
            fresh_user_manager,
            worker_count=worker_count,
        ):
            raise RuntimeError("could not populate the replacement UserManager")
        self.config = fresh_config
        self.user_manager = fresh_user_manager
        if worker_count is not None:
            self.worker_count = max(1, int(worker_count))

    async def _refresh_storage_config_before_task(self) -> None:
        """No-op hook retained for task setup telemetry compatibility."""
        return

    @staticmethod
    def _storage_event_payload(event: dict[str, Any]) -> dict[str, object]:
        return {
            field: event[field]
            for field in ("download_setup", "metadata_setup")
            if field in event
        }

    @staticmethod
    def _storage_event_revision(payload: dict[str, object]) -> str:
        serialized = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    async def _publish_storage_config_reload_ack(
        self,
        event: dict[str, Any],
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        request_id = str(event.get("request_id") or "").strip()
        revision = str(event.get("revision") or "").strip()
        if not self.redis or not request_id or not revision:
            return

        acknowledgement = {
            "type": "storage_config_reload_ack",
            "target_backend_id": self.backend_id,
            "settings_scope": event.get("settings_scope"),
            "config_version": event.get("config_version"),
            "worker_id": self.worker_id,
            "request_id": request_id,
            "revision": revision,
            "success": success,
        }
        if error:
            acknowledgement["error"] = error
        await self.redis.publish(
            CONFIG_EVENTS_CHANNEL,
            json.dumps(acknowledgement),
        )

    async def _apply_storage_config_update_event(
        self,
        event: dict[str, Any],
    ) -> None:
        """Apply an exact storage payload and acknowledge its revision."""

        try:
            payload = self._storage_event_payload(event)
            expected_revision = str(event.get("revision") or "").strip()
            if not payload or not expected_revision:
                raise ValueError("storage config event is missing its payload revision")
            if self._storage_event_revision(payload) != expected_revision:
                raise ValueError("storage config revision does not match its payload")

            settings_scope = str(event.get("settings_scope") or "").strip()
            config_version = _coerce_int(event.get("config_version"))
            if not settings_scope:
                raise ValueError("storage config event is missing settings_scope")
            if config_version is None or config_version <= 0:
                raise ValueError(
                    "storage config event is missing a valid config_version"
                )
            async with self._config_reload_lock:
                applied_version = self._settings_last_applied_versions.get(
                    settings_scope,
                    0,
                )
                canonical_snapshot = await asyncio.to_thread(
                    get_canonical_storage_config_snapshot,
                    settings_scope,
                    set(payload),
                )
                if canonical_snapshot is None:
                    raise ValueError(
                        "canonical storage config version is unavailable for "
                        f"scope {settings_scope}"
                    )
                canonical_revision = self._storage_event_revision(
                    canonical_snapshot.payload
                )
                if config_version > canonical_snapshot.version:
                    raise ValueError(
                        "storage config version "
                        f"{config_version} is not the canonical persisted version "
                        f"{canonical_snapshot.version}"
                    )
                if canonical_revision != expected_revision:
                    if config_version < canonical_snapshot.version:
                        raise ValueError(
                            "stale storage config version "
                            f"{config_version}; canonical version is "
                            f"{canonical_snapshot.version}"
                        )
                    raise ValueError(
                        "storage config revision does not match the canonical "
                        f"payload for version {canonical_snapshot.version}"
                    )

                current_payload: dict[str, object] = {}
                if "download_setup" in payload:
                    current_payload["download_setup"] = (
                        self.config.settings.download_setup.model_dump(mode="json")
                    )
                if "metadata_setup" in payload:
                    current_payload["metadata_setup"] = (
                        self.config.settings.metadata_setup.model_dump(mode="json")
                    )
                current_revision = self._storage_event_revision(current_payload)
                if current_revision == expected_revision:
                    self._settings_last_applied_versions[settings_scope] = max(
                        applied_version,
                        config_version,
                    )
                    self._settings_last_applied_storage_revisions[settings_scope] = (
                        expected_revision
                    )
                    await self._publish_storage_config_reload_ack(
                        event,
                        success=True,
                    )
                    return

                fresh_config = (
                    UltimaScraperCollectionConfig.load_or_create_default_config()
                )
                if "download_setup" in payload:
                    from ..config import DownloadPathSetup

                    fresh_config.settings.download_setup = DownloadPathSetup(
                        **cast(dict[str, Any], payload["download_setup"])
                    )
                if "metadata_setup" in payload:
                    from ..config import GlobalXPathSetup

                    fresh_config.settings.metadata_setup = GlobalXPathSetup(
                        **cast(dict[str, Any], payload["metadata_setup"])
                    )
                await self._install_runtime_storage_config(fresh_config)
                self._settings_last_applied_versions[settings_scope] = max(
                    applied_version,
                    config_version,
                )
                self._settings_last_applied_storage_revisions[settings_scope] = (
                    expected_revision
                )

            await self._publish_storage_config_reload_ack(event, success=True)
            logger.info(
                "[%s] Applied storage config revision %s for backend %s",
                self.worker_id,
                expected_revision or "disk",
                self.backend_id,
            )
        except Exception as exc:
            logger.warning(
                "[%s] Failed to apply storage config update: %s",
                self.worker_id,
                exc,
            )
            await self._publish_storage_config_reload_ack(
                event,
                success=False,
                error=str(exc),
            )

    async def _listen_for_config_reload_events(self) -> None:
        """Listen for settings reload events and debounce runtime refreshes.

        Handles exact, backend-targeted ``storage_config_updated`` events and
        versioned ``settings_updated`` events.
        """
        if not self.redis:
            return

        try:
            from ..redis_channels import CONFIG_EVENTS_CHANNEL

            redis_client: Redis = self.redis.client
            redis_client_any = cast(Any, redis_client)
            pubsub = redis_client_any.pubsub()
            await pubsub.subscribe(CONFIG_EVENTS_CHANNEL)
            logger.debug(
                f"[{self.worker_id}] Started listening for config reload events"
            )

            async for raw_message in pubsub.listen():
                message = cast(dict[str, Any], raw_message)
                if message.get("type") != "message":
                    continue
                try:
                    data = _decode_pubsub_payload(message.get("data"))
                    if data is None:
                        continue
                    event_type = data.get("type")
                    local_scope = self.config.settings.get_backend_id()

                    if event_type == "storage_config_updated":
                        target_backend_id = str(
                            data.get("target_backend_id") or ""
                        ).strip()
                        if target_backend_id != self.backend_id:
                            continue
                        event_scope = str(data.get("settings_scope") or "").strip()
                        if event_scope != local_scope:
                            continue
                        reload_task = asyncio.create_task(
                            self._apply_storage_config_update_event(data)
                        )
                        self._storage_config_reload_tasks.add(reload_task)
                        reload_task.add_done_callback(
                            self._storage_config_reload_tasks.discard
                        )
                        continue

                    if event_type != "settings_updated":
                        continue

                    target_backend_id = str(data.get("target_backend_id") or "").strip()
                    if target_backend_id and target_backend_id != self.backend_id:
                        continue
                    scope = str(data.get("settings_scope") or "").strip()
                    if not scope or scope != local_scope:
                        continue

                    raw_version = data.get("version")
                    version: int | None
                    try:
                        version = int(raw_version) if raw_version is not None else None
                    except (TypeError, ValueError):
                        version = None
                    if (
                        version is not None
                        and version
                        <= self._settings_last_applied_versions.get(scope, 0)
                    ):
                        continue

                    self._settings_reload_latest_events[scope] = data
                    await self._schedule_settings_reload(scope, version)
                except json.JSONDecodeError:
                    pass
                except Exception as exc:
                    logger.warning(
                        f"[{self.worker_id}] Error handling config reload event: {exc}"
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"[{self.worker_id}] Config reload listener error: {exc}")

    async def _schedule_settings_reload(self, scope: str, version: int | None) -> None:
        existing_task = self._settings_reload_tasks.get(scope)
        if existing_task is not None and not existing_task.done():
            return
        self._settings_reload_tasks[scope] = asyncio.create_task(
            self._debounced_settings_reload(scope, version)
        )

    async def _debounced_settings_reload(self, scope: str, version: int | None) -> None:
        selected_event: dict[str, Any] | None = None
        try:
            await asyncio.sleep(0.35)
            from ..config import UltimaScraperCollectionConfig as _Cfg

            selected_event = self._settings_reload_latest_events.get(scope)
            selected_version = (
                _coerce_int(selected_event.get("version"))
                if selected_event is not None
                else version
            )
            if (
                selected_version is not None
                and selected_version
                <= self._settings_last_applied_versions.get(scope, 0)
            ):
                return

            logger.info(f"[{self.worker_id}] Reloading config for scope={scope}")
            previous_worker_count = self.worker_count
            previous_enabled = self.config.settings.worker_pool.enabled
            previous_sites = [str(site).lower() for site in self.supported_sites]
            async with self._config_reload_lock:
                if (
                    selected_version is not None
                    and selected_version
                    <= self._settings_last_applied_versions.get(scope, 0)
                ):
                    return
                fresh_config = _Cfg.load_or_create_default_config()
                fresh_worker_count = max(
                    1,
                    int(fresh_config.settings.worker_pool.size),
                )
                await self._install_runtime_storage_config(
                    fresh_config,
                    worker_count=fresh_worker_count,
                )
                if selected_version is not None and selected_version > 0:
                    self._settings_last_applied_versions[scope] = selected_version

            if self.worker_count != previous_worker_count:
                logger.info(
                    f"[{self.worker_id}] Updated shard worker_count: "
                    f"{previous_worker_count} → {self.worker_count}"
                )

            fresh_sites = [
                str(site).lower() for site in fresh_config.settings.worker_pool.sites
            ]
            if fresh_sites != previous_sites:
                logger.info(
                    f"[{self.worker_id}] worker_pool.sites changed "
                    f"({previous_sites} → {fresh_sites}) but requires restart"
                )
            if fresh_config.settings.worker_pool.enabled != previous_enabled:
                logger.info(
                    f"[{self.worker_id}] worker_pool.enabled changed "
                    f"({previous_enabled} → "
                    f"{fresh_config.settings.worker_pool.enabled}) but requires restart"
                )
            if self.task_job_storage is not None:
                redis_cfg = fresh_config.settings.redis
                self.task_job_storage.completed_job_retention_seconds = max(
                    60,
                    int(
                        getattr(
                            redis_cfg,
                            "completed_job_retention_seconds",
                            self.task_job_storage.DEFAULT_COMPLETED_JOB_RETENTION_SECONDS,
                        )
                    ),
                )
            logger.info(f"[{self.worker_id}] Config and UserManager refreshed")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                f"[{self.worker_id}] Error reloading settings for scope={scope}: {exc}"
            )
        finally:
            self._settings_reload_tasks.pop(scope, None)
            latest_event = self._settings_reload_latest_events.get(scope)
            if latest_event is selected_event:
                self._settings_reload_latest_events.pop(scope, None)
            elif latest_event is not None:
                latest_version = _coerce_int(latest_event.get("version"))
                if (
                    latest_version is None
                    or latest_version
                    > self._settings_last_applied_versions.get(scope, 0)
                ):
                    await self._schedule_settings_reload(scope, latest_version)
                else:
                    self._settings_reload_latest_events.pop(scope, None)

    async def _listen_for_reload_events(self) -> None:
        """
        Listen for reload_user_data events and reload the user's auth when received.

        Pool workers receive only reload events relayed from the public hooks
        channel. Standalone workers retain the public-channel default.
        """
        if not self.redis:
            return

        try:
            # The pool relay preserves public producers without sending every
            # lifecycle hook to every worker's event loop.
            redis_client: Redis = self.redis.client
            redis_client_any = cast(Any, redis_client)
            pubsub = redis_client_any.pubsub()
            await pubsub.subscribe(getattr(self, "reload_events_channel", HOOKS_CHANNEL))

            logger.debug(f"[{self.worker_id}] Started listening for reload events")

            async for raw_message in pubsub.listen():
                message = cast(dict[str, Any], raw_message)
                if message.get("type") != "message":
                    continue

                try:
                    data = _decode_pubsub_payload(message.get("data"))
                    if data is None:
                        continue
                    from ..job_system.idle import WAKE_EVENTS

                    if data.get("event") in WAKE_EVENTS:
                        wake = getattr(self, "_ledger_work_ready", None)
                        if wake is not None:
                            wake.set()
                        continue
                    if data.get("event") != "reload_user_data":
                        continue

                    account_id = data.get("account_id")
                    account_id_int = _coerce_int(account_id)
                    username = data.get("username", "unknown")
                    site = data.get("site")

                    if not site:
                        logger.warning(
                            f"[{self.worker_id}] Reload event missing site information"
                        )
                        continue

                    site = site.lower()

                    # Check if we support this site
                    if site not in self.supported_sites:
                        logger.debug(
                            f"[{self.worker_id}] Ignoring reload for unsupported site: {site}"
                        )
                        continue
                    if account_id_int is None:
                        logger.warning(
                            f"[{self.worker_id}] Reload event missing valid account_id"
                        )
                        continue

                    logger.info(
                        f"[{self.worker_id}] Received reload request for account {account_id_int} ({username}) on {site}"
                    )

                    # Get the correct API instance for this site
                    api = self.apis.get(site)
                    if not api:
                        logger.warning(
                            f"[{self.worker_id}] No API instance for site: {site}"
                        )
                        continue

                    existing_auth = api.find_auth(account_id_int)
                    if not existing_auth:
                        logger.warning(
                            f"[{self.worker_id}] Account {account_id_int} not found in API, cannot reload"
                        )
                        continue

                    # Reload the auth by calling initialize_auth again
                    # This will load fresh subscription data from the database
                    from ..usapi_initializer import (
                        AssignedProxyResolutionError,
                        USAPIInitializer,
                        resolve_assigned_proxy_url,
                    )

                    # Get the database manager for this site
                    db_manager = self.db_managers.get(site)
                    if not db_manager:
                        logger.error(
                            f"[{self.worker_id}] No database manager for site: {site}. "
                            "Cannot reload auth."
                        )
                        continue

                    initializer = USAPIInitializer(db_manager.engine)

                    account = db_manager.get_user_auth(account_id_int)

                    if account:
                        auth_json = {
                            "id": account.user_id,
                            "username": account.username or "",
                            "cookie": account.cookie or "",
                            "x_bc": account.x_bc or "",
                            "user_agent": account.user_agent or "",
                        }
                        try:
                            proxy_url = resolve_assigned_proxy_url(account.proxy_id)
                        except AssignedProxyResolutionError as exc:
                            logger.error(
                                f"[{self.worker_id}] Assigned proxy resolution failed for account {account_id}; "
                                f"aborting reload: {exc}"
                            )
                            continue
                        if proxy_url:
                            auth_json["proxy_url"] = proxy_url
                    else:
                        auth_json = {
                            "id": account_id_int,
                            "username": username,
                            "cookie": getattr(existing_auth, "cookie", ""),
                            "x_bc": getattr(existing_auth, "x_bc", ""),
                            "user_agent": getattr(existing_auth, "user_agent", ""),
                        }

                    # Re-initialize (skip API refresh since we just saved to DB)
                    reloaded_auth = await initializer.initialize_auth(
                        site=site,
                        account_id=account_id_int,
                        api_instance=api,  # type: ignore  # API types validated at runtime
                        auth_json=auth_json,
                        skip_api_refresh=True,
                    )

                    if reloaded_auth:
                        logger.info(
                            f"[{self.worker_id}] Successfully reloaded data for account {account_id_int} "
                            f"({len(reloaded_auth.subscriptions)} subscriptions)"
                        )
                    else:
                        logger.warning(
                            f"[{self.worker_id}] Failed to reload data for account {account_id_int}"
                        )

                except json.JSONDecodeError:
                    continue
                except Exception as exc:
                    logger.warning(
                        f"[{self.worker_id}] Error processing reload event: {exc}"
                    )

        except asyncio.CancelledError:
            logger.info(f"[{self.worker_id}] Reload listener cancelled")
        except Exception as exc:
            logger.exception(f"[{self.worker_id}] Reload listener error: {exc}")

    async def _listen_for_auth_events(self) -> None:
        """
        Listen for auth management events (add/remove/update).

        This background task subscribes to ultimascraper:auth_events and handles
        auth changes using shard-based logic:
        - Shard owner: Full login with WebSocket
        - Non-shard: Do nothing (lazy load on demand)

        Multi-site: Creates one handler per supported site.
        """
        if not self.redis or not self.apis:
            logger.warning(
                f"[{self.worker_id}] Cannot listen for auth events - missing dependencies"
            )
            return

        try:
            from ..auth_events import AuthEventHandler

            # Create handler for each supported site
            handlers: dict[str, AuthEventHandler] = {}
            for site in self.supported_sites:
                api = self.apis.get(site)
                if not api:
                    logger.warning(
                        f"[{self.worker_id}] No API instance for site: {site}"
                    )
                    continue

                # Get the database manager for this site
                db_manager = self.db_managers.get(site)
                if not db_manager:
                    logger.warning(
                        f"[{self.worker_id}] No database manager for site: {site}"
                    )
                    continue
                db_engine = getattr(db_manager, "engine", None)
                if db_engine is None:
                    logger.warning(
                        f"[{self.worker_id}] Database manager for site {site} has no engine"
                    )
                    continue

                handlers[site] = AuthEventHandler(
                    worker_index=self.worker_index,
                    worker_count=self.worker_count,
                    api=api,
                    engine=db_engine,
                    site=site,
                )

            # Subscribe to auth events channel
            redis_client: Redis = self.redis.client
            redis_client_any = cast(Any, redis_client)
            pubsub = redis_client_any.pubsub()
            await pubsub.subscribe(AUTH_EVENTS_CHANNEL)

            logger.debug(
                f"[{self.worker_id}] Started listening for auth events for sites: {self.supported_sites} "
                f"(shard: {self.worker_index}/{self.worker_count - 1})"
            )

            async for raw_message in pubsub.listen():
                message = cast(dict[str, Any], raw_message)
                if message.get("type") != "message":
                    continue

                try:
                    event = _decode_pubsub_payload(message.get("data"))
                    if event is None:
                        logger.warning(f"[{self.worker_id}] Invalid JSON in auth event")
                        continue

                    # Route event to correct handler based on site
                    site = event.get("site", "onlyfans").lower()
                    handler = handlers.get(site)

                    if handler:
                        await handler.handle_event(event)
                    else:
                        logger.debug(
                            f"[{self.worker_id}] Ignoring auth event for unsupported site: {site}"
                        )
                except Exception as exc:
                    logger.warning(
                        f"[{self.worker_id}] Error processing auth event: {exc}",
                        exc_info=True,
                    )

        except asyncio.CancelledError:
            logger.info(f"[{self.worker_id}] Auth events listener cancelled")
        except Exception as exc:
            logger.exception(f"[{self.worker_id}] Auth events listener error: {exc}")

    async def _register_worker(self) -> None:
        """Register worker status in Redis and worker registry."""
        if not self.redis:
            return

        try:
            if self.worker_registry is not None:
                if not self._worker_registered:
                    self._worker_registered = await self.worker_registry.register()
                if self._worker_registered and not self._worker_heartbeat_started:
                    await self.worker_registry.start_heartbeat()
                    self._worker_heartbeat_started = True
                # WorkerRegistry already persists canonical worker info in Redis.
                return

            # Fallback path when WorkerRegistry is unavailable.
            worker_key = f"{WORKER_KEY_PREFIX}{self.worker_id}"
            worker_data: dict[str, Any] = {
                "worker_id": self.worker_id,
                "status": WorkerStatus.IDLE.value,
                "sites": self.supported_sites,  # Now reports all supported sites
                "worker_type": "multi-site",  # Changed from single site
                "backend_id": self.backend_id,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "last_seen": datetime.now(timezone.utc).isoformat(),
                "current_job": None,
                "jobs_completed": 0,
            }
            await self.redis.client.setex(
                worker_key,
                120,
                json.dumps(worker_data),  # TTL in seconds
            )
            logger.debug(f"[{self.worker_id}] Registered in Redis (fallback)")
        except Exception as exc:
            logger.warning(f"[{self.worker_id}] Failed to register worker: {exc}")

    def _collect_memory_telemetry(self) -> dict[str, int | float]:
        """Return cheap per-process/cache gauges for worker status snapshots."""
        sampled = time.monotonic()
        previous = getattr(self, "_memory_telemetry_sample", None)
        if previous is not None and sampled - previous[0] < 1.0:
            return dict(previous[1])
        process = psutil.Process(os.getpid())
        memory = process.memory_info()
        telemetry: dict[str, int | float] = {
            "rss_bytes": int(memory.rss),
            "vms_bytes": int(memory.vms),
            "memory_percent": float(process.memory_percent()),
            "memory_sampled_at": time.time(),
        }

        log_stats = [
            handler.stats() for handler in logging.getLogger().handlers
            if isinstance(handler, RedisLogHandler)
        ]
        for field, key in (
            ("log_queue_entries", "pending_entries"),
            ("log_queue_bytes", "pending_bytes"),
            ("log_dropped", "dropped"),
            ("log_publish_failures", "failed"),
        ):
            telemetry[field] = sum(int(stats[key] or 0) for stats in log_stats)

        auth_count = 0
        cached_user_count = 0
        cached_content_count = 0
        guest_auth_count = 0
        for site_api in self.apis.values():
            auths = site_api.auths
            auth_count += len(auths)
            for auth in auths.values():
                guest_auth_count += int(auth.guest)
                users = auth.users
                cached_user_count += len(users)
                for user in users.values():
                    cached_content_count += sum(
                        len(value)
                        for value in vars(user.scrape_manager.scraped).values()
                    )

        telemetry.update(
            auth_count=auth_count,
            guest_auth_count=guest_auth_count,
            cached_user_count=cached_user_count,
            cached_content_count=cached_content_count,
        )
        if self.ultima_scraper_api is not None:
            ws_stats = self.ultima_scraper_api.websocket_manager.get_statistics()
            telemetry.update(
                websocket_connections=int(ws_stats["total_connections"]),
                websocket_message_history=int(ws_stats["total_message_history"]),
                websocket_event_history=int(ws_stats["total_event_history"]),
            )
        self._memory_telemetry_sample = (sampled, dict(telemetry))
        return telemetry

    async def _update_worker_status(
        self,
        status: str = WorkerStatus.IDLE.value,
        current_job: Optional[str] = None,
        jobs_completed: int = 0,
        job_success: Optional[bool] = None,
    ) -> None:
        """Update worker status in Redis and worker registry."""
        if not self.redis:
            return
        coalesce = getattr(self.worker_registry, "status_coalesce_seconds", 0.0)
        if coalesce > 0 and job_success is None:
            # The claim loop reports BUSY on every pass; repeat reports of an
            # unchanged state within the interval carry no new information.
            state = (status.lower(), current_job)
            now = time.monotonic()
            last = getattr(self, "_last_status_report", None)
            if last is not None and last[0] == state and now - last[1] < coalesce:
                return
            self._last_status_report = (state, now)

        try:
            normalized_status = status.lower()
            memory_telemetry = self._collect_memory_telemetry()
            if self.worker_registry is not None:
                try:
                    self.worker_registry.update_metadata(memory_telemetry)
                    if normalized_status == WorkerStatus.BUSY.value:
                        # Busy state is tracked as status-only here.
                        # The canonical current task is set by _execute_ready_task().
                        await self.worker_registry.update_status(WorkerStatus.BUSY)
                    elif normalized_status == WorkerStatus.IDLE.value:
                        # _execute_ready_task() already finalizes task completion and
                        # increments counters. Avoid double-counting here.
                        await self.worker_registry.update_status(WorkerStatus.IDLE)
                    else:
                        try:
                            mapped_status = WorkerStatus(normalized_status)
                        except ValueError:
                            mapped_status = WorkerStatus.IDLE
                        await self.worker_registry.update_status(mapped_status)
                except Exception as exc:
                    logger.debug(
                        f"[{self.worker_id}] Worker registry update failed: {exc}"
                    )
            else:
                # Fallback path when WorkerRegistry is unavailable.
                worker_key = f"{WORKER_KEY_PREFIX}{self.worker_id}"
                worker_data: dict[str, Any] = {
                    "worker_id": self.worker_id,
                    "status": normalized_status,
                    "sites": self.supported_sites,  # Now reports all supported sites
                    "worker_type": "multi-site",  # Changed from single site
                    "backend_id": self.backend_id,
                    "last_seen": datetime.now(timezone.utc).isoformat(),
                    "current_job": current_job,
                    "jobs_completed": jobs_completed,
                    **memory_telemetry,
                }
                # Update with 60-second TTL
                await self.redis.client.setex(worker_key, 60, json.dumps(worker_data))

            sites_payload: list[JsonValue] = [site for site in self.supported_sites]
            payload: dict[str, JsonValue] = {
                "type": "worker_status",
                "event": "worker_heartbeat",
                "worker_id": self.worker_id,
                "status": normalized_status,
                "backend_id": self.backend_id,
                "current_job": current_job,
                "jobs_completed": jobs_completed,
                "sites": sites_payload,
            }
            for key, value in memory_telemetry.items():
                payload[key] = value
            if job_success is not None:
                payload["job_success"] = job_success
            await publish_ws_event(self.redis, payload)
        except Exception as exc:
            logger.warning(f"[{self.worker_id}] Failed to update worker status: {exc}")

    async def shutdown(self) -> None:
        """Gracefully shutdown worker."""
        if self._shutdown_complete:
            return

        logger.info(f"[{self.worker_id}] Shutting down...")
        self.running = False
        self._shutdown_event.set()
        for task in self._settings_reload_tasks.values():
            task.cancel()
        if self._settings_reload_tasks:
            await asyncio.gather(
                *self._settings_reload_tasks.values(), return_exceptions=True
            )
        self._settings_reload_tasks.clear()
        self._settings_reload_latest_events.clear()
        for task in self._storage_config_reload_tasks:
            task.cancel()
        if self._storage_config_reload_tasks:
            await asyncio.gather(
                *self._storage_config_reload_tasks,
                return_exceptions=True,
            )
        self._storage_config_reload_tasks.clear()
        for task in self._event_listener_tasks:
            task.cancel()
        if self._event_listener_tasks:
            await asyncio.gather(*self._event_listener_tasks, return_exceptions=True)
        self._event_listener_tasks.clear()
        if self.task_job_storage is not None:
            try:
                await self.task_job_storage.flush_runtime_telemetry()
            except Exception:
                logger.debug("Could not flush runtime telemetry", exc_info=True)

        # Remove worker from Redis and registry
        if self.redis:
            try:
                worker_key = f"{WORKER_KEY_PREFIX}{self.worker_id}"
                await self.redis.client.delete(worker_key)
                logger.info(f"[{self.worker_id}] Unregistered from Redis")
            except Exception as exc:
                logger.warning(f"[{self.worker_id}] Failed to unregister worker: {exc}")

            # Publish offline event
            try:
                from ..job_system.events import WorkerStatusEvent

                # Create typed event
                event = WorkerStatusEvent(
                    event="worker_offline",
                    worker_id=self.worker_id,
                )

                await self.redis.publish(
                    HOOKS_CHANNEL,
                    json.dumps(event.model_dump(mode="json")),
                )
            except Exception as exc:
                logger.warning(
                    f"[{self.worker_id}] Failed to publish worker_offline event: {exc}"
                )

        if self.worker_registry is not None:
            with contextlib.suppress(Exception):
                if self._worker_heartbeat_started:
                    await self.worker_registry.stop_heartbeat()
                if self._worker_registered:
                    await self.worker_registry.unregister()
            self.worker_registry = None
            self._worker_registered = False
            self._worker_heartbeat_started = False

        # Close connections
        if self.ultima_scraper_api is not None:
            with contextlib.suppress(Exception):
                await self.ultima_scraper_api.close()
            self.apis.clear()
            self.ultima_scraper_api = None
        for db_manager in self.db_managers.values():
            with contextlib.suppress(Exception):
                db_manager.disconnect()
        self.db_managers.clear()
        # Finish the bounded live-log queue while its transport is available.
        # Durable file handlers remain independent of broadcast admission.
        for handler in logging.getLogger().handlers:
            if isinstance(handler, RedisLogHandler):
                with contextlib.suppress(Exception):
                    await handler.aclose()
        if self.redis:
            from ultima_scraper_api.managers.redis import detach_redis

            detach_redis(self.redis)
            with contextlib.suppress(Exception):
                await self.redis.disconnect()
        self.database_writes = None

        self._shutdown_complete = True
        logger.info(f"[{self.worker_id}] Shutdown complete")


class WorkerPool:
    """
    Manages a pool of worker processes.

    Architecture:
    - Spawns N worker processes at startup
    - Each worker maintains persistent USAPI instance
    - Workers pull jobs from shared Redis queue
    - Provides methods to dispatch jobs to queue
    """

    def __init__(
        self,
        num_workers: int = 5,
        redis_url: str = "redis://localhost:6379",
        backend_id: str = "local",
    ) -> None:
        """
        Initialize worker pool.

        Args:
            num_workers: Number of worker processes to spawn
            redis_url: Redis connection URL
            backend_id: Backend identifier that new workers should register with
        """
        self.num_workers = num_workers
        self.redis_url = redis_url
        self.supported_sites = SUPPORTED_SITES
        self.backend_id = backend_id
        self.processes: list[multiprocessing.Process] = []
        self._process_context = multiprocessing.get_context("spawn")
        self.worker_ids: list[str] = []

        # Track next worker index for spawning new workers
        self._next_worker_index = 0

        # Track workers pending graceful shutdown (PID -> worker_id)
        self._pending_shutdown: dict[int, str] = {}
        self._ledger_service_thread: threading.Thread | None = None
        self._ledger_service_stop_event: threading.Event | None = None
        self._ledger_service_started_event: threading.Event | None = None
        self._ledger_service_start_error: BaseException | None = None
        self._runtime_settings_lock = threading.Lock()
        self._runtime_control_queue: queue.SimpleQueue[dict[str, Any]] = (
            queue.SimpleQueue()
        )
        self._runtime_listener_thread: threading.Thread | None = None
        self._runtime_listener_stop_event: threading.Event | None = None
        self._runtime_listener_ready_event = threading.Event()
        # Separate instances, including pools sharing a backend ID, must not
        # forward a public reload into each other's worker subscriptions.
        self._auth_reload_channel = f"{HOOKS_CHANNEL}:auth-reload:{uuid4().hex}"
        self._settings_last_applied_versions: dict[str, int] = {}
        from .readiness import WorkerReadiness

        self._readiness = WorkerReadiness(os.environ.get("USR_WORKER_READINESS_FILE"))

        # Load config for workers
        self.config = UltimaScraperCollectionConfig.load_or_create_default_config()
        self._settings_scope = self.config.settings.get_backend_id()
        self._desired_max_claimed_jobs = max(
            0, int(self.config.settings.worker_pool.max_claimed_jobs)
        )
        configured_sites = [
            str(site).lower() for site in self.config.settings.worker_pool.sites
        ]
        if configured_sites:
            # Use configured worker sites to avoid opening DB pools for unused platforms.
            self.supported_sites = configured_sites
        else:
            self.supported_sites = SUPPORTED_SITES

    def _get_desired_max_claimed_jobs(self) -> int:
        with self._runtime_settings_lock:
            return self._desired_max_claimed_jobs

    def _should_listen_for_runtime_events(self) -> bool:
        return self.config.settings.redis.enabled and bool(
            self.config.settings.redis.host
        )

    def _matches_runtime_target(self, event: dict[str, Any]) -> bool:
        event_backend_id = str(
            event.get("backend_id") or event.get("target_backend_id") or ""
        ).strip()
        if event_backend_id and event_backend_id == self.backend_id:
            return True

        event_scope = str(event.get("settings_scope") or "").strip()
        if event_scope:
            return event_scope == self._settings_scope

        return False

    def enqueue_runtime_control_event(self, event: dict[str, Any]) -> None:
        self._runtime_control_queue.put(event)

    def _start_runtime_control_listener(self) -> None:
        if not self._should_listen_for_runtime_events():
            logger.info(
                "Skipping worker pool runtime listener for backend %s "
                "(redis_enabled=%s, redis_host=%s)",
                self.backend_id,
                self.config.settings.redis.enabled,
                self.config.settings.redis.host,
            )
            return

        if self._runtime_listener_thread and self._runtime_listener_thread.is_alive():
            return

        stop_event = threading.Event()
        self._runtime_listener_ready_event.clear()

        def _runner() -> None:
            # Auto-reconnect loop so transient Redis outages (connection
            # refused, LOADING during restart, etc.) do not permanently kill
            # the listener and silently drop runtime config/scale events.
            backoff = 1.0
            max_backoff = 30.0
            last_error_signature: str | None = None

            while not stop_event.is_set():
                client: sync_redis.Redis | None = None
                pubsub: sync_redis.client.PubSub | None = None
                try:
                    client = sync_redis.Redis.from_url(
                        self.redis_url,
                        decode_responses=True,
                    )
                    pubsub = client.pubsub(ignore_subscribe_messages=True)
                    pubsub.subscribe(CONFIG_EVENTS_CHANNEL, HOOKS_CHANNEL)
                    self._runtime_listener_ready_event.set()
                    logger.info(
                        "Worker pool runtime listener subscribed to %s for backend %s",
                        CONFIG_EVENTS_CHANNEL,
                        self.backend_id,
                    )
                    # Successful (re)connect resets backoff and error dedupe.
                    backoff = 1.0
                    last_error_signature = None

                    while not stop_event.is_set():
                        message = pubsub.get_message(timeout=1.0)
                        if not message:
                            continue

                        event = _decode_pubsub_payload(message.get("data"))
                        if event is None:
                            continue

                        channel = message.get("channel")
                        if isinstance(channel, bytes):
                            channel = channel.decode()
                        if channel == HOOKS_CHANNEL:
                            from ..job_system.idle import WAKE_EVENTS

                            if event.get("event") in WAKE_EVENTS:
                                # Coalesce lifecycle noise into the existing
                                # private pool subscription as a wake hint.
                                now = time.monotonic()
                                if now - getattr(self, "_last_ledger_wake", float("-inf")) >= 0.25:
                                    client.publish(self._auth_reload_channel, json.dumps({"event": "ledger_wakeup"}))
                                    self._last_ledger_wake = now
                            if event.get("event") == "reload_user_data":
                                # Preserve the original payload, including
                                # legacy/escaped JSON fields. Each pool has
                                # one relay and its own private destination.
                                client.publish(self._auth_reload_channel, message["data"])
                            continue
                        if channel != CONFIG_EVENTS_CHANNEL:
                            continue

                        if event.get("type") not in {
                            "settings_updated",
                            "worker_pool_scale",
                            "max_claimed_jobs_update",
                        }:
                            continue
                        if not self._matches_runtime_target(event):
                            continue

                        self.enqueue_runtime_control_event(event)
                except Exception as exc:
                    if stop_event.is_set():
                        break

                    # Treat Redis LOADING (restart warmup) as a soft retry:
                    # log at INFO, shorter initial backoff.
                    exc_text = str(exc)
                    is_loading = (
                        "LOADING" in exc_text
                        or "loading the dataset" in exc_text.lower()
                    )

                    # Rate-limit warnings: only log on first failure or when
                    # the error class/message changes, otherwise stay quiet
                    # while backing off.
                    signature = (
                        f"{type(exc).__name__}:{'loading' if is_loading else 'conn'}"
                    )
                    if signature != last_error_signature:
                        if is_loading:
                            logger.info(
                                "Worker pool runtime listener waiting for Redis "
                                "to finish loading (backend=%s): %s",
                                self.backend_id,
                                exc,
                            )
                        else:
                            logger.warning(
                                "Worker pool runtime listener lost connection "
                                "for backend %s: %s (reconnecting)",
                                self.backend_id,
                                exc,
                            )
                        last_error_signature = signature
                finally:
                    self._runtime_listener_ready_event.clear()
                    if pubsub is not None:
                        with contextlib.suppress(Exception):
                            pubsub.close()
                    if client is not None:
                        with contextlib.suppress(Exception):
                            client.close()

                if stop_event.is_set():
                    break

                # Interruptible backoff so shutdown is still responsive.
                stop_event.wait(backoff)
                backoff = min(max_backoff, backoff * 2)

        self._runtime_listener_stop_event = stop_event
        self._runtime_listener_thread = threading.Thread(
            target=_runner,
            name=f"WorkerPoolRuntime-{self.backend_id}",
            daemon=True,
        )
        self._runtime_listener_thread.start()
        # If Redis is unavailable, new children use their original direct
        # subscription rather than depending on an unready relay.
        self._runtime_listener_ready_event.wait(timeout=1.0)

    def _stop_runtime_control_listener(self, timeout: float = 5.0) -> None:
        if self._runtime_listener_stop_event is not None:
            self._runtime_listener_stop_event.set()

        if self._runtime_listener_thread and self._runtime_listener_thread.is_alive():
            self._runtime_listener_thread.join(timeout=timeout)
            if self._runtime_listener_thread.is_alive():
                logger.warning(
                    "Worker pool runtime listener for backend %s did not stop within %.1fs",
                    self.backend_id,
                    timeout,
                )

        self._runtime_listener_thread = None
        self._runtime_listener_stop_event = None
        self._runtime_listener_ready_event.clear()

    def _worker_reload_events_channel(self) -> str:
        ready = getattr(self, "_runtime_listener_ready_event", None)
        if ready is not None and ready.is_set():
            return self._auth_reload_channel
        return HOOKS_CHANNEL

    def _reload_runtime_settings(
        self,
        scope: str,
        version: int | None = None,
    ) -> None:
        logger.info(
            "Applying worker pool settings_updated for backend %s scope=%s version=%s",
            self.backend_id,
            scope,
            version,
        )

        previous_enabled = self.config.settings.worker_pool.enabled
        previous_sites = [str(site).lower() for site in self.supported_sites]
        previous_target_size = self.num_workers
        previous_max_claimed_jobs = self._get_desired_max_claimed_jobs()

        fresh_config = UltimaScraperCollectionConfig.load_or_create_default_config()
        self.config = fresh_config
        self._settings_scope = fresh_config.settings.get_backend_id()

        fresh_worker_pool = fresh_config.settings.worker_pool
        fresh_sites = [str(site).lower() for site in fresh_worker_pool.sites]
        if fresh_worker_pool.enabled != previous_enabled:
            logger.info(
                "worker_pool.enabled changed for backend %s (%s → %s) but requires restart",
                self.backend_id,
                previous_enabled,
                fresh_worker_pool.enabled,
            )
        if fresh_sites != previous_sites:
            logger.info(
                "worker_pool.sites changed for backend %s (%s → %s) but requires restart",
                self.backend_id,
                previous_sites,
                fresh_sites,
            )

        new_max_claimed_jobs = max(0, int(fresh_worker_pool.max_claimed_jobs))
        if new_max_claimed_jobs != previous_max_claimed_jobs:
            self.update_max_claimed_jobs(new_max_claimed_jobs)

        new_size = max(0, int(fresh_worker_pool.size))
        if new_size != previous_target_size:
            self.scale_to(new_size)

        if version is not None and version > 0:
            self._settings_last_applied_versions[scope] = version

    def update_max_claimed_jobs(self, new_limit: int) -> None:
        new_limit = max(0, int(new_limit))
        with self._runtime_settings_lock:
            old_limit = self._desired_max_claimed_jobs
            self._desired_max_claimed_jobs = new_limit

        self.config.settings.worker_pool.max_claimed_jobs = new_limit
        if new_limit != old_limit:
            logger.info(
                "Updated worker pool desired max_claimed_jobs for backend %s: %s → %s",
                self.backend_id,
                old_limit,
                new_limit,
            )

    def _drain_runtime_control_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        while True:
            try:
                events.append(self._runtime_control_queue.get_nowait())
            except queue.Empty:
                return events

    def _apply_runtime_control_event(self, event: dict[str, Any]) -> bool:
        event_type = str(event.get("type") or "").strip()
        if event_type == "settings_updated":
            scope = str(event.get("settings_scope") or "").strip()
            if not scope or not self._matches_runtime_target(event):
                return False
            version = _coerce_int(event.get("version"))
            if (
                version is not None
                and version <= self._settings_last_applied_versions.get(scope, 0)
            ):
                return False
            self._reload_runtime_settings(scope, version)
            return True

        if not self._matches_runtime_target(event):
            return False

        if event_type == "worker_pool_scale":
            target_size = _coerce_int(event.get("target_size"))
            if target_size is None:
                return False
            self.config.settings.worker_pool.size = max(0, target_size)
            self.scale_to(target_size)
            return True

        if event_type == "max_claimed_jobs_update":
            new_limit = _coerce_int(event.get("max_claimed_jobs"))
            if new_limit is None:
                return False
            self.update_max_claimed_jobs(new_limit)
            return True

        return False

    def process_runtime_control_events(self) -> int:
        processed = 0
        for event in self._drain_runtime_control_events():
            processed += int(self._apply_runtime_control_event(event))
        return processed

    def service_runtime(self) -> int:
        self.cleanup_stopped_workers()
        processed = self.process_runtime_control_events()
        self._publish_readiness()
        return processed

    def _publish_readiness(self, *, force=False):
        readiness = getattr(self, "_readiness", None)
        if readiness is not None:
            try:
                readiness.publish(self.backend_id, self.num_workers, [
                    worker_id for worker_id, process in zip(self.worker_ids, self.processes)
                    if process.is_alive() and process.pid not in self._pending_shutdown
                ], force=force)
            except OSError:
                logger.warning("Could not publish worker readiness", exc_info=True)


    def _start_job_service(self, startup_timeout: float = 15.0) -> None:
        """Standalone workers own SQL planning, recovery and event delivery."""
        from ..runtime_mode import get_runtime_mode

        if get_runtime_mode() != "worker":
            return  # The WebUI/API lifespan owns its ledger service.
        if self._ledger_service_thread and self._ledger_service_thread.is_alive():
            return
        stop_event = threading.Event()
        started_event = threading.Event()
        self._ledger_service_start_error = None

        def runner():
            async def main():
                from ..job_system.ledger_runtime import service

                redis = RedisManager(config=self.config.settings.redis)
                service_task = None
                try:
                    await redis.connect()
                    service_task = asyncio.create_task(
                        service(SimpleNamespace(redis=redis))
                    )
                    started_event.set()
                    while not stop_event.is_set():
                        if service_task.done():
                            await service_task
                            raise RuntimeError(
                                "Execution ledger service stopped unexpectedly"
                            )
                        await asyncio.sleep(0.1)
                except BaseException as exc:
                    self._ledger_service_start_error = exc
                    started_event.set()
                    logger.exception(
                        "Execution ledger service failed for %s", self.backend_id
                    )
                finally:
                    if service_task is not None:
                        service_task.cancel()
                        await asyncio.gather(service_task, return_exceptions=True)
                    with contextlib.suppress(Exception):
                        await redis.disconnect()

            asyncio.run(main(), loop_factory=_worker_loop_factory())

        self._ledger_service_stop_event = stop_event
        self._ledger_service_started_event = started_event
        self._ledger_service_thread = threading.Thread(
            target=runner,
            name=f"ExecutionLedger-{self.backend_id}",
            daemon=True,
        )
        self._ledger_service_thread.start()
        if not started_event.wait(timeout=startup_timeout):
            stop_event.set()
            raise RuntimeError(
                f"Timed out starting execution ledger for {self.backend_id}"
            )
        if self._ledger_service_start_error is not None:
            raise RuntimeError(
                f"Failed to start execution ledger for {self.backend_id}"
            ) from self._ledger_service_start_error


    def _stop_job_service(self, timeout: float = 10.0) -> None:
        if self._ledger_service_stop_event is not None:
            self._ledger_service_stop_event.set()
        if self._ledger_service_thread and self._ledger_service_thread.is_alive():
            self._ledger_service_thread.join(timeout=timeout)
            if self._ledger_service_thread.is_alive():
                logger.warning(
                    "Execution ledger for %s did not stop within %.1fs",
                    self.backend_id,
                    timeout,
                )
                return
        self._ledger_service_thread = None
        self._ledger_service_stop_event = None
        self._ledger_service_started_event = None
        self._ledger_service_start_error = None


    def _spawn_worker(self, worker_index: int) -> multiprocessing.Process:
        """Spawn a single worker process.

        Args:
            worker_index: Index for shard calculation

        Returns:
            The spawned Process object
        """
        worker_id = f"worker-{uuid4().hex[:8]}"
        self.worker_ids.append(worker_id)

        process = self._process_context.Process(
            target=self._run_worker,
            args=(
                worker_id,
                self.redis_url,
                self.supported_sites,
                self.config,
                self.backend_id,
                worker_index,
                self.num_workers,  # Current target worker count
                self._worker_reload_events_channel(),
            ),
            name=f"UltimaWorker-{worker_index + 1}",
        )
        process.start()
        self.processes.append(process)

        logger.info(
            f"Spawned worker {worker_id} (PID: {process.pid}) - Index: {worker_index}"
        )
        return process

    def start(self) -> None:
        """Start all worker processes with staggered spawning.

        Workers are started with a short delay between each to avoid
        database bootstrap contention and API rate-limiting during the
        simultaneous ``initialize()`` phase.
        """
        logger.info(f"Starting worker pool with {self.num_workers} workers...")
        self._start_runtime_control_listener()
        self._start_job_service()

        for index in range(self.num_workers):
            self._spawn_worker(index)
            # Stagger database/API initialization bursts.
            if index < self.num_workers - 1:
                time.sleep(2)

        self._next_worker_index = self.num_workers
        self._publish_readiness(force=True)
        logger.info(f"Worker pool started with {len(self.processes)} processes")

    @staticmethod
    def _run_worker(
        worker_id: str,
        redis_url: str,
        supported_sites: list[str],
        config: UltimaScraperCollectionConfig,
        backend_id: str,
        worker_index: int,
        worker_count: int,
        reload_events_channel: str = HOOKS_CHANNEL,
    ) -> None:
        """
        Worker process entry point.

        Args:
            worker_id: Unique worker identifier
            redis_url: Redis connection URL
            supported_sites: List of sites this worker should support
            config: UltimaScraperCollectionConfig instance
            backend_id: Backend identifier
            worker_index: Worker index (0-based) for shard calculation
            worker_count: Total number of workers for shard calculation
        """
        # Setup logging for worker process without console output
        try:
            from ultima_scraper_core import load_env

            load_env()
        except ImportError:
            pass
        previous_database_role = os.environ.get("ULTIMA_DATABASE_PROCESS_ROLE")
        os.environ["ULTIMA_DATABASE_PROCESS_ROLE"] = "worker_reader"
        _configure_worker_logging(worker_id, redis_url)

        logger.info(
            f"[{worker_id}] Worker process starting (PID: {os.getpid()}) "
            f"- Shard {worker_index}/{worker_count - 1} - Sites: {supported_sites}"
        )

        # Setup signal handlers
        shutdown_requested = threading.Event()
        worker_ref: Worker | None = None

        def signal_handler(signum: int, frame: Any) -> None:
            logger.info(f"[{worker_id}] Received signal {signum}, shutting down...")
            shutdown_requested.set()
            if worker_ref is not None:
                worker_ref.running = False
                worker_ref._shutdown_event.set()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Run worker
        async def _async_main() -> None:
            nonlocal worker_ref
            logger.info(
                "[%s] Worker event loop: %s", worker_id,
                type(asyncio.get_running_loop()).__module__,
            )
            worker = Worker(
                worker_id,
                redis_url,
                supported_sites,
                config,
                backend_id=backend_id,
                worker_index=worker_index,
                worker_count=worker_count,
                reload_events_channel=reload_events_channel,
            )
            worker_ref = worker

            try:
                # Retry initialization with exponential back-off. Database and
                # API pressure can be transient when many workers start together.
                max_retries = 3
                for attempt in range(1, max_retries + 1):
                    if shutdown_requested.is_set():
                        logger.info(
                            f"[{worker_id}] Shutdown requested during initialization"
                        )
                        return

                    if await worker.initialize():
                        if shutdown_requested.is_set():
                            logger.info(
                                f"[{worker_id}] Shutdown requested before run loop start"
                            )
                            return

                        logger.info(f"[{worker_id}] Starting worker run loop...")
                        await worker.run()
                        logger.info(f"[{worker_id}] Worker run loop exited")
                        return

                    if attempt < max_retries:
                        delay = 2**attempt  # 2s, 4s
                        logger.warning(
                            f"[{worker_id}] Init attempt {attempt}/{max_retries} failed, "
                            f"retrying in {delay}s..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"[{worker_id}] Worker initialization failed after "
                            f"{max_retries} attempts"
                        )
            finally:
                await worker.shutdown()

        try:
            loop_factory = _worker_loop_factory()
            asyncio.run(_async_main(), loop_factory=loop_factory)
        except KeyboardInterrupt:
            logger.info(f"[{worker_id}] Interrupted")
        except Exception as exc:
            logger.exception(f"[{worker_id}] Worker crashed: {exc}")
        finally:
            if previous_database_role is None:
                os.environ.pop("ULTIMA_DATABASE_PROCESS_ROLE", None)
            else:
                os.environ["ULTIMA_DATABASE_PROCESS_ROLE"] = previous_database_role

    def stop(self, timeout: float = 10.0) -> None:
        """
        Stop all worker processes.

        Args:
            timeout: Seconds to wait for graceful shutdown
        """
        logger.info("Stopping worker pool...")

        for process in self.processes:
            if process.is_alive():
                process.terminate()

        # Wait for all to stop
        for process in self.processes:
            process.join(timeout=timeout / len(self.processes))
            if process.is_alive():
                logger.warning(f"Force killing worker PID {process.pid}")
                process.kill()
                process.join()

        self.processes.clear()
        readiness = getattr(self, "_readiness", None)
        if readiness is not None:
            readiness.close()
        self._stop_job_service(timeout=timeout)
        self._stop_runtime_control_listener(timeout=min(timeout, 5.0))
        logger.info("Worker pool stopped")

    def is_alive(self) -> bool:
        """Check if any workers are still running."""
        return any(p.is_alive() for p in self.processes)

    def get_worker_count(self) -> int:
        """Get number of alive workers."""
        return sum(1 for p in self.processes if p.is_alive())

    def scale_to(self, target_count: int, graceful: bool = True) -> None:
        """Scale worker pool to target number of workers.

        Dynamically adds or removes workers to reach the target count.
        When scaling down, workers finish their current task before exiting
        if graceful=True.

        Args:
            target_count: Desired number of workers (must be >= 0)
            graceful: If True, send SIGTERM for graceful shutdown; if False, SIGKILL
        """
        if target_count < 0:
            logger.warning(f"Invalid target_count {target_count}, using 0")
            target_count = 0

        current_count = self.get_worker_count()

        if target_count == current_count:
            self.num_workers = target_count
            self._publish_readiness(force=True)
            logger.info(f"Worker pool already at target size {target_count}")
            return

        if target_count > current_count:
            if current_count == 0:
                self._start_job_service()
            # Scale up: spawn new workers
            workers_to_add = target_count - current_count

            # Update num_workers BEFORE spawning so new workers receive the
            # correct shard denominator for user-partitioning.
            self.num_workers = target_count

            logger.info(
                f"Scaling up worker pool: {current_count} → {target_count} "
                f"(+{workers_to_add} workers)"
            )
            for i in range(workers_to_add):
                self._spawn_worker(self._next_worker_index)
                self._next_worker_index += 1
                # Stagger database/API initialization bursts.
                if i < workers_to_add - 1:
                    time.sleep(2)
        else:
            # Scale down: stop excess workers gracefully
            workers_to_remove = current_count - target_count
            logger.info(
                f"Scaling down worker pool: {current_count} → {target_count} "
                f"(-{workers_to_remove} workers, graceful={graceful})"
            )

            # Get alive processes to terminate (prefer most recently added)
            alive_processes = [p for p in self.processes if p.is_alive()]
            processes_to_stop = alive_processes[-workers_to_remove:]

            for process in processes_to_stop:
                if process.pid:
                    if graceful:
                        # SIGTERM allows worker to finish current task
                        logger.info(
                            f"Sending SIGTERM to worker PID {process.pid} "
                            f"(graceful shutdown)"
                        )
                        process.terminate()
                        # Track for cleanup
                        worker_idx = self.processes.index(process)
                        worker_id = (
                            self.worker_ids[worker_idx]
                            if worker_idx < len(self.worker_ids)
                            else "unknown"
                        )
                        self._pending_shutdown[process.pid] = worker_id
                    else:
                        logger.info(f"Sending SIGKILL to worker PID {process.pid}")
                        process.kill()

            self.num_workers = target_count
            if target_count == 0:
                self._stop_job_service()
        self._publish_readiness(force=True)

    def cleanup_stopped_workers(self, timeout: float = 0.1) -> int:
        """Clean up worker processes that have stopped.

        Checks for workers that have finished (either naturally or after
        graceful shutdown signal) and removes them from tracking.

        Args:
            timeout: Seconds to wait for each process join (non-blocking if 0)

        Returns:
            Number of workers cleaned up
        """
        cleaned = 0
        to_remove: list[multiprocessing.Process] = []

        for process in self.processes:
            if not process.is_alive():
                process.join(timeout=timeout)
                to_remove.append(process)
                cleaned += 1

                # Remove from pending shutdown tracking
                if process.pid in self._pending_shutdown:
                    worker_id = self._pending_shutdown.pop(process.pid)
                    logger.info(
                        f"Worker {worker_id} (PID {process.pid}) gracefully stopped"
                    )

        # Remove stopped processes from tracking
        for process in to_remove:
            idx = self.processes.index(process)
            self.processes.remove(process)
            if idx < len(self.worker_ids):
                self.worker_ids.pop(idx)

        if cleaned > 0:
            logger.info(
                f"Cleaned up {cleaned} stopped workers, "
                f"{len(self.processes)} remaining"
            )

        return cleaned

    def get_pending_shutdown_count(self) -> int:
        """Get number of workers pending graceful shutdown."""
        return len(self._pending_shutdown)

    async def run_job(self, job: Job, *, tasks: Iterable[Task] | None = None) -> str:
        """Prepare a durable job and notify the bounded worker dispatcher.

        Call pool.start() first to spawn workers, then use this to submit jobs.
        Workers claim and execute SQL-owned attempts.

        Args:
            job: The Job to execute
            tasks: Optional lazy input, used instead of job.tasks

        Returns:
            The backend_id the job was routed to

        Example:
            pool = WorkerPool()
            pool.start()  # Spawn worker subprocesses

            task = Task(
                site_name="OnlyFans",
                user_id=123,
                content_type=ContentType.POSTS,
                task_type=TaskType.SCRAPE,
                payload=ScrapeTaskPayload(user_username="someuser"),
            )
            job = Job(name="Test Job", created_by="test", tasks=[task])
            backend_id = await pool.run_job(job)
        """
        from ..job_system.auth_expansion import iter_worker_submission_tasks

        redis_config = self.config.settings.redis
        redis_manager = RedisManager(config=redis_config)
        await redis_manager.connect()
        try:
            manager = JobManager(redis_manager)
            saved = await manager.prepare_job(
                job, priority=10, select_backend=True,
                tasks=iter_worker_submission_tasks(job.tasks if tasks is None else tasks),
            )
            backend_id = saved.metadata["dispatch_backend_id"]
            await manager.activate_job(saved, distributed_mode=False)

            logger.info(
                f"[WorkerPool] Prepared job {job.job_id} ({job.name}) for backend {backend_id}"
            )
            return backend_id

        finally:
            await redis_manager.disconnect()

    def run_job_sync(self, job: Job, *, tasks: Iterable[Task] | None = None) -> str:
        """Synchronous wrapper for run_job.

        Convenience method that creates an event loop and runs the async run_job.
        Use this when calling from synchronous code (e.g., a separate process entry point).

        Args:
            job: The Job to execute
            tasks: Optional lazy input, used instead of job.tasks

        Returns:
            The backend_id the job was routed to

        Example:
            pool = WorkerPool()
            pool.start()  # Spawn workers first
            backend_id = pool.run_job_sync(job)  # Enqueue job
        """
        return asyncio.run(self.run_job(job, tasks=tasks), loop_factory=_worker_loop_factory())


if __name__ == "__main__":
    # Example usage
    configure_runtime_logging("worker-pool")

    pool = WorkerPool(num_workers=5)
    pool.start()

    try:
        logger.info("Worker pool running. Press Ctrl+C to stop.")
        while pool.is_alive():
            import time

            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        pool.stop()
