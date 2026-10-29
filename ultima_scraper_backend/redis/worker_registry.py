"""Worker registration and status tracking using Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, Protocol, cast

import redis as sync_redis

logger = logging.getLogger(__name__)

# Seconds to coalesce worker status snapshots: every task start and completion
# rewrites the worker key, so with a value > 0 only the latest snapshot is
# written once per interval. Unset or 0 writes each change immediately.
STATUS_COALESCE_ENV = "ULTIMA_WORKER_STATUS_COALESCE_SECONDS"


def status_coalesce_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get(STATUS_COALESCE_ENV) or 0))
    except ValueError:
        return 0.0


class WorkerRegistryRedis(Protocol):
    config: Any

    async def set(self, key: str, value: Any, ex: int | None = None) -> bool: ...
    async def delete(self, *keys: str) -> int: ...
    async def exists(self, *keys: str) -> int: ...
    async def get(self, key: str) -> str | None: ...
    async def sadd(self, name: str, *values: str) -> int: ...
    async def srem(self, name: str, *values: str) -> int: ...
    async def smembers(self, name: str) -> set[Any]: ...


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return default
    return default


def _coerce_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return float(stripped)
            except ValueError:
                return default
    return default


def _decode_members(values: set[Any]) -> list[str]:
    decoded = [
        item.decode() if isinstance(item, bytes) else str(item) for item in values
    ]
    return sorted(decoded)


def _empty_metadata() -> dict[str, Any]:
    return {}


def _empty_capabilities() -> list[str]:
    return []


def _normalize_metadata(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    value_dict = cast(dict[object, Any], value)
    return {str(key): item for key, item in value_dict.items()}


class WorkerStatus(str, Enum):
    """Worker status states."""

    IDLE = "idle"
    BUSY = "busy"
    PAUSED = "paused"
    ERROR = "error"
    OFFLINE = "offline"


@dataclass
class TaskInfo:
    """Information about a task being processed."""

    task_id: str
    task_type: str
    started_at: str
    progress: float = 0.0
    metadata: dict[str, Any] = field(default_factory=_empty_metadata)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "started_at": self.started_at,
            "progress": self.progress,
            "metadata": dict(self.metadata),
        }


@dataclass
class WorkerInfo:
    """Information about a worker."""

    worker_id: str
    backend_id: str
    worker_type: str
    status: str = WorkerStatus.IDLE.value
    current_task: Optional[TaskInfo] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    uptime: float = 0.0
    capabilities: list[str] = field(default_factory=_empty_capabilities)
    metadata: dict[str, Any] = field(default_factory=_empty_metadata)
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    last_heartbeat: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "worker_id": self.worker_id,
            "backend_id": self.backend_id,
            "worker_type": self.worker_type,
            "status": self.status,
            "current_task": (
                self.current_task.to_dict() if self.current_task is not None else None
            ),
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "uptime": self.uptime,
            "capabilities": list(self.capabilities),
            "metadata": dict(self.metadata),
            "started_at": self.started_at,
            "last_heartbeat": self.last_heartbeat,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerInfo:
        """Create from dictionary."""
        # Map legacy field names to current schema
        field_mapping = {
            "site": "worker_type",
            "jobs_completed": "tasks_completed",
            "current_job": "current_task",
            "last_seen": "last_heartbeat",
        }
        mapped_data: dict[str, Any] = {}
        for key, value in data.items():
            new_key = field_mapping.get(key, key)
            mapped_data[new_key] = value

        # Provide sensible defaults when legacy payloads omit required fields
        mapped_data.setdefault("worker_type", "scraper")
        mapped_data.setdefault("tasks_completed", 0)
        mapped_data.setdefault("tasks_failed", 0)

        # Normalize status values to known WorkerStatus values
        status_value = mapped_data.get("status")
        if isinstance(status_value, str):
            normalized = status_value.lower()
            if normalized == "online":
                normalized = WorkerStatus.IDLE.value
            elif normalized == "offline":
                normalized = WorkerStatus.OFFLINE.value
            mapped_data["status"] = normalized

        current_task_raw = mapped_data.get("current_task")
        current_task: TaskInfo | None = None
        if isinstance(current_task_raw, TaskInfo):
            current_task = current_task_raw
        elif isinstance(current_task_raw, dict):
            current_task_dict = cast(dict[str, Any], current_task_raw)
            metadata = _normalize_metadata(current_task_dict.get("metadata"))
            current_task = TaskInfo(
                task_id=str(current_task_dict.get("task_id", "")),
                task_type=str(current_task_dict.get("task_type", "")),
                started_at=str(current_task_dict.get("started_at", "")),
                progress=_coerce_float(current_task_dict.get("progress"), 0.0),
                metadata=metadata,
            )

        capabilities_raw = mapped_data.get("capabilities")
        capabilities_items = (
            cast(list[Any], capabilities_raw)
            if isinstance(capabilities_raw, list)
            else []
        )
        capabilities = [str(item) for item in capabilities_items]
        metadata_raw = mapped_data.get("metadata")
        metadata = _normalize_metadata(metadata_raw)

        return cls(
            worker_id=str(mapped_data.get("worker_id", "")),
            backend_id=str(mapped_data.get("backend_id", "")),
            worker_type=str(mapped_data.get("worker_type", "scraper")),
            status=str(mapped_data.get("status", WorkerStatus.IDLE.value)),
            current_task=current_task,
            tasks_completed=_coerce_int(mapped_data.get("tasks_completed"), 0),
            tasks_failed=_coerce_int(mapped_data.get("tasks_failed"), 0),
            uptime=_coerce_float(mapped_data.get("uptime"), 0.0),
            capabilities=capabilities,
            metadata=metadata,
            started_at=str(
                mapped_data.get("started_at", datetime.now(timezone.utc).isoformat())
            ),
            last_heartbeat=str(
                mapped_data.get(
                    "last_heartbeat", datetime.now(timezone.utc).isoformat()
                )
            ),
        )

    @classmethod
    def from_json(cls, json_str: str) -> WorkerInfo:
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


class WorkerRegistry:
    """Manages worker registration and status in Redis."""

    # Redis key patterns
    WORKERS_SET_PREFIX = "ultimascraper:backend:"  # + backend_id + ":workers"
    WORKER_INFO_PREFIX = "ultimascraper:workers:"
    WORKER_HEARTBEAT_PREFIX = "ultimascraper:workers:heartbeat:"
    ALL_WORKERS_SET = "ultimascraper:workers:all"

    # TTL settings (in seconds)
    DEFAULT_TTL = 120  # 120 seconds — generous to survive long event-loop stalls
    HEARTBEAT_INTERVAL = 30  # 30 seconds

    def __init__(
        self,
        redis_manager: WorkerRegistryRedis,
        backend_id: str,
        worker_id: Optional[str] = None,
        worker_type: str = "scraper",
        capabilities: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        ttl: int = DEFAULT_TTL,
        heartbeat_interval: int = HEARTBEAT_INTERVAL,
    ) -> None:
        """Initialize worker registry.

        Args:
            redis_manager: Redis connection manager
            backend_id: Backend this worker belongs to
            worker_id: Unique worker identifier (generated if None)
            worker_type: Type of worker (scraper, downloader, etc.)
            capabilities: List of worker capabilities
            metadata: Additional metadata
            ttl: Time-to-live for worker keys in seconds
            heartbeat_interval: Heartbeat interval in seconds
        """
        self.redis = redis_manager
        self.backend_id = backend_id
        self.worker_id = worker_id or self._generate_worker_id()
        self.worker_type = worker_type
        self.capabilities = capabilities or []
        self.metadata = metadata or {}
        self.ttl = ttl
        self.heartbeat_interval = heartbeat_interval

        self._info: Optional[WorkerInfo] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._running = False
        self._task_start_time: Optional[datetime] = None
        self._active_tasks: dict[str, TaskInfo] = {}
        self.status_coalesce_seconds = status_coalesce_seconds()
        self._status_flush: Optional[asyncio.Task[None]] = None

    @staticmethod
    def _generate_worker_id() -> str:
        """Generate a unique worker ID."""
        return f"worker-{uuid.uuid4().hex[:12]}"

    def _get_worker_info(self) -> WorkerInfo:
        """Get current worker info."""
        if not self._info:
            self._info = WorkerInfo(
                worker_id=self.worker_id,
                backend_id=self.backend_id,
                worker_type=self.worker_type,
                capabilities=self.capabilities,
                metadata=self.metadata,
            )
        return self._info

    def _workers_set_key(self, backend_id: Optional[str] = None) -> str:
        """Get Redis key for backend's workers set."""
        bid = backend_id or self.backend_id
        return f"{self.WORKERS_SET_PREFIX}{bid}:workers"

    def _worker_key(self, worker_id: Optional[str] = None) -> str:
        """Get Redis key for worker info."""
        wid = worker_id or self.worker_id
        return f"{self.WORKER_INFO_PREFIX}{wid}"

    def _heartbeat_key(self, worker_id: Optional[str] = None) -> str:
        """Get Redis key for worker heartbeat."""
        wid = worker_id or self.worker_id
        return f"{self.WORKER_HEARTBEAT_PREFIX}{wid}"

    async def register(self) -> bool:
        """Register this worker in Redis.

        Returns:
            True if successful, False otherwise
        """
        try:
            info = self._get_worker_info()
            info.status = WorkerStatus.IDLE.value
            info.last_heartbeat = datetime.now(timezone.utc).isoformat()

            # Store worker info with TTL
            worker_key = self._worker_key()
            await self.redis.set(worker_key, info.to_json(), ex=self.ttl)

            # Add to backend's workers set
            workers_set = self._workers_set_key()
            await self.redis.sadd(workers_set, self.worker_id)

            # Add to global workers set
            await self.redis.sadd(self.ALL_WORKERS_SET, self.worker_id)

            logger.debug(
                f"Worker {self.worker_id} registered to backend {self.backend_id}"
            )
            return True
        except Exception as exc:
            logger.error(f"Failed to register worker: {exc}")
            return False

    async def unregister(self) -> bool:
        """Unregister this worker from Redis.

        Returns:
            True if successful, False otherwise
        """
        if self._status_flush is not None:
            # A late snapshot must not recreate the deleted worker key.
            self._status_flush.cancel()
            self._status_flush = None
        try:
            # Remove from backend's workers set
            workers_set = self._workers_set_key()
            await self.redis.srem(workers_set, self.worker_id)

            # Remove from global workers set
            await self.redis.srem(self.ALL_WORKERS_SET, self.worker_id)

            # Delete worker info
            await self.redis.delete(self._worker_key())
            await self.redis.delete(self._heartbeat_key())

            logger.info(f"Worker {self.worker_id} unregistered successfully")
            return True
        except Exception as exc:
            logger.error(f"Failed to unregister worker: {exc}")
            return False

    async def update_status(
        self,
        status: WorkerStatus,
        current_task: Optional[TaskInfo] = None,
    ) -> bool:
        """Update worker status in Redis.

        Args:
            status: Worker status
            current_task: Current task information (if any)

        Returns:
            True if successful, False otherwise
        """
        try:
            info = self._get_worker_info()
            info.status = status.value
            info.current_task = current_task
            info.last_heartbeat = datetime.now(timezone.utc).isoformat()

            # Calculate uptime
            started = datetime.fromisoformat(info.started_at)
            info.uptime = (datetime.now(timezone.utc) - started).total_seconds()

            if self.status_coalesce_seconds > 0:
                if self._status_flush is None or self._status_flush.done():
                    self._status_flush = asyncio.create_task(self._flush_status_later())
                return True
            worker_key = self._worker_key()
            await self.redis.set(worker_key, info.to_json(), ex=self.ttl)

            logger.debug(f"Worker {self.worker_id} status updated: {status.value}")
            return True
        except Exception as exc:
            logger.error(f"Failed to update worker status: {exc}")
            return False

    async def _flush_status_later(self) -> None:
        """Write the newest status snapshot once the coalescing interval ends."""
        await asyncio.sleep(self.status_coalesce_seconds)
        try:
            await self.redis.set(
                self._worker_key(), self._get_worker_info().to_json(), ex=self.ttl
            )
        except Exception as exc:
            logger.error(f"Failed to update worker status: {exc}")

    def update_metadata(self, values: dict[str, Any]) -> None:
        """Merge runtime telemetry into the next persisted worker snapshot."""
        self.metadata.update(values)
        if self._info is not None:
            self._info.metadata.update(values)

    async def start_task(
        self,
        task_id: str,
        task_type: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Mark worker as busy with a new task.

        Args:
            task_id: Task identifier
            task_type: Type of task
            metadata: Task metadata

        Returns:
            True if successful, False otherwise
        """
        task_start_time = datetime.now(timezone.utc)
        self._task_start_time = task_start_time
        task_info = TaskInfo(
            task_id=task_id,
            task_type=task_type,
            started_at=task_start_time.isoformat(),
            metadata=metadata or {},
        )
        self._active_tasks[task_id] = task_info
        return await self.update_status(WorkerStatus.BUSY, task_info)

    async def complete_task(
        self,
        success: bool = True,
        task_id: Optional[str] = None,
    ) -> bool:
        """Mark a task as complete.

        Args:
            success: Whether task completed successfully
            task_id: Optional task identifier being completed

        Returns:
            True if successful, False otherwise
        """
        info = self._get_worker_info()
        if success:
            info.tasks_completed += 1
        else:
            info.tasks_failed += 1

        if task_id and task_id in self._active_tasks:
            del self._active_tasks[task_id]
        elif self._active_tasks:
            self._active_tasks.pop(next(iter(self._active_tasks)), None)

        if self._active_tasks:
            latest_task = next(iter(reversed(self._active_tasks.values())))
            info.current_task = latest_task
            return await self.update_status(WorkerStatus.BUSY, latest_task)

        info.current_task = None
        self._task_start_time = None
        return await self.update_status(WorkerStatus.IDLE)

    async def start_heartbeat(self) -> None:
        """Start automatic heartbeat in a **daemon thread**.

        A thread-based heartbeat is used instead of ``asyncio.create_task``
        because worker processes often block the event loop for minutes at a
        time during heavy task processing (SQLite writes, large downloads).
        An asyncio heartbeat task cannot run while the loop is blocked, which
        causes the worker's Redis registration to expire.  A daemon thread
        runs independently and keeps the registration alive regardless of
        event-loop stalls.
        """
        if self._running:
            logger.warning("Heartbeat already running")
            return

        self._running = True

        # Build a *sync* Redis client for the heartbeat thread so we don't
        # share the async client across threads.
        cfg = self.redis.config
        password = getattr(cfg, "password", None) or None
        self._sync_redis: sync_redis.Redis = sync_redis.Redis(
            host=cfg.host,
            port=cfg.port,
            db=cfg.db,
            password=password,
            socket_timeout=10,
            socket_connect_timeout=5,
            decode_responses=False,
        )

        self._heartbeat_thread = threading.Thread(
            target=self._threaded_heartbeat_loop,
            name=f"heartbeat-{self.worker_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()
        logger.debug(f"Worker {self.worker_id} threaded heartbeat started")

    async def stop_heartbeat(self) -> None:
        """Stop automatic heartbeat."""
        self._running = False

        # Thread cleanup
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5.0)
            self._heartbeat_thread = None

        # Close sync Redis client
        if hasattr(self, "_sync_redis"):
            try:
                self._sync_redis.close()
            except Exception:
                pass

        logger.info(f"Worker {self.worker_id} heartbeat stopped")

    # ------------------------------------------------------------------
    # Thread-based heartbeat (runs independently of the asyncio loop)
    # ------------------------------------------------------------------

    def _threaded_heartbeat_loop(self) -> None:
        """Heartbeat loop executed in a daemon thread.

        Uses a **sync** Redis connection so it is completely decoupled from
        the worker's asyncio event loop.
        """
        r = self._sync_redis
        while self._running:
            try:
                now_iso = datetime.now(timezone.utc).isoformat()

                # 1. Refresh heartbeat key
                heartbeat_data = json.dumps(
                    {
                        "worker_id": self.worker_id,
                        "backend_id": self.backend_id,
                        "timestamp": now_iso,
                    }
                )
                heartbeat_key = self._heartbeat_key()
                r.set(heartbeat_key, heartbeat_data, ex=self.ttl)

                # 2. Refresh worker info TTL
                worker_key = self._worker_key()
                r.expire(worker_key, self.ttl)

                # 3. Refresh set memberships (in case cleanup removed us)
                workers_set = self._workers_set_key()
                r.sadd(workers_set, self.worker_id)
                r.sadd(self.ALL_WORKERS_SET, self.worker_id)

                logger.debug(f"Worker {self.worker_id} threaded heartbeat sent")
            except Exception as exc:
                logger.error(f"Threaded heartbeat error for {self.worker_id}: {exc}")

            # Sleep in small increments so stop_heartbeat() doesn't block
            for _ in range(self.heartbeat_interval):
                if not self._running:
                    return
                time.sleep(1)

    @staticmethod
    async def list_backend_workers(
        redis_manager: WorkerRegistryRedis,
        backend_id: str,
    ) -> list[str]:
        """List all workers for a specific backend.

        Args:
            redis_manager: Redis connection manager
            backend_id: Backend identifier

        Returns:
            List of worker IDs
        """
        try:
            workers_set = f"{WorkerRegistry.WORKERS_SET_PREFIX}{backend_id}:workers"
            worker_ids = await redis_manager.smembers(workers_set)
            return _decode_members(worker_ids)
        except Exception as exc:
            logger.error(f"Failed to list workers for backend {backend_id}: {exc}")
            return []

    @staticmethod
    async def list_all_workers(redis_manager: WorkerRegistryRedis) -> list[str]:
        """List all registered workers across all backends.

        Args:
            redis_manager: Redis connection manager

        Returns:
            List of worker IDs
        """
        try:
            worker_ids = await redis_manager.smembers(WorkerRegistry.ALL_WORKERS_SET)
            return _decode_members(worker_ids)
        except Exception as exc:
            logger.error(f"Failed to list all workers: {exc}")
            return []

    @staticmethod
    async def get_worker_info(
        redis_manager: WorkerRegistryRedis,
        worker_id: str,
    ) -> Optional[WorkerInfo]:
        """Get information about a specific worker.

        Args:
            redis_manager: Redis connection manager
            worker_id: Worker identifier

        Returns:
            Worker info or None if not found
        """
        try:
            worker_key = f"{WorkerRegistry.WORKER_INFO_PREFIX}{worker_id}"
            data = await redis_manager.get(worker_key)
            if data:
                return WorkerInfo.from_json(data)
            return None
        except Exception as exc:
            logger.error(f"Failed to get worker info for {worker_id}: {exc}")
            return None

    @staticmethod
    async def get_backend_workers_info(
        redis_manager: WorkerRegistryRedis,
        backend_id: str,
    ) -> dict[str, WorkerInfo]:
        """Get information about all workers for a backend.

        Args:
            redis_manager: Redis connection manager
            backend_id: Backend identifier

        Returns:
            Dictionary mapping worker IDs to WorkerInfo
        """
        workers: dict[str, WorkerInfo] = {}
        try:
            worker_ids = await WorkerRegistry.list_backend_workers(
                redis_manager, backend_id
            )
            for worker_id in worker_ids:
                info = await WorkerRegistry.get_worker_info(redis_manager, worker_id)
                if info:
                    workers[worker_id] = info
        except Exception as exc:
            logger.error(f"Failed to get workers info for backend {backend_id}: {exc}")
        return workers

    @staticmethod
    async def get_all_workers_info(
        redis_manager: WorkerRegistryRedis,
    ) -> dict[str, WorkerInfo]:
        """Get information about all registered workers.

        Args:
            redis_manager: Redis connection manager

        Returns:
            Dictionary mapping worker IDs to WorkerInfo
        """
        workers: dict[str, WorkerInfo] = {}
        try:
            worker_ids = await WorkerRegistry.list_all_workers(redis_manager)
            for worker_id in worker_ids:
                info = await WorkerRegistry.get_worker_info(redis_manager, worker_id)
                if info:
                    workers[worker_id] = info
        except Exception as exc:
            logger.error(f"Failed to get all workers info: {exc}")
        return workers

    @staticmethod
    async def cleanup_dead_workers(
        redis_manager: WorkerRegistryRedis,
        backend_id: Optional[str] = None,
    ) -> int:
        """Remove dead workers from the registry.

        Args:
            redis_manager: Redis connection manager
            backend_id: Optional backend ID to limit cleanup

        Returns:
            Number of workers cleaned up
        """
        cleanup_count = 0
        try:
            if backend_id:
                worker_ids = await WorkerRegistry.list_backend_workers(
                    redis_manager, backend_id
                )
            else:
                worker_ids = await WorkerRegistry.list_all_workers(redis_manager)

            for worker_id in worker_ids:
                worker_key = f"{WorkerRegistry.WORKER_INFO_PREFIX}{worker_id}"
                exists = await redis_manager.exists(worker_key)
                if not exists:
                    # Worker key expired, cleanup
                    if backend_id:
                        workers_set = (
                            f"{WorkerRegistry.WORKERS_SET_PREFIX}{backend_id}:workers"
                        )
                        await redis_manager.srem(workers_set, worker_id)
                    await redis_manager.srem(WorkerRegistry.ALL_WORKERS_SET, worker_id)
                    cleanup_count += 1
                    logger.info(f"Cleaned up dead worker: {worker_id}")
        except Exception as exc:
            logger.error(f"Failed to cleanup dead workers: {exc}")
        return cleanup_count
