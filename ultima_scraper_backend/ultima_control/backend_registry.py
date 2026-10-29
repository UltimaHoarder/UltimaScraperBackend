"""Backend registration and discovery system using Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import platform
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, TypeGuard

from ultima_scraper_backend.cluster import get_cluster_id, public_api_url

logger = logging.getLogger(__name__)


def _empty_capabilities() -> list[str]:
    return []


def _empty_metadata() -> dict[str, object]:
    return {}


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
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


def _coerce_str(value: object, default: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return default
    return str(value)


def _normalize_capabilities(value: object) -> list[str]:
    if not _is_object_list(value):
        return []
    return [str(item) for item in value]


def _normalize_metadata(value: object) -> dict[str, object]:
    if not _is_object_dict(value):
        return {}
    return {str(key): item for key, item in value.items()}


class BackendRegistryRedis(Protocol):
    async def set(self, key: str, value: Any, ex: int | None = None) -> bool: ...
    async def get(self, key: str) -> str | None: ...
    async def delete(self, *keys: str) -> Any: ...
    async def exists(self, *keys: str) -> Any: ...
    async def expire(self, key: str, seconds: int) -> bool: ...
    async def sadd(self, name: str, *values: str) -> Any: ...
    async def srem(self, name: str, *values: str) -> Any: ...
    async def smembers(self, name: str) -> set[str]: ...


@dataclass
class BackendInfo:
    """Information about a backend server."""

    backend_id: str
    hostname: str
    ip_address: str
    platform: str
    version: str
    status: str = "online"
    load: float = 0.0
    worker_count: int = 0
    max_workers: int = 10
    capabilities: list[str] = field(default_factory=_empty_capabilities)
    metadata: dict[str, object] = field(default_factory=_empty_metadata)
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    last_heartbeat: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    # Shared by every backend on the same database (see ultima_scraper_backend.cluster).
    cluster_id: str | None = None
    # Where other machines reach this backend's API, when the deployment says.
    api_url: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "backend_id": self.backend_id,
            "hostname": self.hostname,
            "ip_address": self.ip_address,
            "platform": self.platform,
            "version": self.version,
            "status": self.status,
            "load": self.load,
            "worker_count": self.worker_count,
            "max_workers": self.max_workers,
            "capabilities": list(self.capabilities),
            "metadata": dict(self.metadata),
            "started_at": self.started_at,
            "last_heartbeat": self.last_heartbeat,
            "cluster_id": self.cluster_id,
            "api_url": self.api_url,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> BackendInfo:
        """Create from dictionary."""
        now = datetime.now(timezone.utc).isoformat()
        return cls(
            backend_id=_coerce_str(data.get("backend_id")),
            hostname=_coerce_str(data.get("hostname")),
            ip_address=_coerce_str(data.get("ip_address")),
            platform=_coerce_str(data.get("platform")),
            version=_coerce_str(data.get("version")),
            status=_coerce_str(data.get("status"), "online"),
            load=_coerce_float(data.get("load"), 0.0),
            worker_count=_coerce_int(data.get("worker_count"), 0),
            max_workers=_coerce_int(data.get("max_workers"), 10),
            capabilities=_normalize_capabilities(data.get("capabilities")),
            metadata=_normalize_metadata(data.get("metadata")),
            started_at=_coerce_str(data.get("started_at"), now),
            last_heartbeat=_coerce_str(data.get("last_heartbeat"), now),
            cluster_id=_coerce_str(data.get("cluster_id")) or None,
            api_url=_coerce_str(data.get("api_url")) or None,
        )

    @classmethod
    def from_json(cls, json_str: str) -> BackendInfo:
        """Create from JSON string."""
        data = json.loads(json_str)
        if not _is_object_dict(data):
            raise ValueError("Backend payload must be a JSON object")
        return cls.from_dict({str(key): value for key, value in data.items()})


class BackendRegistry:
    """Manages backend registration and discovery in Redis."""

    # Import KEY_PREFIX from USAPI as source of truth
    from ultima_scraper_api.managers.redis.connection import (
        RedisManager as _USAPIRedisManager,
    )

    _KEY_PREFIX = _USAPIRedisManager.KEY_PREFIX

    # Redis key patterns
    BACKENDS_SET = f"{_KEY_PREFIX}:backends"
    BACKEND_INFO_PREFIX = f"{_KEY_PREFIX}:backend:"
    BACKEND_HEARTBEAT_PREFIX = f"{_KEY_PREFIX}:backend:heartbeat:"

    # TTL settings (in seconds)
    DEFAULT_TTL = 60  # 1 minute
    HEARTBEAT_INTERVAL = 30  # 30 seconds

    def __init__(
        self,
        redis_manager: BackendRegistryRedis,
        backend_id: str | None = None,
        version: str = "1.0.0",
        max_workers: int = 10,
        capabilities: list[str] | None = None,
        metadata: dict[str, object] | None = None,
        status: str = "online",
        ttl: int = DEFAULT_TTL,
        heartbeat_interval: int = HEARTBEAT_INTERVAL,
        status_provider: Callable[[], dict[str, object]] | None = None,
    ) -> None:
        """Initialize backend registry.

        Args:
            redis_manager: Redis connection manager
            backend_id: Unique backend identifier (generated if None)
            version: Backend version
            max_workers: Maximum number of workers
            capabilities: List of backend capabilities
            metadata: Additional metadata
            ttl: Time-to-live for backend keys in seconds
            heartbeat_interval: Heartbeat interval in seconds
        """
        self.redis = redis_manager
        self.backend_id = backend_id or self._generate_backend_id()
        self.version = version
        self.max_workers = max_workers
        self.capabilities = capabilities or []
        self.metadata = metadata or {}
        self.status = status
        self.ttl = ttl
        self.heartbeat_interval = heartbeat_interval
        self.status_provider = status_provider

        self._info: BackendInfo | None = None
        self._cluster_id: str | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._running = False

    @staticmethod
    def _generate_backend_id() -> str:
        """
        Generate a backend ID.

        Priority:
        1. BACKEND_ID environment variable (if set)
        2. Hostname only (stable across restarts)

        Returns:
            Backend ID string
        """
        import os

        # Check environment variable first
        backend_id = os.environ.get("BACKEND_ID")
        if backend_id:
            return backend_id

        # Use hostname only (no random suffix) for stability
        return socket.gethostname()

    def _get_backend_info(self) -> BackendInfo:
        """Get current backend info."""
        if not self._info:
            self._info = BackendInfo(
                backend_id=self.backend_id,
                hostname=socket.gethostname(),
                ip_address=self._get_ip_address(),
                platform=platform.platform(),
                version=self.version,
                status=self.status,
                max_workers=self.max_workers,
                capabilities=self.capabilities,
                metadata=self.metadata,
            )
        else:
            self._info.max_workers = self.max_workers
            self._info.status = self.status
        self._info.cluster_id = self._cluster_id
        self._info.api_url = public_api_url()
        if self.status_provider is not None:
            runtime_status = self.status_provider()
            self._info.metadata.update(runtime_status)
        return self._info

    @staticmethod
    def _get_ip_address() -> str:
        """Get the local IP address."""
        try:
            # Create a socket to determine the local IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

    async def _refresh_cluster_id(self) -> None:
        """Read the cluster id once the database has one; it never changes after."""
        if self._cluster_id is None:
            self._cluster_id = await asyncio.to_thread(get_cluster_id)

    def _backend_key(self, backend_id: str | None = None) -> str:
        """Get Redis key for backend info."""
        bid = backend_id or self.backend_id
        return f"{self.BACKEND_INFO_PREFIX}{bid}"

    def _heartbeat_key(self, backend_id: str | None = None) -> str:
        """Get Redis key for backend heartbeat."""
        bid = backend_id or self.backend_id
        return f"{self.BACKEND_HEARTBEAT_PREFIX}{bid}"

    async def register(self) -> bool:
        """Register this backend in Redis.

        Returns:
            True if successful, False otherwise
        """
        try:
            await self._refresh_cluster_id()
            info = self._get_backend_info()
            info.status = "online"
            info.last_heartbeat = datetime.now(timezone.utc).isoformat()

            # Store backend info with TTL
            backend_key = self._backend_key()
            await self.redis.set(backend_key, info.to_json(), ex=self.ttl)

            # Add to backends set
            await self.redis.sadd(self.BACKENDS_SET, self.backend_id)

            # Set expiration on the set member (Redis 7.0+)
            # Note: For older Redis versions, cleanup must be handled differently
            await self.redis.expire(self._heartbeat_key(), self.ttl)

            logger.info(f"Backend {self.backend_id} registered successfully")
            return True
        except Exception as exc:
            logger.error(f"Failed to register backend: {exc}")
            return False

    async def unregister(self) -> bool:
        """Unregister this backend from Redis.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Remove from backends set
            await self.redis.srem(self.BACKENDS_SET, self.backend_id)

            # Delete backend info
            await self.redis.delete(self._backend_key())
            await self.redis.delete(self._heartbeat_key())

            logger.info(f"Backend {self.backend_id} unregistered successfully")
            return True
        except Exception as exc:
            logger.error(f"Failed to unregister backend: {exc}")
            return False

    async def update_status(
        self,
        status: str = "online",
        load: float = 0.0,
        worker_count: int = 0,
    ) -> bool:
        """Update backend status in Redis.

        Args:
            status: Backend status (online, busy, maintenance, etc.)
            load: Current load (0.0-1.0)
            worker_count: Number of active workers

        Returns:
            True if successful, False otherwise
        """
        try:
            info = self._get_backend_info()
            info.status = status
            info.load = load
            info.worker_count = worker_count
            info.last_heartbeat = datetime.now(timezone.utc).isoformat()

            backend_key = self._backend_key()
            await self.redis.set(backend_key, info.to_json(), ex=self.ttl)

            logger.debug(f"Backend {self.backend_id} status updated: {status}")
            return True
        except Exception as exc:
            logger.error(f"Failed to update backend status: {exc}")
            return False

    async def update_max_workers(self, max_workers: int) -> bool:
        """Update maximum number of workers and persist immediately to Redis.

        Args:
            max_workers: New maximum worker count

        Returns:
            True if successful, False otherwise
        """
        try:
            self.max_workers = max_workers
            info = self._get_backend_info()
            info.max_workers = max_workers
            info.last_heartbeat = datetime.now(timezone.utc).isoformat()

            backend_key = self._backend_key()
            await self.redis.set(backend_key, info.to_json(), ex=self.ttl)
            logger.info(
                f"Backend {self.backend_id} max_workers updated to {max_workers}"
            )
            return True
        except Exception as exc:
            logger.error(f"Failed to update max workers: {exc}")
            return False

    async def heartbeat(self) -> bool:
        """Send a heartbeat to keep backend alive in Redis.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Update heartbeat timestamp
            heartbeat_data = {
                "backend_id": self.backend_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            heartbeat_key = self._heartbeat_key()
            await self.redis.set(heartbeat_key, json.dumps(heartbeat_data), ex=self.ttl)

            # Refresh backend info with latest runtime metadata
            await self._refresh_cluster_id()
            info = self._get_backend_info()
            info.last_heartbeat = heartbeat_data["timestamp"]
            backend_key = self._backend_key()
            await self.redis.set(backend_key, info.to_json(), ex=self.ttl)

            # Refresh set membership
            await self.redis.sadd(self.BACKENDS_SET, self.backend_id)

            logger.debug(f"Backend {self.backend_id} heartbeat sent")
            return True
        except Exception as exc:
            logger.error(f"Failed to send heartbeat: {exc}")
            return False

    async def start_heartbeat(self) -> None:
        """Start automatic heartbeat in background."""
        if self._running:
            logger.warning("Heartbeat already running")
            return

        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Backend {self.backend_id} heartbeat started")

    async def stop_heartbeat(self) -> None:
        """Stop automatic heartbeat."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        logger.info(f"Backend {self.backend_id} heartbeat stopped")

    async def _heartbeat_loop(self) -> None:
        """Background heartbeat loop."""
        while self._running:
            try:
                await self.heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Heartbeat loop error: {exc}")
                await asyncio.sleep(self.heartbeat_interval)

    @staticmethod
    async def list_backends(redis_manager: BackendRegistryRedis) -> list[str]:
        """List all registered backend IDs.

        Args:
            redis_manager: Redis connection manager

        Returns:
            List of backend IDs
        """
        try:
            backend_ids = await redis_manager.smembers(BackendRegistry.BACKENDS_SET)
            return sorted(backend_ids)
        except Exception as exc:
            logger.error(f"Failed to list backends: {exc}")
            return []

    @staticmethod
    async def get_backend_info(
        redis_manager: BackendRegistryRedis,
        backend_id: str,
    ) -> BackendInfo | None:
        """Get information about a specific backend.

        Args:
            redis_manager: Redis connection manager
            backend_id: Backend identifier

        Returns:
            Backend info or None if not found
        """
        try:
            backend_key = f"{BackendRegistry.BACKEND_INFO_PREFIX}{backend_id}"
            data = await redis_manager.get(backend_key)
            if data:
                return BackendInfo.from_json(data)
            return None
        except Exception as exc:
            logger.error(f"Failed to get backend info for {backend_id}: {exc}")
            return None

    @staticmethod
    async def get_all_backends(
        redis_manager: BackendRegistryRedis,
    ) -> dict[str, BackendInfo]:
        """Get information about all registered backends.

        Args:
            redis_manager: Redis connection manager

        Returns:
            Dictionary mapping backend IDs to BackendInfo
        """
        backends: dict[str, BackendInfo] = {}
        try:
            backend_ids = await BackendRegistry.list_backends(redis_manager)
            for backend_id in backend_ids:
                info = await BackendRegistry.get_backend_info(redis_manager, backend_id)
                if info:
                    backends[backend_id] = info
        except Exception as exc:
            logger.error(f"Failed to get all backends: {exc}")
        return backends

    @staticmethod
    async def cleanup_dead_backends(redis_manager: BackendRegistryRedis) -> int:
        """Remove dead backends from the registry.

        Args:
            redis_manager: Redis connection manager

        Returns:
            Number of backends cleaned up
        """
        cleanup_count = 0
        try:
            backend_ids = await BackendRegistry.list_backends(redis_manager)
            for backend_id in backend_ids:
                backend_key = f"{BackendRegistry.BACKEND_INFO_PREFIX}{backend_id}"
                exists = await redis_manager.exists(backend_key)
                if not exists:
                    # Backend key expired, remove from set
                    await redis_manager.srem(BackendRegistry.BACKENDS_SET, backend_id)
                    cleanup_count += 1
                    logger.info(f"Cleaned up dead backend: {backend_id}")
        except Exception as exc:
            logger.error(f"Failed to cleanup dead backends: {exc}")
        return cleanup_count
