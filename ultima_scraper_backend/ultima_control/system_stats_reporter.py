"""System stats reporter for UltimaScraper backend monitoring.

Collects and reports system metrics (CPU, memory, disk I/O, network I/O) to Redis
for live monitoring. Runs periodically without blocking.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import psutil
from ultima_scraper_core.processes import finish_owned, run_blocking
from ultima_scraper_backend.cluster import publish_presence
from ultima_scraper_backend.redis.client import RedisManager

PSUTIL_AVAILABLE = True

logger = logging.getLogger(__name__)


def _empty_disks() -> list[DiskStats]:
    return []


def _empty_network() -> list[NetworkStats]:
    return []


@dataclass
class DiskStats:
    """Disk usage and I/O statistics."""

    device: str
    mount_point: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float
    read_bytes: int = 0
    write_bytes: int = 0
    read_bytes_per_sec: float = 0.0
    write_bytes_per_sec: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class NetworkStats:
    """Network I/O statistics."""

    interface: str
    bytes_sent: int
    bytes_recv: int
    bytes_sent_per_sec: float = 0.0
    bytes_recv_per_sec: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class SystemStats:
    """Complete system statistics snapshot."""

    backend_id: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # CPU stats
    cpu_percent: float = 0.0
    cpu_count: int = 0

    # Memory stats (in bytes)
    memory_total: int = 0
    memory_used: int = 0
    memory_free: int = 0
    memory_percent: float = 0.0

    # Disk stats
    disks: list[DiskStats] = field(default_factory=_empty_disks)

    # Network stats (optional)
    network: list[NetworkStats] = field(default_factory=_empty_network)

    def to_dict(self) -> dict[str, Any]:
        """Convert nested dataclasses once, preserving the public payload."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> SystemStats:
        """Create from JSON string."""
        data = json.loads(json_str)
        # Reconstruct nested dataclasses
        if "disks" in data:
            data["disks"] = [DiskStats(**d) for d in data["disks"]]
        if "network" in data:
            data["network"] = [NetworkStats(**n) for n in data["network"]]
        return cls(**data)


class SystemStatsReporter:
    """Collects and reports system metrics to Redis periodically."""

    # Redis key pattern: ultimascraper:backend:stats:{backend_id}
    STATS_KEY_PREFIX = "ultimascraper:backend:stats:"

    def __init__(
        self,
        redis_manager: RedisManager,
        backend_id: str,
        interval: float = 5.0,
        enable_network: bool = False,
        ttl: int = 60,
    ) -> None:
        """Initialize system stats reporter.

        Args:
            redis_manager: Redis connection manager
            backend_id: Unique backend identifier
            interval: Update interval in seconds (default: 5.0)
            enable_network: Whether to collect network I/O stats (default: False)
            ttl: Time-to-live for stats keys in Redis (default: 60 seconds)
        """
        if not PSUTIL_AVAILABLE:
            raise RuntimeError(
                "psutil is required for system stats collection. "
                "Install it with: pip install psutil"
            )

        self.redis = redis_manager
        self.backend_id = backend_id
        self.interval = interval
        self.enable_network = enable_network
        self.ttl = ttl

        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._stop_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()

        # Track previous I/O counters for speed calculation
        self._prev_disk_io: dict[str, tuple[int, int]] = {}  # device -> (read, write)
        self._prev_network_io: dict[str, tuple[int, int]] = {}  # interface -> (sent, recv)
        self._prev_disk_timestamp: float | None = None
        self._prev_network_timestamp: float | None = None

    def _collect_cpu_stats(self) -> tuple[float, int]:
        """Collect CPU statistics."""
        return psutil.cpu_percent(interval=0.1), psutil.cpu_count() or 0

    def _collect_memory_stats(self) -> tuple[int, int, int, float]:
        """Collect memory statistics."""
        mem = psutil.virtual_memory()
        return mem.total, mem.used, mem.available, mem.percent

    def _collect_disk_stats(self) -> list[DiskStats]:
        """Collect disk usage and I/O statistics."""
        disks: list[DiskStats] = []

        # Get all disk partitions
        partitions = psutil.disk_partitions(all=False)

        # Get disk I/O counters
        try:
            disk_io = psutil.disk_io_counters(perdisk=True)
        except Exception as exc:
            logger.debug(f"Could not read disk I/O counters: {exc}")
            disk_io = {}

        current_timestamp = time.monotonic()
        time_delta = (
            current_timestamp - self._prev_disk_timestamp
            if self._prev_disk_timestamp is not None
            else 0.0
        )
        next_disk_io: dict[str, tuple[int, int]] = {}

        for partition in partitions:
            try:
                usage = psutil.disk_usage(partition.mountpoint)

                # Find device name (without /dev/ prefix)
                device = partition.device.split("/")[-1]

                # Get I/O stats if available
                read_bytes = 0
                write_bytes = 0
                read_speed = 0.0
                write_speed = 0.0

                if device in disk_io:
                    io_stats = disk_io[device]
                    read_bytes = io_stats.read_bytes
                    write_bytes = io_stats.write_bytes

                    # Calculate speed if we have previous data
                    if device in self._prev_disk_io and time_delta > 0:
                        prev_read, prev_write = self._prev_disk_io[device]
                        read_speed = max(0, read_bytes - prev_read) / time_delta
                        write_speed = max(0, write_bytes - prev_write) / time_delta

                    # Store current values for next iteration
                    next_disk_io[device] = (read_bytes, write_bytes)

                disk_stat = DiskStats(
                    device=device,
                    mount_point=partition.mountpoint,
                    total_bytes=usage.total,
                    used_bytes=usage.used,
                    free_bytes=usage.free,
                    percent_used=usage.percent,
                    read_bytes=read_bytes,
                    write_bytes=write_bytes,
                    read_bytes_per_sec=read_speed,
                    write_bytes_per_sec=write_speed,
                )
                disks.append(disk_stat)

            except (PermissionError, OSError) as exc:
                logger.debug(f"Could not read disk stats for {partition.mountpoint}: {exc}")
                continue

        self._prev_disk_io = next_disk_io
        self._prev_disk_timestamp = current_timestamp
        return disks

    def _collect_network_stats(self) -> list[NetworkStats]:
        """Collect network I/O statistics (optional)."""
        if not self.enable_network:
            self._prev_network_io.clear()
            self._prev_network_timestamp = None
            return []

        networks: list[NetworkStats] = []

        try:
            net_io = psutil.net_io_counters(pernic=True)
        except Exception as exc:
            logger.debug(f"Could not read network I/O counters: {exc}")
            self._prev_network_io.clear()
            self._prev_network_timestamp = None
            return []

        current_timestamp = time.monotonic()
        time_delta = (
            current_timestamp - self._prev_network_timestamp
            if self._prev_network_timestamp is not None
            else 0.0
        )
        next_network_io: dict[str, tuple[int, int]] = {}

        for interface, stats in net_io.items():
            bytes_sent = stats.bytes_sent
            bytes_recv = stats.bytes_recv
            sent_speed = 0.0
            recv_speed = 0.0

            # Calculate speed if we have previous data
            if interface in self._prev_network_io and time_delta > 0:
                prev_sent, prev_recv = self._prev_network_io[interface]
                sent_speed = max(0, bytes_sent - prev_sent) / time_delta
                recv_speed = max(0, bytes_recv - prev_recv) / time_delta

            # Store current values for next iteration
            next_network_io[interface] = (bytes_sent, bytes_recv)

            net_stat = NetworkStats(
                interface=interface,
                bytes_sent=bytes_sent,
                bytes_recv=bytes_recv,
                bytes_sent_per_sec=sent_speed,
                bytes_recv_per_sec=recv_speed,
            )
            networks.append(net_stat)

        self._prev_network_io = next_network_io
        self._prev_network_timestamp = current_timestamp
        return networks

    def _collect_stats(self) -> SystemStats:
        """Collect all system statistics."""
        stats = SystemStats(backend_id=self.backend_id)

        try:
            # CPU stats
            stats.cpu_percent, stats.cpu_count = self._collect_cpu_stats()

            # Memory stats
            (
                stats.memory_total,
                stats.memory_used,
                stats.memory_free,
                stats.memory_percent,
            ) = self._collect_memory_stats()

            # Disk stats
            stats.disks = self._collect_disk_stats()

            # Network stats (optional)
            stats.network = self._collect_network_stats()

        except Exception as exc:
            logger.error(f"Error collecting system stats: {exc}", exc_info=True)

        return stats

    def _collect_report(self) -> tuple[SystemStats, str]:
        """Collect and serialize in the same owned worker operation."""
        stats = self._collect_stats()
        return stats, stats.to_json()

    async def _report_loop(self) -> None:
        """Main reporting loop - runs periodically."""
        logger.info(
            f"System stats reporter started for backend {self.backend_id} "
            f"(interval={self.interval}s, network={self.enable_network})"
        )

        try:
            while self._running:
                try:
                    # Collect stats
                    stats, payload = await run_blocking(self._collect_report)
                    if not self._running:
                        break

                    # Store in Redis with TTL
                    key = f"{self.STATS_KEY_PREFIX}{self.backend_id}"
                    await self.redis.set(key, payload, ex=self.ttl)
                    try:
                        await publish_presence(self.redis, self.backend_id, self.ttl)
                    except Exception:
                        logger.debug("Could not publish backend presence", exc_info=True)

                    logger.debug(
                        f"Published system stats to Redis: CPU={stats.cpu_percent:.1f}%, "
                        f"MEM={stats.memory_percent:.1f}%, "
                        f"Disks={len(stats.disks)}"
                    )

                except Exception as exc:
                    logger.error(f"Error in stats reporting loop: {exc}", exc_info=True)

                # Wait for next interval or stop signal
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                    break  # Stop signal received
                except asyncio.TimeoutError:
                    pass  # Continue to next iteration
        finally:
            self._running = False
            self._stop_event.set()
            logger.info(f"System stats reporter stopped for backend {self.backend_id}")

    async def start(self) -> None:
        """Start once, after any preceding reporter has completely stopped."""
        async with self._lifecycle_lock:
            if self._running:
                logger.warning("System stats reporter is already running")
                return

            self._running = True
            self._stop_event.clear()
            self._prev_disk_io.clear()
            self._prev_network_io.clear()
            self._prev_disk_timestamp = None
            self._prev_network_timestamp = None
            self._task = asyncio.create_task(self._report_loop())
            logger.info(f"System stats reporter starting for backend {self.backend_id}")

    async def stop(self) -> None:
        """Stop admission and wait for owned sampling before releasing state."""
        async with self._lifecycle_lock:
            task = self._task
            self._running = False
            self._stop_event.set()
            if task is None:
                return

            async def finish() -> None:
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("Stats reporter did not stop gracefully, cancelling")
                    task.cancel()
                except asyncio.CancelledError:
                    pass  # An already-cancelled reporter is still fully joined.
                try:
                    await finish_owned(task)
                except asyncio.CancelledError:
                    pass

            cleanup = asyncio.create_task(finish())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await finish_owned(cleanup)
                raise
            finally:
                self._task = None
                self._prev_disk_io.clear()
                self._prev_network_io.clear()
                self._prev_disk_timestamp = None
                self._prev_network_timestamp = None
            logger.info(f"System stats reporter stopped for backend {self.backend_id}")

    @property
    def is_running(self) -> bool:
        """Check if reporter is running."""
        return self._running

    @staticmethod
    def get_stats_key(backend_id: str) -> str:
        """Get Redis key for a backend's stats.

        Args:
            backend_id: Backend identifier

        Returns:
            Redis key string
        """
        return f"{SystemStatsReporter.STATS_KEY_PREFIX}{backend_id}"

    @staticmethod
    async def fetch_stats(
        redis_manager: RedisManager,
        backend_id: str,
    ) -> SystemStats | None:
        """Fetch system stats for a backend from Redis.

        Args:
            redis_manager: Redis connection manager
            backend_id: Backend identifier

        Returns:
            SystemStats if found, None otherwise
        """
        try:
            key = SystemStatsReporter.get_stats_key(backend_id)
            data = await redis_manager.get(key)
            if data:
                return SystemStats.from_json(data)
        except Exception as exc:
            logger.error(f"Error fetching stats for {backend_id}: {exc}")
        return None
