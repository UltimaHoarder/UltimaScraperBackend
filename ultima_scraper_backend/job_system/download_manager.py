"""
Download manager for the job system.

Provides DownloadItem class and DownloadManager for managing concurrent downloads
with resource management, multi-part downloads, and progress tracking.

Compatible with UltimaScraperCollection's download_manager.py pattern.
"""

from __future__ import annotations

import asyncio
import errno
import functools
import json
import logging
import os
import re
import shutil
import ssl
import time
from collections.abc import AsyncIterable, Awaitable, Callable, Collection, Iterable
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlparse

import aiohttp
from aiohttp_socks import ProxyConnector
from ultima_scraper_api import user_types
from ultima_scraper_core.concurrency import map_unordered

from ultima_scraper_backend.config import (
    StagingDirectory,
    StagingPolicy,
    StagingUsage,
    UltimaScraperCollectionConfig,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..database.write_queue import DatabaseWriteClient
    from .metadata_models import MediaMetadata


# Type aliases for callback functions
ProgressCallback = Callable[[dict[str, Any]], Awaitable[None] | None]
CompleteCallback = Callable[[Path], None]  # (filepath)
ErrorCallback = Callable[[Exception, str], None]  # (error, url)
StatusCallback = Callable[[dict[str, Any]], Awaitable[None] | None]
DownloadedCallback = Callable[["DownloadItem"], None]


@dataclass
class DownloadWorkspace:
    drm_video_path: Path | None = None
    drm_audio_path: Path | None = None
    drm_temp_directory: Path | None = None
    selected_staging_directory: Path | None = None
    required_operations: list[StagingUsage] = field(
        default_factory=lambda: cast(list[StagingUsage], [])
    )
    staging_fallback_reason: str | None = None
    reserved_bytes: int = 0

    @property
    def work_directory(self) -> Path | None:
        return self.selected_staging_directory or self.drm_temp_directory

    @work_directory.setter
    def work_directory(self, value: Path | None) -> None:
        self.drm_temp_directory = value
        self.selected_staging_directory = value


DRMMediaSettings = DownloadWorkspace


@dataclass
class DownloadItem:
    """
    Represents a single downloadable item.

    Attributes:
        url: Download URL
        filename: Target filename (can be None, will be extracted from URL)
        directory: Target directory path
        media_id: Optional media ID for tracking
        content_id: Optional content ID for tracking
        size: Expected file size in bytes (None if unknown)
        headers: Additional HTTP headers for the request
        drm_media_settings: Transient workspace and DRM stream filepaths
    """

    url: str
    filename: str | None = None
    directory: Path | None = None
    media_id: int | None = None
    content_id: int | None = None
    size: int | None = None
    headers: dict[str, str] = field(default_factory=lambda: {})
    user: user_types | None = None
    media_metadata: MediaMetadata | None = None
    drm_media_settings: DownloadWorkspace = field(default_factory=DownloadWorkspace)
    skip: bool = False
    start_time: float | None = None
    downloaded_this_run: bool = False
    failure_message: str | None = None
    # Where the backend that keeps this file put it, when that is not here.
    offloaded_path: str | None = None

    def get_filepath(self) -> Path:
        """Get the full target filepath."""
        if not self.directory or not self.filename:
            raise ValueError("Directory and filename must be set")
        return self.directory / self.filename

    def get_workspace_directory(self) -> Path:
        """Get the temp workspace directory for the item."""
        if not self.directory:
            raise ValueError("Directory must be set")
        return self.drm_media_settings.work_directory or self.directory

    def get_workspace_filepath(self) -> Path:
        """Get the workspace filepath used for temporary download activity."""
        if not self.filename:
            raise ValueError("Filename must be set")
        return self.get_workspace_directory() / self.filename

    def start_timer(self) -> None:
        """Start the download timer."""
        self.start_time = time.perf_counter()

    def exists(self):
        """Check if the final target file already exists."""
        try:
            filepath = self.get_filepath()
            if "0i9c6ur7qolhf673em5ap" in str(filepath):
                pass
            current_filepath_exists = filepath.exists()
            if self.media_metadata and self.media_metadata.drm_media:
                # For DRM media, check if the final merged file exists
                mpd_filename = self.media_metadata.drm_media.manifest_url.split("/")[-1]
                legacy_filepath = Path(filepath).with_name(
                    Path(mpd_filename).stem + Path(filepath).suffix
                )
                if filepath != legacy_filepath and legacy_filepath.exists():
                    if current_filepath_exists:
                        filepath_size = filepath.stat().st_size
                        legacy_filepath_size = legacy_filepath.stat().st_size
                        if filepath_size == legacy_filepath_size:
                            legacy_filepath.unlink()
                        elif filepath_size - 1 == legacy_filepath_size:
                            legacy_filepath.unlink()
                        else:
                            pass
                    else:
                        pass
                else:
                    pass

            return current_filepath_exists
        except ValueError:
            return False


class ResourceManager:
    """
    Manages download resources (concurrency, buffer sizes).

    Allows dynamic adjustment of download parameters during runtime.
    """

    def __init__(
        self,
        max_concurrency: int = 64,
        buffer_size: int = 10 * 1024 * 1024,  # 10MB default
        per_domain_concurrency: int = 8,
        head_concurrency: int = 64,
        max_file_concurrency: int | None = None,
    ):
        self.max_concurrency = _positive_int(max_concurrency)
        self.buffer_size = _positive_int(buffer_size)
        self.per_domain_concurrency = _positive_int(per_domain_concurrency)
        self.head_concurrency = _positive_int(head_concurrency)
        self.max_file_concurrency = (
            self.max_concurrency
            if max_file_concurrency is None
            else _positive_int(max_file_concurrency)
        )

    @classmethod
    def from_config(
        cls,
        config: UltimaScraperCollectionConfig,
        proxy_url: str | None = None,
    ) -> ResourceManager:
        """Build limits from global or matching per-proxy download settings."""

        network = config.settings.network
        download_settings = network.downloads
        proxy_key = _proxy_endpoint_key(proxy_url)
        if proxy_key is not None:
            for proxy in network.proxies:
                if _proxy_endpoint_key(proxy.url) != proxy_key:
                    continue
                if proxy.downloads is not None:
                    download_settings = proxy.downloads
                break

        return cls(
            max_concurrency=download_settings.max_concurrency,
            max_file_concurrency=download_settings.max_file_concurrency,
            buffer_size=download_settings.buffer_size_bytes,
            per_domain_concurrency=download_settings.per_domain_concurrency,
            head_concurrency=download_settings.head_concurrency,
        )

    def get_current_concurrency(self) -> int:
        """Get current maximum concurrent part requests."""
        return self.max_concurrency

    def get_file_concurrency(self) -> int:
        """Get the independently configurable active-file admission budget."""
        return self.max_file_concurrency

    def get_current_buffer_size(self) -> int:
        """Get current buffer size for downloads (bytes)."""
        return self.buffer_size

    def get_per_domain_concurrency(self) -> int:
        """Get current per-domain concurrency limit."""
        return self.per_domain_concurrency

    def get_head_concurrency(self) -> int:
        """Get current HEAD request concurrency limit."""
        return self.head_concurrency


def _positive_int(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("Download resource limits must be positive integers")
    return value


def _proxy_endpoint_key(proxy_url: str | None) -> tuple[str, str, int | None] | None:
    normalized = _normalize_proxy_url(proxy_url)
    if normalized is None:
        return None
    try:
        parsed = urlparse(normalized)
        if not parsed.scheme or not parsed.hostname:
            return None
        return parsed.scheme.lower(), parsed.hostname.lower(), parsed.port
    except ValueError:
        return None


def _normalize_proxy_url(raw_proxy_url: str | None) -> str | None:
    if not raw_proxy_url:
        return None
    proxy_url = str(raw_proxy_url).strip()
    return proxy_url or None


def _build_ssl_context() -> ssl.SSLContext:
    ssl_context = ssl.create_default_context()
    ssl_context.post_handshake_auth = True
    return ssl_context


def _build_proxy_session_settings(
    proxy_url: str | None,
) -> tuple[aiohttp.BaseConnector | None, str | None]:
    normalized_proxy = _normalize_proxy_url(proxy_url)
    if not normalized_proxy:
        return None, None

    ssl_context = _build_ssl_context()
    scheme = urlparse(normalized_proxy).scheme.lower()
    if scheme.startswith("socks"):
        return ProxyConnector.from_url(normalized_proxy, ssl=ssl_context), None
    return aiohttp.TCPConnector(ssl=ssl_context), normalized_proxy


def _build_temp_merge_path(dest_path: Path) -> Path:
    if dest_path.suffix:
        return dest_path.with_suffix(dest_path.suffix + ".tmp")
    return dest_path.with_name(dest_path.name + ".tmp")


def _merge_parts_sync(
    dest_path: Path, part_paths: list[Path], buffer_size: int
) -> None:
    temp_dest_path = _build_temp_merge_path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with temp_dest_path.open("wb") as final_file:
            for part_path in part_paths:
                if not part_path.exists():
                    raise FileNotFoundError(f"Part file missing: {part_path}")

                with part_path.open("rb") as part_file:
                    while True:
                        data = part_file.read(buffer_size)
                        if not data:
                            break
                        final_file.write(data)

                part_path.unlink()

        os.rename(temp_dest_path, dest_path)
    except Exception:
        if temp_dest_path.exists():
            try:
                temp_dest_path.unlink()
            except OSError:
                pass
        raise

    # Downloads in this directory share the parts folder. Another merge may
    # remove it, or another download may still need it. Cleanup must not turn
    # an already published file into a failed transfer.
    parts_folder = dest_path.parent / "__parts__"
    try:
        parts_folder.rmdir()
    except FileNotFoundError:
        pass
    except OSError as exc:
        if exc.errno not in (errno.ENOTEMPTY, errno.EEXIST):
            logger.warning(
                "Merged %s but could not clean up %s: %s", dest_path, parts_folder, exc
            )


def _download_target(workspace_target: Path) -> Path:
    """The file an item's parts write into, each at its own offset."""
    return workspace_target.with_name(workspace_target.name + ".download")


def _range_ledger(target: Path) -> Path:
    """Lists the file's size, then each finished range, so a restart resumes."""
    return target.with_name(target.name + ".ranges")


def _prepare_download_target(target: Path, file_size: int) -> None:
    """Keep finished ranges of the same file; start again for anything else."""
    ledger = _range_ledger(target)
    header = f"size:{file_size}"
    try:
        lines = ledger.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = []
    if target.exists() and lines[:1] == [header]:
        return
    target.unlink(missing_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(header + "\n", encoding="utf-8")


def _range_recorded(target: Path, record: str) -> bool:
    try:
        lines = _range_ledger(target).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return False
    return record in lines[1:] and target.exists()


def _record_range(target: Path, record: str) -> None:
    with _range_ledger(target).open("a", encoding="utf-8") as ledger:
        ledger.write(record + "\n")


def _write_at(fd: int, data: bytearray, offset: int, limit: int) -> int:
    """Write up to `limit` bytes of `data` at `offset`; returns how many."""
    view = memoryview(data)[: max(limit, 0)]
    done = 0
    while done < len(view):
        done += os.pwrite(fd, view[done:], offset + done)
    return done


def _publish_download(target: Path, workspace_target: Path) -> None:
    os.replace(target, workspace_target)
    _range_ledger(target).unlink(missing_ok=True)


def _resolve_disk_usage_path(path: Path) -> Path:
    candidate = path.expanduser()
    for current in (candidate, *candidate.parents):
        if current.exists():
            return current
    return Path(candidate.anchor or "/")


def _free_space_bytes(path: Path) -> int:
    return shutil.disk_usage(_resolve_disk_usage_path(path)).free


@functools.cache
def _is_tmpfs(path: Path) -> bool:
    resolved = os.path.realpath(_resolve_disk_usage_path(path))
    best_mount, best_type = "", ""
    try:
        with open("/proc/self/mounts", encoding="utf-8") as mounts:
            for line in mounts:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mount_point = fields[1].replace("\\040", " ")
                inside = resolved == mount_point or resolved.startswith(
                    mount_point.rstrip("/") + "/"
                )
                if inside and len(mount_point) >= len(best_mount):
                    best_mount, best_type = mount_point, fields[2]
    except OSError:
        return False
    return best_type in ("tmpfs", "ramfs")


def _memory_available_bytes() -> int | None:
    try:
        with open("/proc/meminfo", encoding="utf-8") as meminfo:
            for line in meminfo:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return None


def _staging_free_bytes(path: Path) -> int:
    free = _free_space_bytes(path)
    # A tmpfs reports its size cap, not the RAM left to back it.
    if _is_tmpfs(path):
        available = _memory_available_bytes()
        if available is not None:
            free = min(free, available)
    return free


def _move_file_sync(src: Path, dest: Path) -> None:
    if src == dest:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))


class DownloadManager:
    """
    Manages concurrent downloads with resource management and progress tracking.

    Features:
    - Concurrent multi-part downloads for large files
    - Per-domain concurrency limits (fairness)
    - Automatic retry on transient failures
    - Resume support for interrupted downloads
    - Progress tracking and callbacks
    """

    def __init__(
        self,
        config: UltimaScraperCollectionConfig | None = None,
        resource_manager: ResourceManager | None = None,
        session: aiohttp.ClientSession | None = None,
        worker_id: int = 0,
        proxy_url: str | None = None,
        database_writes: DatabaseWriteClient | None = None,
        database_write_site: str | None = None,
        database_write_scope: str | None = None,
    ):
        self.config = (
            config or UltimaScraperCollectionConfig.load_or_create_default_config()
        )
        self.session = session
        self.worker_id = worker_id
        self.proxy_url = _normalize_proxy_url(proxy_url)
        self.resource_manager = resource_manager or ResourceManager.from_config(
            self.config,
            self.proxy_url,
        )
        self.database_writes = database_writes
        self.database_write_site = (
            database_write_site.strip().lower() if database_write_site else None
        )
        self.database_write_scope = database_write_scope
        self._session_connector, self._request_proxy_url = (
            _build_proxy_session_settings(self.proxy_url)
        )

        # Semaphores for concurrency control
        self.semaphore = asyncio.Semaphore(
            self.resource_manager.get_current_concurrency()
        )
        self.head_semaphore = asyncio.Semaphore(
            self.resource_manager.get_head_concurrency()
        )
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}

        # Progress tracking
        self.total = 0
        self.completed = 0
        self.failed = 0

        # Aggregate transfer telemetry. Chunk updates are throttled before they
        # reach Redis/WebSocket callbacks, while byte counters remain exact.
        self.progress_update_interval = 1.0
        self._progress_emit_lock = asyncio.Lock()
        self._transfer_started_at = time.perf_counter()
        self._last_progress_emit_at = self._transfer_started_at
        self._transferred_bytes = 0
        self._reported_transferred_bytes = 0
        self._expected_bytes = 0
        self._processed_bytes = 0
        self._expected_bytes_by_item: dict[int, int] = {}
        self._processed_parts_by_item: dict[int, dict[int, int]] = {}

        # Callbacks
        self.on_progress: list[ProgressCallback] = []
        self.on_complete: list[CompleteCallback] = []
        self.on_error: list[ErrorCallback] = []
        self.on_status: list[StatusCallback] = []
        # Called per successful item; a registered hook owns its staging release.
        self.on_downloaded: list[DownloadedCallback] = []

        # Bytes promised to in-flight items, keyed by staging directory path.
        self._staging_reserved: dict[Path, int] = {}

    async def _notify_progress(self, payload: dict[str, Any]) -> None:
        """Invoke registered progress callbacks with the provided payload."""
        if not self.on_progress:
            return

        for callback in list(self.on_progress):
            try:
                result = callback(payload)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Download progress callback failed")

    @staticmethod
    def _initial_expected_bytes(item: DownloadItem) -> int:
        # Resolved DRM streams replace the manifest/media estimate.
        if item.media_metadata is not None and item.media_metadata.drm:
            return 0
        return max(int(item.size or 0), 0)

    def _reset_transfer_progress(
        self, items: Collection[DownloadItem] | None = None
    ) -> None:
        now = time.perf_counter()
        self._transfer_started_at = now
        self._last_progress_emit_at = now
        self._transferred_bytes = 0
        self._reported_transferred_bytes = 0
        self._expected_bytes = (
            sum(self._initial_expected_bytes(item) for item in items)
            if items is not None
            else 0
        )
        self._processed_bytes = 0
        self._expected_bytes_by_item.clear()
        self._processed_parts_by_item.clear()

    def _begin_item_progress(
        self, item: DownloadItem, *, expected_precounted: bool
    ) -> None:
        expected = self._initial_expected_bytes(item)
        self._expected_bytes_by_item[id(item)] = expected
        if not expected_precounted:
            self._expected_bytes += expected

    def _finish_item_progress(self, item: DownloadItem) -> None:
        # Aggregate counters survive completion; per-item state does not.
        self._expected_bytes_by_item.pop(id(item), None)
        self._processed_parts_by_item.pop(id(item), None)

    def _record_expected_bytes(self, item: DownloadItem, expected_bytes: int) -> None:
        expected = max(int(expected_bytes or 0), 0)
        previous = self._expected_bytes_by_item.get(id(item), 0)
        self._expected_bytes_by_item[id(item)] = expected
        self._expected_bytes += expected - previous

    def _record_part_processed(
        self,
        item: DownloadItem,
        part_num: int,
        processed_bytes: int,
        expected_bytes: int,
    ) -> None:
        parts = self._processed_parts_by_item.setdefault(id(item), {})
        processed = min(max(int(processed_bytes), 0), max(int(expected_bytes), 0))
        previous = parts.get(part_num, 0)
        parts[part_num] = max(previous, processed)
        self._processed_bytes += parts[part_num] - previous

    def _mark_item_processed_if_untracked(
        self,
        item: DownloadItem,
        processed_bytes: int,
    ) -> None:
        item_key = id(item)
        expected = self._expected_bytes_by_item.get(item_key)
        if expected is None:
            return
        if item_key in self._processed_parts_by_item:
            return
        self._record_part_processed(item, -1, processed_bytes, expected)

    def _expected_transfer_bytes(self) -> int:
        return self._expected_bytes

    def _processed_transfer_bytes(self) -> int:
        return self._processed_bytes

    async def _emit_transfer_progress(
        self,
        item: DownloadItem,
        *,
        status: Literal["progress", "completed", "failed"],
        force: bool = False,
        path: str | None = None,
    ) -> None:
        if not self.on_progress:
            return

        now = time.perf_counter()
        if (
            not force
            and now - self._last_progress_emit_at < self.progress_update_interval
        ):
            return

        async with self._progress_emit_lock:
            now = time.perf_counter()
            elapsed = now - self._last_progress_emit_at
            bytes_delta = self._transferred_bytes - self._reported_transferred_bytes
            if not force and (
                elapsed < self.progress_update_interval or bytes_delta <= 0
            ):
                return

            self._last_progress_emit_at = now
            self._reported_transferred_bytes = self._transferred_bytes
            await self._notify_progress(
                {
                    "status": status,
                    "item": item,
                    "bytes": max(bytes_delta, 0),
                    "processed_bytes": self._processed_transfer_bytes(),
                    "expected_bytes": self._expected_transfer_bytes(),
                    "duration": now - self._transfer_started_at,
                    "path": path,
                    "completed": self.completed,
                    "failed": self.failed,
                    "total": self.total,
                }
            )

    async def _record_download_chunk(
        self,
        item: DownloadItem,
        chunk_size: int,
    ) -> None:
        self._transferred_bytes += max(int(chunk_size), 0)
        await self._emit_transfer_progress(item, status="progress")

    def _staging_headroom_bytes(self, directory: StagingDirectory) -> int:
        return (
            _staging_free_bytes(directory.path)
            - directory.min_free_bytes
            - self._staging_reserved.get(directory.path, 0)
        )

    def _select_staging_directory(
        self,
        required_operations: list[StagingUsage],
        needed_bytes: int = 0,
    ) -> tuple[Path | None, str | None]:
        staging_directories = list(
            self.config.settings.download_setup.staging_directories
        )
        if not staging_directories:
            return None, None

        required_set = set(required_operations)
        matching_directories = [
            directory
            for directory in staging_directories
            if required_set.issubset(set(directory.usage))
        ]
        if StagingUsage.DRM not in required_set:
            matching_directories = [
                directory
                for directory in matching_directories
                if StagingPolicy.DRM_ONLY not in directory.policy
            ]
        # Settings are shared across backends: skip staging this host cannot reach.
        from .drive_selection import directory_writable_here

        matching_directories = [
            directory for directory in matching_directories if directory_writable_here(Path(directory.path))
        ]
        if not matching_directories:
            required_names = ", ".join(
                operation.value for operation in required_operations
            )
            return (
                None,
                f"No staging directory matched required operations: {required_names}",
            )

        for directory in matching_directories:
            if StagingPolicy.PREFER not in directory.policy:
                continue
            if self._staging_headroom_bytes(directory) >= needed_bytes:
                return directory.path, None

        fallback_directories = [
            directory
            for directory in matching_directories
            if StagingPolicy.PREFER not in directory.policy
        ] or matching_directories
        best_directory = max(fallback_directories, key=self._staging_headroom_bytes)
        return best_directory.path, None

    def _prepare_item_workspace(
        self,
        item: DownloadItem,
        required_operations: list[StagingUsage],
        needed_bytes: int = 0,
    ) -> Path:
        workspace = item.drm_media_settings
        workspace.required_operations = list(required_operations)

        if workspace.work_directory is not None:
            return workspace.work_directory

        selected_directory, warning = self._select_staging_directory(
            required_operations, needed_bytes
        )
        if selected_directory is not None:
            workspace.work_directory = selected_directory
            if needed_bytes > 0:
                workspace.reserved_bytes = needed_bytes
                self._staging_reserved[selected_directory] = (
                    self._staging_reserved.get(selected_directory, 0) + needed_bytes
                )
            return selected_directory

        workspace.staging_fallback_reason = warning
        if warning:
            logger.warning(
                "[download] Falling back to destination workspace for media %s: %s",
                item.media_id,
                warning,
            )
        return item.get_filepath().parent

    def release_workspace(self, item: DownloadItem) -> None:
        """Return the item's staging reservation once its files have left."""
        workspace = item.drm_media_settings
        directory = workspace.selected_staging_directory
        if not workspace.reserved_bytes or directory is None:
            return
        remaining = self._staging_reserved.get(directory, 0) - workspace.reserved_bytes
        if remaining > 0:
            self._staging_reserved[directory] = remaining
        else:
            self._staging_reserved.pop(directory, None)
        workspace.reserved_bytes = 0

    async def _finalize_workspace_file(
        self,
        source_path: Path,
        dest_path: Path,
    ) -> None:
        await asyncio.to_thread(_move_file_sync, source_path, dest_path)

    def _get_domain_key(self, url: str) -> str:
        """Extract domain from URL for per-domain rate limiting."""
        try:
            parsed = urlparse(url)
            return parsed.hostname or "unknown"
        except Exception:
            return "unknown"

    def get_domain_semaphore(self, url: str) -> asyncio.Semaphore:
        """Get or create a semaphore for a specific domain."""
        key = self._get_domain_key(url)
        if key not in self._domain_semaphores:
            limit = self.resource_manager.get_per_domain_concurrency()
            self._domain_semaphores[key] = asyncio.Semaphore(limit)
        return self._domain_semaphores[key]

    def calculate_parts(self, file_size: int) -> int:
        """
        Calculate optimal number of parts for multi-part download.

        Args:
            file_size: Total file size in bytes

        Returns:
            Number of parts to split download into
        """
        buffer_size = self.resource_manager.get_current_buffer_size()
        max_concurrency = self.resource_manager.get_current_concurrency()

        if file_size <= buffer_size:
            return 1

        # Calculate max possible parts based on buffer size
        max_parts = -(-file_size // buffer_size)  # Ceiling division
        max_parts = max(1, max_parts)

        # Limit to available concurrency
        return min(max_parts, max_concurrency)

    async def head_request(
        self,
        item: DownloadItem,
    ) -> aiohttp.ClientResponse | None:
        """
        Perform HEAD request to get file metadata.

        Args:
            item: Download item

        Returns:
            Response object or None if failed
        """
        if not self.session:
            raise ValueError("Session not initialized")

        async with self.head_semaphore:
            try:
                async with self.session.head(
                    item.url,
                    headers=item.headers,
                    allow_redirects=True,
                    proxy=self._request_proxy_url,
                ) as response:
                    if response.status == 200:
                        return response
                    else:
                        logger.warning(
                            f"HEAD request failed: {response.status} for {item.url}"
                        )
                        return None
            except Exception as exc:
                logger.error(f"HEAD request error for {item.url}: {exc}")
                return None

    async def download_part(
        self,
        item: DownloadItem,
        start_byte: int,
        end_byte: int,
        part_num: int,
    ) -> Path | int:
        """
        Download a single part of a file.

        Args:
            item: Download item
            start_byte: Starting byte position
            end_byte: Ending byte position (inclusive)
            part_num: Part number for naming

        Returns:
            Path to the file the part was written into (shared by all the
            item's parts), or error code (1=retry, 2=abort)
        """
        if not self.session:
            raise ValueError("Session not initialized")

        domain_sem = self.get_domain_semaphore(item.url)

        async with self.semaphore, domain_sem:
            # Every part writes its range in place, in one file shared by the
            # parts, so nothing has to be joined afterwards.
            target = _download_target(item.get_workspace_filepath())
            expected_size = end_byte - start_byte + 1
            record = f"{start_byte}-{end_byte}"

            max_attempts = 10
            attempts = 0
            # Kept across attempts, so a retry continues where the last stopped.
            written = 0

            def room() -> int:
                # A known size bounds the range; without one, take what comes.
                return expected_size - written if item.size else len(buffer)

            while attempts < max_attempts:
                attempts += 1

                if await asyncio.to_thread(_range_recorded, target, record):
                    self._record_part_processed(
                        item, part_num, expected_size, expected_size
                    )
                    return target
                self._record_part_processed(item, part_num, written, expected_size)

                actual_start = start_byte + written
                headers = {
                    **item.headers,
                    "Range": f"bytes={actual_start}-{end_byte}",
                }

                try:
                    async with self.session.get(
                        item.url,
                        headers=headers,
                        allow_redirects=True,
                        proxy=self._request_proxy_url,
                    ) as response:
                        if response.status not in (200, 206):
                            logger.warning(
                                f"Download part {part_num} failed: HTTP {response.status}"
                            )
                            await asyncio.sleep(1)
                            continue
                        if response.status == 200 and actual_start > 0:
                            # The whole file, not this range: it can't go at this offset.
                            logger.error(
                                f"Download part {part_num}: the server ignored the range"
                            )
                            return 2

                        target.parent.mkdir(parents=True, exist_ok=True)
                        fd = await asyncio.to_thread(
                            os.open, target, os.O_WRONLY | os.O_CREAT, 0o644
                        )
                        try:
                            buffer = bytearray()
                            chunk_size = 65536  # 64KB chunks

                            async for chunk in response.content.iter_chunked(
                                chunk_size
                            ):
                                buffer.extend(chunk)
                                await self._record_download_chunk(item, len(chunk))

                                # Write buffer when it reaches threshold
                                if len(buffer) >= chunk_size * 16:  # Write every 1MB
                                    written += await asyncio.to_thread(
                                        _write_at,
                                        fd,
                                        buffer,
                                        start_byte + written,
                                        room(),
                                    )
                                    buffer.clear()
                                    self._record_part_processed(
                                        item,
                                        part_num,
                                        written,
                                        expected_size,
                                    )

                            # Write remaining buffer
                            if buffer:
                                written += await asyncio.to_thread(
                                    _write_at,
                                    fd,
                                    buffer,
                                    start_byte + written,
                                    room(),
                                )
                            self._record_part_processed(
                                item,
                                part_num,
                                written,
                                expected_size,
                            )
                        finally:
                            await asyncio.to_thread(os.close, fd)

                        # Verify download completed
                        if written >= expected_size:
                            await asyncio.to_thread(_record_range, target, record)
                            return target
                        else:
                            logger.warning(
                                f"Part {part_num} incomplete: {written}/{expected_size}"
                            )
                            continue

                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    logger.warning(f"Download part {part_num} error: {exc}")
                    await asyncio.sleep(1)
                    continue
                except Exception as exc:
                    logger.error(f"Unexpected error downloading part {part_num}: {exc}")
                    return 2  # Abort

            # Max attempts exceeded
            logger.error(
                f"Failed to download part {part_num} after {max_attempts} attempts"
            )
            return 2

    async def merge_parts(
        self,
        dest_path: Path,
        part_paths: list[Path],
    ) -> bool:
        """
        Merge multiple part files into final destination.

        Args:
            dest_path: Final destination path
            part_paths: List of part file paths

        Returns:
            True if successful, False otherwise
        """
        try:
            buffer_size = self.resource_manager.get_current_buffer_size()
            await asyncio.to_thread(
                _merge_parts_sync, dest_path, part_paths, buffer_size
            )
            return True

        except Exception as exc:
            logger.error(f"Error merging parts: {exc}")
            return False

    def _get_required_operations(
        self,
        item: DownloadItem,
        num_parts: int,
    ) -> list[StagingUsage]:
        required_operations = [StagingUsage.DOWNLOADS]
        media_metadata = item.media_metadata
        if media_metadata is not None and media_metadata.drm_media:
            required_operations.append(StagingUsage.DRM)
        if num_parts > 1:
            required_operations.append(StagingUsage.MERGE)
        return required_operations

    async def _download_to_workspace(
        self,
        item: DownloadItem,
        file_size: int,
        num_parts: int,
        workspace_target: Path,
    ) -> Path | Literal[False]:
        target = _download_target(workspace_target)
        await asyncio.to_thread(_prepare_download_target, target, file_size)
        if num_parts == 1:
            part_result = await self.download_part(item, 0, max(file_size - 1, 0), 0)
            if part_result == target:
                await asyncio.to_thread(_publish_download, target, workspace_target)
                return workspace_target
            if isinstance(part_result, Path):
                await self._finalize_workspace_file(part_result, workspace_target)
                return workspace_target
            logger.error(f"Part download failed with code: {part_result}")
            return False

        part_size = -(-file_size // num_parts)
        part_tasks: list[asyncio.Task[Path | int]] = []
        for part_num in range(num_parts):
            start_byte = part_num * part_size
            end_byte = min(start_byte + part_size - 1, file_size - 1)
            if start_byte > end_byte:
                break
            part_tasks.append(
                asyncio.create_task(
                    self.download_part(item, start_byte, end_byte, part_num)
                )
            )

        part_results = await asyncio.gather(*part_tasks)
        part_paths: list[Path] = []
        for result in part_results:
            if isinstance(result, Path):
                part_paths.append(result)
            else:
                logger.error(f"Part download failed with code: {result}")
                return False

        if all(path == target for path in part_paths):
            # Written in place: publishing is a rename.
            await asyncio.to_thread(_publish_download, target, workspace_target)
            return workspace_target
        if not await self.merge_parts(workspace_target, part_paths):
            return False
        return workspace_target

    async def _complete_download(
        self,
        item: DownloadItem,
        bytes_written: int,
    ) -> bool:
        self._mark_item_processed_if_untracked(item, bytes_written)
        self.completed += 1
        await self._emit_transfer_progress(
            item,
            status="completed",
            force=True,
            path=str(item.get_filepath()),
        )
        return True

    async def _fail_download(
        self,
        item: DownloadItem,
    ) -> Literal[False]:
        self.failed += 1
        await self._emit_transfer_progress(
            item,
            status="failed",
            force=True,
            path=str(item.get_filepath()),
        )
        return False

    async def _download_drm(
        self,
        item: DownloadItem,
    ) -> bool:
        logger.info(f"[DRM] Downloading encrypted streams for media {item.media_id}")
        media_metadata = item.media_metadata
        drm_media = media_metadata.drm_media if media_metadata is not None else None
        user = item.user
        authed = (
            getattr(user, "get_authed", lambda: None)() if user is not None else None
        )
        only_drm = getattr(authed, "drm", None) if authed is not None else None
        if (
            media_metadata is None
            or drm_media is None
            or user is None
            or only_drm is None
        ):
            logger.error("[DRM] Missing DRM context for media %s", item.media_id)
            return await self._fail_download(item)

        cookies = drm_media.get_cookies()
        video_url, audio_url, key = await only_drm.resolve_drm(drm_media)
        media_metadata.drm_key = key

        resolve_filename = getattr(drm_media, "resolve_filename", None)
        if callable(resolve_filename):
            resolved_filename = resolve_filename()
            if resolved_filename:
                item.filename = media_metadata.render_target_filename(
                    resolved_filename,
                    api_user=user,
                    directory_hint=item.directory,
                )
                media_metadata.filename = item.filename

        dest_path = item.get_filepath()

        stream_urls = {
            "video": video_url,
            "audio": audio_url,
        }
        available_streams = {
            stream_type: stream_url
            for stream_type, stream_url in stream_urls.items()
            if stream_url
        }
        if not available_streams:
            logger.error("[DRM] No DRM streams resolved for media %s", item.media_id)
            return await self._fail_download(item)

        stream_items: dict[str, DownloadItem] = {}
        for stream_type, stream_url in available_streams.items():
            stream_ext = (
                "m4a"
                if stream_type == "audio" and "video" not in available_streams
                else "mp4"
            )
            stream_item = DownloadItem(
                user=user,
                url=stream_url,
                filename=f"{dest_path.stem}_{stream_type}.enc.{stream_ext}",
                headers={"Cookie": cookies},
                media_metadata=media_metadata,
            )
            stream_items[stream_type] = stream_item

        downloaded = False
        try:
            stream_type_by_index = list(stream_items)
            async with asyncio.TaskGroup() as heads:
                head_tasks = [
                    heads.create_task(self.head_request(stream_item))
                    for stream_item in stream_items.values()
                ]
            stream_sizes: dict[str, int] = {}
            for stream_type, head_task in zip(
                stream_type_by_index, head_tasks, strict=False
            ):
                head_response = head_task.result()
                if not head_response:
                    logger.error(
                        "[DRM] Failed to prepare %s stream for media %s",
                        stream_type,
                        item.media_id,
                    )
                    return await self._fail_download(item)
                stream_sizes[stream_type] = int(head_response.content_length or 0)

            # Encrypted and decrypted copies coexist during decrypt and merge.
            drm_work_directory = self._prepare_item_workspace(
                item,
                [
                    StagingUsage.DOWNLOADS,
                    StagingUsage.DRM,
                    StagingUsage.MERGE,
                ],
                needed_bytes=2 * sum(stream_sizes.values()),
            )
            item.drm_media_settings.drm_temp_directory = drm_work_directory
            drm_work_directory.mkdir(parents=True, exist_ok=True)

            stream_plans: dict[str, tuple[DownloadItem, int, int, Path]] = {}
            for stream_type in stream_type_by_index:
                stream_item = stream_items[stream_type]
                stream_item.directory = drm_work_directory
                stream_item.drm_media_settings.work_directory = drm_work_directory
                file_size, num_parts, workspace_target = self._plan_download(
                    stream_item, stream_sizes[stream_type]
                )
                assert stream_item.media_metadata is not None
                assert stream_item.media_metadata.drm_media is not None
                if stream_type == "video":
                    stream_item.media_metadata.drm_media.video_size = file_size
                if stream_type == "audio":
                    stream_item.media_metadata.drm_media.audio_size = file_size
                if file_size <= 0:
                    logger.error(
                        "[DRM] Invalid %s stream size for media %s",
                        stream_type,
                        item.media_id,
                    )
                    return await self._fail_download(item)
                stream_plans[stream_type] = (
                    stream_item,
                    file_size,
                    num_parts,
                    workspace_target,
                )

            if item.media_id is not None:
                if self.database_writes is None or self.database_write_site is None:
                    raise RuntimeError("Database writer is required for DRM persistence")
                from ..database.write_queue import (
                    DatabaseWriteOperation,
                    stable_command_id,
                )

                drm_write_payload = {
                    "media_id": item.media_id,
                    "video_url": video_url,
                    "audio_url": audio_url,
                    "video_size": getattr(drm_media, "video_size", None),
                    "audio_size": getattr(drm_media, "audio_size", None),
                }
                await self.database_writes.submit(
                    DatabaseWriteOperation.UPDATE_DRM_STREAMS,
                    drm_write_payload,
                    site=self.database_write_site,
                    command_id=stable_command_id(
                        "drm-streams-v1",
                        self.database_write_scope or "download",
                        item.media_id,
                        json.dumps(
                            drm_write_payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )

            async with asyncio.TaskGroup() as downloads:
                stream_downloads = [
                    downloads.create_task(
                        self._download_to_workspace(
                            stream_item,
                            file_size,
                            num_parts,
                            workspace_target,
                        )
                    )
                    for stream_item, file_size, num_parts, workspace_target in stream_plans.values()
                ]
            download_results = [task.result() for task in stream_downloads]
            if any(result is False for result in download_results):
                result_summary = {
                    stream_type: result
                    for stream_type, result in zip(
                        stream_type_by_index, download_results, strict=False
                    )
                }
                logger.error(
                    "[DRM] Stream download failed for media %s: %s",
                    item.media_id,
                    result_summary,
                )
                return await self._fail_download(item)

            resolved_paths = {
                stream_type: result
                for stream_type, result in zip(
                    stream_type_by_index, download_results, strict=False
                )
                if isinstance(result, Path)
            }
            item.drm_media_settings.drm_video_path = resolved_paths.get("video")
            item.drm_media_settings.drm_audio_path = resolved_paths.get("audio")
            total_size = sum(path.stat().st_size for path in resolved_paths.values())
            downloaded = await self._complete_download(item, total_size)
            return downloaded
        finally:
            for stream_item in stream_items.values():
                self._finish_item_progress(stream_item)
            # Decryption still needs the staged streams after a success.
            if not downloaded:
                self.release_workspace(item)

    async def prepare_download(
        self, item: DownloadItem
    ) -> tuple[int, int, Path] | Literal[False]:
        head_response = await self.head_request(item)
        if not head_response:
            logger.error(f"Failed to get file info for {item.url}")
            return False
        return self._plan_download(item, head_response.content_length or item.size or 0)

    def _plan_download(self, item: DownloadItem, file_size: int) -> tuple[int, int, Path]:
        dest_path = item.get_filepath()
        if file_size == 0:
            logger.warning(f"File size unknown for {item.url}")
        item.size = file_size
        self._record_expected_bytes(item, file_size)
        num_parts = self.calculate_parts(file_size) if file_size > 0 else 1
        required_operations = self._get_required_operations(item, num_parts)
        workspace_directory = self._prepare_item_workspace(
            item, required_operations, needed_bytes=file_size
        )
        workspace_target = workspace_directory / dest_path.name
        return file_size, num_parts, workspace_target

    async def _download_regular(
        self,
        item: DownloadItem,
    ) -> bool:
        dest_path = item.get_filepath()
        try:
            prepared_download = await self.prepare_download(item)
            if not prepared_download:
                return await self._fail_download(item)
            file_size, num_parts, workspace_target = prepared_download

            workspace_result = await self._download_to_workspace(
                item,
                file_size,
                num_parts,
                workspace_target,
            )
            if workspace_result is False:
                return await self._fail_download(item)
            await self._finalize_workspace_file(workspace_result, dest_path)
        finally:
            self.release_workspace(item)
        final_size = dest_path.stat().st_size
        item.size = final_size
        item.downloaded_this_run = True
        return await self._complete_download(item, final_size)

    async def download(self, item: DownloadItem) -> bool:
        """
        Download a single item.

        Args:
            item: Download item

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise ValueError("Session not initialized")
        item.start_timer()
        item.failure_message = None
        dest_path = item.get_filepath()

        try:
            # Check if file already exists
            if dest_path.exists():
                existing_size = dest_path.stat().st_size
                known_size = item.size
                if known_size is None and not (
                    item.media_metadata and item.media_metadata.drm
                ):
                    head_response = await self.head_request(item)
                    if head_response and head_response.content_length:
                        known_size = head_response.content_length
                        item.size = known_size
                if known_size is not None and existing_size == known_size:
                    logger.debug(f"File already exists: {dest_path}")
                    return await self._complete_download(item, existing_size)

            if item.media_metadata and item.media_metadata.drm:
                return await self._download_drm(item)
            return await self._download_regular(item)

        except Exception as exc:
            # Exception URLs may contain signed credentials; retain the cause only.
            safe_message = re.sub(r"https?://[^\s'\"]+", "<redacted URL>", str(exc))
            item.failure_message = f"{type(exc).__name__}: {safe_message}"[:1000]
            logger.error(f"Error downloading {item.url}: {exc}")
            return await self._fail_download(item)

    @asynccontextmanager
    async def session_scope(self):
        """Reuse one HTTP pool across metadata batches and close owned sessions."""
        created = False
        try:
            if self.session is None:
                self.session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=300),
                    connector=self._session_connector or aiohttp.TCPConnector(ssl=_build_ssl_context()),
                )
                created = True
            yield self
        finally:
            if created and self.session is not None:
                await self.session.close()
                self.session = None

    async def bulk_download(
        self,
        items: Iterable[DownloadItem] | AsyncIterable[DownloadItem],
        create_session: bool = True,
    ) -> dict[str, Any]:
        """
        Download multiple items concurrently.

        Args:
            items: Download items, admitted incrementally with backpressure
            create_session: Whether to create a new session (vs using existing)

        Returns:
            Dictionary with download statistics
        """
        known_items = items if isinstance(items, Collection) else None
        self.total = len(known_items) if known_items is not None else 0
        self.completed = 0
        self.failed = 0
        self._reset_transfer_progress(known_items)

        async def download_item(item: DownloadItem) -> bool:
            if known_items is None:
                self.total += 1
            self._begin_item_progress(item, expected_precounted=known_items is not None)
            try:
                downloaded = await self.download(item)
            finally:
                self._finish_item_progress(item)
            if downloaded and self.on_downloaded:
                for callback in list(self.on_downloaded):
                    callback(item)
            else:
                self.release_workspace(item)
            return downloaded

        session_created = False
        try:
            if create_session and not self.session:
                timeout = aiohttp.ClientTimeout(total=300)
                connector = self._session_connector or aiohttp.TCPConnector(
                    ssl=_build_ssl_context()
                )
                self.session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                )
                session_created = True

            if not self.session:
                raise ValueError("No session available")

            async with aclosing(
                map_unordered(
                    download_item,
                    items,
                    limit=self.resource_manager.get_file_concurrency(),
                )
            ) as results:
                async for _ in results:
                    pass

            return {
                "total": self.total,
                "completed": self.completed,
                "failed": self.failed,
                "success_rate": (
                    (self.completed / self.total * 100) if self.total > 0 else 0
                ),
            }

        finally:
            if session_created and self.session:
                await self.session.close()
                self.session = None


__all__ = ["DownloadItem", "DownloadManager", "ResourceManager"]
