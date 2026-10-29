"""System API routes.

Provides endpoints for system information including drives and backend status.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import socket
import threading
import time
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

import psutil
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ...config import Drive, UltimaScraperCollectionConfig
from ...config_settings_sync import (
    ConfigVersionConflict,
    ConfigVersionRequired,
    PersistedConfigSnapshot,
    persist_config_snapshot,
)
from ...database.factory import MultiSiteDatabaseManager
from ...database.threads import run_database_call
from ...models.backend import BackendInstance
from ...services.settings_service import SettingsService
from ...storage_config_propagation import (
    StorageConfigPropagation,
    propagate_storage_config,
)
from ..dependencies import get_backend_instance, get_config
from .setup import (
    DatabaseConnectionRequest,
    RedisConnectionRequest,
    _configured_database_request,
    _configured_redis_request,
    get_backend_startup_id,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/system", tags=["system"])


class DriveInfo(BaseModel):
    """Drive information."""

    path: str
    device: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float
    is_configured: bool = False
    directory_count: int = 0
    configured_directories: list[str] = []


class DrivesResponse(BaseModel):
    """Response with all detected drives."""

    backend_id: str
    drives: list[DriveInfo]


class AddDirectoryRequest(BaseModel):
    """Request to add a directory to a drive."""

    drive_path: str  # Mount point of the drive
    directory_path: str  # Full path to the directory to add
    setup_type: str = "download"  # "download" or "metadata"
    target_backend_id: str | None = None
    expected_version: int | None = None


class AddDirectoryResponse(BaseModel):
    """Response after adding a directory."""

    success: bool
    message: str
    directory_path: str | None = None
    propagation: StorageConfigPropagation | None = None


class RemoveDirectoryRequest(BaseModel):
    """Request to remove a directory from config (does not delete files)."""

    directory_path: str
    setup_type: str = "download"  # "download" or "metadata"
    target_backend_id: str | None = None
    expected_version: int | None = None


class RemoveDirectoryResponse(BaseModel):
    """Response after removing a directory."""

    success: bool
    message: str
    propagation: StorageConfigPropagation | None = None


class DirectoryConfigInfo(BaseModel):
    """Detailed configuration info for a directory."""

    path: str
    setup_type: str  # "download" or "metadata"
    minimum_space: int
    store: bool
    scan_existing: bool
    overflow: bool


class DirectoryConfigRequest(BaseModel):
    """Request to update directory configuration."""

    directory_path: str
    setup_type: str
    minimum_space: int | None = None
    store: bool | None = None
    scan_existing: bool | None = None
    overflow: bool | None = None
    target_backend_id: str | None = None
    expected_version: int | None = None


class DirectoryConfigResponse(BaseModel):
    """Response with directory configuration."""

    success: bool
    message: str
    config: DirectoryConfigInfo | None = None
    propagation: StorageConfigPropagation | None = None


class WorkerInfo(BaseModel):
    """Worker information."""

    worker_id: str
    status: str  # idle, busy, offline
    current_job: str | None = None
    jobs_completed: int = 0


class BackendDiskStats(BaseModel):
    """Current disk utilization and I/O for one backend mount."""

    device: str
    mount_point: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float
    read_bytes_per_sec: float = 0.0
    write_bytes_per_sec: float = 0.0


class BackendSystemStats(BaseModel):
    """Latest host telemetry snapshot reported by the backend."""

    timestamp: str
    cpu_percent: float
    cpu_count: int
    memory_total: int
    memory_used: int
    memory_free: int
    memory_percent: float
    disks: list[BackendDiskStats] = []


# Raised when the WebUI-facing API changes incompatibly.
API_VERSION = 1


class BackendIdentityResponse(BaseModel):
    """Who this backend is, for a WebUI holding several of them."""

    backend_id: str
    # Changes each time the backend starts. Two deployments that report the
    # same backend_id differ here, so a WebUI can say they collide.
    instance: str
    # Shared by every backend on the same database; None before setup.
    cluster_id: str | None
    # Where other machines reach this API (its HTTPS listener), if it has one.
    api_url: str | None
    version: str
    api_version: int
    # SHA-256 of the HTTPS certificate, so a client can pin a self-signed one.
    tls_fingerprint: str | None = None
    capabilities: list[str]


class BackendStatusResponse(BaseModel):
    """Backend system status (local backend only)."""

    backend_id: str
    distributed_mode: bool
    redis_connected: bool
    worker_count: int
    active_workers: int
    pending_jobs: int
    workers: list[WorkerInfo]
    system_stats: BackendSystemStats | None = None
    max_workers: int | None = None


class ProcessMemoryInfo(BaseModel):
    pid: int
    rss_bytes: int
    vms_bytes: int
    memory_percent: float


class WorkerMemoryInfo(BaseModel):
    worker_id: str
    rss_bytes: int = 0
    vms_bytes: int = 0
    memory_percent: float = 0.0
    auth_count: int = 0
    guest_auth_count: int = 0
    cached_user_count: int = 0
    cached_content_count: int = 0
    websocket_connections: int = 0
    websocket_message_history: int = 0
    websocket_event_history: int = 0
    log_queue_entries: int = 0
    log_queue_bytes: int = 0
    log_dropped: int = 0
    log_publish_failures: int = 0


class RedisMemoryInfo(BaseModel):
    used_bytes: int = 0
    peak_bytes: int = 0
    max_bytes: int = 0
    maxmemory_policy: str = "unknown"
    key_count: int = 0


class MemoryStatusResponse(BaseModel):
    backend_id: str
    process: ProcessMemoryInfo
    media_proxy_cache: dict[str, int]
    redis: RedisMemoryInfo | None = None
    workers: list[WorkerMemoryInfo] = []


class RemoteBackendDrive(BaseModel):
    """Drive info from a remote backend."""

    path: str
    device: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float
    configured_directories: list[str] = []


class RemoteBackendWorker(BaseModel):
    """Worker info from a remote backend."""

    worker_id: str
    status: str
    current_job: str | None = None
    jobs_completed: int = 0


class RemoteBackendInfo(BaseModel):
    """Status of a remote backend retrieved from Redis."""

    backend_id: str
    hostname: str
    status: str
    last_seen: str
    max_workers: int
    version: str
    drives: list[RemoteBackendDrive] = []
    workers: list[RemoteBackendWorker] = []
    active_workers: int = 0
    pending_jobs: int = 0
    cluster_id: str | None = None
    api_url: str | None = None
    tls_fingerprint: str | None = None
    # The machine's network address (e.g. for SSH), when it is known.
    address: str | None = None


class AllBackendsResponse(BaseModel):
    """Response with all backends from Redis."""

    success: bool
    local_backend_id: str
    local_hostname: str
    backends: list[RemoteBackendInfo]
    total_backends: int


class SupportMatrix(BaseModel):
    """Supported runtime surfaces exposed to the WebUI shell."""

    ui_mode: str = "webui"
    tui_supported: bool = False
    runtime_database: str = "postgresql"
    postgres_required: bool = True
    sqlite_runtime_supported: bool = False
    sqlite_import_supported: bool = True


class BootstrapFeatures(BaseModel):
    """Feature flags used by the frontend app shell."""

    webhook_stream: bool = True
    startup_migrations: bool = True
    distributed_mode: bool
    remote_backends: bool
    system_bootstrap: bool = True


class SetupSummary(BaseModel):
    """Small setup summary embedded in the bootstrap payload."""

    is_configured: bool = False
    has_config: bool = False
    has_database: bool = False
    data_directory: str = ""
    startup_id: str = ""


class SystemBootstrapResponse(BaseModel):
    """Bootstrap payload used by the WebUI shell."""

    backend_id: str
    hostname: str
    version: str
    supported_sites: list[str]
    setup: SetupSummary
    features: BootstrapFeatures
    support_matrix: SupportMatrix


class DatabaseTableOverview(BaseModel):
    """Database table statistics and vacuum history."""

    table_name: str
    live_tuples: int = 0
    dead_tuples: int = 0
    total_bytes: int = 0
    table_bytes: int = 0
    index_bytes: int = 0
    last_vacuum: str | None = None
    last_autovacuum: str | None = None
    last_analyze: str | None = None
    last_autoanalyze: str | None = None


class DatabaseOverview(BaseModel):
    """Database site / schema status and resource usage."""

    site: str
    database_name: str
    schema_name: str
    host: str
    port: int | None = None
    is_connected: bool = True
    status: Literal["healthy", "warning", "offline"] = "healthy"
    total_size_bytes: int = 0
    total_size_pretty: str = "0 B"
    table_count: int = 0
    total_live_tuples: int = 0
    # PostgreSQL row counts come from planner statistics, not a COUNT(*).
    row_counts_estimated: bool = False
    total_dead_tuples: int = 0
    bloat_percent: float = 0.0
    last_vacuum: str | None = None
    last_analyze: str | None = None
    tables: list[DatabaseTableOverview] = []


class DatabaseActiveOperation(BaseModel):
    """Active or recently tracked database maintenance operation."""

    id: str
    operation_type: Literal["vacuum", "analyze", "reindex"]
    site: str
    table_name: str | None = None
    current_table: str | None = None
    tables_completed: int = 0
    tables_total: int = 0
    phase: str | None = None
    progress_percent: float | None = None
    heap_blks_total: int | None = None
    heap_blks_scanned: int | None = None
    heap_blks_vacuumed: int | None = None
    started_at: str
    duration_seconds: float = 0.0
    status: Literal["running", "completed", "failed"] = "running"
    message: str | None = None


class DatabaseCancelRequest(BaseModel):
    """Request to cancel an active maintenance operation."""

    site: str | None = None
    table_name: str | None = None
    operation_id: str | None = None


class DatabaseCancelResponse(BaseModel):
    """Response from cancel operation."""

    success: bool
    message: str
    cancelled_count: int = 0


class DatabaseOverviewResponse(BaseModel):
    """Response containing overview for all database instances/schemas."""

    success: bool = True
    backend_id: str
    databases: list[DatabaseOverview] = []
    total_databases: int = 0
    total_size_bytes: int = 0
    server_version: str | None = None
    active_operations: list[DatabaseActiveOperation] = []


class DatabaseVacuumRequest(BaseModel):
    """Request to vacuum database or specific table."""

    site: str = "all"
    table_name: str | None = None
    full: bool = False
    analyze: bool = True
    freeze: bool = False


class DatabaseVacuumResponse(BaseModel):
    """Response from database vacuum operation."""

    success: bool
    message: str
    site: str
    table_name: str | None = None
    mode: str
    duration_seconds: float
    size_before_bytes: int | None = None
    size_after_bytes: int | None = None


class DatabaseAnalyzeRequest(BaseModel):
    """Request to analyze database tables for planner statistics."""

    site: str = "all"
    table_name: str | None = None


class DatabaseAnalyzeResponse(BaseModel):
    """Response from analyze operation."""

    success: bool
    message: str
    site: str
    table_name: str | None = None
    duration_seconds: float


class DatabaseReindexRequest(BaseModel):
    """Request to reindex database tables or indexes."""

    site: str = "all"
    table_name: str | None = None
    concurrently: bool = True


class DatabaseReindexResponse(BaseModel):
    """Response from reindex operation."""

    success: bool
    message: str
    site: str
    table_name: str | None = None
    duration_seconds: float


def _find_drive_for_mount(drives: list[Drive], mount_path: str) -> Drive | None:
    for drive in drives:
        for directory in drive.directories:
            try:
                if _get_mount_point(str(directory)) == mount_path:
                    return drive
            except Exception:
                continue
    return None


def _persist_storage_config(
    config: UltimaScraperCollectionConfig,
    *,
    setup_type: str,
    expected_version: int | None,
) -> PersistedConfigSnapshot:
    """Persist one storage edit to the canonical row in PostgreSQL."""

    return persist_config_snapshot(config, expected_version=expected_version)


def _coerce_remote_drives(metadata: object) -> list[RemoteBackendDrive]:
    """Convert backend registry metadata into drive view models."""
    if not isinstance(metadata, dict):
        return []

    metadata_map = cast(dict[str, object], metadata)
    raw_drives = metadata_map.get("drives", [])
    if not isinstance(raw_drives, list):
        return []

    drives: list[RemoteBackendDrive] = []
    for raw_drive_obj in cast(list[object], raw_drives):
        if not isinstance(raw_drive_obj, dict):
            continue
        drive_map = cast(dict[str, object], raw_drive_obj)
        drives.append(
            RemoteBackendDrive(
                path=str(drive_map.get("path", "")),
                device=str(drive_map.get("device", "")),
                total_bytes=_to_int(drive_map.get("total_bytes", 0)),
                used_bytes=_to_int(drive_map.get("used_bytes", 0)),
                free_bytes=_to_int(drive_map.get("free_bytes", 0)),
                percent_used=_to_float(drive_map.get("percent_used", 0.0)),
                configured_directories=[
                    str(item)
                    for item in cast(
                        list[object], drive_map.get("configured_directories", [])
                    )
                    if str(item).strip()
                ],
            )
        )
    return drives


def _coerce_remote_workers(metadata: object) -> list[RemoteBackendWorker]:
    """Convert backend registry metadata into worker view models."""
    if not isinstance(metadata, dict):
        return []

    metadata_map = cast(dict[str, object], metadata)
    raw_workers = metadata_map.get("workers", [])
    if not isinstance(raw_workers, list):
        return []

    workers: list[RemoteBackendWorker] = []
    for raw_worker_obj in cast(list[object], raw_workers):
        if not isinstance(raw_worker_obj, dict):
            continue
        worker_map = cast(dict[str, object], raw_worker_obj)
        current_job = worker_map.get("current_job")
        workers.append(
            RemoteBackendWorker(
                worker_id=str(worker_map.get("worker_id", "")),
                status=str(worker_map.get("status", "idle")),
                current_job=str(current_job) if current_job is not None else None,
                jobs_completed=_to_int(worker_map.get("jobs_completed", 0)),
            )
        )
    return workers


def _to_int(value: object) -> int:
    """Best-effort integer coercion for registry metadata."""
    try:
        return int(cast(Any, value) or 0)
    except (TypeError, ValueError):
        return 0


def _to_float(value: object) -> float:
    """Best-effort float coercion for registry metadata."""
    try:
        return float(cast(Any, value) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _to_bool(value: object) -> bool:
    """Best-effort bool coercion for bootstrap payloads."""
    return bool(value)


def _get_runtime_version() -> str:
    """The release this backend runs (see cluster.runtime_version), with a safe fallback."""
    from ...cluster import runtime_version

    try:
        return runtime_version() or "0.1.0"
    except Exception:
        logger.debug("Failed to resolve the runtime version", exc_info=True)
        return "0.1.0"


def _get_mount_point(path: str) -> str:
    """Get the mount point for a given path."""
    path = os.path.realpath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path


def _is_data_drive(mountpoint: str, device: str, fstype: str) -> bool:
    """Check if a drive is suitable for data storage (not a system/boot partition)."""
    # Exclude boot and EFI partitions
    excluded_mounts = {
        "/boot",
        "/boot/efi",
        "/efi",
        "/var/lib/snapd/snap",
        "/snap",
        "/recovery",
        "/var/log",
        "/var/cache",
    }

    # Check exact matches
    if mountpoint in excluded_mounts:
        return False

    # Exclude mounts that start with certain paths
    excluded_prefixes = (
        "/boot/",
        "/snap/",
        "/var/snap/",
        "/sys/",
        "/proc/",
        "/dev/",
        "/run/",
    )
    if mountpoint.startswith(excluded_prefixes):
        return False

    # Exclude certain filesystem types
    excluded_fstypes = {
        "squashfs",
        "tmpfs",
        "devtmpfs",
        "overlay",
        "ramfs",
        "sysfs",
        "proc",
        "devpts",
        "cgroup",
        "cgroup2",
        "securityfs",
        "debugfs",
        "tracefs",
        "fusectl",
        "configfs",
        "bpf",
        "mqueue",
        "hugetlbfs",
        "pstore",
        "efivarfs",
        "autofs",
    }
    if fstype.lower() in excluded_fstypes:
        return False

    # Exclude very small partitions (< 1GB, likely boot/EFI)
    # This is checked later with actual size data

    return True


def _get_system_drives() -> list[dict[str, Any]]:
    """Get all system drives/partitions suitable for data storage."""
    drives: list[dict[str, Any]] = []
    seen_mounts: set[str] = set()

    try:
        import psutil

        partitions = psutil.disk_partitions(all=True)

        for partition in partitions:
            # Skip non-data drives
            if not _is_data_drive(
                partition.mountpoint, partition.device, partition.fstype
            ):
                continue

            # Skip if already seen this mount point
            if partition.mountpoint in seen_mounts:
                continue
            seen_mounts.add(partition.mountpoint)

            try:
                usage = psutil.disk_usage(partition.mountpoint)

                # Skip very small partitions (< 1GB, likely boot/system)
                if usage.total < 1 * 1024 * 1024 * 1024:  # 1GB
                    continue

                drives.append(
                    {
                        "path": partition.mountpoint,
                        "device": partition.device,
                        "total": usage.total,
                        "used": usage.used,
                        "free": usage.free,
                        "percent": usage.percent,
                    }
                )
            except (PermissionError, OSError):
                continue

    except ImportError:
        # Fallback without psutil
        for mount in ["/home", "/"]:
            try:
                usage = shutil.disk_usage(mount)
                drives.append(
                    {
                        "path": mount,
                        "device": "unknown",
                        "total": usage.total,
                        "used": usage.used,
                        "free": usage.free,
                        "percent": (
                            (usage.used / usage.total) * 100 if usage.total > 0 else 0
                        ),
                    }
                )
            except Exception:
                continue

    return drives


class FilesystemEntry(BaseModel):
    name: str
    path: str
    is_dir: bool


class FilesystemListing(BaseModel):
    path: str
    parent: str | None
    writable: bool
    entries: list[FilesystemEntry]
    truncated: bool = False


_FILESYSTEM_LISTING_LIMIT = 2000


@router.get("/filesystem", response_model=FilesystemListing)
def browse_filesystem(path: str = "/", include_files: bool = False) -> FilesystemListing:
    """List one server directory for the WebUI's directory and file pickers.

    Paths are the backend's own (inside the container on Docker). Folders
    come first; files are listed only for file pickers.
    """
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise HTTPException(status_code=400, detail="Use an absolute path.")
    target = target.resolve()
    if not target.is_dir():
        raise HTTPException(status_code=404, detail=f"{target} is not a directory.")
    entries: list[FilesystemEntry] = []
    try:
        with os.scandir(target) as scanner:
            for entry in scanner:
                try:
                    is_dir = entry.is_dir()
                except OSError:
                    continue
                if is_dir or include_files:
                    entries.append(
                        FilesystemEntry(name=entry.name, path=str(target / entry.name), is_dir=is_dir)
                    )
    except PermissionError:
        raise HTTPException(status_code=403, detail=f"No permission to read {target}.") from None
    entries.sort(key=lambda entry: (not entry.is_dir, entry.name.lower()))
    return FilesystemListing(
        path=str(target),
        parent=str(target.parent) if target.parent != target else None,
        writable=os.access(target, os.W_OK),
        entries=entries[:_FILESYSTEM_LISTING_LIMIT],
        truncated=len(entries) > _FILESYSTEM_LISTING_LIMIT,
    )


@router.get("/drives", response_model=DrivesResponse)
def get_drives(
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> DrivesResponse:
    """Get all system drives with usage information."""
    system_drives: list[dict[str, Any]] = _get_system_drives()

    # Get configured directories from config (map path -> mount point)
    configured_paths: dict[str, str] = {}  # path -> mount point
    for drive in config.settings.download_setup.drives:
        for directory in drive.directories:
            path_str = str(directory)
            try:
                mount = _get_mount_point(path_str)
                configured_paths[path_str] = mount
            except Exception:
                pass
    for drive in config.settings.metadata_setup.drives:
        for directory in drive.directories:
            path_str = str(directory)
            try:
                mount = _get_mount_point(path_str)
                configured_paths[path_str] = mount
            except Exception:
                pass

    drives: list[DriveInfo] = []
    for drive_data in system_drives:
        # Find configured directories on this drive
        drive_mount = drive_data["path"]
        directories_on_drive = [
            path for path, mount in configured_paths.items() if mount == drive_mount
        ]

        drives.append(
            DriveInfo(
                path=drive_data["path"],
                device=drive_data["device"],
                total_bytes=drive_data["total"],
                used_bytes=drive_data["used"],
                free_bytes=drive_data["free"],
                percent_used=drive_data["percent"],
                is_configured=len(directories_on_drive) > 0,
                directory_count=len(directories_on_drive),
                configured_directories=directories_on_drive,
            )
        )

    return DrivesResponse(backend_id=backend.instance_id, drives=drives)


@router.post("/drives/add-directory", response_model=AddDirectoryResponse)
async def add_directory(
    request: AddDirectoryRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> AddDirectoryResponse:
    """Add a directory to the download or metadata setup."""
    if request.target_backend_id and request.target_backend_id != backend.instance_id:
        return AddDirectoryResponse(
            success=False,
            message=(
                f"Drive belongs to backend '{request.target_backend_id}', but this "
                f"endpoint controls '{backend.instance_id}'."
            ),
        )
    directory = Path(request.directory_path)

    # Validate the directory path
    if not directory.is_absolute():
        return AddDirectoryResponse(
            success=False,
            message="Directory path must be absolute",
        )

    # Create directory if it doesn't exist
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        return AddDirectoryResponse(
            success=False,
            message=f"Failed to create directory: {e}",
        )

    # Verify it's on the correct drive
    try:
        actual_mount = _get_mount_point(str(directory))
        if actual_mount != request.drive_path:
            return AddDirectoryResponse(
                success=False,
                message=f"Directory is on {actual_mount}, not {request.drive_path}",
            )
    except Exception as e:
        return AddDirectoryResponse(
            success=False,
            message=f"Failed to verify mount point: {e}",
        )

    working_config = config.model_copy(deep=True)

    # Add to appropriate setup
    if request.setup_type == "download":
        setup = working_config.settings.download_setup
    elif request.setup_type == "metadata":
        setup = working_config.settings.metadata_setup
    else:
        return AddDirectoryResponse(
            success=False,
            message=f"Invalid setup_type: {request.setup_type}. Must be 'download' or 'metadata'",
        )

    # Check if directory already exists in any drive config
    for drive in setup.drives:
        if directory in drive.directories:
            return AddDirectoryResponse(
                success=False,
                message="Directory is already configured",
            )

    target_drive = _find_drive_for_mount(setup.drives, request.drive_path)
    if target_drive is not None:
        target_drive.directories.append(directory)
    else:
        setup.drives.append(Drive(directories=[directory], scan_existing=False))

    # Save config
    try:
        snapshot = _persist_storage_config(
            working_config,
            setup_type=request.setup_type,
            expected_version=request.expected_version,
        )
    except ConfigVersionRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConfigVersionConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "current_version": exc.current_version,
            },
        ) from exc
    except Exception as e:
        return AddDirectoryResponse(
            success=False,
            message=f"Failed to save config: {e}",
        )

    changed_section = f"{request.setup_type}_setup"
    propagation = await propagate_storage_config(
        backend,
        working_config,
        changed_sections={changed_section},
        config_version=snapshot.version,
    )
    return AddDirectoryResponse(
        success=True,
        message=f"Added {directory} to {request.setup_type} setup",
        directory_path=str(directory),
        propagation=propagation,
    )


@router.post("/drives/remove-directory", response_model=RemoveDirectoryResponse)
async def remove_directory(
    request: RemoveDirectoryRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> RemoveDirectoryResponse:
    """Remove a directory from config (does NOT delete the actual directory or files)."""
    if request.target_backend_id and request.target_backend_id != backend.instance_id:
        return RemoveDirectoryResponse(
            success=False,
            message=(
                f"Drive belongs to backend '{request.target_backend_id}', but this "
                f"endpoint controls '{backend.instance_id}'."
            ),
        )
    directory = Path(request.directory_path)

    working_config = config.model_copy(deep=True)

    # Get appropriate setup
    if request.setup_type == "download":
        setup = working_config.settings.download_setup
    elif request.setup_type == "metadata":
        setup = working_config.settings.metadata_setup
    else:
        return RemoveDirectoryResponse(
            success=False,
            message=f"Invalid setup_type: {request.setup_type}. Must be 'download' or 'metadata'",
        )

    # Find and remove the directory from any drive config
    found = False
    empty_drive: Drive | None = None
    for drive in setup.drives:
        if directory in drive.directories:
            drive.directories.remove(directory)
            if not drive.directories:
                empty_drive = drive
            found = True
            break

    if not found:
        return RemoveDirectoryResponse(
            success=False,
            message=f"Directory not found in {request.setup_type} config",
        )

    if empty_drive is not None:
        setup.drives.remove(empty_drive)

    # Save config
    try:
        snapshot = _persist_storage_config(
            working_config,
            setup_type=request.setup_type,
            expected_version=request.expected_version,
        )
    except ConfigVersionRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConfigVersionConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "current_version": exc.current_version,
            },
        ) from exc
    except Exception as e:
        return RemoveDirectoryResponse(
            success=False,
            message=f"Failed to save config: {e}",
        )

    changed_section = f"{request.setup_type}_setup"
    propagation = await propagate_storage_config(
        backend,
        working_config,
        changed_sections={changed_section},
        config_version=snapshot.version,
    )
    return RemoveDirectoryResponse(
        success=True,
        message=f"Removed {directory} from {request.setup_type} setup (files not deleted)",
        propagation=propagation,
    )


@router.get("/drives/directory-config", response_model=DirectoryConfigResponse)
async def get_directory_config(
    directory_path: str,
    setup_type: str = "download",
    target_backend_id: str | None = None,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> DirectoryConfigResponse:
    """Get configuration for a specific directory."""
    if target_backend_id and target_backend_id != backend.instance_id:
        return DirectoryConfigResponse(
            success=False,
            message=(
                f"Drive belongs to backend '{target_backend_id}', but this endpoint "
                f"controls '{backend.instance_id}'."
            ),
        )
    directory = Path(directory_path)

    # Get appropriate setup
    if setup_type == "download":
        setup = config.settings.download_setup
    elif setup_type == "metadata":
        setup = config.settings.metadata_setup
    else:
        return DirectoryConfigResponse(
            success=False,
            message=f"Invalid setup_type: {setup_type}",
        )

    # Find the drive config containing this directory
    for drive in setup.drives:
        if directory in drive.directories:
            return DirectoryConfigResponse(
                success=True,
                message="Directory config found",
                config=DirectoryConfigInfo(
                    path=str(directory),
                    setup_type=setup_type,
                    minimum_space=drive.minimum_space,
                    store=drive.store,
                    scan_existing=drive.scan_existing,
                    overflow=drive.overflow,
                ),
            )

    return DirectoryConfigResponse(
        success=False,
        message="Directory not found in config",
    )


@router.post("/drives/directory-config", response_model=DirectoryConfigResponse)
async def update_directory_config(
    request: DirectoryConfigRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> DirectoryConfigResponse:
    """Update configuration for a specific directory."""
    if request.target_backend_id and request.target_backend_id != backend.instance_id:
        return DirectoryConfigResponse(
            success=False,
            message=(
                f"Drive belongs to backend '{request.target_backend_id}', but this "
                f"endpoint controls '{backend.instance_id}'."
            ),
        )
    directory = Path(request.directory_path)

    working_config = config.model_copy(deep=True)

    # Get appropriate setup
    if request.setup_type == "download":
        setup = working_config.settings.download_setup
    elif request.setup_type == "metadata":
        setup = working_config.settings.metadata_setup
    else:
        return DirectoryConfigResponse(
            success=False,
            message=f"Invalid setup_type: {request.setup_type}",
        )

    # Find the drive config containing this directory
    found_drive = None
    for drive in setup.drives:
        if directory in drive.directories:
            found_drive = drive
            break

    if not found_drive:
        return DirectoryConfigResponse(
            success=False,
            message="Directory not found in config",
        )

    # Update fields if provided
    if request.minimum_space is not None:
        found_drive.minimum_space = request.minimum_space
    if request.store is not None:
        found_drive.store = request.store
    if request.scan_existing is not None:
        found_drive.scan_existing = request.scan_existing
    if request.overflow is not None:
        found_drive.overflow = request.overflow

    # Save config
    try:
        snapshot = _persist_storage_config(
            working_config,
            setup_type=request.setup_type,
            expected_version=request.expected_version,
        )
    except ConfigVersionRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConfigVersionConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "current_version": exc.current_version,
            },
        ) from exc
    except Exception as e:
        return DirectoryConfigResponse(
            success=False,
            message=f"Failed to save config: {e}",
        )

    changed_section = f"{request.setup_type}_setup"
    propagation = await propagate_storage_config(
        backend,
        working_config,
        changed_sections={changed_section},
        config_version=snapshot.version,
    )
    return DirectoryConfigResponse(
        success=True,
        message="Directory config updated",
        config=DirectoryConfigInfo(
            path=str(directory),
            setup_type=request.setup_type,
            minimum_space=found_drive.minimum_space,
            store=found_drive.store,
            scan_existing=found_drive.scan_existing,
            overflow=found_drive.overflow,
        ),
        propagation=propagation,
    )


@router.get("/backend", response_model=BackendStatusResponse)
async def get_backend_status(
    backend: BackendInstance = Depends(get_backend_instance),
) -> BackendStatusResponse:
    """Get backend system status including workers."""
    from ...job_system.storage import JobStorage
    from ...redis.worker_registry import WorkerRegistry
    from ...redis.worker_registry import WorkerStatus as WStatus

    workers: list[WorkerInfo] = []
    active_count = 0
    system_stats: BackendSystemStats | None = None

    # Get worker information from Redis
    if backend.redis:
        try:
            # First, clean up any dead workers (whose keys have expired)
            await WorkerRegistry.cleanup_dead_workers(
                cast(Any, backend.redis), backend.instance_id
            )

            # Get all workers for this backend from Redis
            workers_info = await WorkerRegistry.get_backend_workers_info(
                cast(Any, backend.redis), backend.instance_id
            )

            for worker_id, worker_info in workers_info.items():
                status = (
                    worker_info.status.value
                    if isinstance(worker_info.status, WStatus)
                    else str(worker_info.status)
                )
                current_job = None

                # Check if worker is busy with a task
                if worker_info.current_task:
                    task_metadata = worker_info.current_task.metadata or {}
                    metadata_job_id = task_metadata.get("job_id")

                    if isinstance(metadata_job_id, str) and metadata_job_id:
                        current_job = metadata_job_id
                    else:
                        current_job = worker_info.current_task.task_id

                    candidate_job_id = (
                        current_job if current_job.startswith("job-") else None
                    )

                    # Defensive stale-state filter: if the referenced job is
                    # terminal, don't report the worker as actively busy.
                    is_terminal_job = False
                    if candidate_job_id:
                        state_key = JobStorage.JOB_STATE_KEY.format(
                            job_id=candidate_job_id
                        )
                        raw_status = cast(
                            bytes | str | None,
                            await cast(Any, backend.redis.client).hget(
                                state_key, "status"
                            ),
                        )
                        if raw_status is not None:
                            decoded = (
                                raw_status.decode()
                                if isinstance(raw_status, bytes)
                                else str(raw_status)
                            ).lower()
                            is_terminal_job = decoded in {
                                "cancelled",
                                "completed",
                                "failed",
                            }

                    if is_terminal_job:
                        status = "idle"
                        current_job = None
                    else:
                        status = "busy"
                        active_count += 1

                workers.append(
                    WorkerInfo(
                        worker_id=worker_id,
                        status=status,
                        current_job=current_job,
                        jobs_completed=worker_info.tasks_completed,
                    )
                )
        except Exception as e:
            logger.error(f"Failed to get workers from Redis: {e}")

        try:
            from ...ultima_control.system_stats_reporter import SystemStatsReporter

            reported_stats = await SystemStatsReporter.fetch_stats(
                cast(Any, backend.redis), backend.instance_id
            )
            if reported_stats is not None:
                system_stats = BackendSystemStats.model_validate(
                    reported_stats.to_dict()
                )
        except Exception as exc:
            logger.debug("Failed to get system telemetry: %s", exc)

    # Get pending job count
    pending_jobs = await backend.job_queue.size()

    max_workers: int | None = None
    if getattr(backend, "_registry", None) is not None:
        max_workers = backend._registry.max_workers
    else:
        try:
            from ...config import UltimaScraperCollectionConfig

            cfg = await run_database_call(
                UltimaScraperCollectionConfig.load_or_create_default_config
            )
            max_workers = cfg.settings.worker_pool.size
        except Exception:
            max_workers = None

    return BackendStatusResponse(
        backend_id=backend.instance_id,
        distributed_mode=backend.distributed_mode,
        redis_connected=backend.redis is not None,
        worker_count=len(workers),
        active_workers=active_count,
        pending_jobs=pending_jobs,
        workers=workers,
        system_stats=system_stats,
        max_workers=max_workers,
    )


@router.get("/memory", response_model=MemoryStatusResponse)
async def get_memory_status(
    backend: BackendInstance = Depends(get_backend_instance),
) -> MemoryStatusResponse:
    """Expose process, bounded-cache, worker, and Redis memory gauges."""
    import psutil

    from ...redis.worker_registry import WorkerRegistry
    from .media import get_media_proxy_cache_stats

    process = psutil.Process(os.getpid())
    process_memory = process.memory_info()
    workers: list[WorkerMemoryInfo] = []
    redis_memory: RedisMemoryInfo | None = None

    if backend.redis is not None:
        try:
            workers_info = await WorkerRegistry.get_backend_workers_info(
                cast(Any, backend.redis), backend.instance_id
            )
            for worker_id, worker_info in workers_info.items():
                metadata = worker_info.metadata
                workers.append(
                    WorkerMemoryInfo(
                        worker_id=worker_id,
                        **{
                            field: _to_int(metadata.get(field, 0))
                            for field in (
                                "rss_bytes",
                                "vms_bytes",
                                "auth_count",
                                "guest_auth_count",
                                "cached_user_count",
                                "cached_content_count",
                                "websocket_connections",
                                "websocket_message_history",
                                "websocket_event_history",
                                "log_queue_entries",
                                "log_queue_bytes",
                                "log_dropped",
                                "log_publish_failures",
                            )
                        },
                        memory_percent=_to_float(metadata.get("memory_percent", 0.0)),
                    )
                )
        except Exception as exc:
            logger.debug("Failed to read worker memory telemetry: %s", exc)

        try:
            client = cast(Any, backend.redis.client)
            memory_info = await client.info("memory")
            redis_memory = RedisMemoryInfo(
                used_bytes=_to_int(memory_info.get("used_memory", 0)),
                peak_bytes=_to_int(memory_info.get("used_memory_peak", 0)),
                max_bytes=_to_int(memory_info.get("maxmemory", 0)),
                maxmemory_policy=str(memory_info.get("maxmemory_policy", "unknown")),
                key_count=_to_int(await client.dbsize()),
            )
        except Exception as exc:
            logger.debug("Failed to read Redis memory telemetry: %s", exc)

    return MemoryStatusResponse(
        backend_id=backend.instance_id,
        process=ProcessMemoryInfo(
            pid=process.pid,
            rss_bytes=int(process_memory.rss),
            vms_bytes=int(process_memory.vms),
            memory_percent=float(process.memory_percent()),
        ),
        media_proxy_cache=get_media_proxy_cache_stats(),
        redis=redis_memory,
        workers=workers,
    )


def _merge_presence(backends: list[RemoteBackendInfo], presence: list[dict[str, Any]]) -> None:
    """Add running backends that announce themselves but are not job-routing members.

    Only worker processes join the routing registry; every backend's stats
    reporter publishes presence, with the cluster it belongs to.
    """
    listed = {entry.backend_id: entry for entry in backends}
    for entry in presence:
        backend_id = str(entry["backend_id"])
        cluster_id = entry.get("cluster_id") or None
        api_url = entry.get("api_url") or None
        tls_fingerprint = entry.get("tls_fingerprint") or None
        address = entry.get("address") or None
        if backend_id in listed:
            current = listed[backend_id]
            current.cluster_id = current.cluster_id or cluster_id
            current.api_url = current.api_url or api_url
            current.tls_fingerprint = current.tls_fingerprint or tls_fingerprint
            current.address = current.address or address
            # Presence carries the release it runs; the routing registry may not.
            current.version = str(entry.get("version") or "") or current.version
            continue
        listed[backend_id] = RemoteBackendInfo(
            backend_id=backend_id,
            hostname=str(entry.get("hostname") or ""),
            status="online",
            last_seen=str(entry.get("last_seen") or ""),
            max_workers=0,
            version=str(entry.get("version") or ""),
            cluster_id=cluster_id,
            api_url=api_url,
            tls_fingerprint=tls_fingerprint,
            address=address,
        )
        backends.append(listed[backend_id])


@router.get("/identity", response_model=BackendIdentityResponse)
async def get_backend_identity() -> BackendIdentityResponse:
    """This backend's id, cluster and address.

    A WebUI groups its connections by cluster_id: backends that share a
    database are one cluster, the rest are independent.
    """
    from ...backend_tls import tls_fingerprint
    from ...cluster import backend_id, get_cluster_id, public_api_url

    return BackendIdentityResponse(
        backend_id=backend_id(),
        instance=get_backend_startup_id(),
        cluster_id=await run_database_call(get_cluster_id),
        api_url=public_api_url(),
        tls_fingerprint=tls_fingerprint(),
        version=_get_runtime_version(),
        api_version=API_VERSION,
        capabilities=["clusters"],
    )


class ClusterStoresResponse(BaseModel):
    """Connection details for the database and redis backing this cluster."""

    available: bool
    cluster_id: str | None = None
    database: DatabaseConnectionRequest | None = None
    redis: RedisConnectionRequest | None = None
    secret_key: str | None = None
    settings_id: int | None = None
    backend_id: str | None = None
    api_url: str | None = None


@router.get("/cluster-stores", response_model=ClusterStoresResponse)
async def get_cluster_stores() -> ClusterStoresResponse:
    """Connection details for the database and redis backing this cluster.

    Used by the WebUI installer to let new remote workers or backends join
    this backend's existing database and redis stores.
    """
    from sqlmodel import select
    from ultima_scraper_core import runtime_env_value
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        SettingsModel,
        get_global_session,
    )

    from ...cluster import get_cluster_id
    from ...models.backend import BackendInstance

    db = _configured_database_request()
    redis = _configured_redis_request()
    cluster_id = None
    try:
        cluster_id = await run_database_call(get_cluster_id)
    except Exception:
        pass

    backend_id = runtime_env_value("BACKEND_ID") or socket.gethostname()
    secret_key = runtime_env_value("USR_SECRET_KEY") or os.environ.get("USR_SECRET_KEY")
    settings_id: int | None = None
    try:
        with get_global_session() as session:
            inv = session.exec(
                select(BackendInventoryModel).where(
                    BackendInventoryModel.hostname == backend_id
                )
            ).first()
            if inv:
                settings_id = inv.settings_id
            else:
                st = session.exec(
                    select(SettingsModel).where(
                        SettingsModel.key == "config",
                        SettingsModel.backend == backend_id,
                    )
                ).first()
                if not st:
                    st = session.exec(
                        select(SettingsModel).where(SettingsModel.key == "config")
                    ).first()
                settings_id = st.id if st else None
    except Exception:
        pass

    local_ip = BackendInstance._resolve_local_ip()
    api_port = int(
        runtime_env_value("CONTROL_PORT")
        or runtime_env_value("USR_BACKEND_PORT")
        or 21121
    )
    api_url = f"http://{local_ip}:{api_port}" if local_ip else None

    available = db is not None or redis is not None
    return ClusterStoresResponse(
        available=available,
        cluster_id=cluster_id,
        database=db,
        redis=redis,
        secret_key=secret_key,
        settings_id=settings_id,
        backend_id=backend_id,
        api_url=api_url,
    )


@router.get("/backends", response_model=AllBackendsResponse)
async def get_all_backends(
    backend: BackendInstance = Depends(get_backend_instance),
) -> AllBackendsResponse:
    """Get status of all backends from Redis (distributed mode).

    This endpoint retrieves information about all registered backends
    from Redis, including their drives, workers, and status.
    """
    from ...ultima_control.backend_registry import BackendRegistry

    backends_list: list[RemoteBackendInfo] = []

    if backend.redis is None:
        # Local mode - just return info about this backend
        return AllBackendsResponse(
            success=True,
            local_backend_id=backend.instance_id,
            local_hostname=socket.gethostname(),
            backends=[],
            total_backends=0,
        )

    from ...cluster import read_presence

    try:
        # First, clean up any dead backends whose keys have expired
        await BackendRegistry.cleanup_dead_backends(cast(Any, backend.redis))

        # Get all backends from Redis
        all_backends = await BackendRegistry.get_all_backends(cast(Any, backend.redis))

        for _backend_id, info in all_backends.items():
            metadata = info.metadata
            drives = _coerce_remote_drives(metadata)
            workers = _coerce_remote_workers(metadata)

            backends_list.append(
                RemoteBackendInfo(
                    backend_id=info.backend_id,
                    hostname=info.hostname,
                    status=info.status,
                    last_seen=info.last_heartbeat,
                    max_workers=info.max_workers,
                    version=info.version,
                    drives=drives,
                    workers=workers,
                    active_workers=_to_int(
                        metadata.get("active_workers", info.worker_count)
                    ),
                    pending_jobs=_to_int(metadata.get("pending_jobs", 0)),
                    cluster_id=info.cluster_id,
                    api_url=info.api_url,
                )
            )

        _merge_presence(backends_list, await read_presence(backend.redis))

        # Resolve friendly backend names from inventory (e.g. "atlas")
        inv_map: dict[str, str] = {}
        try:
            from sqlmodel import select
            from ultima_scraper_db_archive.global_db import (
                BackendInventoryModel,
                get_global_session,
            )

            with get_global_session() as session:
                for inv in session.exec(select(BackendInventoryModel)).all():
                    if inv.backend_name:
                        if inv.hostname:
                            inv_map[inv.hostname] = inv.backend_name
                        inv_map[inv.backend_name] = inv.backend_name
        except Exception:
            pass

        from ...redis.worker_registry import WorkerRegistry
        from ...redis.worker_registry import WorkerStatus as WStatus
        from ...ultima_control.system_stats_reporter import SystemStatsReporter

        excluded_disk_prefixes = (
            "/boot",
            "/snap",
            "/proc",
            "/sys",
            "/dev",
            "/run",
            "/tmp",
            "/var/lib/docker",
            "/var/lib/snapd",
            "/etc",
            "/usr",
        )

        for b_info in backends_list:
            if b_info.backend_id in inv_map:
                b_info.hostname = inv_map[b_info.backend_id]
            elif b_info.hostname in inv_map:
                b_info.hostname = inv_map[b_info.hostname]

            if not b_info.workers and backend.redis is not None:
                try:
                    workers_info = await WorkerRegistry.get_backend_workers_info(
                        cast(Any, backend.redis), b_info.backend_id
                    )
                    if workers_info:
                        workers = []
                        active_count = 0
                        for wid, winfo in workers_info.items():
                            status = (
                                winfo.status.value
                                if isinstance(winfo.status, WStatus)
                                else str(winfo.status)
                            )
                            c_job = None
                            if winfo.current_task:
                                task_meta = winfo.current_task.metadata or {}
                                c_job = task_meta.get("job_id") or winfo.current_task.task_id
                            if status in ("busy", "running"):
                                active_count += 1
                            workers.append(
                                RemoteBackendWorker(
                                    worker_id=wid,
                                    status=status,
                                    current_job=str(c_job) if c_job is not None else None,
                                    jobs_completed=winfo.tasks_completed,
                                )
                            )
                        b_info.workers = workers
                        b_info.max_workers = len(workers)
                        b_info.active_workers = active_count
                except Exception as exc:
                    logger.debug("Could not read workers for backend %s: %s", b_info.backend_id, exc)

            if not b_info.drives and backend.redis is not None:
                try:
                    reported_stats = await SystemStatsReporter.fetch_stats(
                        cast(Any, backend.redis), b_info.backend_id
                    )
                    if reported_stats and reported_stats.disks:
                        seen_mounts: set[str] = set()
                        drives = []
                        for d in reported_stats.disks:
                            if any(d.mount_point.startswith(p) for p in excluded_disk_prefixes):
                                continue
                            if d.mount_point in seen_mounts:
                                continue
                            seen_mounts.add(d.mount_point)
                            drives.append(
                                RemoteBackendDrive(
                                    path=d.mount_point,
                                    device=d.device,
                                    total_bytes=d.total_bytes,
                                    used_bytes=d.used_bytes,
                                    free_bytes=d.free_bytes,
                                    percent_used=d.percent_used,
                                    configured_directories=[],
                                )
                            )
                        if not drives and reported_stats.disks:
                            for d in reported_stats.disks:
                                if not d.mount_point.startswith(("/proc", "/sys", "/dev")):
                                    drives.append(
                                        RemoteBackendDrive(
                                            path=d.mount_point,
                                            device=d.device,
                                            total_bytes=d.total_bytes,
                                            used_bytes=d.used_bytes,
                                            free_bytes=d.free_bytes,
                                            percent_used=d.percent_used,
                                            configured_directories=[],
                                        )
                                    )
                                    break
                        b_info.drives = drives
                except Exception as exc:
                    logger.debug("Could not read drives for backend %s: %s", b_info.backend_id, exc)

        return AllBackendsResponse(
            success=True,
            local_backend_id=backend.instance_id,
            local_hostname=socket.gethostname(),
            backends=backends_list,
            total_backends=len(backends_list),
        )

    except Exception as e:
        logger.exception("Failed to get all backends: %s", e)
        return AllBackendsResponse(
            success=False,
            local_backend_id=backend.instance_id,
            local_hostname=socket.gethostname(),
            backends=[],
            total_backends=0,
        )


@router.get("/bootstrap", response_model=SystemBootstrapResponse)
def get_system_bootstrap(
    backend: BackendInstance = Depends(get_backend_instance),
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> SystemBootstrapResponse:
    """Return a small bootstrap payload for the WebUI shell."""
    settings_service = SettingsService()
    setup_state = settings_service.get_setup_status()

    configured_sites = list(config.settings.worker_pool.sites)
    supported_sites = [site.lower() for site in configured_sites if str(site).strip()]

    setup_state["startup_id"] = get_backend_startup_id()
    return SystemBootstrapResponse(
        backend_id=backend.instance_id,
        hostname=socket.gethostname(),
        version=_get_runtime_version(),
        supported_sites=supported_sites,
        setup=SetupSummary(
            is_configured=_to_bool(setup_state.get("is_configured", False)),
            has_config=_to_bool(setup_state.get("has_config", False)),
            has_database=_to_bool(setup_state.get("has_database", False)),
            data_directory=str(setup_state.get("data_directory", "")),
            startup_id=str(setup_state.get("startup_id", "")),
        ),
        features=BootstrapFeatures(
            distributed_mode=backend.distributed_mode,
            remote_backends=backend.redis is not None,
        ),
        support_matrix=SupportMatrix(),
    )


# Set by main.py for its children; see RESTART_SIGNAL there.
SUPERVISOR_PID_ENV = "ULTIMA_SUPERVISOR_PID"
# Let the response reach the browser before the launcher stops the API.
POWER_SIGNAL_DELAY_SECONDS = 0.5

RuntimePowerAction = Literal["restart", "shutdown"]
RuntimeTarget = Literal["api", "database_writer", "worker_pool", "plugins", "webui"]


class RuntimePowerRequest(BaseModel):
    action: RuntimePowerAction
    targets: list[str] | None = None


class RuntimePowerResponse(BaseModel):
    action: RuntimePowerAction
    status: Literal["accepted"] = "accepted"
    supervisor_pid: int
    targets: list[str] | None = None


def _write_runtime_power_command(pid: int, action: str, targets: list[str] | None) -> None:
    if not targets:
        return
    import json
    import tempfile

    command_path = Path(tempfile.gettempdir()) / f"ultimascraper_power_command_{pid}.json"
    try:
        command_path.write_text(json.dumps({"action": action, "targets": targets}))
    except OSError as exc:
        logger.warning("Could not write runtime power command file %s: %s", command_path, exc)


def _runtime_supervisor_pid() -> int:
    """Return the launcher PID, only if it still owns this API process."""
    try:
        pid = int(os.environ.get(SUPERVISOR_PID_ENV, ""))
    except ValueError:
        pid = 0
    try:
        ancestors = {parent.pid for parent in psutil.Process().parents()}
    except psutil.Error:
        ancestors = set()
    if pid <= 1 or pid not in ancestors:
        raise HTTPException(
            status_code=409,
            detail="This backend was not started by the UltimaScraper launcher; "
            "restart or stop it with the service manager that started it.",
        )
    return pid


@router.post("/runtime/power", response_model=RuntimePowerResponse)
async def control_runtime_power(request: RuntimePowerRequest) -> RuntimePowerResponse:
    """Restart or shut down this instance's launcher and every service it owns."""
    pid = _runtime_supervisor_pid()
    signum = (
        signal.SIGTERM
        if request.action == "shutdown"
        else getattr(signal, "SIGUSR1", None)
    )
    if signum is None:
        raise HTTPException(status_code=501, detail="Restart is not supported on this platform")
    logger.warning(
        "Runtime %s requested through the API (launcher PID %s, targets=%s)",
        request.action,
        pid,
        request.targets,
    )
    if request.action == "restart" and request.targets:
        _write_runtime_power_command(pid, request.action, request.targets)
    asyncio.get_running_loop().call_later(POWER_SIGNAL_DELAY_SECONDS, os.kill, pid, signum)
    return RuntimePowerResponse(
        action=request.action, supervisor_pid=pid, targets=request.targets
    )


def _format_bytes(bytes_count: int) -> str:
    if bytes_count < 1024:
        return f"{bytes_count} B"
    val = float(bytes_count)
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        val /= 1024.0
        if val < 1024.0 or unit == "TiB":
            return f"{val:.1f} {unit}"
    return f"{val:.1f} TiB"


def _get_maintenance_engine(site: str | None = None) -> Any:
    """Get an engine with maintenance privileges (owner if configured, or site manager)."""
    try:
        from ...job_system.ledger import owner_engine

        owner = owner_engine()
        if owner is not None:
            return owner
    except Exception:
        pass

    try:
        db_manager = MultiSiteDatabaseManager()
        if site and site != "all":
            try:
                manager = db_manager.get_or_create_manager(site)
                if manager and hasattr(manager, "engine") and manager.engine:
                    return manager.engine
            except Exception:
                pass
        for s in db_manager.get_all_sites():
            mgr = db_manager.get_manager(s)
            if mgr and hasattr(mgr, "engine") and mgr.engine:
                return mgr.engine
    except Exception:
        pass
    return None


def _get_available_database_sites(
    config: UltimaScraperCollectionConfig,
    db_manager: MultiSiteDatabaseManager,
) -> list[str]:
    """Get all database schemas/sites to monitor and maintain.

    Includes the primary 'management' database schema, configured worker pool sites,
    and any active site schemas discovered from PostgreSQL.
    """
    from sqlalchemy import text

    sites: list[str] = ["management"]

    configured_sites = list(config.settings.worker_pool.sites)
    for s in configured_sites:
        clean = str(s).strip().lower()
        if clean and clean not in sites:
            sites.append(clean)

    for s in db_manager.get_all_sites():
        clean = str(s).strip().lower()
        if clean and clean not in sites:
            sites.append(clean)

    # Attempt to discover all user schemas present in the database cluster
    engine = _get_maintenance_engine()
    if not engine:
        for s in sites:
            try:
                mgr = db_manager.get_or_create_manager(s)
                if getattr(mgr, "engine", None):
                    engine = mgr.engine
                    break
            except Exception:
                continue

    if engine and getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
        try:
            with engine.connect() as conn:
                discovered = conn.execute(
                    text("""
                        SELECT DISTINCT n.nspname
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relkind IN ('r', 'p')
                          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'public')
                        ORDER BY n.nspname
                    """)
                ).scalars().all()
                for schema in discovered:
                    clean = str(schema).strip().lower()
                    if clean and clean not in sites:
                        sites.append(clean)
        except Exception as exc:
            logger.debug("Failed to discover database schemas: %s", exc)

    if not sites:
        sites = ["management", "onlyfans", "fansly"]

    return sites


def _collect_database_overview(
    backend_id: str,
    config: UltimaScraperCollectionConfig,
) -> DatabaseOverviewResponse:
    from sqlalchemy import text

    db_manager = MultiSiteDatabaseManager()
    sites = _get_available_database_sites(config, db_manager)

    server_version: str | None = None
    total_cluster_size = 0
    databases: list[DatabaseOverview] = []

    for site in sites:
        try:
            site_db = db_manager.get_or_create_manager(site)
            engine = getattr(site_db, "engine", None)
            if not engine:
                databases.append(
                    DatabaseOverview(
                        site=site,
                        database_name="unknown",
                        schema_name=site,
                        host="localhost",
                        is_connected=False,
                        status="offline",
                    )
                )
                continue

            conn_str = getattr(site_db, "connection_string", "")
            parsed = urlparse(conn_str) if conn_str else None
            host = (parsed.hostname or "localhost") if parsed else "localhost"
            port = parsed.port if parsed else None
            dbname = (parsed.path.lstrip("/") or "ultima_archive") if parsed else "ultima_archive"

            if engine.dialect.name == "postgresql":
                with engine.connect() as conn:
                    if server_version is None:
                        try:
                            row = conn.execute(
                                text("SELECT version(), pg_database_size(current_database())")
                            ).fetchone()
                            if row:
                                server_version = str(row[0])
                                total_cluster_size = int(row[1] or 0)
                        except Exception as exc:
                            logger.debug("Failed to query database cluster size: %s", exc)

                    tables_rows = conn.execute(
                        text("""
                            SELECT
                                c.relname AS table_name,
                                -- reltuples survives restarts and is refreshed by
                                -- VACUUM/ANALYZE; n_live_tup restarts from zero after
                                -- crash recovery. -1 means never analyzed.
                                CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint
                                     ELSE COALESCE(s.n_live_tup, 0) END AS live_tuples,
                                COALESCE(s.n_dead_tup, 0) AS dead_tuples,
                                pg_total_relation_size(c.oid) AS total_bytes,
                                pg_relation_size(c.oid) AS table_bytes,
                                pg_indexes_size(c.oid) AS index_bytes,
                                s.last_vacuum,
                                s.last_autovacuum,
                                s.last_analyze,
                                s.last_autoanalyze
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                            WHERE n.nspname = :schema AND c.relkind = 'r'
                            ORDER BY pg_total_relation_size(c.oid) DESC
                        """),
                        {"schema": site},
                    ).fetchall()

                    tables: list[DatabaseTableOverview] = []
                    site_total_bytes = 0
                    site_live = 0
                    site_dead = 0
                    latest_vacuum: str | None = None
                    latest_analyze: str | None = None

                    for r in tables_rows:
                        t_name = str(r[0])
                        t_live = int(r[1] or 0)
                        t_dead = int(r[2] or 0)
                        t_tot = int(r[3] or 0)
                        t_tbl = int(r[4] or 0)
                        t_idx = int(r[5] or 0)
                        t_vac = r[6] or r[7]
                        t_ana = r[8] or r[9]

                        site_total_bytes += t_tot
                        site_live += t_live
                        site_dead += t_dead

                        if t_vac:
                            iso_vac = t_vac.isoformat() if hasattr(t_vac, "isoformat") else str(t_vac)
                            if latest_vacuum is None or iso_vac > latest_vacuum:
                                latest_vacuum = iso_vac
                        else:
                            iso_vac = None

                        if t_ana:
                            iso_ana = t_ana.isoformat() if hasattr(t_ana, "isoformat") else str(t_ana)
                            if latest_analyze is None or iso_ana > latest_analyze:
                                latest_analyze = iso_ana
                        else:
                            iso_ana = None

                        tables.append(
                            DatabaseTableOverview(
                                table_name=t_name,
                                live_tuples=t_live,
                                dead_tuples=t_dead,
                                total_bytes=t_tot,
                                table_bytes=t_tbl,
                                index_bytes=t_idx,
                                last_vacuum=iso_vac,
                                last_autovacuum=r[7].isoformat() if hasattr(r[7], "isoformat") else (str(r[7]) if r[7] else None),
                                last_analyze=iso_ana,
                                last_autoanalyze=r[9].isoformat() if hasattr(r[9], "isoformat") else (str(r[9]) if r[9] else None),
                            )
                        )

                    total_tups = site_live + site_dead
                    bloat_pct = round((site_dead / total_tups * 100), 2) if total_tups > 0 else 0.0
                    status: Literal["healthy", "warning", "offline"] = (
                        "warning" if (bloat_pct > 15.0 and site_dead > 500) else "healthy"
                    )

                    databases.append(
                        DatabaseOverview(
                            site=site,
                            database_name=dbname,
                            schema_name=site,
                            host=host,
                            port=port,
                            is_connected=True,
                            status=status,
                            total_size_bytes=site_total_bytes,
                            total_size_pretty=_format_bytes(site_total_bytes),
                            table_count=len(tables),
                            total_live_tuples=site_live,
                            row_counts_estimated=True,
                            total_dead_tuples=site_dead,
                            bloat_percent=bloat_pct,
                            last_vacuum=latest_vacuum,
                            last_analyze=latest_analyze,
                            tables=tables,
                        )
                    )
            else:
                with engine.connect() as conn:
                    tables_res = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                    ).fetchall()
                    tables = []
                    total_live = 0
                    for row in tables_res:
                        tbl = str(row[0])
                        try:
                            count_res = conn.execute(text(f'SELECT COUNT(*) FROM "{tbl}"')).scalar() or 0
                        except Exception:
                            count_res = 0
                        total_live += int(count_res)
                        tables.append(
                            DatabaseTableOverview(
                                table_name=tbl,
                                live_tuples=int(count_res),
                                dead_tuples=0,
                                total_bytes=0,
                            )
                        )
                    databases.append(
                        DatabaseOverview(
                            site=site,
                            database_name=dbname,
                            schema_name="main",
                            host="localhost",
                            port=None,
                            is_connected=True,
                            status="healthy",
                            total_size_bytes=0,
                            total_size_pretty="0 B",
                            table_count=len(tables),
                            total_live_tuples=total_live,
                            total_dead_tuples=0,
                            bloat_percent=0.0,
                            tables=tables,
                        )
                    )
        except Exception as exc:
            logger.exception("Failed to query database stats for site %s: %s", site, exc)
            databases.append(
                DatabaseOverview(
                    site=site,
                    database_name="unknown",
                    schema_name=site,
                    host="unknown",
                    is_connected=False,
                    status="offline",
                )
            )

    primary_engine = _get_maintenance_engine()
    if not primary_engine:
        for s in sites:
            try:
                m = db_manager.get_or_create_manager(s)
                if getattr(m, "engine", None):
                    primary_engine = m.engine
                    break
            except Exception:
                continue

    active_operations = _collect_active_operations(primary_engine)

    return DatabaseOverviewResponse(
        success=True,
        backend_id=backend_id,
        databases=databases,
        total_databases=len(databases),
        total_size_bytes=total_cluster_size,
        server_version=server_version,
        active_operations=active_operations,
    )


_active_database_ops_lock = threading.Lock()
_active_database_ops: dict[str, dict[str, Any]] = {}
_recent_database_ops: list[dict[str, Any]] = []
_cancelled_database_ops: set[str] = set()


def _register_database_op(
    operation_type: Literal["vacuum", "analyze", "reindex"],
    site: str,
    table_name: str | None,
    tables_total: int,
) -> str:
    from datetime import datetime, timezone

    op_id = f"op_{int(time.time() * 1000)}_{operation_type}_{site}"
    now_iso = datetime.now(timezone.utc).isoformat()
    with _active_database_ops_lock:
        _active_database_ops[op_id] = {
            "id": op_id,
            "operation_type": operation_type,
            "site": site,
            "table_name": table_name,
            "current_table": table_name,
            "tables_completed": 0,
            "tables_total": tables_total,
            "phase": "starting",
            "progress_percent": 0.0,
            "heap_blks_total": None,
            "heap_blks_scanned": None,
            "heap_blks_vacuumed": None,
            "started_at": now_iso,
            "start_time": time.monotonic(),
            "duration_seconds": 0.0,
            "status": "running",
            "message": f"Starting {operation_type} on {site}...",
        }
    return op_id


def _update_database_op(
    op_id: str,
    current_table: str | None = None,
    tables_completed: int | None = None,
    phase: str | None = None,
    progress_percent: float | None = None,
    message: str | None = None,
) -> None:
    with _active_database_ops_lock:
        op = _active_database_ops.get(op_id)
        if not op:
            return
        if current_table is not None:
            op["current_table"] = current_table
        if tables_completed is not None:
            op["tables_completed"] = tables_completed
        if phase is not None:
            op["phase"] = phase
        if progress_percent is not None:
            op["progress_percent"] = progress_percent
        if message is not None:
            op["message"] = message
        op["duration_seconds"] = round(time.monotonic() - op["start_time"], 1)


def _finish_database_op(
    op_id: str,
    success: bool,
    message: str,
) -> None:
    with _active_database_ops_lock:
        _cancelled_database_ops.discard(op_id)
        op = _active_database_ops.pop(op_id, None)
        if op:
            op["status"] = "completed" if success else "failed"
            op["progress_percent"] = 100.0 if success else op.get("progress_percent", 0.0)
            op["duration_seconds"] = round(time.monotonic() - op["start_time"], 1)
            op["message"] = message
            op["finished_at"] = time.monotonic()
            _recent_database_ops.append(op)
            if len(_recent_database_ops) > 20:
                _recent_database_ops.pop(0)


def _is_database_op_cancelled(op_id: str) -> bool:
    with _active_database_ops_lock:
        return op_id in _cancelled_database_ops


def _collect_active_operations(engine: Any = None) -> list[DatabaseActiveOperation]:
    from datetime import datetime, timezone
    from sqlalchemy import text

    active_ops: list[DatabaseActiveOperation] = []

    # 1. Take all in-memory operations tracked by this backend
    with _active_database_ops_lock:
        for op in list(_active_database_ops.values()):
            dur = round(time.monotonic() - op["start_time"], 1)
            active_ops.append(
                DatabaseActiveOperation(
                    id=op["id"],
                    operation_type=op["operation_type"],
                    site=op["site"],
                    table_name=op["table_name"],
                    current_table=op.get("current_table"),
                    tables_completed=op.get("tables_completed", 0),
                    tables_total=op.get("tables_total", 0),
                    phase=op.get("phase"),
                    progress_percent=op.get("progress_percent"),
                    heap_blks_total=op.get("heap_blks_total"),
                    heap_blks_scanned=op.get("heap_blks_scanned"),
                    heap_blks_vacuumed=op.get("heap_blks_vacuumed"),
                    started_at=op["started_at"],
                    duration_seconds=dur,
                    status="running",
                    message=op.get("message"),
                )
            )

    # 2. Query PostgreSQL live progress if connected to PostgreSQL
    if engine and getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
        try:
            with engine.connect() as conn:
                vac_rows = conn.execute(
                    text("""
                        SELECT
                            p.pid,
                            n.nspname AS schema_name,
                            c.relname AS table_name,
                            p.phase,
                            p.heap_blks_total,
                            p.heap_blks_scanned,
                            p.heap_blks_vacuumed
                        FROM pg_stat_progress_vacuum p
                        LEFT JOIN pg_class c ON c.oid = p.relid
                        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE p.phase IS NOT NULL
                    """)
                ).fetchall()

                for vr in vac_rows:
                    pid = int(vr[0])
                    s_name = str(vr[1]) if vr[1] else "management"
                    t_name = str(vr[2]) if vr[2] else None
                    phase = str(vr[3]) if vr[3] else None
                    tot_blks = int(vr[4] or 0)
                    scan_blks = int(vr[5] or 0)
                    vac_blks = int(vr[6] or 0)
                    pct = round((scan_blks / tot_blks * 100), 1) if tot_blks > 0 else None

                    matched = False
                    for op in active_ops:
                        if op.operation_type == "vacuum" and (op.site == s_name or op.site == "all"):
                            op.phase = phase
                            op.heap_blks_total = tot_blks
                            op.heap_blks_scanned = scan_blks
                            op.heap_blks_vacuumed = vac_blks
                            if pct is not None:
                                op.progress_percent = pct
                            tbl_display = t_name or op.current_table or op.table_name or "table"
                            phase_display = f" · Phase: {phase}" if phase else ""
                            pct_display = f" ({pct}%)" if pct is not None else ""
                            op.message = (
                                f"Vacuuming {tbl_display} ({op.tables_completed + 1}/{max(op.tables_total, 1)})"
                                f" in {op.site}{phase_display}{pct_display}"
                            )
                            matched = True
                            break

                    if not matched and phase:
                        pct_display = f" ({pct}%)" if pct is not None else ""
                        msg = f"PostgreSQL vacuum on {s_name}.{t_name or 'table'} · Phase: {phase}{pct_display}"
                        active_ops.append(
                            DatabaseActiveOperation(
                                id=f"pg_vac_{pid}",
                                operation_type="vacuum",
                                site=s_name,
                                table_name=t_name,
                                current_table=t_name,
                                tables_completed=0,
                                tables_total=1,
                                phase=phase,
                                progress_percent=pct,
                                heap_blks_total=tot_blks,
                                heap_blks_scanned=scan_blks,
                                heap_blks_vacuumed=vac_blks,
                                started_at=datetime.now(timezone.utc).isoformat(),
                                duration_seconds=0.0,
                                status="running",
                                message=msg,
                        )
                    )

                # Also check pg_stat_progress_cluster for VACUUM FULL operations
                try:
                    cluster_rows = conn.execute(
                        text("""
                            SELECT
                                p.pid,
                                n.nspname AS schema_name,
                                c.relname AS table_name,
                                p.phase,
                                p.heap_blks_total,
                                p.heap_blks_scanned,
                                p.heap_tuples_written
                            FROM pg_stat_progress_cluster p
                            LEFT JOIN pg_class c ON c.oid = p.relid
                            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE p.command ~* 'VACUUM'
                        """)
                    ).fetchall()

                    for cr in cluster_rows:
                        pid = int(cr[0])
                        s_name = str(cr[1]) if cr[1] else "management"
                        t_name = str(cr[2]) if cr[2] else None
                        phase = str(cr[3]) if cr[3] else None
                        tot_blks = int(cr[4] or 0)
                        scan_blks = int(cr[5] or 0)
                        pct = round((scan_blks / tot_blks * 100), 1) if tot_blks > 0 else None

                        matched = False
                        for op in active_ops:
                            if op.operation_type == "vacuum" and (op.site == s_name or op.site == "all"):
                                op.phase = phase
                                op.heap_blks_total = tot_blks
                                op.heap_blks_scanned = scan_blks
                                if pct is not None:
                                    op.progress_percent = pct
                                tbl_display = t_name or op.current_table or op.table_name or "table"
                                phase_display = f" · Phase: {phase}" if phase else ""
                                pct_display = f" ({pct}%)" if pct is not None else ""
                                op.message = (
                                    f"Vacuuming (FULL) {tbl_display} ({op.tables_completed + 1}/{max(op.tables_total, 1)})"
                                    f" in {op.site}{phase_display}{pct_display}"
                                )
                                matched = True
                                break

                        if not matched and phase:
                            pct_display = f" ({pct}%)" if pct is not None else ""
                            msg = f"PostgreSQL VACUUM FULL on {s_name}.{t_name or 'table'} · Phase: {phase}{pct_display}"
                            active_ops.append(
                                DatabaseActiveOperation(
                                    id=f"pg_cluster_{pid}",
                                    operation_type="vacuum",
                                    site=s_name,
                                    table_name=t_name,
                                    current_table=t_name,
                                    tables_completed=0,
                                    tables_total=1,
                                    phase=phase,
                                    progress_percent=pct,
                                    heap_blks_total=tot_blks,
                                    heap_blks_scanned=scan_blks,
                                    started_at=datetime.now(timezone.utc).isoformat(),
                                    duration_seconds=0.0,
                                    status="running",
                                    message=msg,
                                )
                            )
                except Exception as exc:
                    logger.debug("Failed querying pg_stat_progress_cluster: %s", exc)

                act_rows = conn.execute(
                    text("""
                        SELECT
                            pid,
                            query,
                            EXTRACT(EPOCH FROM (now() - query_start)) AS duration_seconds
                        FROM pg_stat_activity
                        WHERE state = 'active'
                          AND pid <> pg_backend_pid()
                          AND query ~* '^\\\\s*(VACUUM|ANALYZE|REINDEX)'
                    """)
                ).fetchall()

                for ar in act_rows:
                    pid = int(ar[0])
                    query_text = str(ar[1]).strip()
                    dur = float(ar[2] or 0.0)
                    q_upper = query_text.upper()
                    op_type = "vacuum" if q_upper.startswith("VACUUM") else ("analyze" if q_upper.startswith("ANALYZE") else "reindex")

                    if any(op.operation_type == op_type for op in active_ops):
                        continue

                    active_ops.append(
                        DatabaseActiveOperation(
                            id=f"pg_act_{pid}",
                            operation_type=op_type,
                            site="database",
                            started_at=datetime.now(timezone.utc).isoformat(),
                            duration_seconds=round(dur, 1),
                            status="running",
                            message=f"PostgreSQL executing: {query_text[:80]}",
                        )
                    )
        except Exception as exc:
            logger.debug("Failed to query live PostgreSQL maintenance progress: %s", exc)

    return active_ops


def _execute_vacuum(
    request: DatabaseVacuumRequest,
    config: UltimaScraperCollectionConfig,
) -> DatabaseVacuumResponse:
    from sqlalchemy import text

    start_time = time.monotonic()
    db_manager = MultiSiteDatabaseManager()

    available_sites = _get_available_database_sites(config, db_manager)
    target_sites = available_sites if request.site.lower() == "all" else [request.site.lower()]

    opts = []
    if request.full:
        opts.append("FULL")
    if request.analyze:
        opts.append("ANALYZE")
    if request.freeze:
        opts.append("FREEZE")
    opt_str = f" ({', '.join(opts)})" if opts else ""
    mode_str = f"VACUUM{opt_str}".strip()

    site_tables_map: dict[str, list[str]] = {}
    total_tables_count = 0

    for site in target_sites:
        engine = _get_maintenance_engine(site)
        if not engine:
            try:
                mgr = db_manager.get_or_create_manager(site)
                engine = getattr(mgr, "engine", None)
            except Exception:
                engine = None
        if not engine:
            continue
        if request.table_name:
            site_tables_map[site] = [request.table_name]
            total_tables_count += 1
        else:
            if getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
                try:
                    with engine.connect() as conn:
                        tbls = conn.execute(
                            text("""
                                SELECT c.relname FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = :schema AND c.relkind = 'r'
                                ORDER BY c.relname
                            """),
                            {"schema": site},
                        ).scalars().all()
                        site_tables_map[site] = [str(t) for t in tbls]
                        total_tables_count += len(tbls)
                except Exception as exc:
                    logger.debug("Failed to list tables for vacuum on %s: %s", site, exc)
                    site_tables_map[site] = []
            else:
                site_tables_map[site] = [site]
                total_tables_count += 1

    if total_tables_count == 0 and not site_tables_map:
        return DatabaseVacuumResponse(
            success=False,
            message="No connected database engine found to execute vacuum.",
            site=request.site,
            table_name=request.table_name,
            mode=mode_str,
            duration_seconds=0.0,
        )

    op_id = _register_database_op(
        operation_type="vacuum",
        site=request.site,
        table_name=request.table_name,
        tables_total=total_tables_count,
    )

    executed_tables: list[str] = []
    error_message = ""

    try:
        tables_done = 0
        for site, tables in site_tables_map.items():
            if _is_database_op_cancelled(op_id):
                error_message = "Vacuum operation was cancelled by user."
                break

            engine = _get_maintenance_engine(site)
            if not engine:
                try:
                    mgr = db_manager.get_or_create_manager(site)
                    engine = getattr(mgr, "engine", None)
                except Exception:
                    engine = None
            if not engine:
                continue

            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                safe_schema = site.replace('"', '""')
                for tbl in tables:
                    if _is_database_op_cancelled(op_id):
                        error_message = "Vacuum operation was cancelled by user."
                        break

                    pct = round((tables_done / max(total_tables_count, 1)) * 100, 1)
                    _update_database_op(
                        op_id,
                        current_table=tbl,
                        tables_completed=tables_done,
                        phase="executing",
                        progress_percent=pct,
                        message=f"Vacuuming table {tbl} ({tables_done + 1}/{total_tables_count}) in {site}…",
                    )

                    if getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
                        safe_tbl = tbl.replace('"', '""')
                        conn.exec_driver_sql(f'VACUUM{opt_str} "{safe_schema}"."{safe_tbl}"')
                        executed_tables.append(f"{safe_schema}.{safe_tbl}")
                    else:
                        conn.exec_driver_sql("VACUUM")
                        executed_tables.append(site)

                    tables_done += 1

        duration = time.monotonic() - start_time
        if error_message:
            _finish_database_op(op_id, success=False, message=error_message)
            return DatabaseVacuumResponse(
                success=False,
                message=error_message,
                site=request.site,
                table_name=request.table_name,
                mode=mode_str,
                duration_seconds=round(duration, 3),
            )

        msg = f"Successfully executed {mode_str} on {len(executed_tables)} table(s) across {len(target_sites)} site(s)."
        _finish_database_op(op_id, success=True, message=msg)
        return DatabaseVacuumResponse(
            success=True,
            message=msg,
            site=request.site,
            table_name=request.table_name,
            mode=mode_str,
            duration_seconds=round(duration, 3),
        )
    except Exception as exc:
        duration = time.monotonic() - start_time
        logger.exception("Database vacuum failed: %s", exc)
        err_msg = f"Vacuum failed: {exc}"
        _finish_database_op(op_id, success=False, message=err_msg)
        return DatabaseVacuumResponse(
            success=False,
            message=err_msg,
            site=request.site,
            table_name=request.table_name,
            mode=mode_str,
            duration_seconds=round(duration, 3),
        )


def _execute_analyze(
    request: DatabaseAnalyzeRequest,
    config: UltimaScraperCollectionConfig,
) -> DatabaseAnalyzeResponse:
    from sqlalchemy import text

    start_time = time.monotonic()
    db_manager = MultiSiteDatabaseManager()

    available_sites = _get_available_database_sites(config, db_manager)
    target_sites = available_sites if request.site.lower() == "all" else [request.site.lower()]

    site_tables_map: dict[str, list[str]] = {}
    total_tables_count = 0

    for site in target_sites:
        engine = _get_maintenance_engine(site)
        if not engine:
            try:
                mgr = db_manager.get_or_create_manager(site)
                engine = getattr(mgr, "engine", None)
            except Exception:
                engine = None
        if not engine:
            continue
        if request.table_name:
            site_tables_map[site] = [request.table_name]
            total_tables_count += 1
        else:
            if getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
                try:
                    with engine.connect() as conn:
                        tbls = conn.execute(
                            text("""
                                SELECT c.relname FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = :schema AND c.relkind = 'r'
                                ORDER BY c.relname
                            """),
                            {"schema": site},
                        ).scalars().all()
                        site_tables_map[site] = [str(t) for t in tbls]
                        total_tables_count += len(tbls)
                except Exception as exc:
                    logger.debug("Failed to list tables for analyze on %s: %s", site, exc)
                    site_tables_map[site] = []
            else:
                site_tables_map[site] = [site]
                total_tables_count += 1

    if total_tables_count == 0 and not site_tables_map:
        return DatabaseAnalyzeResponse(
            success=False,
            message="No connected database engine found to execute analyze.",
            site=request.site,
            table_name=request.table_name,
            duration_seconds=0.0,
        )

    op_id = _register_database_op(
        operation_type="analyze",
        site=request.site,
        table_name=request.table_name,
        tables_total=total_tables_count,
    )

    executed_tables: list[str] = []
    error_message = ""

    try:
        tables_done = 0
        for site, tables in site_tables_map.items():
            if _is_database_op_cancelled(op_id):
                error_message = "Analyze operation was cancelled by user."
                break

            engine = _get_maintenance_engine(site)
            if not engine:
                try:
                    mgr = db_manager.get_or_create_manager(site)
                    engine = getattr(mgr, "engine", None)
                except Exception:
                    engine = None
            if not engine:
                continue

            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                safe_schema = site.replace('"', '""')
                for tbl in tables:
                    if _is_database_op_cancelled(op_id):
                        error_message = "Analyze operation was cancelled by user."
                        break

                    pct = round((tables_done / max(total_tables_count, 1)) * 100, 1)
                    _update_database_op(
                        op_id,
                        current_table=tbl,
                        tables_completed=tables_done,
                        phase="analyzing",
                        progress_percent=pct,
                        message=f"Analyzing table {tbl} ({tables_done + 1}/{total_tables_count}) in {site}…",
                    )

                    if getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
                        safe_tbl = tbl.replace('"', '""')
                        conn.exec_driver_sql(f'ANALYZE "{safe_schema}"."{safe_tbl}"')
                        executed_tables.append(f"{safe_schema}.{safe_tbl}")
                    else:
                        conn.exec_driver_sql("PRAGMA optimize")
                        executed_tables.append(site)

                    tables_done += 1

        duration = time.monotonic() - start_time
        if error_message:
            _finish_database_op(op_id, success=False, message=error_message)
            return DatabaseAnalyzeResponse(
                success=False,
                message=error_message,
                site=request.site,
                table_name=request.table_name,
                duration_seconds=round(duration, 3),
            )

        msg = f"Successfully analyzed {len(executed_tables)} table(s)."
        _finish_database_op(op_id, success=True, message=msg)
        return DatabaseAnalyzeResponse(
            success=True,
            message=msg,
            site=request.site,
            table_name=request.table_name,
            duration_seconds=round(duration, 3),
        )
    except Exception as exc:
        duration = time.monotonic() - start_time
        logger.exception("Database analyze failed: %s", exc)
        err_msg = f"Analyze failed: {exc}"
        _finish_database_op(op_id, success=False, message=err_msg)
        return DatabaseAnalyzeResponse(
            success=False,
            message=err_msg,
            site=request.site,
            table_name=request.table_name,
            duration_seconds=round(duration, 3),
        )


def _execute_reindex(
    request: DatabaseReindexRequest,
    config: UltimaScraperCollectionConfig,
) -> DatabaseReindexResponse:
    start_time = time.monotonic()
    db_manager = MultiSiteDatabaseManager()

    available_sites = _get_available_database_sites(config, db_manager)
    target_sites = available_sites if request.site.lower() == "all" else [request.site.lower()]

    op_id = _register_database_op(
        operation_type="reindex",
        site=request.site,
        table_name=request.table_name,
        tables_total=len(target_sites),
    )

    executed_items: list[str] = []
    error_message = ""

    try:
        items_done = 0
        for site in target_sites:
            if _is_database_op_cancelled(op_id):
                error_message = "Reindex operation was cancelled by user."
                break

            engine = _get_maintenance_engine(site)
            if not engine:
                try:
                    mgr = db_manager.get_or_create_manager(site)
                    engine = getattr(mgr, "engine", None)
                except Exception:
                    engine = None
            if not engine:
                continue

            pct = round((items_done / max(len(target_sites), 1)) * 100, 1)
            target_desc = request.table_name or site
            _update_database_op(
                op_id,
                current_table=request.table_name,
                tables_completed=items_done,
                phase="reindexing",
                progress_percent=pct,
                message=f"Reindexing {target_desc} ({items_done + 1}/{len(target_sites)}) in {site}…",
            )

            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                if getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
                    safe_schema = site.replace('"', '""')
                    concurrently_str = " CONCURRENTLY" if request.concurrently else ""
                    if request.table_name:
                        safe_tbl = request.table_name.replace('"', '""')
                        conn.exec_driver_sql(f'REINDEX TABLE{concurrently_str} "{safe_schema}"."{safe_tbl}"')
                        executed_items.append(f"{safe_schema}.{safe_tbl}")
                    else:
                        conn.exec_driver_sql(f'REINDEX SCHEMA{concurrently_str} "{safe_schema}"')
                        executed_items.append(f"{safe_schema}")
                else:
                    conn.exec_driver_sql("REINDEX")
                    executed_items.append(site)

            items_done += 1

        duration = time.monotonic() - start_time
        if error_message:
            _finish_database_op(op_id, success=False, message=error_message)
            return DatabaseReindexResponse(
                success=False,
                message=error_message,
                site=request.site,
                table_name=request.table_name,
                duration_seconds=round(duration, 3),
            )

        msg = f"Successfully reindexed {len(executed_items)} item(s)."
        _finish_database_op(op_id, success=True, message=msg)
        return DatabaseReindexResponse(
            success=True,
            message=msg,
            site=request.site,
            table_name=request.table_name,
            duration_seconds=round(duration, 3),
        )
    except Exception as exc:
        duration = time.monotonic() - start_time
        err_msg = str(exc)
        if "permission denied" in err_msg.lower() or getattr(getattr(exc, "orig", None), "sqlstate", None) == "42501":
            err_msg = "Reindexing requires table owner privileges. Configure ULTIMA_ARCHIVE_OWNER_DATABASE_URL in .env to enable reindexing."
        logger.exception("Database reindex failed: %s", exc)
        full_err = f"Reindex failed: {err_msg}"
        _finish_database_op(op_id, success=False, message=full_err)
        return DatabaseReindexResponse(
            success=False,
            message=full_err,
            site=request.site,
            table_name=request.table_name,
            duration_seconds=round(duration, 3),
        )


@router.get("/database", response_model=DatabaseOverviewResponse)
@router.get("/database/overview", response_model=DatabaseOverviewResponse)
async def get_database_overview(
    backend: BackendInstance = Depends(get_backend_instance),
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> DatabaseOverviewResponse:
    """Return database status, metrics, table sizes, bloat, and active operations for all sites."""
    return await asyncio.to_thread(_collect_database_overview, backend.instance_id, config)


@router.post("/database/vacuum", response_model=DatabaseVacuumResponse)
async def vacuum_database(
    request: DatabaseVacuumRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> DatabaseVacuumResponse:
    """Run VACUUM on database tables (supports options: full, analyze, freeze).

    Shielded from client disconnect cancellation so refreshes do not abort maintenance.
    """
    return await asyncio.shield(asyncio.to_thread(_execute_vacuum, request, config))


@router.post("/database/analyze", response_model=DatabaseAnalyzeResponse)
async def analyze_database(
    request: DatabaseAnalyzeRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> DatabaseAnalyzeResponse:
    """Run ANALYZE on database tables to update planner statistics.

    Shielded from client disconnect cancellation so refreshes do not abort maintenance.
    """
    return await asyncio.shield(asyncio.to_thread(_execute_analyze, request, config))


@router.post("/database/reindex", response_model=DatabaseReindexResponse)
async def reindex_database(
    request: DatabaseReindexRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> DatabaseReindexResponse:
    """Rebuild indexes for database tables or schemas.

    Shielded from client disconnect cancellation so refreshes do not abort maintenance.
    """
    return await asyncio.shield(asyncio.to_thread(_execute_reindex, request, config))


@router.post("/database/cancel", response_model=DatabaseCancelResponse)
async def cancel_database_operation(
    request: DatabaseCancelRequest,
) -> DatabaseCancelResponse:
    """Cancel an active database maintenance operation."""
    from sqlalchemy import text

    cancelled_count = 0
    with _active_database_ops_lock:
        for op_id, op in list(_active_database_ops.items()):
            if request.operation_id and op_id != request.operation_id:
                continue
            if request.site and op.get("site") != request.site and request.site != "all":
                continue
            _cancelled_database_ops.add(op_id)
            cancelled_count += 1

    engine = _get_maintenance_engine(request.site)
    if engine and getattr(engine, "dialect", None) and engine.dialect.name == "postgresql":
        try:
            with engine.connect() as conn:
                pids = conn.execute(
                    text("""
                        SELECT pid FROM pg_stat_activity
                        WHERE state = 'active'
                          AND pid <> pg_backend_pid()
                          AND query ~* '^\\\\s*(VACUUM|ANALYZE|REINDEX)'
                    """)
                ).scalars().all()
                for pid in pids:
                    try:
                        conn.execute(text("SELECT pg_cancel_backend(:pid)"), {"pid": pid})
                        cancelled_count += 1
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("Failed to cancel PG backend query: %s", exc)

    return DatabaseCancelResponse(
        success=True,
        message=f"Cancelled {cancelled_count} active database operation(s).",
        cancelled_count=cancelled_count,
    )
