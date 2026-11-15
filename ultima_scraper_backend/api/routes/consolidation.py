"""Consolidating performers whose folders are split across this backend's drives.

The WebUI lists split performers, previews one (where its files would go and
what happens to each), then starts a run over the chosen ones. One run at a
time per backend; its progress lives in Redis, so every API process reports
the same run.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ...config import UltimaScraperCollectionConfig
from ...job_system import consolidation
from ...models.backend import BackendInstance
from ..dependencies import get_backend_instance, get_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/consolidation", tags=["consolidation"])

_RUN_TTL_SECONDS = 24 * 3600
_LOCK_TTL_SECONDS = 600
_EXAMPLE_LIMIT = 50
_PROGRESS_SECONDS = 2.0
_HEARTBEAT_SECONDS = 2.0
# A scan whose heartbeat is this old lost its process (a restart).
_STALE_SECONDS = 30.0
# Without Redis: this process's own run.
_local_status: dict[str, dict[str, Any]] = {}
_tasks: set[asyncio.Task[None]] = set()


class FolderInfo(BaseModel):
    root: str
    path: str
    priority: int
    files: int | None = None
    bytes: int | None = None
    # Usable free space (less running downloads' claims) and the drive's reserve.
    free_bytes: int | None = None
    reserve_bytes: int | None = None
    # Bytes that would move here if this drive were the target.
    needed_bytes: int | None = None


class SplitPerformerInfo(BaseModel):
    relative_path: str
    folders: list[FolderInfo]


class PreviewRequest(BaseModel):
    site_name: str = "OnlyFans"
    relative_path: str
    # A drive already holding the performer; the best one is picked when unset.
    target_root: str | None = None


class PreviewResponse(BaseModel):
    relative_path: str
    folders: list[FolderInfo]
    target_root: str | None
    reason: str = ""
    # Bytes the target drive gains; using some of its reserve is a warning,
    # not having the free space at all refuses the move.
    needed_bytes: int = 0
    into_reserve: bool = False
    no_space: bool = False
    move_files: int
    move_bytes: int
    duplicates: int
    conflicts: list[str]
    recent: int
    too_long: int


class RunRequest(BaseModel):
    site_name: str = "OnlyFans"
    relative_paths: list[str]
    # Chosen target drive per performer; the rest get the best drive.
    targets: dict[str, str] = Field(default_factory=dict)


def _status_key(backend: BackendInstance) -> str:
    return f"ultimascraper:consolidation:{backend.instance_id}:status"


def _lock_key(backend: BackendInstance) -> str:
    return f"ultimascraper:consolidation:{backend.instance_id}:lock"


def _redis(backend: BackendInstance) -> Any | None:
    manager = backend.redis
    if manager is None or not getattr(manager, "is_connected", False):
        return None
    return getattr(manager, "client", None)


def _setup(config: UltimaScraperCollectionConfig):
    return config.settings.download_setup


def _still_split(performers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A scan's performers without the folders that are gone since, such as those
    consolidated or deleted; a performer left on one drive is no longer split."""
    current = []
    for performer in performers:
        folders = [folder for folder in performer["folders"] if os.path.isdir(folder["path"])]
        if len(folders) > 1:
            current.append({**performer, "folders": folders})
    return current


async def _find(
    backend: BackendInstance, setup, site_name: str, relative_path: str
) -> consolidation.SplitPerformer:
    """The performer from the last scan, with each folder's current drive settings."""
    from ...job_system.drive_selection import _flatten_drive_directories

    wanted = Path(relative_path).as_posix().casefold()
    scan = await _load(backend, _scan_key(backend, site_name))
    entry = next(
        (item for item in scan.get("performers", []) if item["relative_path"].casefold() == wanted),
        None,
    )
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{relative_path} is not in the last scan")
    still_split = await asyncio.to_thread(_still_split, [entry])
    if not still_split:
        raise HTTPException(status_code=404, detail=f"{relative_path} is no longer split across drives")
    entry = still_split[0]
    drives = {drive_dir.path.expanduser(): drive_dir.drive for drive_dir in _flatten_drive_directories(setup)}
    folders = []
    for folder in entry["folders"]:
        root = Path(folder["root"])
        if root not in drives:
            raise HTTPException(status_code=409, detail=f"{root} is no longer a download drive; scan again")
        folders.append(
            consolidation.PerformerFolder(
                root=root, relative_path=Path(folder["path"]).relative_to(root), drive=drives[root]
            )
        )
    return consolidation.SplitPerformer(relative_path=Path(entry["relative_path"]), folders=tuple(folders))


def _folder_info(folder: consolidation.PerformerFolder, plan=None) -> FolderInfo:
    return FolderInfo(
        root=str(folder.root),
        path=str(folder.path),
        priority=folder.drive.priority,
        files=None if plan is None else plan.folder_files.get(folder.root),
        bytes=None if plan is None else plan.folder_bytes.get(folder.root),
        free_bytes=None if plan is None else plan.folder_free.get(folder.root),
        reserve_bytes=None if plan is None else plan.folder_reserve.get(folder.root),
        needed_bytes=None if plan is None else plan.folder_needed.get(folder.root),
    )


def _scan_key(backend: BackendInstance, site_name: str) -> str:
    return f"ultimascraper:consolidation:{backend.instance_id}:split:{site_name.casefold()}"


def _control_key(backend: BackendInstance, site_name: str) -> str:
    return f"{_scan_key(backend, site_name)}:control"


async def _set_control(backend: BackendInstance, site_name: str, command: str | None) -> None:
    key = _control_key(backend, site_name)
    redis = _redis(backend)
    if redis is None:
        if command is None:
            _local_status.pop(key, None)
        else:
            _local_status[key] = {"command": command}
        return
    if command is None:
        await redis.delete(key)
    else:
        await redis.set(key, command, ex=_RUN_TTL_SECONDS)


async def _control(backend: BackendInstance, site_name: str) -> str | None:
    key = _control_key(backend, site_name)
    redis = _redis(backend)
    if redis is None:
        return (_local_status.get(key) or {}).get("command")
    raw = await redis.get(key)
    return raw.decode() if isinstance(raw, bytes) else raw


def _scan_active(scan: dict[str, Any]) -> bool:
    """Scanning, and its process still reporting in (not lost to a restart)."""
    beat = scan.get("heartbeat_at") or scan.get("started_at") or 0
    return bool(scan.get("scanning")) and time.time() - beat < _STALE_SECONDS


async def _scan(backend: BackendInstance, setup, site_name: str) -> None:
    key = _scan_key(backend, site_name)
    previous = await _load(backend, key)
    roots = await asyncio.to_thread(consolidation.scan_roots, setup)
    # The last finished scan stays listed until this one finishes.
    scan: dict[str, Any] = {
        "site_name": site_name,
        "scanning": True,
        "paused": False,
        "started_at": time.time(),
        "heartbeat_at": time.time(),
        "roots_total": len(roots),
        "roots_done": 0,
        "current_root": None,
        "scanned_at": previous.get("scanned_at"),
        "performers": previous.get("performers", []),
    }
    await _store(backend, key, scan)
    command: dict[str, str | None] = {"value": None}
    progress: dict[str, Any] = {"root": None, "done": 0}

    def checkpoint(root: Path, roots_done: int) -> None:
        progress.update(root=str(root), done=roots_done)
        while command["value"] != "cancel":
            if command["value"] != "pause":
                return
            time.sleep(0.5)
        raise consolidation.ScanCancelled

    async def watch() -> None:
        while True:
            command["value"] = await _control(backend, site_name)
            scan.update(
                paused=command["value"] == "pause",
                current_root=progress["root"],
                roots_done=progress["done"],
                heartbeat_at=time.time(),
            )
            await _store(backend, key, scan)
            await asyncio.sleep(_HEARTBEAT_SECONDS)

    watcher = asyncio.create_task(watch())
    try:
        performers = await asyncio.to_thread(
            consolidation.find_split_performers, setup, site_name, checkpoint=checkpoint
        )
        scan["performers"] = [
            SplitPerformerInfo(
                relative_path=performer.relative_path.as_posix(),
                folders=[_folder_info(folder) for folder in performer.folders],
            ).model_dump()
            for performer in performers
        ]
        scan["scanned_at"] = time.time()
        scan["roots_done"] = len(roots)
        scan.pop("error", None)
    except consolidation.ScanCancelled:
        scan["cancelled_at"] = time.time()
    except Exception as exc:
        logger.exception("Scanning for split performers failed")
        scan["error"] = str(exc)
    finally:
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher
    scan.update(scanning=False, paused=False, current_root=None)
    await _store(backend, key, scan)
    await _set_control(backend, site_name, None)


async def _store(backend: BackendInstance, key: str, value: dict[str, Any]) -> None:
    redis = _redis(backend)
    if redis is None:
        _local_status[key] = value
        return
    await redis.set(key, json.dumps(value), ex=_RUN_TTL_SECONDS)


async def _load(backend: BackendInstance, key: str) -> dict[str, Any]:
    redis = _redis(backend)
    if redis is None:
        return dict(_local_status.get(key) or {})
    raw = await redis.get(key)
    return json.loads(raw) if raw else {}


def _start(coroutine) -> None:
    task = asyncio.create_task(coroutine)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


@router.post("/scan", status_code=202)
async def scan(
    site_name: str = "OnlyFans",
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, Any]:
    """Look for performers split across this backend's drives; this can take minutes."""
    if _scan_active(await _load(backend, _scan_key(backend, site_name))):
        return {"started": False}
    await _set_control(backend, site_name, None)
    _start(_scan(backend, _setup(config), site_name))
    return {"started": True}


async def _command_scan(backend: BackendInstance, site_name: str, command: str | None) -> dict[str, Any]:
    if not _scan_active(await _load(backend, _scan_key(backend, site_name))):
        raise HTTPException(status_code=409, detail="No scan is running")
    await _set_control(backend, site_name, command)
    return {"ok": True}


@router.post("/scan/pause")
async def pause_scan(
    site_name: str = "OnlyFans", backend: BackendInstance = Depends(get_backend_instance)
) -> dict[str, Any]:
    """Hold the running scan at its next folder; resume carries on from there."""
    return await _command_scan(backend, site_name, "pause")


@router.post("/scan/resume")
async def resume_scan(
    site_name: str = "OnlyFans", backend: BackendInstance = Depends(get_backend_instance)
) -> dict[str, Any]:
    return await _command_scan(backend, site_name, None)


@router.post("/scan/cancel")
async def cancel_scan(
    site_name: str = "OnlyFans", backend: BackendInstance = Depends(get_backend_instance)
) -> dict[str, Any]:
    """Stop the running scan; the last finished scan's list stays."""
    key = _scan_key(backend, site_name)
    scan = await _load(backend, key)
    if scan.get("scanning") and not _scan_active(scan):
        # Its process is gone: nothing is left to stop.
        scan.update(scanning=False, paused=False, current_root=None, cancelled_at=time.time())
        await _store(backend, key, scan)
        return {"ok": True}
    return await _command_scan(backend, site_name, "cancel")


@router.get("/split")
async def list_split_performers(
    site_name: str = "OnlyFans",
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, Any]:
    """The last scan's split performers (empty until a scan has run)."""
    scan = await _load(backend, _scan_key(backend, site_name)) or {
        "site_name": site_name,
        "scanning": False,
        "performers": [],
    }
    if scan.get("scanning") and not _scan_active(scan):
        scan.update(scanning=False, paused=False, interrupted=True)
    # Checked on every read: a running scan keeps storing the last finished list.
    scan["performers"] = await asyncio.to_thread(_still_split, scan.get("performers", []))
    return scan


@router.post("/preview", response_model=PreviewResponse)
async def preview(
    request: PreviewRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> PreviewResponse:
    """Where a split performer's files would go, without moving anything."""
    performer = await _find(backend, _setup(config), request.site_name, request.relative_path)
    plan = await asyncio.to_thread(
        consolidation.plan_consolidation, performer, target_root=request.target_root
    )
    moves = plan.of_kind("move")
    return PreviewResponse(
        relative_path=performer.relative_path.as_posix(),
        folders=[_folder_info(folder, plan) for folder in performer.folders],
        target_root=None if plan.target is None else str(plan.target.root),
        reason=plan.reason,
        needed_bytes=plan.needed,
        into_reserve=plan.into_reserve,
        no_space=plan.no_space,
        move_files=len(moves),
        move_bytes=sum(move.size for move in moves),
        duplicates=len(plan.of_kind("duplicate")),
        conflicts=[str(move.source) for move in plan.of_kind("conflict")],
        recent=len(plan.of_kind("recent")),
        too_long=len(plan.of_kind("too_long")),
    )


async def _write_status(backend: BackendInstance, status: dict[str, Any]) -> None:
    await _store(backend, _status_key(backend), status)
    redis = _redis(backend)
    if redis is not None:
        await redis.expire(_lock_key(backend), _LOCK_TTL_SECONDS)


async def _run(
    backend: BackendInstance,
    setup,
    site_name: str,
    relative_paths: list[str],
    token: str,
    targets: dict[str, str] | None = None,
) -> None:
    targets = targets or {}
    status: dict[str, Any] = {
        "running": True,
        "site_name": site_name,
        "started_at": time.time(),
        "finished_at": None,
        "total": len(relative_paths),
        "done": 0,
        "current": None,
        "performers": [],
    }
    update_paths = consolidation.database_path_updater(site_name)
    loop = asyncio.get_running_loop()
    try:
        for relative_path in relative_paths:
            status["current"] = relative_path
            await _write_status(backend, status)
            entry: dict[str, Any] = {"relative_path": relative_path}
            status["performers"].append(entry)
            try:
                performer = await _find(backend, setup, site_name, relative_path)
                plan = await asyncio.to_thread(
                    consolidation.plan_consolidation, performer, target_root=targets.get(relative_path)
                )
                if plan.target is None:
                    entry["error"] = plan.reason
                else:
                    entry["target_root"] = str(plan.target.root)

                    reported = [0.0]

                    def progress(result: consolidation.ConsolidationResult, entry=entry) -> None:
                        entry.update(moved=result.moved, moved_bytes=result.moved_bytes, duplicates=result.duplicates)
                        if time.monotonic() - reported[0] >= _PROGRESS_SECONDS:
                            reported[0] = time.monotonic()
                            asyncio.run_coroutine_threadsafe(_write_status(backend, status), loop)

                    result = await asyncio.to_thread(
                        consolidation.execute_consolidation, plan, update_paths, on_progress=progress
                    )
                    entry.update(
                        moved=result.moved,
                        moved_bytes=result.moved_bytes,
                        duplicates=result.duplicates,
                        conflicts=result.conflicts[:_EXAMPLE_LIMIT],
                        conflict_count=len(result.conflicts),
                        skipped=result.skipped,
                        failed=result.failed[:_EXAMPLE_LIMIT],
                        failed_count=len(result.failed),
                    )
            except HTTPException as exc:
                entry["error"] = str(exc.detail)
            except Exception as exc:
                logger.exception("Consolidating %s failed", relative_path)
                entry["error"] = str(exc)
            status["done"] += 1
    finally:
        status.update(running=False, current=None, finished_at=time.time())
        await _write_status(backend, status)
        redis = _redis(backend)
        held = None if redis is None else await redis.get(_lock_key(backend))
        if isinstance(held, bytes):
            held = held.decode()
        if held == token:
            await redis.delete(_lock_key(backend))


@router.post("/run", status_code=202)
async def run(
    request: RunRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, Any]:
    """Consolidate the chosen performers in the background; one run at a time."""
    if not request.relative_paths:
        raise HTTPException(status_code=400, detail="Choose at least one performer")
    token = f"{time.time()}"
    redis = _redis(backend)
    if redis is not None:
        if not await redis.set(_lock_key(backend), token, nx=True, ex=_LOCK_TTL_SECONDS):
            raise HTTPException(status_code=409, detail="A consolidation is already running")
    elif (_local_status.get(_status_key(backend)) or {}).get("running"):
        raise HTTPException(status_code=409, detail="A consolidation is already running")
    _start(
        _run(
            backend,
            _setup(config),
            request.site_name,
            list(request.relative_paths),
            token,
            dict(request.targets),
        )
    )
    return {"started": True, "total": len(request.relative_paths)}


@router.get("/status")
async def status(backend: BackendInstance = Depends(get_backend_instance)) -> dict[str, Any]:
    """The current or last consolidation run on this backend."""
    return await _load(backend, _status_key(backend))
