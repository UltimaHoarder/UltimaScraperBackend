"""Routes other backends use to hand this one finished downloads to keep.

Only backend peer tokens open these routes, whatever the security enforcement
mode: they write files to this backend's drives.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ... import peer_tokens
from ...config import UltimaScraperCollectionConfig
from ...job_system.drive_selection import DriveSelectionError, select_user_destination
from ...offload import media, placement, store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/offload", tags=["offload"])

OFFLOAD_PREFIX = "/api/v1/offload/"


async def require_peer(
    token: str | None = Header(default=None, alias=peer_tokens.PEER_TOKEN_HEADER),
) -> str:
    """The calling backend, or 401: only peer tokens open these routes."""
    await peer_tokens.refresh_for(token)
    caller = peer_tokens.peer_backend(token)
    if caller is None:
        raise HTTPException(status_code=401, detail="A backend peer token is required")
    return caller


def _setup():
    return UltimaScraperCollectionConfig.load_or_create_default_config().settings.download_setup


async def _target(root: str, path: str) -> Path:
    setup = await asyncio.to_thread(_setup)
    try:
        return store.resolve_target(setup, root, path)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class DestinationRequest(BaseModel):
    site_name: str
    user_id: int | None = None
    usernames: list[str] = Field(default_factory=list)
    required_space: int = 0
    ignore_space_requirements: bool = False


class DestinationResponse(BaseModel):
    root: str
    performer_relative_path: str
    directory_format: str
    performer_username: str
    requires_full_directory_render: bool
    site_name: str


@router.post("/destination", response_model=DestinationResponse)
async def destination(request: DestinationRequest, caller: str = Depends(require_peer)) -> DestinationResponse:
    """Where this backend would keep a performer's files: their folder, or the drive with room."""
    config = await asyncio.to_thread(UltimaScraperCollectionConfig.load_or_create_default_config)
    try:
        selection = await asyncio.to_thread(
            select_user_destination,
            user_id=request.user_id,
            usernames=request.usernames,
            site_name=request.site_name,
            user_manager=None,
            config=config,
            required_space=request.required_space,
            ignore_space_requirements=request.ignore_space_requirements,
        )
    except DriveSelectionError as exc:
        raise HTTPException(status_code=507, detail=str(exc)) from exc
    logger.info("Destination for %s on behalf of %s: %s", request.usernames[:1], caller, selection.root)
    return DestinationResponse(
        root=str(selection.root),
        performer_relative_path=selection.performer_relative_path.as_posix(),
        directory_format=str(selection.directory_format),
        performer_username=selection.performer_username,
        requires_full_directory_render=selection.requires_full_directory_render,
        site_name=selection.site_name,
    )


@router.get("/files")
async def upload_state(
    root: str = Query(...), path: str = Query(...), caller: str = Depends(require_peer)
) -> dict[str, Any]:
    """How much of a file has arrived, and whether it is already kept."""
    state = await asyncio.to_thread(store.upload_state, await _target(root, path))
    return {"received": state.received, "exists": state.exists, "size": state.size}


@router.put("/files")
async def upload(
    request: Request,
    root: str = Query(...),
    path: str = Query(...),
    offset: int = Query(0, ge=0),
    content_length: int | None = Header(default=None),
    caller: str = Depends(require_peer),
) -> dict[str, Any]:
    """Write the request body into the file's upload from ``offset`` on."""
    target = await _target(root, path)
    try:
        handle = await asyncio.to_thread(store.open_part, target, offset, content_length)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    received = offset
    try:
        async for chunk in request.stream():
            if chunk:
                await asyncio.to_thread(handle.write, chunk)
                received += len(chunk)
    finally:
        await asyncio.to_thread(handle.close)
    return {"received": received}


class CommitRequest(BaseModel):
    root: str
    path: str
    size: int = Field(ge=0)
    mtime: float | None = None


@router.post("/files/commit")
async def commit(request: CommitRequest, caller: str = Depends(require_peer)) -> dict[str, Any]:
    """Put a complete upload in place under its final name."""
    target = await _target(request.root, request.path)
    try:
        committed = await asyncio.to_thread(store.commit, target, request.size, request.mtime)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    logger.info("Kept %s for %s (%d bytes%s)", committed.path, caller, committed.size, ", already present" if committed.existing else "")
    return {"path": str(committed.path), "size": committed.size, "existing": committed.existing}


# ---- DRM items decrypted and merged here for another backend -----------------


@router.get("/capacity")
async def capacity(caller: str = Depends(require_peer)) -> dict[str, Any]:
    """This backend's measured decrypt throughput and queue, for the placement rule."""
    current = placement.meter.capacity()
    return {
        "rate": current.rate,
        "waiting": current.waiting,
        "slots": current.slots,
        "decrypts": shutil.which("mp4decrypt") is not None,
    }


async def _stream(key: str, name: str, total: int) -> Path:
    setup = await asyncio.to_thread(_setup)
    try:
        return await asyncio.to_thread(media.stream_target, setup, key, name, total)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/streams")
async def stream_state(
    key: str = Query(...), name: str = Query(...), total: int = Query(..., ge=0), caller: str = Depends(require_peer)
) -> dict[str, Any]:
    state = await asyncio.to_thread(store.upload_state, await _stream(key, name, total))
    return {"received": state.received, "exists": state.exists, "size": state.size}


@router.put("/streams")
async def upload_stream(
    request: Request,
    key: str = Query(...),
    name: str = Query(...),
    total: int = Query(..., ge=0),
    offset: int = Query(0, ge=0),
    content_length: int | None = Header(default=None),
    caller: str = Depends(require_peer),
) -> dict[str, Any]:
    target = await _stream(key, name, total)
    try:
        handle = await asyncio.to_thread(store.open_part, target, offset, content_length)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    received = offset
    try:
        async for chunk in request.stream():
            if chunk:
                await asyncio.to_thread(handle.write, chunk)
                received += len(chunk)
    finally:
        await asyncio.to_thread(handle.close)
    return {"received": received}


class StreamCommitRequest(BaseModel):
    key: str
    name: str
    total: int = Field(ge=0)
    size: int = Field(ge=0)


@router.post("/streams/commit")
async def commit_stream(request: StreamCommitRequest, caller: str = Depends(require_peer)) -> dict[str, Any]:
    target = await _stream(request.key, request.name, request.total)
    try:
        committed = await asyncio.to_thread(store.commit, target, request.size)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"path": str(committed.path), "size": committed.size, "existing": committed.existing}


class DecryptRequest(BaseModel):
    key: str
    total: int = Field(ge=0)
    root: str
    path: str
    drm_key: str = Field(min_length=1)
    mtime: float | None = None


@router.post("/drm")
async def decrypt_and_keep(request: DecryptRequest, caller: str = Depends(require_peer)) -> dict[str, Any]:
    """Decrypt the uploaded streams, merge them, and keep the result under ``path``."""
    final = await _target(request.root, request.path)
    setup = await asyncio.to_thread(_setup)
    try:
        directory = await asyncio.to_thread(media.stream_dir, setup, request.key, request.total)
    except store.OffloadRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    streams = [directory / f"{name}.mp4" for name in media.STREAMS if (directory / f"{name}.mp4").exists()]
    if not streams:
        raise HTTPException(status_code=409, detail="no committed streams for this key")
    try:
        committed = await media.decrypt_and_keep(streams, request.drm_key, final, request.mtime)
    except media.MediaUnavailable as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    logger.info("Decrypted %s for %s", committed.path, caller)
    return {"path": str(committed.path), "size": committed.size, "existing": committed.existing}
