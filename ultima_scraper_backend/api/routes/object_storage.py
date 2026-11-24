"""Object storage targets: copy downloaded media into S3-compatible buckets.

See ``ultima_scraper_backend/object_storage.py``. These routes sit behind the
admin token like every other route. Secret keys go in and are never shown again.
"""

from __future__ import annotations

import asyncio
import mimetypes
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Response
from pydantic import BaseModel, Field

from ... import object_storage
from ...config import UltimaScraperCollectionConfig
from ..dependencies import get_config
from .media import resolve_local_media_file

router = APIRouter(prefix="/object-storage", tags=["object-storage"])

TargetName = Path(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")


def _store_for(target: object_storage.StorageTarget) -> object_storage.ObjectStore:
    return object_storage.S3Store(target)


def _target(name: str) -> object_storage.StorageTarget:
    target = object_storage.load_target(name)
    if target is None:
        raise HTTPException(status_code=404, detail=f"No storage target named {name}")
    return target


@router.get("/targets")
async def list_targets() -> dict[str, dict[str, Any]]:
    return await asyncio.to_thread(object_storage.list_targets)


@router.put("/targets/{name}")
async def put_target(target: object_storage.StorageTarget, name: str = TargetName) -> dict[str, Any]:
    await asyncio.to_thread(object_storage.save_target, name, target)
    return target.public()


@router.delete("/targets/{name}", status_code=204)
async def delete_target(name: str = TargetName) -> Response:
    if not await asyncio.to_thread(object_storage.delete_target, name):
        raise HTTPException(status_code=404, detail=f"No storage target named {name}")
    return Response(status_code=204)


class UploadRequest(BaseModel):
    site: str
    media_id: int
    key: str = Field(min_length=1, max_length=1024)
    thumbnail: object_storage.ThumbnailRequest | None = None
    # False: only the thumbnail, for an original that is already in the bucket.
    upload_original: bool = True


class UploadResponse(BaseModel):
    state: str = Field(description="uploaded, or missing when the file isn't on this backend's disk")
    size_bytes: int = 0
    content_type: str | None = None
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None
    thumbnail_state: str | None = Field(
        default=None,
        description="ready, none (not an image or video), failed (unreadable), or unavailable (try again later)",
    )
    thumbnail_bytes: int | None = None
    thumbnail_error: str | None = None


@router.post("/targets/{name}/uploads", response_model=UploadResponse)
async def upload_media(
    request: UploadRequest,
    name: str = TargetName,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> UploadResponse:
    """Upload one downloaded media file (and optionally its thumbnail). Waits until done."""
    target = await asyncio.to_thread(_target, name)
    path = await resolve_local_media_file(request.site, request.media_id, config)
    if path is None or not path.is_file():
        return UploadResponse(state="missing")
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    try:
        result = await asyncio.to_thread(
            object_storage.upload_file,
            _store_for(target),
            path,
            request.key,
            content_type,
            thumbnail=request.thumbnail,
            upload_original=request.upload_original,
        )
    except Exception as exc:  # noqa: BLE001 - the bucket refused or can't be reached
        raise HTTPException(status_code=502, detail=f"Upload failed: {exc}") from exc
    return UploadResponse(**vars(result))


class DeleteRequest(BaseModel):
    keys: list[str] = Field(min_length=1, max_length=1000)


class DeleteResponse(BaseModel):
    deleted: list[str]
    failed: dict[str, str]


@router.post("/targets/{name}/deletes", response_model=DeleteResponse)
async def delete_objects(request: DeleteRequest, name: str = TargetName) -> DeleteResponse:
    target = await asyncio.to_thread(_target, name)
    store = _store_for(target)

    def run() -> DeleteResponse:
        deleted: list[str] = []
        failed: dict[str, str] = {}
        for key in request.keys:
            try:
                store.delete(key)
            except Exception as exc:  # noqa: BLE001
                failed[key] = str(exc)[:500]
            else:
                deleted.append(key)
        return DeleteResponse(deleted=deleted, failed=failed)

    return await asyncio.to_thread(run)
