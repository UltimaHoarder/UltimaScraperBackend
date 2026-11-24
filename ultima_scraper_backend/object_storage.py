"""Object storage targets: copy downloaded media into S3-compatible buckets.

A target is a named bucket (endpoint, bucket, key id, secret key) kept in the
archive's settings table with its keys encrypted. Whoever holds the admin token
adds targets, uploads one media item at a time under a key they choose
(optionally with a WebP thumbnail at a second key), and deletes keys they no
longer want. Only files already on this backend's disk are uploaded; nothing is
fetched from a site for it.

Uploads use boto3 (multipart for large files). Thumbnails use ffmpeg/ffprobe
from PATH for images and videos alike: a video's frame is taken at about 10% of
its length. Without those tools the upload still happens and the thumbnail is
reported as ``unavailable``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

SETTINGS_SCOPE = "object-storage"
THUMB_CONTENT_TYPE = "image/webp"
FRAME_POSITION = 0.10
TOOL_TIMEOUT_SECONDS = 120
# B2 and S3 need parts of at least 5 MB; 64 MB keeps big videos to a few hundred parts.
PART_SIZE = 64 * 1024**2


class StorageTarget(BaseModel):
    endpoint: str = Field(description="S3 API endpoint, e.g. https://s3.us-west-004.backblazeb2.com")
    bucket: str
    key_id: str
    secret_key: str
    region: str | None = None

    def public(self) -> dict[str, Any]:
        return {"endpoint": self.endpoint, "bucket": self.bucket, "key_id": self.key_id, "region": self.region}


# ---------------------------------------------------------------------- targets


def _settings() -> Any:
    from .services.settings_service import SettingsService

    return SettingsService()


def save_target(name: str, target: StorageTarget) -> None:
    _settings().set(SETTINGS_SCOPE, name, target.model_dump(), is_secret=True)


def load_target(name: str) -> StorageTarget | None:
    service = _settings()
    # Another API worker may have changed it: read the table, not this process's cache.
    service.invalidate()
    value = service.get(SETTINGS_SCOPE, name, unmask=True, as_json=True)
    return StorageTarget.model_validate(value) if value else None


def delete_target(name: str) -> bool:
    return bool(_settings().delete(SETTINGS_SCOPE, name))


def list_targets() -> dict[str, dict[str, Any]]:
    service = _settings()
    service.invalidate()
    return {
        name: StorageTarget.model_validate(json.loads(str(raw))).public()
        for name, raw in service.get_all(SETTINGS_SCOPE, unmask=True).items()
    }


# ---------------------------------------------------------------------- buckets


class ObjectStore(Protocol):
    def upload(self, key: str, path: str, content_type: str) -> None: ...

    def delete(self, key: str) -> None: ...


class S3Store:
    def __init__(self, target: StorageTarget) -> None:
        import boto3
        from boto3.s3.transfer import TransferConfig

        self._bucket = target.bucket
        self._client: Any = boto3.client(
            "s3",
            endpoint_url=target.endpoint,
            region_name=target.region,
            aws_access_key_id=target.key_id,
            aws_secret_access_key=target.secret_key,
        )
        self._transfer = TransferConfig(multipart_threshold=PART_SIZE, multipart_chunksize=PART_SIZE)

    def upload(self, key: str, path: str, content_type: str) -> None:
        self._client.upload_file(path, self._bucket, key, ExtraArgs={"ContentType": content_type}, Config=self._transfer)

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)


# ---------------------------------------------------------------------- thumbnails


class ThumbnailToolMissing(RuntimeError):
    """ffmpeg or ffprobe isn't on PATH."""


@dataclass(frozen=True)
class MediaInfo:
    """The original's display size (after rotation) and, for videos, its length."""

    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None


def _number(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def parse_probe(data: dict[str, Any]) -> MediaInfo:
    """Size and length from ``ffprobe -of json``; a quarter-turned video swaps its size."""
    streams = [s for s in data.get("streams") or [] if s.get("codec_type") == "video"]
    if not streams:
        return MediaInfo(duration_seconds=_number((data.get("format") or {}).get("duration")))
    stream = streams[0]
    width, height = _number(stream.get("width")), _number(stream.get("height"))
    rotation = 0.0
    for side in stream.get("side_data_list") or []:
        if "rotation" in side:
            rotation = float(side["rotation"])
    rotation = _number(stream.get("tags", {}).get("rotate")) or rotation
    if width and height and round(abs(rotation)) % 180 == 90:
        width, height = height, width
    duration = _number(stream.get("duration")) or _number((data.get("format") or {}).get("duration"))
    return MediaInfo(
        width=int(width) if width else None,
        height=int(height) if height else None,
        duration_seconds=duration,
    )


class FfmpegThumbnails:
    """A WebP no wider than ``width`` and no taller than ``max_height``, never enlarged."""

    def __init__(self, *, ffmpeg: str | None = None, ffprobe: str | None = None) -> None:
        self._ffmpeg = ffmpeg or shutil.which("ffmpeg")
        self._ffprobe = ffprobe or shutil.which("ffprobe")

    def available(self) -> bool:
        return bool(self._ffmpeg and self._ffprobe)

    def probe(self, source: str) -> MediaInfo:
        assert self._ffprobe
        result = subprocess.run(
            [self._ffprobe, "-v", "error", "-show_streams", "-show_format", "-of", "json", source],
            capture_output=True,
            text=True,
            timeout=TOOL_TIMEOUT_SECONDS,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"ffprobe failed: {result.stderr.strip()[-500:]}")
        return parse_probe(json.loads(result.stdout or "{}"))

    def _render(self, source: str, dest: str, *, width: int, max_height: int, at_seconds: float | None) -> None:
        assert self._ffmpeg
        scale = (
            f"scale=w='min({width},iw)':h='min({max_height},ih)'"
            ":force_original_aspect_ratio=decrease"
        )
        command = [self._ffmpeg, "-v", "error", "-y"]
        if at_seconds is not None:
            command += ["-ss", f"{max(at_seconds, 0.0):.3f}"]
        command += ["-i", source, "-frames:v", "1", "-vf", scale, "-c:v", "libwebp", "-quality", "80", dest]
        result = subprocess.run(command, capture_output=True, text=True, timeout=TOOL_TIMEOUT_SECONDS, check=False)
        if result.returncode != 0 or not os.path.exists(dest) or os.path.getsize(dest) == 0:
            raise RuntimeError(f"ffmpeg made no thumbnail: {result.stderr.strip()[-500:]}")

    def make(self, source: str, content_type: str, dest: str, *, width: int, max_height: int) -> MediaInfo | None:
        """Write the thumbnail to ``dest`` and return the original's size, or None for other types."""
        if not content_type.startswith(("image/", "video/")):
            return None
        if not self.available():
            raise ThumbnailToolMissing("ffmpeg and ffprobe must be on PATH for thumbnails")
        info = self.probe(source)
        if content_type.startswith("image/"):
            self._render(source, dest, width=width, max_height=max_height, at_seconds=None)
            return MediaInfo(width=info.width, height=info.height)
        at = (info.duration_seconds or 0.0) * FRAME_POSITION
        try:
            self._render(source, dest, width=width, max_height=max_height, at_seconds=at)
        except RuntimeError:
            if at <= 0:
                raise
            # Some files report a length they can't seek into; the first frame will do.
            self._render(source, dest, width=width, max_height=max_height, at_seconds=0.0)
        return info


class Thumbnails(Protocol):
    def make(self, source: str, content_type: str, dest: str, *, width: int, max_height: int) -> MediaInfo | None: ...


# ---------------------------------------------------------------------- uploads


class ThumbnailRequest(BaseModel):
    key: str = Field(min_length=1, max_length=1024)
    width: int = Field(default=480, ge=16, le=4096)
    max_height: int | None = Field(default=None, ge=16, le=16384, description="Defaults to 4 x width")


ThumbnailState = Literal["ready", "none", "failed", "unavailable"]


@dataclass
class UploadResult:
    state: Literal["uploaded", "missing"]
    size_bytes: int = 0
    content_type: str | None = None
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None
    thumbnail_state: ThumbnailState | None = None
    thumbnail_bytes: int | None = None
    thumbnail_error: str | None = None


def upload_file(
    store: ObjectStore,
    path: Path,
    key: str,
    content_type: str,
    *,
    thumbnail: ThumbnailRequest | None = None,
    thumbnails: Thumbnails | None = None,
    upload_original: bool = True,
) -> UploadResult:
    """Upload ``path`` as ``key`` and, if asked, its thumbnail. Upload errors raise.

    ``upload_original=False`` makes only the thumbnail (for an original already there).
    """
    if upload_original:
        store.upload(key, str(path), content_type)
    result = UploadResult(state="uploaded", size_bytes=path.stat().st_size, content_type=content_type)
    if thumbnail is None:
        return result
    tools = thumbnails or FfmpegThumbnails()
    with tempfile.TemporaryDirectory(prefix="usr-thumb-") as tmp:
        dest = os.path.join(tmp, "thumb.webp")
        try:
            info = tools.make(
                str(path),
                content_type,
                dest,
                width=thumbnail.width,
                max_height=thumbnail.max_height or thumbnail.width * 4,
            )
        except ThumbnailToolMissing as error:
            result.thumbnail_state, result.thumbnail_error = "unavailable", str(error)
            return result
        except Exception as error:  # noqa: BLE001 - a bad file still has its upload
            logger.warning("Thumbnail of %s failed: %s", key, error)
            result.thumbnail_state, result.thumbnail_error = "failed", str(error)[:2000]
            return result
        if info is None:
            result.thumbnail_state = "none"
            return result
        result.width, result.height, result.duration_seconds = info.width, info.height, info.duration_seconds
        try:
            store.upload(thumbnail.key, dest, THUMB_CONTENT_TYPE)
        except Exception as error:  # noqa: BLE001 - the original is up; the caller can ask again
            logger.warning("Upload of the thumbnail of %s failed: %s", key, error)
            result.thumbnail_state, result.thumbnail_error = "unavailable", f"Thumbnail upload failed: {error}"[:2000]
            return result
        result.thumbnail_state, result.thumbnail_bytes = "ready", os.path.getsize(dest)
    return result
