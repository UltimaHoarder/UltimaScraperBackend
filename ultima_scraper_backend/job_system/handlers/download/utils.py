"""Utility functions for download task handling.

This module contains helper functions for file processing, type detection,
and data normalization used by the download handler.
"""

from __future__ import annotations

import logging
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

from ultima_scraper_backend.config import UltimaScraperCollectionConfig

from ...models import TaskExecutionError, TaskRunContext
from .types import FilesInput, MediaEntry

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def ensure_config(context: TaskRunContext) -> UltimaScraperCollectionConfig:
    """Extract and validate config from task context.

    Args:
        context: Task execution context

    Returns:
        Validated UltimaScraperCollectionConfig instance

    Raises:
        TaskExecutionError: If config is invalid
    """
    cfg = context.config
    if isinstance(cfg, UltimaScraperCollectionConfig):
        return cfg
    if cfg is not None:
        config_dict = cast(dict[str, Any], cfg)
        try:
            return UltimaScraperCollectionConfig(**config_dict)
        except Exception as exc:
            raise TaskExecutionError(
                f"Invalid config provided in context: {exc}"
            ) from exc
    return UltimaScraperCollectionConfig.load_or_create_default_config()


def normalise_files(files: FilesInput) -> list[MediaEntry]:
    """
    Normalise various input formats into a flat list of MediaEntry dicts.

    Supports:
    1. ScrapeTaskOutput format: {"content_by_auth": {"auth_name": {"media_urls": {...}}}}
    2. List[MediaEntry]: Direct list of media entries
    3. Dict[str, MediaEntry]: Mapping of IDs to media entries

    Args:
        files: Input in one of the supported formats

    Returns:
        Flat list of MediaEntry dictionaries

    Raises:
        TaskExecutionError: If input format is invalid or unsupported
    """
    result: list[MediaEntry] = []

    # Handle ScrapeTaskOutput format (nested dict from scrape handler)
    if isinstance(files, dict) and "content_by_auth" in files:
        content_by_auth = files["content_by_auth"]
        for auth_name, content_data in content_by_auth.items():
            content_dict = cast(dict[str, Any], content_data)
            media_urls_raw = content_dict.get("media_urls")
            if media_urls_raw is None:
                logger.warning(
                    f"media_urls missing for {auth_name}"
                )
                continue
            media_urls_by_type = cast(dict[str, list[MediaEntry]], media_urls_raw)

            # Iterate through content types (posts, messages, etc.)
            for _content_type, urls in media_urls_by_type.items():
                for url_entry in urls:
                    result.append(url_entry)

        return result

    # Handle list input
    if not hasattr(files, "__iter__") or isinstance(files, (str, bytes)):
        raise TaskExecutionError(f"files must be iterable, got {type(files)}")

    if isinstance(files, list):
        for entry in files:
            # Require either 'filename' or 'url' to identify the resource
            if "filename" not in entry and "url" not in entry:
                raise TaskExecutionError(
                    "Download file entry requires 'filename' or 'url'"
                )
            result.append(entry)
        return result

    # Remaining supported input is a mapping of id -> entry.
    for entry in files.values():
        if "filename" not in entry and "url" not in entry:
            raise TaskExecutionError(
                "Download file entry requires 'filename' or 'url'"
            )
        result.append(entry)
    return result


def coerce_bytes(entry: MediaEntry) -> bytes:
    """Extract bytes content from a media entry.

    Args:
        entry: Media entry dictionary

    Returns:
        Content as bytes, or empty bytes if no content

    Raises:
        TaskExecutionError: If content type is unsupported
    """
    if "content" in entry and entry.get("content") is not None:
        content = entry["content"]
    elif "data" in entry and entry.get("data") is not None:
        content = entry["data"]
    else:
        # If no content, it might be a URL-based download (handled separately)
        return b""

    content = cast(Any, content)
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    if isinstance(content, bytearray):
        return bytes(content)

    filename = entry.get("filename", "unknown")
    raise TaskExecutionError(
        f"Download entry '{filename}' has unsupported content type {type(content)!r}"
    )


def extract_filename_from_url(url: str, default: str = "download") -> str:
    """Extract filename from URL, falling back to default if not found.

    Args:
        url: URL to extract filename from
        default: Default filename if extraction fails

    Returns:
        Extracted or default filename
    """
    parsed = urlparse(url)
    path = Path(parsed.path)
    if path.name:
        return path.name
    return default


def extract_profile_filename_from_url(url: str) -> str | None:
    """Extract a unique filename from a profile media URL.

    Uses the unique hash segment from the URL path as the filename stem,
    preserving the original file extension.

    For a URL like:
        https://public.onlyfans.com/files/z/ze/zeg/zeg02uqa.../39058172/avatar.jpg
    Returns:
        "zeg02uqa....jpg"

    Args:
        url: Profile media URL.

    Returns:
        Unique filename derived from the URL path, or None if extraction fails.
    """
    parsed = urlparse(url)
    parts = Path(parsed.path).parts
    if len(parts) < 3:
        return None
    # The unique hash is two segments before the final filename
    # e.g. /files/z/ze/zeg/<hash>/<user_id>/avatar.jpg
    extension = Path(parts[-1]).suffix
    unique_hash = parts[-3]
    return f"{unique_hash}{extension}"


def detect_media_type(filename: str, content: bytes | None = None) -> str:
    """
    Detect media type from filename or content.

    Args:
        filename: Name of the file
        content: Optional file content for MIME detection

    Returns:
        One of: "images", "videos", "audio", "other"
    """
    ext = Path(filename).suffix.lower()

    # Map extensions to media types
    image_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg"}
    video_exts = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv", ".m4v", ".mpd"}
    audio_exts = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a"}

    if ext in image_exts:
        return "images"
    elif ext in video_exts:
        return "videos"
    elif ext in audio_exts:
        return "audio"

    # Try MIME type detection from content
    if content:
        mime_type, _ = mimetypes.guess_type(filename)
        if mime_type:
            if mime_type.startswith("image/"):
                return "images"
            elif mime_type.startswith("video/"):
                return "videos"
            elif mime_type.startswith("audio/"):
                return "audio"

    return "other"


def parse_created_at(value: str | int | float | None) -> datetime:
    """Parse created_at value from various formats.

    Args:
        value: ISO string, timestamp, or None

    Returns:
        Parsed datetime or current UTC time
    """
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return datetime.now(timezone.utc)


def is_url(value: str) -> bool:
    """Check if value is a valid URL.

    Args:
        value: String to check

    Returns:
        True if value is a valid URL
    """
    try:
        result = urlparse(value)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def entry_size(entry: MediaEntry, data: bytes) -> int:
    """Get size of media entry, using declared size or calculating from data.

    Args:
        entry: Media entry dictionary
        data: Actual file data

    Returns:
        Size in bytes
    """
    declared = entry.get("size")
    if isinstance(declared, int) and declared >= 0:
        return declared
    return len(data)


def build_filename(
    *,
    filename_format: Path,
    base_filename: str,
    tokens: dict[str, str],
) -> str:
    """Build filename using format template and tokens.

    Args:
        filename_format: Format template path
        base_filename: Original filename
        tokens: Token values for substitution

    Returns:
        Formatted filename

    Raises:
        TaskExecutionError: If format token is missing
    """
    formatted = str(filename_format) if filename_format else base_filename
    stem = Path(base_filename).stem or "download"
    ext = Path(base_filename).suffix.lstrip(".")
    render_tokens = {
        **tokens,
        "filename": stem,
        "ext": ext or "bin",
        "original": base_filename,
    }
    try:
        candidate = formatted.format(**render_tokens)
    except KeyError as exc:
        missing = exc.args[0]
        raise TaskExecutionError(f"Filename format missing token '{missing}'") from exc

    safe_parts = [part for part in Path(candidate).parts if part]
    if not safe_parts:
        safe_parts = [stem]
    return Path(*safe_parts).name
