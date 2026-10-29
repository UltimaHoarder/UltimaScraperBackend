"""Thumbnail generation following XDG thumbnail spec.

Generates thumbnails in ``{thumbnails_dir}/large/`` (256 px max) using
ffmpeg so both video *and* image media are handled without Pillow.
"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import threading
import time
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# XDG thumbnail sizes
LARGE_MAX = 256


@contextmanager
def _thumbnail_output_lock(output_path: Path) -> Iterator[None]:
    """Serialize generation of one cached thumbnail across API workers."""

    lock_path = output_path.with_suffix(f"{output_path.suffix}.lock")
    with lock_path.open("a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            if lock_path.stat().st_size == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            lock_file.seek(0)
            while True:
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _temporary_thumbnail_path(output_path: Path) -> Path:
    """Return a worker-unique PNG path in the final output directory."""

    return output_path.with_name(
        f".{output_path.stem}.{os.getpid()}.{threading.get_ident()}.tmp.png"
    )


def _md5_uri(file_path: Path) -> str:
    """Return the MD5 hex digest of the file URI (XDG spec)."""
    uri = file_path.as_uri()
    return hashlib.md5(uri.encode()).hexdigest()


def get_thumbnail_path(
    file_path: Path,
    thumbnails_dir: Path,
) -> Path:
    """Return the expected thumbnail path for *file_path* (``large/`` bucket)."""
    name = _md5_uri(file_path.resolve()) + ".png"
    return thumbnails_dir / "large" / name


def generate_thumbnail(
    file_path: Path,
    thumbnails_dir: Path,
    *,
    is_video: bool = False,
    overwrite: bool = False,
) -> Path | None:
    """Create a thumbnail for *file_path* and return the output path.

    If the thumbnail already exists (and *overwrite* is ``False``) the
    existing path is returned immediately.

    Returns ``None`` when generation fails.
    """
    out = get_thumbnail_path(file_path, thumbnails_dir)

    if out.exists() and not overwrite:
        return out

    out.parent.mkdir(parents=True, exist_ok=True)

    temporary_out: Path | None = None
    try:
        with _thumbnail_output_lock(out):
            # Another worker may have completed the thumbnail while this one
            # was waiting for the lock.
            if out.exists() and not overwrite:
                return out

            temporary_out = _temporary_thumbnail_path(out)
            temporary_out.unlink(missing_ok=True)

            # Both video and image: ffmpeg handles both via the same pipeline.
            # For video we seek to ~1 s first; for images we just resize.
            cmd: list[str] = ["ffmpeg", "-y"]

            if is_video:
                cmd += ["-ss", "1"]

            cmd += [
                "-i",
                str(file_path),
                "-frames:v",
                "1",
                "-vf",
                (
                    f"scale=w={LARGE_MAX}:h={LARGE_MAX}"
                    ":force_original_aspect_ratio=decrease"
                ),
                "-update",
                "1",
                str(temporary_out),
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode != 0 or not temporary_out.exists():
                logger.warning(
                    "Thumbnail generation failed for %s: %s",
                    file_path,
                    result.stderr[:500],
                )
                return None

            temporary_out.replace(out)
            temporary_out = None
            return out

    except subprocess.TimeoutExpired:
        logger.warning("Thumbnail generation timed out for %s", file_path)
        return None
    except Exception:
        logger.exception("Unexpected error generating thumbnail for %s", file_path)
        return None
    finally:
        if temporary_out is not None:
            temporary_out.unlink(missing_ok=True)


def get_or_create_thumbnail(
    file_path: Path,
    thumbnails_dir: Path,
    *,
    is_video: bool = False,
) -> Path | None:
    """Return an existing thumbnail or generate one on the fly."""
    thumb = get_thumbnail_path(file_path, thumbnails_dir)
    if thumb.exists():
        return thumb
    return generate_thumbnail(file_path, thumbnails_dir, is_video=is_video)


def get_or_create_remote_thumbnail(
    source_url: str,
    cache_key: str,
    thumbnails_dir: Path,
    *,
    is_video: bool = False,
    custom_cookies: str = "",
    user_agent: str = "",
    authorization: str = "",
    proxy_url: str | None = None,
) -> Path | None:
    """Generate a stable cached thumbnail directly from a remote media URL."""

    out = get_remote_thumbnail_path(cache_key, thumbnails_dir)
    if out.exists():
        return out

    proxy_scheme = urlparse(proxy_url).scheme.lower() if proxy_url else ""
    if proxy_url and proxy_scheme not in {"http", "https"}:
        logger.warning(
            "Remote thumbnail generation skipped for %s: FFmpeg cannot safely "
            "use the assigned proxy scheme",
            cache_key,
        )
        return None

    out.parent.mkdir(parents=True, exist_ok=True)
    temporary_out: Path | None = None
    try:
        with _thumbnail_output_lock(out):
            # In-memory request deduplication is per process. Recheck after the
            # filesystem lock so only one uvicorn worker launches FFmpeg.
            if out.exists():
                return out

            temporary_out = _temporary_thumbnail_path(out)
            temporary_out.unlink(missing_ok=True)

            cmd: list[str] = ["ffmpeg", "-y"]
            if is_video:
                cmd += ["-ss", "1"]
            if proxy_url:
                cmd += ["-http_proxy", proxy_url]
            if user_agent:
                cmd += ["-user_agent", user_agent]
            request_headers: list[str] = []
            if custom_cookies:
                request_headers.append(f"Cookie: {custom_cookies}\r\n")
            if authorization:
                request_headers.append(f"Authorization: {authorization}\r\n")
            if request_headers:
                cmd += ["-headers", "".join(request_headers)]
            cmd += [
                "-i",
                source_url,
                "-frames:v",
                "1",
                "-vf",
                (
                    f"scale=w={LARGE_MAX}:h={LARGE_MAX}"
                    ":force_original_aspect_ratio=decrease"
                ),
                "-update",
                "1",
                str(temporary_out),
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0 or not temporary_out.exists():
                logger.warning(
                    "Remote thumbnail generation failed for %s: %s",
                    cache_key,
                    result.stderr[:500],
                )
                return None

            temporary_out.replace(out)
            temporary_out = None
            return out
    except subprocess.TimeoutExpired:
        logger.warning("Remote thumbnail generation timed out for %s", cache_key)
        return None
    except Exception:
        logger.exception("Unexpected remote thumbnail error for %s", cache_key)
        return None
    finally:
        if temporary_out is not None:
            temporary_out.unlink(missing_ok=True)


def get_remote_thumbnail_path(cache_key: str, thumbnails_dir: Path) -> Path:
    """Return the stable cache path for a remotely generated thumbnail."""

    digest = hashlib.md5(f"usr:{cache_key}".encode()).hexdigest()
    return thumbnails_dir / "large" / f"{digest}.png"
