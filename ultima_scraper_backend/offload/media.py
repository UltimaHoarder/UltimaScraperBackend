"""Decrypting and merging DRM items another backend hands to this one.

The sender uploads the encrypted video and audio into this backend's staging
(its preferred tier first, RAM when it has room), then asks for them to be
decrypted, merged and kept under one of this backend's drives. The streams use
the same resumable uploads as finished files. Only mp4decrypt and ffmpeg are
needed here: the key comes with the request, so no Widevine device is loaded.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from pathlib import Path

from ..config import DownloadPathSetup, StagingPolicy, StagingUsage
from ..job_system.download_manager import _staging_free_bytes
from . import store
from .placement import meter

logger = logging.getLogger(__name__)

UPLOAD_KEY = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
STREAMS = ("video", "audio")
_formatter = None


class MediaUnavailable(RuntimeError):
    """This backend cannot decrypt (mp4decrypt or ffmpeg is missing)."""


def _staging_roots(setup: DownloadPathSetup) -> list[Path]:
    """DRM staging directories, preferred tiers first, as the downloader orders them."""
    directories = [d for d in setup.staging_directories if StagingUsage.DRM in d.usage]
    directories.sort(key=lambda d: StagingPolicy.PREFER not in d.policy)
    return [Path(d.path).expanduser() for d in directories]


def stream_dir(setup: DownloadPathSetup, key: str, size: int) -> Path:
    """Where an upload's streams live: where they already are, else a tier with room."""
    if not UPLOAD_KEY.fullmatch(key):
        raise store.OffloadRejected("upload key must be 8-128 letters, digits, _ or -")
    roots = _staging_roots(setup)
    for root in roots:
        candidate = root / "offload" / key
        if candidate.exists():
            return candidate
    for directory in setup.staging_directories:
        root = Path(directory.path).expanduser()
        if StagingUsage.DRM not in directory.usage:
            continue
        # Room for the encrypted streams and their decrypted copies.
        if _staging_free_bytes(root) - directory.min_free_bytes >= 2 * size:
            return root / "offload" / key
    raise store.OffloadRejected("no staging directory here has room for these streams")


def stream_target(setup: DownloadPathSetup, key: str, name: str, size: int) -> Path:
    if name not in STREAMS:
        raise store.OffloadRejected("stream must be video or audio")
    return stream_dir(setup, key, size) / f"{name}.mp4"


def _formatter_instance():
    global _formatter
    if _formatter is None:
        from ultima_scraper_api.apis.onlyfans.classes.only_drm import MediaFormatter

        _formatter = MediaFormatter()
        _formatter.start_workers(_formatter.max_workers)
    return _formatter


async def _decrypt(stream: Path, key: str) -> Path:
    tool = shutil.which("mp4decrypt")
    if tool is None:
        raise MediaUnavailable("mp4decrypt is not installed on this backend")
    output = stream.with_name(f"{stream.stem}.dec.mp4")
    temporary = output.with_name(f"{output.name}.part")
    result = await _formatter_instance().run_process([tool, str(stream), str(temporary), "--key", key])
    if result.returncode:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"mp4decrypt failed: {result.stderr.decode(errors='replace')}")
    temporary.replace(output)
    return output


async def decrypt_and_keep(
    streams: list[Path], key: str, final: Path, mtime: float | None = None
) -> store.Committed:
    """Decrypt each stream, merge them, and put the result in place under ``final``."""
    if final.exists():
        return store.Committed(path=final, size=final.stat().st_size, existing=True)
    size = sum(stream.stat().st_size for stream in streams)
    started = meter.started()
    ok = False
    try:
        decrypted = await asyncio.gather(*(_decrypt(stream, key) for stream in streams))
        merged = streams[0].parent / f"merged{final.suffix or '.mp4'}"
        if not await _formatter_instance().merge(merged, list(decrypted)) or not merged.exists():
            raise RuntimeError("ffmpeg could not merge the decrypted streams")
        part = store.part_path(final)
        final.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.move, str(merged), str(part))
        if mtime is not None:
            os.utime(part, (mtime, mtime))
        os.replace(part, final)
        ok = True
    finally:
        meter.finished(started, size, ok)
        await asyncio.to_thread(shutil.rmtree, streams[0].parent, True)
    logger.info("Decrypted and kept %s (%d bytes of streams)", final, size)
    return store.Committed(path=final, size=final.stat().st_size, existing=False)
