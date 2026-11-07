"""Benchmark the download and DRM pipeline offline, stage by stage.

A local HTTP server with range support stands in for the CDN, serving
CENC-encrypted test media made with ffmpeg. The real DownloadManager,
_DRMPipeline, mp4dump, mp4decrypt and ffmpeg do the work; only the licence
step is skipped, since the key is known. Each stage is timed by wrapping the
functions that do it, so the pipeline itself runs unchanged.

    uv run python scripts/bench/drm_pipeline.py --staging /dev/shm --library /tmp/library

Only directories this script creates (usr-bench-<pid>-staging under --staging
and usr-bench-<pid>-library under --library) are removed afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from aiohttp import web
from ultima_scraper_api.apis.onlyfans.classes.only_drm import MediaFormatter, OnlyDRM

import ultima_scraper_backend.job_system.download_manager as download_manager_module
from ultima_scraper_backend.config import (
    DownloadPathSetup,
    StagingDirectory,
    StagingUsage,
    UltimaScraperCollectionConfig,
)
from ultima_scraper_backend.job_system.download_manager import (
    DownloadItem,
    DownloadManager,
)
from ultima_scraper_backend.job_system.handlers.download.handler import (
    _run_download_pipeline,
)
from ultima_scraper_backend.job_system.metadata_models import MediaMetadata

KID = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
KEY = "0123456789abcdef0123456789abcdef"


# ---- Test media --------------------------------------------------------------


def _ffmpeg(*args: str) -> None:
    subprocess.run(["ffmpeg", "-loglevel", "error", "-y", *args], check=True)


def ensure_media(cache: Path, seconds: int) -> dict[str, Path]:
    """Encrypted video and audio as fragmented DASH-style MP4, plus a plain file.

    Noise keeps the video near its bitrate, so sizes match real downloads.
    """
    cache.mkdir(parents=True, exist_ok=True)
    fragmented = ["-movflags", "frag_keyframe+empty_moov+default_base_moof"]
    encrypt = [
        "-encryption_scheme",
        "cenc-aes-ctr",
        "-encryption_key",
        KEY,
        "-encryption_kid",
        KID,
    ]
    video_source = [
        "-f",
        "lavfi",
        "-i",
        f"testsrc2=duration={seconds}:size=1920x1080:rate=30,noise=alls=30:allf=t",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-b:v",
        "40M",
        "-maxrate",
        "40M",
        "-bufsize",
        "80M",
        "-g",
        "60",
        "-an",
    ]
    media = {
        "video": cache / f"video-{seconds}s.enc.mp4",
        "audio": cache / f"audio-{seconds}s.enc.mp4",
        "plain": cache / f"plain-{seconds}s.mp4",
    }
    if not media["video"].exists():
        _ffmpeg(*video_source, *fragmented, *encrypt, str(media["video"]))
    if not media["audio"].exists():
        _ffmpeg(
            "-f",
            "lavfi",
            "-i",
            f"sine=duration={seconds}",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-vn",
            *fragmented,
            *encrypt,
            str(media["audio"]),
        )
    if not media["plain"].exists():
        _ffmpeg(*video_source, str(media["plain"]))
    return media


async def serve(media: dict[str, Path]) -> tuple[web.AppRunner, str]:
    """Serve /<anything>/<kind>.mp4 from the cache, with ranges, like a CDN."""

    async def file(request: web.Request) -> web.FileResponse:
        return web.FileResponse(media[request.match_info["kind"]])

    app = web.Application()
    app.router.add_get("/{name}/{kind}.mp4", file)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return runner, f"http://127.0.0.1:{port}"


# ---- Stage timing ------------------------------------------------------------


@dataclass
class Stage:
    calls: int = 0
    busy: float = 0.0
    bytes_in: int = 0
    first: float | None = None
    last: float = 0.0
    cross_device: int = 0

    def add(self, started: float, ended: float, size: int) -> None:
        self.calls += 1
        self.busy += ended - started
        self.bytes_in += size
        self.first = started if self.first is None else min(self.first, started)
        self.last = max(self.last, ended)


@dataclass
class Meter:
    stages: dict[str, Stage] = field(default_factory=lambda: defaultdict(Stage))

    def record(
        self, name: str, started: float, size: int, *, cross_device: bool = False
    ) -> None:
        stage = self.stages[name]
        stage.add(started, time.perf_counter(), size)
        stage.cross_device += cross_device


def _size(path: str | Path) -> int:
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


def instrument(meter: Meter) -> None:
    """Time each stage where it happens, leaving its behaviour alone."""
    download_part = DownloadManager.download_part

    async def timed_download_part(self, item, start_byte, end_byte, part_num):
        started = time.perf_counter()
        try:
            return await download_part(self, item, start_byte, end_byte, part_num)
        finally:
            meter.record("1 download parts", started, end_byte - start_byte + 1)

    DownloadManager.download_part = timed_download_part  # type: ignore[method-assign]

    merge_parts = download_manager_module._merge_parts_sync

    def timed_merge_parts(dest_path, part_paths, buffer_size):
        started = time.perf_counter()
        size = sum(_size(part) for part in part_paths)
        try:
            return merge_parts(dest_path, part_paths, buffer_size)
        finally:
            meter.record("2 concatenate parts", started, size)

    download_manager_module._merge_parts_sync = timed_merge_parts

    run_process = MediaFormatter.run_process
    names = {
        "mp4dump": "3 mp4dump probe",
        "mp4decrypt": "4 mp4decrypt",
        "ffprobe": "5 ffprobe",
        "ffmpeg": "5 ffmpeg mux",
    }

    async def timed_run_process(self, args, *, stdout_consumer=None):
        started = time.perf_counter()
        tool = Path(args[0]).name
        if tool == "mp4decrypt":
            size = _size(args[1])
        elif tool == "ffmpeg":
            size = sum(_size(args[i + 1]) for i, arg in enumerate(args) if arg == "-i")
        else:
            size = 0
        try:
            return await run_process(self, args, stdout_consumer=stdout_consumer)
        finally:
            meter.record(names.get(tool, tool), started, size)

    MediaFormatter.run_process = timed_run_process  # type: ignore[method-assign]

    move = shutil.move

    def timed_move(src, dst, *args, **kwargs):
        started = time.perf_counter()
        size = _size(src)
        target = Path(dst)
        parent = target if target.is_dir() else target.parent
        try:
            cross = os.stat(src).st_dev != os.stat(parent).st_dev
        except OSError:
            cross = False
        try:
            return move(src, dst, *args, **kwargs)
        finally:
            meter.record("6 move", started, size, cross_device=cross)

    shutil.move = timed_move


# ---- The run -----------------------------------------------------------------


class BenchDRM(OnlyDRM):
    """The real decrypt and merge, with the licence step answered locally."""

    def __init__(self, base_url: str, workers: int) -> None:  # skips the CDM on purpose
        self.base_url = base_url
        self.mp4decrypt_filepath = Path(shutil.which("mp4decrypt") or "mp4decrypt")
        self.mp4dump_filepath = Path(shutil.which("mp4dump") or "mp4dump")
        self.media_formatter = MediaFormatter(max_workers=workers)
        self.media_formatter.start_workers(self.media_formatter.max_workers)

    async def resolve_drm(self, drm_media: Any) -> tuple[str, str, str]:
        return (
            f"{self.base_url}/{drm_media.name}/video.mp4",
            f"{self.base_url}/{drm_media.name}/audio.mp4",
            f"{KID}:{KEY}",
        )


class _NoDatabase:
    """Accepts the stream URLs a real run records."""

    async def submit(self, *args: Any, **kwargs: Any) -> dict[str, bool]:
        return {"updated": True}


@dataclass
class _DrmMedia:
    name: str

    def get_cookies(self) -> str:
        return ""


def build_items(
    base_url: str, library: Path, drm: BenchDRM, drm_count: int, regular_count: int
) -> list[DownloadItem]:
    authed = SimpleNamespace(drm=drm, username="bench")
    user = SimpleNamespace(
        id=1, get_authed=lambda: authed, get_usernames=lambda: ["bench"]
    )
    items = []
    for media_id in range(1, drm_count + 1):
        manifest = f"{base_url}/drm-{media_id}/manifest.mpd"
        items.append(
            DownloadItem(
                url=manifest,
                filename=f"drm-{media_id}.mp4",
                directory=library,
                media_id=media_id,
                user=user,
                media_metadata=MediaMetadata(
                    media_id=media_id,
                    media_type="videos",
                    urls=[manifest],
                    content_id=media_id,
                    user_id=1,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    drm=True,
                    drm_media=_DrmMedia(f"drm-{media_id}"),
                ),
            )
        )
    for offset in range(regular_count):
        media_id = 1000 + offset
        items.append(
            DownloadItem(
                url=f"{base_url}/plain-{media_id}/plain.mp4",
                filename=f"plain-{media_id}.mp4",
                directory=library,
                media_id=media_id,
                user=user,
            )
        )
    return items


async def run(args: argparse.Namespace) -> dict[str, Any]:
    media = ensure_media(args.cache, args.seconds)
    runner, base_url = await serve(media)
    tag = f"usr-bench-{os.getpid()}"
    staging = args.staging / f"{tag}-staging"
    library = args.library / f"{tag}-library"
    meter = Meter()
    instrument(meter)
    drm = BenchDRM(base_url, args.media_workers)
    try:
        staging.mkdir(parents=True)
        library.mkdir(parents=True)
        config = UltimaScraperCollectionConfig()
        config.settings.download_setup = DownloadPathSetup(
            directory_format=Path("bench"),
            filename_format=Path("{media_id}.{ext}"),
            staging_directories=[
                StagingDirectory(
                    path=staging,
                    usage=[
                        StagingUsage.DOWNLOADS,
                        StagingUsage.DRM,
                        StagingUsage.MERGE,
                    ],
                    policy=[],
                )
            ],
        )
        manager = DownloadManager(
            config=config,
            database_writes=_NoDatabase(),
            database_write_site="onlyfans",
            database_write_scope="bench",
        )
        items = build_items(base_url, library, drm, args.drm, args.regular)
        started = time.perf_counter()
        stats = await _run_download_pipeline(manager, items)
        wall = time.perf_counter() - started
        finals = [item.get_filepath() for item in items]
        missing = [str(path) for path in finals if not path.exists()]
        final_bytes = sum(_size(path) for path in finals)
        if args.drm and not missing:
            probe = await asyncio.to_thread(
                subprocess.run,
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "stream=codec_type",
                    "-of",
                    "csv=p=0",
                    str(finals[0]),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            assert sorted(probe.stdout.split()) == ["audio", "video"], probe.stdout
        return {
            "staging": str(args.staging),
            "library": str(args.library),
            "drm_items": args.drm,
            "regular_items": args.regular,
            "wall_seconds": round(wall, 2),
            "final_bytes": final_bytes,
            "completed": stats.get("completed"),
            "failed": stats.get("failed"),
            "missing": missing,
            "stages": {
                name: {
                    "calls": stage.calls,
                    "busy_seconds": round(stage.busy, 2),
                    "span_seconds": round(stage.last - (stage.first or stage.last), 2),
                    "mb_in": round(stage.bytes_in / 1e6, 1),
                    "cross_device": stage.cross_device,
                }
                for name, stage in sorted(meter.stages.items())
            },
        }
    finally:
        await drm.media_formatter.shutdown()
        await runner.cleanup()
        shutil.rmtree(staging, ignore_errors=True)
        shutil.rmtree(library, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--staging", type=Path, required=True, help="where downloads are staged"
    )
    parser.add_argument(
        "--library", type=Path, required=True, help="where finished files go"
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path.home() / ".cache" / "usr-bench",
        help="test media",
    )
    parser.add_argument(
        "--seconds", type=int, default=60, help="length of the test media"
    )
    parser.add_argument("--drm", type=int, default=6, help="DRM items")
    parser.add_argument("--regular", type=int, default=4, help="regular items")
    parser.add_argument(
        "--media-workers",
        type=int,
        default=None,
        help="concurrent mp4decrypt/ffmpeg processes",
    )
    args = parser.parse_args()
    result = asyncio.run(run(args))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
