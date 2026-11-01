"""Files another backend hands to this one to keep on its download drives.

A sender uploads a finished file straight into its destination folder as a
hidden ``.part`` beside the final name, resuming from whatever already
arrived, then commits it: a size check and a rename, so a file is never
copied twice and never appears half-written under its final name.

Every path is checked here: the root must be one of this backend's configured
download directories and the file must resolve inside it.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..config import DownloadPathSetup
from ..job_system.drive_selection import _flatten_drive_directories

PART_SUFFIX = ".offload.part"
CHUNK_BYTES = 1024 * 1024


class OffloadRejected(ValueError):
    """The request names a place this backend will not write to."""


@dataclass(frozen=True)
class UploadState:
    received: int
    exists: bool
    size: int | None


@dataclass(frozen=True)
class Committed:
    path: Path
    size: int
    existing: bool


def _download_roots(setup: DownloadPathSetup) -> set[Path]:
    return {Path(drive_dir.path).expanduser().resolve() for drive_dir in _flatten_drive_directories(setup)}


def resolve_target(setup: DownloadPathSetup, root: str, relative: str) -> Path:
    """The final path for ``relative`` under ``root``, or OffloadRejected."""
    root_path = Path(root).expanduser().resolve()
    if root_path not in _download_roots(setup):
        raise OffloadRejected("root is not one of this backend's download directories")
    parts = PurePosixPath(relative).parts
    if not parts or PurePosixPath(relative).is_absolute() or any(part in {"", ".", ".."} for part in parts):
        raise OffloadRejected("path must be relative and stay inside the root")
    target = (root_path / Path(*parts)).resolve()
    if not target.is_relative_to(root_path) or target == root_path:
        raise OffloadRejected("path must be relative and stay inside the root")
    if target.name.endswith(PART_SUFFIX):
        raise OffloadRejected("path names an upload in progress")
    return target


def part_path(target: Path) -> Path:
    return target.with_name(f".{target.name}{PART_SUFFIX}")


def upload_state(target: Path) -> UploadState:
    part = part_path(target)
    received = part.stat().st_size if part.exists() else 0
    exists = target.exists()
    return UploadState(received=received, exists=exists, size=target.stat().st_size if exists else None)


def _free_bytes(path: Path) -> int:
    for candidate in (path, *path.parents):
        if candidate.exists():
            return shutil.disk_usage(candidate).free
    return 0


def open_part(target: Path, offset: int, incoming: int | None) -> object:
    """A binary handle positioned at ``offset``, with anything after it dropped.

    ``offset`` may not be past what already arrived, so a lost request resumes
    rather than leaving a gap; ``incoming`` (the request's length) must fit.
    """
    part = part_path(target)
    received = part.stat().st_size if part.exists() else 0
    if offset < 0 or offset > received:
        raise OffloadRejected(f"offset {offset} does not continue the {received} bytes received")
    if incoming is not None and incoming > _free_bytes(target.parent):
        raise OffloadRejected("not enough free space on the destination drive")
    target.parent.mkdir(parents=True, exist_ok=True)
    handle = open(part, "r+b" if part.exists() else "w+b")
    handle.truncate(offset)
    handle.seek(offset)
    return handle


def commit(target: Path, size: int, mtime: float | None = None) -> Committed:
    """Rename a complete upload into place; an existing file is kept, not replaced.

    ``mtime`` carries the timestamp the sender gave the file (the post date).
    """
    part = part_path(target)
    if target.exists():
        part.unlink(missing_ok=True)
        return Committed(path=target, size=target.stat().st_size, existing=True)
    received = part.stat().st_size if part.exists() else 0
    if received != size:
        raise OffloadRejected(f"received {received} of {size} bytes")
    if mtime is not None:
        os.utime(part, (mtime, mtime))
    os.replace(part, target)
    return Committed(path=target, size=size, existing=False)
