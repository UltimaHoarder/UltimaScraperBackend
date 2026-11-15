"""Space running downloads still expect to write, per drive, on this host.

A download task reserves its estimate on the drive it picked, counts down what
it has written as batches finish, and releases the rest when it ends. Every
room check on this host subtracts the other tasks' remaining reservations from
a drive's free space, so two tasks that each fit alone can't overfill a drive
together. Worker processes and the API share the reservations through a locked
file. One not renewed for EXPIRY_SECONDS is dropped, so a task that crashed
can't hold space.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

EXPIRY_SECONDS = 3600.0


def _path() -> Path:
    configured = os.environ.get("ULTIMA_DRIVE_RESERVATIONS_FILE")
    if configured:
        return Path(configured)
    from platformdirs import user_cache_dir

    return Path(user_cache_dir("ultima_scraper_verse")) / "drive_reservations.json"


@contextmanager
def _locked() -> Iterator[dict[str, dict[str, Any]]]:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_name(f".{path.name}.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            reservations = json.loads(path.read_text() or "{}")
        except (OSError, ValueError):
            reservations = {}
        now = time.time()
        reservations = {
            key: entry
            for key, entry in reservations.items()
            if now - float(entry.get("touched", 0)) < EXPIRY_SECONDS
        }
        before = json.dumps(reservations, sort_keys=True)
        yield reservations
        if json.dumps(reservations, sort_keys=True) != before:
            temporary = path.with_name(f".{path.name}.{os.getpid()}")
            temporary.write_text(json.dumps(reservations))
            temporary.replace(path)


@contextmanager
def choosing() -> Iterator[None]:
    """Held while a task picks its drive and reserves on it, so tasks starting
    together each see the others' reservations."""
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_name(f".{path.name}.choose.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def reserve(key: str, root: Path | str, size: int) -> None:
    """Hold ``size`` bytes on ``root`` for ``key``, replacing what it held before."""
    with _locked() as reservations:
        if size > 0:
            reservations[key] = {"root": str(root), "bytes": int(size), "touched": time.time()}
        else:
            reservations.pop(key, None)


def consume(key: str, written: int) -> None:
    """``written`` of the reservation is now on disk, where free space shows it."""
    with _locked() as reservations:
        entry = reservations.get(key)
        if entry is None:
            return
        entry["bytes"] = max(0, int(entry["bytes"]) - max(0, int(written)))
        entry["touched"] = time.time()


def release(key: str) -> None:
    with _locked() as reservations:
        reservations.pop(key, None)


def reserved_on(root: Path | str, *, exclude: str | None = None) -> int:
    """Bytes the other tasks on this host still expect to write to ``root``."""
    root = str(root)
    with _locked() as reservations:
        return sum(
            int(entry["bytes"])
            for key, entry in reservations.items()
            if entry.get("root") == root and key != exclude
        )
