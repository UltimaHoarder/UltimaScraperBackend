"""What this host can take on right now, judged from its own fresh measurements.

A host claims download work only when it can write to a download drive and has
memory and CPU to spare; decrypt and merge run as many processes at once as
its cores and memory allow. Nothing here asks other hosts: each one decides
for itself, so a stale view of the cluster never overloads it.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable

import psutil

from ..config import DownloadPathSetup
from .drive_selection import has_usable_download_root

logger = logging.getLogger(__name__)

# Download tasks require it; a worker offers it only while admission allows.
DOWNLOAD_CAPABILITY = "download"

_GIB = 1024**3
# Ceiling for the derived default, the old fixed value's neighbourhood: more
# processes than this contend for disks rather than finish sooner.
_MAX_MEDIA_SLOTS = 8


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Ignoring %s=%r: not an integer", name, raw)
        return default


def media_process_slots(cpu_count: int | None = None, available_bytes: int | None = None) -> int:
    """Decrypt/merge processes to run at once: one core each, leaving half the
    cores for downloads, and about 2 GiB of available memory each."""
    cpus = cpu_count if cpu_count is not None else os.cpu_count() or 1
    available = available_bytes if available_bytes is not None else psutil.virtual_memory().available
    return max(1, min(cpus // 2, available // (2 * _GIB), _MAX_MEDIA_SLOTS))


def apply_media_process_slots() -> int:
    """Size the media formatter's process limit to this host unless one is set."""
    configured = _env_int("ULTIMA_MEDIA_PROCESS_CONCURRENCY", 0)
    if configured > 0:
        return configured
    slots = media_process_slots()
    os.environ["ULTIMA_MEDIA_PROCESS_CONCURRENCY"] = str(slots)
    logger.info("Media processes sized to this host: %d at once", slots)
    return slots


class DownloadAdmission:
    """Whether this host should claim download work right now.

    The drive probe touches the filesystem, so its answer is kept for
    ``probe_seconds``; memory and CPU are read on every check.
    """

    def __init__(
        self,
        setup: Callable[[], DownloadPathSetup],
        *,
        probe_seconds: float = 30.0,
        min_available_bytes: int | None = None,
        max_cpu_percent: int | None = None,
    ) -> None:
        self._setup = setup
        self._probe_seconds = probe_seconds
        self._drive_ok = False
        self._probed_at: float | None = None
        self.min_available_bytes = (
            min_available_bytes
            if min_available_bytes is not None
            else _env_int("ULTIMA_DOWNLOAD_MIN_AVAILABLE_BYTES", _GIB)
        )
        self.max_cpu_percent = (
            max_cpu_percent
            if max_cpu_percent is not None
            else _env_int("ULTIMA_DOWNLOAD_MAX_CPU_PERCENT", 90)
        )
        # The first non-blocking reading only primes psutil's counters.
        psutil.cpu_percent(interval=None)

    def drive_reachable(self) -> bool:
        now = time.monotonic()
        if self._probed_at is None or now - self._probed_at >= self._probe_seconds:
            try:
                reachable = has_usable_download_root(self._setup())
            except Exception:
                logger.exception("Could not probe this host's download drives")
                reachable = False
            if reachable != self._drive_ok or self._probed_at is None:
                logger.info(
                    "Download drives %s on this host",
                    "reachable" if reachable else "not reachable; not claiming downloads",
                )
            self._drive_ok, self._probed_at = reachable, now
        return self._drive_ok

    def has_headroom(self) -> bool:
        if psutil.virtual_memory().available < self.min_available_bytes:
            return False
        return psutil.cpu_percent(interval=None) <= self.max_cpu_percent

    def allows_downloads(self) -> bool:
        return self.drive_reachable() and self.has_headroom()
