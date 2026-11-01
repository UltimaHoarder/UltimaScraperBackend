"""Where a DRM item is decrypted and merged: here, or on the backend that keeps it.

When the owner keeps the file, the bytes cross the link once either way (the
encrypted streams before decrypting there, the finished file after decrypting
here), so the transfer cancels and the choice is which side finishes the work
sooner. The owner must be clearly faster (MARGIN) and the item big enough for
a move to pay (MIN_OFFLOAD_BYTES); a rate not measured yet falls back to slot
counts alone.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

MARGIN = 1.3
MIN_OFFLOAD_BYTES = 64 * 1024 * 1024
# Weight of the newest measurement in a rate's moving average.
_SMOOTHING = 0.3


@dataclass(frozen=True)
class Capacity:
    """A host's decrypt throughput (bytes/s, None until measured) and its queue."""

    rate: float | None
    waiting: int
    slots: int

    def finish_seconds(self, size: int, rate: float) -> float:
        # Items ahead of this one, spread over the slots, then this one.
        return (self.waiting / max(1, self.slots) + 1) * size / rate


def prefer_owner(size: int, here: Capacity, owner: Capacity) -> bool:
    """Whether the owner should decrypt and merge an item of ``size`` bytes."""
    if size < MIN_OFFLOAD_BYTES:
        return False
    if here.rate is None or owner.rate is None:
        # No measured speeds yet: only move work off a full host onto a free one.
        return here.waiting >= here.slots and owner.waiting < owner.slots
    return here.finish_seconds(size, here.rate) > MARGIN * owner.finish_seconds(size, owner.rate)


class RateMeter:
    """This process's decrypt throughput and how many items it has in hand."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.rate: float | None = None
        self.active = 0

    def started(self) -> float:
        with self._lock:
            self.active += 1
        return time.monotonic()

    def finished(self, started: float, size: int, ok: bool) -> None:
        elapsed = time.monotonic() - started
        with self._lock:
            self.active = max(0, self.active - 1)
            if ok and size > 0 and elapsed > 0:
                sample = size / elapsed
                self.rate = sample if self.rate is None else (1 - _SMOOTHING) * self.rate + _SMOOTHING * sample

    def capacity(self) -> Capacity:
        slots = int(os.environ.get("ULTIMA_MEDIA_PROCESS_CONCURRENCY") or 1)
        with self._lock:
            return Capacity(rate=self.rate, waiting=max(0, self.active - slots), slots=slots)


# One per process: the download worker measures its own decrypts, the API
# process the ones it runs for other backends.
meter = RateMeter()
