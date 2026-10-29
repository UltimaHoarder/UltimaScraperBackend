"""Kernel-backed coordination for archive writers on this storage host.

Scrape and download handlers take shared leases on configured drive roots.
Restoration takes exclusive leases on the roots it touches. Leases survive
control-process restarts and are released by the kernel if a worker exits.
External processes and workers on other hosts require operator coordination.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import time
from contextlib import asynccontextmanager, ExitStack
from pathlib import Path

from platformdirs import user_data_dir

LOCK_ROOT = Path(user_data_dir("ultimascraper")) / "archive_writer_locks"


def acquire(roots: list[str], *, exclusive: bool, wait: bool = False, progress=None) -> ExitStack:
    stack = ExitStack()
    try:
        LOCK_ROOT.mkdir(parents=True, exist_ok=True)
        names = sorted({str(Path(root).resolve()) for root in roots})
        if exclusive:
            # Announce exclusive intent before draining existing readers, so
            # continuous new tasks cannot starve restoration indefinitely.
            for root in names:
                key = hashlib.sha256(root.encode()).hexdigest()
                intent = stack.enter_context((LOCK_ROOT / (key + ".intent.lock")).open("a"))
                fcntl.flock(intent, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for root in names:
            key = hashlib.sha256(root.encode()).hexdigest()
            intent = None
            if not exclusive:
                intent = stack.enter_context((LOCK_ROOT / (key + ".intent.lock")).open("a"))
                try:
                    fcntl.flock(intent, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BaseException:
                    intent.close()
                    raise
            handle = stack.enter_context((LOCK_ROOT / (key + ".lock")).open("a"))
            try:
                while True:
                    try:
                        fcntl.flock(handle, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if not exclusive or not wait:
                            raise
                        if progress:
                            progress(f"Waiting for current archive writers to finish: {root}")
                        time.sleep(1)
            finally:
                if intent is not None:
                    intent.close()
        return stack
    except BaseException:
        stack.close()
        raise


@asynccontextmanager
async def writer_guard(config):
    setup = getattr(getattr(config, "settings", None), "download_setup", None)
    roots = [str(root) for drive in setup.drives for root in drive.directories] if setup else []
    while True:
        try:
            leases = acquire(roots, exclusive=False) if roots else ExitStack()
            break
        except BlockingIOError:
            await asyncio.sleep(1)
    try:
        yield
    finally:
        leases.close()
