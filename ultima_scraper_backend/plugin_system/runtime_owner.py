"""Cross-process ownership for plugin background supervisors.

API workers serve administration and authenticated callbacks. Dedicated plugin
supervisors use a fenced PostgreSQL lease; the file lock is a legacy test adapter.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, BinaryIO, Self

from ultima_scraper_core.processes import finish_owned

from ..database.threads import run_database_call
from .recovery import error_summary
from .watcher import PluginFileWatcher

logger = logging.getLogger(__name__)


class PluginRuntimeOwnershipLost(RuntimeError):
    """Stop all owned work before attempting a new election."""


class PluginRuntimeOwnership:
    """Hold a non-blocking advisory lock for the lifetime of a worker."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._file: BinaryIO | None = None
        self._owned = False

    @property
    def is_owner(self) -> bool:
        return self._owned

    def try_acquire(self) -> bool:
        if self._owned:
            return True
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = self.lock_path.open("a+b")
        try:
            if lock_file.seek(0, os.SEEK_END) == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError, OSError:
            lock_file.close()
            return False

        self._file = lock_file
        self._owned = True
        logger.info("Plugin runtime ownership acquired by PID %s", os.getpid())
        return True

    def release(self) -> None:
        lock_file = self._file
        if lock_file is None:
            self._owned = False
            return
        try:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()
            self._file = None
            self._owned = False
            logger.info("Plugin runtime ownership released by PID %s", os.getpid())

    def __enter__(self) -> Self:
        if not self.try_acquire():
            raise RuntimeError("Plugin runtime ownership is already held")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


class PluginRuntimeCoordinator:
    """Elect and maintain one dedicated background runtime owner."""

    def __init__(
        self,
        platform: Any,
        plugins_dir: Path,
        *,
        election_interval: float = 1.0,
        watcher_interval: float = 5.0,
    ) -> None:
        self.platform = platform
        self.plugins_dir = plugins_dir
        self.election_interval = election_interval
        self.watcher_interval = watcher_interval
        self.ownership = PluginRuntimeOwnership(
            plugins_dir / ".runtime" / "runtime-owner.lock"
        )
        self.owner_id = uuid.uuid4().hex
        if platform.repository.storage_kind == "postgresql":
            self.ownership = DatabaseRuntimeOwnership(
                platform.repository, self.owner_id
            )
        self._watcher: PluginFileWatcher | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def is_owner(self) -> bool:
        return self.ownership.is_owner

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(
                self._run(),
                name="plugin-runtime-coordinator",
            )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _on_plugin_change(
        self,
        added: list[str],
        modified: list[str],
        removed: list[str],
    ) -> None:
        summary = self.platform.handle_filesystem_changes(added, modified, removed)
        logger.info(
            "Applied plugin watcher changes: loaded=%s removed=%s",
            summary["loaded"],
            summary["removed"],
        )

    async def _run(self) -> None:
        runtime_started = False
        try:
            while not await run_database_call(self.ownership.try_acquire):
                await asyncio.sleep(self.election_interval)

            await run_database_call(
                self.platform.repository.claim_runtime_owner, self.owner_id, os.getpid()
            )

            self._watcher = PluginFileWatcher(
                self.plugins_dir,
                poll_interval=self.watcher_interval,
                on_change=self._on_plugin_change,
            )
            self._watcher.start()
            await self.platform.start_runtime()
            runtime_started = True
            logger.info("Plugin runtime coordinator is active")

            while True:
                await asyncio.sleep(self.election_interval)
                if isinstance(
                    self.ownership, DatabaseRuntimeOwnership
                ) and not await run_database_call(self.ownership.try_acquire):
                    raise PluginRuntimeOwnershipLost(
                        "Plugin supervisor lease expired or was replaced"
                    )
                if not await run_database_call(
                    self.platform.repository.heartbeat_runtime_owner, self.owner_id
                ):
                    raise PluginRuntimeOwnershipLost(
                        "Plugin runtime ownership record was replaced"
                    )
        finally:
            primary = sys.exception()
            try:
                await finish_owned(asyncio.create_task(self._cleanup(runtime_started)))
            except Exception as error:
                if primary is None:
                    raise
                logger.warning(
                    "Plugin ownership cleanup failed: %s", error_summary(error)
                )

    async def _cleanup(self, runtime_started: bool) -> None:
        try:
            if self._watcher is not None:
                await self._watcher.stop()
                self._watcher = None
        finally:
            try:
                if runtime_started or self.platform._runtime_started:
                    await self.platform.stop_runtime()
            finally:
                try:
                    await run_database_call(
                        self.platform.repository.release_runtime_owner, self.owner_id
                    )
                finally:
                    await run_database_call(self.ownership.release)


class DatabaseRuntimeOwnership:
    """A transaction-pooled, expiring lease shared across supervisor hosts."""

    def __init__(self, repository, owner):
        self.repository = repository
        self.owner = owner
        self.token = None

    @property
    def is_owner(self):
        return self.token is not None

    def try_acquire(self):
        self.token = self.repository.acquire_lease(
            "plugin-supervisor", self.owner, token=self.token
        )
        return self.token is not None

    def release(self):
        if self.token:
            self.repository.release_lease("plugin-supervisor", self.token)
        self.token = None
