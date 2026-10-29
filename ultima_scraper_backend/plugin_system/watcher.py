"""Plugin file watcher — detects manifest changes on disk.

Uses a polling approach (no external dependencies like watchdog) to
detect additions, modifications, and deletions of plugin manifests.
Integrates with the PluginRegistry to trigger reloads.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
ManifestFingerprint = tuple[int, int, int]


class PluginFileWatcher:
    """Polls the plugins directory for manifest changes."""

    def __init__(
        self,
        plugins_dir: Path,
        *,
        poll_interval: float = 5.0,
        missing_grace_polls: int = 2,
        on_change: Any = None,
    ) -> None:
        self._plugins_dir = plugins_dir
        self._poll_interval = poll_interval
        self._missing_grace_polls = max(1, missing_grace_polls)
        self._on_change = on_change
        self._known: dict[str, ManifestFingerprint] = {}
        self._missing_polls: dict[str, int] = {}
        self._task: asyncio.Task[None] | None = None

    def snapshot(self) -> dict[str, ManifestFingerprint]:
        """Return plugin IDs mapped to nanosecond mtime, size, and inode."""
        result: dict[str, ManifestFingerprint] = {}
        if not self._plugins_dir.is_dir():
            return result
        for child in self._plugins_dir.iterdir():
            if not child.is_dir():
                continue
            manifest = child / "manifest.yaml"
            if not manifest.is_file():
                manifest = child / "manifest.yml"
            if not manifest.is_file():
                continue
            try:
                stat = manifest.stat()
            except FileNotFoundError:
                continue
            result[child.name] = (stat.st_mtime_ns, stat.st_size, stat.st_ino)
        return result

    def detect_changes(self) -> tuple[list[str], list[str], list[str]]:
        """Compare current disk state to last known snapshot.

        Returns (added, modified, removed) plugin id lists.
        """
        current = self.snapshot()
        added = sorted(pid for pid in current if pid not in self._known)
        removed: list[str] = []
        for plugin_id in set(self._known) - set(current):
            missing_polls = self._missing_polls.get(plugin_id, 0) + 1
            self._missing_polls[plugin_id] = missing_polls
            if missing_polls >= self._missing_grace_polls:
                removed.append(plugin_id)
                self._known.pop(plugin_id, None)
                self._missing_polls.pop(plugin_id, None)
        modified = [
            pid
            for pid in current
            if pid in self._known and current[pid] != self._known[pid]
        ]
        for plugin_id, fingerprint in current.items():
            self._known[plugin_id] = fingerprint
            self._missing_polls.pop(plugin_id, None)
        return added, sorted(modified), sorted(removed)

    async def _poll_loop(self) -> None:
        try:
            self._known = self.snapshot()
        except OSError:
            logger.exception("Could not create initial plugin manifest snapshot")
            self._known = {}
        while True:
            await asyncio.sleep(self._poll_interval)
            known_before = dict(self._known)
            missing_before = dict(self._missing_polls)
            try:
                added, modified, removed = self.detect_changes()
            except OSError:
                logger.exception("Could not read plugin manifest directory")
                continue
            if added or modified or removed:
                logger.info(
                    "Plugin changes detected: added=%s modified=%s removed=%s",
                    added,
                    modified,
                    removed,
                )
                if self._on_change is not None:
                    try:
                        result = self._on_change(added, modified, removed)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        # Do not acknowledge a filesystem transition that the
                        # platform failed to apply. Restoring the snapshots
                        # makes the next poll retry the same change, including
                        # a removal that has already passed its grace window.
                        self._known = known_before
                        self._missing_polls = missing_before
                        logger.exception("Error in plugin change callback")

    def start(self) -> None:
        """Begin polling in the current event loop."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll_loop())
            logger.info("Plugin file watcher started (interval=%.1fs)", self._poll_interval)

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Plugin file watcher stopped")
