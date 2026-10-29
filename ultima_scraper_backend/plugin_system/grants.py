"""Persistent operator grants for plugin capabilities."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repository import PluginRepository


class PluginCapabilityGrantStore:
    """Keep requested manifest capabilities separate from operator grants."""

    def __init__(
        self,
        storage_path: Path | None = None,
        *,
        repository: PluginRepository | None = None,
    ) -> None:
        self._storage_path = storage_path
        self._repository = repository
        self._grants: dict[str, set[str]] = {}
        self._load()

    def _load(self) -> None:
        if self._repository is not None:
            return
        if self._storage_path is None or not self._storage_path.exists():
            return
        try:
            payload = json.loads(self._storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        raw_grants = payload.get("grants", {}) if isinstance(payload, dict) else {}
        if not isinstance(raw_grants, dict):
            return
        self._grants = {
            str(plugin_id): {str(value) for value in values}
            for plugin_id, values in raw_grants.items()
            if isinstance(values, list)
        }

    def _save(self) -> None:
        if self._repository is not None:
            for plugin_id, capabilities in self._grants.items():
                self._repository.replace_grants(plugin_id, capabilities)
            return
        if self._storage_path is None:
            return
        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._storage_path.with_suffix(self._storage_path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "grants": {
                        plugin_id: sorted(values)
                        for plugin_id, values in sorted(self._grants.items())
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, self._storage_path)

    def has_record(self, plugin_id: str) -> bool:
        if self._repository is not None:
            return self._repository.has_grant_record(plugin_id)
        return plugin_id in self._grants

    def get(self, plugin_id: str) -> set[str]:
        if self._repository is not None:
            return self._repository.get_grants(plugin_id)
        return set(self._grants.get(plugin_id, set()))

    def grant(self, plugin_id: str, capabilities: set[str]) -> set[str]:
        if self._repository is not None:
            return self._repository.replace_grants(plugin_id, capabilities)
        self._grants[plugin_id] = set(capabilities)
        self._save()
        return self.get(plugin_id)
