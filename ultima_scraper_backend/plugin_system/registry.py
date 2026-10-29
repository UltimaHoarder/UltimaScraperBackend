"""Plugin registry — discovery, validation, lifecycle management.

Responsible for:
- Loading plugin manifests from disk (``plugins/<plugin_id>/manifest.yaml``)
- Validating manifests against the schema
- Tracking enable / disable state
- Providing the runtime lookup used by all platform APIs
- Detecting manifest changes via file-watcher integration
"""

from __future__ import annotations

import enum
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from .manifest import PluginManifest, load_manifest_from_dict

logger = logging.getLogger(__name__)


class PluginStatus(str, enum.Enum):
    """Lifecycle states for a loaded plugin."""

    ENABLED = "enabled"
    DISABLED = "disabled"
    INVALID = "invalid"
    MISSING = "missing"


class PluginRecord:
    """Runtime state for a single plugin."""

    __slots__ = (
        "manifest",
        "status",
        "manifest_path",
        "loaded_at",
        "enabled_at",
        "disabled_at",
        "last_error",
        "revision",
    )

    def __init__(
        self,
        manifest: PluginManifest,
        manifest_path: Path,
        *,
        status: PluginStatus = PluginStatus.DISABLED,
    ) -> None:
        self.manifest = manifest
        self.manifest_path = manifest_path
        self.status = status
        self.loaded_at = datetime.now(UTC)
        self.enabled_at: datetime | None = None
        self.disabled_at: datetime | None = None
        self.last_error: str | None = None
        self.revision: int = 1

    # ── Convenience helpers ───────────────────────────────────────────────

    @property
    def plugin_id(self) -> str:
        # Missing/invalid placeholders have no manifest; their directory is the ID.
        if self.manifest is None:
            return self.manifest_path.parent.name
        return self.manifest.plugin_id

    @property
    def is_active(self) -> bool:
        return self.status == PluginStatus.ENABLED

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "display_name": self.manifest.display_name,
            "plugin_version": self.manifest.plugin_version,
            "status": self.status.value,
            "capabilities": [c.value for c in self.manifest.capabilities],
            "webui": self.manifest.webui.model_dump() if self.manifest.webui else None,
            "loaded_at": self.loaded_at.isoformat(),
            "enabled_at": self.enabled_at.isoformat() if self.enabled_at else None,
            "disabled_at": self.disabled_at.isoformat() if self.disabled_at else None,
            "last_error": self.last_error,
            "revision": self.revision,
        }


class PluginRegistry:
    """In-memory registry of all known plugins."""

    def __init__(self, plugins_dir: Path, *, manifest_loader=None, is_uninstalled=None) -> None:
        self._plugins_dir = plugins_dir
        self._plugins: dict[str, PluginRecord] = {}
        self._manifest_loader = manifest_loader
        self._is_uninstalled = is_uninstalled or (lambda _plugin_id: False)

    # ── Directory scanning ────────────────────────────────────────────────

    def scan_plugins_dir(self) -> list[str]:
        """Scan the plugins directory and load/reload all manifests.

        Returns a list of plugin ids that were loaded or reloaded.
        """
        loaded: list[str] = []
        if not self._plugins_dir.is_dir():
            logger.warning("Plugins directory does not exist: %s", self._plugins_dir)
            return loaded

        for child in sorted(self._plugins_dir.iterdir()):
            if child.name.startswith('.') or not child.is_dir() or self._is_uninstalled(child.name):
                continue
            manifest_path = child / "manifest.yaml"
            if not manifest_path.is_file():
                manifest_path = child / "manifest.yml"
            if not manifest_path.is_file():
                continue

            plugin_id = child.name
            try:
                record = self._load_manifest(plugin_id, manifest_path)
                existing = self._plugins.get(plugin_id)
                if existing is not None:
                    if existing.manifest != record.manifest:
                        record.revision = existing.revision + 1
                        record.status = (
                            existing.status
                            if existing.status
                            not in {PluginStatus.INVALID, PluginStatus.MISSING}
                            else PluginStatus.DISABLED
                        )
                        logger.info(
                            "Plugin %s manifest changed (rev %d→%d)",
                            plugin_id,
                            existing.revision,
                            record.revision,
                        )
                    else:
                        if existing.status not in {
                            PluginStatus.INVALID,
                            PluginStatus.MISSING,
                        }:
                            # No change — keep existing state
                            continue
                        record.revision = existing.revision + 1
                        record.status = PluginStatus.DISABLED
                self._plugins[plugin_id] = record
                loaded.append(plugin_id)
            except Exception as exc:
                logger.error("Failed to load plugin %s: %s", plugin_id, exc)
                self._mark_invalid(plugin_id, str(exc))

        return loaded

    def _load_manifest(self, plugin_id: str, manifest_path: Path) -> PluginRecord:
        raw = manifest_path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        if not isinstance(data, dict):
            raise ValueError("Manifest must be a YAML mapping")
        manifest = load_manifest_from_dict(data)
        if manifest.plugin_id != plugin_id:
            raise ValueError(
                f"Manifest plugin_id {manifest.plugin_id!r} does not match "
                f"directory name {plugin_id!r}"
            )
        if self._manifest_loader is not None:
            manifest = self._manifest_loader(plugin_id, manifest)
        return PluginRecord(manifest, manifest_path)

    def _mark_invalid(self, plugin_id: str, error: str) -> None:
        existing = self._plugins.get(plugin_id)
        if existing is not None:
            existing.status = PluginStatus.INVALID
            existing.last_error = error
        else:
            # Create a placeholder so operators can see the error
            placeholder = PluginRecord.__new__(PluginRecord)
            placeholder.manifest = None  # type: ignore[assignment]
            placeholder.manifest_path = self._plugins_dir / plugin_id / "manifest.yaml"
            placeholder.status = PluginStatus.INVALID
            placeholder.loaded_at = datetime.now(UTC)
            placeholder.enabled_at = None
            placeholder.disabled_at = None
            placeholder.last_error = error
            placeholder.revision = 0
            self._plugins[plugin_id] = placeholder

    def mark_missing(self, plugin_id: str) -> PluginRecord:
        """Quarantine a plugin whose manifest disappeared without deleting data."""
        record = self._plugins.get(plugin_id)
        if record is None:
            record = PluginRecord.__new__(PluginRecord)
            record.manifest = None  # type: ignore[assignment]
            record.manifest_path = self._plugins_dir / plugin_id / "manifest.yaml"
            record.loaded_at = datetime.now(UTC)
            record.enabled_at = None
            record.revision = 0
            self._plugins[plugin_id] = record
        record.status = PluginStatus.MISSING
        record.disabled_at = datetime.now(UTC)
        record.last_error = "Plugin manifest is missing; runtime data was retained"
        logger.warning("Plugin %s manifest missing; plugin quarantined", plugin_id)
        return record

    # ── Reload single plugin ──────────────────────────────────────────────

    def reload_plugin(self, plugin_id: str) -> bool:
        """Reload a single plugin from disk.

        Returns True if the manifest was (re)loaded successfully.
        """
        if self._is_uninstalled(plugin_id):
            self.remove_plugin(plugin_id)
            return False
        plugin_dir = self._plugins_dir / plugin_id
        manifest_path = plugin_dir / "manifest.yaml"
        if not manifest_path.is_file():
            manifest_path = plugin_dir / "manifest.yml"
        if not manifest_path.is_file():
            self._mark_invalid(plugin_id, "manifest.yaml not found")
            return False

        try:
            record = self._load_manifest(plugin_id, manifest_path)
            existing = self._plugins.get(plugin_id)
            if existing is not None:
                record.revision = existing.revision + 1
                record.status = (
                    existing.status
                    if existing.status
                    not in {PluginStatus.INVALID, PluginStatus.MISSING}
                    else PluginStatus.DISABLED
                )
            self._plugins[plugin_id] = record
            return True
        except Exception as exc:
            self._mark_invalid(plugin_id, str(exc))
            return False

    def remove_plugin(self, plugin_id: str) -> bool:
        """Remove a single plugin from the in-memory registry."""
        removed = self._plugins.pop(plugin_id, None)
        if removed is not None:
            logger.info("Plugin %s removed from registry", plugin_id)
            return True
        return False

    # ── Lifecycle operations ──────────────────────────────────────────────

    def enable_plugin(self, plugin_id: str) -> PluginRecord:
        """Enable a plugin — makes it active immediately."""
        record = self._require(plugin_id)
        if record.status in {PluginStatus.INVALID, PluginStatus.MISSING}:
            raise ValueError(
                f"Cannot enable {record.status.value} plugin {plugin_id!r}"
            )
        record.status = PluginStatus.ENABLED
        record.enabled_at = datetime.now(UTC)
        record.disabled_at = None
        logger.info("Plugin %s enabled", plugin_id)
        return record

    def disable_plugin(self, plugin_id: str) -> PluginRecord:
        """Disable a plugin — stops future work; in-flight work finishes."""
        record = self._require(plugin_id)
        record.status = PluginStatus.DISABLED
        record.disabled_at = datetime.now(UTC)
        logger.info("Plugin %s disabled", plugin_id)
        return record

    # ── Queries ───────────────────────────────────────────────────────────

    def get(self, plugin_id: str) -> PluginRecord | None:
        return self._plugins.get(plugin_id)

    def list_all(self) -> list[PluginRecord]:
        return list(self._plugins.values())

    def list_enabled(self) -> list[PluginRecord]:
        return [r for r in self._plugins.values() if r.is_active]

    def has_capability(self, plugin_id: str, capability: str) -> bool:
        """Check whether an enabled plugin holds a given capability."""
        record = self._plugins.get(plugin_id)
        if record is None or not record.is_active:
            return False
        return any(c.value == capability for c in record.manifest.capabilities)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _require(self, plugin_id: str) -> PluginRecord:
        record = self._plugins.get(plugin_id)
        if record is None:
            raise KeyError(f"Unknown plugin {plugin_id!r}")
        return record
