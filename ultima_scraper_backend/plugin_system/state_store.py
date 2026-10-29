"""Plugin state store — durable per-plugin key-value storage.

Rules:
- Normal key-value with overwrite semantics.
- Values are small JSON-like scalars or objects.
- Per-plugin total quota enforcement.
- Operators can list, read, delete, edit, and view history.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .repository import PluginRepository


class StateEntry:
    """A single stored value."""

    __slots__ = ("key", "value", "updated_at", "updated_by", "reason")

    def __init__(
        self,
        key: str,
        value: Any,
        *,
        updated_by: str = "plugin",
        reason: str = "",
        updated_at: datetime | None = None,
    ) -> None:
        self.key = key
        self.value = value
        self.updated_at = updated_at or datetime.now(UTC)
        self.updated_by = updated_by
        self.reason = reason

    def size_bytes(self) -> int:
        return len(json.dumps(self.value, separators=(",", ":")).encode())

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "updated_at": self.updated_at.isoformat(),
            "updated_by": self.updated_by,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StateEntry:
        updated_at_raw = data.get("updated_at")
        updated_at = (
            datetime.fromisoformat(updated_at_raw)
            if isinstance(updated_at_raw, str)
            else None
        )
        return cls(
            key=str(data["key"]),
            value=data.get("value"),
            updated_by=str(data.get("updated_by", "plugin")),
            reason=str(data.get("reason", "")),
            updated_at=updated_at,
        )


class AuditEntry:
    __slots__ = ("key", "old_value", "new_value", "actor", "reason", "timestamp")

    def __init__(
        self,
        key: str,
        old_value: Any,
        new_value: Any,
        actor: str,
        reason: str,
        *,
        timestamp: datetime | None = None,
    ) -> None:
        self.key = key
        self.old_value = old_value
        self.new_value = new_value
        self.actor = actor
        self.reason = reason
        self.timestamp = timestamp or datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "actor": self.actor,
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AuditEntry:
        timestamp_raw = data.get("timestamp")
        timestamp = (
            datetime.fromisoformat(timestamp_raw)
            if isinstance(timestamp_raw, str)
            else None
        )
        return cls(
            key=str(data["key"]),
            old_value=data.get("old_value"),
            new_value=data.get("new_value"),
            actor=str(data.get("actor", "plugin")),
            reason=str(data.get("reason", "")),
            timestamp=timestamp,
        )


class PluginStateStore:
    """Durable per-plugin key-value state store with quota enforcement."""

    def __init__(
        self,
        quota_bytes: int = 65536,
        storage_path: Path | None = None,
        *,
        repository: PluginRepository | None = None,
    ) -> None:
        self._stores: dict[str, dict[str, StateEntry]] = {}
        self._quotas: dict[str, int] = {}
        self._default_quota = quota_bytes
        self._audit_log: dict[str, list[AuditEntry]] = {}
        self._storage_path = storage_path
        self._repository = repository
        self._load()

    def _load(self) -> None:
        if self._repository is not None:
            return
        if self._storage_path is None or not self._storage_path.exists():
            return
        try:
            raw = json.loads(self._storage_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load plugin state from %s: %s", self._storage_path, exc)
            return
        if not isinstance(raw, dict):
            return
        self._quotas = {
            str(plugin_id): int(quota)
            for plugin_id, quota in (raw.get("quotas") or {}).items()
        }
        stores: dict[str, dict[str, StateEntry]] = {}
        for plugin_id, entries in (raw.get("stores") or {}).items():
            if not isinstance(entries, dict):
                continue
            parsed_entries: dict[str, StateEntry] = {}
            for key, entry_data in entries.items():
                if not isinstance(entry_data, dict):
                    continue
                try:
                    parsed_entries[str(key)] = StateEntry.from_dict(entry_data)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Skipping invalid plugin state entry %s/%s: %s", plugin_id, key, exc)
            stores[str(plugin_id)] = parsed_entries
        self._stores = stores

        audits: dict[str, list[AuditEntry]] = {}
        for plugin_id, entries in (raw.get("audit_log") or {}).items():
            if not isinstance(entries, list):
                continue
            audits[str(plugin_id)] = []
            for entry_data in entries:
                if not isinstance(entry_data, dict):
                    continue
                try:
                    audits[str(plugin_id)].append(AuditEntry.from_dict(entry_data))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Skipping invalid plugin audit entry for %s: %s", plugin_id, exc)
        self._audit_log = audits

    def _save(self) -> None:
        if self._repository is not None:
            return
        if self._storage_path is None:
            return
        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "quotas": self._quotas,
            "stores": {
                plugin_id: {
                    key: entry.to_dict()
                    for key, entry in entries.items()
                }
                for plugin_id, entries in self._stores.items()
            },
            "audit_log": {
                plugin_id: [entry.to_dict() for entry in entries]
                for plugin_id, entries in self._audit_log.items()
            },
        }
        tmp_path = self._storage_path.with_suffix(self._storage_path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(tmp_path, self._storage_path)

    def set_quota(self, plugin_id: str, quota_bytes: int) -> None:
        if self._repository is not None:
            self._repository.set_quota(plugin_id, quota_bytes)
            return
        self._quotas[plugin_id] = quota_bytes
        self._save()

    def _quota_for(self, plugin_id: str) -> int:
        if self._repository is not None:
            return self._repository.get_quota(plugin_id, self._default_quota)
        return self._quotas.get(plugin_id, self._default_quota)

    def _total_size(self, plugin_id: str) -> int:
        if self._repository is not None:
            return self._repository.total_state_size(plugin_id)
        store = self._stores.get(plugin_id, {})
        return sum(e.size_bytes() for e in store.values())

    def get(self, plugin_id: str, key: str) -> Any | None:
        if self._repository is not None:
            payload = self._repository.get_state_payload(plugin_id, key)
            if payload is None:
                return None
            return StateEntry.from_dict(payload).value
        store = self._stores.get(plugin_id, {})
        entry = store.get(key)
        return entry.value if entry is not None else None

    def set(
        self,
        plugin_id: str,
        key: str,
        value: Any,
        *,
        actor: str = "plugin",
        reason: str = "",
    ) -> None:
        if self._repository is not None:
            new_entry = StateEntry(key, value, updated_by=actor, reason=reason)

            def _audit(
                old_payload: dict[str, Any] | None,
            ) -> dict[str, Any]:
                old_value = (
                    StateEntry.from_dict(old_payload).value
                    if old_payload is not None
                    else None
                )
                return AuditEntry(
                    key,
                    old_value,
                    value,
                    actor,
                    reason,
                ).to_dict()

            self._repository.set_state_payload(
                plugin_id,
                key,
                new_entry.to_dict(),
                size_bytes=new_entry.size_bytes(),
                quota_bytes=self._quota_for(plugin_id),
                audit_factory=_audit,
            )
            return
        store = self._stores.setdefault(plugin_id, {})
        old_entry = store.get(key)
        old_value = old_entry.value if old_entry is not None else None

        new_entry = StateEntry(key, value, updated_by=actor, reason=reason)
        new_size = new_entry.size_bytes()
        current_total = self._total_size(plugin_id)
        if old_entry is not None:
            current_total -= old_entry.size_bytes()
        if current_total + new_size > self._quota_for(plugin_id):
            raise ValueError(
                f"State quota exceeded for plugin {plugin_id!r} "
                f"({current_total + new_size} > {self._quota_for(plugin_id)})"
            )
        store[key] = new_entry

        self._audit_log.setdefault(plugin_id, []).append(
            AuditEntry(key, old_value, value, actor, reason)
        )
        self._save()

    def delete(
        self,
        plugin_id: str,
        key: str,
        *,
        actor: str = "plugin",
        reason: str = "",
    ) -> bool:
        if self._repository is not None:
            def _audit(old_payload: dict[str, Any]) -> dict[str, Any]:
                old_entry = StateEntry.from_dict(old_payload)
                return AuditEntry(
                    key,
                    old_entry.value,
                    None,
                    actor,
                    reason,
                ).to_dict()

            return self._repository.delete_state_payload(
                plugin_id,
                key,
                audit_factory=_audit,
            )
        store = self._stores.get(plugin_id, {})
        entry = store.pop(key, None)
        if entry is None:
            return False
        self._audit_log.setdefault(plugin_id, []).append(
            AuditEntry(key, entry.value, None, actor, reason)
        )
        self._save()
        return True

    def list_keys(self, plugin_id: str) -> list[str]:
        if self._repository is not None:
            return [
                StateEntry.from_dict(payload).key
                for payload in self._repository.list_state_payloads(plugin_id)
            ]
        store = self._stores.get(plugin_id, {})
        return list(store.keys())

    def list_entries(self, plugin_id: str) -> list[dict[str, Any]]:
        if self._repository is not None:
            return self._repository.list_state_payloads(plugin_id)
        store = self._stores.get(plugin_id, {})
        return [e.to_dict() for e in store.values()]

    def get_audit_log(
        self, plugin_id: str, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        if self._repository is not None:
            return self._repository.list_state_audit(plugin_id, limit)
        entries = self._audit_log.get(plugin_id, [])
        return [e.to_dict() for e in entries[-limit:]]

    def total_size_bytes(self, plugin_id: str) -> int:
        return self._total_size(plugin_id)

    def clear_plugin(self, plugin_id: str) -> None:
        if self._repository is not None:
            self._repository.clear_plugin_state(plugin_id)
            return
        self._stores.pop(plugin_id, None)
        self._audit_log.pop(plugin_id, None)
        self._quotas.pop(plugin_id, None)
        self._save()
