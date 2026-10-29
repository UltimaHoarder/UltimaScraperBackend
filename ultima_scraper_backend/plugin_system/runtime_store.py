"""Durable runtime state for declarative plugin orchestration."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .repository import PluginRepository


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class WebSocketMessageRecord:
    message_id: str
    message_type: str
    action: str
    payload: dict[str, Any]
    correlation_id: str | None = None
    external_job_id: str | None = None
    acknowledged: bool = False
    timestamp: str = field(default_factory=lambda: _utcnow().isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "message_type": self.message_type,
            "action": self.action,
            "payload": self.payload,
            "correlation_id": self.correlation_id,
            "external_job_id": self.external_job_id,
            "acknowledged": self.acknowledged,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WebSocketMessageRecord:
        return cls(
            message_id=str(data["message_id"]),
            message_type=str(data["message_type"]),
            action=str(data["action"]),
            payload=dict(data.get("payload") or {}),
            correlation_id=(
                str(data["correlation_id"]) if data.get("correlation_id") is not None else None
            ),
            external_job_id=(
                str(data["external_job_id"]) if data.get("external_job_id") is not None else None
            ),
            acknowledged=bool(data.get("acknowledged", False)),
            timestamp=str(data.get("timestamp", _utcnow().isoformat())),
        )


@dataclass(slots=True)
class RuntimeCorrelationRecord:
    plugin_id: str
    correlation_id: str
    integration_id: str
    external_job_id: str | None = None
    local_job_id: str | None = None
    local_job_name: str | None = None
    status: str = "pending"
    remote_status: str | None = None
    local_status: str | None = None
    payload_hash: str = ""
    last_attempt_at: str | None = None
    last_error: str | None = None
    remote_response: dict[str, Any] | None = None
    last_outbound_command: dict[str, Any] | None = None
    last_inbound_event: dict[str, Any] | None = None
    retry_count: int = 0
    next_retry_at: str | None = None
    created_at: str = field(default_factory=lambda: _utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: _utcnow().isoformat())

    def touch(self) -> None:
        self.updated_at = _utcnow().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "correlation_id": self.correlation_id,
            "integration_id": self.integration_id,
            "external_job_id": self.external_job_id,
            "local_job_id": self.local_job_id,
            "local_job_name": self.local_job_name,
            "status": self.status,
            "remote_status": self.remote_status,
            "local_status": self.local_status,
            "payload_hash": self.payload_hash,
            "last_attempt_at": self.last_attempt_at,
            "last_error": self.last_error,
            "remote_response": self.remote_response,
            "last_outbound_command": self.last_outbound_command,
            "last_inbound_event": self.last_inbound_event,
            "retry_count": self.retry_count,
            "next_retry_at": self.next_retry_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RuntimeCorrelationRecord:
        return cls(
            plugin_id=str(data["plugin_id"]),
            correlation_id=str(data["correlation_id"]),
            integration_id=str(data.get("integration_id", "")),
            external_job_id=(
                str(data["external_job_id"]) if data.get("external_job_id") is not None else None
            ),
            local_job_id=(
                str(data["local_job_id"]) if data.get("local_job_id") is not None else None
            ),
            local_job_name=(
                str(data["local_job_name"]) if data.get("local_job_name") is not None else None
            ),
            status=str(data.get("status", "pending")),
            remote_status=(
                str(data["remote_status"]) if data.get("remote_status") is not None else None
            ),
            local_status=(
                str(data["local_status"]) if data.get("local_status") is not None else None
            ),
            payload_hash=str(data.get("payload_hash", "")),
            last_attempt_at=(
                str(data["last_attempt_at"])
                if data.get("last_attempt_at") is not None
                else None
            ),
            last_error=(
                str(data["last_error"]) if data.get("last_error") is not None else None
            ),
            remote_response=(
                dict(data["remote_response"])
                if isinstance(data.get("remote_response"), dict)
                else None
            ),
            last_outbound_command=(
                dict(data["last_outbound_command"])
                if isinstance(data.get("last_outbound_command"), dict)
                else None
            ),
            last_inbound_event=(
                dict(data["last_inbound_event"])
                if isinstance(data.get("last_inbound_event"), dict)
                else None
            ),
            retry_count=int(data.get("retry_count", 0)),
            next_retry_at=(
                str(data["next_retry_at"]) if data.get("next_retry_at") is not None else None
            ),
            created_at=str(data.get("created_at", _utcnow().isoformat())),
            updated_at=str(data.get("updated_at", _utcnow().isoformat())),
        )


@dataclass(slots=True)
class WebSocketSessionRecord:
    plugin_id: str
    url: str = ""
    connected: bool = False
    last_connected_at: str | None = None
    last_heartbeat_at: str | None = None
    reconnect_attempts: int = 0
    last_error: str | None = None
    backlog: list[WebSocketMessageRecord] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: _utcnow().isoformat())

    def touch(self) -> None:
        self.updated_at = _utcnow().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "url": self.url,
            "connected": self.connected,
            "last_connected_at": self.last_connected_at,
            "last_heartbeat_at": self.last_heartbeat_at,
            "reconnect_attempts": self.reconnect_attempts,
            "last_error": self.last_error,
            "backlog": [item.to_dict() for item in self.backlog],
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WebSocketSessionRecord:
        backlog = [
            WebSocketMessageRecord.from_dict(item)
            for item in data.get("backlog", [])
            if isinstance(item, dict)
        ]
        return cls(
            plugin_id=str(data["plugin_id"]),
            url=str(data.get("url", "")),
            connected=bool(data.get("connected", False)),
            last_connected_at=(
                str(data["last_connected_at"])
                if data.get("last_connected_at") is not None
                else None
            ),
            last_heartbeat_at=(
                str(data["last_heartbeat_at"])
                if data.get("last_heartbeat_at") is not None
                else None
            ),
            reconnect_attempts=int(data.get("reconnect_attempts", 0)),
            last_error=(
                str(data["last_error"]) if data.get("last_error") is not None else None
            ),
            backlog=backlog,
            updated_at=str(data.get("updated_at", _utcnow().isoformat())),
        )


@dataclass(slots=True)
class CallbackRegistrationRecord:
    plugin_id: str
    endpoint: str
    lifecycle_events: list[str] = field(default_factory=list)
    registered: bool = False
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_error: str | None = None
    updated_at: str = field(default_factory=lambda: _utcnow().isoformat())

    def touch(self) -> None:
        self.updated_at = _utcnow().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "endpoint": self.endpoint,
            "lifecycle_events": list(self.lifecycle_events),
            "registered": self.registered,
            "last_attempt_at": self.last_attempt_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CallbackRegistrationRecord:
        return cls(
            plugin_id=str(data["plugin_id"]),
            endpoint=str(data.get("endpoint", "")),
            lifecycle_events=[
                str(item) for item in data.get("lifecycle_events", []) if item is not None
            ],
            registered=bool(data.get("registered", False)),
            last_attempt_at=(
                str(data["last_attempt_at"]) if data.get("last_attempt_at") is not None else None
            ),
            last_success_at=(
                str(data["last_success_at"]) if data.get("last_success_at") is not None else None
            ),
            last_error=(
                str(data["last_error"]) if data.get("last_error") is not None else None
            ),
            updated_at=str(data.get("updated_at", _utcnow().isoformat())),
        )


class PluginRuntimeStore:
    """File-backed runtime records for plugin orchestration."""

    def __init__(
        self,
        storage_path: Path | None = None,
        *,
        repository: PluginRepository | None = None,
    ) -> None:
        self._storage_path = storage_path
        self._repository = repository
        self._correlations: dict[str, RuntimeCorrelationRecord] = {}
        self._sessions: dict[str, WebSocketSessionRecord] = {}
        self._registrations: dict[str, CallbackRegistrationRecord] = {}
        self._load()

    def _load(self) -> None:
        if self._repository is not None:
            return
        if self._storage_path is None or not self._storage_path.exists():
            return
        try:
            raw = json.loads(self._storage_path.read_text(encoding="utf-8"))
        except Exception:
            return
        for item in raw.get("correlations", []):
            if isinstance(item, dict):
                rec = RuntimeCorrelationRecord.from_dict(item)
                self._correlations[rec.correlation_id] = rec
        for item in raw.get("sessions", []):
            if isinstance(item, dict):
                rec = WebSocketSessionRecord.from_dict(item)
                self._sessions[rec.plugin_id] = rec
        for item in raw.get("registrations", []):
            if isinstance(item, dict):
                rec = CallbackRegistrationRecord.from_dict(item)
                self._registrations[rec.plugin_id] = rec

    def _save(self) -> None:
        if self._repository is not None:
            return
        if self._storage_path is None:
            return
        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "correlations": [rec.to_dict() for rec in self._correlations.values()],
            "sessions": [rec.to_dict() for rec in self._sessions.values()],
            "registrations": [rec.to_dict() for rec in self._registrations.values()],
        }
        tmp_path = self._storage_path.with_suffix(self._storage_path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(tmp_path, self._storage_path)

    def upsert_correlation(
        self, rec: RuntimeCorrelationRecord
    ) -> RuntimeCorrelationRecord:
        rec.touch()
        if self._repository is not None:
            self._repository.upsert_correlation_payload(rec.to_dict())
            return rec
        self._correlations[rec.correlation_id] = rec
        self._save()
        return rec

    def get_correlation(self, correlation_id: str) -> RuntimeCorrelationRecord | None:
        if self._repository is not None:
            payload = self._repository.get_correlation_payload(correlation_id)
            return RuntimeCorrelationRecord.from_dict(payload) if payload else None
        return self._correlations.get(correlation_id)

    def list_correlations(
        self, plugin_id: str | None = None
    ) -> list[RuntimeCorrelationRecord]:
        if self._repository is not None:
            return [
                RuntimeCorrelationRecord.from_dict(payload)
                for payload in self._repository.list_correlation_payloads(plugin_id)
            ]
        items = list(self._correlations.values())
        if plugin_id is not None:
            items = [item for item in items if item.plugin_id == plugin_id]
        items.sort(key=lambda item: item.created_at, reverse=True)
        return items

    def delete_correlation(self, correlation_id: str) -> bool:
        if self._repository is not None:
            return self._repository.delete_correlation(correlation_id)
        removed = self._correlations.pop(correlation_id, None)
        if removed is not None:
            self._save()
            return True
        return False

    def save_session(self, rec: WebSocketSessionRecord) -> WebSocketSessionRecord:
        rec.touch()
        if self._repository is not None:
            self._repository.upsert_session_payload(rec.to_dict())
            return rec
        self._sessions[rec.plugin_id] = rec
        self._save()
        return rec

    def get_session(self, plugin_id: str) -> WebSocketSessionRecord | None:
        if self._repository is not None:
            payload = self._repository.get_session_payload(plugin_id)
            return WebSocketSessionRecord.from_dict(payload) if payload else None
        return self._sessions.get(plugin_id)

    def save_registration(
        self, rec: CallbackRegistrationRecord
    ) -> CallbackRegistrationRecord:
        rec.touch()
        if self._repository is not None:
            self._repository.upsert_registration_payload(rec.to_dict())
            return rec
        self._registrations[rec.plugin_id] = rec
        self._save()
        return rec

    def get_registration(self, plugin_id: str) -> CallbackRegistrationRecord | None:
        if self._repository is not None:
            payload = self._repository.get_registration_payload(plugin_id)
            return CallbackRegistrationRecord.from_dict(payload) if payload else None
        return self._registrations.get(plugin_id)

    def clear_plugin(self, plugin_id: str) -> None:
        if self._repository is not None:
            self._repository.clear_plugin_runtime(plugin_id)
            return
        stale_correlation_ids = [
            correlation_id
            for correlation_id, rec in self._correlations.items()
            if rec.plugin_id == plugin_id
        ]
        for correlation_id in stale_correlation_ids:
            self._correlations.pop(correlation_id, None)
        self._sessions.pop(plugin_id, None)
        self._registrations.pop(plugin_id, None)
        self._save()
