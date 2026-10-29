"""Remote plugin submission correlation model."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .runtime_store import RuntimeCorrelationRecord

_SENSITIVE_RUNTIME_KEYS = frozenset({"auth_bundle"})


def _sanitize_runtime_value(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if key in _SENSITIVE_RUNTIME_KEYS:
                sanitized[key] = {"redacted": True}
                continue
            sanitized[key] = _sanitize_runtime_value(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_runtime_value(item) for item in value]
    return value


class RemoteSubmissionRecord:
    """Durable record of one remote correlation/execution flow."""

    __slots__ = (
        "correlation_id",
        "plugin_id",
        "integration_id",
        "external_job_id",
        "local_job_id",
        "local_job_name",
        "status",
        "payload_hash",
        "remote_status",
        "local_status",
        "created_at",
        "updated_at",
        "last_attempt_at",
        "retry_count",
        "next_retry_at",
        "last_error",
        "remote_response",
        "last_outbound_command",
        "last_inbound_event",
    )

    def __init__(
        self,
        *,
        correlation_id: str,
        plugin_id: str,
        integration_id: str,
        payload_hash: str,
    ) -> None:
        self.correlation_id = correlation_id
        self.plugin_id = plugin_id
        self.integration_id = integration_id
        self.external_job_id: str | None = None
        self.local_job_id: str | None = None
        self.local_job_name: str | None = None
        self.status: str = "pending"
        self.payload_hash = payload_hash
        self.created_at: str = datetime.now(UTC).isoformat()
        self.updated_at: str = self.created_at
        self.remote_status: str | None = None
        self.local_status: str | None = None
        self.last_attempt_at: str | None = None
        self.retry_count: int = 0
        self.next_retry_at: str | None = None
        self.last_error: str | None = None
        self.remote_response: dict[str, Any] | None = None
        self.last_outbound_command: dict[str, Any] | None = None
        self.last_inbound_event: dict[str, Any] | None = None

    def touch(self) -> None:
        self.updated_at = datetime.now(UTC).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlation_id": self.correlation_id,
            "plugin_id": self.plugin_id,
            "integration_id": self.integration_id,
            "external_job_id": self.external_job_id,
            "local_job_id": self.local_job_id,
            "local_job_name": self.local_job_name,
            "status": self.status,
            "payload_hash": self.payload_hash,
            "remote_status": self.remote_status,
            "local_status": self.local_status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_attempt_at": self.last_attempt_at,
            "retry_count": self.retry_count,
            "next_retry_at": self.next_retry_at,
            "last_error": self.last_error,
            "remote_response": _sanitize_runtime_value(self.remote_response),
            "last_outbound_command": _sanitize_runtime_value(
                self.last_outbound_command
            ),
            "last_inbound_event": _sanitize_runtime_value(self.last_inbound_event),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RemoteSubmissionRecord:
        rec = cls(
            correlation_id=data["correlation_id"],
            plugin_id=data["plugin_id"],
            integration_id=data["integration_id"],
            payload_hash=data.get("payload_hash", ""),
        )
        rec.external_job_id = data.get("external_job_id")
        rec.local_job_id = data.get("local_job_id")
        rec.local_job_name = data.get("local_job_name")
        rec.status = data.get("status", "pending")
        rec.created_at = data.get("created_at", rec.created_at)
        rec.updated_at = data.get("updated_at", rec.created_at)
        rec.remote_status = data.get("remote_status")
        rec.local_status = data.get("local_status")
        rec.last_attempt_at = data.get("last_attempt_at")
        rec.retry_count = data.get("retry_count", 0)
        rec.next_retry_at = data.get("next_retry_at")
        rec.last_error = data.get("last_error")
        rec.remote_response = data.get("remote_response")
        rec.last_outbound_command = data.get("last_outbound_command")
        rec.last_inbound_event = data.get("last_inbound_event")
        return rec

    def to_runtime_record(self) -> RuntimeCorrelationRecord:
        return RuntimeCorrelationRecord(
            plugin_id=self.plugin_id,
            correlation_id=self.correlation_id,
            integration_id=self.integration_id,
            external_job_id=self.external_job_id,
            local_job_id=self.local_job_id,
            local_job_name=self.local_job_name,
            status=self.status,
            remote_status=self.remote_status,
            local_status=self.local_status,
            payload_hash=self.payload_hash,
            last_attempt_at=self.last_attempt_at,
            last_error=self.last_error,
            remote_response=_sanitize_runtime_value(self.remote_response),
            last_outbound_command=_sanitize_runtime_value(self.last_outbound_command),
            last_inbound_event=_sanitize_runtime_value(self.last_inbound_event),
            retry_count=self.retry_count,
            next_retry_at=self.next_retry_at,
            created_at=self.created_at,
            updated_at=self.updated_at,
        )

    @classmethod
    def from_runtime_record(
        cls, rec: RuntimeCorrelationRecord
    ) -> RemoteSubmissionRecord:
        result = cls(
            correlation_id=rec.correlation_id,
            plugin_id=rec.plugin_id,
            integration_id=rec.integration_id,
            payload_hash=rec.payload_hash,
        )
        result.external_job_id = rec.external_job_id
        result.local_job_id = rec.local_job_id
        result.local_job_name = rec.local_job_name
        result.status = rec.status
        result.remote_status = rec.remote_status
        result.local_status = rec.local_status
        result.last_attempt_at = rec.last_attempt_at
        result.last_error = rec.last_error
        result.remote_response = rec.remote_response
        result.created_at = rec.created_at
        result.updated_at = rec.updated_at
        result.retry_count = rec.retry_count
        result.next_retry_at = rec.next_retry_at
        result.last_outbound_command = rec.last_outbound_command
        result.last_inbound_event = rec.last_inbound_event
        return result
