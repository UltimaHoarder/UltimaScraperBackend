"""Plugin event delivery engine.

Handles matching events to plugin subscriptions and delivering them to
outbound targets with bounded, in-process retries. Receivers must tolerate
duplicates. Persisted delivery records are operator observability, not a
durable outbox: an interrupted retry schedule is not resumed after restart.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .http_transport import default_http_post

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .repository import PluginRepository


class DeliveryAttempt:
    """Record of a single delivery attempt."""

    __slots__ = (
        "delivery_id",
        "plugin_id",
        "target_name",
        "event_id",
        "event_family",
        "event_type",
        "status",
        "status_code",
        "error",
        "attempted_at",
        "retry_count",
        "next_retry_at",
    )

    def __init__(
        self,
        *,
        delivery_id: str,
        plugin_id: str,
        target_name: str,
        event_id: str,
        event_family: str,
        event_type: str,
    ) -> None:
        self.delivery_id = delivery_id
        self.plugin_id = plugin_id
        self.target_name = target_name
        self.event_id = event_id
        self.event_family = event_family
        self.event_type = event_type
        self.status: str = "pending"
        self.status_code: int | None = None
        self.error: str | None = None
        self.attempted_at: datetime | None = None
        self.retry_count: int = 0
        self.next_retry_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "delivery_id": self.delivery_id,
            "plugin_id": self.plugin_id,
            "target_name": self.target_name,
            "event_id": self.event_id,
            "event_family": self.event_family,
            "event_type": self.event_type,
            "status": self.status,
            "status_code": self.status_code,
            "error": self.error,
            "attempted_at": self.attempted_at.isoformat() if self.attempted_at else None,
            "retry_count": self.retry_count,
            "next_retry_at": (
                self.next_retry_at.isoformat() if self.next_retry_at else None
            ),
        }


class EventEnvelope:
    """Standard outbound event envelope."""

    def __init__(
        self,
        *,
        event_id: str | None = None,
        event_family: str,
        event_type: str,
        plugin_id: str,
        delivery_id: str | None = None,
        timestamp: str | None = None,
        body: dict[str, Any],
        job_id: str | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.event_id = event_id or uuid4().hex
        self.event_family = event_family
        self.event_type = event_type
        self.plugin_id = plugin_id
        self.delivery_id = delivery_id or uuid4().hex
        self.timestamp = timestamp or datetime.now(UTC).isoformat()
        self.body = body
        self.job_id = job_id
        self.worker_id = worker_id

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "event_id": self.event_id,
            "event_family": self.event_family,
            "event_type": self.event_type,
            "plugin_id": self.plugin_id,
            "delivery_id": self.delivery_id,
            "timestamp": self.timestamp,
            "body": self.body,
        }
        if self.job_id is not None:
            d["job_id"] = self.job_id
        if self.worker_id is not None:
            d["worker_id"] = self.worker_id
        return d

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode()


class EndpointDeliveryStatus:
    """Per-endpoint delivery metrics for operator visibility."""

    __slots__ = (
        "plugin_id",
        "target_name",
        "last_error",
        "consecutive_failures",
        "next_retry_at",
        "last_success_at",
        "recent_history",
    )

    def __init__(self, plugin_id: str, target_name: str) -> None:
        self.plugin_id = plugin_id
        self.target_name = target_name
        self.last_error: str | None = None
        self.consecutive_failures: int = 0
        self.next_retry_at: datetime | None = None
        self.last_success_at: datetime | None = None
        self.recent_history: deque[dict[str, Any]] = deque(maxlen=50)

    def record_success(self) -> None:
        self.last_error = None
        self.consecutive_failures = 0
        self.next_retry_at = None
        self.last_success_at = datetime.now(UTC)
        self.recent_history.append(
            {"status": "ok", "at": datetime.now(UTC).isoformat()}
        )

    def record_failure(self, error: str, next_retry: datetime | None) -> None:
        self.last_error = error
        self.consecutive_failures += 1
        self.next_retry_at = next_retry
        self.recent_history.append(
            {"status": "error", "error": error, "at": datetime.now(UTC).isoformat()}
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "target_name": self.target_name,
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
            "next_retry_at": (
                self.next_retry_at.isoformat() if self.next_retry_at else None
            ),
            "last_success_at": (
                self.last_success_at.isoformat() if self.last_success_at else None
            ),
            "recent_history": list(self.recent_history),
        }


# ── HTTP delivery helper (pluggable for testing) ─────────────────────────

async def _default_http_post(
    url: str,
    payload: bytes,
    headers: dict[str, str],
    *,
    timeout: float = 30.0,
) -> int:
    """Compatibility alias for the shared bounded HTTP transport."""
    return await default_http_post(url, payload, headers, timeout=timeout)


class EventDeliveryEngine:
    """Matches platform events to plugin subscriptions and delivers them."""

    def __init__(
        self,
        *,
        http_post: Any = None,
        retention_limit: int = 200,
        max_concurrency: int = 8,
        repository: PluginRepository | None = None,
        url_validator: Any = None,
        url_resolver: Any = None,
    ) -> None:
        self._http_post = http_post or _default_http_post
        self._repository = repository
        self._url_validator = url_validator
        self._url_resolver = url_resolver
        self._semaphore = asyncio.Semaphore(max(1, max_concurrency))
        self._endpoint_status: dict[str, EndpointDeliveryStatus] = {}
        self._recent_deliveries: deque[DeliveryAttempt] = deque(
            maxlen=retention_limit
        )

    def _status_key(self, plugin_id: str, target_name: str) -> str:
        return f"{plugin_id}:{target_name}"

    def get_endpoint_status(
        self, plugin_id: str, target_name: str
    ) -> EndpointDeliveryStatus:
        key = self._status_key(plugin_id, target_name)
        if key not in self._endpoint_status:
            self._endpoint_status[key] = EndpointDeliveryStatus(
                plugin_id, target_name
            )
        return self._endpoint_status[key]

    def list_endpoint_statuses(
        self, plugin_id: str | None = None
    ) -> list[dict[str, Any]]:
        if self._repository is not None:
            attempts = self._repository.list_delivery_attempts(
                plugin_id,
                limit=5000,
            )
            grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for attempt in attempts:
                key = (str(attempt["plugin_id"]), str(attempt["target_name"]))
                grouped.setdefault(key, []).append(attempt)

            result: list[dict[str, Any]] = []
            for (stored_plugin_id, target_name), history in sorted(grouped.items()):
                newest = history[0]
                consecutive_failures = 0
                last_success_at: str | None = None
                for item in history:
                    if item.get("status") == "delivered":
                        if last_success_at is None:
                            last_success_at = item.get("attempted_at")
                        break
                    if item.get("status") == "failed":
                        consecutive_failures += 1
                result.append(
                    {
                        "plugin_id": stored_plugin_id,
                        "target_name": target_name,
                        "last_error": newest.get("error"),
                        "consecutive_failures": consecutive_failures,
                        "next_retry_at": newest.get("next_retry_at"),
                        "last_success_at": last_success_at,
                        "recent_history": history[:50],
                    }
                )
            return result
        statuses = self._endpoint_status.values()
        if plugin_id is not None:
            statuses = [s for s in statuses if s.plugin_id == plugin_id]
        return [s.to_dict() for s in statuses]

    def recent_deliveries(
        self, plugin_id: str | None = None, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        if self._repository is not None:
            return self._repository.list_delivery_attempts(plugin_id, limit=limit)
        items = list(self._recent_deliveries)
        if plugin_id is not None:
            items = [d for d in items if d.plugin_id == plugin_id]
        return [d.to_dict() for d in items[-limit:]]

    async def deliver(
        self,
        envelope: EventEnvelope,
        *,
        url: str,
        target_name: str,
        signing_secret: str | None = None,
        retry_policy_max_retries: int = 10,
        retry_policy_initial_backoff: int = 5,
        retry_policy_max_backoff: int = 3600,
        retry_policy_backoff_multiplier: float = 2.0,
        manifest: Any = None,
    ) -> DeliveryAttempt:
        """Deliver an event envelope to a target URL with retries."""
        attempt = DeliveryAttempt(
            delivery_id=envelope.delivery_id,
            plugin_id=envelope.plugin_id,
            target_name=target_name,
            event_id=envelope.event_id,
            event_family=envelope.event_family,
            event_type=envelope.event_type,
        )

        payload_bytes = envelope.to_json_bytes()
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": envelope.delivery_id,
            "X-Plugin-Event-ID": str(envelope.event_id),
        }
        if signing_secret:
            sig = hmac.new(
                signing_secret.encode(),
                payload_bytes,
                hashlib.sha256,
            ).hexdigest()
            headers["X-Plugin-Signature"] = f"sha256={sig}"

        status_tracker = self.get_endpoint_status(
            envelope.plugin_id, target_name
        )
        if self._repository is not None:
            self._repository.upsert_delivery_attempt(attempt.to_dict())

        backoff = float(retry_policy_initial_backoff)
        for retry in range(retry_policy_max_retries + 1):
            attempt.attempted_at = datetime.now(UTC)
            attempt.retry_count = retry
            attempt.next_retry_at = None
            try:
                if (
                    self._url_validator is not None
                    and manifest is not None
                    and self._http_post in {_default_http_post, default_http_post}
                ):
                    await asyncio.to_thread(self._url_validator, manifest, url)
                async with self._semaphore:
                    status_code = await self._http_post(
                        url, payload_bytes, headers
                    )
                attempt.status_code = status_code
                if 200 <= status_code < 300:
                    attempt.status = "delivered"
                    attempt.error = None
                    status_tracker.record_success()
                    break
                else:
                    attempt.status = "failed"
                    attempt.error = f"HTTP {status_code}"
            except Exception as exc:
                attempt.status = "failed"
                attempt.error = str(exc)

            if retry < retry_policy_max_retries:
                capped_backoff = min(backoff, float(retry_policy_max_backoff))
                delay = (
                    0.0
                    if capped_backoff <= 0
                    else random.uniform(capped_backoff * 0.8, capped_backoff * 1.2)
                )
                attempt.next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)
                status_tracker.record_failure(
                    attempt.error or "Delivery failed",
                    attempt.next_retry_at,
                )
                if self._repository is not None:
                    self._repository.upsert_delivery_attempt(attempt.to_dict())
                await asyncio.sleep(delay)
                backoff = min(
                    backoff * retry_policy_backoff_multiplier,
                    float(retry_policy_max_backoff),
                )
            else:
                status_tracker.record_failure(
                    attempt.error or "Delivery failed",
                    None,
                )

        attempt.next_retry_at = None
        self._recent_deliveries.append(attempt)
        if self._repository is not None:
            self._repository.upsert_delivery_attempt(attempt.to_dict())
        return attempt

    async def dispatch_event(
        self,
        *,
        event_family: str,
        event_type: str,
        body: dict[str, Any],
        job_id: str | None = None,
        worker_id: str | None = None,
        registry: Any,
        secret_manager: Any,
    ) -> list[DeliveryAttempt]:
        """Match an event against all enabled plugins and deliver.

        ``registry`` is a ``PluginRegistry`` and ``secret_manager`` is a
        ``SecretManager``.  Types are ``Any`` to avoid circular imports.
        """
        from .registry import PluginRegistry
        from .secrets import SecretManager

        reg: PluginRegistry = registry
        sm: SecretManager = secret_manager

        pending: list[Any] = []
        for record in reg.list_enabled():
            manifest = record.manifest
            for sub in manifest.event_subscriptions:
                if sub.event_family != event_family:
                    continue
                if sub.event_types and event_type not in sub.event_types:
                    continue

                # Find the delivery target
                target = next(
                    (
                        t
                        for t in manifest.outbound_deliveries
                        if t.target_name == sub.delivery_target
                    ),
                    None,
                )
                if target is None:
                    continue

                envelope = EventEnvelope(
                    event_family=event_family,
                    event_type=event_type,
                    plugin_id=manifest.plugin_id,
                    body=body,
                    job_id=job_id,
                    worker_id=worker_id,
                )

                secret_entry = sm.get_entry(
                    manifest.plugin_id, "outbound", target.target_name
                )
                signing_secret = (
                    secret_entry.active_secret_value if secret_entry else None
                )

                policy = manifest.operational_policy.retry_policy
                pending.append(
                    self.deliver(
                        envelope,
                        url=self._url_resolver(manifest, target.url, target.url_setting) if self._url_resolver else target.url,
                        target_name=target.target_name,
                        signing_secret=signing_secret,
                        retry_policy_max_retries=policy.max_retries,
                        retry_policy_initial_backoff=policy.initial_backoff_seconds,
                        retry_policy_max_backoff=policy.max_backoff_seconds,
                        retry_policy_backoff_multiplier=policy.backoff_multiplier,
                        manifest=manifest,
                    )
                )
        if not pending:
            return []
        return list(await asyncio.gather(*pending))
