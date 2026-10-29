"""Restart-safe event consumption and one-at-a-time delivery attempts."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import timedelta
from uuid import uuid4

from ..database.threads import run_database_call
from ..job_system.ledger import ExecutionLedger
from .delivery import EventEnvelope
from .durable_repository import PluginBacklogFull
from .manifest import load_manifest_from_dict
from .permissions import PluginCapability

logger = logging.getLogger(__name__)
# Events read per pass; the ledger's maximum page.
FEED_PAGE = 500


class DurableDeliveryRuntime:
    def __init__(self, platform, *, ledger=None):
        self.platform = platform
        self.repository = platform.repository
        self.ledger = ledger or ExecutionLedger()
        self.owner = uuid4().hex
        self._delivery_offset = 0

    def deliveries(self, manifest, event):
        output = []
        for subscription in manifest.event_subscriptions:
            if subscription.event_family != event["event_family"]:
                continue
            if (
                subscription.event_types
                and event["event_type"] not in subscription.event_types
            ):
                continue
            envelope = EventEnvelope(
                event_id=str(event.get("event_id") or uuid4().hex),
                event_family=event["event_family"],
                event_type=event["event_type"],
                plugin_id=manifest.plugin_id,
                body=event["body"],
                job_id=event.get("job_id"),
                worker_id=event.get("worker_id"),
            )
            output.append((subscription.delivery_target, envelope.to_dict()))
        return output

    async def enqueue(self, event):
        event = {**event, "event_id": str(event.get("event_id") or uuid4().hex)}
        for record in self.platform.registry.list_enabled():
            if not self.platform._has_capability(record, PluginCapability.EVENTS_READ):
                continue
            await run_database_call(
                self.repository.enqueue_deliveries,
                record.plugin_id,
                self.deliveries(record.manifest, event),
                record.manifest.model_dump(mode="json"),
            )

    async def consume_once(self) -> bool:
        """A stalled subscription retains its cursor without blocking others.

        Returns True when some plugin read a full page and is still behind.
        """
        behind = False
        for record in self.platform.registry.list_enabled():
            manifest = record.manifest
            if not self.platform._has_capability(record, PluginCapability.EVENTS_READ):
                continue
            lease = await run_database_call(
                self.repository.acquire_lease,
                f"feed:{record.plugin_id}",
                self.owner,
                seconds=30,
            )
            if not lease:
                continue
            try:
                head = await run_database_call(self.ledger.event_head)
                cursor = await run_database_call(
                    self.repository.cursor,
                    record.plugin_id,
                    initial=0 if manifest.operational_policy.replay_on_enable else head,
                )
                events = await run_database_call(
                    self.ledger.events,
                    after=cursor,
                    limit=FEED_PAGE,
                    include_api_commands=True,
                )
                behind = behind or len(events) == FEED_PAGE
                manifest_json = manifest.model_dump(mode="json")
                # Most events produce no delivery for a plugin. Advance the
                # cursor past a run of them in one transaction; an event with
                # deliveries still commits alone, with its cursor, in order.
                skipped_to = None

                async def commit(deliveries, sequence):
                    nonlocal cursor
                    committed = await run_database_call(
                        self.repository.enqueue_deliveries,
                        record.plugin_id,
                        deliveries,
                        manifest_json,
                        sequence=sequence,
                        expected_cursor=cursor,
                    )
                    if committed:
                        cursor = sequence
                    return committed

                for raw in events:
                    normalized = self.platform._normalize_local_job_event(raw)
                    deliveries = []
                    if normalized is not None:
                        event = {
                            "event_id": str(raw["event_id"]),
                            "event_family": "job.lifecycle",
                            "event_type": f"job.{normalized['lifecycle_event']}",
                            "body": normalized,
                            "job_id": normalized.get("job_id"),
                            "worker_id": normalized.get("worker_id"),
                        }
                        deliveries = self.deliveries(manifest, event)
                        correlation_id = self.platform._local_job_index.get(
                            normalized.get("job_id")
                        )
                        correlation = self.platform._submissions.get(correlation_id)
                        if (
                            correlation is not None
                            and correlation.plugin_id == record.plugin_id
                        ):
                            # Remote adapters already carry stable correlation /
                            # job identities and must tolerate repeated updates.
                            await self.platform._apply_local_job_event(
                                correlation, normalized
                            )
                    if not deliveries:
                        skipped_to = raw["sequence"]
                        continue
                    if skipped_to is not None:
                        if not await commit([], skipped_to):
                            break
                        skipped_to = None
                    if not await commit(deliveries, raw["sequence"]):
                        break
                else:
                    if skipped_to is not None:
                        await commit([], skipped_to)
            except PluginBacklogFull:
                logger.warning(
                    "Plugin %s is waiting for delivery capacity", record.plugin_id
                )
            except Exception:
                logger.exception(
                    "Plugin %s feed consumption failed; its cursor is retained",
                    record.plugin_id,
                )
            finally:
                await run_database_call(
                    self.repository.release_lease, f"feed:{record.plugin_id}", lease
                )
        return behind

    async def consume_loop(self):
        maintenance_at = 0
        while self.platform._runtime_started:
            behind = False
            try:
                behind = await self.consume_once()
                now = asyncio.get_running_loop().time()
                if now >= maintenance_at:
                    await run_database_call(self.repository.prune_deliveries)
                    maintenance_at = now + 3600
            except Exception:
                logger.exception("Plugin durable feed pass failed")
            # A plugin that is behind keeps reading; otherwise poll twice a second.
            await asyncio.sleep(0 if behind else 0.5)

    async def deliver_once(self):
        records = self.platform.registry.list_enabled()
        if records:
            offset = self._delivery_offset % len(records)
            records = records[offset:] + records[:offset]
            self._delivery_offset += 1
        for record in records:
            if not self.platform._has_capability(record, PluginCapability.EVENTS_READ):
                continue
            item = await run_database_call(
                self.repository.claim_delivery,
                record.plugin_id,
                max_concurrency=record.manifest.operational_policy.delivery_concurrency,
            )
            if item is None:
                continue
            manifest = load_manifest_from_dict(
                self.repository._decode(item["manifest_json"])
            )
            target = next(
                (
                    target
                    for target in manifest.outbound_deliveries
                    if target.target_name == item["target_name"]
                ),
                None,
            )
            error = None
            retry_at = None
            try:
                if target is None:
                    raise ValueError("Pinned delivery target is missing")
                url = self.platform.configured_url(
                    manifest, target.url, target.url_setting
                )
                # Approval is checked against current policy as well as the
                # pinned version; an upgrade cannot resurrect revoked access.
                await self.platform._validate_http_target(record.manifest, url)
                self.platform.require_outbound_scope(record.plugin_id, url)
                payload = self.repository._decode(item["payload_json"])
                payload.pop("delivery_id", None)
                envelope = EventEnvelope(delivery_id=item["delivery_id"], **payload)
                attempt = await self.platform.delivery.deliver(
                    envelope,
                    url=url,
                    target_name=target.target_name,
                    signing_secret=self.platform.secrets.get_raw_secret(
                        record.plugin_id, "outbound", target.target_name
                    ),
                    retry_policy_max_retries=0,
                    manifest=manifest,
                )
                error = attempt.error if attempt.status != "delivered" else None
            except asyncio.CancelledError:
                # Leave the claim recoverable after its lease expires.
                raise
            except Exception as exc:  # noqa: BLE001 - persist adapter failures for durable retry
                error = str(exc)
            policy = manifest.operational_policy.retry_policy
            if error and item["attempts"] <= policy.max_retries:
                delay = min(
                    policy.max_backoff_seconds,
                    policy.initial_backoff_seconds
                    * policy.backoff_multiplier ** min(item["attempts"] - 1, 20),
                )
                retry_at = (
                    self.repository.now()
                    + timedelta(seconds=random.uniform(delay * 0.8, delay))
                ).isoformat()
            await run_database_call(
                self.repository.finish_delivery,
                item["delivery_id"],
                item["lease_token"],
                error=error,
                retry_at=retry_at,
            )
            return True
        return False

    async def delivery_loop(self):
        while self.platform._runtime_started:
            try:
                worked = await self.deliver_once()
            except Exception:
                logger.exception("Plugin durable delivery pass failed")
                worked = False
            if not worked:
                await asyncio.sleep(0.25)
