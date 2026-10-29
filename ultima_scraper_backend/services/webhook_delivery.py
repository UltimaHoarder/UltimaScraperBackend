"""Deliver configured site hooks independently of connected browser clients.

Redis pub/sub is the source. HTTP requests are bounded, retried, and claimed
across API processes; this service does not provide a durable delivery ledger.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

import aiohttp

from ..redis_channels import AUTH_EVENTS_CHANNEL, HOOKS_CHANNEL, KEY_PREFIX, WEBSOCKET_STREAM_CHANNEL
from ..webhook_config import HookName, WebhookDestination, effective_site_routing

logger = logging.getLogger(__name__)
_SENSITIVE_KEYS = {"password", "cookie", "authorization", "token", "access_token", "refresh_token", "wsauthtoken", "x_bc"}


def event_site(event: dict[str, Any]) -> str | None:
    explicit = str(event.get("site") or "").lower()
    source = str(event.get("source_id") or "").split(":", 1)[0].lower()
    if explicit and source and explicit != source:
        return None
    site = explicit or source
    return site if site in {"onlyfans", "fansly"} else None


def classify_hooks(channel: str, event: dict[str, Any]) -> set[HookName]:
    name = event.get("event") or event.get("type")
    if channel == AUTH_EVENTS_CHANNEL:
        return {"auth"} if name in {"auth_added", "auth_updated", "auth_removed"} else set()
    if channel == HOOKS_CHANNEL:
        return {"download"} if name == "download_progress" and event.get("status") in {"completed", "failed"} else set()
    if channel != WEBSOCKET_STREAM_CHANNEL or event_site(event) != "onlyfans":
        return set()
    content = {key for key in event if key not in {"id", "stored_at", "timestamp", "source_id", "username"}}
    if not content or content <= {"act", "connected", "success", "type"}:
        return set()
    hooks: set[HookName] = {"site_message"}
    for key, hook in (("api2_chat_message", "chat_message"), ("post_published", "post_published"), ("post_updated", "post_updated"), ("online", "online")):
        if key in event:
            hooks.add(hook)
    new_message = event.get("new_message")
    if isinstance(new_message, dict) and (new_message.get("subType") in {"new_subscriber", "new_subscriber_trial"} or new_message.get("type") == "subscribed"):
        hooks.add("subscriber")
    toasts = event.get("toasts")
    for toast in toasts if isinstance(toasts, list) else []:
        if not isinstance(toast, dict):
            continue
        data = toast.get("data")
        code = data.get("code") if isinstance(data, dict) else None
        if toast.get("type") == "subscribed" or code in {"new_subscriber", "new_subscriber_trial"}:
            hooks.add("subscriber")
        if code == "new_stream":
            hooks.add("stream_started")
        if code == "new_comment":
            hooks.add("comment")
        if code in {"price_changed_from_free", "price_changed_not_from_free"}:
            hooks.add("price_changed")
    return hooks


def redact_payload(value: Any, hidden: set[str]) -> Any:
    if isinstance(value, dict):
        return {key: redact_payload(item, hidden) for key, item in value.items() if key.lower() not in hidden}
    if isinstance(value, list):
        return [redact_payload(item, hidden) for item in value]
    return value


@dataclass
class Delivery:
    destination: WebhookDestination
    envelope: dict[str, Any]
    claim_key: str
    channel: str
    source_event: dict[str, Any]


def plan_deliveries(config: Any, channel: str, event: dict[str, Any]) -> list[Delivery]:
    site = event_site(event)
    hooks = classify_hooks(channel, event)
    if site is None or not hooks:
        return []
    routing = effective_site_routing(config, site)
    event_id = hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()
    deliveries: list[Delivery] = []
    for destination in routing.destinations:
        rules = [rule for rule in routing.rules if rule.active and destination.id in rule.destination_ids and hooks.intersection(rule.hooks)]
        if not rules:
            continue
        selected = sorted(set().union(*(hooks.intersection(rule.hooks) for rule in rules)))
        hidden = _SENSITIVE_KEYS | {key.lower() for rule in rules for key in rule.hide_info}
        envelope = {"id": event_id, "site": site, "hooks": selected, "event": event.get("event") or "site_message", "timestamp": event.get("timestamp") or event.get("stored_at"), "data": redact_payload(event, hidden)}
        target_id = hashlib.sha256(f"{config.config_write_backend()}:{site}:{destination.url}".encode()).hexdigest()
        deliveries.append(Delivery(destination, envelope, f"{KEY_PREFIX}:webhooks:delivery:{target_id}:{event_id}", channel, event))
    return deliveries


def request_payload(delivery: Delivery) -> dict[str, Any]:
    if delivery.destination.format == "json":
        return delivery.envelope
    data = json.dumps(delivery.envelope["data"], ensure_ascii=False, indent=2, default=str)
    # Discord embed descriptions are limited to 4096 characters.
    description = data[:3500].replace("```", "''' ")
    return {"allowed_mentions": {"parse": []}, "embeds": [{"title": f"{delivery.envelope['site'].capitalize()} · {', '.join(delivery.envelope['hooks'])}"[:256], "description": f"```json\n{description}\n```"}]}


class PublicDestinationResolver(aiohttp.abc.AbstractResolver):
    """Check the addresses actually used by the HTTP connector."""

    def __init__(self) -> None:
        self.delegate = aiohttp.resolver.DefaultResolver()

    async def resolve(self, host: str, port: int = 0, family: int = 0):
        addresses = await self.delegate.resolve(host, port, family)
        if any(not ipaddress.ip_address(address["host"]).is_global for address in addresses):
            raise OSError("Webhook destination must resolve to a public address")
        return addresses

    async def close(self) -> None:
        await self.delegate.close()


class WebhookDeliveryService:
    def __init__(self, redis: Any, get_config: Callable[[], Any]) -> None:
        self.redis = redis.client
        self.get_config = get_config
        self.config: Any = None
        self.queue: asyncio.Queue[Delivery] = asyncio.Queue(maxsize=1000)
        self.tasks: list[asyncio.Task] = []
        self.session: aiohttp.ClientSession | None = None
        self.resolver: PublicDestinationResolver | None = None

    async def start(self) -> None:
        self.config = await asyncio.to_thread(self.get_config)
        self.resolver = PublicDestinationResolver()
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=self.resolver), timeout=aiohttp.ClientTimeout(total=10))
        self.tasks = [asyncio.create_task(self._refresh()), asyncio.create_task(self._listen()), *(asyncio.create_task(self._worker()) for _ in range(4))]

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        if self.session:
            await self.session.close()
            self.session = None
        if self.resolver:
            await self.resolver.close()
            self.resolver = None

    async def _refresh(self) -> None:
        while True:
            await asyncio.sleep(5)
            try:
                self.config = await asyncio.to_thread(self.get_config)
            except Exception:
                logger.warning("Could not refresh webhook routing; retaining last configuration")

    def enqueue(self, channel: str, event: dict[str, Any]) -> None:
        for delivery in plan_deliveries(self.config, channel, event):
            try:
                self.queue.put_nowait(delivery)
            except asyncio.QueueFull:
                logger.warning("Webhook delivery queue full; destination=%s event=%s", delivery.destination.id, delivery.envelope["id"])

    async def _listen(self) -> None:
        while True:
            pubsub = self.redis.pubsub()
            try:
                await pubsub.subscribe(AUTH_EVENTS_CHANNEL, HOOKS_CHANNEL, WEBSOCKET_STREAM_CHANNEL)
                async for message in pubsub.listen():
                    if not isinstance(message, dict) or message.get("type") != "message":
                        continue
                    channel = message.get("channel")
                    if isinstance(channel, bytes):
                        channel = channel.decode()
                    try:
                        event = json.loads(message["data"])
                        if isinstance(event, dict):
                            self.enqueue(channel, event)
                    except (ValueError, TypeError, KeyError):
                        logger.debug("Ignoring malformed webhook source event")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Webhook source subscription interrupted; reconnecting")
                await asyncio.sleep(2)
            finally:
                await pubsub.aclose()

    async def _worker(self) -> None:
        while True:
            delivery = await self.queue.get()
            try:
                await self.deliver(delivery)
            except Exception:
                logger.warning("Webhook delivery unavailable; destination=%s event=%s", delivery.destination.id, delivery.envelope["id"])
            finally:
                self.queue.task_done()

    async def deliver(self, delivery: Delivery) -> bool:
        # An endpoint may have been removed or a rule disabled while queued.
        current = next((candidate for candidate in plan_deliveries(self.config, delivery.channel, delivery.source_event) if candidate.destination.id == delivery.destination.id and candidate.destination.url == delivery.destination.url), None)
        if current is None:
            return False
        delivery = current
        if not await self.redis.set(delivery.claim_key, "sending", nx=True, ex=90):
            return False
        try:
            for attempt in range(3):
                try:
                    assert self.session is not None
                    # Literal IPs bypass aiohttp's resolver, so validate them too.
                    from urllib.parse import urlsplit
                    host = urlsplit(delivery.destination.url).hostname or ""
                    try:
                        address = ipaddress.ip_address(host)
                    except ValueError:
                        address = None
                    if address is not None and not address.is_global:
                        raise ValueError("Webhook destination must be public")
                    async with self.session.post(delivery.destination.url, json=request_payload(delivery), allow_redirects=False, headers={"X-Ultima-Event-ID": delivery.envelope["id"]}) as response:
                        if 200 <= response.status < 300:
                            await self.redis.set(delivery.claim_key, "delivered", ex=86400)
                            return True
                        if response.status < 500 and response.status != 429:
                            break
                except (aiohttp.ClientError, TimeoutError, OSError, ValueError):
                    pass
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
            logger.warning("Webhook delivery failed after retries; destination=%s event=%s", delivery.destination.id, delivery.envelope["id"])
            return False
        finally:
            if await self.redis.get(delivery.claim_key) in {b"sending", "sending"}:
                await self.redis.delete(delivery.claim_key)
