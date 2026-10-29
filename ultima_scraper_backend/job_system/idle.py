"""Bounded idle waits and best-effort scheduling coordination, never ownership."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from uuid import uuid4

from sqlalchemy.engine import make_url

from ultima_scraper_core import archive_database_url

logger = logging.getLogger(__name__)
WAKE_EVENTS = frozenset(
    {
        "job_created",
        "job_resumed",
        "job_cancelled",
        "job_completed",
        "task_completed",
        "task_failed",
        "task_retried",
        "ledger_wakeup",
    }
)


class IdleDelay:
    def __init__(self, initial=0.25, maximum=1.0):
        self.initial, self.maximum = initial, maximum
        self.delay = initial

    def reset(self):
        self.delay = self.initial

    def next(self, idle):
        if not idle:
            self.reset()
            return self.initial
        delay = self.delay
        self.delay = min(self.maximum, delay * 2)
        return delay

    async def wait(self, event, *, idle):
        delay = self.next(idle)
        if not idle:
            # Hints may interrupt an idle backoff, not accelerate active SQL
            # passes into one query per lifecycle event.
            event.clear()
            await asyncio.sleep(delay)
            return
        try:
            await asyncio.wait_for(event.wait(), timeout=delay)
            self.reset()
        except TimeoutError:
            pass
        finally:
            event.clear()


class SchedulingLease:
    """Avoid duplicate passes; SQL locks and attempt tokens retain correctness.

    A lost/expired lease stops new passes, never interrupts a SQL transaction.
    Redis failure permits bounded fallback polling rather than stopping SQL.
    """

    TTL_MS = 6000
    RENEW_SECONDS = 2
    STANDBY_SECONDS = 1
    RENEW = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('pexpire',KEYS[1],ARGV[2]) else return 0 end"
    RELEASE = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end"

    def __init__(self, redis, *, role: str | None = None):
        url = make_url(archive_database_url() or "postgresql://unconfigured")
        identity = json.dumps([url.host, url.port or 5432, url.database])
        self.key = (
            "ultimascraper:ledger:scheduler:"
            + hashlib.sha256(identity.encode()).hexdigest()
        )
        if role:
            # A separate process (e.g. the archiver) elects its own leader so
            # it runs beside, not instead of, the control plane's loops.
            self.key += ":" + role
        self.redis, self.token = redis, uuid4().hex
        self.state, self.deadline = "standby", 0.0

    @property
    def allowed(self):
        return self.state == "fallback" or (
            self.state == "leader" and time.monotonic() < self.deadline
        )

    async def update(self):
        try:
            if self.redis is None or not self.redis.is_connected:
                raise ConnectionError("Redis scheduling transport unavailable")
            client = self.redis.client
            started = time.monotonic()
            acquired = False
            async with asyncio.timeout(1):
                if self.state == "leader":
                    acquired = bool(
                        await client.eval(
                            self.RENEW, 1, self.key, self.token, self.TTL_MS
                        )
                    )
                if not acquired:
                    acquired = bool(
                        await client.set(self.key, self.token, nx=True, px=self.TTL_MS)
                    )
            self.state = "leader" if acquired else "standby"
            # Start the local deadline before the round trip, so a delayed
            # response cannot extend our belief in ownership past Redis TTL.
            self.deadline = started + self.TTL_MS / 1000 if acquired else 0.0
        except asyncio.CancelledError:
            raise
        except Exception:
            if self.state != "fallback":
                logger.warning(
                    "Ledger scheduling coordination unavailable; using bounded SQL fallback",
                    exc_info=True,
                )
            self.state, self.deadline = "fallback", 0.0

    async def run(self):
        while True:
            await self.update()
            await asyncio.sleep(
                self.RENEW_SECONDS if self.state == "leader" else self.STANDBY_SECONDS
            )

    async def wait(self):
        while not self.allowed:
            await asyncio.sleep(self.STANDBY_SECONDS)
        if self.state == "fallback":
            await asyncio.sleep(1)

    async def close(self):
        if self.redis is not None and self.state == "leader":
            try:
                async with asyncio.timeout(1):
                    await self.redis.client.eval(self.RELEASE, 1, self.key, self.token)
            except Exception:
                logger.debug(
                    "Scheduling lease release deferred to expiry", exc_info=True
                )


async def listen_for_wakeups(redis, events):
    """Hints shorten sleeps; bounded SQL polls cover lost publications."""
    from ..redis_channels import HOOKS_CHANNEL

    last_wake = float("-inf")
    while True:
        try:
            if redis is None or not redis.is_connected:
                await asyncio.sleep(1)
                continue
            async with redis.client.pubsub() as subscription:
                await subscription.subscribe(HOOKS_CHANNEL)
                async for message in subscription.listen():
                    if message.get("type") != "message":
                        continue
                    try:
                        payload = json.loads(message["data"])
                    except ValueError, TypeError:
                        continue
                    if (
                        isinstance(payload, dict)
                        and payload.get("event") in WAKE_EVENTS
                        and time.monotonic() - last_wake >= 0.25
                    ):
                        last_wake = time.monotonic()
                        for event in events:
                            event.set()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug(
                "Ledger wake hints unavailable; polling continues", exc_info=True
            )
            await asyncio.sleep(1)
