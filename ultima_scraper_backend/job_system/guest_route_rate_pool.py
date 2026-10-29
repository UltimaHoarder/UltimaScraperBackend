"""Per-route guest request budget shared by every worker through Redis.

``network.guest_route_rate_limit`` is a total per proxy. Without this pool each
worker paces its own equal share, so a busy route behaves like ten small
queues: a worker whose share is momentarily oversubscribed waits while the
others' shares sit unused. With ULTIMA_GUEST_ROUTE_RATE_POOL=1 every worker
reserves its request's slot in one schedule per route (GCRA): the route still
sends at most the configured rate in total, but no request waits while the
route has spare budget.

Each process keeps its adaptive 429 cuts: a reservation is spaced by the
pooled rate times that process's current fraction of its configured share.
If Redis fails, the request falls back to the process's own bucket.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

KEY_PREFIX = "usr:guest_route_rate:"
_FAIL_OPEN_LOG_SECONDS = 60.0

# KEYS[1] = route schedule; ARGV = interval_ms, burst_ms. Returns the wait in
# microseconds before this request's reserved slot.
_RESERVE = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
local interval = tonumber(ARGV[1])
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
local earliest = now - tonumber(ARGV[2])
if tat < earliest then tat = earliest end
local next_tat = tat + interval
redis.call('SET', KEYS[1], string.format('%.3f', next_tat),
  'PX', math.floor(next_tat - now) + 10000)
if tat <= now then return 0 end
return math.floor((tat - now) * 1000)
"""


def rate_pool_enabled() -> bool:
    """ULTIMA_GUEST_ROUTE_RATE_POOL=1 shares each route's budget across workers."""
    return os.environ.get("ULTIMA_GUEST_ROUTE_RATE_POOL", "").strip() == "1"


def pool_rate_cap() -> float | None:
    """ULTIMA_GUEST_ROUTE_RATE_POOL_RATE: optional lower total per route for the pool.

    The pool lets a route reach its full configured rate, which can exceed what
    an upstream IP tolerates; this caps it without changing the setting.
    """
    try:
        value = float(os.environ.get("ULTIMA_GUEST_ROUTE_RATE_POOL_RATE") or 0)
    except ValueError:
        return None
    return value if value > 0 else None


def schedule_key(route_key: str) -> str:
    # Route keys carry proxy credentials; Redis key names must not.
    return KEY_PREFIX + hashlib.sha256(route_key.encode()).hexdigest()[:24]


class GuestRouteRatePool:
    """Async provider for ``configure_guest_route_rate_pool``."""

    def __init__(self, client: Any, rate: float, *, burst_seconds: float) -> None:
        self.client = client
        self.rate = float(rate)
        self.burst_ms = max(0.0, burst_seconds) * 1000.0
        self._reserve = client.register_script(_RESERVE)
        self._fail_open_logged = 0.0

    async def __call__(self, route_key: str, fraction: float) -> bool:
        """Wait for this request's slot; False means pace locally instead."""
        interval_ms = 1000.0 / max(self.rate * max(fraction, 0.0), 1e-6)
        try:
            wait_us = await self._reserve(
                keys=[schedule_key(route_key)],
                args=[f"{interval_ms:.3f}", f"{self.burst_ms:.3f}"],
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - fall back to the local bucket
            now = time.monotonic()
            if now - self._fail_open_logged >= _FAIL_OPEN_LOG_SECONDS:
                self._fail_open_logged = now
                logger.warning(
                    "Shared guest route budget unavailable (%s); pacing per worker",
                    type(exc).__name__,
                )
            return False
        if wait_us and int(wait_us) > 0:
            await asyncio.sleep(int(wait_us) / 1_000_000)
        return True


def configure_route_rate_pool(
    client: Any, limit: float, *, worker_count: int, worker_index: int | None
) -> GuestRouteRatePool | None:
    """Install the shared budget for this worker's guest requests, if switched on."""
    from ultima_scraper_api.managers import session_manager

    from .guest_sessions import INTERACTIVE_GUEST_ROUTE_RATE, guest_route_rate_share

    share = guest_route_rate_share(limit, worker_count=worker_count, worker_index=worker_index)
    if client is None or worker_index is None or share is None or not rate_pool_enabled():
        session_manager.configure_guest_route_rate_pool(None)
        return None
    # Workers share what guest_route_rate_share splits between them; API
    # processes keep their own small interactive share outside the pool.
    pooled = max(limit - INTERACTIVE_GUEST_ROUTE_RATE, INTERACTIVE_GUEST_ROUTE_RATE)
    pooled = min(pooled, pool_rate_cap() or pooled)
    pool = GuestRouteRatePool(
        client, pooled, burst_seconds=session_manager.GUEST_ROUTE_BURST_SECONDS
    )
    session_manager.configure_guest_route_rate_pool(pool)
    return pool
