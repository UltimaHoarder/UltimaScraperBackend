"""Per-proxy guest connection permits shared by every worker through Redis.

A proxy's ``max_connections`` is a total for the whole deployment, not a
per-worker number. Each guest send holds one permit for its proxy; a proxy
with 8 connections therefore has at most 8 requests in flight across all
workers, while every worker can still use every proxy.

Permits are leases: a sorted set per proxy whose members expire, so a worker
that dies while holding one cannot keep it. If Redis is unavailable the send
goes ahead without a permit (logged), rather than stopping all scraping.

With ULTIMA_GUEST_ROUTE_PERMIT_COALESCE_SECONDS > 0, a finished send's release
waits up to that long and rides along with this process's next acquire, so a
request costs one Redis round trip instead of two. A release is only ever
late, never early, so max_connections still holds.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
import uuid
from contextlib import asynccontextmanager, nullcontext
from typing import Any, AsyncIterator

logger = logging.getLogger(__name__)

KEY_PREFIX = "usr:guest_route_permits:"
# Well above any guest request's timeout; only a crashed holder runs it out.
LEASE_SECONDS = 60.0
WAIT_INITIAL_SECONDS = 0.005
WAIT_MAX_SECONDS = 0.1
_FAIL_OPEN_LOG_SECONDS = 60.0

_ACQUIRE = """
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3])
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
  return 1
end
return 0
"""


# Releases KEYS[1..n] (tokens ARGV[2..n+1]), then, when one more key is given,
# acquires it as _ACQUIRE does with ARGV[n+2..n+4] = limit, lease_ms, token.
_RELEASE_AND_ACQUIRE = """
local n = tonumber(ARGV[1])
for i = 1, n do
  redis.call('ZREM', KEYS[i], ARGV[i + 1])
end
if #KEYS == n then
  return 1
end
local key = KEYS[n + 1]
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) < tonumber(ARGV[n + 2]) then
  redis.call('ZADD', key, now + tonumber(ARGV[n + 3]), ARGV[n + 4])
  redis.call('PEXPIRE', key, tonumber(ARGV[n + 3]) * 2)
  return 1
end
return 0
"""


def release_coalesce_seconds() -> float:
    """ULTIMA_GUEST_ROUTE_PERMIT_COALESCE_SECONDS: longest a release may wait (0 = off)."""
    try:
        return max(0.0, float(os.environ.get("ULTIMA_GUEST_ROUTE_PERMIT_COALESCE_SECONDS") or 0))
    except ValueError:
        return 0.0


def route_permits_enabled() -> bool:
    """ULTIMA_GUEST_ROUTE_PERMITS=0/false/off/no disables the shared permits."""
    value = os.environ.get("ULTIMA_GUEST_ROUTE_PERMITS")
    return value is None or value.strip().lower() not in {"0", "false", "off", "no"}


def permit_key(proxy_url: str) -> str:
    # Proxy URLs carry credentials; Redis key names must not.
    return KEY_PREFIX + hashlib.sha256(proxy_url.encode()).hexdigest()[:24]


class GuestRoutePermits:
    """Callable permit provider for ``configure_guest_route_permits``."""

    def __init__(
        self,
        client: Any,
        capacities: dict[str, int],
        *,
        lease_seconds: float = LEASE_SECONDS,
        coalesce_seconds: float | None = None,
    ) -> None:
        self.client = client
        self.capacities = {route: max(1, int(limit)) for route, limit in capacities.items()}
        self.lease_ms = int(lease_seconds * 1000)
        self._acquire = client.register_script(_ACQUIRE)
        # -inf, not 0: time.monotonic() counts from boot, so 0 would hide the first
        # failure on a machine up for less than _FAIL_OPEN_LOG_SECONDS.
        self._fail_open_logged = float("-inf")
        self.coalesce_seconds = (
            release_coalesce_seconds() if coalesce_seconds is None else coalesce_seconds
        )
        self._release_and_acquire = (
            client.register_script(_RELEASE_AND_ACQUIRE) if self.coalesce_seconds > 0 else None
        )
        self._pending_releases: list[tuple[str, str]] = []
        self._release_timer: asyncio.TimerHandle | None = None
        self._release_tasks: set[asyncio.Task[None]] = set()

    def __call__(self, proxy_url: str) -> Any:
        limit = self.capacities.get(proxy_url)
        if limit is None:
            return nullcontext()  # Not a configured proxy: no shared budget to apply.
        return self._permit(proxy_url, limit)

    @asynccontextmanager
    async def _permit(self, proxy_url: str, limit: int) -> AsyncIterator[None]:
        key = permit_key(proxy_url)
        token = await self.acquire(key, limit)
        try:
            yield
        finally:
            if token is not None:
                if self._release_and_acquire is not None:
                    self._defer_release(key, token)
                else:
                    await self.release(key, token)

    async def acquire(self, key: str, limit: int) -> str | None:
        """Wait for a free permit; None means Redis failed and the send goes unbounded."""
        token = uuid.uuid4().hex
        delay = WAIT_INITIAL_SECONDS
        while True:
            releases: list[tuple[str, str]] = []
            try:
                if self._release_and_acquire is None:
                    acquired = await self._acquire(keys=[key], args=[limit, self.lease_ms, token])
                else:
                    releases, self._pending_releases = self._pending_releases, []
                    acquired = await self._release_and_acquire(
                        keys=[*(k for k, _ in releases), key],
                        args=[len(releases), *(t for _, t in releases), limit, self.lease_ms, token],
                    )
                if acquired:
                    return token
            except asyncio.CancelledError:
                # The script may not have run; ZREM is idempotent, so requeue.
                if releases:
                    self._pending_releases[:0] = releases
                    self._schedule_release_flush()
                raise
            except Exception as exc:  # noqa: BLE001 - fail open, never stall scraping
                now = time.monotonic()
                if now - self._fail_open_logged >= _FAIL_OPEN_LOG_SECONDS:
                    self._fail_open_logged = now
                    logger.warning(
                        "Guest route permits unavailable (%s); sending without them",
                        type(exc).__name__,
                    )
                return None
            await asyncio.sleep(delay * (0.5 + random.random()))
            delay = min(delay * 2, WAIT_MAX_SECONDS)

    def _defer_release(self, key: str, token: str) -> None:
        self._pending_releases.append((key, token))
        self._schedule_release_flush()

    def _schedule_release_flush(self) -> None:
        if self._release_timer is None:
            self._release_timer = asyncio.get_running_loop().call_later(
                self.coalesce_seconds, self._start_release_flush
            )

    def _start_release_flush(self) -> None:
        self._release_timer = None
        if self._pending_releases:
            task = asyncio.get_running_loop().create_task(self.flush_releases())
            self._release_tasks.add(task)
            task.add_done_callback(self._release_tasks.discard)

    async def flush_releases(self) -> None:
        """Send releases that no acquire has carried yet."""
        releases, self._pending_releases = self._pending_releases, []
        if not releases or self._release_and_acquire is None:
            return
        try:
            await self._release_and_acquire(
                keys=[k for k, _ in releases],
                args=[len(releases), *(t for _, t in releases)],
            )
        except asyncio.CancelledError:
            self._pending_releases[:0] = releases
            raise
        except Exception:  # noqa: BLE001 - the leases expire on their own
            logger.debug("Could not release guest route permits", exc_info=True)

    async def release(self, key: str, token: str) -> None:
        try:
            await self.client.zrem(key, token)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the lease expires on its own
            logger.debug("Could not release guest route permit", exc_info=True)


def configure_route_permits(client: Any, proxies: Any, *, enabled: bool = True) -> GuestRoutePermits | None:
    """Install Redis-backed permits for this process's guest requests."""
    from ultima_scraper_api.managers.session_manager import configure_guest_route_permits

    from ..usapi_initializer import resolve_proxy_url
    from .guest_sessions import configured_route_capacities

    if not enabled or client is None or not route_permits_enabled():
        configure_guest_route_permits(None)
        return None
    permits = GuestRoutePermits(client, configured_route_capacities(proxies, resolve_proxy_url))
    configure_guest_route_permits(permits)
    return permits
