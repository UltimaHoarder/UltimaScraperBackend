"""MPS guest session (visitor) allocation within existing proxy connection budgets."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import time
from contextvars import ContextVar
from collections.abc import Callable, Iterable
from typing import Any, cast

from .mps_tasks import mps_proxy_connection_limit

logger = logging.getLogger(__name__)


class GuestSessionPool:
    def __init__(
        self, route_capacities: dict[str, int], worker_index: int, worker_count: int,
    ) -> None:
        if worker_count < 1 or not 0 <= worker_index < worker_count:
            raise ValueError("Invalid guest session worker shard")
        # A proxy's connections are dealt round-robin, so together the workers
        # hold about max_connections visitor sessions. Every worker holds at
        # least one on every proxy, even one with fewer connections than there
        # are workers; the shared route permits then keep that proxy's
        # concurrent requests within max_connections across all of them.
        self.slots = {
            route: tuple(
                "guest-" + hashlib.sha256(f"{route}\n{slot}".encode()).hexdigest()
                for slot in range(worker_index, max(capacity, worker_index + 1), worker_count)
            )
            for route, capacity in route_capacities.items()
        }
        self._cursor: dict[str, int] = {}

    def select(self, site_api: Any, route: str) -> str:
        keys = self.slots.get(route)
        if not keys:
            raise ValueError("Guest route is outside the configured bounded session pool")
        cache = getattr(site_api, "_guest_auth_cache", {})
        pending = getattr(site_api, "_guest_auth_pending", {})
        start = self._cursor.get(route, 0) % len(keys)
        ordered = keys[start:] + keys[:start]

        def score(key: str) -> tuple[bool, int]:
            authed = cache.get(key)
            if authed is None:
                return False, int(pending.get(key, {}).get("waiters", 0))
            session = authed.auth_session
            limited = session.session_manager.is_route_rate_limited(
                session.get_rate_limit_key()
            )
            return limited, session.request_waiters

        selected = min(ordered, key=score)
        self._cursor[route] = (keys.index(selected) + 1) % len(keys)
        return selected


def configured_route_capacities(
    proxies: Iterable[Any], resolve_url: Callable[[str], str | None],
) -> dict[str, int]:
    capacities: dict[str, int] = {}
    for proxy in proxies:
        if not getattr(proxy, "enabled", True):
            continue
        capacity = mps_proxy_connection_limit(getattr(proxy, "max_connections", -1))
        route = resolve_url(str(proxy.url).strip())
        if not route:
            raise ValueError("Guest session pool could not resolve a configured route")
        capacities[route] = min(capacities.get(route, capacity), capacity)
    return capacities


def configure_guest_session_pool(
    site_api: Any, proxies: Iterable[Any], *, enabled: bool, worker_index: int, worker_count: int,
) -> bool:
    """Apply network.guest_session_pool; returns whether the pool is active."""
    if not enabled:
        return False
    from ..usapi_initializer import resolve_proxy_url

    try:
        site_api._guest_session_pool = GuestSessionPool(
            configured_route_capacities(proxies, resolve_proxy_url),
            worker_index, worker_count,
        )
    except ValueError as error:
        # e.g. an unresolvable proxy URL: keep the worker running with
        # unpooled guest sessions rather than failing startup.
        logger.warning("Guest session pool disabled: %s", error)
        return False
    return True


# API processes send few guest requests and keep a small fixed share of a
# proxy's budget; workers split the rest, so the setting stays a total per
# proxy across every process. The session manager adapts each worker's share
# downward on 429s and back up while the route stays clean.
INTERACTIVE_GUEST_ROUTE_RATE = 1.0


def guest_route_rate_share(limit: float, *, worker_count: int, worker_index: int | None) -> float | None:
    """This process's guest requests per second per proxy; None disables pacing.

    ``worker_index`` None means a non-worker (API) process.
    """
    if limit <= 0:
        return None
    # Every worker, including the interactive-first worker zero, takes bulk
    # work; only non-worker (API) processes keep the small interactive share.
    if worker_index is None:
        return min(INTERACTIVE_GUEST_ROUTE_RATE, limit)
    remaining = limit - INTERACTIVE_GUEST_ROUTE_RATE
    return max(remaining, INTERACTIVE_GUEST_ROUTE_RATE) / max(1, worker_count)


def configure_guest_route_rate_limit(
    limit: float, *, worker_count: int, worker_index: int | None = None,
) -> float | None:
    """Apply network.guest_route_rate_limit to this process's guest requests."""
    from ultima_scraper_api.managers.session_manager import configure_guest_route_rate

    rate = guest_route_rate_share(limit, worker_count=worker_count, worker_index=worker_index)
    configure_guest_route_rate(rate)
    return rate


# --- Shared guest (visitor) sessions ------------------------------------------
# Every guest request in a process (scrape tasks, profile search and refresh
# commands) resolves its visitor here: one cached login per route, or a pooled
# visitor when network.guest_session_pool is on.


def _proxy_log_label(raw_proxy_url: str | None) -> str:
    """Return a credential-free label for proxy-related log messages."""
    from .handlers.scrape.content import _mask_proxy_url

    return _mask_proxy_url(raw_proxy_url) or "direct"


def _normalize_proxy_url(raw_proxy_url: str | None) -> str | None:
    if not raw_proxy_url:
        return None
    proxy_url = str(raw_proxy_url).strip()
    return proxy_url or None

def _iter_site_auths(site_api: Any) -> list[Any]:
    auths = getattr(site_api, "auths", None)
    if isinstance(auths, dict):
        return list(cast(Any, auths).values())
    if isinstance(auths, list):
        return cast(list[Any], auths)
    return []

def _get_auth_proxy_url(authed: Any) -> str | None:
    details_getter = getattr(authed, "get_auth_details", None)
    if callable(details_getter):
        try:
            details = details_getter()
            if details is not None:
                return _normalize_proxy_url(getattr(details, "proxy_url", None))
        except Exception:
            return None
    return None


def _is_guest_auth(authed: Any) -> bool:
    return bool(getattr(authed, "guest", False))

_guest_auth_observer: ContextVar[Any] = ContextVar("guest_auth_observer", default=None)


def _observe_guest_auth(name: str, started: float | None = None) -> None:
    observer = _guest_auth_observer.get()
    if callable(observer):
        observer("guest_auth_" + name, 0.0 if started is None else
                 max(0.0, (time.monotonic() - started) * 1000.0))


def _guest_cache_key(proxy_url: str | None) -> str:
    return proxy_url or "__direct__"


def _guest_seed_id(cache_key: str) -> int:
    if cache_key.startswith("guest-"):
        return 1_000_000_000_000 + (
            int(hashlib.sha256(cache_key.encode()).hexdigest()[:15], 16)
        )
    digest = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
    return 9_000_000_000 + (int(digest[:8], 16) % 900_000_000)


GUEST_SHARED_INIT_TIMEOUT_SECONDS = 10.0


async def _resolve_guest_auth(
    site_api: Any, *, proxy_url: str | None, worker_id: str,
) -> Any | None:
    """A request owns its wait, while the route owns its bounded login."""
    normalized_proxy = _normalize_proxy_url(proxy_url)
    pool = getattr(site_api, "_guest_session_pool", None)
    session_key = pool.select(site_api, normalized_proxy) if pool and normalized_proxy else None
    key = session_key or _guest_cache_key(normalized_proxy)
    cache = getattr(site_api, "_guest_auth_cache", {})
    cached = cache.get(key)
    if cached and _is_guest_auth(cached) and _get_auth_proxy_url(cached) == normalized_proxy:
        _observe_guest_auth("cache_hit")
        return cached
    pending = getattr(site_api, "_guest_auth_pending", None)
    if not isinstance(pending, dict):
        pending = {}
        setattr(site_api, "_guest_auth_pending", pending)
    entry = pending.get(key)
    joined = entry is not None
    if entry is None:
        entry = {"observations": [], "reported": False, "waiters": 0}

        async def initialize() -> Any | None:
            # Detached initialization must not retain a cancelled task's telemetry
            # callback. One surviving waiter publishes these observations once.
            token = _guest_auth_observer.set(
                lambda name, ms: entry["observations"].append((name, ms))
            )
            try:
                async with asyncio.timeout(GUEST_SHARED_INIT_TIMEOUT_SECONDS):
                    return await _initialize_guest_auth(
                        site_api, proxy_url=normalized_proxy, worker_id=worker_id,
                        **({"session_key": session_key} if session_key else {}),
                    )
            except TimeoutError:
                entry["observations"].append(("guest_auth_shared_init_timeout", 0.0))
                return None
            finally:
                _guest_auth_observer.reset(token)
                if pending.get(key) is entry:
                    pending.pop(key, None)

        entry["task"] = asyncio.create_task(initialize())
        pending[key] = entry
    started = time.monotonic()
    entry["waiters"] = entry.get("waiters", 0) + 1
    try:
        result = await asyncio.shield(entry["task"])
    except asyncio.CancelledError:
        _observe_guest_auth("shared_wait_cancelled", started)
        raise
    finally:
        entry["waiters"] -= 1
    _observe_guest_auth("shared_wait", started)
    observer = _guest_auth_observer.get()
    if observer is not None and not entry["reported"]:
        entry["reported"] = True
        for name, ms in entry["observations"]:
            observer(name, ms)
    if joined and result is not None:
        _observe_guest_auth("cache_hit_after_shared_wait")
    return result


async def _initialize_guest_auth(
    site_api: Any,
    *,
    proxy_url: str | None,
    worker_id: str,
    session_key: str | None = None,
) -> Any | None:
    normalized_proxy = _normalize_proxy_url(proxy_url)
    cache_key = session_key or _guest_cache_key(normalized_proxy)

    cache_attr = "_guest_auth_cache"
    cache = getattr(site_api, cache_attr, None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(site_api, cache_attr, cache)
    cache = cast(dict[str, Any], cache)

    cached = cache.get(cache_key)
    if cached and _is_guest_auth(cached):
        cached_proxy = _get_auth_proxy_url(cached)
        if cached_proxy == normalized_proxy:
            _observe_guest_auth("cache_hit")
            return cached

    locks_attr = "_guest_auth_locks"
    locks = getattr(site_api, locks_attr, None)
    if not isinstance(locks, dict):
        locks = {}
        setattr(site_api, locks_attr, locks)
    locks = cast(dict[str, asyncio.Lock], locks)
    lock = locks.get(cache_key)
    if lock is None:
        lock = asyncio.Lock()
        locks[cache_key] = lock

    # Route assignment can send the same proxy to concurrent tasks in one
    # worker. Serialize only cache misses per route, then recheck after waiting
    # so one guest login/session is shared by all of them.
    wait_started = time.monotonic()
    try:
        await lock.acquire()
    except asyncio.CancelledError:
        _observe_guest_auth("lock_wait_cancelled", wait_started)
        raise
    _observe_guest_auth("lock_wait", wait_started)
    try:
        cached = cache.get(cache_key)
        if cached and _is_guest_auth(cached):
            cached_proxy = _get_auth_proxy_url(cached)
            if cached_proxy == normalized_proxy:
                _observe_guest_auth("cache_hit_after_lock")
                return cached

        for authed in _iter_site_auths(site_api):
            if not _is_guest_auth(authed):
                continue
            if session_key is not None and getattr(
                getattr(authed, "auth_session", None), "guest_session_key", None
            ) != session_key:
                continue
            if _get_auth_proxy_url(authed) == normalized_proxy:
                cache[cache_key] = authed
                _observe_guest_auth("existing_auth")
                return authed

        login_fn = getattr(site_api, "login", None)
        if not callable(login_fn):
            _observe_guest_auth("no_login_method")
            logger.error(
                "[%s] [guest_sessions] Site API has no login() for guest auth resolution",
                worker_id,
            )
            return None

        auth_json: dict[str, Any] = {"id": _guest_seed_id(cache_key)}
        if normalized_proxy:
            auth_json["proxy_url"] = normalized_proxy

        login_started = time.monotonic()
        try:
            result = login_fn(
                auth_json=auth_json, guest=True,
                **({"guest_session_key": session_key} if session_key else {}),
            )
            if not inspect.isawaitable(result):
                _observe_guest_auth("nonawaitable_login")
                logger.error(
                    "[%s] [guest_sessions] Guest login returned non-awaitable result",
                    worker_id,
                )
                return None
            authed = await result
        except asyncio.CancelledError:
            _observe_guest_auth("login_cancelled")
            raise
        except Exception as exc:
            _observe_guest_auth("login_timeout" if isinstance(exc, TimeoutError) else "login_error")
            logger.error(
                "[%s] [guest_sessions] Failed guest login (proxy=%s): %s",
                worker_id,
                _proxy_log_label(normalized_proxy),
                exc,
            )
            return None

        finally:
            _observe_guest_auth("login_wall", login_started)

        if authed is None:
            _observe_guest_auth("empty_login_result")
            return None

        _observe_guest_auth("login_success")
        cache[cache_key] = authed
        logger.debug(
            "[%s] [guest_sessions] Created guest auth for proxy=%s",
            worker_id,
            _proxy_log_label(normalized_proxy),
        )
        return authed
    finally:
        lock.release()


# One site API per site and process holds that process's guest visitors.
_GUEST_SITE_APIS: dict[str, Any] = {}


def register_guest_site_api(site_name: str, site_api: Any) -> None:
    """Workers register the site API they already run, so guests are shared."""
    _GUEST_SITE_APIS[site_name.lower()] = site_api


def guest_site_api(site_name: str) -> Any | None:
    """The process's site API for guest requests, created on first use."""
    key = site_name.lower()
    if key not in _GUEST_SITE_APIS:
        from ultima_scraper_api import FanslyAPI, OnlyFansAPI

        factory = {"onlyfans": OnlyFansAPI, "fansly": FanslyAPI}.get(key)
        if factory is None:
            return None
        _GUEST_SITE_APIS[key] = factory()
    return _GUEST_SITE_APIS[key]
