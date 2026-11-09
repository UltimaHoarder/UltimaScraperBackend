"""Main scrape task handler.

This module contains the main scrape_task_handler function that orchestrates
the scrape process including:
- Content scraping (posts, messages, stories, highlights)
- Subscription fetching and synchronization
- Progress event publishing
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import time
from contextlib import aclosing, nullcontext
from datetime import UTC, datetime, timezone
from typing import Any, cast

from ultima_scraper_api import ContentType
from ultima_scraper_api.apis.onlyfans.classes.auth_model import OnlyFansAuthModel

from ....database.write_executor import serialize_user_write_payload
from ....database.write_queue import DatabaseWriteOperation, stable_command_id

from ...events import OperationProgressEvent, publish_event
from ...guest_sessions import (
    _get_auth_proxy_url,
    _guest_auth_observer,
    _is_guest_auth,
    _normalize_proxy_url,
    _observe_guest_auth,
    _resolve_guest_auth,
)
from ...models import Task, TaskResult, TaskRunContext, TaskType
from ...payloads import DownloadTaskPayload, ScrapeTaskPayload
from .content import _mask_proxy_url, scrape_content_type, scrape_paid_content_feed
from .events import publish_hook_event
from .subscriptions import handle_get_subscriptions

__all__ = ["scrape_task_handler"]

logger = logging.getLogger(__name__)
DEFAULT_MPS_GUEST_ROUTE_TIMEOUT_SECONDS = 3.0
DEFAULT_MPS_GUEST_LOOKUP_TIMEOUT_SECONDS = 3.5

MPS_GUEST_ROUTE_TIMEOUT_SECONDS = float(
    os.environ.get(
        "ULTIMA_MPS_GUEST_ROUTE_TIMEOUT",
        DEFAULT_MPS_GUEST_ROUTE_TIMEOUT_SECONDS,
    )
)
# A 429 benches only the visitor session that hit it (OnlyFans limits each
# guest session); one more lookup on another session of the same route costs
# no extra route hop.
GUEST_SESSION_ATTEMPTS_PER_ROUTE = 2
MPS_GUEST_LOOKUP_TIMEOUT_SECONDS = float(
    os.environ.get(
        "ULTIMA_MPS_GUEST_LOOKUP_TIMEOUT",
        DEFAULT_MPS_GUEST_LOOKUP_TIMEOUT_SECONDS,
    )
)
MPS_GUEST_ROUTE_MAX_ATTEMPTS = int(
    os.environ.get("ULTIMA_MPS_GUEST_ROUTE_MAX_ATTEMPTS", 3)
)
MPS_GUEST_ROUTE_RETRY_DELAY_SECONDS = float(
    os.environ.get("ULTIMA_MPS_GUEST_ROUTE_RETRY_DELAY_SECONDS", 0.25)
)
# Start one bounded alternate-route hedge after typical sub-second lookups have
# had time to finish. Set the environment value to zero to preserve serial lookup.
MPS_GUEST_LOOKUP_HEDGE_DELAY_SECONDS = max(
    0.0,
    float(os.environ.get("ULTIMA_MPS_GUEST_LOOKUP_HEDGE_DELAY_SECONDS", 1.5)),
)
# Routes that repeatedly time out yield to healthier same-country reserves
# before lookup, or remain available as the final fallback without reserves.
MPS_GUEST_ROUTE_TIMEOUT_DEFER_THRESHOLD = 3
MPS_GUEST_ROUTE_TIMEOUT_DEFER_SECONDS = 30.0
MPS_GUEST_PROXY_AUTH_DEFER_SECONDS = 300.0
# Backward-compatibility alias
MPS_GUEST_LOOKUP_ROUTE_TIMEOUT_SECONDS = MPS_GUEST_LOOKUP_TIMEOUT_SECONDS


# ULTIMA_MPS_GUEST_PERMIT_BEFORE_TIMEOUT=1: an MPS lookup takes its proxy's
# connection permit before its timeout starts, as it already does its rate
# token. With few proxy connections, queueing for one is not a slow provider.
MPS_GUEST_PERMIT_BEFORE_TIMEOUT = (
    os.environ.get("ULTIMA_MPS_GUEST_PERMIT_BEFORE_TIMEOUT", "").strip() == "1"
)


def _resolve_mps_guest_lookup_timeout(
    context: TaskRunContext | None = None,
    route_proxy_url: str | None = None,
) -> float:
    """Resolve the guest lookup timeout from config settings, env var, or default.

    Checks the proxy's own override, then its group's
    (network.proxy_groups[proxy.group]), then global network.guest_lookup_timeout,
    then the ULTIMA_MPS_GUEST_LOOKUP_TIMEOUT environment variable, and defaults to 3.5s.
    """
    if context is not None:
        cfg = getattr(context, "config", None)
        if cfg is None and hasattr(context, "context") and isinstance(context.context, dict):
            cfg = context.context.get("config")
        if cfg is not None and hasattr(cfg, "settings") and hasattr(cfg.settings, "network"):
            from ultima_scraper_api.config import resolve_guest_lookup_timeout

            network = cfg.settings.network
            route_proxy = None
            if route_proxy_url and hasattr(network, "proxies") and network.proxies:
                route_masked = _mask_proxy_url(route_proxy_url)
                for p in network.proxies:
                    p_url = getattr(p, "url", None)
                    if p_url == route_proxy_url or (route_masked and _mask_proxy_url(p_url) == route_masked):
                        route_proxy = p
                        break
            configured = resolve_guest_lookup_timeout(network, route_proxy)
            if configured is not None:
                return configured

    return float(
        os.environ.get(
            "ULTIMA_MPS_GUEST_LOOKUP_TIMEOUT",
            DEFAULT_MPS_GUEST_LOOKUP_TIMEOUT_SECONDS,
        )
    )


def _resolve_mps_guest_route_timeout(
    context: TaskRunContext | None = None,
) -> float:
    """Resolve the guest route timeout from config settings, env var, or default.

    Checks global network.guest_route_timeout, then the ULTIMA_MPS_GUEST_ROUTE_TIMEOUT
    environment variable, and defaults to 3.0s.
    """
    if context is not None:
        cfg = getattr(context, "config", None)
        if cfg is None and hasattr(context, "context") and isinstance(context.context, dict):
            cfg = context.context.get("config")
        if cfg is not None and hasattr(cfg, "settings") and hasattr(cfg.settings, "network"):
            network = cfg.settings.network
            global_timeout = getattr(network, "guest_route_timeout", None)
            if global_timeout is not None and global_timeout > 0:
                return float(global_timeout)

    return float(
        os.environ.get(
            "ULTIMA_MPS_GUEST_ROUTE_TIMEOUT",
            DEFAULT_MPS_GUEST_ROUTE_TIMEOUT_SECONDS,
        )
    )


def _proxy_log_label(raw_proxy_url: str | None) -> str:
    """Return a credential-free label for proxy-related log messages."""

    return _mask_proxy_url(raw_proxy_url) or "direct"


def _route_telemetry_id(raw_proxy_url: str | None) -> str:
    """Return a stable route label without exposing proxy URL details."""

    normalized_proxy_url = _normalize_proxy_url(raw_proxy_url)
    if normalized_proxy_url is None:
        return "direct"
    digest = hashlib.sha256(normalized_proxy_url.encode("utf-8")).hexdigest()[:16]
    return f"route-{digest}"


MPS_GUEST_ROUTE_HEALTH_WINDOW = 32
MPS_GUEST_ROUTE_HEALTH_MIN_SAMPLES = 8
MPS_GUEST_ROUTE_HEALTH_MAX_AGE_SECONDS = 60.0
MPS_GUEST_ROUTE_HEALTH_TIMEOUT_RATIO = 0.5


def _mps_guest_route_state(site_api: Any) -> dict[str, dict[str, Any]]:
    """Get bounded worker-local health observations keyed by opaque route ID."""
    state_attr = "_mps_guest_route_timeout_state"
    state = getattr(site_api, state_attr, None)
    if not isinstance(state, dict):
        state = {}
        setattr(site_api, state_attr, state)
    return cast(dict[str, dict[str, Any]], state)


def _record_mps_guest_route_outcome(
    site_api: Any,
    proxy_url: str | None,
    outcome: str,
) -> None:
    if outcome == "proxy_auth_failure":
        route_state = _mps_guest_route_state(site_api)
        entry = route_state.setdefault(
            _route_telemetry_id(proxy_url),
            {"consecutive_timeouts": 0, "deferred_until": 0.0},
        )
        # Invalid proxy credentials need a longer recovery interval than a
        # transient socket timeout. Reuse existing same-country reserves.
        entry["deferred_until"] = time.monotonic() + MPS_GUEST_PROXY_AUTH_DEFER_SECONDS
        return
    if outcome not in {"found", "not_found", "lookup_timeout", "guest_init_failure"}:
        # Hedge cancellation and unrelated errors provide no timeout evidence.
        return
    route_state = _mps_guest_route_state(site_api)
    route_id = _route_telemetry_id(proxy_url)
    now = time.monotonic()
    entry = route_state.setdefault(route_id, {"consecutive_timeouts": 0, "deferred_until": 0.0})
    failed = outcome in {"lookup_timeout", "guest_init_failure"}
    recent = [(stamp, failure) for stamp, failure in entry.get("recent", [])
              if now - stamp <= MPS_GUEST_ROUTE_HEALTH_MAX_AGE_SECONDS]
    recent.append((now, failed))
    recent = recent[-MPS_GUEST_ROUTE_HEALTH_WINDOW:]
    entry["recent"] = recent
    count = int(entry.get("consecutive_timeouts", 0)) + 1 if failed else 0
    entry["consecutive_timeouts"] = count
    frequent_timeouts = (
        len(recent) >= MPS_GUEST_ROUTE_HEALTH_MIN_SAMPLES
        and sum(failure for _, failure in recent) / len(recent) >= MPS_GUEST_ROUTE_HEALTH_TIMEOUT_RATIO
    )
    if count >= MPS_GUEST_ROUTE_TIMEOUT_DEFER_THRESHOLD or frequent_timeouts:
        entry["deferred_until"] = now + MPS_GUEST_ROUTE_TIMEOUT_DEFER_SECONDS
        if count >= MPS_GUEST_ROUTE_TIMEOUT_DEFER_THRESHOLD:
            entry["consecutive_timeouts"] = 0
    elif not failed:
        # Recover when recent conclusive replies outweigh failures. One miss
        # must not erase evidence of frequent intermittent timeouts.
        entry["deferred_until"] = 0.0


def _mps_guest_route_is_deferred(
    site_api: Any,
    proxy_url: str | None,
    *,
    now: float | None = None,
) -> bool:
    route_state = _mps_guest_route_state(site_api)
    route_id = _route_telemetry_id(proxy_url)
    entry = route_state.get(route_id)
    if entry is None:
        return False

    deferred_until = float(entry.get("deferred_until", 0.0))
    if deferred_until <= (time.monotonic() if now is None else now):
        if deferred_until > 0:
            route_state.pop(route_id, None)
        return False
    return True


def _select_mps_guest_routes(
    site_api: Any,
    routes: list[str | None],
    alternatives: dict[str, list[str]],
) -> list[str | None]:
    """Replace deferred slots once, before any lookup or miss evidence exists.

    Keep direct placement and route count. Reserves are frozen same-country
    candidates; retries use only the chosen routes and retain slot identities.
    """
    selected = routes.copy()
    assigned = {url for url in routes if url is not None}
    for index, url in enumerate(routes):
        if url is None:
            continue
        reserves = alternatives.get(url, [])
        if not reserves or not _mps_guest_route_is_deferred(site_api, url):
            continue
        for raw_reserve in reserves:
            reserve = _normalize_proxy_url(raw_reserve)
            if reserve is None or reserve in assigned:
                continue
            if _mps_guest_route_is_deferred(site_api, reserve):
                continue
            selected[index] = reserve
            assigned.add(reserve)
            break
    return selected


def _select_mps_retry_routes(
    site_api: Any,
    configured_routes: list[tuple[int, str | None]],
    failed_routes: list[tuple[int, str | None]],
    definitive_misses: set[int],
    recovery_probed: set[int],
) -> list[tuple[int, str | None]]:
    """Give healthier alternatives one recovery probe before deferred retries.

    Preserve configured route identities and failed candidates. Repeated misses
    remain the same distinct-route evidence, never additional confirmations.
    """
    if not any(_mps_guest_route_is_deferred(site_api, url) for _, url in failed_routes):
        return failed_routes
    failed_indexes = {index for index, _ in failed_routes}
    alternatives = []
    for index, url in configured_routes:
        if index in failed_indexes or index in recovery_probed:
            continue
        if _mps_guest_route_is_deferred(site_api, url):
            continue
        # A previously conclusive route gets at most one recovery probe per
        # profile. Untried routes remain eligible without inventing new routes.
        if index in definitive_misses:
            recovery_probed.add(index)
        alternatives.append((index, url))
    return sorted(alternatives + failed_routes,
                  key=lambda route: _mps_guest_route_is_deferred(site_api, route[1]))


MPS_MAX_ROUTE_HOPS = 3
DEFAULT_MPS_GUEST_CONCLUSIVE_NOT_FOUND_THRESHOLD = 3
_raw_mps_conclusive_threshold = os.environ.get(
    "ULTIMA_MPS_GUEST_CONCLUSIVE_NOT_FOUND_THRESHOLD"
)
MPS_GUEST_CONCLUSIVE_NOT_FOUND_THRESHOLD = (
    int(_raw_mps_conclusive_threshold)
    if _raw_mps_conclusive_threshold is not None
    else None
)


def _build_guest_proxy_routes(
    *,
    guest_proxy_url: str | None,
    guest_fallback_proxy_urls: list[str],
    guest_direct_route_index: int | None,
) -> list[str | None]:
    """Build the ordered guest lookup routes, using ``None`` for direct."""

    proxy_routes: list[str] = []
    for raw_proxy_url in [guest_proxy_url, *guest_fallback_proxy_urls]:
        proxy_url = _normalize_proxy_url(raw_proxy_url)
        if proxy_url and proxy_url not in proxy_routes:
            proxy_routes.append(proxy_url)

    if guest_direct_route_index is None:
        return proxy_routes or [None]

    direct_route_index = min(guest_direct_route_index, len(proxy_routes))
    routes: list[str | None] = list(proxy_routes)
    routes.insert(direct_route_index, None)
    return routes


def _content_type_name(content_type: object | None) -> str:
    if hasattr(content_type, "value"):
        return str(getattr(content_type, "value"))
    if content_type is None:
        return "paid_content"
    return str(content_type)


def _coerce_user_id(raw_user_id: object) -> int:
    try:
        return int(str(raw_user_id).strip())
    except (TypeError, ValueError):
        return 0


def _normalize_username(raw_username: object) -> str | None:
    if raw_username is None:
        return None
    username = str(raw_username).strip()
    return username or None


def _resolve_identifier_user_id(
    *,
    user_id: int,
    username: str | None,
) -> int:
    if user_id > 0:
        return user_id
    return _coerce_user_id(username)


def _user_matches_identifier(
    user: Any,
    *,
    user_id: int,
    username: str | None,
) -> bool:
    if user is None:
        return False

    resolved_user_id = _resolve_identifier_user_id(
        user_id=user_id,
        username=username,
    )
    if (
        resolved_user_id > 0
        and _coerce_user_id(getattr(user, "id", 0)) == resolved_user_id
    ):
        return True

    normalized_username = _normalize_username(username)
    if normalized_username is None:
        return resolved_user_id <= 0

    candidate_names: set[str] = set()
    direct_username = _normalize_username(getattr(user, "username", None))
    if direct_username:
        candidate_names.add(direct_username.casefold())

    get_usernames = getattr(user, "get_usernames", None)
    if callable(get_usernames):
        try:
            aliases = get_usernames(ignore_id=True)
        except TypeError:
            aliases = get_usernames()
        except Exception:
            aliases = []
        if isinstance(aliases, list):
            for alias in cast(list[object], aliases):
                normalized_alias = _normalize_username(alias)
                if normalized_alias:
                    candidate_names.add(normalized_alias.casefold())

    return normalized_username.casefold() in candidate_names


async def _try_get_user(
    authed: Any, identifier: int | str, *, refresh: bool = False
) -> Any | None:
    get_user = getattr(authed, "get_user", None)
    if not callable(get_user):
        return None
    kwargs: dict[str, Any] = {}
    if refresh:
        kwargs["refresh"] = True
    if isinstance(authed, OnlyFansAuthModel):
        # Task lookups must not accumulate users or freshness timers on the auth.
        kwargs["cache"] = False
        if authed.guest:
            # Only authoritative misses may contribute to invalid-user writes.
            kwargs["strict"] = True
            session = authed.auth_session
            manager = getattr(session, "session_manager", None)
            wait_for_recovery = getattr(manager, "wait_for_route_recovery", None)
            if callable(wait_for_recovery):
                rate_key_fn = getattr(session, "get_rate_limit_key", session.get_route_key)
                route_key = rate_key_fn()
                if manager.is_route_rate_limited(route_key):
                    wait_started = time.monotonic()
                    try:
                        await wait_for_recovery(route_key)
                    finally:
                        _observe_guest_auth("route_rate_limit_wait", wait_started)
    try:
        result = get_user(identifier, **kwargs)
    except TypeError:
        result = get_user(str(identifier), **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _resolve_user_from_chats(
    authed: Any,
    *,
    user_id: int,
    username: str | None,
    worker_id: str,
) -> Any | None:
    normalized_username = _normalize_username(username)
    if isinstance(authed, OnlyFansAuthModel):
        lookups: list[tuple[str | None, int | None]] = (
            [(normalized_username, 50)] if normalized_username else []
        )
        lookups.append((None, None))
        for query, limit in lookups:
            try:
                async with aclosing(authed.iter_chats(query=query, limit=limit)) as chats:
                    async for chat in chats:
                        if _user_matches_identifier(
                            chat.user, user_id=user_id, username=normalized_username
                        ):
                            return chat.user
            except Exception as exc:
                logger.debug(
                    "[%s] [scrape_handler] chat lookup failed for user=%s query=%s: %s",
                    worker_id,
                    user_id,
                    query,
                    exc,
                )
        return None

    search_chats = getattr(authed, "search_chats", None)
    if normalized_username and callable(search_chats):
        try:
            chats_result = search_chats(
                query=normalized_username,
                limit=50,
                offset=0,
            )
            chats = (
                await chats_result
                if inspect.isawaitable(chats_result)
                else chats_result
            )
        except Exception as exc:
            logger.debug(
                "[%s] [scrape_handler] search_chats lookup failed for '%s': %s",
                worker_id,
                normalized_username,
                exc,
            )
        else:
            if isinstance(chats, list):
                for chat in cast(list[object], chats):
                    candidate_user = getattr(chat, "user", None)
                    if _user_matches_identifier(
                        candidate_user,
                        user_id=user_id,
                        username=normalized_username,
                    ):
                        return candidate_user

    get_chats = getattr(authed, "get_chats", None)
    if callable(get_chats):
        try:
            chats_result = get_chats(limit=100, offset=0)
            chats = (
                await chats_result
                if inspect.isawaitable(chats_result)
                else chats_result
            )
        except Exception as exc:
            logger.debug(
                "[%s] [scrape_handler] get_chats lookup failed for user=%s username=%s: %s",
                worker_id,
                user_id,
                normalized_username,
                exc,
            )
        else:
            if isinstance(chats, list):
                for chat in cast(list[object], chats):
                    candidate_user = getattr(chat, "user", None)
                    if _user_matches_identifier(
                        candidate_user,
                        user_id=user_id,
                        username=normalized_username,
                    ):
                        return candidate_user

    return None


async def _resolve_scrape_user(
    authed: Any,
    *,
    user_id: int,
    username: str | None,
    worker_id: str,
) -> Any | None:
    normalized_username = _normalize_username(username)
    resolved_user_id = _resolve_identifier_user_id(
        user_id=user_id,
        username=normalized_username,
    )
    username_repeats_explicit_id = user_id > 0 and normalized_username == str(
        resolved_user_id
    )
    find_user = getattr(authed, "find_user", None)

    if resolved_user_id > 0 and callable(find_user):
        try:
            candidate = find_user(resolved_user_id)
        except Exception as exc:
            logger.debug(
                "[%s] [scrape_handler] find_user(%s) failed: %s",
                worker_id,
                resolved_user_id,
                exc,
            )
        else:
            if _user_matches_identifier(
                candidate,
                user_id=resolved_user_id,
                username=normalized_username,
            ):
                return candidate

    if resolved_user_id > 0:
        candidate = await _try_get_user(authed, str(resolved_user_id))
        if _user_matches_identifier(
            candidate,
            user_id=resolved_user_id,
            username=normalized_username,
        ):
            return candidate

        # MPS supplies the numeric ID in both fields because Task requires a
        # display username. Once the authoritative ID lookup returns no user,
        # retrying the same value as a username and then scanning chats cannot
        # improve the result. Guest chat endpoints can also block for many
        # seconds, turning a definitive not-found response into a route timeout.
        if username_repeats_explicit_id:
            return None

    if normalized_username:
        candidate = await _try_get_user(authed, normalized_username)
        if _user_matches_identifier(
            candidate,
            user_id=resolved_user_id,
            username=normalized_username,
        ):
            return candidate

    return await _resolve_user_from_chats(
        authed,
        user_id=resolved_user_id,
        username=normalized_username,
        worker_id=worker_id,
    )


def _release_paid_content_working_set(authed: Any) -> None:
    """Release paid-feed objects once result rows and download tasks exist."""
    paid_items = authed.paid_content
    author_ids: set[int] = set()
    for item in paid_items:
        author_id = _coerce_user_id(item.get_author().id)
        if author_id > 0:
            author_ids.add(author_id)
    paid_items.clear()

    for author_id in author_ids:
        authed.release_user(author_id)

    authed.cache.paid_content.deactivate()


async def _fetch_profile_socials(
    user: Any,
    site_api: Any,
    *,
    authed: Any,
    user_id: int,
    uses_guest_auth: bool,
    guest_proxy_routes: list[str | None],
    worker_id: str,
) -> list[dict[str, Any]]:
    """Fetch social links for a profile, failing over across guest proxy routes if needed."""
    from ....database.helpers.user_sync import _fetch_socials

    if not uses_guest_auth or site_api is None:
        return await _fetch_socials(user)

    endpoint_fn = getattr(site_api, "endpoint_links", None)
    socials_url = endpoint_fn(user_id).socials if callable(endpoint_fn) else None
    if not socials_url:
        return await _fetch_socials(user)

    failed_route_urls: set[str | None] = set()
    current_proxy = _get_auth_proxy_url(authed)
    normalized_current = _normalize_proxy_url(current_proxy)

    # Attempt 1: Try with primary route/session
    primary_requester = getattr(user, "get_requester", None)
    primary_session = (
        primary_requester()
        if callable(primary_requester)
        else getattr(authed, "auth_session", None)
    )

    if primary_session is not None and hasattr(primary_session, "json_request"):
        try:
            res = await primary_session.json_request(socials_url)
            if isinstance(res, list):
                return [item for item in res if isinstance(item, dict)]
            logger.warning(
                "[%s] [scrape_handler] Primary route %s failed to get socials for user %s (%s); trying fallback routes",
                worker_id,
                _proxy_log_label(current_proxy),
                user_id,
                res.get("error") if isinstance(res, dict) else res,
            )
            failed_route_urls.add(normalized_current)
        except Exception as exc:
            logger.warning(
                "[%s] [scrape_handler] Primary route %s error getting socials for user %s: %s; trying fallback routes",
                worker_id,
                _proxy_log_label(current_proxy),
                user_id,
                exc,
            )
            failed_route_urls.add(normalized_current)
    else:
        failed_route_urls.add(normalized_current)

    # Attempt 2+: Fail over across assigned fallback routes
    for fallback_proxy_url in guest_proxy_routes:
        normalized_fallback = _normalize_proxy_url(fallback_proxy_url)
        if normalized_fallback in failed_route_urls:
            continue
        try:
            fallback_authed = await _resolve_guest_auth(
                site_api,
                proxy_url=fallback_proxy_url,
                worker_id=worker_id,
            )
            if fallback_authed is None:
                failed_route_urls.add(normalized_fallback)
                continue
            fallback_session = getattr(fallback_authed, "auth_session", None)
            if fallback_session is None or not hasattr(fallback_session, "json_request"):
                failed_route_urls.add(normalized_fallback)
                continue
            res = await fallback_session.json_request(socials_url)
            if isinstance(res, list):
                logger.info(
                    "[%s] [scrape_handler] Successfully retrieved socials for user %s via fallback route %s",
                    worker_id,
                    user_id,
                    _proxy_log_label(fallback_proxy_url),
                )
                return [item for item in res if isinstance(item, dict)]
            failed_route_urls.add(normalized_fallback)
        except Exception as exc:
            logger.debug(
                "[%s] [scrape_handler] Fallback route %s failed socials for user %s: %s",
                worker_id,
                _proxy_log_label(fallback_proxy_url),
                user_id,
                exc,
            )
            failed_route_urls.add(normalized_fallback)

    logger.warning(
        "[%s] [scrape_handler] Could not retrieve socials for user %s across all available guest routes",
        worker_id,
        user_id,
    )
    return []


def _prepare_profile_completion(context, result):
    callback = context.context.get("prepare_profile_completion")
    if callable(callback):
        callback(result)


def _scrape_success_result(*, user_id, content_type_str, auth_username,
        items_scraped, items_saved, media_urls, auth_id, username_str,
        media_selection, resolved_route_url, task_proxy_routes,
        resolved_route_index, worker_id, scrape_errors):
    message = (
        f"Scraped {user_id} ({content_type_str}) as {auth_username}: "
        f"scraped={items_scraped}, saved={items_saved}"
    )

    # Store only the fields the download handler actually reads
    # (media_id, url, content_id) to avoid bloating Redis with
    # full signed CDN URLs, DRM keys, and other metadata that can
    # reach 5 MB+ per task.
    slim_media_urls: list[dict[str, str | int]] = [
        {
            k: v
            for k, v in (
                ("media_id", entry.get("media_id")),
                ("url", entry.get("url")),
                ("content_id", entry.get("content_id")),
            )
            if v is not None
        }
        for entry in (media_urls or [])
        if entry.get("media_id") is not None
    ]

    return TaskResult(
        success=True,
        message=message,
        output={
            "user_id": user_id,
            "auth_id": auth_id,
            "username": username_str,
            "content_type": content_type_str,
            "items_scraped": items_scraped,
            "items_saved": items_saved,
            "items_total": items_scraped,
            "media_urls": slim_media_urls,
            "media_selection": media_selection,
            "proxy_url": resolved_route_url,
            "proxy_routes": task_proxy_routes,
            "proxy_hop_used": resolved_route_index,
        },
        metrics={
            "items_scraped": items_scraped,
            "items_saved": items_saved,
            "items_processed": items_scraped,
            "items_total": items_scraped,
            "total_items": items_scraped,
            "worker_id": worker_id,
            "proxy_url": resolved_route_url,
            "proxy_routes": task_proxy_routes,
            "proxy_hop_used": resolved_route_index,
        },
        errors=scrape_errors,
    )


async def _mark_user_invalid(
    *,
    task: Task,
    context: TaskRunContext,
    user_id: int,
    reason: str = "not_found",
) -> None:
    site_key = (task.site_name or context.site or "").lower()
    if not site_key:
        raise RuntimeError("Cannot persist an invalid user without a site")
    if context.database_writes is None:
        raise RuntimeError("Database writer is unavailable")
    checked_at = datetime.now(timezone.utc).isoformat()
    release_fetch_capacity = context.context.get("release_fetch_capacity")
    if callable(release_fetch_capacity):
        release_fetch_capacity()
    await context.database_writes.submit(
        DatabaseWriteOperation.UPSERT_INVALID_USER,
        {
            "user_id": int(user_id),
            "reason": reason,
            "checked_at": checked_at,
        },
        site=site_key,
        command_id=stable_command_id(
            # An expired attempt must replay the first committed check. New
            # tasks have different IDs and still persist a fresh checked_at.
            "invalid-user-v3",
            task.task_id,
            site_key,
            user_id,
            reason,
        ),
    )


async def scrape_task_handler(task: Task, context: TaskRunContext) -> TaskResult:
    """
    Execute a scrape task using USAPI.

    Expected task fields:
    - user_id: Numeric user ID to scrape (must be positive integer)
    - content_type: Type of content ("profile", "posts", "messages", etc.)
    - auth_id: Optional - specific auth account ID to use (if null, iterates through all auths)
    - payload: Additional scraping configuration
      - max_items: Maximum number of items to scrape (REQUIRED - no default)

    Note: user_id must be a positive integer. Use resolve_user_identifier() to convert
    usernames to user IDs before creating tasks.

    Args:
        task: Task to execute
        context: Execution context containing job, storage, and worker context

    Returns:
        TaskResult with scrape outcome
    """
    transient_user: Any | None = None
    transient_auth: Any | None = None
    try:
        if context.database_writes is None:
            raise RuntimeError("Database writer is unavailable")
        user_id = _coerce_user_id(task.user_id)
        content_type = task.content_type
        payload = task.payload
        site_key = (task.site_name or context.site or "").strip().lower()
        if not site_key:
            raise RuntimeError("Scrape task is missing a database site")
        is_paid_content_feed = (
            isinstance(payload, ScrapeTaskPayload) and payload.paid_content_feed
        )

        if content_type is None and not is_paid_content_feed:
            error_msg = "Task.content_type is required for SCRAPE tasks"
            logger.error(f"[scrape_handler] {error_msg}")
            return TaskResult(success=False, message=error_msg, errors=[error_msg])

        auth_id = task.auth_id
        worker_id = context.worker_id or "unknown"

        # Special case: Get subscriptions for authenticated account
        if content_type == ContentType.SUBSCRIPTIONS:
            logger.info(
                f"[{worker_id}] [scrape_handler] Starting subscription scrape: auth_id={auth_id}"
            )
            return await handle_get_subscriptions(task, context)

        # Validate max_items exists in payload
        if not task.payload or not hasattr(task.payload, "max_items"):
            error_msg = "max_items is required in task payload"
            logger.error(f"[scrape_handler] {error_msg}")
            return TaskResult(success=False, message=error_msg, errors=[error_msg])

        max_items = getattr(payload, "max_items")  # type: ignore[no-any-return]
        uses_guest_auth = (
            isinstance(payload, ScrapeTaskPayload) and payload.use_guest_auth
        )
        guest_proxy_url = (
            _normalize_proxy_url(payload.guest_proxy_url)
            if isinstance(payload, ScrapeTaskPayload)
            else None
        )
        guest_fallback_proxy_urls = (
            [
                normalized
                for raw_fallback in getattr(payload, "guest_fallback_proxy_urls", [])
                or []
                if (normalized := _normalize_proxy_url(raw_fallback))
                and normalized != guest_proxy_url
            ]
            if isinstance(payload, ScrapeTaskPayload)
            else []
        )
        guest_direct_route_index = (
            payload.guest_direct_route_index
            if isinstance(payload, ScrapeTaskPayload)
            else None
        )
        guest_proxy_routes = (
            _build_guest_proxy_routes(
                guest_proxy_url=guest_proxy_url,
                guest_fallback_proxy_urls=guest_fallback_proxy_urls,
                guest_direct_route_index=guest_direct_route_index,
            )
            if uses_guest_auth
            else []
        )
        track_invalid_user = (
            bool(payload.track_invalid_user)
            if isinstance(payload, ScrapeTaskPayload)
            else False
        )


        # Get site API instance
        site_api = context.site_api
        if not site_api:
            error_msg = "No API instance available - worker not properly initialized"
            logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
            return TaskResult(
                success=False, message=error_msg, errors=["Missing API context"]
            )

        authed = None
        resolved_route_url: str | None = None
        resolved_route_index: int | None = None
        if uses_guest_auth:
            auth_id = None
            if is_paid_content_feed:
                for route_index, route_proxy_url in enumerate(guest_proxy_routes):
                    try:
                        authed = await asyncio.wait_for(
                            _resolve_guest_auth(
                                site_api,
                                proxy_url=route_proxy_url,
                                worker_id=worker_id,
                            ),
                            timeout=_resolve_mps_guest_route_timeout(context),
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "[%s] [scrape_handler] Guest route %s failed to initialize for paid feed (%s)",
                            worker_id,
                            _proxy_log_label(route_proxy_url),
                            type(exc).__name__,
                        )
                        authed = None
                    if authed is not None:
                        resolved_route_url = route_proxy_url
                        resolved_route_index = route_index
                        break
                if authed is None:
                    error_msg = (
                        "Failed to initialize guest auth for scrape task across "
                        f"{len(guest_proxy_routes)} proxy route(s)"
                    )
                    logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
                    return TaskResult(
                        success=False,
                        message=error_msg,
                        errors=[error_msg],
                        output={
                            "proxy_url": None,
                            "proxy_routes": guest_proxy_routes,
                            "proxy_hop_used": None,
                        },
                        metrics={
                            "proxy_url": None,
                            "proxy_routes": guest_proxy_routes,
                            "proxy_hop_used": None,
                        },
                    )
        else:
            if auth_id is None:
                error_msg = "SCRAPE tasks require auth_id (preflight should fan out per-auth tasks)."
                logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
                return TaskResult(success=False, message=error_msg, errors=[error_msg])

            raw_auth_id = auth_id
            normalized_auth_id = auth_id
            if context.db_managers is not None:
                try:
                    db_manager = context.db_managers.get(task.site_name.lower())
                    if db_manager is not None:
                        auth_record = await asyncio.to_thread(
                            db_manager.get_user_auth, auth_id
                        )
                        if auth_record and auth_record.user_id:
                            normalized_auth_id = int(auth_record.user_id)
                            if normalized_auth_id != raw_auth_id:
                                logger.info(
                                    f"[{worker_id}] [scrape_handler] Normalized auth_id "
                                    f"{raw_auth_id} -> {normalized_auth_id} for site {task.site_name}"
                                )
                except Exception as auth_resolve_exc:
                    logger.warning(
                        f"[{worker_id}] [scrape_handler] Failed auth_id normalization "
                        f"for {auth_id}: {auth_resolve_exc}"
                    )

            # Canonical auth_id on tasks should be users.id for DB writes/reads.
            auth_id = normalized_auth_id
            task.auth_id = normalized_auth_id

            lookup_auth_ids: list[int] = []
            for candidate_auth_id in (raw_auth_id, normalized_auth_id):
                if candidate_auth_id not in lookup_auth_ids:
                    lookup_auth_ids.append(candidate_auth_id)

            for lookup_auth_id in lookup_auth_ids:
                authed = site_api.find_auth(lookup_auth_id)
                if authed:
                    break
            if not authed:
                error_msg = f"Auth ID {auth_id} not found"
                logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
                return TaskResult(success=False, message=error_msg, errors=[error_msg])
            resolved_route_url = _get_auth_proxy_url(authed)
            resolved_route_index = 0 if resolved_route_url is not None else None

        if is_paid_content_feed:
            task_proxy_routes = (
                guest_proxy_routes
                if uses_guest_auth
                else ([resolved_route_url] if resolved_route_url is not None else [])
            )
            username_str = (
                payload.paid_content_performer
                or f"paid-feed:{getattr(authed, 'username', auth_id or 'unknown')}"
            )
            content_type_str = "paid_content"
            expected_total_hint = max_items if max_items and max_items > 0 else None

            started_event = OperationProgressEvent(
                event="scrape_progress",
                job_id=context.job.job_id,
                task_id=task.task_id,
                username=username_str,
                user_id=user_id,
                content_type=content_type_str,
                status="started",
                items_scraped=0,
                items_saved=0,
                total_items=expected_total_hint,
                worker_id=worker_id,
            )

            await publish_hook_event(
                context.storage,
                publish_event(started_event),
                "started scrape_progress event",
            )
            task.result = TaskResult(
                success=True,
                message="Scrape started",
                metrics={
                    "items_processed": 0,
                    "items_total": expected_total_hint or 0,
                    "items_scraped": 0,
                    "total_items": expected_total_hint or 0,
                    "items_saved": 0,
                    "proxy_url": resolved_route_url,
                    "proxy_routes": task_proxy_routes,
                    "proxy_hop_used": resolved_route_index,
                },
                output={
                    "proxy_url": resolved_route_url,
                    "proxy_routes": task_proxy_routes,
                    "proxy_hop_used": resolved_route_index,
                },
                errors=[],
            )
            await context.storage.persist_task(
                job_id=context.job.job_id,
                task=task,
            )

            try:

                async def persist_scrape_progress_update(
                    completed_pages: int, total_pages: int, items_so_far: int
                ) -> None:
                    items_scraped_metric = max(0, int(items_so_far))
                    items_total_metric = max(
                        items_scraped_metric,
                        int(expected_total_hint or 0),
                    )
                    task.result = TaskResult(
                        success=True,
                        message=(
                            f"Scrape in progress: {items_scraped_metric}/{items_total_metric}"
                            if items_total_metric > 0
                            else f"Scrape in progress: {items_scraped_metric}"
                        ),
                        metrics={
                            "items_processed": items_scraped_metric,
                            "items_total": items_total_metric,
                            "items_scraped": items_scraped_metric,
                            "total_items": items_total_metric,
                            "items_saved": (
                                int((task.result.metrics or {}).get("items_saved", 0))
                                if task.result
                                else 0
                            ),
                            "proxy_url": resolved_route_url,
                            "proxy_routes": task_proxy_routes,
                            "proxy_hop_used": resolved_route_index,
                        },
                        output={
                            "proxy_url": resolved_route_url,
                            "proxy_routes": task_proxy_routes,
                            "proxy_hop_used": resolved_route_index,
                        },
                        errors=[],
                    )
                    await context.storage.persist_task(
                        job_id=context.job.job_id,
                        task=task,
                    )

                async def persist_save_progress_update(
                    processed: int, total: int, saved: int
                ) -> None:
                    items_scraped_metric = max(0, int(total))
                    items_saved_metric = max(0, int(saved))
                    items_total_metric = max(items_scraped_metric, items_saved_metric)
                    task.result = TaskResult(
                        success=True,
                        message=(
                            f"Save in progress: {items_saved_metric}/{items_total_metric}"
                            if items_total_metric > 0
                            else f"Save in progress: {items_saved_metric}"
                        ),
                        metrics={
                            "items_processed": items_saved_metric,
                            "items_total": items_total_metric,
                            "items_scraped": items_scraped_metric,
                            "total_items": items_total_metric,
                            "items_saved": items_saved_metric,
                            "proxy_url": resolved_route_url,
                            "proxy_routes": task_proxy_routes,
                            "proxy_hop_used": resolved_route_index,
                        },
                        output={
                            "proxy_url": resolved_route_url,
                            "proxy_routes": task_proxy_routes,
                            "proxy_hop_used": resolved_route_index,
                        },
                        errors=[],
                    )
                    await context.storage.persist_task(
                        job_id=context.job.job_id,
                        task=task,
                    )

                from uuid import uuid4

                media_selection = {
                    "selection_id": uuid4().hex,
                    "job_id": context.job.job_id,
                    "task_id": task.task_id,
                    "site": site_key,
                    "auth_id": auth_id,
                }
                (
                    items_fetched,
                    items_scraped,
                    items_saved,
                    scrape_errors,
                    media_urls,
                    download_targets,
                ) = await scrape_paid_content_feed(
                    authed,
                    max_items=max_items,
                    performer_filter=payload.paid_content_performer,
                    paid_content_types=payload.paid_content_types,
                    auth_user_id=auth_id,
                    worker_id=worker_id,
                    job_id=context.job.job_id,
                    rescrape_existing=(
                        bool(payload.rescrape_existing)
                        if isinstance(payload, ScrapeTaskPayload)
                        else True
                    ),
                    database_writes=context.database_writes,
                    site=site_key,
                    media_selection=media_selection,
                    on_scrape_progress=persist_scrape_progress_update,
                    on_save_progress=persist_save_progress_update,
                )

                new_tasks: list[Task] = []
                if payload.download_media:
                    new_tasks.append(Task(
                        site_name=task.site_name,
                        user_id=int(authed.id),
                        user_username=username_str,
                        content_type=None,
                        task_type=TaskType.DOWNLOAD,
                        payload=DownloadTaskPayload(
                            paid_content_feed=True,
                            user_username=username_str,
                            ignore_space_requirements=bool(payload.ignore_space_requirements),
                        ),
                        auth_id=auth_id,
                        depends_on=[task.task_id],
                    ))

                completed_event = OperationProgressEvent(
                    event="scrape_progress",
                    job_id=context.job.job_id,
                    task_id=task.task_id,
                    username=username_str,
                    user_id=user_id,
                    content_type=content_type_str,
                    status="completed",
                    items_scraped=items_scraped,
                    items_saved=items_saved,
                    total_items=items_scraped,
                    worker_id=worker_id,
                )

                await publish_hook_event(
                    context.storage,
                    publish_event(completed_event),
                    "completed scrape_progress event",
                )

                return TaskResult(
                    success=True,
                    message=(
                        f"Scraped paid content feed as {getattr(authed, 'username', 'unknown')}: "
                        f"fetched={items_fetched}, scraped={items_scraped}, saved={items_saved}"
                    ),
                    output={
                        "user_id": user_id,
                        "auth_id": auth_id,
                        "username": username_str,
                        "content_type": content_type_str,
                        "items_fetched": items_fetched,
                        "items_scraped": items_scraped,
                        "items_saved": items_saved,
                        "media_selection": media_selection,
                        "error_count": media_selection["error_count"],
                        "proxy_url": resolved_route_url,
                        "proxy_routes": task_proxy_routes,
                        "proxy_hop_used": resolved_route_index,
                    },
                    metrics={
                        "items_fetched": items_fetched,
                        "items_scraped": items_scraped,
                        "items_saved": items_saved,
                        "proxy_url": resolved_route_url,
                        "proxy_routes": task_proxy_routes,
                        "proxy_hop_used": resolved_route_index,
                    },
                    errors=scrape_errors,
                    new_tasks=new_tasks,
                )
            except Exception as exc:
                logger.warning(
                    f"[{worker_id}] [scrape_handler] Paid feed scrape failed: {exc}"
                )

                failed_event = OperationProgressEvent(
                    event="scrape_progress",
                    job_id=context.job.job_id,
                    task_id=task.task_id,
                    username=username_str,
                    user_id=user_id,
                    content_type=content_type_str,
                    status="failed",
                    items_scraped=0,
                    items_saved=0,
                    total_items=0,
                    worker_id=worker_id,
                )

                await publish_hook_event(
                    context.storage,
                    publish_event(failed_event),
                    "failed scrape_progress event",
                )

                raise
            finally:
                if not callable(getattr(authed, "iter_paid_content", None)):
                    _release_paid_content_working_set(authed)

        normalized_task_username = _normalize_username(task.user_username)
        guest_route_init_failures = 0
        guest_lookup_failures = 0
        guest_lookup_routes: set[int] = set()
        definitive_not_found_routes: set[int] = set()
        if uses_guest_auth:
            user = None
            last_guest_lookup_label: str | None = None
            last_guest_route_failure: str | None = None
            fallback_route_count = max(len(guest_proxy_routes) - 1, 0)
            is_mps_lookup = track_invalid_user and content_type == ContentType.PROFILE
            if is_mps_lookup:
                guest_proxy_routes = _select_mps_guest_routes(
                    site_api,
                    guest_proxy_routes,
                    payload.guest_proxy_alternatives,
                )
            effective_conclusive_threshold = (
                payload.conclusive_miss_threshold
                if payload.conclusive_miss_threshold is not None
                else MPS_GUEST_CONCLUSIVE_NOT_FOUND_THRESHOLD
            )
            max_attempts = MPS_GUEST_ROUTE_MAX_ATTEMPTS if is_mps_lookup else 1
            configured_routes = list(enumerate(guest_proxy_routes))
            pending_routes = configured_routes.copy()
            recovery_probed: set[int] = set()
            record_route_timing = context.context.get("record_route_timing")
            record_route_observation = context.context.get("record_route_observation")
            record_handler_timing = context.context.get("record_handler_timing")

            def finish_route_timing(
                started: float,
                route_proxy_url: str | None,
                *,
                guest_init_ms: float,
                lookup_ms: float,
                lookup_observed: bool,
                lookup_outcome: str,
            ) -> None:
                route_active_ms = max(0.0, (time.monotonic() - started) * 1000.0)
                if callable(record_route_timing):
                    record_route_timing(route_active_ms)
                if callable(record_route_observation):
                    record_route_observation(
                        _route_telemetry_id(route_proxy_url),
                        route_active_ms,
                        guest_init_ms,
                        lookup_ms,
                        lookup_observed,
                        lookup_outcome,
                    )

            def finish_handler_timing(name: str, started: float) -> float:
                duration_ms = max(0.0, (time.monotonic() - started) * 1000.0)
                if callable(record_handler_timing):
                    record_handler_timing(name, duration_ms)
                return duration_ms

            async def run_guest_route(
                route_index: int,
                route_proxy_url: str | None,
                *,
                internal_cancel_event: asyncio.Event | None = None,
                lookup_started_event: asyncio.Event | None = None,
                selection_role: str = "fallback",
            ) -> dict[str, Any]:
                record_selection = context.context.get("record_route_selection")
                if is_mps_lookup and callable(record_selection):
                    record_selection(_route_telemetry_id(route_proxy_url), selection_role,
                                     _mps_guest_route_is_deferred(site_api, route_proxy_url), attempt > 0)
                route_timing_started = time.monotonic()
                route_guest_init_ms = 0.0
                route_lookup_ms = 0.0
                route_lookup_observed = False
                route_authed: Any | None = None
                route_user: Any | None = None
                lookup_started: float | None = None
                lookup_outcome = "guest_init_failure"
                propagate_cancellation = False

                auth_observer_token = _guest_auth_observer.set(record_handler_timing)
                init_started = time.monotonic()
                try:
                    resolve_auth = _resolve_guest_auth(
                        site_api,
                        proxy_url=route_proxy_url,
                        worker_id=worker_id,
                    )
                    if is_mps_lookup:
                        route_authed = await asyncio.wait_for(
                            resolve_auth,
                            timeout=_resolve_mps_guest_route_timeout(context),
                        )
                    else:
                        route_authed = await resolve_auth
                except asyncio.CancelledError:
                    route_guest_init_ms = finish_handler_timing(
                        "guest_init", init_started
                    )
                    lookup_outcome = "guest_init_cancelled"
                    propagate_cancellation = not (
                        internal_cancel_event is not None
                        and internal_cancel_event.is_set()
                    )
                except Exception as exc:  # noqa: BLE001
                    route_guest_init_ms = finish_handler_timing(
                        "guest_init", init_started
                    )
                    lookup_outcome = "guest_init_failure"
                    _observe_guest_auth("route_init_timeout" if isinstance(exc, TimeoutError) else "route_init_error")
                    logger.warning(
                        "[%s] [scrape_handler] Guest route %s failed to initialize "
                        "for user %s (%s)",
                        worker_id,
                        _proxy_log_label(route_proxy_url),
                        user_id or normalized_task_username,
                        type(exc).__name__,
                    )
                else:
                    route_guest_init_ms = finish_handler_timing(
                        "guest_init", init_started
                    )
                    if route_authed is None:
                        lookup_outcome = "guest_init_failure"
                    else:
                        guest_lookup_routes.add(route_index)
                        route_lookup_observed = True
                        if lookup_started_event is not None:
                            lookup_started_event.set()
                        for session_attempt in range(GUEST_SESSION_ATTEMPTS_PER_ROUTE):
                            retry_session = False
                            lookup = _resolve_scrape_user(
                                route_authed,
                                user_id=user_id,
                                username=normalized_task_username,
                                worker_id=worker_id,
                            )
                            try:
                                if is_mps_lookup:
                                    # Wait for the route's request budget before the
                                    # provider timeout starts; the lookup's first
                                    # request then uses the token taken here.
                                    route_session = getattr(route_authed, "auth_session", None)
                                    prepay = getattr(route_session, "prepay_route_rate", None)
                                    prepaid = None
                                    if callable(prepay):
                                        prepay_started = time.monotonic()
                                        try:
                                            prepaid = await prepay()
                                        except BaseException:
                                            lookup.close()  # Never started; no request was sent.
                                            raise
                                        # Time queued for the route's request budget,
                                        # reported under guest_auth_stats.
                                        finish_handler_timing(
                                            "guest_auth_route_rate_wait", prepay_started
                                        )
                                    lookup_started = time.monotonic()
                                    lookup_timeout = _resolve_mps_guest_lookup_timeout(
                                        context, route_proxy_url
                                    )
                                    hold_permit = (
                                        getattr(route_session, "hold_route_permit", None)
                                        if MPS_GUEST_PERMIT_BEFORE_TIMEOUT
                                        else None
                                    )
                                    try:
                                        permit_started = time.monotonic()
                                        async with (
                                            hold_permit() if callable(hold_permit)
                                            else nullcontext()
                                        ):
                                            if callable(hold_permit):
                                                # Queued for a connection of the
                                                # proxy's shared budget.
                                                finish_handler_timing(
                                                    "guest_auth_route_permit_wait",
                                                    permit_started,
                                                )
                                            lookup_started = time.monotonic()
                                            route_user = await asyncio.wait_for(
                                                lookup,
                                                timeout=lookup_timeout,
                                            )
                                    finally:
                                        if prepaid is not None:
                                            route_session.release_prepaid_route_rate(prepaid)
                                else:
                                    lookup_started = time.monotonic()
                                    route_user = await lookup
                                lookup_outcome = "found" if route_user else "not_found"
                            except TimeoutError:
                                lookup_outcome = "lookup_timeout"
                                logger.warning(
                                    "[%s] [scrape_handler] Guest lookup for user %s timed out "
                                    "after %.1fs via %s",
                                    worker_id,
                                    user_id or normalized_task_username,
                                    lookup_timeout,
                                    _proxy_log_label(route_proxy_url),
                                )
                            except asyncio.CancelledError:
                                lookup_outcome = "lookup_cancelled"
                                propagate_cancellation = not (
                                    internal_cancel_event is not None
                                    and internal_cancel_event.is_set()
                                )
                            except Exception as exc:  # noqa: BLE001
                                lookup_outcome = "lookup_error"
                                if is_mps_lookup and getattr(exc, "proxy_code", None) == 407:
                                    _record_mps_guest_route_outcome(
                                        site_api, route_proxy_url, "proxy_auth_failure"
                                    )
                                    _observe_guest_auth("proxy_auth_failure")
                                code = getattr(exc, "code", None)
                                if isinstance(code, int):
                                    bucket = (
                                        str(code)
                                        if code in {0, 400, 401, 403, 404, 429, 500, 502, 503, 504}
                                        else "other"
                                    )
                                    _observe_guest_auth(f"lookup_error_code_{bucket}")
                                retry_session = (
                                    is_mps_lookup
                                    and getattr(exc, "code", None) == 429
                                    and session_attempt + 1 < GUEST_SESSION_ATTEMPTS_PER_ROUTE
                                    and getattr(site_api, "_guest_session_pool", None) is not None
                                )
                                if retry_session:
                                    # The session manager benched that visitor session;
                                    # this route's other sessions keep their own windows.
                                    logger.debug(
                                        "[%s] [scrape_handler] Guest session rate-limited for user %s "
                                        "via %s; retrying on another session of the route",
                                        worker_id,
                                        user_id or normalized_task_username,
                                        _proxy_log_label(route_proxy_url),
                                    )
                                else:
                                    logger.warning(
                                        "[%s] [scrape_handler] Guest lookup for user %s failed via "
                                        "%s (%s)",
                                        worker_id,
                                        user_id or normalized_task_username,
                                        _proxy_log_label(route_proxy_url),
                                        type(exc).__name__,
                                    )
                            if not retry_session:
                                break
                            try:
                                next_authed = await asyncio.wait_for(
                                    _resolve_guest_auth(
                                        site_api,
                                        proxy_url=route_proxy_url,
                                        worker_id=worker_id,
                                    ),
                                    timeout=_resolve_mps_guest_route_timeout(context),
                                )
                            except asyncio.CancelledError:
                                lookup_outcome = "lookup_cancelled"
                                propagate_cancellation = not (
                                    internal_cancel_event is not None
                                    and internal_cancel_event.is_set()
                                )
                                break
                            except Exception:  # noqa: BLE001 - keep the first failure
                                break
                            if next_authed is None or next_authed is route_authed:
                                break
                            route_authed = next_authed
                            _observe_guest_auth("session_retry_after_429")
                finally:
                    _guest_auth_observer.reset(auth_observer_token)
                    if lookup_started is not None:
                        route_lookup_ms = finish_handler_timing(
                            "initial_lookup", lookup_started
                        )
                    if is_mps_lookup:
                        _record_mps_guest_route_outcome(site_api, route_proxy_url, lookup_outcome)
                    finish_route_timing(
                        route_timing_started,
                        route_proxy_url,
                        guest_init_ms=route_guest_init_ms,
                        lookup_ms=route_lookup_ms,
                        lookup_observed=route_lookup_observed,
                        lookup_outcome=lookup_outcome,
                    )

                if propagate_cancellation:
                    raise asyncio.CancelledError
                return {
                    "route_index": route_index,
                    "route_proxy_url": route_proxy_url,
                    "route_authed": route_authed,
                    "route_user": route_user,
                    "status": lookup_outcome,
                    "completed_at": time.monotonic(),
                }

            async def run_hedged_guest_routes(
                primary: tuple[int, str | None],
                alternate: tuple[int, str | None],
                primary_role: str,
            ) -> tuple[list[dict[str, Any]], bool]:
                primary_cancel = asyncio.Event()
                alternate_cancel = asyncio.Event()
                alternate_started = asyncio.Event()
                primary_lookup_started = asyncio.Event()
                primary_task = asyncio.create_task(
                    run_guest_route(
                        *primary,
                        selection_role=primary_role,
                        internal_cancel_event=primary_cancel,
                        lookup_started_event=primary_lookup_started,
                    )
                )

                async def start_alternate_after_delay() -> dict[str, Any] | None:
                    await primary_lookup_started.wait()
                    await asyncio.sleep(MPS_GUEST_LOOKUP_HEDGE_DELAY_SECONDS)
                    if primary_task.done():
                        return None
                    alternate_started.set()
                    if callable(record_handler_timing):
                        record_handler_timing("fallback_retry", 0.0)
                    logger.debug(
                        "[%s] [scrape_handler] Starting delayed guest lookup hedge "
                        "for user %s via route %s (%s)",
                        worker_id,
                        user_id or normalized_task_username,
                        alternate[0],
                        _proxy_log_label(alternate[1]),
                    )
                    return await run_guest_route(
                        *alternate,
                        selection_role="hedge",
                        internal_cancel_event=alternate_cancel,
                    )

                alternate_task = asyncio.create_task(start_alternate_after_delay())
                pending: set[asyncio.Task[Any]] = {primary_task, alternate_task}
                results: list[dict[str, Any]] = []
                try:
                    while pending:
                        done, pending = await asyncio.wait(
                            pending,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for completed_task in done:
                            try:
                                result = completed_task.result()
                            except asyncio.CancelledError:
                                continue
                            if result is not None:
                                results.append(result)

                        if primary_task in done and not alternate_started.is_set():
                            if not alternate_task.done():
                                alternate_task.cancel()
                                await asyncio.gather(
                                    alternate_task,
                                    return_exceptions=True,
                                )
                            break

                        if any(result["status"] == "found" for result in results):
                            for pending_task in pending:
                                if pending_task is primary_task:
                                    primary_cancel.set()
                                elif pending_task is alternate_task:
                                    alternate_cancel.set()
                                pending_task.cancel()
                            if pending:
                                cancelled_results = await asyncio.gather(
                                    *pending,
                                    return_exceptions=True,
                                )
                                results.extend(
                                    result
                                    for result in cancelled_results
                                    if isinstance(result, dict)
                                )
                            break

                    results.sort(key=lambda result: result["completed_at"])
                    return results, alternate_started.is_set()
                except asyncio.CancelledError:
                    for pending_task in {primary_task, alternate_task}:
                        if not pending_task.done():
                            pending_task.cancel()
                    await asyncio.gather(
                        primary_task,
                        alternate_task,
                        return_exceptions=True,
                    )
                    raise

            # Try every alternative before retrying inconclusive routes. Keep
            # definitive misses so a retry never repeats healthy route lookups.
            for attempt in range(max_attempts):
                if attempt:
                    retry_started = time.monotonic()
                    await asyncio.sleep(MPS_GUEST_ROUTE_RETRY_DELAY_SECONDS * attempt)
                    finish_handler_timing("fallback_retry", retry_started)
                retry_routes: list[tuple[int, str | None]] = []
                guest_route_init_failures = 0
                guest_lookup_failures = 0

                if is_mps_lookup:
                    # Stable partition of the existing candidates, not a filter:
                    # a deferred route still gets its turn if alternatives do
                    # not resolve the profile or reach the conclusive threshold.
                    pending_routes = sorted(
                        pending_routes,
                        key=lambda route: _mps_guest_route_is_deferred(site_api, route[1]),
                    )
                handled_route_indexes: set[int] = set()
                conclusive_not_found = False
                for route_position, (route_index, route_proxy_url) in enumerate(
                    pending_routes
                ):
                    if route_index in handled_route_indexes:
                        continue
                    if attempt:
                        logger.debug(
                            "[%s] [scrape_handler] Retrying guest route %s for user "
                            "%s (attempt %s/%s)",
                            worker_id,
                            _proxy_log_label(route_proxy_url),
                            user_id or normalized_task_username,
                            attempt + 1,
                            max_attempts,
                        )
                    elif route_index > 0:
                        if callable(record_handler_timing):
                            record_handler_timing("fallback_retry", 0.0)
                        if last_guest_route_failure is not None:
                            failure_reason = last_guest_route_failure
                        elif last_guest_lookup_label is None:
                            failure_reason = "Previous guest route could not initialize"
                        else:
                            failure_reason = (
                                f"User {user_id or normalized_task_username} not found via "
                                f"guest proxy ({last_guest_lookup_label})"
                            )
                        logger.debug(
                            f"[{worker_id}] [scrape_handler] {failure_reason}; "
                            f"attempting geo-fallback route {route_index}/{fallback_route_count}: "
                            f"{_proxy_log_label(route_proxy_url)}"
                        )

                    route_results: list[dict[str, Any]]
                    if (
                        is_mps_lookup
                        and MPS_GUEST_LOOKUP_HEDGE_DELAY_SECONDS > 0
                        and attempt == 0
                        and route_position + 1 < len(pending_routes)
                    ):
                        # A fast not-found on the primary can make a later
                        # geo-fallback the slowest lookup. Let that route use
                        # the same bounded hedge against the next distinct
                        # route; subsequent retry waves remain serial.
                        alternate_route = pending_routes[route_position + 1]
                        route_results, alternate_started = await run_hedged_guest_routes(
                            (route_index, route_proxy_url),
                            alternate_route,
                            "primary" if route_position == 0 else "fallback",
                        )
                        if alternate_started:
                            handled_route_indexes.add(alternate_route[0])
                    else:
                        route_results = [
                            await run_guest_route(route_index, route_proxy_url, selection_role="primary" if route_position == 0 else "fallback")
                        ]

                    found_result = next(
                        (
                            result
                            for result in route_results
                            if result["status"] == "found"
                        ),
                        None,
                    )
                    if found_result is not None:
                        resolved_index = int(found_result["route_index"])
                        resolved_url = cast(str | None, found_result["route_proxy_url"])
                        if resolved_index > 0 or attempt:
                            logger.debug(
                                "[%s] [scrape_handler] Resolved user %s via guest "
                                "route %s (%s, attempt %s/%s)",
                                worker_id,
                                user_id or normalized_task_username,
                                resolved_index,
                                _proxy_log_label(resolved_url),
                                attempt + 1,
                                max_attempts,
                            )
                        user = found_result["route_user"]
                        authed = found_result["route_authed"]
                        resolved_route_url = resolved_url
                        resolved_route_index = resolved_index
                        break

                    for route_result in route_results:
                        result_route_index = int(route_result["route_index"])
                        result_route_url = cast(
                            str | None,
                            route_result["route_proxy_url"],
                        )
                        result_status = route_result["status"]
                        if result_status == "guest_init_failure":
                            guest_route_init_failures += 1
                            retry_routes.append((result_route_index, result_route_url))
                            last_guest_route_failure = (
                                "Previous guest route failed during initialization"
                            )
                        elif result_status in {"lookup_timeout", "lookup_error"}:
                            guest_lookup_failures += 1
                            retry_routes.append((result_route_index, result_route_url))
                            last_guest_route_failure = (
                                "Previous guest lookup timed out"
                                if result_status == "lookup_timeout"
                                else "Previous guest lookup failed"
                            )
                        elif result_status == "not_found":
                            last_guest_route_failure = None
                            last_guest_lookup_label = _proxy_log_label(result_route_url)
                            definitive_not_found_routes.add(result_route_index)

                    if (
                        effective_conclusive_threshold is not None
                        and is_mps_lookup
                        and len(definitive_not_found_routes) >= effective_conclusive_threshold
                    ):
                        conclusive_not_found = True
                        break

                if user or not retry_routes or (
                    effective_conclusive_threshold is not None
                    and is_mps_lookup
                    and len(definitive_not_found_routes) >= effective_conclusive_threshold
                ) or conclusive_not_found:
                    break
                pending_routes = (
                    _select_mps_retry_routes(site_api, configured_routes, retry_routes,
                                             definitive_not_found_routes, recovery_probed)
                    if is_mps_lookup else retry_routes
                )
        else:
            resolved_route_url = _get_auth_proxy_url(authed)
            resolved_route_index = 0 if resolved_route_url is not None else None
            lookup_started = time.monotonic()
            record_handler_timing = context.context.get("record_handler_timing")
            try:
                user = await _resolve_scrape_user(
                    authed,
                    user_id=user_id,
                    username=normalized_task_username,
                    worker_id=worker_id,
                )
            finally:
                if callable(record_handler_timing):
                    record_handler_timing(
                        "initial_lookup", (time.monotonic() - lookup_started) * 1000.0
                    )

        task_proxy_routes = (
            guest_proxy_routes
            if uses_guest_auth
            else ([resolved_route_url] if resolved_route_url is not None else [])
        )

        guest_route_failures = guest_route_init_failures + guest_lookup_failures
        definitive_not_found_count = len(definitive_not_found_routes)
        effective_threshold = (
            payload.conclusive_miss_threshold
            if payload.conclusive_miss_threshold is not None
            else MPS_GUEST_CONCLUSIVE_NOT_FOUND_THRESHOLD
        )
        conclusive_target = (
            effective_threshold
            if effective_threshold is not None
            else 2
        )
        has_conclusive_not_found = (
            definitive_not_found_count >= conclusive_target
            and track_invalid_user
            and content_type == ContentType.PROFILE
            and user_id > 0
        )
        if (
            not user
            and uses_guest_auth
            and guest_route_failures > 0
            and not has_conclusive_not_found
        ):
            if guest_lookup_failures:
                failure_detail = (
                    f"{guest_route_failures} of "
                    f"{len(guest_proxy_routes)} proxy route(s) failed "
                    f"({guest_route_init_failures} initialization, "
                    f"{guest_lookup_failures} lookup)"
                )
            else:
                failure_detail = (
                    f"{guest_route_init_failures} of "
                    f"{len(guest_proxy_routes)} proxy route(s) failed to initialize"
                )
            error_msg = (
                f"Could not conclusively resolve user "
                f"'{user_id or normalized_task_username}' via guest auth: "
                f"{failure_detail}"
            )
            logger.warning(f"[{worker_id}] [scrape_handler] {error_msg}")
            return TaskResult(
                success=False,
                message=error_msg,
                errors=[error_msg],
                metrics={
                    "failure_kind": "guest_route_unavailable",
                    "proxy_route_failures": guest_route_failures,
                    "proxy_route_count": len(guest_proxy_routes),
                    "proxy_url": None,
                    "proxy_routes": guest_proxy_routes,
                    "proxy_hop_used": None,
                    "proxy_routes_attempted": sorted(guest_lookup_routes),
                },
                output={
                    "proxy_url": None,
                    "proxy_routes": guest_proxy_routes,
                    "proxy_hop_used": None,
                    "proxy_routes_attempted": sorted(guest_lookup_routes),
                },
                # MPS profile tasks are independent. Let the worker consume a
                # bounded recent-failure budget instead of cancelling the
                # entire batch on the first inconclusive proxy lookup.
                continue_job_on_failure=(
                    track_invalid_user and content_type == ContentType.PROFILE
                ),
            )

        if user_id <= 0:
            if not normalized_task_username:
                error_msg = (
                    f"Invalid user_id: {user_id}. Task.user_id must be a positive integer "
                    f"for content_type '{content_type}' unless user_username is provided."
                )
                logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
                return TaskResult(success=False, message=error_msg, errors=[error_msg])
            if not user or getattr(user, "id", 0) <= 0:
                error_msg = f"Could not resolve user '{normalized_task_username}'"
                logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
                return TaskResult(success=False, message=error_msg, errors=[error_msg])
            user_id = user.id
            task.user_id = user_id
        else:
            if not user:
                if uses_guest_auth and track_invalid_user:
                    if guest_route_failures > 0:
                        route_summary = f": {definitive_not_found_count} route(s) returned 404, {guest_route_failures} route(s) inconclusive"
                    else:
                        route_summary = ""
                    message = (
                        f"User ID '{user_id}' not found via guest auth "
                        f"(attempted {len(guest_lookup_routes)} proxy route(s){route_summary}); "
                        "recorded in invalid_users"
                    )
                    result = TaskResult(
                        success=True,
                        message=message,
                        output={
                            "invalid_user": True,
                            "user_id": user_id,
                            "proxy_url": None,
                            "proxy_routes": task_proxy_routes,
                            "proxy_hop_used": None,
                            "proxy_routes_attempted": sorted(guest_lookup_routes),
                        },
                        metrics={
                            "items_processed": 0,
                            "items_total": 0,
                            "items_scraped": 0,
                            "total_items": 0,
                            "items_saved": 0,
                            "worker_id": worker_id,
                            "proxy_url": None,
                            "proxy_routes": task_proxy_routes,
                            "proxy_hop_used": None,
                            "proxy_routes_attempted": sorted(guest_lookup_routes),
                        },
                    )
                    _prepare_profile_completion(context, result)
                    await _mark_user_invalid(
                        task=task,
                        context=context,
                        user_id=user_id,
                        reason="not_found",
                    )
                    result.completed_at = datetime.now(UTC).isoformat()
                    return result
                error_msg = f"User ID '{user_id}' not found (auth: {getattr(authed, 'username', 'unknown')})"
                logger.error(f"[{worker_id}] [scrape_handler] {error_msg}")
                return TaskResult(success=False, message=error_msg, errors=[error_msg])

        if (
            uses_guest_auth
            and track_invalid_user
            and content_type == ContentType.PROFILE
        ):
            transient_user = user
            transient_auth = authed
        if (
            site_key == "onlyfans"
            and content_type == ContentType.POSTS
            and max_items is None
            and max(0, int(getattr(user, "posts_count", 0) or 0)) == 0
        ):
            refresh_user = getattr(authed, "get_user", None)
            if not callable(refresh_user):
                raise RuntimeError(
                    f"Cannot confirm empty Posts count for user {user_id}: "
                    "authenticated account does not support profile refresh"
                )
            try:
                refresh_started = time.monotonic()
                refreshed_user = await _try_get_user(authed, user_id, refresh=True)
            except Exception as refresh_exc:
                record_handler_timing = context.context.get("record_handler_timing")
                if callable(record_handler_timing):
                    record_handler_timing(
                        "refresh_api", (time.monotonic() - refresh_started) * 1000.0
                    )
                raise RuntimeError(
                    f"Could not refresh Posts count for user {user_id}: {refresh_exc}"
                ) from refresh_exc
            record_handler_timing = context.context.get("record_handler_timing")
            if callable(record_handler_timing):
                record_handler_timing(
                    "refresh_api", (time.monotonic() - refresh_started) * 1000.0
                )
            if not _user_matches_identifier(
                refreshed_user,
                user_id=user_id,
                username=normalized_task_username,
            ):
                raise RuntimeError(
                    f"Could not confirm empty Posts count for user {user_id}"
                )
            user = refreshed_user
            logger.info(
                "[%s] [scrape_handler] Refreshed zero-count OnlyFans profile "
                "before unbounded Posts scrape for user %s",
                worker_id,
                user_id,
            )

        username_str = user.username
        content_type_str = _content_type_name(content_type)
        profile_posts_total_hint: int | None = None
        if content_type == ContentType.POSTS:
            profile_posts_total_hint = max(0, int(getattr(user, "posts_count", 0) or 0))
        expected_total_hint: int | None = (
            max_items if max_items and max_items > 0 else None
        )
        if expected_total_hint is None:
            if content_type == ContentType.POSTS:
                expected_total_hint = profile_posts_total_hint
            elif content_type == ContentType.ARCHIVED_POSTS:
                expected_total_hint = max(
                    0, int(getattr(user, "archived_posts_count", 0) or 0)
                )
            elif content_type == ContentType.PROFILE:
                expected_total_hint = 1

        # Publish started event
        started_event = OperationProgressEvent(
            event="scrape_progress",
            job_id=context.job.job_id,
            task_id=task.task_id,
            username=username_str,
            user_id=user_id,
            content_type=content_type_str,
            status="started",
            items_scraped=0,
            items_saved=0,
            total_items=expected_total_hint,
            worker_id=worker_id,
        )

        await publish_hook_event(
            context.storage,
            publish_event(started_event),
            "started scrape_progress event",
        )
        # Reset persisted task counters at run start to prevent stale resume data
        # from a previous attempt from leaking into the current run.
        task.result = TaskResult(
            success=True,
            message="Scrape started",
            metrics={
                "items_processed": 0,
                "items_total": expected_total_hint or 0,
                "items_scraped": 0,
                "total_items": expected_total_hint or 0,
                "items_saved": 0,
                "proxy_url": resolved_route_url,
                "proxy_routes": task_proxy_routes,
                "proxy_hop_used": resolved_route_index,
            },
            output={
                "proxy_url": resolved_route_url,
                "proxy_routes": task_proxy_routes,
                "proxy_hop_used": resolved_route_index,
            },
            errors=[],
        )
        await context.storage.persist_task(
            job_id=context.job.job_id,
            task=task,
        )

        try:

            async def persist_scrape_progress_update(
                completed_pages: int, total_pages: int, items_so_far: int
            ) -> None:
                safe_completed_pages = max(0, int(completed_pages))
                safe_total_pages = max(0, int(total_pages))
                safe_scraped = max(0, int(items_so_far))
                previous_metrics = (
                    dict(task.result.metrics or {}) if task.result else {}
                )
                prev_scraped = max(
                    0, int(previous_metrics.get("items_scraped", 0) or 0)
                )
                prev_saved = max(0, int(previous_metrics.get("items_saved", 0) or 0))
                prev_total = max(0, int(previous_metrics.get("items_total", 0) or 0))
                expected_total = max(0, int(expected_total_hint or 0))
                items_scraped_metric = max(prev_scraped, safe_scraped)
                items_saved_metric = prev_saved
                estimated_total = 0
                if (
                    safe_completed_pages > 0
                    and safe_total_pages > 0
                    and safe_scraped > 0
                ):
                    avg_per_page = max(
                        1,
                        (safe_scraped + safe_completed_pages - 1)
                        // safe_completed_pages,
                    )
                    estimated_total = avg_per_page * safe_total_pages

                items_total_metric = max(
                    prev_total,
                    expected_total,
                    estimated_total,
                    items_scraped_metric,
                )
                # Once scrape pagination reports completion, trust exact scraped count.
                if (
                    safe_total_pages > 0
                    and safe_completed_pages >= safe_total_pages
                    and safe_scraped > 0
                ):
                    items_total_metric = items_scraped_metric
                task.result = TaskResult(
                    success=True,
                    message=(
                        f"Scrape in progress: {items_scraped_metric}/{items_total_metric}"
                        if items_total_metric > 0
                        else f"Scrape in progress: {items_scraped_metric}"
                    ),
                    metrics={
                        "items_processed": items_scraped_metric,
                        "items_total": items_total_metric,
                        "items_scraped": items_scraped_metric,
                        "total_items": items_total_metric,
                        "items_saved": items_saved_metric,
                        "proxy_url": resolved_route_url,
                        "proxy_routes": task_proxy_routes,
                        "proxy_hop_used": resolved_route_index,
                    },
                    output={
                        "proxy_url": resolved_route_url,
                        "proxy_routes": task_proxy_routes,
                        "proxy_hop_used": resolved_route_index,
                    },
                    errors=[],
                )
                await context.storage.persist_task(
                    job_id=context.job.job_id,
                    task=task,
                )

            async def persist_save_progress_update(
                processed: int, total: int, saved: int
            ) -> None:
                safe_processed = max(0, int(processed))
                safe_total = max(0, int(total))
                safe_saved = max(0, int(saved))
                previous_metrics = (
                    dict(task.result.metrics or {}) if task.result else {}
                )
                prev_scraped = max(
                    0, int(previous_metrics.get("items_scraped", 0) or 0)
                )
                prev_saved = max(0, int(previous_metrics.get("items_saved", 0) or 0))
                prev_total = max(0, int(previous_metrics.get("items_total", 0) or 0))
                expected_total = max(0, int(expected_total_hint or 0))

                items_scraped_metric = max(prev_scraped, safe_total, safe_processed)
                items_saved_metric = max(prev_saved, safe_saved)
                # Save callback's `total` is the exact number of scraped items selected
                # for DB persistence. Prefer it over optimistic scrape-stage totals.
                if safe_total > 0:
                    items_total_metric = max(items_scraped_metric, safe_total)
                else:
                    items_total_metric = max(
                        prev_total,
                        expected_total,
                        items_scraped_metric,
                        items_saved_metric,
                    )
                task.result = TaskResult(
                    success=True,
                    message=(
                        f"Save in progress: {items_saved_metric}/{items_total_metric}"
                        if items_total_metric > 0
                        else f"Save in progress: {items_saved_metric}"
                    ),
                    metrics={
                        "items_processed": items_saved_metric,
                        "items_total": items_total_metric,
                        "items_scraped": items_scraped_metric,
                        "total_items": items_total_metric,
                        "items_saved": items_saved_metric,
                        "proxy_url": resolved_route_url,
                        "proxy_routes": task_proxy_routes,
                        "proxy_hop_used": resolved_route_index,
                    },
                    output={
                        "proxy_url": resolved_route_url,
                        "proxy_routes": task_proxy_routes,
                        "proxy_hop_used": resolved_route_index,
                    },
                    errors=[],
                )
                await context.storage.persist_task(
                    job_id=context.job.job_id,
                    task=task,
                )

            media_selection = None
            profile_completion = None
            # Execute scrape for supported content types
            if content_type in (
                ContentType.POSTS,
                ContentType.ARCHIVED_POSTS,
                ContentType.MESSAGES,
                ContentType.STORIES,
                ContentType.HIGHLIGHTS,
            ):
                assert content_type is not None
                # Flush the API-layer in-memory cache when rescrape_existing is
                # enabled so that get_messages() (which has a 1-hour CacheStats
                # TTL) re-fetches from the API instead of returning stale data.
                rescrape_existing = (
                    bool(payload.rescrape_existing)
                    if isinstance(payload, ScrapeTaskPayload)
                    else True
                )
                if rescrape_existing and hasattr(user, "cache"):
                    user.cache.flush()
                    logger.debug(
                        f"[{worker_id}] [scrape_handler] Flushed API cache for user "
                        f"{user_id} (rescrape_existing=True)"
                    )

                # Content rows reference the performer, and the download task
                # reads its identity from the database. Guest-auth and
                # unsubscribed targets have no subscription sync to create it.
                performer_payload = await serialize_user_write_payload(user)
                await context.database_writes.submit(
                    DatabaseWriteOperation.UPSERT_USER,
                    performer_payload,
                    site=site_key,
                    command_id=stable_command_id(
                        "performer-v1",
                        task.task_id,
                        site_key,
                        user_id,
                        json.dumps(
                            performer_payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )

                from uuid import uuid4
                media_selection = {"selection_id": uuid4().hex,
                    "job_id": context.job.job_id, "task_id": task.task_id,
                    "site": site_key, "auth_id": None if _is_guest_auth(authed) else auth_id}
                (
                    items_scraped,
                    items_saved,
                    scrape_errors,
                    media_urls,
                ) = await scrape_content_type(
                    user,
                    content_type,
                    max_items,
                    auth_name=str(getattr(authed, "username", "unknown")),
                    worker_id=worker_id,
                    job_id=context.job.job_id,
                    auth_user_id=auth_id,
                    on_scrape_progress=persist_scrape_progress_update,
                    on_save_progress=persist_save_progress_update,
                    database_writes=context.database_writes,
                    site=site_key,
                    media_selection=media_selection,
                )
                if items_scraped > 0 and items_saved == 0 and (
                    scrape_errors or media_selection.get("error_count")
                ):
                    raise RuntimeError(
                        f"None of {items_scraped} scraped {content_type_str} "
                        "item(s) could be saved"
                        + (f": {scrape_errors[0]}" if scrape_errors else "")
                    )
                if (
                    content_type == ContentType.POSTS
                    and site_key == "onlyfans"
                    and max_items is None
                    and items_scraped == 0
                ):
                    if scrape_errors:
                        raise RuntimeError(scrape_errors[0])
                    if profile_posts_total_hint and profile_posts_total_hint > 0:
                        raise RuntimeError(
                            "Posts scrape returned no items although the profile "
                            f"reports {profile_posts_total_hint} posts"
                        )
            elif content_type == ContentType.PROFILE:
                items_scraped = 1
                items_saved = 0
                scrape_errors: list[str] = []
                media_urls: list[dict[str, Any]] = []

                profile_user = user
                # Only attempt profile refresh if not already freshly resolved via guest auth
                if not uses_guest_auth and hasattr(authed, "get_user"):
                    refresh_started = time.monotonic()
                    try:
                        refreshed_user = await asyncio.wait_for(
                            _try_get_user(authed, user_id, refresh=True),
                            timeout=_resolve_mps_guest_lookup_timeout(
                                context, resolved_route_url
                            ),
                        )
                        if refreshed_user is not None:
                            profile_user = refreshed_user
                    except Exception as refresh_exc:
                        logger.debug(
                            f"[{worker_id}] [scrape_handler] Failed to refresh profile user {user_id}: {refresh_exc}"
                        )
                    finally:
                        record_handler_timing = context.context.get("record_handler_timing")
                        if callable(record_handler_timing):
                            record_handler_timing(
                                "refresh_api",
                                (time.monotonic() - refresh_started) * 1000.0,
                            )

                record_handler_timing = context.context.get("record_handler_timing")
                api_started = time.monotonic()
                try:
                    fetched_socials = await _fetch_profile_socials(
                        profile_user,
                        site_api=site_api,
                        authed=authed,
                        user_id=user_id,
                        uses_guest_auth=uses_guest_auth,
                        guest_proxy_routes=guest_proxy_routes,
                        worker_id=worker_id,
                    )
                finally:
                    if callable(record_handler_timing):
                        record_handler_timing(
                            "refresh_api", (time.monotonic() - api_started) * 1000.0
                        )

                serialization_started = time.monotonic()
                try:
                    profile_payload = await serialize_user_write_payload(
                        profile_user,
                        sync_socials=True,
                        socials=fetched_socials,
                    )
                finally:
                    if callable(record_handler_timing):
                        record_handler_timing(
                            "serialization",
                            (time.monotonic() - serialization_started) * 1000.0,
                        )

                profile_completion = _scrape_success_result(
                user_id=user_id, content_type_str=content_type_str,
                auth_username=getattr(authed, "username", "unknown"),
                items_scraped=items_scraped, items_saved=1,
                media_urls=media_urls, auth_id=auth_id, username_str=username_str,
                media_selection=media_selection, resolved_route_url=resolved_route_url,
                task_proxy_routes=task_proxy_routes, resolved_route_index=resolved_route_index,
                worker_id=worker_id, scrape_errors=scrape_errors,
            )
                _prepare_profile_completion(context, profile_completion)

                release_fetch_capacity = context.context.get("release_fetch_capacity")
                if callable(release_fetch_capacity):
                    release_fetch_capacity()
                await context.database_writes.submit(
                    DatabaseWriteOperation.UPSERT_USER,
                    profile_payload,
                    site=site_key,
                    command_id=stable_command_id(
                        "profile-v1",
                        task.task_id,
                        site_key,
                        user_id,
                        json.dumps(
                            profile_payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                items_saved = 1
                # This single-item write is already acknowledged. The completed
                # event and final TaskResult below carry its exact counters;
                # intermediate checkpoints would immediately be superseded.
            else:
                items_scraped = 1
                items_saved = 0
                scrape_errors = []
                media_urls = []

            # Publish completed event
            completed_event = OperationProgressEvent(
                event="scrape_progress",
                job_id=context.job.job_id,
                task_id=task.task_id,
                username=username_str,
                user_id=user_id,
                content_type=content_type_str,
                status="completed",
                items_scraped=items_scraped,
                items_saved=items_saved,
                total_items=items_scraped,
                worker_id=worker_id,
            )

            await publish_hook_event(
                context.storage,
                publish_event(completed_event),
                "completed scrape_progress event",
            )

            if profile_completion is not None:
                profile_completion.completed_at = datetime.now(UTC).isoformat()
                return profile_completion
            return _scrape_success_result(
                user_id=user_id, content_type_str=content_type_str,
                auth_username=getattr(authed, "username", "unknown"),
                items_scraped=items_scraped, items_saved=items_saved,
                media_urls=media_urls, auth_id=auth_id, username_str=username_str,
                media_selection=media_selection, resolved_route_url=resolved_route_url,
                task_proxy_routes=task_proxy_routes, resolved_route_index=resolved_route_index,
                worker_id=worker_id, scrape_errors=scrape_errors,
            )
        except Exception as exc:
            logger.warning(f"[{worker_id}] [scrape_handler] Scrape failed: {exc}")

            failed_event = OperationProgressEvent(
                event="scrape_progress",
                job_id=context.job.job_id,
                task_id=task.task_id,
                username=username_str,
                user_id=user_id,
                content_type=content_type_str,
                status="failed",
                items_scraped=0,
                items_saved=0,
                total_items=0,
                worker_id=worker_id,
            )

            await publish_hook_event(
                context.storage,
                publish_event(failed_event),
                "failed scrape_progress event",
            )

            raise

    except Exception as exc:
        from ...ledger_runtime import caused_by_database_outage

        if caused_by_database_outage(exc):
            raise  # A failed result would never be retried.
        worker_id = context.worker_id or "unknown"
        logger.exception(f"[{worker_id}] [scrape_handler] Scrape task failed: {exc}")
        return TaskResult(
            success=False,
            message=f"Scrape failed: {str(exc)}",
            errors=[str(exc)],
        )
    finally:
        if transient_user is not None and transient_auth is not None:
            transient_auth.release_user(transient_user.id)
