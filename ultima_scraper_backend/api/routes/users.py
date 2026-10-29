"""User search API routes.

Provides endpoints for searching/looking up users on supported sites.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Literal, Sequence, cast

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import and_, case, exists, func, or_
from sqlalchemy import select as sa_select
from sqlalchemy import text as sa_text
from sqlalchemy.sql import visitors
from sqlmodel import select
from ultima_scraper_api import get_enabled_sites, user_types
from ultima_scraper_api.apis.fansly.fansly import FanslyAPI
from ultima_scraper_api.apis.onlyfans.onlyfans import OnlyFansAPI

from ultima_scraper_db_archive import (
    HistoUserInfoModel,
    SocialModel,
    SubscriptionModel,
    UserAliasModel,
    UserAuthModel,
    UserInfoModel,
    UserModel,
    UserProfileMediaModel,
)

from ...database.factory import MultiSiteDatabaseManager
from ...database.helpers.user_sync import (
    record_user_alias,
    schedule_user_sync,
)
from ...helpers.user_refresh import needs_user_refresh, normalize_utc
from ...models.backend import BackendInstance
from ...manager import BackendManager
from ...services.statistics_service import StatisticsService
from ...services.user_stats_cache import user_stats_cache
from ...usapi_initializer import (
    AssignedProxyResolutionError,
    resolve_assigned_proxy_url,
)
from ..dependencies import get_backend_instance, get_backend_manager
from ...database.threads import run_database_call

logger = logging.getLogger(__name__)

from ...services.profile_refresh import queue_cached_profile_refresh
from ...services import favourites_service

router = APIRouter(prefix="/users", tags=["users"])


def _apply_identifier_filter(q: Any, search: str) -> Any:
    """Find matching IDs before sorting users, preserving alias deduplication."""
    trimmed_search = search.strip()
    if not trimmed_search:
        return q

    pattern = f"%{trimmed_search}%"
    matches = [
        sa_select(UserModel.id).where(UserModel.username.ilike(pattern)),
        sa_select(UserAliasModel.user_id).where(UserAliasModel.username.ilike(pattern)),
    ]
    if trimmed_search.isdigit():
        matches.append(sa_select(UserModel.id).where(UserModel.id == int(trimmed_search)))

    # OR with a correlated alias EXISTS can make LIMIT favor a backwards
    # primary-key scan of the entire users table. Independent UNION branches
    # let both username searches use their trigram indexes instead.
    return q.where(UserModel.id.in_(matches[0].union(*matches[1:])))


def _paginate_user_query(
    q: Any, sort_expression: Any, offset: int, limit: int, *, profile_probe: bool = False
) -> Any:
    """Select the page using narrow rows before loading full user profiles."""
    narrow_query = q.with_only_columns(UserModel.id)
    if profile_probe:
        # For ID-ordered active performers, probe profiles in user-index order.
        # OFFSET 0 keeps PostgreSQL from flattening this correlated lookup into
        # a broad profile join followed by a sort before LIMIT.
        info_table = UserInfoModel.__table__
        probe = (sa_select(*info_table.c).where(UserInfoModel.user_id == UserModel.id)
                 .correlate(UserModel).offset(0).lateral("profile_probe"))
        replacements = {info_table: probe, **{column: probe.c[column.name] for column in info_table.c}}

        def adapt(expression: Any) -> Any:
            return visitors.replacement_traverse(expression, {}, replacements.get)

        narrow_query = (sa_select(UserModel.id)
                        .select_from(*(adapt(source) for source in q.get_final_froms()))
                        .where(adapt(q.whereclause)).order_by(sort_expression))
    page = narrow_query.offset(offset).limit(limit).subquery()
    return (
        select(UserModel, UserInfoModel)
        .join(page, UserModel.id == page.c.id)
        .outerjoin(UserInfoModel, UserModel.id == UserInfoModel.user_id)
        .order_by(sort_expression)
    )

UserWithInfoRow = tuple[UserModel, UserInfoModel | None]
SubscriptionWithAuthRow = tuple[SubscriptionModel, UserAuthModel]
UserStatsRow = tuple[int | None, int | None, int | None, float | None, int | None]

PROFILE_REFRESH_TIMEOUT_SECONDS = 45.0
PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS = 10.0
PROFILE_LOOKUP_MAX_PROXY_ATTEMPTS = 3
# ID and saved-username lookups each try direct plus configured proxies.
# Every route can spend one timeout on login and another on get_user.
PROFILE_REFRESH_WORKER_TIMEOUT_SECONDS = (
    2 * (1 + PROFILE_LOOKUP_MAX_PROXY_ATTEMPTS)
    * 2 * PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS
    + PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS
)
PROFILE_REFRESH_QUEUE_WAIT_SECONDS = 60.0
PROFILE_REFRESH_WORKER_WAIT_SECONDS = (
    PROFILE_REFRESH_WORKER_TIMEOUT_SECONDS + PROFILE_REFRESH_QUEUE_WAIT_SECONDS
)
_profile_refresh_tasks: dict[tuple[str, int], asyncio.Task[bool]] = {}
_profile_refresh_errors: dict[tuple[str, int], str] = {}
_claimant_reconciliation_tasks: dict[tuple[str, int], asyncio.Task[None]] = {}


@dataclass(frozen=True)
class SavedUserRefreshState:
    id: int
    username: str | None
    last_checked_at: datetime | None


@dataclass(frozen=True)
class SiteUserLookupResult:
    """Outcome of checking a profile through the available network routes."""

    user: Any | None
    conclusive_missing: bool
    attempted_routes: int
    completed_routes: int


class UserSearchResult(BaseModel):
    """User search result."""

    id: int
    username: str
    name: str | None = None
    avatar_url: str | None = None
    header_url: str | None = None
    bio: str | None = None
    is_verified: bool = False
    is_subscribed: bool = False
    subscription_price: float | None = None
    posts_count: int | None = None
    photos_count: int | None = None
    videos_count: int | None = None
    site: str


class UserSearchResponse(BaseModel):
    """User search response."""

    success: bool
    user: UserSearchResult | None = None
    error: str | None = None


class MultiUserSearchResponse(BaseModel):
    """Multiple user search response."""

    results: List[UserSearchResult]
    errors: List[str] = []


class UserIdentifierCandidate(BaseModel):
    """One local account that claims a profile URL identifier."""

    user_id: int
    username: str
    active: bool
    last_checked_at: str | None = None
    matched_by: list[Literal["user_id", "username", "alias"]]


class UserIdentifierResolutionResponse(BaseModel):
    """Result of resolving a profile URL's ID, username, or historical alias."""

    success: bool
    status: Literal["resolved", "ambiguous", "not_found", "unavailable"]
    user_id: int | None = None
    username: str | None = None
    candidates: list[UserIdentifierCandidate] = []
    total: int = 0
    page: int = 1
    page_size: int = 20
    error: str | None = None


# Supported sites for user search
SUPPORTED_SITES = [site_name.lower() for site_name in get_enabled_sites()]
DESCRIPTION_TERM_PATTERN = re.compile(r'"([^"]+)"|(\S+)')


async def get_value(obj: Any, *attrs: str, default: Any = None) -> Any:
    """Helper to get value (handles sync/async methods and properties)"""
    for attr in attrs:
        if hasattr(obj, attr):
            val = getattr(obj, attr)
            if inspect.iscoroutinefunction(val):
                val = await val()
            elif callable(val):
                val = val()
            elif inspect.iscoroutine(val):
                val = await val
            if val is not None:
                return val
    return default


def _normalize_utc(dt: datetime | None) -> datetime | None:
    """Normalize database timestamps to timezone-aware UTC."""
    return normalize_utc(dt)


def _get_site_display_name(site: str) -> str:
    """Return the human-readable site name used by the auth bootstrap flow."""

    return "OnlyFans" if site.strip().lower() == "onlyfans" else "Fansly"


def _extract_description_terms(value: str | None) -> list[str]:
    """Split a search string into terms, preserving quoted phrases."""

    if not value:
        return []

    terms: list[str] = []
    for match in DESCRIPTION_TERM_PATTERN.finditer(value):
        term = (match.group(1) or match.group(2) or "").strip()
        if term:
            terms.append(term)
    return terms


def _find_exact_existing_user(session: Any, identifier: str) -> UserModel | None:
    """Find an exact local user match for a username or numeric identifier."""

    normalized_identifier = identifier.strip().lstrip("@")
    if not normalized_identifier:
        return None

    if normalized_identifier.isdigit():
        return session.get(UserModel, int(normalized_identifier))

    return session.exec(
        select(UserModel).where(
            func.lower(UserModel.username) == normalized_identifier.lower()
        )
    ).first()


def _set_existing_user_status(
    site_db: Any,
    user_id: int,
    site: str,
    *,
    is_active: bool,
    username: str | None = None,
    avatar_url: str | None = None,
    header_url: str | None = None,
) -> bool:
    """Persist a minimal upstream-confirmed status update for an existing user."""

    with site_db.get_session() as session:
        existing_user = session.get(UserModel, user_id)
        if existing_user is None:
            return False

        existing_user.active = is_active
        normalized_username = username.strip().lstrip("@") if username else ""
        if normalized_username:
            record_user_alias(
                session,
                user_id,
                existing_user.username,
                canonical_username=normalized_username,
            )
            existing_user.username = normalized_username
        existing_user.last_checked_at = datetime.now(timezone.utc)
        existing_user.touch()
        if avatar_url or header_url:
            latest_media = session.exec(
                select(UserProfileMediaModel)
                .where(UserProfileMediaModel.user_id == user_id)
                .order_by(UserProfileMediaModel.created_at.desc())
                .limit(1)
            ).first()
            should_insert = (
                latest_media is None
                or latest_media.avatar_url != avatar_url
                or latest_media.header_url != header_url
            )
            if should_insert:
                session.add(
                    UserProfileMediaModel(
                        user_id=user_id,
                        avatar_url=avatar_url,
                        header_url=header_url,
                    )
                )
        session.add(existing_user)
        session.commit()
        logger.info(
            "Updated local user %s on %s after upstream profile lookup (active=%s)",
            user_id,
            site,
            is_active,
        )
        return True


def _mark_existing_user_inactive(site_db: Any, user_id: int, site: str) -> bool:
    """Mark a local user inactive after the upstream API confirms it no longer exists."""

    return _set_existing_user_status(site_db, user_id, site, is_active=False)


async def _apply_minimal_user_refresh(
    site_db: Any, api_user: Any, user_id: int, site: str
) -> bool:
    """Fallback when a full sync fails but the upstream lookup succeeded."""

    upstream_is_active = bool(
        await get_value(api_user, "is_active", "active", default=True)
    )
    upstream_username = await get_value(api_user, "username", default=None)
    upstream_avatar = await get_value(
        api_user, "avatar", "avatarUrl", "avatar_url", default=None
    )
    upstream_header = await get_value(
        api_user, "header", "headerUrl", "banner", default=None
    )
    username = str(upstream_username) if upstream_username is not None else None
    avatar_url = str(upstream_avatar) if upstream_avatar is not None else None
    header_url = str(upstream_header) if upstream_header is not None else None
    return await run_database_call(
        _set_existing_user_status,
        site_db,
        user_id,
        site,
        is_active=upstream_is_active,
        username=username,
        avatar_url=avatar_url,
        header_url=header_url,
    )


async def _refresh_stale_existing_user(
    identifier: str, site: str, site_db: Any, *, use_worker_pool: bool = True,
    lookup_results: dict[str, Any] | None = None,
) -> bool:
    """Refresh a locally known user from the site API when its cache is stale."""

    def read_refresh_state() -> SavedUserRefreshState | None:
        with site_db.get_session() as session:
            user = _find_exact_existing_user(session, identifier)
            if user is None:
                return None
            return SavedUserRefreshState(user.id, user.username, user.last_checked_at)

    existing_user = await run_database_call(read_refresh_state)
    if existing_user is None:
        return False

    if not needs_user_refresh(existing_user.last_checked_at):
        return False

    site_lower = site.strip().lower()
    logger.info(
        "Refreshing stale user %s on %s; last_checked_at=%s",
        existing_user.id,
        site_lower,
        existing_user.last_checked_at,
    )

    async def lookup(value: str | int) -> SiteUserLookupResult:
        if lookup_results is None:
            return await _lookup_site_user(site_lower, value, use_worker_pool=use_worker_pool)
        from types import SimpleNamespace
        from ...manager import _wrap_worker_api_data

        result = lookup_results[str(value)]
        data = result.get("user")
        user = None
        if data is not None:
            user = SimpleNamespace(**_wrap_worker_api_data(data))
            user.get_socials = lambda: result.get("socials", [])
        return SiteUserLookupResult(
            user=user,
            conclusive_missing=result["conclusive_missing"],
            attempted_routes=result["attempted_routes"],
            completed_routes=result["completed_routes"],
        )

    id_lookup = await lookup(existing_user.id)
    api_user = id_lookup.user

    if not api_user:
        saved_username = str(existing_user.username or "").strip().lstrip("@")
        username_lookup: SiteUserLookupResult | None = None
        if saved_username and saved_username.casefold() != f"u{existing_user.id}":
            logger.info(
                "Numeric ID %s did not resolve on %s; trying saved username %s",
                existing_user.id,
                site_lower,
                saved_username,
            )
            username_lookup = await lookup(saved_username)
            api_user = username_lookup.user

        if api_user is not None:
            resolved_user_id = int(getattr(api_user, "id", 0) or 0)
            if resolved_user_id > 0 and resolved_user_id != existing_user.id:
                synced = await _schedule_user_sync(
                    api_user,
                    site_lower,
                    observed_aliases=(saved_username,),
                    wait=True,
                )
                if not synced:
                    logger.warning(
                        "Saved username %s resolved from stale ID %s to replacement ID %s "
                        "on %s, but the replacement could not be persisted",
                        saved_username,
                        existing_user.id,
                        resolved_user_id,
                        site_lower,
                    )
                    return False

                await run_database_call(
                    _retire_missing_profile_claimant, site_db, existing_user.id
                )
                logger.info(
                    "Replaced stale user ID %s with %s for username %s on %s",
                    existing_user.id,
                    resolved_user_id,
                    saved_username,
                    site_lower,
                )
                return True

        if api_user is None and (
            not id_lookup.conclusive_missing
            or (username_lookup is not None and not username_lookup.conclusive_missing)
        ):
            attempted_routes = id_lookup.attempted_routes + (
                username_lookup.attempted_routes if username_lookup else 0
            )
            logger.warning(
                "Unable to confirm whether stale user %s or saved username %s exists "
                "on %s after %s network route attempt(s); preserving the saved profile",
                existing_user.id,
                saved_username or "<empty>",
                site_lower,
                attempted_routes,
            )
            return False

        if api_user is None:
            completed_routes = id_lookup.completed_routes + (
                username_lookup.completed_routes if username_lookup else 0
            )
            logger.warning(
                "All %s network route lookup(s) returned no profile for stale user %s "
                "or saved username %s on %s",
                completed_routes,
                existing_user.id,
                saved_username or "<empty>",
                site_lower,
            )
            await run_database_call(
                _mark_existing_user_inactive, site_db, existing_user.id, site_lower
            )
            return False

    sync_succeeded = await _sync_site_resolved_profile_user(
        site_lower,
        api_user,
        observed_aliases=(str(existing_user.username or ""),),
        use_worker_pool=use_worker_pool,
    )
    if not sync_succeeded:
        logger.warning(
            "Full sync failed while refreshing stale user %s on %s; applying minimal status update",
            existing_user.id,
            site_lower,
        )
        return await _apply_minimal_user_refresh(
            site_db, api_user, existing_user.id, site_lower
        )
    return True


def _profile_refresh_in_progress(site: str, user_id: int) -> bool:
    task = _profile_refresh_tasks.get((site.strip().lower(), user_id))
    return task is not None and not task.done()


async def _publish_profile_updated(site: str, user_id: int) -> None:
    # A hint follows committed persistence; clients re-read authoritative state.
    try:
        from ...redis_events import publish_ws_event
        instance = await get_backend_instance()
        await publish_ws_event(getattr(instance, "redis", None),
                               {"type": "user_profile_updated", "site": site, "user_id": user_id})
    except Exception:
        logger.warning("Could not publish profile hint for %s/%s", site, user_id, exc_info=True)


async def _run_profile_refresh(
    site: str,
    user_id: int,
    site_db: Any,
    *, force: bool = False,
) -> bool:
    """Run a profile refresh with a bounded wait for an upstream API response."""

    key = (site, user_id)
    try:
        manager = get_backend_manager()
        if manager is not None:
            def read_username() -> tuple[str, datetime | None] | None:
                with site_db.get_session() as session:
                    user = _find_exact_existing_user(session, str(user_id))
                    if user is None:
                        return None
                    return str(user.username or ""), getattr(user, "last_checked_at", None)

            saved = await run_database_call(read_username)
            if saved is None:
                return False
            username, checked_at = saved
            if not force and not needs_user_refresh(checked_at):
                return True
            from uuid import uuid4
            version = normalize_utc(checked_at).isoformat() if checked_at else "missing"
            attempt = uuid4().hex if force else version
            from ...job_system.api_command_policy import expired_worker_result

            identity = f"profile:{site}:{user_id}:{attempt}"

            async def wait_for_profile(job_identity: str):
                while True:
                    result = await manager._submit_worker_api_command(
                        site,
                        "refresh_profile",
                        {"user_id": user_id, "username": username},
                        None,
                        guest=True,
                        timeout_seconds=PROFILE_REFRESH_WORKER_WAIT_SECONDS,
                        idempotency_key=job_identity,
                    )
                    if not (result.payload or {}).get("pending"):
                        return result
                    # A wait timeout is not a terminal job failure. Keep observing
                    # this same SQL job through queue delays and worker recovery.

            result = await wait_for_profile(identity)
            if not force and expired_worker_result(result):
                # Previously admitted single-attempt jobs are immutable. Join one
                # replacement per saved profile version across API processes;
                # its normal three-attempt budget bounds recovery work.
                result = await wait_for_profile(f"{identity}:lease-recovery-v1")
            if not result.success:
                _profile_refresh_errors[key] = result.message
                return False
            refreshed = await _refresh_stale_existing_user(
                str(user_id), site, site_db,
                lookup_results=(result.payload or {})["lookups"],
            )
            await _publish_profile_updated(site, user_id)
            return refreshed
        refreshed = await asyncio.wait_for(
            _refresh_stale_existing_user(str(user_id), site, site_db),
            timeout=PROFILE_REFRESH_TIMEOUT_SECONDS,
        )
        await _publish_profile_updated(site, user_id)
        return refreshed
    except TimeoutError:
        message = (
            "The upstream profile refresh timed out after "
            f"{PROFILE_REFRESH_TIMEOUT_SECONDS:.0f} seconds."
        )
        _profile_refresh_errors[key] = message
        logger.warning("Profile refresh timed out for %s/%s", site, user_id)
        return False


def _ensure_profile_refresh_started(
    site: str,
    user_id: int,
    site_db: Any,
    *,
    force: bool = False,
) -> bool:
    """Start one background refresh per profile and return whether it is running."""

    site_key = site.strip().lower()
    key = (site_key, user_id)
    existing = _profile_refresh_tasks.get(key)
    if existing is not None and not existing.done():
        return True

    # Failed automatic attempts stay visible to the operator instead of silently
    # cycling forever every time the profile page polls.  A forced refresh clears
    # the error and starts a genuinely new attempt.
    if not force and key in _profile_refresh_errors:
        return False

    _profile_refresh_errors.pop(key, None)
    task = asyncio.create_task(
        (_run_profile_refresh(site_key, user_id, site_db, force=True) if force else
         _run_profile_refresh(site_key, user_id, site_db)),
        name=f"profile-refresh:{site_key}:{user_id}",
    )
    _profile_refresh_tasks[key] = task

    def _finish(completed: asyncio.Task[bool]) -> None:
        is_current = _profile_refresh_tasks.get(key) is completed
        if is_current:
            _profile_refresh_tasks.pop(key, None)
        try:
            refreshed = completed.result()
        except asyncio.CancelledError:
            logger.debug("Profile refresh cancelled for %s/%s", site_key, user_id)
        except Exception:
            logger.exception("Background profile refresh failed for %s/%s", site_key, user_id)
        else:
            if is_current:
                if refreshed:
                    _profile_refresh_errors.pop(key, None)
                else:
                    _profile_refresh_errors.setdefault(
                        key,
                        "The latest upstream profile refresh could not be completed.",
                    )
            logger.info(
                "Background profile refresh finished for %s/%s (updated=%s)",
                site_key,
                user_id,
                refreshed,
            )

    task.add_done_callback(_finish)
    return True


def _is_subscription_expired(expires_at: datetime | None) -> bool:
    """Check whether a subscription expiry is in the past."""

    normalized = _normalize_utc(expires_at)
    if normalized is None:
        return False
    return normalized <= datetime.now(timezone.utc)


def _select_profile_subscription(
    subscriptions: Sequence[SubscriptionWithAuthRow],
) -> SubscriptionWithAuthRow | None:
    """Choose the canonical subscription row to display on the profile page."""

    best_row: tuple[SubscriptionModel, UserAuthModel] | None = None
    for candidate_sub, candidate_auth in subscriptions:
        if best_row is None:
            best_row = (candidate_sub, candidate_auth)
            continue

        current_sub, current_auth = best_row
        current_expired = _is_subscription_expired(current_sub.expires_at)
        candidate_expired = _is_subscription_expired(candidate_sub.expires_at)

        if current_expired and not candidate_expired:
            best_row = (candidate_sub, candidate_auth)
            continue

        if (not current_sub.active) and candidate_sub.active:
            best_row = (candidate_sub, candidate_auth)
            continue

        if current_sub.active == candidate_sub.active:
            if current_sub.expires_at is None and candidate_sub.expires_at is not None:
                best_row = (candidate_sub, candidate_auth)
            elif (
                current_sub.expires_at is not None
                and candidate_sub.expires_at is not None
            ):
                candidate_expires = _normalize_utc(candidate_sub.expires_at)
                current_expires = _normalize_utc(current_sub.expires_at)
                if (
                    candidate_expires is not None
                    and current_expires is not None
                    and candidate_expires > current_expires
                ):
                    best_row = (candidate_sub, candidate_auth)
            elif (
                current_sub.expires_at == candidate_sub.expires_at
                and current_auth.active is False
                and candidate_auth.active is True
            ):
                best_row = (candidate_sub, candidate_auth)

    return best_row


async def _schedule_user_sync(
    user: user_types,
    site: str,
    *,
    observed_aliases: Sequence[str] = (),
    wait: bool = False,
) -> bool:
    try:
        task = schedule_user_sync(
            user,
            site,
            sync_socials=True,
            observed_aliases=observed_aliases,
        )
        if task is None:
            return False
        if wait:
            return bool(await task)
        return True
    except Exception as exc:
        logger.debug("Failed to schedule user sync: %s", exc, exc_info=True)
        return False


def _normalise_profile_identifier(identifier: str) -> str:
    """Normalise a profile URL identifier without treating it as a user ID yet."""

    return identifier.strip().lstrip("@").casefold()


def _identifier_candidate(
    user: UserModel, match_kind: Literal["user_id", "username", "alias"]
) -> UserIdentifierCandidate:
    return UserIdentifierCandidate(
        user_id=int(user.id),
        username=(user.username or f"u{user.id}"),
        active=bool(user.active),
        last_checked_at=(
            user.last_checked_at.isoformat() if user.last_checked_at else None
        ),
        matched_by=[match_kind],
    )


def _local_profile_identifier_candidates(
    site_db: Any,
    identifier: str,
) -> list[UserIdentifierCandidate]:
    """Return every local account that claims an ID, username, or alias.

    A handle is not unique in historical data, so this intentionally returns a
    candidate set rather than choosing the first canonical username match.
    """

    candidates_by_id: dict[int, UserIdentifierCandidate] = {}

    def add(user: UserModel, match_kind: Literal["user_id", "username", "alias"]):
        candidate = candidates_by_id.get(int(user.id))
        if candidate is None:
            candidates_by_id[int(user.id)] = _identifier_candidate(user, match_kind)
        elif match_kind not in candidate.matched_by:
            candidate.matched_by.append(match_kind)

    with site_db.get_session() as session:
        if identifier.isdigit() and int(identifier) > 0:
            user = session.get(UserModel, int(identifier))
            if user is not None:
                add(user, "user_id")
        else:
            canonical_users = session.exec(
                select(UserModel).where(
                    func.lower(UserModel.username) == identifier,
                )
            ).all()
            for user in canonical_users:
                add(user, "username")

            alias_users = session.exec(
                select(UserModel)
                .join(UserAliasModel, UserAliasModel.user_id == UserModel.id)
                .where(func.lower(UserAliasModel.username) == identifier)
            ).all()
            for user in alias_users:
                add(user, "alias")

    return sorted(
        candidates_by_id.values(),
        key=lambda candidate: (
            "username" not in candidate.matched_by,
            not candidate.active,
            candidate.username.casefold(),
            candidate.user_id,
        ),
    )


def _conflicting_profile_claimant_ids(
    site_db: Any,
    username: str,
    *,
    exclude_user_id: int,
) -> list[int]:
    """Find other local IDs whose current name or aliases claim ``username``."""

    normalized_username = _normalise_profile_identifier(username)
    if not normalized_username:
        return []
    # Both tables index lower(username), so this stays an index lookup.
    with site_db.get_session() as session:
        canonical_ids = (
            select(UserModel.id).where(
                func.lower(UserModel.username) == func.lower(normalized_username),
                UserModel.id != exclude_user_id,
            )
        )
        alias_ids = (
            select(UserAliasModel.user_id).where(
                func.lower(UserAliasModel.username)
                == func.lower(normalized_username),
                UserAliasModel.user_id != exclude_user_id,
            )
        )
        return sorted(
            int(user_id)
            for user_id in session.scalars(canonical_ids.union(alias_ids))
        )


def _retire_missing_profile_claimant(site_db: Any, user_id: int) -> None:
    """Release a stale canonical handle while keeping its lookup history."""

    with site_db.get_session() as session:
        user = session.get(UserModel, user_id)
        if user is None:
            return
        fallback_username = f"u{user_id}"
        record_user_alias(
            session,
            user_id,
            user.username,
            canonical_username=fallback_username,
        )
        user.username = fallback_username
        user.active = False
        user.last_checked_at = datetime.now(timezone.utc)
        user.touch()
        session.add(user)
        session.commit()


async def _reconcile_profile_claimant(
    site: str, user_id: int, *, use_worker_pool: bool = True
) -> None:
    """Refresh a conflicting local claimant by stable ID, never by handle."""

    site_lower = site.strip().lower()
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_lower)
    if site_db is None:
        return

    lookup = await _lookup_site_user(site_lower, user_id, use_worker_pool=use_worker_pool)
    api_user = lookup.user

    if api_user is None:
        if lookup.conclusive_missing:
            await run_database_call(_retire_missing_profile_claimant, site_db, user_id)
        else:
            logger.warning(
                "Unable to confirm conflicting user %s is missing on %s; "
                "preserving its canonical username",
                user_id,
                site_lower,
            )
        return

    await _schedule_user_sync(api_user, site_lower, wait=True)


def _schedule_profile_claimant_reconciliation(
    site: str, user_id: int, *, use_worker_pool: bool = True
) -> None:
    """Deduplicate non-blocking reconciliation of stale username claimants."""

    key = (site.strip().lower(), user_id)
    existing_task = _claimant_reconciliation_tasks.get(key)
    if existing_task is not None and not existing_task.done():
        return

    task = asyncio.create_task(
        _reconcile_profile_claimant(key[0], user_id, use_worker_pool=use_worker_pool),
        name=f"profile-claimant-reconciliation:{key[0]}:{user_id}",
    )
    _claimant_reconciliation_tasks[key] = task

    def finish(completed: asyncio.Task[None]) -> None:
        if _claimant_reconciliation_tasks.get(key) is completed:
            _claimant_reconciliation_tasks.pop(key, None)
        try:
            completed.result()
        except asyncio.CancelledError:
            logger.debug("Profile claimant reconciliation cancelled for %s/%s", *key)
        except Exception:
            logger.exception("Profile claimant reconciliation failed for %s/%s", *key)

    task.add_done_callback(finish)


async def _sync_site_resolved_profile_user(
    site: str,
    api_user: user_types,
    *,
    observed_aliases: Sequence[str] = (),
    use_worker_pool: bool = True,
) -> bool:
    """Persist one site-resolved account and repair conflicting local claimants."""

    site_lower = site.strip().lower()
    synced = await _schedule_user_sync(
        api_user,
        site_lower,
        observed_aliases=observed_aliases,
        wait=True,
    )
    if not synced:
        return False

    resolved_user_id = int(getattr(api_user, "id"))
    current_username = str(getattr(api_user, "username", "") or "").strip()
    if not current_username:
        return True

    try:
        def read_claimants() -> list[int]:
            db_manager = MultiSiteDatabaseManager()
            site_db = db_manager.get_or_create_manager(site_lower)
            if site_db is None:
                return []
            return _conflicting_profile_claimant_ids(
                site_db,
                current_username,
                exclude_user_id=resolved_user_id,
            )

        for claimant_id in await run_database_call(read_claimants):
            if use_worker_pool:
                _schedule_profile_claimant_reconciliation(site_lower, claimant_id)
            else:
                await _reconcile_profile_claimant(
                    site_lower, claimant_id, use_worker_pool=False
                )
    except Exception:
        # The resolved account is already committed. A best-effort repair must
        # never turn an otherwise successful profile lookup into a failure.
        logger.exception(
            "Unable to schedule claimant reconciliation for %s/%s",
            site_lower,
            resolved_user_id,
        )
    return True


@router.get("/sites", response_model=List[str])
async def get_supported_sites() -> List[str]:
    """Get list of supported sites for user search.

    Returns:
        List of site names that can be searched
    """
    return SUPPORTED_SITES


async def _get_guest_api(
    site: str,
    *,
    proxy_url: str | None = None,
) -> Any | None:
    """This process's shared anonymous session on the requested route."""
    from ...job_system.guest_sessions import _resolve_guest_auth, guest_site_api

    site_api = guest_site_api(site.strip())
    if site_api is None:
        logger.warning("Unsupported site for guest authentication: %s", site)
        return None
    try:
        authed = await _resolve_guest_auth(site_api, proxy_url=proxy_url, worker_id="api")
    except Exception as guest_exc:
        logger.warning(
            "Guest authentication failed for %s via %s: %s",
            site,
            "configured proxy" if proxy_url else "direct connection",
            guest_exc,
        )
        return None
    return authed if authed and authed.is_authed() else None


async def _get_authenticated_api(
    site: str,
    backend_manager: BackendManager | None = None,
) -> Any | None:
    """Get an authenticated API instance for the given site.

    Loads auth credentials from the database and authenticates with the API.

    Args:
        site: Site name (OnlyFans, Fansly)

    Returns:
        Authenticated auth object or None
    """
    try:
        site_lower = site.strip().lower()

        if backend_manager is not None and site_lower == "onlyfans":
            for account in backend_manager.list_accounts(site_lower):
                auth = await backend_manager.get_authed_session(
                    site_lower, account.id
                )
                if auth is None:
                    continue
                try:
                    user = await auth.get_authed_user()
                except Exception as auth_exc:
                    logger.warning(
                        "Worker API authentication failed for OnlyFans auth_id=%s: %s",
                        account.user_id or account.id,
                        auth_exc,
                    )
                    continue
                if user is not None:
                    return auth
            return None

        if site_lower not in {"onlyfans", "fansly"}:
            logger.warning("Unsupported site for authenticated lookup: %s", site)
            return None

        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_lower)

        if not site_db:
            logger.warning(f"No database manager for site: {site}")
            return None

        with site_db.get_session() as session:
            statement = select(UserAuthModel).where(
                UserAuthModel.site == site_lower,
                UserAuthModel.active.is_(True),
            )
            auths = session.exec(statement).all()

        if not auths:
            logger.warning(
                "No active auth records for site: %s; falling back to guest auth",
                site,
            )
            return await _get_guest_api(site)

        guest_proxy_url: str | None = None
        for auth_record in auths:
            if site_lower == "onlyfans":
                auth_json = {
                    "id": auth_record.user_id,
                    "username": auth_record.username or "",
                    "cookie": auth_record.cookie or "",
                    "x_bc": auth_record.x_bc or "",
                    "user_agent": auth_record.user_agent or "",
                }
                api = OnlyFansAPI()
            elif site_lower == "fansly":
                auth_json = {
                    "id": auth_record.user_id,
                    "username": auth_record.username or "",
                    "authorization": auth_record.authorization or "",
                    "user_agent": auth_record.user_agent or "",
                }
                api = FanslyAPI()
            else:
                logger.warning(f"Unsupported site: {site}")
                return None

            try:
                proxy_url = resolve_assigned_proxy_url(auth_record.proxy_id)
            except AssignedProxyResolutionError as exc:
                logger.warning(
                    "Assigned proxy resolution failed for %s auth_id=%s: %s",
                    site,
                    auth_record.user_id,
                    exc,
                )
                continue

            if proxy_url:
                auth_json["proxy_url"] = proxy_url
                if guest_proxy_url is None:
                    guest_proxy_url = proxy_url

            try:
                authed = await api.login(auth_json=auth_json)
            except Exception as auth_exc:
                logger.warning(
                    "Authenticated API login failed for %s auth_id=%s: %s",
                    site,
                    auth_record.user_id,
                    auth_exc,
                )
                continue

            if authed and authed.is_authed():
                return authed

        logger.warning(
            "Failed to authenticate with active %s auth(s); falling back to guest auth",
            site,
        )
        return await _get_guest_api(site, proxy_url=guest_proxy_url)

    except Exception as e:
        logger.error(f"Failed to get authenticated API for {site}: {e}", exc_info=True)
        return None


def _configured_profile_proxy_urls() -> tuple[str, ...]:
    """Return the configured proxy URLs in stable, deduplicated order."""

    try:
        from ...config import UltimaScraperCollectionConfig

        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        raw_proxies = getattr(config.settings.network, "proxies", ()) or ()
    except Exception:
        logger.warning("Unable to load configured profile proxies", exc_info=True)
        return ()

    proxy_urls: list[str] = []
    seen: set[str] = set()
    for proxy in raw_proxies:
        if not getattr(proxy, "enabled", True):
            continue
        proxy_url = str(getattr(proxy, "url", "") or "").strip()
        if not proxy_url or proxy_url in seen:
            continue
        seen.add(proxy_url)
        proxy_urls.append(proxy_url)
    return tuple(proxy_urls)


async def _lookup_site_user(
    site: str,
    identifier: str | int,
    *,
    use_worker_pool: bool = True,
) -> SiteUserLookupResult:
    """Look up a profile through guest direct and alternate proxy sessions.

    A missing response is only conclusive when every attempted route completed.
    Login failures, request errors, and timeouts therefore cannot retire a saved
    account or release its username. Stored authenticated accounts are never used.
    """

    site_display = _get_site_display_name(site)
    proxy_urls = _configured_profile_proxy_urls()[:PROFILE_LOOKUP_MAX_PROXY_ATTEMPTS]
    routes: tuple[str | None, ...] = (None, *proxy_urls)
    attempted_routes = 0
    completed_routes = 0

    worker_manager = (
        get_backend_manager()
        if use_worker_pool and site_display.lower() == "onlyfans"
        else None
    )

    if worker_manager is not None:
        from types import SimpleNamespace
        from ...manager import _wrap_worker_api_data

        response = await worker_manager._submit_worker_api_command(
            site.strip().lower(), "refresh_profile", {"identifier": identifier}, None,
            guest=True, timeout_seconds=PROFILE_REFRESH_WORKER_WAIT_SECONDS,
        )
        if not response.success:
            logger.warning("Profile lookup worker failed for %s/%s: %s", site, identifier, response.message)
            return SiteUserLookupResult(
                user=None, conclusive_missing=False, attempted_routes=0, completed_routes=0
            )
        result = (response.payload or {})["lookups"][str(identifier)]
        data = result.get("user")
        user = None
        if data is not None:
            user = SimpleNamespace(**_wrap_worker_api_data(data))
            user.get_socials = lambda: result.get("socials", [])
        return SiteUserLookupResult(
            user=user, conclusive_missing=result["conclusive_missing"],
            attempted_routes=result["attempted_routes"], completed_routes=result["completed_routes"],
        )

    for route_index, proxy_url in enumerate(routes, start=1):
        attempted_routes += 1
        try:
            authed = await asyncio.wait_for(
                _get_guest_api(site_display, proxy_url=proxy_url),
                timeout=PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning(
                "Profile route %s timed out while authenticating on %s",
                route_index,
                site_display,
            )
            continue
        except Exception as exc:
            logger.warning(
                "Profile route %s failed while authenticating on %s: %s",
                route_index,
                site_display,
                exc,
            )
            continue

        if authed is None:
            continue

        try:
            user = await asyncio.wait_for(
                authed.get_user(identifier),
                timeout=PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning(
                "Profile route %s timed out looking up %s on %s",
                route_index,
                identifier,
                site_display,
            )
            continue
        except Exception as exc:
            logger.warning(
                "Profile route %s failed looking up %s on %s: %s",
                route_index,
                identifier,
                site_display,
                exc,
            )
            continue

        completed_routes += 1
        if user is not None:
            if route_index > 1:
                logger.info(
                    "Resolved profile %s on %s through alternate proxy route %s",
                    identifier,
                    site_display,
                    route_index,
                )
            return SiteUserLookupResult(
                user=user,
                conclusive_missing=False,
                attempted_routes=attempted_routes,
                completed_routes=completed_routes,
            )

    return SiteUserLookupResult(
        user=None,
        conclusive_missing=(
            attempted_routes > 0 and completed_routes == attempted_routes
        ),
        attempted_routes=attempted_routes,
        completed_routes=completed_routes,
    )


@router.get("/search", response_model=UserSearchResponse)
async def search_user(
    username: str = Query(..., description="Username to search for"),
    site: str = Query("onlyfans", description="Site to search on (onlyfans, fansly)"),
) -> UserSearchResponse:
    """Search for a user by username on a specific site.

    This endpoint looks up a user profile on the specified site
    using the configured authenticated accounts.

    Args:
        username: The username to search for
        site: The site to search on (onlyfans, fansly)

    Returns:
        User details if found
    """
    try:
        # Normalize site name
        site_lower = site.strip().lower()
        if site_lower == "onlyfans":
            site_display = "OnlyFans"
        elif site_lower == "fansly":
            site_display = "Fansly"
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported site: {site}. Supported: onlyfans, fansly",
            )

        # Clean username (remove @ if present)
        username = username.strip().lstrip("@")

        if not username:
            raise HTTPException(status_code=400, detail="Username is required")

        lookup = await _lookup_site_user(site_lower, username)
        user = lookup.user

        if not user:
            if not lookup.conclusive_missing:
                return UserSearchResponse(
                    success=False,
                    error=(
                        f"Could not confirm whether '{username}' exists on {site_display}; "
                        "the available direct/proxy routes did not all complete"
                    ),
                )
            return UserSearchResponse(
                success=False, error=f"User '{username}' not found on {site_display}"
            )

        # Extract user info
        result = await _extract_user_info(user, site_display)
        # A platform can resolve a retired handle to the creator's current
        # handle. Persist the requested handle before returning so it is
        # immediately searchable and visible on the profile page.
        await _sync_site_resolved_profile_user(
            site_lower,
            user,
            observed_aliases=(username,),
        )

        return UserSearchResponse(success=True, user=result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"User search failed: {e}", exc_info=True)
        return UserSearchResponse(success=False, error=str(e))


@router.get("/resolve", response_model=UserIdentifierResolutionResponse)
async def resolve_profile_identifier(
    identifier: str = Query(..., min_length=1, description="User ID, username, or alias"),
    site: str = Query("onlyfans", description="Site to resolve on"),
    page: int = Query(1, ge=1, description="Candidate page"),
    page_size: int = Query(20, ge=1, le=100, description="Candidates per page"),
) -> UserIdentifierResolutionResponse:
    """Resolve a profile URL identifier without ever guessing among local claims."""

    site_lower = site.strip().lower()
    if site_lower not in {"onlyfans", "fansly"}:
        return UserIdentifierResolutionResponse(
            success=False,
            status="unavailable",
            page=page,
            page_size=page_size,
            error=f"Unsupported site: {site}",
        )

    normalized_identifier = _normalise_profile_identifier(identifier)
    if not normalized_identifier:
        return UserIdentifierResolutionResponse(
            success=False,
            status="not_found",
            page=page,
            page_size=page_size,
            error="A user ID, username, or alias is required",
        )

    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_lower)
    if site_db is None:
        return UserIdentifierResolutionResponse(
            success=False,
            status="unavailable",
            page=page,
            page_size=page_size,
            error=f"Site '{site}' not found",
        )

    local_candidates = _local_profile_identifier_candidates(
        site_db,
        normalized_identifier,
    )
    if len(local_candidates) == 1:
        candidate = local_candidates[0]
        return UserIdentifierResolutionResponse(
            success=True,
            status="resolved",
            user_id=candidate.user_id,
            username=candidate.username,
            total=1,
            page=page,
            page_size=page_size,
        )

    if len(local_candidates) > 1:
        start = (page - 1) * page_size
        return UserIdentifierResolutionResponse(
            success=True,
            status="ambiguous",
            candidates=local_candidates[start : start + page_size],
            total=len(local_candidates),
            page=page,
            page_size=page_size,
        )

    lookup_value: str | int = (
        int(normalized_identifier)
        if normalized_identifier.isdigit() and int(normalized_identifier) > 0
        else normalized_identifier
    )
    lookup = await _lookup_site_user(site_lower, lookup_value)
    api_user = lookup.user

    if api_user is None and not lookup.conclusive_missing:
        return UserIdentifierResolutionResponse(
            success=False,
            status="unavailable",
            page=page,
            page_size=page_size,
            error=(
                f"Could not resolve '{identifier}' on {_get_site_display_name(site_lower)} "
                "after trying the available direct/proxy routes"
            ),
        )

    if api_user is None:
        return UserIdentifierResolutionResponse(
            success=False,
            status="not_found",
            page=page,
            page_size=page_size,
            error=f"User '{identifier}' not found on {_get_site_display_name(site_lower)}",
        )

    observed_aliases = (
        (normalized_identifier,)
        if isinstance(lookup_value, str)
        else ()
    )
    if not await _sync_site_resolved_profile_user(
        site_lower,
        api_user,
        observed_aliases=observed_aliases,
    ):
        return UserIdentifierResolutionResponse(
            success=False,
            status="unavailable",
            page=page,
            page_size=page_size,
            error=f"Resolved '{identifier}' but could not save its local profile",
        )

    resolved_user_id = int(getattr(api_user, "id"))
    resolved_username = str(
        getattr(api_user, "username", None) or f"u{resolved_user_id}"
    )
    return UserIdentifierResolutionResponse(
        success=True,
        status="resolved",
        user_id=resolved_user_id,
        username=resolved_username,
        total=1,
        page=page,
        page_size=page_size,
    )


async def _extract_user_info(user: Any, site: str) -> UserSearchResult:
    """Extract standardized user info from API user object.

    Args:
        user: User object from the API
        site: Site name

    Returns:
        Standardized user search result
    """
    # Try different attribute names based on site
    user_id = getattr(user, "id", 0)
    username_val = getattr(user, "username", "") or ""
    name = getattr(user, "name", None) or getattr(user, "displayName", None)

    # Avatar
    avatar_url = await get_value(user, "avatar", "avatarUrl", "avatar_url")

    # Header/banner
    header_url = await get_value(user, "header", "headerUrl", "banner")

    # Bio/about
    bio = await get_value(user, "about", "bio", "rawAbout")

    # Verification
    is_verified = bool(await get_value(user, "isVerified", "verified", default=False))

    # Subscription status
    is_subscribed = getattr(
        user, "subscribedByData", None
    ) is not None or await get_value(user, "isSubscribed", default=False)

    # Price
    subscription_price: float | None = None
    try:
        price_val = await get_value(user, "subscribePrice", "subscription_price")
        if price_val is not None:
            subscription_price = float(price_val)
    except (ValueError, TypeError):
        pass

    # Stats
    posts_count = await get_value(user, "postsCount", "posts_count")
    photos_count = await get_value(user, "photosCount", "photos_count")
    videos_count = await get_value(user, "videosCount", "videos_count")

    return UserSearchResult(
        id=user_id,
        username=username_val,
        name=name,
        avatar_url=avatar_url,
        header_url=header_url,
        bio=bio,
        is_verified=is_verified,
        is_subscribed=is_subscribed,
        subscription_price=subscription_price,
        posts_count=posts_count,
        photos_count=photos_count,
        videos_count=videos_count,
        site=site,
    )


class DatabaseUserInfo(BaseModel):
    """User info from local database."""

    id: int
    user_id: int
    username: str
    name: str | None = None
    description: str | None = None
    sex: int | None = None
    gender: int | None = None
    profile_type: int | None = None
    sexual_content: int | None = None
    partner_sex: int | None = None
    partner_gender: int | None = None
    ai_assisted: bool = False
    verified: bool = False
    avatar_url: str | None = None
    header_url: str | None = None
    price: float = 0.0
    post_count: int = 0
    media_count: int = 0
    image_count: int = 0
    video_count: int = 0
    favorite: bool = False
    is_active: bool = True
    is_subscribed: bool = False
    is_pending_order: bool = False
    site: str
    downloaded_at: str | None = None
    last_checked_at: str | None = None


class UsersListResponse(BaseModel):
    """Response for listing users from local database."""

    success: bool
    users: List[DatabaseUserInfo] = []
    total: int | None = 0
    active: int | None = 0
    inactive: int | None = 0
    avg_price: float | None = 0.0
    free: int | None = 0
    paid: int | None = 0
    has_more: bool | None = None
    error: str | None = None


class UserCardKey(BaseModel):
    site: str
    user_id: int = Field(gt=0, lt=2**63)


class UserCardsRequest(BaseModel):
    keys: list[UserCardKey] = Field(max_length=200)


@router.post("/cards")
async def user_cards(payload: UserCardsRequest, background_tasks: BackgroundTasks):
    keys = [{"site": key.site.lower(), "entity_type": "user", "entity_id": key.user_id}
            for key in payload.keys]
    for key in keys:
        favourites_service.validate_key(key["site"], "user", key["entity_id"])

    def read_cards():
        from ultima_scraper_db_archive.global_db import global_session
        with global_session() as session:
            metadata = favourites_service.resolve_metadata(session, keys, favourite_users=False)
            return list(metadata.values())

    users = await run_database_call(read_cards)
    queue_cached_profile_refresh(background_tasks, users)
    return {"users": users}


@router.get("/", response_model=UsersListResponse)
async def list_users(
    background_tasks: BackgroundTasks = None,
    site: str | None = Query(None, description="Filter by site"),
    search: str | None = Query(
        None, description="Search by identifier (username or user ID)"
    ),
    search_description: str | None = Query(None, description="Search by description"),
    sex: List[int] | None = Query(
        None,
        description="Filter by sex (repeatable): 0=female, 1=male, 2=other, -1=null/unset",
    ),
    gender: List[int] | None = Query(
        None,
        description="Filter by gender (repeatable): 0=woman, 1=man, 2=other, -1=null/unset",
    ),
    profile_type: List[int] | None = Query(
        None,
        description="Filter by profile type (repeatable): 0=solo, 1=couple, 2=group, 3=other, -1=null/unset",
    ),
    sexual_content: List[int] | None = Query(
        None,
        description="Filter by sexual content (repeatable): 0=heterosexual, 1=lesbian, 2=gay, 3=bisexual, 4=trans_queer, 5=solo, 6=other, -1=null/unset",
    ),
    active_only: bool = Query(False, description="Only show active users"),
    min_price: float | None = Query(None, description="Minimum price filter"),
    max_price: float | None = Query(None, description="Maximum price filter"),
    min_post_count: int | None = Query(None, description="Minimum post count filter"),
    max_post_count: int | None = Query(None, description="Maximum post count filter"),
    min_image_count: int | None = Query(None, description="Minimum image count filter"),
    max_image_count: int | None = Query(None, description="Maximum image count filter"),
    min_video_count: int | None = Query(None, description="Minimum video count filter"),
    max_video_count: int | None = Query(None, description="Maximum video count filter"),
    subscribed_only: bool = Query(False, description="Only show subscribed users"),
    exclude_subscribed: bool = Query(False, description="Exclude subscribed users"),
    favorite_only: bool = Query(False, description="Only show favorited users"),
    performer_only: bool = Query(False, description="Only show performers"),
    exclude_performers: bool = Query(False, description="Exclude performers"),
    sort_by: str = Query(
        "id",
        description="Sort field: id, username, price, post_count, last_checked_at, created_at",
    ),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(100, ge=1, le=500, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    include_stats: bool = Query(True, description="Calculate exact filtered totals; omit for bounded pages"),
    stats_only: bool = Query(False, description="Return exact totals without loading a user page"),
) -> UsersListResponse:
    """List users from the local database.

    Flow:
    1. Search DB for matching users
    2. If no results AND specific search term provided, search site API
    3. If found via API, save to DB
    4. Return DB results

    Args:
        site: Filter by site (onlyfans, fansly)
        search: Search username with LIKE %search%
        active_only: Only return active users
        performer_only: Only return performer users
        exclude_performers: Exclude performer users
        sort_by: Sort field (username, price, post_count, last_checked_at, created_at)
        sort_order: Sort order (asc or desc)
        limit: Maximum number of results
        offset: Offset for pagination

    Returns:
        List of users with their info
    """
    try:
        if sex is not None and isinstance(sex, (list, tuple, set)):
            invalid_sex = [v for v in sex if v not in (-1, 0, 1, 2)]
            if invalid_sex:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid sex filter value(s). Allowed values: -1=null/unset, 0=female, 1=male, 2=other",
                )
        else:
            sex = None
        if gender is not None and isinstance(gender, (list, tuple, set)):
            invalid_gender = [v for v in gender if v not in (-1, 0, 1, 2)]
            if invalid_gender:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid gender filter value(s). Allowed values: -1=null/unset, 0=woman, 1=man, 2=other",
                )
        else:
            gender = None
        if profile_type is not None and isinstance(profile_type, (list, tuple, set)):
            invalid_profile_type = [v for v in profile_type if v not in (-1, 0, 1, 2, 3)]
            if invalid_profile_type:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid profile_type filter value(s). Allowed values: -1=null/unset, 0=solo, 1=couple, 2=group, 3=other",
                )
        else:
            profile_type = None
        if sexual_content is not None and isinstance(sexual_content, (list, tuple, set)):
            invalid_sexual_content = [
                v for v in sexual_content if v not in (-1, 0, 1, 2, 3, 4, 5, 6)
            ]
            if invalid_sexual_content:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid sexual_content filter value(s). Allowed values: -1=null/unset, 0=heterosexual, 1=lesbian, 2=gay, 3=bisexual, 4=trans_queer, 5=solo, 6=other",
                )
        else:
            sexual_content = None

        min_price = min_price if isinstance(min_price, (int, float)) else None
        max_price = max_price if isinstance(max_price, (int, float)) else None
        min_post_count = min_post_count if isinstance(min_post_count, int) else None
        max_post_count = max_post_count if isinstance(max_post_count, int) else None
        min_image_count = min_image_count if isinstance(min_image_count, int) else None
        max_image_count = max_image_count if isinstance(max_image_count, int) else None
        min_video_count = min_video_count if isinstance(min_video_count, int) else None
        max_video_count = max_video_count if isinstance(max_video_count, int) else None
        active_only = bool(active_only) if isinstance(active_only, bool) else False
        subscribed_only = bool(subscribed_only) if isinstance(subscribed_only, bool) else False
        exclude_subscribed = (
            bool(exclude_subscribed) if isinstance(exclude_subscribed, bool) else False
        )
        favorite_only = bool(favorite_only) if isinstance(favorite_only, bool) else False
        performer_only = (
            bool(performer_only) if isinstance(performer_only, bool) else False
        )
        exclude_performers = (
            bool(exclude_performers) if isinstance(exclude_performers, bool) else False
        )
        sort_by = sort_by if isinstance(sort_by, str) else "id"
        sort_order = sort_order if isinstance(sort_order, str) else "desc"
        limit = limit if isinstance(limit, int) else 100
        offset = offset if isinstance(offset, int) else 0
        include_stats = include_stats if isinstance(include_stats, bool) else True
        stats_only = stats_only if isinstance(stats_only, bool) else False
        if stats_only and not include_stats:
            raise HTTPException(422, "stats_only requires include_stats")

        trimmed_search = search.strip() if isinstance(search, str) and search.strip() else ""
        description_terms = (
            _extract_description_terms(search_description)
            if isinstance(search_description, str)
            else []
        )

        db_manager = MultiSiteDatabaseManager()
        all_users: List[DatabaseUserInfo] = []
        total_count = 0
        active_count = 0
        inactive_count = 0
        free_count = 0
        paid_count = 0
        price_total = 0.0
        has_more = False

        sites_to_query = [site.lower()] if site else ["onlyfans", "fansly"]

        if trimmed_search and site and not stats_only:
            site_db = db_manager.get_or_create_manager(site.lower())
            if site_db:
                await _refresh_stale_existing_user(trimmed_search, site, site_db)

        def read_users() -> None:
            nonlocal total_count, active_count, inactive_count
            nonlocal price_total, free_count, paid_count
            nonlocal has_more
            for site_name in sites_to_query:
                site_db = db_manager.get_or_create_manager(site_name)
                if not site_db:
                    continue

                with site_db.get_session() as session:
                    if description_terms and session.get_bind().dialect.name == "postgresql":
                        # Common terms match ~1M price-index rows; at the default 4MB
                        # the BitmapAnd goes lossy and rechecks millions of heap rows.
                        session.execute(sa_text("SET LOCAL work_mem = '64MB'"))

                    def apply_identifier_filter(q: Any) -> Any:
                        return _apply_identifier_filter(q, trimmed_search)

                    def apply_user_info_filters(
                        q: Any, *, include_description: bool = True
                    ) -> Any:
                        if description_terms and include_description:
                            # Match non-empty bio descriptions to allow using GIN trigram indexes
                            current_matches = apply_user_info_filters(
                                sa_select(UserInfoModel.user_id).where(
                                    UserInfoModel.description.isnot(None),
                                    UserInfoModel.description != "",
                                    and_(
                                        *[
                                            UserInfoModel.description.ilike(f"%{term}%")
                                            for term in description_terms
                                        ]
                                    ),
                                ),
                                include_description=False,
                            )
                            historical_matches = sa_select(
                                HistoUserInfoModel.user_id
                            ).where(
                                HistoUserInfoModel.description.isnot(None),
                                HistoUserInfoModel.description != "",
                                and_(
                                    *[
                                        HistoUserInfoModel.description.ilike(f"%{term}%")
                                        for term in description_terms
                                    ]
                                ),
                            )
                            # Performer / active filters stay on the outer query: joining
                            # users here repeated a random users lookup per candidate.
                            # UNION ALL avoids the expensive sort/deduplicate of UNION
                            # while maintaining set semantics inside IN (subquery).
                            q = q.where(
                                UserModel.id.in_(
                                    current_matches.union_all(historical_matches)
                                )
                            )
                        if sex is not None:
                            include_null_sex = -1 in sex
                            concrete_sex = [
                                value for value in sex if value in (0, 1, 2)
                            ]
                            if include_null_sex and concrete_sex:
                                q = q.where(
                                    or_(
                                        UserInfoModel.sex.in_(concrete_sex),
                                        UserInfoModel.partner_sex.in_(concrete_sex),
                                        UserInfoModel.sex.is_(None),
                                    )
                                )
                            elif include_null_sex:
                                q = q.where(UserInfoModel.sex.is_(None))
                            elif concrete_sex:
                                q = q.where(
                                    or_(
                                        UserInfoModel.sex.in_(concrete_sex),
                                        UserInfoModel.partner_sex.in_(concrete_sex),
                                    )
                                )
                        if gender is not None:
                            include_null_gender = -1 in gender
                            concrete_gender = [
                                value for value in gender if value in (0, 1, 2)
                            ]
                            if include_null_gender and concrete_gender:
                                q = q.where(
                                    or_(
                                        UserInfoModel.gender.in_(concrete_gender),
                                        UserInfoModel.partner_gender.in_(concrete_gender),
                                        UserInfoModel.gender.is_(None),
                                    )
                                )
                            elif include_null_gender:
                                q = q.where(UserInfoModel.gender.is_(None))
                            elif concrete_gender:
                                q = q.where(
                                    or_(
                                        UserInfoModel.gender.in_(concrete_gender),
                                        UserInfoModel.partner_gender.in_(concrete_gender),
                                    )
                                )
                        if profile_type is not None:
                            include_null_profile_type = -1 in profile_type
                            concrete_profile_type = [
                                value for value in profile_type if value in (0, 1, 2, 3)
                            ]
                            if include_null_profile_type and concrete_profile_type:
                                q = q.where(
                                    or_(
                                        UserInfoModel.profile_type.in_(concrete_profile_type),
                                        UserInfoModel.profile_type.is_(None),
                                    )
                                )
                            elif include_null_profile_type:
                                q = q.where(UserInfoModel.profile_type.is_(None))
                            elif concrete_profile_type:
                                q = q.where(
                                    UserInfoModel.profile_type.in_(concrete_profile_type)
                                )
                        if sexual_content is not None:
                            include_null_sexual_content = -1 in sexual_content
                            concrete_sexual_content = [
                                value
                                for value in sexual_content
                                if value in (0, 1, 2, 3, 4, 5, 6)
                            ]
                            if include_null_sexual_content and concrete_sexual_content:
                                q = q.where(
                                    or_(
                                        UserInfoModel.sexual_content.in_(
                                            concrete_sexual_content
                                        ),
                                        UserInfoModel.sexual_content.is_(None),
                                    )
                                )
                            elif include_null_sexual_content:
                                q = q.where(UserInfoModel.sexual_content.is_(None))
                            elif concrete_sexual_content:
                                q = q.where(
                                    UserInfoModel.sexual_content.in_(
                                        concrete_sexual_content
                                    )
                                )
                        if min_price is not None:
                            if min_price > 0:
                                q = q.where(UserInfoModel.price >= min_price)
                            elif min_price < 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.price >= min_price,
                                        UserInfoModel.price.is_(None),
                                    )
                                )
                        if max_price is not None:
                            if max_price >= 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.price <= max_price,
                                        UserInfoModel.price.is_(None),
                                    )
                                )
                            else:
                                q = q.where(UserInfoModel.price <= max_price)
                        if min_post_count is not None:
                            if min_post_count > 0:
                                q = q.where(UserInfoModel.post_count >= min_post_count)
                            elif min_post_count < 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.post_count >= min_post_count,
                                        UserInfoModel.post_count.is_(None),
                                    )
                                )
                        if max_post_count is not None:
                            if max_post_count >= 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.post_count <= max_post_count,
                                        UserInfoModel.post_count.is_(None),
                                    )
                                )
                            else:
                                q = q.where(UserInfoModel.post_count <= max_post_count)
                        if min_image_count is not None:
                            if min_image_count > 0:
                                q = q.where(
                                    UserInfoModel.image_count >= min_image_count
                                )
                            elif min_image_count < 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.image_count >= min_image_count,
                                        UserInfoModel.image_count.is_(None),
                                    )
                                )
                        if max_image_count is not None:
                            if max_image_count >= 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.image_count <= max_image_count,
                                        UserInfoModel.image_count.is_(None),
                                    )
                                )
                            else:
                                q = q.where(
                                    UserInfoModel.image_count <= max_image_count
                                )
                        if min_video_count is not None:
                            if min_video_count > 0:
                                q = q.where(
                                    UserInfoModel.video_count >= min_video_count
                                )
                            elif min_video_count < 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.video_count >= min_video_count,
                                        UserInfoModel.video_count.is_(None),
                                    )
                                )
                        if max_video_count is not None:
                            if max_video_count >= 0:
                                q = q.where(
                                    or_(
                                        UserInfoModel.video_count <= max_video_count,
                                        UserInfoModel.video_count.is_(None),
                                    )
                                )
                            else:
                                q = q.where(
                                    UserInfoModel.video_count <= max_video_count
                                )
                        return q

                    # Join users with their info
                    query = select(UserModel, UserInfoModel).outerjoin(
                        UserInfoModel, UserModel.id == UserInfoModel.user_id
                    )

                    # Apply search filter (wildcard)
                    query = apply_identifier_filter(query)

                    query = apply_user_info_filters(query)

                    if subscribed_only:
                        query = query.where(
                            exists(
                                select(1).where(
                                    SubscriptionModel.user_id == UserModel.id,
                                    SubscriptionModel.active.is_(True),
                                )
                            )
                        )
                    elif exclude_subscribed:
                        query = query.where(
                            ~exists(
                                select(1).where(
                                    SubscriptionModel.user_id == UserModel.id,
                                    SubscriptionModel.active.is_(True),
                                )
                            )
                        )

                    # Apply active filter
                    if active_only:
                        query = query.where(UserModel.active.is_(True))
                    if favorite_only:
                        from ultima_scraper_db_archive.favourites_models import FavouriteModel

                        favourites_service.ensure_active(session, site_name)
                        query = query.join(FavouriteModel, and_(FavouriteModel.site == site_name, FavouriteModel.entity_type == "user", FavouriteModel.entity_id == UserModel.id))
                    if performer_only:
                        query = query.where(UserModel.performer.is_(True))
                    elif exclude_performers:
                        query = query.where(UserModel.performer.is_(False))

                    # Apply sorting
                    sort_column = None
                    if sort_by == "id":
                        sort_column = UserModel.id
                    elif sort_by == "username":
                        sort_column = func.lower(UserModel.username)
                    elif sort_by == "price" and UserInfoModel:
                        sort_column = UserInfoModel.price
                    elif sort_by == "post_count" and UserInfoModel:
                        sort_column = UserInfoModel.post_count
                    elif sort_by == "last_checked_at":
                        sort_column = UserModel.last_checked_at
                    elif sort_by == "created_at":
                        sort_column = UserModel.created_at
                    else:
                        # idx_users_username_lower serves this ordering.
                        sort_column = func.lower(UserModel.username)

                    if sort_order.lower() == "desc":
                        sort_expression = sort_column.desc()
                    else:
                        sort_expression = sort_column.asc()
                    query = query.order_by(sort_expression)

                    if include_stats:
                        # Get all aggregate counts in a single query using conditional aggregation
                        stats_query: Any = (
                            sa_select(
                                func.count(UserModel.id),
                                func.sum(case((UserModel.active.is_(True), 1), else_=0)),
                                func.sum(case((UserModel.active.is_(False), 1), else_=0)),
                                func.sum(func.coalesce(UserInfoModel.price, 0.0)),
                                func.sum(
                                    case(
                                        (func.coalesce(UserInfoModel.price, 0.0) <= 0.0, 1),
                                        else_=0,
                                    )
                                ),
                            )
                            .select_from(UserModel)
                            .outerjoin(UserInfoModel, UserModel.id == UserInfoModel.user_id)
                        )
                        stats_query = apply_user_info_filters(stats_query)
                        if subscribed_only:
                            stats_query = stats_query.where(
                                exists(
                                    select(1).where(
                                        SubscriptionModel.user_id == UserModel.id,
                                        SubscriptionModel.active.is_(True),
                                    )
                                )
                            )
                        elif exclude_subscribed:
                            stats_query = stats_query.where(
                                ~exists(
                                    select(1).where(
                                        SubscriptionModel.user_id == UserModel.id,
                                        SubscriptionModel.active.is_(True),
                                    )
                                )
                            )
                        stats_query = apply_identifier_filter(stats_query)
                        if active_only:
                            stats_query = stats_query.where(UserModel.active.is_(True))
                        if favorite_only:
                            stats_query = stats_query.join(FavouriteModel, and_(FavouriteModel.site == site_name, FavouriteModel.entity_type == "user", FavouriteModel.entity_id == UserModel.id))
                        if performer_only:
                            stats_query = stats_query.where(UserModel.performer.is_(True))
                        elif exclude_performers:
                            stats_query = stats_query.where(UserModel.performer.is_(False))

                        compiled_stats = stats_query.compile()
                        stats_key = (site_name, session.get_bind(), str(compiled_stats),
                                     json.dumps(compiled_stats.params, sort_keys=True, default=str))
                        stats_row = user_stats_cache.get(stats_key, lambda: session.exec(stats_query).one())
                        (
                            raw_total,
                            raw_active,
                            raw_inactive,
                            raw_price_sum,
                            raw_free,
                        ) = cast(UserStatsRow, stats_row)

                        site_total = int(raw_total or 0)
                        site_active = int(raw_active or 0)
                        site_inactive = 0 if active_only else int(raw_inactive or 0)
                        site_price_sum = float(raw_price_sum or 0.0)
                        site_free = int(raw_free or 0)
                        site_paid = max(0, site_total - site_free)

                        total_count += site_total
                        active_count += site_active
                        inactive_count += site_inactive
                        price_total += site_price_sum
                        free_count += site_free
                        paid_count += site_paid

                    if stats_only:
                        continue

                    # Apply pagination
                    query = _paginate_user_query(
                        query, sort_expression, offset, limit + 1,
                        profile_probe=(session.get_bind().dialect.name == "postgresql"
                                       and sort_by == "id" and active_only and performer_only
                                       and min_price is not None and min_price > 0
                                       and min_video_count is not None and min_video_count > 0
                                       and not trimmed_search and not description_terms and not favorite_only),
                    )

                    results = cast(list[UserWithInfoRow], session.exec(query).all())
                    has_more = has_more or len(results) > limit
                    results = results[:limit]

                    # Optimized bulk check for pending/processing orders
                    user_ids = [u.id for u, _ in results]
                    pending_order_user_ids: set[int] = set()
                    latest_media_by_user_id: dict[int, UserProfileMediaModel] = {}
                    subscribed_user_ids: set[int] = set()
                    if user_ids:
                        from .orders import OrderedPerformerModel, create_orders_session

                        latest_media_subquery = (
                            select(
                                UserProfileMediaModel.user_id.label("user_id"),
                                func.max(UserProfileMediaModel.created_at).label(
                                    "created_at"
                                ),
                            )
                            .where(UserProfileMediaModel.user_id.in_(user_ids))
                            .group_by(UserProfileMediaModel.user_id)
                            .subquery()
                        )
                        # Several snapshots can share a timestamp. Select one
                        # deterministic ID per user before transferring any rows.
                        latest_media_ids = (
                            select(func.max(UserProfileMediaModel.id).label("id"))
                            .join(
                                latest_media_subquery,
                                and_(
                                    UserProfileMediaModel.user_id
                                    == latest_media_subquery.c.user_id,
                                    UserProfileMediaModel.created_at
                                    == latest_media_subquery.c.created_at,
                                ),
                            )
                            .group_by(UserProfileMediaModel.user_id)
                            .subquery()
                        )
                        latest_media_rows = session.exec(
                            select(UserProfileMediaModel).join(
                                latest_media_ids,
                                UserProfileMediaModel.id == latest_media_ids.c.id,
                            )
                        ).all()
                        latest_media_by_user_id = {
                            row.user_id: row for row in latest_media_rows
                        }

                        subscribed_user_ids = set(
                            cast(
                                list[int],
                                session.exec(
                                    select(SubscriptionModel.user_id)
                                    .where(
                                        SubscriptionModel.user_id.in_(user_ids),
                                        SubscriptionModel.active.is_(True),
                                    )
                                    .distinct()
                                ).all(),
                            )
                        )

                        with create_orders_session() as orders_session:
                            orders_query = orders_session.query(
                                OrderedPerformerModel.user_id
                            ).filter(
                                OrderedPerformerModel.user_id.in_(user_ids),
                                OrderedPerformerModel.status.in_(
                                    ["pending", "processing"]
                                ),
                            )
                            if hasattr(orders_query, "distinct"):
                                orders_query = orders_query.distinct()
                            pending_items = cast(
                                list[tuple[int]],
                                orders_query.all(),
                            )
                            pending_order_user_ids = {row[0] for row in pending_items}

                    favourited_ids = favourites_service.user_states(session, site_name, [u for u, _ in results])
                    for user, info in results:
                        latest_media = latest_media_by_user_id.get(user.id)
                        is_subscribed = user.id in subscribed_user_ids

                        user_data = DatabaseUserInfo(
                            id=user.id,
                            user_id=user.id,
                            username=user.username or f"user_{user.id}",
                            name=info.name if info else None,
                            description=info.description if info else None,
                            sex=getattr(info, "sex", None) if info else None,
                            gender=getattr(info, "gender", None) if info else None,
                            profile_type=getattr(info, "profile_type", None) if info else None,
                            sexual_content=getattr(info, "sexual_content", None) if info else None,
                            partner_sex=getattr(info, "partner_sex", None) if info else None,
                            partner_gender=getattr(info, "partner_gender", None) if info else None,
                            ai_assisted=bool(getattr(info, "ai_assisted", False)) if info else False,
                            verified=bool(getattr(info, "verified", False)) if info else False,
                            avatar_url=latest_media.avatar_url
                            if latest_media
                            else None,
                            header_url=latest_media.header_url
                            if latest_media
                            else None,
                            price=info.price if info else 0.0,
                            post_count=info.post_count if info else 0,
                            media_count=info.media_count if info else 0,
                            image_count=info.image_count if info else 0,
                            video_count=info.video_count if info else 0,
                            favorite=user.id in favourited_ids,
                            is_active=user.active,
                            is_subscribed=is_subscribed,
                            is_pending_order=user.id in pending_order_user_ids,
                            site=site_name,
                            downloaded_at=(
                                user.downloaded_at.isoformat()
                                if user.downloaded_at
                                else None
                            ),
                            last_checked_at=(
                                user.last_checked_at.isoformat()
                                if user.last_checked_at
                                else None
                            ),
                        )
                        all_users.append(user_data)

        await run_database_call(read_users)

        # If no results found and we have a specific site+search, try API search.
        # Explicit username lookup should still work even when list filters are active.
        if not stats_only and not all_users and trimmed_search and site:
            logger.info(
                f"No DB results for '{trimmed_search}' on {site}, trying API search..."
            )
            api_user = await _search_and_save_user(trimmed_search, site, db_manager)
            if api_user:
                all_users.append(api_user)
                total_count = 1
                active_count = 1 if api_user.is_active else 0
                inactive_count = 0 if api_user.is_active else 1
                price_total = float(api_user.price)
                free_count = 1 if float(api_user.price) <= 0.0 else 0
                paid_count = 0 if free_count else 1

        avg_price = (price_total / total_count) if total_count > 0 else 0.0
        if active_only:
            inactive_count = 0

        if not stats_only:
            queue_cached_profile_refresh(background_tasks, all_users)

        return UsersListResponse(
            success=True,
            users=all_users,
            total=total_count if include_stats else None,
            active=active_count if include_stats else None,
            inactive=inactive_count if include_stats else None,
            avg_price=round(avg_price, 2) if include_stats else None,
            free=free_count if include_stats else None,
            paid=paid_count if include_stats else None,
            has_more=None if stats_only else has_more,
        )

    except Exception as e:
        logger.error(f"Failed to list users: {e}", exc_info=True)
        return UsersListResponse(
            success=False,
            error=str(e),
        )


class UserProfileMediaInfo(BaseModel):
    """Profile media history entry."""

    id: int
    avatar_url: str | None = None
    header_url: str | None = None
    created_at: str | None = None


class SocialInfo(BaseModel):
    """Social media link info."""

    site: str
    identifier: str


class DetailedUserInfo(BaseModel):
    """Full user profile info (extends DatabaseUserInfo with extra fields)."""

    id: int
    user_id: int
    username: str
    name: str | None = None
    description: str | None = None
    avatar_url: str | None = None
    header_url: str | None = None
    price: float = 0.0
    post_count: int = 0
    media_count: int = 0
    image_count: int = 0
    video_count: int = 0
    audio_count: int = 0
    stream_count: int = 0
    archived_post_count: int = 0
    subscribers_count: int = 0
    favourited_count: int = 0
    favourites_count: int = 0
    size: int = 0
    display_size: int = 0
    estimated_size: int | None = None
    size_source: str = "unknown"
    statistics_bucket_date: str | None = None
    location: str | None = None
    website: str | None = None
    sex: int | None = None
    gender: int | None = None
    profile_type: int | None = None
    sexual_content: int | None = None
    partner_sex: int | None = None
    partner_gender: int | None = None
    ai_assisted: bool = False
    verified: bool = False
    promotion: bool = False
    is_active: bool = True
    is_subscribed: bool = False
    subscription_expires_at: str | None = None
    balance: float = 0.0
    spend: bool = False
    performer: bool = False
    favorite: bool = False
    site: str
    join_date: str | None = None
    downloaded_at: str | None = None
    first_downloaded_at: str | None = None
    last_checked_at: str | None = None
    created_at: str | None = None
    profile_media_history: List[UserProfileMediaInfo] = []
    socials: List[SocialInfo] = []


class DetailedUserResponse(BaseModel):
    """Response for detailed user profile."""

    success: bool
    user: DetailedUserInfo | None = None
    error: str | None = None
    is_stale: bool = False
    is_refreshing: bool = False
    refresh_error: str | None = None


class UserUpdateRequest(BaseModel):
    """Fields that can be updated for a user profile."""

    username: str | None = None
    name: str | None = None
    alias: str | None = None
    description: str | None = None
    sex: int | None = None
    gender: int | None = None
    profile_type: int | None = None
    sexual_content: int | None = None
    partner_sex: int | None = None
    partner_gender: int | None = None
    favorite: bool | None = None
    ai_assisted: bool | None = None
    verified: bool | None = None


class UserUpdateResponse(BaseModel):
    """Response for user update operations."""

    success: bool
    user_id: int | None = None
    site: str | None = None
    username: str | None = None
    name: str | None = None
    alias: str | None = None
    description: str | None = None
    sex: int | None = None
    gender: int | None = None
    profile_type: int | None = None
    sexual_content: int | None = None
    partner_sex: int | None = None
    partner_gender: int | None = None
    favorite: bool | None = None
    ai_assisted: bool | None = None
    verified: bool | None = None
    error: str | None = None


@router.patch("/{user_id}", response_model=UserUpdateResponse)
async def update_user_profile(
    user_id: int,
    payload: UserUpdateRequest,
    site: str = Query("onlyfans", description="Site to update (onlyfans, fansly)"),
) -> UserUpdateResponse:
    """Update editable user profile fields stored in local DB."""
    try:
        site_lower = site.strip().lower()
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_lower)

        if not site_db:
            return UserUpdateResponse(
                success=False,
                error=f"Site '{site}' not found",
            )

        provided_fields = set(payload.model_fields_set)
        editable_fields = {
            "username",
            "name",
            "alias",
            "description",
            "sex",
            "gender",
            "profile_type",
            "sexual_content",
            "partner_sex",
            "partner_gender",
            "favorite",
            "ai_assisted",
            "verified",
        }
        requested_updates = provided_fields.intersection(editable_fields)
        info_fields = {
            "name",
            "description",
            "sex",
            "gender",
            "profile_type",
            "sexual_content",
            "partner_sex",
            "partner_gender",
            "ai_assisted",
            "verified",
        }

        if not requested_updates:
            return UserUpdateResponse(
                success=False,
                error="No editable fields were provided",
            )

        if "sex" in requested_updates and payload.sex not in (None, 0, 1, 2):
            raise HTTPException(
                status_code=400,
                detail="Invalid sex value. Allowed values: 0=female, 1=male, 2=other, null=unset",
            )
        if "gender" in requested_updates and payload.gender not in (None, 0, 1, 2):
            raise HTTPException(
                status_code=400,
                detail="Invalid gender value. Allowed values: 0=woman, 1=man, 2=other, null=unset",
            )
        if "partner_sex" in requested_updates and payload.partner_sex not in (None, 0, 1, 2):
            raise HTTPException(
                status_code=400,
                detail="Invalid partner_sex value. Allowed values: 0=female, 1=male, 2=other, null=unset",
            )
        if "partner_gender" in requested_updates and payload.partner_gender not in (None, 0, 1, 2):
            raise HTTPException(
                status_code=400,
                detail="Invalid partner_gender value. Allowed values: 0=woman, 1=man, 2=other, null=unset",
            )
        if "profile_type" in requested_updates and payload.profile_type not in (None, 0, 1, 2, 3):
            raise HTTPException(
                status_code=400,
                detail="Invalid profile_type value. Allowed values: 0=solo, 1=couple, 2=group, 3=other, null=unset",
            )
        if "sexual_content" in requested_updates and payload.sexual_content not in (
            None,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
        ):
            raise HTTPException(
                status_code=400,
                detail="Invalid sexual_content value. Allowed values: 0=heterosexual, 1=lesbian, 2=gay, 3=bisexual, 4=trans_queer, 5=solo, 6=other, null=unset",
            )

        with site_db.get_session() as session:
            user = session.exec(
                select(UserModel).where(UserModel.id == user_id)
            ).first()
            if not user:
                return UserUpdateResponse(
                    success=False,
                    error=f"User {user_id} not found on {site_lower}",
                )

            info = session.exec(
                select(UserInfoModel).where(UserInfoModel.user_id == user_id)
            ).first()
            if info is None and requested_updates.intersection(info_fields):
                info = UserInfoModel(user_id=user_id)
                session.add(info)
                session.flush()

            normalized_alias: str | None = None

            if "username" in requested_updates:
                candidate_username = (
                    payload.username.strip().lstrip("@") if payload.username else ""
                )
                if not candidate_username:
                    raise HTTPException(status_code=400, detail="Username is required")
                current_username = (user.username or "").strip().lstrip("@")
                if candidate_username.casefold() != current_username.casefold():
                    record_user_alias(
                        session,
                        user_id,
                        current_username,
                        canonical_username=candidate_username,
                    )
                    user.username = candidate_username

            if "name" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.name = payload.name
            if "description" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.description = payload.description
            if "sex" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.sex = payload.sex
            if "gender" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.gender = payload.gender
            if "profile_type" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.profile_type = payload.profile_type
            if "sexual_content" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.sexual_content = payload.sexual_content
            if "partner_sex" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.partner_sex = payload.partner_sex
            if "partner_gender" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.partner_gender = payload.partner_gender
            if "ai_assisted" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.ai_assisted = payload.ai_assisted
            if "verified" in requested_updates:
                if info is None:
                    info = UserInfoModel(user_id=user_id)
                    session.add(info)
                    session.flush()
                info.verified = payload.verified
            if "favorite" in requested_updates:
                favourites_service.set_user_state(session, site_lower, user, bool(payload.favorite))
            if "alias" in requested_updates:
                candidate_alias = (
                    payload.alias.strip().lstrip("@") if payload.alias else ""
                )
                canonical_username = (user.username or "").strip().lstrip("@")
                if (
                    candidate_alias
                    and candidate_alias.lower() != canonical_username.lower()
                ):
                    record_user_alias(
                        session,
                        user_id,
                        candidate_alias,
                        canonical_username=canonical_username,
                    )
                    normalized_alias = candidate_alias

            if info is not None:
                session.add(info)
            session.add(user)
            session.commit()
            user_stats_cache.invalidate_site(site_lower)
            if info is not None:
                session.refresh(info)
            session.refresh(user)

            if "favorite" in requested_updates:
                from ...redis_events import publish_ws_event
                from ..dependencies import get_backend_instance
                try:
                    instance = await get_backend_instance()
                    await publish_ws_event(getattr(instance, "redis", None), {"type": "favourite_changed", "site": site_lower, "entity_type": "user", "entity_id": user_id})
                except Exception:
                    # The state is committed. Focus/reconnect checks recover a
                    # missed hint without reporting the successful edit as failed.
                    logger.warning("Unable to publish committed favourite change", exc_info=True)

            return UserUpdateResponse(
                success=True,
                user_id=user_id,
                site=site_lower,
                username=user.username,
                name=info.name if info else None,
                alias=normalized_alias,
                description=info.description if info else None,
                sex=info.sex if info else None,
                gender=info.gender if info else None,
                profile_type=info.profile_type if info else None,
                sexual_content=info.sexual_content if info else None,
                partner_sex=info.partner_sex if info else None,
                partner_gender=info.partner_gender if info else None,
                favorite=favourites_service.user_state(session, site_lower, user),
                ai_assisted=info.ai_assisted if info else None,
                verified=info.verified if info else None,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update user profile: {e}", exc_info=True)
        return UserUpdateResponse(success=False, error=str(e))


# ── Eligible auth accounts for a performer ────────────────


class EligibleAuth(BaseModel):
    """An auth account that has subscription history with a performer."""

    auth_id: int
    auth_username: str | None = None
    is_active: bool = True


class EligibleAuthsResponse(BaseModel):
    """Response listing auth accounts with performer subscription history."""

    auths: list[EligibleAuth] = []
    error: str | None = None


class UserProfileRefreshResponse(BaseModel):
    """Acknowledgement for an operator-requested profile refresh."""

    accepted: bool
    is_refreshing: bool
    error: str | None = None


@router.get("/{user_id}/eligible-auths", response_model=EligibleAuthsResponse)
async def get_eligible_auths(
    user_id: int,
    site: str = Query("onlyfans", description="Site name (onlyfans, fansly)"),
) -> EligibleAuthsResponse:
    """Return auth accounts that have subscription history with this performer.

    Used by the frontend to let the user pick which auth to use for S+D jobs.
    Active subscriptions are preferred operationally, but an expired historical
    subscription is still a valid explicit auth choice for profile S+D jobs.
    """
    from ...job_system.subscription_validation import get_subscribed_auths

    def read_auths() -> EligibleAuthsResponse:
        site_lower = site.strip().lower()
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_lower)

        if not site_db:
            return EligibleAuthsResponse(error=f"Site '{site}' not supported")

        with site_db.get_session() as session:
            subs = get_subscribed_auths(session, user_id, include_expired=True)

        return EligibleAuthsResponse(
            auths=[
                EligibleAuth(
                    auth_id=s.auth_id,
                    auth_username=s.auth_username,
                    is_active=s.is_active,
                )
                for s in subs
            ],
        )

    return await run_database_call(read_auths)


@router.post("/{user_id}/refresh", response_model=UserProfileRefreshResponse)
async def refresh_user_profile(
    user_id: int,
    site: str = Query("onlyfans", description="Site to refresh (onlyfans, fansly)"),
) -> UserProfileRefreshResponse:
    """Request a deduplicated background refresh for a saved profile."""

    site_lower = site.strip().lower()
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_lower)
    if not site_db:
        return UserProfileRefreshResponse(
            accepted=False,
            is_refreshing=False,
            error=f"Site '{site}' not found",
        )

    def user_exists() -> bool:
        with site_db.get_session() as session:
            return session.get(UserModel, user_id) is not None

    if not await run_database_call(user_exists):
        return UserProfileRefreshResponse(
            accepted=False,
            is_refreshing=False,
            error=f"User {user_id} not found on {site}",
        )

    is_refreshing = _ensure_profile_refresh_started(
        site_lower,
        user_id,
        site_db,
        force=True,
    )
    return UserProfileRefreshResponse(
        accepted=is_refreshing,
        is_refreshing=is_refreshing,
    )


@router.get("/{user_id}", response_model=DetailedUserResponse)
async def get_user_profile(
    user_id: int,
    site: str = Query("onlyfans", description="Site to look up (onlyfans, fansly)"),
    backend: BackendInstance = Depends(get_backend_instance),
) -> DetailedUserResponse:
    """Get detailed profile information for a single user.

    Args:
        user_id: The database ID of the user
        site: Site name (onlyfans, fansly)

    Returns:
        Full user profile with all available details
    """
    try:
        db_manager = MultiSiteDatabaseManager()
        site_lower = site.strip().lower()
        site_db = db_manager.get_or_create_manager(site_lower)

        if not site_db:
            return DetailedUserResponse(
                success=False,
                error=f"Site '{site}' not found",
            )

        def read_profile_state() -> tuple[bool, bool]:
            found_in_db = False
            needs_api_refresh = False
            with site_db.get_session() as session:
                result = session.exec(
                    select(UserModel.id, UserModel.last_checked_at).where(
                        UserModel.id == user_id
                    )
                ).first()
                if result:
                    found_in_db = True
                    needs_api_refresh = needs_user_refresh(result[1])

            return found_in_db, needs_api_refresh

        found_in_db, needs_api_refresh = await run_database_call(read_profile_state)

        profile_refreshing = False
        if found_in_db and needs_api_refresh:
            logger.info(
                "User %s on %s is stale; returning the saved snapshot and refreshing in the background",
                user_id,
                site_lower,
            )
            profile_refreshing = _ensure_profile_refresh_started(
                site_lower,
                user_id,
                site_db,
            )
        elif not found_in_db:
            logger.info(f"User {user_id} not found in DB for {site}, trying API...")
            lookup = await _lookup_site_user(site_lower, user_id)
            if lookup.user:
                await _sync_site_resolved_profile_user(site_lower, lookup.user)

        # Resolve cache/statistics availability before opening the profile read
        # transaction. This may await Redis or a background database lookup and
        # must not leave a PgBouncer server connection idle in transaction.
        try:
            statistics_availability = (
                await StatisticsService.get_statistics_availability(
                    site_lower,
                    redis_manager=backend.redis,
                    schedule_if_missing=True,
                )
            )
        except Exception as exc:
            logger.warning(
                "Failed to preload profile statistics for %s/%s: %s",
                site_lower,
                user_id,
                exc,
            )
            statistics_availability = None

        def read_profile_data() -> DetailedUserInfo | None:
            # Query user data to build response (runs after potential API sync)
            with site_db.get_session() as session:
                result = session.exec(
                    select(UserModel, UserInfoModel)
                    .outerjoin(UserInfoModel, UserModel.id == UserInfoModel.user_id)
                    .where(UserModel.id == user_id)
                ).first()

                if not result:
                    return None

                user, info = result

                # Get profile media history (most recent first, up to 20)
                profile_media_records = session.exec(
                    select(UserProfileMediaModel)
                    .where(UserProfileMediaModel.user_id == user.id)
                    .order_by(
                        UserProfileMediaModel.created_at.desc(),
                        UserProfileMediaModel.id.desc(),
                    )
                    .limit(20)
                ).all()

                latest_media = (
                    profile_media_records[0] if profile_media_records else None
                )

                social_records = session.exec(
                    select(SocialModel).where(SocialModel.user_id == user.id)
                ).all()
                subscription_rows = cast(
                    list[SubscriptionWithAuthRow],
                    session.exec(
                        select(SubscriptionModel, UserAuthModel)
                        .join(
                            UserAuthModel,
                            SubscriptionModel.subscriber_id == UserAuthModel.user_id,
                        )
                        .where(
                            SubscriptionModel.user_id == user.id,
                            UserAuthModel.active.is_(True),
                        )
                    ).all(),
                )

                canonical_subscription = _select_profile_subscription(subscription_rows)
                active_subscription = any(
                    sub.active and not _is_subscription_expired(sub.expires_at)
                    for sub, _ in subscription_rows
                )

                socials = [
                    SocialInfo(site=s.site, identifier=s.identifier)
                    for s in social_records
                ]

                profile_media_history = [
                    UserProfileMediaInfo(
                        id=pm.id,
                        avatar_url=pm.avatar_url,
                        header_url=pm.header_url,
                        created_at=(
                            pm.created_at.isoformat() if pm.created_at else None
                        ),
                    )
                    for pm in profile_media_records
                ]

                resolved_size = StatisticsService.resolve_user_size_from_statistics(
                    session=session,
                    user_id=user.id,
                    site=site_lower,
                    stored_size=info.size if info else 0,
                    media_count=info.media_count if info else 0,
                    image_count=info.image_count if info else 0,
                    video_count=info.video_count if info else 0,
                    audio_count=info.audio_count if info else 0,
                    stream_count=info.stream_count if info else 0,
                    statistics_availability=statistics_availability,
                )

                user_data = DetailedUserInfo(
                    id=user.id,
                    user_id=user.id,
                    username=user.username or f"user_{user.id}",
                    name=info.name if info else None,
                    description=info.description if info else None,
                    avatar_url=latest_media.avatar_url if latest_media else None,
                    header_url=latest_media.header_url if latest_media else None,
                    price=info.price if info else 0.0,
                    post_count=info.post_count if info else 0,
                    media_count=info.media_count if info else 0,
                    image_count=info.image_count if info else 0,
                    video_count=info.video_count if info else 0,
                    audio_count=info.audio_count if info else 0,
                    stream_count=info.stream_count if info else 0,
                    archived_post_count=info.archived_post_count if info else 0,
                    subscribers_count=info.subscribers_count if info else 0,
                    favourited_count=info.favourited_count if info else 0,
                    favourites_count=info.favourites_count if info else 0,
                    size=resolved_size.size,
                    display_size=resolved_size.display_size,
                    estimated_size=resolved_size.estimated_size,
                    size_source=resolved_size.size_source,
                    statistics_bucket_date=resolved_size.statistics_bucket_date,
                    location=getattr(info, "location", None) if info else None,
                    website=getattr(info, "website", None) if info else None,
                    sex=getattr(info, "sex", None) if info else None,
                    gender=getattr(info, "gender", None) if info else None,
                    profile_type=getattr(info, "profile_type", None) if info else None,
                    sexual_content=getattr(info, "sexual_content", None) if info else None,
                    partner_sex=getattr(info, "partner_sex", None) if info else None,
                    partner_gender=getattr(info, "partner_gender", None) if info else None,
                    ai_assisted=bool(getattr(info, "ai_assisted", False)) if info else False,
                    verified=bool(getattr(info, "verified", False)) if info else False,
                    promotion=getattr(info, "promotion", False) if info else False,
                    is_active=user.active,
                    is_subscribed=active_subscription,
                    subscription_expires_at=(
                        canonical_subscription[0].expires_at.isoformat()
                        if canonical_subscription
                        and canonical_subscription[0].expires_at is not None
                        else None
                    ),
                    balance=user.balance,
                    spend=user.spend,
                    performer=user.performer,
                    favorite=favourites_service.user_state(session, site_lower, user),
                    site=site_lower,
                    join_date=(user.join_date.isoformat() if user.join_date else None),
                    downloaded_at=(
                        user.downloaded_at.isoformat() if user.downloaded_at else None
                    ),
                    first_downloaded_at=(
                        info.first_downloaded_at.isoformat()
                        if info and info.first_downloaded_at
                        else None
                    ),
                    last_checked_at=(
                        user.last_checked_at.isoformat()
                        if user.last_checked_at
                        else None
                    ),
                    created_at=(
                        user.created_at.isoformat() if user.created_at else None
                    ),
                    profile_media_history=profile_media_history,
                    socials=socials,
                )

                return user_data

        user_data = await run_database_call(read_profile_data)
        if user_data is None:
            return DetailedUserResponse(
                success=False,
                error=f"User {user_id} not found on {site}",
            )

        is_refreshing = profile_refreshing or _profile_refresh_in_progress(
            site_lower, user_id
        )
        return DetailedUserResponse(
            success=True,
            user=user_data,
            is_stale=needs_api_refresh,
            is_refreshing=is_refreshing,
            refresh_error=(
                None
                if is_refreshing
                else _profile_refresh_errors.get((site_lower, user_id))
            ),
        )

    except Exception as e:
        logger.error(f"Failed to get user profile: {e}", exc_info=True)
        return DetailedUserResponse(success=False, error=str(e))


async def _search_and_save_user(
    identifier: str, site: str, db_manager: MultiSiteDatabaseManager
) -> DatabaseUserInfo | None:
    """Search for user via site API (username or numeric ID) and save to database.

    Args:
        identifier: Username or user ID to search for
        site: Site name (onlyfans, fansly)
        db_manager: Database manager instance

    Returns:
        DatabaseUserInfo if found and saved, None otherwise
    """
    try:
        site_lower = site.lower()
        site_display = "OnlyFans" if site_lower == "onlyfans" else "Fansly"

        ident = identifier.strip()
        lookup_value: str | int
        if ident.isdigit():
            lookup_value = int(ident)
        else:
            lookup_value = ident.lstrip("@")
        lookup = await _lookup_site_user(site_lower, lookup_value)
        user = lookup.user

        if not user:
            if lookup.conclusive_missing:
                logger.info(f"User '{identifier}' not found on {site_display}")
            else:
                logger.warning(
                    "Unable to resolve user '%s' on %s through the available routes",
                    identifier,
                    site_display,
                )
            return None

        # Extract user data from API response
        user_id = getattr(user, "id", 0)
        username_val = getattr(user, "username", identifier) or identifier
        name = getattr(user, "name", None) or getattr(user, "displayName", None)
        _bio = (
            getattr(user, "about", None)
            or getattr(user, "bio", None)
            or getattr(user, "rawAbout", None)
        )

        # Get subscription price
        price_val = await get_value(
            user, "subscribePrice", "subscription_price", default=0
        )
        price = float(price_val or 0)

        # Get counts
        post_count = int(
            await get_value(user, "postsCount", "posts_count", default=0) or 0
        )
        media_count = int(
            await get_value(user, "mediasCount", "medias_count", default=0) or 0
        )
        _image_count = int(
            await get_value(user, "photosCount", "photos_count", default=0) or 0
        )
        _video_count = int(
            await get_value(user, "videosCount", "videos_count", default=0) or 0
        )
        sex = await get_value(user, "sex", default=None)
        sex_value: int | None = None
        if sex is not None:
            try:
                sex_value = int(sex)
            except (TypeError, ValueError):
                sex_value = None

        gender = await get_value(user, "gender", default=None)
        gender_value: int | None = None
        if gender is not None:
            try:
                gender_value = int(gender)
            except (TypeError, ValueError):
                gender_value = None

        # Get avatar and header URLs
        avatar_url = await get_value(
            user, "avatar", "avatarUrl", "avatar_url", default=None
        )
        header_url = await get_value(
            user, "header", "headerUrl", "banner", default=None
        )

        # Get subscription status
        is_subscribed = getattr(
            user, "subscribedByData", None
        ) is not None or await get_value(user, "isSubscribed", default=False)

        # Schedule DB sync (wait if we need it immediately)
        observed_aliases = (lookup_value,) if isinstance(lookup_value, str) else ()
        await _sync_site_resolved_profile_user(
            site_lower,
            user,
            observed_aliases=observed_aliases,
        )

        return DatabaseUserInfo(
            id=user_id,
            user_id=user_id,
            username=username_val,
            name=name,
            sex=sex_value,
            gender=gender_value,
            price=price,
            post_count=post_count,
            media_count=media_count,
            avatar_url=avatar_url,
            header_url=header_url,
            is_active=True,
            is_subscribed=bool(is_subscribed),
            site=site_lower,
        )

    except Exception as e:
        logger.error(f"Failed to search and save user: {e}", exc_info=True)
        return None
        return None
