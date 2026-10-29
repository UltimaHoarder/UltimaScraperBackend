"""Helpers to synchronize API user models into the local database."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, cast

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session as SASession
from sqlmodel import Session, select
from ultima_scraper_api import user_types
from ultima_scraper_db_archive import (
    HistoUserInfoModel,
    SocialModel,
    UserAliasModel,
    UserInfoModel,
    UserModel,
    UserProfileMediaModel,
    WebsiteModel,
)

from ultima_scraper_backend.helpers.background_tasks import spawn_background_task

from ..factory import MultiSiteDatabaseManager

logger = logging.getLogger(__name__)


class SessionProvider(Protocol):
    """Protocol for database managers that can provide SQLModel sessions."""

    def get_session(self) -> Session:  # pragma: no cover - structural typing
        """Return a SQLModel session."""
        ...


@dataclass(slots=True)
class _UserSyncProfileCache:
    """Profile rows preloaded for one homogeneous writer transaction."""

    users: dict[int, UserModel] = field(default_factory=dict)
    infos: dict[int, UserInfoModel] = field(default_factory=dict)
    latest_media: dict[int, UserProfileMediaModel] = field(default_factory=dict)
    websites: set[tuple[int, str]] = field(default_factory=set)


def _prefetch_user_sync_profile_cache(
    session: Session,
    user_payloads: Sequence[Mapping[str, Any]],
) -> _UserSyncProfileCache:
    """Load existing profile rows in bounded queries for a writer batch."""

    user_ids = tuple(
        dict.fromkeys(
            int(user_payload["id"])
            for user_payload in user_payloads
            if user_payload.get("id") is not None
        )
    )
    cache = _UserSyncProfileCache()
    if not user_ids:
        return cache

    cache.users = {
        user.id: user
        for user in session.exec(
            select(UserModel).where(UserModel.id.in_(user_ids))
        ).all()
    }
    cache.infos = {
        info.user_id: info
        for info in session.exec(
            select(UserInfoModel).where(UserInfoModel.user_id.in_(user_ids))
        ).all()
    }

    media_user_ids = tuple(
        dict.fromkeys(
            int(user_payload["id"])
            for user_payload in user_payloads
            if user_payload.get("id") is not None
            if _coerce_str(user_payload.get("avatar"))
            or _coerce_str(user_payload.get("header"))
        )
    )
    if media_user_ids:
        ranked_media = (
            select(
                UserProfileMediaModel.id.label("media_id"),
                func.row_number()
                .over(
                    partition_by=UserProfileMediaModel.user_id,
                    order_by=UserProfileMediaModel.created_at.desc(),
                )
                .label("row_number"),
            )
            .where(UserProfileMediaModel.user_id.in_(media_user_ids))
            .subquery()
        )
        cache.latest_media = {
            media.user_id: media
            for media in session.exec(
                select(UserProfileMediaModel)
                .join(
                    ranked_media,
                    ranked_media.c.media_id == UserProfileMediaModel.id,
                )
                .where(ranked_media.c.row_number == 1)
            ).all()
        }

    requested_websites = {
        (int(user_payload["id"]), website)
        for user_payload in user_payloads
        if user_payload.get("id") is not None
        if (
            website := _normalize_website_url(
                _coerce_str(user_payload.get("website"))
            )
        )
    }
    if requested_websites:
        website_user_ids = tuple(
            dict.fromkeys(user_id for user_id, _ in requested_websites)
        )
        website_urls = tuple(dict.fromkeys(url for _, url in requested_websites))
        cache.websites = {
            (int(user_id), url)
            for user_id, url in session.exec(
                select(WebsiteModel.user_id, WebsiteModel.url).where(
                    WebsiteModel.user_id.in_(website_user_ids),
                    WebsiteModel.url.in_(website_urls),
                )
            ).all()
        }
    return cache


def schedule_user_sync(
    user: user_types,
    site: str,
    *,
    db_manager: SessionProvider | None = None,
    sync_socials: bool = False,
    observed_aliases: Iterable[str] = (),
) -> asyncio.Task[bool] | None:
    """Schedule a user sync and return its task when scheduling succeeds."""

    site_name = site.lower()
    try:
        return spawn_background_task(
            _sync_user_snapshot(
                user,
                site=site_name,
                db_manager=db_manager,
                sync_socials=sync_socials,
                observed_aliases=observed_aliases,
            )
        )
    except Exception as exc:
        logger.debug("Failed to schedule user sync: %s", exc, exc_info=True)
        return None


async def _sync_user_snapshot(
    user: user_types,
    *,
    site: str,
    db_manager: SessionProvider | None,
    sync_socials: bool,
    observed_aliases: Iterable[str],
) -> bool:
    """Keep upstream I/O on its loop and owned synchronous SQL on a worker."""
    from ..threads import run_database_call
    from ..write_executor import _QueuedUser, serialize_user_write_payload

    try:
        payload = await serialize_user_write_payload(
            user, sync_socials=sync_socials, observed_aliases=tuple(observed_aliases)
        )

        def persist() -> bool:
            site_db = db_manager or MultiSiteDatabaseManager().get_or_create_manager(
                site
            )
            if not site_db:
                return False
            return asyncio.run(
                sync_user_from_api(
                    site_db,
                    cast(user_types, _QueuedUser(payload["user"])),
                    site=site,
                    sync_socials=payload["sync_socials"],
                    observed_aliases=payload["observed_aliases"],
                )
            )

        return await run_database_call(persist)
    except Exception:
        logger.exception("Failed to sync profile snapshot for %s", site)
        return False


def _alias_candidate(alias: str | None, canonical_username: str | None) -> str | None:
    """Return the alias ``record_user_alias`` would store, or ``None`` if trivial."""

    candidate = (alias or "").strip().lstrip("@")
    canonical = (canonical_username or "").strip().lstrip("@")
    if not candidate or candidate.casefold() == canonical.casefold():
        return None
    return candidate


def record_user_alias(
    session: Session,
    user_id: int,
    alias: str | None,
    *,
    canonical_username: str | None = None,
) -> bool:
    """Persist a non-canonical username for a user when it is not known yet."""

    candidate = _alias_candidate(alias, canonical_username)
    if candidate is None:
        return False

    # Writer sessions disable autoflush. A prior username and the observed
    # handle can therefore name the same alias before either reaches SQL.
    for pending in getattr(session, "new", ()):
        if (
            isinstance(pending, UserAliasModel)
            and pending.user_id == user_id
            and pending.username.casefold() == candidate.casefold()
        ):
            return False

    real_session = isinstance(session, SASession)
    with session.no_autoflush if real_session else contextlib.nullcontext():
        existing_alias = session.exec(
            select(UserAliasModel).where(
                UserAliasModel.user_id == user_id,
                func.lower(UserAliasModel.username) == candidate.casefold(),
            )
        ).first()
        if existing_alias is not None:
            return False

        if real_session and session.get_bind().dialect.name == "postgresql":
            # An immediate upsert handles concurrent refreshes as well as
            # repeated aliases in a deferred writer batch. A new user must
            # exist for the FK, but unrelated profile rows remain deferred.
            for pending in session.new:
                if isinstance(pending, UserModel) and pending.id == user_id:
                    session.flush([pending])
                    break
            inserted = session.execute(
                pg_insert(UserAliasModel.__table__)
                .values(user_id=user_id, username=candidate)
                .on_conflict_do_nothing(index_elements=["user_id", "username"])
                .returning(UserAliasModel.id)
            ).scalar_one_or_none()
            if inserted is None:
                return False
        else:
            session.add(UserAliasModel(user_id=user_id, username=candidate))
    logger.debug("Recorded username alias '%s' for user %s", candidate, user_id)
    return True


class ExternalIdCollisionError(ValueError):
    """Two different site ids hashed to the same archive key."""


def check_external_id(existing: object | None, external_id: str | None) -> None:
    """Refuse to merge a site id into a row that already holds a different one."""
    stored = getattr(existing, "external_id", None)
    if external_id is not None and stored is not None and stored != external_id:
        raise ExternalIdCollisionError(
            f"Archive id {getattr(existing, 'id', None)} already belongs to site id "
            f"{stored!r}, not {external_id!r}"
        )


@dataclass(slots=True, frozen=True)
class _ProfileFields:
    """Profile values read from an API user, shared by the ORM and set paths."""

    user_id: int
    external_id: str | None
    username: str | None
    name: str | None
    bio: str | None
    price: float | None
    post_count: int | None
    media_count: int | None
    image_count: int | None
    video_count: int | None
    avatar_url: str | None
    header_url: str | None
    location: str | None
    website: str | None
    promotion: bool
    balance: float | None
    performer: bool | None
    is_active: bool
    join_date: datetime | None


async def _read_profile_fields(user: object) -> _ProfileFields:
    """Coerce one API user's profile values exactly as ``sync_user_from_api`` stores them."""
    user_id = int(user.id)
    # Sites with string ids (LoyalFans) expose the hashed key as ``id`` and
    # the site's own id as ``uid``.
    external_id = _coerce_str(getattr(user, "uid", None))

    username = _coerce_str(await _get_value(user, "username"))
    name = _coerce_str(await _get_value(user, "name", "displayName"))
    bio = _coerce_str(await _get_value(user, "about", "bio", "rawAbout"))

    price = _coerce_float(
        await _get_value(user, "subscribePrice", "subscription_price")
    )
    post_count = _coerce_int(await _get_value(user, "postsCount", "posts_count"))
    media_count = _coerce_int(await _get_value(user, "mediasCount", "medias_count"))
    image_count = _coerce_int(await _get_value(user, "photosCount", "photos_count"))
    video_count = _coerce_int(await _get_value(user, "videosCount", "videos_count"))

    avatar_url = _coerce_str(
        await _get_value(user, "avatar", "avatarUrl", "avatar_url")
    )
    header_url = _coerce_str(
        await _get_value(user, "header", "headerUrl", "banner")
    )
    location = _coerce_str(await _get_value(user, "location"))
    website = _normalize_website_url(_coerce_str(await _get_value(user, "website")))
    promotion = bool(await _get_value(user, "promotions"))

    balance = _coerce_float(
        await _get_value(user, "credit_balance", "creditBalance", "balance")
    )
    performer = _coerce_bool(await _get_value(user, "is_performer", "performer"))
    if performer is None and hasattr(user, "is_performer"):
        performer_value = getattr(user, "is_performer")
        if callable(performer_value):
            performer_result = performer_value()
            if isinstance(performer_result, bool):
                performer = performer_result

    is_active = _coerce_bool(await _get_value(user, "is_active", "active"))
    if is_active is None:
        is_active = True

    join_date = _coerce_datetime(
        await _get_value(user, "join_date", "joinDate", "joined_at", "joinedAt")
    )
    return _ProfileFields(
        user_id=user_id,
        external_id=external_id,
        username=username,
        name=name,
        bio=bio,
        price=price,
        post_count=post_count,
        media_count=media_count,
        image_count=image_count,
        video_count=video_count,
        avatar_url=avatar_url,
        header_url=header_url,
        location=location,
        website=website,
        promotion=promotion,
        balance=balance,
        performer=performer,
        is_active=is_active,
        join_date=join_date,
    )


async def sync_user_from_api(
    db_manager: SessionProvider,
    user: user_types,
    *,
    site: str | None = None,
    session: Session | None = None,
    sync_socials: bool = False,
    observed_aliases: Iterable[str] = (),
    timing: dict[str, float] | None = None,
    _defer_flush: bool = False,
    _profile_cache: _UserSyncProfileCache | None = None,
) -> bool:
    """Upsert a user and related profile records from an API user model.

    Args:
        db_manager: Database manager capable of providing SQLModel sessions.
        user: User model returned from UltimaScraperAPI.
        site: Optional site name for logging.
        session: Optional existing session to reuse. If provided, this helper
            flushes but does not commit.
        sync_socials: When ``True``, fetches and persists social entries from
            ``user.get_socials()`` when available.
        observed_aliases: Handles that were resolved to this user by the
            upstream site. These are retained when the current API username
            differs, including on the user's first local sync.
    Returns:
        ``True`` when sync completed, ``False`` when an exception occurred.
    """
    active_session: Session | None = None
    owns_session = False
    observed_aliases = tuple(
        alias.replace("\x00", "") if isinstance(alias, str) else alias
        for alias in observed_aliases
    )
    payload_started = time.perf_counter()
    if timing is not None:
        timing.update(
            payload_ms=0.0,
            user_row_ms=0.0,
            alias_ms=0.0,
            authority_ms=0.0,
            profile_ms=0.0,
            social_ms=0.0,
            flush_ms=0.0,
        )
    try:
        fields = await _read_profile_fields(user)
        user_id = fields.user_id
        username, name, bio = fields.username, fields.name, fields.bio
        price, post_count = fields.price, fields.post_count
        media_count, image_count = fields.media_count, fields.image_count
        video_count = fields.video_count
        avatar_url, header_url = fields.avatar_url, fields.header_url
        location, website, promotion = fields.location, fields.website, fields.promotion
        balance, performer = fields.balance, fields.performer
        is_active, join_date = fields.is_active, fields.join_date
        if timing is not None:
            timing["payload_ms"] = (time.perf_counter() - payload_started) * 1000.0

        owns_session = session is None
        active_session = session or db_manager.get_session()

        user_row_started = time.perf_counter()
        alias_ms = 0.0
        existing = (
            _profile_cache.users.get(user_id)
            if _profile_cache is not None
            else active_session.get(UserModel, user_id)
        )
        check_external_id(existing, fields.external_id)
        if existing is None:
            existing = UserModel(
                id=user_id,
                external_id=fields.external_id,
                username=username,
                balance=balance or 0.0,
                performer=performer or False,
                active=is_active,
                join_date=join_date,
            )
            active_session.add(existing)
            if _profile_cache is not None:
                _profile_cache.users[user_id] = existing
        else:
            if username and existing.username != username:
                # Keep previous DB username as alias before switching to API username.
                previous_username = existing.username
                alias_started = time.perf_counter()
                record_user_alias(
                    active_session,
                    user_id,
                    previous_username,
                    canonical_username=username,
                )
                alias_ms += (time.perf_counter() - alias_started) * 1000.0
                existing.username = username
            if balance is not None:
                existing.balance = balance
            if performer is not None:
                existing.performer = performer
            existing.active = is_active
            if join_date is not None and existing.join_date != join_date:
                existing.join_date = join_date
            if fields.external_id is not None:
                existing.external_id = fields.external_id

        for observed_alias in observed_aliases:
            alias_started = time.perf_counter()
            record_user_alias(
                active_session,
                user_id,
                observed_alias,
                canonical_username=existing.username,
            )
            alias_ms += (time.perf_counter() - alias_started) * 1000.0

        if timing is not None:
            timing["alias_ms"] = alias_ms
            timing["user_row_ms"] = max(
                0.0,
                (time.perf_counter() - user_row_started) * 1000.0 - alias_ms,
            )

        profile_started = time.perf_counter()
        existing.last_checked_at = datetime.now(timezone.utc)
        existing.touch()

        info = (
            _profile_cache.infos.get(user_id)
            if _profile_cache is not None
            else active_session.exec(
                select(UserInfoModel).where(UserInfoModel.user_id == user_id)
            ).first()
        )
        had_existing_info = info is not None
        if info is None:
            info = UserInfoModel(user_id=user_id)
            active_session.add(info)
            if _profile_cache is not None:
                _profile_cache.infos[user_id] = info

        should_archive_current_info = had_existing_info and _should_archive_user_info(
            info,
            incoming_name=name,
            incoming_description=bio,
            incoming_website=website,
        )

        if should_archive_current_info:
            _archive_user_info_snapshot(active_session, user_id, info)

        if name is not None:
            info.name = name
        if bio is not None:
            info.description = bio
        if price is not None:
            info.price = price
        if post_count is not None:
            info.post_count = post_count
        if media_count is not None:
            info.media_count = media_count
        if image_count is not None:
            info.image_count = image_count
        if video_count is not None:
            info.video_count = video_count
        if location is not None:
            info.location = location
        if website is not None:
            info.website = website
        if promotion is not None:
            info.promotion = promotion

        if avatar_url or header_url:
            latest_media = (
                _profile_cache.latest_media.get(user_id)
                if _profile_cache is not None
                else active_session.exec(
                    select(UserProfileMediaModel)
                    .where(UserProfileMediaModel.user_id == user_id)
                    .order_by(UserProfileMediaModel.created_at.desc())
                    .limit(1)
                ).first()
            )

            should_insert = (
                latest_media is None
                or latest_media.avatar_url != avatar_url
                or latest_media.header_url != header_url
            )

            if should_insert:
                latest_media = UserProfileMediaModel(
                    user_id=user_id,
                    avatar_url=avatar_url,
                    header_url=header_url,
                )
                active_session.add(latest_media)
                if _profile_cache is not None:
                    _profile_cache.latest_media[user_id] = latest_media

        # --- WebsiteModel: track known website URLs per user ---
        if website:
            if _profile_cache is None:
                _sync_user_website(active_session, user_id, website)
            else:
                website_key = (user_id, website)
                if website_key not in _profile_cache.websites:
                    active_session.add(
                        WebsiteModel(user_id=user_id, url=website)
                    )
                    _profile_cache.websites.add(website_key)

        if timing is not None:
            timing["profile_ms"] = (time.perf_counter() - profile_started) * 1000.0

        # --- SocialModel: track socials from API payload ---
        social_started = time.perf_counter()
        if sync_socials:
            socials_payload = await _fetch_socials(user)
            if socials_payload:
                _sync_user_socials(active_session, user_id, socials_payload)
        if timing is not None:
            timing["social_ms"] = (time.perf_counter() - social_started) * 1000.0

        if owns_session:
            flush_started = time.perf_counter()
            active_session.commit()
            if timing is not None:
                timing["flush_ms"] += (time.perf_counter() - flush_started) * 1000.0
        else:
            # Keep caller transaction boundaries intact.
            if not _defer_flush:
                flush_started = time.perf_counter()
                active_session.flush()
                if timing is not None:
                    timing["flush_ms"] += (time.perf_counter() - flush_started) * 1000.0

        # DEBUG: MPS syncs thousands of profiles per second. At INFO, every
        # writer lane queued on the log handlers' locks for this line (~37%
        # of writer thread time). The writer logs one summary per batch.
        if site:
            logger.debug("Synced user %s for site %s", user_id, site)
        else:
            logger.debug("Synced user %s", user_id)
        if owns_session:
            active_session.close()
        return True
    except Exception as exc:
        if owns_session and active_session is not None:
            with contextlib.suppress(Exception):
                active_session.rollback()
            with contextlib.suppress(Exception):
                active_session.close()
        logger.error("Failed to sync user from API: %s", exc, exc_info=True)
        return False


async def sync_authed_user_from_api(
    db_manager: SessionProvider,
    auth: object,
    *,
    site: str | None = None,
) -> bool:
    """Best-effort sync of the authenticated account user into the local DB."""

    user_obj: object | None = None

    get_authed_user = getattr(auth, "get_authed_user", None)
    if callable(get_authed_user):
        try:
            user_obj = await cast(Any, get_authed_user)()
        except Exception as exc:
            logger.debug("Failed to fetch authed user for sync: %s", exc)

    if user_obj is None:
        auth_id = getattr(auth, "id", None)
        get_user = getattr(auth, "get_user", None)
        if auth_id is not None and callable(get_user):
            for identifier in (auth_id, str(auth_id)):
                try:
                    user_obj = await cast(Any, get_user)(identifier, refresh=True)
                except TypeError:
                    try:
                        user_obj = await cast(Any, get_user)(identifier)
                    except Exception as exc:
                        logger.debug(
                            "Fallback get_user sync lookup failed for %s: %s",
                            identifier,
                            exc,
                        )
                        user_obj = None
                except Exception as exc:
                    logger.debug(
                        "get_user sync lookup failed for %s: %s",
                        identifier,
                        exc,
                    )
                    user_obj = None

                if user_obj is not None:
                    break

    if user_obj is None and hasattr(auth, "user"):
        user_obj = getattr(auth, "user")

    if user_obj is None or getattr(user_obj, "id", None) is None:
        logger.debug("Authenticated session did not expose a syncable user")
        return False

    return await sync_user_from_api(db_manager, cast(user_types, user_obj), site=site)


async def _get_value(user: object, *attrs: str) -> object | None:
    for attr in attrs:
        if not hasattr(user, attr):
            continue
        raw = getattr(user, attr)
        if inspect.iscoroutinefunction(raw):
            value = await raw()
        elif callable(raw):
            value = raw()
        elif inspect.iscoroutine(raw):
            value = await raw
        else:
            value = raw
        if value is not None:
            return value
    return None


def _coerce_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    # Scraped strings can contain NUL padding; PostgreSQL text rejects it.
    return text.replace("\x00", "")


def _coerce_int(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    if isinstance(value, float):
        return int(value)
    return None


def _coerce_float(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _coerce_bool(value: object | None) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _coerce_datetime(value: object | None) -> datetime | None:
    """Best-effort conversion of API join_date values to a timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        # Fansly returns unix timestamps; values > 1e12 are milliseconds.
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except OverflowError, OSError, ValueError:
            return None
    if isinstance(value, str):
        text_value = value.strip()
        if not text_value:
            return None
        # Support numeric strings (unix timestamps).
        try:
            numeric = float(text_value)
        except ValueError:
            numeric = None
        if numeric is not None:
            return _coerce_datetime(numeric)
        # Normalize trailing Z for fromisoformat.
        normalized = (
            text_value[:-1] + "+00:00" if text_value.endswith("Z") else text_value
        )
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _normalize_website_url(value: str | None) -> str | None:
    """Normalize website URL strings for stable dedupe/storage."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


async def _fetch_socials(user: object) -> list[dict[str, Any]]:
    """Fetch social payload from user model when supported."""
    getter = getattr(user, "get_socials", None)
    if not callable(getter):
        return []
    try:
        result = getter()
        if inspect.isawaitable(result):
            result = await result
    except Exception as exc:
        logger.debug("Failed to fetch socials for user: %s", exc)
        return []
    if not isinstance(result, list):
        return []
    socials: list[dict[str, Any]] = []
    for item in cast(list[Any], result):
        if isinstance(item, dict):
            socials.append(cast(dict[str, Any], item))
    return socials


def _extract_social_site(payload: dict[str, Any]) -> str | None:
    """Extract social network/site label from raw payload."""
    for key in ("socialMedia", "site", "type", "provider"):
        candidate = _coerce_str(payload.get(key))
        if candidate and candidate.strip():
            return candidate.strip()
    return None


def _extract_social_identifier(payload: dict[str, Any]) -> str | None:
    """Extract a stable identifier/handle/url for a social entry."""
    for key in ("username", "identifier", "url", "link", "displayName", "name"):
        candidate = _coerce_str(payload.get(key))
        if candidate and candidate.strip():
            return candidate.strip()
    return None


def _sync_user_website(session: Session, user_id: int, website_url: str) -> None:
    """Insert website row when this URL has not yet been recorded for user."""
    exists = session.exec(
        select(WebsiteModel).where(
            WebsiteModel.user_id == user_id,
            WebsiteModel.url == website_url,
        )
    ).first()
    if exists is None:
        session.add(WebsiteModel(user_id=user_id, url=website_url))


def _sync_user_socials(
    session: Session,
    user_id: int,
    socials_payload: list[dict[str, Any]],
) -> None:
    """Upsert social rows for a user from API social payload."""
    existing_socials = session.exec(
        select(SocialModel).where(SocialModel.user_id == user_id)
    ).all()
    existing_keys: set[tuple[str, str]] = set()
    for row in existing_socials:
        if not row.site or not row.identifier:
            continue
        existing_keys.add((row.site.strip().lower(), row.identifier.strip().lower()))

    for payload in socials_payload:
        site_name = _extract_social_site(payload)
        identifier = _extract_social_identifier(payload)
        if not site_name or not identifier:
            continue
        site_key = site_name.strip().lower()
        identifier_key = identifier.strip().lower()
        if not site_key or not identifier_key:
            continue
        key = (site_key, identifier_key)
        if key in existing_keys:
            continue
        session.add(
            SocialModel(
                user_id=user_id,
                site=site_name.strip(),
                identifier=identifier.strip(),
            )
        )
        existing_keys.add(key)


# ── Snapshot helpers ──────────────────────────────────────────────

_HISTO_FIELDS: tuple[str, ...] = (
    "name",
    "price",
    "description",
    "promotion",
    "post_count",
    "media_count",
    "image_count",
    "video_count",
    "audio_count",
    "stream_count",
    "archived_post_count",
    "private_archived_post_count",
    "favourited_count",
    "favourites_count",
    "subscribers_count",
    "size",
    "location",
    "website",
)


def _should_archive_user_info(
    info: UserInfoModel,
    *,
    incoming_name: str | None,
    incoming_description: str | None,
    incoming_website: str | None,
) -> bool:
    """Return ``True`` when tracked identity fields differ from the DB row."""

    return any(
        incoming_value is not None and incoming_value != getattr(info, field_name, None)
        for field_name, incoming_value in (
            ("name", incoming_name),
            ("description", incoming_description),
            ("website", incoming_website),
        )
    )


def _archive_user_info_snapshot(
    session: Session,
    user_id: int,
    info: UserInfoModel,
) -> None:
    """Archive the current ``user_info`` row into ``histo_user_info``.

    This stores the pre-update state so profile history reflects what
    the database held before the latest API sync overwrote it.
    """
    snapshot = HistoUserInfoModel(
        user_id=user_id,
        **{field: getattr(info, field, None) for field in _HISTO_FIELDS},
    )
    session.add(snapshot)
    logger.debug("Archived current profile snapshot for user %s", user_id)
