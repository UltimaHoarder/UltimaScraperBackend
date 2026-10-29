"""Set-based profile writes for plain writer batches (MPS probes).

``sync_user_from_api`` builds ORM objects per profile, and the unit of work
then sends ``users`` / ``user_infos`` statements one shape at a time; psycopg
re-parses each one. Here the same row values become one fixed-text
``unnest`` statement per table and batch.

A profile takes this path only when the ORM path would write nothing beyond
its profile rows: no alias to record and no repeat of its user ID in the
batch. Every other profile keeps ``sync_user_from_api``. Values come from the
same reader and prefetched rows, and a statement leaves out a value exactly
when the ORM would (an unset or ``None`` attribute keeps the column's server
default on INSERT, or its stored value on UPDATE).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from sqlalchemy import String, text
from sqlalchemy.dialects import postgresql
from sqlmodel import Session, select
from ultima_scraper_core import runtime_env_value
from ultima_scraper_db_archive import (
    HistoUserInfoModel,
    SocialModel,
    UserInfoModel,
    UserModel,
    UserProfileMediaModel,
    WebsiteModel,
)

from .user_sync import (
    _HISTO_FIELDS,
    _alias_candidate,
    _extract_social_identifier,
    _extract_social_site,
    _fetch_socials,
    _read_profile_fields,
    _should_archive_user_info,
    _UserSyncProfileCache,
)

_DISABLED = {"0", "false", "off", "no"}
_INFO_FIELDS = (
    "name",
    "description",
    "price",
    "post_count",
    "media_count",
    "image_count",
    "video_count",
    "location",
    "website",
    "promotion",
)
_USER_UPDATE_FIELDS = ("username", "balance", "performer", "join_date", "external_id")


def bulk_profiles_enabled() -> bool:
    """ULTIMA_WRITER_BULK_PROFILES=0/false/off/no keeps every profile on the ORM path."""
    value = runtime_env_value("ULTIMA_WRITER_BULK_PROFILES")
    return value is None or value.strip().lower() not in _DISABLED


@dataclass(slots=True)
class BulkProfileWrite:
    """Rows for the profiles that skip the ORM, grouped by statement."""

    indices: set[int] = field(default_factory=set)
    users_new: list[dict[str, Any]] = field(default_factory=list)
    users_update: list[dict[str, Any]] = field(default_factory=list)
    histo: list[dict[str, Any]] = field(default_factory=list)
    infos_new: list[dict[str, Any]] = field(default_factory=list)
    infos_update: list[dict[str, Any]] = field(default_factory=list)
    media: list[dict[str, Any]] = field(default_factory=list)
    websites: list[dict[str, Any]] = field(default_factory=list)
    socials: list[dict[str, Any]] = field(default_factory=list)


def _dialect() -> Any:
    return postgresql.dialect()


def _array_type(table: Any, column: str, dialect: Any) -> str:
    # A CAST to varchar(n) truncates silently; the column checks length instead.
    type_ = table.c[column].type
    if isinstance(type_, String) and type_.length:
        type_ = String()
    return type_.compile(dialect=dialect)


def _arrays(table: Any, columns: tuple[str, ...], dialect: Any) -> str:
    quote = dialect.identifier_preparer.quote
    casts = ", ".join(
        f"CAST(:{column} AS {_array_type(table, column, dialect)}[])" for column in columns
    )
    return f"unnest({casts}) AS v({', '.join(quote(column) for column in columns)})"


@lru_cache(maxsize=32)
def _insert(table: Any, columns: tuple[str, ...]) -> Any:
    """INSERT … SELECT from arrays; a NULL takes the server default, as the ORM omits it."""
    dialect = _dialect()
    quote = dialect.identifier_preparer.quote
    values = []
    for column in columns:
        default = table.c[column].server_default
        value = f"v.{quote(column)}"
        if default is not None:
            value = f"COALESCE({value}, {default.arg})"
        values.append(value)
    return text(
        f"INSERT INTO {dialect.identifier_preparer.format_table(table)} "
        f"({', '.join(quote(column) for column in columns)}) "
        f"SELECT {', '.join(values)} FROM {_arrays(table, columns, dialect)}"
    )


@lru_cache(maxsize=8)
def _update(table: Any, key: str, keep: tuple[str, ...], always: tuple[str, ...]) -> Any:
    """UPDATE from arrays: a NULL in ``keep`` leaves the stored value.

    Rows whose result equals what is stored are skipped, as the ORM issues no
    UPDATE for an object without net changes.
    """
    dialect = _dialect()
    quote = dialect.identifier_preparer.quote
    columns = (key, *keep, *always)
    targets = {
        **{column: f"COALESCE(v.{quote(column)}, t.{quote(column)})" for column in keep},
        **{column: f"v.{quote(column)}" for column in always},
    }
    assignments = ", ".join(f"{quote(column)} = {value}" for column, value in targets.items())
    stored = ", ".join(f"t.{quote(column)}" for column in targets)
    incoming = ", ".join(targets.values())
    return text(
        f"UPDATE {dialect.identifier_preparer.format_table(table)} AS t SET {assignments} "
        f"FROM {_arrays(table, columns, dialect)} "
        f"WHERE t.{quote(key)} = v.{quote(key)} "
        f"AND ROW({stored}) IS DISTINCT FROM ROW({incoming})"
    )


async def plan_bulk_profiles(
    session: Session,
    payloads: Sequence[Mapping[str, Any]],
    cache: _UserSyncProfileCache,
) -> BulkProfileWrite:
    """Choose the profiles that need no ORM work and build their rows."""
    from ..write_executor import _QueuedUser

    plan = BulkProfileWrite()
    counts = Counter(
        int(payload["user"]["id"])
        for payload in payloads
        if payload["user"].get("id") is not None
    )
    pending_socials: list[tuple[dict[str, Any], int, list[dict[str, Any]]]] = []
    for index, payload in enumerate(payloads):
        user_payload = payload["user"]
        if user_payload.get("id") is None or counts[int(user_payload["id"])] != 1:
            continue
        queued = _QueuedUser(user_payload)
        fields = await _read_profile_fields(queued)
        user_id = fields.user_id
        existing = cache.users.get(user_id)
        username = fields.username
        rename = existing is not None and bool(username) and existing.username != username
        if rename and _alias_candidate(existing.username, username) is not None:
            continue
        canonical = username if existing is None or rename else existing.username
        aliases = tuple(
            alias.replace("\x00", "") if isinstance(alias, str) else alias
            for alias in payload.get("observed_aliases") or ()
        )
        if any(_alias_candidate(alias, canonical) is not None for alias in aliases):
            continue
        # A differing stored site id is a hash collision; the ORM path raises it.
        if (
            existing is not None
            and existing.external_id is not None
            and fields.external_id is not None
            and existing.external_id != fields.external_id
        ):
            continue

        checked_at = datetime.now(timezone.utc)
        updated_at = datetime.now(timezone.utc)
        if existing is None:
            plan.users_new.append({
                "id": user_id,
                "external_id": fields.external_id,
                "username": username,
                "balance": fields.balance or 0.0,
                "performer": fields.performer or False,
                "active": fields.is_active,
                "join_date": fields.join_date,
                "last_checked_at": checked_at,
                "updated_at": updated_at,
            })
        else:
            plan.users_update.append({
                "id": user_id,
                "external_id": fields.external_id,
                "username": username if rename else None,
                "balance": fields.balance,
                "performer": fields.performer,
                "join_date": fields.join_date,
                "active": fields.is_active,
                "last_checked_at": checked_at,
                "updated_at": updated_at,
            })

        info_values = {
            "name": fields.name,
            "description": fields.bio,
            "price": fields.price,
            "post_count": fields.post_count,
            "media_count": fields.media_count,
            "image_count": fields.image_count,
            "video_count": fields.video_count,
            "location": fields.location,
            "website": fields.website,
            "promotion": fields.promotion,
        }
        info = cache.infos.get(user_id)
        if info is None:
            plan.infos_new.append({"user_id": user_id, **info_values})
        else:
            if _should_archive_user_info(
                info,
                incoming_name=fields.name,
                incoming_description=fields.bio,
                incoming_website=fields.website,
            ):
                plan.histo.append({
                    "user_id": user_id,
                    **{name: getattr(info, name, None) for name in _HISTO_FIELDS},
                })
            plan.infos_update.append({"user_id": user_id, **info_values})

        if fields.avatar_url or fields.header_url:
            latest = cache.latest_media.get(user_id)
            if (
                latest is None
                or latest.avatar_url != fields.avatar_url
                or latest.header_url != fields.header_url
            ):
                plan.media.append({
                    "user_id": user_id,
                    "avatar_url": fields.avatar_url,
                    "header_url": fields.header_url,
                })
        if fields.website and (user_id, fields.website) not in cache.websites:
            plan.websites.append({"user_id": user_id, "url": fields.website})

        if payload.get("sync_socials"):
            socials = await _fetch_socials(queued)
            if socials:
                pending_socials.append((user_payload, user_id, socials))
        plan.indices.add(index)

    if pending_socials:
        social_users = tuple(user_id for _, user_id, _ in pending_socials)
        known: dict[int, set[tuple[str, str]]] = {user_id: set() for user_id in social_users}
        for user_id, site, identifier in session.exec(
            select(SocialModel.user_id, SocialModel.site, SocialModel.identifier).where(
                SocialModel.user_id.in_(social_users)
            )
        ).all():
            if site and identifier:
                known[int(user_id)].add((site.strip().lower(), identifier.strip().lower()))
        for _, user_id, socials in pending_socials:
            keys = known[user_id]
            for social in socials:
                site_name = _extract_social_site(social)
                identifier = _extract_social_identifier(social)
                if not site_name or not identifier:
                    continue
                key = (site_name.strip().lower(), identifier.strip().lower())
                if not key[0] or not key[1] or key in keys:
                    continue
                plan.socials.append({
                    "user_id": user_id,
                    "site": site_name.strip(),
                    "identifier": identifier.strip(),
                })
                keys.add(key)
    return plan


def _columns(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    return tuple(rows[0])


def _params(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    return {column: [row[column] for row in rows] for column in rows[0]}


def apply_bulk_profiles(session: Session, plan: BulkProfileWrite) -> None:
    """Write the planned rows in the caller's transaction, parents first."""
    users = UserModel.__table__
    infos = UserInfoModel.__table__
    statements: list[tuple[Any, list[dict[str, Any]]]] = []
    if plan.users_new:
        statements.append((_insert(users, _columns(plan.users_new)), plan.users_new))
    if plan.users_update:
        statements.append((
            _update(users, "id", _USER_UPDATE_FIELDS, ("active", "last_checked_at", "updated_at")),
            plan.users_update,
        ))
    if plan.histo:
        statements.append((_insert(HistoUserInfoModel.__table__, _columns(plan.histo)), plan.histo))
    if plan.infos_update:
        statements.append((
            _update(infos, "user_id", tuple(field for field in _INFO_FIELDS if field != "promotion"), ("promotion",)),
            plan.infos_update,
        ))
    if plan.infos_new:
        statements.append((_insert(infos, _columns(plan.infos_new)), plan.infos_new))
    for table, rows in (
        (UserProfileMediaModel.__table__, plan.media),
        (WebsiteModel.__table__, plan.websites),
        (SocialModel.__table__, plan.socials),
    ):
        if rows:
            statements.append((_insert(table, _columns(rows)), rows))
    for statement, rows in statements:
        session.execute(statement, _params(rows))
