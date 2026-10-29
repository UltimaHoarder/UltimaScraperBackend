"""Canonical PostgreSQL mutation executor for the durable write stream."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import urlparse

from sqlmodel import Session, select

from .write_queue import DatabaseWriteCommand, DatabaseWriteOperation, WRITE_TIMING_KEY

logger = logging.getLogger(__name__)


_INVALID_USER_COLUMNS = ("site", "user_id", "reason", "last_checked_at", "updated_at")


def _invalid_user_upsert(table: Any, rows: list[dict[str, Any]]) -> Any:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    statement = pg_insert(table).values(rows)
    return statement.on_conflict_do_update(
        index_elements=["site", "user_id"],
        set_={
            "reason": statement.excluded.reason,
            "last_checked_at": statement.excluded.last_checked_at,
            "updated_at": statement.excluded.last_checked_at,
        },
    )


@lru_cache(maxsize=128)
def _compiled_invalid_user_upsert(table: Any, dialect: Any, count: int) -> Any:
    """Cache bounded statement structure; each execution binds fresh values."""
    from sqlalchemy import bindparam

    return _invalid_user_upsert(table, [
        {
            column: bindparam(f"invalid_{index}_{column}", type_=table.c[column].type)
            for column in _INVALID_USER_COLUMNS
        }
        for index in range(count)
    ]).compile(dialect=dialect)


def _json_value(value: Any) -> Any:
    """Convert supported API values to deterministic JSON primitives."""

    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        # PostgreSQL text and JSONB values cannot contain U+0000.
        return value.replace("\x00", "")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (Path, Enum)):
        return str(value.value if isinstance(value, Enum) else value)
    if isinstance(value, Mapping):
        return {
            str(key).replace("\x00", ""): _json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    raw = getattr(value, "__raw__", None)
    if isinstance(raw, Mapping):
        return _json_value(raw)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _json_value(model_dump(mode="json"))
    raise TypeError(f"Unsupported database write payload value: {type(value).__name__}")


async def serialize_user_write_payload(
    user: object,
    *,
    sync_socials: bool = False,
    observed_aliases: Sequence[str] = (),
    socials: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Snapshot the API fields needed by ``sync_user_from_api``."""

    from .helpers.user_sync import _fetch_socials, _get_value

    user_id = getattr(user, "id", None)
    if user_id is None:
        raise ValueError("Cannot queue a user without an id")

    async def value(*names: str) -> Any:
        return _json_value(await _get_value(user, *names))

    if socials is not None:
        resolved_socials = _json_value(socials)
    elif sync_socials:
        resolved_socials = _json_value(await _fetch_socials(user))
    else:
        resolved_socials = []

    user_payload = {
        "id": int(user_id),
        "uid": _json_value(getattr(user, "uid", None)),
        "username": _json_value(getattr(user, "username", None)),
        "name": await value("name", "displayName"),
        "about": await value("about", "bio", "rawAbout"),
        "subscribePrice": await value("subscribePrice", "subscription_price"),
        "postsCount": await value("postsCount", "posts_count"),
        "mediasCount": await value("mediasCount", "medias_count"),
        "photosCount": await value("photosCount", "photos_count"),
        "videosCount": await value("videosCount", "videos_count"),
        "avatar": await value("avatar", "avatarUrl", "avatar_url"),
        "header": await value("header", "headerUrl", "banner"),
        "location": await value("location"),
        "website": await value("website"),
        "promotions": await value("promotions"),
        "credit_balance": await value("credit_balance", "creditBalance", "balance"),
        "is_performer": await value("is_performer", "performer"),
        "active": await value("is_active", "active"),
        "join_date": await value("join_date", "joinDate", "joined_at", "joinedAt"),
        "socials": resolved_socials,
    }
    return {
        "user": user_payload,
        "sync_socials": bool(sync_socials or (socials is not None and bool(socials))),
        "observed_aliases": [
            _json_value(alias.strip())
            for alias in observed_aliases
            if isinstance(alias, str) and alias.replace("\x00", "").strip()
        ],
    }


def _read_field(value: object, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value and value[name] is not None:
                return value[name]
        return None
    for name in names:
        candidate = getattr(value, name, None)
        if candidate is not None:
            return candidate
    raw = getattr(value, "__raw__", None)
    if isinstance(raw, Mapping):
        for name in names:
            if name in raw and raw[name] is not None:
                return raw[name]
    return None


def _content_kind(content_type: object, item: object) -> str:
    value = getattr(content_type, "value", content_type)
    normalized = str(value or "").strip().lower()
    if normalized in {"posts", "archived_posts", "post"}:
        return "post"
    if normalized in {"messages", "message"}:
        return "message"
    if normalized in {"stories", "highlights", "story"}:
        return "story"
    if normalized in {"mass_messages", "mass_message"}:
        return "mass_message"

    class_name = type(item).__name__.lower()
    if "mass" in class_name and "message" in class_name:
        return "mass_message"
    for kind in ("post", "message", "story"):
        if kind in class_name:
            return kind
    raise ValueError(f"Unsupported queued content type: {value or type(item).__name__}")


def _sequence(value: object) -> list[Any]:
    if isinstance(value, (str, bytes, bytearray)) or value is None:
        return []
    if isinstance(value, Sequence):
        return list(cast(Sequence[Any], value))
    return []


def _serialize_user_ref(value: object) -> dict[str, Any] | None:
    from ..manager import BackendManager

    user_id = BackendManager._extract_nested_user_id(value)
    if user_id is None:
        return None
    return {
        "id": user_id,
        "username": _read_field(value, "username", "name"),
        "name": _read_field(value, "name"),
    }


def _serialize_media(item: object, media: object) -> dict[str, Any]:
    from ..content_persistence import (
        _resolve_usapi_media_duration,
        _resolve_usapi_media_size,
    )
    from ..manager import BackendManager

    media_id = BackendManager._normalise_identifier(_read_field(media, "id"))
    if media_id is None:
        raise ValueError("Cannot queue media without an id")

    picker = getattr(item, "url_picker", None)
    if not callable(picker):
        raise ValueError(f"Content {getattr(item, 'id', 'unknown')} has no URL picker")
    parsed_url = picker(media)
    resolved_url = parsed_url.geturl() if parsed_url is not None else None
    has_drm, drm_payload = BackendManager._extract_drm_manifest_payload(media)
    created_at = BackendManager._resolve_media_created_at(media, item)
    return {
        "id": media_id,
        "url": resolved_url,
        "type": _read_field(media, "type", "category"),
        "filename": _read_field(media, "filename", "file_name", "name"),
        "size": _resolve_usapi_media_size(media),
        "duration": _resolve_usapi_media_duration(media),
        "created_at": created_at.isoformat() if created_at else None,
        "drm": has_drm,
        "drm_payload": _json_value(drm_payload),
    }


def serialize_content_write_item(
    item: object,
    *,
    content_type: object,
    user_id: int,
    auth_user_id: int | None,
    record_bought_content: bool,
    _depth: int = 0,
) -> dict[str, Any]:
    """Normalize a live USAPI content object for durable JSON storage."""

    from ..manager import BackendManager

    kind = _content_kind(content_type, item)
    content_id = BackendManager._normalise_identifier(_read_field(item, "id"))
    if content_id is None:
        raise ValueError("Cannot queue content without an id")
    author_id = BackendManager._extract_content_author_id(
        item, fallback_user_id=user_id
    )
    receiver_id = (
        BackendManager._extract_message_receiver_id(item, user_id, auth_user_id)
        if kind == "message"
        else None
    )
    previews = _sequence(_read_field(item, "preview", "preview_ids", "previews"))
    linked_users = [
        ref
        for value in _sequence(_read_field(item, "linkedUsers", "linked_users"))
        if (ref := _serialize_user_ref(value)) is not None
    ]
    linked_posts: list[dict[str, Any]] = []
    if _depth == 0:
        for linked in _sequence(_read_field(item, "linkedPosts", "linked_posts")):
            linked_posts.append(
                serialize_content_write_item(
                    linked,
                    content_type="post",
                    user_id=(
                        BackendManager._extract_content_author_id(
                            linked, fallback_user_id=user_id
                        )
                        or user_id
                    ),
                    auth_user_id=auth_user_id,
                    record_bought_content=False,
                    _depth=1,
                )
            )

    return {
        "kind": kind,
        "id": content_id,
        "user_id": int(user_id),
        "author_id": author_id,
        "receiver_id": receiver_id,
        "queue_id": BackendManager._extract_message_queue_id(item),
        "response_type": str(
            _read_field(item, "responseType", "response_type") or kind
        ),
        "text": _read_field(item, "text", "rawText", "raw_text"),
        "price": BackendManager._normalize_price(cast(Any, item)),
        "paid": BackendManager._extract_paid_status(
            cast(Any, item), record_bought_content=record_bought_content
        ),
        "media_count": int(_read_field(item, "media_count", "mediaCount") or 0),
        "created_at": _json_value(
            _read_field(item, "created_at", "createdAt", "postedAt")
        ),
        "expires_at": _json_value(
            _read_field(item, "expires_at", "expiredAt", "expiresAt")
        ),
        "previews": [
            preview_id
            for raw_preview in previews
            if (preview_id := BackendManager._extract_nested_user_id(raw_preview))
            is not None
        ],
        "can_view_media": _read_field(item, "canViewMedia", "can_view_media"),
        "is_opened": _read_field(item, "isOpened", "is_opened"),
        "linked_users": linked_users,
        "linked_posts": linked_posts,
        "media": [
            _serialize_media(item, media)
            for media in _sequence(_read_field(item, "media"))
        ],
    }


_USER_SYNC_PHASES = (
    "payload_ms",
    "user_row_ms",
    "alias_ms",
    "authority_ms",
    "profile_ms",
    "social_ms",
    "flush_ms",
)


def _postgres_session(session: Any) -> bool:
    """True for a real SQLAlchemy session bound to PostgreSQL (not a test double)."""
    get_bind = getattr(session, "get_bind", None)
    if not callable(get_bind):
        return False
    try:
        return get_bind().dialect.name == "postgresql"
    except Exception:
        return False


class _QueuedUser:
    def __init__(self, payload: Mapping[str, Any]) -> None:
        for key, value in payload.items():
            setattr(self, key, value)
        self.id = int(payload["id"])
        self.username = payload.get("username")
        self._socials = list(payload.get("socials") or [])

    async def get_socials(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self._socials)


class _QueuedUserRef(SimpleNamespace):
    id: int


class _QueuedMedia:
    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.id = int(payload["id"])
        self.resolved_url = payload.get("url")
        self.type = payload.get("type")
        self.filename = payload.get("filename")
        self.size = payload.get("size")
        self.duration = payload.get("duration")
        self.created_at = payload.get("created_at")
        self.drm = bool(payload.get("drm"))
        self.drm_payload = dict(payload.get("drm_payload") or {})


class _QueuedContent:
    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.id = int(payload["id"])
        self.text = payload.get("text")
        self.rawText = payload.get("text")
        self.price = payload.get("price")
        self.paid = payload.get("paid")
        self.created_at = payload.get("created_at")
        self.expires_at = payload.get("expires_at")
        self.expiredAt = payload.get("expires_at")
        self.responseType = payload.get("response_type")
        self.queueId = payload.get("queue_id")
        self.queue_id = payload.get("queue_id")
        self.receiver_id = payload.get("receiver_id")
        self.media_count = payload.get("media_count")
        self.canViewMedia = payload.get("can_view_media")
        self.isOpened = payload.get("is_opened")
        self.previews = list(payload.get("previews") or [])
        self.preview = self.previews
        author_id = payload.get("author_id") or payload.get("user_id")
        self.author = _QueuedUserRef(id=int(author_id))
        self.fromUser = {"id": int(author_id)}
        self.linkedUsers = list(payload.get("linked_users") or [])
        self.linkedPosts = [
            _QueuedPost(linked) for linked in payload.get("linked_posts") or []
        ]
        self.media = [_QueuedMedia(media) for media in payload.get("media") or []]

    def get_author(self) -> _QueuedUserRef:
        return self.author

    def url_picker(self, media: _QueuedMedia) -> Any:
        return urlparse(str(media.resolved_url)) if media.resolved_url else None

    def is_bought(self) -> bool:
        return bool(self.paid)


class _QueuedPost(_QueuedContent):
    pass


class _QueuedMessage(_QueuedContent):
    pass


class _QueuedStory(_QueuedContent):
    pass


class _QueuedMassMessage(_QueuedContent):
    pass


def _queued_content(payload: Mapping[str, Any]) -> _QueuedContent:
    kind = str(payload["kind"])
    content_class = {
        "post": _QueuedPost,
        "message": _QueuedMessage,
        "story": _QueuedStory,
        "mass_message": _QueuedMassMessage,
    }.get(kind)
    if content_class is None:
        raise ValueError(f"Unsupported queued content kind: {kind}")
    return content_class(payload)


class PostgreSQLWriteExecutor:
    """Own queued worker-data and job-lifecycle PostgreSQL mutations."""

    def __init__(self, redis_manager: object) -> None:
        from ..manager import BackendManager

        self.redis = redis_manager
        self.backend = BackendManager(enable_redis=False)
        self._site_manager_lock = threading.Lock()

    def _site_manager(self, site: str | None) -> Any:
        if not site:
            raise ValueError("Site-specific database write is missing site")
        with self._site_manager_lock:
            manager = self.backend.get_site_db_manager(site)
            if manager is None:
                raise RuntimeError(
                    f"No database manager is configured for site '{site}'"
                )
            manager.ensure_connected()
        return manager

    async def execute_batch(
        self, commands: Sequence[DatabaseWriteCommand]
    ) -> Mapping[str, dict[str, Any]]:
        if not commands:
            return {}
        operation = commands[0].operation
        site = commands[0].site
        if any(
            command.operation != operation or command.site != site
            for command in commands
        ):
            raise ValueError("Writer batches must have one operation and one site")

        if operation == DatabaseWriteOperation.UPDATE_MEDIA_SELECTION:
            return await asyncio.to_thread(self._media_selections, commands, site)
        if operation == DatabaseWriteOperation.UPSERT_CONTENT_BATCH:
            return await asyncio.to_thread(
                self._run_async_batch,
                self._upsert_content_batch,
                commands,
                site,
            )
        if operation == DatabaseWriteOperation.UPSERT_USER:
            return await asyncio.to_thread(
                self._run_async_batch,
                self._upsert_users,
                commands,
                site,
            )
        if operation == DatabaseWriteOperation.UPSERT_SUBSCRIPTIONS:
            return await asyncio.to_thread(
                self._run_async_batch,
                self._upsert_subscriptions,
                commands,
                site,
            )
        if operation == DatabaseWriteOperation.UPSERT_INVALID_USER:
            return await asyncio.to_thread(self._upsert_invalid_users, commands, site)
        if operation == DatabaseWriteOperation.FINALIZE_DOWNLOAD:
            return await asyncio.to_thread(self._finalize_downloads, commands, site)
        if operation == DatabaseWriteOperation.UPDATE_DRM_STREAMS:
            return await asyncio.to_thread(self._update_drm_streams, commands, site)
        if operation == DatabaseWriteOperation.SYNC_JOB_STATUS:
            return await asyncio.to_thread(self._sync_job_statuses, commands)
        if operation == DatabaseWriteOperation.SYNC_TASK_STATUS:
            return await asyncio.to_thread(self._sync_task_statuses, commands)
        if operation == DatabaseWriteOperation.PREPARE_MPS_JOB:
            return await asyncio.to_thread(self._prepare_mps_jobs, commands, site)
        if operation == DatabaseWriteOperation.RETRY_MPS_JOB:
            return await asyncio.to_thread(self._retry_mps_jobs, commands, site)
        if operation == DatabaseWriteOperation.ADVANCE_MPS_RETRY:
            return await asyncio.to_thread(self._advance_mps_retries, commands)
        if operation == DatabaseWriteOperation.UPDATE_MPS_CONFIG:
            return await asyncio.to_thread(self._update_mps_configs, commands)
        if operation in {
            DatabaseWriteOperation.BEGIN_MPS_REPLACEMENT,
            DatabaseWriteOperation.APPEND_MPS_REPLACEMENT,
            DatabaseWriteOperation.COMMIT_MPS_REPLACEMENT,
        }:
            return await asyncio.to_thread(self._replace_mps_definitions, commands)
        if operation == DatabaseWriteOperation.ARCHIVE_MPS_WINDOW:
            return await asyncio.to_thread(self._archive_mps_windows, commands)
        if operation == DatabaseWriteOperation.TERMINATE_MPS_JOB:
            return await asyncio.to_thread(self._terminate_mps_jobs, commands)
        if operation == DatabaseWriteOperation.ARCHIVE_JOB_TASKS:
            return await self._archive_job_tasks(commands)
        if operation == DatabaseWriteOperation.RECONCILE_CENTRAL_JOBS:
            return await asyncio.to_thread(self._reconcile_central_jobs, commands)
        raise ValueError(f"Unsupported database write operation: {operation.value}")

    def _media_selections(self, commands, site):
        from .media_selection import MediaSelectionStore
        from .write_receipts import receipt_batch

        manager = self._site_manager(site)
        store = MediaSelectionStore()
        results = {}
        with manager.get_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    spec = command.payload["spec"]
                    if spec["site"] != site:
                        raise ValueError("Media selection site does not match writer command")
                    results[command.command_id] = store.mutate(session, spec, command.payload)
            session.commit()
        return results

    def _prepare_mps_jobs(
        self, commands: Sequence[DatabaseWriteCommand], site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        from .mps_manifest import MPSManifestStore, NoMPSCandidates
        from .write_receipts import receipt_batch

        manager = self._site_manager(site)
        store = MPSManifestStore()
        results: dict[str, dict[str, Any]] = {}
        with manager.get_session() as session:
            session.connection().exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            )
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    try:
                        results[command.command_id] = store.create(
                            job_id=command.payload["job_id"], site=cast(str, site),
                            job_config=command.payload["job_config"],
                            selection=command.payload["selection"],
                            proxy_pool=command.payload["proxy_pool"],
                            site_session_factory=manager.get_session,
                            db_session=session,
                            candidate_count_hint=command.payload.get(
                                "candidate_count_hint"
                            ),
                        )
                    except NoMPSCandidates:
                        results[command.command_id] = {"empty": True}
            session.commit()
        return results

    def _retry_mps_jobs(self, commands, site):
        from .mps_manifest import MPSManifestStore
        from .write_receipts import receipt_batch

        manager = self._site_manager(site)
        store = MPSManifestStore()
        results = {}
        with manager.get_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    auth_id = command.payload.get("auth_id")
                    if auth_id is not None:
                        from ultima_scraper_db_archive import UserAuthModel

                        if (
                            isinstance(auth_id, bool)
                            or not isinstance(auth_id, int)
                            or auth_id <= 0
                        ):
                            raise ValueError("Retry auth_id must be a positive integer")
                        active = session.execute(
                            select(UserAuthModel.user_id)
                            .where(
                                UserAuthModel.user_id == auth_id,
                                UserAuthModel.active.is_(True),
                            )
                            .limit(1)
                        ).scalar_one_or_none()
                        if active is None:
                            raise ValueError(f"Auth {auth_id} is not active for {site}")
                    results[command.command_id] = store.clone_for_retry(
                        source_job_id=command.payload["source_job_id"],
                        job_id=command.payload["job_id"],
                        job_config=command.payload["job_config"],
                        site=site,
                        live_failed_user_ids=command.payload.get("live_failed_user_ids", []),
                        live_failed_tasks=command.payload.get("live_failed_tasks", []),
                        **({"expected_manifest_id": command.payload["expected_manifest_id"]}
                           if "expected_manifest_id" in command.payload else {}),
                        auth_id=command.payload.get("auth_id"),
                        db_session=session,
                        incremental=True,
                    )
                    if results[command.command_id]["status"] == "preparing":
                        results[command.command_id] = store.advance_retry(
                            command.payload["job_id"], after=-1, db_session=session,
                        )
            session.commit()
        return results

    @staticmethod
    def _advance_mps_retries(commands):
        from ultima_scraper_db_archive.global_db import global_session
        from .mps_manifest import MPSManifestStore
        from .write_receipts import receipt_batch

        store = MPSManifestStore()
        results = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    results[command.command_id] = store.advance_retry(
                        command.payload["job_id"], after=command.payload["after"], db_session=session,
                    )
            session.commit()
        return results

    @staticmethod
    def _update_mps_configs(commands):
        from ultima_scraper_db_archive.global_db import global_session
        from .mps_manifest import MPSManifestStore
        from .write_receipts import receipt_batch

        store = MPSManifestStore()
        results = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    results[command.command_id] = store.update_config(
                        command.payload["job_id"],
                        name=command.payload.get("name"),
                        schedule=command.payload.get("schedule"),
                        start_schedule_revision=command.payload.get(
                            "start_schedule_revision"
                        ),
                        db_session=session,
                    )
            session.commit()
        return results

    @staticmethod
    def _replace_mps_definitions(commands):
        from ultima_scraper_db_archive.global_db import global_session
        from .mps_manifest import MPSManifestStore
        from .write_receipts import receipt_batch

        definitions = MPSManifestStore().definitions
        results = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    operation = command.operation
                    payload = command.payload
                    manifest_id = payload["manifest_id"]
                    from ..job_system.ledger import TERMINAL, ExecutionLedger, revise, _event
                    ledger_run = ExecutionLedger()._locked_run(session, payload["job_id"])
                    if ledger_run is not None and operation in {
                        DatabaseWriteOperation.BEGIN_MPS_REPLACEMENT,
                        DatabaseWriteOperation.COMMIT_MPS_REPLACEMENT,
                    } and ledger_run.started_at is not None:
                        raise ValueError("A started ledger run keeps its inputs; create a retry run to change tasks")
                    if (
                        ledger_run is not None
                        and ledger_run.state in TERMINAL
                        and operation == DatabaseWriteOperation.COMMIT_MPS_REPLACEMENT
                    ):
                        # Reopening a never-started finished run writes events
                        # and mutations into leaves history retirement drops.
                        from .job_partitions import restore_retired_history

                        restore_retired_history(session, payload["job_id"])
                    if operation == DatabaseWriteOperation.BEGIN_MPS_REPLACEMENT:
                        result = definitions.begin(
                            payload["job_id"], manifest_id,
                            name=payload.get("name"), schedule=payload.get("schedule"),
                            db_session=session,
                        )
                    elif operation == DatabaseWriteOperation.APPEND_MPS_REPLACEMENT:
                        result = definitions.append(
                            manifest_id, offset=payload["offset"], tasks=payload["tasks"],
                            db_session=session,
                        )
                    else:
                        result = definitions.commit(
                            payload["job_id"], manifest_id, total=payload["total"],
                            db_session=session,
                        )
                        if ledger_run is not None:
                            from sqlalchemy import delete
                            from ultima_scraper_db_archive.execution_models import ExecutionWorkModel
                            session.execute(delete(ExecutionWorkModel).where(ExecutionWorkModel.run_id == payload["job_id"]))
                            ledger_run.state = "pending"
                            ledger_run.completed_at = None
                            result["job_config"] = revise(session, ledger_run, result)
                            _event(session, payload["job_id"], "job_updated", status="pending")
                    results[command.command_id] = result
            session.commit()
        return results

    @staticmethod
    def _archive_mps_windows(
        commands: Sequence[DatabaseWriteCommand],
    ) -> Mapping[str, dict[str, Any]]:
        from ultima_scraper_db_archive.global_db import global_session
        from .mps_manifest import MPSManifestStore
        from .write_receipts import receipt_batch
        from ..job_system.models import Task
        from ..job_system.storage import _task_to_central_row

        store = MPSManifestStore()
        results: dict[str, dict[str, Any]] = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    job_id = command.payload["job_id"]
                    rows = [_task_to_central_row(Task.from_dict(value), job_id)
                            for value in command.payload["tasks"]]
                    results[command.command_id] = store.archive_window(
                        job_id, start=command.payload["start"],
                        end=command.payload["end"], rows=rows,
                        manifest_id=command.payload.get("manifest_id"), db_session=session,
                        runtime=command.payload.get("runtime"),
                        candidate_ordinals=command.payload.get("candidate_ordinals"),
                    )
            session.commit()
        return results

    @staticmethod
    def _terminate_mps_jobs(
        commands: Sequence[DatabaseWriteCommand],
    ) -> Mapping[str, dict[str, Any]]:
        from ultima_scraper_db_archive.global_db import global_session
        from .mps_manifest import MPSManifestStore
        from .write_receipts import receipt_batch

        store = MPSManifestStore()
        from ..job_system.mps_attempt import sql_definitions, sql_is_current

        results: dict[str, dict[str, Any]] = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                definitions = sql_definitions(session, (command.payload for command in pending))
                for command in pending:
                    if not sql_is_current(command.payload, definitions):
                        results[command.command_id] = {"committed": False, "stale": True}
                        continue
                    store.terminate(command.payload["job_id"],
                        status=command.payload["status"], db_session=session)
                    results[command.command_id] = {"committed": True}
            session.commit()
        return results

    @staticmethod
    def _run_async_batch(
        operation: Any,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        """Run an async-shaped synchronous SQL batch in a writer thread."""

        return asyncio.run(operation(commands, site))

    async def _upsert_content_batch(
        self,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        manager = self._site_manager(site)
        engine = manager.engine
        if engine is None:
            raise RuntimeError(f"Database engine for site '{site}' is unavailable")
        results: dict[str, dict[str, Any]] = {}
        with Session(engine) as session:
            try:
                from .write_receipts import receipt_batch

                with receipt_batch(session, commands, results) as pending:
                    for command in pending:
                        saved = 0
                        selected_items = []
                        errors: list[str] = []
                        for item_payload in command.payload.get("items") or []:
                            content = _queued_content(cast(Mapping[str, Any], item_payload))
                            try:
                                with session.begin_nested():
                                    result = await self.backend.save_content(
                                        cast(Any, content),
                                        int(command.payload["user_id"]),
                                        auth_user_id=(
                                            int(command.payload["auth_user_id"])
                                            if command.payload.get("auth_user_id")
                                            is not None
                                            else None
                                        ),
                                        record_bought_content=bool(
                                            command.payload.get("record_bought_content")
                                        ),
                                        db_session=session,
                                        site=cast(str, site),
                                    )
                                    if not result.success:
                                        raise RuntimeError(result.message)
                                saved += 1
                                if command.payload.get("media_selection") is not None:
                                    selected_items.append(item_payload)
                            except Exception as exc:
                                errors.append(
                                    f"Failed to save {content.id}: {type(exc).__name__}: {exc}"
                                )
                        selection = command.payload.get("media_selection")
                        if selection is not None:
                            from .media_selection import MediaSelectionStore
                            if selection["site"] != site or selection["auth_id"] != command.payload.get("auth_user_id"):
                                raise ValueError("Content selection has a different site or auth scope")
                            MediaSelectionStore.add_content(session, selection, selected_items,
                                command.payload["selection_content_type"])
                            MediaSelectionStore.put_evidence(session, selection["selection_id"],
                                "errors", int(command.payload.get("error_offset", 0)), errors)
                        results[command.command_id] = {
                            "items_saved": saved,
                            "errors": errors,
                        }
                session.commit()
            except Exception:
                session.rollback()
                raise
        return results

    async def _upsert_users(
        self,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        from .helpers.profile_bulk import (
            apply_bulk_profiles,
            bulk_profiles_enabled,
            plan_bulk_profiles,
        )
        from .helpers.user_sync import (
            _prefetch_user_sync_profile_cache,
            sync_user_from_api,
        )

        manager = self._site_manager(site)
        results: dict[str, dict[str, Any]] = {}
        timings: dict[str, dict[str, Any]] = {}
        with manager.get_session() as session:
            try:
                from .write_receipts import receipt_batch

                connection_started = time.perf_counter()
                session.connection()
                connection_checkout_ms = (
                    time.perf_counter() - connection_started
                ) * 1000.0
                receipt_phases: dict[str, float] = {}
                receipt_context = receipt_batch(session, commands, results, timing=receipt_phases)
                receipt_started = time.perf_counter()
                with receipt_context as pending:
                    receipt_lookup_ms = (
                        time.perf_counter() - receipt_started
                    ) * 1000.0
                    profile_prefetch_started = time.perf_counter()
                    profile_cache = _prefetch_user_sync_profile_cache(
                        session,
                        [
                            cast(Mapping[str, Any], command.payload["user"])
                            for command in pending
                        ],
                    )
                    profile_prefetch_share_ms = (
                        (time.perf_counter() - profile_prefetch_started) * 1000.0
                        / max(1, len(pending))
                    )
                    bulk_started = time.perf_counter()
                    bulk = None
                    if _postgres_session(session) and bulk_profiles_enabled():
                        bulk = await plan_bulk_profiles(
                            session, [command.payload for command in pending], profile_cache
                        )
                    bulk_plan_ms = (time.perf_counter() - bulk_started) * 1000.0
                    for index, command in enumerate(pending):
                        if bulk is not None and index in bulk.indices:
                            continue
                        operation_started = time.perf_counter()
                        user_timing: dict[str, float] = {}
                        payload = command.payload
                        synced = await sync_user_from_api(
                            manager,
                            cast(
                                Any, _QueuedUser(cast(Mapping[str, Any], payload["user"]))
                            ),
                            site=site,
                            session=session,
                            sync_socials=bool(payload.get("sync_socials")),
                            observed_aliases=tuple(payload.get("observed_aliases") or ()),
                            timing=user_timing,
                            _defer_flush=True,
                            _profile_cache=profile_cache,
                        )
                        if not synced:
                            raise RuntimeError(
                                f"User sync failed for {payload['user'].get('id')}"
                            )
                        user_timing["profile_ms"] = (
                            user_timing.get("profile_ms", 0.0)
                            + profile_prefetch_share_ms
                        )
                        timing = {
                            "execution_ms": (
                                (time.perf_counter() - operation_started) * 1000.0
                                + profile_prefetch_share_ms
                            ),
                            "execution_measured": True,
                            "commit_ms": 0.0,
                            "commit_measured": False,
                            **user_timing,
                        }
                        timings[command.command_id] = timing
                        results[command.command_id] = {
                            "synced": True,
                            WRITE_TIMING_KEY: timing,
                        }
                    if bulk is not None and bulk.indices:
                        # One set-based statement per table for every profile
                        # that needs no ORM work; their cost is shared evenly.
                        bulk_write_started = time.perf_counter()
                        apply_bulk_profiles(session, bulk)
                        bulk_ms = bulk_plan_ms + (
                            time.perf_counter() - bulk_write_started
                        ) * 1000.0
                        bulk_share_ms = bulk_ms / len(bulk.indices)
                        for index in sorted(bulk.indices):
                            command = pending[index]
                            timing = {
                                "execution_ms": bulk_share_ms + profile_prefetch_share_ms,
                                "execution_measured": True,
                                "commit_ms": 0.0,
                                "commit_measured": False,
                                **dict.fromkeys(_USER_SYNC_PHASES, 0.0),
                                "profile_ms": profile_prefetch_share_ms,
                                "flush_ms": bulk_share_ms,
                            }
                            timings[command.command_id] = timing
                            results[command.command_id] = {
                                "synced": True,
                                WRITE_TIMING_KEY: timing,
                            }
                    flush_started = time.perf_counter()
                    session.flush()
                    flush_ms = (time.perf_counter() - flush_started) * 1000.0
                    flush_share_ms = flush_ms / max(1, len(timings))
                    for timing in timings.values():
                        timing["flush_ms"] += flush_share_ms
                        timing["execution_ms"] += flush_share_ms
                    receipt_finalize_started = time.perf_counter()
                receipt_finalize_ms = (
                    time.perf_counter() - receipt_finalize_started
                ) * 1000.0
                timing_count = max(1, len(timings))
                for timing in timings.values():
                    timing["connection_checkout_ms"] = (
                        connection_checkout_ms / timing_count
                    )
                    timing["receipt_lookup_ms"] = receipt_lookup_ms / timing_count
                    timing.update({name: value/timing_count for name,value in receipt_phases.items()})
                    timing['receipt_phases_measured'] = True
                    timing["receipt_finalize_ms"] = (
                        receipt_finalize_ms / timing_count
                    )
                commit_started = time.perf_counter()
                session.commit()
                commit_ms = (time.perf_counter() - commit_started) * 1000.0
                for timing in timings.values():
                    timing["commit_ms"] = commit_ms / timing_count
                    timing["commit_measured"] = True
                if timings:
                    # One line per batch; per-profile lines are DEBUG in user_sync.
                    logger.info("Synced %d user(s) for site %s", len(timings), site)
            except Exception:
                session.rollback()
                raise
        return results

    async def _upsert_subscriptions(
        self,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        from ultima_scraper_db_archive import UserModel

        from .helpers.user_sync import sync_user_from_api

        manager = self._site_manager(site)
        results: dict[str, dict[str, Any]] = {}
        with manager.get_session() as session:
            try:
                from .write_receipts import receipt_batch

                with receipt_batch(session, commands, results) as pending:
                    for command in pending:
                        subscriber_id = int(command.payload["subscriber_id"])
                        if session.get(UserModel, subscriber_id) is None:
                            raise RuntimeError(
                                f"Subscriber user {subscriber_id} does not exist"
                            )
                        saved = 0
                        updated = 0
                        for row in command.payload.get("subscriptions") or []:
                            row = cast(dict[str, Any], row)
                            sync_payload = row.get("user")
                            user_id = int(row["user_id"])
                            if isinstance(sync_payload, dict):
                                user_payload = sync_payload.get("user")
                                if not isinstance(user_payload, dict):
                                    raise ValueError(
                                        f"Invalid queued subscription user {user_id}"
                                    )
                                synced = await sync_user_from_api(
                                    manager,
                                    cast(Any, _QueuedUser(user_payload)),
                                    site=site,
                                    session=session,
                                    sync_socials=bool(sync_payload.get("sync_socials")),
                                    observed_aliases=tuple(
                                        sync_payload.get("observed_aliases") or ()
                                    ),
                                )
                                if not synced:
                                    raise RuntimeError(
                                        f"Subscription user sync failed for {user_id}"
                                    )
                            elif session.get(UserModel, user_id) is None:
                                session.add(
                                    UserModel(
                                        id=user_id,
                                        username=row.get("username") or f"user_{user_id}",
                                    )
                                )
                                session.flush()
                            _, created = manager.create_or_update_subscription(
                                user_id=user_id,
                                subscriber_id=subscriber_id,
                                data={
                                    "active": bool(row.get("active", True)),
                                    "expires_at": _as_optional_datetime(
                                        row.get("expires_at")
                                    ),
                                },
                                session=session,
                            )
                            if created:
                                saved += 1
                            else:
                                updated += 1
                        results[command.command_id] = {
                            "processed": saved + updated,
                            "saved": saved,
                            "updated": updated,
                        }
                session.commit()
            except Exception:
                session.rollback()
                raise
        return results

    def _upsert_invalid_users(
        self,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        from ultima_scraper_db_archive import InvalidUserModel

        manager = self._site_manager(site)
        results: dict[str, dict[str, Any]] = {}
        timings: dict[str, dict[str, Any]] = {}
        with manager.get_session() as session:
            try:
                from .write_receipts import receipt_batch

                connection_started = time.perf_counter()
                session.connection()
                connection_checkout_ms = (
                    time.perf_counter() - connection_started
                ) * 1000.0
                receipt_phases: dict[str, float] = {}
                receipt_context = receipt_batch(session, commands, results, timing=receipt_phases)
                receipt_started = time.perf_counter()
                with receipt_context as pending:
                    receipt_lookup_ms = (
                        time.perf_counter() - receipt_started
                    ) * 1000.0
                    operation_started = time.perf_counter()
                    batches: list[dict[int, dict[str, Any]]] = []
                    rows: dict[int, dict[str, Any]] = {}
                    for command in pending:
                        user_id = int(command.payload["user_id"])
                        checked_at = _as_datetime(command.payload.get("checked_at"))
                        # PostgreSQL cannot update one row twice in an upsert.
                        # Split at repeated IDs so each mutation is validated
                        # and timestamped in its original per-record order.
                        if user_id in rows:
                            batches.append(rows)
                            rows = {}
                        rows[user_id] = {
                            "site": site,
                            "user_id": user_id,
                            "reason": str(command.payload.get("reason") or "not_found"),
                            "last_checked_at": checked_at,
                            "updated_at": None,
                        }
                    if rows:
                        batches.append(rows)
                    for batch in batches:
                        records = [batch[user_id] for user_id in sorted(batch)]
                        table = InvalidUserModel.__table__
                        connection = session.connection(bind_arguments={"clause": table})
                        if connection.dialect.name != "postgresql":
                            session.execute(_invalid_user_upsert(table, records))
                            continue
                        # Preserve the caller's transaction and flush boundary.
                        # Duplicate IDs already split batches in command order.
                        session.flush()
                        compiled = _compiled_invalid_user_upsert(
                            table, connection.dialect, len(records)
                        )
                        connection.execute(compiled, {
                            f"invalid_{index}_{column}": row[column]
                            for index, row in enumerate(records)
                            for column in _INVALID_USER_COLUMNS
                        })
                    operation_ms = (time.perf_counter() - operation_started) * 1000.0
                    for command in pending:
                        timing = {
                            "execution_ms": operation_ms / len(pending),
                            "execution_measured": True,
                            "commit_ms": 0.0,
                            "commit_measured": False,
                            "connection_checkout_ms": connection_checkout_ms / len(pending),
                            "receipt_lookup_ms": receipt_lookup_ms / len(pending),
                        }
                        timings[command.command_id] = timing
                        results[command.command_id] = {
                            "upserted": True,
                            WRITE_TIMING_KEY: timing,
                        }
                    receipt_finalize_started = time.perf_counter()
                receipt_finalize_ms = (
                    time.perf_counter() - receipt_finalize_started
                ) * 1000.0
                for timing in timings.values():
                    timing["receipt_finalize_ms"] = receipt_finalize_ms / len(timings)
                    timing.update({name: value/len(timings) for name,value in receipt_phases.items()})
                    timing['receipt_phases_measured'] = True
                commit_started = time.perf_counter()
                session.commit()
                commit_ms = (time.perf_counter() - commit_started) * 1000.0
                for timing in timings.values():
                    timing["commit_ms"] = commit_ms / len(timings)
                    timing["commit_measured"] = True
            except Exception:
                session.rollback()
                raise
        return results

    def _finalize_downloads(
        self,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        manager = self._site_manager(site)
        results: dict[str, dict[str, Any]] = {}
        with manager.get_session() as session:
            try:
                from .write_receipts import receipt_batch

                with receipt_batch(session, commands, results) as pending:
                    for command in pending:
                        payload = command.payload
                        media_ids = [int(value) for value in payload.get("media_ids") or []]
                        marked = manager.mark_media_downloaded(media_ids, session=session)
                        user_updated = False
                        if payload.get("downloaded_at") is not None:
                            user_updated = (
                                manager.update_user(
                                    int(payload["user_id"]),
                                    {
                                        "downloaded_at": _as_datetime(
                                            payload["downloaded_at"]
                                        )
                                    },
                                    session=session,
                                )
                                is not None
                            )
                        filepaths = [
                            (
                                int(row[0]),
                                str(row[1]),
                                bool(row[2]),
                                bool(row[3]),
                            )
                            for row in payload.get("filepaths") or []
                        ]
                        filepath_count = manager.batch_record_filepaths(
                            filepaths, session=session
                        )
                        media_sizes = {
                            int(media_id): int(size)
                            for media_id, size in (payload.get("media_sizes") or {}).items()
                        }
                        size_count = manager.update_media_sizes_bulk(
                            media_sizes, session=session
                        )
                        results[command.command_id] = {
                            "marked_downloaded": marked,
                            "user_updated": user_updated,
                            "filepaths": filepath_count,
                            "media_sizes": size_count,
                        }
                session.commit()
            except Exception:
                session.rollback()
                raise
        return results

    def _update_drm_streams(
        self,
        commands: Sequence[DatabaseWriteCommand],
        site: str | None,
    ) -> Mapping[str, dict[str, Any]]:
        manager = self._site_manager(site)
        results: dict[str, dict[str, Any]] = {}
        with manager.get_session() as session:
            try:
                from .write_receipts import receipt_batch

                with receipt_batch(session, commands, results) as pending:
                    for command in pending:
                        payload = command.payload
                        updated = manager.update_drm_stream_urls(
                            int(payload["media_id"]),
                            payload.get("video_url"),
                            payload.get("audio_url"),
                            video_size=payload.get("video_size"),
                            audio_size=payload.get("audio_size"),
                            session=session,
                        )
                        if not updated:
                            raise RuntimeError(
                                f"DRM manifest for media {payload['media_id']} does not exist"
                            )
                        results[command.command_id] = {"updated": bool(updated)}
                session.commit()
            except Exception:
                session.rollback()
                raise
        return results

    @staticmethod
    def _sync_job_statuses(
        commands: Sequence[DatabaseWriteCommand],
    ) -> Mapping[str, dict[str, Any]]:
        from ..job_system.storage import JobStorage

        from ultima_scraper_db_archive.global_db import global_session
        from .write_receipts import receipt_batch

        from ..job_system.mps_attempt import sql_definitions, sql_is_current
        from ..job_system import ordinary_edit

        results: dict[str, dict[str, Any]] = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                definitions = sql_definitions(session, (command.payload for command in pending))
                ordinary_edit.sql_definitions(session, (command.payload for command in pending))
                for command in pending:
                    payload = command.payload
                    if not sql_is_current(payload, definitions):
                        results[command.command_id] = {"synced": False, "stale": True}
                        continue
                    synced = JobStorage._update_job_status_database_sync(
                        str(payload["job_id"]),
                        str(payload["status"]),
                        error=(str(payload["error"]) if payload.get("error") else None),
                        completed_at=(
                            str(payload["completed_at"])
                            if payload.get("completed_at")
                            else None
                        ),
                        job_snapshot=(None if "mps_task_manifest" in payload else cast(dict[str, Any] | None, payload.get("job_snapshot"))),
                        db_session=session,
                    )
                    if synced is False:
                        results[command.command_id] = {"synced": False, "stale": True}
                        continue
                    if payload["status"] in {"failed", "cancelled"}:
                        from .mps_manifest import MPSManifestStore
                        MPSManifestStore().terminate(
                            str(payload["job_id"]), status=str(payload["status"]), db_session=session,
                        )
                    results[command.command_id] = {"synced": True}
            session.commit()
        return results

    @staticmethod
    def _sync_task_statuses(
        commands: Sequence[DatabaseWriteCommand],
    ) -> Mapping[str, dict[str, Any]]:
        from ultima_scraper_db_archive.global_db import CentralTaskModel, global_session

        from ..job_system.mps_attempt import sql_definitions, sql_is_current
        from ..job_system import ordinary_edit

        results: dict[str, dict[str, Any]] = {}
        with global_session() as session:
            try:
                from .write_receipts import receipt_batch

                with receipt_batch(session, commands, results) as pending:
                    definitions = sql_definitions(session, (command.payload for command in pending))
                    ordinary_definitions = ordinary_edit.sql_definitions(session, (command.payload for command in pending))
                    for command in pending:
                        payload = command.payload
                        task_id = str(payload["task_id"])
                        if not sql_is_current(payload, definitions):
                            results[command.command_id] = {"synced": False, "stale": True}
                            continue
                        job_id = str(payload["job_id"])
                        if ("mps_task_manifest" not in payload and
                            str(payload.get("task_definition_id", job_id)) != ordinary_definitions.get(job_id, job_id)):
                            results[command.command_id] = {"synced": False, "stale": True}
                            continue
                        row = session.execute(select(CentralTaskModel).where(
                            CentralTaskModel.job_id == job_id,
                            CentralTaskModel.task_id == task_id,
                        )).scalar_one_or_none()
                        if row is not None:
                            status = str(payload["status"])
                            timestamp = _as_datetime(payload.get("event_time"))
                            previous_event_at = (
                                _as_datetime(row.last_event_at)
                                if row.last_event_at is not None
                                else None
                            )
                            if (
                                previous_event_at is not None
                                and previous_event_at > timestamp
                            ):
                                results[command.command_id] = {
                                    "synced": False,
                                    "stale": True,
                                }
                                continue
                            row.status = status
                            row.last_event_at = timestamp
                            if status == "running":
                                row.started_at = row.started_at or timestamp
                                row.error = None
                            elif status in {"completed", "failed", "cancelled"}:
                                if status in {"completed", "failed"}:
                                    row.started_at = row.started_at or timestamp
                                row.completed_at = timestamp
                                row.error = (
                                    str(payload.get("error") or "")
                                    if status == "failed"
                                    else None
                                )
                            session.add(row)
                        results[command.command_id] = {"synced": row is not None}
                session.commit()
            except Exception:
                session.rollback()
                raise
        return results

    async def _archive_job_tasks(
        self,
        commands: Sequence[DatabaseWriteCommand],
    ) -> Mapping[str, dict[str, Any]]:
        from ..job_system import mps_attempt, retention
        from contextlib import nullcontext
        from ..job_system.storage import JobStorage
        from .write_receipts import lookup, record_success
        from .write_queue import DatabaseWriteClient

        storage = JobStorage(cast(Any, self.redis))
        results: dict[str, dict[str, Any]] = {}
        for command in commands:
            if not command.payload.get("generation"):
                raise ValueError("Task archive command is missing its job generation")
            receipt = (await asyncio.to_thread(lookup, [command.command_id])).get(
                command.command_id
            )
            if receipt is not None:
                results[command.command_id] = DatabaseWriteClient._unwrap_result(
                    receipt
                )
                continue
            job_id = str(command.payload["job_id"])
            identity = None
            if "mps_task_manifest" in command.payload or await storage._client().hget(
                storage.JOB_STATE_KEY.format(job_id=job_id), "mps_manifest"
            ) in (b"1", "1"):
                identity = mps_attempt.Identity(
                    job_id, "", command.payload.get("mps_task_manifest") or ""
                )
            try:
                snapshot = None
                if identity is None or command.payload.get("archive_header"):
                    snapshot = await retention.capture_terminal(
                        storage, job_id, allow_updated_at=True,
                        max_bytes=(retention.header_byte_limit(storage, value=command.payload.get("header_max_bytes"))
                                   if command.payload.get("archive_header") else None),
                    )
                    if (snapshot is None
                            or (snapshot.manifest_flag == "1" and identity is None)
                            or snapshot.generation != str(command.payload["generation"])
                            or ("archive_signature" in command.payload and
                                retention.archive_signature(snapshot) != command.payload["archive_signature"])
                            or (identity is None and retention.definition_id(job_id, snapshot.config) != str(
                                command.payload.get("task_definition_id", job_id)
                            ))):
                        raise retention.StaleRetirement(
                            f"Archive command for {job_id} no longer matches its terminal header"
                        )
                with (
                    mps_attempt.scope(identity),
                    retention.scope(snapshot) if snapshot is not None else nullcontext(),
                ):
                    count = await storage._archive_job_tasks_to_db(
                        job_id,
                        batch_size=command.payload.get("batch_size"),
                        max_bytes=command.payload.get("max_bytes"),
                    )
                    if command.payload.get("archive_header"):
                        from ..database.threads import run_database_call
                        from ultima_scraper_db_archive.global_db import global_session
                        from .job_history import archive_runtime_header

                        assert snapshot is not None
                        await retention.evaluate(storage, job_id, "return 1", [], [])
                        await run_database_call(
                            archive_runtime_header, global_session, snapshot,
                            max_bytes=retention.header_byte_limit(storage, value=command.payload.get("header_max_bytes")),
                        )
                        await retention.evaluate(storage, job_id, "return 1", [], [])
                outcome = {"archived_tasks": count}
            except (mps_attempt.StaleTaskDefinition, retention.StaleRetirement):
                outcome = {"archived_tasks": 0, "stale": True}
            # Archive-before-purge is a resumable sequence of bounded SQL
            # transactions. Its final receipt follows the verified archive;
            # an interrupted sequence resumes through the existing checkpoint.
            results[command.command_id] = await asyncio.to_thread(
                record_success,
                command,
                outcome,
            )
        return results

    @staticmethod
    def _reconcile_central_jobs(
        commands: Sequence[DatabaseWriteCommand],
    ) -> Mapping[str, dict[str, Any]]:
        from ultima_scraper_db_archive.global_db import global_session
        from .write_receipts import receipt_batch
        from ..job_system.storage import JobStorage

        results: dict[str, dict[str, Any]] = {}
        with global_session() as session:
            with receipt_batch(session, commands, results) as pending:
                for command in pending:
                    result = JobStorage._reconcile_database_jobs_sync(
                        batch_size=command.payload.get("batch_size"),
                        after_task_id=command.payload.get("after_task_id"),
                        db_session=session,
                    )
                    # Already queued commands from an older worker requested a
                    # complete sweep. Preserve that contract within their receipt
                    # transaction; current workers explicitly request one page.
                    if "after_task_id" not in command.payload:
                        while result["next_task_id"] is not None:
                            page = JobStorage._reconcile_database_jobs_sync(
                                batch_size=command.payload.get("batch_size"),
                                after_task_id=result["next_task_id"],
                                db_session=session,
                            )
                            result["repaired_tasks"] += page["repaired_tasks"]
                            result["next_task_id"] = page["next_task_id"]
                    results[command.command_id] = result
            session.commit()
        return results

    async def persist_outcome(self, command: DatabaseWriteCommand, result: Any) -> Any:
        from .write_receipts import record_failure

        return await asyncio.to_thread(record_failure, command, result)

    async def persist_invalid(self, entry_id: str, fields: Mapping[str, str], error: str) -> None:
        from .write_receipts import record_invalid

        await asyncio.to_thread(record_invalid, entry_id, fields, error)

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            self.backend.multi_site_db_manager.disconnect_all()
        with contextlib.suppress(Exception):
            from ultima_scraper_db_archive.global_db import get_postgres_engine

            engine = get_postgres_engine()
            if engine is not None:
                engine.dispose()


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        if normalized:
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _as_optional_datetime(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    return _as_datetime(value)
