"""Durable media membership, signed URLs and pageable per-task evidence.

The writer owns mutations. Readers use the selection primary key and bound
both rows and text bytes before retrieving payloads from PostgreSQL.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from sqlalchemy import (
    and_,
    bindparam,
    case,
    func,
    literal,
    select,
    true,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from ultima_scraper_core import runtime_env_value
from ultima_scraper_db_archive.global_db import (
    TaskMediaEvidenceModel as Evidence,
    TaskMediaPlanModel as Plan,
    TaskMediaSelectionModel as Media,
    global_session,
)


def positive_budget(name: str, fallback: int) -> int:
    raw = runtime_env_value(name)
    value = fallback if raw is None or not raw.strip() else int(raw)
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def page_byte_budget() -> int:
    from .write_queue import DatabaseWriteClient

    return positive_budget(
        "ULTIMA_MEDIA_PAGE_MAX_BYTES",
        positive_budget(
            "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES",
            DatabaseWriteClient.MAX_COMMAND_BYTES,
        ),
    )


def normalize_content_type(value: str) -> str:
    value = value.lower().replace("_", "")
    return {
        "archivedposts": "archived_posts",
        "massmessages": "mass_messages",
        "highlights": "stories",
    }.get(value, value)


def _date(value: str | datetime) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    return result if result.tzinfo else result.replace(tzinfo=timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _text_size(column):
    return func.octet_length(func.coalesce(column, ""))


class BoundedPreview:
    """A display preview only; callers persist complete evidence separately."""

    def __init__(self, *, limit: int, max_bytes: int):
        if limit <= 0 or max_bytes <= 0:
            raise ValueError("Preview budgets must be positive")
        self.limit, self.max_bytes = limit, max_bytes
        self.values: list[Any] = []
        self.count = 0
        self.bytes = 0
        self.full = False

    def extend(self, values):
        for value in values:
            self.count += 1
            if self.full or len(self.values) >= self.limit:
                continue
            size = len(_json(value))
            if self.bytes + size <= self.max_bytes:
                self.values.append(value)
                self.bytes += size
            else:
                self.full = True


def _bounded_ids(rows, max_bytes: int) -> list[int]:
    if max_bytes <= 0:
        raise ValueError("Media page byte budget must be positive")
    selected = []
    used = 0
    for identifier, size in rows:
        if used + int(size) > max_bytes:
            if not selected:
                raise ValueError(
                    f"Media/evidence entry {identifier} needs {size} text bytes; "
                    f"increase ULTIMA_MEDIA_PAGE_MAX_BYTES (currently {max_bytes})"
                )
            break
        selected.append(int(identifier))
        used += int(size)
    return selected


class MediaSelectionStore:
    def __init__(self, session_factory: Callable = global_session):
        self.session_factory = session_factory

    @staticmethod
    def ensure(session, spec: Mapping[str, Any], *, writing: bool = True):
        values = {
            key: spec[key]
            for key in ("selection_id", "job_id", "task_id", "site", "auth_id")
        }
        if not all(
            isinstance(values[key], str) and values[key]
            for key in ("selection_id", "job_id", "task_id", "site")
        ):
            raise ValueError("Media selection identity is incomplete")
        session.execute(
            insert(Plan)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Plan.selection_id])
        )
        row = (
            session.execute(
                select(Plan.__table__)
                .where(Plan.selection_id == values["selection_id"])
                .with_for_update()
            )
            .mappings()
            .one()
        )
        if any(row[key] != value for key, value in values.items()):
            raise ValueError(
                "Media selection belongs to a different task or auth scope"
            )
        if writing and row["ready"]:
            raise ValueError("A published media selection is immutable")
        return dict(row)

    @staticmethod
    def upsert_rows(session, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        statement = insert(Media.__table__)
        incoming = statement.excluded
        newer = tuple_(incoming.content_created_at, incoming.content_id) > tuple_(
            Media.content_created_at, Media.content_id
        )
        statement = statement.on_conflict_do_update(
            index_elements=[
                Media.selection_id,
                Media.user_id,
                Media.content_type,
                Media.media_id,
            ],
            set_={
                "content_id": case(
                    (newer, incoming.content_id), else_=Media.content_id
                ),
                "content_created_at": case(
                    (newer, incoming.content_created_at), else_=Media.content_created_at
                ),
                # Signatures reflect the last fetched version even if its content
                # occurrence is older than the occurrence chosen for path metadata.
                "url": func.coalesce(incoming.url, Media.url),
                "drm_json": func.coalesce(incoming.drm_json, Media.drm_json),
            },
        )
        session.execute(statement, list(rows))

    @classmethod
    def add_content(cls, session, spec, items, content_type: str):
        cls.ensure(session, spec)
        content_type = normalize_content_type(content_type)
        rows = []
        for item in items:
            created_at = _date(item["created_at"])
            for media in item.get("media") or ():
                rows.append(
                    {
                        "selection_id": spec["selection_id"],
                        "user_id": int(item["user_id"]),
                        "content_type": content_type,
                        "media_id": int(media["id"]),
                        "content_id": int(item["id"]),
                        "content_created_at": created_at,
                        "url": media.get("url"),
                        "drm_json": _json(media["drm_payload"])
                        if media.get("drm_payload")
                        else None,
                    }
                )
        cls.upsert_rows(session, rows)

    @staticmethod
    def put_evidence(
        session, selection_id: str, kind: str, offset: int, values: Sequence[Any]
    ):
        if kind not in {"errors", "download_results", "files"} or offset < 0:
            raise ValueError("Invalid media evidence category or offset")
        rows = [
            {
                "selection_id": selection_id,
                "kind": kind,
                "ordinal": offset + i,
                "payload_json": _json(value),
            }
            for i, value in enumerate(values)
        ]
        if rows:
            statement = insert(Evidence.__table__)
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        Evidence.selection_id,
                        Evidence.kind,
                        Evidence.ordinal,
                    ],
                    set_={"payload_json": statement.excluded.payload_json},
                ),
                rows,
            )

    def scope(
        self,
        selection_id: str,
        *,
        job_id: str,
        task_id: str | None = None,
        site: str | None = None,
        auth_id: int | None = None,
    ):
        with self.session_factory() as session:
            row = (
                session.execute(
                    select(Plan.__table__).where(Plan.selection_id == selection_id)
                )
                .mappings()
                .one_or_none()
            )
            if (
                row is None
                or row["job_id"] != job_id
                or (task_id is not None and row["task_id"] != task_id)
                or (site is not None and row["site"] != site)
                or (auth_id is not None and row["auth_id"] != auth_id)
            ):
                raise ValueError(
                    "Media selection is unavailable in this task/auth scope"
                )
            return dict(row)

    @staticmethod
    def seal(session, spec):
        plan = MediaSelectionStore.ensure(session, spec, writing=False)
        if not plan["ready"]:
            total = session.scalar(
                select(func.count())
                .select_from(Media)
                .where(Media.selection_id == spec["selection_id"])
            )
            session.execute(
                update(Plan)
                .where(Plan.selection_id == spec["selection_id"])
                .values(ready=True, total=total)
            )
            plan.update(ready=True, total=total)
        return plan

    def media_page(
        self,
        selection_id: str,
        *,
        user_id: int,
        content_type: str,
        after: int,
        limit: int,
        max_bytes: int,
        session=None,
    ):
        from contextlib import nullcontext

        if limit <= 0:
            raise ValueError("Media page size must be positive")
        conditions = (
            Media.selection_id == selection_id,
            Media.user_id == user_id,
            Media.content_type == normalize_content_type(content_type),
            Media.media_id > after,
        )
        size = _text_size(Media.url) + _text_size(Media.drm_json) + 128
        with self.session_factory() if session is None else nullcontext(session) as db:
            ids = _bounded_ids(
                db.execute(
                    select(Media.media_id, size)
                    .where(*conditions)
                    .order_by(Media.media_id)
                    .limit(limit)
                ),
                max_bytes,
            )
            if not ids:
                return []
            return [
                dict(row)
                for row in db.execute(
                    select(Media.__table__)
                    .where(*conditions, Media.media_id.in_(ids))
                    .order_by(Media.media_id)
                ).mappings()
            ]

    def evidence_page(
        self,
        selection_id: str,
        *,
        kind: str,
        after: int = -1,
        limit: int,
        max_bytes: int,
    ):
        if limit <= 0 or kind not in {"errors", "download_results", "files"}:
            raise ValueError("Invalid evidence page request")
        conditions = (
            Evidence.selection_id == selection_id,
            Evidence.kind == kind,
            Evidence.ordinal > after,
        )
        with self.session_factory() as session:
            ids = _bounded_ids(
                session.execute(
                    select(Evidence.ordinal, _text_size(Evidence.payload_json))
                    .where(*conditions)
                    .order_by(Evidence.ordinal)
                    .limit(limit)
                ),
                max_bytes,
            )
            if not ids:
                return {"items": [], "ordinals": [], "next_cursor": None}
            rows = session.execute(
                select(Evidence.ordinal, Evidence.payload_json)
                .where(*conditions, Evidence.ordinal.in_(ids))
                .order_by(Evidence.ordinal)
            ).all()
            more = session.scalar(
                select(Evidence.ordinal)
                .where(
                    Evidence.selection_id == selection_id,
                    Evidence.kind == kind,
                    Evidence.ordinal > ids[-1],
                )
                .limit(1)
            )
            return {
                "items": [json.loads(row[1]) for row in rows],
                "ordinals": ids,
                "next_cursor": ids[-1] if more is not None else None,
            }

    @staticmethod
    def _content_table(content_type: str):
        from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates import (
            postgres_site as models,
        )

        name, association = {
            "posts": ("PostModel", "post_id"),
            "archived_posts": ("PostModel", "post_id"),
            "messages": ("MessageModel", "message_id"),
            "stories": ("StoryModel", "story_id"),
            "mass_messages": ("MassMessageModel", "mass_message_id"),
        }[normalize_content_type(content_type)]
        return getattr(models, name).__table__, association

    @classmethod
    def archive_page(
        cls, session, spec, *, user_id, content_type, cursor, limit, content_ids=None
    ):
        """Select a bounded number of content IDs; media stays in SQL during staging."""
        from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates import (
            postgres_site as models,
        )

        if limit <= 0:
            raise ValueError("Archive selection page size must be positive")
        cls.ensure(session, spec)
        content_type = normalize_content_type(content_type)
        content, association_name = cls._content_table(content_type)
        conditions = [content.c.user_id == user_id]
        if content_type in {"posts", "archived_posts"}:
            conditions.append(content.c.archived.is_(content_type == "archived_posts"))
        if content_ids is not None:
            conditions.append(content.c.id.in_(content_ids))
        if cursor:
            conditions.append(
                tuple_(content.c.created_at, content.c.id)
                < tuple_(_date(cursor[0]), int(cursor[1]))
            )
        page = session.execute(
            select(content.c.id, content.c.created_at)
            .where(*conditions)
            .order_by(content.c.created_at.desc(), content.c.id.desc())
            .limit(limit)
        ).all()
        if not page:
            return {"cursor": None, "count": 0}
        association = models.ContentMediaAssoModel.__table__
        media = models.MediaModel.__table__
        chosen = (
            select(
                literal(spec["selection_id"]),
                literal(user_id),
                literal(content_type),
                media.c.id,
                content.c.id,
                content.c.created_at,
                literal(None),
                literal(None),
            )
            .select_from(
                content.join(
                    association, association.c[association_name] == content.c.id
                ).join(media, media.c.id == association.c.media_id)
            )
            .where(content.c.id.in_([row.id for row in page]))
        )
        chosen = chosen.distinct(media.c.id).order_by(
            media.c.id, content.c.created_at.desc(), content.c.id.desc()
        )
        statement = insert(Media.__table__).from_select(
            [
                "selection_id",
                "user_id",
                "content_type",
                "media_id",
                "content_id",
                "content_created_at",
                "url",
                "drm_json",
            ],
            chosen,
        )
        incoming = statement.excluded
        newer = tuple_(incoming.content_created_at, incoming.content_id) > tuple_(
            Media.content_created_at, Media.content_id
        )
        session.execute(
            statement.on_conflict_do_update(
                index_elements=[
                    Media.selection_id,
                    Media.user_id,
                    Media.content_type,
                    Media.media_id,
                ],
                set_={
                    "content_id": incoming.content_id,
                    "content_created_at": incoming.content_created_at,
                },
                where=newer,
            )
        )
        return {
            "cursor": [page[-1].created_at.isoformat(), page[-1].id],
            "count": len(page),
        }

    @classmethod
    def _download_query(cls, selection_id, user_id, content_type, auth_id):
        from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates import (
            postgres_site as models,
        )

        content_type = normalize_content_type(content_type)
        content, _ = cls._content_table(content_type)
        media = models.MediaModel.__table__
        urls = models.MediaUrlModel.__table__
        drm_table = models.DRMManifestModel.__table__
        drm = (
            select(drm_table)
            .where(drm_table.c.media_id == media.c.id)
            .order_by(drm_table.c.id.desc())
            .limit(1)
            .lateral("selected_drm")
        )
        source = Media.__table__.join(
            content,
            and_(content.c.id == Media.content_id, content.c.user_id == user_id),
        )
        source = (
            source.join(media, media.c.id == Media.media_id)
            .outerjoin(
                urls, and_(urls.c.media_id == media.c.id, urls.c.auth_id == auth_id)
            )
            .outerjoin(drm, true())
        )
        content_columns = [
            column.label("content_" + column.name) for column in content.c
        ]
        media_columns = [column.label("media_" + column.name) for column in media.c]
        drm_columns = [column.label("drm_" + column.name) for column in drm.c]
        columns = [
            Media.media_id.label("selected_id"),
            Media.url.label("selected_url"),
            Media.drm_json.label("selected_drm"),
            urls.c.url.label("auth_url"),
            *content_columns,
            *media_columns,
            *drm_columns,
        ]
        size = (
            _text_size(Media.url)
            + _text_size(Media.drm_json)
            + _text_size(media.c.url)
            + _text_size(urls.c.url)
            + 512
        )
        if "text" in content.c:
            size += _text_size(content.c.text)
        for column in drm.c:
            if column.name not in {
                "id",
                "media_id",
                "created_at",
                "updated_at",
                "video_size",
                "audio_size",
            }:
                size += _text_size(column)
        filters = (
            Media.selection_id == selection_id,
            Media.user_id == user_id,
            Media.content_type == content_type,
        )
        return source, columns, size, filters, media

    def download_target_page(self, selection_id, *, after=None, limit):
        """Page distinct author/type pairs using the existing selection key."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("Download target page size must be positive")
        conditions = [Media.selection_id == selection_id]
        if after is not None:
            conditions.append(tuple_(Media.user_id, Media.content_type) > tuple_(*after))
        with self.session_factory() as session:
            return [
                tuple(row)
                for row in session.execute(
                    select(Media.user_id, Media.content_type)
                    .where(*conditions)
                    .distinct()
                    .order_by(Media.user_id, Media.content_type)
                    .limit(limit)
                )
            ]

    def feed_download_totals(self, selection_id):
        from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates import (
            postgres_site as models,
        )

        media = models.MediaModel.__table__
        with self.session_factory() as session:
            row = session.execute(
                select(
                    func.count(),
                    func.coalesce(func.sum(media.c.size), 0),
                    func.coalesce(func.sum(case((media.c.drm.is_(True), 1), else_=0)), 0),
                )
                .select_from(Media.__table__.outerjoin(media, media.c.id == Media.media_id))
                .where(Media.selection_id == selection_id)
            ).one()
            return {"count": int(row[0]), "bytes": int(row[1]), "drm": int(row[2])}

    def download_totals(self, selection_id, *, user_id, content_type, auth_id):
        from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates import (
            postgres_site as models,
        )

        content_type = normalize_content_type(content_type)
        content, _ = self._content_table(content_type)
        media = models.MediaModel.__table__
        source = Media.__table__.join(
            content,
            and_(content.c.id == Media.content_id, content.c.user_id == user_id),
        )
        source = source.join(media, media.c.id == Media.media_id)
        with self.session_factory() as session:
            row = session.execute(
                select(
                    func.count(),
                    func.coalesce(func.sum(media.c.size), 0),
                    func.coalesce(
                        func.sum(case((media.c.drm.is_(True), 1), else_=0)), 0
                    ),
                )
                .select_from(source)
                .where(
                    Media.selection_id == selection_id,
                    Media.user_id == user_id,
                    Media.content_type == content_type,
                )
            ).one()
            return {"count": int(row[0]), "bytes": int(row[1]), "drm": int(row[2])}

    def download_page(
        self, selection_id, *, user_id, content_type, auth_id, after, limit, max_bytes
    ):
        from ..job_system.metadata_models import ContentMetadata

        if limit <= 0:
            raise ValueError("Download page size must be positive")
        source, columns, size, filters, _ = self._download_query(
            selection_id, user_id, content_type, auth_id
        )
        with self.session_factory() as session:
            ids = _bounded_ids(
                session.execute(
                    select(Media.media_id, size)
                    .select_from(source)
                    .where(*filters, Media.media_id > after)
                    .order_by(Media.media_id)
                    .limit(limit)
                ),
                max_bytes,
            )
            if not ids:
                return []
            rows = session.execute(
                select(*columns)
                .select_from(source)
                .where(*filters, Media.media_id.in_(ids))
                .order_by(Media.media_id)
            ).mappings()
            result = []
            for row in rows:
                content = {
                    key.removeprefix("content_"): value
                    for key, value in row.items()
                    if key.startswith("content_")
                }
                media = {
                    key.removeprefix("media_"): value
                    for key, value in row.items()
                    if key.startswith("media_")
                }
                drm = {
                    key.removeprefix("drm_"): value
                    for key, value in row.items()
                    if key.startswith("drm_")
                }
                fresh_drm = (
                    json.loads(row["selected_drm"]) if row["selected_drm"] else None
                )
                if fresh_drm:
                    # Cached stream URLs must match this exact signed manifest scope.
                    if any(drm.get(key) != fresh_drm.get(key) for key in (
                        "dash_manifest", "dash_key_pair_id", "dash_policy", "dash_signature"
                    )):
                        drm = {}
                    drm.update(fresh_drm)
                drm.setdefault("video_url", None)
                drm.setdefault("audio_url", None)
                media["drm_manifest"] = (
                    SimpleNamespace(**drm) if drm.get("dash_manifest") else None
                )
                media["url"] = (
                    row["selected_url"] or row["auth_url"] or media.get("url")
                )
                media["media_urls"] = []
                content["media"] = [SimpleNamespace(**media)]
                result.append(
                    ContentMetadata.from_db_record(
                        SimpleNamespace(**content),
                        content_type=normalize_content_type(content_type),
                        auth_id=auth_id,
                    )
                )
            return result

    def mutate(self, session, spec, payload):
        action = payload["action"]
        if action == "seal":
            plan = self.seal(session, spec)
            return {"selection_id": plan["selection_id"], "total": plan["total"]}
        if action == "archive_page":
            return self.archive_page(
                session,
                spec,
                user_id=int(payload["user_id"]),
                content_type=payload["content_type"],
                cursor=payload.get("cursor"),
                limit=int(payload["limit"]),
                content_ids=payload.get("content_ids"),
            )
        if action in {"copy_page", "adopt"}:
            self.ensure(session, spec)
            source = (
                session.execute(
                    select(Plan.__table__).where(
                        Plan.selection_id == payload["source_id"]
                    )
                )
                .mappings()
                .one()
            )
            if not source["ready"] or any(
                source[key] != spec[key] for key in ("job_id", "site", "auth_id")
            ):
                raise ValueError(
                    "Source media selection is not ready in this job/auth scope"
                )
            if source["task_id"] != payload["source_task_id"]:
                raise ValueError("Source media selection belongs to another task")
            if action == "adopt":
                # A normal scrape/download pair shares immutable membership and
                # signatures. Only the download evidence needs a separate header.
                self.seal(session, spec)
                return {
                    "selection_id": source["selection_id"],
                    "total": source["total"],
                }
            rows = self.media_page(
                source["selection_id"],
                user_id=int(payload["user_id"]),
                content_type=payload["content_type"],
                after=int(payload.get("after", 0)),
                limit=int(payload["limit"]),
                max_bytes=int(payload["max_bytes"]),
                session=session,
            )
            for row in rows:
                row["selection_id"] = spec["selection_id"]
            self.upsert_rows(session, rows)
            return {
                "cursor": rows[-1]["media_id"] if rows else None,
                "count": len(rows),
            }
        if action == "legacy_urls":
            self.ensure(session, spec)
            table = Media.__table__
            rows = [
                {"selected_media_id": int(row["media_id"]), "selected_url": row["url"]}
                for row in payload["rows"]
            ]
            if rows:
                session.execute(
                    update(table)
                    .where(
                        table.c.selection_id == spec["selection_id"],
                        table.c.user_id == int(payload["user_id"]),
                        table.c.content_type
                        == normalize_content_type(payload["content_type"]),
                        table.c.media_id == bindparam("selected_media_id"),
                    )
                    .values(url=bindparam("selected_url")),
                    rows,
                )
            return {"count": len(rows)}
        if action == "evidence":
            self.ensure(session, spec, writing=False)
            for category in payload["categories"]:
                self.put_evidence(
                    session,
                    spec["selection_id"],
                    category["kind"],
                    int(category["offset"]),
                    category["values"],
                )
            return {"stored": True}
        raise ValueError(f"Unsupported media selection operation: {action}")
