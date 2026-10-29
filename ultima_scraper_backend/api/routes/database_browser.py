"""Database browser API routes.

Provides endpoints for browsing users and content in the database.
"""

from __future__ import annotations

from ultima_scraper_backend.services import favourites_service
from ultima_scraper_backend.services.content_previews import media_preview

import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Protocol, TypeAlias, cast
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import and_, case, or_
from sqlalchemy import func as sa_func
from sqlalchemy import literal, union_all
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from ultima_scraper_db_archive import (
    BoughtContentModel,
    ContentMediaAssoModel,
    FilePathModel,
    HistoUserInfoModel,
    LinkedPostModel,
    ContentMentionModel,
    MassMessageModel,
    MediaModel,
    MessageModel,
    PostModel,
    RemoteURLModel,
    StoryModel,
    UserAliasModel,
    UserInfoModel,
    UserModel,
    UserProfileMediaModel,
)

from ...database.factory import MultiSiteDatabaseManager
from ...database.threads import run_database_call
from ...helpers.background_tasks import spawn_background_task

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database", tags=["database"])


class ContentMentionBackfillRequest(BaseModel):
    """Bounded, operator-triggered content-mention backfill request."""

    site: str
    batch_size: int = Field(default=5_000, ge=1)
    limit: int | None = Field(default=None, ge=1)
    apply: bool = False
    include_preview_context: bool = True
    include_legacy_links: bool = True
    worker_count: int = Field(default=3, ge=1)


class ContentMentionBackfillResponse(BaseModel):
    success: bool
    site: str
    dry_run: bool
    linked_users_seen: int = 0
    linked_users_inserted: int = 0
    content_rows_seen: int = 0
    handles_found: int = 0
    handles_resolved: int = 0
    text_mentions_inserted: int = 0
    duration_seconds: float | None = None
    oversized_profile_links: list["OversizedProfileLink"] = []
    ambiguous_profile_links: list["AmbiguousProfileLink"] = []
    details_total: int = 0
    same_platform_total: int = 0
    cross_platform_total: int = 0
    excluded_total: int = 0
    details_page: int = 1
    details_page_size: int = 100
    details: list["ContentMentionBackfillDetail"] = []
    error: str | None = None


class ContentMentionBackfillDetail(BaseModel):
    raw_mention: str
    normalized_mention: str
    mention_kind: str
    mentioned_user_id: int | None = None
    occurrence_count: int
    taggers: list["ContentMentionTagger"] = []
    occurrences: list["ContentMentionOccurrence"] = []


class ContentMentionTagger(BaseModel):
    user_id: int
    username: str | None = None


class ContentMentionOccurrence(BaseModel):
    content_type: str
    content_id: int
    content_text: str | None = None
    tagger_user_id: int | None = None
    tagger_username: str | None = None


class OversizedProfileLink(BaseModel):
    url: str
    occurrence_count: int


class AmbiguousProfileLink(BaseModel):
    url: str
    occurrence_count: int


class ContentMentionBackfillJobStart(BaseModel):
    job_id: str


class ContentMentionBackfillJobStatus(BaseModel):
    job_id: str
    status: str
    phase: str
    content_rows_scanned: int = 0
    total_content_rows: int = 0
    started_at: float | None = None
    result: ContentMentionBackfillResponse | None = None
    error: str | None = None


_content_mention_backfill_jobs: dict[str, dict[str, Any]] = {}


def _content_mention_backfill_response(
    stats: Any,
    *,
    apply: bool,
    duration_seconds: float | None = None,
    page: int = 1,
    page_size: int = 100,
    platform_scope: str = "all",
    multiple_taggers_only: bool = False,
    search: str = "",
) -> ContentMentionBackfillResponse:
    sorted_details = sorted(
        stats.details.values(),
        key=lambda detail: (-detail.occurrence_count, detail.normalized_mention),
    )
    excluded_total = sum(
        detail.mention_kind.startswith("excluded_") for detail in sorted_details
    )
    same_platform_total = sum(
        detail.mention_kind != "cross_platform_profile_link"
        and not detail.mention_kind.startswith("excluded_")
        for detail in sorted_details
    )
    cross_platform_total = len(sorted_details) - same_platform_total - excluded_total
    if platform_scope == "same":
        sorted_details = [
            detail
            for detail in sorted_details
            if detail.mention_kind != "cross_platform_profile_link"
            and not detail.mention_kind.startswith("excluded_")
        ]
    elif platform_scope == "cross":
        sorted_details = [
            detail
            for detail in sorted_details
            if detail.mention_kind == "cross_platform_profile_link"
        ]
    elif platform_scope == "excluded":
        sorted_details = [
            detail
            for detail in sorted_details
            if detail.mention_kind.startswith("excluded_")
        ]
    if multiple_taggers_only:
        sorted_details = [
            detail for detail in sorted_details if len(detail.taggers) > 1
        ]
    search = search.strip().casefold()
    if search:
        sorted_details = [
            detail
            for detail in sorted_details
            if (
                search in detail.raw_mention.casefold()
                or any(search in raw.casefold() for raw in detail.raw_mentions)
                or search in detail.normalized_mention.casefold()
                or (
                    detail.mentioned_user_id is not None
                    and search in str(detail.mentioned_user_id)
                )
                or any(
                    search in str(user_id)
                    or (username is not None and search in username.casefold())
                    for user_id, username in detail.taggers.items()
                )
            )
        ]
    details_total = len(sorted_details)
    page_details = sorted_details[(page - 1) * page_size : page * page_size]
    return ContentMentionBackfillResponse(
        success=True,
        site=stats.site,
        dry_run=not apply,
        linked_users_seen=stats.linked_users_seen,
        linked_users_inserted=stats.linked_users_inserted,
        content_rows_seen=stats.content_rows_seen,
        handles_found=stats.handles_found,
        handles_resolved=stats.handles_resolved,
        text_mentions_inserted=stats.text_mentions_inserted,
        duration_seconds=duration_seconds,
        oversized_profile_links=[
            OversizedProfileLink(url=url, occurrence_count=count)
            for url, count in sorted(
                stats.oversized_profile_links.items(),
                key=lambda item: (-item[1], item[0]),
            )[:100]
        ],
        ambiguous_profile_links=[
            AmbiguousProfileLink(url=url, occurrence_count=count)
            for url, count in sorted(
                stats.ambiguous_profile_links.items(),
                key=lambda item: (-item[1], item[0]),
            )[:100]
        ],
        details_total=details_total,
        same_platform_total=same_platform_total,
        cross_platform_total=cross_platform_total,
        excluded_total=excluded_total,
        details_page=page,
        details_page_size=page_size,
        details=[
            ContentMentionBackfillDetail(
                raw_mention=(
                    next(
                        (
                            raw
                            for raw in sorted(detail.raw_mentions)
                            if search and search in raw.casefold()
                        ),
                        (
                            detail.normalized_mention
                            if detail.mention_kind.startswith("excluded_")
                            else detail.raw_mention
                        ),
                    )
                ),
                normalized_mention=detail.normalized_mention,
                mention_kind=detail.mention_kind,
                mentioned_user_id=detail.mentioned_user_id,
                occurrence_count=detail.occurrence_count,
                taggers=[
                    ContentMentionTagger(user_id=user_id, username=username)
                    for user_id, username in sorted(detail.taggers.items())
                ],
                occurrences=[
                    ContentMentionOccurrence(
                        content_type=occurrence.content_type,
                        content_id=occurrence.content_id,
                        content_text=occurrence.content_text,
                        tagger_user_id=occurrence.tagger_user_id,
                        tagger_username=occurrence.tagger_username,
                    )
                    for occurrence in detail.occurrences
                ],
            )
            for detail in page_details
        ],
    )


@router.post(
    "/content-mentions/backfill", response_model=ContentMentionBackfillResponse
)
async def backfill_content_mentions(
    request: ContentMentionBackfillRequest,
) -> ContentMentionBackfillResponse:
    """Preview or write a bounded backfill; schema migrations are never run here."""
    site = request.site.strip().lower()
    if not site:
        raise HTTPException(status_code=422, detail="A site is required")

    try:
        # Kept in the maintenance script so CLI and WebUI produce identical rows.
        from scripts.backfill_content_mentions import backfill_site

        started_at = time.monotonic()
        stats = await asyncio.to_thread(
            backfill_site,
            site,
            batch_size=request.batch_size,
            limit=request.limit,
            apply=request.apply,
            include_preview_context=request.include_preview_context,
            include_legacy_links=request.include_legacy_links,
            worker_count=request.worker_count,
        )
        return _content_mention_backfill_response(
            stats, apply=request.apply, duration_seconds=time.monotonic() - started_at
        )
    except Exception as exc:
        logger.exception("Content mention backfill failed for site %s", site)
        return ContentMentionBackfillResponse(
            success=False,
            site=site,
            dry_run=not request.apply,
            error=str(exc),
        )


@router.post(
    "/content-mentions/backfill/jobs", response_model=ContentMentionBackfillJobStart
)
async def start_content_mention_backfill_job(
    request: ContentMentionBackfillRequest,
) -> ContentMentionBackfillJobStart:
    """Start a backfill in the background so the WebUI can show live progress."""
    site = request.site.strip().lower()
    if not site:
        raise HTTPException(status_code=422, detail="A site is required")

    job_id = str(uuid4())
    state: dict[str, Any] = {
        "job_id": job_id,
        "status": "queued",
        "phase": "queued",
        "content_rows_scanned": 0,
        "total_content_rows": 0,
        "started_at": time.time(),
        "result": None,
        "error": None,
    }
    _content_mention_backfill_jobs[job_id] = state

    def run() -> None:
        state["status"] = "running"

        def report_progress(phase: str, scanned: int, total: int) -> None:
            state["phase"] = phase
            state["content_rows_scanned"] = scanned
            state["total_content_rows"] = total

        try:
            from scripts.backfill_content_mentions import backfill_site

            stats = backfill_site(
                site,
                batch_size=request.batch_size,
                limit=request.limit,
                apply=request.apply,
                progress_callback=report_progress,
                include_preview_context=request.include_preview_context,
                include_legacy_links=request.include_legacy_links,
                worker_count=request.worker_count,
            )
            state["result"] = _content_mention_backfill_response(
                stats,
                apply=request.apply,
                duration_seconds=time.time() - state["started_at"],
            )
            state["stats"] = stats
            state["apply"] = request.apply
            state["duration_seconds"] = time.time() - state["started_at"]
            state["status"] = "complete"
            state["phase"] = "complete"
        except Exception as exc:
            logger.exception("Content mention backfill job failed for site %s", site)
            state["status"] = "failed"
            state["error"] = str(exc)

    spawn_background_task(asyncio.to_thread(run))
    return ContentMentionBackfillJobStart(job_id=job_id)


@router.get(
    "/content-mentions/backfill/jobs/{job_id}",
    response_model=ContentMentionBackfillJobStatus,
)
async def get_content_mention_backfill_job(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    platform_scope: str = Query("all", pattern="^(all|same|cross|excluded)$"),
    multiple_taggers_only: bool = Query(False),
    search: str = Query("", max_length=512),
) -> ContentMentionBackfillJobStatus:
    state = _content_mention_backfill_jobs.get(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    payload = dict(state)
    if state.get("status") == "complete" and state.get("stats") is not None:
        payload["result"] = _content_mention_backfill_response(
            state["stats"],
            apply=bool(state["apply"]),
            duration_seconds=state.get("duration_seconds"),
            page=page,
            page_size=page_size,
            platform_scope=platform_scope,
            multiple_taggers_only=multiple_taggers_only,
            search=search,
        )
    return ContentMentionBackfillJobStatus(**payload)


ContentModelType: TypeAlias = (
    type[PostModel] | type[StoryModel] | type[MessageModel] | type[MassMessageModel]
)


class FilePathPreviewRecord(Protocol):
    preview: bool


class MediaRecord(Protocol):
    id: int
    category: str | None
    url: str | None
    preview: bool
    duration: int | None
    filepaths: Sequence[FilePathPreviewRecord]


@dataclass(frozen=True)
class ProjectedContentRow:
    site: str
    id: int
    user_id: int
    content_type: str
    created_at: datetime | None
    price: float
    media_count: int
    expires_at: datetime | None


def _model_has_attr(model: ContentModelType, name: str) -> bool:
    return hasattr(cast(Any, model), name)


def _model_attr(model: ContentModelType, name: str) -> Any:
    return getattr(cast(Any, model), name)


def _coerce_int(value: object, default: int = 0) -> int:
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return default


def _coerce_float(value: object, default: float = 0.0) -> float:
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return default


def _coerce_datetime(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _coerce_row_mapping(row: object) -> Mapping[str, object]:
    mapping = getattr(row, "_mapping", row)
    if isinstance(mapping, Mapping):
        return cast(Mapping[str, object], mapping)
    raise TypeError("Unexpected row mapping")


def _detail_media(detail_obj: object | None) -> list[MediaRecord]:
    if detail_obj is None:
        return []
    media = getattr(detail_obj, "media", None)
    if media is None:
        return []
    return list(cast(Sequence[MediaRecord], media))


def _display_media_category(media: MediaRecord) -> str:
    """Return a usable media category for legacy rows without one.

    Older archive rows commonly have a null/``unknown`` category even though
    the CDN URL contains a reliable file extension.  The content card needs a
    real category to avoid rendering JPEGs with a video play overlay.
    """
    category = str(getattr(media, "category", "") or "").strip().lower()
    if category and category not in {"unknown", "none", "null"}:
        if category in {"photo", "photos", "image", "images"}:
            return "image"
        if category in {"stream", "streams", "video", "videos"}:
            return "video"
        return category

    url = str(getattr(media, "url", "") or "").split("?", 1)[0].lower()
    if url.endswith((".jpg", ".jpeg", ".png", ".webp", ".avif")):
        return "image"
    if url.endswith(".gif"):
        return "gif"
    if url.endswith((".mp4", ".m3u8", ".mpd", ".webm", ".mov")):
        return "video"
    if url.endswith((".mp3", ".m4a", ".aac", ".ogg", ".wav")):
        return "audio"
    return "unknown"


def _detail_text(detail_obj: object | None) -> str | None:
    if detail_obj is None:
        return None
    text = getattr(detail_obj, "text", None)
    return text if isinstance(text, str) else None


def _detail_price(detail_obj: object | None) -> float | None:
    if detail_obj is None:
        return None
    return _coerce_float(getattr(detail_obj, "price", None), 0.0)


def _detail_bool(detail_obj: object | None, attr: str) -> bool:
    if detail_obj is None:
        return False
    return bool(getattr(detail_obj, attr, False))


def _detail_optional_bool(detail_obj: object | None, attr: str) -> bool | None:
    if detail_obj is None:
        return None
    value = getattr(detail_obj, attr, None)
    return bool(value) if value is not None else None


def _detail_int(detail_obj: object | None, attr: str) -> int | None:
    if detail_obj is None:
        return None
    value = getattr(detail_obj, attr, None)
    return _coerce_int(value) if value is not None else None


def _detail_datetime(detail_obj: object | None, attr: str) -> datetime | None:
    if detail_obj is None:
        return None
    return _coerce_datetime(getattr(detail_obj, attr, None))


def _isoformat_or_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class UserBrowserItem(BaseModel):
    """User item for database browser."""

    id: int
    username: str | None = None
    name: str | None = None
    active: bool = True
    performer: bool = False
    favorite: bool = False
    post_count: int = 0
    media_count: int = 0
    image_count: int = 0
    video_count: int = 0
    price: float = 0.0
    size: int = 0
    avatar: str | None = None
    header: str | None = None
    downloaded_at: str | None = None
    last_checked_at: str | None = None
    join_date: str | None = None
    created_at: str | None = None
    site: str


class UserBrowserResponse(BaseModel):
    """Response for user browser."""

    success: bool
    users: List[UserBrowserItem] = []
    total: int = 0
    page: int = 1
    page_size: int = 50
    error: str | None = None


class ContentItem(BaseModel):
    """Content item for database browser."""

    id: int
    user_id: int
    content_type: str  # post, message, story, etc.
    text: str | None = None
    created_at: str | None = None
    media_count: int = 0
    site: str


class ContentBrowserResponse(BaseModel):
    """Response for content browser."""

    success: bool
    content: List[ContentItem] = []
    total: int = 0
    page: int = 1
    page_size: int = 50
    error: str | None = None


class MediaItem(BaseModel):
    """Media item for database browser."""

    id: int
    post_id: int | None = None
    user_id: int | None = None
    media_type: str | None = None
    filename: str | None = None
    size: int = 0
    downloaded: bool = False
    drm: bool = False
    created_at: str | None = None
    site: str


class MediaBrowserResponse(BaseModel):
    """Response for media browser."""

    success: bool
    media: List[MediaItem] = []
    total: int = 0
    page: int = 1
    page_size: int = 50
    error: str | None = None


class ArchivePurchaseFact(BaseModel):
    external_id: str
    acquisition_type: str
    amount_usd: float | None = None
    value_status: str
    occurred_at: str | None = None
    content_type: str | None = None
    content_id: int | None = None


class ArchiveMetricItem(BaseModel):
    site: str
    user_id: int
    username: str | None = None
    available: bool | None = None
    downloaded_at: str | None = None
    card_media_id: int | None = None
    current_subscription_price_usd: float | None = None
    feed_post_count: int | None = None
    ppv_post_count: int | None = None
    ppv_message_count: int | None = None
    source_media_count: int | None = None
    downloaded_media_count: int
    source_bytes: int | None = None
    downloaded_bytes: int
    media_types: dict[str, int]
    completeness: dict[str, object]
    purchase_facts: list[ArchivePurchaseFact]
    purchase_facts_complete: bool
    purchase_facts_next_cursor: int | None = None
    last_content_at: str | None = None
    refreshed_at: str | None = None


class ArchiveMetricsResponse(BaseModel):
    items: list[ArchiveMetricItem]
    next_cursor: str | None = None
    has_more: bool = False


class ArchivePurchaseFactsResponse(BaseModel):
    facts: list[ArchivePurchaseFact]
    next_cursor: int | None = None
    has_more: bool = False


def _parse_archive_metrics_cursor(
    site: str | None, cursor: str | None
) -> tuple[str, int]:
    supported_sites = ("onlyfans", "fansly")
    resolved_site = site.lower() if site else supported_sites[0]
    if resolved_site not in supported_sites:
        raise ValueError("Unsupported site")
    if not cursor:
        return resolved_site, 0
    cursor_site, separator, cursor_id = cursor.partition(":")
    if (
        separator != ":"
        or cursor_site not in supported_sites
        or not cursor_id.isdigit()
    ):
        raise ValueError("Invalid archive metrics cursor")
    if site and cursor_site != resolved_site:
        raise ValueError("Cursor does not match site filter")
    return cursor_site, int(cursor_id)


def _archive_completeness(
    source_media_count: int | None, downloaded_media_count: int
) -> dict[str, object]:
    if source_media_count is None:
        return {"status": "unknown", "ratio": None}
    if source_media_count == 0:
        return {"status": "complete", "ratio": 1.0}
    ratio = min(1.0, downloaded_media_count / source_media_count)
    return {
        "status": (
            "complete" if downloaded_media_count >= source_media_count else "partial"
        ),
        "ratio": ratio,
    }


def _archive_purchase_fact(
    site_name: str,
    bought: BoughtContentModel,
    post_prices: dict[int, float | None],
    message_prices: dict[int, float | None],
) -> ArchivePurchaseFact:
    if bought.post_id is not None:
        content_type, content_id = "post", int(bought.post_id)
        amount = post_prices.get(content_id)
    else:
        content_type = "message"
        content_id = int(bought.message_id) if bought.message_id is not None else None
        amount = message_prices.get(content_id) if content_id is not None else None
    return ArchivePurchaseFact(
        external_id=f"{site_name}:bought_content:{bought.id}",
        acquisition_type="ppv",
        amount_usd=amount,
        value_status="actual" if amount is not None else "unknown",
        occurred_at=(bought.created_at.isoformat() if bought.created_at else None),
        content_type=content_type,
        content_id=content_id,
    )


def _archive_purchase_page_state(
    site_name: str,
    facts: list[ArchivePurchaseFact],
    *,
    complete: bool,
    next_cursor: int | None,
) -> tuple[bool, int | None]:
    """Keep incomplete fact pages resumable even if an aggregate omits its cursor."""

    if complete:
        return True, None
    if next_cursor is not None:
        return False, next_cursor
    prefix = f"{site_name}:bought_content:"
    for fact in reversed(facts):
        if fact.external_id.startswith(prefix):
            suffix = fact.external_id.removeprefix(prefix)
            if suffix.isdigit():
                return False, int(suffix)
    return False, None


class DatabaseStats(BaseModel):
    """Database statistics."""

    site: str
    user_count: int = 0
    post_count: int = 0
    media_count: int = 0
    downloaded_media: int = 0
    total_size_bytes: int = 0
    alias_count: int = 0
    filepath_count: int = 0
    remote_url_count: int = 0
    bought_content_count: int = 0
    histo_user_info_count: int = 0
    linked_post_count: int = 0
    linked_user_count: int = 0


@router.get("/stats", response_model=List[DatabaseStats])
def get_database_stats() -> List[DatabaseStats]:
    """Get database statistics for all sites."""
    try:
        db_manager = MultiSiteDatabaseManager()
        stats: List[DatabaseStats] = []

        for site_name in ["onlyfans", "fansly"]:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue

            session = site_db.get_session()
            try:
                from sqlmodel import func

                # Count users
                user_count = session.exec(
                    select(func.count()).select_from(UserModel)
                ).one()

                # Count posts
                post_count = session.exec(
                    select(func.count()).select_from(PostModel)
                ).one()

                # Count media
                media_count = session.exec(
                    select(func.count()).select_from(MediaModel)
                ).one()
                downloaded = session.exec(
                    select(func.count())
                    .select_from(MediaModel)
                    .where(MediaModel.downloaded.is_(True))
                ).one()
                total_size = session.exec(select(func.sum(MediaModel.size))).one() or 0

                # Counts for new tables (safe — tables may not exist in old DBs)
                def _safe_count(model: type[object]) -> int:
                    try:
                        return int(
                            session.exec(select(func.count()).select_from(model)).one()
                            or 0
                        )
                    except Exception:
                        return 0

                stats.append(
                    DatabaseStats(
                        site=site_name,
                        user_count=user_count,
                        post_count=post_count,
                        media_count=media_count,
                        downloaded_media=downloaded,
                        total_size_bytes=total_size,
                        alias_count=_safe_count(UserAliasModel),
                        filepath_count=_safe_count(FilePathModel),
                        remote_url_count=_safe_count(RemoteURLModel),
                        bought_content_count=_safe_count(BoughtContentModel),
                        histo_user_info_count=_safe_count(HistoUserInfoModel),
                        linked_post_count=_safe_count(LinkedPostModel),
                        linked_user_count=_safe_count(ContentMentionModel),
                    )
                )
            finally:
                session.close()

        return stats

    except Exception as e:
        logger.exception("Failed to get database stats")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/archive-metrics", response_model=ArchiveMetricsResponse)
def archive_metrics(
    site: str | None = Query(None, description="Optional site cursor scope"),
    cursor: str | None = Query(
        None, description="Opaque site:user cursor from the previous page"
    ),
    limit: int = Query(100, ge=1, le=1000),
    purchase_fact_limit: int = Query(500, ge=1, le=2000),
    downloaded_only: bool = Query(
        False,
        description="Only return performers with an authoritative downloaded-at date",
    ),
) -> ArchiveMetricsResponse:
    """Return generic archive and acquisition source facts for downstream readers.

    The endpoint returns source facts only. Unknown source totals stay null and
    are never rewritten as zero. The stable ``site:user_id`` cursor makes
    ingestion resumable.
    """

    supported_sites = ("onlyfans", "fansly")
    try:
        start_site, start_user_id = _parse_archive_metrics_cursor(site, cursor)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    sites = (
        (start_site,) if site else supported_sites[supported_sites.index(start_site) :]
    )
    remaining = int(limit)
    items: list[ArchiveMetricItem] = []
    next_cursor: str | None = None
    has_more = False
    db_manager = MultiSiteDatabaseManager()

    for site_name in sites:
        if remaining <= 0:
            has_more = True
            break
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            continue
        session = site_db.get_session()
        try:
            after_id = start_user_id if site_name == start_site else 0
            user_query = (
                select(UserModel, UserInfoModel)
                .outerjoin(UserInfoModel, UserModel.id == UserInfoModel.user_id)
                .where(
                    UserModel.id > after_id,
                    UserModel.performer.is_(True),
                )
                .order_by(UserModel.id.asc())
                .limit(remaining + 1)
            )
            if downloaded_only:
                user_query = user_query.where(
                    or_(
                        UserModel.downloaded_at.is_not(None),
                        UserInfoModel.downloaded_at.is_not(None),
                    )
                )
            user_rows = session.exec(user_query).all()
            page_rows = user_rows[:remaining]
            if len(user_rows) > remaining:
                has_more = True
            user_ids = [user.id for user, _info in page_rows]
            card_media_by_user: dict[int, int] = {}
            if user_ids:
                card_media_rows = session.exec(
                    select(
                        MediaModel.user_id,
                        MediaModel.id,
                        FilePathModel.filepath,
                    )
                    .join(FilePathModel, FilePathModel.media_id == MediaModel.id)
                    .where(
                        MediaModel.user_id.in_(user_ids),
                        MediaModel.downloaded.is_(True),
                        FilePathModel.downloaded.is_(True),
                        sa_func.lower(MediaModel.category).in_(
                            ("photo", "image", "images", "gif")
                        ),
                    )
                    .order_by(MediaModel.user_id, MediaModel.id.desc())
                )
                for user_id, media_id, filepath in card_media_rows:
                    normalized_user_id = int(user_id)
                    if normalized_user_id in card_media_by_user:
                        continue
                    if Path(str(filepath)).is_file():
                        card_media_by_user[normalized_user_id] = int(media_id)
            media_by_user: dict[int, dict[str, object]] = {}
            if user_ids:
                media_rows = session.exec(
                    select(
                        MediaModel.user_id,
                        MediaModel.category,
                        sa_func.count(MediaModel.id),
                        sa_func.sum(
                            case((MediaModel.downloaded.is_(True), 1), else_=0)
                        ),
                        sa_func.sum(
                            case(
                                (
                                    MediaModel.downloaded.is_(True),
                                    sa_func.coalesce(MediaModel.size, 0),
                                ),
                                else_=0,
                            )
                        ),
                        sa_func.max(MediaModel.created_at),
                    )
                    .where(MediaModel.user_id.in_(user_ids))
                    .group_by(MediaModel.user_id, MediaModel.category)
                ).all()
                for (
                    user_id,
                    category,
                    media_count,
                    downloaded_count,
                    downloaded_bytes,
                    last_content_at,
                ) in media_rows:
                    bucket = media_by_user.setdefault(
                        int(user_id),
                        {
                            "downloaded_count": 0,
                            "downloaded_bytes": 0,
                            "types": {},
                            "last_content_at": None,
                        },
                    )
                    bucket["downloaded_count"] = int(bucket["downloaded_count"]) + int(
                        downloaded_count or 0
                    )
                    bucket["downloaded_bytes"] = int(bucket["downloaded_bytes"]) + int(
                        downloaded_bytes or 0
                    )
                    media_types = cast(dict[str, int], bucket["types"])
                    media_key = str(category or "unknown").lower()
                    media_types[media_key] = media_types.get(media_key, 0) + int(
                        media_count or 0
                    )
                    prior_date = bucket["last_content_at"]
                    if last_content_at and (
                        prior_date is None or last_content_at > prior_date
                    ):
                        bucket["last_content_at"] = last_content_at

            post_rows = (
                session.exec(
                    select(
                        PostModel.user_id,
                        sa_func.count(PostModel.id),
                        sa_func.sum(case((PostModel.price > 0, 1), else_=0)),
                    )
                    .where(PostModel.user_id.in_(user_ids))
                    .group_by(PostModel.user_id)
                ).all()
                if user_ids
                else []
            )
            message_rows = (
                session.exec(
                    select(
                        MessageModel.user_id,
                        sa_func.sum(case((MessageModel.price > 0, 1), else_=0)),
                    )
                    .where(MessageModel.user_id.in_(user_ids))
                    .group_by(MessageModel.user_id)
                ).all()
                if user_ids
                else []
            )
            post_counts: dict[int, list[int]] = {
                user_id: [0, 0] for user_id in user_ids
            }
            message_ppv_counts: dict[int, int] = {user_id: 0 for user_id in user_ids}
            for user_id, post_count, ppv_count in post_rows:
                post_counts[int(user_id)] = [
                    int(post_count or 0),
                    int(ppv_count or 0),
                ]
            for user_id, ppv_count in message_rows:
                message_ppv_counts[int(user_id)] = int(ppv_count or 0)

            purchases_by_supplier: dict[int, list[ArchivePurchaseFact]] = {
                user_id: [] for user_id in user_ids
            }
            purchase_complete: dict[int, bool] = {user_id: True for user_id in user_ids}
            purchase_next_cursor: dict[int, int | None] = {
                user_id: None for user_id in user_ids
            }
            if user_ids:
                ranked_bought = (
                    select(
                        BoughtContentModel.id.label("bought_id"),
                        sa_func.row_number()
                        .over(
                            partition_by=BoughtContentModel.supplier_id,
                            order_by=BoughtContentModel.id.asc(),
                        )
                        .label("supplier_position"),
                    )
                    .where(BoughtContentModel.supplier_id.in_(user_ids))
                    .subquery()
                )
                bought_with_position = session.exec(
                    select(
                        BoughtContentModel,
                        ranked_bought.c.supplier_position,
                    )
                    .join(
                        ranked_bought,
                        ranked_bought.c.bought_id == BoughtContentModel.id,
                    )
                    .where(ranked_bought.c.supplier_position <= purchase_fact_limit + 1)
                    .order_by(
                        BoughtContentModel.supplier_id.asc(),
                        BoughtContentModel.id.asc(),
                    )
                ).all()
            else:
                bought_with_position = []
            bought_rows = [row for row, _position in bought_with_position]
            bought_post_ids = {
                int(row.post_id) for row in bought_rows if row.post_id is not None
            }
            bought_message_ids = {
                int(row.message_id) for row in bought_rows if row.message_id is not None
            }
            post_prices = {
                int(content_id): (float(price) if price is not None else None)
                for content_id, price in (
                    session.exec(
                        select(PostModel.id, PostModel.price).where(
                            PostModel.id.in_(bought_post_ids)
                        )
                    ).all()
                    if bought_post_ids
                    else []
                )
            }
            message_prices = {
                int(content_id): (float(price) if price is not None else None)
                for content_id, price in (
                    session.exec(
                        select(MessageModel.id, MessageModel.price).where(
                            MessageModel.id.in_(bought_message_ids)
                        )
                    ).all()
                    if bought_message_ids
                    else []
                )
            }
            for bought, supplier_position in bought_with_position:
                supplier_id = int(bought.supplier_id)
                facts = purchases_by_supplier[supplier_id]
                if int(supplier_position) > purchase_fact_limit:
                    purchase_complete[supplier_id] = False
                    continue
                facts.append(
                    _archive_purchase_fact(
                        site_name,
                        bought,
                        post_prices,
                        message_prices,
                    )
                )
                purchase_next_cursor[supplier_id] = int(bought.id)

            for user, info in page_rows:
                media = media_by_user.get(
                    user.id,
                    {
                        "downloaded_count": 0,
                        "downloaded_bytes": 0,
                        "types": {},
                        "last_content_at": None,
                    },
                )
                source_count = (
                    int(info.media_count)
                    if info is not None and info.media_count is not None
                    else None
                )
                downloaded_count = int(media["downloaded_count"])
                completeness = _archive_completeness(source_count, downloaded_count)
                downloaded_at = user.downloaded_at or (
                    info.downloaded_at if info else None
                )
                refreshed_at = user.last_checked_at or user.updated_at or downloaded_at
                purchase_facts = purchases_by_supplier.get(user.id, [])
                facts_complete, facts_next_cursor = _archive_purchase_page_state(
                    site_name,
                    purchase_facts,
                    complete=purchase_complete.get(user.id, True),
                    next_cursor=purchase_next_cursor.get(user.id),
                )
                items.append(
                    ArchiveMetricItem(
                        site=site_name,
                        user_id=user.id,
                        username=user.username,
                        available=user.active,
                        downloaded_at=(
                            downloaded_at.isoformat() if downloaded_at else None
                        ),
                        card_media_id=card_media_by_user.get(user.id),
                        current_subscription_price_usd=(
                            float(info.price)
                            if info is not None and info.price is not None
                            else None
                        ),
                        feed_post_count=max(
                            0,
                            post_counts.get(user.id, [0, 0])[0]
                            - post_counts.get(user.id, [0, 0])[1],
                        ),
                        ppv_post_count=post_counts.get(user.id, [0, 0])[1],
                        ppv_message_count=message_ppv_counts.get(user.id, 0),
                        source_media_count=source_count,
                        downloaded_media_count=downloaded_count,
                        source_bytes=(
                            int(info.size)
                            if info is not None and info.size is not None
                            else None
                        ),
                        downloaded_bytes=int(media["downloaded_bytes"]),
                        media_types=cast(dict[str, int], media["types"]),
                        completeness=completeness,
                        purchase_facts=purchase_facts,
                        purchase_facts_complete=facts_complete,
                        purchase_facts_next_cursor=facts_next_cursor,
                        last_content_at=(
                            media["last_content_at"].isoformat()
                            if media["last_content_at"]
                            else None
                        ),
                        refreshed_at=refreshed_at.isoformat() if refreshed_at else None,
                    )
                )
                next_cursor = f"{site_name}:{user.id}"
            remaining -= len(page_rows)
            if has_more:
                break
        finally:
            session.close()

    if not site and not has_more and next_cursor:
        last_site = next_cursor.partition(":")[0]
        has_more = supported_sites.index(last_site) < len(supported_sites) - 1
    return ArchiveMetricsResponse(
        items=items, next_cursor=next_cursor if has_more else None, has_more=has_more
    )


@router.get(
    "/archive-metrics/purchase-facts",
    response_model=ArchivePurchaseFactsResponse,
)
def archive_purchase_facts(
    site: str = Query(..., description="Archive site name"),
    user_id: int = Query(..., gt=0),
    cursor: int = Query(0, ge=0),
    limit: int = Query(2000, ge=1, le=10000),
) -> ArchivePurchaseFactsResponse:
    """Continue generic acquisition facts for one purchase-heavy performer."""

    site_name = site.lower()
    if site_name not in {"onlyfans", "fansly"}:
        raise HTTPException(status_code=422, detail="Unsupported site")
    site_db = MultiSiteDatabaseManager().get_or_create_manager(site_name)
    if not site_db:
        raise HTTPException(status_code=404, detail="Archive site is unavailable")
    session = site_db.get_session()
    try:
        bought_rows = session.exec(
            select(BoughtContentModel)
            .where(
                BoughtContentModel.supplier_id == user_id,
                BoughtContentModel.id > cursor,
            )
            .order_by(BoughtContentModel.id.asc())
            .limit(limit + 1)
        ).all()
        page_rows = bought_rows[:limit]
        post_ids = {int(row.post_id) for row in page_rows if row.post_id is not None}
        message_ids = {
            int(row.message_id) for row in page_rows if row.message_id is not None
        }
        post_prices = {
            int(content_id): (float(price) if price is not None else None)
            for content_id, price in (
                session.exec(
                    select(PostModel.id, PostModel.price).where(
                        PostModel.id.in_(post_ids)
                    )
                ).all()
                if post_ids
                else []
            )
        }
        message_prices = {
            int(content_id): (float(price) if price is not None else None)
            for content_id, price in (
                session.exec(
                    select(MessageModel.id, MessageModel.price).where(
                        MessageModel.id.in_(message_ids)
                    )
                ).all()
                if message_ids
                else []
            )
        }
        facts = [
            _archive_purchase_fact(
                site_name,
                row,
                post_prices,
                message_prices,
            )
            for row in page_rows
        ]
        has_more = len(bought_rows) > limit
        next_cursor = int(page_rows[-1].id) if has_more and page_rows else None
        return ArchivePurchaseFactsResponse(
            facts=facts,
            next_cursor=next_cursor,
            has_more=has_more,
        )
    finally:
        session.close()


@router.get("/users", response_model=UserBrowserResponse)
def browse_users(
    site: str | None = Query(None, description="Filter by site"),
    search: str | None = Query(None, description="Search by username"),
    user_ids: str | None = Query(
        None, description="Filter by comma-separated user IDs"
    ),
    active_only: bool = Query(False, description="Only show active users"),
    performer_only: bool = Query(False, description="Only show performers"),
    order_by: str | None = Query(
        None, description="Sort column (media_count, post_count, id, username)"
    ),
    order_direction: str = Query("asc", description="Sort direction: asc or desc"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
) -> UserBrowserResponse:
    """Browse users in the database."""
    try:
        db_manager = MultiSiteDatabaseManager()
        all_users: List[UserBrowserItem] = []

        site_str = str(site).lower() if site and isinstance(site, str) else None
        sites_to_query = [site_str] if site_str else ["onlyfans", "fansly"]
        single_site = len(sites_to_query) == 1
        total = 0

        search_str = str(search).strip() if search and isinstance(search, str) else None
        user_ids_str = (
            str(user_ids).strip() if user_ids and isinstance(user_ids, str) else None
        )
        order_by_str = (
            str(order_by).strip() if order_by and isinstance(order_by, str) else None
        )
        order_dir_str = (
            str(order_direction).strip().lower()
            if order_direction and isinstance(order_direction, str)
            else "asc"
        )
        performer_only_bool = (
            bool(performer_only) if not hasattr(performer_only, "default") else False
        )
        active_only_bool = (
            bool(active_only) if not hasattr(active_only, "default") else False
        )

        page_int = (
            int(page)
            if (isinstance(page, int) or (isinstance(page, str) and page.isdigit()))
            else 1
        )
        page_size_int = (
            int(page_size)
            if (
                isinstance(page_size, int)
                or (isinstance(page_size, str) and page_size.isdigit())
            )
            else 50
        )

        for site_name in sites_to_query:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue

            session = site_db.get_session()
            try:
                # Query users with info
                query = select(UserModel, UserInfoModel).outerjoin(
                    UserInfoModel, UserModel.id == UserInfoModel.user_id
                )

                # Apply filters
                if search_str:
                    query = query.where(UserModel.username.ilike(f"%{search_str}%"))
                if user_ids_str:
                    parsed_ids = [
                        int(x) for x in user_ids_str.split(",") if x.strip().isdigit()
                    ]
                    if parsed_ids:
                        query = query.where(UserModel.id.in_(parsed_ids))
                if active_only_bool:
                    query = query.where(UserModel.active.is_(True))
                if performer_only_bool:
                    query = query.where(
                        or_(
                            UserModel.performer.is_(True),
                            UserModel.username.isnot(None),
                        )
                    )

                # Get total count via SQL
                count_query = select(sa_func.count(UserModel.id))
                if search_str:
                    count_query = count_query.where(
                        UserModel.username.ilike(f"%{search_str}%")
                    )
                if user_ids_str:
                    parsed_ids = [
                        int(x) for x in user_ids_str.split(",") if x.strip().isdigit()
                    ]
                    if parsed_ids:
                        count_query = count_query.where(UserModel.id.in_(parsed_ids))
                if active_only_bool:
                    count_query = count_query.where(UserModel.active.is_(True))
                if performer_only_bool:
                    count_query = count_query.where(
                        or_(
                            UserModel.performer.is_(True),
                            UserModel.username.isnot(None),
                        )
                    )
                site_total = int(session.exec(count_query).one() or 0)
                total += site_total

                # Determine sort column
                if order_by_str == "media_count":
                    sort_col = sa_func.coalesce(UserInfoModel.media_count, 0)
                elif order_by_str == "post_count":
                    sort_col = sa_func.coalesce(UserInfoModel.post_count, 0)
                elif order_by_str == "image_count":
                    sort_col = sa_func.coalesce(UserInfoModel.image_count, 0)
                elif order_by_str == "video_count":
                    sort_col = sa_func.coalesce(UserInfoModel.video_count, 0)
                elif order_by_str == "price":
                    sort_col = sa_func.coalesce(UserInfoModel.price, 0.0)
                elif order_by_str == "downloaded_at":
                    sort_col = sa_func.coalesce(
                        UserModel.downloaded_at, UserInfoModel.downloaded_at
                    )
                elif order_by_str == "last_checked_at":
                    sort_col = sa_func.coalesce(
                        UserModel.last_checked_at, UserModel.updated_at
                    )
                elif order_by_str == "join_date":
                    sort_col = UserModel.join_date
                elif order_by_str == "created_at":
                    sort_col = UserModel.created_at
                elif order_by_str == "size":
                    sort_col = sa_func.coalesce(UserInfoModel.size, 0)
                elif order_by_str == "id":
                    sort_col = UserModel.id
                else:
                    sort_col = sa_func.coalesce(UserModel.username, "")

                if order_dir_str == "desc":
                    query = query.order_by(sort_col.desc().nulls_last())
                else:
                    query = query.order_by(sort_col.asc().nulls_last())

                if single_site:
                    query = query.offset((page_int - 1) * page_size_int).limit(
                        page_size_int
                    )

                results = session.exec(query).all()

                favourited_ids = favourites_service.user_states(session, site_name, [user for user, _ in results])
                result_uids = [user.id for user, _ in results]
                avatar_map: dict[int, tuple[str | None, str | None]] = {}
                if result_uids:
                    try:
                        latest_sq = (
                            select(
                                UserProfileMediaModel.user_id,
                                UserProfileMediaModel.avatar_url,
                                UserProfileMediaModel.header_url,
                                sa_func.row_number()
                                .over(
                                    partition_by=UserProfileMediaModel.user_id,
                                    order_by=UserProfileMediaModel.created_at.desc(),
                                )
                                .label("rn"),
                            )
                            .where(UserProfileMediaModel.user_id.in_(result_uids))
                            .subquery()
                        )
                        pm_rows = session.exec(
                            select(
                                latest_sq.c.user_id,
                                latest_sq.c.avatar_url,
                                latest_sq.c.header_url,
                            ).where(latest_sq.c.rn == 1)
                        ).all()
                        for uid, a_url, h_url in pm_rows:
                            avatar_map[uid] = (a_url, h_url)
                    except Exception:
                        pass

                for user, info in results:
                    dl_at = user.downloaded_at or (info.downloaded_at if info else None)
                    lc_at = user.last_checked_at or (user.updated_at if user else None)
                    jd = user.join_date
                    ca = user.created_at
                    a_url, h_url = avatar_map.get(user.id, (None, None))
                    all_users.append(
                        UserBrowserItem(
                            id=user.id,
                            username=user.username,
                            name=info.name if info else None,
                            active=user.active,
                            performer=user.performer,
                            favorite=user.id in favourited_ids,
                            post_count=info.post_count if info else 0,
                            media_count=info.media_count if info else 0,
                            image_count=info.image_count if info else 0,
                            video_count=info.video_count if info else 0,
                            price=info.price if info else 0.0,
                            size=info.size if info and info.size else 0,
                            avatar=a_url,
                            header=h_url,
                            downloaded_at=dl_at.isoformat() if dl_at else None,
                            last_checked_at=lc_at.isoformat() if lc_at else None,
                            join_date=jd.isoformat() if jd else None,
                            created_at=ca.isoformat() if ca else None,
                            site=site_name,
                        )
                    )
            finally:
                session.close()

        # For multi-site queries, sort in Python and paginate
        if not single_site:
            all_users.sort(key=lambda u: u.username or "", reverse=False)
            start = (page_int - 1) * page_size_int
            all_users = all_users[start : start + page_size_int]

        return UserBrowserResponse(
            success=True,
            users=all_users,
            total=total,
            page=page_int,
            page_size=page_size_int,
        )

    except Exception as e:
        logger.exception("Failed to browse users")
        return UserBrowserResponse(
            success=False,
            error=str(e),
        )


@router.get("/users/{user_id}/content", response_model=ContentBrowserResponse)
def browse_user_content(
    user_id: int,
    site: str = Query(..., description="Site name"),
    content_type: str | None = Query(None, description="Filter by content type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
) -> ContentBrowserResponse:
    """Browse content for a specific user."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_str = str(site).lower() if site and isinstance(site, str) else "onlyfans"
        site_db = db_manager.get_or_create_manager(site_str)

        if not site_db:
            return ContentBrowserResponse(
                success=False,
                error=f"Site '{site}' not found",
            )

        _CONTENT_MODELS: dict[str, ContentModelType] = {
            "post": PostModel,
            "message": MessageModel,
            "story": StoryModel,
            "mass_message": MassMessageModel,
        }

        content_type_str = (
            str(content_type).strip()
            if content_type and isinstance(content_type, str)
            else None
        )
        page_int = (
            int(page)
            if (isinstance(page, int) or (isinstance(page, str) and page.isdigit()))
            else 1
        )
        page_size_int = (
            int(page_size)
            if (
                isinstance(page_size, int)
                or (isinstance(page_size, str) and page_size.isdigit())
            )
            else 50
        )

        if content_type_str and content_type_str not in _CONTENT_MODELS:
            return ContentBrowserResponse(
                success=False,
                error=f"Unknown content_type '{content_type_str}'. Allowed: {', '.join(_CONTENT_MODELS)}",
            )

        models_to_query = (
            {content_type_str: _CONTENT_MODELS[content_type_str]}
            if content_type_str
            else _CONTENT_MODELS
        )

        content_items: List[ContentItem] = []
        total = 0
        sql_offset = (page_int - 1) * page_size_int

        session = site_db.get_session()
        try:
            for ct_name, model in models_to_query.items():
                base_query = select(model).where(model.user_id == user_id)

                # Count
                count_q = select(sa_func.count()).select_from(base_query.subquery())
                ct_total = int(session.exec(count_q).one() or 0)
                total += ct_total

            # When filtering by content_type, do SQL pagination directly
            if content_type_str:
                model = _CONTENT_MODELS[content_type_str]
                q = (
                    select(model)
                    .where(model.user_id == user_id)
                    .order_by(model.created_at.desc())
                    .offset(sql_offset)
                    .limit(page_size_int)
                )
                rows = session.exec(q).all()
                for row in rows:
                    content_items.append(
                        ContentItem(
                            id=row.id,
                            user_id=user_id,
                            content_type=content_type_str,
                            text=getattr(row, "text", None),
                            created_at=_isoformat_or_none(
                                getattr(row, "created_at", None)
                            ),
                            media_count=getattr(row, "media_count", 0),
                            site=site_str,
                        )
                    )
            else:
                # All content types: SQL UNION ALL with pagination
                _union_parts = []
                for ct_name, model in models_to_query.items():
                    has_text = hasattr(model, "text")
                    has_media_count = hasattr(model, "media_count")
                    _union_parts.append(
                        select(
                            model.id.label("id"),
                            literal(ct_name).label("content_type"),
                            (model.text if has_text else literal(None)).label("text"),
                            model.created_at.label("created_at"),
                            (
                                model.media_count if has_media_count else literal(0)
                            ).label("media_count"),
                        ).where(model.user_id == user_id)
                    )
                combined = union_all(*_union_parts).subquery()
                stmt = (
                    select(
                        combined.c.id,
                        combined.c.content_type,
                        combined.c.text,
                        combined.c.created_at,
                        combined.c.media_count,
                    )
                    .order_by(combined.c.created_at.desc())
                    .offset(sql_offset)
                    .limit(page_size_int)
                )
                rows = session.execute(stmt).mappings().all()
                for m in rows:
                    content_items.append(
                        ContentItem(
                            id=int(m["id"]),
                            user_id=user_id,
                            content_type=str(m["content_type"]),
                            text=m["text"] if m["text"] is not None else None,
                            created_at=_isoformat_or_none(m["created_at"]),
                            media_count=int(m["media_count"] or 0),
                            site=site_str,
                        )
                    )
        finally:
            session.close()

        return ContentBrowserResponse(
            success=True,
            content=content_items,
            total=total,
            page=page_int,
            page_size=page_size_int,
        )

    except Exception as e:
        logger.exception("Failed to browse user content")
        return ContentBrowserResponse(
            success=False,
            error=str(e),
        )


class UserProfileMediaResponse(BaseModel):
    """Response for user profile media (avatar and header)."""

    success: bool
    user_id: int
    avatar: str | None = None
    header: str | None = None
    error: str | None = None


@router.get("/users/{user_id}/profile_media", response_model=UserProfileMediaResponse)
def get_user_profile_media(
    user_id: int,
    site: str = Query("onlyfans", description="Site name"),
) -> UserProfileMediaResponse:
    """Get latest avatar and header URLs for a user."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return UserProfileMediaResponse(
                success=False, user_id=user_id, error=f"Site '{site}' not found"
            )
        session = site_db.get_session()
        try:
            pm = session.exec(
                select(UserProfileMediaModel)
                .where(UserProfileMediaModel.user_id == user_id)
                .order_by(UserProfileMediaModel.created_at.desc())
            ).first()
            return UserProfileMediaResponse(
                success=True,
                user_id=user_id,
                avatar=pm.avatar_url if pm else None,
                header=pm.header_url if pm else None,
            )
        finally:
            session.close()
    except Exception as e:
        logger.exception("Failed to get user profile media")
        return UserProfileMediaResponse(success=False, user_id=user_id, error=str(e))


class MediaInfo(BaseModel):
    """Minimal media metadata for content cards."""

    id: int
    category: str  # image, video, audio, gif
    preview: bool = False
    duration: float | None = None  # Duration in seconds for videos/audio
    thumb_url: str | None = None  # Thumbnail image URL
    fallback_thumb_url: str | None = None  # Remote thumbnail fallback URL
    local_url: str | None = None  # Local file stream URL


class ContentLibraryItem(BaseModel):
    """Rich content item for the content library page."""

    id: int
    user_id: int
    username: str | None = None
    content_type: str  # post, story, message, mass_message
    text: str | None = None
    price: float = 0.0
    paid: bool | None = None
    deleted: bool = False
    archived: bool = False
    media_count: int = 0
    media: list[MediaInfo] = []
    expires_at: str | None = None
    created_at: str | None = None
    site: str
    favorite: bool = False


class ContentLibraryResponse(BaseModel):
    """Response for the content library browser."""

    success: bool
    content: list[ContentLibraryItem] = []
    total: int = 0
    total_is_exact: bool = True
    page: int = 1
    page_size: int = 30
    has_more: bool = False
    error: str | None = None


@router.get("/content", response_model=ContentLibraryResponse)
def browse_all_content(
    site: str | None = Query(None, description="Filter by site"),
    content_type: str | None = Query(
        None, description="Filter by type: post, story, message, mass_message"
    ),
    user_id: int | None = Query(None, description="Filter by user ID"),
    exclude_user_ids: str | None = Query(
        None, description="Comma-separated user IDs to exclude"
    ),
    search: str | None = Query(None, description="Search text content"),
    sort_by: str = Query(
        "created_at",
        description=(
            "Sort field: created_at, price, media_count, expires_at, user_id, id"
        ),
    ),
    order: str = Query("desc", description="Sort order: asc, desc"),
    include_flags: str | None = Query(
        None,
        description=(
            "Comma-separated include flags (+): purchased,locked,free,has_media,"
            "no_media,archived,deleted,expiring,text"
        ),
    ),
    exclude_flags: str | None = Query(
        None,
        description=(
            "Comma-separated exclude flags (-): purchased,locked,free,has_media,"
            "no_media,archived,deleted,expiring,text"
        ),
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(30, ge=1, le=100, description="Items per page"),
    include_total: bool = Query(
        True,
        description="Calculate the full filtered total; disable after the first page",
    ),
) -> ContentLibraryResponse:
    """Browse all content across types with sorting and filtering."""
    try:
        from sqlmodel import func

        db_manager = MultiSiteDatabaseManager()
        sites_to_query = [site.lower()] if site else ["onlyfans", "fansly"]
        single_site_query = len(sites_to_query) == 1
        types_to_query = (
            [content_type.lower()]
            if content_type
            else ["post", "story", "message", "mass_message"]
        )
        valid_sort_keys = {
            "created_at",
            "price",
            "media_count",
            "expires_at",
            "user_id",
            "id",
        }
        sort_key = (
            sort_by.lower() if sort_by.lower() in valid_sort_keys else "created_at"
        )
        descending = order.lower() != "asc"
        offset = (page - 1) * page_size
        trimmed_search = search.strip() if search else ""
        search_pattern = f"%{trimmed_search}%" if trimmed_search else None
        include_total_mode = include_total if isinstance(include_total, bool) else False
        # Count each content table independently below. This keeps exact totals
        # without rebuilding and counting the cross-type pagination UNION.
        calculate_total = include_total_mode
        exclude_user_ids_value = (
            exclude_user_ids if isinstance(exclude_user_ids, str) else ""
        )
        parsed_exclude_user_ids = [
            int(token.strip())
            for token in exclude_user_ids_value.split(",")
            if token.strip().lstrip("-").isdigit()
        ]

        valid_flags = {
            "purchased",
            "locked",
            "free",
            "has_media",
            "no_media",
            "has_photo",
            "photo",
            "has_video",
            "video",
            "has_audio",
            "audio",
            "archived",
            "deleted",
            "expiring",
            "text",
        }
        include_filter_flags = [
            flag
            for flag in (
                token.strip().lower()
                for token in (include_flags or "").split(",")
                if token.strip()
            )
            if flag in valid_flags
        ]
        exclude_filter_flags = [
            flag
            for flag in (
                token.strip().lower()
                for token in (exclude_flags or "").split(",")
                if token.strip()
            )
            if flag in valid_flags and flag not in include_filter_flags
        ]

        model_map: dict[str, tuple[ContentModelType, bool]] = {
            "post": (PostModel, True),
            "story": (StoryModel, False),
            "message": (MessageModel, True),
            "mass_message": (MassMessageModel, True),
        }
        purchased_scope = "purchased" in include_filter_flags and user_id is not None
        if purchased_scope:
            types_to_query = [
                current_type
                for current_type in types_to_query
                if model_map.get(current_type, (None, False))[0]
                in (PostModel, MessageModel)
            ]

        def _supports_bought_content(model: ContentModelType) -> bool:
            return model in (PostModel, MessageModel)

        def _apply_bought_content_scope(stmt: Any, model: ContentModelType) -> Any:
            if user_id is None:
                return stmt
            if model is PostModel:
                return (
                    stmt.select_from(BoughtContentModel)
                    .join(model, BoughtContentModel.post_id == _model_attr(model, "id"))
                    .where(BoughtContentModel.buyer_id == user_id)
                )
            if model is MessageModel:
                return (
                    stmt.select_from(BoughtContentModel)
                    .join(
                        model,
                        BoughtContentModel.message_id == _model_attr(model, "id"),
                    )
                    .where(BoughtContentModel.buyer_id == user_id)
                )
            return stmt.where(literal(False))

        def _purchased_expr(
            model: ContentModelType, buyer_user_id: int | None = None
        ) -> Any:
            link_clause = None
            if model is PostModel:
                link_clause = BoughtContentModel.post_id == _model_attr(model, "id")
            elif model is MessageModel:
                link_clause = BoughtContentModel.message_id == _model_attr(model, "id")
            else:
                return literal(False)

            where_clauses = [link_clause]
            if buyer_user_id is not None:
                where_clauses.append(BoughtContentModel.buyer_id == buyer_user_id)

            return (
                select(literal(1))
                .select_from(BoughtContentModel)
                .where(*where_clauses)
                .exists()
            )

        def _flag_expr(model: ContentModelType, flag: str) -> Any:
            content_media_fk = {
                PostModel: ContentMediaAssoModel.post_id,
                MessageModel: ContentMediaAssoModel.message_id,
                StoryModel: ContentMediaAssoModel.story_id,
                MassMessageModel: ContentMediaAssoModel.mass_message_id,
            }.get(model)

            def _media_exists(category_clause: Any = None) -> Any:
                if content_media_fk is None:
                    return literal(False)
                media_query = (
                    select(literal(1))
                    .select_from(ContentMediaAssoModel)
                    .join(
                        MediaModel,
                        ContentMediaAssoModel.media_id == MediaModel.id,
                    )
                    .where(content_media_fk == _model_attr(model, "id"))
                )
                if category_clause is not None:
                    media_query = media_query.where(category_clause)
                return media_query.exists()

            if flag == "purchased":
                return _purchased_expr(model)

            if flag == "locked":
                if _model_has_attr(model, "price") and _model_has_attr(model, "paid"):
                    return and_(
                        _model_attr(model, "price") > 0,
                        _model_attr(model, "paid").is_not(True),
                    )
                if _model_has_attr(model, "price"):
                    return _model_attr(model, "price") > 0
                return literal(False)

            if flag == "free":
                if _model_has_attr(model, "price"):
                    return _model_attr(model, "price") <= 0
                return literal(True)

            if flag == "has_media":
                return _media_exists()

            if flag == "no_media":
                return ~_media_exists()

            if flag in ("has_photo", "photo"):
                return _media_exists(
                    or_(
                        sa_func.lower(MediaModel.category).like("%photo%"),
                        sa_func.lower(MediaModel.category).like("%image%"),
                        sa_func.lower(MediaModel.url).like("%.jpg%"),
                        sa_func.lower(MediaModel.url).like("%.jpeg%"),
                        sa_func.lower(MediaModel.url).like("%.png%"),
                        sa_func.lower(MediaModel.url).like("%.webp%"),
                    )
                )

            if flag in ("has_video", "video"):
                return _media_exists(
                    or_(
                        sa_func.lower(MediaModel.category).like("%video%"),
                        sa_func.lower(MediaModel.category).like("%stream%"),
                        sa_func.lower(MediaModel.category).like("%mp4%"),
                        sa_func.lower(MediaModel.category).like("%webm%"),
                        sa_func.lower(MediaModel.category).like("%mov%"),
                        sa_func.lower(MediaModel.url).like("%.mp4%"),
                        sa_func.lower(MediaModel.url).like("%.m3u8%"),
                        sa_func.lower(MediaModel.url).like("%.mpd%"),
                        sa_func.lower(MediaModel.url).like("%.webm%"),
                        sa_func.lower(MediaModel.url).like("%.mov%"),
                    )
                )

            if flag in ("has_audio", "audio"):
                return _media_exists(
                    or_(
                        sa_func.lower(MediaModel.category).like("%audio%"),
                        sa_func.lower(MediaModel.url).like("%.mp3%"),
                        sa_func.lower(MediaModel.url).like("%.m4a%"),
                        sa_func.lower(MediaModel.url).like("%.aac%"),
                        sa_func.lower(MediaModel.url).like("%.ogg%"),
                        sa_func.lower(MediaModel.url).like("%.wav%"),
                    )
                )

            if flag == "archived":
                return (
                    _model_attr(model, "archived").is_(True)
                    if _model_has_attr(model, "archived")
                    else literal(False)
                )

            if flag == "deleted":
                return (
                    _model_attr(model, "deleted").is_(True)
                    if _model_has_attr(model, "deleted")
                    else literal(False)
                )

            if flag == "expiring":
                return (
                    _model_attr(model, "expires_at").is_not(None)
                    if _model_has_attr(model, "expires_at")
                    else literal(False)
                )

            if flag == "text":
                if _model_has_attr(model, "text"):
                    return and_(
                        _model_attr(model, "text").is_not(None),
                        _model_attr(model, "text") != "",
                    )
                return literal(False)

            return literal(True)

        def _apply_filters(
            stmt: Any,
            model: ContentModelType,
            can_search: bool,
            *,
            bought_content_scoped: bool = False,
        ) -> Any:
            if user_id is not None:
                if bought_content_scoped:
                    pass
                elif "purchased" in include_filter_flags:
                    stmt = stmt.where(_purchased_expr(model, buyer_user_id=user_id))
                else:
                    stmt = stmt.where(_model_attr(model, "user_id") == user_id)
            if parsed_exclude_user_ids:
                stmt = stmt.where(
                    _model_attr(model, "user_id").notin_(parsed_exclude_user_ids)
                )
            if search_pattern and can_search:
                stmt = stmt.where(_model_attr(model, "text").ilike(search_pattern))

            media_type_flags = {
                "has_photo",
                "photo",
                "has_video",
                "video",
                "has_audio",
                "audio",
            }
            included_media_flags = [
                flag for flag in include_filter_flags if flag in media_type_flags
            ]
            other_include_flags = [
                flag for flag in include_filter_flags if flag not in media_type_flags
            ]

            if included_media_flags:
                media_clauses = [
                    _flag_expr(model, flag) for flag in included_media_flags
                ]
                stmt = stmt.where(or_(*media_clauses))

            for flag in other_include_flags:
                if flag == "purchased" and (
                    user_id is not None or bought_content_scoped
                ):
                    continue
                stmt = stmt.where(_flag_expr(model, flag))
            for flag in exclude_filter_flags:
                if flag == "purchased" and bought_content_scoped:
                    continue
                stmt = stmt.where(~_flag_expr(model, flag))
            return stmt

        def _content_projection(
            model: ContentModelType, current_type: str, can_search: bool
        ) -> Any:
            price_expr = (
                _model_attr(model, "price")
                if _model_has_attr(model, "price")
                else literal(0.0)
            )
            media_count_expr = (
                _model_attr(model, "media_count")
                if _model_has_attr(model, "media_count")
                else literal(0)
            )
            expires_at_expr = (
                _model_attr(model, "expires_at")
                if _model_has_attr(model, "expires_at")
                else literal(None)
            )
            stmt: Any = cast(Any, select)(
                _model_attr(model, "id").label("content_id"),
                _model_attr(model, "user_id").label("user_id"),
                literal(current_type).label("content_type"),
                _model_attr(model, "created_at").label("created_at"),
                price_expr.label("price"),
                media_count_expr.label("media_count"),
                expires_at_expr.label("expires_at"),
            )
            bought_content_scoped = purchased_scope and _supports_bought_content(model)
            if bought_content_scoped:
                stmt = _apply_bought_content_scope(stmt, model)
            return _apply_filters(
                stmt,
                model,
                can_search,
                bought_content_scoped=bought_content_scoped,
            )

        def _content_count_statement(model: ContentModelType, can_search: bool) -> Any:
            """Build an index-friendly exact count for one content table."""

            stmt: Any = cast(Any, select)(func.count(_model_attr(model, "id")))
            bought_content_scoped = purchased_scope and _supports_bought_content(model)
            if bought_content_scoped:
                stmt = _apply_bought_content_scope(stmt, model)
            return _apply_filters(
                stmt,
                model,
                can_search,
                bought_content_scoped=bought_content_scoped,
            )

        def _apply_projection_order(stmt: Any, subquery: Any) -> Any:
            if sort_key == "price":
                primary = subquery.c.price
                secondary = subquery.c.created_at
            elif sort_key == "media_count":
                primary = subquery.c.media_count
                secondary = subquery.c.created_at
            elif sort_key == "expires_at":
                primary = subquery.c.expires_at
                secondary = subquery.c.created_at
            elif sort_key == "user_id":
                primary = subquery.c.user_id
                secondary = subquery.c.created_at
            elif sort_key == "id":
                primary = subquery.c.content_id
                secondary = subquery.c.created_at
            else:
                primary = subquery.c.created_at
                secondary = subquery.c.content_id

            if descending:
                return stmt.order_by(
                    primary.desc(),
                    secondary.desc(),
                    subquery.c.content_id.desc(),
                )
            return stmt.order_by(
                primary.asc(),
                secondary.asc(),
                subquery.c.content_id.asc(),
            )

        def _limit_projection(stmt: Any, limit: int) -> Any:
            """Bound each branch before the cross-type UNION and global sort."""

            subquery = stmt.subquery()
            limited_stmt: Any = cast(Any, select)(
                subquery.c.content_id,
                subquery.c.user_id,
                subquery.c.content_type,
                subquery.c.created_at,
                subquery.c.price,
                subquery.c.media_count,
                subquery.c.expires_at,
            )
            return _apply_projection_order(limited_stmt, subquery).limit(limit)

        def _media_infos(
            media_list: Sequence[MediaRecord], current_site: str
        ) -> list[MediaInfo]:
            items: list[MediaInfo] = []
            for m in media_list:
                items.append(MediaInfo(**media_preview(current_site, m.id, _display_media_category(m), m.preview, m.duration)))
            return items

        def _to_sort_value(item: ProjectedContentRow) -> tuple[float, float, float]:
            row_id = float(item.id)
            user_id_val = float(item.user_id)
            created = item.created_at
            if created is None:
                created_ts = float("-inf")
            elif created.tzinfo is None:
                created_ts = created.replace(tzinfo=timezone.utc).timestamp()
            else:
                created_ts = created.timestamp()
            expires = item.expires_at
            if expires is None:
                expires_ts = float("-inf")
            elif expires.tzinfo is None:
                expires_ts = expires.replace(tzinfo=timezone.utc).timestamp()
            else:
                expires_ts = expires.timestamp()
            media_count_val = float(item.media_count)

            if sort_key == "price":
                price_val = float(item.price)
                return (price_val, created_ts, row_id)
            if sort_key == "media_count":
                return (media_count_val, created_ts, row_id)
            if sort_key == "expires_at":
                return (expires_ts, created_ts, row_id)
            if sort_key == "user_id":
                return (user_id_val, created_ts, row_id)
            if sort_key == "id":
                return (row_id, created_ts, row_id)
            return (created_ts, row_id, row_id)

        def _serialize_page_row(row: object, site_name: str) -> ProjectedContentRow:
            mapping = _coerce_row_mapping(row)
            return ProjectedContentRow(
                site=site_name,
                id=_coerce_int(mapping["content_id"]),
                user_id=_coerce_int(mapping["user_id"]),
                content_type=str(mapping["content_type"]),
                created_at=_coerce_datetime(mapping["created_at"]),
                price=_coerce_float(mapping["price"]),
                media_count=_coerce_int(mapping["media_count"]),
                expires_at=_coerce_datetime(mapping["expires_at"]),
            )

        # Keep sessions open until page-level enrichment (usernames + media) is complete.
        sessions_to_close: list[Session] = []
        site_sessions: dict[str, Session] = {}
        total = 0
        has_more = False
        selected_content: list[ProjectedContentRow] = []
        fetch_limit = page_size + 1 if single_site_query else offset + page_size + 1

        try:
            site_candidates: list[ProjectedContentRow] = []
            for site_name in sites_to_query:
                site_db = db_manager.get_or_create_manager(site_name)
                if not site_db:
                    continue

                session = site_db.get_session()
                sessions_to_close.append(session)
                site_sessions[site_name] = session

                projections: list[Any] = []
                site_total = 0
                for current_type in types_to_query:
                    model_info = model_map.get(current_type)
                    if not model_info:
                        continue
                    model, can_search = model_info
                    # A text search must never turn a non-text content type
                    # into an unfiltered UNION branch. Stories currently have
                    # no searchable text field, so they cannot match a word.
                    if search_pattern and not can_search:
                        continue
                    projection = _content_projection(
                        model,
                        current_type,
                        can_search,
                    )
                    projections.append(projection)
                    if calculate_total:
                        site_total += int(
                            session.exec(
                                _content_count_statement(model, can_search)
                            ).one()
                            or 0
                        )

                if not projections:
                    continue

                if len(projections) > 1:
                    # Global page N can contain rows from any one type, so
                    # retain offset + page_size + 1 from every branch. This is
                    # also important for media filters: without the pushdown,
                    # PostgreSQL must evaluate every matching association in
                    # every content table before the UNION can return one page.
                    projections = [
                        _limit_projection(projection, offset + page_size + 1)
                        for projection in projections
                    ]

                if calculate_total:
                    total += site_total
                    if site_total <= 0:
                        continue

                projection_source: Any = (
                    projections[0].subquery()
                    if len(projections) == 1
                    else union_all(*projections).subquery()
                )

                page_stmt: Any = cast(Any, select)(
                    projection_source.c.content_id,
                    projection_source.c.user_id,
                    projection_source.c.content_type,
                    projection_source.c.created_at,
                    projection_source.c.price,
                    projection_source.c.media_count,
                    projection_source.c.expires_at,
                )
                page_stmt = _apply_projection_order(page_stmt, projection_source)

                if single_site_query:
                    page_stmt = page_stmt.offset(offset).limit(fetch_limit)
                    page_rows = [
                        _serialize_page_row(row, site_name)
                        for row in cast(
                            list[object], cast(Any, session.exec(page_stmt)).all()
                        )
                    ]
                    has_more = len(page_rows) > page_size
                    selected_content = page_rows[:page_size]
                else:
                    page_stmt = page_stmt.limit(fetch_limit)
                    site_candidates.extend(
                        _serialize_page_row(row, site_name)
                        for row in cast(
                            list[object], cast(Any, session.exec(page_stmt)).all()
                        )
                    )

            if not single_site_query and site_candidates:
                site_candidates.sort(key=_to_sort_value, reverse=descending)
                page_candidates = site_candidates[offset : offset + page_size + 1]
                has_more = len(page_candidates) > page_size
                selected_content = page_candidates[:page_size]

            # Username lookup in one query per site.
            username_lookup: dict[tuple[str, int], str | None] = {}
            user_ids_by_site: dict[str, set[int]] = {}
            for item in selected_content:
                user_ids_by_site.setdefault(item.site, set()).add(item.user_id)
            for site_name, user_ids in user_ids_by_site.items():
                if not user_ids:
                    continue
                session = site_sessions.get(site_name)
                if session is None:
                    continue
                users = session.exec(
                    select(UserModel).where(UserModel.id.in_(user_ids))
                ).all()
                for user in users:
                    username_lookup[(site_name, user.id)] = user.username

            # Hydrate media only for paged rows.
            detail_lookup: dict[tuple[str, str, int], object] = {}
            ids_by_source: dict[tuple[str, str], list[int]] = {}
            for item in selected_content:
                ids_by_source.setdefault((item.site, item.content_type), []).append(
                    item.id
                )

            for (site_name, ctype), ids in ids_by_source.items():
                model_info = model_map.get(ctype)
                session = site_sessions.get(site_name)
                if not model_info or session is None:
                    continue
                model = model_info[0]
                stmt = (
                    select(model)
                    .where(_model_attr(model, "id").in_(ids))
                    .options(
                        selectinload(_model_attr(model, "media")).selectinload(
                            MediaModel.filepaths
                        )
                    )
                )
                for obj in session.exec(stmt).all():
                    detail_lookup[(site_name, ctype, int(obj.id))] = obj

            favourite_lookup = set()
            if selected_content:
                favourite_session = next(iter(site_sessions.values()))
                favourites_service.ensure_active(favourite_session)
                keys = [{"site": item.site, "entity_type": item.content_type, "entity_id": item.id}
                        for item in selected_content if item.content_type in favourites_service.TARGETS]
                favourite_lookup = {(item["site"], item["entity_type"], item["entity_id"])
                                    for item in favourites_service.batch_states(favourite_session, keys) if item["favourited"]}
            response_items: list[ContentLibraryItem] = []
            for item in selected_content:
                site_name = item.site
                ctype = item.content_type
                row_id = item.id
                detail_obj = detail_lookup.get((site_name, ctype, row_id))
                media_list = _detail_media(detail_obj)
                media_count = (
                    len(media_list)
                    if media_list
                    else (_detail_int(detail_obj, "media_count") or item.media_count)
                )
                response_items.append(
                    ContentLibraryItem(
                        id=row_id,
                        favorite=(site_name, ctype, row_id) in favourite_lookup,
                        user_id=item.user_id,
                        username=username_lookup.get((site_name, item.user_id)),
                        content_type=ctype,
                        text=_detail_text(detail_obj),
                        price=_detail_price(detail_obj) or item.price,
                        paid=_detail_optional_bool(detail_obj, "paid"),
                        deleted=_detail_bool(detail_obj, "deleted"),
                        archived=_detail_bool(detail_obj, "archived"),
                        media_count=media_count,
                        media=_media_infos(media_list, site_name),
                        expires_at=(
                            _isoformat_or_none(
                                _detail_datetime(detail_obj, "expires_at")
                            )
                            if detail_obj is not None
                            else _isoformat_or_none(item.expires_at)
                        ),
                        created_at=(
                            _isoformat_or_none(
                                _detail_datetime(detail_obj, "created_at")
                            )
                            if detail_obj is not None
                            else _isoformat_or_none(item.created_at)
                        ),
                        site=site_name,
                    )
                )
        finally:
            for session in sessions_to_close:
                session.close()

        if not calculate_total:
            total = len(response_items) if page == 1 else 0
        total_is_exact = calculate_total or (page == 1 and not has_more)

        return ContentLibraryResponse(
            success=True,
            content=response_items,
            total=total,
            total_is_exact=total_is_exact,
            page=page,
            page_size=page_size,
            has_more=has_more,
        )

    except Exception as e:
        logger.exception("Failed to browse content library")
        return ContentLibraryResponse(
            success=False,
            error=str(e),
        )


@router.get("/media", response_model=MediaBrowserResponse)
def browse_media(
    site: str | None = Query(None, description="Filter by site"),
    user_id: int | None = Query(None, description="Filter by user ID"),
    media_type: str | None = Query(
        None, description="Filter by media type (image, video)"
    ),
    downloaded_only: bool = Query(False, description="Only show downloaded media"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
) -> MediaBrowserResponse:
    """Browse media in the database with proper SQL pagination."""
    try:
        from sqlmodel import func

        db_manager = MultiSiteDatabaseManager()
        all_media: List[MediaItem] = []
        total = 0

        def _filename_from_url(url: str | None) -> str | None:
            if not url:
                return None
            name = url.split("/")[-1].split("?")[0].strip()
            return name or None

        sites_to_query = [site.lower()] if site else ["onlyfans", "fansly"]
        offset = (page - 1) * page_size

        for site_name in sites_to_query:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue

            session = site_db.get_session()
            try:
                # Build base query with filters
                base_query = select(MediaModel)
                if user_id is not None:
                    base_query = base_query.where(MediaModel.user_id == user_id)
                if media_type:
                    if hasattr(MediaModel, "category"):
                        base_query = base_query.where(MediaModel.category == media_type)
                if downloaded_only:
                    base_query = base_query.where(MediaModel.downloaded.is_(True))

                # Get total count for this site
                count_query = select(func.count()).select_from(base_query.subquery())
                site_total = session.exec(count_query).one()
                total += site_total

                # Only fetch if we need items from this site
                if len(all_media) < page_size:
                    remaining = page_size - len(all_media)
                    # Calculate offset for this site
                    site_offset = max(0, offset - (total - site_total))

                    query = base_query.offset(site_offset).limit(remaining)
                    media = session.exec(query).all()

                    for m in media:
                        m_type = getattr(m, "media_type", None)
                        if not m_type:
                            m_type = getattr(m, "category", None)
                        filename = getattr(m, "filename", None)
                        if not filename:
                            filename = _filename_from_url(getattr(m, "url", None))

                        all_media.append(
                            MediaItem(
                                id=m.id,
                                post_id=getattr(m, "post_id", None),
                                user_id=m.user_id,
                                media_type=m_type,
                                filename=filename,
                                size=m.size or 0,
                                downloaded=getattr(m, "downloaded", False),
                                drm=bool(getattr(m, "drm", False)),
                                created_at=(
                                    m.created_at.isoformat()
                                    if getattr(m, "created_at", None)
                                    else None
                                ),
                                site=site_name,
                            )
                        )
            finally:
                session.close()

        return MediaBrowserResponse(
            success=True,
            media=all_media,
            total=total,
            page=page,
            page_size=page_size,
        )

    except Exception as e:
        logger.exception("Failed to browse media")
        return MediaBrowserResponse(
            success=False,
            error=str(e),
        )


# ---------------------------------------------------------------------------
# New model endpoints
# ---------------------------------------------------------------------------


class UserAliasItem(BaseModel):
    id: int
    user_id: int
    username: str
    created_at: str | None = None
    site: str


class UserAliasResponse(BaseModel):
    success: bool
    aliases: list[UserAliasItem] = []
    total: int = 0
    error: str | None = None


class UserAliasDeleteResponse(BaseModel):
    success: bool
    alias_id: int | None = None
    error: str | None = None


@router.get("/users/{user_id}/aliases", response_model=UserAliasResponse)
def get_user_aliases(
    user_id: int,
    site: str = Query(..., description="Site name"),
) -> UserAliasResponse:
    """Return all known username aliases for a user."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return UserAliasResponse(success=False, error=f"Site '{site}' not found")

        with site_db.get_session() as session:
            rows = session.exec(
                select(UserAliasModel).where(UserAliasModel.user_id == user_id)
            ).all()
            items = [
                UserAliasItem(
                    id=r.id,
                    user_id=r.user_id,
                    username=r.username,
                    created_at=_isoformat_or_none(r.created_at),
                    site=site.lower(),
                )
                for r in rows
            ]
        return UserAliasResponse(success=True, aliases=items, total=len(items))
    except Exception as e:
        logger.exception("Failed to get user aliases")
        return UserAliasResponse(success=False, error=str(e))


@router.delete(
    "/users/{user_id}/aliases/{alias_id}", response_model=UserAliasDeleteResponse
)
def delete_user_alias(
    user_id: int,
    alias_id: int,
    site: str = Query(..., description="Site name"),
) -> UserAliasDeleteResponse:
    """Remove one stored username alias without deleting the user."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return UserAliasDeleteResponse(
                success=False, error=f"Site '{site}' not found"
            )

        with site_db.get_session() as session:
            alias = session.get(UserAliasModel, alias_id)
            if alias is None or int(alias.user_id) != user_id:
                raise HTTPException(status_code=404, detail="Alias not found")

            session.delete(alias)
            session.commit()

        return UserAliasDeleteResponse(success=True, alias_id=alias_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to delete user alias")
        return UserAliasDeleteResponse(success=False, error=str(e))


class HistoUserInfoItem(BaseModel):
    id: int
    user_id: int
    name: str | None = None
    username: str | None = None
    about: str | None = None
    header: str | None = None
    avatar: str | None = None
    post_count: int = 0
    media_count: int = 0
    image_count: int = 0
    video_count: int = 0
    audio_count: int = 0
    price: float = 0.0
    fans_count: int = 0
    following_count: int = 0
    subscriber_count: int = 0
    archived_post_count: int = 0
    private_archived_post_count: int = 0
    created_at: str | None = None
    site: str


class HistoUserInfoResponse(BaseModel):
    success: bool
    snapshots: list[HistoUserInfoItem] = []
    total: int = 0
    error: str | None = None


@router.get("/users/{user_id}/history", response_model=HistoUserInfoResponse)
async def get_user_history(
    user_id: int,
    site: str = Query(..., description="Site name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
) -> HistoUserInfoResponse:
    """Return historical snapshots of a user's profile info."""

    def read_history() -> HistoUserInfoResponse:
        try:
            db_manager = MultiSiteDatabaseManager()
            site_db = db_manager.get_or_create_manager(site.lower())
            if not site_db:
                return HistoUserInfoResponse(
                    success=False, error=f"Site '{site}' not found"
                )

            with site_db.get_session() as session:
                query = (
                    select(HistoUserInfoModel, UserModel.username)
                    .outerjoin(UserModel, UserModel.id == HistoUserInfoModel.user_id)
                    .where(HistoUserInfoModel.user_id == user_id)
                    .order_by(
                        HistoUserInfoModel.created_at.desc(),
                        HistoUserInfoModel.id.desc(),
                    )
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
                total = session.exec(
                    select(sa_func.count(HistoUserInfoModel.id)).where(
                        HistoUserInfoModel.user_id == user_id
                    )
                ).one()
                page_rows = session.exec(query).all()
                items = [
                    HistoUserInfoItem(
                        id=r.id,
                        user_id=r.user_id,
                        name=r.name,
                        username=username,
                        about=r.description,
                        header=None,
                        avatar=None,
                        post_count=r.post_count,
                        media_count=r.media_count,
                        image_count=r.image_count,
                        video_count=r.video_count,
                        audio_count=r.audio_count,
                        price=r.price,
                        fans_count=0,
                        following_count=0,
                        subscriber_count=r.subscribers_count,
                        archived_post_count=r.archived_post_count,
                        private_archived_post_count=r.private_archived_post_count,
                        created_at=_isoformat_or_none(getattr(r, "created_at", None)),
                        site=site.lower(),
                    )
                    for r, username in page_rows
                ]
            return HistoUserInfoResponse(success=True, snapshots=items, total=total)
        except Exception as e:
            logger.exception("Failed to get user history")
            return HistoUserInfoResponse(success=False, error=str(e))

    return await run_database_call(read_history)


class LinkedPostItem(BaseModel):
    id: int
    post_id: int
    linked_post_id: int
    created_at: str | None = None
    site: str


class LinkedUserItem(BaseModel):
    id: int
    post_id: int
    linked_user_id: int | None
    raw_mention: str | None = None
    mention_kind: str = "platform_tag"
    created_at: str | None = None
    site: str


class LinkedDataResponse(BaseModel):
    success: bool
    linked_posts: list[LinkedPostItem] = []
    linked_users: list[LinkedUserItem] = []
    error: str | None = None


@router.get("/posts/{post_id}/linked", response_model=LinkedDataResponse)
def get_post_linked_data(
    post_id: int,
    site: str = Query(..., description="Site name"),
) -> LinkedDataResponse:
    """Return linked posts and @mentioned users for a post."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return LinkedDataResponse(success=False, error=f"Site '{site}' not found")

        with site_db.get_session() as session:
            lp_rows = session.exec(
                select(LinkedPostModel).where(LinkedPostModel.post_id == post_id)
            ).all()
            lu_rows = session.exec(
                select(ContentMentionModel).where(
                    ContentMentionModel.post_id == post_id
                )
            ).all()

        return LinkedDataResponse(
            success=True,
            linked_posts=[
                LinkedPostItem(
                    id=r.id,
                    post_id=r.post_id,
                    linked_post_id=r.linked_post_id,
                    created_at=_isoformat_or_none(getattr(r, "created_at", None)),
                    site=site.lower(),
                )
                for r in lp_rows
            ],
            linked_users=[
                LinkedUserItem(
                    id=r.id,
                    post_id=r.post_id,
                    linked_user_id=r.mentioned_user_id,
                    raw_mention=r.raw_mention,
                    mention_kind=r.mention_kind,
                    created_at=_isoformat_or_none(getattr(r, "created_at", None)),
                    site=site.lower(),
                )
                for r in lu_rows
            ],
        )
    except Exception as e:
        logger.exception("Failed to get linked data")
        return LinkedDataResponse(success=False, error=str(e))


class FilePathItem(BaseModel):
    id: int
    media_id: int
    filepath: str
    preview: bool = False
    downloaded: bool = False
    created_at: str | None = None
    site: str


class FilePathResponse(BaseModel):
    success: bool
    filepaths: list[FilePathItem] = []
    total: int = 0
    error: str | None = None


@router.get("/media/{media_id}/filepaths", response_model=FilePathResponse)
def get_media_filepaths(
    media_id: int,
    site: str = Query(..., description="Site name"),
) -> FilePathResponse:
    """Return local filepaths associated with a media item."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return FilePathResponse(success=False, error=f"Site '{site}' not found")

        with site_db.get_session() as session:
            rows = session.exec(
                select(FilePathModel).where(FilePathModel.media_id == media_id)
            ).all()
            items = [
                FilePathItem(
                    id=r.id,
                    media_id=r.media_id,
                    filepath=r.filepath,
                    preview=r.preview,
                    downloaded=r.downloaded,
                    created_at=r.created_at.isoformat() if r.created_at else None,
                    site=site.lower(),
                )
                for r in rows
            ]
        return FilePathResponse(success=True, filepaths=items, total=len(items))
    except Exception as e:
        logger.exception("Failed to get media filepaths")
        return FilePathResponse(success=False, error=str(e))


class BoughtContentItem(BaseModel):
    id: int
    supplier_id: int
    buyer_id: int
    post_id: int | None = None
    message_id: int | None = None
    created_at: str | None = None
    site: str


class BoughtContentResponse(BaseModel):
    success: bool
    purchases: list[BoughtContentItem] = []
    total: int = 0
    page: int = 1
    page_size: int = 50
    error: str | None = None


@router.get("/users/{user_id}/purchases", response_model=BoughtContentResponse)
def get_user_purchases(
    user_id: int,
    site: str = Query(..., description="Site name"),
    role: str = Query("buyer", description="Role of user_id: 'buyer' or 'supplier'"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
) -> BoughtContentResponse:
    """Return bought-content records for a user (as buyer or supplier)."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return BoughtContentResponse(
                success=False, error=f"Site '{site}' not found"
            )

        with site_db.get_session() as session:
            col = (
                BoughtContentModel.buyer_id
                if role == "buyer"
                else BoughtContentModel.supplier_id
            )
            base = select(BoughtContentModel).where(col == user_id)

            total = int(
                session.exec(select(sa_func.count()).select_from(base.subquery())).one()
                or 0
            )

            sql_offset = (page - 1) * page_size
            rows = session.exec(
                base.order_by(BoughtContentModel.created_at.desc())
                .offset(sql_offset)
                .limit(page_size)
            ).all()
            items = [
                BoughtContentItem(
                    id=r.id,
                    supplier_id=r.supplier_id,
                    buyer_id=r.buyer_id,
                    post_id=r.post_id,
                    message_id=r.message_id,
                    created_at=r.created_at.isoformat() if r.created_at else None,
                    site=site.lower(),
                )
                for r in rows
            ]
        return BoughtContentResponse(
            success=True,
            purchases=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        logger.exception("Failed to get user purchases")
        return BoughtContentResponse(success=False, error=str(e))


class RemoteURLItem(BaseModel):
    id: int
    user_id: int
    url: str | None = None
    uploaded_at: str | None = None
    downloaded_at: str | None = None
    created_at: str | None = None
    site: str


class RemoteURLResponse(BaseModel):
    success: bool
    urls: list[RemoteURLItem] = []
    total: int = 0
    page: int = 1
    page_size: int = 50
    error: str | None = None


@router.get("/users/{user_id}/remote-urls", response_model=RemoteURLResponse)
def get_user_remote_urls(
    user_id: int,
    site: str = Query(..., description="Site name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
) -> RemoteURLResponse:
    """Return remote URLs tracked for a user."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())
        if not site_db:
            return RemoteURLResponse(success=False, error=f"Site '{site}' not found")

        with site_db.get_session() as session:
            from sqlmodel import func

            total = session.exec(
                select(func.count())
                .select_from(RemoteURLModel)
                .where(RemoteURLModel.user_id == user_id)
            ).one()

            offset = (page - 1) * page_size
            rows = session.exec(
                select(RemoteURLModel)
                .where(RemoteURLModel.user_id == user_id)
                .order_by(RemoteURLModel.created_at.desc())
                .offset(offset)
                .limit(page_size)
            ).all()

            items = [
                RemoteURLItem(
                    id=r.id,
                    user_id=r.user_id,
                    url=r.url,
                    uploaded_at=(r.uploaded_at.isoformat() if r.uploaded_at else None),
                    downloaded_at=(
                        r.downloaded_at.isoformat() if r.downloaded_at else None
                    ),
                    created_at=(r.created_at.isoformat() if r.created_at else None),
                    site=site.lower(),
                )
                for r in rows
            ]
        return RemoteURLResponse(
            success=True,
            urls=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        logger.exception("Failed to get user remote URLs")
        return RemoteURLResponse(success=False, error=str(e))
        return RemoteURLResponse(success=False, error=str(e))
