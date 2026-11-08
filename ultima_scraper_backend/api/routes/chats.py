"""Chats API routes.

Provides endpoints for listing chats, messages, and sending messages.

Data Flow (DB-first with background sync):
1. Return cached data from the local database immediately.
2. Schedule a background task to sync fresh data from the API.
3. When background sync finishes, push a WebSocket event so the
   frontend knows to re-query.
4. Real-time updates arrive via WebSocket (webhook handler already
   saves them to the database).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import func as sa_func
from sqlalchemy import or_
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select
from ultima_scraper_api import ContentType, user_types
from ultima_scraper_api.helpers import media_types

from ultima_scraper_db_archive import (
    ContentMediaAssoModel,
    DRMManifestModel,
    MediaModel,
    MessageModel,
    UserAuthModel,
    UserInfoModel,
    UserModel,
    UserProfileMediaModel,
)

from ...database.helpers.user_sync import schedule_user_sync
from ...helpers.background_tasks import spawn_background_task
from ...content_persistence import _resolve_usapi_media_size
from ...manager import BackendManager
from ..dependencies import get_backend_instance, get_backend_manager
from ...redis_events import JsonValue, publish_ws_event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats", tags=["chats"])

# Track running background sync tasks to avoid duplicates
_active_chat_syncs: set[str] = set()
_active_message_syncs: set[str] = set()

# Cooldown: remember when each sync last completed to avoid re-syncing too often
_last_chat_sync: dict[str, float] = {}
_last_message_sync: dict[str, float] = {}
_SYNC_COOLDOWN_SECONDS = 60.0  # Minimum seconds between syncs for the same key

ContentMediaAssociationRow = tuple[int, int | None, int | None, int | None]
PaidMessageRow = tuple[int, float | None, bool | None]
ChatSummaryRow = tuple[int, datetime | None, int | None, int]


def _stringify_mapping(value: Mapping[Any, Any] | None) -> dict[str, object]:
    if not value:
        return {}
    return {str(key): item for key, item in value.items()}


def _schedule_user_sync(
    backend_manager: BackendManager,
    site: str,
    user: user_types,
) -> None:
    try:
        site_lower = site.lower()
        db_manager = None
        if (
            hasattr(backend_manager, "db_managers")
            and site_lower in backend_manager.db_managers
        ):
            db_manager = backend_manager.db_managers[site_lower]
        if not db_manager:
            logger.debug("No db_manager available for site %s", site_lower)
            return
        schedule_user_sync(user, site_lower, db_manager=db_manager)
    except Exception as exc:
        logger.debug("Failed to schedule user sync: %s", exc, exc_info=True)


class ChatUserInfo(BaseModel):
    """User info for chat list."""

    id: int
    username: str
    name: str | None = None
    avatar_url: str | None = None


class ChatInfo(BaseModel):
    """Chat preview info."""

    id: int  # User ID of the chat partner
    user: ChatUserInfo
    last_message_text: str | None = None
    last_message_at: str | None = None
    unread_count: int = 0
    has_purchased_feed: bool = False
    pinned_count: int = 0


class ChatListResponse(BaseModel):
    """Response for listing chats."""

    success: bool
    chats: list[ChatInfo] = []
    total: int = 0
    source: Literal["db", "api"] = "db"
    syncing: bool = False
    error: str | None = None


class MediaItemInfo(BaseModel):
    """Media item info (image, video, audio, gif)."""

    id: int
    media_type: str  # "photo", "video", "audio", "gif"
    url: str | None = None  # Direct URL (may be None for DRM-protected content)
    thumb_url: str | None = None  # Thumbnail URL
    preview_url: str | None = None  # Preview/smaller version URL
    local_url: str | None = (
        None  # Local file stream URL (fallback for DRM / expired CDN)
    )
    width: int | None = None
    height: int | None = None
    duration: int | None = None  # For videos/audio (seconds)
    can_view: bool = False  # Whether current user can view this media
    has_drm: bool = False  # Whether media is DRM-protected (requires special handling)
    is_ready: bool = False  # Whether media is processed and ready
    already_owned: bool = False  # Media exists in another paid/free message or post


class MessageInfo(BaseModel):
    """Single message info."""

    id: int
    text: str
    from_user_id: int
    from_username: str
    is_from_me: bool
    created_at: str
    is_tip: bool = False
    is_free: bool | None = None
    price: float | None = None
    media_count: int = 0
    media: list[MediaItemInfo] = []  # Media items attached to message
    is_opened: bool | None = None
    is_liked: bool = False
    has_rebundled_media: bool = False  # Some media already owned from another purchase


class MessageListResponse(BaseModel):
    """Response for listing messages."""

    success: bool
    messages: list[MessageInfo] = []
    has_more: bool = False
    next_offset_id: int | None = None
    source: Literal["db", "api"] = "db"
    syncing: bool = False
    error: str | None = None


class SendMessageRequest(BaseModel):
    """Request to send a message."""

    text: str
    price: float | None = None
    locked_text: bool | None = None


class SendMessageResponse(BaseModel):
    """Response after sending a message."""

    success: bool
    message: MessageInfo | None = None
    error: str | None = None


class BuyMessageResponse(BaseModel):
    """Response after purchasing a PPV message."""

    success: bool
    message: MessageInfo | None = None
    error: str | None = None


class LikeMessageResponse(BaseModel):
    """Response after liking/unliking a message."""

    success: bool
    is_liked: bool = False
    error: str | None = None


def _save_message_to_db(
    engine: Any,
    msg: Any,
    auth_id: int,
    chat_user_id: int,
    site: str,
) -> None:
    """Save a message and its media to the database.

    Args:
        engine: Database engine
        msg: Message object from USAPI
        auth_id: The authenticated account ID
        chat_user_id: The chat partner's user ID
    """
    try:
        # Get author info - try multiple methods
        author = None
        author_id = 0

        # Method 1: get_author() method
        if hasattr(msg, "get_author") and callable(msg.get_author):
            author = msg.get_author()
            if author:
                author_id = getattr(author, "id", 0)

        # Method 2: fromUser attribute (common in OF API)
        if not author_id and hasattr(msg, "fromUser") and msg.fromUser:
            author = msg.fromUser
            author_id = getattr(author, "id", 0)

        # Method 3: user_id attribute directly on message
        if not author_id and hasattr(msg, "user_id") and msg.user_id:
            author_id = msg.user_id

        # If still no author_id, log warning but continue
        if not author_id:
            logger.warning(
                f"Could not determine author_id for message {getattr(msg, 'id', 'unknown')}. "
                f"Available attrs: {[a for a in dir(msg) if not a.startswith('_')][:20]}"
            )
            # Default to chat_user_id if not from auth
            author_id = chat_user_id

        is_from_me = author_id == auth_id

        # Determine receiver_id
        if is_from_me:
            receiver_id = chat_user_id
        else:
            receiver_id = auth_id

        # Parse created_at
        created_at = datetime.now(timezone.utc)
        if hasattr(msg, "created_at") and msg.created_at:
            created_at = msg.created_at

        # Get price and media count
        price = float(getattr(msg, "price", 0) or 0)
        media_list = getattr(msg, "media", []) or []

        with Session(engine) as session:  # type: ignore[arg-type]
            # Check if message already exists
            existing = session.exec(
                select(MessageModel).where(MessageModel.id == msg.id)
            ).first()

            if not existing:
                # Create message record
                new_message = MessageModel(
                    id=msg.id,
                    user_id=author_id,
                    receiver_id=receiver_id,
                    text=getattr(msg, "text", "") or "",
                    price=price,
                    paid=getattr(msg, "paid", None),
                    liked=bool(getattr(msg, "isLiked", False)),
                    media_count=len(media_list),
                    created_at=created_at,
                )
                session.add(new_message)
            else:
                # Update mutable fields only after immutable identity matched.
                changed = False
                api_paid = getattr(msg, "paid", None)
                merged_paid = (
                    existing.paid
                    if existing.paid is True
                    else (api_paid if api_paid is not None else existing.paid)
                )
                if existing.paid != merged_paid:
                    existing.paid = merged_paid
                    changed = True
                api_liked = bool(getattr(msg, "isLiked", False))
                if hasattr(existing, "liked") and existing.liked != api_liked:
                    existing.liked = api_liked
                    changed = True
                api_text = getattr(msg, "text", "") or ""
                if api_text and existing.text != api_text:
                    existing.text = api_text
                    changed = True
                if changed:
                    session.add(existing)

            # Always process media (backfills duration/DRM on existing records)
            for media in media_list:
                _save_media_to_db(
                    session,
                    media,
                    msg.id,
                    author_id,
                    site=site,
                    authoritative_created_at=created_at,
                )

            session.commit()
            logger.debug(
                f"Saved message {msg.id} to DB: from={author_id}, to={receiver_id}, "
                f"is_from_me={is_from_me}, media={len(media_list)}"
            )

    except Exception as exc:
        logger.error(
            f"Failed to save message {getattr(msg, 'id', 'unknown')} to DB: {exc}",
            exc_info=True,
        )


def _save_media_to_db(
    session: Session,
    media: Any,
    message_id: int,
    user_id: int,
    *,
    site: str,
    authoritative_created_at: datetime | None,
) -> None:
    """Save a media item and its content association to the database.

    Args:
        session: Active database session
        media: Media object from USAPI
        message_id: Parent message ID
        user_id: User who owns the media
    """
    media_id = getattr(media, "id", None)
    if not media_id:
        return

    # Determine DRM status up-front (needed for both existing & new paths)
    has_drm = False
    if hasattr(media, "files") and media.files:
        if hasattr(media.files, "drm") and media.files.drm:
            has_drm = True

    # Check if media exists
    existing = session.exec(select(MediaModel).where(MediaModel.id == media_id)).first()

    if existing:
        # Backfill mutable/site metadata after immutable identity matched.
        changed = False
        raw_duration = getattr(media, "duration", 0) or 0
        if raw_duration and existing.duration is None:
            existing.duration = int(raw_duration)
            changed = True
        media_size = _resolve_usapi_media_size(media)
        if media_size is not None and int(existing.size or 0) <= 0:
            existing.size = media_size
            changed = True
        if has_drm and not existing.drm:
            existing.drm = True
            changed = True
        # Update URL if it was previously null (for example, after purchase).
        if not existing.url:
            new_url = None
            if hasattr(media, "files") and media.files:
                files = media.files
                if hasattr(files, "full") and files.full:
                    new_url = files.full.url
            elif hasattr(media, "source") and media.source:
                source = cast(object, media.source)
                if isinstance(source, dict):
                    source_map = _stringify_mapping(
                        cast(Mapping[Any, Any], source)
                    )
                    raw_url = source_map.get("source") or source_map.get("url")
                    new_url = raw_url if isinstance(raw_url, str) else None
            if new_url:
                existing.url = new_url
                changed = True
        if changed:
            session.add(existing)
    else:
        # Get URL info
        url: str | None = None

        if hasattr(media, "files") and media.files:
            files = media.files
            if hasattr(files, "full") and files.full:
                url = files.full.url
        elif hasattr(media, "source") and media.source:
            source = cast(object, media.source)
            if isinstance(source, dict):
                source_map = _stringify_mapping(cast(Mapping[Any, Any], source))
                raw_url = source_map.get("source") or source_map.get("url")
                url = raw_url if isinstance(raw_url, str) else None

        # Get created_at
        created_at = datetime.now(timezone.utc)
        if hasattr(media, "created_at") and media.created_at:
            created_at = media.created_at

        # Normalize category: photo -> image, stream -> video, etc.
        raw_category = getattr(media, "type", "photo")
        normalized_category = (
            media_types.normalize(raw_category) if raw_category else "image"
        )

        # Extract duration (seconds) for video/audio
        raw_duration = getattr(media, "duration", 0) or 0
        duration: int | None = int(raw_duration) if raw_duration else None

        size = _resolve_usapi_media_size(media)

        media_payload: dict[str, Any] = {
            "id": media_id,
            "user_id": user_id,
            "url": url,
            "duration": duration,
            "drm": has_drm,
            "category": normalized_category,
            "preview": False,
            "downloaded": False,
            "created_at": created_at,
        }
        if size is not None:
            media_payload["size"] = size

        new_media = MediaModel(**media_payload)
        session.add(new_media)

    # Persist DRM manifest data when available (USAPI MediaModel objects)
    if has_drm and hasattr(media, "files") and media.files and media.files.drm:
        drm = media.files.drm
        existing_manifest = session.exec(
            select(DRMManifestModel).where(DRMManifestModel.media_id == media_id)
        ).first()
        if not existing_manifest:
            manifest = DRMManifestModel(
                media_id=media_id,
                dash_manifest=drm.dash.manifest_url,
                dash_key_pair_id=drm.dash.key_pair_id,
                dash_policy=drm.dash.policy,
                dash_signature=drm.dash.signature,
                hls_manifest=drm.hls.manifest_url,
                hls_key_pair_id=drm.hls.key_pair_id,
                hls_policy=drm.hls.policy,
                hls_signature=drm.hls.signature,
            )
            session.add(manifest)
        else:
            existing_manifest.dash_manifest = drm.dash.manifest_url
            existing_manifest.dash_key_pair_id = drm.dash.key_pair_id
            existing_manifest.dash_policy = drm.dash.policy
            existing_manifest.dash_signature = drm.dash.signature
            existing_manifest.hls_manifest = drm.hls.manifest_url
            existing_manifest.hls_key_pair_id = drm.hls.key_pair_id
            existing_manifest.hls_policy = drm.hls.policy
            existing_manifest.hls_signature = drm.hls.signature

    # Create content-media association if not exists
    existing_asso = session.exec(
        select(ContentMediaAssoModel).where(
            ContentMediaAssoModel.message_id == message_id,
            ContentMediaAssoModel.media_id == media_id,
        )
    ).first()

    if not existing_asso:
        new_asso = ContentMediaAssoModel(
            message_id=message_id,
            media_id=media_id,
        )
        session.add(new_asso)


# ---------------------------------------------------------------------------
# DB-first query helpers
# ---------------------------------------------------------------------------


def _get_db_engine(backend_manager: BackendManager, site: str) -> object | None:
    """Return the SQLAlchemy engine for *site*, or ``None``."""
    site_lower = site.lower()
    if (
        hasattr(backend_manager, "db_managers")
        and site_lower in backend_manager.db_managers
    ):
        db_mgr = backend_manager.db_managers[site_lower]
        if hasattr(db_mgr, "engine") and db_mgr.engine:
            return db_mgr.engine
    return None


def _get_redis_manager(backend_manager: BackendManager) -> object | None:
    """Return the Redis manager (if available) for publishing WS events."""
    manager = getattr(backend_manager, "redis_manager", None)
    if manager is not None:
        return manager

    manager = getattr(backend_manager, "redis", None)
    if manager is not None:
        return manager

    manager = getattr(backend_manager, "_redis_manager", None)
    if manager is not None:
        return manager

    return None


async def _publish_sync_event(
    backend_manager: BackendManager,
    event: dict[str, JsonValue],
) -> None:
    """Publish a chat sync event to clients.

    Uses Redis pub/sub when available; falls back to direct local WebSocket
    broadcast so DB-first routes still notify connected clients in local mode.
    """
    redis = _get_redis_manager(backend_manager)
    if redis is not None:
        await publish_ws_event(redis, event)
        return

    try:
        from .websocket import broadcast_event

        broadcast = cast(Any, broadcast_event)
        await broadcast(event)
    except Exception as exc:
        logger.debug("Failed to broadcast sync event directly: %s", exc)


def _get_chats_from_db(
    engine: Any,
    auth_ids: list[int],
    limit: int = 100,
) -> list[ChatInfo]:
    """Build a chat list from the local database.

    Finds all distinct users who have exchanged messages with any of the
    given *auth_ids* and returns them sorted by most recent message.
    """
    chats: list[ChatInfo] = []
    try:
        with Session(engine) as session:  # type: ignore[arg-type]
            # Get distinct chat partners: users who are either sender or receiver
            # for any of our auth IDs
            from sqlalchemy import case, literal_column

            # Sub-query: for each message, determine the "other" user
            # If user_id is one of our auths → partner is receiver_id
            # If receiver_id is one of our auths → partner is user_id
            partner_col = case(
                (MessageModel.user_id.in_(auth_ids), MessageModel.receiver_id),
                else_=MessageModel.user_id,
            ).label("partner_id")

            # Get the latest message per partner
            subq = (
                select(
                    partner_col,
                    sa_func.max(MessageModel.created_at).label("last_msg_at"),
                    sa_func.max(MessageModel.id).label("last_msg_id"),
                    sa_func.count(MessageModel.id).label("msg_count"),
                )
                .where(
                    or_(
                        MessageModel.user_id.in_(auth_ids),
                        MessageModel.receiver_id.in_(auth_ids),
                    )
                )
                .group_by(literal_column("partner_id"))
                .order_by(sa_func.max(MessageModel.created_at).desc())
                .limit(limit)
                .subquery()
            )

            rows = cast(
                list[ChatSummaryRow],
                session.exec(
                    select(
                        subq.c.partner_id,
                        subq.c.last_msg_at,
                        subq.c.last_msg_id,
                        subq.c.msg_count,
                    )
                ).all(),
            )

            if not rows:
                return chats

            partner_ids = [partner_id for partner_id, _, _, _ in rows if partner_id]

            # Batch-load user info
            users_map: dict[int, UserModel] = {}
            if partner_ids:
                user_rows = session.exec(
                    select(UserModel).where(UserModel.id.in_(partner_ids))
                ).all()
                users_map = {u.id: u for u in user_rows}

            # Batch-load user info (names)
            info_map: dict[int, UserInfoModel] = {}
            if partner_ids:
                info_rows = session.exec(
                    select(UserInfoModel).where(UserInfoModel.user_id.in_(partner_ids))
                ).all()
                info_map = {ui.user_id: ui for ui in info_rows}

            # Batch-load latest avatar URLs (single query instead of N)
            avatar_map: dict[int, str | None] = {}
            if partner_ids:
                # Use a lateral subquery to get the latest avatar per user
                latest_sq = (
                    select(
                        UserProfileMediaModel.user_id,
                        UserProfileMediaModel.avatar_url,
                        sa_func.row_number()
                        .over(
                            partition_by=UserProfileMediaModel.user_id,
                            order_by=UserProfileMediaModel.created_at.desc(),
                        )
                        .label("rn"),
                    )
                    .where(UserProfileMediaModel.user_id.in_(partner_ids))
                    .subquery()
                )
                avatar_rows = cast(
                    list[tuple[int, str | None]],
                    session.exec(
                        select(latest_sq.c.user_id, latest_sq.c.avatar_url).where(
                            latest_sq.c.rn == 1
                        )
                    ).all(),
                )
                for uid, avatar_url in avatar_rows:
                    avatar_map[uid] = avatar_url

            # Batch-load last message text
            last_msg_map: dict[int, MessageModel | None] = {}
            last_msg_ids = [last_msg_id for _, _, last_msg_id, _ in rows if last_msg_id]
            if last_msg_ids:
                msg_rows = session.exec(
                    select(MessageModel).where(MessageModel.id.in_(last_msg_ids))
                ).all()
                id_to_msg = {m.id: m for m in msg_rows}
                for partner_id, _, last_msg_id, _ in rows:
                    if partner_id and last_msg_id and last_msg_id in id_to_msg:
                        last_msg_map[partner_id] = id_to_msg[last_msg_id]

            for pid, _, _, _ in rows:
                if not pid:
                    continue
                user = users_map.get(pid)
                info = info_map.get(pid)
                username = (user.username if user else None) or f"user_{pid}"
                name = info.name if info else None
                avatar_url = avatar_map.get(pid)

                last_msg = last_msg_map.get(pid)
                last_text = (last_msg.text or "")[:100] if last_msg else None
                last_at = (
                    last_msg.created_at.isoformat()
                    if last_msg and last_msg.created_at
                    else None
                )

                chats.append(
                    ChatInfo(
                        id=pid,
                        user=ChatUserInfo(
                            id=pid,
                            username=username,
                            name=name,
                            avatar_url=avatar_url,
                        ),
                        last_message_text=last_text,
                        last_message_at=last_at,
                    )
                )

    except Exception as exc:
        logger.error("Failed to load chats from DB: %s", exc, exc_info=True)

    return chats


def _get_messages_from_db(
    engine: Any,
    auth_ids: list[int],
    chat_user_id: int,
    site_name: str,
    limit: int = 100,
    offset_id: int | None = None,
) -> tuple[list[MessageInfo], bool, int | None]:
    """Load messages for a conversation from the local database.

    Returns ``(messages, has_more, next_offset_id)`` where messages are in
    API order (newest first).

    Pagination is keyset-based using ``MessageModel.id`` (descending): callers
    pass ``offset_id`` and receive items with ``id < offset_id``.
    """
    messages: list[MessageInfo] = []
    has_more = False
    next_offset_id: int | None = None
    try:
        with Session(engine) as session:  # type: ignore[arg-type]
            # Messages where (user_id in auth_ids AND receiver_id == chat_user_id)
            # OR (user_id == chat_user_id AND receiver_id in auth_ids)
            stmt = (
                select(MessageModel)
                .where(
                    or_(
                        (
                            MessageModel.user_id.in_(auth_ids)
                            & (MessageModel.receiver_id == chat_user_id)
                        ),
                        (
                            (MessageModel.user_id == chat_user_id)
                            & MessageModel.receiver_id.in_(auth_ids)
                        ),
                    )
                )
                .options(
                    selectinload(
                        MessageModel.media  # type: ignore[arg-type]
                    ).selectinload(
                        MediaModel.filepaths  # type: ignore[arg-type]
                    ),
                )
            )

            if offset_id is not None:
                stmt = stmt.where(MessageModel.id < offset_id)

            stmt = stmt.order_by(MessageModel.id.desc()).limit(limit + 1)
            rows = cast(list[MessageModel], session.exec(stmt).all())

            if len(rows) > limit:
                has_more = True
                rows = rows[:limit]

            if has_more and rows:
                next_offset_id = rows[-1].id

            # Build username lookup for authors
            author_ids = {r.user_id for r in rows}
            user_rows = (
                cast(
                    list[UserModel],
                    session.exec(
                        select(UserModel).where(UserModel.id.in_(list(author_ids)))
                    ).all(),
                )
                if author_ids
                else []
            )
            username_map = {u.id: u.username or f"user_{u.id}" for u in user_rows}

            # ----- Rebundle detection: find media IDs owned elsewhere -----
            # Collect all media IDs from locked PPV messages so we can batch-query.
            locked_msg_media_ids: set[int] = set()
            locked_msg_ids: set[int] = set()
            for msg in rows:
                msg_is_locked = msg.price > 0 and not msg.paid
                if msg_is_locked and msg.media:
                    locked_msg_ids.add(msg.id)
                    for m in msg.media:
                        locked_msg_media_ids.add(m.id)

            # Query which of those media IDs also appear in OTHER content
            # (different messages that are paid/free, or posts/stories).
            already_owned_ids: set[int] = set()
            if locked_msg_media_ids:
                # Find content_media_asso rows for these media IDs that point
                # to a DIFFERENT message (already paid or free) or to a post/story.
                assoc_rows = cast(
                    list[ContentMediaAssociationRow],
                    session.exec(
                        select(
                            ContentMediaAssoModel.media_id,
                            ContentMediaAssoModel.message_id,
                            ContentMediaAssoModel.post_id,
                            ContentMediaAssoModel.story_id,
                        ).where(
                            ContentMediaAssoModel.media_id.in_(
                                list(locked_msg_media_ids)
                            )
                        )
                    ).all(),
                )

                # Build a lookup of message_id -> paid status for ownership check
                related_msg_ids = {
                    message_id
                    for _, message_id, _, _ in assoc_rows
                    if message_id and message_id not in locked_msg_ids
                }
                paid_msg_ids: set[int] = set()
                if related_msg_ids:
                    paid_rows = cast(
                        list[PaidMessageRow],
                        session.exec(
                            select(
                                MessageModel.id, MessageModel.price, MessageModel.paid
                            ).where(MessageModel.id.in_(list(related_msg_ids)))
                        ).all(),
                    )
                    for paid_message_id, price, paid in paid_rows:
                        is_free = price is None or price == 0
                        is_paid = bool(paid)
                        if is_free or is_paid:
                            paid_msg_ids.add(paid_message_id)

                for media_id, message_id, post_id, story_id in assoc_rows:
                    # Owned via post or story (always accessible)
                    if post_id or story_id:
                        already_owned_ids.add(media_id)
                    # Owned via a different message that's free or already paid
                    elif message_id and message_id not in locked_msg_ids:
                        if message_id in paid_msg_ids:
                            already_owned_ids.add(media_id)

            # ----- Build response messages -----
            for msg in rows:
                is_from_me = msg.user_id in auth_ids

                # A message is locked when it has a price and hasn't been paid.
                msg_is_locked = msg.price > 0 and not msg.paid

                media_items: list[MediaItemInfo] = []
                has_rebundled = False
                for m in msg.media:
                    # Media is viewable only when not DRM-locked AND the
                    # parent message isn't behind an unpaid paywall.
                    can_view = not m.drm and not msg_is_locked
                    owned = m.id in already_owned_ids if msg_is_locked else False
                    if owned:
                        has_rebundled = True

                    # Check for a downloaded (non-preview) file on disk
                    filepaths = getattr(m, "filepaths", [])
                    has_local = any(
                        Path(fp.filepath).exists() for fp in filepaths if not fp.preview
                    )

                    # Build local_url fallback for DRM or missing-URL media
                    local_url: str | None = None
                    if m.drm or (can_view and not m.url):
                        if has_local:
                            local_url = f"/api/v1/media/stream/{site_name}/{m.id}"

                    # Build thumb_url — serve a thumbnail for any media
                    # that has a downloaded file on disk
                    thumb_url: str | None = None
                    if has_local:
                        thumb_url = f"/api/v1/media/thumbnail/{site_name}/{m.id}"

                    media_items.append(
                        MediaItemInfo(
                            id=m.id,
                            media_type=m.category or "photo",
                            url=m.url if can_view else None,
                            thumb_url=thumb_url,
                            preview_url=None,
                            local_url=local_url,
                            width=None,
                            height=None,
                            duration=getattr(m, "duration", None),
                            can_view=can_view or local_url is not None,
                            has_drm=m.drm,
                            is_ready=True,
                            already_owned=owned,
                        )
                    )

                messages.append(
                    MessageInfo(
                        id=msg.id,
                        text=msg.text or "",
                        from_user_id=msg.user_id,
                        from_username=username_map.get(
                            msg.user_id, f"user_{msg.user_id}"
                        ),
                        is_from_me=is_from_me,
                        created_at=msg.created_at.isoformat() if msg.created_at else "",
                        is_tip=False,
                        is_free=msg.price == 0,
                        price=msg.price,
                        media_count=len(media_items),
                        media=media_items,
                        is_opened=bool(msg.paid),
                        is_liked=bool(getattr(msg, "liked", False)),
                        has_rebundled_media=has_rebundled,
                    )
                )

    except Exception as exc:
        logger.error("Failed to load messages from DB: %s", exc, exc_info=True)

    return messages, has_more, next_offset_id


def _get_auth_ids_for_site(engine: object, site: str, auth_id: int | None) -> list[int]:
    """Return auth user-IDs from the database for *site*.

    If *auth_id* is given, accepts either:
    - ``UserAuthModel.user_id`` (site account user id), or
    - ``UserAuthModel.id`` (auth record id)

    and always returns normalized site account user IDs.
    """
    try:
        with Session(engine) as session:  # type: ignore[arg-type]
            base_stmt = select(UserAuthModel).where(
                UserAuthModel.site == site.lower(),
                UserAuthModel.active.is_(True),
            )

            if auth_id is None:
                rows = session.exec(base_stmt).all()
                return list({int(row.user_id) for row in rows if row.user_id})

            # Resolve auth_id as either user_id or auth row id.
            resolved_row = session.exec(
                base_stmt.where(UserAuthModel.user_id == auth_id)
            ).first()

            if resolved_row is None:
                resolved_row = session.exec(
                    base_stmt.where(UserAuthModel.id == auth_id)
                ).first()

            if resolved_row and resolved_row.user_id:
                return [int(resolved_row.user_id)]

            return []
    except Exception as exc:
        logger.debug("Failed to query auth IDs from DB: %s", exc)
        return [auth_id] if auth_id is not None else []


# ---------------------------------------------------------------------------
# Background sync tasks
# ---------------------------------------------------------------------------


def _sync_on_cooldown(sync_key: str, last_map: dict[str, float]) -> bool:
    """Return ``True`` if *sync_key* was synced recently (within cooldown)."""
    last = last_map.get(sync_key)
    if last is not None and (time.monotonic() - last) < _SYNC_COOLDOWN_SECONDS:
        return True
    return False


async def _fetch_single_page_chats(
    auth: Any,
    limit: int = 50,
    offset: int = 0,
) -> list[Any]:
    """Fetch a *single page* of chats via the API (no recursion).

    This avoids the fully-recursive ``auth.get_chats()`` which walks
    every page and can take minutes for large accounts.
    """
    try:
        from ultima_scraper_api.apis.onlyfans.classes.chat_model import (
            ChatModel as APIChatModel,
        )
        from ultima_scraper_api.apis.onlyfans.classes.extras import (
            endpoint_links,
        )

        link = endpoint_links(global_limit=limit, global_offset=offset).list_chats
        result = await auth.auth_session.json_request(link)  # type: ignore[union-attr]

        result_dict = cast(dict[str, Any], result) if isinstance(result, dict) else None
        items = (
            cast(list[dict[str, Any]], result_dict.get("list", []))
            if result_dict
            else []
        )
        chats: list[Any] = []
        for item in items:
            try:
                chats.append(APIChatModel(item, auth))  # type: ignore[arg-type]
            except Exception:
                continue
        return chats
    except Exception as exc:
        logger.warning("Single-page chat fetch failed: %s", exc, exc_info=True)
        return []


async def _fetch_single_page_messages(
    user: Any,
    limit: int = 50,
) -> list[Any]:
    """Fetch a *single page* of messages via the API (no recursion).

    Avoids ``user.get_messages()`` which recursively walks all pages.
    """
    try:
        from ultima_scraper_api.apis.onlyfans.classes.extras import endpoint_links
        from ultima_scraper_api.apis.onlyfans.classes.message_model import (
            MessageModel as APIMessageModel,
        )

        link = endpoint_links().list_messages(
            user.id,
            global_limit=limit,
            global_offset=None,  # type: ignore[union-attr]
        )
        result = await user.get_requester().json_request(link)  # type: ignore[union-attr]

        result_dict = cast(dict[str, Any], result) if isinstance(result, dict) else None
        items = (
            cast(list[dict[str, Any]], result_dict.get("list", []))
            if result_dict
            else []
        )
        messages: list[Any] = []
        authed = user.get_authed()
        for item in items:
            try:
                from_user_data = cast(dict[str, Any] | None, item.get("fromUser"))
                if from_user_data:
                    from_user = authed.resolve_user(from_user_data)
                else:
                    from_user = user
                messages.append(APIMessageModel(item, from_user))
            except Exception:
                continue
        return messages
    except Exception as exc:
        logger.warning("Single-page message fetch failed: %s", exc, exc_info=True)
        return []


async def _background_sync_chats(
    backend_manager: BackendManager,
    site: str,
    auth_id: int | None,
    limit: int,
    offset: int,
) -> None:
    """Fetch the most recent page of chats from the API and save to DB.

    Only fetches a *single page* instead of recursively walking all
    chats — keeping this fast and truly background-friendly.
    Publishes a ``chats_synced`` WebSocket event when finished.
    """
    sync_key = f"chats:{site}:{auth_id}"
    if sync_key in _active_chat_syncs:
        return
    if _sync_on_cooldown(sync_key, _last_chat_sync):
        logger.debug("Chat sync on cooldown for %s — skipping", sync_key)
        return
    _active_chat_syncs.add(sync_key)

    try:
        # Yield immediately so the calling endpoint returns first
        await asyncio.sleep(0)

        logger.debug("Background chat sync started: site=%s auth_id=%s", site, auth_id)

        if auth_id is not None:
            auth = await asyncio.wait_for(
                backend_manager.get_authed_session(site, auth_id),
                timeout=30.0,
            )
            auths = [auth] if auth else []
        else:
            auths = await asyncio.wait_for(
                backend_manager.get_all_authed_sessions(site),
                timeout=30.0,
            )

        if not auths:
            return

        db_engine = _get_db_engine(backend_manager, site)
        # Cap the per-page limit to keep the request fast
        page_limit = min(limit, 50)

        for auth in auths:
            try:
                chats = await asyncio.wait_for(
                    _fetch_single_page_chats(auth, limit=page_limit, offset=offset),
                    timeout=30.0,
                )
                logger.info(
                    "Background sync: got %d chats for auth %s",
                    len(chats),
                    getattr(auth, "id", "?"),
                )

                for chat in chats:
                    user = chat.user  # type: ignore[union-attr]
                    _schedule_user_sync(backend_manager, site, user)

                    if db_engine and chat.last_message:  # type: ignore[union-attr]
                        _save_message_to_db(
                            db_engine,
                            chat.last_message,
                            auth.id,
                            user.id,
                            site,
                        )  # type: ignore[union-attr]

                    # Yield to event loop between chat processing
                    await asyncio.sleep(0)

            except asyncio.TimeoutError:
                logger.warning(
                    "Background chat sync timed out for auth %s",
                    getattr(auth, "id", "?"),
                )
            except Exception as exc:
                logger.warning(
                    "Background sync failed for auth %s: %s",
                    getattr(auth, "id", "?"),
                    exc,
                    exc_info=True,
                )

        # Notify frontend via WebSocket
        await _publish_sync_event(
            backend_manager,
            {
                "event": "chats_synced",
                "site": site,
                "auth_id": auth_id,
            },
        )

    except asyncio.TimeoutError:
        logger.warning("Background chat sync timed out: %s", sync_key)
    except Exception as exc:
        logger.error("Background chat sync error: %s", exc, exc_info=True)
    finally:
        _active_chat_syncs.discard(sync_key)
        _last_chat_sync[sync_key] = time.monotonic()


async def _background_sync_messages(
    backend_manager: BackendManager,
    site: str,
    auth_id: int,
    chat_user_id: int,
    limit: int,
) -> None:
    """Fetch the most recent page of messages from the API and save to DB.

    Only fetches a *single page* instead of all messages.
    Publishes a ``messages_synced`` WebSocket event when finished.
    """
    sync_key = f"messages:{site}:{auth_id}:{chat_user_id}"
    if sync_key in _active_message_syncs:
        return
    if _sync_on_cooldown(sync_key, _last_message_sync):
        logger.debug("Message sync on cooldown for %s — skipping", sync_key)
        return
    _active_message_syncs.add(sync_key)

    try:
        # Yield so the endpoint returns first
        await asyncio.sleep(0)

        logger.debug(
            "Background message sync started: site=%s auth=%s user=%s",
            site,
            auth_id,
            chat_user_id,
        )

        auth = await asyncio.wait_for(
            backend_manager.get_authed_session(site, auth_id),
            timeout=30.0,
        )
        if not auth:
            return

        user = None
        if hasattr(auth, "find_user"):
            user = auth.find_user(chat_user_id)
        if not user and hasattr(auth, "get_user"):
            user = await asyncio.wait_for(
                auth.get_user(str(chat_user_id)),
                timeout=15.0,
            )
        if not user:
            logger.warning("Background sync: user %s not found", chat_user_id)
            return

        _schedule_user_sync(backend_manager, site, user)

        # Fetch only one page of recent messages
        page_limit = min(limit, 50)
        messages = await asyncio.wait_for(
            _fetch_single_page_messages(user, limit=page_limit),
            timeout=30.0,
        )
        logger.info(
            "Background sync: got %d messages for user %s",
            len(messages),
            chat_user_id,
        )

        db_engine = _get_db_engine(backend_manager, site)
        if db_engine:
            for msg in messages:
                _save_message_to_db(db_engine, msg, auth_id, chat_user_id, site)

        # Notify frontend via WebSocket
        await _publish_sync_event(
            backend_manager,
            {
                "event": "messages_synced",
                "site": site,
                "auth_id": auth_id,
                "chat_user_id": chat_user_id,
            },
        )

    except asyncio.TimeoutError:
        logger.warning("Background message sync timed out: %s", sync_key)
    except Exception as exc:
        logger.error("Background message sync error: %s", exc, exc_info=True)
    finally:
        _active_message_syncs.discard(sync_key)
        _last_message_sync[sync_key] = time.monotonic()


@router.get("/sites")
def get_chat_sites() -> list[str]:
    """Get list of supported sites for chats."""
    return ["onlyfans"]


@router.get("/{site}", response_model=ChatListResponse)
async def list_chats(
    site: str,
    auth_id: int | None = Query(None, description="Filter by auth ID"),
    limit: int = Query(100, ge=1, le=500, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> ChatListResponse:
    """List all chats for a site/auth.

    Returns cached data from the local DB immediately and schedules a
    background API sync.  The frontend receives a ``chats_synced``
    WebSocket event when the sync finishes and can re-query.
    """
    try:
        logger.debug("list_chats called: site=%s, auth_id=%s", site, auth_id)

        db_engine = _get_db_engine(backend_manager, site)
        if not db_engine:
            return ChatListResponse(
                success=False,
                error=f"No database available for {site}",
            )

        # Resolve auth user-IDs from the DB (auth_id may be user_id or auth row id)
        auth_ids = await asyncio.to_thread(
            _get_auth_ids_for_site, db_engine, site, auth_id
        )
        effective_auth_user_id = auth_ids[0] if auth_ids else None
        if not auth_ids:
            # No auth rows yet – still schedule a sync so the API can
            # populate the DB on first run.
            sync_key = f"chats:{site}:{effective_auth_user_id}"
            already_syncing = sync_key in _active_chat_syncs
            if not already_syncing and not _sync_on_cooldown(sync_key, _last_chat_sync):
                spawn_background_task(
                    _background_sync_chats(
                        backend_manager, site, effective_auth_user_id, limit, offset
                    )
                )
                already_syncing = True
            return ChatListResponse(
                success=True,
                chats=[],
                total=0,
                source="db",
                syncing=already_syncing,
            )

        # 1. Return DB data immediately
        chats_list = await asyncio.to_thread(
            _get_chats_from_db, db_engine, auth_ids, limit=limit
        )

        # 2. Schedule background API sync (non-blocking)
        sync_key = f"chats:{site}:{effective_auth_user_id}"
        already_syncing = sync_key in _active_chat_syncs
        if not already_syncing and not _sync_on_cooldown(sync_key, _last_chat_sync):
            spawn_background_task(
                _background_sync_chats(
                    backend_manager,
                    site,
                    effective_auth_user_id,
                    limit,
                    offset,
                )
            )
            already_syncing = True

        return ChatListResponse(
            success=True,
            chats=chats_list,
            total=len(chats_list),
            source="db",
            syncing=already_syncing,
        )

    except Exception as exc:
        logger.error("Error listing chats: %s", exc, exc_info=True)
        return ChatListResponse(success=False, error=str(exc))


@router.get("/{site}/{user_id}/messages", response_model=MessageListResponse)
async def list_messages(
    site: str,
    user_id: int,
    auth_id: int | None = Query(None, description="Auth ID to use"),
    limit: int = Query(50, ge=1, le=200, description="Max messages to return"),
    offset_id: int | None = Query(None, description="Message ID to start from"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> MessageListResponse:
    """Get messages for a specific chat.

    Returns cached messages from the local DB immediately and schedules
    a background API sync.  A ``messages_synced`` WebSocket event is
    published when the sync completes.
    """
    try:
        db_engine = _get_db_engine(backend_manager, site)
        if not db_engine:
            return MessageListResponse(
                success=False, error=f"No database available for {site}"
            )

        # Resolve auth IDs (auth_id may be user_id or auth row id)
        auth_ids = await asyncio.to_thread(
            _get_auth_ids_for_site, db_engine, site, auth_id
        )
        effective_auth_id = auth_ids[0] if auth_ids else None

        # 1. Return DB data immediately
        messages_list, has_more, next_offset_id = await asyncio.to_thread(
            _get_messages_from_db,
            db_engine,
            auth_ids,
            user_id,
            site_name=site,
            limit=limit,
            offset_id=offset_id,
        )

        # 2. Schedule background API sync (only for the first page – no offset)
        syncing = False
        if effective_auth_id is not None and offset_id is None:
            sync_key = f"messages:{site}:{effective_auth_id}:{user_id}"
            syncing = sync_key in _active_message_syncs
            if not syncing and not _sync_on_cooldown(sync_key, _last_message_sync):
                spawn_background_task(
                    _background_sync_messages(
                        backend_manager,
                        site,
                        effective_auth_id,
                        user_id,
                        limit,
                    )
                )
                syncing = True

        return MessageListResponse(
            success=True,
            messages=messages_list,
            has_more=has_more,
            next_offset_id=next_offset_id,
            source="db",
            syncing=syncing,
        )

    except Exception as exc:
        logger.error("Error listing messages: %s", exc, exc_info=True)
        return MessageListResponse(success=False, error=str(exc))


@router.post("/{site}/{user_id}/messages", response_model=SendMessageResponse)
async def send_message(
    site: str,
    user_id: int,
    request: SendMessageRequest,
    auth_id: int | None = Query(None, description="Auth ID to use"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> SendMessageResponse:
    """Send a message to a user."""
    try:
        # Get an authenticated session
        if auth_id is not None:
            auth = await backend_manager.get_authed_session(site, auth_id)
        else:
            auths = await backend_manager.get_all_authed_sessions(site)
            auth = auths[0] if auths else None

        if not auth:
            return SendMessageResponse(
                success=False, error="No authenticated account available"
            )

        # Send the message
        auth_obj = cast(Any, auth)
        result = await auth_obj.send_message(
            to_user_id=user_id,
            text=request.text,
            price=request.price,
            lockedText=request.locked_text,
        )

        if isinstance(result, dict):
            result_data = cast(dict[str, Any], result)
            error_message = result_data.get("message", "Unknown error")
        else:
            result_data = {}
            error_message = None

        if isinstance(result, dict) and "error" in result_data:
            return SendMessageResponse(
                success=False,
                error=str(error_message),
            )

        # Parse result into MessageInfo
        msg_data = result_data
        created_at = datetime.now(timezone.utc)
        if "createdAt" in msg_data:
            try:
                created_at = datetime.fromisoformat(str(msg_data["createdAt"]))
            except (ValueError, TypeError):
                pass

        message_info = MessageInfo(
            id=int(msg_data.get("id", 0) or 0),
            text=request.text,
            from_user_id=int(auth.id),
            from_username=auth.username or f"user_{int(auth.id)}",
            is_from_me=True,
            created_at=created_at.isoformat(),
            is_tip=False,
            is_free=request.price is None,
            price=request.price,
            media_count=0,
            is_opened=None,
        )

        # Save sent message to local DB so it appears immediately on re-query
        db_engine = _get_db_engine(backend_manager, site)
        if db_engine and message_info.id:
            try:
                with Session(db_engine) as session:  # type: ignore[arg-type]
                    existing = session.exec(
                        select(MessageModel).where(MessageModel.id == message_info.id)
                    ).first()
                    if not existing:
                        new_msg = MessageModel(
                            id=message_info.id,
                            user_id=int(auth.id),
                            receiver_id=user_id,
                            text=request.text or "",
                            price=request.price or 0.0,
                            paid=None,
                            media_count=0,
                            created_at=created_at,
                        )
                        session.add(new_msg)
                        session.commit()
            except Exception as db_exc:
                logger.debug("Failed to persist sent message to DB: %s", db_exc)

        return SendMessageResponse(success=True, message=message_info)

    except Exception as exc:
        logger.error(f"Error sending message: {exc}", exc_info=True)
        return SendMessageResponse(success=False, error=str(exc))


# Content types to scrape after purchasing PPV messages
_PPV_CONTENT_TYPES: list[ContentType] = [
    ContentType.MESSAGES,
]

_PPV_SITE_NAME_MAP: dict[str, Literal["OnlyFans", "Fansly"]] = {
    "onlyfans": "OnlyFans",
    "fansly": "Fansly",
}


async def _create_ppv_scrape_job(
    site: str,
    auth_id: int,
    user_id: int,
    username: str | None,
    backend_manager: BackendManager,
) -> None:
    """Create a Scrape+Download job for a purchased PPV message.

    Downloads Messages content from the creator so the newly-unlocked media
    is scraped and persisted locally.
    """
    from uuid import uuid4

    from ...job_system import (
        Job,
        JobManager,
        JobStatus,
        Task,
        TaskType,
        build_task,
    )
    from ...models.backend import BackendInstance

    if not username:
        logger.warning(
            "Cannot create PPV S+D job without username for user_id=%s", user_id
        )
        return

    site_name = _PPV_SITE_NAME_MAP.get(site.lower())
    if not site_name:
        logger.warning("Unknown site %s for PPV S+D job creation", site)
        return

    job_id = str(uuid4())
    job_name = f"S+D: {username} Messages (PPV Purchase)"

    tasks: list[Task] = []
    for ct in _PPV_CONTENT_TYPES:
        scrape_task = build_task(
            task_type=TaskType.SCRAPE,
            site_name=site_name,
            auth_id=auth_id,
            user_id=user_id,
            user_username=username,
            content_type=ct,
            download_media=True,
            save_to_db=True,
        )
        tasks.append(scrape_task)

        download_task = build_task(
            task_type=TaskType.DOWNLOAD,
            site_name=site_name,
            auth_id=auth_id,
            user_id=user_id,
            user_username=username,
            content_type=ct,
        )
        download_task.depends_on = [scrape_task.task_id]
        tasks.append(download_task)

    for task in tasks:
        task.job_id = job_id

    job = Job(
        job_id=job_id,
        name=job_name,
        tasks=tasks,
        state=JobStatus.PENDING,
        created_by="chats_ppv_buy",
    )

    redis_manager = getattr(
        backend_manager, "redis", getattr(backend_manager, "_redis_manager", None)
    )

    backend_instance: BackendInstance | None = None
    try:
        backend_instance = await get_backend_instance()
    except Exception:
        pass

    if (
        backend_instance
        and backend_instance.distributed_mode
        and backend_instance.redis
        and backend_instance.redis.is_connected
    ):
        manager = JobManager(backend_instance.redis)
        job = await manager.prepare_job(job)
        await manager.activate_job(job, distributed_mode=True)
    elif redis_manager and getattr(redis_manager, "is_connected", False):
        manager = JobManager(redis_manager)
        job = await manager.prepare_job(
            job, backend_id=backend_instance.instance_id if backend_instance else None,
            select_backend=backend_instance is None,
        )
        await manager.activate_job(job, distributed_mode=False)
    elif backend_instance is not None:
        await backend_instance.active_queue.enqueue(cast(Any, job))
    else:
        logger.warning(
            "Redis not connected and backend unavailable, cannot create PPV S+D job"
        )
        return

    logger.info(
        "Created PPV S+D job %s for %s (user_id=%s) on %s",
        job_id,
        username,
        user_id,
        site,
    )


@router.post(
    "/{site}/{user_id}/messages/{message_id}/buy",
    response_model=BuyMessageResponse,
)
async def buy_message(
    site: str,
    user_id: int,
    message_id: int,
    auth_id: int | None = Query(None, description="Auth ID to use"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> BuyMessageResponse:
    """Buy/unlock a PPV message.

    Note: Messages are per-conversation and must be purchased by the auth
    that is chatting with that user. Cannot use spender selection for messages.
    """
    try:
        if auth_id is not None:
            auth = await backend_manager.get_authed_session(site, auth_id)
        else:
            auths = await backend_manager.get_all_authed_sessions(site)
            auth = auths[0] if auths else None

        if not auth:
            return BuyMessageResponse(
                success=False, error="No authenticated account available"
            )

        user = None
        auth_obj = cast(Any, auth)
        if hasattr(auth_obj, "find_user"):
            user = auth_obj.find_user(user_id)
        if not user and hasattr(auth_obj, "get_user"):
            user = await auth_obj.get_user(str(user_id))

        if not user:
            return BuyMessageResponse(success=False, error=f"User {user_id} not found")

        _schedule_user_sync(backend_manager, site, user)

        # ── Fetch the target message ──────────────────────────────────
        message = None

        # Strategy 1: fetch a small window around the message ID
        user_obj: Any = user
        if hasattr(user_obj, "get_messages"):
            try:
                msgs = cast(
                    list[Any],
                    await user_obj.get_messages(
                        limit=20,
                        offset_id=message_id + 1,
                    ),
                )
                message = next(
                    (m for m in msgs if getattr(m, "id", None) == message_id),
                    None,
                )
            except Exception as fetch_exc:
                logger.warning(
                    "get_messages(offset_id=%s) failed: %s",
                    message_id,
                    fetch_exc,
                )

        # Strategy 2: dedicated endpoint (may error on some API versions)
        if not message and hasattr(user_obj, "get_message_by_id"):
            try:
                message = await user_obj.get_message_by_id(
                    user_id=user_id,
                    message_id=message_id,
                    limit=10,
                    offset=0,
                )
                # get_message_by_id always returns a MessageModel, but it may
                # have been built from an empty dict if the message wasn't in
                # the response.  Treat id=0/None as "not found".
                if not getattr(message, "id", None):
                    message = None
            except Exception as fetch_exc:
                logger.warning(
                    "get_message_by_id(%s) failed: %s",
                    message_id,
                    fetch_exc,
                )

        if not message:
            return BuyMessageResponse(
                success=False, error=f"Message {message_id} not found"
            )

        # Validate the message is actually purchasable
        if hasattr(message, "canPurchase") and message.canPurchase is False:
            reason = getattr(
                message, "canPurchaseReason", "already purchased or not available"
            )
            return BuyMessageResponse(
                success=False,
                error=f"Message cannot be purchased: {reason}",
            )

        msg_price = getattr(message, "price", None)
        logger.info(
            "Buying message %s (price=%s, canPurchase=%s) for user %s",
            message_id,
            msg_price,
            getattr(message, "canPurchase", "N/A"),
            user_id,
        )

        message_obj = message
        if hasattr(message_obj, "buy_message") and callable(message_obj.buy_message):
            result = await cast(Any, message_obj.buy_message)()
        elif hasattr(message_obj, "buy_ppv") and callable(message_obj.buy_ppv):
            result = await cast(Any, message_obj.buy_ppv)()
        else:
            return BuyMessageResponse(
                success=False, error="Message does not support PPV purchase"
            )

        if isinstance(result, dict) and "error" in result:
            result_map = _stringify_mapping(cast(Mapping[Any, Any], result))
            error_value = result_map.get("error")
            if isinstance(error_value, dict):
                error_map = _stringify_mapping(cast(Mapping[Any, Any], error_value))
                code = str(error_map.get("code", "") or "")
                detail = str(error_map.get("message", "Purchase failed"))
                error_message = f"{detail} (code {code})" if code else detail
            else:
                error_message = str(error_value)
            logger.warning(
                "PPV purchase failed for message %s: %s", message_id, error_message
            )
            return BuyMessageResponse(success=False, error=error_message)

        # Mark message as paid in local DB so UI updates immediately
        db_engine = _get_db_engine(backend_manager, site)
        if db_engine:
            try:
                with Session(db_engine) as session:  # type: ignore[arg-type]
                    db_msg = session.exec(
                        select(MessageModel).where(MessageModel.id == message_id)
                    ).first()
                    if db_msg:
                        db_msg.paid = True
                        session.commit()
            except Exception as db_exc:
                logger.debug("Failed to update paid status in DB: %s", db_exc)

        # Create a scrape+download job so the purchased media gets downloaded
        try:
            await _create_ppv_scrape_job(
                site=site,
                auth_id=int(auth.id),
                user_id=user_id,
                username=getattr(user, "username", None) or getattr(user, "name", None),
                backend_manager=backend_manager,
            )
        except Exception as job_exc:
            logger.warning(
                "Failed to create scrape+download job after PPV purchase: %s",
                job_exc,
            )

        return BuyMessageResponse(success=True, message=None)

    except Exception as exc:
        logger.error(f"Error buying message: {exc}", exc_info=True)
        return BuyMessageResponse(success=False, error=str(exc))


@router.post(
    "/{site}/{user_id}/messages/{message_id}/like",
    response_model=LikeMessageResponse,
)
async def like_message(
    site: str,
    user_id: int,
    message_id: int,
    auth_id: int | None = Query(None, description="Auth ID to use"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> LikeMessageResponse:
    """Toggle like/unlike on a message.

    Calls the OnlyFans API to toggle the like status, then updates the local DB.
    The API is bidirectional: if the message is liked it unlikes, and vice versa.
    """
    try:
        if auth_id is not None:
            auth = await backend_manager.get_authed_session(site, auth_id)
        else:
            auths = await backend_manager.get_all_authed_sessions(site)
            auth = auths[0] if auths else None

        if not auth:
            return LikeMessageResponse(
                success=False, error="No authenticated account available"
            )

        # Fetch the message object to call like_message() with proper signing
        user = None
        auth_obj = auth
        if hasattr(auth_obj, "find_user"):
            user = auth_obj.find_user(user_id)
        if not user and hasattr(auth_obj, "get_user"):
            user = await auth_obj.get_user(str(user_id))

        if not user:
            return LikeMessageResponse(success=False, error=f"User {user_id} not found")

        # Fetch the target message
        message = None
        user_obj: Any = user
        if hasattr(user_obj, "get_messages"):
            try:
                msgs = cast(
                    list[Any],
                    await user_obj.get_messages(limit=20, offset_id=message_id + 1),
                )
                message = next(
                    (m for m in msgs if getattr(m, "id", None) == message_id), None
                )
            except Exception as fetch_exc:
                logger.warning(
                    "get_messages(offset_id=%s) failed: %s", message_id, fetch_exc
                )

        if not message and hasattr(user_obj, "get_message_by_id"):
            try:
                message = await user_obj.get_message_by_id(
                    user_id=user_id, message_id=message_id, limit=10, offset=0
                )
                if not getattr(message, "id", None):
                    message = None
            except Exception as fetch_exc:
                logger.warning(
                    "get_message_by_id(%s) failed: %s", message_id, fetch_exc
                )

        if not message:
            return LikeMessageResponse(
                success=False, error=f"Message {message_id} not found"
            )

        # Call the like toggle via the USAPI model method
        message_obj: Any = message
        like_message = getattr(message_obj, "like_message", None)
        if callable(like_message):
            result = await cast(Any, like_message)()
        else:
            # Fallback: direct requester call
            requester = cast(Any, auth).get_requester()
            like_url = f"https://onlyfans.com/api2/v2/messages/{message_id}/like"
            # Check current liked state from DB to determine method
            currently_liked = False
            db_engine = _get_db_engine(backend_manager, site)
            if db_engine:
                try:
                    with Session(db_engine) as sess:  # type: ignore[arg-type]
                        db_msg_check = sess.exec(
                            select(MessageModel).where(MessageModel.id == message_id)
                        ).first()
                        if db_msg_check:
                            currently_liked = bool(db_msg_check.liked)
                except Exception:
                    pass
            method: str = "DELETE" if currently_liked else "POST"
            result = await requester.json_request(
                like_url, method=method, payload={"withUserId": user_id}
            )

        if isinstance(result, dict) and "error" in result:
            result_map = _stringify_mapping(cast(Mapping[Any, Any], result))
            error_msg = result_map.get("error", {})
            if isinstance(error_msg, dict):
                error_map = _stringify_mapping(cast(Mapping[Any, Any], error_msg))
                detail = str(error_map.get("message", "Like toggle failed"))
            else:
                detail = str(error_msg)
            return LikeMessageResponse(success=False, error=detail)

        # Toggle liked in local DB
        new_liked = True  # Default assumption
        db_engine = _get_db_engine(backend_manager, site)
        if db_engine:
            try:
                with Session(db_engine) as session:  # type: ignore[arg-type]
                    db_msg = session.exec(
                        select(MessageModel).where(MessageModel.id == message_id)
                    ).first()
                    if db_msg:
                        db_msg.liked = not db_msg.liked
                        new_liked = db_msg.liked
                        session.commit()
            except Exception as db_exc:
                logger.debug("Failed to update liked status in DB: %s", db_exc)

        logger.info(
            "Toggled like on message %s (now liked=%s) for user %s",
            message_id,
            new_liked,
            user_id,
        )

        return LikeMessageResponse(success=True, is_liked=new_liked)

    except Exception as exc:
        logger.error(f"Error toggling like on message: {exc}", exc_info=True)
        return LikeMessageResponse(success=False, error=str(exc))
