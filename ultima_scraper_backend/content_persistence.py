"""Persist scraped content and its media rows for BackendManager."""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlmodel import Session, select
from ultima_scraper_api import content_types
from ultima_scraper_db_archive import UserModel, utcnow

from .backend_result import BackendResult

# Records keep the logger name they had while this code lived in manager.py.
logger = logging.getLogger("ultima_scraper_backend.manager")


def _coerce_positive_media_size(value: object) -> int | None:
    try:
        normalized = int(value) if value is not None else 0
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _resolve_usapi_media_size(media_item: object) -> int | None:
    """Return a positive media size from USAPI payloads, or ``None`` if unknown."""

    def _read_attr(value: object, key: str) -> object | None:
        if isinstance(value, Mapping):
            return value.get(key)
        return getattr(value, key, None)

    for candidate in (
        _read_attr(media_item, "size"),
        _read_attr(_read_attr(_read_attr(media_item, "files"), "full"), "size"),
    ):
        normalized = _coerce_positive_media_size(candidate)
        if normalized is not None:
            return normalized
    return None


def _resolve_usapi_media_duration(media_item: object) -> float | None:
    """Return a positive duration in seconds from a USAPI media payload."""

    raw_duration = (
        media_item.get("duration")
        if isinstance(media_item, Mapping)
        else getattr(media_item, "duration", None)
    )
    try:
        duration = float(raw_duration) if raw_duration is not None else 0.0
    except (TypeError, ValueError):
        return None
    return duration if math.isfinite(duration) and duration > 0 else None


class ContentPersistenceMixin:
    """Content and media persistence shared by BackendManager."""

    if TYPE_CHECKING:

        def _normalise_identifier(self, *args, **kwargs): ...

        def _site_session(self, *args, **kwargs): ...

    @staticmethod
    def _extract_bought_content_links(content: object) -> tuple[int | None, int | None]:
        """Map paid content payloads to bought_content link columns."""
        content_id = getattr(content, "id", None)
        if not isinstance(content_id, int):
            return None, None

        response_type = str(getattr(content, "responseType", "")).lower()
        queue_id = getattr(content, "queueId", None)
        content_class_name = type(content).__name__.lower()
        is_mass_message = (
            queue_id is not None
            or "mass" in response_type
            or "mass" in content_class_name
        )

        if response_type == "post":
            return content_id, None
        if response_type == "message" and not is_mass_message:
            return None, content_id
        return None, None

    @staticmethod
    def _read_content_field(content: object, *field_names: str) -> Any:
        """Read a field from an API object or its raw payload fallback."""
        for field_name in field_names:
            value = getattr(content, field_name, None)
            if value is not None:
                return value
            raw = getattr(content, "raw", None)
            if isinstance(raw, dict):
                raw_value = raw.get(field_name)
                if raw_value is not None:
                    return raw_value
        return None

    @staticmethod
    def _extract_drm_manifest_payload(
        media: object,
    ) -> tuple[bool, dict[str, str]]:
        """Extract DRM manifest fields from parsed USAPI media objects."""

        queued_drm = getattr(media, "drm_payload", None)
        if isinstance(queued_drm, Mapping):
            return bool(getattr(media, "drm", False)), {
                str(key): str(value)
                for key, value in queued_drm.items()
                if isinstance(value, str) and value.strip()
            }

        def _append_if_str(target: dict[str, str], key: str, value: Any) -> None:
            if isinstance(value, str):
                normalized = value.strip()
                if normalized:
                    target[key] = normalized

        def _as_mapping(value: object) -> Mapping[str, Any] | None:
            if isinstance(value, Mapping):
                return cast(Mapping[str, Any], value)
            for attr_name in ("__raw__", "raw"):
                raw_value = getattr(value, attr_name, None)
                if isinstance(raw_value, Mapping):
                    return cast(Mapping[str, Any], raw_value)
            return None

        def _read_nested(value: object, *path: str) -> Any:
            current: Any = value
            for key in path:
                if current is None:
                    return None
                mapping = _as_mapping(current)
                if mapping is not None:
                    current = mapping.get(key)
                    continue
                current = getattr(current, key, None)
            return current

        def _append_manifest_fields(
            payload: dict[str, str],
            prefix: str,
            manifest_value: object,
            signature_value: object,
        ) -> None:
            manifest_url = (
                manifest_value
                if isinstance(manifest_value, str)
                else _read_nested(manifest_value, "manifest_url")
            )
            _append_if_str(payload, f"{prefix}_manifest", manifest_url)
            _append_if_str(
                payload,
                f"{prefix}_key_pair_id",
                _read_nested(signature_value, "CloudFront-Key-Pair-Id")
                or _read_nested(manifest_value, "key_pair_id"),
            )
            _append_if_str(
                payload,
                f"{prefix}_policy",
                _read_nested(signature_value, "CloudFront-Policy")
                or _read_nested(manifest_value, "policy"),
            )
            _append_if_str(
                payload,
                f"{prefix}_signature",
                _read_nested(signature_value, "CloudFront-Signature")
                or _read_nested(manifest_value, "signature"),
            )

        files = getattr(media, "files", None)
        drm = getattr(files, "drm", None) if files is not None else None
        raw_media = _as_mapping(media)
        raw_drm = _read_nested(raw_media, "files", "drm") or _read_nested(
            raw_media, "drm"
        )
        if drm is None and raw_drm is None:
            return False, {}

        dash = getattr(drm, "dash", None)
        hls = getattr(drm, "hls", None)
        payload: dict[str, str] = {}

        _append_manifest_fields(
            payload,
            "dash",
            dash if dash is not None else _read_nested(raw_drm, "manifest", "dash"),
            _read_nested(raw_drm, "signature", "dash"),
        )
        _append_manifest_fields(
            payload,
            "hls",
            hls if hls is not None else _read_nested(raw_drm, "manifest", "hls"),
            _read_nested(raw_drm, "signature", "hls"),
        )

        return True, payload

    @classmethod
    def _extract_nested_user_id(cls, value: object) -> int | None:
        """Extract a user identifier from an object, dict, or raw scalar value."""
        normalized = cls._normalise_identifier(value)
        if normalized is not None:
            return normalized

        if isinstance(value, dict):
            for key in ("id", "user_id", "userId"):
                normalized = cls._normalise_identifier(value.get(key))
                if normalized is not None:
                    return normalized
            return None

        for attr_name in ("id", "user_id", "userId"):
            normalized = cls._normalise_identifier(getattr(value, attr_name, None))
            if normalized is not None:
                return normalized
        return None

    @classmethod
    def _extract_content_author_id(
        cls,
        content: object,
        fallback_user_id: int | None = None,
    ) -> int | None:
        """Best-effort author extraction across post/message API payload shapes."""
        get_author = getattr(content, "get_author", None)
        if callable(get_author):
            author_id = cls._extract_nested_user_id(get_author())
            if author_id is not None:
                return author_id

        for field_name in ("author", "fromUser", "from_user", "sender"):
            author_id = cls._extract_nested_user_id(
                cls._read_content_field(content, field_name)
            )
            if author_id is not None:
                return author_id

        direct_user_id = cls._normalise_identifier(
            cls._read_content_field(content, "user_id", "userId")
        )
        if direct_user_id is not None:
            return direct_user_id
        return cls._normalise_identifier(fallback_user_id)

    @classmethod
    def _extract_message_receiver_id(
        cls,
        content: object,
        owner_user_id: int,
        auth_user_id: int | None,
    ) -> int | None:
        """Infer the other side of a message when the API payload omits it."""
        explicit_receiver = cls._extract_nested_user_id(
            cls._read_content_field(content, "receiver", "toUser", "to_user")
        )
        if explicit_receiver is not None:
            return explicit_receiver

        explicit_receiver = cls._normalise_identifier(
            cls._read_content_field(
                content,
                "receiver_id",
                "receiverId",
                "toUserId",
                "to_user_id",
                "withUserId",
                "with_user_id",
                "chatUserId",
                "chat_user_id",
            )
        )
        if explicit_receiver is not None:
            return explicit_receiver

        normalized_auth_id = cls._normalise_identifier(auth_user_id)
        if normalized_auth_id is None:
            return None

        author_id = cls._extract_content_author_id(
            content, fallback_user_id=owner_user_id
        )
        owner_id = cls._normalise_identifier(owner_user_id)
        if author_id is None:
            return normalized_auth_id if owner_id != normalized_auth_id else None
        if author_id == normalized_auth_id:
            return owner_id if owner_id != normalized_auth_id else None
        return normalized_auth_id

    @classmethod
    def _extract_message_queue_id(cls, content: object) -> int | None:
        """Extract queue linkage for queued/mass messages when present."""
        return cls._normalise_identifier(
            cls._read_content_field(content, "queueId", "queue_id", "queueID")
        )

    @staticmethod
    def _normalize_datetime(value: datetime | str | None) -> datetime | None:
        """Normalize datetime-like value to datetime object.

        Args:
            value: datetime object, ISO string, or None

        Returns:
            Normalized datetime object, or ``None`` when no valid timestamp exists.
        """
        if isinstance(value, datetime):
            # Ensure timezone-aware
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            try:
                # Parse ISO format strings
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except (ValueError, AttributeError):
                return None
        return None

    @staticmethod
    def _resolve_media_created_at(media: object, content: object) -> datetime | None:
        """Resolve media.created_at with content.created_at as the only fallback."""

        raw_media_created_at = (
            media.get("created_at") or media.get("createdAt")
            if isinstance(media, Mapping)
            else getattr(media, "created_at", None)
        )
        media_created_at = ContentPersistenceMixin._normalize_datetime(
            cast(datetime | str | None, raw_media_created_at)
        )
        if media_created_at is not None:
            return media_created_at
        return ContentPersistenceMixin._normalize_datetime(getattr(content, "created_at", None))

    @staticmethod
    def _extract_preview_media_ids(content: object) -> set[int]:
        """Collect content-level preview media IDs from common API attributes."""

        preview_media_ids: set[int] = set()
        for attr in ("preview", "preview_ids", "previews"):
            raw_ids = getattr(content, attr, None)
            if not isinstance(raw_ids, list):
                continue
            for raw_id in raw_ids:
                try:
                    preview_media_ids.add(int(raw_id))
                except (TypeError, ValueError):
                    continue
            break
        return preview_media_ids

    @staticmethod
    def _normalize_price(content: content_types) -> float:
        """Extract price from content, ensuring it's never None (database constraint).

        Args:
            content: Content object (PostModel, MessageModel, or StoryModel)

        Returns:
            Price as float, defaulting to 0.0 if None or missing
        """
        price_value = getattr(content, "price", None)
        return float(price_value) if price_value is not None else 0.0

    @staticmethod
    def _extract_paid_status(
        content: content_types,
        *,
        record_bought_content: bool = False,
    ) -> bool | None:
        """Resolve whether paid content has been opened/purchased."""

        if record_bought_content:
            return True

        explicit_paid = getattr(content, "paid", None)
        if isinstance(explicit_paid, bool):
            return explicit_paid

        price_value = ContentPersistenceMixin._normalize_price(content)
        if price_value <= 0:
            return False

        is_bought = getattr(content, "is_bought", None)
        if callable(is_bought):
            try:
                return bool(is_bought())
            except Exception:
                logger.debug(
                    "Failed to evaluate is_bought() for content %s",
                    getattr(content, "id", "unknown"),
                    exc_info=True,
                )

        is_opened = ContentPersistenceMixin._read_content_field(content, "isOpened", "is_opened")
        if isinstance(is_opened, bool):
            return is_opened

        can_view_media = ContentPersistenceMixin._read_content_field(
            content, "canViewMedia", "can_view_media"
        )
        if isinstance(can_view_media, bool):
            return can_view_media

        media_items = getattr(content, "media", None)
        if isinstance(media_items, list) and media_items:
            media_view_states = [
                getattr(media_item, "canView", None) for media_item in media_items
            ]
            known_states = [
                state for state in media_view_states if isinstance(state, bool)
            ]
            if known_states:
                return all(known_states)

        return False

    @staticmethod
    def _merge_paid_status(
        existing_paid: bool | None,
        incoming_paid: bool | None,
    ) -> bool | None:
        """Keep paid/opened content sticky once it has been confirmed true."""

        if existing_paid is True:
            return True
        if incoming_paid is None:
            return existing_paid
        return incoming_paid

    @staticmethod
    def _get_content_text(content: content_types) -> Optional[str]:
        """Extract text from content, trying both 'text' and 'rawText' attributes.

        Args:
            content: Content object (PostModel, MessageModel, or StoryModel)

        Returns:
            Text string or None if not available
        """
        return getattr(content, "text", None) or getattr(content, "rawText", None)

    def _save_media_for_content(
        self,
        session: Any,
        content: content_types,
        user_id: int,
        content_id: int,
        content_type: str,
        *,
        site: str = "onlyfans",
        batch_size: int | None = None,
    ) -> int:
        """Persist bounded media batches, isolating failures by individual retry."""
        from contextlib import ExitStack, closing
        from itertools import batched

        from sqlalchemy import text
        from sqlmodel import select
        from ultima_scraper_core import runtime_env_value
        from ultima_scraper_db_archive import DRMManifestModel
        from ultima_scraper_db_archive import MediaModel as DBMediaModel

        from .database.write_queue import _positive_int_env

        if (
            batch_size is None
            and not (runtime_env_value("ULTIMA_MEDIA_WRITE_BATCH_SIZE") or "").strip()
        ):
            batch_size = _positive_int_env("ULTIMA_CONTENT_BATCH_SIZE", 100)
        batch_size = _positive_int_env(
            "ULTIMA_MEDIA_WRITE_BATCH_SIZE", 100, value=batch_size
        )
        medias = getattr(content, "media", [])
        if not medias:
            return 0
        content_column = {
            "post": "post_id",
            "message": "message_id",
            "story": "story_id",
            "mass_message": "mass_message_id",
        }[content_type]
        association_statement = text(
            f"INSERT INTO content_media_asso (media_id, {content_column}) "
            "SELECT incoming.media_id, :content_id "
            "FROM unnest(CAST(:media_ids AS bigint[])) WITH ORDINALITY "
            "AS incoming(media_id, ordinal) WHERE NOT EXISTS ("
            f"SELECT 1 FROM content_media_asso WHERE {content_column} = :content_id "
            "AND media_id = incoming.media_id) ORDER BY incoming.ordinal "
            f"ON CONFLICT ({content_column}, media_id) DO NOTHING"
        )
        preview_media_ids = self._extract_preview_media_ids(content)

        def save_batch(batch):
            saved_ids = []
            manifest_updates = {}
            with session.begin_nested():
                ids = [media.id for media in batch if media.id]
                # Repeated IDs must retain their original sequential merge
                # behavior. The individual retry path uses the same writer.
                if len({int(value) for value in ids}) != len(ids):
                    raise ValueError("Repeated media IDs require ordered individual writes")
                with closing(
                    session.exec(select(DBMediaModel).where(DBMediaModel.id.in_(ids)))
                ) as rows:
                    existing_by_id = {row.id: row for row in rows}
                for media in batch:
                    media_id = media.id
                    if not media_id:
                        logger.warning("Media item missing ID, skipping")
                        continue
                    existing_media = existing_by_id.get(int(media_id))
                    incoming_is_preview = media_id in preview_media_ids
                    # Media rows are shared across content, so once any content marks
                    # a media ID as preview it should remain preview.
                    is_preview = (
                        incoming_is_preview
                        if existing_media is None
                        else bool(existing_media.preview) or incoming_is_preview
                    )
                    has_drm, drm_payload = self._extract_drm_manifest_payload(media)

                    media_url = None
                    parsed_url = content.url_picker(media)

                    if parsed_url:
                        media_url = parsed_url.geturl()

                    media_size = _resolve_usapi_media_size(media)
                    media_duration = _resolve_usapi_media_duration(media)

                    media_created_at = self._resolve_media_created_at(media, content)

                    if not existing_media:
                        # Create new media record
                        media_payload: dict[str, Any] = {
                            "id": media_id,
                            "user_id": user_id,
                            "url": media_url,
                            "drm": has_drm,
                            "category": getattr(media, "type", None),
                            "preview": is_preview,
                            "created_at": media_created_at,
                        }
                        if media_size is not None:
                            media_payload["size"] = media_size
                        if media_duration is not None:
                            media_payload["duration"] = media_duration
                        db_media = DBMediaModel(
                            **media_payload,
                        )
                        session.add(db_media)
                        logger.debug(
                            f"Saved new media {media_id} for user {user_id} (preview={is_preview})"
                        )
                    else:
                        # Update existing media record
                        existing_media.url = media_url
                        if media_size is not None:
                            existing_media.size = media_size
                        if media_duration is not None and not existing_media.duration:
                            existing_media.duration = media_duration
                        existing_media.drm = existing_media.drm or has_drm
                        existing_media.category = getattr(media, "type", None)
                        existing_media.preview = is_preview
                        if media_created_at is not None and (
                            existing_media.created_at is None
                            or media_created_at < existing_media.created_at
                        ):
                            existing_media.created_at = media_created_at
                        session.add(existing_media)
                        logger.debug(
                            f"Updated existing media {media_id} for user {user_id} (preview={is_preview})"
                        )

                    if has_drm:
                        manifest_updates[media_id] = drm_payload
                    saved_ids.append(media_id)

                session.flush()
                if manifest_updates:
                    with closing(
                        session.exec(
                            select(DRMManifestModel)
                            .where(DRMManifestModel.media_id.in_(manifest_updates))
                            .distinct(DRMManifestModel.media_id)
                            .order_by(DRMManifestModel.media_id, DRMManifestModel.id)
                        )
                    ) as rows:
                        manifests = {row.media_id: row for row in rows}
                    for media_id, payload in manifest_updates.items():
                        existing_manifest = manifests.get(int(media_id))
                        if existing_manifest is None:
                            session.add(DRMManifestModel(media_id=media_id, **payload))
                        else:
                            for name, value in payload.items():
                                setattr(existing_manifest, name, value)
                            existing_manifest.updated_at = utcnow()
                            session.add(existing_manifest)
                if saved_ids:
                    # The anti-join avoids sequence churn on rescrapes; the
                    # unique constraint still handles an overlapping writer.
                    session.execute(
                        association_statement,
                        {"media_ids": saved_ids, "content_id": content_id},
                    ).close()
            return len(saved_ids)

        def save_one(media):
            try:
                return save_batch((media,))
            except Exception as exc:
                logger.warning(
                    "Failed to save media item %s for %s %s: %s",
                    getattr(media, "id", "unknown"),
                    content_type,
                    content_id,
                    exc,
                )
                return 0

        media_count = 0
        with ExitStack() as owned:
            source = iter(medias)
            if (close := getattr(source, "close", None)) is not None:
                owned.callback(close)
            for batch in batched(source, batch_size):
                if len(batch) == 1:
                    media_count += save_one(batch[0])
                    continue
                try:
                    media_count += save_batch(batch)
                except Exception:
                    # The failed group has rolled back. Retry in input order,
                    # retaining the established per-media failure isolation.
                    for media in batch:
                        media_count += save_one(media)
        return media_count

    async def save_content(
        self,
        content: content_types,
        user_id: int,
        db_path: Optional[str] = None,
        *,
        site: str = "onlyfans",
        auth_user_id: Optional[int] = None,
        record_bought_content: bool = False,
        db_session: Optional[Session] = None,
    ) -> BackendResult:
        """Save content (post, message, etc.) to the PostgreSQL database with deduplication.

        Args:
            content: Content object from USAPI (PostModel, MessageModel, etc.)
            user_id: User ID that owns this content
            db_path: DEPRECATED – ignored, kept for call-site compatibility.
            site: Site name used to resolve the correct PostgreSQL schema.
            auth_user_id: Optional auth user id. Used as the buyer for paid-content
                          records when ``record_bought_content`` is true, and as a
                          receiver fallback for message saves.
            record_bought_content: Whether this save originated from the paid-content
                          scraper and should write bought_content rows.
            db_session: Optional pre-configured session (must already have
                        ``search_path`` set to the correct site schema).

        Returns:
            BackendResult with success status and message
        """
        try:
            from ultima_scraper_db_archive import (
                BoughtContentModel,
                ContentMentionModel,
                LinkedPostModel,
            )
            from ultima_scraper_db_archive import (
                MassMessageModel as DBMassMessageModel,
            )
            from ultima_scraper_db_archive import (
                MessageModel as DBMessageModel,
            )
            from ultima_scraper_db_archive import PostModel as DBPostModel
            from ultima_scraper_db_archive import StoryModel as DBStoryModel

            # Determine content type
            content_type = type(content).__name__
            supplier_user_id = self._extract_content_author_id(
                content, fallback_user_id=user_id
            )
            buyer_user_id = self._normalise_identifier(auth_user_id)
            paid_status = self._extract_paid_status(
                content, record_bought_content=record_bought_content
            )

            def _persist(session: Session) -> None:
                # The caller-owned path is committed by its batch coordinator.
                # The owned path commits once after the whole content/media save,
                # so a source-authority replacement and its ledger handoff cannot
                # be separated by an intermediate PostgreSQL commit.
                session.flush()

            def _ensure_user_row(session: Session, target_user_id: int | None) -> None:
                if target_user_id is None or target_user_id <= 0:
                    return
                existing_user = session.get(UserModel, target_user_id)
                if existing_user is None:
                    session.add(UserModel(id=target_user_id))

            def _save_post_mentions(
                session: Session,
                post_id: int,
                source_content: object = content,
            ) -> None:
                """Persist API-provided post mentions as content evidence."""
                mentions = (
                    self._read_content_field(
                        source_content, "linkedUsers", "linked_users"
                    )
                    or []
                )
                if not isinstance(mentions, (list, tuple)):
                    return
                for mention in mentions:
                    mentioned_user_id = self._extract_nested_user_id(mention)
                    raw_mention = self._read_content_field(mention, "username", "name")
                    if not isinstance(raw_mention, str) or not raw_mention.strip():
                        raw_mention = (
                            str(mentioned_user_id)
                            if mentioned_user_id is not None
                            else ""
                        )
                    raw_mention = raw_mention.strip()
                    if not raw_mention:
                        continue
                    normalized = raw_mention.lstrip("@").casefold()
                    _ensure_user_row(session, mentioned_user_id)
                    existing_mention = session.exec(
                        select(ContentMentionModel).where(
                            ContentMentionModel.post_id == post_id,
                            ContentMentionModel.normalized_mention == normalized,
                            ContentMentionModel.mention_kind == "platform_tag",
                        )
                    ).first()
                    if existing_mention is None:
                        session.add(
                            ContentMentionModel(
                                post_id=post_id,
                                mentioned_user_id=mentioned_user_id,
                                raw_mention=raw_mention,
                                normalized_mention=normalized,
                                mention_kind="platform_tag",
                            )
                        )

            def _save_linked_posts(session: Session, post_id: int) -> None:
                """Persist linked post rows and their referentially required posts."""
                linked_posts = (
                    self._read_content_field(content, "linkedPosts", "linked_posts")
                    or []
                )
                if not isinstance(linked_posts, (list, tuple)):
                    return

                for linked_content in linked_posts:
                    linked_post_id = self._normalise_identifier(
                        self._read_content_field(
                            linked_content, "id", "post_id", "postId"
                        )
                    )
                    if linked_post_id is None or linked_post_id == post_id:
                        continue

                    linked_user_id = self._extract_content_author_id(
                        linked_content,
                        fallback_user_id=user_id,
                    )
                    if linked_user_id is None:
                        continue
                    _ensure_user_row(session, linked_user_id)

                    linked_post = session.get(DBPostModel, linked_post_id)
                    if linked_post is None:
                        linked_media = getattr(linked_content, "media", None) or []
                        linked_post = DBPostModel(
                            id=linked_post_id,
                            user_id=linked_user_id,
                            text=self._get_content_text(linked_content),
                            price=self._normalize_price(linked_content),
                            paid=self._extract_paid_status(linked_content),
                            media_count=len(linked_media),
                            created_at=self._normalize_datetime(
                                getattr(linked_content, "created_at", None)
                            ),
                        )
                        session.add(linked_post)
                        _persist(session)
                        self._save_media_for_content(
                            session,
                            linked_content,
                            linked_user_id,
                            linked_post_id,
                            "post",
                            site=site,
                        )
                        _save_post_mentions(session, linked_post_id, linked_content)

                    existing_link = session.exec(
                        select(LinkedPostModel).where(
                            LinkedPostModel.post_id == post_id,
                            LinkedPostModel.linked_post_id == linked_post_id,
                        )
                    ).first()
                    if existing_link is None:
                        session.add(
                            LinkedPostModel(
                                post_id=post_id,
                                linked_post_id=linked_post_id,
                            )
                        )

            def _save_bought_content_link(session: Session) -> None:
                if not record_bought_content:
                    return
                if supplier_user_id is None or buyer_user_id is None:
                    return

                post_id, message_id = self._extract_bought_content_links(content)
                if post_id is None and message_id is None:
                    return

                _ensure_user_row(session, supplier_user_id)
                _ensure_user_row(session, buyer_user_id)

                if post_id is not None:
                    existing_link = session.exec(
                        select(BoughtContentModel).where(
                            BoughtContentModel.buyer_id == buyer_user_id,
                            BoughtContentModel.post_id == post_id,
                        )
                    ).first()
                else:
                    existing_link = session.exec(
                        select(BoughtContentModel).where(
                            BoughtContentModel.buyer_id == buyer_user_id,
                            BoughtContentModel.message_id == message_id,
                        )
                    ).first()

                if existing_link is None:
                    session.add(
                        BoughtContentModel(
                            supplier_id=supplier_user_id,
                            buyer_id=buyer_user_id,
                            post_id=post_id,
                            message_id=message_id,
                        )
                    )
                    _persist(session)
                    return

                if existing_link.supplier_id != supplier_user_id:
                    existing_link.supplier_id = supplier_user_id
                    session.add(existing_link)
                    _persist(session)

            def _resolve_message_queue_id(session: Session) -> int | None:
                raw_queue_id = self._extract_message_queue_id(content)
                if raw_queue_id is None:
                    return None
                existing_queue = session.get(DBMassMessageModel, raw_queue_id)
                if existing_queue is None:
                    logger.debug(
                        "Skipping queue_id=%s for message %s because x_mass_messages row is missing",
                        raw_queue_id,
                        getattr(content, "id", "unknown"),
                    )
                    return None
                return raw_queue_id

            def _do_save(session: Session) -> BackendResult:
                if "Post" in content_type:
                    # Scrapes persist the performer first; this keeps paid-feed
                    # authors and other callers from failing x_posts_user_id_fkey.
                    _ensure_user_row(session, user_id)
                    # Check if post already exists (update or insert)
                    existing = session.exec(
                        select(DBPostModel).where(DBPostModel.id == content.id)
                    ).first()
                    if existing:
                        # Update existing post
                        existing.text = self._get_content_text(content)
                        existing.price = self._normalize_price(content)
                        existing.paid = self._merge_paid_status(
                            existing.paid, paid_status
                        )
                        existing.media_count = len(content.media)
                        # Don't update created_at, but update modified timestamp if available
                        session.add(existing)
                        _persist(session)

                        # Save media and associations
                        media_saved = self._save_media_for_content(
                            session,
                            content,
                            user_id,
                            content.id,
                            "post",
                            site=site,
                        )
                        _save_post_mentions(session, content.id)
                        _save_linked_posts(session, content.id)
                        _persist(session)
                        _save_bought_content_link(session)

                        logger.debug(
                            f"Updated existing post {content.id} for user {user_id}, saved {media_saved} media items"
                        )
                        return BackendResult(
                            True,
                            f"Post {content.id} updated successfully with {media_saved} media items",
                        )

                    # Create new post record
                    db_post = DBPostModel(
                        id=content.id,
                        user_id=user_id,
                        text=self._get_content_text(content),
                        price=self._normalize_price(content),
                        paid=paid_status,
                        media_count=len(content.media),
                        created_at=self._normalize_datetime(content.created_at),
                    )
                    session.add(db_post)
                    _persist(session)

                    # Save media and associations
                    media_saved = self._save_media_for_content(
                        session,
                        content,
                        user_id,
                        content.id,
                        "post",
                        site=site,
                    )
                    _save_post_mentions(session, content.id)
                    _save_linked_posts(session, content.id)
                    _persist(session)
                    _save_bought_content_link(session)

                    logger.debug(
                        f"Saved post {content.id} for user {user_id} with {media_saved} media items"
                    )
                    return BackendResult(
                        True,
                        f"Post {content.id} saved successfully with {media_saved} media items",
                    )

                elif "MassMessage" in content_type:
                    expires_at = self._normalize_datetime(
                        getattr(content, "expires_at", None)
                        or self._read_content_field(content, "expiredAt", "expiresAt")
                        or getattr(content, "created_at", None)
                    )
                    _ensure_user_row(session, user_id)

                    existing = session.exec(
                        select(DBMassMessageModel).where(
                            DBMassMessageModel.id == content.id
                        )
                    ).first()

                    if existing:
                        existing.text = self._get_content_text(content)
                        existing.price = self._normalize_price(content)
                        existing.media_count = len(getattr(content, "media", []) or [])
                        if existing.media_count == 0:
                            existing.media_count = int(
                                getattr(content, "media_count", 0) or 0
                            )
                        existing.expires_at = expires_at
                        session.add(existing)
                        _persist(session)

                        logger.debug(
                            f"Updated existing mass message {content.id} for user {user_id}"
                        )
                        return BackendResult(
                            True,
                            f"Mass message {content.id} updated successfully",
                        )

                    db_mass_message = DBMassMessageModel(
                        id=content.id,
                        user_id=user_id,
                        text=self._get_content_text(content),
                        price=self._normalize_price(content),
                        media_count=(
                            len(getattr(content, "media", []) or [])
                            or int(getattr(content, "media_count", 0) or 0)
                        ),
                        expires_at=expires_at,
                        created_at=self._normalize_datetime(content.created_at),
                    )
                    session.add(db_mass_message)
                    _persist(session)

                    logger.debug(f"Saved mass message {content.id} for user {user_id}")
                    return BackendResult(
                        True,
                        f"Mass message {content.id} saved successfully",
                    )

                elif "Message" in content_type:
                    receiver_id = self._extract_message_receiver_id(
                        content,
                        user_id,
                        buyer_user_id,
                    )
                    queue_id = _resolve_message_queue_id(session)
                    _ensure_user_row(session, user_id)
                    _ensure_user_row(session, receiver_id)

                    # Check if message already exists (update or insert)
                    existing = session.exec(
                        select(DBMessageModel).where(DBMessageModel.id == content.id)
                    ).first()

                    if existing:
                        # Update existing message
                        existing.text = self._get_content_text(content)
                        existing.price = self._normalize_price(content)
                        existing.paid = self._merge_paid_status(
                            existing.paid, paid_status
                        )
                        existing.media_count = len(content.media)
                        if receiver_id is not None:
                            existing.receiver_id = receiver_id
                        if queue_id is not None:
                            existing.queue_id = queue_id
                        # Don't update created_at, but update modified timestamp if available
                        session.add(existing)
                        _persist(session)

                        # Save media and associations
                        media_saved = self._save_media_for_content(
                            session,
                            content,
                            user_id,
                            content.id,
                            "message",
                            site=site,
                        )
                        _persist(session)
                        _save_bought_content_link(session)

                        logger.debug(
                            f"Updated existing message {content.id} for user {user_id}, saved {media_saved} media items"
                        )
                        return BackendResult(
                            True,
                            f"Message {content.id} updated successfully with {media_saved} media items",
                        )

                    # Create new message record
                    db_message = DBMessageModel(
                        id=content.id,
                        user_id=user_id,
                        receiver_id=receiver_id,
                        queue_id=queue_id,
                        text=self._get_content_text(content),
                        price=self._normalize_price(content),
                        paid=paid_status,
                        media_count=len(content.media),
                        created_at=self._normalize_datetime(content.created_at),
                    )
                    session.add(db_message)
                    _persist(session)

                    # Save media and associations
                    media_saved = self._save_media_for_content(
                        session,
                        content,
                        user_id,
                        content.id,
                        "message",
                        site=site,
                    )
                    _persist(session)
                    _save_bought_content_link(session)

                    logger.debug(
                        f"Saved message {content.id} for user {user_id} with {media_saved} media items"
                    )
                    return BackendResult(
                        True,
                        f"Message {content.id} saved successfully with {media_saved} media items",
                    )

                elif "Story" in content_type:
                    _ensure_user_row(session, user_id)
                    # Check if story already exists (update or insert)
                    existing = session.exec(
                        select(DBStoryModel).where(DBStoryModel.id == content.id)
                    ).first()

                    if existing:
                        # Update existing story - stories only have id, user_id, and created_at
                        # so there's not much to update except media associations
                        session.add(existing)
                        _persist(session)

                        # Save media and associations
                        media_saved = self._save_media_for_content(
                            session,
                            content,
                            user_id,
                            content.id,
                            "story",
                            site=site,
                        )
                        _persist(session)

                        logger.debug(
                            f"Updated existing story {content.id} for user {user_id}, saved {media_saved} media items"
                        )
                        return BackendResult(
                            True,
                            f"Story {content.id} updated successfully with {media_saved} media items",
                        )

                    # Create new story record
                    db_story = DBStoryModel(
                        id=content.id,
                        user_id=user_id,
                        created_at=self._normalize_datetime(content.created_at),
                    )
                    session.add(db_story)
                    _persist(session)

                    # Save media and associations
                    media_saved = self._save_media_for_content(
                        session,
                        content,
                        user_id,
                        content.id,
                        "story",
                        site=site,
                    )
                    _persist(session)

                    logger.debug(
                        f"Saved story {content.id} for user {user_id} with {media_saved} media items"
                    )
                    return BackendResult(
                        True,
                        f"Story {content.id} saved successfully with {media_saved} media items",
                    )

                else:
                    return BackendResult(
                        False, f"Unsupported content type: {content_type}"
                    )

            # Use provided session or open a site-scoped one
            if db_session is not None:
                return _do_save(db_session)
            with self._site_session(site) as session:
                result = _do_save(session)
                if result.success:
                    session.commit()
                else:
                    session.rollback()
            return result

        except Exception as exc:
            logger.exception("Failed to save content: %s", exc)
            return BackendResult(False, f"Failed to save content: {exc}")
