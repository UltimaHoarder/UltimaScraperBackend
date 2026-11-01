"""
Metadata models for the job system download architecture.

These models provide an intermediate layer between API models and the download system,
enabling clean separation of concerns and type-safe data transformation.

Architecture:
    API Model → Metadata Model → DownloadItem
                     ↓
                 DB Model (optional)

Key Features:
- Type-safe representation of media and content
- Handles OnlyFans and Fansly API differences
- Supports dict and object input formats
- Automatic preview detection
- URL extraction from multiple API formats
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from inflection import pluralize
from ultima_scraper_api import MediaType as APIMediaType
from ultima_scraper_api.apis.onlyfans.classes.only_drm import (
    DRMMedia,
)

from ultima_scraper_db_archive import content_types as db_content_types

from .path_formatter import FormatAttributes, PathFormatter

if TYPE_CHECKING:
    from .download_manager import DownloadItem

__all__ = ["ContentMetadata", "MediaMetadata"]

logger = logging.getLogger(__name__)


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except (OSError, OverflowError, ValueError):
            return None
    return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        mapping = cast(dict[Any, Any], value)
        return {str(key): item for key, item in mapping.items()}
    return None


def _append_unique_url(urls: list[str], url_value: object) -> None:
    if not url_value:
        return
    url = str(url_value)
    if url.startswith(("http://", "https://")) and url not in urls:
        urls.append(url)


def _normalize_media_type_token(value: object) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "other"

    media_type: APIMediaType | None
    if isinstance(value, APIMediaType):
        media_type = value
    else:
        media_type = APIMediaType.from_api_type(raw)
        if media_type is None:
            try:
                media_type = APIMediaType(raw)
            except ValueError:
                media_type = None

    if media_type is APIMediaType.AUDIOS:
        return "audio"
    if media_type is not None:
        return media_type.value

    return pluralize(raw)


@dataclass(frozen=True)
class _MediaRenderContext:
    download_setup: Any
    active_directory_format: Path
    site_name: str
    authed_username: str
    user_username: str


@dataclass
class MediaMetadata:
    """
    Metadata for a single media item (image, video, audio).

    Represents media from API responses with normalized fields across different sites.
    Can be converted to DownloadItem for the download system.

    Attributes:
        media_id: Unique media identifier
        media_type: Category - "images", "videos", "audio", "other"
        urls: List of download URLs (primary URL first)
        content_id: Parent content ID (post, message, story)
        user_id: Creator user ID
        created_at: Media creation timestamp
        preview: Whether this is a preview/teaser/thumbnail
        size: File size in bytes
        drm: Whether media has DRM protection
        drm_key: DRM decryption key (if available)
        filename: Target filename (set during path formatting)
        directory: Target directory (set during path formatting)
    """

    media_id: int
    media_type: str
    urls: list[str]
    content_id: int
    user_id: int
    created_at: datetime
    preview: bool = False
    size: int = 0
    drm: bool = False
    drm_key: str = ""
    drm_media: DRMMedia | None = field(default=None, repr=False)
    filename: str | None = None
    directory: Path | None = None
    content_metadata: ContentMetadata | None = field(default=None, repr=False)

    # Raw API data for debugging
    __raw__: dict[str, Any] = field(default_factory=lambda: {}, repr=False)

    @classmethod
    def from_api_media(
        cls,
        media_item: dict[str, Any] | Any,
        content_id: int,
        user_id: int,
        api_type: str,
        created_at: datetime | None = None,
        preview_media_ids: list[int] | None = None,
    ) -> MediaMetadata:
        """
        Create MediaMetadata from API media item.

        Supports both dict and object formats for compatibility with different API clients.
        Automatically extracts URLs from OnlyFans, Fansly, and legacy formats.

        Args:
            media_item: Media item from API (dict or object with attributes)
            content_id: Parent content ID
            user_id: Creator user ID
            api_type: Content type (posts, messages, stories)
            created_at: Creation timestamp (falls back to media timestamp or now)
            preview_media_ids: List of media IDs marked as previews

        Returns:
            MediaMetadata instance with normalized fields

        Examples:
            >>> # OnlyFans format
            >>> media = MediaMetadata.from_api_media(
            ...     media_item={"id": 123, "files": {"full": {"url": "https://..."}}},
            ...     content_id=456,
            ...     user_id=789,
            ...     api_type="posts"
            ... )
            >>>
            >>> # Fansly format
            >>> media = MediaMetadata.from_api_media(
            ...     media_item={"id": 123, "locations": [{"location": "https://..."}]},
            ...     content_id=456,
            ...     user_id=789,
            ...     api_type="messages"
            ... )
        """

        # Helper to safely get value from dict or object
        def get_value(obj: Any, key: str, default: Any = None) -> Any:
            """Extract value from dict or object attribute."""
            obj_dict = _as_dict(obj)
            if obj_dict is not None:
                return obj_dict.get(key, default)
            return getattr(obj, key, default)

        # Extract and validate media ID
        media_id = get_value(media_item, "id")
        if not media_id:
            raise ValueError("Media item has no ID")
        normalized_media_id = _coerce_int(media_id)
        if normalized_media_id is None:
            raise ValueError(f"Media item has invalid ID: {media_id}")

        # Determine media type from type field or mimetype
        raw_media_type = get_value(media_item, "type")
        if not raw_media_type:
            # Try mimetype as fallback (e.g., "image/jpeg" → "image")
            mimetype = get_value(media_item, "mimetype")
            if mimetype and "/" in mimetype:
                raw_media_type = mimetype.split("/")[0]
            else:
                raise ValueError(f"Media {media_id} has no type or mimetype")

        api_media_type = APIMediaType.from_api_type(str(raw_media_type))
        if api_media_type is None:
            raise ValueError(f"Media {media_id} has unsupported type: {raw_media_type}")
        media_type = _normalize_media_type_token(api_media_type)

        # Extract URLs from various API formats
        urls: list[str] = []

        def add_url(url_value: Any) -> None:
            """Normalize and add URL to list."""
            if not url_value:
                return

            # Convert to string
            if isinstance(url_value, bytes):
                url_str = url_value.decode("utf-8")
            elif isinstance(url_value, str):
                url_str = url_value
            else:
                url_str = str(url_value)

            # Validate and add
            if url_str and url_str.startswith(("http://", "https://")):
                urls.append(url_str)

        # Extract URLs from different API formats
        # OnlyFans: media.files.full.url
        files = get_value(media_item, "files")
        if files:
            if hasattr(files, "full") and hasattr(files.full, "url"):
                add_url(files.full.url)
            else:
                files_dict = _as_dict(files)
                if files_dict is not None:
                    full_file = _as_dict(files_dict.get("full"))
                    if full_file is not None:
                        add_url(full_file.get("url"))

        # Fansly: media.locations[0].location
        locations = get_value(media_item, "locations")
        if locations and isinstance(locations, list) and locations:
            location_url = get_value(locations[0], "location")
            if location_url:
                add_url(location_url)

        # Legacy: media.source.source or media.source (string)
        source = get_value(media_item, "source")
        if source:
            if isinstance(source, str):
                add_url(source)
            else:
                source_dict = _as_dict(source)
                if source_dict is not None:
                    add_url(source_dict.get("source"))
                elif hasattr(source, "source"):
                    add_url(source.source)

        # Simple: media.url or media.link
        add_url(get_value(media_item, "url"))
        add_url(get_value(media_item, "link"))

        # Remove duplicates (preserving order)
        urls = list(dict.fromkeys(urls))

        # Parse created_at timestamp
        if not created_at:
            created_at_raw = get_value(media_item, "createdAt")
            if not created_at_raw:
                raise ValueError(f"Media {media_id} has no createdAt timestamp")

            if isinstance(created_at_raw, str):
                try:
                    created_at = datetime.fromisoformat(created_at_raw)
                except (ValueError, TypeError) as e:
                    raise ValueError(
                        f"Invalid createdAt timestamp for media {media_id}: {created_at_raw}"
                    ) from e
            elif isinstance(created_at_raw, (int, float)):
                try:
                    created_at = datetime.fromtimestamp(created_at_raw)
                except (ValueError, OSError) as e:
                    raise ValueError(
                        f"Invalid timestamp for media {media_id}: {created_at_raw}"
                    ) from e
            else:
                raise ValueError(
                    f"Unsupported createdAt type for media {media_id}: {type(created_at_raw)}"
                )

        # Extract DRM info
        drm = bool(get_value(media_item, "drm"))
        drm_key_value = get_value(media_item, "drm_key")
        drm_key = drm_key_value if drm_key_value is not None else ""
        drm_media: Any | None = None
        files_value = get_value(media_item, "files")
        drm_value = get_value(files_value, "drm") if files_value else None
        dash_value = get_value(drm_value, "dash") if drm_value else None
        drm_media_candidate = (
            get_value(dash_value, "__drm_media__") if dash_value else None
        )
        if drm_media_candidate is not None:
            drm_media = drm_media_candidate
            drm = True

        # Extract file size
        size_value = get_value(media_item, "size")
        size = size_value if size_value is not None else 0

        # Detect preview media
        is_preview = bool(
            preview_media_ids and normalized_media_id in preview_media_ids
        )
        if is_preview:
            logger.debug(
                f"Media {normalized_media_id} marked as preview (in list: {preview_media_ids})"
            )

        # Create instance
        return cls(
            media_id=normalized_media_id,
            media_type=media_type,
            urls=urls,
            content_id=content_id,
            user_id=user_id,
            created_at=created_at,
            preview=is_preview,
            size=size,
            drm=drm,
            drm_key=drm_key,
            drm_media=drm_media,
            __raw__=_as_dict(media_item)
            or (media_item.__dict__ if hasattr(media_item, "__dict__") else {}),
        )

    @property
    def __content_metadata__(self) -> ContentMetadata | None:
        return self.content_metadata

    def _resolve_render_context(
        self,
        *,
        api_user: Any,
        directory_hint: Path | None = None,
        directory_format_override: Path | None = None,
        download_setup: Any | None = None,
    ) -> _MediaRenderContext | None:
        if self.content_metadata is None:
            return None

        authed = getattr(api_user, "get_authed", lambda: None)()
        api = getattr(authed, "get_api", lambda: None)() if authed is not None else None
        if download_setup is None:
            # A worker's site API carries the slimmer API config, without the
            # download settings: callers that have the backend's pass them in.
            config = getattr(api, "config", None)
            download_setup = getattr(getattr(config, "settings", None), "download_setup", None)
        if download_setup is None:
            return None
        active_directory_format = Path(
            directory_format_override or download_setup.directory_format
        )
        usernames = list(getattr(api_user, "get_usernames", lambda: [])() or [])
        user_username = usernames[0] if usernames else f"u{self.user_id}"
        if directory_hint is not None:
            for username in usernames:
                if username and username in directory_hint.parts:
                    user_username = username
                    break

        return _MediaRenderContext(
            download_setup=download_setup,
            active_directory_format=active_directory_format,
            site_name=str(
                getattr(api, "site_name", getattr(api_user, "site_name", ""))
            ),
            authed_username=str(getattr(authed, "username", "") or ""),
            user_username=user_username,
        )

    def _build_format_attributes(
        self,
        base_name: str,
        *,
        render_context: _MediaRenderContext,
    ) -> FormatAttributes:
        content_metadata = self.content_metadata
        if content_metadata is None:
            raise ValueError(
                f"MediaMetadata {self.media_id} has no content metadata for formatting"
            )

        normalized_base_name = base_name.replace(".mpd", ".mp4")
        content_value = "Free"
        if content_metadata.price > 0:
            content_value = (
                "Paid" if content_metadata.paid and not self.preview else "Previews"
            )

        format_attrs = FormatAttributes()
        format_attrs.site_name = render_context.site_name
        format_attrs.user_id = str(self.user_id)
        format_attrs.user_username = render_context.user_username
        format_attrs.first_username_letter = (
            render_context.user_username[0].upper()
            if render_context.user_username
            else "0"
        )
        format_attrs.first_letter = format_attrs.first_username_letter
        format_attrs.first_id_digit = str(self.user_id)[0] if self.user_id else "0"
        format_attrs.content_type = str(content_metadata.api_type)
        format_attrs.content_value = str(content_value)
        format_attrs.media_type = _normalize_media_type_token(self.media_type)
        format_attrs.content_id = str(content_metadata.content_id)
        format_attrs.media_id = str(self.media_id)
        format_attrs.content_date = content_metadata.created_at
        format_attrs.content_text = content_metadata.text or ""
        format_attrs.authed_username = render_context.authed_username
        format_attrs.media_filename = (
            Path(normalized_base_name).stem or f"media_{self.media_id}"
        )
        format_attrs.media_ext = Path(normalized_base_name).suffix.lstrip(".") or "bin"
        return format_attrs

    def _render_formatted_filename(
        self,
        fallback_filename: str,
        *,
        render_context: _MediaRenderContext,
        format_attrs: FormatAttributes,
    ) -> str:
        formatter = PathFormatter(
            directory_format=Path(),
            filename_format=render_context.download_setup.filename_format,
            date_format=render_context.download_setup.date_format,
            text_length=render_context.download_setup.text_length,
        )
        formatted_filename = formatter.format_path(
            format_attrs,
            str(render_context.download_setup.filename_format),
            render_context.download_setup.date_format,
            render_context.download_setup.text_length,
        )
        return Path(formatted_filename).name or fallback_filename

    def to_download_item(
        self,
        filename: str | None = None,
        directory: Path | None = None,
        *,
        api_user: Any | None = None,
        base_directory: Path | None = None,
        directory_format_override: Path | None = None,
        render_full_directory_format: bool = False,
        download_setup: Any | None = None,
    ) -> DownloadItem:
        """
        Convert to DownloadItem for the download manager.

        Args:
            filename: Target filename
            directory: Target directory path
            api_user: Optional API user associated with this media
            base_directory: Fallback base directory when an explicit directory
                is not provided
            directory_format_override: Optional directory format selected by
                drive resolution for this task
            render_full_directory_format: Whether *base_directory* should be
                treated as the drive root and the full selected directory format
                rendered under it

        Returns:
            DownloadItem ready for download queue

        Raises:
            ValueError: If media has no URLs
        """
        from .download_manager import DownloadItem

        if not self.urls and self.drm_media is None:
            raise ValueError(f"MediaMetadata {self.media_id} has no URLs to download")
        resolved_url = (
            self.urls[0]
            if self.urls
            else str(getattr(self.drm_media, "manifest_url", "") or "")
        )
        if not resolved_url:
            raise ValueError(f"MediaMetadata {self.media_id} has no primary URL")

        target_filename = filename or self.filename or self.extract_filename_from_url()
        target_filename = target_filename.replace(".mpd", ".mp4")
        target_directory = directory or self.directory or base_directory

        if (
            api_user is not None
            and base_directory is not None
            and self.content_metadata is not None
        ):
            render_context = self._resolve_render_context(
                api_user=api_user,
                directory_hint=base_directory,
                directory_format_override=directory_format_override,
                download_setup=download_setup,
            )
            if render_context is not None:
                formatter = PathFormatter(
                    directory_format=render_context.active_directory_format,
                    filename_format=render_context.download_setup.filename_format,
                    date_format=render_context.download_setup.date_format,
                    text_length=render_context.download_setup.text_length,
                )
                format_attrs = self._build_format_attributes(
                    target_filename,
                    render_context=render_context,
                )

                if render_full_directory_format:
                    formatted_dir = formatter.format_path(
                        format_attrs,
                        str(render_context.active_directory_format),
                        render_context.download_setup.date_format,
                        render_context.download_setup.text_length,
                    )
                    target_directory = base_directory / Path(formatted_dir)
                else:
                    file_path_format = formatter.get_file_path_format()
                    if file_path_format:
                        formatted_dir = formatter.format_path(
                            format_attrs,
                            file_path_format,
                            render_context.download_setup.date_format,
                            render_context.download_setup.text_length,
                        )
                        target_directory = base_directory / Path(formatted_dir)
                    else:
                        target_directory = base_directory

                target_filename = self._render_formatted_filename(
                    target_filename,
                    render_context=render_context,
                    format_attrs=format_attrs,
                )

        if target_directory is None:
            raise ValueError(
                f"MediaMetadata {self.media_id} has no target directory configured"
            )

        return DownloadItem(
            url=resolved_url,
            filename=target_filename,
            directory=target_directory,
            media_id=self.media_id if self.media_id else None,
            content_id=self.content_id if self.content_id else None,
            size=self.size,
            user=api_user,
            media_metadata=self,
        )

    def render_target_filename(
        self,
        base_name: str,
        *,
        api_user: Any | None = None,
        directory_hint: Path | None = None,
    ) -> str:
        target_filename = base_name.replace(".mpd", ".mp4")
        if api_user is None:
            return target_filename

        render_context = self._resolve_render_context(
            api_user=api_user,
            directory_hint=directory_hint,
        )
        if render_context is None:
            return target_filename

        format_attrs = self._build_format_attributes(
            target_filename,
            render_context=render_context,
        )
        return self._render_formatted_filename(
            target_filename,
            render_context=render_context,
            format_attrs=format_attrs,
        )

    def extract_filename_from_url(self) -> str:
        """
        Extract filename from primary URL.

        Returns:
            Filename from URL path

        Raises:
            ValueError: If no URLs available or filename cannot be extracted
        """
        if not self.urls and self.drm_media is None:
            raise ValueError(f"Media {self.media_id} has no URLs")

        url = (
            self.urls[0]
            if self.urls
            else str(getattr(self.drm_media, "manifest_url", "") or "")
        )

        # Parse and extract filename
        parsed = urlparse(url)
        if not parsed.path:
            raise ValueError(f"Media {self.media_id} URL has no path: {url}")

        filename = Path(parsed.path).name
        if not filename:
            raise ValueError(f"Media {self.media_id} URL path has no filename: {url}")
        filename = filename.replace(".mpd", ".mp4")

        return filename


@dataclass
class ContentMetadata:
    """
    Metadata for a content item (post, message, story).

    Represents content from API responses with nested media items.
    Used for bulk operations and database storage.

    Attributes:
        content_id: Unique content identifier
        api_type: Content category (Posts, Messages, Stories)
        user_id: Creator user ID
        created_at: Content creation timestamp
        medias: List of attached media items
        text: Content text/caption
        price: Content price (0 for free)
        paid: Whether content is locked behind paywall
        archived: Whether content is archived
        preview_media_ids: IDs of media that are previews
        receiver_id: Receiver user ID (for messages)
        queue_id: Queue ID (for stories)
    """

    content_id: int
    api_type: str
    user_id: int
    created_at: datetime
    medias: list[MediaMetadata] = field(
        default_factory=lambda: cast(list[MediaMetadata], [])
    )
    text: str | None = None
    price: float = 0.0
    paid: bool = False
    archived: bool = False
    expires_at: datetime | None = None
    preview_media_ids: list[int] = field(default_factory=lambda: cast(list[int], []))
    receiver_id: int | None = None
    queue_id: int | None = None

    # Raw API data for debugging
    __raw__: dict[str, Any] | None = field(default=None, repr=False)

    @classmethod
    def from_api_content(
        cls,
        content_item: Any,
        api_type: str,
    ) -> ContentMetadata:
        """
        Create ContentMetadata from API content item.

        Args:
            content_item: Content from API (PostModel, MessageModel, etc.)
            api_type: Content category (Posts, Messages, Stories)

        Returns:
            ContentMetadata instance with nested MediaMetadata items
        """
        # Extract core fields
        content_id = _coerce_int(getattr(content_item, "id", None))
        author = getattr(content_item, "get_author", lambda: None)()
        user_id = _coerce_int(getattr(author, "id", None))
        if content_id is None or user_id is None:
            raise ValueError("Content item is missing required ID fields")

        # Extract text/caption
        raw_text = getattr(content_item, "rawText", None)
        text_value = (
            raw_text if raw_text is not None else getattr(content_item, "text", None)
        )
        text = text_value if isinstance(text_value, str) else None

        # Parse timestamp
        created_at_raw = getattr(content_item, "created_at", None)
        if not created_at_raw:
            raise ValueError(f"Content {content_id} has no created_at timestamp")
        created_at = _coerce_datetime(created_at_raw)
        if created_at is None:
            raise ValueError(
                f"Unsupported created_at type for content {content_id}: {type(created_at_raw)}"
            )

        # Extract price and paid status
        price_value = getattr(content_item, "price", None)
        if price_value is None:
            raise ValueError(f"Content {content_id} has no price attribute")
        price = float(price_value)

        paid = False
        if price > 0 and hasattr(content_item, "media"):
            # Check if user can view all media (means they purchased)
            paid = all(getattr(m, "canView", False) for m in content_item.media)

        # Extract metadata
        archived_value = getattr(content_item, "isArchived", None)
        if archived_value is None:
            raise ValueError(f"Content {content_id} has no isArchived attribute")
        archived = bool(archived_value)

        # Extract preview media IDs
        preview_media_ids: list[int] = []
        for attr in ("preview", "preview_ids", "previews"):
            ids = getattr(content_item, attr, None)
            if isinstance(ids, list):
                ids_list = cast(list[Any], ids)
                preview_media_ids = [
                    preview_id
                    for preview_id in (_coerce_int(x) for x in ids_list)
                    if preview_id is not None
                ]
                break

        # Create content metadata instance
        content_metadata = cls(
            content_id=content_id,
            api_type=api_type,
            user_id=user_id,
            created_at=created_at,
            text=text,
            price=price,
            paid=paid,
            archived=archived,
            preview_media_ids=preview_media_ids,
            __raw__=(
                content_item.__dict__ if hasattr(content_item, "__dict__") else None
            ),
        )

        # Process attached media
        media_items = getattr(content_item, "media", None)
        if isinstance(media_items, list) and media_items:
            media_items_list = cast(list[Any], media_items)
            for media_item in media_items_list:
                try:
                    media_metadata = MediaMetadata.from_api_media(
                        media_item=media_item,
                        content_id=content_id,
                        user_id=user_id,
                        api_type=api_type,
                        created_at=created_at,
                        preview_media_ids=preview_media_ids,
                    )
                    media_metadata.content_metadata = content_metadata
                    content_metadata.medias.append(media_metadata)
                except Exception as e:
                    logger.error(
                        f"Failed to convert media for content {content_id}: {e}",
                        exc_info=True,
                    )

        return content_metadata

    @classmethod
    def from_db_record(
        cls,
        db_content: db_content_types,
        *,
        content_type: str,
        auth_id: int | None = None,
    ) -> ContentMetadata:
        """Create ContentMetadata from a database content model."""
        content_id = _coerce_int(getattr(db_content, "id", None))
        user_id = _coerce_int(getattr(db_content, "user_id", None))
        created_at = _coerce_datetime(getattr(db_content, "created_at", None))
        if content_id is None or user_id is None or created_at is None:
            raise ValueError("Database content record is missing required fields")

        text_value = getattr(db_content, "text", None)
        text = text_value if isinstance(text_value, str) else None
        price_raw = getattr(db_content, "price", 0.0)
        try:
            price = float(price_raw or 0.0)
        except (TypeError, ValueError):
            price = 0.0

        paid_raw = getattr(db_content, "paid", None)
        paid = bool(paid_raw) if paid_raw is not None else price > 0
        archived = bool(getattr(db_content, "archived", False))
        expires_at = _coerce_datetime(getattr(db_content, "expires_at", None))
        receiver_id = _coerce_int(getattr(db_content, "receiver_id", None))
        queue_id = _coerce_int(getattr(db_content, "queue_id", None))

        content_metadata = cls(
            content_id=content_id,
            api_type=content_type,
            user_id=user_id,
            created_at=created_at,
            text=text,
            price=price,
            paid=paid,
            archived=archived,
            expires_at=expires_at,
            receiver_id=receiver_id,
            queue_id=queue_id,
            __raw__=(db_content.__dict__ if hasattr(db_content, "__dict__") else None),
        )

        media_items = getattr(db_content, "media", None) or getattr(
            db_content, "medias", None
        )
        if isinstance(media_items, list):
            media_items_list = cast(list[Any], media_items)
            for media_item in media_items_list:
                media_id = _coerce_int(getattr(media_item, "id", None))
                media_created_at = _coerce_datetime(
                    getattr(media_item, "created_at", None)
                )
                if media_id is None or media_created_at is None:
                    continue

                urls: list[str] = []
                _append_unique_url(urls, getattr(media_item, "url", None))

                media_url_records = getattr(media_item, "media_urls", None)
                if isinstance(media_url_records, list):
                    media_url_list = cast(list[Any], media_url_records)
                    matching_urls: list[Any] = []
                    if auth_id is not None:
                        matching_urls = [
                            record
                            for record in media_url_list
                            if _coerce_int(getattr(record, "auth_id", None)) == auth_id
                        ]
                    for url_record in matching_urls or media_url_list:
                        _append_unique_url(urls, getattr(url_record, "url", None))

                filename = Path(urlparse(urls[0]).path).name if urls else None
                drm_manifest = getattr(media_item, "drm_manifest", None)
                drm_media: Any | None = None
                dash_manifest = getattr(drm_manifest, "dash_manifest", None)
                dash_key_pair_id = getattr(drm_manifest, "dash_key_pair_id", None)
                dash_policy = getattr(drm_manifest, "dash_policy", None)
                dash_signature = getattr(drm_manifest, "dash_signature", None)
                if (
                    dash_manifest
                    and dash_key_pair_id
                    and dash_policy
                    and dash_signature
                ):

                    drm_media = DRMMedia(
                        media_id=media_id,
                        manifest_url=str(dash_manifest),
                        key_pair_id=str(dash_key_pair_id),
                        policy=str(dash_policy),
                        signature=str(dash_signature),
                        content_id=content_id,
                        video_url=drm_manifest.video_url,
                        audio_url=drm_manifest.audio_url,
                        video_size=getattr(drm_manifest, "video_size", None),
                        audio_size=getattr(drm_manifest, "audio_size", None),
                        content_type=content_type,
                    )
                    filename = drm_media.resolve_filename() or filename
                if filename:
                    pass
                else:
                    pass

                media_metadata = MediaMetadata(
                    media_id=media_id,
                    media_type=_normalize_media_type_token(
                        getattr(media_item, "category", None) or "other"
                    ),
                    urls=urls,
                    content_id=content_id,
                    user_id=user_id,
                    created_at=media_created_at,
                    preview=bool(getattr(media_item, "preview", False)),
                    size=_coerce_int(getattr(media_item, "size", None)) or 0,
                    drm=bool(
                        getattr(media_item, "drm", False) or drm_manifest is not None
                    ),
                    drm_media=drm_media,
                    filename=filename,
                    content_metadata=content_metadata,
                    __raw__=cast(
                        dict[str, Any],
                        media_item.__dict__ if hasattr(media_item, "__dict__") else {},
                    ),
                )
                content_metadata.medias.append(media_metadata)

        return content_metadata

    def find_media_by_id(self, media_id: int) -> MediaMetadata | None:
        """
        Find media by ID.

        Args:
            media_id: Media identifier to search for

        Returns:
            MediaMetadata if found, None otherwise
        """
        return next((m for m in self.medias if m.media_id == media_id), None)

    def find_media_by_url(self, url: str) -> MediaMetadata | None:
        """
        Find media by URL path matching.

        Args:
            url: URL to search for

        Returns:
            MediaMetadata if found, None otherwise
        """
        try:
            parsed_url = urlparse(url)
            target_path = parsed_url.path

            for media in self.medias:
                for media_url in media.urls:
                    if urlparse(media_url).path == target_path:
                        return media
        except Exception as e:
            logger.warning(f"Failed to parse URL in find_media_by_url: {e}")

        return None
