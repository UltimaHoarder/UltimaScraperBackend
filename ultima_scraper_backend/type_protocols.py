"""
Type protocols for UltimaScraperAPI interfaces.

These protocols define common interfaces used across different site APIs
(OnlyFans, Fansly, etc.) to improve type checking and reduce # type: ignore
comments throughout the codebase.

Usage:
    from ultima_scraper_backend.type_protocols import HasCreatedAt, HasAuthor

    def process_content(item: HasCreatedAt & HasAuthor) -> dict[str, Any]:
        # Type checker now knows item has .created_at and .author
        return {
            "timestamp": item.created_at,
            "author_id": item.author.id,
        }
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol


class HasCreatedAt(Protocol):
    """Object with a created_at timestamp."""

    @property
    def created_at(self) -> datetime | str | None:
        """Creation timestamp."""
        ...


class HasMedia(Protocol):
    """Object with media attachments."""

    @property
    def media(self) -> list[Any]:
        """List of media items (images, videos, etc.)."""
        ...


class UserLike(Protocol):
    """Protocol for user-like objects across different APIs."""

    @property
    def id(self) -> int | str:
        """User ID."""
        ...

    @property
    def username(self) -> str | None:
        """Username."""
        ...


class ContentItemLike(Protocol):
    """Protocol for content items (posts, messages)."""

    @property
    def id(self) -> int | str:
        """Content ID."""
        ...

    @property
    def text(self) -> str | None:
        """Content text."""
        ...

    @property
    def created_at(self) -> datetime | str | None:
        """Creation timestamp."""
        ...

    @property
    def price(self) -> float | int | None:
        """Content price (if paid)."""
        ...


class HasAuthor(Protocol):
    """Object with an author/user reference."""

    @property
    def author(self) -> UserLike:
        """Content author."""
        ...


class PostLike(ContentItemLike, HasMedia, HasAuthor, Protocol):
    """Protocol for post-like objects."""

    pass


class MessageLike(ContentItemLike, HasMedia, Protocol):
    """Protocol for message-like objects."""

    @property
    def fromUser(self) -> UserLike:  # noqa: N802 - API field name
        """Message sender (Fansly-style)."""
        ...


class SubscriptionLike(Protocol):
    """Protocol for subscription objects."""

    @property
    def id(self) -> int | str:
        """Subscription ID."""
        ...

    @property
    def user(self) -> UserLike:
        """Subscribed user/performer."""
        ...

    @property
    def subscribed_by_expire_date(self) -> datetime | str | None:
        """Subscription expiration date."""
        ...

    def is_active(self) -> bool:
        """Check if subscription is currently active."""
        ...


class ChatLike(Protocol):
    """Protocol for chat/conversation objects."""

    @property
    def user(self) -> UserLike:
        """User involved in the chat."""
        ...

    @property
    def last_message(self) -> MessageLike | None:
        """Most recent message in chat."""
        ...

    @property
    def last_read_message_id(self) -> int | str | None:
        """ID of last read message."""
        ...


# Type aliases for common unions
ContentItem = PostLike | MessageLike
