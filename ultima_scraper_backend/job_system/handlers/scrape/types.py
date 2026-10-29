"""Type definitions and constants for scrape task handling.

This module contains type definitions and constants used throughout
the scrape handler for type safety and documentation.
"""

from __future__ import annotations

from typing import TypedDict

# Batch size for publishing subscription progress updates
SUBSCRIPTION_PROGRESS_BATCH = 25


class MediaEntry(TypedDict, total=False):
    """
    Single media file entry extracted during scraping.

    This structure represents a downloadable media item with all
    metadata needed for the download handler.
    """

    url: str  # Primary download URL
    urls: list[str]  # All available URLs
    filename: str  # Target filename
    content_id: str  # Content/post ID
    media_id: str  # Unique media identifier
    media_type: str  # Type of media (photo, video, audio, etc.)
    size: int  # File size in bytes
    created_at: str  # Creation timestamp (ISO format)
    drm: bool  # Whether media is DRM protected
    drm_key: str | None  # DRM decryption key
    preview: bool  # Whether this is a preview/thumbnail
    price: float | int  # Content price
    value: str  # Calculated value (Free, Previews, Paid)


class ScrapeResult(TypedDict, total=False):
    """Result of a content scrape operation."""

    items_scraped: int
    items_saved: int
    errors: list[str]
    media_urls: list[MediaEntry]


class SubscriptionProgress(TypedDict):
    """Progress metrics for subscription synchronization."""

    total: int
    processed: int
    saved: int
    updated: int
