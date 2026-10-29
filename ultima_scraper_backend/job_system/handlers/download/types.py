"""Type definitions for download task handling.

This module contains TypedDict and type alias definitions used throughout
the download handler for type safety and documentation.
"""

from __future__ import annotations

from typing import TypedDict


class MediaEntry(TypedDict, total=False):
    """
    Single media file entry for download.

    This structure represents a downloadable media item, typically extracted
    from scraper output or provided directly for download tasks.

    Required fields: url, media_id, media_type
    """

    url: str  # Download URL - REQUIRED
    media_id: str | int  # Unique media identifier - REQUIRED
    media_type: str  # Type of media (photo, video, audio, etc.) - REQUIRED
    filename: str  # Target filename
    directory: str  # Target directory path
    content: bytes  # Pre-downloaded content (if already fetched)
    data: bytes  # Alias for content
    size: int  # File size in bytes
    created_at: str  # Creation timestamp
    preview: bool  # Whether this is a preview/thumbnail
    content_id: str | int  # Content/post ID
    # Additional optional fields that may appear in scraper output
    duration: int | float  # Duration for video/audio
    width: int  # Image/video width
    height: int  # Image/video height
    value: str  # Content value (Free, Paid, etc.)
    text: str  # Content text/caption
    urls: list[str]  # Alternative URLs
    drm: bool  # Whether media is DRM protected
    drm_key: str  # DRM decryption key


class MediaUrlsByType(TypedDict, total=False):
    """Media URLs grouped by content type from scraper output."""

    posts: list[MediaEntry]
    messages: list[MediaEntry]
    stories: list[MediaEntry]
    highlights: list[MediaEntry]
    archived: list[MediaEntry]
    streams: list[MediaEntry]


class ContentData(TypedDict, total=False):
    """Content data for a single authenticated user from scraper output."""

    media_urls: MediaUrlsByType
    # Additional fields that may appear
    username: str
    user_id: int
    download_info: dict[str, str | int | float | bool | None]


class ScrapeTaskOutput(TypedDict):
    """
    Top-level structure from scrape task output.

    Format: {"content_by_auth": {"auth_name": ContentData}}
    """

    content_by_auth: dict[str, ContentData]


# Union type for all possible input formats
FilesInput = ScrapeTaskOutput | list[MediaEntry] | dict[str, MediaEntry]
