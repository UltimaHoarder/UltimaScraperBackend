"""Pydantic models for task payloads.

This module defines the canonical payload structures for all task types.
Import these types when creating tasks to ensure field name consistency.

Using Pydantic instead of TypedDict provides:
- Runtime validation
- Better IDE support
- Consistent serialization
- Type coercion
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from ultima_scraper_api import CONTENT_VALUES


class DefaultPayload(BaseModel):
    """Base payload for content tasks (scrape, download).

    Routing fields (site_name, user_id, content_type, auth_id) live on Task.
    This payload contains handler-specific execution config.
    """

    model_config = {"extra": "forbid"}  # Prevent extra fields (inherited by subclasses)

    user_username: str | None = None  # Creator username for API lookups
    content_value: CONTENT_VALUES = "All"  # Content value filter (Free, Paid, etc.)
    max_items: int | None = None  # Maximum items to scrape
    ignore_space_requirements: bool = False


class ScrapeTaskPayload(DefaultPayload):
    """Payload for scrape tasks.

    Contains handler-specific execution config.
    Routing fields (site_name, user_id, content_type, auth_id) live on Task.
    """

    content_ids: list[int] | None = (
        None  # Specific content IDs to download (optional - downloads all if not provided)
    )
    download_media: bool = False  # Whether to download media after scraping
    save_to_db: bool = True  # Whether to save scraped data to database
    rescrape_existing: bool = True  # Re-fetch all content from API and update stored media URLs
    reload_workers: bool = False  # Whether to tell workers to reload user data
    use_guest_auth: bool = False  # Force worker to use unauthenticated guest auth
    guest_proxy_url: str | None = None  # Optional proxy URL for guest-auth scraping
    guest_fallback_proxy_urls: list[str] = Field(
        default_factory=list
    )  # Optional fallback proxy URLs for guest-auth geo-block retry
    # Same-country reserves for pre-lookup substitution of a deferred MPS route.
    guest_proxy_alternatives: dict[str, list[str]] = Field(default_factory=dict)
    # Insert the direct connection at this position in the guest route order.
    guest_direct_route_index: int | None = Field(default=None, ge=0)
    track_invalid_user: bool = False  # Mark user_id in invalid_users when upstream reports missing
    paid_content_feed: bool = False  # Pull from auth.get_paid_content() instead of performer endpoints
    paid_content_performer: str | None = None  # Optional username/user_id filter for paid feed
    paid_content_types: list[str] | None = None  # Optional paid-feed content type filter
    conclusive_miss_threshold: int | None = (
        None  # Number of distinct 404 misses across routes before early-exiting
    )
    max_route_hops: int | None = None  # Max number of route hops to assign to the task


class DownloadTaskPayload(DefaultPayload):
    """Payload for download tasks.

    Contains handler-specific execution config.
    Routing fields (site_name, user_id, content_type, auth_id) live on Task.
    """

    paid_content_feed: bool = False  # Iterate a dependency selection across authors.

    media_ids: list[int] | None = (
        None  # Specific content IDs to download (optional - downloads all if not provided)
    )


class APICommandTaskPayload(BaseModel):
    """
    Required payload structure for API command tasks.

    Used for API Explorer commands that execute arbitrary API calls.
    """

    command: str  # API command to execute (get_authed_user, get_user, etc.)
    args: dict[str, Any] = Field(default_factory=dict)  # Command arguments

    model_config = {"extra": "forbid"}  # Prevent extra fields


__all__ = [
    "DefaultPayload",
    "ScrapeTaskPayload",
    "DownloadTaskPayload",
    "APICommandTaskPayload",
]
