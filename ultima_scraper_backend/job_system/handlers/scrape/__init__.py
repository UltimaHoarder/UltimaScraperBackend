"""Scrape task handler package.

This package provides the scrape task handler functionality, organized into:
- types.py: Type definitions and constants
- events.py: Event publishing helpers
- content.py: Content scraping functions
- subscriptions.py: Subscription handling
- handler.py: Main scrape_task_handler function

Usage:
    from .scrape import scrape_task_handler
"""

from __future__ import annotations

from ...models import TaskType, register_task_handler
from .content import save_content_to_db, scrape_content_type
from .events import publish_hook_event, publish_subscription_progress
from .handler import scrape_task_handler
from .subscriptions import handle_get_subscriptions
from .types import (
    SUBSCRIPTION_PROGRESS_BATCH,
    MediaEntry,
    ScrapeResult,
    SubscriptionProgress,
)

__all__ = [
    # Main handler
    "scrape_task_handler",
    # Content scraping
    "save_content_to_db",
    "scrape_content_type",
    # Subscriptions
    "handle_get_subscriptions",
    # Events
    "publish_hook_event",
    "publish_subscription_progress",
    # Types
    "MediaEntry",
    "ScrapeResult",
    "SubscriptionProgress",
    "SUBSCRIPTION_PROGRESS_BATCH",
]

# Register the scrape task handler
register_task_handler(TaskType.SCRAPE, scrape_task_handler)
