"""Download task handler package.

This package provides the download task handler functionality, organized into:
- types.py: TypedDict definitions for type safety
- utils.py: Helper functions for file processing
- results.py: Result handling and categorization
- handler.py: Main download_task_handler function

Usage:
    from .download import download_task_handler
"""

from __future__ import annotations

from ...models import TaskType, register_task_handler
from .handler import download_task_handler
from .results import (
    categorize_media_items,
    create_failed_download_result,
    handle_download_http_error,
    resolve_content_id,
)
from .types import (
    ContentData,
    FilesInput,
    MediaEntry,
    MediaUrlsByType,
    ScrapeTaskOutput,
)
from .utils import (
    build_filename,
    coerce_bytes,
    detect_media_type,
    ensure_config,
    entry_size,
    extract_filename_from_url,
    is_url,
    normalise_files,
    parse_created_at,
    utcnow,
)

__all__ = [
    # Main handler
    "download_task_handler",
    # Types
    "ContentData",
    "FilesInput",
    "MediaEntry",
    "MediaUrlsByType",
    "ScrapeTaskOutput",
    # Utils
    "build_filename",
    "coerce_bytes",
    "detect_media_type",
    "ensure_config",
    "entry_size",
    "extract_filename_from_url",
    "is_url",
    "normalise_files",
    "parse_created_at",
    "utcnow",
    # Results
    "categorize_media_items",
    "create_failed_download_result",
    "handle_download_http_error",
    "resolve_content_id",
]

# Register the download task handler
register_task_handler(TaskType.DOWNLOAD, download_task_handler)
