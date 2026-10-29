"""Pydantic models for task payloads.

Re-exports canonical payload structures from job_system.payloads.
"""

from __future__ import annotations

from ..job_system.payloads import (
    APICommandTaskPayload,
    DefaultPayload,
    DownloadTaskPayload,
    ScrapeTaskPayload,
)

__all__ = [
    "DefaultPayload",
    "ScrapeTaskPayload",
    "DownloadTaskPayload",
    "APICommandTaskPayload",
]

