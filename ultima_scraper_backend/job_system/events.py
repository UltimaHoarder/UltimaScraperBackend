"""Typed event models for the job system.

This module is a compatibility shim.

The canonical event models now live in `ultima_scraper_api.job_events` so that
UltimaScraper projects share a single source of truth.
"""

from __future__ import annotations

from ultima_scraper_api.job_events import *  # noqa: F403
