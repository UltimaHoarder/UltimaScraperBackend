"""
Redis Channel Constants

Centralized definitions for all Redis pub/sub channels used in UltimaScraper.
All channels and keys use the KEY_PREFIX from USAPI as the single source of truth.
"""

from __future__ import annotations

from ultima_scraper_api.managers.redis.connection import RedisManager

# Import the canonical Redis key prefix from USAPI
KEY_PREFIX = RedisManager.KEY_PREFIX
"""Root Redis key prefix - imported from USAPI RedisManager as single source of truth."""

# Pub/Sub Channels
HOOKS_CHANNEL = f"{KEY_PREFIX}:hooks"
"""General event hooks for scraping progress and subscription sync events."""

AUTH_EVENTS_CHANNEL = f"{KEY_PREFIX}:auth_events"
"""Authentication management events (create, delete, login, logout)."""

JOB_EVENTS_CHANNEL = f"{KEY_PREFIX}:jobs:events"
"""Job lifecycle events (created, started, completed, failed)."""

# Key Prefixes (not channels, but related)
BACKEND_QUEUE_PREFIX = f"{KEY_PREFIX}:backend:ready:"
"""Prefix for per-backend task queue keys."""

BACKEND_LOAD_KEY = f"{KEY_PREFIX}:backend:load"
"""Sorted set tracking backend load scores."""

IP_LOCK_PREFIX = f"{KEY_PREFIX}:backend:iplock:"
"""Prefix for IP-based task locking."""

BACKEND_CAPABILITIES_PREFIX = f"{KEY_PREFIX}:backend:capabilities:"
"""Prefix for backend capability tracking."""

TASK_OWNER_PREFIX = f"{KEY_PREFIX}:tasks:owners:"
"""Prefix for task ownership tracking."""

READY_SEQUENCE_PREFIX = f"{KEY_PREFIX}:backend:ready_seq:"
"""Prefix for backend ready sequence tracking."""

BACKEND_ACTIVE_JOBS_PREFIX = f"{KEY_PREFIX}:backend:active_jobs:"
"""Prefix for backend active job tracking."""

JOB_ENVELOPE_PREFIX = f"{KEY_PREFIX}:jobs:envelopes:"
"""Prefix for job envelope storage."""

WORKER_KEY_PREFIX = f"{KEY_PREFIX}:workers:"
"""Prefix for worker registration and heartbeat keys."""

JOB_DEDUP_PREFIX = f"{KEY_PREFIX}:jobs:dedup:"
"""Prefix for job deduplication keys."""

WATCHES_PAUSED_KEY = f"{KEY_PREFIX}:watches:paused"
"""Flag key — when present, all watch-triggered task creation is suppressed."""
