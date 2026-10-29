"""Topic routing for /ws/events: each client receives only what it subscribed to.

Clients send ``{"type": "subscribe", "topics": [...]}``. Topics:

- ``jobs``: job-level changes, with per-task activity folded into one
  ``job_changed`` event per job per second
- ``job:<id>``: every event for one job, including per-task and progress events
- ``logs``, ``chats``, ``system``: log lines, chat events, worker/queue/health
- ``api_hooks``: per-call API hook events (webhooks page). Workers publish these
  only while someone subscribes; see ``API_HOOK_WATCHERS_KEY``.

A connection that never subscribes keeps receiving every event, so existing
external clients are unaffected.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Mapping
from typing import Any

from ...redis_channels import HOOKS_CHANNEL

TOPICS = frozenset({"jobs", "logs", "chats", "system", "api_hooks", "favourites", "profiles"})
JOB_TOPIC_PREFIX = "job:"
MAX_TOPICS = 32
# Present while a browser watches per-call API hooks; workers skip them otherwise.
API_HOOK_WATCHERS_KEY = f"{HOOKS_CHANNEL}:api-watchers"
API_HOOK_WATCHERS_TTL_SECONDS = 30
JOB_CHANGE_INTERVAL_SECONDS = 1.0
_PROGRESS_FIELDS = ("status", "items_scraped", "items_saved", "total_items", "content_type")


def valid_topics(raw: Any) -> frozenset[str]:
    """Known topics from a subscribe message; anything else is ignored."""
    if not isinstance(raw, list):
        return frozenset()
    topics = set()
    for topic in raw[:MAX_TOPICS]:
        if not isinstance(topic, str):
            continue
        if topic in TOPICS:
            topics.add(topic)
        elif topic.startswith(JOB_TOPIC_PREFIX) and 0 < len(topic) - len(JOB_TOPIC_PREFIX) <= 80:
            topics.add(topic)
    return frozenset(topics)


def _job_id(event: Mapping[str, Any]) -> str | None:
    job_id = event.get("job_id")
    if job_id is None and "job" in str(event.get("type") or ""):
        job_id = event.get("id")
    return job_id if isinstance(job_id, str) and job_id else None


def is_task_activity(event: Mapping[str, Any]) -> bool:
    """Per-task lifecycle and progress events: folded into ``job_changed``."""
    name = str(event.get("event") or event.get("event_type") or event.get("type") or "")
    if name.endswith("_progress") or event.get("type") == "download_status":
        return True
    return event.get("type") == "job_event" and name.startswith("task_")


def event_topics(event: Mapping[str, Any]) -> tuple[str, ...]:
    kind = str(event.get("type") or "")
    if kind == "user_profile_updated":
        return ("profiles",)
    if kind == "favourite_changed":
        return ("favourites",)
    if kind == "log":
        return ("logs",)
    if "function" in event and event.get("event_type") in {"started", "finished", "error"}:
        return ("api_hooks",)
    name = str(event.get("event") or "")
    if (
        "chat" in kind or "chat" in name or name == "messages_synced"
        or "chat_user_id" in event or "api2_chat_message" in event or "new_message" in event
    ):
        return ("chats",)
    job_id = _job_id(event)
    if job_id:
        if is_task_activity(event):
            return (JOB_TOPIC_PREFIX + job_id,)
        return ("jobs", JOB_TOPIC_PREFIX + job_id)
    return ("system",)


def wants(subscribed: frozenset[str] | None, topics: tuple[str, ...]) -> bool:
    """None means the connection never subscribed: it receives everything."""
    return subscribed is None or any(topic in subscribed for topic in topics)


class JobChangeCoalescer:
    """Folds per-task activity into at most one ``job_changed`` per job per interval."""

    def __init__(self) -> None:
        self._counts: dict[str, Counter[str]] = {}
        self._latest: dict[str, dict[str, Any]] = {}

    def add(self, event: Mapping[str, Any]) -> None:
        job_id = _job_id(event)
        if not job_id:
            return
        name = str(event.get("event") or event.get("event_type") or event.get("type"))
        self._counts.setdefault(job_id, Counter())[name] += 1
        progress = {field: event[field] for field in _PROGRESS_FIELDS if field in event}
        if progress:
            self._latest[job_id] = progress

    def drain(self) -> list[dict[str, Any]]:
        if not self._counts:
            return []
        timestamp = time.time()
        changes = [
            {
                "type": "job_event",
                "event": "job_changed",
                "job_id": job_id,
                # Activity since the previous change; consumers refetch totals.
                "activity": dict(counts),
                **({"latest": self._latest[job_id]} if job_id in self._latest else {}),
                "timestamp": timestamp,
            }
            for job_id, counts in self._counts.items()
        ]
        self._counts.clear()
        self._latest.clear()
        return changes
