"""Event publishing helpers for scrape task handling.

This module contains functions for publishing progress and hook events
to Redis channels during scrape operations.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from ....redis_channels import HOOKS_CHANNEL, JOB_EVENTS_CHANNEL
from ...models import Job

logger = logging.getLogger(__name__)


# Progress events (one or more per task) are throttled to one per job per
# interval in each worker; the newest state is still sent when it elapses.
PROGRESS_INTERVAL_SECONDS = 1.0
_last_progress: dict[str, float] = {}
_pending_progress: dict[str, tuple[Any, dict[str, Any], str]] = {}


async def publish_hook_event(
    storage: Any,
    event_data: dict[str, Any],
    event_description: str = "hook event",
) -> None:
    """Publish a serialized event dict to the HOOKS_CHANNEL via Redis.

    Args:
        storage: Storage context with Redis client
        event_data: Event data to publish
        event_description: Description for logging
    """
    job_id = event_data.get("job_id")
    if str(event_data.get("event") or "").endswith("_progress") and isinstance(job_id, str) and job_id:
        now = time.monotonic()
        wait = PROGRESS_INTERVAL_SECONDS - (now - _last_progress.get(job_id, float("-inf")))
        if wait > 0:
            if job_id not in _pending_progress:
                asyncio.get_running_loop().call_later(
                    wait, lambda: asyncio.ensure_future(_flush_progress(job_id))
                )
            _pending_progress[job_id] = (storage, event_data, event_description)
            return
        _last_progress[job_id] = now
        if len(_last_progress) > 4096:
            for stale in [key for key, at in _last_progress.items() if now - at > 60]:
                del _last_progress[stale]
    await _publish_now(storage, event_data, event_description)


async def _flush_progress(job_id: str) -> None:
    pending = _pending_progress.pop(job_id, None)
    if pending is not None:
        _last_progress[job_id] = time.monotonic()
        await _publish_now(*pending)


async def _publish_now(storage: Any, event_data: dict[str, Any], event_description: str) -> None:
    if storage and hasattr(storage, "redis") and storage.redis:
        try:
            await storage.redis.client.publish(HOOKS_CHANNEL, json.dumps(event_data))
        except Exception as exc:
            logger.debug(f"Failed to publish {event_description}: {exc}")


async def publish_subscription_progress(
    *,
    job: Job,
    worker_id: str,
    status: str,
    total: int,
    processed: int,
    saved: int,
    updated: int,
    completed_tasks: int = 0,
    storage: Any = None,
) -> None:
    """Publish progress metrics for subscription synchronization.

    Args:
        job: Job instance for context
        worker_id: Worker identifier
        status: Current status (running, completed, etc.)
        total: Total number of subscriptions
        processed: Number processed so far
        saved: Number of new subscriptions saved
        updated: Number of existing subscriptions updated
        completed_tasks: Number of completed tasks
        storage: Storage context with Redis client
    """
    # Try to get total_tasks from Redis state for accuracy
    actual_total_tasks = job.total_tasks if job.total_tasks > 0 else 1

    # If storage has Redis, fetch the authoritative total_tasks from job state
    if storage and hasattr(storage, "redis") and storage.redis:
        try:
            state_key = f"ultimascraper:job:{job.job_id}:state"
            redis_total = await storage.redis.client.hget(state_key, "total_tasks")
            if redis_total:
                # Handle both bytes and string responses
                if isinstance(redis_total, bytes):
                    redis_total = redis_total.decode()
                parsed_total = int(redis_total)
                if parsed_total > 0:
                    actual_total_tasks = parsed_total
        except Exception as exc:
            logger.debug(f"Failed to fetch total_tasks from Redis: {exc}")

    event = {
        "event": "job_progress",
        "job_id": job.job_id,
        "job_name": job.name,
        "status": status,
        "total_tasks": actual_total_tasks,
        "completed_tasks": completed_tasks,
        "failed_tasks": 0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "subscriptions_total": total,
            "subscriptions_processed": processed,
            "subscriptions_saved": saved,
            "subscriptions_updated": updated,
        },
        "worker_id": worker_id,
    }
    logger.info(
        f"[{worker_id}] [scrape_handler:subscriptions] Publishing progress: "
        f"total_tasks={actual_total_tasks}, completed_tasks={completed_tasks}, status={status}, "
        f"subscriptions={processed}/{total}"
    )

    # Publish to Redis if storage with redis is available
    if storage and hasattr(storage, "redis") and storage.redis:
        try:
            await storage.redis.client.publish(JOB_EVENTS_CHANNEL, json.dumps(event))
        except Exception as exc:
            logger.debug(f"Failed to publish subscription progress: {exc}")
