"""Fire-and-forget asyncio tasks that cannot be garbage-collected mid-flight.

The event loop keeps only weak references to tasks, so a task created without a
stored reference may disappear before it finishes. Failures are logged when the
task completes instead of surfacing later as "Task exception was never retrieved".
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)

_background_tasks: set[asyncio.Task[Any]] = set()


def spawn_background_task[T](
    coro: Coroutine[Any, Any, T], *, name: str | None = None
) -> asyncio.Task[T]:
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)
    task.add_done_callback(_on_background_task_done)
    return task


def _on_background_task_done(task: asyncio.Task[Any]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "Background task %s failed", task.get_name(), exc_info=exc
        )
