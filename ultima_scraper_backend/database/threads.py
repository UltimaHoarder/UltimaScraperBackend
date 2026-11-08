"""Bounded interactive SQL admission with owned worker-thread cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from functools import partial
from typing import Any, TypeVar

from anyio import CancelScope, CapacityLimiter, to_thread
from anyio.lowlevel import RunVar
from ultima_scraper_core import runtime_env_value
from ultima_scraper_core.processes import finish_owned

T = TypeVar("T")
_database_limiter: RunVar[CapacityLimiter] = RunVar("interactive_database_limiter")
_api_command_limiter: RunVar[CapacityLimiter] = RunVar("api_command_admission_limiter")
# Half the API's connection pool: a page's burst of commands queues here, where
# there's no timeout, and leaves connections for everything else.
API_COMMAND_ADMISSION_LIMIT = 4


def database_thread_limiter() -> CapacityLimiter:
    """Inherit ASGI thread admission unless the operator supplies an SQL budget."""
    raw = runtime_env_value("ULTIMA_INTERACTIVE_DB_CONCURRENCY")
    if raw is None or not raw.strip():
        return to_thread.current_default_thread_limiter()
    try:
        limit = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "ULTIMA_INTERACTIVE_DB_CONCURRENCY must be a positive integer"
        ) from exc
    if limit <= 0:
        raise ValueError("ULTIMA_INTERACTIVE_DB_CONCURRENCY must be a positive integer")
    limiter = _database_limiter.get(None)
    if limiter is None:
        limiter = CapacityLimiter(limit)
        _database_limiter.set(limiter)
    elif limiter.total_tokens != limit:
        limiter.total_tokens = limit
    return limiter


def api_command_admission_limiter() -> CapacityLimiter:
    """Bound how many worker API commands are written to SQL at once."""
    limiter = _api_command_limiter.get(None)
    if limiter is None:
        limiter = CapacityLimiter(API_COMMAND_ADMISSION_LIMIT)
        _api_command_limiter.set(limiter)
    return limiter


async def run_database_call(function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Create, use and close SQL sessions inside ``function``, returning plain data.

    Waiting requests do not enter the thread pool or open a session. Once admitted,
    cancellation waits for session cleanup before releasing admission. This also
    covers direct asyncio cancellation, outside AnyIO's cancellation scopes.
    """
    async with database_thread_limiter():
        return await run_owned_thread_call(function, *args, **kwargs)


async def run_owned_thread_call(
    function: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """Run one already-admitted call without adding another concurrency ceiling.

    The caller owns admission. A per-call AnyIO token avoids the shared default
    pool limit; cancellation still waits for the owned worker to stop.
    """
    owned = asyncio.create_task(
        to_thread.run_sync(
            partial(function, *args, **kwargs), limiter=CapacityLimiter(1)
        )
    )
    try:
        return await asyncio.shield(owned)
    except asyncio.CancelledError:
        with CancelScope(shield=True):
            try:
                await finish_owned(owned)
            except Exception:
                pass
        raise
