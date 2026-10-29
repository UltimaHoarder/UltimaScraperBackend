"""Admin API routes for backend management.

Provides endpoints for:
- Mode switching (enable/disable distributed mode)
- Queue statistics
- Backend health checks
"""

from __future__ import annotations

import asyncio
import logging
import os

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from ultima_scraper_db_archive.global_db import get_global_engine

from ...models.backend import BackendInstance
from ...runtime_mode import is_api_only_runtime
from ..dependencies import get_backend_instance
from ...redis_events import publish_ws_event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


class ModeToggleRequest(BaseModel):
    """Request to toggle distributed mode."""

    enabled: bool


class ModeToggleResponse(BaseModel):
    """Response from mode toggle."""

    backend_id: str
    distributed_mode: bool
    transitioning: bool
    message: str


class QueueStatsResponse(BaseModel):
    """Queue statistics response."""

    instance_id: str
    distributed_mode: bool
    transitioning: bool
    local_queue_size: int
    redis_queue_size: int
    active_queue_type: str
    recovery_interval: int = 30
    next_recovery_seconds: int | None = None
    last_recovery_at: str | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str


def _check_postgres_readiness(expected_database: str) -> None:
    """Probe the configured PostgreSQL pool and validate its database target."""

    with get_global_engine().connect() as connection:
        current_database = str(
            connection.execute(text("SELECT current_database()")).scalar_one()
        )
    if current_database != expected_database:
        raise RuntimeError(
            "PostgreSQL connected to an unexpected database "
            f"({current_database!r}, expected {expected_database!r})"
        )


async def ensure_api_postgres_dependency() -> None:
    """Validate the strict API database target before initialization may write."""

    if not is_api_only_runtime():
        return
    expected_database = (
        os.environ.get("USR_EXPECTED_DATABASE", "ultima_archive").strip()
        or "ultima_archive"
    )
    await asyncio.to_thread(_check_postgres_readiness, expected_database)


async def ensure_api_runtime_dependencies(backend: BackendInstance) -> None:
    """Fail closed when strict API mode loses Redis or PostgreSQL readiness."""

    if not is_api_only_runtime():
        return

    if backend.redis is None or not await backend.redis.health_check():
        raise RuntimeError("Redis is unavailable")

    await ensure_api_postgres_dependency()


@router.post("/distributed-mode", response_model=ModeToggleResponse)
async def toggle_distributed_mode(
    request: ModeToggleRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> ModeToggleResponse:
    """Toggle distributed mode on/off.

    **Enable (local → Redis):**
    - Initializes Redis queue and event bus
    - Migrates pending local jobs to Redis
    - Other backends can now help with workload

    **Disable (Redis → local):**
    - Switches to local queue
    - Leaves Redis jobs for other backends
    - Stops consuming from shared Redis queue

    Args:
        request: Mode toggle request
        backend: Backend instance (injected)

    Returns:
        Mode toggle response with current state
    """
    try:
        if request.enabled:
            await backend.enable_distributed_mode()
            message = "Distributed mode enabled (jobs migrated to Redis)"
        else:
            await backend.disable_distributed_mode()
            message = "Distributed mode disabled (local queue active)"

        await publish_ws_event(
            backend.redis,
            {
                "type": "config_toggled",
                "event": "distributed_mode",
                "enabled": backend.distributed_mode,
                "backend_id": backend.instance_id,
            },
        )

        return ModeToggleResponse(
            backend_id=backend.instance_id,
            distributed_mode=backend.distributed_mode,
            transitioning=backend.transitioning,
            message=message,
        )

    except Exception as e:
        logger.error(f"Failed to toggle distributed mode: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/queue-stats", response_model=QueueStatsResponse)
async def get_queue_stats(
    backend: BackendInstance = Depends(get_backend_instance),
) -> QueueStatsResponse:
    """Get current queue statistics.

    Returns:
        Queue stats including sizes and active queue type
    """
    stats = await backend.get_queue_stats()
    return QueueStatsResponse(
        instance_id=str(stats["instance_id"]),
        distributed_mode=bool(stats["distributed_mode"]),
        transitioning=bool(stats["transitioning"]),
        local_queue_size=_coerce_int(stats["local_queue_size"]),
        redis_queue_size=_coerce_int(stats["redis_queue_size"]),
        active_queue_type=str(stats["active_queue_type"]),
        recovery_interval=_coerce_int(stats["recovery_interval"], 30),
        next_recovery_seconds=(
            _coerce_int(stats["next_recovery_seconds"])
            if stats["next_recovery_seconds"] is not None
            else None
        ),
        last_recovery_at=(
            str(stats["last_recovery_at"])
            if stats["last_recovery_at"] is not None
            else None
        ),
    )


@router.get("/health", response_model=HealthResponse)
async def health_check(
    backend: BackendInstance = Depends(get_backend_instance),
) -> HealthResponse:
    """Health check endpoint.

    Returns:
        Health status and backend info
    """
    try:
        await ensure_api_runtime_dependencies(backend)
    except Exception as exc:
        logger.warning("API runtime readiness check failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Required runtime dependencies are unavailable",
        ) from exc
    return HealthResponse(status="ok")
