"""Watch list API routes for webhook auto-download.

Provides endpoints for managing which performers are "watched" —
media from watched users arriving via webhooks is automatically
queued for download.
"""

from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlmodel import func, select

from ultima_scraper_db_archive import UserModel

from ...models.backend import BackendInstance
from ...redis.keys import WATCHES_PAUSED_KEY
from ...database.factory import MultiSiteDatabaseManager
from ..dependencies import get_backend_instance

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/watch", tags=["webhooks"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class WatchedUserInfo(BaseModel):
    """A single watched performer."""

    id: int
    username: str | None = None
    site: str
    watched: bool = True
    downloaded_at: datetime | None = None
    created_at: datetime | None = None


class WatchListResponse(BaseModel):
    """Paginated watched-user list."""

    users: list[WatchedUserInfo]
    total: int


class WatchStatsResponse(BaseModel):
    """Aggregate watch-list statistics."""

    total_watched: int
    by_site: dict[str, int]
    paused: bool = False


class WatchPauseResponse(BaseModel):
    """Result of a pause/resume operation."""

    paused: bool
    message: str


class WatchToggleResponse(BaseModel):
    """Result of a watch/unwatch operation."""

    user_id: int
    watched: bool
    message: str


class IgnoreToggleResponse(BaseModel):
    """Result of an ignore/unignore operation."""

    user_id: int
    ignored: bool
    message: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/", response_model=WatchListResponse)
async def get_watch_list(
    site: str | None = Query(None, description="Filter by site"),
    search: str | None = Query(None, description="Search by username"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> WatchListResponse:
    """Return all users whose ``watched`` flag is True."""
    db_manager = MultiSiteDatabaseManager()
    users: list[WatchedUserInfo] = []
    total = 0

    sites_to_query = [site.lower()] if site else ["onlyfans", "fansly"]

    for site_name in sites_to_query:
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            continue

        with site_db.get_session() as session:
            query = select(UserModel).where(UserModel.watched.is_(True))

            if search:
                query = query.where(UserModel.username.ilike(f"%{search}%"))

            count_query = select(func.count()).select_from(query.subquery())
            total += session.exec(count_query).one()

            query = query.order_by(UserModel.username).offset(offset).limit(limit)
            results = session.exec(query).all()

            for user in results:
                users.append(
                    WatchedUserInfo(
                        id=user.id,
                        username=user.username,
                        site=site_name,
                        watched=True,
                        downloaded_at=user.downloaded_at,
                        created_at=user.created_at,
                    )
                )

    return WatchListResponse(users=users, total=total)


@router.get("/stats", response_model=WatchStatsResponse)
async def get_watch_stats(
    backend: BackendInstance = Depends(get_backend_instance),
) -> WatchStatsResponse:
    """Return aggregate watch-list counts per site and global pause state."""
    db_manager = MultiSiteDatabaseManager()
    by_site: dict[str, int] = {}
    total = 0

    for site_name in ("onlyfans", "fansly"):
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            by_site[site_name] = 0
            continue

        with site_db.get_session() as session:
            count = session.exec(
                select(func.count())
                .select_from(UserModel)
                .where(UserModel.watched.is_(True))
            ).one()
            by_site[site_name] = count
            total += count

    paused = False
    if backend.redis and backend.redis.is_connected:
        paused = bool(await backend.redis.client.get(WATCHES_PAUSED_KEY))

    return WatchStatsResponse(total_watched=total, by_site=by_site, paused=paused)


@router.put("/{user_id}", response_model=WatchToggleResponse)
async def add_to_watch_list(
    user_id: int,
    site: str = Query(..., description="Site name (onlyfans or fansly)"),
) -> WatchToggleResponse:
    """Mark a performer as watched."""
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site.lower())
    if not site_db:
        raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

    with site_db.get_session() as session:
        user = session.get(UserModel, user_id)
        if not user:
            raise HTTPException(
                status_code=404,
                detail=f"User {user_id} not found in {site} database",
            )
        user.watched = True
        session.commit()

        logger.info(
            "👁️ User %s (@%s) added to watch list [%s]", user_id, user.username, site
        )

    return WatchToggleResponse(
        user_id=user_id,
        watched=True,
        message=f"User {user_id} is now watched",
    )


@router.delete("/{user_id}", response_model=WatchToggleResponse)
async def remove_from_watch_list(
    user_id: int,
    site: str = Query(..., description="Site name (onlyfans or fansly)"),
) -> WatchToggleResponse:
    """Remove a performer from the watch list."""
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site.lower())
    if not site_db:
        raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

    with site_db.get_session() as session:
        user = session.get(UserModel, user_id)
        if not user:
            raise HTTPException(
                status_code=404,
                detail=f"User {user_id} not found in {site} database",
            )
        user.watched = False
        session.commit()

        logger.info(
            "👁️ User %s (@%s) removed from watch list [%s]",
            user_id,
            user.username,
            site,
        )

    return WatchToggleResponse(
        user_id=user_id,
        watched=False,
        message=f"User {user_id} is no longer watched",
    )


# ---------------------------------------------------------------------------
# Pause / Resume
# ---------------------------------------------------------------------------


@router.post("/pause", response_model=WatchPauseResponse)
async def pause_watches(
    backend: BackendInstance = Depends(get_backend_instance),
) -> WatchPauseResponse:
    """Globally pause all watch-triggered task creation.

    Existing tasks already in the queue are unaffected — only new
    webhook events will be suppressed until resumed.
    """
    if not backend.redis or not backend.redis.is_connected:
        raise HTTPException(status_code=503, detail="Redis unavailable")

    await backend.redis.client.set(WATCHES_PAUSED_KEY, "1")
    logger.info("⏸️ All watches paused")

    return WatchPauseResponse(paused=True, message="All watches paused")


@router.post("/resume", response_model=WatchPauseResponse)
async def resume_watches(
    backend: BackendInstance = Depends(get_backend_instance),
) -> WatchPauseResponse:
    """Resume watch-triggered task creation after a pause."""
    if not backend.redis or not backend.redis.is_connected:
        raise HTTPException(status_code=503, detail="Redis unavailable")

    await backend.redis.client.delete(WATCHES_PAUSED_KEY)
    logger.info("▶️ All watches resumed")

    return WatchPauseResponse(paused=False, message="All watches resumed")


# ---------------------------------------------------------------------------
# Ignore list
# ---------------------------------------------------------------------------


@router.put("/ignore/{user_id}", response_model=IgnoreToggleResponse)
async def ignore_user(
    user_id: int,
    site: str = Query(..., description="Site name (onlyfans or fansly)"),
) -> IgnoreToggleResponse:
    """Mark a performer as ignored so they are skipped when scraping."""
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site.lower())
    if not site_db:
        raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

    with site_db.get_session() as session:
        user = session.get(UserModel, user_id)
        if not user:
            raise HTTPException(
                status_code=404,
                detail=f"User {user_id} not found in {site} database",
            )
        user.ignored = True
        session.commit()
        logger.info(
            "🚫 User %s (@%s) added to ignore list [%s]", user_id, user.username, site
        )

    return IgnoreToggleResponse(
        user_id=user_id,
        ignored=True,
        message=f"User {user_id} is now ignored",
    )


@router.delete("/ignore/{user_id}", response_model=IgnoreToggleResponse)
async def unignore_user(
    user_id: int,
    site: str = Query(..., description="Site name (onlyfans or fansly)"),
) -> IgnoreToggleResponse:
    """Remove a performer from the ignore list."""
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site.lower())
    if not site_db:
        raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

    with site_db.get_session() as session:
        user = session.get(UserModel, user_id)
        if not user:
            raise HTTPException(
                status_code=404,
                detail=f"User {user_id} not found in {site} database",
            )
        user.ignored = False
        session.commit()
        logger.info(
            "✅ User %s (@%s) removed from ignore list [%s]",
            user_id,
            user.username,
            site,
        )

    return IgnoreToggleResponse(
        user_id=user_id,
        ignored=False,
        message=f"User {user_id} is no longer ignored",
    )
