"""Subscriptions API routes.

Provides endpoints for listing performer/creator subscriptions.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, case, func, or_
from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import select

from ultima_scraper_db_archive import (
    SubscriptionModel,
    UserAuthModel,
    UserInfoModel,
    UserModel,
    UserProfileMediaModel,
)

from ...database.factory import MultiSiteDatabaseManager
from ...services.profile_refresh import queue_cached_profile_refresh
from ...services import favourites_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_expired(expires_at: datetime | None) -> bool:
    """Check if a subscription is expired, handling naive datetimes."""
    if expires_at is None:
        return False
    now = _utc_now()
    # Handle naive datetime from database by assuming UTC
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at <= now


def _load_latest_profile_media_map(
    site_db: object,
    user_ids: set[int],
) -> dict[int, UserProfileMediaModel]:
    if not user_ids:
        return {}

    with site_db.get_session() as session:
        latest_media_subquery = (
            select(
                UserProfileMediaModel.user_id.label("user_id"),
                func.max(UserProfileMediaModel.created_at).label("created_at"),
            )
            .where(UserProfileMediaModel.user_id.in_(user_ids))
            .group_by(UserProfileMediaModel.user_id)
            .subquery()
        )

        rows = session.exec(
            select(UserProfileMediaModel).join(
                latest_media_subquery,
                and_(
                    UserProfileMediaModel.user_id == latest_media_subquery.c.user_id,
                    UserProfileMediaModel.created_at
                    == latest_media_subquery.c.created_at,
                ),
            )
        ).all()
        return {row.user_id: row for row in rows}


class SubscriptionInfo(BaseModel):
    """Subscription/performer information."""

    id: int
    user_id: int
    username: str | None = None
    name: str | None = None
    description: str | None = None
    sex: int | None = None
    gender: int | None = None
    profile_type: int | None = None
    sexual_content: int | None = None
    partner_sex: int | None = None
    partner_gender: int | None = None
    avatar_url: str | None = None
    header_url: str | None = None
    price: float = 0.0
    post_count: int = 0
    media_count: int = 0
    image_count: int = 0
    video_count: int = 0
    is_active: bool = True
    expires_at: datetime | None = None
    ignored: bool = False
    favorite: bool = False
    last_checked_at: datetime | None = None
    site: str


class SubscriptionListResponse(BaseModel):
    """Response for listing subscriptions."""

    success: bool
    subscriptions: List[SubscriptionInfo] = []
    total: int = 0
    error: str | None = None


class SubscriptionStatsResponse(BaseModel):
    """Stats about subscriptions per site."""

    site: str
    total: int
    active: int
    expired: int


@router.get("/sites")
async def get_subscription_sites() -> List[str]:
    """Get list of supported sites for subscriptions."""
    return ["onlyfans", "fansly"]


@router.get("/", response_model=SubscriptionListResponse)
def list_subscriptions(
    background_tasks: BackgroundTasks = None,
    site: str | None = Query(None, description="Filter by site"),
    search: str | None = Query(
        None, description="Search by identifier (username or user ID) or name"
    ),
    active_only: bool = Query(False, description="Only show active subscriptions"),
    auth_id: int | None = Query(None, description="Filter by auth ID"),
    sex: List[int] | None = Query(
        None,
        description="Filter by sex (repeatable): 0=female, 1=male, 2=other",
    ),
    order_by: str = Query(
        "id",
        description="Order by field: id, created_at, username, expires_at, price, post_count",
    ),
    order_dir: str = Query("desc", description="Order direction: asc or desc"),
    limit: int = Query(100, ge=1, le=500, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> SubscriptionListResponse:
    """List all subscriptions/performers across sites."""
    try:
        if sex is not None:
            invalid_sex = [v for v in sex if v not in (0, 1, 2)]
            if invalid_sex:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid sex filter value(s). Allowed values: 0=female, 1=male, 2=other",
                )

        db_manager = MultiSiteDatabaseManager()
        sites_to_query = [site.lower()] if site else ["onlyfans", "fansly"]
        single_site_query = len(sites_to_query) == 1
        raw_rows: list[tuple[str, Mapping[str, object]]] = []
        total_count = 0

        trimmed_search = search.strip() if search else ""
        search_pattern = f"%{trimmed_search}%" if trimmed_search else None
        search_id = int(trimmed_search) if trimmed_search.isdigit() else None

        def _apply_order(stmt: object, query_source: object) -> object:
            if order_by == "expires_at":
                column = query_source.c.expires_at
                if order_dir.lower() == "desc":
                    return stmt.order_by(column.desc().nullsfirst())
                return stmt.order_by(column.asc().nullslast())
            if order_by == "created_at":
                column = query_source.c.created_at
            if order_by == "price":
                column = query_source.c.price
            elif order_by == "post_count":
                column = query_source.c.post_count
            elif order_by == "username":
                column = func.lower(func.coalesce(query_source.c.username, ""))
            elif order_by != "created_at":
                column = query_source.c.user_id

            if order_dir.lower() == "desc":
                return stmt.order_by(column.desc(), query_source.c.user_id.desc())
            return stmt.order_by(column.asc(), query_source.c.user_id.asc())

        for site_name in sites_to_query:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue

            with site_db.get_session() as session:
                active_rank = case((SubscriptionModel.active.is_(True), 1), else_=0)
                no_expiry_rank = case(
                    (SubscriptionModel.expires_at.is_(None), 1), else_=0
                )
                auth_active_rank = case((UserAuthModel.active.is_(True), 1), else_=0)

                query = (
                    select(
                        UserModel.id.label("user_id"),
                        UserModel.username.label("username"),
                        UserModel.ignored.label("ignored"),
                        UserModel.last_checked_at.label("last_checked_at"),
                        favourites_service.user_expression(site_name).label("favorite"),
                        SubscriptionModel.created_at.label("created_at"),
                        UserInfoModel.name.label("name"),
                        UserInfoModel.description.label("description"),
                        UserInfoModel.sex.label("sex"),
                        UserInfoModel.gender.label("gender"),
                        UserInfoModel.profile_type.label("profile_type"),
                        UserInfoModel.sexual_content.label("sexual_content"),
                        UserInfoModel.partner_sex.label("partner_sex"),
                        UserInfoModel.partner_gender.label("partner_gender"),
                        UserInfoModel.price.label("price"),
                        UserInfoModel.post_count.label("post_count"),
                        UserInfoModel.media_count.label("media_count"),
                        UserInfoModel.image_count.label("image_count"),
                        UserInfoModel.video_count.label("video_count"),
                        SubscriptionModel.id.label("subscription_id"),
                        SubscriptionModel.active.label("subscription_active"),
                        SubscriptionModel.expires_at.label("expires_at"),
                        UserAuthModel.active.label("auth_active"),
                        func.row_number()
                        .over(
                            partition_by=UserModel.id,
                            order_by=(
                                active_rank.desc(),
                                no_expiry_rank.desc(),
                                SubscriptionModel.expires_at.desc().nullslast(),
                                auth_active_rank.desc(),
                                SubscriptionModel.id.desc(),
                            ),
                        )
                        .label("subscription_rank"),
                    )
                    .join(SubscriptionModel, SubscriptionModel.user_id == UserModel.id)
                    .join(
                        UserAuthModel,
                        SubscriptionModel.subscriber_id == UserAuthModel.user_id,
                    )
                    .outerjoin(UserInfoModel, UserModel.id == UserInfoModel.user_id)
                )

                if trimmed_search and search_pattern is not None:
                    conditions: list[ColumnElement[bool]] = [
                        UserModel.username.ilike(search_pattern),
                        UserInfoModel.name.ilike(search_pattern),
                    ]
                    if search_id is not None:
                        conditions.append(UserModel.id == search_id)
                    query = query.where(or_(*conditions))

                now_naive = _utc_now().replace(tzinfo=None)
                query = query.where(
                    (SubscriptionModel.expires_at.is_(None))
                    | (SubscriptionModel.expires_at > now_naive)
                )

                if active_only:
                    query = query.where(
                        SubscriptionModel.active.is_(True),
                        UserAuthModel.active.is_(True),
                    )

                if auth_id is not None:
                    query = query.where(UserAuthModel.id == auth_id)
                if sex is not None:
                    query = query.where(
                        or_(
                            UserInfoModel.sex.in_(sex),
                            UserInfoModel.partner_sex.in_(sex),
                        )
                    )

                ranked_query = query.subquery()
                deduped_query = (
                    select(ranked_query)
                    .where(ranked_query.c.subscription_rank == 1)
                    .subquery()
                )

                site_total = int(
                    session.exec(select(func.count()).select_from(deduped_query)).one()
                    or 0
                )
                total_count += site_total
                if site_total <= 0:
                    continue

                page_query = _apply_order(select(deduped_query), deduped_query)
                if single_site_query:
                    page_query = page_query.offset(offset).limit(limit)
                site_rows = session.execute(page_query).all()
                raw_rows.extend((site_name, row._mapping) for row in site_rows)

        # Sort the results based on order_by and order_dir
        reverse = order_dir.lower() == "desc"

        def get_sort_key(item: tuple[str, Mapping[str, object]]):
            _, row = item
            if order_by == "expires_at":
                expires_at = row["expires_at"]
                if expires_at is None:
                    return (1, datetime.max) if not reverse else (0, datetime.min)
                return (0, expires_at) if not reverse else (1, expires_at)
            if order_by == "price":
                return float(row["price"] or 0.0)
            if order_by == "post_count":
                return int(row["post_count"] or 0)
            if order_by == "created_at":
                return row["created_at"] or datetime.min
            if order_by == "id":
                return int(row["user_id"])
            return str(row["username"] or "").lower()

        if not single_site_query:
            raw_rows.sort(key=get_sort_key, reverse=reverse)
            raw_rows = raw_rows[offset : offset + limit]

        user_ids_by_site: dict[str, set[int]] = {}
        for site_name, row in raw_rows:
            user_ids_by_site.setdefault(site_name, set()).add(int(row["user_id"]))

        latest_media_by_site: dict[str, dict[int, UserProfileMediaModel]] = {}
        for site_name, user_ids in user_ids_by_site.items():
            site_db = db_manager.get_or_create_manager(site_name)
            if site_db is None:
                continue
            latest_media_by_site[site_name] = _load_latest_profile_media_map(
                site_db, user_ids
            )

        subscriptions: List[SubscriptionInfo] = []
        for site_name, row in raw_rows:
            latest_media = latest_media_by_site.get(site_name, {}).get(
                int(row["user_id"])
            )
            expires_at = row["expires_at"]
            is_expired = _is_expired(expires_at)
            subscriptions.append(
                SubscriptionInfo(
                    id=int(row["subscription_id"]),
                    user_id=int(row["user_id"]),
                    username=row["username"],
                    name=row["name"],
                    description=row["description"],
                    sex=row["sex"],
                    gender=row["gender"],
                    profile_type=row["profile_type"],
                    sexual_content=row["sexual_content"],
                    partner_sex=row["partner_sex"],
                    partner_gender=row["partner_gender"],
                    avatar_url=latest_media.avatar_url if latest_media else None,
                    header_url=latest_media.header_url if latest_media else None,
                    price=float(row["price"] or 0.0),
                    post_count=int(row["post_count"] or 0),
                    media_count=int(row["media_count"] or 0),
                    image_count=int(row["image_count"] or 0),
                    video_count=int(row["video_count"] or 0),
                    is_active=bool(
                        row["subscription_active"]
                        and row["auth_active"]
                        and not is_expired
                    ),
                    expires_at=expires_at,
                    ignored=bool(row["ignored"]),
                    favorite=bool(row["favorite"]),
                    last_checked_at=row["last_checked_at"],
                    site=site_name,
                )
            )

        queue_cached_profile_refresh(background_tasks, subscriptions)
        return SubscriptionListResponse(
            success=True,
            subscriptions=subscriptions,
            total=total_count,
        )

    except Exception as e:
        logger.exception("Failed to list subscriptions")
        return SubscriptionListResponse(
            success=False,
            error=str(e),
        )


@router.get("/stats", response_model=List[SubscriptionStatsResponse])
def get_subscription_stats() -> List[SubscriptionStatsResponse]:
    """Get subscription statistics per site."""
    try:
        db_manager = MultiSiteDatabaseManager()
        stats: List[SubscriptionStatsResponse] = []

        for site_name in ["onlyfans", "fansly"]:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue

            with site_db.get_session() as session:
                total = int(
                    session.exec(
                        select(func.count(SubscriptionModel.user_id.distinct()))
                    ).one()
                    or 0
                )
                active = int(
                    session.exec(
                        select(func.count(SubscriptionModel.user_id.distinct())).where(
                            SubscriptionModel.active.is_(True)
                        )
                    ).one()
                    or 0
                )

                stats.append(
                    SubscriptionStatsResponse(
                        site=site_name,
                        total=total,
                        active=active,
                        expired=total - active,
                    )
                )

        return stats

    except Exception as e:
        logger.exception("Failed to get subscription stats")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{user_id}", response_model=SubscriptionInfo)
def get_subscription(
    user_id: int,
    site: str = Query(..., description="Site name"),
) -> SubscriptionInfo:
    """Get detailed information about a specific subscription."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            user = session.exec(
                select(UserModel).where(UserModel.id == user_id)
            ).first()

            if not user:
                raise HTTPException(status_code=404, detail="Subscription not found")

            info = session.exec(
                select(UserInfoModel).where(UserInfoModel.user_id == user_id)
            ).first()

            return SubscriptionInfo(
                id=user.id,
                user_id=user.id,
                username=user.username,
                name=info.name if info else None,
                description=info.description if info else None,
                sex=info.sex if info else None,
                price=info.price if info else 0.0,
                post_count=info.post_count if info else 0,
                media_count=info.media_count if info else 0,
                image_count=info.image_count if info else 0,
                video_count=info.video_count if info else 0,
                is_active=user.active,
                site=site.lower(),
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get subscription")
        raise HTTPException(status_code=500, detail=str(e)) from e
