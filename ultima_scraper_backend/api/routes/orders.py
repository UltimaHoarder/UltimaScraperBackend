"""Orders API routes."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, func
from sqlalchemy import select as sa_select
from sqlmodel import Session as SQLModelSession
from sqlmodel import select as sql_select
from ultima_scraper_api import (
    SUPPORTED_SITES_LITERALS,
    ContentType,
    auth_types,
    user_types,
)

from ultima_scraper_db_archive import (
    UserAuthModel,
    UserInfoModel,
    UserModel,
)

from ...database.factory import MultiSiteDatabaseManager
from ...database.helpers.user_sync import schedule_user_sync, sync_user_from_api
from ...database.orders_db import (
    OrderedPerformerModel,
    OrderedPpvModel,
    OrderModel,
    SiteModel,
    create_orders_session,
    ensure_orders_schema,
    get_or_create_site,
)
from ...helpers.background_tasks import spawn_background_task
from ...job_system import (
    Job,
    JobManager,
    JobStatus,
    Task,
    TaskType,
    build_task,
)
from ...manager import BackendManager
from ...models.backend import BackendInstance
from ...redis.client import RedisManager
from ..dependencies import get_backend_instance, get_backend_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])


async def _refresh_spender_balance_after_purchase(
    *,
    site: str,
    authed: auth_types,
    auth_id: int,
) -> float | None:
    site_name = _normalise_site(site)
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_name)
    if not site_db:
        logger.debug("No database manager for site %s", site_name)
        return None

    refreshed_user: object | None = None

    get_user_attr = getattr(authed, "get_user", None)
    if callable(get_user_attr):
        try:
            refreshed_user = await cast(
                Callable[..., Awaitable[object]], get_user_attr
            )(str(auth_id), refresh=True)
        except Exception as exc:
            logger.debug(
                "Failed to refresh spender %s balance via get_user on %s: %s",
                auth_id,
                site_name,
                exc,
            )

    if refreshed_user is None:
        get_authed_user_attr = getattr(authed, "get_authed_user", None)
        if callable(get_authed_user_attr):
            try:
                refreshed_user = await cast(
                    Callable[[], Awaitable[object]], get_authed_user_attr
                )()
            except Exception as exc:
                logger.debug(
                    "Failed to refresh spender %s via get_authed_user on %s: %s",
                    auth_id,
                    site_name,
                    exc,
                )

    if refreshed_user is None:
        get_me_attr = getattr(authed, "get_me", None)
        if callable(get_me_attr):
            try:
                refreshed_user = await cast(
                    Callable[[], Awaitable[object]], get_me_attr
                )()
            except Exception as exc:
                logger.debug(
                    "Failed to refresh spender %s via get_me on %s: %s",
                    auth_id,
                    site_name,
                    exc,
                )

    if refreshed_user is None and hasattr(authed, "user"):
        refreshed_user = getattr(authed, "user")

    if refreshed_user is None or not hasattr(refreshed_user, "id"):
        logger.debug(
            "No refreshed spender profile available for auth_id=%s on %s",
            auth_id,
            site_name,
        )
        return None

    try:
        await sync_user_from_api(
            site_db, cast(user_types, refreshed_user), site=site_name
        )
    except Exception as exc:
        logger.debug(
            "Failed to sync refreshed spender %s balance on %s: %s",
            auth_id,
            site_name,
            exc,
            exc_info=True,
        )
    return _extract_balance(refreshed_user)


class OrderedPerformerCreateRequest(BaseModel):
    """Request to create a performer order."""

    site: str = Field(..., description="Site name (onlyfans, fansly)")
    user_id: int | None = Field(None, description="Performer user ID")
    username: str | None = Field(None, description="Performer username")
    price: float | None = Field(None, description="Subscription price override")
    post_id: int | None = Field(None, description="PPV post/message identifier")
    content_type: str | None = Field(None, description="Content type (post, message)")
    auto_execute: bool = Field(True, description="Purchase immediately")


class OrderedPerformerCreateResponse(BaseModel):
    """Response for performer order creation."""

    success: bool
    order_id: int | None = None
    ordered_performer_id: int | None = None
    site: str | None = None
    user_id: int | None = None
    username: str | None = None
    price: float | None = None
    spender_user_id: int | None = None
    status: str | None = None
    error: str | None = None
    message: str | None = None


class AssignSpenderRequest(BaseModel):
    """Request to manually assign a spender to an ordered performer."""

    spender_user_id: int | None = Field(
        None, description="Spender auth user ID, or null to unassign"
    )


class OrderedPerformerInfo(BaseModel):
    """Performer order line item."""

    id: int
    order_id: int
    site: str
    user_id: int
    username: str | None = None
    price: float | None = None
    status: str
    spender_user_id: int | None = None
    error_message: str | None = None
    created_at: str
    updated_at: str | None = None


class OrderedPerformerListResponse(BaseModel):
    """List response for performer orders."""

    success: bool
    items: list[OrderedPerformerInfo] = []
    total: int = 0
    error: str | None = None


class OrderedPerformerActionResponse(BaseModel):
    """Response for actions on a performer order."""

    success: bool
    item: OrderedPerformerInfo | None = None
    error: str | None = None


class OrderSummary(BaseModel):
    """Summary information about an order."""

    id: int
    status: str
    total_price: float | None = None
    created_at: str
    updated_at: str | None = None
    item_count: int = 0


class OrderItemInfo(BaseModel):
    """Order item details for display."""

    id: int
    user_id: int
    username: str | None = None
    price: float | None = None
    status: str
    spender_user_id: int | None = None
    spender_username: str | None = None
    error_message: str | None = None


class OrderListResponse(BaseModel):
    """Response for listing orders."""

    success: bool
    orders: list[OrderSummary] = []
    total: int = 0
    error: str | None = None


class OrderDetailResponse(BaseModel):
    """Response for order details."""

    success: bool
    order: OrderSummary | None = None
    items: list[OrderItemInfo] = []
    error: str | None = None


class SpenderAssignment(BaseModel):
    """A spender and the items assigned to it during calculation."""

    auth_id: int
    username: str | None = None
    spender_type: str | None = None
    balance: float = 0.0
    assigned_count: int = 0
    assigned_cost: float = 0.0


class CalculateOrderResponse(BaseModel):
    """Response for the calculate preview endpoint."""

    success: bool
    order: OrderSummary | None = None
    items: list[OrderItemInfo] = []
    spenders: list[SpenderAssignment] = []
    unassigned_count: int = 0
    total_cost: float = 0.0
    error: str | None = None


def _extract_balance(user: object) -> float | None:
    for attr in ("credit_balance", "balance"):
        value = getattr(user, attr, None)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _get_stored_user_balances(site_name: str, auth_ids: set[int]) -> dict[int, float]:
    """Return balances from site DB for many spender auth IDs in one query."""
    if not auth_ids:
        return {}
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            return {}

        with site_db.get_session() as session:
            rows: list[tuple[int, float | None]] = list(
                session.exec(
                    sql_select(UserModel.id, UserModel.balance).where(
                        UserModel.id.in_(list(auth_ids))  # type: ignore[attr-defined]
                    )
                ).all()
            )
            return {
                int(user_id): float(balance) if balance is not None else 0.0
                for user_id, balance in rows
            }
    except Exception:
        return {}


def _build_performer_info(
    performer: OrderedPerformerModel, site_name: str
) -> OrderedPerformerInfo:
    return OrderedPerformerInfo(
        id=performer.id,
        order_id=performer.order_id,
        site=site_name,
        user_id=performer.user_id,
        username=performer.username,
        price=performer.price,
        status=performer.status,
        spender_user_id=performer.spender_user_id,
        error_message=performer.error_message,
        created_at=performer.created_at.isoformat(),
        updated_at=performer.updated_at.isoformat() if performer.updated_at else None,
    )


def _build_order_summary(order: OrderModel, item_count: int) -> OrderSummary:
    return OrderSummary(
        id=order.id,
        status=order.status,
        total_price=order.total_price,
        created_at=order.created_at.isoformat(),
        updated_at=order.updated_at.isoformat() if order.updated_at else None,
        item_count=item_count,
    )


def _build_order_items(
    items: list[OrderedPerformerModel],
    spender_usernames: dict[int, str | None] | None = None,
) -> list[OrderItemInfo]:
    name_map = spender_usernames or {}
    return [
        OrderItemInfo(
            id=item.id,
            user_id=item.user_id,
            username=item.username,
            price=item.price,
            status=item.status,
            spender_user_id=item.spender_user_id,
            spender_username=(
                name_map.get(item.spender_user_id) if item.spender_user_id else None
            ),
            error_message=item.error_message,
        )
        for item in items
    ]


def _resolve_spender_usernames(site: str, auth_ids: set[int]) -> dict[int, str | None]:
    """Look up usernames for a set of spender auth/user IDs."""
    if not auth_ids:
        return {}
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site)
    if not site_db:
        return {}

    with site_db.get_session() as session:
        session: SQLModelSession = session
        rows = session.exec(
            sql_select(UserModel.id, UserModel.username).where(
                UserModel.id.in_(list(auth_ids))  # type: ignore[attr-defined]
            )
        ).all()
        return {int(uid): uname for uid, uname in rows}


def _recalculate_order_total(
    items: list[OrderedPerformerModel],
) -> float | None:
    total = 0.0
    has_price = False
    for item in items:
        if isinstance(item.price, (int, float)):
            total += float(item.price)
            has_price = True
    return total if has_price else None


def _derive_order_status(items: list[OrderedPerformerModel]) -> str:
    if not items:
        return "pending"

    total_items = len(items)
    completed_count = sum(1 for item in items if item.status == "completed")
    failed_count = sum(1 for item in items if item.status == "failed")
    pending_count = sum(1 for item in items if item.status == "pending")

    if completed_count == total_items:
        return "completed"
    if completed_count == 0 and failed_count > 0 and pending_count == 0:
        return "failed"
    if completed_count == 0 and pending_count == total_items:
        return "pending"
    return "incomplete"


def _merge_execution_price(
    existing_price: float | None,
    resolved_price: float | None,
) -> float | None:
    existing = (
        float(existing_price) if isinstance(existing_price, (int, float)) else None
    )
    resolved = (
        float(resolved_price) if isinstance(resolved_price, (int, float)) else None
    )

    if resolved is None:
        return existing

    # Keep an explicitly-priced paid item from being downgraded to free (0.0)
    # by subscription API responses (e.g., already-subscribed/free edge cases).
    if existing is not None and existing > 0 and resolved <= 0:
        return existing

    if resolved > 0:
        return resolved

    return existing if existing is not None else resolved


def _normalise_site(site: str) -> str:
    return site.strip().lower()


def _coerce_subscription_expires_at(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


async def _upsert_subscription_after_purchase(
    *,
    site: str,
    authed: auth_types,
    subscriber_id: int,
    user_id: int,
    username: str | None,
    price: float | None,
) -> None:
    site_name = _normalise_site(site)
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_name)
    if not site_db:
        logger.debug("No database manager for site %s", site_name)
        return

    identifier: object = username if username else str(user_id)
    subscription_obj: object | None = None
    get_subscription_attr = getattr(authed, "get_subscription", None)
    if callable(get_subscription_attr):
        try:
            subscription_obj = await cast(
                Callable[[object], Awaitable[object]], get_subscription_attr
            )(identifier)
        except Exception as exc:
            logger.debug(
                "Failed to fetch subscription state for %s on %s: %s",
                user_id,
                site_name,
                exc,
            )

    expires_at: datetime | None = None
    is_active = True
    if subscription_obj is not None:
        raw_expires = getattr(subscription_obj, "subscribed_by_expire_date", None)
        if callable(raw_expires):
            try:
                raw_expires = raw_expires()
            except Exception:
                raw_expires = None
        expires_at = _coerce_subscription_expires_at(raw_expires)

        active_attr = getattr(subscription_obj, "is_active", None)
        active_value: object | None
        if callable(active_attr):
            try:
                active_value = active_attr()
            except Exception:
                active_value = None
        else:
            active_value = active_attr
        if isinstance(active_value, bool):
            is_active = active_value

    try:
        existing_user = site_db.get_user(user_id)
        if existing_user is None:
            site_db.create_or_update_user(
                user_data={
                    "id": user_id,
                    "username": username or f"user_{user_id}",
                }
            )

        sub_data: dict[str, object] = {
            "active": is_active,
            "paid_content": bool(price is not None and price > 0),
        }
        if expires_at is not None:
            sub_data["expires_at"] = expires_at
        else:
            logger.debug(
                "No expires_at resolved for user %s on %s; subscription will be stored without expiry",
                user_id,
                site_name,
            )

        site_db.create_or_update_subscription(
            user_id=user_id,
            subscriber_id=subscriber_id,
            data=sub_data,
        )
    except Exception as exc:
        logger.debug(
            "Failed to persist subscription row for user %s on %s: %s",
            user_id,
            site_name,
            exc,
            exc_info=True,
        )


def _add_to_watch_list(site: str, user_id: int) -> None:
    """Mark a user as watched in the site database after a successful subscription."""
    site_name = _normalise_site(site)
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            logger.debug(
                "No database manager for site %s, cannot add to watch list", site_name
            )
            return
        with site_db.get_session() as session:
            user = session.get(UserModel, user_id)
            if user is None:
                logger.debug(
                    "User %s not found in %s DB; skipping watch list update",
                    user_id,
                    site_name,
                )
                return
            if not user.watched:
                user.watched = True
                session.commit()
                logger.info(
                    "👁️ Auto-added user %s (@%s) to watch list after subscription [%s]",
                    user_id,
                    user.username,
                    site_name,
                )
    except Exception as exc:
        logger.debug(
            "Failed to add user %s to watch list on %s: %s",
            user_id,
            site_name,
            exc,
            exc_info=True,
        )


def _resolve_user_from_db(
    site: str, user_id: int | None, username: str | None
) -> tuple[int, str | None, float | None] | None:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site)
    if not site_db:
        return None

    with site_db.get_session() as session:
        session: SQLModelSession = session
        query = sql_select(UserModel, UserInfoModel).outerjoin(
            UserInfoModel, UserModel.id == UserInfoModel.user_id
        )
        if user_id is not None:
            query = query.where(UserModel.id == user_id)
        elif username:
            # The lower() term lets idx_users_username_lower find the row.
            query = query.where(
                func.lower(UserModel.username) == func.lower(username),
                UserModel.username == username,
            )
        else:
            return None

        result = session.exec(query).first()
        if not result:
            return None
        user, info = result
        price = info.price if info else None
        return user.id, user.username, price


def _get_candidate_auth_ids(
    site: str, price: float | None = None
) -> list[tuple[int, str | None]]:
    """Get candidate auth IDs with their spender_type.

    Returns list of (auth_id, spender_type) tuples filtered by price if provided.
    """
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site)
    if not site_db:
        return []

    with site_db.get_session() as session:
        session: SQLModelSession = session
        query = (
            sql_select(UserAuthModel.user_id, UserModel.spender_type)
            .join(UserModel, UserAuthModel.user_id == UserModel.id)
            .where(UserAuthModel.active.is_(True))
            .where(UserModel.spend.is_(True))
        )
        results = session.exec(query).all()

        # Filter by spender_type based on price
        candidates: list[tuple[int, str | None]] = []
        for user_id, spender_type in results:
            # Default to "all" if spender_type is None
            sp_type = spender_type

            # Filter based on price and spender_type
            if price is not None:
                is_free = price == 0.0
                if sp_type == "free" and not is_free:
                    # Spender only handles free, but subscription is paid
                    continue
                elif sp_type == "paid" and is_free:
                    # Spender only handles paid, but subscription is free
                    continue
                # "all" accepts both free and paid

            candidates.append((int(user_id), sp_type))

        return candidates


def _get_any_active_auth_id(site: str) -> int | None:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site)
    if not site_db:
        return None

    with site_db.get_session() as session:
        session: SQLModelSession = session
        auth = session.exec(
            sql_select(UserAuthModel).where(UserAuthModel.active.is_(True)).limit(1)
        ).first()
        if not auth:
            return None
        return int(auth.user_id)


def _get_site_url(site: str) -> str | None:
    if site == "onlyfans":
        return "https://onlyfans.com"
    if site == "fansly":
        return "https://fansly.com"
    return None


_SITE_NAME_MAP: dict[str, SUPPORTED_SITES_LITERALS] = {
    "onlyfans": "OnlyFans",
    "fansly": "Fansly",
}

# Content types to scrape+download after a purchase
_POST_PURCHASE_CONTENT_TYPES: list[ContentType] = [
    ContentType.PROFILE,
    ContentType.POSTS,
    ContentType.STORIES,
    ContentType.HIGHLIGHTS,
    ContentType.MESSAGES,
    ContentType.ARCHIVED_POSTS,
]


async def _create_scrape_download_job(
    site: str,
    auth_id: int,
    user_id: int,
    username: str | None,
    backend_manager: BackendManager,
    backend: BackendInstance | None = None,
) -> None:
    """Create a Scrape+Download job for a purchased performer.

    Attempts to use the built-in "Scrape + Download" JobTemplate when Redis is
    available.  Falls back to manual task construction for local-queue deployments.

    Args:
        site: Site name (onlyfans, fansly)
        auth_id: Authentication ID to use for scraping
        user_id: Performer user ID
        username: Performer username (required for display / template rendering)
        backend_manager: Backend manager for job storage
        backend: Optional backend instance override
    """
    if not username:
        logger.warning("Cannot create S+D job without username for user_id=%s", user_id)
        return

    site_name = _SITE_NAME_MAP.get(site.lower())
    if not site_name:
        logger.warning("Unknown site %s for S+D job creation", site)
        return

    try:
        backend_instance = backend
        if backend_instance is None:
            try:
                backend_instance = await get_backend_instance()
            except Exception as exc:
                logger.warning(
                    "Failed to get backend instance for S+D enqueue: %s", exc
                )

        redis_manager = getattr(
            backend_manager, "redis", getattr(backend_manager, "_redis_manager", None)
        )

        # Resolve active Redis connection
        _active_redis = None
        if (
            backend_instance
            and backend_instance.distributed_mode
            and backend_instance.redis
            and backend_instance.redis.is_connected
        ):
            _active_redis = backend_instance.redis
        elif redis_manager and getattr(redis_manager, "is_connected", False):
            _active_redis = redis_manager

        if _active_redis is not None:
            # -- Template path --------------------------------------------------
            from ...job_system.templates import JobTemplateStore, seed_builtin_templates

            active_redis = cast(RedisManager, _active_redis)
            store = JobTemplateStore(active_redis)
            template = await store.get_template_by_name("Scrape + Download")
            if template is None:
                await seed_builtin_templates(store)
                template = await store.get_template_by_name("Scrape + Download")

            if template is not None:
                instantiation = template.instantiate(
                    {
                        "site": site_name,
                        "user_id": user_id,
                        "username": username,
                    },
                    created_by="orders_api",
                )
                for task in instantiation.tasks:
                    task.site_name = site_name
                    task.auth_id = auth_id
                    task.user_username = username

                job = Job(
                    name=instantiation.job_name or f"S+D: {username} (Post-Purchase)",
                    tasks=instantiation.tasks,
                    state=JobStatus.PENDING,
                    created_by="orders_api",
                    metadata=instantiation.metadata,
                )
                manager = JobManager(active_redis)
                job = await manager.prepare_job(
                    job,
                    backend_id=(
                        None
                        if backend_instance and backend_instance.distributed_mode
                        else backend_instance.instance_id if backend_instance else None
                    ),
                    select_backend=backend_instance is None,
                )
                await manager.activate_job(
                    job, distributed_mode=bool(backend_instance and backend_instance.distributed_mode),
                )
                logger.info(
                    "Created S+D job %s via template for performer %s (user_id=%s) on %s",
                    job.job_id,
                    username,
                    user_id,
                    site,
                )
                return

        # -- Fallback: manual task construction (local-queue / no Redis) --------
        job_id = str(uuid4())
        job_name = f"S+D: {username} (Post-Purchase)"

        tasks: list[Task] = []
        for ct in _POST_PURCHASE_CONTENT_TYPES:
            scrape_task = build_task(
                task_type=TaskType.SCRAPE,
                site_name=site_name,
                auth_id=auth_id,
                user_id=user_id,
                user_username=username,
                content_type=ct,
                download_media=True,
                save_to_db=True,
            )
            tasks.append(scrape_task)

            download_task = build_task(
                task_type=TaskType.DOWNLOAD,
                site_name=site_name,
                auth_id=auth_id,
                user_id=user_id,
                user_username=username,
                content_type=ct,
            )
            download_task.depends_on = [scrape_task.task_id]
            tasks.append(download_task)

        job = Job(
            job_id=job_id,
            name=job_name,
            tasks=tasks,
            state=JobStatus.PENDING,
            created_by="orders_api",
        )

        if backend_instance is not None:
            await cast(Any, backend_instance.active_queue).enqueue(job)
            logger.info(
                "Created S+D job %s for performer %s (user_id=%s) on %s via local queue",
                job_id,
                username,
                user_id,
                site,
            )
        else:
            logger.warning(
                "Redis not connected and backend unavailable, cannot create S+D job"
            )
    except Exception as exc:
        logger.exception("Failed to create S+D job for performer %s: %s", user_id, exc)


def _knapsack_optimize(
    items: list[OrderedPerformerModel],
    spenders: list[tuple[int, float]],  # (auth_id, balance)
) -> dict[int, list[OrderedPerformerModel]]:
    """Optimize item allocation across spenders using 0/1 knapsack DP.

    Algorithm:
    1. For each spender, run DP knapsack to find optimal item subset
    2. Use dynamic programming table: dp[i][b] = max items with budget b using first i items
    3. Track which items are included in optimal solution
    4. Return allocation: {spender_auth_id: [items]}

    This ensures optimal packing of items into available spender balances,
    maximizing the number of successfully purchased items.

    Args:
        items: List of OrderedPerformerModel items to allocate
        spenders: List of (auth_id, balance) tuples

    Returns:
        Dictionary mapping spender auth_id to list of items to purchase
    """
    allocation: dict[int, list[OrderedPerformerModel]] = {}
    remaining_items: list[OrderedPerformerModel] = items.copy()

    # Sort spenders by balance (descending) to prioritize high-balance accounts
    sorted_spenders = sorted(spenders, key=lambda x: x[1], reverse=True)

    for auth_id, balance in sorted_spenders:
        if not remaining_items:
            break

        # Convert balance to cents for integer DP
        balance_cents = int(balance * 100)

        # Build DP table
        n = len(remaining_items)
        dp = [[0] * (balance_cents + 1) for _ in range(n + 1)]
        included: list[list[list[int]]] = [
            [[] for _ in range(balance_cents + 1)] for _ in range(n + 1)
        ]

        for i in range(1, n + 1):
            item = remaining_items[i - 1]
            item_price = item.price if item.price is not None else 0.0
            item_cents = int(item_price * 100)

            for b in range(balance_cents + 1):
                # Option 1: Don't take this item
                dp[i][b] = dp[i - 1][b]
                included[i][b] = included[i - 1][b].copy()

                # Option 2: Take this item (if it fits)
                if item_cents <= b:
                    take_value = dp[i - 1][b - item_cents] + 1
                    if take_value > dp[i][b]:
                        dp[i][b] = take_value
                        included[i][b] = included[i - 1][b - item_cents].copy()
                        included[i][b].append(i - 1)  # Item index

        # Extract optimal solution
        best_count = dp[n][balance_cents]
        best_indices: list[int] = included[n][balance_cents]

        if best_count > 0:
            allocated_items = [remaining_items[idx] for idx in best_indices]
            allocation[auth_id] = allocated_items

            # Remove allocated items from remaining pool
            remaining_items = [
                item for i, item in enumerate(remaining_items) if i not in best_indices
            ]

    return allocation


async def _select_spender(
    site: str, price: float | None, backend_manager: BackendManager
) -> tuple[auth_types, int, float | None] | None:
    """Select a spender auth that can handle the subscription/PPV price.

    Algorithm:
    1. Get all active spenders for the site (UserModel.spend == True)
    2. Filter by spender_type and price:
       - "all": can purchase any price (free or paid)
       - "free": only $0.00 items
       - "paid": only items > $0.00
    3. Check each spender's balance to ensure they can afford the item
    4. Sort by balance (lowest first) to distribute load evenly
    5. Select the spender with the lowest balance that can afford it

    This greedy approach ensures:
    - No single spender accumulates all purchases
    - Free accounts only handle free content
    - Paid-only spenders don't waste time on free content
    - Individual items are purchased by spenders who can afford them

    For orders with multiple items, this is called once per item, allowing
    different spenders to handle different items based on their balances.

    Args:
        site: Site name (onlyfans, fansly)
        price: Item price (None means unknown/check anyway)
        backend_manager: Backend manager for auth sessions

    Returns:
        Tuple of (auth_session, spender_user_id, balance) or None if no suitable spender
    """
    candidate_auth_ids = _get_candidate_auth_ids(site, price)
    if not candidate_auth_ids:
        return None

    candidates: list[tuple[float, auth_types, int]] = []
    for auth_id, _spender_type in candidate_auth_ids:
        authed = await backend_manager.get_authed_session(site, auth_id)
        if not authed:
            continue
        get_authed_user = getattr(authed, "get_authed_user", None)
        if not callable(get_authed_user):
            continue
        user = await cast(Callable[[], Awaitable[object]], get_authed_user)()
        balance = _extract_balance(user)

        # Check if spender has sufficient balance for paid items
        if price is not None and price > 0:
            if balance is None or balance < price:
                continue

        balance_value = balance if balance is not None else 0.0
        candidates.append((balance_value, authed, auth_id))

    if not candidates:
        return None

    # Sort by balance (lowest first) to distribute load across spenders
    candidates.sort(key=lambda item: item[0])
    best_balance, best_authed, best_auth_id = candidates[0]
    return best_authed, best_auth_id, best_balance


async def _get_cached_authed_session(
    *,
    site: str,
    auth_id: int | None,
    backend_manager: BackendManager,
    auth_cache: dict[int, auth_types | None],
) -> auth_types | None:
    if auth_id is None:
        return None

    if auth_id in auth_cache:
        return auth_cache[auth_id]

    authed = await backend_manager.get_authed_session(site, auth_id)
    auth_cache[auth_id] = authed
    if authed is None:
        logger.warning(
            "Skipping spender auth_id=%s on %s because no live session could be established",
            auth_id,
            site,
        )
    return authed


async def _build_live_spender_list(
    *,
    site: str,
    candidates: list[tuple[int, str | None]],
    stored_balances: dict[int, float],
    backend_manager: BackendManager,
    auth_cache: dict[int, auth_types | None],
    require_positive_balance: bool,
) -> list[tuple[int, float]]:
    live_spenders: list[tuple[int, float]] = []
    seen_auth_ids: set[int] = set()

    for auth_id, _spender_type in candidates:
        if auth_id in seen_auth_ids:
            continue
        seen_auth_ids.add(auth_id)

        balance = stored_balances.get(auth_id)
        if balance is None:
            continue
        if require_positive_balance and balance <= 0:
            continue

        authed = await _get_cached_authed_session(
            site=site,
            auth_id=auth_id,
            backend_manager=backend_manager,
            auth_cache=auth_cache,
        )
        if authed is None:
            continue

        live_spenders.append((auth_id, balance))

    return live_spenders


async def _execute_subscription(
    authed: auth_types,
    user_id: int,
    username: str | None,
    site: str,
) -> tuple[bool, str | None, float | None]:
    identifier = username if username else str(user_id)
    user_obj: user_types | None = await authed.get_user(identifier)
    if not user_obj:
        return False, "Failed to resolve performer", None

    schedule_user_sync(user_obj, site)

    price = None
    subscription_price_attr = getattr(user_obj, "subscription_price", None)
    if callable(subscription_price_attr):
        price_value = await cast(
            Callable[[], Awaitable[object]], subscription_price_attr
        )()
        if isinstance(price_value, (int, float)):
            price = float(price_value)
    elif isinstance(subscription_price_attr, (int, float)):
        price = float(subscription_price_attr)

    # If we're already subscribed, treat as success and avoid re-purchase API errors.
    get_subscription_attr = getattr(authed, "get_subscription", None)
    if callable(get_subscription_attr):
        try:
            existing_sub = await cast(
                Callable[[object], Awaitable[object]], get_subscription_attr
            )(identifier)
            if existing_sub is not None:
                return True, None, price
        except Exception:
            pass

    # Free subscriptions often don't support buy_subscription; try subscribe if available.
    if price is not None and price <= 0:
        subscribe_attr = getattr(user_obj, "subscribe", None)
        if callable(subscribe_attr):
            try:
                subscribe_result = await cast(
                    Callable[[], Awaitable[object]], subscribe_attr
                )()
                if isinstance(subscribe_result, dict):
                    subscribe_dict = cast(dict[str, object], subscribe_result)
                    error_details = subscribe_dict.get("error")
                    if error_details is None:
                        return True, None, price
                    if isinstance(error_details, dict):
                        err_dict = cast(dict[str, object], error_details)
                        msg_value = err_dict.get("message")
                        if isinstance(msg_value, str):
                            lowered = msg_value.lower()
                            if "already" in lowered or "subscribed" in lowered:
                                return True, None, price
                            return False, msg_value, price
                    return False, "Subscription failed", price
                return True, None, price
            except Exception:
                pass

        # If free and no explicit purchase method works, assume no purchase needed.
        return True, None, price

    buy_subscription_attr = getattr(user_obj, "buy_subscription", None)
    if not callable(buy_subscription_attr):
        return False, "Subscription purchase not supported", price

    result = await cast(Callable[[], Awaitable[object]], buy_subscription_attr)()
    if isinstance(result, dict):
        result_dict = cast(dict[str, object], result)
        error_details = result_dict.get("error")
        if error_details is None:
            return True, None, price
        error_msg = None
        if isinstance(error_details, dict):
            error_dict = cast(dict[str, object], error_details)
            msg_value = error_dict.get("message")
            if isinstance(msg_value, str):
                lowered = msg_value.lower()
                if "already" in lowered or "subscribed" in lowered:
                    return True, None, price
                error_msg = msg_value
        return False, error_msg or "Subscription failed", price
    return True, None, price


async def _execute_ppv(
    authed: auth_types,
    user_id: int,
    post_id: int,
    content_type: str | None,
    site: str,
) -> tuple[bool, str | None, float | None, str | None]:
    user_obj = None
    find_user_attr = getattr(authed, "find_user", None)
    if callable(find_user_attr):
        user_obj = find_user_attr(user_id)
    if not user_obj:
        get_user_attr = getattr(authed, "get_user", None)
        if callable(get_user_attr):
            user_obj = await cast(Callable[[object], Awaitable[object]], get_user_attr)(
                user_id
            )

    if not user_obj:
        return False, "Failed to resolve performer", None, None

    username_value = getattr(user_obj, "username", None)
    username = username_value if isinstance(username_value, str) else None
    schedule_user_sync(cast(user_types, user_obj), site)

    normalized_type = (content_type or "post").strip().lower()
    is_message_ppv = normalized_type in {"message", "mass_message"}

    target_price: float | None = None
    result: object | None = None

    message_obj: object | None = None
    if is_message_ppv:
        get_message_by_id_attr = getattr(user_obj, "get_message_by_id", None)
        if callable(get_message_by_id_attr):
            try:
                message_obj = await cast(
                    Callable[..., Awaitable[object]], get_message_by_id_attr
                )(user_id=user_id, message_id=post_id, limit=10, offset=0)
            except Exception:
                message_obj = None
        if not message_obj:
            get_messages_attr = getattr(user_obj, "get_messages", None)
            if callable(get_messages_attr):
                try:
                    messages = await cast(
                        Callable[..., Awaitable[list[object]]], get_messages_attr
                    )(limit=50)
                    message_obj = next(
                        (m for m in messages if getattr(m, "id", None) == post_id),
                        None,
                    )
                except Exception:
                    message_obj = None

        if not message_obj:
            return False, f"Message {post_id} not found", None, username

        message_price = getattr(message_obj, "price", None)
        if isinstance(message_price, (int, float)):
            target_price = float(message_price)

        # Check if already bought
        can_view = getattr(message_obj, "canViewMedia", None)
        if can_view is True:
            return True, None, target_price, username

        buy_message_attr = getattr(message_obj, "buy_message", None)
        buy_ppv_attr = getattr(message_obj, "buy_ppv", None)
        if callable(buy_message_attr):
            result = await cast(Callable[[], Awaitable[object]], buy_message_attr)()
        elif callable(buy_ppv_attr):
            result = await cast(Callable[[], Awaitable[object]], buy_ppv_attr)()
        else:
            return (
                False,
                "Message does not support PPV purchase",
                target_price,
                username,
            )
    else:
        get_post_attr = getattr(user_obj, "get_post", None)
        post_obj = None
        if callable(get_post_attr):
            try:
                post_obj = await cast(Callable[..., Awaitable[object]], get_post_attr)(
                    identifier=post_id
                )
            except Exception:
                post_obj = None

        if not post_obj:
            return False, f"Post {post_id} not found", None, username

        post_price = getattr(post_obj, "price", None)
        if isinstance(post_price, (int, float)):
            target_price = float(post_price)

        # Check if already bought before attempting purchase
        can_view = getattr(post_obj, "canViewMedia", None)
        if can_view is True:
            return True, None, target_price, username

        buy_ppv_attr = getattr(post_obj, "buy_ppv", None)
        if not callable(buy_ppv_attr):
            return False, "Post does not support PPV purchase", target_price, username
        result = await cast(Callable[[], Awaitable[object]], buy_ppv_attr)()

    if isinstance(result, dict):
        result_dict = cast(dict[str, object], result)
        error_details = result_dict.get("error")
        if error_details is None:
            # Success! Create scrape+download job for messages and mass messages
            if is_message_ppv:
                scrape_job_params = getattr(message_obj, "get_scrape_job_params", None)
                if callable(scrape_job_params):
                    try:
                        raw_job_params = scrape_job_params()
                        job_params = (
                            cast(dict[str, object], raw_job_params)
                            if isinstance(raw_job_params, dict)
                            else {}
                        )
                        raw_site = job_params.get("site")
                        job_site = str(raw_site) if raw_site is not None else "onlyfans"
                        raw_auth_id = job_params.get("auth_id")
                        authed_id_raw = getattr(authed, "id", None)
                        fallback_auth_id = (
                            int(authed_id_raw)
                            if isinstance(authed_id_raw, int)
                            else None
                        )
                        job_auth_id = (
                            int(raw_auth_id)
                            if isinstance(raw_auth_id, int)
                            else fallback_auth_id
                        )
                        raw_user_id = job_params.get("user_id")
                        job_user_id = (
                            int(raw_user_id)
                            if isinstance(raw_user_id, int)
                            else user_id
                        )
                        raw_username = job_params.get("username")
                        job_username = (
                            raw_username if isinstance(raw_username, str) else username
                        )
                        if job_auth_id is None:
                            raise ValueError("Missing auth_id in scrape job params")
                        await _create_scrape_download_job(
                            site=job_site,
                            auth_id=job_auth_id,
                            user_id=job_user_id,
                            username=job_username,
                            backend_manager=get_backend_manager(),
                        )
                    except Exception as exc:
                        logger.debug(
                            "Failed to create scrape+download job after message purchase: %s",
                            exc,
                        )
            return True, None, target_price, username
        if isinstance(error_details, dict):
            error_dict = cast(dict[str, object], error_details)
            message_value = error_dict.get("message")
            if isinstance(message_value, str):
                lowered = message_value.lower()
                # "Bad Request" / "already" from OF API means content is already
                # purchased — treat as success so the order item doesn't stay failed.
                if "bad request" in lowered or "already" in lowered:
                    return True, None, target_price, username
                return False, message_value, target_price, username
        return False, "PPV purchase failed", target_price, username

    return True, None, target_price, username


@router.get("/performers", response_model=OrderedPerformerListResponse)
async def list_ordered_performers(
    site: str | None = Query(None, description="Filter by site"),
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> OrderedPerformerListResponse:
    """List performer order line items."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            query = sa_select(OrderedPerformerModel, SiteModel).join(
                SiteModel, OrderedPerformerModel.site_id == SiteModel.id
            )

            if site:
                query = query.where(SiteModel.name == _normalise_site(site))
            if status:
                query = query.where(OrderedPerformerModel.status == status)

            total_query = sa_select(func.count()).select_from(OrderedPerformerModel)
            if site:
                total_query = total_query.join(SiteModel).where(
                    SiteModel.name == _normalise_site(site)
                )
            if status:
                total_query = total_query.where(OrderedPerformerModel.status == status)

            total = session.execute(total_query).scalar_one()

            query = query.order_by(OrderedPerformerModel.created_at.desc())
            query = query.offset(offset).limit(limit)
            rows = session.execute(query).all()

            items: list[OrderedPerformerInfo] = []
            for performer, site_model in rows:
                items.append(_build_performer_info(performer, site_model.name))

            return OrderedPerformerListResponse(success=True, items=items, total=total)
    except Exception as exc:
        logger.exception("Failed to list performer orders")
        return OrderedPerformerListResponse(
            success=False, error=str(exc), items=[], total=0
        )


@router.get("/", response_model=OrderListResponse)
async def list_orders(
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> OrderListResponse:
    """List orders with summary details."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            total_query = sa_select(func.count()).select_from(OrderModel)
            if status:
                total_query = total_query.where(OrderModel.status == status)
            total = session.execute(total_query).scalar_one()

            orders_query = sa_select(
                OrderModel,
                func.count(OrderedPerformerModel.id).label("item_count"),
            ).outerjoin(
                OrderedPerformerModel, OrderedPerformerModel.order_id == OrderModel.id
            )
            if status:
                orders_query = orders_query.where(OrderModel.status == status)
            orders_query = orders_query.group_by(OrderModel.id)
            orders_query = orders_query.order_by(OrderModel.created_at.desc())
            orders_query = orders_query.offset(offset).limit(limit)
            rows = session.execute(orders_query).all()

            summaries: list[OrderSummary] = []
            for order, item_count in rows:
                summaries.append(_build_order_summary(order, int(item_count or 0)))

            return OrderListResponse(success=True, orders=summaries, total=total)
    except Exception as exc:
        logger.exception("Failed to list orders")
        return OrderListResponse(success=False, error=str(exc), orders=[], total=0)


@router.get("/{order_id}", response_model=OrderDetailResponse)
async def get_order_details(order_id: int) -> OrderDetailResponse:
    """Fetch order details and items."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()

            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            items = (
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order_id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )

            summary = _build_order_summary(order, len(items))
            return OrderDetailResponse(
                success=True,
                order=summary,
                items=_build_order_items(list(items)),
            )
    except Exception as exc:
        logger.exception("Failed to get order details")
        return OrderDetailResponse(success=False, error=str(exc))


@router.post("/performers", response_model=OrderedPerformerCreateResponse)
async def create_ordered_performer(
    request: OrderedPerformerCreateRequest,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> OrderedPerformerCreateResponse:
    """Create a performer order line item."""
    ensure_orders_schema()

    site = _normalise_site(request.site)
    if not site:
        raise HTTPException(status_code=400, detail="Site is required")

    user_info = _resolve_user_from_db(site, request.user_id, request.username)
    if not user_info:
        if not request.auto_execute and request.user_id is not None:
            user_info = (
                request.user_id,
                request.username or f"user_{request.user_id}",
                request.price,
            )
        else:
            auth_id = _get_any_active_auth_id(site)
            if auth_id is None:
                return OrderedPerformerCreateResponse(
                    success=False,
                    error="No authenticated accounts available",
                    status="failed",
                )
            authed = await backend_manager.get_authed_session(site, auth_id)
            if not authed:
                return OrderedPerformerCreateResponse(
                    success=False,
                    error="Failed to authenticate account",
                    status="failed",
                )
            if not request.username and request.user_id is None:
                return OrderedPerformerCreateResponse(
                    success=False,
                    error="username or user_id is required",
                    status="failed",
                )
            identifier = request.username or str(request.user_id)
            user_obj = await authed.get_user(identifier)
            if not user_obj:
                return OrderedPerformerCreateResponse(
                    success=False,
                    error="Performer not found",
                    status="failed",
                )
            schedule_user_sync(user_obj, site)
            price = request.price
            subscription_price_attr = getattr(user_obj, "subscription_price", None)
            if price is None and callable(subscription_price_attr):
                price_value = await cast(
                    Callable[[], Awaitable[object]], subscription_price_attr
                )()
                if isinstance(price_value, (int, float)):
                    price = float(price_value)
            user_info = (int(user_obj.id), user_obj.username, price)

    user_id, username, default_price = user_info
    price = request.price if request.price is not None else default_price

    is_ppv_request = request.post_id is not None
    order_type = "ppv" if is_ppv_request else "subscription"

    with create_orders_session() as session:
        site_model = get_or_create_site(session, site, _get_site_url(site))
        existing_order = session.execute(
            sa_select(OrderModel)
            .where(OrderModel.site_id == site_model.id)
            .where(OrderModel.order_type == order_type)
            .where(OrderModel.status == "pending")
            .order_by(OrderModel.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        # Check for existing pending/processing performer order to avoid duplicates
        existing_performer = session.execute(
            sa_select(OrderedPerformerModel)
            .where(OrderedPerformerModel.site_id == site_model.id)
            .where(OrderedPerformerModel.user_id == user_id)
            .where(OrderedPerformerModel.post_id == request.post_id)
            .where(OrderedPerformerModel.status.in_(["pending", "processing"]))
        ).scalar_one_or_none()

        if existing_performer:
            return OrderedPerformerCreateResponse(
                success=True,
                order_id=existing_performer.order_id,
                ordered_performer_id=existing_performer.id,
                site=site,
                user_id=user_id,
                username=username,
                price=existing_performer.price,
                status=existing_performer.status,
                message="Already ordered and pending/processing",
            )

        if existing_order:
            order = existing_order
            if price is not None:
                if order.total_price is None:
                    order.total_price = price
                else:
                    order.total_price += price
            order.touch()
        else:
            order = OrderModel(
                site_id=site_model.id,
                order_type=order_type,
                status="pending",
                total_price=price,
            )
            session.add(order)
            session.flush()

        performer = OrderedPerformerModel(
            order_id=order.id,
            site_id=site_model.id,
            user_id=user_id,
            username=username,
            price=price,
            post_id=request.post_id,
            content_type=request.content_type,
            status="pending",
        )
        session.add(performer)
        session.flush()

        order_id = order.id
        performer_id = performer.id

        if not request.auto_execute:
            return OrderedPerformerCreateResponse(
                success=True,
                order_id=order_id,
                ordered_performer_id=performer_id,
                site=site,
                user_id=user_id,
                username=username,
                price=price,
                status="pending",
            )

        selection = await _select_spender(site, price, backend_manager)
        if not selection:
            performer.status = "pending"
            performer.error_message = "No spender with sufficient balance"
            performer.touch()
            session.flush()
            all_items = list(
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )
            order.status = _derive_order_status(all_items)
            order.total_price = _recalculate_order_total(all_items)
            order.touch()
            return OrderedPerformerCreateResponse(
                success=True,
                order_id=order_id,
                ordered_performer_id=performer_id,
                site=site,
                user_id=user_id,
                username=username,
                price=price,
                status="pending",
                error="No spender with sufficient balance",
            )

        authed, spender_id, _balance = selection
        if is_ppv_request and request.post_id is not None:
            ok, error_message, resolved_price, resolved_username = await _execute_ppv(
                authed,
                user_id,
                request.post_id,
                request.content_type,
                site,
            )
            if resolved_username:
                performer.username = resolved_username
                username = resolved_username
        else:
            ok, error_message, resolved_price = await _execute_subscription(
                authed, user_id, username, site
            )
        performer.price = _merge_execution_price(performer.price, resolved_price)
        performer.spender_user_id = spender_id
        order.spender_user_id = spender_id

        if ok:
            performer.status = "completed"
            performer.touch()
            session.flush()
            all_items = list(
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )
            order.status = _derive_order_status(all_items)
            order.total_price = _recalculate_order_total(all_items)
            order.touch()

            await _refresh_spender_balance_after_purchase(
                site=site,
                authed=authed,
                auth_id=spender_id,
            )

            if not is_ppv_request:
                await _upsert_subscription_after_purchase(
                    site=site,
                    authed=authed,
                    subscriber_id=spender_id,
                    user_id=user_id,
                    username=username,
                    price=performer.price,
                )
                _add_to_watch_list(site, user_id)

            # Create S+D job for successful purchase
            await _create_scrape_download_job(
                site, spender_id, user_id, username, backend_manager
            )

            return OrderedPerformerCreateResponse(
                success=True,
                order_id=order_id,
                ordered_performer_id=performer_id,
                site=site,
                user_id=user_id,
                username=username,
                price=performer.price,
                spender_user_id=spender_id,
                status="completed",
            )

        performer.status = "failed"
        performer.error_message = error_message
        performer.touch()
        session.flush()
        all_items = list(
            session.execute(
                sa_select(OrderedPerformerModel)
                .where(OrderedPerformerModel.order_id == order.id)
                .order_by(OrderedPerformerModel.created_at.asc())
            )
            .scalars()
            .all()
        )
        order.status = _derive_order_status(all_items)
        order.total_price = _recalculate_order_total(all_items)
        order.touch()
        return OrderedPerformerCreateResponse(
            success=False,
            order_id=order_id,
            ordered_performer_id=performer_id,
            site=site,
            user_id=user_id,
            username=username,
            price=performer.price,
            spender_user_id=spender_id,
            status="failed",
            error=error_message,
        )


@router.post(
    "/performers/{performer_id}/check", response_model=OrderedPerformerActionResponse
)
async def check_ordered_performer(
    performer_id: int,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> OrderedPerformerActionResponse:
    """Validate and refresh a performer order line item."""
    ensure_orders_schema()

    with create_orders_session() as session:
        row = session.execute(
            sa_select(OrderedPerformerModel, SiteModel)
            .join(SiteModel, OrderedPerformerModel.site_id == SiteModel.id)
            .where(OrderedPerformerModel.id == performer_id)
        ).first()

        if not row:
            return OrderedPerformerActionResponse(
                success=False, error="Order item not found"
            )

        performer, site_model = row
        auth_id = _get_any_active_auth_id(site_model.name)
        if auth_id is None:
            performer.error_message = "No authenticated accounts available"
            performer.touch()
            return OrderedPerformerActionResponse(
                success=False,
                item=_build_performer_info(performer, site_model.name),
                error="No authenticated accounts available",
            )

        authed = await backend_manager.get_authed_session(site_model.name, auth_id)
        if not authed:
            performer.error_message = "Failed to authenticate account"
            performer.touch()
            return OrderedPerformerActionResponse(
                success=False,
                item=_build_performer_info(performer, site_model.name),
                error="Failed to authenticate account",
            )

        identifier = performer.username or str(performer.user_id)
        user_obj = await authed.get_user(identifier)
        if not user_obj:
            performer.error_message = "Performer not found"
            performer.touch()
            return OrderedPerformerActionResponse(
                success=False,
                item=_build_performer_info(performer, site_model.name),
                error="Performer not found",
            )

        schedule_user_sync(user_obj, site_model.name)

        performer.user_id = int(user_obj.id)
        performer.username = user_obj.username

        subscription_price_attr = getattr(user_obj, "subscription_price", None)
        if callable(subscription_price_attr):
            price_value = await cast(
                Callable[[], Awaitable[object]], subscription_price_attr
            )()
            if isinstance(price_value, (int, float)):
                performer.price = float(price_value)

        performer.error_message = None
        performer.touch()
        return OrderedPerformerActionResponse(
            success=True,
            item=_build_performer_info(performer, site_model.name),
        )


@router.post(
    "/performers/{performer_id}/execute", response_model=OrderedPerformerActionResponse
)
async def execute_ordered_performer(
    performer_id: int,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> OrderedPerformerActionResponse:
    """Execute a performer order line item."""
    ensure_orders_schema()

    with create_orders_session() as session:
        row = session.execute(
            sa_select(OrderedPerformerModel, OrderModel, SiteModel)
            .join(OrderModel, OrderedPerformerModel.order_id == OrderModel.id)
            .join(SiteModel, OrderedPerformerModel.site_id == SiteModel.id)
            .where(OrderedPerformerModel.id == performer_id)
        ).first()

        if not row:
            return OrderedPerformerActionResponse(
                success=False, error="Order item not found"
            )

        performer, order, site_model = row

        # For PPV items that already have a spender recorded, prefer that auth
        # so the same account that bought the PPV is used for re-execution/S+D.
        selection: tuple[auth_types, int, float | None] | None = None
        if performer.post_id is not None and performer.spender_user_id is not None:
            prev_authed = await backend_manager.get_authed_session(
                site_model.name, performer.spender_user_id
            )
            if prev_authed:
                selection = (prev_authed, performer.spender_user_id, None)

        if selection is None:
            selection = await _select_spender(
                site_model.name, performer.price, backend_manager
            )
        if not selection:
            performer.status = "pending"
            performer.error_message = "No spender with sufficient balance"
            performer.touch()
            session.flush()
            all_items = list(
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )
            order.status = _derive_order_status(all_items)
            order.total_price = _recalculate_order_total(all_items)
            order.touch()
            return OrderedPerformerActionResponse(
                success=True,
                item=_build_performer_info(performer, site_model.name),
                error="No spender with sufficient balance",
            )

        authed, spender_id, _balance = selection

        # Route to PPV or subscription execution based on stored post_id
        if performer.post_id is not None:
            ok, error_message, resolved_price, resolved_username = await _execute_ppv(
                authed,
                performer.user_id,
                performer.post_id,
                performer.content_type,
                site_model.name,
            )
            if resolved_username:
                performer.username = resolved_username
        else:
            ok, error_message, resolved_price = await _execute_subscription(
                authed, performer.user_id, performer.username, site_model.name
            )

        performer.price = _merge_execution_price(performer.price, resolved_price)
        performer.spender_user_id = spender_id
        order.spender_user_id = spender_id

        if ok:
            performer.status = "completed"
            performer.error_message = None
            performer.touch()
            session.flush()
            all_items = list(
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )
            order.status = _derive_order_status(all_items)
            order.total_price = _recalculate_order_total(all_items)
            order.touch()

            await _refresh_spender_balance_after_purchase(
                site=site_model.name,
                authed=authed,
                auth_id=spender_id,
            )

            if performer.post_id is None:
                await _upsert_subscription_after_purchase(
                    site=site_model.name,
                    authed=authed,
                    subscriber_id=spender_id,
                    user_id=performer.user_id,
                    username=performer.username,
                    price=performer.price,
                )
                _add_to_watch_list(site_model.name, performer.user_id)

            # Create S+D job for successful purchase
            await _create_scrape_download_job(
                site_model.name,
                spender_id,
                performer.user_id,
                performer.username,
                backend_manager,
            )

            return OrderedPerformerActionResponse(
                success=True,
                item=_build_performer_info(performer, site_model.name),
            )

        performer.status = "failed"
        performer.error_message = error_message
        performer.touch()
        session.flush()
        all_items = list(
            session.execute(
                sa_select(OrderedPerformerModel)
                .where(OrderedPerformerModel.order_id == order.id)
                .order_by(OrderedPerformerModel.created_at.asc())
            )
            .scalars()
            .all()
        )
        order.status = _derive_order_status(all_items)
        order.total_price = _recalculate_order_total(all_items)
        order.touch()
        return OrderedPerformerActionResponse(
            success=False,
            item=_build_performer_info(performer, site_model.name),
            error=error_message,
        )


@router.post("/{order_id}/check", response_model=OrderDetailResponse)
async def check_order(order_id: int) -> OrderDetailResponse:
    """Check all users in an order and refresh their details."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()
            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            site = session.execute(
                sa_select(SiteModel).where(SiteModel.id == order.site_id)
            ).scalar_one_or_none()
            if not site:
                return OrderDetailResponse(success=False, error="Site not found")

            items = (
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )

            summary = _build_order_summary(order, len(items))
            return OrderDetailResponse(
                success=True,
                order=summary,
                items=_build_order_items(list(items)),
            )
    except Exception as exc:
        logger.exception("Failed to check order")
        return OrderDetailResponse(success=False, error=str(exc))


@router.post("/{order_id}/calculate", response_model=CalculateOrderResponse)
async def calculate_order(
    order_id: int,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> CalculateOrderResponse:
    """Preview spender assignments without executing purchases.

    Runs the same knapsack optimisation as execute but returns the plan
    instead of buying.  The result shows which spenders would handle which
    items and which items cannot be assigned.
    """
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()
            if not order:
                return CalculateOrderResponse(success=False, error="Order not found")

            site = session.execute(
                sa_select(SiteModel).where(SiteModel.id == order.site_id)
            ).scalar_one_or_none()
            if not site:
                return CalculateOrderResponse(success=False, error="Site not found")

            items = list(
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )

            if not items:
                summary = _build_order_summary(order, 0)
                return CalculateOrderResponse(
                    success=True,
                    order=summary,
                    items=[],
                    spenders=[],
                    unassigned_count=0,
                    total_cost=0.0,
                )

            # Mirror execute_order: any non-completed item can be recalculated
            # and manually reassigned, including previously failed rows.
            pending_items = [item for item in items if item.status != "completed"]

            # Partition into free / paid
            free_items = [
                it for it in pending_items if it.price is None or it.price == 0.0
            ]
            paid_items = [
                it for it in pending_items if it.price is not None and it.price > 0.0
            ]

            # Fetch candidates only for item types that exist in this order.
            free_candidates: list[tuple[int, str | None]] = (
                _get_candidate_auth_ids(site.name, price=0.0) if free_items else []
            )
            paid_candidates: list[tuple[int, str | None]] = (
                _get_candidate_auth_ids(site.name, price=1.0) if paid_items else []
            )
            stored_balances = _get_stored_user_balances(
                site.name,
                {auth_id for auth_id, _ in free_candidates + paid_candidates},
            )

            # Build assignment maps: item_id -> auth_id
            item_spender_map: dict[int, int] = {}
            # Spender info accumulator: auth_id -> SpenderAssignment
            spender_info: dict[int, SpenderAssignment] = {}

            seen_auth_ids: set[int] = set()

            # --- Free spenders ---
            free_spender_ids: list[int] = []
            if free_items:
                for auth_id, spender_type in free_candidates:
                    if auth_id in seen_auth_ids:
                        continue
                    seen_auth_ids.add(auth_id)
                    balance = stored_balances.get(auth_id)
                    if balance is None:
                        continue
                    sp_type = spender_type
                    spender_info[auth_id] = SpenderAssignment(
                        auth_id=auth_id,
                        spender_type=sp_type,
                        balance=balance,
                    )
                    free_spender_ids.append(auth_id)

            # Round-robin free items
            if free_items and free_spender_ids:
                for i, item in enumerate(free_items):
                    chosen = free_spender_ids[i % len(free_spender_ids)]
                    item_spender_map[item.id] = chosen
                    sa = spender_info[chosen]
                    sa.assigned_count += 1
                    cost = float(item.price) if item.price else 0.0
                    sa.assigned_cost += cost

            # --- Paid spenders ---
            paid_spender_balances: list[tuple[int, float]] = []
            if paid_items:
                for auth_id, spender_type in paid_candidates:
                    if auth_id not in seen_auth_ids:
                        seen_auth_ids.add(auth_id)
                        balance = stored_balances.get(auth_id)
                        if balance is None:
                            continue
                        sp_type = spender_type
                        spender_info[auth_id] = SpenderAssignment(
                            auth_id=auth_id,
                            spender_type=sp_type,
                            balance=balance,
                        )
                    else:
                        balance = spender_info[auth_id].balance

                    if balance > 0:
                        paid_spender_balances.append((auth_id, balance))

            # Knapsack optimise paid items
            if paid_items and paid_spender_balances:
                allocation = _knapsack_optimize(paid_items, paid_spender_balances)
                allocated_ids: set[int] = set()
                for auth_id, allocated_list in allocation.items():
                    sa = spender_info[auth_id]
                    for item in allocated_list:
                        item_spender_map[item.id] = auth_id
                        allocated_ids.add(item.id)
                        sa.assigned_count += 1
                        cost = float(item.price) if item.price else 0.0
                        sa.assigned_cost += cost

            # Resolve spender usernames
            spender_usernames = _resolve_spender_usernames(site.name, seen_auth_ids)
            for auth_id, sa in spender_info.items():
                sa.username = spender_usernames.get(auth_id)

            # Build enriched item list with assignments
            enriched_items: list[OrderItemInfo] = []
            unassigned = 0
            total_cost = 0.0
            for item in items:
                assigned = item_spender_map.get(item.id)
                enriched_items.append(
                    OrderItemInfo(
                        id=item.id,
                        user_id=item.user_id,
                        username=item.username,
                        price=item.price,
                        status=item.status,
                        spender_user_id=assigned,
                        spender_username=(
                            spender_usernames.get(assigned) if assigned else None
                        ),
                        error_message=item.error_message,
                    )
                )
                if assigned:
                    total_cost += float(item.price) if item.price else 0.0
                elif item.status != "completed":
                    unassigned += 1

            summary = _build_order_summary(order, len(items))
            return CalculateOrderResponse(
                success=True,
                order=summary,
                items=enriched_items,
                spenders=[
                    sa
                    for sa in spender_info.values()
                    if sa.assigned_count > 0 or sa.balance > 0
                ],
                unassigned_count=unassigned,
                total_cost=total_cost,
            )
    except Exception as exc:
        logger.exception("Failed to calculate order")
        return CalculateOrderResponse(success=False, error=str(exc))


@router.post("/{order_id}/execute", response_model=OrderDetailResponse)
async def execute_order(
    order_id: int,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> OrderDetailResponse:
    """Execute all pending items in an order using knapsack optimization.

    Order Execution Algorithm:
    1. Fetch all items (subscriptions/PPV) in the order
    2. Set order status to "running"
    3. Separate items by type:
       - Message items: Must use specific auth (conversation-locked)
       - Packable items: Can be optimally allocated across spenders
    4. Process message items first (if any):
       - Use required auth_id for each message
       - Deduct from that auth's balance
    5. Get updated spender balances after messages
    6. Apply dynamic programming 0/1 knapsack optimization:
       - For each spender, find optimal subset of items that fits balance
       - Maximize number of items purchased
       - Distribute load efficiently across accounts
    7. Execute purchases according to optimal allocation
    8. Create Scrape+Download jobs for successfully purchased performers
    9. Update order status based on results:
       - "completed": All items succeeded
       - "incomplete": Some items succeeded, some failed/pending
       - "failed": No items succeeded (all failed, no pending)
       - "pending": All items still pending (no action taken)

    This hybrid approach ensures:
    - Critical message purchases happen first (no optimization possible)
    - Remaining items are optimally packed into spender balances
    - Maximum purchase success rate
    - Efficient balance utilization
    - Automatic content scraping for purchased performers
    """
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()
            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            site = session.execute(
                sa_select(SiteModel).where(SiteModel.id == order.site_id)
            ).scalar_one_or_none()
            if not site:
                return OrderDetailResponse(success=False, error="Site not found")

            # Set order to running status
            order.status = "running"
            order.touch()
            session.flush()

            items = (
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )

            completed_count = 0
            failed_count = 0
            pending_count = 0
            total_price = 0.0
            successful_purchases: list[tuple[int, int, str | None]] = (
                []
            )  # (auth_id, user_id, username)
            auth_session_cache: dict[int, auth_types | None] = {}

            # Track already completed items
            for item in items:
                if item.status == "completed":
                    completed_count += 1
                    if isinstance(item.price, (int, float)):
                        total_price += float(item.price)
                elif item.status == "failed":
                    failed_count += 1

            # Completed items are final; failed rows remain retryable so they
            # can be recalculated or re-run with a different spender.
            pending_items = [item for item in items if item.status != "completed"]

            if pending_items:
                # Pre-process: PPV items with a prior spender bypass knapsack
                # entirely (the content may already be bought — no balance needed).
                remaining_pending: list[OrderedPerformerModel] = []
                for item in pending_items:
                    if item.post_id is not None and item.spender_user_id is not None:
                        prev_authed = await _get_cached_authed_session(
                            site=site.name,
                            auth_id=item.spender_user_id,
                            backend_manager=backend_manager,
                            auth_cache=auth_session_cache,
                        )
                        if prev_authed:
                            ok, error_message, resolved_price, resolved_username = (
                                await _execute_ppv(
                                    prev_authed,
                                    item.user_id,
                                    item.post_id,
                                    item.content_type,
                                    site.name,
                                )
                            )
                            if resolved_username:
                                item.username = resolved_username
                            item.price = _merge_execution_price(
                                item.price, resolved_price
                            )
                            item.spender_user_id = item.spender_user_id
                            if ok:
                                item.status = "completed"
                                item.error_message = None
                                item.touch()
                                completed_count += 1
                                item_price = item.price
                                if isinstance(item_price, (int, float)):
                                    total_price += float(item_price)
                                spender_user_id = item.spender_user_id
                                if spender_user_id is not None:
                                    successful_purchases.append(
                                        (
                                            spender_user_id,
                                            item.user_id,
                                            item.username,
                                        )
                                    )
                            else:
                                item.status = "failed"
                                item.error_message = error_message
                                item.touch()
                                failed_count += 1
                            continue
                    remaining_pending.append(item)

                pending_items = remaining_pending

            if pending_items:
                # Separate free vs paid items for proper spender matching
                free_items = [
                    item
                    for item in pending_items
                    if item.price is None or item.price == 0.0
                ]
                paid_items = [
                    item
                    for item in pending_items
                    if item.price is not None and item.price > 0.0
                ]

                # Get candidates filtered by price type
                free_candidates = _get_candidate_auth_ids(site.name, price=0.0)
                paid_candidates = _get_candidate_auth_ids(
                    site.name, price=1.0
                )  # Any >0 price
                stored_balances = _get_stored_user_balances(
                    site.name,
                    {auth_id for auth_id, _ in free_candidates + paid_candidates},
                )

                free_spenders = await _build_live_spender_list(
                    site=site.name,
                    candidates=free_candidates,
                    stored_balances=stored_balances,
                    backend_manager=backend_manager,
                    auth_cache=auth_session_cache,
                    require_positive_balance=False,
                )
                paid_spenders = await _build_live_spender_list(
                    site=site.name,
                    candidates=paid_candidates,
                    stored_balances=stored_balances,
                    backend_manager=backend_manager,
                    auth_cache=auth_session_cache,
                    require_positive_balance=True,
                )

                if not free_spenders and not paid_spenders:
                    # No spenders available with valid live sessions.
                    for item in pending_items:
                        item.status = "pending"
                        item.error_message = "No spenders with valid sessions available"
                        item.touch()
                    pending_count += len(pending_items)
                else:
                    # Allocate free items first (simple round-robin, they cost nothing)
                    if free_items and free_spenders:
                        logger.info(
                            "Allocating %d free items across %d free-eligible spenders",
                            len(free_items),
                            len(free_spenders),
                        )
                        for i, item in enumerate(free_items):
                            # For PPV items with a prior spender, reuse that auth
                            if (
                                item.post_id is not None
                                and item.spender_user_id is not None
                            ):
                                spender_auth_id = item.spender_user_id
                            else:
                                spender_auth_id = free_spenders[i % len(free_spenders)][
                                    0
                                ]
                            authed = await _get_cached_authed_session(
                                site=site.name,
                                auth_id=spender_auth_id,
                                backend_manager=backend_manager,
                                auth_cache=auth_session_cache,
                            )
                            if not authed:
                                item.status = "pending"
                                item.error_message = "Lost spender session"
                                item.touch()
                                pending_count += 1
                                continue

                            if item.post_id is not None:
                                ok, error_message, resolved_price, resolved_username = (
                                    await _execute_ppv(
                                        authed,
                                        item.user_id,
                                        item.post_id,
                                        item.content_type,
                                        site.name,
                                    )
                                )
                                if resolved_username:
                                    item.username = resolved_username
                            else:
                                ok, error_message, resolved_price = (
                                    await _execute_subscription(
                                        authed, item.user_id, item.username, site.name
                                    )
                                )
                            item.price = _merge_execution_price(
                                item.price, resolved_price
                            )
                            item.spender_user_id = spender_auth_id

                            if ok:
                                item.status = "completed"
                                item.error_message = None
                                item.touch()
                                completed_count += 1

                                await _refresh_spender_balance_after_purchase(
                                    site=site.name,
                                    authed=authed,
                                    auth_id=spender_auth_id,
                                )

                                if item.post_id is None:
                                    await _upsert_subscription_after_purchase(
                                        site=site.name,
                                        authed=authed,
                                        subscriber_id=spender_auth_id,
                                        user_id=item.user_id,
                                        username=item.username,
                                        price=item.price,
                                    )
                                    _add_to_watch_list(site.name, item.user_id)
                                item_price = item.price
                                if isinstance(item_price, (int, float)):
                                    total_price += float(item_price)
                                successful_purchases.append(
                                    (spender_auth_id, item.user_id, item.username)
                                )
                            else:
                                item.status = "failed"
                                item.error_message = error_message
                                item.touch()
                                failed_count += 1
                    elif free_items:
                        for item in free_items:
                            item.status = "pending"
                            item.error_message = "No free-eligible spenders with valid sessions available"
                            item.touch()
                        pending_count += len(free_items)

                    # Allocate paid items with knapsack optimization
                    if paid_items and paid_spenders:
                        logger.info(
                            "Optimizing %d paid items across %d paid-eligible spenders for order %d",
                            len(paid_items),
                            len(paid_spenders),
                            order_id,
                        )
                        allocation = _knapsack_optimize(paid_items, paid_spenders)

                        allocated_items: set[int] = set()
                        for auth_id, allocated_list in allocation.items():
                            authed = await _get_cached_authed_session(
                                site=site.name,
                                auth_id=auth_id,
                                backend_manager=backend_manager,
                                auth_cache=auth_session_cache,
                            )
                            if not authed:
                                logger.warning(
                                    "Lost auth session for spender %d", auth_id
                                )
                                continue

                            for item in allocated_list:
                                allocated_items.add(item.id)
                                # For PPV with a prior spender, use that auth instead
                                exec_auth = authed
                                exec_auth_id = auth_id
                                if (
                                    item.post_id is not None
                                    and item.spender_user_id is not None
                                ):
                                    prev_authed = await _get_cached_authed_session(
                                        site=site.name,
                                        auth_id=item.spender_user_id,
                                        backend_manager=backend_manager,
                                        auth_cache=auth_session_cache,
                                    )
                                    if prev_authed:
                                        exec_auth = prev_authed
                                        exec_auth_id = item.spender_user_id
                                if item.post_id is not None:
                                    (
                                        ok,
                                        error_message,
                                        resolved_price,
                                        resolved_username,
                                    ) = await _execute_ppv(
                                        exec_auth,
                                        item.user_id,
                                        item.post_id,
                                        item.content_type,
                                        site.name,
                                    )
                                    if resolved_username:
                                        item.username = resolved_username
                                else:
                                    ok, error_message, resolved_price = (
                                        await _execute_subscription(
                                            exec_auth,
                                            item.user_id,
                                            item.username,
                                            site.name,
                                        )
                                    )
                                item.price = _merge_execution_price(
                                    item.price, resolved_price
                                )

                                item.spender_user_id = exec_auth_id

                                if ok:
                                    item.status = "completed"
                                    item.error_message = None
                                    item.touch()
                                    completed_count += 1

                                    await _refresh_spender_balance_after_purchase(
                                        site=site.name,
                                        authed=exec_auth,
                                        auth_id=exec_auth_id,
                                    )

                                    if item.post_id is None:
                                        await _upsert_subscription_after_purchase(
                                            site=site.name,
                                            authed=exec_auth,
                                            subscriber_id=exec_auth_id,
                                            user_id=item.user_id,
                                            username=item.username,
                                            price=item.price,
                                        )
                                        _add_to_watch_list(site.name, item.user_id)
                                    item_price = item.price
                                    if isinstance(item_price, (int, float)):
                                        total_price += float(item_price)
                                    successful_purchases.append(
                                        (exec_auth_id, item.user_id, item.username)
                                    )
                                else:
                                    item.status = "failed"
                                    item.error_message = error_message
                                    item.touch()
                                    failed_count += 1

                        # Handle unallocated paid items
                        for item in paid_items:
                            if item.id not in allocated_items:
                                item.status = "pending"
                                item.error_message = "Insufficient spender capacity"
                                item.touch()
                                pending_count += 1
                    elif paid_items:
                        for item in paid_items:
                            item.status = "pending"
                            item.error_message = "No paid-eligible spenders with valid sessions available"
                            item.touch()
                        pending_count += len(paid_items)

            # Update order status based on item results
            order.status = _derive_order_status(items)

            order.total_price = total_price if items else order.total_price
            order.touch()

            # Create S+D jobs for successful purchases
            if successful_purchases:
                logger.info(
                    "Queueing %d S+D jobs in background for order %d",
                    len(successful_purchases),
                    order_id,
                )
                for auth_id, user_id, username in successful_purchases:
                    spawn_background_task(
                        _create_scrape_download_job(
                            site.name, auth_id, user_id, username, backend_manager
                        )
                    )

            summary = _build_order_summary(order, len(items))
            return OrderDetailResponse(
                success=True,
                order=summary,
                items=_build_order_items(list(items)),
            )
    except Exception as exc:
        logger.exception("Failed to execute order")
        return OrderDetailResponse(success=False, error=str(exc))


@router.patch(
    "/performers/{performer_id}/spender", response_model=OrderedPerformerActionResponse
)
async def assign_ordered_performer_spender(
    performer_id: int,
    request: AssignSpenderRequest,
) -> OrderedPerformerActionResponse:
    """Manually assign (or unassign) a spender to an ordered performer."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            performer = session.execute(
                sa_select(OrderedPerformerModel).where(
                    OrderedPerformerModel.id == performer_id
                )
            ).scalar_one_or_none()
            if not performer:
                return OrderedPerformerActionResponse(
                    success=False, error="Order item not found"
                )

            performer.spender_user_id = request.spender_user_id
            performer.touch()

            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == performer.order_id)
            ).scalar_one_or_none()
            if order:
                order.touch()

            site_model = session.execute(
                sa_select(SiteModel).where(SiteModel.id == performer.site_id)
            ).scalar_one_or_none()
            site_name = site_model.name if site_model else ""

            return OrderedPerformerActionResponse(
                success=True,
                item=_build_performer_info(performer, site_name),
            )
    except Exception as exc:
        logger.exception("Failed to assign spender")
        return OrderedPerformerActionResponse(success=False, error=str(exc))


@router.delete("/performers/{performer_id}", response_model=OrderDetailResponse)
async def delete_ordered_performer(performer_id: int) -> OrderDetailResponse:
    """Delete a user from an order."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            performer = session.execute(
                sa_select(OrderedPerformerModel).where(
                    OrderedPerformerModel.id == performer_id
                )
            ).scalar_one_or_none()
            if not performer:
                return OrderDetailResponse(success=False, error="Order item not found")

            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == performer.order_id)
            ).scalar_one_or_none()
            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            if (
                isinstance(performer.price, (int, float))
                and order.total_price is not None
            ):
                order.total_price = max(
                    0.0, float(order.total_price) - float(performer.price)
                )

            session.delete(performer)
            session.flush()

            items = (
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order.id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )

            order.status = _derive_order_status(list(items))
            order.total_price = _recalculate_order_total(list(items))
            order.touch()

            summary = _build_order_summary(order, len(items))
            return OrderDetailResponse(
                success=True,
                order=summary,
                items=_build_order_items(list(items)),
            )
    except Exception as exc:
        logger.exception("Failed to delete order item")
        return OrderDetailResponse(success=False, error=str(exc))


@router.delete("/{order_id}/performers", response_model=OrderDetailResponse)
async def delete_all_ordered_performers(order_id: int) -> OrderDetailResponse:
    """Delete all users from an order."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()
            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            session.execute(
                delete(OrderedPerformerModel).where(
                    OrderedPerformerModel.order_id == order_id
                )
            )

            order.total_price = None
            order.status = "pending"
            order.touch()

            summary = _build_order_summary(order, 0)
            return OrderDetailResponse(success=True, order=summary, items=[])
    except Exception as exc:
        logger.exception("Failed to delete all users from order")
        return OrderDetailResponse(success=False, error=str(exc))


@router.post("/{order_id}/performers/clear", response_model=OrderDetailResponse)
async def clear_all_ordered_performers(order_id: int) -> OrderDetailResponse:
    """Clear all users from an order (POST variant)."""
    return await delete_all_ordered_performers(order_id)


@router.delete("/{order_id}/ppv", response_model=OrderDetailResponse)
async def delete_all_ordered_ppv(order_id: int) -> OrderDetailResponse:
    """Delete all PPV items from an order."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()
            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            session.execute(
                delete(OrderedPpvModel).where(OrderedPpvModel.order_id == order_id)
            )
            session.flush()

            items = (
                session.execute(
                    sa_select(OrderedPerformerModel)
                    .where(OrderedPerformerModel.order_id == order_id)
                    .order_by(OrderedPerformerModel.created_at.asc())
                )
                .scalars()
                .all()
            )

            order.status = _derive_order_status(list(items))
            order.total_price = _recalculate_order_total(list(items))
            order.touch()

            summary = _build_order_summary(order, len(items))
            return OrderDetailResponse(
                success=True,
                order=summary,
                items=_build_order_items(list(items)),
            )
    except Exception as exc:
        logger.exception("Failed to delete all PPV from order")
        return OrderDetailResponse(success=False, error=str(exc))


@router.post("/{order_id}/ppv/clear", response_model=OrderDetailResponse)
async def clear_all_ordered_ppv(order_id: int) -> OrderDetailResponse:
    """Clear all PPV items from an order (POST variant)."""
    return await delete_all_ordered_ppv(order_id)


@router.delete("/{order_id}", response_model=OrderDetailResponse)
async def delete_order(order_id: int) -> OrderDetailResponse:
    """Delete an order and all its items."""
    ensure_orders_schema()

    try:
        with create_orders_session() as session:
            order = session.execute(
                sa_select(OrderModel).where(OrderModel.id == order_id)
            ).scalar_one_or_none()
            if not order:
                return OrderDetailResponse(success=False, error="Order not found")

            session.execute(
                delete(OrderedPerformerModel).where(
                    OrderedPerformerModel.order_id == order_id
                )
            )
            session.execute(
                delete(OrderedPpvModel).where(OrderedPpvModel.order_id == order_id)
            )

            summary = _build_order_summary(order, 0)
            session.delete(order)

            return OrderDetailResponse(success=True, order=summary, items=[])
    except Exception as exc:
        logger.exception("Failed to delete order")
        return OrderDetailResponse(success=False, error=str(exc))


@router.post("/{order_id}/delete", response_model=OrderDetailResponse)
async def delete_order_post(order_id: int) -> OrderDetailResponse:
    """Delete an order (POST variant)."""
    return await delete_order(order_id)
