"""
Job subscription validation and multi-auth handling.

This module provides utilities for:
1. Validating that an auth is subscribed to a performer
2. Splitting "All Auths" jobs into separate per-auth tasks
3. Determining effective auth for a performer

Key Concepts:
- SubscriptionModel links auth accounts (subscriber_id) to performers (user_id)
- When "All Auths" is selected, only auths with active subscriptions are used
- Auth selection is handled at the job level, not per-subscription
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence, cast

from sqlmodel import Session, select

from ultima_scraper_db_archive import SubscriptionModel, UserAuthModel

__all__ = [
    "SubscriptionInfo",
    "get_subscribed_auths",
    "validate_subscription",
    "get_effective_auth_for_performer",
    "split_tasks_by_auth",
]

logger = logging.getLogger(__name__)

type SubscriptionRow = tuple[int, int, int, bool, datetime | None, str | None]


def utcnow() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


@dataclass
class SubscriptionInfo:
    """Information about a subscription relationship."""

    subscription_id: int
    performer_id: int
    auth_id: int
    auth_username: str | None
    is_active: bool
    expires_at: datetime | None

    @property
    def is_expired(self) -> bool:
        """Check if subscription has expired."""
        if self.expires_at is None:
            return False
        return self.expires_at < utcnow()


def get_subscribed_auths(
    session: Session,
    performer_id: int,
    *,
    include_expired: bool = False,
) -> list[SubscriptionInfo]:
    """
    Get all auth accounts subscribed to a performer.

    Args:
        session: Database session
        performer_id: The performer's user ID
        include_expired: Whether to include expired subscriptions

    Returns:
        List of SubscriptionInfo for each subscribed auth
    """
    # Build query joining subscriptions with auth accounts.
    # SubscriptionModel semantics:
    #   user_id       = the performer (person being subscribed to)
    #   subscriber_id = the auth/subscriber account
    select_any = cast(Any, select)
    stmt = select_any(
        SubscriptionModel.id,
        SubscriptionModel.user_id,
        SubscriptionModel.subscriber_id,
        SubscriptionModel.active,
        SubscriptionModel.expires_at,
        UserAuthModel.username,
    )
    stmt = stmt.join(
        UserAuthModel,
        SubscriptionModel.subscriber_id == UserAuthModel.user_id,
    )
    stmt = stmt.where(SubscriptionModel.user_id == performer_id)
    stmt = stmt.where(UserAuthModel.active.is_(True))

    if not include_expired:
        stmt = stmt.where(SubscriptionModel.active.is_(True))

    results: list[SubscriptionRow] = list(cast(Any, session.exec(stmt)).all())

    subscriptions: list[SubscriptionInfo] = []
    for row in results:
        sub_info = SubscriptionInfo(
            subscription_id=row[0],
            performer_id=row[1],  # user_id = performer
            auth_id=row[2],  # subscriber_id = auth account
            auth_username=row[5],  # From join
            is_active=row[3],
            expires_at=row[4],
        )
        subscriptions.append(sub_info)

    return subscriptions


def validate_subscription(
    session: Session,
    performer_id: int,
    auth_id: int,
) -> SubscriptionInfo | None:
    """
    Validate that an auth account is subscribed to a performer.

    Args:
        session: Database session
        performer_id: The performer's user ID
        auth_id: The auth account ID to validate

    Returns:
        SubscriptionInfo if subscribed, None otherwise
    """
    # user_id = performer, subscriber_id = auth account
    select_any = cast(Any, select)
    stmt = select_any(
        SubscriptionModel.id,
        SubscriptionModel.user_id,
        SubscriptionModel.subscriber_id,
        SubscriptionModel.active,
        SubscriptionModel.expires_at,
        UserAuthModel.username,
    )
    stmt = stmt.join(
        UserAuthModel,
        SubscriptionModel.subscriber_id == UserAuthModel.user_id,
    )
    stmt = stmt.where(
        SubscriptionModel.user_id == performer_id,
        SubscriptionModel.subscriber_id == auth_id,
    )

    result = cast(SubscriptionRow | None, cast(Any, session.exec(stmt)).first())

    if not result:
        return None

    return SubscriptionInfo(
        subscription_id=result[0],
        performer_id=result[1],  # user_id = performer
        auth_id=result[2],  # subscriber_id = auth account
        auth_username=result[5],
        is_active=result[3],
        expires_at=result[4],
    )


def get_effective_auth_for_performer(
    session: Session,
    performer_id: int,
    global_auth_id: int,
) -> int:
    """
    Get the effective auth ID to use for a performer.

    Validates that the global_auth_id has an active subscription to the performer.
    Auth selection is handled at the job level, so this simply validates
    and returns the provided auth_id.

    Args:
        session: Database session
        performer_id: The performer's user ID
        global_auth_id: The globally selected auth ID

    Returns:
        The validated auth ID to use (same as global_auth_id)
    """
    # Validate subscription exists
    sub = validate_subscription(session, performer_id, global_auth_id)

    if sub is None:
        logger.warning(
            f"Auth {global_auth_id} not subscribed to performer {performer_id}"
        )

    return global_auth_id


@dataclass
class TaskSplitResult:
    """Result of splitting tasks by auth."""

    auth_id: int
    auth_username: str | None
    performer_ids: list[int]
    task_count: int


def split_tasks_by_auth(
    session: Session,
    performer_ids: Sequence[int],
    auth_id: int | None,  # None = "All Auths"
) -> list[TaskSplitResult]:
    """
    Split performers into groups by effective auth.

    When auth_id is None ("All Auths"):
    - For each performer, find all auths subscribed to them
    - Group by auth so we can create separate tasks per auth
    - This prevents concurrent download conflicts

    When auth_id is specified:
    - Validate that auth is subscribed to all performers
    - Return single group with all performers

    Args:
        session: Database session
        performer_ids: List of performer IDs to process
        auth_id: Selected auth ID, or None for "All Auths"

    Returns:
        List of TaskSplitResult, one per auth group

    Raises:
        ValueError: If auth is not subscribed to some performers
    """
    if auth_id is not None:
        # Single auth mode - batch-validate subscriptions for all performers
        select_any = cast(Any, select)
        stmt = (
            select_any(
                SubscriptionModel.user_id,
                SubscriptionModel.active,
            )
            .join(
                UserAuthModel,
                SubscriptionModel.subscriber_id == UserAuthModel.user_id,
            )
            .where(
                SubscriptionModel.user_id.in_(list(performer_ids)),
                SubscriptionModel.subscriber_id == auth_id,
            )
        )
        rows = cast(Any, session.exec(stmt)).all()
        active_performer_ids = {pid for pid, active in rows if active}
        unsubscribed = [pid for pid in performer_ids if pid not in active_performer_ids]

        if unsubscribed:
            raise ValueError(
                f"Auth {auth_id} is not subscribed to performers: {unsubscribed}"
            )

        # Get auth username (subscriber_id = auth platform user_id)
        auth = session.exec(
            select(UserAuthModel).where(UserAuthModel.user_id == auth_id)
        ).first()
        auth_username = auth.username if auth else None

        return [
            TaskSplitResult(
                auth_id=auth_id,
                auth_username=auth_username,
                performer_ids=list(performer_ids),
                task_count=len(performer_ids),
            )
        ]

    # "All Auths" mode - batch-load subscriptions for all performers at once
    auth_groups: dict[int, list[int]] = {}  # auth_id -> [performer_ids]
    auth_usernames: dict[int, str | None] = {}

    select_any = cast(Any, select)
    stmt = (
        select_any(
            SubscriptionModel.user_id,  # performer_id
            SubscriptionModel.subscriber_id,  # auth_id
            UserAuthModel.username,
        )
        .join(
            UserAuthModel,
            SubscriptionModel.subscriber_id == UserAuthModel.user_id,
        )
        .where(
            SubscriptionModel.user_id.in_(list(performer_ids)),
            SubscriptionModel.active.is_(True),
            UserAuthModel.active.is_(True),
        )
    )
    all_subs = cast(Any, session.exec(stmt)).all()

    # Build performer -> subscriptions lookup
    performer_subs: dict[int, list[tuple[int, str | None]]] = {}
    for perf_id, sub_auth_id, username in all_subs:
        performer_subs.setdefault(perf_id, []).append((sub_auth_id, username))

    for performer_id in performer_ids:
        subs = performer_subs.get(performer_id)
        if not subs:
            logger.warning(
                f"Performer {performer_id} has no active subscriptions - skipping"
            )
            continue

        # Use the first subscribed auth for this performer
        # (In a more advanced implementation, we could use load balancing)
        first_auth_id, first_username = subs[0]

        if first_auth_id not in auth_groups:
            auth_groups[first_auth_id] = []
            auth_usernames[first_auth_id] = first_username

        auth_groups[first_auth_id].append(performer_id)

    # Convert to results
    results: list[TaskSplitResult] = []
    for auth_id, performers in auth_groups.items():
        results.append(
            TaskSplitResult(
                auth_id=auth_id,
                auth_username=auth_usernames[auth_id],
                performer_ids=performers,
                task_count=len(performers),
            )
        )

    return results
