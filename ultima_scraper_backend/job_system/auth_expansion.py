"""Helpers for expanding tasks based on available auth access.

This module supports the "job-template expansion" approach:
- If a scrape task is created without an explicit auth_id, expand it into
  one task per eligible auth account (based on DB subscription/access).

We keep this logic out of handlers so routing/queueing sees the final task set.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable, Iterator
from contextlib import ExitStack, closing, suppress
from dataclasses import dataclass

from pydantic import BaseModel as PydanticBaseModel
from sqlalchemy import case, func, or_
from sqlmodel import select

from ..database import BaseDatabaseManager
from .models import Task, TaskType
from .payloads import DownloadTaskPayload, ScrapeTaskPayload


def iter_worker_submission_tasks(tasks: Iterable[Task]) -> Iterator[Task]:
    """Preserve the pool's expansion policy without collecting account tasks.

    No eligible accounts, or failure before expansion starts, retains the input
    task for the handler's existing auth fallback. A failure after yielding an
    expanded task aborts the owning preparation transaction.
    """
    from ..database import create_usr_database_manager

    def disconnect(manager):
        with suppress(Exception):
            manager.disconnect()

    with ExitStack() as stack:
        source = iter(tasks)
        if hasattr(source, "close"):
            stack.callback(source.close)
        managers = {}
        for task in source:
            if (
                task.task_type != TaskType.SCRAPE
                or task.auth_id is not None
                or (isinstance(task.payload, ScrapeTaskPayload) and task.payload.use_guest_auth)
                or task.user_id == 0
                or task.depends_on
                or task.subtasks
            ):
                yield task
                continue
            try:
                site = str(task.site_name).lower()
                manager = managers.get(site)
                if manager is None:
                    manager = create_usr_database_manager(site=site, read_only=True)
                    managers[site] = manager
                    stack.callback(disconnect, manager)
                expansion = iter_scrape_tasks_by_db_subscriptions(db_manager=manager, task=task)
                first = next(expansion, None)
            except Exception as exc:
                logging.getLogger(__name__).warning("[WorkerPool] Auth expansion skipped: %s", exc)
                yield task
                continue
            with closing(expansion):
                if first is None:
                    yield task
                else:
                    yield first
                    yield from expansion


@dataclass(frozen=True, slots=True)
class AuthExpansionResult:
    """Result of expanding a single task into per-auth tasks."""

    tasks: list[Task]
    eligible_auth_ids: list[int]


def auth_page_size() -> int:
    from ..database.write_queue import _positive_int_env
    from .storage import JobStorage

    return _positive_int_env(
        "ULTIMA_JOB_AUTH_PAGE_SIZE", JobStorage.DEFAULT_WRITE_BATCH_SIZE
    )


def iter_eligible_auth_ids_for_user(
    *,
    db_manager: BaseDatabaseManager,
    performer_user_id: int,
    include_inactive_subscriptions: bool = False,
) -> Iterator[int]:
    """Read unique eligible account IDs in bounded keyset pages.

    Uses:
    - active auth accounts from `user_auths` (UserAuthModel.user_id)
    - active subscriptions from `subscriptions`

    Schema semantics:
    - SubscriptionModel.user_id       = the performer (person being subscribed to)
    - SubscriptionModel.subscriber_id = the auth/subscriber account
    """

    from ultima_scraper_db_archive import SubscriptionModel, UserAuthModel

    db_manager.ensure_connected()

    subscription = select(SubscriptionModel.subscriber_id).where(
        SubscriptionModel.user_id == performer_user_id,
        SubscriptionModel.subscriber_id == UserAuthModel.user_id,
    )
    if not include_inactive_subscriptions:
        subscription = subscription.where(SubscriptionModel.active.is_(True))
    base = (
        select(UserAuthModel.user_id)
        .where(
            UserAuthModel.site == db_manager.site,
            UserAuthModel.active.is_(True),
            subscription.exists(),
        )
        .distinct()
        .order_by(UserAuthModel.user_id)
    )
    limit = auth_page_size()
    after = None
    while True:
        query = base if after is None else base.where(UserAuthModel.user_id > after)
        with db_manager.get_session() as session:
            rows = list(session.exec(query.limit(limit)))
        # No connection, ORM entities, credentials or total-sized set survives
        # a yield. DISTINCT handles multiple auth records for the same user.
        yield from (int(value) for value in rows)
        if len(rows) < limit:
            return
        after = int(rows[-1])


def get_eligible_auth_ids_for_user(
    *,
    db_manager: BaseDatabaseManager,
    performer_user_id: int,
    include_inactive_subscriptions: bool = False,
) -> list[int]:
    """Materialize IDs for the existing in-memory helper interface."""
    return list(
        iter_eligible_auth_ids_for_user(
            db_manager=db_manager,
            performer_user_id=performer_user_id,
            include_inactive_subscriptions=include_inactive_subscriptions,
        )
    )


def iter_scrape_tasks_by_db_subscriptions(
    *,
    task: Task,
    db_manager: BaseDatabaseManager,
    include_inactive_subscriptions: bool = False,
) -> Iterator[Task]:
    """Expand a no-auth scrape task into one task per eligible auth.

    If the task already has `auth_id`, no expansion happens.

    Also creates download tasks for each scrape task when download_media=True.
    """

    if task.auth_id is not None:
        # Even with explicit auth, we may need to add download task
        yield task
        if _should_create_download_task(task):
            yield _create_download_task_for_scrape(task)
        return

    eligible_auth_ids = iter_eligible_auth_ids_for_user(
        db_manager=db_manager,
        performer_user_id=int(task.user_id),
        include_inactive_subscriptions=include_inactive_subscriptions,
    )

    copied_payload = (
        task.payload.model_copy(deep=True)
        if isinstance(task.payload, PydanticBaseModel)
        else task.payload
    )

    for auth_id in eligible_auth_ids:
        scrape_task = Task(
            site_name=task.site_name,
            user_id=task.user_id,
            user_username=task.user_username,
            auth_id=auth_id,
            content_type=task.content_type,
            task_type=task.task_type,
            payload=copied_payload,
            capabilities=list(task.capabilities),
            ip_lock_key=task.ip_lock_key,
            depends_on=list(task.depends_on),
            max_attempts=task.max_attempts,
            parent_task_id=task.parent_task_id,
            subtasks=[st.model_copy(deep=True) for st in task.subtasks],
        )
        yield scrape_task

        # Create download task if download_media=True
        if _should_create_download_task(scrape_task):
            yield _create_download_task_for_scrape(scrape_task)


async def iter_subscription_refresh_tasks(
    *,
    multi_site_db_manager,
    site_name: str,
    requested_auth_id: int | None,
) -> AsyncIterator[Task]:
    """Construct refresh work without loading credential rows or all accounts."""
    from ultima_scraper_api import ContentType
    from ultima_scraper_db_archive import UserAuthModel

    from ..database.write_queue import DatabaseWriteClient, _positive_int_env
    from .job_builder import build_task
    from .storage import TaskPayloadBudgetExceeded

    limit = auth_page_size()
    input_bytes = _positive_int_env(
        "ULTIMA_JOB_TASK_INPUT_MAX_BYTES", 0
    ) or _positive_int_env(
        "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
    )
    page_bytes = _positive_int_env("ULTIMA_JOB_AUTH_PAGE_MAX_BYTES", input_bytes)
    username_limit = min(input_bytes, page_bytes)
    after = None
    match_record = None

    def read_page():
        site_db = multi_site_db_manager.get_or_create_manager(site_name.lower())
        if site_db is None:
            return [], False
        username_bytes = func.octet_length(func.coalesce(UserAuthModel.username, ""))
        base = select(
            UserAuthModel.id,
            UserAuthModel.user_id,
            case(
                (username_bytes <= username_limit, UserAuthModel.username), else_=None
            ).label("username"),
            username_bytes.label("username_bytes"),
        ).where(UserAuthModel.site == site_name.lower(), UserAuthModel.active.is_(True))
        session = site_db.get_session()
        try:
            if requested_auth_id is not None:
                if match_record is None:
                    exact = session.exec(
                        base.where(UserAuthModel.id == requested_auth_id).limit(1)
                    ).all()
                    if exact:
                        return exact, True
                base = base.where(UserAuthModel.user_id == requested_auth_id)
            if after is not None:
                base = base.where(UserAuthModel.id > after)
            # Sum only a count-bounded candidate page. Include its first row
            # even when oversized so the caller reports the byte rejection.
            candidates = base.order_by(UserAuthModel.id).limit(limit).subquery()
            sizes = select(
                candidates,
                func.sum(candidates.c.username_bytes + 32)
                .over(order_by=candidates.c.id)
                .label("page_bytes"),
                func.row_number().over(order_by=candidates.c.id).label("position"),
            ).subquery()
            query = (
                select(
                    sizes.c.id,
                    sizes.c.user_id,
                    sizes.c.username,
                    sizes.c.username_bytes,
                )
                .where(or_(sizes.c.page_bytes <= page_bytes, sizes.c.position == 1))
                .order_by(sizes.c.id)
                .limit(limit)
            )
            return session.exec(query).all(), False
        finally:
            session.close()

    while True:
        rows, match_record = await asyncio.to_thread(read_page)
        if not rows:
            return
        for row in rows:
            if row.username_bytes + 32 > page_bytes or row.username_bytes > input_bytes:
                raise TaskPayloadBudgetExceeded(
                    f"auth-{row.id}",
                    row.username_bytes + 32,
                    min(input_bytes, page_bytes),
                    phase="subscription account input read",
                )
            yield build_task(
                TaskType.SCRAPE,
                site_name=site_name,
                user_id=row.user_id,
                user_username=row.username or f"auth-{row.user_id}",
                content_type=ContentType.SUBSCRIPTIONS,
                download_media=False,
                max_items=None,
                auth_id=row.user_id,
            )
        if match_record:
            return
        after = rows[-1].id


def _should_create_download_task(scrape_task: Task) -> bool:
    """Check if a scrape task should have a download task created."""
    payload = scrape_task.payload
    return (
        isinstance(payload, ScrapeTaskPayload)
        and payload.download_media
        and not payload.paid_content_feed
    )


def _create_download_task_for_scrape(scrape_task: Task) -> Task:
    """Create a download task that depends on the given scrape task."""
    scrape_payload = scrape_task.payload
    assert isinstance(scrape_payload, ScrapeTaskPayload)

    download_payload = DownloadTaskPayload(
        user_username=scrape_payload.user_username,
        content_value=scrape_payload.content_value,
        max_items=None,  # Download all scraped media
    )

    return Task(
        site_name=scrape_task.site_name,
        user_id=scrape_task.user_id,
        user_username=scrape_task.user_username,
        content_type=scrape_task.content_type,
        task_type=TaskType.DOWNLOAD,
        payload=download_payload,
        auth_id=scrape_task.auth_id,
        depends_on=[scrape_task.task_id],  # Depends on the scrape task
        capabilities=list(scrape_task.capabilities),
        ip_lock_key=scrape_task.ip_lock_key,
    )
