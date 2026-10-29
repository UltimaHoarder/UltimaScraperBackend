"""Subscription handling for the scrape handler.

This module contains functions for fetching and synchronizing
user subscriptions from the API.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import aclosing
from datetime import datetime
from typing import Any, cast

from ultima_scraper_api import user_types
from ultima_scraper_core import runtime_env_value

from ....database.write_executor import serialize_user_write_payload
from ....database.write_queue import (
    DatabaseWriteClient,
    DatabaseWriteOperation,
    stable_command_id,
)
from ...models import Task, TaskExecutionError, TaskResult, TaskRunContext
from .events import publish_subscription_progress

logger = logging.getLogger(__name__)
SUBSCRIPTION_WRITE_BATCH_SIZE = 100


def _subscription_batch_size() -> int:
    raw = runtime_env_value("ULTIMA_SUBSCRIPTION_BATCH_SIZE")
    if raw is None or not raw.strip():
        return SUBSCRIPTION_WRITE_BATCH_SIZE
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "ULTIMA_SUBSCRIPTION_BATCH_SIZE must be a positive integer"
        ) from exc
    if value <= 0:
        raise ValueError("ULTIMA_SUBSCRIPTION_BATCH_SIZE must be a positive integer")
    return value


async def _subscription_batches(
    subscriptions: Any,
) -> AsyncIterator[tuple[int, list[Any]]]:
    batch_size = _subscription_batch_size()
    offset = 0
    batch = []

    async def source():
        if isinstance(subscriptions, AsyncIterable):
            try:
                async for item in subscriptions:
                    yield item
            finally:
                close = getattr(subscriptions, "aclose", None)
                if callable(close):
                    await close()
        else:
            for item in subscriptions:
                yield item

    async with aclosing(source()) as items:
        async for item in items:
            batch.append(item)
            if len(batch) == batch_size:
                yield offset, batch
                offset += len(batch)
                batch = []
        if batch:
            yield offset, batch


def _subscription_progress_metrics(
    *,
    total: int,
    processed: int,
    saved: int,
    updated: int,
    include_generic_saved: bool = False,
) -> dict[str, int]:
    """Build both subscription-specific and generic counters for task progress."""

    return {
        "items_processed": processed,
        "items_total": total,
        "items_scraped": processed,
        "total_items": total,
        # Keep the live dashboard on the "processed" lane for subscriptions.
        "items_saved": processed if include_generic_saved else 0,
        "subscriptions_total": total,
        "subscriptions_processed": processed,
        "subscriptions_saved": saved,
        "subscriptions_updated": updated,
    }


async def _persist_subscription_task_progress(
    *,
    task: Task,
    storage: Any,
    job_id: str,
    message: str,
    total: int,
    processed: int,
    saved: int,
    updated: int,
    include_generic_saved: bool = False,
) -> None:
    """Persist live subscription counters so active-task APIs can surface them."""

    task.result = TaskResult(
        success=True,
        message=message,
        metrics=_subscription_progress_metrics(
            total=total,
            processed=processed,
            saved=saved,
            updated=updated,
            include_generic_saved=include_generic_saved,
        ),
        output={
            "subscriptions_count": total,
            "processed_count": processed,
            "saved_count": saved,
            "updated_count": updated,
        },
        errors=[],
    )
    await storage.persist_task(job_id=job_id, task=task)


async def handle_get_subscriptions(task: Task, context: TaskRunContext) -> TaskResult:
    """
    Handle getting subscriptions for an authenticated account.

    This is called when content_type="subscriptions" - it's different from
    scraping content because it operates on the authenticated account itself,
    not on another user.

    Args:
        task: Task with user_id=account_id and content_type="subscriptions"
        context: Execution context

    Returns:
        TaskResult with subscription data
    """
    auth_account_id = int(task.user_id)  # The authenticated account owner
    save_to_db = task.payload.save_to_db if task.payload else True
    reload_workers = task.payload.reload_workers if task.payload else False

    worker_id = context.worker_id or "unknown"

    logger.info(
        f"[{worker_id}] [scrape_handler:subscriptions] Getting subscriptions for account {auth_account_id}, "
        f"save_to_db={save_to_db}, reload_workers={reload_workers}"
    )

    # Get site API instance and worker context
    site_api = context.site_api
    site = context.site
    if not site:
        raise TaskExecutionError("Worker context must provide 'site'")
    if not site_api:
        logger.error(
            f"[{worker_id}] [scrape_handler:subscriptions] No API instance in context"
        )
        return TaskResult(
            success=False,
            message="No API instance available",
            errors=["Missing API context"],
        )

    # Find the authenticated user
    authed = _find_auth(site_api, auth_account_id)
    if not authed:
        logger.error(
            f"[{worker_id}] [scrape_handler:subscriptions] Auth {auth_account_id} not found"
        )
        return TaskResult(
            success=False,
            message=f"Authentication for account {auth_account_id} not found",
            errors=["Auth not found"],
        )

    # Get subscriptions from API
    job = context.job
    processed_count = 0
    saved_count = 0
    updated_count = 0

    try:
        iterator = getattr(authed, "iter_subscriptions", None)
        if callable(iterator):
            subscriptions = iterator(job_id=job.job_id, cache=False)
            count = 0  # Discovery proceeds as pages arrive; final count is exact.
        else:
            subscriptions = await authed.get_subscriptions()
            count = len(subscriptions)
        logger.info(
            f"[{worker_id}] [scrape_handler:subscriptions] Subscription retrieval started"
        )

        await _persist_subscription_task_progress(
            task=task,
            storage=context.storage,
            job_id=job.job_id,
            message=(
                f"Subscriptions fetched: 0/{count}"
                if count > 0
                else "Fetching subscriptions"
            ),
            total=count,
            processed=processed_count,
            saved=saved_count,
            updated=updated_count,
        )

        await publish_subscription_progress(
            job=job,
            worker_id=worker_id,
            status="running",
            total=count,
            processed=processed_count,
            saved=saved_count,
            updated=updated_count,
            completed_tasks=0,
            storage=context.storage,
        )

    except Exception as api_exc:
        logger.exception(
            f"[{worker_id}] [scrape_handler:subscriptions] Failed to get subscriptions"
        )
        return TaskResult(
            success=False,
            message=f"Failed to retrieve subscriptions: {str(api_exc)}",
            errors=[str(api_exc)],
        )

    # Save to database if requested
    if save_to_db and subscriptions:
        if context.database_writes is None:
            raise TaskExecutionError("Database writer is unavailable")

        result = await _save_subscriptions_to_db(
            subscriptions=subscriptions,
            authed=authed,
            database_writes=context.database_writes,
            site=site,
            task=task,
            job=job,
            worker_id=worker_id,
            storage=context.storage,
        )

        count = result["count"]
        if not result["success"]:
            return TaskResult(
                success=False,
                message=result["message"],
                output={"subscriptions_count": count},
                errors=result["errors"],
            )

        processed_count = result["processed"]
        saved_count = result["saved"]
        updated_count = result["updated"]
    else:
        try:
            count = 0
            async with aclosing(_subscription_batches(subscriptions)) as batches:
                async for _offset, batch in batches:
                    count += len(batch)
                    processed_count += len(batch)
                    await _persist_subscription_task_progress(
                        task=task,
                        storage=context.storage,
                        job_id=job.job_id,
                        message=(
                            f"Subscriptions processed: {processed_count}/{count}"
                            if count
                            else "Subscriptions processed"
                        ),
                        total=count,
                        processed=processed_count,
                        saved=saved_count,
                        updated=updated_count,
                    )
                    await publish_subscription_progress(
                        job=job,
                        worker_id=worker_id,
                        status="running",
                        total=count,
                        processed=processed_count,
                        saved=saved_count,
                        updated=updated_count,
                        completed_tasks=0,
                        storage=context.storage,
                    )

        except Exception as exc:
            logger.exception(
                f"[{worker_id}] [scrape_handler:subscriptions] Subscription retrieval failed"
            )
            return TaskResult(
                success=False,
                message=f"Failed to retrieve subscriptions: {exc}",
                output={"subscriptions_count": count},
                errors=[str(exc)],
            )

    # Publish reload event if requested
    if reload_workers and save_to_db:
        await _publish_reload_event(authed, job, context, worker_id)

    # Build result
    message = f"Retrieved {count} subscriptions for account {auth_account_id}"
    if save_to_db:
        message += f" (saved {saved_count} new, updated {updated_count} existing)"

    final_metrics = _subscription_progress_metrics(
        total=count,
        processed=processed_count,
        saved=saved_count,
        updated=updated_count,
        include_generic_saved=True,
    )

    await publish_subscription_progress(
        job=job,
        worker_id=worker_id,
        status="completed",
        total=count,
        processed=processed_count,
        saved=saved_count,
        updated=updated_count,
        completed_tasks=1,
        storage=context.storage,
    )

    return TaskResult(
        success=True,
        message=message,
        output={
            "account_id": auth_account_id,
            "subscriptions_count": count,
            "saved_count": saved_count,
            "updated_count": updated_count,
            "saved_to_db": save_to_db,
        },
        metrics=final_metrics,
    )


def _find_auth(site_api: Any, auth_account_id: int) -> Any:
    """Find authenticated user by ID.

    Args:
        site_api: Site API instance
        auth_account_id: Account ID to find

    Returns:
        Authenticated user or None
    """
    if not site_api.auths:
        return None

    auths_obj: Any = site_api.auths
    if isinstance(auths_obj, list):
        all_auths: list[Any] = cast(list[Any], auths_obj)
    elif isinstance(auths_obj, dict):
        all_auths = list(cast(dict[Any, Any], auths_obj).values())
    else:
        return None
    for auth in all_auths:
        auth_id = getattr(auth, "id", None)
        if auth_id is not None and str(auth_id) == str(auth_account_id):
            return auth
    return None


async def _save_subscriptions_to_db(
    subscriptions: Any,
    authed: Any,
    database_writes: DatabaseWriteClient,
    site: str,
    task: Task,
    job: Any,
    worker_id: str,
    storage: Any,
) -> dict[str, Any]:
    """Save subscriptions to database.

    Args:
        subscriptions: Async stream or compatibility list of subscription objects
        authed: Authenticated user
        database_writes: Durable database write producer
        job: Job instance
        worker_id: Worker identifier
        storage: Storage context

    Returns:
        Dict with success, message, errors, processed, saved, updated
    """
    count = len(subscriptions) if isinstance(subscriptions, list) else 0
    processed_count = 0
    saved_count = 0
    updated_count = 0

    try:
        subscriber_id = int(authed.id)
        normalized_site = site.strip().lower()
        async with aclosing(_subscription_batches(subscriptions)) as batches:
            async for offset, batch in batches:
                count = max(count, offset + len(batch))
                queued_rows: list[dict[str, Any]] = []
                for sub in batch:
                    sub_user_id = _get_subscription_user_id(sub)
                    if sub_user_id is None:
                        logger.warning(
                            f"[{worker_id}] [scrape_handler:subscriptions] Skipping subscription - cannot find user ID"
                        )
                        continue

                    sub_username = _get_subscription_username(sub)
                    user_payload = await _subscription_user_payload(
                        sub,
                        authed,
                        worker_id,
                    )

                    expires_at = getattr(sub, "subscribed_by_expire_date", None)
                    if isinstance(expires_at, str):
                        from dateutil.parser import parse

                        expires_at = parse(expires_at)

                    is_active = (
                        getattr(sub, "is_active", lambda: True)()
                        if callable(getattr(sub, "is_active", None))
                        else getattr(sub, "is_active", True)
                    )

                    queued_rows.append(
                        {
                            "user_id": sub_user_id,
                            "username": sub_username or f"user_{sub_user_id}",
                            "user": user_payload,
                            "active": is_active,
                            "expires_at": (
                                expires_at.isoformat()
                                if isinstance(expires_at, datetime)
                                else expires_at
                            ),
                        }
                    )
                if queued_rows:
                    write_payload = {
                        "subscriber_id": subscriber_id,
                        "subscriptions": queued_rows,
                    }
                    result = await database_writes.submit(
                        DatabaseWriteOperation.UPSERT_SUBSCRIPTIONS,
                        write_payload,
                        site=normalized_site,
                        command_id=stable_command_id(
                            "subscriptions-v1",
                            task.task_id,
                            offset,
                            json.dumps(
                                write_payload,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        ),
                    )
                    processed_count += int(result.get("processed") or 0)
                    saved_count += int(result.get("saved") or 0)
                    updated_count += int(result.get("updated") or 0)
                    await _persist_subscription_task_progress(
                        task=task,
                        storage=storage,
                        job_id=job.job_id,
                        message=f"Subscriptions processed: {processed_count}/{count}",
                        total=count,
                        processed=processed_count,
                        saved=saved_count,
                        updated=updated_count,
                    )
                    await publish_subscription_progress(
                        job=job,
                        worker_id=worker_id,
                        status="running",
                        total=count,
                        processed=processed_count,
                        saved=saved_count,
                        updated=updated_count,
                        completed_tasks=0,
                        storage=storage,
                    )

        logger.info(
            f"[{worker_id}] [scrape_handler:subscriptions] Saved {saved_count} new, "
            f"updated {updated_count} existing subscriptions (total: {count})"
        )

        return {
            "success": True,
            "message": "Subscriptions saved",
            "errors": [],
            "count": count,
            "processed": processed_count,
            "saved": saved_count,
            "updated": updated_count,
        }

    except Exception as db_exc:
        logger.exception(
            f"[{worker_id}] [scrape_handler:subscriptions] Subscription retrieval or database save failed"
        )
        return {
            "success": False,
            "message": f"Subscription processing failed: {str(db_exc)}",
            "errors": [str(db_exc)],
            "count": count,
            "processed": processed_count,
            "saved": saved_count,
            "updated": updated_count,
        }


def _get_subscription_user_id(sub: Any) -> int | None:
    """Extract user ID from subscription object.

    Args:
        sub: Subscription object

    Returns:
        User ID or None
    """
    sub_user_id = getattr(sub, "id", None)
    if sub_user_id is None and hasattr(sub, "user"):
        sub_user_id = getattr(sub.user, "id", None)
    try:
        return None if sub_user_id is None else int(sub_user_id)
    except (TypeError, ValueError):
        return None


def _get_subscription_username(sub: Any) -> str | None:
    """Extract username from subscription object.

    Args:
        sub: Subscription object

    Returns:
        Username or None
    """
    sub_username = getattr(sub, "username", None)
    if sub_username is None and hasattr(sub, "user"):
        sub_username = getattr(sub.user, "username", "unknown")
    return sub_username


async def _subscription_user_payload(
    sub: Any,
    authed: Any,
    worker_id: str,
) -> dict[str, Any] | None:
    """Snapshot a subscription user for the database writer.

    Args:
        sub: Subscription object
        authed: Authenticated user
        worker_id: Worker identifier
    """
    try:
        candidate_user = getattr(sub, "user", None) or sub
        refreshed_user = None

        sub_user_id = _get_subscription_user_id(sub)
        if (
            sub_user_id is not None
            and hasattr(authed, "get_user")
            and not getattr(sub, "profile_refreshed", False)
        ):
            try:
                if getattr(sub, "transient_profile", False):
                    refreshed_user = await authed.get_user(
                        sub_user_id, refresh=True, cache=False
                    )
                else:
                    refreshed_user = await authed.get_user(sub_user_id, refresh=True)
            except Exception as refresh_exc:
                logger.debug(
                    f"[{worker_id}] [scrape_handler:subscriptions] Failed to refresh user {sub_user_id}: {refresh_exc}"
                )

        sync_target = refreshed_user or candidate_user
        if hasattr(sync_target, "id"):
            return await serialize_user_write_payload(
                cast(user_types, sync_target),
            )
    except Exception as sync_exc:
        logger.warning(
            f"[{worker_id}] [scrape_handler:subscriptions] Failed to snapshot user: {sync_exc}"
        )
    return None


async def _publish_reload_event(
    authed: Any,
    job: Any,
    context: TaskRunContext,
    worker_id: str,
) -> None:
    """Publish reload event to trigger worker data refresh.

    Args:
        authed: Authenticated user
        job: Job instance
        context: Task context
        worker_id: Worker identifier
    """
    from ...events import ReloadUserDataEvent, publish_event

    event = ReloadUserDataEvent(
        account_id=authed.id,
        username=getattr(authed, "username", "unknown"),
        site=context.site or "unknown",
        worker_id=worker_id,
        from_job=job.job_id,
    )

    if context.storage and hasattr(context.storage, "redis") and context.storage.redis:
        try:
            from ....redis_channels import HOOKS_CHANNEL

            await context.storage.redis.publish(
                HOOKS_CHANNEL, json.dumps(publish_event(event))
            )
            logger.info(
                f"[{worker_id}] [scrape_handler:subscriptions] Published reload_user_data event for account {authed.id}"
            )
        except Exception as exc:
            logger.debug(f"Failed to publish reload_user_data event: {exc}")
    else:
        logger.debug("No Redis available to publish reload_user_data event")
