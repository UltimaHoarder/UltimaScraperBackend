"""Main download task handler.

This module contains the main download_task_handler function that orchestrates
the download process including:
- Fetching content metadata from database
- Categorizing items by expiry status
- Executing bulk downloads via DownloadManager
- DRM decryption for protected content
- Progress reporting and result tracking
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, cast

from sqlmodel import select
from ultima_scraper_api.apis.onlyfans.classes.auth_model import OnlyFansAuthModel
from ultima_scraper_api.apis.onlyfans.classes.only_drm import OnlyDRM
from ultima_scraper_api.job_events import (
    DownloadFailureReason,
    DownloadResultItem,
    TaskProgressEvent,
    publish_event,
)

from ultima_scraper_backend.ultima_control.redis_service import (
    publish_download_progress,
)
from ultima_scraper_backend.redis_events import JsonValue, publish_ws_event

from ....database.write_queue import DatabaseWriteOperation, stable_command_id
from ...download_manager import DownloadItem, DownloadManager, _build_temp_merge_path
from ...metadata_models import ContentMetadata
from ...models import (
    Task,
    TaskExecutionError,
    TaskResult,
    TaskRunContext,
)
from ...payloads import DownloadTaskPayload
from ..scrape.events import publish_hook_event
from ....offload import placement
from ....offload.client import OffloadError
from .results import categorize_media_items, resolve_content_id
from .types import MediaEntry
from .utils import (
    ensure_config,
    extract_profile_filename_from_url,
    utcnow,
)

__all__ = ["download_task_handler"]

logger = logging.getLogger(__name__)
_CREATE_DOWNLOAD_PROGRESS_EVENT = getattr(
    importlib.import_module("ultima_scraper_api.job_events"),
    "create_download_progress_event",
)
DRMProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _content_type_name(value: Any) -> str:
    if hasattr(value, "value"):
        return str(getattr(value, "value")).lower()
    return str(value).lower()


def _db_user_alias_usernames(db_user: Any) -> list[str]:
    """Return every known alias username stored on ``db_user``.

    Aliases represent historical usernames for a performer. Drive selection
    needs them so a renamed performer continues reusing their existing folder
    (e.g. ``mxgroup`` → ``lunadiaz12``) instead of creating a fresh one on a
    different drive.
    """

    aliases = getattr(db_user, "aliases", None) or []
    names: list[str] = []
    for alias in aliases:
        alias_username = getattr(alias, "username", None)
        if alias_username:
            names.append(str(alias_username))
    return names


@dataclass
class _DatabaseBackedUser:
    """Minimal user shim for DB-only downloads when API lookup fails."""

    id: int
    authed: Any
    site_name: str
    _usernames: list[str] = field(default_factory=list)

    @property
    def username(self) -> str | None:
        return self._usernames[0] if self._usernames else None

    def add_aliases(self, aliases: list[str | None]) -> None:
        for alias in aliases:
            cleaned = str(alias or "").strip()
            if cleaned and cleaned not in self._usernames:
                self._usernames.append(cleaned)

    def get_usernames(self, ignore_id: bool = False) -> list[str]:
        usernames = list(self._usernames)
        if not ignore_id:
            user_id = str(self.id).strip()
            if user_id and user_id not in usernames:
                usernames.append(user_id)
        return usernames

    def get_authed(self) -> Any:
        return self.authed


async def _load_db_user(
    db_manager: Any,
    *,
    user_id: int = 0,
    username: str | None = None,
) -> Any | None:
    """Load a user from the database by ID or username."""

    if user_id > 0:
        return await asyncio.to_thread(db_manager.get_user, user_id)
    if username:
        cleaned = username.strip()
        if cleaned:
            return await asyncio.to_thread(db_manager.get_user, cleaned)
    return None


async def download_task_handler(
    task: Task,
    context: TaskRunContext,
) -> TaskResult:
    """
    Handle a download task: download files for a user's content.

    This handler:
    1. Validates task payload and context
    2. Retrieves user and database records
    3. Collects media entries from dependent scrape tasks
    4. Fetches content metadata from database
    5. Categorizes items by expiry status (valid vs skipped)
    6. Executes bulk download via DownloadManager
    7. Handles DRM decryption for protected content
    8. Reports progress and tracks results

    Args:
        task: Task instance with DownloadTaskPayload
        context: Execution context with API, database, and storage access

    Returns:
        TaskResult with downloaded files info and metrics

    Raises:
        TaskExecutionError: If required resources are missing
    """
    config = ensure_config(context)
    if context.database_writes is None:
        raise TaskExecutionError("Database writer is unavailable")
    payload = task.payload
    if not payload:
        raise TaskExecutionError("Download task requires a payload")

    if not isinstance(payload, DownloadTaskPayload):
        raise TaskExecutionError(
            f"Invalid payload type: expected DownloadTaskPayload, got {type(payload).__name__}"
        )

    # Validate API and auth
    site_api = context.site_api
    assert site_api is not None, "site_api must be available in context"

    # Get routing fields from task
    user_id = _coerce_int(task.user_id, 0)
    site_name = task.site_name
    content_type = task.content_type
    if not payload.paid_content_feed:
        assert content_type is not None, "content_type must be provided in task"

    db_managers = context.db_managers
    assert db_managers is not None
    db_manager = db_managers[site_name.lower()]

    authed, resolved_auth_id = _resolve_auth(task, site_api, db_manager)
    if payload.paid_content_feed:
        from .streaming import download_selected_media

        return await download_selected_media(
            task=task,
            context=context,
            user=None,
            db_user=None,
            db_manager=db_manager,
            authed=authed,
            config=config,
            content_type="paid_content",
            resolved_auth_id=resolved_auth_id,
        )

    if (
        user_id <= 0
        and context.previous_output
        and "user_id" in context.previous_output
    ):
        user_id = _coerce_int(context.previous_output["user_id"], 0)
        task.user_id = user_id

    db_user = await _load_db_user(
        db_manager,
        user_id=user_id,
        username=task.user_username,
    )
    if db_user is not None and user_id <= 0:
        user_id = _coerce_int(getattr(db_user, "id", 0), 0)
        task.user_id = user_id

    # Download lookups have task lifetime, just like their preceding scrape.
    user_lookup_options = {"cache": False} if isinstance(authed, OnlyFansAuthModel) else {}
    user = None
    if user_id <= 0 and task.user_username:
        logger.info(
            f"[download_handler] Resolving user_id by username: '{task.user_username}'"
        )
        user = await authed.get_user(task.user_username, **user_lookup_options)
        resolved_user_id = _coerce_int(getattr(user, "id", 0), 0) if user else 0
        if user and resolved_user_id > 0:
            user_id = resolved_user_id
            task.user_id = user_id
            if db_user is None:
                db_user = await _load_db_user(db_manager, user_id=user_id)
    else:
        user = await authed.get_user(str(user_id), **user_lookup_options)

    if db_user is None and user_id > 0:
        db_user = await _load_db_user(db_manager, user_id=user_id)

    if user is None and db_user is not None and user_id > 0:
        fallback_user = _DatabaseBackedUser(
            id=user_id,
            authed=authed,
            site_name=site_name,
        )
        fallback_user.add_aliases(
            [
                getattr(db_user, "username", None),
                task.user_username,
                *_db_user_alias_usernames(db_user),
            ]
        )
        user = fallback_user
        logger.warning(
            "[download_handler] User ID %s not found via API for auth %s; "
            "falling back to database identity",
            user_id,
            authed.id,
        )

    if user_id <= 0 or not user:
        raise TaskExecutionError(
            f"User ID {user_id} not found via API for auth {authed.id}"
        )

    user_manager = context.user_manager

    # Optional fields from payload
    if not db_user:
        raise TaskExecutionError(
            f"User id={user_id} not found in database for site '{site_name}'"
        )

    db_username = getattr(db_user, "username", None)
    user.add_aliases(
        [db_username, task.user_username, *_db_user_alias_usernames(db_user)]
    )

    # Determine content type - Highlights are stored as Stories
    content_type_value = _content_type_name(content_type)
    if content_type_value == "highlights":
        content_type_value = "stories"

    # Handle profile content type separately
    if content_type_value == "profile":
        from ....offload.remote import open_offload

        # A backend without drives hands profile media over too, like other media.
        async with open_offload(config, getattr(context, "redis", None)) as offload:
            return await _handle_profile_download(
                task=task,
                context=context,
                user=user,
                db_user=db_user,
                db_manager=db_manager,
                authed=authed,
                user_manager=user_manager,
                config=config,
                offload=offload,
            )

    from .streaming import download_selected_media

    return await download_selected_media(
        task=task,
        context=context,
        user=user,
        db_user=db_user,
        db_manager=db_manager,
        authed=authed,
        config=config,
        content_type=content_type_value,
        resolved_auth_id=resolved_auth_id,
    )


async def _download_content_batch(
    *,
    task,
    context,
    user,
    db_user,
    authed,
    config,
    content_metadatas,
    content_type,
    destination_selection,
    download_manager,
    aggregate,
    evidence_spec,
    remote_store=None,
):
    """Process one bounded metadata batch using the existing file/DRM pipeline.

    With ``remote_store``, files are built under its local mirror and handed to
    the backend that keeps them once each is finished.
    """
    user_id, site_name = int(user.id), task.site_name
    written = []
    bytes_written = 0
    dependent_media_entries = []  # The SQL reader already applied task-scoped signatures.
    selected_drive_root = destination_selection.root
    base_destination = (
        selected_drive_root
        if destination_selection.requires_full_directory_render
        else destination_selection.absolute_path
    )
    # Build download items from content metadata, dedup, and apply fresh URLs
    construction_failures = []
    download_items = _build_download_items(
        content_metadatas,
        user,
        base_destination,
        dependent_media_entries,
        failures=construction_failures,
        directory_format_override=destination_selection.directory_format,
        render_full_directory_format=destination_selection.requires_full_directory_render,
        download_setup=config.settings.download_setup,
    )

    # Categorize media items by expiry status
    valid_items, skipped_items = categorize_media_items(download_items)
    skipped_items.extend(construction_failures)
    download_results: list[DownloadResultItem] = list(skipped_items)
    if valid_items:
        pass

    if skipped_items:
        logger.info(
            f"Categorized {len(download_items)} items: "
            f"{len(valid_items)} valid, {len(skipped_items)} skipped"
        )
        for item in skipped_items:
            errors_str = ", ".join(e.reason.value for e in item.errors) or "unknown"
            logger.info(f"Skipped media {item.media_id}: {errors_str}")

    # Early return if no valid items
    if not valid_items:
        failure_breakdown: dict[str, int] = {}
        for item in skipped_items:
            for error in item.errors:
                reason_key = error.reason.value
                failure_breakdown[reason_key] = failure_breakdown.get(reason_key, 0) + 1

        return TaskResult(
            success=True,
            output={
                "files": written,
                "bytes_written": bytes_written,
                "download_results": [r.model_dump() for r in download_results],
                "failure_breakdown": failure_breakdown,
            },
            message=f"No valid files to download ({len(skipped_items)} skipped)",
            metrics={
                "bytes_written": 0,
                "total_items": len(download_items) + len(construction_failures),
                "valid_items": 0,
                "skipped_items": len(skipped_items),
            },
        )

    logger.info(f"Downloading {len(valid_items)} files using DownloadManager")

    # Set up progress tracking
    total_downloads = len(valid_items)
    total_drm_items = sum(
        1
        for item in valid_items
        if item.media_metadata is not None and item.media_metadata.drm
    )
    known_total_bytes = sum(
        max(int(item.size or 0), 0)
        for item in valid_items
        if item.media_metadata is None or not item.media_metadata.drm
    )
    total_progress_units = total_downloads + total_drm_items
    progress_start = time.perf_counter()
    progress_state: dict[str, Any] = {
        "completed": 0,
        "failed": 0,
        "total_bytes": 0,
        "processed_bytes": 0,
        "expected_bytes": known_total_bytes,
        "speed_bps": 0.0,
        "last_time": progress_start,
        "drm_processed": 0,
        "drm_message": "",
    }

    download_id = f"{context.job.job_id}_{task.task_id}"
    username = db_user.username or str(user_id)

    def build_download_progress_message(
        *,
        downloaded_processed: int,
        drm_processed: int,
        drm_message: str = "",
    ) -> str:
        if total_drm_items > 0 and downloaded_processed >= total_downloads:
            base_message = f"DRM merge in progress: {drm_processed}/{total_drm_items}"
            if drm_processed >= total_drm_items:
                base_message = f"DRM merge complete: {drm_processed}/{total_drm_items}"
            if drm_message:
                return f"{base_message} ({drm_message})"
            return base_message
        if total_downloads > 0:
            return f"Download in progress: {downloaded_processed}/{total_downloads}"
        return "Download in progress"

    def task_expected_bytes(batch_expected_bytes: int) -> int:
        """The task's bytes to write: earlier pages' plus this batch's, at least the estimate.

        The estimate counts only sizes known before downloading, so it can be far
        below what the files turn out to be; this batch's expected bytes grow as
        each file's real size is learned, and never fall below what it wrote.
        """
        return max(
            int(aggregate["expected_bytes"]),
            int(aggregate.get("bytes_written", 0)) + max(int(batch_expected_bytes), 0),
        )

    async def persist_download_task_progress(
        *,
        downloaded_completed: int,
        failed_count: int,
        transferred_bytes_count: int,
        processed_bytes_count: int,
        expected_bytes_count: int,
        speed_bps: float,
        avg_speed_bps: float,
        drm_processed: int | None = None,
        drm_message: str | None = None,
    ) -> tuple[str, int, int]:
        if drm_processed is None:
            drm_processed = max(0, int(progress_state.get("drm_processed", 0)))
        else:
            progress_state["drm_processed"] = drm_processed

        if drm_message is None:
            drm_message = str(progress_state.get("drm_message", ""))
        else:
            progress_state["drm_message"] = drm_message

        downloaded_processed = max(0, downloaded_completed + failed_count)
        processed_count = aggregate.get("progress_units", 0) + max(
            0, downloaded_processed + drm_processed
        )
        total_count = max(aggregate["progress_total"], processed_count)
        message = build_download_progress_message(
            downloaded_processed=downloaded_processed,
            drm_processed=drm_processed,
            drm_message=drm_message,
        )

        task.result = TaskResult(
            success=True,
            message=message,
            metrics={
                "items_processed": processed_count,
                "items_total": total_count,
                "items_downloaded": aggregate.get("successful", 0)
                + downloaded_completed,
                "downloads_completed": aggregate.get("successful", 0)
                + downloaded_completed,
                "failed": aggregate.get("failed", 0) + failed_count,
                "total_items": total_count,
                "bytes_written": aggregate.get("bytes_written", 0)
                + processed_bytes_count,
                "bytes_transferred": aggregate.get("bytes_transferred", 0)
                + transferred_bytes_count,
                "total_bytes": task_expected_bytes(expected_bytes_count),
                "speed_bps": speed_bps,
                "avg_speed_bps": avg_speed_bps,
                "download_items_total": aggregate["total_items"],
                "drm_items_total": total_drm_items,
                "drm_items_processed": drm_processed,
            },
            output={"media_evidence": evidence_spec},
            errors=[],
        )
        await context.storage.persist_task(
            job_id=context.job.job_id,
            task=task,
        )
        return message, processed_count, total_count

    async def publish_task_phase_progress(
        *,
        message: str,
        current: int,
        total: int,
    ) -> None:
        if total <= 0:
            return
        try:
            event = TaskProgressEvent(
                job_id=context.job.job_id,
                task_id=task.task_id,
                progress=min(max((current / total) * 100.0, 0.0), 100.0),
                message=message,
                current=current,
                total=aggregate["progress_total"],
            )
            await publish_hook_event(
                context.storage,
                publish_event(event),
                "download task_progress event",
            )
        except Exception as progress_err:
            logger.debug(f"Failed to publish DRM task progress: {progress_err}")

    await persist_download_task_progress(
        downloaded_completed=0,
        failed_count=0,
        transferred_bytes_count=0,
        processed_bytes_count=0,
        expected_bytes_count=known_total_bytes,
        speed_bps=0.0,
        avg_speed_bps=0.0,
    )

    # Set up progress callbacks
    async def emit_progress(dm_event: dict[str, Any]) -> None:
        """Convert DownloadManager callback to typed OperationProgressEvent."""
        try:
            progress_event = _CREATE_DOWNLOAD_PROGRESS_EVENT(
                job_id=context.job.job_id,
                task_id=task.task_id,
                download_id=download_id,
                username=username,
                user_id=user_id,
                content_type=content_type,
                worker_id=context.worker_id,
                dm_event=dm_event,
                progress_state=progress_state,
                progress_start=progress_start,
                total_downloads=total_progress_units,
            )
            event_dict = progress_event.model_dump(mode="json")
            event_dict["site"] = site_name.lower()
            event_dict.update(
                completed=int(event_dict.get("completed") or 0)
                + aggregate.get("successful", 0),
                failed=int(event_dict.get("failed") or 0) + aggregate.get("failed", 0),
                total=aggregate["progress_total"],
                total_bytes=int(event_dict.get("total_bytes") or 0)
                + aggregate.get("bytes_transferred", 0),
                processed_bytes=int(event_dict.get("processed_bytes") or 0)
                + aggregate.get("bytes_written", 0),
                expected_bytes=task_expected_bytes(
                    int(event_dict.get("expected_bytes") or 0)
                ),
                # The file named by media_id; the fields above are the whole task's.
                media_processed_bytes=dm_event.get("item_processed_bytes"),
                media_expected_bytes=dm_event.get("item_expected_bytes"),
            )
            await publish_download_progress(event_dict)

            completed_count = max(0, int(progress_event.completed or 0))
            failed_count = max(0, int(progress_event.failed or 0))
            transferred_bytes_count = max(0, int(progress_event.total_bytes or 0))
            processed_bytes_count = max(0, int(progress_event.processed_bytes or 0))
            expected_bytes_count = max(0, int(progress_event.expected_bytes or 0))
            speed_bps = max(0.0, float(progress_event.speed_bps or 0.0))
            avg_speed_bps = max(0.0, float(progress_event.avg_speed_bps or 0.0))
            await persist_download_task_progress(
                downloaded_completed=completed_count,
                failed_count=failed_count,
                transferred_bytes_count=transferred_bytes_count,
                processed_bytes_count=processed_bytes_count,
                expected_bytes_count=expected_bytes_count,
                speed_bps=speed_bps,
                avg_speed_bps=avg_speed_bps,
            )
        except Exception as progress_err:
            logger.debug(f"Failed to process download progress: {progress_err}")

    download_manager.on_progress.append(emit_progress)

    async def emit_status(dm_event: dict[str, Any]) -> None:
        payload: dict[str, JsonValue] = {
            "type": "download_status",
            "event": str(dm_event.get("event", "download_status")),
            "job_id": context.job.job_id,
            "task_id": task.task_id,
            "worker_id": context.worker_id or "",
            "url": dm_event.get("url"),
            "media_id": dm_event.get("media_id"),
            "attempt": dm_event.get("attempt"),
            "max_attempts": dm_event.get("max_attempts"),
            "backoff": dm_event.get("backoff"),
            "reason": dm_event.get("reason"),
            "error": dm_event.get("error"),
        }
        await publish_ws_event(context.storage.redis, payload)

    download_manager.on_status.append(emit_status)

    async def emit_drm_progress(drm_event: dict[str, Any]) -> None:
        drm_stage = str(drm_event.get("stage") or "progress")
        drm_processed = max(0, int(drm_event.get("current") or 0))
        media_id = drm_event.get("media_id")

        if drm_stage == "started":
            drm_message = "preparing DRM merge"
        elif drm_stage == "decrypting":
            drm_message = (
                f"decrypting media {media_id}"
                if media_id is not None
                else "decrypting DRM streams"
            )
        elif drm_stage == "merging":
            drm_message = (
                f"merging media {media_id}"
                if media_id is not None
                else "merging DRM streams"
            )
        elif drm_stage == "completed":
            drm_message = (
                f"merged media {media_id}"
                if media_id is not None
                else "merged DRM item"
            )
        elif drm_stage == "failed":
            drm_message = (
                f"failed media {media_id}"
                if media_id is not None
                else "DRM merge failed"
            )
        elif drm_stage == "finished":
            drm_message = "finalizing DRM merge"
        else:
            drm_message = drm_stage.replace("_", " ")

        completed_count = max(0, int(progress_state.get("completed", 0)))
        failed_count = max(0, int(progress_state.get("failed", 0)))
        transferred_bytes_count = max(0, int(progress_state.get("total_bytes", 0)))
        processed_bytes_count = max(0, int(progress_state.get("processed_bytes", 0)))
        expected_bytes_count = max(0, int(progress_state.get("expected_bytes", 0)))
        message, processed_count, total_count = await persist_download_task_progress(
            downloaded_completed=completed_count,
            failed_count=failed_count,
            transferred_bytes_count=transferred_bytes_count,
            processed_bytes_count=processed_bytes_count,
            expected_bytes_count=expected_bytes_count,
            speed_bps=0.0,
            avg_speed_bps=0.0,
            drm_processed=drm_processed,
            drm_message=drm_message,
        )
        await publish_task_phase_progress(
            message=message,
            current=processed_count,
            total=total_count,
        )

    # Download, decrypting each DRM item as soon as its streams are staged
    stats = await _run_download_pipeline(
        download_manager,
        valid_items,
        on_drm_progress=emit_drm_progress,
        remote_store=remote_store,
    )

    # Clean up progress callbacks
    download_manager.on_progress.clear()
    download_manager.on_status.clear()

    # Process download results concurrently — each item is independent
    from contextlib import aclosing
    from ultima_scraper_core.concurrency import map_unordered

    download_results = list(skipped_items)

    async def finalize_item(item):
        if item.offloaded_path is not None:
            # Decrypted and kept by the owner: nothing of it is here to check.
            written.append({"path": item.offloaded_path, "bytes": item.size or 0, "skipped": False})
            return DownloadResultItem(
                media_id=item.media_id or 0,
                content_id=resolve_content_id(item),
                status="downloaded",
                path=item.offloaded_path,
                bytes_written=item.size or 0,
            )
        return await _process_download_result(item, written)

    async with aclosing(
        map_unordered(
            finalize_item,
            valid_items,
            limit=download_manager.resource_manager.get_file_concurrency(),
        )
    ) as results:
        async for result in results:
            download_results.append(result)
    if remote_store is not None:
        await _hand_over(remote_store, download_results)
    bytes_written = sum(
        r.bytes_written or 0 for r in download_results if r.status == "downloaded"
    )

    logger.info(
        f"Download complete: {stats['completed']}/{stats['total']} files "
        f"({stats['success_rate']:.1f}% success), {bytes_written:,} bytes"
    )

    # Mark successfully downloaded media in database
    successful_media_ids: list[int] = []
    skipped_media_ids = 0
    for result in download_results:
        is_completed = result.status == "downloaded"
        is_existing = result.status == "skipped" and result.has_error(
            DownloadFailureReason.ALREADY_EXISTS
        )
        if (is_completed or is_existing) and result.media_id > 0:
            successful_media_ids.append(result.media_id)
        elif is_completed or is_existing:
            skipped_media_ids += 1
            logger.debug(
                f"[download_handler] Skipped result with invalid media_id: "
                f"media_id={result.media_id}, status={result.status}"
            )

    logger.info(
        f"[download_handler] Collected {len(successful_media_ids)} media IDs to mark as downloaded"
    )

    # Update user's last download timestamp when at least one file was newly downloaded
    newly_downloaded = sum(1 for r in download_results if r.status == "downloaded")

    if skipped_media_ids:
        logger.warning(
            f"[download_handler] Skipped {skipped_media_ids} downloaded items without valid media IDs"
        )

    filepath_rows = _build_filepath_rows(download_results, valid_items)

    # Persist downloaded file sizes to the database
    media_sizes: dict[int, int] = {}
    for item in valid_items:
        if item.media_id and item.size and item.size > 0:
            media_sizes[item.media_id] = item.size
    if successful_media_ids or filepath_rows or media_sizes or newly_downloaded > 0:
        finalize_payload = {
            "user_id": user_id,
            "media_ids": successful_media_ids,
            "downloaded_at": utcnow().isoformat() if newly_downloaded > 0 else None,
            "filepaths": filepath_rows,
            "media_sizes": media_sizes,
        }
        write_result = await context.database_writes.submit(
            DatabaseWriteOperation.FINALIZE_DOWNLOAD,
            finalize_payload,
            site=site_name,
            command_id=stable_command_id(
                "download-finalize-v1",
                task.task_id,
                json.dumps(finalize_payload, sort_keys=True, separators=(",", ":")),
            ),
        )
        logger.info(
            "[download_handler] Database writer finalized %d media and %d filepaths",
            int(write_result.get("marked_downloaded") or 0),
            int(write_result.get("filepaths") or 0),
        )

    # Build failure breakdown
    failure_breakdown: dict[str, int] = {}
    for result in download_results:
        if result.status in ("failed", "skipped") and result.errors:
            for error in result.errors:
                reason_key = error.reason.value
                failure_breakdown[reason_key] = failure_breakdown.get(reason_key, 0) + 1

    # Final summary
    total_items = len(download_items) + len(construction_failures)
    skipped_before_download = len(skipped_items)
    successful = sum(1 for r in download_results if r.status == "downloaded")
    failed_count = sum(1 for r in download_results if r.status == "failed")
    skipped_count = sum(1 for r in download_results if r.status == "skipped")
    corrupted = failure_breakdown.get("size_mismatch", 0)
    selected_staging_directories = sorted(
        {
            str(item.drm_media_settings.selected_staging_directory)
            for item in valid_items
            if item.drm_media_settings.selected_staging_directory
        }
    )
    staging_fallback_items = sum(
        1 for item in valid_items if item.drm_media_settings.staging_fallback_reason
    )

    logger.info(
        f"Download report: total={total_items}, valid={len(valid_items)}, "
        f"skipped={skipped_before_download}, successful={successful}, "
        f"failed={failed_count}, skipped={skipped_count}, corrupted={corrupted}, "
        f"bytes_written={bytes_written:,}, selected_drive={selected_drive_root}, "
        f"selected_directory_format={destination_selection.directory_format}, "
        f"selected_staging_directories={selected_staging_directories}, "
        f"staging_fallback_items={staging_fallback_items}"
    )
    if failure_breakdown:
        logger.info(f"Failure breakdown: {failure_breakdown}")

    return TaskResult(
        success=True,
        output={
            "files": written,
            "download_results": [r.model_dump() for r in download_results],
            "failure_breakdown": failure_breakdown,
            "selected_staging_directories": selected_staging_directories,
            "staging_fallback_items": staging_fallback_items,
        },
        message=f"Downloaded {successful} file(s) to {selected_drive_root}",
        metrics={
            "items_processed": successful + failed_count + skipped_count,
            "items_total": total_items,
            "bytes_written": bytes_written,
            "selected_drive": str(selected_drive_root),
            "selected_directory_format": str(destination_selection.directory_format),
            "selected_staging_directory": (
                selected_staging_directories[0]
                if len(selected_staging_directories) == 1
                else None
            ),
            "selected_staging_directory_count": len(selected_staging_directories),
            "staging_fallback_items": staging_fallback_items,
            "total_items": total_items,
            "valid_items": len(valid_items),
            "skipped_items": skipped_before_download,
            "successful": successful,
            "downloads_completed": successful,
            "failed": failed_count,
            "skipped": skipped_count,
            "success_rate": stats["success_rate"],
            "progress_units": total_progress_units + len(skipped_items),
            "bytes_transferred": int(progress_state.get("total_bytes", 0)),
        },
    )


def _resolve_auth(task: Task, site_api: Any, db_manager: Any) -> tuple[Any, int | None]:
    """Resolve auth from task or fall back to first available auth.

    Returns:
        Tuple of (authed, resolved_auth_id).

    Raises:
        TaskExecutionError: If no auth can be resolved.
    """
    raw_auth_id = task.auth_id
    resolved_auth_id = raw_auth_id

    if resolved_auth_id is not None:
        try:
            auth_record = db_manager.get_user_auth(resolved_auth_id)
            normalized_auth_id = (
                int(auth_record.user_id)
                if auth_record is not None and getattr(auth_record, "user_id", None)
                else None
            )
            if normalized_auth_id is not None:
                if normalized_auth_id != resolved_auth_id:
                    logger.info(
                        "[download_handler] Normalized auth_id %s -> %s for site %s",
                        resolved_auth_id,
                        normalized_auth_id,
                        getattr(task, "site_name", "unknown"),
                    )
                resolved_auth_id = normalized_auth_id
                task.auth_id = normalized_auth_id
        except Exception as auth_resolve_exc:
            logger.warning(
                "[download_handler] Failed auth_id normalization for %s: %s",
                resolved_auth_id,
                auth_resolve_exc,
            )

    authed = None
    lookup_auth_ids: list[int] = []
    for candidate_auth_id in (raw_auth_id, resolved_auth_id):
        if candidate_auth_id is not None and candidate_auth_id not in lookup_auth_ids:
            lookup_auth_ids.append(candidate_auth_id)
    for lookup_auth_id in lookup_auth_ids:
        authed = site_api.find_auth(lookup_auth_id)
        if authed is not None:
            break

    if authed is None and resolved_auth_id is not None:
        raise TaskExecutionError(
            f"Auth ID {resolved_auth_id} not found in site API for download task"
        )

    if authed is None:
        fallback_auth = None
        auth_collection = cast(Any, getattr(site_api, "auths", None))
        if isinstance(auth_collection, dict):
            auth_values = cast(dict[Any, Any], auth_collection).values()
            fallback_auth = next(iter(auth_values), None)
        elif isinstance(auth_collection, list):
            auth_list = cast(list[Any], auth_collection)
            fallback_auth = auth_list[0] if auth_list else None

        if fallback_auth is None:
            raise TaskExecutionError("No auth available in site API for download task")

        authed = fallback_auth
        fallback_auth_id_raw = getattr(authed, "id", None)
        fallback_auth_id: int | None = None
        if isinstance(fallback_auth_id_raw, int):
            fallback_auth_id = fallback_auth_id_raw
        elif isinstance(fallback_auth_id_raw, str) and fallback_auth_id_raw.strip():
            try:
                fallback_auth_id = int(fallback_auth_id_raw)
            except ValueError:
                fallback_auth_id = None

        if fallback_auth_id is not None:
            normalized_fallback_auth_id = fallback_auth_id
            try:
                auth_record = db_manager.get_user_auth(fallback_auth_id)
                if auth_record is not None and getattr(auth_record, "user_id", None):
                    normalized_fallback_auth_id = int(auth_record.user_id)
            except Exception as fallback_normalize_exc:
                logger.warning(
                    "[download_handler] Failed fallback auth normalization for %s: %s",
                    fallback_auth_id,
                    fallback_normalize_exc,
                )
            resolved_auth_id = normalized_fallback_auth_id
            task.auth_id = normalized_fallback_auth_id

        logger.warning(
            "[download_handler] Missing/invalid auth_id for task %s, using fallback auth %s",
            task.task_id,
            getattr(authed, "id", "unknown"),
        )

    return authed, resolved_auth_id


def _build_download_items(
    content_metadatas: list[ContentMetadata],
    user: Any,
    base_destination: Path,
    dependent_media_entries: list[MediaEntry],
    *,
    failures: list[DownloadResultItem] | None = None,
    directory_format_override: Path | None = None,
    render_full_directory_format: bool = False,
    download_setup: Any | None = None,
) -> list[DownloadItem]:
    """Build deduplicated download items and apply fresh URLs from scrape output.

    For each media_id, keeps only the newest version (by ``created_at``).
    If *dependent_media_entries* contain fresh URLs, they override the
    database-stored URLs so that IP-locked signatures are current.

    Returns:
        Deduplicated list of DownloadItems with up-to-date URLs.
    """
    seen_media: dict[int, DownloadItem] = {}

    for content_metadata in content_metadatas:
        for media_metadata in content_metadata.medias:
            try:
                download_item = media_metadata.to_download_item(
                    api_user=user,
                    base_directory=base_destination,
                    directory_format_override=directory_format_override,
                    render_full_directory_format=render_full_directory_format,
                    download_setup=download_setup,
                )
            except ValueError as exc:
                if failures is not None:
                    failure = DownloadResultItem(media_id=media_metadata.media_id,
                        content_id=media_metadata.content_id, status="skipped",
                        url=media_metadata.urls[0] if media_metadata.urls else None)
                    failure.add_error(
                        reason=DownloadFailureReason.NO_URL if not media_metadata.urls else DownloadFailureReason.DOWNLOAD_ERROR,
                        message=str(exc),
                    )
                    failures.append(failure)
                logger.warning(
                    "[download_handler] Skipping media %s for content %s: %s",
                    media_metadata.media_id,
                    media_metadata.content_id,
                    exc,
                )
                continue

            if not download_item.media_metadata:
                continue

            media_id = download_item.media_metadata.media_id
            created_at = download_item.media_metadata.created_at

            existing_item = seen_media.get(media_id)
            if existing_item is None:
                seen_media[media_id] = download_item
            elif (
                existing_item.media_metadata is not None
                and existing_item.media_metadata.created_at < created_at
            ):
                seen_media[media_id] = download_item

    download_items = list(seen_media.values())

    # Override with fresh URLs from scrape output (freshly signed for proxy IP)
    if dependent_media_entries:
        fresh_url_map: dict[int, str] = {}
        for entry in dependent_media_entries:
            mid = entry.get("media_id")
            url = entry.get("url")
            if mid is not None and url:
                try:
                    fresh_url_map[int(mid)] = url
                except (ValueError, TypeError):
                    pass

        if fresh_url_map:
            overridden = 0
            for item in download_items:
                if (
                    item.media_metadata
                    and item.media_metadata.media_id in fresh_url_map
                ):
                    fresh_url = fresh_url_map[item.media_metadata.media_id]
                    if item.media_metadata.drm and fresh_url.lower().split("?", 1)[
                        0
                    ].endswith(".mpd"):
                        continue
                    if item.url != fresh_url:
                        item.url = fresh_url
                        item.media_metadata.urls = [fresh_url]
                        overridden += 1
            if overridden:
                logger.info(
                    f"[download_handler] Overrode {overridden} media URLs with fresh scrape URLs"
                )

    return download_items


def _resolve_proxy_url(authed: Any, db_manager: Any) -> str | None:
    """Resolve proxy URL from auth details or database.

    Resolution order:
    1. authed.auth_details.proxy_url (set during API login)
    2. UserAuthModel.proxy_id in database

    No global config fallback — a proxy is only used when explicitly
    assigned to the account.

    Args:
        authed: Authenticated user object
        db_manager: Database manager for fallback lookup

    Returns:
        Proxy URL string or None
    """
    proxy_url = (
        getattr(authed.auth_details, "proxy_url", None)
        if hasattr(authed, "auth_details")
        else None
    )

    if not proxy_url:
        logger.info(
            f"[download_handler] proxy_url not in auth_details, querying database for auth_id={authed.id}"
        )
        from ultima_scraper_backend.usapi_initializer import (
            AssignedProxyResolutionError,
            resolve_assigned_proxy_url,
        )
        from ultima_scraper_db_archive import UserAuthModel

        try:
            with db_manager.get_session() as session:
                auth_record = session.exec(
                    select(UserAuthModel).where(UserAuthModel.user_id == authed.id)
                ).first()
                if auth_record and auth_record.proxy_id:
                    try:
                        proxy_url = resolve_assigned_proxy_url(auth_record.proxy_id)
                    except AssignedProxyResolutionError as exc:
                        raise TaskExecutionError(
                            f"Assigned proxy resolution failed for auth {authed.id} "
                            f"(proxy_id={auth_record.proxy_id}): {exc}"
                        ) from exc
                    logger.info(
                        f"[download_handler] Found proxy from database: {proxy_url}"
                    )
                else:
                    logger.info(
                        f"[download_handler] No proxy_id set for auth {authed.id} in database"
                    )
        except TaskExecutionError:
            raise
        except Exception as exc:
            logger.error(
                f"[download_handler] Error querying database for proxy: {exc}",
                exc_info=True,
            )
    else:
        logger.info(f"[download_handler] Got proxy from auth_details: {proxy_url}")

    if proxy_url:
        logger.info(f"[download_handler] ✓ Using proxy: {proxy_url}")
    else:
        logger.warning(
            "[download_handler] ⚠ No proxy configured - downloads may fail for IP-locked URLs"
        )

    return proxy_url


def _build_filepath_rows(
    download_results: list[DownloadResultItem],
    valid_items: list[DownloadItem],
) -> list[tuple[int, str, bool, bool]]:
    """Build canonical filepath rows for the database writer.

    Creates or updates filepath records so the database tracks where each
    media item was saved on disk.

    Args:
        download_results: Results from the download phase
        valid_items: Download items that carry ``media_metadata``
    """
    # Build media_id → DownloadItem lookup for preview flag
    item_by_media: dict[int, DownloadItem] = {}
    for item in valid_items:
        if item.media_id is not None:
            item_by_media[item.media_id] = item

    rows_to_insert: list[tuple[int, str, bool, bool]] = []
    for result in download_results:
        if result.path is None or result.media_id <= 0:
            continue
        is_ok = result.status == "downloaded" or (
            result.status == "skipped"
            and result.has_error(DownloadFailureReason.ALREADY_EXISTS)
        )
        if not is_ok:
            continue

        preview = False
        di = item_by_media.get(result.media_id)
        if di is not None and di.media_metadata is not None:
            preview = di.media_metadata.preview

        rows_to_insert.append(
            (
                result.media_id,
                result.path,
                preview,
                result.status == "downloaded",
            )
        )

    if not rows_to_insert:
        return []
    return rows_to_insert


class _DRMPipeline:
    """Decrypts and merges each DRM item as soon as its streams are staged.

    Items run concurrently; the media formatter's process slots bound the
    CPU-heavy decrypt/merge work, while final moves overlap with it.
    """

    def __init__(
        self,
        total: int,
        on_progress: DRMProgressCallback | None = None,
        release: Callable[[DownloadItem], None] | None = None,
        remote_store: Any = None,
    ) -> None:
        self.total = total
        # With a backend that keeps the files, the placement rule may give it the work.
        self.remote_store = remote_store
        self._owner_capacity: tuple[float, Any] | None = None
        self.processed = 0
        self.on_progress = on_progress
        self.release = release
        self._started = False
        self._tasks: set[asyncio.Task[None]] = set()

    def submit(self, item: DownloadItem) -> None:
        if not (
            item.media_metadata
            and item.media_metadata.drm
            and item.media_metadata.drm_key
        ):
            if self.release is not None:
                self.release(item)
            return
        task = asyncio.create_task(self._run(item))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def cancel(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def finish(self) -> None:
        try:
            while self._tasks:
                await asyncio.gather(*self._tasks)
        except BaseException:
            await self.cancel()
            raise
        if self._started:
            logger.info(
                f"[DRM] Completed decryption and merge for {self.processed} files"
            )
            await self._notify("finished")

    async def _notify(self, stage: str, item: DownloadItem | None = None) -> None:
        if self.on_progress is None:
            return
        await self.on_progress(
            {
                "stage": stage,
                "current": self.processed,
                "total": self.total,
                "media_id": item.media_id if item is not None else None,
                "path": str(item.get_filepath()) if item is not None else None,
            }
        )

    async def _run(self, item: DownloadItem) -> None:
        if not self._started:
            self._started = True
            logger.info(f"[DRM] Decrypting up to {self.total} DRM-protected files")
            await self._notify("started")
        try:
            succeeded = await self._process(item)
        except Exception:
            logger.exception(f"[DRM] Processing failed for media {item.media_id}")
            succeeded = False
        finally:
            if self.release is not None:
                self.release(item)
        self.processed += 1
        await self._notify("completed" if succeeded else "failed", item)

    async def _process(self, item: DownloadItem) -> bool:
        media_metadata = item.media_metadata
        assert media_metadata is not None, "DRM items must have media metadata"
        drm_key = media_metadata.drm_key
        assert drm_key, f"DRM key missing for media {item.media_id}"

        enc_streams: list[Path] = [
            x
            for x in [
                item.drm_media_settings.drm_video_path,
                item.drm_media_settings.drm_audio_path,
            ]
            if x
        ]
        item_user = item.user
        authed = item_user.get_authed() if item_user is not None else None
        only_drm: OnlyDRM | None = getattr(authed, "drm", None)

        dest_path = item.get_filepath()
        if dest_path.exists():
            logger.info(f"[DRM] Final file already exists: {dest_path}")
            return True

        stream_bytes = sum(stream.stat().st_size for stream in enc_streams if stream.exists())
        if self.remote_store is not None and await self._owner_decrypts(
            stream_bytes, can_decrypt_here=only_drm is not None
        ):
            return await self._decrypt_on_owner(item, enc_streams, drm_key, dest_path)

        if not only_drm:
            logger.error(f"[DRM] DRM module not available for media {item.media_id}")
            return False

        started = placement.meter.started()
        succeeded = False
        try:
            succeeded = await self._decrypt_here(item, enc_streams, drm_key, dest_path, only_drm)
        finally:
            placement.meter.finished(started, stream_bytes, succeeded)
        return succeeded

    async def _owner_capacity_now(self) -> Any:
        """The owner's throughput and queue, asked at most every 10 seconds."""
        now = time.monotonic()
        if self._owner_capacity is None or now - self._owner_capacity[0] > 10:
            try:
                answer = await self.remote_store.client.capacity()
            except OffloadError as exc:
                logger.info(f"[DRM] Owner capacity unavailable, decrypting here: {exc}")
                answer = None
            self._owner_capacity = (now, answer)
        return self._owner_capacity[1]

    async def _owner_decrypts(self, stream_bytes: int, *, can_decrypt_here: bool = True) -> bool:
        answer = await self._owner_capacity_now()
        if not answer or not answer.get("decrypts"):
            return False
        if not can_decrypt_here:
            # No mp4decrypt here (or no DRM module): the owner is the only choice.
            return True
        owner = placement.Capacity(
            rate=answer.get("rate"), waiting=int(answer.get("waiting") or 0), slots=int(answer.get("slots") or 1)
        )
        return placement.prefer_owner(stream_bytes, placement.meter.capacity(), owner)

    async def _decrypt_on_owner(
        self, item: DownloadItem, enc_streams: list[Path], drm_key: str, dest_path: Path
    ) -> bool:
        """Hand the encrypted streams over; the owner decrypts, merges and keeps the file."""
        remote = self.remote_store
        names = ("video", "audio")
        streams = {name: stream for name, stream in zip(names, enc_streams)}
        relative = dest_path.relative_to(remote.local_root).as_posix()
        key = hashlib.sha256(f"{remote.owner_root}/{relative}".encode()).hexdigest()[:32]
        created_at = getattr(item.media_metadata, "created_at", None)
        mtime = created_at.timestamp() if isinstance(created_at, datetime) else None
        await self._notify("decrypting", item)
        logger.info(f"[DRM] Handing media {item.media_id} to {remote.client.owner.backend_id} to decrypt")
        try:
            kept = await remote.client.decrypt(
                streams, key=key, root=remote.owner_root, path=relative, drm_key=drm_key, mtime=mtime
            )
        except OffloadError as exc:
            logger.error(f"[DRM] Owner could not decrypt media {item.media_id}: {exc}")
            return False
        for stream in enc_streams:
            await asyncio.to_thread(stream.unlink, missing_ok=True)
        item.offloaded_path = str(kept["path"])
        item.size = int(kept["size"])
        item.downloaded_this_run = True
        return True

    async def _decrypt_here(
        self,
        item: DownloadItem,
        enc_streams: list[Path],
        drm_key: str,
        dest_path: Path,
        only_drm: OnlyDRM,
    ) -> bool:
        drm_temp_directory = item.drm_media_settings.work_directory
        if drm_temp_directory is not None:
            drm_temp_directory.mkdir(parents=True, exist_ok=True)
        temp_merge_target = _merge_target(dest_path)

        await self._notify("decrypting", item)
        logger.info(f"[DRM] Decrypting streams for media {item.media_id}")
        success = await _decrypt_and_merge(
            only_drm, enc_streams, drm_key, temp_merge_target
        )
        if not success:
            success = await self._decrypt_then_merge(
                item, enc_streams, drm_key, temp_merge_target, only_drm
            )

        if not (success and temp_merge_target.exists()):
            logger.error(f"[DRM] Merge failed for media {item.media_id}")
            await asyncio.to_thread(temp_merge_target.unlink, missing_ok=True)
            return False
        await asyncio.to_thread(os.replace, temp_merge_target, dest_path)
        final_size = dest_path.stat().st_size
        item.downloaded_this_run = True
        # Update item.size so _process_download_result doesn't flag a
        # size mismatch (encrypted stream sizes != final muxed size).
        item.size = final_size
        logger.info(f"[DRM] Successfully created {dest_path} ({final_size:,} bytes)")
        return True

    async def _decrypt_then_merge(
        self,
        item: DownloadItem,
        enc_streams: list[Path],
        drm_key: str,
        temp_merge_target: Path,
        only_drm: OnlyDRM,
    ) -> bool:
        """Decrypt each stream with mp4decrypt, then merge the decrypted copies."""
        drm_temp_directory = item.drm_media_settings.work_directory
        decrypted = await asyncio.gather(
            *(
                only_drm.decrypt_file_async(
                    stream,
                    drm_key,
                    temp_output_path=drm_temp_directory,
                )
                for stream in enc_streams
            )
        )
        dec_streams: list[Path] = []
        for dec_stream_filepath in decrypted:
            if dec_stream_filepath and dec_stream_filepath.exists():
                logger.info(
                    f"[DRM] Decrypted stream created: {dec_stream_filepath} "
                    f"({dec_stream_filepath.stat().st_size:,} bytes)"
                )
                dec_streams.append(dec_stream_filepath)

        # Merge decrypted streams
        logger.info(
            f"[DRM] Merging streams for media {item.media_id} to {temp_merge_target}"
        )
        await self._notify("merging", item)
        return await only_drm.merge_media(temp_merge_target, dec_streams)


async def _hand_over(remote_store: Any, download_results: list[DownloadResultItem]) -> None:
    """Give finished files to the backend that keeps them, a few at a time.

    The recorded path becomes the owner's; a file that cannot be handed over
    stays here and its item fails as recoverable, so a retry sends it again.
    """
    from contextlib import aclosing

    from ultima_scraper_core.concurrency import map_unordered

    async def hand_over(result: DownloadResultItem) -> None:
        kept = result.status == "downloaded" or (
            result.status == "skipped" and result.has_error(DownloadFailureReason.ALREADY_EXISTS)
        )
        if not kept or result.path is None or not remote_store.holds(result.path):
            return
        try:
            result.path = await remote_store.keep(result.path)
        except (OffloadError, OSError) as exc:
            logger.warning(f"[download_handler] Could not hand media {result.media_id} over: {exc}")
            result.status = "failed"
            result.add_error(DownloadFailureReason.DOWNLOAD_ERROR, f"hand-over to the storing backend failed: {exc}")

    async with aclosing(map_unordered(hand_over, download_results, limit=2)) as runs:
        async for _ in runs:
            pass


def _merge_target(dest_path: Path) -> Path:
    """Where the merge writes: beside the final file, so publishing is a rename.

    Writing it in staging instead would mean copying the whole file again
    whenever staging and the library are on different filesystems.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    return _build_temp_merge_path(dest_path)


_CONTENT_KEY = re.compile(r"[0-9a-fA-F]{32}")


async def _decrypt_and_merge(
    only_drm: OnlyDRM, streams: list[Path], drm_key: str, output: Path
) -> bool:
    """Decrypt and merge the CENC streams in one ffmpeg pass, straight to `output`.

    It reads each encrypted stream once and writes nothing else, where
    mp4decrypt writes a decrypted copy of each stream and the merge reads
    those again. False when it can't, and the caller decrypts with mp4decrypt.
    """
    formatter = getattr(only_drm, "media_formatter", None)
    # The key is KID:KEY; ffmpeg wants the key alone.
    key = drm_key.rsplit(":", 1)[-1]
    if formatter is None or not streams or not _CONTENT_KEY.fullmatch(key):
        return False
    args = ["ffmpeg", "-nostdin", "-loglevel", "error", "-y"]
    for stream in streams:
        args += ["-decryption_key", key, "-i", stream.as_posix()]
    for index in range(len(streams)):
        args += ["-map", str(index)]
    args += ["-c", "copy", "-f", "mp4", output.as_posix()]
    try:
        result = await formatter.run_process(args)
    except (OSError, RuntimeError) as exc:
        # ffmpeg is missing, or the formatter is shutting down.
        logger.warning(f"[DRM] ffmpeg could not decrypt {output.name}: {exc}")
        result = None
    if result is None or result.returncode or not output.exists():
        if result is not None:
            logger.warning(
                f"[DRM] ffmpeg could not decrypt {output.name}, using mp4decrypt: "
                f"{result.stderr.decode(errors='replace')[-500:]}"
            )
        await asyncio.to_thread(output.unlink, missing_ok=True)
        return False
    for stream in streams:
        await asyncio.to_thread(stream.unlink, missing_ok=True)
    return True


def _count_drm_items(items: list[DownloadItem]) -> int:
    return sum(1 for item in items if item.media_metadata and item.media_metadata.drm)


async def _process_drm_items(
    valid_items: list[DownloadItem],
    on_progress: DRMProgressCallback | None = None,
) -> None:
    """Process already-downloaded DRM items concurrently.

    Decrypts and merges DRM-protected video/audio streams.
    Updates item.size and the database with the real final file size
    (which differs from the encrypted stream sizes).

    Args:
        valid_items: List of downloaded items to check for DRM
    """
    pipeline = _DRMPipeline(_count_drm_items(valid_items), on_progress)
    for item in valid_items:
        pipeline.submit(item)
    await pipeline.finish()


async def _run_download_pipeline(
    download_manager: DownloadManager,
    valid_items: list[DownloadItem],
    on_drm_progress: DRMProgressCallback | None = None,
    remote_store: Any = None,
) -> dict[str, Any]:
    """Download items and post-process each DRM item as soon as it lands."""
    pipeline = _DRMPipeline(
        _count_drm_items(valid_items),
        on_drm_progress,
        release=download_manager.release_workspace,
        remote_store=remote_store,
    )
    download_manager.on_downloaded.append(pipeline.submit)
    try:
        stats = await download_manager.bulk_download(valid_items, create_session=True)
    except BaseException:
        await pipeline.cancel()
        raise
    finally:
        download_manager.on_downloaded.remove(pipeline.submit)
    await pipeline.finish()
    return stats


async def _process_download_result(
    item: DownloadItem,
    written: list[dict[str, Any]],
) -> DownloadResultItem:
    """Process a single download result, checking file status.

    Args:
        item: Download item to check
        written: List to append write records to

    Returns:
        DownloadResultItem with status
    """
    filepath = item.get_filepath()
    media_id = item.media_id or 0
    content_id = resolve_content_id(item)

    # Stat/unlink/utime are blocking — run them off the event-loop thread.
    # Resolve the stat in one step so shared filepath processing can't race an
    # exists() check against a sibling task deleting the file.
    try:
        stat_result = await asyncio.to_thread(filepath.stat)
    except FileNotFoundError:
        stat_result = None
    except OSError as exc:
        logger.warning(f"Failed to stat download result {filepath}: {exc}")
        stat_result = None

    if stat_result is not None:
        size = stat_result.st_size
        expected_size = item.size

        # Validate file size
        if expected_size and expected_size > 0 and size != expected_size:
            if item.downloaded_this_run:
                logger.warning(
                    f"Size mismatch for {filepath.name}: "
                    f"downloaded {size:,} bytes, expected {expected_size:,} bytes. "
                    f"Deleting corrupted file."
                )
                try:
                    await asyncio.to_thread(filepath.unlink)
                except FileNotFoundError:
                    logger.warning(
                        f"Corrupted file already removed before cleanup: {filepath}"
                    )
                except OSError as e:
                    logger.error(f"Failed to delete corrupted file {filepath}: {e}")
                written.append(
                    {
                        "path": str(filepath),
                        "bytes": 0,
                        "skipped": False,
                        "failed": True,
                        "reason": "size_mismatch",
                    }
                )
                result = DownloadResultItem(
                    media_id=media_id,
                    content_id=content_id,
                    status="failed",
                    path=str(filepath),
                    bytes_written=0,
                )
                result.add_error(
                    DownloadFailureReason.SIZE_MISMATCH,
                    f"Size mismatch: got {size}, expected {expected_size}",
                )
                return result

            logger.warning(
                f"Existing file size mismatch for {filepath.name}: "
                f"found {size:,} bytes, expected {expected_size:,} bytes. "
                f"Leaving file in place because this run did not replace it."
            )
            written.append(
                {
                    "path": str(filepath),
                    "bytes": 0,
                    "skipped": False,
                    "failed": True,
                    "reason": "size_mismatch",
                }
            )
            result = DownloadResultItem(
                media_id=media_id,
                content_id=content_id,
                status="failed",
                path=str(filepath),
                bytes_written=0,
            )
            result.add_error(
                DownloadFailureReason.SIZE_MISMATCH,
                f"Size mismatch: got {size}, expected {expected_size}",
            )
            return result

        written.append({"path": str(filepath), "bytes": size, "skipped": False})

        # Set file timestamp from created_at
        if item.media_metadata:
            created_at_dt = getattr(item.media_metadata, "created_at", None)
            if created_at_dt and isinstance(created_at_dt, datetime):
                try:
                    timestamp = created_at_dt.timestamp()
                    await asyncio.to_thread(os.utime, filepath, (timestamp, timestamp))
                    logger.debug(
                        f"Set timestamp for {filepath.name} to {created_at_dt.isoformat()}"
                    )
                except (OSError, ValueError) as ts_err:
                    logger.warning(
                        f"Failed to set timestamp for {filepath.name}: {ts_err}"
                    )

        return DownloadResultItem(
            media_id=media_id,
            content_id=content_id,
            status="downloaded",
            path=str(filepath),
            bytes_written=size,
        )
    else:
        # File not created - diagnose URL
        result = DownloadResultItem(
            media_id=media_id,
            content_id=content_id,
            status="failed",
            path=str(filepath),
            url=item.url,
            bytes_written=0,
        )

        from .results import add_url_diagnostics

        # DRM authenticates via cookies; an unsigned manifest URL is expected.
        diag_msg = (
            "DRM manifest authentication uses cookies"
            if item.media_metadata and item.media_metadata.drm
            else add_url_diagnostics(result, item.url)
        )
        url_preview = (
            (item.url[:100] + "...")
            if item.url and len(item.url) > 100
            else (item.url or "N/A")
        )

        # Always add base download error
        result.add_error(
            DownloadFailureReason.DOWNLOAD_ERROR,
            item.failure_message or "Download failed - file not created",
        )

        logger.warning(
            f"Download failed for media_id={media_id}, content_id={content_id}\n"
            f"  URL: {url_preview}\n"
            f"  Target: {filepath}\n"
            f"  Diagnosis: {diag_msg}\n"
            f"  Errors: {[e.reason.value for e in result.errors]}"
        )

        written.append(
            {
                "path": str(filepath),
                "bytes": 0,
                "skipped": False,
                "failed": True,
            }
        )

        return result


async def _handle_profile_download(
    task: Task,
    context: TaskRunContext,
    user: Any,
    db_user: Any,
    db_manager: Any,
    authed: Any,
    user_manager: Any,
    config: Any,
    offload: Any = None,
) -> TaskResult:
    """Handle downloading profile media (avatar and header).

    Profile media is stored differently from regular content - it has
    avatar_url and header_url as direct string attributes rather than
    a media relationship.

    Args:
        task: The download task
        context: Task execution context
        user: API user object
        db_user: Database user model
        db_manager: Database manager instance
        authed: Authenticated API user
        user_manager: User directory manager
        config: Application config
        offload: The backends to hand files to, when this one has no drive

    Returns:
        TaskResult with download results
    """
    from ultima_scraper_db_archive import UserProfileMediaModel

    user_id = _coerce_int(task.user_id, 0)
    site_name = task.site_name
    payload = task.payload
    ignore_space_requirements = (
        bool(payload.ignore_space_requirements)
        if isinstance(payload, DownloadTaskPayload)
        else False
    )

    written: list[dict[str, Any]] = []
    bytes_written = 0
    download_results: list[DownloadResultItem] = []

    # Fetch profile media from database
    profile_media_items = await asyncio.to_thread(
        db_manager.get_content,
        user_id=user_id,
        content_type="profile",
        limit=1,  # Get most recent profile media only
    )

    if not profile_media_items:
        logger.info(f"[profile_download] No profile media found for user {user_id}")
        return TaskResult(
            success=True,
            output={
                "files": [],
                "bytes_written": 0,
                "download_results": [],
            },
            message="No profile media to download",
            metrics={
                "bytes_written": 0,
                "total_items": 0,
            },
        )

    profile_media: UserProfileMediaModel = profile_media_items[0]

    # Resolve proxy URL
    proxy_url = _resolve_proxy_url(authed, db_manager)

    from .streaming import _destination

    # Select destination directory: this backend's drive, or the owner's mirror here
    destination_selection, remote = await _destination(
        offload,
        user_id=user_id,
        usernames=user.get_usernames(),
        site_name=site_name,
        user_manager=user_manager,
        config=config,
        required_space=10 * 1024 * 1024,  # 10MB estimate
        ignore_space_requirements=ignore_space_requirements,
    )
    base_destination = destination_selection.absolute_path

    # Create Profile subdirectory
    profile_directory = base_destination / "Profile"
    profile_directory.mkdir(parents=True, exist_ok=True)

    # Build download items for avatar and header
    download_items: list[DownloadItem] = []

    if profile_media.avatar_url:
        filename = extract_profile_filename_from_url(profile_media.avatar_url)
        if filename:
            download_items.append(
                DownloadItem(
                    user=user,
                    url=profile_media.avatar_url,
                    filename=filename,
                    directory=profile_directory / "Avatars",
                    media_id=profile_media.id * 10 + 1,  # Unique ID for avatar
                    content_id=profile_media.id,
                    size=None,
                    headers={},
                )
            )

    if profile_media.header_url:
        filename = extract_profile_filename_from_url(profile_media.header_url)
        if filename:
            download_items.append(
                DownloadItem(
                    user=user,
                    url=profile_media.header_url,
                    filename=filename,
                    directory=profile_directory / "Headers",
                    media_id=profile_media.id * 10 + 2,  # Unique ID for header
                    content_id=profile_media.id,
                    size=None,
                    headers={},
                )
            )

    if not download_items:
        logger.info(
            f"[profile_download] No avatar/header URLs found for user {user_id}"
        )
        return TaskResult(
            success=True,
            output={
                "files": [],
                "bytes_written": 0,
                "download_results": [],
            },
            message="No avatar/header URLs to download",
            metrics={
                "bytes_written": 0,
                "total_items": 0,
            },
        )

    logger.info(
        f"[profile_download] Downloading {len(download_items)} profile media items for user {user_id}"
    )

    # Create download manager
    download_manager = DownloadManager(
        config=config,
        worker_id=0,
        proxy_url=proxy_url,
        database_writes=context.database_writes,
        database_write_site=task.site_name,
        database_write_scope=task.task_id,
    )

    # Filter out items that already exist
    items_to_download: list[DownloadItem] = []
    for item in download_items:
        if not item.url or not item.filename or not item.directory:
            continue
        filepath = item.directory / item.filename
        if filepath.exists():
            existing = DownloadResultItem(
                media_id=item.media_id or 0,
                content_id=item.content_id,
                status="skipped",
                path=str(filepath),
                bytes_written=0,
            )
            # Left in the mirror by a hand-over that failed: sent again below.
            existing.add_error(DownloadFailureReason.ALREADY_EXISTS, "Profile media already exists")
            download_results.append(existing)
            written.append(
                {
                    "path": str(filepath),
                    "bytes": 0,
                    "skipped": True,
                    "failed": False,
                }
            )
        else:
            items_to_download.append(item)

    if items_to_download:
        # Use bulk_download for efficient downloading
        await download_manager.bulk_download(items_to_download, create_session=True)

        # Check results
        for item in items_to_download:
            if not item.filename or not item.directory:
                continue
            filepath = item.directory / item.filename

            if filepath.exists():
                file_bytes = filepath.stat().st_size
                bytes_written += file_bytes
                download_results.append(
                    DownloadResultItem(
                        media_id=item.media_id or 0,
                        content_id=item.content_id,
                        status="downloaded",
                        path=str(filepath),
                        bytes_written=file_bytes,
                    )
                )
                written.append(
                    {
                        "path": str(filepath),
                        "bytes": file_bytes,
                        "skipped": False,
                        "failed": False,
                    }
                )
            else:
                result = DownloadResultItem(
                    media_id=item.media_id or 0,
                    content_id=item.content_id,
                    status="failed",
                    path=str(filepath),
                    bytes_written=0,
                    url=item.url,
                )
                result.add_error(
                    DownloadFailureReason.DOWNLOAD_ERROR,
                    "Profile media download failed",
                )
                download_results.append(result)
                written.append(
                    {
                        "path": str(filepath),
                        "bytes": 0,
                        "skipped": False,
                        "failed": True,
                    }
                )

    if remote is not None:
        await _hand_over(remote, download_results)
        # Each written entry pairs with the result appended beside it.
        for entry, result in zip(written, download_results):
            entry["path"] = result.path
            if result.status == "failed" and not entry["failed"]:
                entry["failed"] = True
                bytes_written -= entry["bytes"]

    success_count = sum(1 for r in download_results if r.status == "downloaded")
    skip_count = sum(1 for r in download_results if r.status == "skipped")
    fail_count = sum(1 for r in download_results if r.status == "failed")

    return TaskResult(
        success=True,
        output={
            "files": written,
            "bytes_written": bytes_written,
            "download_results": [r.model_dump() for r in download_results],
            "proxy_url": proxy_url,
            "proxy_routes": [proxy_url] if proxy_url else [],
            "proxy_hop_used": 0 if proxy_url else None,
        },
        message=f"Downloaded {success_count} profile media ({skip_count} skipped, {fail_count} failed)",
        metrics={
            "bytes_written": bytes_written,
            "total_items": len(download_items),
            "items_downloaded": success_count + skip_count,
            "success_count": success_count,
            "skip_count": skip_count,
            "fail_count": fail_count,
            "proxy_url": proxy_url,
            "proxy_routes": [proxy_url] if proxy_url else [],
            "proxy_hop_used": 0 if proxy_url else None,
        },
    )
