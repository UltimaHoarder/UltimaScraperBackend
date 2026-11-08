"""Bounded SQL selection, metadata batches and durable download reports."""

from __future__ import annotations

import asyncio
import json
from contextlib import aclosing
from uuid import uuid4

from ....database.media_selection import (
    BoundedPreview,
    MediaSelectionStore,
    page_byte_budget,
    positive_budget,
)
from ....database.write_queue import DatabaseWriteOperation, stable_command_id
from ...download_manager import DownloadManager, ResourceManager
from ...drive_selection import DriveSelectionError, select_user_destination
from ...models import TaskResult
from .utils import normalise_files


async def _write(writer, spec, **payload):
    payload["spec"] = spec
    return await writer.submit(
        DatabaseWriteOperation.UPDATE_MEDIA_SELECTION,
        payload,
        site=spec["site"],
        command_id=stable_command_id(
            "media-selection-v1",
            spec["selection_id"],
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
        ),
    )


async def _archive_selection(
    writer, spec, *, user_id, content_type, batch_size, max_items=None, content_ids=None
):
    cursor = None
    count = 0
    while max_items is None or count < max_items:
        limit = batch_size if max_items is None else min(batch_size, max_items - count)
        page = await _write(
            writer,
            spec,
            action="archive_page",
            user_id=user_id,
            content_type=content_type,
            limit=limit,
            cursor=cursor,
            content_ids=content_ids,
        )
        if not page["count"]:
            break
        count += page["count"]
        cursor = page["cursor"]


async def prepare_download_selection(
    task, context, spec, *, content_type, batch_size, max_bytes
):
    """Merge dependency selections in SQL, or stage the requested archive pages."""
    writer = context.database_writes
    selected = False
    for dependency in task.depends_on:
        output = await context.storage.get_task_output(context.job.job_id, dependency)
        if not isinstance(output, dict):
            continue
        reference = output.get("media_selection")
        if isinstance(reference, dict):
            if len(task.depends_on) == 1:
                return await _write(
                    writer,
                    spec,
                    action="adopt",
                    source_id=reference["selection_id"],
                    source_task_id=dependency,
                )
            selected = True  # An explicitly empty scrape must stay empty.
            after = 0
            while True:
                page = await _write(
                    writer,
                    spec,
                    action="copy_page",
                    source_id=reference["selection_id"],
                    source_task_id=dependency,
                    user_id=int(task.user_id),
                    content_type=content_type,
                    after=after,
                    limit=batch_size,
                    max_bytes=max_bytes,
                )
                if page["cursor"] is None:
                    break
                after = page["cursor"]
            continue
        # Previously queued tasks already contain a legacy full Redis blob. Drain
        # that format without creating another whole-profile ID/URL mapping.
        raw = output.get("media_urls", output.get("content_by_auth"))
        if not isinstance(raw, (dict, list)):
            continue
        entries = normalise_files(raw)
        for start in range(0, len(entries), batch_size):
            batch = entries[start : start + batch_size]
            ids = sorted(
                {
                    int(entry["content_id"])
                    for entry in batch
                    if entry.get("content_id") is not None
                }
            )
            if not ids:
                continue
            selected = True
            await _archive_selection(
                writer,
                spec,
                user_id=int(task.user_id),
                content_type=content_type,
                batch_size=batch_size,
                content_ids=ids,
            )
            urls = [
                {"media_id": int(entry["media_id"]), "url": str(entry["url"])}
                for entry in batch
                if entry.get("media_id") and entry.get("url")
            ]
            if urls:
                await _write(
                    writer,
                    spec,
                    action="legacy_urls",
                    user_id=int(task.user_id),
                    content_type=content_type,
                    rows=urls,
                )
    if not selected:
        await _archive_selection(
            writer,
            spec,
            user_id=int(task.user_id),
            content_type=content_type,
            batch_size=batch_size,
            max_items=task.payload.max_items,
            content_ids=task.payload.media_ids,
        )
    return await _write(writer, spec, action="seal")


async def prepare_paid_feed_selection(task, context, spec):
    """A paid download consumes exactly its completed producer's selection."""
    if len(task.depends_on) != 1:
        raise ValueError("Paid-feed downloads require one scrape dependency")
    dependency = task.depends_on[0]
    output = await context.storage.get_task_output(context.job.job_id, dependency)
    reference = output.get("media_selection") if isinstance(output, dict) else None
    if not isinstance(reference, dict) or not reference.get("selection_id"):
        raise ValueError("Paid-feed scrape has no durable media selection")
    return await _write(
        context.database_writes,
        spec,
        action="adopt",
        source_id=reference["selection_id"],
        source_task_id=dependency,
    )


async def _paid_download_targets(
    store, selection_id, *, db_manager, authed, site_name, batch_size
):
    from .handler import _DatabaseBackedUser, _db_user_alias_usernames, _load_db_user
    from ...models import TaskExecutionError

    after = None
    while True:
        page = await asyncio.to_thread(
            store.download_target_page, selection_id, after=after, limit=batch_size
        )
        if not page:
            return
        for user_id, content_type in page:
            # No API/cache-wide user materialization: paid signatures were saved
            # by the producer, and identity/aliases come from the canonical DB.
            db_user = await _load_db_user(db_manager, user_id=user_id)
            if db_user is None:
                raise TaskExecutionError(
                    f"Paid-feed user {user_id} is absent from the archive"
                )
            user = _DatabaseBackedUser(id=user_id, authed=authed, site_name=site_name)
            user.add_aliases(
                [getattr(db_user, "username", None), *_db_user_alias_usernames(db_user)]
            )
            yield user, db_user, content_type
        after = page[-1]


async def download_selected_media(*, context, config, **kwargs):
    """Download into this backend's drives, or for another backend to keep."""
    from ....offload.remote import open_offload

    async with open_offload(config, getattr(context, "redis", None)) as offload:
        return await _download_selected_media(context=context, config=config, offload=offload, **kwargs)


async def _destination(offload, **request):
    """This backend's drive for a performer, or the owner's when it has none."""
    if offload is None:
        return await asyncio.to_thread(select_user_destination, **request), None
    from ....offload.client import OffloadError

    try:
        remote = await offload.destination(
            site_name=request["site_name"],
            user_id=request["user_id"],
            usernames=list(request["usernames"]),
            required_space=request["required_space"],
            ignore_space_requirements=request["ignore_space_requirements"],
        )
    except OffloadError as exc:
        raise DriveSelectionError(str(exc)) from exc
    return remote.selection, remote


async def _download_selected_media(
    *,
    task,
    context,
    user,
    db_user,
    db_manager,
    authed,
    config,
    content_type,
    resolved_auth_id,
    offload,
):
    from .handler import (
        _download_content_batch,
        _proxy_route_fields,
        _resolve_proxy_url,
    )

    paid_feed = bool(getattr(task.payload, "paid_content_feed", False))
    # Guest IDs identify ephemeral API sessions, not archive account scopes.
    # Keep the producer's unauthenticated scope when adopting its selection.
    selection_auth_id = None if bool(getattr(authed, "guest", False)) else resolved_auth_id
    user_id = int(user.id) if user is not None else 0
    proxy_url = await asyncio.to_thread(_resolve_proxy_url, authed, db_manager)
    # The route shows while the selection is prepared, before any progress.
    task.result = TaskResult(
        success=True,
        message="Preparing the download",
        output={},
        metrics=_proxy_route_fields(proxy_url),
    )
    await context.storage.persist_task(job_id=context.job.job_id, task=task)
    resources = ResourceManager.from_config(config, proxy_url=proxy_url)
    batch_size = positive_budget(
        "ULTIMA_DOWNLOAD_BATCH_SIZE", resources.get_file_concurrency()
    )
    max_bytes = page_byte_budget()
    spec = {
        "selection_id": uuid4().hex,
        "job_id": context.job.job_id,
        "task_id": task.task_id,
        "site": task.site_name.lower(),
        "auth_id": selection_auth_id,
    }
    if paid_feed:
        prepared = await prepare_paid_feed_selection(task, context, spec)
    else:
        prepared = await prepare_download_selection(
            task,
            context,
            spec,
            content_type=content_type,
            batch_size=batch_size,
            max_bytes=max_bytes,
        )
    input_selection_id = prepared["selection_id"]
    spec["input_selection_id"] = input_selection_id
    store = MediaSelectionStore(db_manager.get_session)
    selection = None
    remote = None
    if paid_feed:
        totals = await asyncio.to_thread(store.feed_download_totals, input_selection_id)
    else:
        totals = await asyncio.to_thread(
            store.download_totals,
            input_selection_id,
            user_id=user_id,
            content_type=content_type,
            auth_id=selection_auth_id,
        )
        selection, remote = await _destination(
            offload,
            user_id=user_id,
            usernames=user.get_usernames(),
            site_name=task.site_name,
            user_manager=context.user_manager,
            config=config,
            required_space=totals["bytes"],
            ignore_space_requirements=bool(task.payload.ignore_space_requirements),
        )
    manager = DownloadManager(
        config=config,
        worker_id=0,
        proxy_url=proxy_url,
        database_writes=context.database_writes,
        database_write_site=task.site_name,
        database_write_scope=task.task_id,
    )
    preview_limit = positive_budget(
        "ULTIMA_TASK_RESULT_PREVIEW_MAX_ENTRIES", batch_size
    )
    preview_bytes = positive_budget("ULTIMA_TASK_RESULT_PREVIEW_MAX_BYTES", max_bytes)
    previews = {
        kind: BoundedPreview(limit=preview_limit, max_bytes=preview_bytes)
        for kind in ("files", "download_results")
    }
    counters = {
        key: 0
        for key in (
            "items_processed",
            "bytes_written",
            "bytes_transferred",
            "valid_items",
            "skipped_items",
            "successful",
            "downloads_completed",
            "failed",
            "skipped",
            "progress_units",
            "staging_fallback_items",
        )
    }
    counters.update(
        total_items=totals["count"],
        expected_bytes=totals["bytes"],
        progress_total=totals["count"] + totals.get("drm", 0),
    )
    failure_breakdown = {}
    staging_directories = set()
    after = 0

    def outcome():
        return TaskResult(
            success=True,
            message=(
                f"Downloaded {counters['successful']} paid-feed file(s)"
                if paid_feed
                else f"Downloaded {counters['successful']} file(s) to {selection.root}"
            ),
            output={
                "files": previews["files"].values,
                "download_results": previews["download_results"].values,
                "files_total": previews["files"].count,
                "download_results_total": previews["download_results"].count,
                "media_evidence": spec,
                "failure_breakdown": dict(failure_breakdown),
                "selected_staging_directories": sorted(staging_directories),
                "staging_fallback_items": counters["staging_fallback_items"],
                **_proxy_route_fields(proxy_url),
            },
            metrics={
                **counters,
                "items_total": counters["total_items"],
                "selected_drive": str(selection.root)
                if selection is not None
                else None,
                "selected_directory_format": str(selection.directory_format)
                if selection is not None
                else None,
                "selected_staging_directory": next(iter(staging_directories))
                if len(staging_directories) == 1
                else None,
                "selected_staging_directory_count": len(staging_directories),
                "success_rate": counters["successful"] / counters["valid_items"] * 100
                if counters["valid_items"]
                else 0.0,
                **_proxy_route_fields(proxy_url),
            },
        )

    async def targets():
        if not paid_feed:
            yield user, db_user, content_type
            return
        async with aclosing(
            _paid_download_targets(
                store,
                input_selection_id,
                db_manager=db_manager,
                authed=authed,
                site_name=task.site_name,
                batch_size=positive_budget(
                    "ULTIMA_DOWNLOAD_TARGET_BATCH_SIZE", batch_size
                ),
            )
        ) as pairs:
            async for pair in pairs:
                yield pair

    task.result = outcome()
    await context.storage.persist_task(job_id=context.job.job_id, task=task)
    async with manager.session_scope(), aclosing(targets()) as target_stream:
        async for user, db_user, content_type in target_stream:
            user_id = int(user.id)
            if paid_feed:
                target_totals = await asyncio.to_thread(
                    store.download_totals,
                    input_selection_id,
                    user_id=user_id,
                    content_type=content_type,
                    auth_id=selection_auth_id,
                )
                selection, remote = await _destination(
                    offload,
                    user_id=user_id,
                    usernames=user.get_usernames(),
                    site_name=task.site_name,
                    user_manager=context.user_manager,
                    config=config,
                    required_space=target_totals["bytes"],
                    ignore_space_requirements=bool(
                        task.payload.ignore_space_requirements
                    ),
                )
            after = 0
            while True:
                page = await asyncio.to_thread(
                    store.download_page,
                    input_selection_id,
                    user_id=user_id,
                    content_type=content_type,
                    auth_id=selection_auth_id,
                    after=after,
                    limit=batch_size,
                    max_bytes=max_bytes,
                )
                if not page:
                    break
                after = max(
                    media.media_id for content in page for media in content.medias
                )
                result = await _download_content_batch(
                    task=task,
                    context=context,
                    user=user,
                    db_user=db_user,
                    authed=authed,
                    config=config,
                    content_metadatas=page,
                    content_type=content_type,
                    destination_selection=selection,
                    remote_store=remote,
                    download_manager=manager,
                    aggregate=counters,
                    evidence_spec=spec,
                    proxy_url=proxy_url,
                )
                output = result.output or {}
                categories = [
                    {
                        "kind": kind,
                        "offset": preview.count,
                        "values": output.get(kind, []),
                    }
                    for kind, preview in previews.items()
                ]
                await _write(
                    context.database_writes,
                    spec,
                    action="evidence",
                    categories=categories,
                )
                for kind, preview in previews.items():
                    preview.extend(output.get(kind, []))
                for key in counters:
                    if key not in {"total_items", "expected_bytes", "progress_total"}:
                        counters[key] += int(result.metrics.get(key, 0))
                # The no-valid-files branch has only skip counts, so include those in progress.
                if "items_processed" not in result.metrics:
                    skipped = int(result.metrics.get("skipped_items", 0))
                    counters["items_processed"] += skipped
                    counters["skipped"] += skipped
                    counters["progress_units"] += skipped
                for reason, count in output.get("failure_breakdown", {}).items():
                    failure_breakdown[reason] = failure_breakdown.get(reason, 0) + int(
                        count
                    )
                staging_directories.update(
                    output.get("selected_staging_directories", [])
                )
                task.result = outcome()
                await context.storage.persist_task(job_id=context.job.job_id, task=task)
    return outcome()
