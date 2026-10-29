"""Content scraping functions for the scrape handler.

This module contains functions for scraping different content types
from the API and saving them to the database.
"""

from __future__ import annotations

import inspect
import json
import logging
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import aclosing
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, cast
from urllib.parse import urlparse

from ultima_scraper_api import ContentType

from ....database.media_selection import BoundedPreview, page_byte_budget, positive_budget
from ...metadata_models import ContentMetadata, MediaMetadata
from ....database.write_executor import serialize_content_write_item
from ....database.write_queue import (
    DatabaseWriteClient,
    DatabaseWriteError,
    DatabaseWriteOperation,
    stable_command_id,
)

if TYPE_CHECKING:
    from ultima_scraper_api import content_types, user_types

logger = logging.getLogger(__name__)

SAVE_PROGRESS_BATCH = 10
SAVE_PROGRESS_MAX_SILENCE_SECONDS = 5.0
CONTENT_WRITE_BATCH_SIZE = 100


class AuthorLike(Protocol):
    id: int | str
    username: str | None

    def get_authed(self) -> object: ...


class PreviewEntryLike(Protocol):
    id: int | str


class ParsedURLLike(Protocol):
    hostname: str | None

    def geturl(self) -> str: ...


def _coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _content_type_name(value: ContentType | str) -> str:
    return value.value if isinstance(value, ContentType) else str(value)


def _release_scrape_working_set(user: user_types, content_type: ContentType) -> None:
    """Drop API objects after extraction and database persistence complete."""
    names_by_type = {
        ContentType.POSTS: ("Posts",),
        ContentType.ARCHIVED_POSTS: ("Posts",),
        ContentType.MESSAGES: ("Messages", "MassMessages"),
        ContentType.STORIES: ("Stories",),
        ContentType.HIGHLIGHTS: ("Highlights", "Stories"),
        ContentType.MASS_MESSAGES: ("MassMessages",),
    }
    names = names_by_type.get(content_type, ())
    user.release_cached_content(*names)


def _as_mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return cast(Mapping[str, object], value)
    return None


def _as_sequence(value: object) -> Sequence[object] | None:
    if isinstance(value, (str, bytes, bytearray)):
        return None
    if isinstance(value, Sequence):
        return cast(Sequence[object], value)
    return None


def _call_author(item: object) -> AuthorLike | None:
    getter = getattr(item, "get_author", None)
    if not callable(getter):
        return None
    author = getter()
    return cast(AuthorLike | None, author)


def _get_fetcher(
    target: object,
    name: str,
) -> Callable[..., Awaitable[list[Any]]] | None:
    candidate = getattr(target, name, None)
    if not callable(candidate):
        return None
    return cast(Callable[..., Awaitable[list[Any]]], candidate)


def _mask_proxy_url(raw_proxy_url: object) -> str | None:
    if not isinstance(raw_proxy_url, str):
        return None
    normalized = raw_proxy_url.strip()
    if not normalized:
        return None
    try:
        parsed = urlparse(normalized)
    except Exception:
        return "<configured>"
    if not parsed.scheme or not parsed.hostname:
        return "<configured>"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _summarize_auth_transport(authed: object) -> str:
    auth_details = None
    details_getter = getattr(authed, "get_auth_details", None)
    if callable(details_getter):
        try:
            auth_details = details_getter()
        except Exception:
            auth_details = None
    if auth_details is None:
        auth_details = getattr(authed, "auth_details", None)

    proxy_url = _mask_proxy_url(getattr(auth_details, "proxy_url", None))

    auth_session = getattr(authed, "auth_session", None)
    if auth_session is None:
        requester_getter = getattr(authed, "get_requester", None)
        if callable(requester_getter):
            try:
                auth_session = requester_getter()
            except Exception:
                auth_session = None

    active_session = getattr(auth_session, "active_session", None)
    connector = getattr(active_session, "connector", None) if active_session else None
    connector_name = type(connector).__name__ if connector is not None else "unknown"
    session_closed = getattr(active_session, "closed", None)

    proxy_manager = None
    proxy_manager_getter = getattr(auth_session, "get_proxy_manager", None)
    if callable(proxy_manager_getter):
        try:
            proxy_manager = proxy_manager_getter()
        except Exception:
            proxy_manager = None

    proxy_pool_size = 0
    current_proxy = None
    proxies = getattr(proxy_manager, "proxies", None)
    if isinstance(proxies, list):
        proxy_pool_size = len(proxies)
    current_proxy_getter = getattr(proxy_manager, "get_current_proxy", None)
    if callable(current_proxy_getter):
        try:
            current_proxy_value = current_proxy_getter()
            current_host = getattr(current_proxy_value, "host", None)
            current_port = getattr(current_proxy_value, "port", None)
            if current_host:
                current_proxy = (
                    f"{current_host}:{current_port}"
                    if current_port is not None
                    else str(current_host)
                )
        except Exception:
            current_proxy = None

    parts = [
        f"auth_id={getattr(authed, 'id', 'unknown')}",
        f"username={getattr(authed, 'username', None) or 'unknown'}",
        f"guest={bool(getattr(authed, 'guest', False))}",
        f"proxy_url={proxy_url or 'direct'}",
        f"connector={connector_name}",
        f"proxy_pool_size={proxy_pool_size}",
    ]
    if session_closed is not None:
        parts.append(f"session_closed={session_closed}")
    if current_proxy:
        parts.append(f"current_proxy={current_proxy}")
    return ", ".join(parts)


def _determine_paid_status(item: object, item_price: float) -> bool:
    if item_price <= 0:
        return False
    media_value = getattr(item, "media", None) or getattr(item, "medias", None)
    media_sequence = _as_sequence(media_value)
    if not media_sequence:
        return False
    can_view_values = [
        bool(getattr(media_item, "canView", False)) for media_item in media_sequence
    ]
    return bool(can_view_values) and all(can_view_values)


def _collect_media_urls(
    item: object, media: object, fallback_urls: list[str]
) -> list[str]:
    urls = list(fallback_urls)
    for picker_name in ("url_picker", "preview_url_picker"):
        picker = getattr(item, picker_name, None)
        if not callable(picker):
            continue
        try:
            parsed_url = picker(media)
        except Exception:
            continue
        if parsed_url is None:
            continue
        parsed_like = cast(ParsedURLLike, parsed_url)
        if parsed_like.hostname:
            urls.append(parsed_like.geturl())
    return list(dict.fromkeys(urls))


def _coerce_preview_ids(raw_value: object) -> list[int]:
    ids: list[int] = []
    sequence = _as_sequence(raw_value)
    if sequence is None:
        return ids
    for item in sequence:
        mapping = _as_mapping(item)
        if mapping is not None:
            media_id = _coerce_int(mapping.get("id"), 0)
        else:
            media_id = _coerce_int(getattr(cast(PreviewEntryLike, item), "id", None), 0)
        if media_id > 0:
            ids.append(media_id)
    return ids


def _read_item_field(item: Any, *field_names: str) -> Any:
    for field_name in field_names:
        value = getattr(item, field_name, None)
        if value is not None:
            return value
        raw = getattr(item, "raw", None)
        raw_mapping = _as_mapping(raw)
        if raw_mapping is not None:
            raw_value = raw_mapping.get(field_name)
            if raw_value is not None:
                return raw_value
    return None


def _is_mass_message_item(item: Any) -> bool:
    item_type = str(type(item).__name__)
    if "MassMessage" in item_type:
        return True

    raw_flag = _read_item_field(
        item, "isFromQueue", "is_from_queue", "isMassMessage", "is_mass_message"
    )
    explicit_flag = _coerce_bool(raw_flag)
    if explicit_flag is not None:
        return explicit_flag

    queue_id = _read_item_field(item, "queueId", "queue_id", "queueID")
    return queue_id is not None


def _collect_scraped_mass_messages(user: Any) -> list[Any]:
    scraped = getattr(getattr(user, "scrape_manager", None), "scraped", None)
    if scraped is None:
        return []

    mass_cached = getattr(scraped, "MassMessages", None)
    mass_cached_mapping = _as_mapping(mass_cached)
    if mass_cached_mapping is not None:
        candidates: list[object] = list(mass_cached_mapping.values())
    elif isinstance(mass_cached, list):
        candidates = list(cast(list[object], mass_cached))
    else:
        return []

    deduped: dict[int, Any] = {}
    passthrough: list[Any] = []
    for item in candidates:
        item_id = getattr(item, "id", None)
        if isinstance(item_id, int):
            deduped[item_id] = item
        elif item is not None:
            passthrough.append(item)
    return list(deduped.values()) + passthrough


def _collect_mass_messages_for_items(owner: Any, items: list[Any]) -> list[Any]:
    """Return mass-message parents referenced by the provided message items."""
    queue_ids = {
        queue_id
        for queue_id in (
            _coerce_int(_read_item_field(item, "queueId", "queue_id", "queueID"), 0)
            for item in items
            if _is_mass_message_item(item)
        )
        if queue_id > 0
    }
    if not queue_ids:
        return []

    direct = [item.mass_message for item in items if getattr(item, "mass_message", None) is not None]
    mass_items = direct or _collect_scraped_mass_messages(owner)
    filtered: list[Any] = []
    seen_ids: set[int] = set()
    for item in mass_items:
        item_id = _coerce_int(getattr(item, "id", None), 0)
        if item_id <= 0 or item_id not in queue_ids or item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        filtered.append(item)
    return filtered


async def _invoke_fetch(
    fetcher: Callable[..., Any],
    *,
    on_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    **kwargs: Any,
) -> list[Any]:
    call_kwargs: dict[str, Any] = {}
    try:
        sig = inspect.signature(fetcher)
        supports_kwargs = any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in sig.parameters.values()
        )
        for key, value in kwargs.items():
            if value is None:
                continue
            if key in sig.parameters or supports_kwargs:
                call_kwargs[key] = value
        if on_progress is not None and (
            "on_progress" in sig.parameters or supports_kwargs
        ):
            call_kwargs["on_progress"] = on_progress
    except (TypeError, ValueError):
        call_kwargs = {key: value for key, value in kwargs.items() if value is not None}
        if on_progress is not None:
            call_kwargs["on_progress"] = on_progress
    return await fetcher(**call_kwargs)


def _normalize_paid_content_types(raw_value: list[str] | None) -> set[ContentType]:
    if not raw_value:
        return {ContentType.POSTS, ContentType.MESSAGES}

    normalized: set[ContentType] = set()
    for value in raw_value:
        lowered = str(value).strip().lower()
        if lowered == "posts":
            normalized.add(ContentType.POSTS)
        elif lowered == "messages":
            normalized.add(ContentType.MESSAGES)
    return normalized or {ContentType.POSTS, ContentType.MESSAGES}


def _detect_paid_content_type(item: Any) -> ContentType | None:
    response_type = (
        str(_read_item_field(item, "responseType", "response_type") or "")
        .strip()
        .lower()
    )
    if response_type == "post":
        return ContentType.POSTS
    if response_type == "message":
        return ContentType.MESSAGES

    item_type = type(item).__name__.lower()
    if "post" in item_type:
        return ContentType.POSTS
    if "message" in item_type:
        return ContentType.MESSAGES
    return None


def _coerce_paid_content_performer(value: str | None) -> int | str | None:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    try:
        return int(trimmed)
    except ValueError:
        return trimmed


async def _iter_paid_batches(authed, *, performer_id, max_items, job_id, batch_size):
    iterator = getattr(authed, "iter_paid_content", None)
    if callable(iterator):
        batch = []
        async with aclosing(
            iterator(performer_id=performer_id, limit=max_items, cache=False)
        ) as items:
            async for item in items:
                batch.append(item)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch
        return
    fetcher = getattr(authed, "get_paid_contents", None)
    if not callable(fetcher):
        fetcher = getattr(authed, "get_paid_content", None)
    if not callable(fetcher):
        raise ValueError("Authenticated account does not support get_paid_content")
    items = await _invoke_fetch(
        fetcher, performer_id=performer_id, limit=max_items, job_id=job_id
    )
    for offset in range(0, len(items), batch_size):
        yield items[offset : offset + batch_size]


async def scrape_paid_content_feed(
    authed: Any,
    *,
    max_items: int | None,
    performer_filter: str | None,
    paid_content_types: list[str] | None,
    auth_user_id: int | None,
    worker_id: str,
    job_id: str | None,
    rescrape_existing: bool = True,
    on_scrape_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    on_save_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    database_writes: DatabaseWriteClient,
    site: str,
    media_selection: dict[str, Any] | None = None,
) -> tuple[int, int, int, list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    """Commit a bounded paid-feed batch and its media selection before reading more.

    Worker tasks retain only a SQL selection reference. The optional legacy list
    outputs are collected only for callers that explicitly omit that reference.
    """
    items_fetched = items_scraped = items_saved = error_count = 0
    errors = []
    media_urls = []
    download_targets = {}
    performer = _coerce_paid_content_performer(performer_filter)
    selected_types = _normalize_paid_content_types(paid_content_types)
    batch_size = positive_budget("ULTIMA_CONTENT_BATCH_SIZE", CONTENT_WRITE_BATCH_SIZE)
    preview = BoundedPreview(
        limit=positive_budget("ULTIMA_TASK_RESULT_PREVIEW_MAX_ENTRIES", batch_size),
        max_bytes=positive_budget(
            "ULTIMA_TASK_RESULT_PREVIEW_MAX_BYTES", page_byte_budget()
        ),
    )
    if rescrape_existing and not callable(getattr(authed, "iter_paid_content", None)):
        paid_cache = getattr(getattr(authed, "cache", None), "paid_content", None)
        if paid_cache is not None and hasattr(paid_cache, "deactivate"):
            paid_cache.deactivate()
        cached = getattr(authed, "paid_content", None)
        if isinstance(cached, list):
            cached.clear()

    async def record_errors(values, *, persisted=False):
        nonlocal error_count
        if media_selection is not None and values and not persisted:
            await _write_scrape_evidence(
                database_writes, media_selection, error_count, values
            )
        error_count += len(values)
        preview.extend(values)
        if media_selection is None:
            errors.extend(values)

    async with aclosing(
        _iter_paid_batches(
            authed,
            performer_id=performer,
            max_items=max_items,
            job_id=job_id,
            batch_size=batch_size,
        )
    ) as batches:
        async for batch in batches:
            items_fetched += len(batch)
            groups = {}
            for item in batch:
                content_type = _detect_paid_content_type(item)
                if content_type not in selected_types:
                    continue
                author = _call_author(item)
                author_id = getattr(author, "id", None)
                if performer is not None and performer not in {
                    author_id,
                    getattr(author, "username", None),
                    getattr(item, "user_id", None),
                    getattr(item, "username", None),
                }:
                    continue
                if not isinstance(author_id, int) or author_id <= 0:
                    await record_errors(
                        [
                            f"Skipped paid item {getattr(item, 'id', 'unknown')}: missing author"
                        ]
                    )
                    continue
                groups.setdefault((author_id, content_type), []).append(item)
                items_scraped += 1
            if on_scrape_progress:
                await on_scrape_progress(1, 1, items_scraped)
            for (author_id, content_type), items in groups.items():
                author = _call_author(items[0])
                if media_selection is None:
                    media_urls.extend(
                        _extract_media_urls(items, content_type, author, worker_id)
                    )
                    download_targets[author_id, content_type] = {
                        "user_id": author_id,
                        "username": getattr(author, "username", None) or str(author_id),
                        "content_type": content_type,
                    }
                if content_type == ContentType.MESSAGES:
                    mass_items = _collect_mass_messages_for_items(author, items)
                    if mass_items:
                        saved, batch_errors = await save_content_to_db(
                            mass_items,
                            author_id,
                            ContentType.MASS_MESSAGES,
                            worker_id,
                            auth_user_id=auth_user_id,
                            record_bought_content=True,
                            database_writes=database_writes,
                            site=site,
                        )
                        items_saved += saved
                        await record_errors(batch_errors)
                saved, batch_errors = await save_content_to_db(
                    items,
                    author_id,
                    content_type,
                    worker_id,
                    auth_user_id=auth_user_id,
                    record_bought_content=True,
                    database_writes=database_writes,
                    site=site,
                    media_selection=media_selection,
                    error_offset=error_count,
                )
                items_saved += saved
                await record_errors(batch_errors, persisted=media_selection is not None)
            if on_save_progress:
                await on_save_progress(
                    items_scraped, max_items or items_scraped, items_saved
                )
    if media_selection is not None:
        await database_writes.submit(
            DatabaseWriteOperation.UPDATE_MEDIA_SELECTION,
            {"action": "seal", "spec": media_selection},
            site=site,
            command_id=stable_command_id(
                "media-selection-seal-v1", media_selection["selection_id"]
            ),
        )
        media_selection["error_count"] = error_count
        errors = preview.values
    return (
        items_fetched,
        items_scraped,
        items_saved,
        errors,
        media_urls,
        list(download_targets.values()),
    )


async def save_content_to_db(
    content_items: list[content_types],  # type: ignore[name-defined]
    user_id: int,
    content_type: ContentType | str,
    worker_id: str = "unknown",
    auth_user_id: int | None = None,
    record_bought_content: bool = False,
    on_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    *,
    database_writes: DatabaseWriteClient,
    site: str,
    media_selection: dict[str, Any] | None = None,
    error_offset: int = 0,
) -> tuple[int, list[str]]:
    """
    Save scraped content items to database.

    Args:
        content_items: List of content items (posts, messages, etc.)
        user_id: User ID the content belongs to
        content_type: Type of content being saved (for logging)
        worker_id: Worker identifier for logging

    Returns:
        Tuple of (items_saved_count, error_messages)
    """
    items_saved = 0
    errors: list[str] = []
    total_items = len(content_items)
    normalized_site = site.strip().lower()
    batch_size = positive_budget("ULTIMA_CONTENT_BATCH_SIZE", CONTENT_WRITE_BATCH_SIZE)
    for offset in range(0, total_items, batch_size):
        batch = content_items[offset : offset + batch_size]
        try:
            serialized = [
                serialize_content_write_item(
                    item,
                    content_type=content_type,
                    user_id=user_id,
                    auth_user_id=auth_user_id,
                    record_bought_content=record_bought_content,
                )
                for item in batch
            ]
            payload = {
                "user_id": int(user_id),
                "auth_user_id": auth_user_id,
                "record_bought_content": bool(record_bought_content),
                "items": serialized,
            }
            if media_selection is not None:
                payload.update(media_selection=media_selection,
                    selection_content_type=_content_type_name(content_type),
                    error_offset=error_offset + len(errors))
            command_id = stable_command_id(
                "content-v1",
                normalized_site,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
            )
            result = await database_writes.submit(
                DatabaseWriteOperation.UPSERT_CONTENT_BATCH,
                payload,
                site=normalized_site,
                command_id=command_id,
            )
        except DatabaseWriteError:
            raise
        except Exception as exc:
            raise DatabaseWriteError(
                f"Could not queue {len(batch)} {content_type} item(s) for {normalized_site}"
            ) from exc
        items_saved += int(result.get("items_saved") or 0)
        errors.extend(str(error) for error in result.get("errors") or [])
        if on_progress:
            await on_progress(
                min(offset + len(batch), total_items),
                total_items,
                items_saved,
            )

    return items_saved, errors


async def scrape_content_type(
    user: user_types,  # type: ignore[name-defined]
    content_type: ContentType,
    max_items: int | None,
    auth_name: str = "unknown",
    worker_id: str = "unknown",
    job_id: str | None = None,
    auth_user_id: int | None = None,
    on_scrape_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    on_save_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    *,
    database_writes: DatabaseWriteClient,
    site: str,
    media_selection: dict[str, Any] | None = None,
) -> tuple[int, int, list[str], list[dict[str, Any]]]:
    """Persist a bounded working set before accepting the next API items.

    Production tasks use a SQL media selection. The list output remains a
    compatibility contract for callers that explicitly use this helper alone.
    """
    items_scraped = items_saved = error_count = 0
    errors: list[str] = []
    media_urls: list[dict[str, Any]] = []
    batch_size = positive_budget("ULTIMA_CONTENT_BATCH_SIZE", CONTENT_WRITE_BATCH_SIZE)
    preview = BoundedPreview(limit=positive_budget("ULTIMA_TASK_RESULT_PREVIEW_MAX_ENTRIES", batch_size),
        max_bytes=positive_budget("ULTIMA_TASK_RESULT_PREVIEW_MAX_BYTES", page_byte_budget()))
    try:
        async with aclosing(_iter_content_batches(user, content_type, max_items,
            job_id, worker_id, batch_size=batch_size, on_progress=on_scrape_progress)) as pages:
            async for content_items in pages:
                items_scraped += len(content_items)
                if media_selection is None:
                    media_urls.extend(_extract_media_urls(content_items, content_type, user, worker_id))
                if content_type == ContentType.MESSAGES:
                    mass_items = _collect_mass_messages_for_items(user, content_items)
                    if mass_items:
                        mass_saved, mass_errors = await save_content_to_db(
                            mass_items, int(user.id), ContentType.MASS_MESSAGES, worker_id,
                            auth_user_id=auth_user_id, database_writes=database_writes, site=site)
                        items_saved += mass_saved
                        if media_selection is not None and mass_errors:
                            await _write_scrape_evidence(database_writes, media_selection, error_count, mass_errors)
                        error_count += len(mass_errors)
                        preview.extend(mass_errors)
                        if media_selection is None:
                            errors.extend(mass_errors)
                saved, batch_errors = await save_content_to_db(
                    content_items, int(user.id), content_type, worker_id, auth_user_id=auth_user_id,
                    database_writes=database_writes, site=site, media_selection=media_selection,
                    error_offset=error_count)
                items_saved += saved
                error_count += len(batch_errors)
                preview.extend(batch_errors)
                if media_selection is None:
                    errors.extend(batch_errors)
                if on_save_progress:
                    await on_save_progress(items_scraped, max_items or items_scraped, items_saved)
                _release_scrape_working_set(user, content_type)
    except DatabaseWriteError:
        raise
    except Exception as exc:
        error = f"Failed to scrape {content_type} (auth: {auth_name}): {exc}"
        logger.exception("[%s] %s", worker_id, error)
        if media_selection is not None:
            await _write_scrape_evidence(database_writes, media_selection, error_count, [error])
        preview.extend([error])
        errors.append(error)
        error_count += 1
    finally:
        _release_scrape_working_set(user, content_type)
    if media_selection is not None:
        await database_writes.submit(DatabaseWriteOperation.UPDATE_MEDIA_SELECTION,
            {"action": "seal", "spec": media_selection}, site=site,
            command_id=stable_command_id("media-selection-seal-v1", media_selection["selection_id"]))
        media_selection["error_count"] = error_count
        errors = preview.values
    return items_scraped, items_saved, errors, media_urls


async def _write_scrape_evidence(writer, selection, offset, errors):
    payload = {"action": "evidence", "spec": selection,
        "categories": [{"kind": "errors", "offset": offset, "values": errors}]}
    await writer.submit(DatabaseWriteOperation.UPDATE_MEDIA_SELECTION, payload,
        site=selection["site"], command_id=stable_command_id("scrape-errors-v1",
            selection["selection_id"], json.dumps(payload, sort_keys=True)))


async def _iter_content_batches(
    user, content_type, max_items, job_id, worker_id, *, batch_size, on_progress=None
) -> AsyncGenerator[list[Any], None]:
    method = {
        ContentType.POSTS: "iter_posts",
        ContentType.ARCHIVED_POSTS: "iter_posts",
        ContentType.MESSAGES: "iter_messages",
        ContentType.STORIES: "iter_stories",
        ContentType.HIGHLIGHTS: "iter_highlight_stories",
    }.get(content_type)
    iterator = getattr(user, method, None) if method else None
    if callable(iterator):
        kwargs = {"on_progress": on_progress, "job_id": job_id}
        if content_type in (
            ContentType.MESSAGES,
            ContentType.STORIES,
            ContentType.HIGHLIGHTS,
        ):
            kwargs["max_items"] = max_items
        else:
            kwargs["limit"] = max_items
        if content_type == ContentType.ARCHIVED_POSTS:
            kwargs["label"] = "archived"
        batch = []
        async with aclosing(iterator(**kwargs)) as items:
            async for item in items:
                batch.append(item)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch
        return
    items = await _fetch_content_items(
        user, content_type, max_items, job_id, worker_id, on_progress=on_progress
    )
    if items is None:
        raise ValueError(f"Unsupported content type: {content_type}")
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


async def _fetch_content_items(
    user: user_types,  # type: ignore[name-defined]
    content_type: ContentType,
    max_items: int | None,
    job_id: str | None,
    worker_id: str,
    on_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
) -> list[Any] | None:
    """Fetch content items from the API based on content type.

    Args:
        user: User object to scrape from
        content_type: Type of content to fetch
        max_items: Maximum number of items (None for unlimited)
        job_id: Job ID for progress tracking
        worker_id: Worker identifier for logging

    Returns:
        List of content items, or None if content type is unsupported
    """

    if content_type == ContentType.POSTS:
        fetcher = _get_fetcher(user, "get_posts")
        if fetcher is None:
            return []
        return (
            await _invoke_fetch(fetcher, on_progress=on_progress, job_id=job_id)
            if max_items is None
            else await _invoke_fetch(
                fetcher,
                on_progress=on_progress,
                limit=max_items,
                job_id=job_id,
            )
        )

    elif content_type == ContentType.MESSAGES:
        fetcher = _get_fetcher(user, "get_messages")
        if fetcher is None:
            return []
        return (
            await _invoke_fetch(fetcher, on_progress=on_progress, job_id=job_id)
            if max_items is None
            else await _invoke_fetch(
                fetcher,
                on_progress=on_progress,
                limit=max_items,
                job_id=job_id,
            )
        )

    elif content_type == ContentType.STORIES:
        fetcher = _get_fetcher(user, "get_stories")
        if fetcher is None:
            logger.warning(
                f"[{worker_id}] [scrape_handler] Stories not supported for this API"
            )
            return []
        return (
            await _invoke_fetch(fetcher, on_progress=on_progress, job_id=job_id)
            if max_items is None
            else await _invoke_fetch(
                fetcher,
                on_progress=on_progress,
                limit=max_items,
                job_id=job_id,
            )
        )

    elif content_type == ContentType.ARCHIVED_POSTS:
        fetcher = _get_fetcher(user, "get_posts")
        if fetcher is None:
            logger.warning(
                f"[{worker_id}] [scrape_handler] Archived posts not supported for this API"
            )
            return []
        return (
            await _invoke_fetch(
                fetcher,
                on_progress=on_progress,
                label="archived",
                job_id=job_id,
            )
            if max_items is None
            else await _invoke_fetch(
                fetcher,
                on_progress=on_progress,
                label="archived",
                limit=max_items,
                job_id=job_id,
            )
        )

    elif content_type == ContentType.HIGHLIGHTS:
        fetcher = _get_fetcher(user, "get_highlights")
        if fetcher is None:
            logger.warning(
                f"[{worker_id}] [scrape_handler] Highlights not supported for this API"
            )
            return []
        # get_highlights() returns HighlightModel list (albums)
        # We need to fetch stories from each highlight
        highlights = await _invoke_fetch(fetcher)
        content_items: list[Any] = []
        for highlight in highlights:
            # Fetch the stories inside this highlight
            highlight_id = str(_coerce_int(getattr(highlight, "id", None), 0))
            highlight_stories = await _invoke_fetch(
                fetcher,
                hightlight_id=highlight_id,
            )
            content_items.extend(highlight_stories)
        logger.info(
            f"[{worker_id}] [scrape_handler] Fetched {len(content_items)} stories from {len(highlights)} highlights"
        )
        return content_items

    elif content_type == ContentType.MASS_MESSAGES:
        # Preferred path: direct mass-message endpoint on the user model.
        for getter_name in (
            "get_mass_messages",
            "get_massmessages",
            "get_mass_message",
        ):
            fetcher = getattr(user, getter_name, None)
            if not callable(fetcher):
                continue
            return (
                await _invoke_fetch(fetcher, on_progress=on_progress, job_id=job_id)
                if max_items is None
                else await _invoke_fetch(
                    fetcher,
                    on_progress=on_progress,
                    limit=max_items,
                    job_id=job_id,
                )
            )

        # Fallback path: use scraper cache if upstream populated MassMessages.
        scraped = getattr(getattr(user, "scrape_manager", None), "scraped", None)
        mass_cached = (
            getattr(scraped, "MassMessages", None) if scraped is not None else None
        )
        mass_cached_mapping = _as_mapping(mass_cached)
        if mass_cached_mapping is not None:
            return [value for value in mass_cached_mapping.values()]
        if isinstance(mass_cached, list):
            return list(cast(list[Any], mass_cached))

        # Last resort: derive mass messages from paid content feed.
        authed = getattr(user, "get_authed", lambda: None)()
        paid_fetcher = getattr(authed, "get_paid_contents", None) if authed else None
        if callable(paid_fetcher):
            paid_items = (
                await _invoke_fetch(
                    paid_fetcher,
                    on_progress=on_progress,
                    limit=max_items,
                    job_id=job_id,
                )
                if max_items is not None
                else await _invoke_fetch(
                    paid_fetcher,
                    on_progress=on_progress,
                    job_id=job_id,
                )
            )
            return [item for item in paid_items if _is_mass_message_item(item)]
        return []

    return None


def _extract_media_urls(
    content_items: list[Any],
    content_type: ContentType,
    user: object,
    worker_id: str,
) -> list[dict[str, Any]]:
    """Extract media URLs from content items.

    Args:
        content_items: List of content items
        content_type: Type of content
        user: User object for context
        worker_id: Worker identifier for logging

    Returns:
        List of media entry dictionaries
    """
    media_urls: list[dict[str, Any]] = []
    items_with_media = 0

    for item in content_items:
        item_id = getattr(item, "id", "unknown")
        # Convert item_id to int if possible
        try:
            item_id_int = int(item_id) if item_id != "unknown" else 0
        except (ValueError, TypeError):
            item_id_int = 0

        # Check for both .media and .medias attributes (different APIs use different names)
        media_list = getattr(item, "media", []) or getattr(item, "medias", [])
        if not media_list:
            continue

        items_with_media += 1
        logger.debug(
            f"Processing {len(media_list)} media items for content_id={item_id}"
        )

        # Extract preview media IDs
        preview_media_ids = _extract_preview_media_ids(item)

        # Get created_at from item
        item_created_at = _parse_created_at(getattr(item, "created_at", None))

        # Process each media item
        for media in media_list:
            try:
                media_entry = _convert_media_to_entry(
                    media,
                    item,
                    content_type,
                    user,
                    item_id_int,
                    item_created_at,
                    preview_media_ids,
                    worker_id,
                )
                if media_entry:
                    media_urls.append(media_entry)
            except Exception as exc:
                logger.error(
                    f"[{worker_id}] ❌ Failed to convert media to metadata for item {item_id}: {exc}",
                    exc_info=True,
                )
                raise

    logger.info(
        f"[{worker_id}] [scrape_handler] Found {items_with_media}/{len(content_items)} {content_type} with media, "
        f"extracted {len(media_urls)} media URLs"
    )

    return media_urls


def _extract_preview_media_ids(item: Any) -> list[int]:
    """Extract preview media IDs from a content item.

    Args:
        item: Content item

    Returns:
        List of preview media IDs
    """
    preview_media_ids: list[int] = []

    get_previews = getattr(item, "get_previews", None)
    if callable(get_previews):
        try:
            preview_medias = get_previews()
            preview_media_ids = _coerce_preview_ids(preview_medias)
            logger.debug(
                f"Content has {len(preview_media_ids)} preview media IDs from get_previews(): {preview_media_ids}"
            )
        except Exception as e:
            logger.warning(f"Failed to get previews via get_previews(): {e}")
            # Fall back to direct attribute access
            preview_media_ids = _coerce_preview_ids(
                getattr(item, "previews", None) or getattr(item, "preview", [])
            )
    else:
        preview_media_ids = _coerce_preview_ids(
            getattr(item, "previews", None) or getattr(item, "preview", [])
        )
        if preview_media_ids:
            logger.debug(
                f"Content has {len(preview_media_ids)} preview media IDs from attribute: {preview_media_ids}"
            )

    return preview_media_ids


def _parse_created_at(value: Any) -> datetime | None:
    """Parse created_at value to datetime.

    Args:
        value: String, int, float, or datetime

    Returns:
        Parsed datetime or None
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    return None


def _convert_media_to_entry(
    media: object,
    item: object,
    content_type: ContentType,
    user: object,
    item_id_int: int,
    item_created_at: datetime | None,
    preview_media_ids: list[int],
    worker_id: str,
) -> dict[str, Any] | None:
    """Convert a single media item to a media entry dictionary.

    Args:
        media: Media item from API
        item: Parent content item
        content_type: Type of content
        user: User object
        item_id_int: Content item ID
        item_created_at: Content creation timestamp
        preview_media_ids: List of preview media IDs
        worker_id: Worker identifier

    Returns:
        Media entry dictionary or None if conversion fails
    """
    media_mapping = _as_mapping(media)
    media_id_for_log = (
        media_mapping.get("id")
        if media_mapping is not None
        else getattr(media, "id", None)
    )
    logger.debug(
        f"Converting media to MediaMetadata: type={type(media)}, "
        f"media_id={media_id_for_log}"
    )

    # Create a minimal ContentMetadata for the media conversion
    user_id = _coerce_int(getattr(user, "id", None), 0)
    item_price = float(getattr(item, "price", 0) or 0)
    content_type_name = _content_type_name(content_type)
    content_is_paid = _determine_paid_status(item, item_price)
    content_metadata = ContentMetadata(
        content_id=item_id_int,
        api_type=content_type_name,
        user_id=user_id,
        created_at=item_created_at or datetime.now(),
        text=getattr(item, "text", None) or "",
        price=item_price,
        paid=content_is_paid,
        medias=[],
    )

    # Convert media to MediaMetadata
    media_metadata = MediaMetadata.from_api_media(
        media_item=media,
        content_id=content_metadata.content_id,
        user_id=user_id,
        api_type=content_type_name,
        created_at=item_created_at,
        preview_media_ids=preview_media_ids,
    )

    media_metadata.urls = _collect_media_urls(item, media, media_metadata.urls)

    # Get price from content item
    item_price = getattr(item, "price", 0) or 0

    # Calculate value based on price and preview status
    # Preview media within paid content stays in "Previews";
    # only non-preview media goes to "Paid".
    value = "Free"
    if item_price > 0:
        if content_is_paid and not media_metadata.preview:
            value = "Paid"
        else:
            value = "Previews"

    # Only add if we successfully extracted URLs
    if not media_metadata.urls:
        has_files = (
            "files" in media_mapping
            if media_mapping is not None
            else hasattr(media, "files")
        )
        has_locations = (
            "locations" in media_mapping
            if media_mapping is not None
            else hasattr(media, "locations")
        )
        has_source = (
            "source" in media_mapping
            if media_mapping is not None
            else hasattr(media, "source")
        )
        has_url = (
            "url" in media_mapping
            if media_mapping is not None
            else hasattr(media, "url")
        )
        logger.debug(
            f"Raw media data: type={type(media)}, "
            f"has_files={has_files}, "
            f"has_locations={has_locations}, "
            f"has_source={has_source}, "
            f"has_url={has_url}"
        )
        return None

    # Serialize metadata to dict
    media_entry = {
        "url": media_metadata.urls[0],
        "urls": media_metadata.urls,
        "filename": media_metadata.extract_filename_from_url(),
        "content_id": str(content_metadata.content_id),
        "media_id": str(media_metadata.media_id),
        "media_type": media_metadata.media_type,
        "size": media_metadata.size,
        "created_at": media_metadata.created_at.isoformat(),
        "drm": media_metadata.drm,
        "drm_key": media_metadata.drm_key,
        "preview": media_metadata.preview,
        "price": item_price,
        "value": value,
    }
    logger.debug(
        f"Serialized media_entry keys: {list(media_entry.keys())}, value={value}"
    )

    return media_entry
