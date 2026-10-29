"""Media database routes for USDBM."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import mimetypes
import os
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Iterable, cast
from urllib.parse import urlparse

import aiohttp
from aiohttp_socks import ProxyConnector
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.orm import selectinload
from sqlmodel import select
from ultima_scraper_api.apis.onlyfans.classes.only_drm import DRMMedia
from ultima_scraper_api.apis.onlyfans.urls import APIRoutes
from ultima_scraper_api.helpers.url_diagnostics import (
    decode_cloudfront_policy,
    diagnose_url,
)
from ultima_scraper_db_archive import (
    ContentMediaAssoModel,
    ContentMentionModel,
    FilePathModel,
    MassMessageModel,
    MediaModel,
    MessageModel,
    PostModel,
    UserModel,
)
from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates.postgres_site import (
    MediaUrlModel,
)

from ...config import UltimaScraperCollectionConfig
from ...database.factory import MultiSiteDatabaseManager
from ...manager import AccountRequestContext, BackendManager
from ...models.backend import BackendInstance
from ...security import validate_outbound_url
from ..dependencies import get_backend_instance, get_backend_manager, get_config
from .files import (
    _candidate_content_values,
    _candidate_filenames,
    _resolve_all_content_info,
    _resolve_media_path,
    find_performer_media_file,
)

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


_THUMBNAIL_GENERATION_LIMIT = max(1, _env_int("USR_THUMBNAIL_GENERATION_LIMIT", 3))
_MEDIA_RENAME_SEARCH_TIMEOUT_SECONDS = max(
    1, _env_int("USR_MEDIA_RENAME_SEARCH_TIMEOUT_SECONDS", 5)
)
_REMOTE_THUMBNAIL_PROBE_TIMEOUT_SECONDS = max(
    0.5, _env_float("USR_REMOTE_THUMBNAIL_PROBE_TIMEOUT_SECONDS", 3.0)
)
_REMOTE_THUMBNAIL_RESOLUTION_LIMIT = max(
    1, _env_int("USR_REMOTE_THUMBNAIL_RESOLUTION_LIMIT", 3)
)
_REMOTE_THUMBNAIL_FAILURE_TTL_SECONDS = max(
    1, _env_int("USR_REMOTE_THUMBNAIL_FAILURE_TTL_SECONDS", 60)
)
_REMOTE_THUMBNAIL_JOB_LIMIT = max(1, _env_int("USR_REMOTE_THUMBNAIL_JOB_LIMIT", 100))
_REMOTE_THUMBNAIL_FAILURE_CACHE_MAX_ENTRIES = max(
    0, _env_int("USR_REMOTE_THUMBNAIL_FAILURE_CACHE_MAX_ENTRIES", 1000)
)
_thumbnail_generation_slots = asyncio.Semaphore(_THUMBNAIL_GENERATION_LIMIT)
_remote_thumbnail_resolution_slots = asyncio.Semaphore(
    _REMOTE_THUMBNAIL_RESOLUTION_LIMIT
)
_remote_thumbnail_jobs: dict[str, asyncio.Task[None]] = {}
_remote_thumbnail_failures: OrderedDict[str, float] = OrderedDict()


def _prune_remote_thumbnail_failures(now: float) -> None:
    """Expire and cap negative thumbnail lookups."""
    if not _REMOTE_THUMBNAIL_FAILURE_CACHE_MAX_ENTRIES:
        _remote_thumbnail_failures.clear()
        return

    expired = [
        key
        for key, expires_at in _remote_thumbnail_failures.items()
        if expires_at <= now
    ]
    for key in expired:
        _remote_thumbnail_failures.pop(key, None)
    while len(_remote_thumbnail_failures) > _REMOTE_THUMBNAIL_FAILURE_CACHE_MAX_ENTRIES:
        _remote_thumbnail_failures.popitem(last=False)


def _remember_remote_thumbnail_failure(job_key: str) -> None:
    now = time.monotonic()
    _prune_remote_thumbnail_failures(now)
    if not _REMOTE_THUMBNAIL_FAILURE_CACHE_MAX_ENTRIES:
        return
    _remote_thumbnail_failures.pop(job_key, None)
    _remote_thumbnail_failures[job_key] = now + _REMOTE_THUMBNAIL_FAILURE_TTL_SECONDS
    _prune_remote_thumbnail_failures(now)


_ASSOCIATED_CONTENT_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60
_ASSOCIATED_CONTENT_CACHE_PREFIX = "ultimascraper:associated-content:v2"

router = APIRouter(
    prefix="/media", tags=["media"], responses={404: {"description": "Not found"}}
)


class MediaIDs(BaseModel):
    media_ids: list[int]


class ContentIDs(BaseModel):
    content_ids: list[int]


class MediaResponse(BaseModel):
    id: int
    user_id: int
    url: str | None = None
    size: int = 0
    drm: bool = False
    category: str | None = None
    preview: bool = False
    downloaded: bool = False
    created_at: str | None = None


class AssociatedContentResponse(BaseModel):
    id: int
    user_id: int
    text: str | None = None


class AssociatedMentionResponse(BaseModel):
    content_id: int
    content_type: str
    mentioned_user_id: int | None = None
    raw_mention: str | None = None
    normalized_mention: str
    mention_kind: str = "platform_tag"


class FilenameLookupResponse(BaseModel):
    media: list[MediaResponse]
    posts: list[AssociatedContentResponse] = []
    messages: list[AssociatedContentResponse] = []
    mass_messages: list[AssociatedContentResponse] = []
    mentions: list[AssociatedMentionResponse] = []


def _associated_content_cache_key(
    site_name: str, lookup_kind: str, lookup_value: str | int
) -> str:
    """Build a Redis-safe key without exposing filenames or local paths."""
    if lookup_kind == "media":
        return f"{_ASSOCIATED_CONTENT_CACHE_PREFIX}:{site_name}:media:{lookup_value}"
    digest = hashlib.sha256(str(lookup_value).encode()).hexdigest()
    return f"{_ASSOCIATED_CONTENT_CACHE_PREFIX}:{site_name}:{lookup_kind}:{digest}"


async def _get_cached_associated_content(
    backend_instance: BackendInstance, cache_key: str
) -> FilenameLookupResponse | None:
    """Return a cached response, treating Redis as an optional optimization."""
    redis = backend_instance.redis
    if redis is None:
        return None
    try:
        raw = await redis.get(cache_key)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return FilenameLookupResponse.model_validate(json.loads(raw))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Ignoring invalid associated-content cache entry %s: %s", cache_key, exc
        )
    except Exception as exc:
        logger.debug("Associated-content cache read failed: %s", exc)
    return None


async def _cache_associated_content(
    backend_instance: BackendInstance,
    cache_key: str,
    response: FilenameLookupResponse,
) -> None:
    """Best-effort cache write; archive lookups remain available without Redis."""
    redis = backend_instance.redis
    if redis is None:
        return
    try:
        await redis.set(
            cache_key,
            response.model_dump_json(),
            ex=_ASSOCIATED_CONTENT_CACHE_TTL_SECONDS,
        )
    except Exception as exc:
        logger.debug("Associated-content cache write failed: %s", exc)


def _serialize_media(media: MediaModel) -> MediaResponse:
    created_at = getattr(media, "created_at", None)
    return MediaResponse(
        id=media.id,
        user_id=media.user_id,
        url=media.url,
        size=media.size or 0,
        drm=bool(getattr(media, "drm", False)),
        category=getattr(media, "category", None),
        preview=bool(getattr(media, "preview", False)),
        downloaded=bool(getattr(media, "downloaded", False)),
        created_at=created_at.isoformat() if created_at else None,
    )


def _normalized_site(site_name: str) -> str:
    site = site_name.strip().lower()
    if not site:
        raise HTTPException(status_code=400, detail="Site name is required")
    return site


def _associated_content_for_media_ids(
    session: Any, media_ids: set[int]
) -> FilenameLookupResponse:
    media = session.exec(
        select(MediaModel).where(MediaModel.id.in_(sorted(media_ids)))
    ).all()
    associations = session.exec(
        select(ContentMediaAssoModel).where(
            ContentMediaAssoModel.media_id.in_(sorted(media_ids))
        )
    ).all()

    post_ids = {
        association.post_id for association in associations if association.post_id
    }
    message_ids = {
        association.message_id for association in associations if association.message_id
    }
    mass_message_ids = {
        association.mass_message_id
        for association in associations
        if association.mass_message_id
    }

    content_ids_by_type = {
        "post": post_ids,
        "message": message_ids,
        "mass_message": mass_message_ids,
    }
    content_type_by_id = {
        (content_type, int(content_id))
        for content_type, ids in content_ids_by_type.items()
        for content_id in ids
    }
    mention_conditions = []
    if post_ids:
        mention_conditions.append(ContentMentionModel.post_id.in_(post_ids))
    if message_ids:
        mention_conditions.append(ContentMentionModel.message_id.in_(message_ids))
    if mass_message_ids:
        mention_conditions.append(
            ContentMentionModel.mass_message_id.in_(mass_message_ids)
        )
    mention_rows = (
        session.exec(select(ContentMentionModel).where(or_(*mention_conditions))).all()
        if mention_conditions
        else []
    )

    posts = (
        session.exec(select(PostModel).where(PostModel.id.in_(post_ids))).all()
        if post_ids
        else []
    )
    messages = (
        session.exec(select(MessageModel).where(MessageModel.id.in_(message_ids))).all()
        if message_ids
        else []
    )
    mass_messages = (
        session.exec(
            select(MassMessageModel).where(MassMessageModel.id.in_(mass_message_ids))
        ).all()
        if mass_message_ids
        else []
    )

    return FilenameLookupResponse(
        media=[_serialize_media(item) for item in media],
        posts=[
            AssociatedContentResponse(id=item.id, user_id=item.user_id, text=item.text)
            for item in posts
        ],
        messages=[
            AssociatedContentResponse(id=item.id, user_id=item.user_id, text=item.text)
            for item in messages
        ],
        mass_messages=[
            AssociatedContentResponse(id=item.id, user_id=item.user_id, text=item.text)
            for item in mass_messages
        ],
        mentions=[
            AssociatedMentionResponse(
                content_id=int(content_id),
                content_type=content_type,
                mentioned_user_id=(
                    int(item.mentioned_user_id)
                    if item.mentioned_user_id is not None
                    else None
                ),
                raw_mention=item.raw_mention,
                normalized_mention=item.normalized_mention,
                mention_kind=item.mention_kind,
            )
            for item in mention_rows
            for content_type, content_id in (
                [("post", item.post_id)]
                if item.post_id is not None
                else (
                    [("message", item.message_id)]
                    if item.message_id is not None
                    else [("mass_message", item.mass_message_id)]
                )
            )
            if (content_type, int(content_id)) in content_type_by_id
        ],
    )


def _content_field(content_type: str) -> str:
    content = content_type.strip().lower()
    mapping = {
        "stories": "story_id",
        "story": "story_id",
        "posts": "post_id",
        "post": "post_id",
        "messages": "message_id",
        "message": "message_id",
        "mass_messages": "mass_message_id",
        "mass_message": "mass_message_id",
        "mass-messages": "mass_message_id",
    }
    field = mapping.get(content)
    if not field:
        raise HTTPException(status_code=400, detail="Unsupported content type")
    return field


def _validate_ids(values: Iterable[int], label: str, max_size: int = 500) -> list[int]:
    ids = list(values)
    if not ids:
        raise HTTPException(status_code=400, detail=f"{label} cannot be empty")
    if len(ids) > max_size:
        raise HTTPException(
            status_code=400,
            detail=f"{label} exceeds maximum of {max_size} items",
        )
    return ids


@dataclass(frozen=True)
class _RemoteStreamCandidate:
    url: str
    auth_id: int | None = None
    custom_cookies: str = ""


@dataclass(frozen=True)
class _RemoteRequestContext:
    cookie_header: str = ""
    authorization: str = ""
    user_agent: str = ""
    proxy_url: str | None = None


@dataclass(frozen=True)
class _MediaResolutionContext:
    """Detached media data needed after the database session is released."""

    media: Any
    username: str | None
    aliases: tuple[str, ...]
    usernames: tuple[str, ...]
    content_entries: tuple[tuple[str, int | None], ...]
    primary_content: tuple[str | None, int | None]


def _coerce_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _is_expired_datetime(value: object) -> bool:
    if not isinstance(value, datetime):
        return False
    if value.tzinfo is None:
        return value < datetime.utcnow()
    return value < datetime.now(value.tzinfo)


def _is_url_usable(url: object) -> bool:
    if not isinstance(url, str) or not url.strip():
        return False

    diagnostics = diagnose_url(url)
    if diagnostics.error:
        logger.debug(
            "URL diagnostics failed for media stream candidate: %s", diagnostics.error
        )
        return True

    return not bool(diagnostics.policy and diagnostics.policy.is_expired)


def _is_media_url_record_usable(record: object) -> bool:
    url = getattr(record, "url", None)
    if not _is_url_usable(url):
        return False

    is_expired = getattr(record, "is_expired", None)
    if callable(is_expired):
        try:
            if bool(is_expired()):
                return False
        except Exception:
            logger.debug("Media URL expiry check raised unexpectedly", exc_info=True)

    return not _is_expired_datetime(getattr(record, "expires_at", None))


def _preferred_media_auth_id(media: object) -> int | None:
    for record in getattr(media, "media_urls", []) or []:
        auth_id = _coerce_int(getattr(record, "auth_id", None))
        if auth_id is not None:
            return auth_id
    return None


def _iter_remote_stream_candidates(media: object) -> list[_RemoteStreamCandidate]:
    candidates: list[_RemoteStreamCandidate] = []
    seen_urls: set[str] = set()
    preferred_auth_id = _preferred_media_auth_id(media)

    def _append_candidate(candidate: _RemoteStreamCandidate | None) -> None:
        if candidate is None:
            return
        normalized_url = candidate.url.strip()
        if normalized_url in seen_urls:
            return
        seen_urls.add(normalized_url)
        candidates.append(
            _RemoteStreamCandidate(
                url=normalized_url,
                auth_id=candidate.auth_id,
                custom_cookies=candidate.custom_cookies,
            )
        )

    media_url = getattr(media, "url", None)
    if _is_url_usable(media_url):
        _append_candidate(
            _RemoteStreamCandidate(
                url=str(media_url).strip(),
                auth_id=preferred_auth_id,
            )
        )

    for record in getattr(media, "media_urls", []) or []:
        if not _is_media_url_record_usable(record):
            continue
        record_url = getattr(record, "url", None)
        if not isinstance(record_url, str):
            continue
        _append_candidate(
            _RemoteStreamCandidate(
                url=record_url,
                auth_id=_coerce_int(getattr(record, "auth_id", None)),
            )
        )

    return candidates


def _primary_content_info(session: Any, media_id: int) -> tuple[str | None, int | None]:
    for content_type, content_id in _resolve_all_content_info(session, media_id):
        if content_id is not None:
            return content_type, content_id
    return None, None


def _is_drm_policy_usable(policy: object) -> bool:
    if not isinstance(policy, str) or not policy.strip():
        return False
    try:
        return not decode_cloudfront_policy(policy).is_expired
    except Exception:
        logger.debug("Failed to decode DRM policy", exc_info=True)
        return True


def _select_drm_stream_candidate(
    drm_media: DRMMedia,
    media_category: object,
) -> _RemoteStreamCandidate | None:
    if not _is_drm_policy_usable(drm_media.policy):
        return None

    normalized_category = str(media_category or "").strip().lower()
    preferred_urls = (
        [drm_media.audio_url, drm_media.video_url]
        if normalized_category in {"audio", "audios"}
        else [drm_media.video_url, drm_media.audio_url]
    )
    for candidate_url in preferred_urls:
        if _is_url_usable(candidate_url):
            return _RemoteStreamCandidate(
                url=str(candidate_url).strip(),
                custom_cookies=drm_media.get_cookies(),
            )
    return None


async def _resolve_drm_stream_candidate(
    session: Any | None,
    media: MediaModel,
    site: str,
    backend_manager: BackendManager,
    *,
    primary_content: tuple[str | None, int | None] | None = None,
    allow_api_login: bool = True,
) -> _RemoteStreamCandidate | None:
    drm_manifest = getattr(media, "drm_manifest", None)
    if drm_manifest is None:
        return None

    dash_manifest = getattr(drm_manifest, "dash_manifest", None)
    dash_key_pair_id = getattr(drm_manifest, "dash_key_pair_id", None)
    dash_policy = getattr(drm_manifest, "dash_policy", None)
    dash_signature = getattr(drm_manifest, "dash_signature", None)
    if not all((dash_manifest, dash_key_pair_id, dash_policy, dash_signature)):
        return None

    content_type, content_id = (
        primary_content
        if primary_content is not None
        else _primary_content_info(session, media.id)
    )
    if content_id is None:
        return None

    drm_media = DRMMedia(
        media_id=media.id,
        manifest_url=str(dash_manifest),
        key_pair_id=str(dash_key_pair_id),
        policy=str(dash_policy),
        signature=str(dash_signature),
        content_id=content_id,
        video_url=getattr(drm_manifest, "video_url", None),
        audio_url=getattr(drm_manifest, "audio_url", None),
        video_size=getattr(drm_manifest, "video_size", None),
        audio_size=getattr(drm_manifest, "audio_size", None),
        content_type=content_type,
    )

    candidate = _select_drm_stream_candidate(
        drm_media, getattr(media, "category", None)
    )
    if candidate is not None:
        return candidate

    if not allow_api_login:
        return None

    if not _is_drm_policy_usable(drm_media.policy):
        return None

    auth_id = _preferred_media_auth_id(media)
    auth = await backend_manager.get_authed_session(site, auth_id)
    only_drm = getattr(auth, "drm", None) if auth is not None else None
    if only_drm is None:
        return None

    try:
        video_url, audio_url, _ = await only_drm.resolve_drm(drm_media)
    except Exception:
        logger.exception("Failed to resolve DRM stream URLs for media %d", media.id)
        return None

    drm_media.video_url = video_url
    drm_media.audio_url = audio_url
    drm_manifest.video_url = video_url
    drm_manifest.audio_url = audio_url
    candidate = _select_drm_stream_candidate(
        drm_media, getattr(media, "category", None)
    )
    return candidate


@router.get("/", response_model=MediaResponse)
def get_media(
    site_name: str = Query(..., description="Site name"),
    media_id: int = Query(..., description="Media ID"),
) -> MediaResponse:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(_normalized_site(site_name))

    with site_db.get_session() as session:
        media = session.exec(
            select(MediaModel).where(MediaModel.id == media_id)
        ).first()
        if not media:
            raise HTTPException(status_code=404, detail="Media not found")
        return _serialize_media(media)


@router.post("/{site_name}/bulk", response_model=list[MediaResponse])
def get_bulk_media(site_name: str, item: MediaIDs) -> list[MediaResponse]:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(_normalized_site(site_name))
    ids = _validate_ids(item.media_ids, "media_ids")

    with site_db.get_session() as session:
        stmt = select(MediaModel).where(MediaModel.id.in_(ids))
        return [_serialize_media(media) for media in session.exec(stmt).all()]


@router.post("/{site_name}/{content_type}/bulk", response_model=list[MediaResponse])
def get_bulk_media_by_content_id(
    site_name: str,
    content_type: str,
    item: ContentIDs,
) -> list[MediaResponse]:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(_normalized_site(site_name))
    ids = _validate_ids(item.content_ids, "content_ids")
    content_field = _content_field(content_type)

    with site_db.get_session() as session:
        stmt = (
            select(MediaModel)
            .join(
                ContentMediaAssoModel, ContentMediaAssoModel.media_id == MediaModel.id
            )
            .where(getattr(ContentMediaAssoModel, content_field).in_(ids))
        )
        return [_serialize_media(media) for media in session.exec(stmt).all()]


@router.get("/{site_name}/by_url", response_model=MediaResponse)
def get_media_by_url(
    site_name: str,
    url: str = Query(..., description="Media URL"),
) -> MediaResponse:
    if not url.strip():
        raise HTTPException(status_code=400, detail="Invalid URL format")

    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(_normalized_site(site_name))

    with site_db.get_session() as session:
        stmt = select(MediaModel).where(MediaModel.url.like(f"%{url}%"))
        media = session.exec(stmt).first()
        if not media:
            raise HTTPException(status_code=404, detail="Media not found")
        return _serialize_media(media)


@router.get("/{site_name}/by_id", response_model=FilenameLookupResponse)
async def get_content_by_media_id(
    site_name: str,
    media_id: int = Query(..., description="Stable archive media ID"),
    backend_instance: BackendInstance = Depends(get_backend_instance),
) -> FilenameLookupResponse:
    """Resolve associated posts and messages from a stable media identifier."""
    site_name = _normalized_site(site_name)
    cache_key = _associated_content_cache_key(site_name, "media", media_id)
    cached = await _get_cached_associated_content(backend_instance, cache_key)
    if cached is not None:
        return cached

    def lookup() -> FilenameLookupResponse:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_name)
        with site_db.get_session() as session:
            media = session.exec(
                select(MediaModel.id).where(MediaModel.id == media_id)
            ).first()
            if media is None:
                raise HTTPException(status_code=404, detail="Media ID not found")
            return _associated_content_for_media_ids(session, {int(media_id)})

    response = await asyncio.to_thread(lookup)
    await _cache_associated_content(backend_instance, cache_key, response)
    return response


@router.get("/{site_name}/by_filename", response_model=FilenameLookupResponse)
async def get_media_by_filename(
    site_name: str,
    filename: str = Query(..., description="Downloaded media filename"),
    backend_instance: BackendInstance = Depends(get_backend_instance),
) -> FilenameLookupResponse:
    """Find media and its associated content from a downloaded filename.

    A face record stores a local image path, while archive paths and media URLs
    can differ across machines. Search the filename across all three archive
    sources and resolve every matching media record to its linked content.
    """

    filename = filename.strip()
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    site_name = _normalized_site(site_name)
    cache_key = _associated_content_cache_key(
        site_name, "filename", filename.casefold()
    )
    cached = await _get_cached_associated_content(backend_instance, cache_key)
    if cached is not None:
        return cached

    def lookup() -> FilenameLookupResponse:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_name)
        pattern = f"%{filename}%"
        with site_db.get_session() as session:
            media_ids = {
                int(media_id)
                for media_id in session.exec(
                    select(FilePathModel.media_id).where(
                        FilePathModel.filepath.ilike(pattern)
                    )
                ).all()
            }
            media_ids.update(
                int(media_id)
                for media_id in session.exec(
                    select(MediaModel.id).where(MediaModel.url.ilike(pattern))
                ).all()
            )
            media_ids.update(
                int(media_id)
                for media_id in session.exec(
                    select(MediaUrlModel.media_id).where(
                        MediaUrlModel.url.ilike(pattern)
                    )
                ).all()
            )
            if not media_ids:
                raise HTTPException(status_code=404, detail="Media filename not found")
            return _associated_content_for_media_ids(session, media_ids)

    response = await asyncio.to_thread(lookup)
    await _cache_associated_content(backend_instance, cache_key, response)
    return response


@router.get("/{site_name}/by_filepath", response_model=FilenameLookupResponse)
async def get_media_by_filepath(
    site_name: str,
    filepath: str = Query(..., description="Exact downloaded media filepath"),
    backend_instance: BackendInstance = Depends(get_backend_instance),
) -> FilenameLookupResponse:
    """Resolve archive content from an exact local media filepath.

    This is the fast path for consumers that have already resolved a moved
    media file. It avoids the broad filename and URL wildcard searches.
    """

    filepath = filepath.strip()
    if not filepath:
        raise HTTPException(status_code=400, detail="Filepath is required")
    site_name = _normalized_site(site_name)
    cache_key = _associated_content_cache_key(site_name, "filepath", filepath)
    cached = await _get_cached_associated_content(backend_instance, cache_key)
    if cached is not None:
        return cached

    def lookup() -> FilenameLookupResponse:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_name)
        with site_db.get_session() as session:
            media_ids = {
                int(media_id)
                for media_id in session.exec(
                    select(FilePathModel.media_id).where(
                        FilePathModel.filepath == filepath
                    )
                ).all()
            }
            if not media_ids:
                raise HTTPException(status_code=404, detail="Media filepath not found")
            return _associated_content_for_media_ids(session, media_ids)

    response = await asyncio.to_thread(lookup)
    await _cache_associated_content(backend_instance, cache_key, response)
    return response


# ---------------------------------------------------------------------------
# Local file streaming – serves downloaded media from disk
# ---------------------------------------------------------------------------


def _build_media_resolution_context(
    session: Any,
    media: Any,
) -> _MediaResolutionContext:
    """Snapshot all database-backed resolution data before slow I/O starts."""

    user = session.exec(
        select(UserModel)
        .where(UserModel.id == media.user_id)
        .options(selectinload(UserModel.aliases))
    ).first()
    username_value = getattr(user, "username", None)
    username = str(username_value).strip() if username_value else None
    aliases = tuple(
        str(getattr(alias, "username", "")).strip()
        for alias in (getattr(user, "aliases", None) or [])
        if getattr(alias, "username", None)
    )

    usernames: list[str] = []
    for name in (username, *aliases, f"u{media.user_id}"):
        cleaned = str(name or "").strip()
        if cleaned and cleaned.casefold() not in {
            item.casefold() for item in usernames
        }:
            usernames.append(cleaned)

    content_entries = tuple(_resolve_all_content_info(session, media.id))
    primary_content = next(
        (
            (content_type, content_id)
            for content_type, content_id in content_entries
            if content_id is not None
        ),
        (None, None),
    )
    return _MediaResolutionContext(
        media=media,
        username=username,
        aliases=aliases,
        usernames=tuple(usernames),
        content_entries=content_entries,
        primary_content=primary_content,
    )


def _get_media_resolution_context(
    site_db: Any,
    media_id: int,
) -> _MediaResolutionContext:
    """Load and detach media resolution state using a short-lived session."""

    with site_db.get_session() as session:
        stmt = (
            select(MediaModel)
            .where(MediaModel.id == media_id)
            .options(selectinload(MediaModel.filepaths))  # type: ignore[arg-type]
            .options(selectinload(MediaModel.media_urls))  # type: ignore[arg-type]
            .options(selectinload(MediaModel.drm_manifest))  # type: ignore[arg-type]
        )
        media = session.exec(stmt).first()
        if not media:
            raise HTTPException(status_code=404, detail="Media not found")
        return _build_media_resolution_context(session, media)


def _get_site_media_resolution_context(
    site: str,
    media_id: int,
) -> _MediaResolutionContext:
    """Resolve a site manager and media snapshot in a worker thread."""

    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site)
    return _get_media_resolution_context(site_db, media_id)


async def _resolve_stored_media_file(
    context: _MediaResolutionContext,
    config: UltimaScraperCollectionConfig,
    site: str,
) -> Path | None:
    """Resolve an exact or renamed file from configured performer storage."""

    media = context.media

    def find_exact_file() -> tuple[Path | None, Path | None]:
        stale_filepath: Path | None = None
        for fp in sorted(media.filepaths, key=lambda filepath: filepath.preview):
            candidate = Path(fp.filepath)
            if candidate.is_file():
                return candidate, stale_filepath
            stale_filepath = stale_filepath or candidate
        return None, stale_filepath

    exact_file, stale_filepath = await asyncio.to_thread(find_exact_file)
    if exact_file is not None:
        return exact_file

    if stale_filepath is None:
        return None

    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                find_performer_media_file,
                config,
                site_name=site,
                usernames=list(context.usernames),
                filename_stem=stale_filepath.stem,
                suffix=stale_filepath.suffix,
            ),
            timeout=_MEDIA_RENAME_SEARCH_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "Timed out searching renamed file for media %d after %d seconds",
            media.id,
            _MEDIA_RENAME_SEARCH_TIMEOUT_SECONDS,
        )
        return None


async def _resolve_configured_media_file(
    context: _MediaResolutionContext,
    config: UltimaScraperCollectionConfig,
    site: str,
) -> Path | None:
    """Reconstruct configured download paths when database paths are absent."""

    media = context.media
    if not media.url:
        return None

    filenames = _candidate_filenames(
        config,
        site,
        media.user_id,
        media.id,
        media.url,
        getattr(media, "category", None),
        getattr(media, "created_at", None),
    )
    if not filenames:
        return None

    media_type = getattr(media, "category", None) or ""
    created_at = getattr(media, "created_at", None)
    content_entries = context.content_entries or (("", None),)

    def resolve_from_config() -> Path | None:
        for content_type_str, content_id in content_entries:
            try:
                return _resolve_media_path(
                    config,
                    site,
                    media.user_id,
                    context.username,
                    filenames,
                    content_type_str,
                    _candidate_content_values(content_id),
                    media_type,
                    media.id,
                    created_at if isinstance(created_at, datetime) else None,
                    context.aliases,
                )
            except HTTPException:
                continue
        return None

    return await asyncio.to_thread(resolve_from_config)


def _merge_cookie_headers(*values: str) -> str:
    return "; ".join(value.strip().strip(";") for value in values if value.strip())


async def _resolve_remote_request_context(
    backend_manager: BackendManager,
    site: str,
    candidate: _RemoteStreamCandidate,
) -> _RemoteRequestContext:
    """Load stored CDN headers without creating a deprecated API session."""

    account_context: AccountRequestContext | None = None
    get_request_context = getattr(
        backend_manager,
        "get_account_request_context",
        None,
    )
    if callable(get_request_context):
        account_context = await asyncio.to_thread(
            get_request_context,
            site,
            candidate.auth_id,
        )
    return _RemoteRequestContext(
        cookie_header=_merge_cookie_headers(
            account_context.cookie if account_context is not None else "",
            candidate.custom_cookies,
        ),
        authorization=(
            account_context.authorization if account_context is not None else ""
        ),
        user_agent=account_context.user_agent if account_context is not None else "",
        proxy_url=account_context.proxy_url if account_context is not None else None,
    )


async def _remote_thumbnail_source_is_reachable(
    backend_manager: BackendManager,
    site: str,
    candidate: _RemoteStreamCandidate,
    *,
    request_context: _RemoteRequestContext | None = None,
) -> bool:
    """Quickly verify a CDN source without logging into the site API."""

    validate_outbound_url(
        candidate.url,
        allowed_domains=_DEFAULT_MEDIA_PROXY_ALLOWED_DOMAINS,
    )

    resolved_context = request_context or await _resolve_remote_request_context(
        backend_manager,
        site,
        candidate,
    )
    headers: dict[str, str] = {}
    if resolved_context.cookie_header:
        headers["Cookie"] = resolved_context.cookie_header
    if resolved_context.authorization:
        headers["Authorization"] = resolved_context.authorization
    if resolved_context.user_agent:
        headers["User-Agent"] = resolved_context.user_agent

    proxy_url = resolved_context.proxy_url
    proxy_scheme = urlparse(proxy_url).scheme.lower() if proxy_url else ""
    connector = (
        ProxyConnector.from_url(proxy_url)
        if proxy_url and proxy_scheme.startswith("socks")
        else None
    )
    request_proxy = proxy_url if proxy_scheme in {"http", "https"} else None

    async def probe() -> bool:
        timeout = aiohttp.ClientTimeout(total=_REMOTE_THUMBNAIL_PROBE_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            trust_env=False,
        ) as client:
            async with client.head(
                candidate.url,
                headers=headers,
                proxy=request_proxy,
                allow_redirects=True,
            ) as response:
                return 200 <= response.status < 400

    try:
        return await asyncio.wait_for(
            probe(),
            timeout=_REMOTE_THUMBNAIL_PROBE_TIMEOUT_SECONDS,
        )
    except (TimeoutError, HTTPException):
        logger.info(
            "Remote thumbnail HEAD probe failed for %s",
            candidate.url,
        )
    except Exception:
        logger.debug(
            "Remote thumbnail HEAD probe raised for %s",
            candidate.url,
            exc_info=True,
        )
    return False


@router.get("/stream/{site_name}/{media_id}", response_model=None)
async def stream_local_media(
    site_name: str,
    media_id: int,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> FileResponse | StreamingResponse:
    """Stream a downloaded media file from local storage.

    Serves an existing local file first so browsers receive the correct MIME
    type and byte-range support needed for MP4/WebM playback. If no local
    file can be resolved, falls back to proxying the remote ``media.url``
    through the backend's authenticated session.
    """

    site = _normalized_site(site_name)
    context = await asyncio.to_thread(
        _get_site_media_resolution_context,
        site,
        media_id,
    )
    media = context.media

    def local_file_response(path: Path) -> FileResponse:
        content_type, _ = mimetypes.guess_type(str(path))
        return FileResponse(
            path=path,
            media_type=content_type or "application/octet-stream",
            filename=path.name,
            headers={"Cache-Control": "public, max-age=86400"},
        )

    # Database session is already closed before storage/network work starts.
    file_path = await _resolve_stored_media_file(context, config, site)
    if file_path is not None:
        return local_file_response(file_path)

    file_path = await _resolve_configured_media_file(context, config, site)
    if file_path is not None:
        return local_file_response(file_path)

    remote_candidates = _iter_remote_stream_candidates(media)
    drm_candidate = await _resolve_drm_stream_candidate(
        None,
        media,
        site,
        backend_manager,
        primary_content=context.primary_content,
    )
    if drm_candidate is not None:
        remote_candidates.append(drm_candidate)

    for remote_candidate in remote_candidates:
        try:
            logger.info(
                "Proxying remote media %d after filepaths lookup missed via %s",
                media_id,
                remote_candidate.url,
            )
            return await proxy_stream_url(
                remote_candidate.url,
                site,
                backend_manager,
                remote_candidate.auth_id,
                custom_cookies=remote_candidate.custom_cookies,
            )
        except HTTPException as exc:
            logger.warning(
                "Proxy stream failed for media %d with status %d from %s; trying next fallback",
                media_id,
                exc.status_code,
                remote_candidate.url,
            )
        except Exception:
            logger.exception(
                "Proxy stream failed unexpectedly for media %d from %s; trying next fallback",
                media_id,
                remote_candidate.url,
            )

    raise HTTPException(status_code=404, detail="File not found on disk")


# ---------------------------------------------------------------------------
# Thumbnail – serves (or generates on the fly) a thumbnail for a media item
# ---------------------------------------------------------------------------

from ...helpers.thumbnails import (  # noqa: E402
    get_or_create_remote_thumbnail,
    get_or_create_thumbnail,
    get_remote_thumbnail_path,
)


async def _generate_remote_media_thumbnail(
    *,
    context: _MediaResolutionContext,
    site: str,
    media_id: int,
    thumbnails_dir: Path,
    is_video: bool,
    backend_manager: BackendManager,
) -> Path | None:
    """Resolve and generate a remote thumbnail after its inbound request ends."""

    media = context.media

    async def try_candidates(
        candidates: Iterable[_RemoteStreamCandidate],
    ) -> Path | None:
        for remote_candidate in candidates:
            try:
                request_context = await _resolve_remote_request_context(
                    backend_manager,
                    site,
                    remote_candidate,
                )
                if not await _remote_thumbnail_source_is_reachable(
                    backend_manager,
                    site,
                    remote_candidate,
                    request_context=request_context,
                ):
                    continue
                async with _thumbnail_generation_slots:
                    remote_thumb_path = await asyncio.to_thread(
                        get_or_create_remote_thumbnail,
                        remote_candidate.url,
                        f"{site}:{media_id}",
                        thumbnails_dir,
                        is_video=is_video,
                        custom_cookies=request_context.cookie_header,
                        user_agent=request_context.user_agent,
                        authorization=request_context.authorization,
                        proxy_url=request_context.proxy_url,
                    )
            except HTTPException:
                logger.warning(
                    "Rejected remote thumbnail candidate for media %d",
                    media_id,
                )
                continue
            if remote_thumb_path is not None:
                return remote_thumb_path
        return None

    thumb_path = await try_candidates(_iter_remote_stream_candidates(media))
    if thumb_path is not None:
        return thumb_path

    # Reuse already-resolved DRM URLs, but never log into the site API from a
    # thumbnail job. Active DRM resolution belongs to the worker pool.
    drm_candidate = await _resolve_drm_stream_candidate(
        None,
        media,
        site,
        backend_manager,
        primary_content=context.primary_content,
        allow_api_login=False,
    )
    if drm_candidate is None:
        return None
    return await try_candidates((drm_candidate,))


async def _run_deferred_remote_thumbnail_job(
    job_key: str,
    *,
    context: _MediaResolutionContext,
    site: str,
    media_id: int,
    thumbnails_dir: Path,
    is_video: bool,
    backend_manager: BackendManager,
) -> None:
    """Run remote work in the background and retain short-lived failures."""

    thumb_path: Path | None = None
    try:
        async with _remote_thumbnail_resolution_slots:
            thumb_path = await _generate_remote_media_thumbnail(
                context=context,
                site=site,
                media_id=media_id,
                thumbnails_dir=thumbnails_dir,
                is_video=is_video,
                backend_manager=backend_manager,
            )
    except Exception:
        logger.exception("Deferred thumbnail generation failed for %s", job_key)
    finally:
        if thumb_path is None or not thumb_path.exists():
            _remember_remote_thumbnail_failure(job_key)
        else:
            _remote_thumbnail_failures.pop(job_key, None)
        _remote_thumbnail_jobs.pop(job_key, None)


def _deferred_thumbnail_response() -> Response:
    return Response(
        status_code=202,
        headers={"Retry-After": "1", "Cache-Control": "no-store"},
    )


@router.get("/thumbnail/{site_name}/{media_id}", response_model=None)
async def get_media_thumbnail(
    site_name: str,
    media_id: int,
    local_only: bool = Query(False),
    remote_only: bool = Query(False),
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> FileResponse | Response:
    """Return a thumbnail for a media item, generating one if needed.

    Thumbnails follow the XDG spec and are stored in the configured
    ``thumbnails_directory/large/`` folder as PNG files.
    """
    local_only_mode = local_only if isinstance(local_only, bool) else False
    remote_only_mode = remote_only if isinstance(remote_only, bool) else False
    if local_only_mode and remote_only_mode:
        raise HTTPException(
            status_code=400,
            detail="local_only and remote_only cannot both be enabled",
        )
    site = _normalized_site(site_name)

    # Resolve thumbnails directory from config
    thumbnails_dir: Path = Path.home() / ".cache" / "thumbnails"
    configured = getattr(config.settings, "thumbnails_directory", None)
    if configured:
        thumbnails_dir = Path(configured)

    cache_key = f"{site}:{media_id}"

    def thumbnail_response(path: Path) -> FileResponse:
        return FileResponse(
            path=path,
            media_type="image/png",
            filename=path.name,
            headers={"Cache-Control": "public, max-age=604800"},
        )

    if remote_only_mode:
        # Polls for deferred work never touch the database or wait behind
        # FFmpeg. This keeps same-origin browser connections free for paging.
        remote_thumb_path = get_remote_thumbnail_path(cache_key, thumbnails_dir)
        if remote_thumb_path.exists():
            _remote_thumbnail_failures.pop(cache_key, None)
            return thumbnail_response(remote_thumb_path)

        now = time.monotonic()
        _prune_remote_thumbnail_failures(now)
        failure_until = _remote_thumbnail_failures.get(cache_key)
        if failure_until is not None:
            _remote_thumbnail_failures.move_to_end(cache_key)
            if failure_until > now:
                raise HTTPException(
                    status_code=404,
                    detail="No reachable thumbnail source found",
                )
            _remote_thumbnail_failures.pop(cache_key, None)

        if cache_key in _remote_thumbnail_jobs:
            return _deferred_thumbnail_response()

    context = await asyncio.to_thread(
        _get_site_media_resolution_context,
        site,
        media_id,
    )
    media = context.media

    is_video = (media.category or "").lower() in ("video", "videos")

    if remote_only_mode:
        if len(_remote_thumbnail_jobs) >= _REMOTE_THUMBNAIL_JOB_LIMIT:
            return _deferred_thumbnail_response()
        task = asyncio.create_task(
            _run_deferred_remote_thumbnail_job(
                cache_key,
                context=context,
                site=site,
                media_id=media_id,
                thumbnails_dir=thumbnails_dir,
                is_video=is_video,
                backend_manager=backend_manager,
            )
        )
        _remote_thumbnail_jobs[cache_key] = task
        return _deferred_thumbnail_response()

    # The database connection is released before storage scans or FFmpeg.
    source_file = await _resolve_stored_media_file(context, config, site)
    if source_file is None:
        source_file = await _resolve_configured_media_file(context, config, site)

    is_video = is_video or (
        source_file is not None
        and source_file.suffix.lower() in {".mp4", ".m4v", ".mov", ".webm", ".mkv"}
    )
    thumb_path: Path | None = None
    if source_file is not None:
        async with _thumbnail_generation_slots:
            thumb_path = await asyncio.to_thread(
                get_or_create_thumbnail,
                source_file,
                thumbnails_dir,
                is_video=is_video,
            )
    elif local_only_mode:
        # Library grids use this mode so a stale downloaded-file record cannot
        # silently become a long-running remote FFmpeg job.
        raise HTTPException(status_code=404, detail="Local thumbnail source not found")
    else:
        thumb_path = await _generate_remote_media_thumbnail(
            context=context,
            site=site,
            media_id=media_id,
            thumbnails_dir=thumbnails_dir,
            is_video=is_video,
            backend_manager=backend_manager,
        )

        if thumb_path is None:
            raise HTTPException(
                status_code=404,
                detail="No reachable thumbnail source found",
            )

    if thumb_path is None or not thumb_path.exists():
        raise HTTPException(status_code=500, detail="Thumbnail generation failed")

    return thumbnail_response(thumb_path)


# ---------------------------------------------------------------------------
# Media proxy – streams CDN content through the auth's session (with proxy)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _MediaProxyCacheEntry:
    content_type: str
    data: bytes
    expires_at: float


class _MediaProxyCache:
    """Byte-bounded TTL/LRU cache for small proxied media responses."""

    def __init__(
        self,
        *,
        max_bytes: int,
        max_entry_bytes: int,
        ttl_seconds: int,
        max_entries: int = 500,
    ) -> None:
        self.max_bytes = max(0, max_bytes)
        self.max_entry_bytes = max(0, min(max_entry_bytes, self.max_bytes))
        self.ttl_seconds = max(0, ttl_seconds)
        self.max_entries = max(0, max_entries)
        self._entries: OrderedDict[str, _MediaProxyCacheEntry] = OrderedDict()
        self._total_bytes = 0

    def get(self, key: str) -> tuple[str, bytes] | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= time.monotonic():
            self._remove(key)
            return None
        self._entries.move_to_end(key)
        return entry.content_type, entry.data

    def put(self, key: str, content_type: str, data: bytes) -> bool:
        size = len(data)
        if (
            not self.max_bytes
            or not self.max_entry_bytes
            or not self.ttl_seconds
            or not self.max_entries
            or size <= 0
            or size > self.max_entry_bytes
        ):
            return False

        self._remove(key)
        now = time.monotonic()
        self._purge_expired(now)
        while self._entries and (
            self._total_bytes + size > self.max_bytes
            or len(self._entries) >= self.max_entries
        ):
            oldest_key = next(iter(self._entries))
            self._remove(oldest_key)

        self._entries[key] = _MediaProxyCacheEntry(
            content_type=content_type,
            data=data,
            expires_at=now + self.ttl_seconds,
        )
        self._total_bytes += size
        return True

    def clear(self) -> None:
        self._entries.clear()
        self._total_bytes = 0

    def stats(self) -> dict[str, int]:
        self._purge_expired(time.monotonic())
        return {
            "entries": len(self._entries),
            "bytes": self._total_bytes,
            "max_bytes": self.max_bytes,
            "max_entry_bytes": self.max_entry_bytes,
            "max_entries": self.max_entries,
            "ttl_seconds": self.ttl_seconds,
        }

    def _remove(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._total_bytes -= len(entry.data)

    def _purge_expired(self, now: float) -> None:
        expired = [
            key for key, entry in self._entries.items() if entry.expires_at <= now
        ]
        for key in expired:
            self._remove(key)


_proxy_cache = _MediaProxyCache(
    max_bytes=_env_int("USR_MEDIA_PROXY_CACHE_MAX_BYTES", 128 * 1024 * 1024),
    max_entry_bytes=_env_int("USR_MEDIA_PROXY_CACHE_MAX_ENTRY_BYTES", 2 * 1024 * 1024),
    ttl_seconds=_env_int("USR_MEDIA_PROXY_CACHE_TTL_SECONDS", 15 * 60),
    max_entries=_env_int("USR_MEDIA_PROXY_CACHE_MAX_ENTRIES", 500),
)


def get_media_proxy_cache_stats() -> dict[str, int]:
    """Return bounded proxy-cache usage for process memory telemetry."""
    _prune_remote_thumbnail_failures(time.monotonic())
    return {
        **_proxy_cache.stats(),
        "thumbnail_jobs": len(_remote_thumbnail_jobs),
        "thumbnail_job_limit": _REMOTE_THUMBNAIL_JOB_LIMIT,
        "thumbnail_failures": len(_remote_thumbnail_failures),
        "thumbnail_failure_limit": _REMOTE_THUMBNAIL_FAILURE_CACHE_MAX_ENTRIES,
    }


_DEFAULT_MEDIA_PROXY_ALLOWED_DOMAINS = [
    "onlyfans.com",
    "fansly.com",
    "ipwho.is",
    "checkip.amazonaws.com",
    "ipv4.download.thinkbroadband.com",
]


async def proxy_stream_url(
    url: str,
    site: str,
    backend_manager: BackendManager,
    auth_id: int | None = None,
    *,
    custom_cookies: str = "",
    allowed_domains: list[str] | None = None,
) -> StreamingResponse:
    """Stream a CDN URL using detached stored credentials and proxy settings.

    Reusable helper used by the ``/proxy`` endpoint and as a fallback when
    local files are not available on disk. This deliberately avoids opening a
    site API session in the control process.
    """
    validate_outbound_url(
        url,
        allowed_domains=allowed_domains or _DEFAULT_MEDIA_PROXY_ALLOWED_DOMAINS,
    )
    request_timeout_seconds = _env_float("USR_OUTBOUND_TIMEOUT_SECONDS", 120.0)
    max_proxy_bytes = _env_int("USR_MEDIA_PROXY_MAX_BYTES", 500 * 1024 * 1024)

    # Check cache first
    use_cache = not custom_cookies
    cache_key = url
    cached = _proxy_cache.get(cache_key) if use_cache else None
    if cached is not None:
        content_type, data = cached
        return StreamingResponse(
            iter([data]),
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=86400"},
        )

    candidate = _RemoteStreamCandidate(
        url=url,
        auth_id=auth_id,
        custom_cookies=custom_cookies,
    )
    request_context = await _resolve_remote_request_context(
        backend_manager,
        site,
        candidate,
    )
    headers: dict[str, str] = {}
    if request_context.cookie_header:
        headers["Cookie"] = request_context.cookie_header
    if request_context.authorization:
        headers["Authorization"] = request_context.authorization
    if request_context.user_agent:
        headers["User-Agent"] = request_context.user_agent

    proxy_url = request_context.proxy_url
    proxy_scheme = urlparse(proxy_url).scheme.lower() if proxy_url else ""
    connector = (
        ProxyConnector.from_url(proxy_url)
        if proxy_url and proxy_scheme.startswith("socks")
        else None
    )
    request_proxy = proxy_url if proxy_scheme in {"http", "https"} else None
    timeout = aiohttp.ClientTimeout(total=request_timeout_seconds)
    session = aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        trust_env=False,
    )
    try:
        resp = await asyncio.wait_for(
            session.get(url, headers=headers, proxy=request_proxy),
            timeout=request_timeout_seconds,
        )
    except Exception:
        await session.close()
        raise

    if resp.status not in (200, 206):
        resp.close()
        await session.close()
        raise HTTPException(
            status_code=resp.status,
            detail=f"Upstream returned {resp.status}",
        )
    content_type = resp.content_type or "application/octet-stream"
    if resp.content_length and resp.content_length > max_proxy_bytes:
        resp.close()
        await session.close()
        raise HTTPException(
            status_code=413,
            detail="Upstream response exceeds size limit",
        )

    cache_threshold = _proxy_cache.max_entry_bytes

    async def _stream_remote() -> AsyncGenerator[bytes, None]:
        try:
            chunks: list[bytes] | None = []
            total_bytes = 0
            async for chunk in resp.content.iter_chunked(65536):
                chunk_bytes = bytes(chunk)
                total_bytes += len(chunk_bytes)
                if total_bytes > max_proxy_bytes:
                    return
                if chunks is not None:
                    if total_bytes <= cache_threshold:
                        chunks.append(chunk_bytes)
                    else:
                        chunks = None
                yield chunk_bytes
            if use_cache and chunks is not None and total_bytes <= cache_threshold:
                _proxy_cache.put(cache_key, content_type, b"".join(chunks))
        finally:
            resp.close()
            await session.close()

    passthrough_headers = {"Cache-Control": "public, max-age=86400"}
    for header_name in ("Accept-Ranges", "Content-Length", "Content-Range", "ETag"):
        value = resp.headers.get(header_name)
        if value:
            passthrough_headers[header_name] = value
    return StreamingResponse(
        _stream_remote(),
        status_code=resp.status,
        media_type=content_type,
        headers=passthrough_headers,
    )


@router.get("/proxy")
async def proxy_media(
    url: str = Query(..., description="CDN URL to proxy"),
    site: str = Query("onlyfans", description="Site name"),
    auth_id: int | None = Query(None, description="Auth ID for proxy/session lookup"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> StreamingResponse:
    """Stream media content through the backend's authenticated session.

    This is needed when the auth account uses a proxy (SOCKS5, etc.) that
    the browser cannot use directly.  The backend fetches the CDN content
    using the auth's aiohttp session (which has the proxy connector) and
    streams it back to the browser.
    """
    if not url:
        raise HTTPException(status_code=400, detail="Invalid URL")

    config = get_config()
    allowed_domains = _DEFAULT_MEDIA_PROXY_ALLOWED_DOMAINS
    security_cfg = getattr(getattr(config, "settings", None), "security", None)
    configured_domains = (
        list(getattr(security_cfg, "media_proxy_allowed_domains", []))
        if security_cfg is not None
        else []
    )
    if configured_domains:
        allowed_domains = configured_domains
    validate_outbound_url(url, allowed_domains=allowed_domains, allow_private=False)

    return await proxy_stream_url(
        url,
        site,
        backend_manager,
        auth_id,
        allowed_domains=allowed_domains,
    )


# ---------------------------------------------------------------------------
# DRM browser playback – rewritten DASH manifest + segment/license proxies
# ---------------------------------------------------------------------------
#
# Chrome/Edge/Firefox ship the Widevine CDM and can play CENC DASH streams via
# EME (e.g. video.js + @videojs/contrib-eme). The browser cannot talk to
# OnlyFans' CDN or license server directly because:
#   * CDN segments require CloudFront-Key-Pair-Id/Policy/Signature cookies
#     that are IP-locked to the authed backend's outbound address;
#   * the Widevine license endpoint requires the authed OF session's headers.
#
# These three endpoints let the browser play DRM media through our backend:
#   GET  /media/drm/{site}/{media_id}/manifest.mpd
#   GET  /media/drm/{site}/{media_id}/segment/{filename}
#   POST /media/drm/{site}/{media_id}/license

_DRM_SEGMENT_FILENAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_MAX_DRM_LICENSE_BYTES = 64 * 1024  # Widevine challenges are small (< 10 KiB).


@dataclass(frozen=True)
class _DRMPlaybackContext:
    drm_media: DRMMedia
    cookies: str
    auth_id: int | None
    manifest_directory: str
    content_type: str | None
    content_id: int | None


def _load_drm_playback_context_sync(
    site: str,
    media_id: int,
) -> _DRMPlaybackContext:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site)

    with site_db.get_session() as session:
        stmt = (
            select(MediaModel)
            .where(MediaModel.id == media_id)
            .options(selectinload(MediaModel.drm_manifest))  # type: ignore[arg-type]
            .options(selectinload(MediaModel.media_urls))  # type: ignore[arg-type]
        )
        media = session.exec(stmt).first()
        if not media:
            raise HTTPException(status_code=404, detail="Media not found")

        drm_manifest = getattr(media, "drm_manifest", None)
        dash_manifest = (
            getattr(drm_manifest, "dash_manifest", None) if drm_manifest else None
        )
        dash_key_pair_id = (
            getattr(drm_manifest, "dash_key_pair_id", None) if drm_manifest else None
        )
        dash_policy = (
            getattr(drm_manifest, "dash_policy", None) if drm_manifest else None
        )
        dash_signature = (
            getattr(drm_manifest, "dash_signature", None) if drm_manifest else None
        )

        if not all((dash_manifest, dash_key_pair_id, dash_policy, dash_signature)):
            raise HTTPException(
                status_code=404, detail="Media has no usable DRM manifest"
            )

        if not _is_drm_policy_usable(dash_policy):
            raise HTTPException(
                status_code=410,
                detail="DRM policy is expired; refresh content metadata",
            )

        content_type, content_id = _primary_content_info(session, media.id)

        drm_media = DRMMedia(
            media_id=media.id,
            manifest_url=str(dash_manifest),
            key_pair_id=str(dash_key_pair_id),
            policy=str(dash_policy),
            signature=str(dash_signature),
            content_id=content_id if content_id is not None else 0,
            video_url=getattr(drm_manifest, "video_url", None),
            audio_url=getattr(drm_manifest, "audio_url", None),
            video_size=getattr(drm_manifest, "video_size", None),
            audio_size=getattr(drm_manifest, "audio_size", None),
            content_type=content_type,
        )

        parsed = urlparse(str(dash_manifest))
        manifest_directory = parsed._replace(
            path=parsed.path.rsplit("/", 1)[0] + "/"
        ).geturl()

        return _DRMPlaybackContext(
            drm_media=drm_media,
            cookies=drm_media.get_cookies(),
            auth_id=_preferred_media_auth_id(media),
            manifest_directory=manifest_directory,
            content_type=content_type,
            content_id=content_id,
        )


async def _load_drm_playback_context(
    site: str,
    media_id: int,
) -> _DRMPlaybackContext:
    return await asyncio.to_thread(
        _load_drm_playback_context_sync,
        site,
        media_id,
    )


def _rewrite_dash_manifest(manifest_xml: str, segment_base: str) -> str:
    """Rewrite ``<BaseURL>`` elements to point at our segment proxy.

    Segments in OnlyFans DASH manifests are relative filenames (e.g.
    ``0ibvf8g1wqq8yyo2hed3s_source.mp4``). Rewriting them to the proxy path
    keeps path-traversal impossible (see ``_DRM_SEGMENT_FILENAME_RE``).
    """

    def _replace(match: "re.Match[str]") -> str:
        original = match.group(1).strip()
        # Only rewrite already-basename relative URLs. Anything with a slash or
        # a URL scheme is left alone so the proxy never fetches an unexpected
        # path on behalf of the client.
        bare = original.split("?", 1)[0]
        if "/" in bare or not _DRM_SEGMENT_FILENAME_RE.match(bare):
            return match.group(0)
        return f"<BaseURL>{segment_base}{bare}</BaseURL>"

    return re.sub(r"<BaseURL>([^<]+)</BaseURL>", _replace, manifest_xml)


async def _fetch_via_auth(
    url: str,
    *,
    site: str,
    backend_manager: BackendManager,
    auth_id: int | None,
    method: str = "GET",
    custom_cookies: str = "",
    custom_headers: dict[str, Any] | None = None,
    data: bytes | None = None,
) -> Any:
    """Thin wrapper around the authed session's ``request`` for DRM flows."""
    auth = await backend_manager.get_authed_session(site, auth_id)
    auth_session = getattr(auth, "auth_session", None)
    if auth_session is None:
        raise HTTPException(
            status_code=502, detail="No authenticated session available"
        )

    kwargs: dict[str, Any] = {
        "method": method,
        "premade_settings": "",
    }
    if custom_cookies:
        kwargs["custom_cookies"] = custom_cookies
    if custom_headers:
        kwargs["custom_headers"] = custom_headers
    if data is not None:
        kwargs["data"] = data

    try:
        response = await asyncio.wait_for(
            auth_session.request(url, **kwargs),
            timeout=_env_float("USR_OUTBOUND_TIMEOUT_SECONDS", 120.0),
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504, detail="Upstream DRM request timed out"
        ) from exc
    if response is None:
        raise HTTPException(status_code=502, detail="Upstream returned no response")
    return response


@router.get("/drm/{site_name}/{media_id}/manifest.mpd")
async def get_drm_manifest(
    site_name: str,
    media_id: int,
    request: Request,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> Response:
    """Return a rewritten DASH manifest safe for browser playback.

    The original manifest's ``<BaseURL>`` entries are replaced with URLs that
    go through the segment proxy so the browser never talks to the CDN
    directly.
    """
    site = _normalized_site(site_name)
    ctx = await _load_drm_playback_context(site, media_id)

    response = await _fetch_via_auth(
        ctx.drm_media.manifest_url,
        site=site,
        backend_manager=backend_manager,
        auth_id=ctx.auth_id,
        custom_cookies=ctx.cookies,
    )
    status = int(getattr(response, "status", 0) or 0)
    if status != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Upstream manifest returned {status or 'no'} status",
        )
    manifest_text = await response.text()

    # Segments are served at ``segment/{filename}`` relative to this endpoint,
    # so a relative BaseURL keeps the proxy path opaque to the client and
    # avoids depending on the externally visible origin.
    segment_base = "segment/"
    rewritten = _rewrite_dash_manifest(manifest_text, segment_base)

    return Response(
        content=rewritten,
        media_type="application/dash+xml",
        headers={
            "Cache-Control": "no-store",
            # MPDs live under a time-limited CloudFront policy; clients should
            # re-fetch through us if playback pauses long enough to expire.
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get(
    "/drm/{site_name}/{media_id}/segment/{filename}",
    name="proxy_drm_segment",
)
async def proxy_drm_segment(
    site_name: str,
    media_id: int,
    filename: str,
    request: Request,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> StreamingResponse:
    """Proxy a DRM media segment (encrypted MP4 range) through the auth session."""
    if not _DRM_SEGMENT_FILENAME_RE.match(filename):
        raise HTTPException(status_code=400, detail="Invalid segment filename")

    site = _normalized_site(site_name)
    ctx = await _load_drm_playback_context(site, media_id)
    target_url = ctx.manifest_directory + filename

    range_header_value = request.headers.get("range")
    range_header: dict[str, Any] | None = (
        {"Range": range_header_value} if range_header_value else None
    )

    auth = await backend_manager.get_authed_session(site, ctx.auth_id)
    auth_session = getattr(auth, "auth_session", None)
    if auth_session is None:
        raise HTTPException(
            status_code=502, detail="No authenticated session available"
        )

    try:
        response = await asyncio.wait_for(
            auth_session.request(
                target_url,
                method="GET",
                premade_settings="",
                custom_cookies=ctx.cookies,
                range_header=range_header,
            ),
            timeout=_env_float("USR_OUTBOUND_TIMEOUT_SECONDS", 120.0),
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504, detail="Upstream segment timed out"
        ) from exc
    if response is None:
        raise HTTPException(status_code=502, detail="Upstream returned no response")

    status = int(getattr(response, "status", 0) or 0)
    if status not in (200, 206):
        raise HTTPException(
            status_code=502, detail=f"Upstream segment returned {status}"
        )

    upstream_headers = getattr(response, "headers", None) or {}
    content_type = (
        upstream_headers.get("Content-Type")
        or getattr(response, "content_type", None)
        or "video/mp4"
    )
    passthrough_headers = {
        "Cache-Control": "no-store",
        "Accept-Ranges": "bytes",
    }
    for header_name in ("Content-Length", "Content-Range", "Last-Modified", "ETag"):
        value = upstream_headers.get(header_name)
        if value:
            passthrough_headers[header_name] = value

    iter_chunked = getattr(getattr(response, "content", None), "iter_chunked", None)
    if not callable(iter_chunked):
        raise HTTPException(
            status_code=502, detail="Upstream response is not streamable"
        )

    async def _iter() -> AsyncGenerator[bytes, None]:
        async for chunk in cast(Any, iter_chunked)(65536):
            yield bytes(chunk)

    return StreamingResponse(
        _iter(),
        status_code=status,
        media_type=content_type,
        headers=passthrough_headers,
    )


@router.post("/drm/{site_name}/{media_id}/license")
async def proxy_drm_license(
    site_name: str,
    media_id: int,
    request: Request,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> Response:
    """Forward a Widevine license challenge from the browser CDM to OF."""
    site = _normalized_site(site_name)
    ctx = await _load_drm_playback_context(site, media_id)
    if ctx.content_id is None or not ctx.content_type:
        raise HTTPException(
            status_code=409,
            detail="Media is missing content association required for license",
        )

    challenge = await request.body()
    if not challenge:
        raise HTTPException(status_code=400, detail="Missing license challenge body")
    if len(challenge) > _MAX_DRM_LICENSE_BYTES:
        raise HTTPException(status_code=413, detail="License challenge too large")

    license_url = APIRoutes().drm_resolver(media_id, ctx.content_type, ctx.content_id)

    response = await _fetch_via_auth(
        license_url,
        site=site,
        backend_manager=backend_manager,
        auth_id=ctx.auth_id,
        method="POST",
        data=challenge,
    )
    status = int(getattr(response, "status", 0) or 0)
    if status != 200:
        raise HTTPException(
            status_code=502, detail=f"Upstream license returned {status}"
        )
    body = await response.read()
    return Response(
        content=bytes(body),
        media_type="application/octet-stream",
        headers={"Cache-Control": "no-store"},
    )
