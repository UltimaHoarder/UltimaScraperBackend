"""File streaming routes for the mini database."""

from __future__ import annotations

import asyncio
import glob
import logging
import mimetypes
import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import aiofiles
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from inflection import pluralize
from pydantic import BaseModel
from sqlalchemy import func
from sqlmodel import Session, select
from ultima_scraper_api import get_site_title

from ultima_scraper_db_archive import (
    ContentMediaAssoModel,
    MediaModel,
    UserAliasModel,
    UserModel,
)

from ...config import UltimaScraperCollectionConfig
from ...database.factory import MultiSiteDatabaseManager
from ...job_system.path_formatter import FormatAttributes, PathFormatter
from ...manager import BackendManager
from ..dependencies import get_backend_manager, get_config

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/files", tags=["files"], responses={404: {"description": "Not found"}}
)

TOKEN_PATTERN = re.compile(r"{[^}]+}")
EXPLICIT_MOUNT_ROOTS = frozenset({"/mnt", "/media", "/run/media"})


async def _proxy_fallback(url: str, site_name: str, backend_manager: BackendManager):
    """Proxy a CDN URL through the backend when a local file is missing."""
    from .media import proxy_stream_url

    return await proxy_stream_url(url, site_name.lower(), backend_manager)


class UsernameDirectoryResult(BaseModel):
    root: str
    path: str


class UsernameDirectorySearchResponse(BaseModel):
    success: bool
    username: str
    site: str
    user_directory_format: str
    matches: list[UsernameDirectoryResult]
    checked_patterns: list[str]
    usernames_searched: list[str] = []
    resolved_user_id: int | None = None


async def iterfile(
    file_path: Path, start: int, size: int, chunk_size: int = 1024 * 1024
):
    async with aiofiles.open(file_path, "rb") as f:
        await f.seek(start)
        bytes_left = size
        while bytes_left > 0:
            read_size = min(bytes_left, chunk_size)
            chunk = await f.read(read_size)
            if not chunk:
                break
            yield chunk
            bytes_left -= len(chunk)


def _extract_filename_from_url(media_url: str) -> str:
    filename = media_url.split("/")[-1].split("?")[0].strip()
    if not filename:
        raise HTTPException(status_code=404, detail="Media filename not found")
    return filename


def _candidate_usernames(
    username: str | None,
    user_id: int,
    aliases: Iterable[str] | None = None,
) -> list[str]:
    candidates = [username, *(aliases or []), f"u{user_id}"]
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        cleaned = str(candidate or "").strip()
        folded = cleaned.casefold()
        if cleaned and folded not in seen:
            seen.add(folded)
            result.append(cleaned)
    return result


def _iter_download_directory_formats(
    config: UltimaScraperCollectionConfig,
) -> list[Path]:
    download_setup = config.settings.download_setup
    seen: set[str] = set()
    ordered: list[Path] = []

    for directory_format in [
        download_setup.directory_format,
        *download_setup.alt_directory_formats,
    ]:
        key = str(directory_format)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(directory_format)

    return ordered


def _build_directory_candidates(
    config: UltimaScraperCollectionConfig,
    site_name: str,
    user_id: int,
    username: str,
    content_type: str,
    content_value: str,
    media_type: str,
    media_id: int,
    created_at: datetime | None,
) -> list[Path]:
    site_title = get_site_title(site_name)
    download_setup = config.settings.download_setup

    format_attrs = FormatAttributes()
    format_attrs.site_name = site_title
    format_attrs.user_id = str(user_id)
    format_attrs.user_username = username
    format_attrs.first_username_letter = username[0].upper() if username else "0"
    format_attrs.first_letter = format_attrs.first_username_letter
    format_attrs.first_id_digit = str(user_id)[0] if user_id else "0"
    format_attrs.content_type = content_type
    format_attrs.content_value = content_value
    format_attrs.media_type = pluralize(media_type)
    format_attrs.media_id = str(media_id)
    format_attrs.media_filename = ""
    format_attrs.media_ext = ""
    format_attrs.content_date = created_at.strftime("%Y-%m-%d") if created_at else ""
    format_attrs.content_text = ""
    format_attrs.content_id = ""
    format_attrs.authed_username = ""

    roots: list[Path] = []
    for drive in download_setup.drives:
        for directory in drive.directories:
            roots.append(directory)

    candidates: list[Path] = []
    seen: set[Path] = set()
    site_names = [site_title]
    for alias in download_setup.get_site_directory_aliases(site_name):
        if alias.casefold() not in {s.casefold() for s in site_names}:
            site_names.append(alias)

    for current_site_name in site_names:
        format_attrs.site_name = current_site_name
        for directory_format in _iter_download_directory_formats(config):
            formatter = PathFormatter(
                directory_format=directory_format,
                filename_format=download_setup.filename_format,
                date_format=download_setup.date_format,
                text_length=download_setup.text_length,
            )
            formatted = formatter.format_path(
                format_attrs,
                str(directory_format),
                download_setup.date_format,
                download_setup.text_length,
            )
            formatted_path = Path(formatted)
            for root in roots:
                candidate = root / formatted_path
                if candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)

    return candidates


def _resolve_media_path(
    config: UltimaScraperCollectionConfig,
    site_name: str,
    user_id: int,
    username: str | None,
    filenames: list[str],
    content_type: str,
    content_values: list[str],
    media_type: str,
    media_id: int,
    created_at: datetime | None,
    aliases: Iterable[str] | None = None,
) -> Path:
    usernames = _candidate_usernames(username, user_id, aliases)
    for candidate_username in usernames:
        for content_value in content_values:
            directories = _build_directory_candidates(
                config,
                site_name,
                user_id,
                candidate_username,
                content_type,
                content_value,
                media_type,
                media_id,
                created_at,
            )

            for directory in directories:
                for filename in filenames:
                    candidate = directory / filename
                    if candidate.exists() and candidate.is_file():
                        return candidate

    raise HTTPException(status_code=404, detail="File not found")


def _resolve_all_content_info(
    session: Session,
    media_id: int,
) -> list[tuple[str, int | None]]:
    stmt = select(ContentMediaAssoModel).where(
        ContentMediaAssoModel.media_id == media_id
    )
    associations = session.exec(stmt).all()
    seen: set[str] = set()
    results: list[tuple[str, int | None]] = []
    for assoc in associations:
        if assoc.post_id is not None and "posts" not in seen:
            seen.add("posts")
            results.append(("posts", assoc.post_id))
        if assoc.story_id is not None and "stories" not in seen:
            seen.add("stories")
            results.append(("stories", assoc.story_id))
        if assoc.message_id is not None and "messages" not in seen:
            seen.add("messages")
            results.append(("messages", assoc.message_id))
        if assoc.mass_message_id is not None and "mass_messages" not in seen:
            seen.add("mass_messages")
            results.append(("mass_messages", assoc.mass_message_id))
    return results


def _candidate_content_values(content_id: int | None) -> list[str]:
    candidates = ["Free", "Paid", "All"]
    if content_id is not None:
        candidates.append(str(content_id))
    return candidates


def _candidate_filenames(
    config: UltimaScraperCollectionConfig,
    site_name: str,
    user_id: int,
    media_id: int,
    media_url: str | None,
    media_category: str | None,
    created_at: object | None,
) -> list[str]:
    candidates: list[str] = []
    site_title = get_site_title(site_name)
    if media_url:
        candidates.append(_extract_filename_from_url(media_url))

    download_setup = config.settings.download_setup
    filename_format = str(download_setup.filename_format)
    if (
        "{media_id}" in filename_format
        or "{filename}" in filename_format
        or "{media_filename}" in filename_format
    ):
        media_filename = candidates[0] if candidates else ""
        name_parts = media_filename.rsplit(".", 1)
        stem = name_parts[0] if name_parts else ""
        ext = name_parts[1] if len(name_parts) == 2 else ""

        format_attrs = FormatAttributes()
        format_attrs.site_name = site_title
        format_attrs.user_id = str(user_id)
        format_attrs.user_username = ""
        format_attrs.first_username_letter = ""
        format_attrs.first_letter = ""
        format_attrs.first_id_digit = str(user_id)[0] if user_id else "0"
        format_attrs.content_type = ""
        format_attrs.content_value = ""
        format_attrs.media_type = media_category or ""
        format_attrs.content_id = ""
        format_attrs.media_id = str(media_id)
        format_attrs.media_filename = stem
        format_attrs.media_ext = ext
        if isinstance(created_at, datetime):
            format_attrs.content_date = created_at.strftime("%Y-%m-%d")
        else:
            format_attrs.content_date = ""
        format_attrs.content_text = ""
        format_attrs.authed_username = ""

        formatter = PathFormatter(
            directory_format=download_setup.directory_format,
            filename_format=download_setup.filename_format,
            date_format=download_setup.date_format,
            text_length=download_setup.text_length,
        )
        try:
            formatted = formatter.format_path(
                format_attrs,
                filename_format,
                download_setup.date_format,
                download_setup.text_length,
            )
            formatted_name = Path(formatted).name
            if formatted_name:
                candidates.append(formatted_name)
        except ValueError:
            pass

    # De-duplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for item in candidates:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def _build_user_directory_relpaths(
    config: UltimaScraperCollectionConfig,
    site_name: str,
    username: str,
) -> list[str]:
    download_setup = config.settings.download_setup
    rel_paths: list[str] = []
    seen: set[str] = set()
    first_letter = username[0].upper() if username else "0"
    site_title = get_site_title(site_name)

    for directory_format in _iter_download_directory_formats(config):
        formatter = PathFormatter(
            directory_format=directory_format,
            filename_format=download_setup.filename_format,
            date_format=download_setup.date_format,
            text_length=download_setup.text_length,
        )
        user_dir_format = formatter.get_user_directory_format() or str(directory_format)

        site_names = [site_title]
        for alias in download_setup.get_site_directory_aliases(site_name):
            if alias.casefold() not in {s.casefold() for s in site_names}:
                site_names.append(alias)

        replacement_sets: list[dict[str, str]] = [
            {
                "{site_name}": current_site_name,
                "{user_username}": username,
                "{model_username}": username,
                "{first_username_letter}": first_letter,
                "{first_letter}": first_letter,
                "{first_identifier_character}": first_letter,
                "{user_id}": "*",
                "{first_id_digit}": "*",
            }
            for current_site_name in site_names
        ]

        for replacements in replacement_sets:
            rel_path = user_dir_format
            for token, value in replacements.items():
                rel_path = rel_path.replace(token, value)
            if "{site_name}" not in user_dir_format and replacements["{site_name}"] != site_title:
                rel_path = rel_path.replace(site_title, replacements["{site_name}"])

            # Replace any custom/unknown token with wildcard to keep search flexible.
            rel_path = TOKEN_PATTERN.sub("*", rel_path)
            if rel_path in seen:
                continue
            seen.add(rel_path)
            rel_paths.append(rel_path)

    return rel_paths


def _decode_mountinfo_path(value: str) -> Path:
    return Path(
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _load_mount_points() -> set[Path]:
    mount_points: set[Path] = {Path("/")}
    try:
        with Path("/proc/self/mountinfo").open("r", encoding="utf-8") as mountinfo:
            for line in mountinfo:
                fields = line.split()
                if len(fields) >= 5:
                    mount_points.add(_decode_mountinfo_path(fields[4]))
    except OSError:
        logger.debug("Unable to read mountinfo for directory search", exc_info=True)
    return mount_points


def _path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _requires_explicit_mount(root: Path) -> bool:
    root_text = root.as_posix()
    return any(
        root_text == mount_root or root_text.startswith(f"{mount_root}/")
        for mount_root in EXPLICIT_MOUNT_ROOTS
    )


def _has_live_mount(root: Path, mount_points: set[Path]) -> bool:
    if not root.is_absolute() or not _requires_explicit_mount(root):
        return True

    return any(
        mount_point != Path("/")
        and (root == mount_point or _path_is_relative_to(root, mount_point))
        for mount_point in mount_points
    )


def _resolve_all_usernames(
    site_name: str,
    username: str | None,
    user_id: int | None,
) -> tuple[int | None, list[str]]:
    """Look up a user in the DB by username or user_id and return all known usernames."""
    normalized_username = username.strip().lstrip("@") if username else None
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_name.lower())
    if not site_db:
        return user_id, [normalized_username] if normalized_username else []

    with site_db.get_session() as session:
        db_user: UserModel | None = None

        if user_id is not None:
            db_user = session.exec(
                select(UserModel).where(UserModel.id == user_id)
            ).first()

        if db_user is None and normalized_username:
            db_user = session.exec(
                select(UserModel).where(
                    func.lower(UserModel.username) == normalized_username.lower()
                )
            ).first()

        if db_user is None and normalized_username:
            alias = session.exec(
                select(UserAliasModel).where(
                    func.lower(UserAliasModel.username) == normalized_username.lower()
                )
            ).first()
            if alias:
                db_user = session.exec(
                    select(UserModel).where(UserModel.id == alias.user_id)
                ).first()

        if db_user is None:
            return user_id, [normalized_username] if normalized_username else []

        resolved_user_id = db_user.id
        seen: set[str] = set()
        all_usernames: list[str] = []

        if db_user.username and db_user.username not in seen:
            seen.add(db_user.username)
            all_usernames.append(db_user.username)

        # Fetch aliases — reuse the alias we already found (if any) to avoid a
        # redundant query when the user was resolved through an alias lookup.
        alias_rows = session.exec(
            select(UserAliasModel.username).where(UserAliasModel.user_id == db_user.id)
        ).all()
        for alias_username in alias_rows:
            if alias_username and alias_username not in seen:
                seen.add(alias_username)
                all_usernames.append(alias_username)

        if not all_usernames and normalized_username:
            all_usernames = [normalized_username]

        return resolved_user_id, all_usernames


def _search_username_directories(
    config: UltimaScraperCollectionConfig,
    usernames: list[str],
    site_name: str,
) -> tuple[list[UsernameDirectoryResult], list[str], str]:
    checked_patterns: list[str] = []
    matches: list[UsernameDirectoryResult] = []
    rel_patterns = (
        _build_user_directory_relpaths(config, site_name, usernames[0])
        if usernames
        else []
    )
    rel_pattern = rel_patterns[0] if rel_patterns else ""
    matched_rel_pattern: str | None = None
    mount_points = _load_mount_points()

    for username in usernames:
        username_rel_patterns = _build_user_directory_relpaths(
            config, site_name, username
        )

        for username_rel_pattern in username_rel_patterns:
            for drive in config.settings.download_setup.drives:
                if not drive.scan_existing:
                    continue
                for root in drive.directories:
                    if not _has_live_mount(root, mount_points):
                        logger.info(
                            "Skipping unavailable mounted directory during user directory search: %s",
                            root,
                        )
                        continue
                    pattern_path = root / username_rel_pattern
                    pattern_str = str(pattern_path)
                    if pattern_str not in checked_patterns:
                        checked_patterns.append(pattern_str)

                    try:
                        if "*" in pattern_str:
                            for candidate in root.glob(username_rel_pattern):
                                if candidate.exists() and candidate.is_dir():
                                    if matched_rel_pattern is None:
                                        matched_rel_pattern = username_rel_pattern
                                    matches.append(
                                        UsernameDirectoryResult(
                                            root=str(root),
                                            path=str(candidate),
                                        )
                                    )
                        elif pattern_path.exists() and pattern_path.is_dir():
                            if matched_rel_pattern is None:
                                matched_rel_pattern = username_rel_pattern
                            matches.append(
                                UsernameDirectoryResult(
                                    root=str(root),
                                    path=str(pattern_path),
                                )
                            )
                    except OSError:
                        logger.warning(
                            "Unable to scan directory during user directory search: %s",
                            root,
                            exc_info=True,
                        )

    # De-duplicate by path in case multiple roots/drive configs overlap.
    deduped: dict[str, UsernameDirectoryResult] = {}
    for match in matches:
        deduped[match.path] = match

    return list(deduped.values()), checked_patterns, matched_rel_pattern or rel_pattern


def find_performer_media_file(
    config: UltimaScraperCollectionConfig,
    *,
    site_name: str,
    usernames: list[str],
    filename_stem: str,
    suffix: str = "",
) -> Path | None:
    """Find a renamed media file below the configured performer directories.

    This is intentionally a config-only helper: callers do not need a
    FastAPI request, backend manager, or an activated ``FilesystemManager``.
    It is used only after an exact database filepath has gone stale.
    """
    if not usernames or not filename_stem:
        return None

    directories, _, _ = _search_username_directories(config, usernames, site_name)
    pattern = f"*{glob.escape(filename_stem)}*"
    normalized_suffix = suffix.lower()

    fallback: Path | None = None
    for directory in directories:
        try:
            for candidate in Path(directory.path).rglob(pattern):
                if not candidate.is_file():
                    continue
                if normalized_suffix and candidate.suffix.lower() == normalized_suffix:
                    return candidate
                fallback = fallback or candidate
        except OSError:
            logger.warning(
                "Unable to scan performer directory for renamed media: %s",
                directory.path,
                exc_info=True,
            )
    return fallback


@router.get("/users/search", response_model=UsernameDirectorySearchResponse)
async def search_username_directories(
    username: str | None = Query(None, description="Username to find in directories"),
    user_id: int | None = Query(None, description="User ID to find in directories"),
    site: str = Query("onlyfans", description="Site key (e.g., onlyfans, fansly)"),
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> UsernameDirectorySearchResponse:
    normalized_username: str | None = username.strip().lstrip("@") if username else None
    if not normalized_username and user_id is None:
        raise HTTPException(status_code=400, detail="username or user_id is required")

    resolved_user_id, all_usernames = _resolve_all_usernames(
        site_name=site.lower(),
        username=normalized_username,
        user_id=user_id,
    )

    if not all_usernames:
        raise HTTPException(status_code=404, detail="User not found")

    matches, checked_patterns, rel_pattern = await asyncio.to_thread(
        _search_username_directories,
        config,
        all_usernames,
        site.lower(),
    )

    return UsernameDirectorySearchResponse(
        success=True,
        username=all_usernames[0],
        site=site.lower(),
        user_directory_format=rel_pattern,
        matches=matches,
        checked_patterns=checked_patterns,
        usernames_searched=all_usernames,
        resolved_user_id=resolved_user_id,
    )


@router.get("/")
@router.head("/")
async def get_file(
    request: Request,
    site_name: str = Query(..., description="Site name"),
    media_id: int = Query(..., description="Media ID"),
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend_manager: BackendManager = Depends(get_backend_manager),
):
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(site_name.lower())

    with site_db.get_session() as session:
        media = session.exec(
            select(MediaModel).where(MediaModel.id == media_id)
        ).first()
        if not media:
            raise HTTPException(status_code=404, detail="Media not found")

        user = session.exec(
            select(UserModel).where(UserModel.id == media.user_id)
        ).first()
        username = user.username if user and user.username else None
        all_content_info = _resolve_all_content_info(session, media_id)

    if not media.url:
        raise HTTPException(status_code=404, detail="Media URL not found")

    filenames = _candidate_filenames(
        config,
        site_name.lower(),
        media.user_id,
        media_id,
        media.url,
        getattr(media, "category", None),
        getattr(media, "created_at", None),
    )
    if not filenames:
        raise HTTPException(status_code=404, detail="Media filename not found")

    media_type = getattr(media, "category", None) or ""
    created_at = getattr(media, "created_at", None)

    # Try each content type association until a file is found on disk
    content_entries = all_content_info or [("", None)]
    file_path: Path | None = None
    for content_type, content_id in content_entries:
        content_values = _candidate_content_values(content_id)
        try:
            file_path = _resolve_media_path(
                config,
                site_name.lower(),
                media.user_id,
                username,
                filenames,
                content_type,
                content_values,
                media_type,
                media_id,
                created_at if isinstance(created_at, datetime) else None,
            )
            break
        except HTTPException:
            continue
    if file_path is None:
        logger.info("File not resolved for media %d – falling back to proxy", media_id)
        return await _proxy_fallback(media.url, site_name, backend_manager)

    if not file_path.exists() or not file_path.is_file():
        logger.info("File not on disk for media %d – falling back to proxy", media_id)
        return await _proxy_fallback(media.url, site_name, backend_manager)

    file_size = file_path.stat().st_size
    mime_type, _ = mimetypes.guess_type(file_path.name)
    mime_type = mime_type or "application/octet-stream"

    headers = {
        "Content-Disposition": f'inline; filename="{file_path.name}"',
        "Accept-Ranges": "bytes",
        "Content-Length": str(file_size),
    }

    if request.method == "HEAD":
        return Response(
            content=None,
            status_code=200,
            headers=headers,
            media_type=mime_type,
        )

    range_header = request.headers.get("Range")
    if range_header:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            start_byte = int(range_match.group(1))
            end_byte_str = range_match.group(2)
            end_byte = int(end_byte_str) if end_byte_str else file_size - 1

            if start_byte >= file_size:
                raise HTTPException(
                    status_code=416, detail="Request range not satisfiable"
                )

            end_byte = min(end_byte, file_size - 1)
            content_length = end_byte - start_byte + 1

            headers["Content-Length"] = str(content_length)
            headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"

            return StreamingResponse(
                iterfile(file_path, start=start_byte, size=content_length),
                status_code=206,
                headers=headers,
                media_type=mime_type,
            )

    return StreamingResponse(
        iterfile(file_path, start=0, size=file_size),
        headers=headers,
        media_type=mime_type,
    )
