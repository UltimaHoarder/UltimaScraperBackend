"""Drive selection utilities for download tasks.

Phase 3 introduces storage logic that selects the correct download drive based on
user configuration, available space, and existing performer folders.
"""

from __future__ import annotations

import logging
import os
import re
import stat
import shutil
from contextvars import ContextVar
from dataclasses import dataclass, replace
from functools import cached_property
from pathlib import Path
from typing import Callable, Iterable, Literal, Mapping, Tuple

from ultima_scraper_backend.config import (
    DownloadPathSetup,
    Drive,
    UltimaScraperCollectionConfig,
)
from ultima_scraper_backend.job_system.path_formatter import PathFormatter

__all__ = [
    "DriveSelectionError",
    "DriveSelectionResult",
    "DriveSelector",
    "UserDirectorySelection",
    "has_usable_download_root",
    "select_download_destination",
    "select_user_destination",
    "select_user_directory",
]

logger = logging.getLogger(__name__)


class DriveSelectionError(RuntimeError):
    """Raised when no valid download drive can be selected."""


def _sanitize_segment(segment: str) -> str:
    cleaned = re.sub(r"[\\/:]+", "_", str(segment).strip())
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", cleaned)
    return cleaned or "_"


def _site_directory_segment(site_name: str) -> str:
    # Site aliases are literal existing directory names, including spaces and
    # parentheses. Only remove path separators and traversal segments.
    cleaned = re.sub(r"[\\/:]+", "_", str(site_name).strip())
    return cleaned if cleaned and cleaned not in {".", ".."} else "_"


@dataclass(frozen=True)
class DriveSelectionResult:
    """Represents the chosen drive and relative directory for downloads."""

    root: Path
    relative_path: Path
    directory_format: Path = Path()
    performer_username: str = ""

    @cached_property
    def absolute_path(self) -> Path:
        return (self.root / self.relative_path).resolve()


@dataclass(frozen=True)
class UserDirectorySelection:
    """Represents the chosen drive and performer root for user-scoped downloads."""

    root: Path
    performer_relative_path: Path
    directory_format: Path = Path()
    performer_username: str = ""
    requires_full_directory_render: bool = False
    site_name: str = ""
    # The performer's folder under a find-only alias: read for finished files only.
    kept_root: Path | None = None
    kept_relative_path: Path | None = None
    kept_site_name: str = ""

    @cached_property
    def absolute_path(self) -> Path:
        return (self.root / self.performer_relative_path).resolve()

    def kept_directory_for(self, directory: Path, base: Path) -> Path | None:
        """Where ``directory``, under the download ``base``, sits in the find-only folder."""
        if (
            self.kept_root is None
            or self.kept_relative_path is None
            or not directory.is_relative_to(base)
        ):
            return None
        relative = directory.relative_to(base)
        if self.requires_full_directory_render:
            return self.kept_root / Path(
                *(
                    self.kept_site_name if part == self.site_name else part
                    for part in relative.parts
                )
            )
        return self.kept_root / self.kept_relative_path / relative


@dataclass(frozen=True)
class _DriveDirectory:
    """Internal helper to associate a directory path with its parent drive settings."""

    path: Path
    drive: Drive


@dataclass(frozen=True)
class _DriveRootProbe:
    """Internal helper describing a usable root and the path used for disk checks."""

    root: Path
    disk_usage_path: Path


class DriveSelector:
    """Encapsulates drive selection rules derived from the project requirements."""

    def __init__(self, config: UltimaScraperCollectionConfig) -> None:
        self.config = config

    def select(
        self,
        *,
        site_name: str,
        performer_username: str,
        user_id: int | None = None,
        api_type: str,
        value: str,
        media_type: str,
        required_space: int = 0,
        ignore_space_requirements: bool = False,
    ) -> DriveSelectionResult:
        site_key = site_name.lower()
        if not hasattr(self.config.site_apis, site_key):
            raise DriveSelectionError(f"Site '{site_name}' not configured")

        setup: DownloadPathSetup = self.config.settings.download_setup
        return select_download_destination(
            setup,
            site_name=site_name,
            performer_username=performer_username,
            user_id=user_id,
            api_type=api_type,
            value=value,
            media_type=media_type,
            required_space=required_space,
            ignore_space_requirements=ignore_space_requirements,
        )


def select_download_destination(
    setup: DownloadPathSetup,
    *,
    site_name: str,
    performer_username: str,
    user_id: int | None = None,
    api_type: str,
    value: str,
    media_type: str,
    required_space: int = 0,
    ignore_space_requirements: bool = False,
) -> DriveSelectionResult:
    drive_directories = _flatten_drive_directories(setup)
    if not drive_directories:
        raise DriveSelectionError("No download directories configured")

    selection = _select_user_destination_from_setup(
        setup,
        drive_directories=drive_directories,
        user_id=user_id,
        usernames=[performer_username],
        site_name=site_name,
        required_space=required_space,
        ignore_space_requirements=ignore_space_requirements,
    )
    tokens = _build_path_tokens(
        site_name=selection.site_name or site_name,
        performer_username=selection.performer_username or performer_username,
        user_id=user_id,
        api_type=api_type,
        value=value,
        media_type=media_type,
    )

    if selection.requires_full_directory_render:
        relative_path = _format_directory(selection.directory_format, tokens)
    else:
        formatter = PathFormatter(
            directory_format=selection.directory_format,
            filename_format=Path(),
        )
        file_path_format = formatter.get_file_path_format()
        relative_path = selection.performer_relative_path
        if file_path_format:
            suffix = _format_directory(Path(file_path_format), tokens)
            if suffix.parts:
                relative_path = relative_path / suffix

    return DriveSelectionResult(
        root=selection.root,
        relative_path=relative_path,
        directory_format=selection.directory_format,
        performer_username=selection.performer_username,
    )


def select_user_destination(
    *,
    user_id: int,
    usernames: Iterable[str],
    site_name: str,
    user_manager: object | None,
    config: UltimaScraperCollectionConfig | None = None,
    required_space: int = 0,
    ignore_space_requirements: bool = False,
) -> UserDirectorySelection:
    """Select the destination drive and directory format for a user's downloads."""

    setup = _resolve_download_setup(user_manager, config=config)
    drive_directories = _flatten_drive_directories(setup)
    if not drive_directories:
        raise DriveSelectionError("No download directories configured")

    return _select_user_destination_from_setup(
        setup,
        drive_directories=drive_directories,
        user_id=user_id,
        usernames=usernames,
        site_name=site_name,
        required_space=required_space,
        ignore_space_requirements=ignore_space_requirements,
    )


def select_user_directory(
    *,
    user_id: int,
    usernames: Iterable[str],
    site_name: str,
    user_manager: object | None,
    config: UltimaScraperCollectionConfig | None = None,
    required_space: int = 0,
    ignore_space_requirements: bool = False,
) -> Path:
    """Select a performer root directory for downloads.

    This is a compatibility wrapper for legacy callers that expect a single
    performer directory like ``site/first_letter/username`` rather than the
    full formatted content path.
    """

    return select_user_destination(
        user_id=user_id,
        usernames=usernames,
        site_name=site_name,
        user_manager=user_manager,
        config=config,
        required_space=required_space,
        ignore_space_requirements=ignore_space_requirements,
    ).absolute_path


def _resolve_download_setup(
    user_manager: object | None,
    *,
    config: UltimaScraperCollectionConfig | None = None,
) -> DownloadPathSetup:
    if isinstance(config, UltimaScraperCollectionConfig):
        return config.settings.download_setup

    if user_manager is not None:
        direct_config = getattr(user_manager, "config", None)
        if isinstance(direct_config, UltimaScraperCollectionConfig):
            config = direct_config
        else:
            storage_manager = getattr(user_manager, "storage_manager", None)
            storage_config = getattr(storage_manager, "config", None)
            if isinstance(storage_config, UltimaScraperCollectionConfig):
                config = storage_config

    if config is None:
        config = UltimaScraperCollectionConfig.load_or_create_default_config()

    return config.settings.download_setup


def _flatten_drive_directories(setup: DownloadPathSetup) -> list[_DriveDirectory]:
    drive_directories: list[_DriveDirectory] = []
    for drive in setup.drives:
        for directory_path in drive.directories:
            if directory_path:
                drive_directories.append(
                    _DriveDirectory(path=directory_path, drive=drive)
                )
    return drive_directories


# Set while only checking whether any drive works, which repeats every few
# seconds per worker: a drive missing on this host is expected, not news.
_QUIET_DRIVE_PROBE: ContextVar[bool] = ContextVar("quiet_drive_probe", default=False)


def has_usable_download_root(setup: DownloadPathSetup) -> bool:
    """Whether this host can write to at least one configured download drive."""
    token = _QUIET_DRIVE_PROBE.set(True)
    try:
        return any(
            drive_dir.drive.store
            and _prepare_drive_root_for_storage(drive_dir.path) is not None
            for drive_dir in _flatten_drive_directories(setup)
        )
    finally:
        _QUIET_DRIVE_PROBE.reset(token)


def directory_writable_here(path: Path) -> bool:
    """Whether this host can write under ``path``: an unmounted /mnt/<name> is not."""
    token = _QUIET_DRIVE_PROBE.set(True)
    try:
        return _prepare_drive_root_for_storage(path) is not None
    finally:
        _QUIET_DRIVE_PROBE.reset(token)


def _log_unavailable_drive_root(root: Path, reason: object) -> None:
    if _QUIET_DRIVE_PROBE.get():
        return
    logger.warning("Skipping unavailable download root %s: %s", root, reason)


def _directory_state(path: Path) -> Literal["existing", "missing", "unavailable"]:
    try:
        path_stat = path.stat()
    except FileNotFoundError:
        return "missing"
    except OSError as exc:
        _log_unavailable_drive_root(path, exc)
        return "unavailable"

    if not stat.S_ISDIR(path_stat.st_mode):
        _log_unavailable_drive_root(path, "not a directory")
        return "unavailable"
    return "existing"


def _has_directory_access(path: Path, *, require_write: bool) -> bool:
    mode = os.X_OK | (os.W_OK if require_write else os.R_OK)
    return os.access(path, mode)


def _expected_mount_root(path: Path) -> Path | None:
    parts = path.parts
    if len(parts) >= 3 and parts[0] == "/" and parts[1] == "mnt":
        return Path(parts[0], parts[1], parts[2])
    return None


def _nearest_existing_directory(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if _directory_state(candidate) == "existing":
            return candidate
    return None


def _prepare_drive_root_for_scan(path: Path) -> Path | None:
    root = path.expanduser()
    if _directory_state(root) != "existing":
        return None
    if _has_directory_access(root, require_write=False):
        return root

    _log_unavailable_drive_root(root, "missing read/execute access")
    return None


def _prepare_drive_root_for_storage(path: Path) -> _DriveRootProbe | None:
    root = path.expanduser()
    root_state = _directory_state(root)
    if root_state == "existing":
        if _has_directory_access(root, require_write=True):
            return _DriveRootProbe(root=root, disk_usage_path=root)

        _log_unavailable_drive_root(root, "missing write/execute access")
        return None

    if root_state == "unavailable":
        return None

    expected_mount = _expected_mount_root(root)
    if expected_mount is not None and not expected_mount.is_mount():
        _log_unavailable_drive_root(root, f"mount {expected_mount} is not available")
        return None

    nearest_parent = _nearest_existing_directory(root.parent)
    if nearest_parent is None:
        _log_unavailable_drive_root(root, "no accessible parent directory")
        return None
    if _has_directory_access(nearest_parent, require_write=True):
        return _DriveRootProbe(root=root, disk_usage_path=nearest_parent)

    _log_unavailable_drive_root(
        root,
        f"parent directory {nearest_parent} is not writable/executable",
    )
    return None


def _safe_path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False


def _normalize_usernames(user_id: int | None, usernames: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []

    for username in usernames:
        cleaned = str(username).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)

    fallback_username = f"u{user_id}" if user_id and user_id > 0 else "unknown"
    if fallback_username not in seen:
        normalized.append(fallback_username)

    if not normalized:
        normalized.append(fallback_username)

    return normalized


def _build_legacy_performer_relative(
    site_name: str, username: str, *, uppercase: bool = True
) -> Path:
    username_token = _sanitize_segment(username or "unknown")
    first_letter = username_token[0] if username_token else "_"
    first_letter = first_letter.upper() if uppercase else first_letter.lower()
    return Path(
        _site_directory_segment(site_name or "unknown"), first_letter, username_token
    )


def _build_path_tokens(
    *,
    site_name: str,
    performer_username: str,
    user_id: int | None = None,
    api_type: str,
    value: str,
    media_type: str,
    uppercase_first_letter: bool = True,
) -> Mapping[str, str]:
    username = performer_username.strip() or (
        f"u{user_id}" if user_id and user_id > 0 else "unknown"
    )
    first_letter = username[0] if username else "_"
    normalized_first_letter = (
        first_letter.upper() if uppercase_first_letter else first_letter.lower()
    )
    user_id_token = str(user_id) if user_id is not None and user_id >= 0 else ""

    return {
        "site_name": _site_directory_segment(site_name or "unknown"),
        "first_letter": _sanitize_segment(normalized_first_letter),
        "first_username_letter": _sanitize_segment(normalized_first_letter),
        "model_username": _sanitize_segment(username),
        "user_username": _sanitize_segment(username),
        "user_id": _sanitize_segment(user_id_token) if user_id_token else "",
        "first_id_digit": user_id_token[0] if user_id_token else "",
        "api_type": _sanitize_segment(api_type or "default"),
        "content_type": _sanitize_segment(api_type or "default"),
        "value": _sanitize_segment(value or "default"),
        "content_value": _sanitize_segment(value or "default"),
        "media_type": _sanitize_segment(media_type or "content"),
    }


def _format_directory(format_path: Path, tokens: Mapping[str, str]) -> Path:
    formatted = str(format_path) if format_path else ""
    if ("{user_id}" in formatted or "{first_id_digit}" in formatted) and not tokens.get(
        "user_id"
    ):
        raise DriveSelectionError("Directory format requires a user_id")
    try:
        rendered = formatted.format(**tokens)
    except KeyError as exc:  # pragma: no cover - defensive
        missing = exc.args[0]
        raise DriveSelectionError(
            f"Directory format missing token '{missing}'"
        ) from exc

    if not rendered:
        rendered = tokens["model_username"]

    parts = [part for part in Path(rendered).parts if part and part != "."]
    return Path(*parts)


def _select_user_destination_from_setup(
    setup: DownloadPathSetup,
    *,
    drive_directories: list[_DriveDirectory],
    user_id: int | None,
    usernames: Iterable[str],
    site_name: str,
    required_space: int,
    ignore_space_requirements: bool = False,
) -> UserDirectorySelection:
    normalized_usernames = _normalize_usernames(user_id, usernames)
    find_only = {
        name.casefold() for name in setup.get_find_only_site_directories(site_name)
    }

    def in_find_only(relative: Path) -> bool:
        return any(part.casefold() in find_only for part in relative.parts)

    listings: dict[Path, list[str]] = {}

    def find(accepts: Callable[[Path], bool]) -> UserDirectorySelection | None:
        return _find_existing_user_directory(
            drive_directories=drive_directories,
            directory_formats=[setup.directory_format, *setup.alt_directory_formats],
            site_name=site_name,
            usernames=normalized_usernames,
            user_id=user_id,
            site_aliases=setup.get_site_directory_aliases(site_name),
            accepts=accepts,
            listings=listings,
        )

    existing = find(lambda relative: not in_find_only(relative))
    # Also once the new folder exists, or files not yet reached fetch again.
    kept = find(in_find_only) if find_only else None
    kept_fields = (
        {
            "kept_root": kept.root,
            "kept_relative_path": kept.performer_relative_path,
            "kept_site_name": kept.site_name,
        }
        if kept is not None
        else {}
    )
    if existing is not None:
        return replace(existing, **kept_fields)

    selected_username = normalized_usernames[0]
    selected_root = _select_root_by_capacity(
        drive_directories,
        # Much of a performer's selection may already be in the find-only folder.
        required_space=0 if kept is not None else required_space,
        ignore_space_requirements=ignore_space_requirements,
    )
    if selected_root is None:
        raise DriveSelectionError("No download directories satisfy space requirements")

    return UserDirectorySelection(
        root=selected_root,
        performer_relative_path=_build_preferred_performer_relative(
            setup.directory_format,
            site_name=site_name,
            performer_username=selected_username,
            user_id=user_id,
        ),
        directory_format=setup.directory_format,
        performer_username=selected_username,
        requires_full_directory_render=_requires_full_directory_render(
            setup.directory_format
        ),
        site_name=site_name,
        **kept_fields,
    )


def _find_existing_user_directory(
    *,
    drive_directories: Iterable[_DriveDirectory],
    directory_formats: Iterable[Path],
    site_name: str,
    usernames: Iterable[str],
    user_id: int | None,
    site_aliases: Iterable[str] | None = None,
    accepts: Callable[[Path], bool] = lambda relative: True,
    listings: dict[Path, list[str]] | None = None,
) -> UserDirectorySelection | None:
    # The same folders come up for every alias, format and letter case:
    # list each once per lookup, which matters on slow network drives.
    if listings is None:
        listings = {}
    searchable_roots = [
        _prepare_drive_root_for_scan(drive_dir.path)
        for drive_dir in drive_directories
        if drive_dir.drive.scan_existing
    ]
    searchable_roots = [root for root in searchable_roots if root is not None]

    all_site_names = [site_name]
    if site_aliases:
        for alias in site_aliases:
            if alias.casefold() not in {s.casefold() for s in all_site_names}:
                all_site_names.append(alias)

    for directory_format in directory_formats:
        performer_root_format = _get_performer_root_format(directory_format)
        requires_full_directory_render = _requires_full_directory_render(
            directory_format
        )

        for current_site_name in all_site_names:
            for username in usernames:
                if performer_root_format is None:
                    performer_candidates = _build_legacy_performer_candidates(
                        site_name=current_site_name,
                        performer_username=username,
                    )
                    for performer_relative in performer_candidates:
                        for root in searchable_roots:
                            performer_folder = root / performer_relative
                            if accepts(performer_relative) and _safe_path_exists(
                                performer_folder
                            ):
                                return UserDirectorySelection(
                                    root=root,
                                    performer_relative_path=performer_relative,
                                    directory_format=directory_format,
                                    performer_username=username,
                                    requires_full_directory_render=requires_full_directory_render,
                                    site_name=current_site_name,
                                )
                    continue

                template_parts = [
                    part
                    for part in Path(str(performer_root_format)).parts
                    if part and part != "."
                ]
                seen: set[Path] = set()
                for uppercase in (True, False):
                    tokens = _build_performer_lookup_tokens(
                        site_name=current_site_name,
                        performer_username=username,
                        user_id=user_id,
                        uppercase_first_letter=uppercase,
                    )
                    for root in searchable_roots:
                        performer_relative = _find_matching_relative_path(
                            root, template_parts, tokens, listings
                        )
                        if (
                            performer_relative is None
                            or performer_relative in seen
                            or not accepts(performer_relative)
                        ):
                            continue
                        seen.add(performer_relative)
                        return UserDirectorySelection(
                            root=root,
                            performer_relative_path=performer_relative,
                            directory_format=directory_format,
                            performer_username=username,
                            requires_full_directory_render=requires_full_directory_render,
                            site_name=current_site_name,
                        )
    return None


def _build_legacy_performer_candidates(
    *,
    site_name: str,
    performer_username: str,
) -> list[Path]:
    upper = _build_legacy_performer_relative(
        site_name, performer_username, uppercase=True
    )
    lower = _build_legacy_performer_relative(
        site_name, performer_username, uppercase=False
    )
    return [upper] if upper == lower else [upper, lower]


def _build_performer_lookup_tokens(
    *,
    site_name: str,
    performer_username: str,
    user_id: int | None,
    uppercase_first_letter: bool,
) -> dict[str, str]:
    tokens = dict(
        _build_path_tokens(
            site_name=site_name,
            performer_username=performer_username,
            user_id=user_id,
            api_type="",
            value="",
            media_type="",
            uppercase_first_letter=uppercase_first_letter,
        )
    )
    for token_name in (
        "api_type",
        "content_type",
        "value",
        "content_value",
        "media_type",
    ):
        tokens[token_name] = ""
    return tokens


def _child_directories(parent: Path, listings: dict[Path, list[str]] | None) -> list[str]:
    """Subfolder names, sorted; scandir reads each entry's type from the listing
    itself instead of a stat per entry, so a big network folder lists in one call."""
    if listings is not None and parent in listings:
        return listings[parent]
    children: list[str] = []
    try:
        with os.scandir(parent) as entries:
            for entry in entries:
                try:
                    if entry.is_dir():
                        children.append(entry.name)
                except OSError:
                    continue
    except OSError:
        pass
    children.sort()
    if listings is not None:
        listings[parent] = children
    return children


def _find_matching_relative_path(
    root: Path,
    template_parts: list[str],
    tokens: Mapping[str, str],
    listings: dict[Path, list[str]] | None = None,
) -> Path | None:
    current_matches = [Path()]
    for template_part in template_parts:
        next_matches: list[Path] = []
        matcher = _compile_segment_matcher(template_part, tokens)
        for current in current_matches:
            parent = root / current
            for child_name in _child_directories(parent, listings):
                if matcher.fullmatch(child_name):
                    next_matches.append(
                        current / child_name if current != Path() else Path(child_name)
                    )
        if not next_matches:
            return None
        current_matches = next_matches
    return current_matches[0] if current_matches else None


def _compile_segment_matcher(
    template_part: str,
    tokens: Mapping[str, str],
) -> re.Pattern[str]:
    pattern_parts: list[str] = []
    position = 0
    for match in re.finditer(r"\{([^}]+)\}", template_part):
        pattern_parts.append(re.escape(template_part[position : match.start()]))
        token_name = match.group(1)
        token_value = tokens.get(token_name)
        if token_name in {"user_id", "first_id_digit"} and not token_value:
            return re.compile(r"(?!)")
        if token_value:
            pattern_parts.append(re.escape(token_value))
        else:
            pattern_parts.append(r"[^/]+")
        position = match.end()
    pattern_parts.append(re.escape(template_part[position:]))
    return re.compile("^" + "".join(pattern_parts) + "$")


def _get_performer_root_format(directory_format: Path) -> Path | None:
    formatter = PathFormatter(directory_format=directory_format, filename_format=Path())
    user_directory_format = formatter.get_user_directory_format()
    if not user_directory_format:
        return None
    return Path(user_directory_format)


def _build_preferred_performer_relative(
    directory_format: Path,
    *,
    site_name: str,
    performer_username: str,
    user_id: int | None,
) -> Path:
    performer_root_format = _get_performer_root_format(directory_format)
    if performer_root_format is None:
        return _build_legacy_performer_relative(
            site_name, performer_username, uppercase=True
        )

    try:
        return _format_directory(
            performer_root_format,
            _build_path_tokens(
                site_name=site_name,
                performer_username=performer_username,
                user_id=user_id,
                api_type="default",
                value="default",
                media_type="content",
            ),
        )
    except DriveSelectionError:
        return _build_legacy_performer_relative(
            site_name, performer_username, uppercase=True
        )


def _requires_full_directory_render(directory_format: Path) -> bool:
    performer_root_format = _get_performer_root_format(directory_format)
    if performer_root_format is None:
        return False

    allowed_root_tokens = {
        "site_name",
        "first_letter",
        "first_username_letter",
        "model_username",
        "user_username",
        "user_id",
        "first_id_digit",
    }
    root_template = str(performer_root_format)
    return any(
        match.group(1) not in allowed_root_tokens
        for match in re.finditer(r"\{([^}]+)\}", root_template)
    )


def _select_root_by_capacity(
    drive_directories: Iterable[_DriveDirectory],
    *,
    required_space: int,
    ignore_space_requirements: bool = False,
) -> Path | None:
    best_primary: Tuple[int, _DriveRootProbe] | None = None
    best_overflow: Tuple[int, _DriveRootProbe] | None = None
    for drive_dir in drive_directories:
        if not drive_dir.drive.store:
            continue

        prepared_root = _prepare_drive_root_for_storage(drive_dir.path)
        if prepared_root is None:
            continue

        try:
            usage = shutil.disk_usage(prepared_root.disk_usage_path)
        except OSError as exc:
            _log_unavailable_drive_root(prepared_root.root, exc)
            continue

        free_bytes = usage.free
        min_space_bytes = _minimum_space_bytes(drive_dir.drive.minimum_space)
        below_minimum = min_space_bytes is not None and free_bytes < min_space_bytes
        if not ignore_space_requirements:
            if below_minimum and not drive_dir.drive.overflow:
                continue
            if required_space and free_bytes < required_space:
                continue

        candidate = (free_bytes, prepared_root)
        if drive_dir.drive.overflow:
            if best_overflow is None or free_bytes > best_overflow[0]:
                best_overflow = candidate
        elif best_primary is None or free_bytes > best_primary[0]:
            best_primary = candidate

    best = best_primary or best_overflow
    if not best:
        return None

    _, selected_root = best
    return selected_root.root


def _minimum_space_bytes(minimum_space_bytes: int) -> int | None:
    if minimum_space_bytes < 0:
        return None
    return minimum_space_bytes
