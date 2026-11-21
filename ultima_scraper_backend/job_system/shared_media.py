"""Reuse a finished copy of media another post, story or message already downloaded.

OnlyFans lets a creator attach one upload to several pieces of content: a video
can be in a post and in a story kept as a highlight. Each content type has its
own folder, so each download task used to fetch the same file again (and for a
DRM video, ask for another licence). The archive already records every piece
of content a media id belongs to (``content_media_asso``). Before a batch makes
any network request, one query reads those for the whole batch, and each file
another content type already saved is hardlinked into place (copied when it is
on another drive). The batch then sees it as already on disk.
"""

from __future__ import annotations

import errno
import logging
import os
import shutil
import threading
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import UltimaScraperCollectionConfig
    from .download_manager import DownloadItem

logger = logging.getLogger(__name__)

# The folder each kind of content renders as ({content_type} in the path format).
CONTENT_FOLDERS: dict[str, tuple[str, ...]] = {
    "post": ("Posts", "Archived/Posts"),
    "story": ("Stories",),
    "message": ("Messages",),
    "mass_message": ("Messages",),
}
CONTENT_VALUES = ("Free", "Paid", "Previews")
TAIL_TOKENS = frozenset({"{content_type}", "{content_value}", "{media_type}"})

AssociationLookup = Callable[[list[int]], dict[int, frozenset[str]]]


def content_kind(api_type: str | None) -> str | None:
    """Which association column a content type's media hangs off."""
    value = str(api_type or "").lower()
    if "message" in value:
        return "message"
    if "stor" in value or "highlight" in value:
        return "story"
    if "post" in value:
        return "post"
    return None


def performer_tail_format(directory_format: Path | str) -> str | None:
    """The part of the directory format below the performer's folder.

    Only a tail made of {content_type}, {content_value} and {media_type}, ending
    in {media_type}, is reusable: anything else (a date, a content id) names a
    folder this module cannot predict for another piece of content.
    """
    text = str(directory_format).replace("\\", "/")
    marker = "{user_username}"
    if marker not in text:
        return None
    tail = text.split(marker, 1)[1].strip("/")
    parts = [part for part in tail.split("/") if part]
    if not parts or parts[-1] != "{media_type}":
        return None
    if any(part not in TAIL_TOKENS for part in parts):
        return None
    return "/".join(parts)


def _render_tail(tail: str, content_type: str, content_value: str, media_folder: str) -> str:
    return (
        tail.replace("{content_type}", content_type)
        .replace("{content_value}", content_value)
        .replace("{media_type}", media_folder)
    )


def _kept_directories(item: DownloadItem) -> list[Path]:
    kept = getattr(item, "kept_directories", None)
    if kept is None:
        single = getattr(item, "kept_directory", None)
        kept = [single] if single is not None else []
    return list(kept)


def _performer_folder(directory: Path, tail: str, content_type: str) -> Path | None:
    """The performer's folder an item directory was rendered under."""
    depth = len(tail.split("/"))
    if "{content_type}" in tail:
        depth += len(Path(content_type).parts) - 1
    if depth <= 0 or len(directory.parts) <= depth:
        return None
    return directory.parents[depth - 1]


def _has_content(path: Path) -> int:
    try:
        stat = path.stat()
    except OSError:
        return 0
    return stat.st_size if stat.st_size > 0 else 0


def place_existing_copy(source: Path, dest: Path) -> tuple[str, int] | None:
    """Hardlink *source* at *dest*, or copy it when a link can't be made.

    Returns how it was placed and the size, or None when nothing was placed
    (the caller then downloads as usual).
    """
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, dest)
        return "linked", dest.stat().st_size
    except FileExistsError:
        size = _has_content(dest)
        return ("exists", size) if size else None
    except OSError as exc:
        if exc.errno not in (
            errno.EXDEV,
            errno.EPERM,
            errno.EMLINK,
            errno.ENOTSUP,
            errno.EOPNOTSUPP,
            errno.EACCES,
        ):
            logger.warning("Could not link %s to %s: %s", source, dest, exc)
            return None
    temporary = dest.with_name(dest.name + ".copy")
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, dest)
        return "copied", dest.stat().st_size
    except OSError as exc:
        logger.warning("Could not copy %s to %s: %s", source, dest, exc)
        temporary.unlink(missing_ok=True)
        return None


class SharedMediaCopies:
    """Finds a finished copy of an item's media under another content type."""

    def __init__(
        self,
        *,
        tail_format: str | None,
        associations: AssociationLookup,
    ) -> None:
        self.tail_format = tail_format
        self._associations = associations
        self._lock = threading.Lock()
        self._kinds: dict[int, frozenset[str]] = {}

    def prefetch(self, items: Iterable[DownloadItem]) -> None:
        """Read the content every item's media belongs to, in one query."""
        if self.tail_format is None:
            return
        with self._lock:
            wanted = sorted(
                {
                    item.media_id
                    for item in items
                    if item.media_id is not None and item.media_id not in self._kinds
                }
            )
        if not wanted:
            return
        found = self._associations(wanted)
        with self._lock:
            for media_id in wanted:
                self._kinds[media_id] = found.get(media_id, frozenset())

    def candidates(self, item: DownloadItem) -> Iterable[Path]:
        if self.tail_format is None or item.media_id is None:
            return
        if not item.directory or not item.filename:
            return
        metadata = item.media_metadata
        content = metadata.content_metadata if metadata is not None else None
        own_type = str(getattr(content, "api_type", None) or "")
        own_kind = content_kind(own_type)
        self.prefetch([item])
        with self._lock:
            kinds = self._kinds.get(item.media_id, frozenset())
        # Media in this kind of content only: nothing else would have saved it.
        if not kinds - {own_kind}:
            return
        dest = item.get_filepath()
        media_folder = item.directory.name
        folders: list[Path] = []
        for directory in [item.directory, *_kept_directories(item)]:
            folder = _performer_folder(directory, self.tail_format, own_type)
            if folder is not None and folder not in folders:
                folders.append(folder)
        seen: set[Path] = {dest}
        for folder in folders:
            for kind in sorted(kinds):
                for content_type in CONTENT_FOLDERS.get(kind, ()):
                    for content_value in CONTENT_VALUES:
                        tail = _render_tail(
                            self.tail_format, content_type, content_value, media_folder
                        )
                        candidate = folder / tail / item.filename
                        if candidate not in seen:
                            seen.add(candidate)
                            yield candidate

    def __call__(self, item: DownloadItem) -> Path | None:
        expected = None
        if item.size and not (item.media_metadata and item.media_metadata.drm):
            expected = item.size
        for candidate in self.candidates(item):
            size = _has_content(candidate)
            if size and (expected is None or size == expected):
                return candidate
        return None


def reuse_shared_copies(items: Iterable[DownloadItem], finder: SharedMediaCopies) -> dict[str, int]:
    """Place every item another content type already saved; no network involved.

    Runs before a batch's link refresh, HEAD requests and DRM licences, so
    the items it places are skipped as already on disk.
    """
    pending = [
        item
        for item in items
        if item.directory and item.filename and not _has_content(item.get_filepath())
    ]
    counts: dict[str, int] = {}
    if not pending:
        return counts
    finder.prefetch(pending)
    for item in pending:
        source = finder(item)
        if source is None:
            continue
        placed = place_existing_copy(source, item.get_filepath())
        if placed is None:
            continue
        how, _size = placed
        counts[how] = counts.get(how, 0) + 1
        logger.debug("%s media %s from %s", how.capitalize(), item.media_id, source)
    if counts:
        logger.info(
            "Reused %s file(s) other content already saved: %s",
            sum(counts.values()),
            ", ".join(f"{count} {how}" for how, count in sorted(counts.items())),
        )
    return counts


def shared_media_copies(
    db_manager: Any,
    config: UltimaScraperCollectionConfig,
) -> SharedMediaCopies:
    """The lookup a download task uses, backed by the site's archive."""

    def associations(media_ids: list[int]) -> dict[int, frozenset[str]]:
        from sqlmodel import select
        from ultima_scraper_db_archive import ContentMediaAssoModel

        model = ContentMediaAssoModel
        try:
            with db_manager.get_session() as session:
                rows = session.exec(
                    select(
                        model.media_id,
                        model.post_id,
                        model.story_id,
                        model.message_id,
                        model.mass_message_id,
                    ).where(model.media_id.in_(media_ids))  # type: ignore[union-attr]
                ).all()
        except Exception:
            logger.warning(
                "Could not read the content %s media belong to", len(media_ids), exc_info=True
            )
            return {}
        kinds: dict[int, set[str]] = {}
        for media_id, post_id, story_id, message_id, mass_message_id in rows:
            found = kinds.setdefault(media_id, set())
            if post_id is not None:
                found.add("post")
            if story_id is not None:
                found.add("story")
            if message_id is not None:
                found.add("message")
            if mass_message_id is not None:
                found.add("mass_message")
        return {media_id: frozenset(found) for media_id, found in kinds.items()}

    return SharedMediaCopies(
        tail_format=performer_tail_format(config.settings.download_setup.directory_format),
        associations=associations,
    )
