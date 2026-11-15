"""Merge a performer whose folders are split across drives onto one of them.

A performer spills onto a second drive when theirs fills up (see
drive_selection). Consolidation, started from the WebUI, finds performers
whose folder exists on more than one drive, plans where their files go, and
moves them once the plan has been previewed.

Only drives that already hold one of the performer's folders can take them:
the highest priority with room for every file wins, then the one already
holding the most bytes, so the least moves. A file whose name and size match
one in the target is a duplicate and the extra copy is deleted; a same-named
file of another size stays where it is and is listed. Files changed in the
last few minutes are left for a later run, since a download may be writing
them. The database's file paths follow each move.
"""

from __future__ import annotations

import errno
import logging
import os
import re
import shutil
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from ultima_scraper_backend.config import DownloadPathSetup, Drive
from ultima_scraper_backend.job_system.drive_selection import (
    _child_directories,
    _compile_segment_matcher,
    _flatten_drive_directories,
    _get_performer_root_format,
    _prepare_drive_root_for_scan,
    _prepare_drive_root_for_storage,
)

logger = logging.getLogger(__name__)


def _usable_free(root: Path) -> int | None:
    """A drive's free bytes less what running downloads expect to write there;
    None when its space can't be read."""
    from ultima_scraper_backend.job_system import drive_reservations

    try:
        free = shutil.disk_usage(root.expanduser()).free
    except OSError:
        return None
    return free - drive_reservations.reserved_on(root.expanduser())

# A file changed this recently may still be arriving from a download.
RECENT_SECONDS = 600
# filepaths.filepath is VARCHAR(255).
PATH_LIMIT = 255
_PART_SUFFIX = ".consolidate.part"
# Files moved at once within a performer. Each move is a few round trips to a
# network drive, so running several hides most of that latency.
MOVE_WORKERS = 8
_LEGACY_PERFORMER_ROOT = Path("{site_name}/{first_username_letter}/{user_username}")

MoveKind = Literal["move", "duplicate", "conflict", "recent", "too_long", "same_file"]


def _identity(path: Path) -> tuple[int, int, int] | None:
    """Inode and timestamps: equal for one directory or file seen through two
    mounts of the same export, which must never count as two copies."""
    try:
        stat_result = path.stat()
    except OSError:
        return None
    return (stat_result.st_ino, stat_result.st_mtime_ns, stat_result.st_ctime_ns)


@dataclass(frozen=True)
class PerformerFolder:
    """One drive's copy of a performer folder."""

    root: Path
    relative_path: Path
    drive: Drive

    @property
    def path(self) -> Path:
        return self.root / self.relative_path


@dataclass(frozen=True)
class SplitPerformer:
    """A performer folder found on more than one drive."""

    relative_path: Path
    folders: tuple[PerformerFolder, ...]


@dataclass(frozen=True)
class FileMove:
    source: Path
    target: Path
    size: int
    kind: MoveKind


@dataclass
class ConsolidationPlan:
    performer: SplitPerformer
    target: PerformerFolder | None
    folder_bytes: dict[Path, int] = field(default_factory=dict)
    folder_files: dict[Path, int] = field(default_factory=dict)
    moves: list[FileMove] = field(default_factory=list)
    reason: str = ""
    # Per drive: usable free bytes (None when unreadable) and its reserve.
    folder_free: dict[Path, int | None] = field(default_factory=dict)
    folder_reserve: dict[Path, int] = field(default_factory=dict)
    # Per drive: bytes that would move there if it were the target.
    folder_needed: dict[Path, int] = field(default_factory=dict)
    # Bytes the move adds to the target drive.
    needed: int = 0
    # The move fits only by using some of the target drive's reserve.
    into_reserve: bool = False
    # Refused because no drive (or the chosen one) has the free space at all.
    no_space: bool = False

    def of_kind(self, kind: MoveKind) -> list[FileMove]:
        return [move for move in self.moves if move.kind == kind]


@dataclass
class ConsolidationResult:
    moved: int = 0
    moved_bytes: int = 0
    duplicates: int = 0
    conflicts: list[str] = field(default_factory=list)
    skipped: int = 0
    failed: list[str] = field(default_factory=list)


def _usable_site_names(setup: DownloadPathSetup, site_name: str) -> list[str]:
    find_only = {name.casefold() for name in setup.get_find_only_site_directories(site_name)}
    names = [site_name, *setup.get_site_directory_aliases(site_name)]
    usable: list[str] = []
    for name in names:
        if name.casefold() not in find_only and name.casefold() not in {n.casefold() for n in usable}:
            usable.append(name)
    return usable


def _segment_matchers(template_part: str, site_names: Iterable[str]) -> list[re.Pattern[str]]:
    if "{site_name}" not in template_part:
        return [_compile_segment_matcher(template_part, {})]
    return [
        re.compile(_compile_segment_matcher(template_part, {"site_name": name}).pattern, re.IGNORECASE)
        for name in site_names
    ]


class ScanCancelled(Exception):
    """A scan's checkpoint asked it to stop."""


def scan_roots(setup: DownloadPathSetup) -> list[tuple[Path, Drive]]:
    """The drive roots a scan reads, once each."""
    roots: dict[Path, Drive] = {}
    for drive_dir in _flatten_drive_directories(setup):
        if not drive_dir.drive.scan_existing:
            continue
        root = _prepare_drive_root_for_scan(drive_dir.path)
        if root is not None:
            roots.setdefault(root, drive_dir.drive)
    return list(roots.items())


def find_split_performers(
    setup: DownloadPathSetup,
    site_name: str,
    *,
    checkpoint: Callable[[Path, int], None] | None = None,
) -> list[SplitPerformer]:
    """Performer folders, under the download format, that exist on more than one drive.

    ``checkpoint(root, roots_done)`` runs before every folder listing; it may
    block to pause the scan or raise ScanCancelled to stop it.
    """
    # Without a performer folder in the format, downloads use the legacy layout.
    root_format = _get_performer_root_format(setup.directory_format) or _LEGACY_PERFORMER_ROOT
    template_parts = [part for part in root_format.parts if part and part != "."]
    matchers = [_segment_matchers(part, _usable_site_names(setup, site_name)) for part in template_parts]
    listings: dict[Path, list[str]] = {}
    found: dict[str, list[PerformerFolder]] = {}
    for roots_done, (root, drive) in enumerate(scan_roots(setup)):

        def children(parent: Path) -> list[str]:
            if checkpoint is not None:
                checkpoint(root, roots_done)
            return _child_directories(parent, listings)

        relatives = [Path()]
        for part_matchers in matchers:
            relatives = [
                relative / child
                for relative in relatives
                for child in children(root / relative)
                if any(matcher.fullmatch(child) for matcher in part_matchers)
            ]
        for relative in relatives:
            found.setdefault(relative.as_posix().casefold(), []).append(
                PerformerFolder(root=root, relative_path=relative, drive=drive)
            )
    split: list[SplitPerformer] = []
    for _, folders in sorted(found.items()):
        distinct: dict[object, PerformerFolder] = {}
        for folder in folders:
            distinct.setdefault(_identity(folder.path) or folder.root, folder)
        if len(distinct) > 1:
            split.append(SplitPerformer(relative_path=folders[0].relative_path, folders=tuple(distinct.values())))
    return split


def _list_files(folder: Path) -> list[tuple[Path, int, float]]:
    """(path inside the folder, size, mtime) of each finished file."""
    files: list[tuple[Path, int, float]] = []
    for directory, _, names in os.walk(folder):
        for name in names:
            if name.startswith(".") and name.endswith(".part"):
                continue
            path = Path(directory) / name
            try:
                stat_result = path.stat()
            except OSError:
                continue
            files.append((path.relative_to(folder), stat_result.st_size, stat_result.st_mtime))
    files.sort()
    return files


def _classify_moves(
    performer: SplitPerformer,
    listed: dict[Path, list[tuple[Path, int, float]]],
    target: PerformerFolder,
    now: float,
) -> list[FileMove]:
    """What would happen to each file outside ``target`` if it were the target."""
    moves: list[FileMove] = []
    in_target = {relative: size for relative, size, _ in listed[target.root]}
    for folder in performer.folders:
        if folder is target:
            continue
        for relative, size, mtime in listed[folder.root]:
            source = folder.path / relative
            destination = target.path / relative
            if now - mtime < RECENT_SECONDS:
                kind: MoveKind = "recent"
            elif relative in in_target:
                kind = "duplicate" if in_target[relative] == size else "conflict"
            elif len(str(destination)) > PATH_LIMIT:
                kind = "too_long"
            else:
                kind = "move"
                in_target[relative] = size
            moves.append(FileMove(source=source, target=destination, size=size, kind=kind))
    return moves


def plan_consolidation(
    performer: SplitPerformer,
    *,
    target_root: Path | str | None = None,
    now: float | None = None,
) -> ConsolidationPlan:
    """Where a split performer's files would go, and what happens to each.

    The best drive keeps its reserve; when none can, the best one with the free
    space at all is used and the plan is marked ``into_reserve``. A drive whose
    space can't be read counts as roomy, as in drive selection. ``target_root``
    picks the drive instead; it must already hold the performer.
    """
    now = time.time() if now is None else now
    plan = ConsolidationPlan(performer=performer, target=None)
    candidates = performer.folders
    if target_root is not None:
        candidates = [folder for folder in performer.folders if str(folder.root) == str(target_root)]
        if not candidates:
            plan.reason = f"{target_root} does not hold this performer"
            return plan
    listed = {folder.root: _list_files(folder.path) for folder in performer.folders}
    for folder in performer.folders:
        plan.folder_bytes[folder.root] = sum(size for _, size, _ in listed[folder.root])
        plan.folder_files[folder.root] = len(listed[folder.root])

    # Only files that would really move take space: duplicates, clashes and
    # files left behind (recent, paths too long) need none.
    moves_to: dict[Path, list[FileMove]] = {}
    for folder in performer.folders:
        plan.folder_free[folder.root] = _usable_free(folder.root)
        plan.folder_reserve[folder.root] = max(folder.drive.minimum_space, 0)
        moves_to[folder.root] = _classify_moves(performer, listed, folder, now)
        plan.folder_needed[folder.root] = sum(move.size for move in moves_to[folder.root] if move.kind == "move")

    # (keeps its reserve, priority, bytes already there): the highest wins.
    best: tuple[tuple[bool, int, int], PerformerFolder] | None = None
    for folder in candidates:
        if _prepare_drive_root_for_storage(folder.root) is None:
            if target_root is not None:
                plan.reason = f"{folder.root} is not available for storage"
            continue
        here = plan.folder_bytes[folder.root]
        free = plan.folder_free[folder.root]
        # Moving nothing needs no room, even on a full drive.
        needed = plan.folder_needed[folder.root]
        if needed > 0 and free is not None and free < needed:
            plan.no_space = True
            if target_root is not None:
                plan.reason = f"{folder.root} has {free} bytes free; this needs {needed}"
            continue
        keeps_reserve = needed <= 0 or free is None or free - plan.folder_reserve[folder.root] >= needed
        key = (keeps_reserve, folder.drive.priority, here)
        if best is None or key > best[0]:
            best = (key, folder)
    if best is None:
        plan.reason = plan.reason or "No drive holding this performer has the free space for what would move"
        return plan
    plan.no_space = False
    target = plan.target = best[1]
    plan.needed = plan.folder_needed[target.root]
    plan.into_reserve = not best[0][0]
    plan.moves = moves_to[target.root]
    return plan


def _move_file(source: Path, target: Path) -> MoveKind:
    """Move one file, rechecking the target first; returns what was done."""
    size = source.stat().st_size
    try:
        existing = target.stat().st_size
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if existing != size:
            return "conflict"
        if _identity(source) == _identity(target):
            return "same_file"
        source.unlink()
        return "duplicate"
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(source, target)
        return "move"
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
    # Another drive: copy under a temporary name, so a half-copied file is
    # never mistaken for a finished one.
    part = target.with_name(f".{target.name}{_PART_SUFFIX}")
    try:
        shutil.copy2(source, part)
        os.replace(part, target)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    source.unlink()
    return "move"


def _prune_empty(folder: Path) -> None:
    for directory, _, _ in sorted(os.walk(folder), key=lambda entry: len(entry[0]), reverse=True):
        try:
            os.rmdir(directory)
        except OSError:
            continue


def _prune_empty_parents(folder: PerformerFolder) -> None:
    """Remove the folders above a removed performer folder that it leaves empty,
    such as its letter folder, up to but not including the site folder."""
    if len(folder.relative_path.parts) < 2:
        return
    stop = folder.root / folder.relative_path.parts[0]
    parent = folder.path.parent
    while parent != stop:
        try:
            os.rmdir(parent)
        except OSError:
            return
        parent = parent.parent


PathUpdater = Callable[[list[tuple[str, str]]], None]


def database_path_updater(site_name: str) -> PathUpdater:
    """Point the site's file path rows from each moved file's old path to its new one."""

    def update(pairs: list[tuple[str, str]]) -> None:
        from sqlalchemy import text

        from ultima_scraper_backend.database.factory import MultiSiteDatabaseManager

        rows = [{"old": old, "new": new} for old, new in pairs]
        site_db = MultiSiteDatabaseManager().get_or_create_manager(site_name.lower())
        with site_db.get_session() as session:
            # A media row may already point at the kept copy of a duplicate.
            session.execute(
                text(
                    "DELETE FROM filepaths AS f WHERE f.filepath = :old AND EXISTS "
                    "(SELECT 1 FROM filepaths AS n WHERE n.media_id = f.media_id AND n.filepath = :new)"
                ),
                rows,
            )
            session.execute(text("UPDATE filepaths SET filepath = :new WHERE filepath = :old"), rows)
            session.commit()

    return update


def execute_consolidation(
    plan: ConsolidationPlan,
    update_paths: PathUpdater,
    *,
    batch_size: int = 200,
    workers: int = MOVE_WORKERS,
    on_progress: Callable[[ConsolidationResult], None] | None = None,
) -> ConsolidationResult:
    """Carry out a plan, ``workers`` files at a time; files that changed since it
    was made are rechecked. Moves to the same path run in order on one worker,
    and a full drive stops new moves from starting."""
    result = ConsolidationResult()
    if plan.target is None:
        return result
    lock = threading.Lock()
    full = threading.Event()
    pending: list[tuple[str, str]] = []

    def flush() -> None:
        if pending:
            update_paths(list(pending))
            pending.clear()

    by_target: dict[Path, list[FileMove]] = {}
    for move in plan.moves:
        if move.kind in {"recent", "too_long"}:
            result.skipped += 1
        elif move.kind == "conflict":
            result.conflicts.append(str(move.source))
        else:
            by_target.setdefault(move.target, []).append(move)

    def record(move: FileMove, done: MoveKind) -> None:
        with lock:
            if done == "conflict":
                result.conflicts.append(str(move.source))
                return
            if done == "same_file":
                result.skipped += 1
                return
            pending.append((str(move.source), str(move.target)))
            if done == "duplicate":
                result.duplicates += 1
            else:
                result.moved += 1
                result.moved_bytes += move.size
            if len(pending) >= batch_size:
                flush()
            if on_progress is not None:
                on_progress(result)

    def carry_out(moves: list[FileMove]) -> None:
        for move in moves:
            if full.is_set():
                return
            try:
                done = _move_file(move.source, move.target)
            except FileNotFoundError:
                with lock:
                    result.skipped += 1
                continue
            except OSError as exc:
                logger.warning("Could not move %s to %s: %s", move.source, move.target, exc)
                with lock:
                    result.failed.append(f"{move.source}: {exc}")
                if exc.errno == errno.ENOSPC:
                    full.set()
                continue
            record(move, done)

    try:
        with ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="consolidate") as pool:
            for future in [pool.submit(carry_out, moves) for moves in by_target.values()]:
                future.result()
    finally:
        # Every file that did move has its database path updated, even after an error.
        with lock:
            flush()
    for folder in plan.performer.folders:
        if folder is not plan.target:
            _prune_empty(folder.path)
            if not folder.path.exists():
                _prune_empty_parents(folder)
    if on_progress is not None:
        on_progress(result)
    return result
