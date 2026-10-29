"""Review and restore performer files to an existing server archive.

scan writes a folder inventory; plan snapshots files and optionally hashes collisions;
apply accepts only a reviewed plan and never overwrites destination files.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import stat
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4


def file_snapshot(path: Path) -> dict:
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode):
        raise ValueError(f"Not a regular file: {path}")
    return {
        "size": s.st_size,
        "mtime_ns": s.st_mtime_ns,
        "ctime_ns": s.st_ctime_ns,
        "device": s.st_dev,
        "inode": s.st_ino,
    }


def verify_snapshot(path: Path, expected: dict, cached_stat: os.stat_result | None = None) -> bool:
    if "sha256" in expected:
        try:
            actual = fingerprint(path)
        except OSError:
            return False
        return (
            actual.get("size") == expected.get("size")
            and actual.get("sha256") == expected.get("sha256")
        )
    try:
        s = cached_stat if cached_stat is not None else path.lstat()
    except OSError:
        return False
    if not stat.S_ISREG(s.st_mode):
        return False
    if s.st_size != expected.get("size"):
        return False
    exp_mtime = expected.get("mtime_ns")
    if exp_mtime is not None:
        if s.st_mtime_ns != exp_mtime and s.st_mtime_ns // 1_000_000_000 != exp_mtime // 1_000_000_000:
            return False
    return True


def copy_matches(actual: dict, expected: dict) -> bool:
    return (
        all(key in actual and key in expected for key in ("size", "sha256", "mtime_ns"))
        and actual["size"] == expected["size"]
        and actual["sha256"] == expected["sha256"]
        and actual["mtime_ns"] // 1_000_000_000 == expected["mtime_ns"] // 1_000_000_000
    )


def content_matches(actual: dict, expected: dict) -> bool:
    return (
        all(key in actual and key in expected for key in ("size", "sha256"))
        and actual["size"] == expected["size"]
        and actual["sha256"] == expected["sha256"]
    )



def sync_directory(path: Path) -> None:
    handle = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def fingerprint(path: Path, *, progress=None) -> dict:
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode):
        raise ValueError(f"Not a regular file: {path}")
    digest = hashlib.sha256()
    started = last_update = time.monotonic()
    read_bytes = 0
    if progress:
        progress(f"Hashing {path}: 0 / {s.st_size:,} bytes")
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
            read_bytes += len(block)
            now = time.monotonic()
            if progress and now - last_update >= 2:
                progress(f"Hashing {path}: {read_bytes:,} / {s.st_size:,} bytes ({read_bytes / max(now - started, 0.001) / 1024 ** 2:.2f} MiB/s)")
                last_update = now
    if progress:
        progress(f"Hashed {path}: {read_bytes:,} bytes")
    after = path.lstat()
    if (s.st_size, s.st_mtime_ns, s.st_ctime_ns) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise ValueError(f"File changed during hashing: {path}")
    return {"size": s.st_size, "mtime_ns": s.st_mtime_ns, "sha256": digest.hexdigest()}


def folders(site: Path, names: set[str] | None = None) -> list[Path]:
    if not site.is_dir() or site.is_symlink():
        return []
    result = []
    with os.scandir(site) as letters:
        for letter in letters:
            if len(letter.name) != 1 or not letter.is_dir(follow_symlinks=False):
                continue
            with os.scandir(letter.path) as performers:
                result.extend(
                    Path(p.path)
                    for p in performers
                    if (names is None or p.name.casefold() in names)
                    and p.is_dir(follow_symlinks=False)
                )
    return sorted(result)


def scan(server: Path, roots: list[Path], aliases: list[str], *, progress=None) -> dict:
    emit = progress or (lambda message: print(message, flush=True))
    server = server.resolve(strict=True)
    emit(f"Indexing server roster: {server}")
    targets: dict[str, list[Path]] = {}
    for folder in folders(server):
        targets.setdefault(folder.name.casefold(), []).append(folder)
    emit(f"Server roster: {len(targets)} performers")
    matches = []
    unavailable = []
    root_results = []
    seen = set()
    for root in roots:
        if root.resolve() == server.parent:
            root_results.append({"path": str(root), "status": "destination", "matched_folders": 0})
            continue
        if not root.is_dir():
            unavailable.append(str(root))
            root_results.append({"path": str(root), "status": "unavailable", "matched_folders": 0})
            continue
        before = len(matches)
        for alias in aliases:
            emit(f"Scanning {root / alias}")
            for source in folders(root / alias, set(targets)):
                source = source.resolve(strict=True)
                if source == server or server in source.parents or source in seen:
                    continue
                seen.add(source)
                destinations = targets.get(source.name.casefold(), [])
                if destinations:
                    matches.append(
                        {
                            "performer": source.name,
                            "source": str(source),
                            "destinations": [str(p) for p in destinations],
                        }
                    )
        emit(f"Matched source folders so far: {len(matches)}")
        root_results.append({"path": str(root), "status": "scanned", "matched_folders": len(matches) - before})
    return {
        "version": 1,
        "server": str(server),
        "server_performers": len(targets),
        "matches": matches,
        "unavailable_roots": unavailable,
        "roots": root_results,
        "aliases": aliases,
    }


def make_plan(
    inventory: dict,
    performers: list[str],
    *,
    hash_new_files: bool = False,
    hash_collisions: bool = False,
    remove_duplicates: bool = False,
    repair_zero_bytes: bool = False,
    cleanup_empty: bool = False,
    ignore_db_json: bool = True,
    destination_choices: dict[str, str] | None = None,
    preferred_sources: dict[str, str] | None = None,
    retained_sources: list[str] | None = None,
    isolate_performers: bool = False,
    progress=None,
) -> dict:
    emit = progress or (lambda message: print(message, flush=True))
    selected = {name.casefold() for name in performers}
    choices = {name.casefold(): path for name, path in (destination_choices or {}).items()}
    preferred = preferred_sources or {}
    retained = set(retained_sources or [])
    options = {"hash_collisions": hash_collisions, "remove_duplicates": remove_duplicates,
               "repair_zero_bytes": repair_zero_bytes, "cleanup_empty": cleanup_empty,
               "ignore_db_json": ignore_db_json,
               "destination_choices": choices, "preferred_sources": preferred,
               "retained_sources": sorted(retained)}
    if isolate_performers:
        selected_roots = [Path(m["source"]) for m in inventory["matches"] if m["performer"].casefold() in selected]
        if any(not any(Path(src).is_relative_to(root) for root in selected_roots) for src in [*preferred.values(), *retained]):
            raise ValueError("File decisions must belong to selected performer folders")
        combined = {"version": 2, "server": inventory["server"], "entries": [],
                    "root_snapshots": {}, "cleanup_directories": {}, "blocked_performers": [],
                    "performers": performers, "options": options}
        found = {m["performer"].casefold() for m in inventory["matches"]}
        if selected - found:
            raise ValueError(f"Performers absent from inventory: {sorted(selected - found)}")
        for name in sorted(selected):
            subset = {**inventory, "matches": [m for m in inventory["matches"] if m["performer"].casefold() == name]}
            try:
                result = make_plan(subset, [name], hash_new_files=hash_new_files,
                                   progress=progress, **{**options,
                                   "preferred_sources": {dst: src for dst, src in preferred.items() if any(Path(src).is_relative_to(Path(m["source"])) for m in subset["matches"])},
                                   "retained_sources": [src for src in retained if any(Path(src).is_relative_to(Path(m["source"])) for m in subset["matches"])]})
            except (ValueError, OSError) as exc:
                combined["blocked_performers"].append({"performer": name, "error": str(exc),
                    "destinations": sorted({d for m in subset["matches"] for d in m["destinations"]})})
                continue
            combined["entries"].extend(result["entries"])
            combined["root_snapshots"].update(result["root_snapshots"])
            combined["cleanup_directories"].update(result["cleanup_directories"])
        return combined
    entries = []
    root_snapshots = {}
    cleanup_directories = {}
    reserved: dict[str, dict] = {}
    for match in inventory["matches"]:
        if match["performer"].casefold() not in selected:
            continue
        destination_choice = choices.get(match["performer"].casefold())
        if destination_choice is not None and destination_choice not in match["destinations"]:
            raise ValueError(f"Destination is not in the reviewed server roster: {match['performer']}")
        if len(match["destinations"]) != 1 and not destination_choice:
            raise ValueError(f"Ambiguous server performer: {match['performer']}")
        source, destination = Path(match["source"]), Path(destination_choice or match["destinations"][0])
        for root in (source, destination):
            root_stat = root.stat()
            root_snapshots[str(root)] = {
                "device": root_stat.st_dev,
                "inode": root_stat.st_ino,
            }
        emit(f"Planning {source} -> {destination}")
        if source.is_symlink() or destination.is_symlink():
            raise ValueError("Symlink performer roots are unsupported")

        def walk_error(error):
            raise error

        for parent, dirs, files in os.walk(
            source, followlinks=False, onerror=walk_error
        ):
            if cleanup_empty:
                directory = Path(parent)
                st = directory.stat()
                cleanup_directories[str(directory)] = {
                    "device": st.st_dev,
                    "inode": st.st_ino,
                    "source_root": str(source),
                }
            for name in dirs:
                if (Path(parent) / name).is_symlink():
                    raise ValueError(f"Symlink directory: {Path(parent) / name}")
            for name in sorted(files):
                if ignore_db_json and name.lower().endswith(
                    (".db", ".json", ".db-wal", ".db-shm", ".db-journal")
                ):
                    continue
                src = Path(parent) / name
                dst = destination / src.relative_to(source)
                snapshot = file_snapshot(src)
                existing = None
                reason = "missing"
                if dst.exists() or dst.is_symlink():
                    existing = file_snapshot(dst)
                    action = "conflict"
                    reason = "zero_byte" if existing["size"] == 0 and snapshot["size"] > 0 else "size_mismatch"
                    if snapshot["size"] == existing["size"]:
                        action = "collision"
                        reason = "unverified"
                        if hash_collisions:
                            snapshot, existing = fingerprint(src, progress=emit), fingerprint(dst, progress=emit)
                            same_content = content_matches(snapshot, existing)
                            same_exact = snapshot == existing
                            if remove_duplicates and same_content:
                                action = "remove_duplicate"
                                reason = "verified_duplicate"
                            elif same_exact:
                                action = "duplicate"
                                reason = "verified_duplicate"
                            elif same_content:
                                action = "conflict"
                                reason = "mtime_mismatch"
                            else:
                                action = "conflict"
                                reason = "content_mismatch"
                        elif remove_duplicates:
                            action = "remove_duplicate"
                            reason = "verified_duplicate"
                    if (
                        repair_zero_bytes
                        and existing["size"] == 0
                        and snapshot["size"] > 0
                    ):
                        snapshot = fingerprint(src, progress=emit)
                        action = "repair_zero_byte"
                        reason = "zero_byte"
                elif str(dst) in reserved:
                    # Multiple source drives need separate review, never automatic deletion.
                    action = "conflict"
                    reason = "competing_source"
                else:
                    action = "move"
                    if hash_new_files:
                        snapshot = fingerprint(src, progress=emit)
                    reserved[str(dst)] = snapshot
                if action == "repair_zero_byte":
                    if str(dst) in reserved:
                        action = "conflict"
                        reason = "competing_source"
                    else:
                        reserved[str(dst)] = snapshot
                if str(src) in retained or (str(dst) in preferred and preferred[str(dst)] != str(src)):
                    if reserved.get(str(dst)) is snapshot:
                        reserved.pop(str(dst))
                    action, reason = "retain", "reviewed_keep"
                entries.append(
                    {
                        "performer": match["performer"],
                        "source": str(src),
                        "destination": str(dst),
                        "source_root": str(source),
                        "destination_root": str(destination),
                        "action": action,
                        "reason": reason,
                        "relative_path": str(src.relative_to(source)),
                        "source_snapshot": snapshot,
                        "destination_snapshot": existing,

                    }
                )
                if len(entries) % 100 == 0:
                    emit(f"Inspected {len(entries)} source files")
    found = {m["performer"].casefold() for m in inventory["matches"]}
    if selected - found:
        raise ValueError(
            f"Performers absent from inventory: {sorted(selected - found)}"
        )
    if any(not any(e["source"] == src and e["destination"] == dst for e in entries) for dst, src in preferred.items()):
        raise ValueError("Preferred source is not a candidate for the reviewed destination")
    if retained - {e["source"] for e in entries}:
        raise ValueError("Retained source is not in the reviewed inventory")
    return {
        "version": 2,
        "server": inventory["server"],
        "entries": entries,
        "root_snapshots": root_snapshots,
        "cleanup_directories": cleanup_directories,
        "performers": performers,
        "options": options,
    }


def check_path(
    path: Path,
    root: Path,
    *,
    checked_dirs: set[Path] | None = None,
    cached_stat: os.stat_result | None = None,
    known_absent: bool = False,
) -> None:
    if ".." in path.parts or ".." in root.parts:
        raise ValueError("Traversal segments are unsupported")
    path.relative_to(root)
    if checked_dirs is not None:
        curr = path.parent
        to_add = []
        while curr != root and root in curr.parents:
            if curr in checked_dirs:
                break
            if curr.is_symlink():
                raise ValueError(f"Symlink path is unsupported: {curr}")
            to_add.append(curr)
            curr = curr.parent
        checked_dirs.update(to_add)
    else:
        for component in path.parents:
            if component.is_symlink():
                raise ValueError(f"Symlink path is unsupported: {component}")
    if cached_stat is not None:
        if stat.S_ISLNK(cached_stat.st_mode):
            raise ValueError(f"Symlink path is unsupported: {path}")
    elif not known_absent and path.is_symlink():
        raise ValueError(f"Symlink path is unsupported: {path}")


def apply_plan(plan: dict, journal: Path, *, validate_only: bool = False, progress=None) -> None:
    server = Path(plan["server"])
    directory_times = {}
    for root_name, expected in plan.get("root_snapshots", {}).items():
        root = Path(root_name)
        if not root.is_dir() or root.is_symlink():
            raise ValueError(f"Archive root unavailable: {root}")
        current = root.stat()
        if {"device": current.st_dev, "inode": current.st_ino} != expected:
            if current.st_dev != expected.get("device"):
                raise ValueError(f"Archive root changed since review: {root}")
    for name, expected in plan.get("cleanup_directories", {}).items():
        directory = Path(name)
        source_root = Path(expected["source_root"])
        check_path(directory, source_root)
        if (
            server == source_root
            or server in source_root.parents
            or source_root in server.parents
        ):
            raise ValueError("Cleanup must be outside the server archive")
        st = directory.stat()
        if not stat.S_ISDIR(st.st_mode) or {
            "device": st.st_dev,
            "inode": st.st_ino,
        } != {k: expected[k] for k in ("device", "inode")}:
            raise ValueError(f"Cleanup directory changed: {directory}")
        if not validate_only:
            for retained in (directory, source_root.parent):
                recorded = retained.stat()
                directory_times[retained] = (recorded.st_atime_ns, recorded.st_mtime_ns)
    # Validate the complete approved snapshot before changing anything.
    roots_cache: dict[tuple[str, str], tuple[Path, Path]] = {}
    checked_dirs: set[Path] = set()

    # Pre-scan destination directories concurrently to eliminate tens of thousands of individual SSHFS roundtrips.
    dst_parents = {str(Path(e["destination"]).parent) for e in plan["entries"]}
    dst_dir_cache: dict[str, dict[str, os.stat_result] | None] = {}

    def scan_dir(d_str: str) -> tuple[str, dict[str, os.stat_result] | None]:
        try:
            entries: dict[str, os.stat_result] = {}
            with os.scandir(d_str) as it:
                for entry in it:
                    try:
                        entries[entry.name] = entry.stat(follow_symlinks=False)
                    except OSError:
                        pass
            return d_str, entries
        except (FileNotFoundError, NotADirectoryError):
            return d_str, None

    if len(dst_parents) > 1 and any(e.get("action") in {"move", "remove_duplicate", "repair_zero_byte"} for e in plan["entries"]):
        max_workers = min(32, max(4, len(dst_parents)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for d_str, entries in executor.map(scan_dir, dst_parents):
                dst_dir_cache[d_str] = entries

    total_entries = len(plan["entries"])
    for idx, entry in enumerate(plan["entries"], start=1):
        src, dst = Path(entry["source"]), Path(entry["destination"])
        if progress and (idx % 25 == 0 or idx == total_entries or idx == 1):
            progress({
                "phase": "validate_snapshot",
                "source": str(src),
                "destination": str(dst),
                "completed": idx,
                "total": total_entries,
                "message": f"Validating file snapshots ({idx:,} / {total_entries:,})",
            })
        roots_key = (entry["source_root"], entry["destination_root"])
        cached_roots = roots_cache.get(roots_key)
        if cached_roots is None:
            source_root, destination_root = (
                Path(entry["source_root"]),
                Path(entry["destination_root"]),
            )
            if not source_root.is_dir() or not destination_root.is_dir():
                raise ValueError("A reviewed performer root is unavailable")
            roots_cache[roots_key] = (source_root, destination_root)
        else:
            source_root, destination_root = cached_roots
        if not all(
            p.is_absolute() for p in (src, dst, source_root, destination_root, server)
        ):
            raise ValueError("Plan paths must be absolute")

        dir_entries = dst_dir_cache.get(str(dst.parent))
        cached_dst_stat = dir_entries.get(dst.name) if dir_entries is not None else None
        dst_absent = dir_entries is None or cached_dst_stat is None

        check_path(src, source_root, checked_dirs=checked_dirs)
        check_path(dst, destination_root, checked_dirs=checked_dirs, cached_stat=cached_dst_stat, known_absent=dst_absent)
        if src.relative_to(source_root) != dst.relative_to(destination_root):
            raise ValueError("Source and destination relative paths differ")
        if entry["action"] not in {
            "move",
            "duplicate",
            "conflict",
            "collision",
            "remove_duplicate",
            "repair_zero_byte",
            "retain",
            "finish_move",
        }:
            raise ValueError("Unknown plan action")
        destination_root.relative_to(server)
        if entry["action"] == "remove_duplicate":
            if not entry.get("destination_snapshot"):
                raise ValueError("Duplicate removal requires destination snapshot")
            src_snap = entry["source_snapshot"]
            dst_snap = entry["destination_snapshot"]
            if "sha256" in src_snap and "sha256" in dst_snap:
                if not content_matches(src_snap, dst_snap):
                    raise ValueError(
                        "Duplicate removal requires identical reviewed hashes"
                    )
            elif src_snap.get("size") != dst_snap.get("size"):
                raise ValueError(
                    "Duplicate removal requires identical reviewed file sizes"
                )
        if entry["action"] == "finish_move" and (
            not entry.get("recovered_copy")
            or not copy_matches(entry["source_snapshot"], entry["recovered_copy"])
            or not copy_matches(entry["destination_snapshot"] or {}, entry["recovered_copy"])
        ):
            raise ValueError("Recovery requires a verified copy checkpoint and matching contents")
        if entry["action"] == "repair_zero_byte":
            if (
                not entry["destination_snapshot"]
                or entry["destination_snapshot"]["size"] != 0
                or entry["source_snapshot"]["size"] <= 0
            ):
                raise ValueError(
                    "Repair requires a zero-byte destination and a nonempty source"
                )
            if entry.get("backup"):
                backup = Path(entry["backup"])
                check_path(backup, destination_root)
                if backup.parent != dst.parent or backup.exists():
                    raise ValueError("Repair backup path unavailable")
        if server == source_root or server in source_root.parents:
            raise ValueError("Source is inside server archive")
        if not verify_snapshot(src, entry["source_snapshot"]):
            raise ValueError(f"Source changed since review: {src}")

        if entry["destination_snapshot"] is not None:
            if not verify_snapshot(dst, entry["destination_snapshot"], cached_stat=cached_dst_stat):
                raise ValueError(f"Destination changed since review: {dst}")
        elif entry["action"] == "move":
            if cached_dst_stat is not None:
                raise ValueError(f"Destination appeared since review: {dst}")
            elif str(dst.parent) not in dst_dir_cache:
                try:
                    dst.lstat()
                    raise ValueError(f"Destination appeared since review: {dst}")
                except (FileNotFoundError, NotADirectoryError):
                    pass
        if not validate_only:
            for root, directory in ((source_root.parent, src.parent), (server, dst.parent)):
                while directory == root or root in directory.parents:
                    if directory.is_dir() and directory not in directory_times:
                        s = directory.stat()
                        directory_times[directory] = (s.st_atime_ns, s.st_mtime_ns)
                    if directory == root:
                        break
                    directory = directory.parent
    if validate_only:
        return
    # Only explicitly reviewed file actions and empty directory checks execute.
    with journal.open("x", encoding="utf-8") as log:
        sync_directory(journal.parent)
        try:
            for entry in plan["entries"]:
                if entry["action"] not in {
                    "move",
                    "remove_duplicate",
                    "repair_zero_byte",
                    "finish_move",
                }:
                    continue
                src, dst = Path(entry["source"]), Path(entry["destination"])
                if progress:
                    progress({"phase": "verify_source", "source": str(src), "destination": str(dst)})
                if not verify_snapshot(src, entry["source_snapshot"]):
                    raise ValueError(f"Source changed: {src}")
                if entry["action"] in {"remove_duplicate", "finish_move"}:
                    original_source = file_snapshot(src)
                    if not verify_snapshot(dst, entry["destination_snapshot"]):
                        raise ValueError(f"Server duplicate changed: {dst}")
                    if file_snapshot(src) != original_source:
                        raise ValueError(
                            f"Source changed during duplicate verification: {src}"
                        )
                    log.write(
                        json.dumps({"event": "verified_duplicate", **entry}) + "\n"
                    )
                    log.flush()
                    os.fsync(log.fileno())
                    src.unlink()
                    sync_directory(src.parent)
                    log.write(
                        json.dumps(
                            {
                                "event": "source_removed",
                                "source": str(src),
                                "destination": str(dst),
                                "action": entry["action"],
                                "bytes": 0,
                            }
                        )
                        + "\n"
                    )
                    log.flush()
                    os.fsync(log.fileno())
                    if progress:
                        progress({"phase": "source_removed", "source": str(src), "destination": str(dst), "bytes": 0})
                    continue
                target = dst
                if entry["action"] == "repair_zero_byte":
                    dst = target.with_name(".restore-" + uuid4().hex + ".tmp")
                verified_source = fingerprint(src)
                if not verify_snapshot(src, entry["source_snapshot"]):
                    raise ValueError(f"Source changed during verification: {src}")
                source_stat = src.stat()
                missing = []
                parent = dst.parent
                while not parent.exists():
                    missing.append(parent)
                    parent = parent.parent
                dst.parent.mkdir(parents=True, exist_ok=True)
                for parent in missing:
                    original = Path(entry["source_root"]) / parent.relative_to(
                        Path(entry["destination_root"])
                    )
                    shutil.copystat(original, parent)
                    s = parent.stat()
                    directory_times[parent] = (s.st_atime_ns, s.st_mtime_ns)
                # Exclusive creation protects existing server files from overwrite.
                with dst.open("xb") as output:
                    try:
                        log.write(json.dumps({"event": "copy_started", "temporary": str(dst), "verified_source": verified_source, **entry}) + "\n")
                        log.flush()
                        os.fsync(log.fileno())
                        with src.open("rb") as input_stream:
                            if progress:
                                class ProgressReader:
                                    def __init__(self):
                                        self.copied = 0
                                        self.last_update = 0.0

                                    def read(self, size):
                                        block = input_stream.read(size)
                                        self.copied += len(block)
                                        now = time.monotonic()
                                        if now - self.last_update >= 1 or not block:
                                            progress({"phase": "copy", "source": str(src), "destination": str(target), "file_bytes": self.copied, "file_total": verified_source["size"]})
                                            self.last_update = now
                                        return block
                                shutil.copyfileobj(ProgressReader(), output, 1024 * 1024)
                            else:
                                shutil.copyfileobj(input_stream, output, 1024 * 1024)
                        output.flush()
                        os.fsync(output.fileno())
                    except BaseException:
                        dst.unlink()
                        raise
                shutil.copystat(src, dst)
                log.write(json.dumps({"event": "copied", "temporary": str(dst), "verified_source": verified_source, **entry}) + "\n")
                log.flush()
                os.fsync(log.fileno())
                if progress:
                    progress({"phase": "verify_destination", "source": str(src), "destination": str(target)})
                os.utime(dst, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))
                copied = fingerprint(dst)
                if copied["size"] != verified_source["size"] or copied["sha256"] != verified_source["sha256"]:
                    raise ValueError(f"Verification failed; source retained: {src}")
                if copied["mtime_ns"] // 1_000_000_000 != verified_source["mtime_ns"] // 1_000_000_000:
                    raise ValueError(
                        f"Destination cannot preserve the source modification time exactly "
                        f"(source mtime_ns={verified_source['mtime_ns']}, "
                        f"destination mtime_ns={copied['mtime_ns']}). "
                        f"Contents match; source retained: {src}"
                    )
                if fingerprint(src) != verified_source or not verify_snapshot(src, entry["source_snapshot"]):
                    raise ValueError(f"Source changed during copying; source retained: {src}")
                os.utime(dst, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))
                with dst.open("rb") as verified_file:
                    os.fsync(verified_file.fileno())
                if entry["action"] == "repair_zero_byte":
                    if not verify_snapshot(target, entry["destination_snapshot"]):
                        raise ValueError(
                            f"Server repair target changed; verified temp retained: {dst}"
                        )
                    if entry.get("backup"):
                        backup = Path(entry["backup"])
                        # Retain the original zero-byte stub without requiring hardlink support.
                        with backup.open("xb") as stub:
                            stub.flush()
                            os.fsync(stub.fileno())
                        shutil.copystat(target, backup)
                        log.write(
                            json.dumps(
                                {
                                    "event": "zero_byte_backup",
                                    "backup": str(backup),
                                    "temporary": str(dst),
                                    **entry,
                                }
                            )
                            + "\n"
                        )
                        log.flush()
                        os.fsync(log.fileno())
                    dst.replace(target)
                    dst = target
                    if not copy_matches(fingerprint(dst), verified_source) or not verify_snapshot(
                        src, entry["source_snapshot"]
                    ):
                        raise ValueError(
                            f"Repair verification failed; source retained: {src}"
                        )
                sync_directory(dst.parent)
                for created in missing:
                    sync_directory(created.parent)
                log.write(json.dumps({"event": "verified_copy", "verified_source": verified_source, **entry}) + "\n")
                log.flush()
                os.fsync(log.fileno())
                src.unlink()
                sync_directory(src.parent)
                log.write(
                    json.dumps(
                        {
                            "event": "source_removed",
                            "source": str(src),
                            "destination": str(dst),
                            "action": entry["action"],
                            "bytes": verified_source["size"],
                        }
                    )
                    + "\n"
                )
                log.flush()
                os.fsync(log.fileno())
                if progress:
                    progress({"phase": "source_removed", "source": str(src), "destination": str(dst), "bytes": verified_source["size"]})
            for name in sorted(
                plan.get("cleanup_directories", {}),
                key=lambda name: len(Path(name).parts),
                reverse=True,
            ):
                directory = Path(name)
                try:
                    directory.rmdir()
                except OSError as exc:
                    if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST}:
                        raise
                else:
                    log.write(
                        json.dumps({"event": "empty_directory_removed", "path": name})
                        + "\n"
                    )
                    log.flush()
                    os.fsync(log.fileno())
            parent_roots = {Path(info["source_root"]).parent for info in plan.get("cleanup_directories", {}).values()}
            for parent_root in sorted(parent_roots, key=lambda p: len(p.parts), reverse=True):
                if (
                    server != parent_root
                    and server not in parent_root.parents
                    and parent_root not in server.parents
                ):
                    try:
                        parent_root.rmdir()
                    except OSError as exc:
                        if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOENT}:
                            raise
                    else:
                        log.write(
                            json.dumps({"event": "empty_directory_removed", "path": str(parent_root)})
                            + "\n"
                        )
                        log.flush()
                        os.fsync(log.fileno())
        finally:
            for path, times in sorted(
                directory_times.items(),
                key=lambda item: len(item[0].parts),
                reverse=True,
            ):
                if path.exists():
                    os.utime(path, ns=times)


def write_report(path: Path, payload: dict) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2)
    lines = [
        "# Server archive restoration review",
        "",
        f"Server: `{payload['server']}`",
        "",
    ]
    if "matches" in payload:
        lines += [
            f"Server performers: {payload['server_performers']}",
            f"Matching source folders: {len(payload['matches'])}",
            "",
        ]
        for m in payload["matches"]:
            lines += [
                f"- **{m['performer']}**: `{m['source']}` → "
                + ", ".join(f"`{d}`" for d in m["destinations"])
            ]
        lines += ["", "Unavailable roots: " + ", ".join(payload["unavailable_roots"])]
    else:
        for action in (
            "move",
            "remove_duplicate",
            "repair_zero_byte",
            "duplicate",
            "conflict",
            "collision",
        ):
            items = [e for e in payload["entries"] if e["action"] == action]
            lines += [
                f"{action}: {len(items)} files, {sum(e['source_snapshot']['size'] for e in items):,} bytes"
            ]
        lines += [
            "",
            "Approved move, remove_duplicate and repair_zero_byte entries execute. Other conflicts and unverified collisions remain untouched.",
            "",
        ]
        lines += [
            f"Empty source directory checks: {len(payload.get('cleanup_directories', {}))}",
            "",
        ]
        for e in payload["entries"]:
            lines += [f"- **{e['action']}** `{e['source']}` → `{e['destination']}`"]
    with path.with_suffix(".md").open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines) + "\n")
