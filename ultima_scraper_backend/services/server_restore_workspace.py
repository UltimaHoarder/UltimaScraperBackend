"""Persistent restoration sessions and detached, host-local filesystem workers."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from . import archive_write_guard
from .server_restore_files import apply_plan, copy_matches, fingerprint, make_plan, scan

EXECUTABLE = {"move", "repair_zero_byte", "remove_duplicate", "finish_move"}


class RestorationStopped(Exception):
    """Stop at a checkpoint while retaining a reviewable filesystem state."""


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    with temporary.open("x") as output:
        json.dump(value, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    # Persist the name as well as contents before a worker changes media files.
    handle = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def locked(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False


class Store:
    def __init__(self, root: Path):
        self.root = root

    def directory(self, identifier: str) -> Path:
        return self.root / str(UUID(identifier))

    def state(self, identifier: str) -> dict:
        directory = self.directory(identifier)
        job = json.loads((directory / "state.json").read_text())
        if (directory / "performers.json").exists():
            job["outcomes"] = json.loads((directory / "performers.json").read_text())["outcomes"]
        if job["kind"] == "apply" and job.get("plan_id") and not job.get("scan_id"):
            job["scan_id"] = self.state(job["plan_id"]).get("scan_id")
        if job["kind"] == "plan" and job["status"] == "completed" and not job.get("summary", {}).get("categories"):
            result_path = directory / "result.json"
            if result_path.exists():
                result = json.loads(result_path.read_text())
                job["summary"] = summary(result, "plan")
                job.setdefault("performers", sorted({e["performer"] for e in result["entries"]}))
        if job["status"] == "running" and not locked(directory / "operation.lock"):
            job["status"] = "interrupted"
            job["error"] = "The filesystem worker stopped. Reconcile its checkpoints and review a fresh plan before continuing."
        return job

    def load(self, identifier: str, kind: str) -> dict:
        job = self.state(identifier)
        if job["kind"] != kind or job["status"] != "completed":
            raise ValueError(f"A completed {kind} review is required")
        return json.loads((self.directory(identifier) / "result.json").read_text())

    def reviews(self) -> list[dict]:
        paths = sorted(self.root.glob("*/state.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        return [self.state(path.parent.name) for path in paths]

    def start(self, kind: str, request: dict, *, launch=None, **extra) -> dict:
        self.root.mkdir(parents=True, exist_ok=True)
        global_lock = (self.root / "operation.lock").open("a")
        try:
            fcntl.flock(global_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            global_lock.close()
            raise ValueError("A restoration operation is already running") from None
        identifier = str(uuid4())
        folder = self.directory(identifier)
        folder.mkdir()
        review_lock = (folder / "operation.lock").open("a")
        fcntl.flock(review_lock, fcntl.LOCK_EX)
        job = {"id": identifier, "kind": kind, "status": "running", "progress": "Starting",
               "created_at": datetime.now(UTC).isoformat(), **extra}
        job["session_id"] = job.get("scan_id") or identifier
        try:
            save(folder / "request.json", request)
            save(folder / "state.json", job)
            if kind == "apply":
                parent = self.state(request["plan_id"])
                if parent.get("apply_id"):
                    raise ValueError("This plan has already been submitted; reconcile before retrying")
                parent["apply_id"] = identifier
                save(self.directory(request["plan_id"]) / "state.json", parent)
            if launch:
                launch(self, identifier, global_lock, review_lock)
                global_lock = review_lock = None
            else:
                with (folder / "worker.log").open("ab") as output:
                    process = subprocess.Popen(
                        [sys.executable, "-m", "ultima_scraper_backend.services.server_restore_worker",
                         str(self.root), identifier, str(global_lock.fileno()), str(review_lock.fileno())],
                        pass_fds=(global_lock.fileno(), review_lock.fileno()), start_new_session=True,
                        stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                    )
                # Reap without tying the filesystem worker to API cancellation.
                import threading
                threading.Thread(target=process.wait, daemon=True).start()
        except BaseException as exc:
            job.update(status="failed", error=str(exc))
            save(folder / "state.json", job)
            raise
        finally:
            if review_lock is not None:
                review_lock.close()
            if global_lock is not None:
                global_lock.close()
        return job


def category(entry: dict) -> str:
    action = entry["action"]
    if action in {"move", "finish_move"}:
        return "restore"
    if action == "repair_zero_byte" or entry.get("reason") == "zero_byte":
        return "repair"
    if action in {"duplicate", "remove_duplicate"}:
        return "duplicate"
    if action == "collision":
        return "collision"
    if action == "retain":
        return "retain"
    return "conflict"


def performer_rows(result: dict, kind: str) -> list[dict]:
    grouped = {}
    for entry in result.get("matches" if kind == "scan" else "entries", []):
        key = entry["performer"].casefold()
        item = grouped.setdefault(key, {"performer": entry["performer"], "sources": set(),
            "destinations": set(), "counts": Counter(), "bytes": 0, "files": 0})
        item["sources"].add(entry.get("source_root", entry["source"]))
        item["destinations"].update(entry.get("destinations", [entry.get("destination_root", entry.get("destination"))]))
        if kind == "plan":
            item["counts"][category(entry)] += 1
            item["files"] += 1
            if entry["action"] in {"move", "repair_zero_byte"}:
                item["bytes"] += entry["source_snapshot"]["size"]
    for blocked in result.get("blocked_performers", []):
        item = grouped.setdefault(blocked["performer"].casefold(), {"performer": blocked["performer"],
            "sources": set(), "destinations": set(), "counts": Counter(), "bytes": 0, "files": 0})
        item["error"] = blocked["error"]
        item["destinations"].update(blocked.get("destinations", []))
    return [{**item, "sources": sorted(item["sources"]), "destinations": sorted(item["destinations"]),
             "counts": dict(item["counts"])} for _, item in sorted(grouped.items())]


def summary(result: dict, kind: str) -> dict:
    if kind == "scan":
        return {"server_performers": result["server_performers"],
                "matched_performers": len({m["performer"].casefold() for m in result["matches"]}),
                "matched_folders": len(result["matches"]), "unavailable_roots": result["unavailable_roots"],
                "roots": result.get("roots", [])}
    if kind == "plan":
        return {"counts": dict(Counter(e["action"] for e in result["entries"])),
                "categories": dict(Counter(category(e) for e in result["entries"])),
                "cleanup_directories": len(result.get("cleanup_directories", {})),
                "move_bytes": sum(e["source_snapshot"]["size"] for e in result["entries"] if e["action"] in {"move", "repair_zero_byte"}),
                "blocked_performers": result.get("blocked_performers", []),
                "recovery": result.get("recovery")}
    return result


def writer_roots(inventory: dict, plan: dict) -> list[str]:
    candidates = [Path(item["path"]) for item in inventory.get("roots", [])]
    roots = {Path(name) for name in plan.get("root_snapshots", {})}
    roots.update(Path(e[key]) for e in plan["entries"] for key in ("source_root", "destination_root"))
    return sorted({str(next((p for p in candidates if root.is_relative_to(p)), root.parent.parent.parent)) for root in roots})


def preflight(plan: dict, inventory: dict, *, writers_reserved: bool = False, progress=None) -> dict:
    checks = []
    devices = {}
    required = sum(e["source_snapshot"]["size"] for e in plan["entries"] if e["action"] in {"move", "repair_zero_byte"})
    try:
        if progress:
            progress("Checking archive roots")
        for name, expected in plan.get("root_snapshots", {}).items():
            root_path = Path(name)
            if not root_path.is_dir() or root_path.is_symlink():
                raise ValueError(f"Archive root changed or mount unavailable: {name}")
            current = root_path.stat()
            if {"device": current.st_dev, "inode": current.st_ino} != expected:
                if current.st_dev != expected.get("device"):
                    raise ValueError(f"Archive root changed or mount unavailable: {name}")
        checks.append({"name": "Reviewed archive roots", "ok": True, "detail": "Source and destination identities match"})
    except (OSError, ValueError) as exc:
        checks.append({"name": "Reviewed archive roots", "ok": False, "detail": str(exc)})
    try:
        dst_ancestor_cache: dict[Path, tuple[Path, int]] = {}
        checked_access: set[Path] = set()
        total_entries = len(plan["entries"])
        for idx, entry in enumerate(plan["entries"], start=1):
            if entry["action"] not in EXECUTABLE:
                continue
            parent = Path(entry["destination"]).parent
            cached = dst_ancestor_cache.get(parent)
            if cached is None:
                curr = parent
                while not curr.exists():
                    curr = curr.parent
                dev = curr.stat().st_dev
                cached = (curr, dev)
                dst_ancestor_cache[parent] = cached
            existing_parent, device = cached
            if device not in devices:
                devices[device] = {"path": str(existing_parent), "required": 0, "free": shutil.disk_usage(existing_parent).free}
            info = devices[device]
            if entry["action"] in {"move", "repair_zero_byte"}:
                info["required"] += entry["source_snapshot"]["size"]
            if existing_parent not in checked_access:
                if not os.access(existing_parent, os.W_OK | os.X_OK):
                    raise ValueError(f"Write permission unavailable: {existing_parent}")
                checked_access.add(existing_parent)
            src_parent = Path(entry["source"]).parent
            if src_parent not in checked_access:
                if not os.access(src_parent, os.W_OK | os.X_OK):
                    raise ValueError(f"Write or source-removal permission unavailable: {entry['source']}")
                checked_access.add(src_parent)
            if progress and (idx % 250 == 0 or idx == total_entries):
                progress(f"Checking destination space and permissions ({idx:,} / {total_entries:,})")
        for name in plan.get("cleanup_directories", {}):
            clean_parent = Path(name).parent
            if clean_parent not in checked_access:
                if not os.access(clean_parent, os.W_OK | os.X_OK):
                    raise ValueError(f"Folder cleanup permission unavailable: {name}")
                checked_access.add(clean_parent)
        if any(item["required"] > item["free"] for item in devices.values()):
            raise ValueError("Insufficient destination space for reviewed copies")
        checks.append({"name": "Space and permissions", "ok": True, "detail": f"{required:,} bytes required", "volumes": list(devices.values())})
    except (OSError, ValueError) as exc:
        checks.append({"name": "Space and permissions", "ok": False, "detail": str(exc)})
    try:
        if progress:
            progress("Checking managed writer reservations")
        if not writers_reserved:
            with archive_write_guard.acquire(writer_roots(inventory, plan), exclusive=True):
                pass
        checks.append({"name": "Managed writers on this host", "ok": True, "detail": "Affected drive roots are reserved" if writers_reserved else "Affected drive roots can be reserved"})
    except (OSError, ValueError) as exc:
        checks.append({"name": "Managed writers on this host", "ok": False, "detail": "A scraper/download writer is active" if isinstance(exc, BlockingIOError) else str(exc)})
    try:
        apply_plan(plan, Path("unused-preflight-journal"), validate_only=True, progress=progress)
        checks.append({"name": "Reviewed file snapshots", "ok": True, "detail": "Files still match the exact plan"})
    except (OSError, ValueError) as exc:
        checks.append({"name": "Reviewed file snapshots", "ok": False, "detail": str(exc)})
    executable = sum(e["action"] in EXECUTABLE for e in plan["entries"]) + len(plan.get("cleanup_directories", {}))
    checks.append({"name": "Approved operations", "ok": bool(executable), "detail": f"{executable:,} executable file and folder actions"})
    return {"ready": all(check["ok"] for check in checks), "checks": checks, "plan_sha256": digest(plan)}


def recovery_plan(store: Store, application_id: str, progress) -> dict:
    application = store.state(application_id)
    if application["kind"] != "apply" or application["status"] == "running":
        raise ValueError("A stopped restoration is required for reconciliation")
    original = store.load(application["plan_id"], "plan")
    inventory = store.load(store.state(application["plan_id"])["scan_id"], "scan")
    checkpoints = {}
    for journal in sorted(store.directory(application_id).glob("performer-*.jsonl")):
        for line in journal.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue  # A killed worker may leave one incomplete final record.
            if event.get("event") in {"copy_started", "copied", "verified_copy", "verified_duplicate"}:
                checkpoints[event["source"]] = event
    completed, finish, blocked, temporary = [], {}, [], []
    for entry in original["entries"]:
        if entry["action"] not in EXECUTABLE:
            continue
        source, target = Path(entry["source"]), Path(entry["destination"])
        checkpoint = checkpoints.get(str(source))
        expected = (checkpoint or {}).get("verified_source") or (checkpoint or {}).get("source_snapshot")
        progress(f"Reconciling {source}")
        try:
            if not source.exists():
                if not checkpoint or not expected or "sha256" not in expected or not copy_matches(fingerprint(target), expected):
                    raise ValueError(f"Source is absent and the destination cannot be verified: {source}")
                completed.append(str(source))
            elif checkpoint and expected and "sha256" in expected and target.is_file() and copy_matches(fingerprint(target), expected) and copy_matches(fingerprint(source), expected):
                finish[str(source)] = {**entry, "action": "finish_move", "reason": "verified_recovery",
                    "source_snapshot": fingerprint(source), "destination_snapshot": fingerprint(target), "recovered_copy": expected}
            if checkpoint and checkpoint.get("temporary") and checkpoint["temporary"] != str(target) and Path(checkpoint["temporary"]).exists():
                temporary.append(checkpoint["temporary"])
        except (OSError, ValueError) as exc:
            blocked.append({"performer": entry["performer"], "error": str(exc)})
    roots = [Path(item["path"]) for item in inventory.get("roots", []) if item["status"] != "excluded"]
    if not roots:
        roots = list({Path(m["source"]).parents[2] for m in inventory["matches"]})
    fresh = scan(Path(inventory["server"]), roots, inventory.get("aliases", ["OnlyFans", Path(inventory["server"]).name]), progress=progress)
    selected = original.get("performers") or sorted({entry["performer"] for entry in original["entries"]})
    available = {m["performer"].casefold() for m in fresh["matches"]}
    options = original.get("options", {}).copy()
    options["preferred_sources"] = {dst: src for dst, src in options.get("preferred_sources", {}).items() if Path(src).is_file()}
    options["retained_sources"] = [src for src in options.get("retained_sources", []) if Path(src).is_file()]
    result = make_plan(fresh, [name for name in selected if name.casefold() in available],
                       isolate_performers=True, progress=progress, **options)
    result["entries"] = [finish.get(entry["source"], entry) for entry in result["entries"]]
    bad = {item["performer"].casefold() for item in blocked}
    result["entries"] = [entry for entry in result["entries"] if entry["performer"].casefold() not in bad]
    result["cleanup_directories"] = {name: info for name, info in result["cleanup_directories"].items() if Path(info["source_root"]).name.casefold() not in bad}
    result["blocked_performers"].extend(blocked)
    result["recovery"] = {"completed_files": len(completed), "verified_copies_to_finish": len(finish),
                          "retained_temporary_files": sorted(set(temporary)), "application_id": application_id}
    return result


def run(store: Store, identifier: str) -> None:
    folder = store.directory(identifier)
    job = json.loads((folder / "state.json").read_text())
    request = json.loads((folder / "request.json").read_text())
    started = time.monotonic()
    last_saved = 0.0
    counters = {"completed_files": 0, "completed_bytes": 0}

    def progress(value):
        nonlocal last_saved
        if isinstance(value, dict):
            if value.get("phase") == "source_removed":
                counters["completed_files"] += 1
                counters["completed_bytes"] += value.get("bytes", 0)
            job["telemetry"] = {**job.get("telemetry", {}), **counters, **value,
                "elapsed_seconds": time.monotonic() - started,
                "bytes_per_second": counters["completed_bytes"] / max(time.monotonic() - started, .001)}
            if "message" in value:
                job["progress"] = value["message"]
            elif value.get("phase") == "validate_snapshot" and "completed" in value and "total" in value:
                job["progress"] = f"Validating file snapshots ({value['completed']:,} / {value['total']:,})"
            else:
                job["progress"] = value.get("phase", "Working").replace("_", " ")
        else:
            job["progress"] = value
        if (folder / "stop.json").exists() and (not isinstance(value, dict) or value.get("phase") in {"source_removed", "validate_snapshot"}):
            raise RestorationStopped("Stopped at a safe file boundary. Reconcile completed checkpoints before continuing restoration.")
        if isinstance(value, dict) and value.get("phase") != "source_removed" and time.monotonic() - last_saved < 1:
            return
        save(folder / "state.json", job)
        last_saved = time.monotonic()

    try:
        kind = job["kind"]
        if kind == "scan":
            result = scan(Path(request["server"]), [Path(p) for p in request["roots"]], request["aliases"], progress=progress)
            result["roots"].extend({"path": path, "status": "excluded", "matched_folders": 0} for path in request.get("excluded_roots", []))
        elif kind == "plan":
            if request.get("recovery_from"):
                result = recovery_plan(store, request["recovery_from"], progress)
            else:
                inventory = store.load(request["scan_id"], "scan")
                result = make_plan(inventory, request["performers"], isolate_performers=True, progress=progress, **request["options"])
            job["performers"] = result.get("performers", request.get("performers", []))
            job["options"] = result.get("options", {})
        elif kind in {"preflight", "apply"}:
            plan = store.load(request["plan_id"], "plan")
            if request["approved_sha256"] != digest(plan):
                raise ValueError("The exact reviewed plan has changed")
            inventory = store.load(job["scan_id"], "scan")
            if kind == "preflight":
                with archive_write_guard.acquire(writer_roots(inventory, plan), exclusive=True, wait=True, progress=progress):
                    result = preflight(plan, inventory, writers_reserved=True, progress=progress)
            else:
                # Hold exclusive leases through revalidation and all filesystem work.
                with archive_write_guard.acquire(writer_roots(inventory, plan), exclusive=True, wait=True, progress=progress):
                    readiness = preflight(plan, inventory, writers_reserved=True, progress=progress)
                    if not all(c["ok"] for c in readiness["checks"]):
                        raise ValueError(next(c["detail"] for c in readiness["checks"] if not c["ok"]))
                    groups = {}
                    for entry in plan["entries"]:
                        groups.setdefault(entry["performer"].casefold(), []).append(entry)
                    for info in plan.get("cleanup_directories", {}).values():
                        groups.setdefault(Path(info["source_root"]).name.casefold(), [])
                    job["telemetry"] = {"total_files": sum(e["action"] in EXECUTABLE for e in plan["entries"]),
                        "total_bytes": sum(e["source_snapshot"]["size"] for e in plan["entries"] if e["action"] in {"move", "repair_zero_byte"})}
                    outcomes = []
                    for index, (performer, entries) in enumerate(groups.items(), 1):
                        progress(f"Restoring performer {index}/{len(groups)}: {performer}")
                        roots = {entry[key] for entry in entries for key in ("source_root", "destination_root")}
                        roots.update(name for name in plan.get("root_snapshots", {}) if Path(name).name.casefold() == performer)
                        subset = {**plan, "entries": entries,
                            "root_snapshots": {k: v for k, v in plan.get("root_snapshots", {}).items() if k in roots},
                            "cleanup_directories": {k: v for k, v in plan.get("cleanup_directories", {}).items() if v["source_root"] in roots}}
                        try:
                            apply_plan(subset, folder / f"performer-{index}.jsonl", progress=progress)
                        except RestorationStopped:
                            raise
                        except Exception as exc:
                            outcomes.append({"performer": performer, "status": "failed", "error": str(exc)})
                        else:
                            outcomes.append({"performer": performer, "status": "completed"})
                        save(folder / "performers.json", {"outcomes": outcomes})
                    result = {"plan_id": request["plan_id"], "outcomes": outcomes,
                        "categories": dict(Counter(category(e) for e in plan["entries"] if e["action"] not in EXECUTABLE)),
                        "blocked_performers": plan.get("blocked_performers", [])}
                    job["outcomes"] = outcomes
                    if any(item["status"] == "failed" for item in outcomes):
                        job["status"] = "failed"
                        job["error"] = "Some performers need checkpoint reconciliation and a new review."
        else:
            raise ValueError("Unsupported restoration operation")
        save(folder / "result.json", result)
        job["sha256"] = digest(result)
        job["summary"] = summary(result, kind)
        if job["status"] != "failed":
            job["status"] = "completed"
            job["progress"] = "Completed"
    except RestorationStopped as exc:
        job.update(status="interrupted", error=str(exc))
    except Exception as exc:
        job.update(status="failed", error=str(exc))
    finally:
        job["completed_at"] = datetime.now(UTC).isoformat()
        save(folder / "state.json", job)
