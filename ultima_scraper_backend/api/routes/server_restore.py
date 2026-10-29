"""Review-first archive restoration sessions with durable filesystem workers."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from platformdirs import user_data_dir
from pydantic import BaseModel, Field

from ...config import UltimaScraperCollectionConfig
from ...services.server_restore_workspace import Store, category, digest, performer_rows, save
from ..dependencies import get_config

router = APIRouter(prefix="/server-restore", tags=["server-restore"])
ROOT = Path(user_data_dir("ultimascraper")) / "server_restore_reviews"
_launcher = None


def directory(identifier: str) -> Path:
    try:
        return Store(ROOT).directory(identifier)
    except ValueError as exc:
        raise HTTPException(400, "Invalid review ID") from exc


def state(identifier: str) -> dict:
    directory(identifier)
    try:
        return Store(ROOT).state(identifier)
    except FileNotFoundError as exc:
        raise HTTPException(404, "Review not found") from exc


def load_result(identifier: str, kind: str) -> dict:
    job = state(identifier)
    if job["kind"] != kind or job["status"] != "completed":
        raise HTTPException(409, f"A completed {kind} review is required")
    return json.loads((directory(identifier) / "result.json").read_text())


def start(kind: str, request: dict, **extra) -> dict:
    try:
        return Store(ROOT).start(kind, request, launch=_launcher, **extra)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/archives")
async def archives(config: Annotated[UltimaScraperCollectionConfig, Depends(get_config)]):
    def read():
        setup = config.settings.download_setup
        aliases = list(dict.fromkeys(["OnlyFans", *setup.get_site_directory_aliases("onlyfans")]))
        roots = [{"path": str(path.resolve()), "available": path.is_dir(), "accepts_new_performers": drive.store}
                 for drive in setup.drives for path in drive.directories]
        destinations = [{"path": str(Path(root["path"]) / alias), "available": (Path(root["path"]) / alias).is_dir()}
                        for root in roots for alias in aliases if (Path(root["path"]) / alias).is_dir()]
        return {"archives": destinations, "roots": roots}
    return await asyncio.to_thread(read)


class ScanRequest(BaseModel):
    server: str
    excluded_roots: list[str] = Field(default_factory=list)


@router.post("/scan")
async def create_scan(body: ScanRequest, config: Annotated[UltimaScraperCollectionConfig, Depends(get_config)]):
    setup = config.settings.download_setup
    roots = list(dict.fromkeys(str(p.resolve()) for drive in setup.drives for p in drive.directories))
    aliases = list(dict.fromkeys(["OnlyFans", *setup.get_site_directory_aliases("onlyfans")]))
    server = Path(body.server)
    if not server.is_absolute() or server.is_symlink() or server.name not in aliases or str(server.resolve().parent) not in roots:
        raise HTTPException(400, "Choose an OnlyFans site directory directly inside a configured drive root")
    excluded = {str(Path(path).resolve()) for path in body.excluded_roots}
    if excluded - set(roots) or str(server.resolve().parent) in excluded:
        raise HTTPException(400, "Exclude only configured source roots")
    return start("scan", {"server": str(server), "roots": [path for path in roots if path not in excluded],
                         "aliases": aliases, "excluded_roots": sorted(excluded)}, server=str(server))


class PlanRequest(BaseModel):
    scan_id: str
    performers: list[str]
    hash_collisions: bool = False
    remove_duplicates: bool = False
    repair_zero_bytes: bool = False
    cleanup_empty: bool = False
    ignore_db_json: bool = True
    destination_choices: dict[str, str] = Field(default_factory=dict)
    preferred_sources: dict[str, str] = Field(default_factory=dict)
    retained_sources: list[str] = Field(default_factory=list)


@router.post("/plan")
async def create_plan(body: PlanRequest):
    inventory = await asyncio.to_thread(load_result, body.scan_id, "scan")
    if not body.performers:
        raise HTTPException(400, "Select at least one performer")
    options = body.model_dump(exclude={"scan_id", "performers"})
    return start("plan", {"scan_id": body.scan_id, "performers": body.performers, "options": options},
                 server=inventory["server"], scan_id=body.scan_id, performers=body.performers, options=options)


class PreflightRequest(BaseModel):
    plan_id: str
    approved_sha256: str


def reviewed_plan(body: PreflightRequest):
    job = state(body.plan_id)
    plan = load_result(body.plan_id, "plan")
    if body.approved_sha256 != digest(plan):
        raise HTTPException(409, "Approve the exact reviewed plan")
    if job.get("apply_id"):
        raise HTTPException(409, "This plan has already been submitted; reconcile before retrying")
    return job, plan


@router.post("/preflight")
async def create_preflight(body: PreflightRequest):
    job, _plan = await asyncio.to_thread(reviewed_plan, body)
    return start("preflight", body.model_dump(), server=job["server"], scan_id=job["scan_id"], plan_id=body.plan_id)


class ApplyRequest(PreflightRequest):
    writers_paused: bool
    preflight_id: str | None = None


@router.post("/apply")
async def apply_review(body: ApplyRequest):
    job, plan = await asyncio.to_thread(reviewed_plan, body)
    if not body.writers_paused:
        raise HTTPException(409, "Confirm external writers and writers on other storage hosts are paused")
    if body.preflight_id:
        readiness = await asyncio.to_thread(load_result, body.preflight_id, "preflight")
        if state(body.preflight_id).get("plan_id") != body.plan_id or not readiness["ready"] or readiness["plan_sha256"] != body.approved_sha256:
            raise HTTPException(409, "A successful preflight for this exact plan is required")
    return start("apply", body.model_dump(), plan_id=body.plan_id, scan_id=job.get("scan_id"), server=plan["server"])


class RecoverRequest(BaseModel):
    application_id: str


@router.post("/recover")
async def recover(body: RecoverRequest):
    application = state(body.application_id)
    if application["kind"] != "apply" or application["status"] == "running":
        raise HTTPException(409, "A stopped restoration is required for reconciliation")
    parent = state(application["plan_id"])
    return start("plan", {"recovery_from": body.application_id}, scan_id=parent["scan_id"],
                 recovery_from=body.application_id, server=parent["server"])


@router.get("/reviews")
async def reviews():
    return await asyncio.to_thread(Store(ROOT).reviews)


@router.get("/sessions")
async def sessions():
    def read():
        grouped = {}
        for job in Store(ROOT).reviews():
            key = job.get("session_id") or job.get("scan_id") or job["id"]
            item = grouped.setdefault(key, {"id": key, "server": job.get("server"), "latest": job, "reviews": []})
            item["reviews"].append(job)
        return list(grouped.values())
    return await asyncio.to_thread(read)


@router.get("/reviews/{identifier}")
async def get_review(identifier: str):
    return await asyncio.to_thread(state, identifier)


@router.post("/reviews/{identifier}/stop")
async def stop_review(identifier: str):
    def request_stop():
        job = state(identifier)
        if job["status"] != "running":
            raise HTTPException(409, "Only a running restoration operation can be stopped")
        save(directory(identifier) / "stop.json", {"requested": True})
        return {**job, "progress": "Stop requested; waiting for a safe file boundary"}
    return await asyncio.to_thread(request_stop)


@router.get("/reviews/{identifier}/performers")
async def performers(identifier: str, offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200), search: str = ""):
    def read():
        job = state(identifier)
        if job["kind"] not in {"scan", "plan"}:
            raise HTTPException(409, "Select a performer scan or file review")
        result = load_result(identifier, job["kind"])
        items = [item for item in performer_rows(result, job["kind"]) if search.casefold() in item["performer"].casefold()]
        return {"total": len(items), "rows": items[offset:offset + limit], "offset": offset}
    return await asyncio.to_thread(read)


@router.get("/reviews/{identifier}/rows")
async def rows(identifier: str, offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200), performer: str = "", action: str = "all"):
    def read():
        job = state(identifier)
        if job["status"] != "completed" or job["kind"] not in {"scan", "plan"}:
            raise HTTPException(409, "Review is not ready")
        result = load_result(identifier, job["kind"])
        if job["kind"] == "scan":
            data = result["matches"]
        else:
            data = result["entries"]
            competing = {}
            for entry in data:
                competing.setdefault(entry["destination"], []).append({"source": entry["source"], "snapshot": entry["source_snapshot"]})
            data = [{**entry, "competing_sources": competing[entry["destination"]]} if len(competing[entry["destination"]]) > 1 else entry for entry in data]
            data = [{**entry, **{key: {**entry[key], "mtime_ns_exact": str(entry[key]["mtime_ns"])} for key in ("source_snapshot", "destination_snapshot") if entry.get(key)}} for entry in data]
            if action == "all":
                data = [*data, *[{"performer": Path(info["source_root"]).name, "source": name,
                    "action": "remove_empty_if_empty", "destination": "—"} for name, info in result.get("cleanup_directories", {}).items()]]
            else:
                data = [entry for entry in data if category(entry) == action]
        if performer:
            data = [entry for entry in data if entry["performer"].casefold() == performer.casefold()]
        return {"total": len(data), "offset": offset, "rows": data[offset:offset + limit]}
    return await asyncio.to_thread(read)
