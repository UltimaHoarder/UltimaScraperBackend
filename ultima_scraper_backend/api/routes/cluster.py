"""The cluster as clients see it: its name and members, invites, and who joined.

A WebUI joins with a one-time invite code and gets its own token, which every
backend in the cluster accepts (see ultima_scraper_backend.cluster_clients).
Everything here but /join needs a token: the backend's admin token or a
client's.
"""

from __future__ import annotations

import ipaddress
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ... import cluster_clients
from ...database.threads import run_database_call
from ...models.backend import BackendInstance
from ...security import extract_admin_token_from_request
from ..dependencies import get_backend_instance

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cluster", tags=["cluster"])


class ClusterMember(BaseModel):
    backend_id: str
    name: str
    # Its HTTPS address for other machines; None when it serves none.
    api_url: str | None = None
    tls_fingerprint: str | None = None
    version: str = ""


class ClusterInfo(BaseModel):
    cluster_id: str | None
    name: str | None
    members: list[ClusterMember]


class NameIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class InviteIn(BaseModel):
    hours: int = Field(default=cluster_clients.DEFAULT_INVITE_HOURS, ge=1, le=cluster_clients.MAX_INVITE_HOURS)


class InviteOut(BaseModel):
    code: str
    expires_at: datetime


class JoinIn(BaseModel):
    code: str = Field(min_length=8, max_length=200)
    name: str = Field(min_length=1, max_length=120)
    kind: str = Field(default="webui", pattern=r"^[a-z][a-z0-9-]{0,30}$")
    version: str | None = Field(default=None, max_length=120)
    # Where the client is reached, as it says; shown in the client list.
    address: str | None = Field(default=None, max_length=300)


class JoinOut(ClusterInfo):
    client_id: str
    # The client's own credential; shown only here.
    token: str


class HeartbeatIn(BaseModel):
    name: str | None = Field(default=None, max_length=120)
    version: str | None = Field(default=None, max_length=120)
    address: str | None = Field(default=None, max_length=300)


class ClientOut(BaseModel):
    id: str
    name: str
    kind: str
    created_at: datetime
    last_seen_at: datetime | None = None
    last_address: str | None = None
    version: str | None = None
    revoked_at: datetime | None = None
    # The client asking.
    is_self: bool = False


async def _members(backend: BackendInstance) -> tuple[str | None, list[ClusterMember]]:
    """This backend and every live member of its cluster, with their HTTPS addresses."""
    from ...backend_tls import tls_fingerprint
    from ...cluster import backend_id, get_cluster_id, public_api_url, read_presence
    from .system import _get_runtime_version

    cluster_id = await run_database_call(get_cluster_id)
    me = backend_id()
    members: dict[str, ClusterMember] = {
        me: ClusterMember(
            backend_id=me,
            name=me,
            api_url=public_api_url(),
            tls_fingerprint=tls_fingerprint(),
            version=_get_runtime_version(),
        )
    }
    if backend.redis is not None:
        try:
            entries = await read_presence(backend.redis)
        except Exception:
            logger.debug("Could not read cluster presence", exc_info=True)
            entries = []
        for entry in entries:
            member_id = str(entry.get("backend_id") or "")
            if not member_id or member_id in members or entry.get("cluster_id") not in (cluster_id, None):
                continue
            members[member_id] = ClusterMember(
                backend_id=member_id,
                name=str(entry.get("hostname") or member_id),
                api_url=entry.get("api_url") or None,
                tls_fingerprint=entry.get("tls_fingerprint") or None,
                version=str(entry.get("version") or ""),
            )
    return cluster_id, sorted(members.values(), key=lambda item: item.name.lower())


def _over_tls_or_local(request: Request) -> bool:
    """A token may only travel over HTTPS, or within this machine."""
    if request.url.scheme == "https":
        return True
    host = request.client.host if request.client else ""
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _requester(request: Request) -> str:
    token = extract_admin_token_from_request(request)
    return "client" if cluster_clients.is_client_token(token) else "admin"


@router.get("", response_model=ClusterInfo)
async def get_cluster(backend: BackendInstance = Depends(get_backend_instance)) -> ClusterInfo:
    cluster_id, members = await _members(backend)
    name = await run_database_call(cluster_clients.cluster_name) if cluster_id else None
    return ClusterInfo(cluster_id=cluster_id, name=name, members=members)


@router.put("/name", response_model=NameIn)
async def set_cluster_name(body: NameIn) -> NameIn:
    return NameIn(name=await run_database_call(cluster_clients.rename_cluster, body.name))


@router.post("/invites", response_model=InviteOut, status_code=201)
async def create_invite(body: InviteIn, request: Request) -> InviteOut:
    code, expires_at = await run_database_call(cluster_clients.create_invite, _requester(request), body.hours)
    return InviteOut(code=code, expires_at=expires_at)


@router.post("/join", response_model=JoinOut)
async def join(body: JoinIn, request: Request, backend: BackendInstance = Depends(get_backend_instance)) -> JoinOut:
    """Redeem an invite: no token needed, the code is the credential."""
    if not _over_tls_or_local(request):
        raise HTTPException(status_code=400, detail="Join over HTTPS: the reply carries a credential.")
    try:
        joined = await run_database_call(
            cluster_clients.redeem_invite,
            body.code,
            name=body.name,
            kind=body.kind,
            version=body.version,
            address=body.address or (request.client.host if request.client else None),
        )
    except cluster_clients.InviteRejected as exc:
        logger.warning("Cluster join refused from %s", request.client.host if request.client else "unknown")
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    cluster_id, members = await _members(backend)
    name = await run_database_call(cluster_clients.cluster_name) if cluster_id else None
    logger.info("Client %s (%s) joined the cluster", body.name, joined.client_id)
    return JoinOut(cluster_id=cluster_id, name=name, members=members, client_id=joined.client_id, token=joined.token)


@router.post("/clients/heartbeat", status_code=204)
async def heartbeat(body: HeartbeatIn, request: Request) -> None:
    token = extract_admin_token_from_request(request) or ""
    if not cluster_clients.is_client_token(token):
        raise HTTPException(status_code=400, detail="Only a client token checks in.")
    address = body.address or (request.client.host if request.client else None)
    if not await run_database_call(
        cluster_clients.record_heartbeat, token, name=body.name, version=body.version, address=address
    ):
        raise HTTPException(status_code=401, detail="This client was revoked.")


@router.get("/clients", response_model=list[ClientOut])
async def list_clients(request: Request) -> list[ClientOut]:
    token = extract_admin_token_from_request(request) or ""
    me = await run_database_call(cluster_clients.client_for_token, token) if cluster_clients.is_client_token(token) else None
    rows: list[dict[str, Any]] = await run_database_call(cluster_clients.list_clients)
    return [ClientOut(**row, is_self=row["id"] == me) for row in rows]


@router.delete("/clients/{client_id}", status_code=204)
async def revoke_client(client_id: str) -> None:
    if not await run_database_call(cluster_clients.revoke_client, client_id):
        raise HTTPException(status_code=404, detail="No such client, or it was already revoked.")
