"""Clients (WebUIs) that join the cluster with an invite, and their tokens.

Every backend on the cluster's database accepts a client's token next to its
own USR_ADMIN_TOKEN. Invites are one-time and expire; a client's token works
until it is revoked. Only SHA-256 hashes of codes and tokens are stored
(management.cluster_clients, management.cluster_invites).

Request authentication must not wait on the database, so each backend keeps
the hashes of active tokens in memory: reloaded every REFRESH_SECONDS, and
right away (at most once a second) when an unknown client token arrives, so a
client that just joined through another backend is accepted at once. A
revocation reaches the other backends within REFRESH_SECONDS.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

CLIENT_TOKEN_PREFIX = "usrc_"
INVITE_CODE_PREFIX = "usri_"
REFRESH_SECONDS = 30.0
_MIN_REFRESH_GAP = 1.0
DEFAULT_INVITE_HOURS = 24
MAX_INVITE_HOURS = 24 * 7

_active_hashes: frozenset[str] = frozenset()
_loaded_at = 0.0
_refresh_lock = asyncio.Lock()


class InviteRejected(ValueError):
    """The code is unknown, used or expired."""


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def is_client_token(token: str | None) -> bool:
    """Whether a token is an active client's, from the in-memory set (no I/O)."""
    return bool(token) and token.startswith(CLIENT_TOKEN_PREFIX) and hash_secret(token) in _active_hashes  # type: ignore[union-attr]


def _session():
    from sqlalchemy.orm import Session
    from ultima_scraper_db_archive.global_db import get_global_engine

    return Session(get_global_engine())


def _load_active_hashes() -> frozenset[str]:
    with _session() as session:
        rows = session.execute(
            text("SELECT token_hash FROM management.cluster_clients WHERE revoked_at IS NULL")
        ).scalars()
        return frozenset(rows)


async def refresh(*, force: bool = False) -> None:
    """Reload the active token hashes; skipped when done within the last second."""
    global _active_hashes, _loaded_at
    from .database.threads import run_database_call

    async with _refresh_lock:
        if not force and time.monotonic() - _loaded_at < _MIN_REFRESH_GAP:
            return
        try:
            _active_hashes = await run_database_call(_load_active_hashes)
        except Exception:
            # Before setup, or a database without the cluster tables yet.
            logger.debug("Could not load cluster client tokens", exc_info=True)
        _loaded_at = time.monotonic()


async def refresh_for(token: str | None) -> None:
    """Before checking a token: reload once if it looks like an unknown client token."""
    if token and token.startswith(CLIENT_TOKEN_PREFIX) and not is_client_token(token):
        await refresh()


async def refresh_loop() -> None:
    while True:
        await refresh(force=True)
        await asyncio.sleep(REFRESH_SECONDS)


def _remember(token_hash: str) -> None:
    global _active_hashes
    _active_hashes = _active_hashes | {token_hash}


def _forget(token_hash: str) -> None:
    global _active_hashes
    _active_hashes = _active_hashes - {token_hash}


# ---- Database operations (run with run_database_call) -------------------------


def create_invite(created_by: str, hours: int = DEFAULT_INVITE_HOURS) -> tuple[str, datetime]:
    """A new one-time code, and when it expires."""
    hours = max(1, min(int(hours), MAX_INVITE_HOURS))
    code = f"{INVITE_CODE_PREFIX}{secrets.token_urlsafe(18)}"
    expires_at = datetime.now(timezone.utc) + timedelta(hours=hours)
    with _session() as session:
        session.execute(
            text(
                "INSERT INTO management.cluster_invites (code_hash, created_by, expires_at) "
                "VALUES (:code_hash, :created_by, :expires_at)"
            ),
            {"code_hash": hash_secret(code), "created_by": created_by, "expires_at": expires_at},
        )
        session.commit()
    return code, expires_at


@dataclass(frozen=True)
class Joined:
    client_id: str
    token: str


def redeem_invite(code: str, *, name: str, kind: str, version: str | None, address: str | None) -> Joined:
    """Use a code once: create the client and its token, or raise InviteRejected."""
    token = f"{CLIENT_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
    with _session() as session:
        invite = session.execute(
            text(
                "SELECT id FROM management.cluster_invites "
                "WHERE code_hash = :code_hash AND used_at IS NULL AND expires_at > now() "
                "FOR UPDATE"
            ),
            {"code_hash": hash_secret(code.strip())},
        ).scalar()
        if invite is None:
            raise InviteRejected("The invite is unknown, already used, or expired.")
        client_id = session.execute(
            text(
                "INSERT INTO management.cluster_clients (name, kind, token_hash, last_seen_at, last_address, version) "
                "VALUES (:name, :kind, :token_hash, now(), :address, :version) RETURNING id"
            ),
            {
                "name": name.strip()[:120] or "WebUI",
                "kind": kind,
                "token_hash": hash_secret(token),
                "address": address,
                "version": version,
            },
        ).scalar_one()
        session.execute(
            text("UPDATE management.cluster_invites SET used_at = now(), used_by = :client WHERE id = :id"),
            {"client": client_id, "id": invite},
        )
        session.commit()
    _remember(hash_secret(token))
    return Joined(client_id=str(client_id), token=token)


def client_for_token(token: str) -> str | None:
    """The id of the active client holding this token."""
    with _session() as session:
        found = session.execute(
            text("SELECT id FROM management.cluster_clients WHERE token_hash = :h AND revoked_at IS NULL"),
            {"h": hash_secret(token)},
        ).scalar()
    return str(found) if found else None


def record_heartbeat(token: str, *, name: str | None, version: str | None, address: str | None) -> bool:
    """A client says it is still here; False for an unknown or revoked token."""
    with _session() as session:
        updated = session.execute(
            text(
                "UPDATE management.cluster_clients SET last_seen_at = now(), "
                "name = COALESCE(:name, name), version = COALESCE(:version, version), "
                "last_address = COALESCE(:address, last_address) "
                "WHERE token_hash = :h AND revoked_at IS NULL"
            ),
            {"h": hash_secret(token), "name": (name or "").strip()[:120] or None, "version": version, "address": address},
        ).rowcount
        session.commit()
    return bool(updated)


def list_clients() -> list[dict[str, Any]]:
    with _session() as session:
        rows = session.execute(
            text(
                "SELECT id, name, kind, created_at, last_seen_at, last_address, version, revoked_at "
                "FROM management.cluster_clients ORDER BY revoked_at IS NOT NULL, name"
            )
        ).mappings()
        return [{**row, "id": str(row["id"])} for row in rows]


def revoke_client(client_id: str) -> bool:
    try:
        uuid.UUID(client_id)
    except ValueError:
        return False
    with _session() as session:
        token_hash = session.execute(
            text(
                "UPDATE management.cluster_clients SET revoked_at = now() "
                "WHERE id = :id AND revoked_at IS NULL RETURNING token_hash"
            ),
            {"id": client_id},
        ).scalar()
        session.commit()
    if token_hash:
        _forget(token_hash)
    return token_hash is not None


def cluster_name() -> str | None:
    with _session() as session:
        return session.execute(text("SELECT name FROM management.cluster WHERE id = 1")).scalar()


def rename_cluster(name: str) -> str:
    cleaned = name.strip()[:80]
    with _session() as session:
        session.execute(text("UPDATE management.cluster SET name = :name WHERE id = 1"), {"name": cleaned})
        session.commit()
    return cleaned
