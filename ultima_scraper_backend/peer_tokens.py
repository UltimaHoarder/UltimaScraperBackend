"""Each backend's own token for calling the other backends on its database.

A backend keeps its token in a file beside its ``.env`` (owner-only), and the
shared database keeps only the token's SHA-256 (management.backend_peers), so
every backend can check a caller without a round trip. Revoking one backend
(revoked_at) stops its calls everywhere within REFRESH_SECONDS and it does not
issue itself another; the other backends keep working.

Peer tokens open only the offload routes, never the admin API, and those
routes always demand one, whatever the security enforcement mode.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import secrets
import time
from pathlib import Path

from sqlalchemy import text

from .cluster_clients import hash_secret

logger = logging.getLogger(__name__)

PEER_TOKEN_PREFIX = "usrp_"
PEER_TOKEN_HEADER = "x-usr-peer-token"
REFRESH_SECONDS = 30.0
_MIN_REFRESH_GAP = 1.0

# token hash -> the backend it belongs to
_active: dict[str, str] = {}
_loaded_at = 0.0
_refresh_lock = asyncio.Lock()
_own_token: str | None = None


class PeerRevoked(RuntimeError):
    """This backend's peer token was revoked; it must not issue itself another."""


def token_path() -> Path:
    from ultima_scraper_core.environment import get_env_path

    return get_env_path().parent / "peer_token"


def peer_backend(token: str | None) -> str | None:
    """The backend a peer token belongs to, from the in-memory set (no I/O)."""
    if not token or not token.startswith(PEER_TOKEN_PREFIX):
        return None
    return _active.get(hash_secret(token))


def own_token() -> str | None:
    """This backend's token, once issued."""
    global _own_token
    if _own_token is None:
        try:
            _own_token = token_path().read_text().strip() or None
        except OSError:
            return None
    return _own_token


def _session():
    from sqlalchemy.orm import Session
    from ultima_scraper_db_archive.global_db import get_global_engine

    return Session(get_global_engine())


def _write_token(path: Path, token: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(token)
    temporary.replace(path)


def ensure_own_token(backend_id: str) -> str:
    """Issue this backend a token unless its file already matches the database.

    Every process of a backend calls this at start; a file lock makes one of
    them issue and the rest read what it wrote.
    """
    global _own_token
    path = token_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_name(f".{path.name}.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            current = path.read_text().strip()
        except OSError:
            current = ""
        with _session() as session:
            row = session.execute(
                text("SELECT token_hash, revoked_at FROM management.backend_peers WHERE backend_id = :backend_id"),
                {"backend_id": backend_id},
            ).one_or_none()
            if row is not None and row.revoked_at is not None:
                raise PeerRevoked(f"Peer token for {backend_id} is revoked")
            if row is not None and current and row.token_hash == hash_secret(current):
                _own_token = current
                return current
            token = f"{PEER_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
            session.execute(
                text(
                    "INSERT INTO management.backend_peers (backend_id, token_hash) VALUES (:backend_id, :token_hash) "
                    "ON CONFLICT (backend_id) DO UPDATE SET token_hash = EXCLUDED.token_hash, created_at = now() "
                    "WHERE management.backend_peers.revoked_at IS NULL"
                ),
                {"backend_id": backend_id, "token_hash": hash_secret(token)},
            )
            session.commit()
        _write_token(path, token)
        _own_token = token
        _active[hash_secret(token)] = backend_id
        logger.info("Issued this backend's peer token (%s)", backend_id)
        return token


def revoke(backend_id: str) -> bool:
    """Stop a backend's peer token everywhere; it will not issue itself another."""
    with _session() as session:
        updated = session.execute(
            text("UPDATE management.backend_peers SET revoked_at = now() WHERE backend_id = :backend_id AND revoked_at IS NULL"),
            {"backend_id": backend_id},
        ).rowcount
        session.commit()
    for token_hash, owner in list(_active.items()):
        if owner == backend_id:
            _active.pop(token_hash, None)
    return bool(updated)


def _load_active() -> dict[str, str]:
    with _session() as session:
        rows = session.execute(
            text("SELECT token_hash, backend_id FROM management.backend_peers WHERE revoked_at IS NULL")
        ).all()
        return {row.token_hash: row.backend_id for row in rows}


async def refresh(*, force: bool = False) -> None:
    """Reload the active token hashes; skipped when done within the last second."""
    global _active, _loaded_at
    from .database.threads import run_database_call

    async with _refresh_lock:
        if not force and time.monotonic() - _loaded_at < _MIN_REFRESH_GAP:
            return
        try:
            _active = await run_database_call(_load_active)
        except Exception:
            # Before setup, or a database without the peer table yet.
            logger.debug("Could not load backend peer tokens", exc_info=True)
        _loaded_at = time.monotonic()


async def refresh_for(token: str | None) -> None:
    """Before checking a token: reload once if it looks like an unknown peer token."""
    if token and token.startswith(PEER_TOKEN_PREFIX) and peer_backend(token) is None:
        await refresh()


async def refresh_loop() -> None:
    while True:
        await refresh(force=True)
        await asyncio.sleep(REFRESH_SECONDS)
