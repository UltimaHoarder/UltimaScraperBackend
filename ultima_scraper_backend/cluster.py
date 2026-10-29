"""Which cluster this backend belongs to, and where it can be reached.

Backends that share one PostgreSQL database form a cluster. The database holds
a single cluster id (``management.cluster``), so every member reports the same
value without any coordination. A backend without a configured database, or on
a schema older than that table, reports no cluster.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from ultima_scraper_core import archive_database_url, runtime_env_value

logger = logging.getLogger(__name__)

# Created by the migration; inserted here too for databases built without it.
_ENSURE_SQL = text(
    "INSERT INTO management.cluster (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
)
_SELECT_SQL = text("SELECT cluster_id FROM management.cluster WHERE id = 1")

# One short-lived record per backend, refreshed by its stats reporter. Kept
# apart from the job-routing registry, which only worker processes join.
PRESENCE_KEY_PREFIX = "ultimascraper:backend:presence:"

_lock = threading.Lock()
_cluster_id: str | None = None


def backend_id() -> str:
    """The id this backend registers under, as the registry derives it."""
    return runtime_env_value("BACKEND_ID") or socket.gethostname()


def public_api_url() -> str | None:
    """Where other machines reach this backend's API: its HTTPS listener.

    Only a backend serving HTTPS on the LAN (shared in its settings, or
    USR_BACKEND_TLS, with USR_ALLOW_REMOTE_BIND) has one. The address is the
    one set for other machines (settings or USR_ADVERTISE_HOST), else this
    machine's LAN address; the port is the HTTPS port. In a container both are
    the container's own, so the deployment passes the host's:
    USR_ADVERTISE_HOST and USR_ADVERTISE_PORT.
    """
    from .backend_tls import advertise_host, tls_enabled, tls_port
    from .security import allow_remote_bind

    if not tls_enabled() or not allow_remote_bind(False):
        return None
    host = advertise_host() or _lan_address()
    if not host:
        return None
    port = (runtime_env_value("USR_ADVERTISE_PORT") or "").strip() or str(tls_port())
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"https://{host}:{port}"


def _lan_address() -> str | None:
    """The address this machine uses on its network (the outbound interface)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            # No packet is sent; connecting picks the route and its source address.
            probe.connect(("192.0.2.1", 9))
            address = probe.getsockname()[0]
    except OSError:
        return None
    return None if address.startswith("127.") else address


def _in_container() -> bool:
    return Path("/.dockerenv").exists() or bool(runtime_env_value("USR_CONTAINER_ROLE"))


async def _observed_address(redis: Any) -> str | None:
    """The address Redis sees this process connect from."""
    try:
        info = await redis.client.client_info()
    except Exception:
        logger.debug("Redis did not say where this backend connects from", exc_info=True)
        return None
    raw = str((info or {}).get("addr") or "")
    host = raw.rsplit(":", 1)[0].strip("[]") if ":" in raw else raw
    try:
        return None if ipaddress.ip_address(host).is_loopback else host
    except ValueError:
        return None


async def host_address(redis: Any) -> str | None:
    """The address of the machine this backend runs on, as its network knows it.

    The address set for other machines (settings or USR_ADVERTISE_HOST), when
    there is one. Otherwise a process outside a
    container knows its own LAN address. A container only knows its Docker
    address, but when Redis is on another machine, Docker translates its
    connections to the host's address, which Redis reports. When Redis sees
    the container's own address (Redis on the same host), there is no answer.
    """
    from .backend_tls import advertise_host

    advertised = advertise_host()
    if advertised:
        return advertised
    own = _lan_address()
    if not _in_container():
        return own
    observed = await _observed_address(redis)
    return observed if observed and observed != own else None


def get_cluster_id() -> str | None:
    """This database's cluster id. Blocking; a found id is cached for good."""
    global _cluster_id
    if _cluster_id is not None:
        return _cluster_id
    if not archive_database_url():
        return None
    with _lock:
        if _cluster_id is not None:
            return _cluster_id
        try:
            from sqlalchemy.orm import Session
            from ultima_scraper_db_archive.global_db import get_global_engine

            with Session(get_global_engine()) as session:
                session.execute(_ENSURE_SQL)
                found = session.execute(_SELECT_SQL).scalar()
                session.commit()
        except Exception:
            # No database yet, or a schema from before clusters.
            logger.debug("Could not read the cluster id", exc_info=True)
            return None
        _cluster_id = str(found) if found is not None else None
        return _cluster_id


def reset_cluster_id_cache() -> None:
    """Forget the cached id (tests, or after the database is replaced)."""
    global _cluster_id
    _cluster_id = None


def runtime_version() -> str:
    """The release this backend runs: the image's version (e.g. 0.1.0-edge.0123abcd4567),
    which the image records in USR_IMAGE_VERSION, else the installed package's."""
    from importlib.metadata import PackageNotFoundError, version

    image = (runtime_env_value("USR_IMAGE_VERSION") or "").strip()
    if image and image != "dev":
        return image
    try:
        return version("ultimascraper")
    except PackageNotFoundError:
        return ""


async def publish_presence(redis: Any, backend_id_value: str, ttl: int) -> None:
    """Say this backend is running, with its cluster and address, for ttl seconds."""
    cluster_id = await asyncio.to_thread(get_cluster_id)
    from .backend_tls import tls_fingerprint

    payload = {
        "backend_id": backend_id_value,
        "hostname": socket.gethostname(),
        "address": await host_address(redis),
        "cluster_id": cluster_id,
        "api_url": public_api_url(),
        "tls_fingerprint": tls_fingerprint(),
        "version": runtime_version(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }
    await redis.set(f"{PRESENCE_KEY_PREFIX}{backend_id_value}", json.dumps(payload), ex=ttl)


async def read_presence(redis: Any) -> list[dict[str, Any]]:
    """Every backend that announced itself within its ttl."""
    entries: list[dict[str, Any]] = []
    client = redis.client
    async for raw_key in client.scan_iter(match=f"{PRESENCE_KEY_PREFIX}*", count=100):
        key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
        raw = await client.get(key)
        if not raw:
            continue
        try:
            entry = json.loads(raw)
        except ValueError:
            continue
        if isinstance(entry, dict) and entry.get("backend_id"):
            entries.append(entry)
    return entries
