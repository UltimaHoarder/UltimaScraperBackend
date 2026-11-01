"""Calling the backend that keeps a download: find it, ask where, upload, commit.

Owners are the running backends that announce an address and a certificate
fingerprint; every call pins that fingerprint and carries this backend's peer
token. Uploads resume from whatever the owner already holds.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

from .. import peer_tokens
from ..cluster import read_presence

logger = logging.getLogger(__name__)

CHUNK_BYTES = 1024 * 1024
UPLOAD_ATTEMPTS = 3


class OffloadError(RuntimeError):
    """The owner could not be reached or refused the request."""


@dataclass(frozen=True)
class Owner:
    backend_id: str
    api_url: str
    fingerprint: str


async def find_owners(redis: Any, *, self_id: str, cluster_id: str | None) -> list[Owner]:
    """Other running backends in this cluster that can be reached and pinned."""
    owners = []
    for entry in await read_presence(redis):
        backend_id = str(entry.get("backend_id"))
        api_url, fingerprint = entry.get("api_url"), entry.get("tls_fingerprint")
        if backend_id == self_id or not api_url or not fingerprint:
            continue
        if cluster_id and entry.get("cluster_id") and entry.get("cluster_id") != cluster_id:
            continue
        owners.append(Owner(backend_id=backend_id, api_url=str(api_url).rstrip("/"), fingerprint=str(fingerprint)))
    return sorted(owners, key=lambda owner: owner.backend_id)


async def _file_chunks(path: Path, offset: int) -> AsyncIterator[bytes]:
    handle = await asyncio.to_thread(open, path, "rb")
    try:
        await asyncio.to_thread(handle.seek, offset)
        while chunk := await asyncio.to_thread(handle.read, CHUNK_BYTES):
            yield chunk
    finally:
        await asyncio.to_thread(handle.close)


class OffloadClient:
    """One owner's offload routes, over a session pinned to its certificate."""

    def __init__(self, owner: Owner, token: str, *, session: aiohttp.ClientSession | None = None) -> None:
        self.owner = owner
        self._headers = {peer_tokens.PEER_TOKEN_HEADER: token}
        self._session = session
        self._owns_session = session is None
        self._ssl: Any = (
            aiohttp.Fingerprint(bytes.fromhex(owner.fingerprint.replace(":", "")))
            if owner.api_url.startswith("https://")
            else None
        )

    async def __aenter__(self) -> OffloadClient:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=300))
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def _call(self, method: str, route: str, **kwargs: Any) -> Any:
        assert self._session is not None, "use OffloadClient as an async context manager"
        url = f"{self.owner.api_url}/api/v1/offload/{route}"
        headers = {**self._headers, **kwargs.pop("headers", {})}
        try:
            async with self._session.request(method, url, headers=headers, ssl=self._ssl, **kwargs) as response:
                body = await response.json(content_type=None)
                if response.status >= 400:
                    detail = body.get("detail") if isinstance(body, dict) else body
                    raise OffloadError(f"{self.owner.backend_id} refused {route}: {response.status} {detail}")
                return body
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise OffloadError(f"{self.owner.backend_id} unreachable for {route}: {exc}") from exc

    async def destination(self, **request: Any) -> dict[str, Any]:
        return await self._call("POST", "destination", json=request)

    async def _upload(self, local: Path, route: str, query: dict[str, str], commit: dict[str, Any]) -> dict[str, Any]:
        """Send ``local`` to ``route``, resuming after what arrived, then commit it."""
        size = local.stat().st_size
        last_error: OffloadError | None = None
        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            try:
                state = await self._call("GET", route, params=query)
                if not state.get("exists"):
                    offset = int(state.get("received") or 0)
                    if offset > size:
                        offset = 0
                    if offset < size:
                        await self._call(
                            "PUT",
                            route,
                            params={**query, "offset": str(offset)},
                            data=_file_chunks(local, offset),
                            headers={"Content-Length": str(size - offset)},
                        )
                return await self._call("POST", f"{route}/commit", json={**commit, "size": size})
            except OffloadError as exc:
                last_error = exc
                logger.warning("Upload of %s to %s, attempt %d: %s", local.name, self.owner.backend_id, attempt, exc)
                await asyncio.sleep(min(2**attempt, 10))
        assert last_error is not None
        raise last_error

    async def store(self, local: Path, *, root: str, path: str) -> dict[str, Any]:
        """Upload ``local`` to ``path`` under the owner's ``root`` and commit it."""
        query = {"root": root, "path": path}
        return await self._upload(local, "files", query, {**query, "mtime": local.stat().st_mtime})

    async def capacity(self) -> dict[str, Any]:
        return await self._call("GET", "capacity")

    async def decrypt(
        self, streams: dict[str, Path], *, key: str, root: str, path: str, drm_key: str, mtime: float | None
    ) -> dict[str, Any]:
        """Send encrypted streams for the owner to decrypt, merge and keep at ``path``."""
        total = sum(stream.stat().st_size for stream in streams.values())
        for name, stream in streams.items():
            query = {"key": key, "name": name, "total": str(total)}
            await self._upload(stream, "streams", query, {"key": key, "name": name, "total": total})
        request = {"key": key, "total": total, "root": root, "path": path, "drm_key": drm_key, "mtime": mtime}
        return await self._call("POST", "drm", json=request)


def mirror_base() -> Path:
    """Where a backend without drives builds files before handing them over."""
    configured = os.environ.get("ULTIMA_OFFLOAD_DIR")
    if configured:
        return Path(configured).expanduser()
    from platformdirs import user_cache_dir

    return Path(user_cache_dir("ultima_scraper_verse")) / "offload"
