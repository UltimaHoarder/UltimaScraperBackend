"""A download task on a backend with no download drive of its own.

Files are built locally under a mirror of the owner's folder layout (the same
performer path and format), exactly as they would be on a drive, then handed
to the owner, which keeps them under its own root. Database rows record the
owner's path, since that is where the file lives.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .. import peer_tokens
from ..config import UltimaScraperCollectionConfig
from ..job_system.drive_selection import UserDirectorySelection, has_usable_download_root
from .client import OffloadClient, OffloadError, find_owners, mirror_base

logger = logging.getLogger(__name__)


@dataclass
class RemoteStore:
    """One performer's destination on the owner, and its local mirror."""

    client: OffloadClient
    owner_root: str
    local_root: Path
    selection: UserDirectorySelection
    # The performer's find-only folder on the owner, which is never written to.
    kept_root: str | None = None
    kept_relative_path: str | None = None

    def holds(self, local_path: str | Path) -> bool:
        return Path(local_path).is_relative_to(self.local_root)

    async def keep(self, local_path: str | Path) -> str:
        """Hand a finished file to the owner; returns its path there."""
        local = Path(local_path)
        relative = local.relative_to(self.local_root).as_posix()
        kept_path = await self._kept_copy(local)
        if kept_path is None:
            committed = await self.client.store(local, root=self.owner_root, path=relative)
            kept_path = str(committed["path"])
        await asyncio.to_thread(local.unlink, missing_ok=True)
        await asyncio.to_thread(_prune_empty, local.parent, self.local_root)
        return kept_path

    async def _kept_copy(self, local: Path) -> str | None:
        """The owner's path to this same file in the find-only folder, if it has one."""
        performer_folder = self.local_root / self.selection.performer_relative_path
        if (
            self.kept_root is None
            or self.kept_relative_path is None
            or not local.is_relative_to(performer_folder)
        ):
            return None
        path = f"{self.kept_relative_path}/{local.relative_to(performer_folder).as_posix()}"
        state = await self.client.file_state(root=self.kept_root, path=path)
        if state.get("exists") and state.get("size") == local.stat().st_size:
            return f"{self.kept_root}/{path}"
        return None


def _prune_empty(directory: Path, stop: Path) -> None:
    while directory != stop and directory.is_relative_to(stop):
        try:
            directory.rmdir()
        except OSError:
            return
        directory = directory.parent


class Offload:
    """Destinations from the first owner that can take each performer."""

    def __init__(self, clients: list[OffloadClient]) -> None:
        self.clients = clients

    async def destination(self, **request: Any) -> RemoteStore:
        errors = []
        for client in self.clients:
            try:
                answer = await client.destination(**request)
            except OffloadError as exc:
                errors.append(str(exc))
                continue
            owner_root = answer["root"]
            local_root = mirror_base() / client.owner.backend_id / hashlib.sha256(owner_root.encode()).hexdigest()[:16]
            await asyncio.to_thread(local_root.mkdir, parents=True, exist_ok=True)
            selection = UserDirectorySelection(
                root=local_root,
                performer_relative_path=Path(answer["performer_relative_path"]),
                directory_format=Path(answer["directory_format"]),
                performer_username=answer["performer_username"],
                requires_full_directory_render=bool(answer["requires_full_directory_render"]),
                site_name=answer["site_name"],
            )
            return RemoteStore(
                client=client,
                owner_root=owner_root,
                local_root=local_root,
                selection=selection,
                kept_root=answer.get("kept_root"),
                kept_relative_path=answer.get("kept_relative_path"),
            )
        raise OffloadError("No backend can keep these files: " + "; ".join(errors or ["none reachable"]))


@asynccontextmanager
async def open_offload(
    config: UltimaScraperCollectionConfig, redis: Any
) -> AsyncIterator[Offload | None]:
    """Owners to hand files to, or None when this backend can store them itself."""
    if await asyncio.to_thread(has_usable_download_root, config.settings.download_setup):
        yield None
        return
    token = peer_tokens.own_token()
    if token is None or redis is None:
        logger.warning("No download drive here and no peer token or Redis to reach one")
        yield None
        return
    from ..cluster import get_cluster_id

    owners = await find_owners(
        redis,
        self_id=config.settings.get_backend_id(),
        cluster_id=await asyncio.to_thread(get_cluster_id),
    )
    async with AsyncExitStack() as stack:
        clients = [await stack.enter_async_context(OffloadClient(owner, token)) for owner in owners]
        yield Offload(clients) if clients else None
