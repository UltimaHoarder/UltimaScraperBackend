"""Plan a download batch: everything that needs no network request.

A batch turns content metadata into download items, then settles every item
it can from what is already known: a finished file in a kept folder, a copy
another post, story or message already saved (hardlinked into place), content
or links that expired, media without a link. What is left, ``valid_items``,
is the only part that makes requests. Link refresh, HEAD requests and DRM
licences all come after the plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ...download_manager import DownloadItem
from ...drive_selection import UserDirectorySelection
from ...events import DownloadFailureReason, DownloadResultItem
from ...metadata_models import ContentMetadata
from ...shared_media import SharedMediaCopies, reuse_shared_copies
from .results import categorize_media_items, use_kept_copies
from .types import MediaEntry


@dataclass
class DownloadPlan:
    """What a batch downloads, what it skips and why, decided offline."""

    items: list[DownloadItem]
    valid_items: list[DownloadItem]
    skipped_items: list[DownloadResultItem]
    # Built from the metadata but not downloadable; also in skipped_items.
    construction_failures: list[DownloadResultItem] = field(default_factory=list)
    # Skipped only because their signed link expired: a fresh link saves them.
    expired_items: list[DownloadItem] = field(default_factory=list)
    reused_copies: dict[str, int] = field(default_factory=dict)
    _expired_results: dict[int, list[DownloadResultItem]] = field(
        default_factory=dict, repr=False
    )

    @property
    def total_items(self) -> int:
        return len(self.items) + len(self.construction_failures)

    def _settle(self, item: DownloadItem) -> None:
        valid, skipped = categorize_media_items([item])
        self.valid_items.extend(valid)
        self.skipped_items.extend(skipped)
        if skipped and all(
            result.has_error(DownloadFailureReason.URL_EXPIRED) for result in skipped
        ):
            self.expired_items.append(item)
            self._expired_results[id(item)] = skipped

    def replan(self, items: list[DownloadItem]) -> None:
        """Settle expired *items* again, once their links were refreshed."""
        again = [item for item in items if id(item) in self._expired_results]
        stale = {
            id(result)
            for item in again
            for result in self._expired_results.pop(id(item))
        }
        self.skipped_items = [r for r in self.skipped_items if id(r) not in stale]
        retried = {id(item) for item in again}
        self.expired_items = [i for i in self.expired_items if id(i) not in retried]
        for item in again:
            self._settle(item)


def plan_downloads(
    content_metadatas: list[ContentMetadata],
    user: Any,
    base_destination: Path,
    dependent_media_entries: list[MediaEntry],
    *,
    destination_selection: UserDirectorySelection,
    download_setup: Any | None = None,
    shared_copies: SharedMediaCopies | None = None,
) -> DownloadPlan:
    """Build a batch's items and settle all that can be settled offline.

    Reads the archive and the filesystem only; run it in a thread.
    """
    from .handler import _build_download_items

    construction_failures: list[DownloadResultItem] = []
    items = _build_download_items(
        content_metadatas,
        user,
        base_destination,
        dependent_media_entries,
        failures=construction_failures,
        directory_format_override=destination_selection.directory_format,
        render_full_directory_format=destination_selection.requires_full_directory_render,
        download_setup=download_setup,
    )

    use_kept_copies(items, destination_selection, base_destination)
    reused = reuse_shared_copies(items, shared_copies) if shared_copies is not None else {}

    plan = DownloadPlan(
        items=items,
        valid_items=[],
        skipped_items=[],
        construction_failures=construction_failures,
        reused_copies=reused,
    )
    for item in items:
        plan._settle(item)
    plan.skipped_items.extend(construction_failures)
    return plan
