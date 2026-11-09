"""Result handling and categorization for downloads.

This module contains functions for categorizing download items,
handling HTTP errors, and resolving content IDs.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ultima_scraper_api.helpers.url_diagnostics import diagnose_url

from ...download_manager import DownloadItem, file_has_content
from ...drive_selection import UserDirectorySelection
from ...events import DownloadFailureReason, DownloadResultItem
from .utils import utcnow

logger = logging.getLogger(__name__)


def resolve_content_id(item: DownloadItem) -> int | None:
    """Resolve content ID from download item or its metadata.

    Args:
        item: Download item to resolve ID from

    Returns:
        Content ID or None if not available
    """
    if item.content_id is not None:
        return item.content_id
    media_metadata = item.media_metadata
    if media_metadata is not None and media_metadata.content_metadata is not None:
        return media_metadata.content_metadata.content_id
    return None


def use_kept_copies(
    download_items: list[DownloadItem],
    selection: UserDirectorySelection,
    base_destination: Path,
) -> None:
    """Point items at finished files in a find-only folder, so nothing fetches them again.

    A copy of the wrong size is left alone and the item downloads to its own
    folder, since a find-only folder is never written to.
    """
    for item in download_items:
        if not item.directory or not item.filename:
            continue
        kept = selection.kept_directory_for(item.directory, base_destination)
        if kept is None:
            continue
        item.kept_directory = kept
        copy = kept / item.filename
        if file_has_content(item.get_filepath()) or not file_has_content(copy):
            continue
        is_drm = item.media_metadata is not None and item.media_metadata.drm
        if is_drm or item.size is None or copy.stat().st_size == item.size:
            item.directory = kept


def categorize_media_items(
    download_items: list[DownloadItem],
) -> tuple[
    list[DownloadItem],  # valid items (ready to download)
    list[DownloadResultItem],  # skipped items with failure reasons
]:
    """
    Categorize media items by their download eligibility.

    Checks content expiry (PostModel.expires_at, MessageModel.expires_at) and
    URL expiry (MediaUrlModel.expires_at) to determine download viability.

    Categories:
    - valid: Content not expired, URL not expired (or URL expiry unknown)
    - content_expired: Content has expired (creator deleted scheduled content)
    - no_url: Media has no download URL (needs rescrape to fetch)
    - url_expired: URL signature has expired (needs re-scrape)

    Note: NULL expires_at means unknown/permanent, which we treat optimistically.

    Args:
        download_items: List of DownloadItem instances to categorize

    Returns:
        Tuple of (valid_items, skipped_items)
        - valid_items: Items ready for download
        - skipped_items: Items with pre-download failure reasons
    """
    valid_items: list[DownloadItem] = []
    skipped_items: list[DownloadResultItem] = []
    now = utcnow()

    for item in download_items:
        content_id = resolve_content_id(item)
        media_metadata = item.media_metadata
        media_exists = item.exists()
        pass

        # Check content expiry (if metadata is available)
        if media_metadata and media_metadata.content_metadata:
            content_expires = media_metadata.content_metadata.expires_at
            if content_expires and content_expires < now:
                result = DownloadResultItem(
                    media_id=item.media_id,
                    content_id=content_id,
                    status="skipped",
                    path=str(item.get_filepath()) if item.filename else None,
                    url=item.url,
                    bytes_written=0,
                )
                result.add_error(
                    DownloadFailureReason.CONTENT_EXPIRED,
                    f"Content expired at {content_expires.isoformat()}",
                )
                skipped_items.append(result)
                continue

        # Check for missing URL (media has no download URL at all)
        if not item.url:
            result = DownloadResultItem(
                media_id=item.media_id,
                content_id=content_id,
                status="skipped",
                path=str(item.get_filepath()) if item.filename else None,
                url=None,
                bytes_written=0,
            )
            result.add_error(
                DownloadFailureReason.NO_URL,
                "No download URL available for this media",
            )
            skipped_items.append(result)
            continue

        # Check URL expiry using diagnose_url (analyzes CloudFront signed URLs)
        if item.url:
            url_diag = diagnose_url(item.url)
            if (
                url_diag.policy
                and url_diag.policy.is_expired
                and url_diag.policy.expires_at
            ):
                result = DownloadResultItem(
                    media_id=item.media_id,
                    content_id=content_id,
                    status="skipped",
                    path=str(item.get_filepath()) if item.filename else None,
                    url=item.url,
                    bytes_written=0,
                )
                result.add_error(
                    DownloadFailureReason.URL_EXPIRED,
                    f"URL expired at {url_diag.policy.expires_at.isoformat()}",
                )
                skipped_items.append(result)
                continue

        # Skip files that already exist on disk with the correct size
        if media_exists:
            existing_size = item.get_filepath().stat().st_size
            if item.size is None or existing_size == item.size:
                result = DownloadResultItem(
                    media_id=item.media_id,
                    content_id=content_id,
                    status="skipped",
                    path=str(item.get_filepath()) if item.filename else None,
                    url=item.url,
                    bytes_written=existing_size,
                )
                result.add_error(
                    DownloadFailureReason.ALREADY_EXISTS,
                    f"File already exists on disk ({existing_size:,} bytes)",
                )
                skipped_items.append(result)
                continue

        valid_items.append(item)

    return valid_items, skipped_items


def add_url_diagnostics(
    result: DownloadResultItem,
    url: str | None,
    *,
    http_status: int | None = None,
) -> str:
    """Inspect *url* and add expiry/IP-lock errors to *result*.

    IP-locked URLs are **not** automatically flagged as ``IP_MISMATCH``.
    An IP restriction in the CloudFront policy is normal when downloading
    through a proxy — the URL is bound to the proxy's exit IP.  We only
    flag ``IP_MISMATCH`` when an HTTP 401/403 indicates the CDN actually
    rejected the request *and* the URL carries an IP restriction (meaning
    the download IP likely differs from the locked IP).

    Args:
        result: Result object to attach errors to.
        url: CDN URL to diagnose.
        http_status: HTTP status code from the failed request, if known.

    Returns:
        Human-readable diagnosis string.
    """
    if not url:
        return "No URL available"

    url_diag = diagnose_url(url)
    if url_diag.policy:
        if url_diag.policy.is_expired and url_diag.policy.expires_at:
            result.add_error(
                reason=DownloadFailureReason.URL_EXPIRED,
                message=f"URL expired at {url_diag.policy.expires_at.isoformat()}",
                details={"expires_at": url_diag.policy.expires_at.isoformat()},
            )
        # Only flag IP_MISMATCH when an HTTP auth error confirms the CDN
        # rejected the request — presence of an IP lock alone is expected
        # when using a proxy.
        if (
            url_diag.policy.is_ip_locked
            and http_status is not None
            and http_status in (401, 403)
        ):
            result.add_error(
                reason=DownloadFailureReason.IP_MISMATCH,
                message=f"URL is IP-locked to {url_diag.policy.ip_address}",
                details={"locked_ip": url_diag.policy.ip_address},
            )

    return url_diag.get_diagnosis()


def handle_download_http_error(
    item: DownloadItem,
    status_code: int,
    error_message: str,
) -> DownloadResultItem:
    """
    Convert HTTP error codes to appropriate failure reasons.

    HTTP Status Code Mapping:
    - 403/401: URL signature expired OR IP mismatch (CDN rejected auth)
    - 404: Content was deleted during download
    - Other: Generic download error

    Args:
        item: The download item that failed
        status_code: HTTP status code from the failed request
        error_message: Error message from the exception

    Returns:
        DownloadResultItem with appropriate failure reason(s)
    """
    media_id = item.media_id or 0
    content_id = resolve_content_id(item)

    result = DownloadResultItem(
        media_id=media_id,
        content_id=content_id,
        status="failed",
        path=str(item.get_filepath()) if item.filename else None,
        url=item.url,
        bytes_written=0,
        http_status=status_code,
    )

    add_url_diagnostics(result, item.url, http_status=status_code)

    # Add primary HTTP error
    if status_code in (401, 403):
        # If diagnostics already identified IP_MISMATCH or URL_EXPIRED,
        # still record the HTTP error for completeness.
        if not result.has_error(DownloadFailureReason.IP_MISMATCH):
            result.add_error(
                reason=DownloadFailureReason.URL_EXPIRED_RUNTIME,
                message=f"HTTP {status_code}: CDN rejected request",
                details={"http_status": status_code},
            )
    elif status_code == 404:
        result.add_error(
            reason=DownloadFailureReason.CONTENT_DELETED_RUNTIME,
            message="HTTP 404: Content was deleted during download",
            details={"http_status": status_code},
        )
    else:
        result.add_error(
            reason=DownloadFailureReason.DOWNLOAD_ERROR,
            message=f"HTTP {status_code}: {error_message}",
            details={"http_status": status_code},
        )

    return result


def create_failed_download_result(
    item: DownloadItem,
    error_message: str,
) -> DownloadResultItem:
    """Create a DownloadResultItem for a failed download with URL diagnostics.

    Args:
        item: The download item that failed
        error_message: Base error message

    Returns:
        DownloadResultItem with diagnostic info and multiple errors if applicable
    """
    media_id = item.media_id or 0
    content_id = resolve_content_id(item)

    result = DownloadResultItem(
        media_id=media_id,
        content_id=content_id,
        status="failed",
        path=str(item.get_filepath()) if item.filename else None,
        url=item.url,
        bytes_written=0,
    )

    # Diagnose URL to understand why download failed
    diag_msg = add_url_diagnostics(result, item.url)
    url_preview = (
        (item.url[:100] + "...")
        if item.url and len(item.url) > 100
        else (item.url or "N/A")
    )

    # Add the base download error
    result.add_error(
        reason=DownloadFailureReason.DOWNLOAD_ERROR,
        message=error_message,
    )

    logger.warning(
        f"Download failed for media_id={media_id}, content_id={content_id}\n"
        f"  URL: {url_preview}\n"
        f"  Diagnosis: {diag_msg}\n"
        f"  Errors: {[e.reason.value for e in result.errors]}"
    )

    return result
