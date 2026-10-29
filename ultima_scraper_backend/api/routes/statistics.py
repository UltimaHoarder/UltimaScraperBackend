"""Daily site statistics endpoints."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from ultima_scraper_api import SUPPORTED_SITES

from ...models.backend import BackendInstance
from ...services.statistics_activity_service import (
    AccountDateField,
    ActivityInterval,
    ActivityMetric,
    ActivityRange,
    ActivityStatistics,
    StatisticsActivityService,
)
from ...services.statistics_seasonality_service import (
    SeasonalityCycle,
    SeasonalityStatistics,
    StatisticsSeasonalityService,
)
from ...services.statistics_service import (
    DailyStatisticsSnapshot,
    SiteStatisticsAvailability,
    StatisticsBuildStatus,
    StatisticsHistoryRange,
    StatisticsService,
)
from ..dependencies import get_backend_instance

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/statistics", tags=["statistics"])


def _supported_sites() -> list[str]:
    return [site.lower() for site in SUPPORTED_SITES]


class SiteStatisticsSnapshotResponse(BaseModel):
    """Serialized daily site statistics snapshot."""

    site: str
    bucket_date: str
    bucket_started_at: str
    bucket_ends_at: str
    calculated_at: str
    total_media_count: int
    sized_media_count: int
    zero_size_media_count: int
    image_media_count: int
    image_sized_count: int
    video_media_count: int
    video_sized_count: int
    audio_media_count: int
    audio_sized_count: int
    other_media_count: int
    other_sized_count: int
    total_known_bytes: int
    avg_media_size_bytes: int
    avg_image_size_bytes: int
    avg_video_size_bytes: int
    avg_audio_size_bytes: int
    avg_other_size_bytes: int
    size_coverage_ratio: float
    user_record_count: int | None = None
    performer_profile_count: int | None = None
    active_user_count: int | None = None
    archived_content_count: int | None = None
    post_count: int | None = None
    message_count: int | None = None
    story_count: int | None = None
    mass_message_count: int | None = None
    downloaded_media_count: int | None = None

    @classmethod
    def from_snapshot(
        cls,
        snapshot: DailyStatisticsSnapshot,
    ) -> "SiteStatisticsSnapshotResponse":
        return cls(**asdict(snapshot))


class SiteStatisticsResponse(BaseModel):
    """Availability state for a site's daily statistics snapshot."""

    site: str
    target_bucket_date: str
    snapshot_source: str
    is_current: bool
    is_building: bool
    has_snapshot: bool
    build_state: str | None = None
    build_progress_ratio: float | None = None
    build_processed_rows: int | None = None
    build_message: str | None = None
    build_error: str | None = None
    build_updated_at: str | None = None
    availability_error: str | None = None
    snapshot: SiteStatisticsSnapshotResponse | None = None

    @classmethod
    def from_availability(
        cls,
        availability: SiteStatisticsAvailability,
    ) -> "SiteStatisticsResponse":
        build_status: StatisticsBuildStatus | None = availability.build_status
        return cls(
            site=availability.site,
            target_bucket_date=availability.target_bucket_date,
            snapshot_source=availability.snapshot_source,
            is_current=availability.is_current,
            is_building=availability.is_building,
            has_snapshot=availability.has_snapshot,
            build_state=build_status.state if build_status is not None else None,
            build_progress_ratio=(
                build_status.progress_ratio if build_status is not None else None
            ),
            build_processed_rows=(
                build_status.processed_rows if build_status is not None else None
            ),
            build_message=build_status.message if build_status is not None else None,
            build_error=build_status.error if build_status is not None else None,
            build_updated_at=(
                build_status.updated_at if build_status is not None else None
            ),
            snapshot=(
                SiteStatisticsSnapshotResponse.from_snapshot(availability.snapshot)
                if availability.snapshot is not None
                else None
            ),
        )


class SiteStatisticsHistoryResponse(BaseModel):
    """Historical snapshot series for an enabled site."""

    site: str
    available: bool
    availability_error: str | None = None
    points: list[SiteStatisticsSnapshotResponse]


class StatisticsHistoryResponse(BaseModel):
    """Historical daily snapshots for the requested range."""

    range: StatisticsHistoryRange
    start_date: str | None
    end_date: str
    sites: list[SiteStatisticsHistoryResponse]


def _unavailable_statistics_response(site: str) -> SiteStatisticsResponse:
    target_bucket_date = StatisticsService.utc_day_window()[0].isoformat()
    return SiteStatisticsResponse(
        site=site,
        target_bucket_date=target_bucket_date,
        snapshot_source="none",
        is_current=False,
        is_building=False,
        has_snapshot=False,
        availability_error="Statistics could not be loaded for this site.",
        snapshot=None,
    )


@router.get("", response_model=list[SiteStatisticsResponse])
async def get_all_statistics(
    refresh: bool = Query(
        False,
        description="Queue a background recompute for today's snapshot before returning",
    ),
    backend: BackendInstance = Depends(get_backend_instance),
) -> list[SiteStatisticsResponse]:
    """Return the latest available daily statistics state for each enabled site."""

    async def _load_site(site: str) -> SiteStatisticsResponse:
        try:
            availability = await StatisticsService.get_statistics_availability(
                site,
                redis_manager=backend.redis,
                schedule_if_missing=True,
                schedule_refresh=refresh,
            )
        except Exception as exc:
            logger.warning("Failed to load statistics for %s: %s", site, exc)
            return _unavailable_statistics_response(site)
        return SiteStatisticsResponse.from_availability(availability)

    results = await asyncio.gather(
        *(_load_site(site) for site in _supported_sites()),
        return_exceptions=False,
    )
    return sorted(results, key=lambda item: item.site)


@router.get("/history", response_model=StatisticsHistoryResponse)
async def get_statistics_history(
    range_key: Literal["7d", "30d", "90d", "all"] = Query("all", alias="range"),
    site: str | None = Query(None),
) -> StatisticsHistoryResponse:
    """Return saved daily snapshots for the selected range and enabled sites."""
    supported_sites = _supported_sites()
    selected_site = site.strip().lower() if site else None
    if selected_site is not None and selected_site not in supported_sites:
        raise HTTPException(status_code=404, detail=f"Unsupported site '{site}'")

    history_now = datetime.now(timezone.utc)
    start_date, end_date = StatisticsService.history_date_window(range_key, history_now)
    site_names = [selected_site] if selected_site is not None else supported_sites

    async def _load_site_history(site_name: str) -> SiteStatisticsHistoryResponse:
        try:
            points = await StatisticsService.get_statistics_history(
                site_name,
                range_key=range_key,
                now=history_now,
            )
        except Exception as exc:
            logger.warning(
                "Failed to load statistics history for %s: %s", site_name, exc
            )
            return SiteStatisticsHistoryResponse(
                site=site_name,
                available=False,
                availability_error="Statistics history could not be loaded for this site.",
                points=[],
            )
        return SiteStatisticsHistoryResponse(
            site=site_name,
            available=True,
            availability_error=None,
            points=[
                SiteStatisticsSnapshotResponse.from_snapshot(point) for point in points
            ],
        )

    sites = await asyncio.gather(*(_load_site_history(name) for name in site_names))
    return StatisticsHistoryResponse(
        range=range_key,
        start_date=start_date.isoformat() if start_date is not None else None,
        end_date=end_date.isoformat(),
        sites=sorted(sites, key=lambda item: item.site),
    )


@router.get("/activity", response_model=ActivityStatistics)
async def get_statistics_activity(
    metric: ActivityMetric = Query("accounts"),
    interval: ActivityInterval = Query("day"),
    range_key: ActivityRange = Query("all", alias="range"),
    account_date: AccountDateField = Query("join_date"),
    site: str | None = Query(None),
    refresh: bool = Query(False),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    bucket_offset: Annotated[int | None, Query(ge=0)] = None,
) -> ActivityStatistics:
    """Count archived accounts by join_date and posts by publication date."""
    supported_sites = _supported_sites()
    selected_site = site.strip().lower() if site else None
    if selected_site is not None and selected_site not in supported_sites:
        raise HTTPException(status_code=404, detail=f"Unsupported site '{site}'")
    try:
        return await StatisticsActivityService.get_activity(
            [selected_site] if selected_site else supported_sites,
            metric,
            interval,
            range_key,
            account_date,
            refresh,
            start_date,
            end_date,
            bucket_offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/seasonality", response_model=SeasonalityStatistics)
async def get_statistics_seasonality(
    metric: ActivityMetric = Query("posts"),
    cycle: SeasonalityCycle = Query("weekday"),
    account_date: AccountDateField = Query("join_date"),
    site: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
) -> SeasonalityStatistics:
    selected = site.strip().lower() if site else None
    supported = _supported_sites()
    if selected is not None and selected not in supported:
        raise HTTPException(status_code=404, detail=f"Unsupported site '{site}'")
    try:
        return await StatisticsSeasonalityService.get_seasonality(
            [selected] if selected else supported,
            metric,
            cycle,
            account_date,
            start_date,
            end_date,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{site}", response_model=SiteStatisticsResponse)
async def get_site_statistics(
    site: str,
    refresh: bool = Query(
        False,
        description="Queue a background recompute for today's snapshot before returning",
    ),
    backend: BackendInstance = Depends(get_backend_instance),
) -> SiteStatisticsResponse:
    """Return the latest available daily statistics state for a single site."""

    site_key = site.strip().lower()
    if site_key not in _supported_sites():
        raise HTTPException(status_code=404, detail=f"Unsupported site '{site}'")

    try:
        availability = await StatisticsService.get_statistics_availability(
            site_key,
            redis_manager=backend.redis,
            schedule_if_missing=True,
            schedule_refresh=refresh,
        )
    except Exception as exc:
        logger.warning("Failed to load statistics for %s: %s", site_key, exc)
        return _unavailable_statistics_response(site_key)
    return SiteStatisticsResponse.from_availability(availability)
