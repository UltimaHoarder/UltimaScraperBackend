"""Mass Profile Scraper (MPS) routes."""

from __future__ import annotations

from ...job_system.mps_tasks import (
    MPS_MAX_PROXY_ROUTES_PER_PROFILE as MPS_MAX_PROXY_ROUTES_PER_PROFILE,
    MPS_MAX_ROUTE_HOPS,
    _build_mps_route_alternatives,
    _group_mps_proxy_countries,
    _resolve_mps_task_routes,
    get_cached_proxy_geo_map,
    mps_proxy_connection_limit,
)

import asyncio
from collections.abc import Mapping, Sequence
import json
import logging
import math
import time
from heapq import merge
from itertools import groupby
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, text
from sqlmodel import select

from ultima_scraper_db_archive import (
    InvalidUserModel,
    UserAuthModel,
    UserModel,
)

from ...boundary_discovery import (
    BoundaryDiscoveryConfig,
    MPSBoundaryDiscoveryResult,
    ProbeObservation,
    discover_user_id_boundaries,
)
from ...config import UltimaScraperCollectionConfig
from ...manager import BackendManager
from ...models.backend import BackendInstance
from ...usapi_initializer import resolve_proxy_url
from ..dependencies import (
    get_backend_instance,
    get_backend_manager,
    get_config,
)
from .jobs import (
    JobResponse,
    TaskPayload,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mps", tags=["mps"])


SUPPORTED_SITES = {
    "onlyfans": "OnlyFans",
    "fansly": "Fansly",
    "loyalfans": "LoyalFans",
}

VALID_PROXY_SCHEMES = frozenset(
    {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
)
LARGE_CANDIDATE_WARNING_THRESHOLD = 25_000
MPS_BALANCED_BATCH_SIZE = 5_000


class MPSProxyMode(str, Enum):
    NONE = "none"
    UNASSIGNED_ONLY = "unassigned_only"
    ALL = "all"
    CUSTOM = "custom"


class MissingRange(BaseModel):
    start_id: int
    end_id: int
    count: int


class MPSDensityBucket(BaseModel):
    start_id: int
    end_id: int
    missing_count: int = 0
    invalid_count: int = 0
    existing_count: int = 0
    candidate_count: int = 0
    is_exact: bool = True


class MPSDensePocket(BaseModel):
    start_id: int
    end_id: int
    candidate_count: int
    density_percentage: float = 0.0
    missing_count: int = 0
    invalid_count: int = 0
    existing_count: int = 0


class MPSBatchRecommendation(BaseModel):
    batch_size: int
    label: str
    reason: str


type MPSBatchRecommendationList = list[MPSBatchRecommendation]


class MPSAnalyzeRequest(BaseModel):
    site: str = "OnlyFans"
    start_id: int | None = None
    end_id: int | None = None
    ignore_missing_ids: bool = False
    include_invalid: bool = False
    invalid_last_checked_older_than_hours: float | None = Field(default=None, gt=0)
    include_existing_users: bool = False
    existing_last_checked_older_than_hours: float | None = Field(default=None, gt=0)
    candidate_offset: int = Field(default=0, ge=0)
    batch_size: int | None = Field(default=None, ge=1)
    missing_preview_limit: int = Field(default=250, ge=1, le=10000)
    range_preview_limit: int = Field(default=250, ge=1, le=5000)
    density_resolution: int = Field(default=512, ge=64, le=2048)
    bounded: bool = False
    routes: int = Field(default=1, ge=1)
    enable_adaptive_discovery: bool | None = None
    adaptive_discovery_max_probes: int | None = Field(default=None, ge=5, le=500)
    adaptive_discovery_ceiling: int | None = Field(default=None, ge=10)


class MPSAnalyzeResponse(BaseModel):
    site: str
    database_min_id: int | None
    database_max_id: int | None
    start_id: int
    end_id: int
    range_size: int
    existing_user_count: int
    invalid_user_count: int
    invalid_table_ready: bool
    invalid_eligible_count: int
    existing_eligible_count: int
    missing_count: int
    candidate_count: int
    missing_range_count: int
    missing_range_count_exact: bool = False
    missing_ranges_truncated: bool
    missing_ranges: list[MissingRange]
    missing_ids_preview: list[int]
    frontier_range: MissingRange | None = None
    candidate_ranges_preview: list[MissingRange] = Field(default_factory=list)
    candidate_ids_preview: list[int] = Field(default_factory=list)
    candidate_preview_truncated: bool = False
    candidate_range_count: int | None = None
    candidate_ranges_truncated: bool = False
    density_resolution: int = 512
    density_buckets: list[MPSDensityBucket] = Field(default_factory=list)
    density_is_exact: bool = True
    dense_pockets: list[MPSDensePocket] = Field(default_factory=list)
    candidate_offset: int
    planned_batch_size: int | None
    planned_candidate_count: int
    remaining_candidate_count: int
    next_candidate_offset: int | None
    has_next_batch: bool
    recommended_batch_size: int | None
    adaptive_batch_size: int | None = None
    batch_recommendations: MPSBatchRecommendationList
    warnings: list[str] = Field(default_factory=list)
    requires_confirmation: bool = False
    population_estimate: int | None = None
    population_confidence_interval: list[int] | None = None
    highest_id_confidence_interval: list[int] | None = None
    density_estimate: float | None = None
    adaptive_discovery_performed: bool = False


class MPSSiteBoundsResponse(BaseModel):
    site: str
    database_min_id: int | None = None
    database_max_id: int | None = None


class MPSBoundaryDiscoveryRequest(BaseModel):
    site: str = "OnlyFans"
    min_id: int | None = Field(default=None, ge=1)
    max_probes: int = Field(default=60, ge=5, le=500)
    max_id_ceiling: int = Field(default=100_000_000_000, ge=10)
    probe_multiplier: float = Field(default=10.0, ge=1.5, le=100.0)
    samples_per_step: int = Field(default=3, ge=1, le=20)
    refinement_steps: int = Field(default=8, ge=1, le=30)
    confidence_level: float = Field(default=0.95, ge=0.50, le=0.999)
    target_margin_of_error: float = Field(default=0.05, gt=0.0, le=0.50)


class MPSBoundaryDiscoveryResponse(BaseModel):
    site: str
    discovered_start_id: int
    discovered_end_id: int
    highest_observed_id: int | None
    lowest_observed_id: int | None
    estimated_total_users: int
    population_confidence_interval: list[int]
    estimated_density: float
    density_margin_of_error: float
    confidence_level: float
    highest_id_confidence_interval: list[int]
    total_probes_used: int
    valid_probes_count: int
    warnings: list[str] = Field(default_factory=list)
    observations: list[ProbeObservation] = Field(default_factory=list)


class MPSCreateJobRequest(BaseModel):
    site: str = "OnlyFans"
    start_id: int | None = None
    end_id: int | None = None
    ignore_missing_ids: bool = False
    include_invalid: bool = False
    invalid_last_checked_older_than_hours: float | None = Field(default=None, gt=0)
    include_existing_users: bool = False
    existing_last_checked_older_than_hours: float | None = Field(default=None, gt=0)
    candidate_offset: int = Field(default=0, ge=0)
    max_profiles: int | None = Field(default=None, ge=1)
    use_proxies: bool = False
    proxy_mode: MPSProxyMode = MPSProxyMode.UNASSIGNED_ONLY
    proxy_urls: list[str] | None = None
    include_direct_connection: bool = False
    max_route_hops: int = Field(default=3, ge=1, le=5)
    conclusive_miss_threshold: int = Field(default=3, ge=1, le=5)
    job_name: str | None = None
    created_by: str = "webui_mps"
    # Candidate total from the analysis that preceded this submission. A
    # finite missing-only batch stops scanning after its window, so the
    # response reports the remaining count from this hint (display only).
    candidate_count_hint: int | None = Field(default=None, ge=0)


class MPSProxyPreviewRequest(BaseModel):
    site: str = "OnlyFans"
    use_proxies: bool = True
    proxy_mode: MPSProxyMode = MPSProxyMode.UNASSIGNED_ONLY
    proxy_urls: list[str] | None = None
    include_direct_connection: bool = False


class MPSCreateJobResponse(BaseModel):
    job: JobResponse
    candidate_offset: int
    selected_profiles: int
    total_missing_in_range: int
    total_invalid_eligible_in_range: int
    total_existing_eligible_in_range: int
    total_candidates_in_range: int
    remaining_candidate_count: int
    next_candidate_offset: int | None
    has_next_batch: bool
    recommended_batch_size: int | None
    adaptive_batch_size: int | None = None
    next_batch_size: int | None
    batch_recommendations: MPSBatchRecommendationList
    warnings: list[str] = Field(default_factory=list)
    proxies_configured: int
    proxies_used: int
    direct_connection_included: bool
    proxy_mode: MPSProxyMode
    proxy_urls_preview: list[str] = Field(default_factory=list)


class MPSProxyPreviewResponse(BaseModel):
    site: str
    use_proxies: bool
    proxy_mode: MPSProxyMode
    proxies_configured: int
    proxies_used: int
    direct_connection_included: bool
    proxy_urls: list[str] = Field(default_factory=list)
    proxy_capacity: int = 0


@dataclass(slots=True)
class _AnalyzeComputation:
    site_display: str
    site_key: str
    database_min_id: int | None
    database_max_id: int | None
    start_id: int
    end_id: int
    existing_count: int
    eligible_existing_ids: list[int]
    invalid_count: int
    eligible_invalid_ids: list[int]
    invalid_table_ready: bool
    missing_ranges: list[tuple[int, int]]
    candidate_ranges: list[tuple[int, int]]
    missing_count: int
    include_missing_ids: bool
    candidate_count: int


@dataclass(slots=True)
class _AnalysisScope:
    site_display: str
    site_key: str
    db_manager: Any
    database_min_id: int | None
    database_max_id: int | None
    start_id: int
    end_id: int
    discovery_result: MPSBoundaryDiscoveryResult | None = None


@dataclass(slots=True)
class _BatchPlan:
    candidate_offset: int
    requested_batch_size: int | None
    planned_candidate_count: int
    remaining_candidate_count: int
    next_candidate_offset: int | None
    has_next_batch: bool
    recommended_batch_size: int | None
    batch_recommendations: MPSBatchRecommendationList
    warnings: list[str]
    requires_confirmation: bool
    adaptive_batch_size: int | None = None


def _normalize_site_name(site: str) -> tuple[str, str]:
    site_key = str(site).strip().lower()
    if site_key not in SUPPORTED_SITES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported site '{site}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_SITES.values()))}"
            ),
        )
    return SUPPORTED_SITES[site_key], site_key


def _build_missing_ranges(
    *,
    start_id: int,
    end_id: int,
    known_ids: list[int],
) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    cursor = start_id
    for known_id in known_ids:
        if known_id < cursor:
            continue
        if known_id > end_id:
            break
        if known_id > cursor:
            ranges.append((cursor, known_id - 1))
        cursor = known_id + 1
    if cursor <= end_id:
        ranges.append((cursor, end_id))
    return ranges


def _preview_missing_ids(
    ranges: list[tuple[int, int]],
    limit: int,
) -> list[int]:
    remaining = limit
    preview: list[int] = []
    for start_id, end_id in ranges:
        if remaining <= 0:
            break
        count = min(remaining, end_id - start_id + 1)
        preview.extend(range(start_id, start_id + count))
        remaining -= count
    return preview


def _merge_sorted_unique_ids(left: list[int], right: list[int]) -> list[int]:
    merged: list[int] = []
    left_index = 0
    right_index = 0

    while left_index < len(left) and right_index < len(right):
        left_value = left[left_index]
        right_value = right[right_index]
        if left_value == right_value:
            merged.append(left_value)
            left_index += 1
            right_index += 1
        elif left_value < right_value:
            merged.append(left_value)
            left_index += 1
        else:
            merged.append(right_value)
            right_index += 1

    if left_index < len(left):
        merged.extend(left[left_index:])
    if right_index < len(right):
        merged.extend(right[right_index:])

    return merged


def _ids_to_ranges(ids: list[int]) -> list[tuple[int, int]]:
    if not ids:
        return []

    ranges: list[tuple[int, int]] = []
    range_start = ids[0]
    range_end = ids[0]
    for user_id in ids[1:]:
        if user_id <= range_end:
            continue
        if user_id == range_end + 1:
            range_end = user_id
            continue
        ranges.append((range_start, range_end))
        range_start = user_id
        range_end = user_id
    ranges.append((range_start, range_end))
    return ranges


def _select_candidate_ids(
    *,
    candidate_ranges: list[tuple[int, int]],
    eligible_existing_ids: list[int],
    offset: int,
    limit: int | None,
) -> list[int]:
    if limit is not None and limit <= 0:
        return []

    remaining_to_skip = offset
    remaining_to_take = limit
    selected: list[int] = []

    for start_id, end_id in candidate_ranges:
        range_count = end_id - start_id + 1
        if remaining_to_skip >= range_count:
            remaining_to_skip -= range_count
            continue

        take_start = start_id + remaining_to_skip
        available = end_id - take_start + 1
        take_count = (
            available
            if remaining_to_take is None
            else min(available, remaining_to_take)
        )
        selected.extend(range(take_start, take_start + take_count))
        remaining_to_skip = 0
        if remaining_to_take is not None:
            remaining_to_take -= take_count
            if remaining_to_take <= 0:
                return selected

    if remaining_to_skip >= len(eligible_existing_ids):
        return selected

    existing_start = remaining_to_skip
    if remaining_to_take is None:
        selected.extend(eligible_existing_ids[existing_start:])
    else:
        selected.extend(
            eligible_existing_ids[existing_start : existing_start + remaining_to_take]
        )
    return selected


def _build_batch_recommendations(candidate_count: int) -> list[MPSBatchRecommendation]:
    if candidate_count <= 0:
        return []

    recommendations = [
        MPSBatchRecommendation(
            batch_size=min(1_000, candidate_count),
            label="Conservative",
            reason="Keeps Redis preparation and worker routing short and easy to retry.",
        ),
        MPSBatchRecommendation(
            batch_size=min(MPS_BALANCED_BATCH_SIZE, candidate_count),
            label="Balanced",
            reason="A bounded batch balances throughput with predictable startup time.",
        ),
    ]
    unique: list[MPSBatchRecommendation] = []
    seen_sizes: set[int] = set()
    for recommendation in recommendations:
        if recommendation.batch_size in seen_sizes:
            continue
        seen_sizes.add(recommendation.batch_size)
        unique.append(recommendation)
    return unique


def _build_planning_warnings(
    *,
    candidate_offset: int,
    invalid_table_ready: bool,
    include_invalid: bool,
    invalid_last_checked_older_than_hours: float | None,
    include_existing_users: bool,
    existing_last_checked_older_than_hours: float | None,
    **_extra: object,
) -> list[str]:
    warnings: list[str] = []
    if not invalid_table_ready:
        warnings.append(
            "The invalid_users table is unavailable, so previously not-found profiles cannot be filtered reliably."
        )
    if include_invalid and invalid_last_checked_older_than_hours is None:
        warnings.append(
            "Previously not-found profile refresh is unbounded by age, so every stored invalid record in range is eligible."
        )
    if include_existing_users and existing_last_checked_older_than_hours is None:
        warnings.append(
            "Existing-user refresh is unbounded by age, so every stored user in range is eligible for this batch."
        )
    if candidate_offset > 0:
        warnings.append(
            f"This batch starts after skipping the first {candidate_offset:,} candidate(s) from the filtered result set."
        )
    return warnings


def calculate_adaptive_batch_size(
    *,
    total_ids: int,
    missing_count: int,
    candidate_count: int,
    include_missing: bool = True,
    base_batch_size: int = 5_000,
    max_recommendation: int = 1_000_000,
    routes: int = 1,
) -> int:
    """Calculate an adaptive batch size recommendation scaling inversely with missing-ID density
    and dynamically with available routes/proxies.

    Formula:
        missing_density = missing_count / total_ids
        route_multiplier = max(1, routes)
        effective_base = base_batch_size * route_multiplier
        effective_max = max_recommendation * route_multiplier
        adaptive_batch = ceil(effective_base / missing_density)
    """
    if total_ids <= 0 or candidate_count <= 0:
        return 0

    density = 0.0
    if include_missing and missing_count > 0:
        density = missing_count / total_ids
    elif candidate_count > 0:
        density = candidate_count / total_ids

    if density <= 0.0:
        return 0

    clamped_density = min(1.0, max(0.0, density))
    route_multiplier = max(1, routes)
    effective_base = base_batch_size * route_multiplier
    effective_max = max_recommendation * route_multiplier
    raw_batch = math.ceil(effective_base / clamped_density)
    return max(effective_base, min(effective_max, raw_batch))


def _build_batch_plan(
    *,
    candidate_count: int,
    candidate_offset: int,
    requested_batch_size: int | None,
    range_size: int,
    invalid_table_ready: bool,
    include_invalid: bool,
    invalid_last_checked_older_than_hours: float | None,
    include_existing_users: bool,
    existing_last_checked_older_than_hours: float | None,
    missing_count: int = 0,
    include_missing: bool = True,
    routes: int = 1,
) -> _BatchPlan:
    normalized_offset = min(candidate_offset, candidate_count)
    remaining_candidate_count = max(candidate_count - normalized_offset, 0)
    effective_batch_size = (
        remaining_candidate_count
        if requested_batch_size is None
        else requested_batch_size
    )
    planned_candidate_count = min(effective_batch_size, remaining_candidate_count)
    next_candidate_offset = normalized_offset + planned_candidate_count
    has_next_batch = next_candidate_offset < candidate_count
    recommendations = _build_batch_recommendations(candidate_count)
    recommended_batch_size = (
        recommendations[1].batch_size
        if len(recommendations) >= 2
        else (recommendations[0].batch_size if recommendations else None)
    )
    adaptive_batch_size = calculate_adaptive_batch_size(
        total_ids=range_size,
        missing_count=missing_count,
        candidate_count=candidate_count,
        include_missing=include_missing,
        routes=routes,
    )
    warnings = _build_planning_warnings(
        range_size=range_size,
        candidate_count=candidate_count,
        candidate_offset=normalized_offset,
        planned_candidate_count=planned_candidate_count,
        requested_batch_size=requested_batch_size,
        invalid_table_ready=invalid_table_ready,
        include_invalid=include_invalid,
        invalid_last_checked_older_than_hours=invalid_last_checked_older_than_hours,
        include_existing_users=include_existing_users,
        existing_last_checked_older_than_hours=existing_last_checked_older_than_hours,
    )
    requires_confirmation = bool(
        planned_candidate_count >= 5_000
        or candidate_count >= LARGE_CANDIDATE_WARNING_THRESHOLD
    )
    return _BatchPlan(
        candidate_offset=normalized_offset,
        requested_batch_size=requested_batch_size,
        planned_candidate_count=planned_candidate_count,
        remaining_candidate_count=remaining_candidate_count,
        next_candidate_offset=next_candidate_offset if has_next_batch else None,
        has_next_batch=has_next_batch,
        recommended_batch_size=recommended_batch_size,
        batch_recommendations=recommendations,
        warnings=warnings,
        requires_confirmation=requires_confirmation,
        adaptive_batch_size=adaptive_batch_size,
    )


def _validate_proxy_urls(proxy_urls: list[str] | None) -> list[str]:
    normalized_urls: list[str] = []
    for raw_url in proxy_urls or []:
        proxy_url = str(raw_url).strip()
        if not proxy_url:
            continue
        parsed = urlparse(proxy_url)
        if parsed.scheme.lower() not in VALID_PROXY_SCHEMES or not parsed.netloc:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid proxy URL '{proxy_url}'. Supported schemes: "
                    f"{', '.join(sorted(VALID_PROXY_SCHEMES))}."
                ),
            )
        normalized_urls.append(proxy_url)
    return list(dict.fromkeys(normalized_urls))


def _build_mps_task_payloads(
    *,
    site_display: str,
    target_user_ids: list[int],
    proxy_pool: list[str],
    proxy_start_index: int = 0,
    include_direct: bool = False,
    proxy_geo_map: Mapping[str, str] | None = None,
    max_route_hops: int = MPS_MAX_ROUTE_HOPS,
    conclusive_miss_threshold: int = MPS_MAX_ROUTE_HOPS,
) -> list[TaskPayload]:
    tasks: list[TaskPayload] = []
    if proxy_geo_map is None:
        proxy_geo_map = get_cached_proxy_geo_map()
    country_groups = _group_mps_proxy_countries(proxy_pool, proxy_geo_map)
    for index, target_user_id in enumerate(target_user_ids):
        guest_proxy_url, fallback_urls, guest_direct_route_index = (
            _resolve_mps_task_routes(
                task_index=index,
                proxy_pool=proxy_pool,
                proxy_start_index=proxy_start_index,
                include_direct=include_direct,
                proxy_geo_map=proxy_geo_map,
                max_route_hops=max_route_hops,
                proxy_country_groups=country_groups,
            )
        )
        params: dict[str, Any] = {
            "site": site_display,
            "user_id": target_user_id,
            "username": str(target_user_id),
            "content_types": ["Profile"],
            "download_media": False,
            "max_items": 1,
            "use_guest_auth": True,
            "guest_direct_route_index": guest_direct_route_index,
            "track_invalid_user": True,
            "conclusive_miss_threshold": conclusive_miss_threshold,
            "max_route_hops": max_route_hops,
        }
        if guest_proxy_url:
            params["guest_proxy_url"] = guest_proxy_url
        if fallback_urls:
            params["guest_fallback_proxy_urls"] = fallback_urls
        alternatives = _build_mps_route_alternatives(
            selected_proxy_urls=(
                ([guest_proxy_url] if guest_proxy_url else []) + fallback_urls
            ),
            route_offset=proxy_start_index + index,
            proxy_geo_map=proxy_geo_map,
            proxy_country_groups=country_groups,
        )
        if alternatives:
            params["guest_proxy_alternatives"] = alternatives
        tasks.append(TaskPayload(task_type="scrape_user", params=params))
    return tasks


async def _query_min_max(
    db_manager: Any,
    *,
    site_key: str,
    include_user_bounds: bool,
    include_invalid_bounds: bool,
) -> tuple[int | None, int | None]:
    def _run() -> tuple[int | None, int | None]:
        minimums: list[int] = []
        maximums: list[int] = []
        with db_manager.get_session() as session:
            if include_user_bounds:
                user_min_id = session.exec(select(func.min(UserModel.id))).first()
                user_max_id = session.exec(select(func.max(UserModel.id))).first()
                if user_min_id is not None:
                    minimums.append(int(user_min_id))
                if user_max_id is not None:
                    maximums.append(int(user_max_id))

            if include_invalid_bounds:
                try:
                    invalid_filters = (InvalidUserModel.site == site_key,)
                    invalid_min_id = session.exec(
                        select(func.min(InvalidUserModel.user_id)).where(
                            *invalid_filters
                        )
                    ).first()
                    invalid_max_id = session.exec(
                        select(func.max(InvalidUserModel.user_id)).where(
                            *invalid_filters
                        )
                    ).first()
                    if invalid_min_id is not None:
                        minimums.append(int(invalid_min_id))
                    if invalid_max_id is not None:
                        maximums.append(int(invalid_max_id))
                except Exception as exc:
                    logger.warning(
                        "invalid_users bounds query failed for site %s (likely pending migration): %s",
                        site_key,
                        exc,
                    )

            return (
                min(minimums) if minimums else None,
                max(maximums) if maximums else None,
            )

    return await asyncio.to_thread(_run)


async def _resolve_analysis_scope(
    request: MPSAnalyzeRequest | MPSCreateJobRequest,
    backend_manager: BackendManager,
) -> _AnalysisScope:
    site_display, site_key = _normalize_site_name(request.site)
    db_manager = backend_manager.get_site_db_manager(site_display)

    include_user_bounds = (
        not request.ignore_missing_ids or request.include_existing_users
    )
    db_min_id: int | None = None
    db_max_id: int | None = None
    # Database extents are analysis output. A submission with explicit bounds
    # (the web UI always sends the analyzed range) does not need them.
    if not (
        isinstance(request, MPSCreateJobRequest)
        and request.start_id is not None
        and request.end_id is not None
    ):
        db_min_id, db_max_id = await _query_min_max(
            db_manager,
            site_key=site_key,
            include_user_bounds=include_user_bounds,
            include_invalid_bounds=request.include_invalid,
        )
    discovery_result: MPSBoundaryDiscoveryResult | None = None
    if request.start_id is None and request.end_id is None:
        if db_min_id is None or db_max_id is None:
            if getattr(request, "enable_adaptive_discovery", None) is False:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No records found for the selected profile sources. "
                        "Provide explicit start_id and end_id, or discover boundaries."
                    ),
                )
            max_probes = (
                getattr(request, "adaptive_discovery_max_probes", None)
                or 60
            )
            ceiling = (
                getattr(request, "adaptive_discovery_ceiling", None)
                or 1_000_000_000
            )
            cfg = BoundaryDiscoveryConfig(
                max_probes=max_probes,
                max_id_ceiling=ceiling,
            )
            discovery_result = await discover_user_id_boundaries(
                site=site_display,
                backend_manager=backend_manager,
                config=cfg,
            )
            if discovery_result.highest_observed_id is None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Adaptive boundary discovery found no valid user IDs for site '{site_display}' "
                        f"up to ceiling {cfg.max_id_ceiling}."
                    ),
                )
            db_min_id = discovery_result.discovered_start_id
            db_max_id = discovery_result.discovered_end_id

    start_id = request.start_id if request.start_id is not None else db_min_id
    end_id = request.end_id if request.end_id is not None else db_max_id
    if start_id is None or end_id is None:
        raise HTTPException(
            status_code=400,
            detail="Both start_id and end_id are required when database is empty.",
        )
    if start_id <= 0 or end_id <= 0:
        raise HTTPException(
            status_code=400,
            detail="start_id and end_id must be positive integers.",
        )
    if start_id > end_id:
        raise HTTPException(
            status_code=400,
            detail="start_id cannot be greater than end_id.",
        )

    return _AnalysisScope(
        site_display=site_display,
        site_key=site_key,
        db_manager=db_manager,
        database_min_id=db_min_id,
        database_max_id=db_max_id,
        start_id=start_id,
        end_id=end_id,
        discovery_result=discovery_result,
    )


async def _query_ids_in_range(
    db_manager: Any,
    *,
    site_key: str,
    start_id: int,
    end_id: int,
    include_invalid: bool,
    invalid_last_checked_older_than_hours: float | None,
) -> tuple[list[int], list[int], list[int], bool]:
    def _run() -> tuple[list[int], list[int], list[int], bool]:
        with db_manager.get_session() as session:
            existing_rows = session.exec(
                select(UserModel.id)
                .where(
                    UserModel.id >= start_id,
                    UserModel.id <= end_id,
                )
                .order_by(UserModel.id)
            ).all()
            existing_ids = [int(value) for value in existing_rows if value is not None]

            invalid_ids: list[int] = []
            eligible_invalid_ids: list[int] = []
            invalid_table_ready = True
            try:
                invalid_rows = session.exec(
                    select(
                        InvalidUserModel.user_id,
                        InvalidUserModel.last_checked_at,
                        InvalidUserModel.updated_at,
                    )
                    .where(
                        InvalidUserModel.site == site_key,
                        InvalidUserModel.user_id >= start_id,
                        InvalidUserModel.user_id <= end_id,
                    )
                    .order_by(InvalidUserModel.user_id)
                ).all()
                existing_id_set = set(existing_ids)
                cutoff = (
                    datetime.now(timezone.utc)
                    - timedelta(hours=invalid_last_checked_older_than_hours)
                    if invalid_last_checked_older_than_hours is not None
                    else None
                )
                for raw_user_id, last_checked_at, updated_at in invalid_rows:
                    if raw_user_id is None:
                        continue
                    invalid_user_id = int(raw_user_id)
                    invalid_ids.append(invalid_user_id)
                    if not include_invalid or invalid_user_id in existing_id_set:
                        continue
                    checked_at = updated_at or last_checked_at
                    if checked_at is not None and checked_at.tzinfo is None:
                        checked_at = checked_at.replace(tzinfo=timezone.utc)
                    if cutoff is None or checked_at is None or checked_at <= cutoff:
                        eligible_invalid_ids.append(invalid_user_id)
            except Exception as exc:
                invalid_table_ready = False
                logger.warning(
                    "invalid_users query failed for site %s (likely pending migration): %s",
                    site_key,
                    exc,
                )
                invalid_ids = []
                eligible_invalid_ids = []

            return (
                existing_ids,
                invalid_ids,
                eligible_invalid_ids,
                invalid_table_ready,
            )

    return await asyncio.to_thread(_run)


# Optional partition span for statement ID spans; defaults to None (unified full-range counts).
MPS_ANALYSIS_COUNT_ID_SPAN: int | None = None
MPS_ANALYSIS_SCAN_BLOCK_SIZE = 1_000_000
_INVALID_COUNT_CACHE: dict[tuple[str, int, int], tuple[float, int]] = {}
_USER_COUNT_CACHE: dict[tuple[str, int, int], tuple[float, int]] = {}
_INVALID_ANALYSIS_FAILED_DETAIL = (
    "Invalid-profile analysis query failed. Analysis stopped because previously "
    "not-found profiles could not be filtered reliably. Check backend database "
    "errors and retry."
)


def _invalid_table_unavailable(exc: BaseException) -> bool:
    """True when ``invalid_users`` is missing or not yet migrated."""
    code = getattr(getattr(exc, "orig", None), "sqlstate", None)
    return code in {"42P01", "42703"}


def _count_coverage_blocks(
    session: Any,
    *,
    site_key: str,
    base_id: int,
    end_id: int,
    span: int,
    include_invalid_table: bool,
) -> tuple[dict[int, tuple[int, int]], int, int, bool]:
    """Count users and invalid-only IDs for every coverage block in one pass each.

    Blocks partition ``[base_id, end_id]`` exactly, so the per-block counts sum
    to the authoritative user and invalid inventories of the caller's snapshot.
    Returns ``(blocks, user_total, invalid_row_total, invalid_table_ready)``
    where ``blocks`` maps block index to ``(user_count, invalid_only_count)``.
    """
    params = {"base": base_id, "span": span, "lower": base_id, "upper": end_id, "site": site_key}
    block_of = "({column} - CAST(:base AS BIGINT)) / CAST(:span AS BIGINT)"
    blocks: dict[int, list[int]] = {}
    user_total = 0
    for block, count in session.execute(text(
        f"SELECT {block_of.format(column='id')} AS block, count(*) FROM users "
        "WHERE id BETWEEN :lower AND :upper GROUP BY 1"
    ), params):
        blocks[int(block)] = [int(count), 0]
        user_total += int(count)

    invalid_total = 0
    invalid_table_ready = include_invalid_table
    invalid_rows: list[Any] = []
    if include_invalid_table:
        try:
            # A savepoint keeps the snapshot usable when the table is missing.
            with session.begin_nested():
                bind = session.get_bind() if hasattr(session, "get_bind") else None
                dialect_name = getattr(getattr(bind, "dialect", None), "name", None)
                if dialect_name == "postgresql" and (end_id - base_id > 10_000_000):
                    invalid_rows = session.execute(text(
                        f"SELECT {block_of.format(column='user_id')} AS block, count(user_id), "
                        "count(user_id) "
                        "FROM invalid_users "
                        "WHERE site = :site AND user_id BETWEEN :lower AND :upper GROUP BY 1"
                    ), params).all()
                else:
                    invalid_rows = session.execute(text(
                        f"SELECT {block_of.format(column='i.user_id')} AS block, count(i.user_id), "
                        "count(DISTINCT CASE WHEN u.id IS NULL THEN i.user_id END) "
                        "FROM invalid_users i LEFT JOIN users u ON u.id = i.user_id "
                        "WHERE i.site = :site AND i.user_id BETWEEN :lower AND :upper GROUP BY 1"
                    ), params).all()
        except Exception as exc:
            if not _invalid_table_unavailable(exc):
                logger.exception("MPS invalid-profile coverage count failed for site %s", site_key)
                raise HTTPException(status_code=503, detail=_INVALID_ANALYSIS_FAILED_DETAIL) from exc
            logger.warning(
                "invalid_users query failed for site %s (likely pending migration): %s",
                site_key,
                exc,
            )
            invalid_table_ready = False
        for block, row_count, invalid_only in invalid_rows:
            blocks.setdefault(int(block), [0, 0])[1] = int(invalid_only or 0)
            invalid_total += int(row_count or 0)

    return (
        {block: (users, invalid_only) for block, (users, invalid_only) in blocks.items()},
        user_total,
        invalid_total,
        invalid_table_ready,
    )


def _query_known_islands(
    session: Any,
    *,
    site_key: str,
    lower: int,
    upper: int,
    include_invalid_table: bool,
) -> list[tuple[int, int]]:
    """Return the maximal runs of known IDs in ``[lower, upper]`` in ID order.

    The database marks only run boundaries, so a block transfers at most two
    rows per run instead of every known ID. An ID present in both sources is
    one identity: each boundary flag lands on exactly one of its rows, and
    run-start rows sort first, so duplicates never split a run.
    """
    known_sql = "SELECT id AS user_id FROM users WHERE id BETWEEN :lower AND :upper"
    if include_invalid_table:
        known_sql += (
            " UNION ALL SELECT user_id FROM invalid_users"
            " WHERE site = :site AND user_id BETWEEN :lower AND :upper"
        )
    result = session.execute(text(
        "SELECT user_id, starts_run, ends_run FROM ("
        " SELECT user_id,"
        " CASE WHEN prev_id IS NULL OR user_id > prev_id + 1 THEN 1 ELSE 0 END AS starts_run,"
        " CASE WHEN next_id IS NULL OR next_id > user_id + 1 THEN 1 ELSE 0 END AS ends_run"
        " FROM (SELECT user_id, lag(user_id) OVER w AS prev_id, lead(user_id) OVER w AS next_id"
        f" FROM ({known_sql}) known WINDOW w AS (ORDER BY user_id)) neighbours"
        ") bounds WHERE starts_run = 1 OR ends_run = 1"
        " ORDER BY user_id, starts_run DESC"
    ).execution_options(yield_per=16384), {"lower": lower, "upper": upper, "site": site_key})
    islands: list[tuple[int, int]] = []
    run_start = lower
    try:
        for user_id, starts_run, ends_run in result.tuples():
            if starts_run:
                run_start = int(user_id)
            if ends_run:
                islands.append((run_start, int(user_id)))
    finally:
        result.close()
    return islands


async def _query_existing_count(
    db_manager: Any,
    *,
    start_id: int,
    end_id: int,
    bounded: bool = False,
    db_min_id: int | None = None,
    db_max_id: int | None = None,
) -> int:
    def _run() -> int:
        if not hasattr(db_manager, "get_session"):
            return 0
        with db_manager.get_session() as session:
            is_postgres = False
            if session.get_bind().dialect.name == "postgresql":
                is_postgres = True
                session.connection().exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                )
            cache_key = ("users", start_id, end_id)
            if bounded:
                cached = _USER_COUNT_CACHE.get(cache_key)
                if cached is not None and time.monotonic() - cached[0] < 300.0:
                    return cached[1]

            if is_postgres and bounded:
                min_val = db_min_id
                max_val = db_max_id
                if min_val is None or max_val is None:
                    min_val = session.exec(select(func.min(UserModel.id))).first()
                    max_val = session.exec(select(func.max(UserModel.id))).first()
                if (
                    min_val is not None
                    and max_val is not None
                    and start_id <= int(min_val)
                    and end_id >= int(max_val)
                ):
                    stat_val = session.execute(
                        text("SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = current_schema() AND relname = 'users'")
                    ).scalar()
                    if stat_val is not None and int(stat_val) > 0:
                        total = int(stat_val)
                        _USER_COUNT_CACHE[cache_key] = (time.monotonic(), total)
                        return total
                    rel_val = session.execute(
                        text("SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname = 'users'")
                    ).scalar()
                    if rel_val is not None and int(rel_val) > 0:
                        total = int(rel_val)
                        _USER_COUNT_CACHE[cache_key] = (time.monotonic(), total)
                        return total

                session.connection().exec_driver_sql("SET LOCAL enable_seqscan = off")

            if MPS_ANALYSIS_COUNT_ID_SPAN is not None and MPS_ANALYSIS_COUNT_ID_SPAN > 0:
                total = 0
                for lower in range(start_id, end_id + 1, MPS_ANALYSIS_COUNT_ID_SPAN):
                    upper = min(end_id, lower + MPS_ANALYSIS_COUNT_ID_SPAN - 1)
                    value = session.exec(
                        select(func.count(UserModel.id)).where(
                            UserModel.id >= lower,
                            UserModel.id <= upper,
                        )
                    ).first()
                    total += int(value or 0)
                _USER_COUNT_CACHE[cache_key] = (time.monotonic(), total)
                return total

            value = session.exec(
                select(func.count(UserModel.id)).where(
                    UserModel.id >= start_id,
                    UserModel.id <= end_id,
                )
            ).first()
            total = int(value or 0)
            _USER_COUNT_CACHE[cache_key] = (time.monotonic(), total)
            return total

    return await asyncio.to_thread(_run)


async def _query_invalid_counts(
    db_manager: Any,
    *,
    site_key: str,
    start_id: int,
    end_id: int,
    include_invalid: bool,
    last_checked_older_than_hours: float | None,
    bounded: bool = False,
    include_total: bool = True,
    db_min_id: int | None = None,
    db_max_id: int | None = None,
) -> tuple[int, int, bool]:
    """Count invalid inventory and eligible invalid candidates.

    ``include_total=False`` skips the inventory count for callers that already
    derive it from the full coverage pass; the returned total is then 0.
    """
    def _run() -> tuple[int, int, bool]:
        if not hasattr(db_manager, "get_session"):
            return 0, 0, False
        with db_manager.get_session() as session:
            try:
                is_postgres = False
                if session.get_bind().dialect.name == "postgresql":
                    is_postgres = True
                    session.connection().exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cache_key = (site_key, start_id, end_id)
                cached = _INVALID_COUNT_CACHE.get(cache_key)
                if include_total and bounded and cached is not None and time.monotonic() - cached[0] < 300.0:
                    total_count = cached[1]
                elif is_postgres and bounded and include_total:
                    min_val = db_min_id
                    max_val = db_max_id
                    if min_val is None or max_val is None:
                        min_val = session.exec(
                            select(func.min(InvalidUserModel.user_id)).where(InvalidUserModel.site == site_key)
                        ).first()
                        max_val = session.exec(
                            select(func.max(InvalidUserModel.user_id)).where(InvalidUserModel.site == site_key)
                        ).first()
                    if (
                        min_val is not None
                        and max_val is not None
                        and start_id <= int(min_val)
                        and end_id >= int(max_val)
                    ):
                        stat_val = session.execute(
                            text("SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = current_schema() AND relname = 'invalid_users'")
                        ).scalar()
                        if stat_val is not None and int(stat_val) >= 0:
                            total_count = int(stat_val)
                            _INVALID_COUNT_CACHE[cache_key] = (time.monotonic(), total_count)
                        else:
                            rel_val = session.execute(
                                text("SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname = 'invalid_users'")
                            ).scalar()
                            total_count = int(rel_val or 0)
                            _INVALID_COUNT_CACHE[cache_key] = (time.monotonic(), total_count)
                    else:
                        session.connection().exec_driver_sql("SET LOCAL enable_seqscan = off")
                        val = session.exec(
                            select(func.count(InvalidUserModel.user_id)).where(
                                InvalidUserModel.site == site_key,
                                InvalidUserModel.user_id >= start_id,
                                InvalidUserModel.user_id <= end_id,
                            )
                        ).first()
                        total_count = int(val or 0)
                        _INVALID_COUNT_CACHE[cache_key] = (time.monotonic(), total_count)
                elif MPS_ANALYSIS_COUNT_ID_SPAN is not None and MPS_ANALYSIS_COUNT_ID_SPAN > 0:
                    total_count = eligible_count = 0
                    cutoff = (datetime.now(timezone.utc) - timedelta(hours=last_checked_older_than_hours)) if last_checked_older_than_hours is not None else None
                    for lower in range(start_id, end_id + 1, MPS_ANALYSIS_COUNT_ID_SPAN):
                        upper = min(end_id, lower + MPS_ANALYSIS_COUNT_ID_SPAN - 1)
                        filters = (
                            InvalidUserModel.site == site_key,
                            InvalidUserModel.user_id >= lower,
                            InvalidUserModel.user_id <= upper,
                        )
                        if include_total:
                            total_value = session.exec(
                                select(func.count(InvalidUserModel.user_id)).where(
                                    *filters,
                                )
                            ).first()
                            total_count += int(total_value or 0)
                        if include_invalid:
                            eligible_query = select(func.count(InvalidUserModel.user_id)).where(
                                *filters,
                                ~select(UserModel.id)
                                .where(UserModel.id == InvalidUserModel.user_id)
                                .exists(),
                            )
                            if last_checked_older_than_hours is not None:
                                effective_last_checked = func.coalesce(
                                    InvalidUserModel.updated_at,
                                    InvalidUserModel.last_checked_at,
                                )
                                eligible_query = eligible_query.where(
                                    or_(
                                        effective_last_checked.is_(None),
                                        effective_last_checked <= cutoff,
                                    )
                                )
                            eligible_value = session.exec(eligible_query).first()
                            eligible_count += int(eligible_value or 0)
                    if include_total:
                        _INVALID_COUNT_CACHE[cache_key] = (time.monotonic(), total_count)
                    return total_count, eligible_count, True
                elif not include_total:
                    total_count = 0
                else:
                    val = session.exec(
                        select(func.count(InvalidUserModel.user_id)).where(
                            InvalidUserModel.site == site_key,
                            InvalidUserModel.user_id >= start_id,
                            InvalidUserModel.user_id <= end_id,
                        )
                    ).first()
                    total_count = int(val or 0)
                    _INVALID_COUNT_CACHE[cache_key] = (time.monotonic(), total_count)

                if not include_invalid:
                    return total_count, 0, True

                if is_postgres and bounded and last_checked_older_than_hours is None and total_count > 0:
                    return total_count, total_count, True

                eligible_filters = [
                    InvalidUserModel.site == site_key,
                    InvalidUserModel.user_id >= start_id,
                    InvalidUserModel.user_id <= end_id,
                ]
                if last_checked_older_than_hours is not None:
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=last_checked_older_than_hours)
                    effective_last_checked = func.coalesce(
                        InvalidUserModel.updated_at,
                        InvalidUserModel.last_checked_at,
                    )
                    eligible_filters.append(
                        or_(
                            effective_last_checked.is_(None),
                            effective_last_checked <= cutoff,
                        )
                    )

                if is_postgres and (bounded or (end_id - start_id > 10_000_000)):
                    eligible_query = select(func.count(InvalidUserModel.user_id)).where(*eligible_filters)
                else:
                    eligible_query = select(func.count(InvalidUserModel.user_id)).where(
                        *eligible_filters,
                        ~select(UserModel.id)
                        .where(UserModel.id == InvalidUserModel.user_id)
                        .exists(),
                    )
                eligible_value = session.exec(eligible_query).first()
                return total_count, int(eligible_value or 0), True
            except Exception as exc:
                if not _invalid_table_unavailable(exc):
                    logger.exception("MPS invalid-profile count failed for site %s", site_key)
                    raise HTTPException(
                        status_code=503,
                        detail=_INVALID_ANALYSIS_FAILED_DETAIL,
                    ) from exc
                logger.warning(
                    "invalid_users query failed for site %s (likely pending migration): %s",
                    site_key,
                    exc,
                )
                return 0, 0, False

    return await asyncio.to_thread(_run)


async def _query_invalid_candidate_ids_preview(
    db_manager: Any,
    *,
    site_key: str,
    start_id: int,
    end_id: int,
    last_checked_older_than_hours: float | None,
    limit: int,
) -> list[int]:
    """Return a bounded preview of eligible ``invalid_users`` candidates."""

    def _run() -> list[int]:
        with db_manager.get_session() as session:
            try:
                bind = session.get_bind() if hasattr(session, "get_bind") else None
                dialect_name = getattr(getattr(bind, "dialect", None), "name", None)
                if dialect_name == "postgresql" and hasattr(session, "connection"):
                    session.connection().exec_driver_sql(
                        "SET LOCAL enable_hashjoin = off; SET LOCAL enable_seqscan = off"
                    )
                query = (
                    select(InvalidUserModel.user_id)
                    .where(
                        InvalidUserModel.site == site_key,
                        InvalidUserModel.user_id >= start_id,
                        InvalidUserModel.user_id <= end_id,
                        ~select(UserModel.id)
                        .where(UserModel.id == InvalidUserModel.user_id)
                        .exists(),
                    )
                    .order_by(InvalidUserModel.user_id)
                    .limit(limit)
                )
                if last_checked_older_than_hours is not None:
                    cutoff = datetime.now(timezone.utc) - timedelta(
                        hours=last_checked_older_than_hours
                    )
                    effective_last_checked = func.coalesce(
                        InvalidUserModel.updated_at,
                        InvalidUserModel.last_checked_at,
                    )
                    query = query.where(
                        or_(
                            effective_last_checked.is_(None),
                            effective_last_checked <= cutoff,
                        )
                    )

                rows = session.exec(query).all()
                return [int(value) for value in rows if value is not None]
            except Exception as exc:
                logger.warning(
                    "invalid_users candidate preview query failed for site %s: %s",
                    site_key,
                    exc,
                )
                return []

    return await asyncio.to_thread(_run)


async def _query_existing_candidate_ids(
    db_manager: Any,
    *,
    start_id: int,
    end_id: int,
    last_checked_older_than_hours: float | None,
) -> list[int]:
    def _run() -> list[int]:
        with db_manager.get_session() as session:
            query = (
                select(UserModel.id)
                .where(
                    UserModel.id >= start_id,
                    UserModel.id <= end_id,
                )
                .order_by(UserModel.id)
            )
            if last_checked_older_than_hours is not None:
                cutoff = datetime.now(timezone.utc) - timedelta(
                    hours=last_checked_older_than_hours
                )
                query = query.where(
                    or_(
                        UserModel.last_checked_at.is_(None),
                        UserModel.last_checked_at <= cutoff,
                    )
                )

            rows = session.exec(query).all()
            return [int(value) for value in rows if value is not None]

    return await asyncio.to_thread(_run)


async def _query_existing_candidate_count(
    db_manager: Any,
    *,
    start_id: int,
    end_id: int,
    last_checked_older_than_hours: float | None,
) -> int:
    def _run() -> int:
        with db_manager.get_session() as session:
            query = select(func.count(UserModel.id)).where(
                UserModel.id >= start_id,
                UserModel.id <= end_id,
            )
            if last_checked_older_than_hours is not None:
                cutoff = datetime.now(timezone.utc) - timedelta(
                    hours=last_checked_older_than_hours
                )
                query = query.where(
                    or_(
                        UserModel.last_checked_at.is_(None),
                        UserModel.last_checked_at <= cutoff,
                    )
                )

            value = session.exec(query).first()
            return int(value or 0)

    return await asyncio.to_thread(_run)


async def _query_existing_candidate_ids_preview(
    db_manager: Any,
    *,
    start_id: int,
    end_id: int,
    last_checked_older_than_hours: float | None,
    limit: int,
) -> list[int]:
    """Return a bounded preview of eligible existing-user candidates."""

    def _run() -> list[int]:
        with db_manager.get_session() as session:
            query = (
                select(UserModel.id)
                .where(
                    UserModel.id >= start_id,
                    UserModel.id <= end_id,
                )
                .order_by(UserModel.id)
                .limit(limit)
            )
            if last_checked_older_than_hours is not None:
                cutoff = datetime.now(timezone.utc) - timedelta(
                    hours=last_checked_older_than_hours
                )
                query = query.where(
                    or_(
                        UserModel.last_checked_at.is_(None),
                        UserModel.last_checked_at <= cutoff,
                    )
                )

            rows = session.exec(query).all()
            return [int(value) for value in rows if value is not None]

    return await asyncio.to_thread(_run)


class DensityAccumulator:
    """Bucket accumulator tracking mutual classifications and candidate density."""

    def __init__(self, start_id: int, end_id: int, resolution: int = 512):
        self.start_id = start_id
        self.end_id = end_id
        self.span = max(1, end_id - start_id + 1)
        self.num_buckets = min(max(1, resolution), self.span)
        self.bucket_starts: list[int] = []
        self.bucket_ends: list[int] = []
        for i in range(self.num_buckets):
            b_start = self.start_id + (i * self.span) // self.num_buckets
            b_end = self.start_id + ((i + 1) * self.span) // self.num_buckets - 1
            self.bucket_starts.append(b_start)
            self.bucket_ends.append(b_end)

        self.missing = [0] * self.num_buckets
        self.invalid = [0] * self.num_buckets
        self.existing = [0] * self.num_buckets
        self.candidate = [0] * self.num_buckets
        self.is_exact = [True] * self.num_buckets
        self._recent_idx = 0

    def bucket_index(self, user_id: int) -> int:
        """Return the bucket whose ``[start, end]`` contains ``user_id``."""
        idx = min(self.num_buckets - 1, max(0, ((user_id - self.start_id) * self.num_buckets) // self.span))
        # Proportional division can land one bucket early, never late.
        if idx + 1 < self.num_buckets and self.bucket_starts[idx + 1] <= user_id:
            idx += 1
        return idx

    def add_range(self, lo: int, hi: int, category: str, is_candidate: bool = False) -> None:
        if lo > hi or hi < self.start_id or lo > self.end_id:
            return
        lo = max(lo, self.start_id)
        hi = min(hi, self.end_id)
        # Ordered scans add many small ranges to the bucket they used last.
        recent = self._recent_idx
        if self.bucket_starts[recent] <= lo and hi <= self.bucket_ends[recent]:
            self._add(recent, hi - lo + 1, category, is_candidate)
            return
        first_idx = self.bucket_index(lo)
        last_idx = self.bucket_index(hi)
        self._recent_idx = last_idx

        if first_idx == last_idx:
            count = hi - lo + 1
            self._add(first_idx, count, category, is_candidate)
            return

        for idx in range(first_idx, last_idx + 1):
            b_lo = max(lo, self.bucket_starts[idx])
            b_hi = min(hi, self.bucket_ends[idx])
            if b_lo <= b_hi:
                self._add(idx, b_hi - b_lo + 1, category, is_candidate)

    def _add(self, idx: int, count: int, category: str, is_candidate: bool) -> None:
        if category == "missing":
            self.missing[idx] += count
        elif category == "invalid":
            self.invalid[idx] += count
        elif category == "existing":
            self.existing[idx] += count
        if is_candidate:
            self.candidate[idx] += count

    def distribute_unscanned(
        self,
        scanned_up_to: int,
        target_missing: int,
        target_existing: int,
        target_invalid: int,
        include_missing: bool = True,
        include_invalid_candidates: bool = False,
        include_existing_candidates: bool = False,
        invalid_candidate_ratio: float = 1.0,
        existing_candidate_ratio: float = 1.0,
    ) -> None:
        """Distribute known remaining counts proportionally across unscanned buckets."""
        if scanned_up_to >= self.end_id:
            return
        rem_missing = max(0, target_missing - sum(self.missing))
        rem_existing = max(0, target_existing - sum(self.existing))
        rem_invalid = max(0, target_invalid - sum(self.invalid))

        first_unscanned_idx = min(
            self.num_buckets - 1,
            max(0, ((max(self.start_id, scanned_up_to + 1) - self.start_id) * self.num_buckets) // self.span)
        )

        unscanned_indices = [
            i for i in range(first_unscanned_idx, self.num_buckets)
            if self.bucket_ends[i] > scanned_up_to
        ]
        if not unscanned_indices:
            return

        total_unscanned_width = sum(
            self.bucket_ends[i] - max(self.bucket_starts[i], scanned_up_to + 1) + 1
            for i in unscanned_indices
        )
        if total_unscanned_width <= 0:
            return

        alloc_missing = 0
        alloc_existing = 0
        alloc_invalid = 0

        for idx in unscanned_indices:
            self.is_exact[idx] = False
            w = self.bucket_ends[idx] - max(self.bucket_starts[idx], scanned_up_to + 1) + 1
            ratio = w / total_unscanned_width

            m = round(ratio * rem_missing)
            e = round(ratio * rem_existing)
            inv = round(ratio * rem_invalid)

            self.missing[idx] += m
            self.existing[idx] += e
            self.invalid[idx] += inv

            alloc_missing += m
            alloc_existing += e
            alloc_invalid += inv

            c = 0
            if include_missing:
                c += m
            if include_invalid_candidates:
                c += round(inv * invalid_candidate_ratio)
            if include_existing_candidates:
                c += round(e * existing_candidate_ratio)
            self.candidate[idx] += c

        # Reconcile rounding differences onto the last unscanned bucket
        diff_m = rem_missing - alloc_missing
        diff_e = rem_existing - alloc_existing
        diff_inv = rem_invalid - alloc_invalid
        last = unscanned_indices[-1]
        self.missing[last] = max(0, self.missing[last] + diff_m)
        self.existing[last] = max(0, self.existing[last] + diff_e)
        self.invalid[last] = max(0, self.invalid[last] + diff_inv)
        if include_missing:
            self.candidate[last] = max(0, self.candidate[last] + diff_m)

    def is_all_exact(self) -> bool:
        return all(self.is_exact)

    def to_models(self) -> list[MPSDensityBucket]:
        return [
            MPSDensityBucket(
                start_id=self.bucket_starts[i],
                end_id=self.bucket_ends[i],
                missing_count=self.missing[i],
                invalid_count=self.invalid[i],
                existing_count=self.existing[i],
                candidate_count=self.candidate[i],
                is_exact=self.is_exact[i],
            )
            for i in range(self.num_buckets)
        ]



async def _sample_density_buckets(
    db_manager: Any,
    *,
    request: MPSAnalyzeRequest,
    site_key: str,
    start_id: int,
    end_id: int,
    invalid_table_ready: bool,
) -> list[MPSDensityBucket]:
    """Estimate every interval using bounded, stratified primary-key probes.

    A prefix of candidate ranges cannot describe the rest of ID space. These
    samples are independent of pagination and never replace authoritative totals.
    At most 8192 IDs are checked, regardless of the provider ID-space size.
    """
    def _run() -> list[MPSDensityBucket]:
        accumulator = DensityAccumulator(start_id, end_id, request.density_resolution)
        per_bucket = min(32, max(1, 8192 // accumulator.num_buckets))
        samples: list[list[int]] = []
        for lo, hi in zip(accumulator.bucket_starts, accumulator.bucket_ends):
            width = hi - lo + 1
            count = min(per_bucket, width)
            samples.append([lo + ((2 * index + 1) * width) // (2 * count) for index in range(count)])
        ids = [uid for bucket in samples for uid in bucket]
        with db_manager.get_session() as session:
            existing = dict(session.exec(
                select(UserModel.id, UserModel.last_checked_at).where(UserModel.id.in_(ids))
            ).all())
            invalid = {}
            if invalid_table_ready:
                invalid = {
                    int(uid): updated_at or checked_at
                    for uid, checked_at, updated_at in session.exec(
                        select(InvalidUserModel.user_id, InvalidUserModel.last_checked_at, InvalidUserModel.updated_at)
                        .where(InvalidUserModel.site == site_key, InvalidUserModel.user_id.in_(ids))
                    ).all()
                }
        now = datetime.now(timezone.utc)
        def eligible(checked_at: datetime | None, hours: float | None) -> bool:
            if hours is None or checked_at is None:
                return True
            if checked_at.tzinfo is None:
                checked_at = checked_at.replace(tzinfo=timezone.utc)
            return checked_at <= now - timedelta(hours=hours)

        for index, sample in enumerate(samples):
            width = accumulator.bucket_ends[index] - accumulator.bucket_starts[index] + 1
            missing = invalid_count = candidates = 0
            for uid in sample:
                if uid in existing:
                    candidates += int(request.include_existing_users and eligible(existing[uid], request.existing_last_checked_older_than_hours))
                elif uid in invalid:
                    invalid_count += 1
                    candidates += int(request.include_invalid and eligible(invalid[uid], request.invalid_last_checked_older_than_hours))
                else:
                    missing += 1
                    candidates += int(not request.ignore_missing_ids)
            scale = width / len(sample)
            accumulator.missing[index] = round(missing * scale)
            accumulator.invalid[index] = min(width - accumulator.missing[index], round(invalid_count * scale))
            # Keep mutually exclusive classifications partitioning the interval.
            accumulator.existing[index] = width - accumulator.missing[index] - accumulator.invalid[index]
            accumulator.candidate[index] = round(candidates * scale)
            accumulator.is_exact[index] = len(sample) == width
        return accumulator.to_models()

    return await asyncio.to_thread(_run)


def extract_dense_pockets(
    buckets: list[MPSDensityBucket],
    max_pockets: int = 20,
) -> list[MPSDensePocket]:
    """Find spatially meaningful candidate-rich clusters from density buckets."""
    if not buckets:
        return []

    clusters: list[list[MPSDensityBucket]] = []
    current_cluster: list[MPSDensityBucket] = []

    for b in buckets:
        cnt = b.candidate_count or 0
        if cnt > 0:
            if not current_cluster:
                current_cluster.append(b)
            else:
                prev = current_cluster[-1]
                if b.start_id == prev.end_id + 1:
                    current_cluster.append(b)
                else:
                    clusters.append(current_cluster)
                    current_cluster = [b]
        else:
            if current_cluster:
                clusters.append(current_cluster)
                current_cluster = []
    if current_cluster:
        clusters.append(current_cluster)

    pockets: list[MPSDensePocket] = []
    for cluster in clusters:
        c_start = cluster[0].start_id
        c_end = cluster[-1].end_id
        c_cand = sum(b.candidate_count or 0 for b in cluster)
        c_miss = sum(b.missing_count or 0 for b in cluster)
        c_inv = sum(b.invalid_count or 0 for b in cluster)
        c_exist = sum(b.existing_count or 0 for b in cluster)
        c_width = max(1, c_end - c_start + 1)
        density_pct = round((c_cand / c_width) * 100.0, 2)
        pockets.append(
            MPSDensePocket(
                start_id=c_start,
                end_id=c_end,
                candidate_count=c_cand,
                density_percentage=density_pct,
                missing_count=c_miss,
                invalid_count=c_inv,
                existing_count=c_exist,
            )
        )

    pockets.sort(key=lambda p: p.candidate_count, reverse=True)
    return pockets[:max_pockets]


async def _query_missing_ranges_summary(
    db_manager: Any, *, site_key: str, start_id: int, end_id: int,
    include_invalid_table: bool, preview_limit: int,
    existing_count: int | None = None, invalid_count: int | None = None,
    include_missing: bool = True, include_invalid_candidates: bool = False,
    include_existing_candidates: bool = False,
    invalid_cutoff: datetime | None = None, existing_cutoff: datetime | None = None,
    candidate_summary: dict[str, Any] | None = None,
    bounded: bool = False,
    density_resolution: int = 512,
    authoritative_missing: int | None = None,
    authoritative_candidate: int | None = None,
    density_summary: dict[str, Any] | None = None,
    coverage_summary: dict[str, Any] | None = None,
) -> tuple[int, int, list[tuple[int, int]], tuple[int, int] | None]:
    """Verify dense coverage or scan membership; cap previews, never analysis.

    Default missing-only analysis counts bounded spans in one snapshot and
    transfers IDs only for spans whose current union contains a hole. Full
    (unbounded) analysis counts every span with one grouped pass and reports
    the resulting inventories through ``coverage_summary``.
    Other selections merge ordered, bounded database streams. Duplicate known IDs cannot create
    gaps, and consecutive eligible IDs merge across source boundaries. Preview
    entries retain their complete boundaries even when their ID preview is short.
    """
    def _run():
        missing_count = missing_groups = candidate_count = candidate_groups = 0
        missing_preview: list[tuple[int, int]] = []
        candidate_preview: list[tuple[int, int]] = []
        current_group: tuple[int, int] | None = None
        last_missing: tuple[int, int] | None = None
        last_candidate: tuple[int, int] | None = None
        previous = start_id - 1
        invalid_table = include_invalid_table
        block_counts: dict[int, tuple[int, int]] | None = None
        coverage_count_ms = 0.0

        accumulator = DensityAccumulator(start_id, end_id, resolution=density_resolution)

        def flush_group():
            nonlocal candidate_groups, current_group, last_candidate
            if current_group is not None:
                candidate_groups += 1
                last_candidate = current_group
                if len(candidate_preview) < preview_limit:
                    candidate_preview.append(current_group)
                current_group = None

        def add_candidates(lo: int, hi: int):
            nonlocal candidate_count, current_group
            candidate_count += hi - lo + 1
            if current_group is not None and lo == current_group[1] + 1:
                current_group = (current_group[0], hi)
            else:
                flush_group()
                current_group = (lo, hi)

        def add_missing(lo: int, hi: int):
            nonlocal missing_count, missing_groups, last_missing
            if lo > hi:
                return
            missing_count += hi - lo + 1
            missing_groups += 1
            last_missing = (lo, hi)
            if len(missing_preview) < preview_limit:
                missing_preview.append((lo, hi))
            if include_missing:
                add_candidates(lo, hi)

        def eligible(checked: Any, cutoff: datetime | None):
            if cutoff is None or checked is None:
                return True
            if isinstance(checked, str):
                checked = datetime.fromisoformat(checked)
            if checked.tzinfo is None:
                checked = checked.replace(tzinfo=timezone.utc)
            return checked <= cutoff

        with db_manager.get_session() as session:
            params = {"start_id": start_id, "end_id": end_id, "site": site_key}
            stream_start = start_id
            is_postgres = False
            try:
                if session.get_bind().dialect.name == "postgresql":
                    is_postgres = True
                    session.connection().exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            except Exception:
                pass

            if bounded and is_postgres and start_id <= 1:
                try:
                    hint = session.execute(text(
                        "SELECT r.start_id FROM management.mps_plans p "
                        "JOIN management.mps_candidate_ranges r ON r.job_id = p.job_id AND r.first_ordinal = 0 "
                        "WHERE p.site = :site AND r.start_id > :start_id AND r.start_id <= :end_id "
                        "ORDER BY p.created_at DESC LIMIT 1"
                    ), {"site": site_key, "start_id": start_id, "end_id": end_id}).scalar()
                    if hint is not None and int(hint) > start_id:
                        stream_start = int(hint)
                        previous = stream_start - 1
                        accumulator.add_range(start_id, stream_start - 1, "existing", is_candidate=include_existing_candidates)
                except Exception:
                    pass

            scan_span = MPS_ANALYSIS_COUNT_ID_SPAN or MPS_ANALYSIS_SCAN_BLOCK_SIZE
            if not bounded:
                # Full analysis visits every span, so count them all at once in
                # this snapshot instead of issuing two statements per span.
                began = time.monotonic()
                block_counts, users_total, invalid_total, invalid_table = _count_coverage_blocks(
                    session,
                    site_key=site_key,
                    base_id=stream_start,
                    end_id=end_id,
                    span=scan_span,
                    include_invalid_table=include_invalid_table,
                )
                coverage_count_ms = (time.monotonic() - began) * 1000
                if coverage_summary is not None:
                    coverage_summary.update(
                        existing_count=users_total,
                        invalid_count=invalid_total,
                        invalid_table_ready=invalid_table,
                    )

            if include_missing and not include_invalid_candidates and not include_existing_candidates:
                count_ms = coverage_count_ms
                scan_ms = 0.0
                existing_total = skipped_ids = skipped_blocks = 0
                scanned_up_to = stream_start - 1
                for lower in range(stream_start, end_id + 1, scan_span):
                    if bounded and len(candidate_preview) >= preview_limit:
                        break
                    upper = min(end_id, lower + scan_span - 1)
                    scanned_up_to = upper
                    block_params = {"lower": lower, "upper": upper, "site": site_key}
                    if block_counts is not None:
                        user_count, invalid_only = block_counts.get((lower - stream_start) // scan_span, (0, 0))
                    else:
                        began = time.monotonic()
                        user_count = int(session.execute(text(
                            "SELECT count(*) FROM users WHERE id BETWEEN :lower AND :upper"
                        ), block_params).scalar_one())
                        invalid_only = 0
                        if invalid_table:
                            invalid_only = int(session.execute(text(
                                "SELECT count(DISTINCT i.user_id) FROM invalid_users i "
                                "WHERE i.site=:site AND i.user_id BETWEEN :lower AND :upper "
                                "AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id=i.user_id)"
                            ), block_params).scalar_one())
                        count_ms += (time.monotonic() - began) * 1000
                    existing_total += user_count
                    if user_count + invalid_only == upper - lower + 1:
                        if previous + 1 <= lower - 1:
                            add_missing(previous + 1, lower - 1)
                            accumulator.add_range(previous + 1, lower - 1, "missing", is_candidate=True)
                        accumulator.add_range(lower, upper, "existing", is_candidate=False)
                        previous = upper
                        skipped_blocks += 1
                        skipped_ids += upper - lower + 1
                        continue
                    began = time.monotonic()
                    if not bounded:
                        # Full analysis needs whole blocks, so read only the
                        # runs of known IDs; the gaps between them are exact.
                        for island_lo, island_hi in _query_known_islands(
                            session,
                            site_key=site_key,
                            lower=lower,
                            upper=upper,
                            include_invalid_table=invalid_table,
                        ):
                            if previous + 1 <= island_lo - 1:
                                add_missing(previous + 1, island_lo - 1)
                                accumulator.add_range(previous + 1, island_lo - 1, "missing", is_candidate=True)
                            accumulator.add_range(island_lo, island_hi, "existing", is_candidate=False)
                            previous = island_hi
                        scan_ms += (time.monotonic() - began) * 1000
                        continue
                    known_sql ="SELECT id AS user_id FROM users WHERE id BETWEEN :lower AND :upper"
                    if invalid_table:
                        known_sql += " UNION SELECT user_id FROM invalid_users WHERE site=:site AND user_id BETWEEN :lower AND :upper"
                    result = session.execute(text(known_sql + " ORDER BY user_id").execution_options(yield_per=16384), block_params)
                    try:
                        for row in result:
                            if bounded and len(candidate_preview) >= preview_limit:
                                scanned_up_to = previous
                                break
                            user_id = int(row[0])
                            if previous + 1 <= user_id - 1:
                                add_missing(previous + 1, user_id - 1)
                                accumulator.add_range(previous + 1, user_id - 1, "missing", is_candidate=True)
                            accumulator.add_range(user_id, user_id, "existing", is_candidate=False)
                            previous = user_id
                    finally:
                        result.close()
                    scan_ms += (time.monotonic() - began) * 1000
                    if bounded and len(candidate_preview) >= preview_limit:
                        break

                if not bounded or scanned_up_to >= end_id:
                    if previous + 1 <= end_id:
                        add_missing(previous + 1, end_id)
                        accumulator.add_range(previous + 1, end_id, "missing", is_candidate=True)
                    flush_group()
                    frontier = last_missing if last_missing is not None and last_missing[1] >= end_id - 1 else None
                else:
                    flush_group()
                    frontier = None
                    target_m = (
                        authoritative_missing
                        if authoritative_missing is not None
                        else max(0, (end_id - start_id + 1) - (existing_count if existing_count is not None else existing_total) - (invalid_count or 0))
                    )
                    target_e = existing_count if existing_count is not None else existing_total
                    target_inv = invalid_count if invalid_count is not None else 0
                    accumulator.distribute_unscanned(
                        scanned_up_to=scanned_up_to,
                        target_missing=target_m,
                        target_existing=target_e,
                        target_invalid=target_inv,
                        include_missing=True,
                    )
                    if authoritative_missing is not None:
                        missing_count = authoritative_missing
                    if authoritative_candidate is not None:
                        candidate_count = authoritative_candidate

                if preview_limit > 1 and last_candidate is not None and last_candidate not in candidate_preview:
                    candidate_preview = candidate_preview[:preview_limit - 1] + [last_candidate]

                density_models = accumulator.to_models()
                dense_pockets = extract_dense_pockets(density_models)

                if candidate_summary is not None:
                    summary_payload: dict[str, Any] = {
                        "count": candidate_count,
                        "range_count": candidate_groups,
                        "ranges": candidate_preview,
                    }
                    if not bounded:
                        summary_payload["existing_count"] = existing_total
                    elif existing_count is not None:
                        summary_payload["existing_count"] = existing_count
                    candidate_summary.update(summary_payload)

                if density_summary is not None:
                    density_summary.update(
                        density_buckets=density_models,
                        density_is_exact=accumulator.is_all_exact(),
                        dense_pockets=dense_pockets,
                    )

                logger.info("MPS analysis coverage count_ms=%.1f sparse_scan_ms=%.1f skipped_blocks=%s skipped_ids=%s existing_count=%s", count_ms, scan_ms, skipped_blocks, skipped_ids, existing_total)
                return missing_count, missing_groups, missing_preview, frontier

            params["stream_start"] = stream_start
            users = session.execute(text(
                "SELECT id" + (", last_checked_at" if include_existing_candidates else "") +
                " FROM users WHERE id BETWEEN :stream_start AND :end_id ORDER BY id"
            ).execution_options(yield_per=16384), params)
            invalid = None
            scanned_up_to = stream_start - 1
            try:
                if invalid_table:
                    invalid = session.execute(text(
                        "SELECT user_id" + (", COALESCE(updated_at, last_checked_at)" if include_invalid_candidates else "") +
                        " FROM invalid_users WHERE site=:site AND user_id BETWEEN :stream_start AND :end_id ORDER BY user_id"
                    ).execution_options(yield_per=16384), params)
                user_stream = ((int(row[0]), True, row[1] if include_existing_candidates else None) for row in users)
                invalid_stream = ((int(row[0]), False, row[1] if include_invalid_candidates else None) for row in invalid) if invalid is not None else iter(())
                for user_id, records in groupby(merge(user_stream, invalid_stream, key=lambda row: row[0]), key=lambda row: row[0]):
                    if bounded and len(candidate_preview) >= preview_limit:
                        break
                    scanned_up_to = user_id
                    if previous + 1 <= user_id - 1:
                        add_missing(previous + 1, user_id - 1)
                        accumulator.add_range(previous + 1, user_id - 1, "missing", is_candidate=include_missing)
                    rows = list(records)
                    existing = next((row for row in rows if row[1]), None)
                    if existing is not None:
                        selected = include_existing_candidates and eligible(existing[2], existing_cutoff)
                        accumulator.add_range(user_id, user_id, "existing", is_candidate=selected)
                    else:
                        selected = include_invalid_candidates and eligible(rows[0][2], invalid_cutoff)
                        accumulator.add_range(user_id, user_id, "invalid", is_candidate=selected)
                    if selected:
                        add_candidates(user_id, user_id)
                    previous = user_id

                if not bounded or scanned_up_to >= end_id:
                    if previous + 1 <= end_id:
                        add_missing(previous + 1, end_id)
                        accumulator.add_range(previous + 1, end_id, "missing", is_candidate=include_missing)
                    flush_group()
                    frontier = last_missing if last_missing is not None and last_missing[1] >= end_id - 1 else None
                else:
                    flush_group()
                    frontier = None
                    target_m = (
                        authoritative_missing
                        if authoritative_missing is not None
                        else max(0, (end_id - start_id + 1) - (existing_count or 0) - (invalid_count or 0))
                    )
                    target_e = existing_count if existing_count is not None else 0
                    target_inv = invalid_count if invalid_count is not None else 0
                    accumulator.distribute_unscanned(
                        scanned_up_to=scanned_up_to,
                        target_missing=target_m,
                        target_existing=target_e,
                        target_invalid=target_inv,
                        include_missing=include_missing,
                        include_invalid_candidates=include_invalid_candidates,
                        include_existing_candidates=include_existing_candidates,
                    )
                    if authoritative_missing is not None:
                        missing_count = authoritative_missing
                    if authoritative_candidate is not None:
                        candidate_count = authoritative_candidate
            finally:
                users.close()
                if invalid is not None:
                    invalid.close()

        if preview_limit > 1 and last_candidate is not None and last_candidate not in candidate_preview:
            candidate_preview = candidate_preview[:max(0, preview_limit - 1)] + [last_candidate]

        density_models = accumulator.to_models()
        dense_pockets = extract_dense_pockets(density_models)

        if candidate_summary is not None:
            summary_payload = {
                "count": candidate_count,
                "range_count": candidate_groups,
                "ranges": candidate_preview,
            }
            if existing_count is not None:
                summary_payload["existing_count"] = existing_count
            candidate_summary.update(summary_payload)

        if density_summary is not None:
            density_summary.update(
                density_buckets=density_models,
                density_is_exact=accumulator.is_all_exact(),
                dense_pockets=dense_pockets,
            )

        frontier = last_missing if last_missing is not None and last_missing[1] >= end_id - 1 else None
        return (missing_count, missing_groups, missing_preview, frontier) if include_missing else (0, 0, [], None)

    return await asyncio.to_thread(_run)


async def _compute_analysis(
    request: MPSAnalyzeRequest | MPSCreateJobRequest,
    backend_manager: BackendManager,
) -> _AnalyzeComputation:
    scope = await _resolve_analysis_scope(request, backend_manager)

    (
        existing_ids,
        invalid_ids,
        eligible_invalid_ids,
        invalid_table_ready,
    ) = await _query_ids_in_range(
        scope.db_manager,
        site_key=scope.site_key,
        start_id=scope.start_id,
        end_id=scope.end_id,
        include_invalid=request.include_invalid,
        invalid_last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
    )

    include_missing_ids = not bool(request.ignore_missing_ids)
    known_ids = _merge_sorted_unique_ids(existing_ids, invalid_ids)
    missing_ranges = (
        _build_missing_ranges(
            start_id=scope.start_id,
            end_id=scope.end_id,
            known_ids=known_ids,
        )
        if include_missing_ids
        else []
    )
    missing_count = sum((end - start + 1) for start, end in missing_ranges)

    if include_missing_ids:
        if request.include_invalid:
            eligible_invalid_id_set = set(eligible_invalid_ids)
            excluded_invalid_ids = [
                invalid_id
                for invalid_id in invalid_ids
                if invalid_id not in eligible_invalid_id_set
            ]
            candidate_known_ids = _merge_sorted_unique_ids(
                existing_ids, excluded_invalid_ids
            )
            candidate_ranges = _build_missing_ranges(
                start_id=scope.start_id,
                end_id=scope.end_id,
                known_ids=candidate_known_ids,
            )
        else:
            candidate_ranges = missing_ranges
    elif request.include_invalid:
        candidate_ranges = _ids_to_ranges(eligible_invalid_ids)
    else:
        candidate_ranges = []
    eligible_existing_ids: list[int] = []
    if request.include_existing_users:
        if request.existing_last_checked_older_than_hours is None:
            eligible_existing_ids = list(existing_ids)
        else:
            eligible_existing_ids = await _query_existing_candidate_ids(
                scope.db_manager,
                start_id=scope.start_id,
                end_id=scope.end_id,
                last_checked_older_than_hours=request.existing_last_checked_older_than_hours,
            )
    ranged_candidate_count = sum((end - start + 1) for start, end in candidate_ranges)
    candidate_count = ranged_candidate_count + len(eligible_existing_ids)

    return _AnalyzeComputation(
        site_display=scope.site_display,
        site_key=scope.site_key,
        database_min_id=scope.database_min_id,
        database_max_id=scope.database_max_id,
        start_id=scope.start_id,
        end_id=scope.end_id,
        existing_count=len(existing_ids),
        eligible_existing_ids=eligible_existing_ids,
        invalid_count=len(invalid_ids),
        eligible_invalid_ids=eligible_invalid_ids,
        invalid_table_ready=invalid_table_ready,
        missing_ranges=missing_ranges,
        candidate_ranges=candidate_ranges,
        missing_count=missing_count,
        include_missing_ids=include_missing_ids,
        candidate_count=candidate_count,
    )


async def _query_recent_plan_preview(
    db_manager: Any,
    *,
    site_key: str,
    start_id: int,
    end_id: int,
    candidate_offset: int,
    preview_limit: int,
    ignore_missing_ids: bool,
    include_invalid: bool,
    include_existing_users: bool,
) -> dict[str, Any] | None:
    def _run() -> dict[str, Any] | None:
        try:
            with db_manager.get_session() as session:
                if session.get_bind().dialect.name != "postgresql":
                    return None
                plan_query = text("""
                    WITH latest_plan AS (
                        SELECT job_id, summary_json, selection_json, total FROM management.mps_plans
                        WHERE site = :site AND total > 0
                          AND COALESCE((selection_json::jsonb ->> 'ignore_missing_ids')::boolean, false) = :ignore_missing_ids
                          AND COALESCE((selection_json::jsonb ->> 'include_invalid')::boolean, false) = :include_invalid
                          AND COALESCE((selection_json::jsonb ->> 'include_existing_users')::boolean, false) = :include_existing_users
                          AND COALESCE((selection_json::jsonb ->> 'start_id')::bigint, 1) = :start_id
                          AND COALESCE((selection_json::jsonb ->> 'end_id')::bigint, :end_id) = :end_id
                          AND COALESCE((summary_json::jsonb ->> 'candidate_count_exact')::boolean, true) = true
                        ORDER BY created_at DESC LIMIT 1
                    )
                    SELECT lp.summary_json, lp.selection_json, r.start_id, r.end_id
                    FROM latest_plan lp
                    JOIN management.mps_candidate_ranges r ON r.job_id = lp.job_id
                    WHERE r.first_ordinal >= :offset
                    ORDER BY r.first_ordinal ASC
                    LIMIT :limit;
                """)
                rows = session.execute(plan_query, {
                    "site": site_key,
                    "ignore_missing_ids": ignore_missing_ids,
                    "include_invalid": include_invalid,
                    "include_existing_users": include_existing_users,
                    "start_id": start_id,
                    "end_id": end_id,
                    "offset": candidate_offset,
                    "limit": preview_limit,
                }).fetchall()
                if not rows:
                    return None
                summary_raw = rows[0][0]
                summary = json.loads(summary_raw) if summary_raw else {}
                ranges = [(int(r[2]), int(r[3])) for r in rows]
                return {
                    "summary": summary,
                    "ranges": ranges,
                }
        except Exception as exc:
            logger.debug("Fast plan preview check bypassed: %s", exc)
            return None

    return await asyncio.to_thread(_run)


async def _compute_analysis_summary(
    request: MPSAnalyzeRequest,
    backend_manager: BackendManager,
) -> MPSAnalyzeResponse:
    analysis_started = time.monotonic()
    scope = await _resolve_analysis_scope(request, backend_manager)

    if request.bounded:
        fast_plan_data = await _query_recent_plan_preview(
            scope.db_manager,
            site_key=scope.site_key,
            start_id=scope.start_id,
            end_id=scope.end_id,
            candidate_offset=request.candidate_offset,
            preview_limit=max(request.range_preview_limit, request.missing_preview_limit),
            ignore_missing_ids=request.ignore_missing_ids,
            include_invalid=request.include_invalid,
            include_existing_users=request.include_existing_users,
        )
        if fast_plan_data is not None and fast_plan_data.get("summary", {}).get("candidate_count_exact") is False:
            fast_plan_data = None
        if fast_plan_data is not None:
            plan_summary = fast_plan_data["summary"]
            preview_ranges = fast_plan_data["ranges"]
            missing_count = plan_summary.get("missing_count", 0)
            invalid_eligible_count = plan_summary.get("invalid_eligible_count", 0)
            existing_eligible_count = plan_summary.get("existing_eligible_count", 0)
            candidate_count = plan_summary.get("candidate_count", missing_count)

            ranges_preview = [
                MissingRange(
                    start_id=gap_start,
                    end_id=gap_end,
                    count=(gap_end - gap_start + 1),
                )
                for gap_start, gap_end in preview_ranges[: request.range_preview_limit]
            ]
            missing_ids_preview = (
                _preview_missing_ids(preview_ranges, request.missing_preview_limit)
                if not request.ignore_missing_ids
                else []
            )
            candidate_ids_preview = list(missing_ids_preview)
            range_size = max(scope.end_id - scope.start_id + 1, 0)
            existing_count = _USER_COUNT_CACHE.get(
                ("users", scope.start_id, scope.end_id),
                (0, max(range_size - candidate_count, 0)),
            )[1]
            # Source inventory is independent of which sources enter the job.
            # A cold count cache must not make invalid records disappear.
            invalid_count, _, invalid_table_ready = await _query_invalid_counts(
                scope.db_manager, site_key=scope.site_key,
                start_id=scope.start_id, end_id=scope.end_id,
                include_invalid=False, last_checked_older_than_hours=None, bounded=True,
                db_min_id=scope.database_min_id, db_max_id=scope.database_max_id,
            )

            fast_buckets = await _sample_density_buckets(
                scope.db_manager, request=request, site_key=scope.site_key,
                start_id=scope.start_id, end_id=scope.end_id, invalid_table_ready=invalid_table_ready,
            )
            fast_pockets = extract_dense_pockets(fast_buckets)
            candidate_ranges_preview = (
                [MissingRange(start_id=p.start_id, end_id=p.end_id, count=p.candidate_count) for p in fast_pockets]
                if fast_pockets
                else ranges_preview
            )

            batch_plan = _build_batch_plan(
                candidate_count=candidate_count,
                candidate_offset=request.candidate_offset,
                requested_batch_size=request.batch_size,
                range_size=range_size,
                invalid_table_ready=invalid_table_ready,
                include_invalid=request.include_invalid,
                invalid_last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
                include_existing_users=request.include_existing_users,
                existing_last_checked_older_than_hours=request.existing_last_checked_older_than_hours,
                missing_count=missing_count,
                include_missing=not request.ignore_missing_ids,
                routes=request.routes,
            )

            return MPSAnalyzeResponse(
                site=scope.site_display,
                database_min_id=scope.database_min_id,
                database_max_id=scope.database_max_id,
                start_id=scope.start_id,
                end_id=scope.end_id,
                range_size=range_size,
                existing_user_count=existing_count,
                invalid_user_count=invalid_count,
                invalid_table_ready=invalid_table_ready,
                invalid_eligible_count=invalid_eligible_count,
                existing_eligible_count=existing_eligible_count,
                missing_count=missing_count,
                candidate_count=candidate_count,
                missing_range_count=len(preview_ranges),
                missing_range_count_exact=False,
                missing_ranges_truncated=candidate_count > len(missing_ids_preview),
                missing_ranges=ranges_preview,
                missing_ids_preview=missing_ids_preview,
                frontier_range=None,
                candidate_ranges_preview=candidate_ranges_preview,
                candidate_ids_preview=candidate_ids_preview,
                candidate_range_count=len(preview_ranges),
                candidate_preview_truncated=candidate_count > len(candidate_ids_preview),
                candidate_ranges_truncated=True,
                density_resolution=request.density_resolution,
                density_buckets=fast_buckets,
                density_is_exact=all(bucket.is_exact for bucket in fast_buckets),
                dense_pockets=fast_pockets,
                candidate_offset=batch_plan.candidate_offset,
                planned_batch_size=batch_plan.requested_batch_size,
                planned_candidate_count=batch_plan.planned_candidate_count,
                remaining_candidate_count=batch_plan.remaining_candidate_count,
                next_candidate_offset=batch_plan.next_candidate_offset,
                has_next_batch=batch_plan.has_next_batch,
                recommended_batch_size=batch_plan.recommended_batch_size,
                adaptive_batch_size=batch_plan.adaptive_batch_size,
                batch_recommendations=batch_plan.batch_recommendations,
                warnings=([f"Discovered user boundaries: IDs {scope.start_id:,} to {scope.end_id:,} (estimated population: {scope.discovery_result.estimated_total_users:,} users with {int(scope.discovery_result.confidence_level * 100)}% confidence)."] if scope.discovery_result is not None else []) + batch_plan.warnings,
                requires_confirmation=batch_plan.requires_confirmation,
                population_estimate=scope.discovery_result.estimated_total_users if scope.discovery_result else None,
                population_confidence_interval=list(scope.discovery_result.population_confidence_interval) if scope.discovery_result else None,
                highest_id_confidence_interval=list(scope.discovery_result.highest_id_confidence_interval) if scope.discovery_result else None,
                density_estimate=scope.discovery_result.estimated_density if scope.discovery_result else None,
                adaptive_discovery_performed=scope.discovery_result is not None,
            )

    range_size = max(scope.end_id - scope.start_id + 1, 0)
    counts_started = time.monotonic()

    eligibility_queries = (
        (
            _query_existing_candidate_count(
                scope.db_manager,
                start_id=scope.start_id,
                end_id=scope.end_id,
                last_checked_older_than_hours=request.existing_last_checked_older_than_hours,
            )
            if request.include_existing_users
            and request.existing_last_checked_older_than_hours is not None
            else asyncio.sleep(0, result=0)
        ),
        (
            _query_invalid_candidate_ids_preview(
                scope.db_manager,
                site_key=scope.site_key,
                start_id=scope.start_id,
                end_id=scope.end_id,
                last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
                limit=request.missing_preview_limit,
            )
            if request.include_invalid
            else asyncio.sleep(0, result=[])
        ),
        (
            _query_existing_candidate_ids_preview(
                scope.db_manager,
                start_id=scope.start_id,
                end_id=scope.end_id,
                last_checked_older_than_hours=request.existing_last_checked_older_than_hours,
                limit=request.missing_preview_limit,
            )
            if request.include_existing_users
            else asyncio.sleep(0, result=[])
        ),
    )

    missing_count = 0
    missing_range_count = 0
    preview_ranges: list[tuple[int, int]] = []
    frontier_range_obj: MissingRange | None = None
    candidate_summary: dict[str, Any] = {}
    density_summary: dict[str, Any] = {}
    preview_limit = max(request.range_preview_limit, request.missing_preview_limit)
    missing_summary_kwargs: dict[str, Any] = dict(
        site_key=scope.site_key,
        start_id=scope.start_id,
        end_id=scope.end_id,
        preview_limit=preview_limit,
        include_missing=not request.ignore_missing_ids,
        include_invalid_candidates=request.include_invalid,
        include_existing_candidates=request.include_existing_users,
        invalid_cutoff=(datetime.now(timezone.utc) - timedelta(hours=request.invalid_last_checked_older_than_hours)) if request.invalid_last_checked_older_than_hours is not None else None,
        existing_cutoff=(datetime.now(timezone.utc) - timedelta(hours=request.existing_last_checked_older_than_hours)) if request.existing_last_checked_older_than_hours is not None else None,
        candidate_summary=candidate_summary,
        density_summary=density_summary,
        bounded=request.bounded,
        density_resolution=request.density_resolution,
    )

    if request.bounded:
        (
            existing_count,
            invalid_result,
            existing_eligible_count,
            invalid_ids_preview,
            existing_ids_preview,
        ) = await asyncio.gather(
            _query_existing_count(
                scope.db_manager,
                start_id=scope.start_id,
                end_id=scope.end_id,
                bounded=True,
                db_min_id=scope.database_min_id,
                db_max_id=scope.database_max_id,
            ),
            _query_invalid_counts(
                scope.db_manager,
                site_key=scope.site_key,
                start_id=scope.start_id,
                end_id=scope.end_id,
                include_invalid=request.include_invalid,
                last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
                bounded=True,
                db_min_id=scope.database_min_id,
                db_max_id=scope.database_max_id,
            ),
            *eligibility_queries,
        )

        logger.info("MPS analysis independent_counts_ms=%.1f", (time.monotonic() - counts_started) * 1000)
        invalid_count, invalid_eligible_count, invalid_table_ready = invalid_result
        if (
            request.include_existing_users
            and request.existing_last_checked_older_than_hours is None
        ):
            existing_eligible_count = existing_count

        authoritative_missing = max(0, range_size - existing_count - invalid_count) if not request.ignore_missing_ids else 0
        authoritative_candidate = (
            authoritative_missing
            + (invalid_eligible_count if request.include_invalid else 0)
            + (existing_eligible_count if request.include_existing_users else 0)
        )

        ranges_started = time.monotonic()
        missing_summary_result = await _query_missing_ranges_summary(
            scope.db_manager,
            include_invalid_table=invalid_table_ready,
            existing_count=existing_count,
            invalid_count=invalid_count,
            authoritative_missing=authoritative_missing,
            authoritative_candidate=authoritative_candidate,
            **missing_summary_kwargs,
        )
    else:
        # Full analysis reads user and invalid inventories from the coverage
        # pass itself: the range is counted once, in the snapshot the gap scan
        # reads, while eligibility filters run concurrently.
        coverage_summary: dict[str, Any] = {}
        ranges_started = counts_started
        (
            invalid_result,
            existing_eligible_count,
            invalid_ids_preview,
            existing_ids_preview,
            missing_summary_result,
        ) = await asyncio.gather(
            (
                _query_invalid_counts(
                    scope.db_manager,
                    site_key=scope.site_key,
                    start_id=scope.start_id,
                    end_id=scope.end_id,
                    include_invalid=True,
                    last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
                    include_total=False,
                )
                if request.include_invalid
                else asyncio.sleep(0, result=(0, 0, True))
            ),
            *eligibility_queries,
            _query_missing_ranges_summary(
                scope.db_manager,
                include_invalid_table=True,
                coverage_summary=coverage_summary,
                **missing_summary_kwargs,
            ),
        )

        existing_count = coverage_summary["existing_count"]
        invalid_count = coverage_summary["invalid_count"]
        invalid_table_ready = coverage_summary["invalid_table_ready"]
        invalid_eligible_count = invalid_result[1] if invalid_table_ready else 0
        if (
            request.include_existing_users
            and request.existing_last_checked_older_than_hours is None
        ):
            existing_eligible_count = existing_count

        authoritative_missing = max(0, range_size - existing_count - invalid_count) if not request.ignore_missing_ids else 0
        authoritative_candidate = (
            authoritative_missing
            + (invalid_eligible_count if request.include_invalid else 0)
            + (existing_eligible_count if request.include_existing_users else 0)
        )
        # Keep quick analysis' inventory caches warm, as the standalone
        # full-range counts did.
        counted_at = time.monotonic()
        _USER_COUNT_CACHE[("users", scope.start_id, scope.end_id)] = (counted_at, existing_count)
        if invalid_table_ready:
            _INVALID_COUNT_CACHE[(scope.site_key, scope.start_id, scope.end_id)] = (counted_at, invalid_count)

    logger.info("MPS analysis missing_ranges_ms=%.1f elapsed_ms=%.1f", (time.monotonic() - ranges_started) * 1000, (time.monotonic() - analysis_started) * 1000)
    if len(missing_summary_result) >= 4 and missing_summary_result[3] is not None:
        f_start, f_end = missing_summary_result[3]
        frontier_range_obj = MissingRange(
            start_id=f_start,
            end_id=f_end,
            count=(f_end - f_start + 1),
        )
    missing_count, missing_range_count, preview_ranges = missing_summary_result[:3]

    ranges_preview = [
        MissingRange(
            start_id=gap_start,
            end_id=gap_end,
            count=(gap_end - gap_start + 1),
        )
        for gap_start, gap_end in preview_ranges[: request.range_preview_limit]
    ]
    missing_ids_preview = (
        _preview_missing_ids(preview_ranges, request.missing_preview_limit)
        if not request.ignore_missing_ids
        else []
    )
    nonexisting_ids_preview = _merge_sorted_unique_ids(
        missing_ids_preview,
        invalid_ids_preview,
    )[: request.missing_preview_limit]
    candidate_ids_preview = list(nonexisting_ids_preview)
    remaining_preview_slots = request.missing_preview_limit - len(candidate_ids_preview)
    if remaining_preview_slots > 0:
        candidate_ids_preview.extend(existing_ids_preview[:remaining_preview_slots])

    density_buckets = density_summary.get("density_buckets", [])
    if request.bounded:
        density_buckets = await _sample_density_buckets(
            scope.db_manager, request=request, site_key=scope.site_key,
            start_id=scope.start_id, end_id=scope.end_id, invalid_table_ready=invalid_table_ready,
        )
    density_is_exact = all(bucket.is_exact for bucket in density_buckets)
    dense_pockets = extract_dense_pockets(density_buckets)

    candidate_ranges_preview = [
        MissingRange(
            start_id=range_start,
            end_id=range_end,
            count=(range_end - range_start + 1),
        )
        for range_start, range_end in _ids_to_ranges(sorted(candidate_ids_preview))[
            : request.range_preview_limit
        ]
    ]
    candidate_count = missing_count + invalid_eligible_count + existing_eligible_count
    candidate_range_count = None
    if candidate_summary and "count" in candidate_summary:
        candidate_count = candidate_summary["count"]
        candidate_range_count = candidate_summary.get("range_count")
        if "ranges" in candidate_summary and candidate_summary["ranges"]:
            candidate_ranges_preview = [
                MissingRange(start_id=lo, end_id=hi, count=hi - lo + 1)
                for lo, hi in candidate_summary["ranges"][: request.range_preview_limit]
            ]
    elif not request.include_invalid and not request.include_existing_users and not request.ignore_missing_ids:
        candidate_ranges_preview = ranges_preview
        candidate_range_count = missing_range_count
    elif candidate_count == len(candidate_ids_preview):
        candidate_range_count = len(_ids_to_ranges(sorted(candidate_ids_preview)))

    if request.bounded:
        missing_count = authoritative_missing
        candidate_count = candidate_summary.get("count", authoritative_candidate)
        missing_range_count_exact = False
    else:
        missing_range_count_exact = missing_range_count is not None

    batch_plan = _build_batch_plan(
        candidate_count=candidate_count,
        candidate_offset=request.candidate_offset,
        requested_batch_size=request.batch_size,
        range_size=range_size,
        invalid_table_ready=invalid_table_ready,
        include_invalid=request.include_invalid,
        invalid_last_checked_older_than_hours=request.invalid_last_checked_older_than_hours,
        include_existing_users=request.include_existing_users,
        existing_last_checked_older_than_hours=request.existing_last_checked_older_than_hours,
        missing_count=missing_count,
        include_missing=not request.ignore_missing_ids,
        routes=request.routes,
    )

    return MPSAnalyzeResponse(
        site=scope.site_display,
        database_min_id=scope.database_min_id,
        database_max_id=scope.database_max_id,
        start_id=scope.start_id,
        end_id=scope.end_id,
        range_size=range_size,
        existing_user_count=existing_count,
        invalid_user_count=invalid_count,
        invalid_table_ready=invalid_table_ready,
        invalid_eligible_count=invalid_eligible_count,
        existing_eligible_count=existing_eligible_count,
        missing_count=missing_count,
        candidate_count=candidate_count,
        missing_range_count=missing_range_count,
        missing_range_count_exact=missing_range_count_exact,
        missing_ranges_truncated=missing_range_count is None or missing_range_count > request.range_preview_limit,
        missing_ranges=ranges_preview,
        missing_ids_preview=missing_ids_preview,
        frontier_range=frontier_range_obj,
        candidate_ranges_preview=candidate_ranges_preview,
        candidate_ids_preview=candidate_ids_preview,
        candidate_range_count=candidate_range_count,
        candidate_preview_truncated=candidate_count > len(candidate_ids_preview),
        candidate_ranges_truncated=(candidate_range_count > len(candidate_ranges_preview)) if candidate_range_count is not None else candidate_count > len(candidate_ids_preview),
        density_resolution=request.density_resolution,
        density_buckets=density_buckets,
        density_is_exact=density_is_exact,
        dense_pockets=dense_pockets,
        candidate_offset=batch_plan.candidate_offset,
        planned_batch_size=batch_plan.requested_batch_size,
        planned_candidate_count=batch_plan.planned_candidate_count,
        remaining_candidate_count=batch_plan.remaining_candidate_count,
        next_candidate_offset=batch_plan.next_candidate_offset,
        has_next_batch=batch_plan.has_next_batch,
        recommended_batch_size=batch_plan.recommended_batch_size,
        adaptive_batch_size=batch_plan.adaptive_batch_size,
        batch_recommendations=batch_plan.batch_recommendations,
        warnings=([f"Discovered user boundaries: IDs {scope.start_id:,} to {scope.end_id:,} (estimated population: {scope.discovery_result.estimated_total_users:,} users with {int(scope.discovery_result.confidence_level * 100)}% confidence)."] if scope.discovery_result is not None else []) + batch_plan.warnings,
        requires_confirmation=batch_plan.requires_confirmation,
        population_estimate=scope.discovery_result.estimated_total_users if scope.discovery_result else None,
        population_confidence_interval=list(scope.discovery_result.population_confidence_interval) if scope.discovery_result else None,
        highest_id_confidence_interval=list(scope.discovery_result.highest_id_confidence_interval) if scope.discovery_result else None,
        density_estimate=scope.discovery_result.estimated_density if scope.discovery_result else None,
        adaptive_discovery_performed=scope.discovery_result is not None,
    )


def _smooth_weighted_round_robin(
    items_with_weights: Sequence[tuple[str, int]],
) -> list[str]:
    """Interleave items according to their capacity weights using Nginx Smooth Weighted Round-Robin.

    Guarantees:
    1. Items appear proportionally to their max_connections weight.
    2. Items are interleaved as uniformly as possible across the sequence (no consecutive bursts).
    3. Cycle length is minimized via GCD reduction.
    """
    if not items_with_weights:
        return []
    if len(items_with_weights) == 1:
        return [items_with_weights[0][0]]

    weights = [max(1, w) for _, w in items_with_weights]
    common_gcd = math.gcd(*weights)
    reduced_weights = [w // common_gcd for w in weights]
    total_weight = sum(reduced_weights)

    current_weights = [0] * len(items_with_weights)
    result: list[str] = []

    for _ in range(total_weight):
        best_idx = 0
        best_weight = -float("inf")
        for i, weight in enumerate(reduced_weights):
            current_weights[i] += weight
            if current_weights[i] > best_weight:
                best_weight = current_weights[i]
                best_idx = i

        current_weights[best_idx] -= total_weight
        result.append(items_with_weights[best_idx][0])

    return result


def _active_assigned_proxy_keys(backend_manager: BackendManager) -> set[str]:
    """Proxy URLs and keys bound to an active account on any supported site."""
    assigned_proxy_keys: set[str] = set()
    for site_name in ("OnlyFans", "Fansly", "LoyalFans"):
        try:
            db_manager = backend_manager.get_site_db_manager(site_name)
        except Exception:
            continue
        if db_manager is None:
            continue
        with db_manager.get_session() as session:
            rows = session.exec(
                select(UserAuthModel.proxy_id).where(
                    UserAuthModel.proxy_id.is_not(None),
                    UserAuthModel.active.is_(True),
                )
            ).all()
            for proxy_id in rows:
                if not proxy_id:
                    continue
                proxy_key = str(proxy_id).strip()
                if not proxy_key:
                    continue
                assigned_proxy_keys.add(proxy_key)
                resolved_assigned = resolve_proxy_url(proxy_key)
                if resolved_assigned:
                    assigned_proxy_keys.add(resolved_assigned)
    return assigned_proxy_keys


def _resolve_proxy_pool(
    *,
    request: MPSCreateJobRequest,
    config: UltimaScraperCollectionConfig,
    backend_manager: BackendManager,
    expand_weights: bool = True,
    assigned_proxy_keys: set[str] | None = None,
) -> list[str]:
    if not request.use_proxies or request.proxy_mode == MPSProxyMode.NONE:
        return []

    configured_proxies = list(getattr(config.settings.network, "proxies", []) or [])
    enabled_proxies = [
        proxy for proxy in configured_proxies if getattr(proxy, "enabled", True)
    ]
    if not enabled_proxies and request.proxy_mode != MPSProxyMode.CUSTOM:
        return []

    # Map configured proxies to their max_connections
    configured_weights: dict[str, int] = {}
    for p in configured_proxies:
        p_url = str(getattr(p, "url", "") or "").strip()
        if p_url:
            resolved_p_url = resolve_proxy_url(p_url) or p_url
            conn = getattr(p, "max_connections", 8)
            limit = mps_proxy_connection_limit(conn)
            configured_weights[p_url] = limit
            configured_weights[resolved_p_url] = limit

    if request.proxy_mode == MPSProxyMode.CUSTOM:
        custom_urls = _validate_proxy_urls(request.proxy_urls)
        if not custom_urls:
            raise HTTPException(
                status_code=400,
                detail="proxy_mode='custom' requires proxy_urls.",
            )
        disabled_proxy_urls = {
            str(getattr(proxy, "url", "") or "").strip()
            for proxy in configured_proxies
            if not getattr(proxy, "enabled", True)
        }
        if any(proxy_url in disabled_proxy_urls for proxy_url in custom_urls):
            raise HTTPException(
                status_code=400,
                detail="Custom proxy URLs include a disabled configured proxy.",
            )
        # Preserve order while deduplicating
        distinct_custom = list(dict.fromkeys(custom_urls))
        if not expand_weights:
            return distinct_custom
        weighted_custom = [
            (url, configured_weights.get(url, 8))
            for url in distinct_custom
        ]
        return _smooth_weighted_round_robin(weighted_custom)

    # Only unassigned_only excludes proxies bound to an active account.
    if request.proxy_mode != MPSProxyMode.UNASSIGNED_ONLY:
        assigned_proxy_keys = set()
    elif assigned_proxy_keys is None:
        assigned_proxy_keys = _active_assigned_proxy_keys(backend_manager)

    distinct_items: dict[str, int] = {}
    for proxy in enabled_proxies:
        proxy_id = str(getattr(proxy, "url", "") or "").strip()
        if not proxy_id:
            continue
        resolved_url = resolve_proxy_url(proxy_id) or proxy_id
        if proxy_id in assigned_proxy_keys or resolved_url in assigned_proxy_keys:
            continue

        conn = getattr(proxy, "max_connections", 8)
        limit = mps_proxy_connection_limit(conn)
        if resolved_url not in distinct_items:
            distinct_items[resolved_url] = limit

    if not expand_weights:
        return list(distinct_items.keys())

    return _smooth_weighted_round_robin(list(distinct_items.items()))


def _resolve_proxy_pool_capacity(
    *,
    request: MPSCreateJobRequest,
    config: UltimaScraperCollectionConfig,
    backend_manager: BackendManager,
    assigned_proxy_keys: set[str] | None = None,
) -> int:
    """Return total concurrent connection capacity across all distinct proxies in the pool."""
    distinct_proxies = _resolve_proxy_pool(
        request=request,
        config=config,
        backend_manager=backend_manager,
        expand_weights=False,
        assigned_proxy_keys=assigned_proxy_keys,
    )
    if not distinct_proxies:
        return 0

    configured_proxies = list(getattr(config.settings.network, "proxies", []) or [])
    weight_map: dict[str, int] = {}
    for p in configured_proxies:
        p_url = str(getattr(p, "url", "") or "").strip()
        if p_url:
            resolved_p_url = resolve_proxy_url(p_url) or p_url
            conn = getattr(p, "max_connections", 8)
            limit = mps_proxy_connection_limit(conn)
            weight_map[p_url] = limit
            weight_map[resolved_p_url] = limit

    return sum(weight_map.get(url, 8) for url in distinct_proxies)


def _resolve_mps_proxies(
    *,
    request: MPSCreateJobRequest,
    config: UltimaScraperCollectionConfig,
    backend_manager: BackendManager,
) -> tuple[list[str], int]:
    """Return the weighted pool and its capacity from one assignment lookup.

    Blocking: reads account assignments from every site database. Call it
    from a worker thread, not the event loop.
    """
    assigned_proxy_keys: set[str] | None = None
    if (
        request.use_proxies
        and request.proxy_mode == MPSProxyMode.UNASSIGNED_ONLY
        and any(
            getattr(proxy, "enabled", True)
            for proxy in getattr(config.settings.network, "proxies", []) or []
        )
    ):
        assigned_proxy_keys = _active_assigned_proxy_keys(backend_manager)
    proxies = _resolve_proxy_pool(
        request=request,
        config=config,
        backend_manager=backend_manager,
        assigned_proxy_keys=assigned_proxy_keys,
    )
    capacity = _resolve_proxy_pool_capacity(
        request=request,
        config=config,
        backend_manager=backend_manager,
        assigned_proxy_keys=assigned_proxy_keys,
    )
    return proxies, capacity


@router.post("/analyze", response_model=MPSAnalyzeResponse)
async def analyze_mps(
    request: MPSAnalyzeRequest,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> MPSAnalyzeResponse:
    return await _compute_analysis_summary(request, backend_manager)


@router.post("/create-job", response_model=MPSCreateJobResponse)
async def create_mps_job(
    request: MPSCreateJobRequest,
    backend: BackendInstance = Depends(get_backend_instance),
    backend_manager: BackendManager = Depends(get_backend_manager),
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> MPSCreateJobResponse:
    if backend.redis is None or not getattr(backend.redis, "is_connected", False):
        raise HTTPException(503, "Job execution requires connected Redis")
    from .mps_durable import create_job as create_durable_mps_job

    return await create_durable_mps_job(request, backend, backend_manager, config)


@router.post("/proxy-preview", response_model=MPSProxyPreviewResponse)
async def preview_mps_proxies(
    request: MPSProxyPreviewRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> MPSProxyPreviewResponse:
    site_display, _ = _normalize_site_name(request.site)
    normalized_request = MPSCreateJobRequest(
        site=site_display,
        use_proxies=request.use_proxies,
        proxy_mode=request.proxy_mode,
        proxy_urls=request.proxy_urls,
        include_direct_connection=request.include_direct_connection,
    )
    proxies, capacity = await asyncio.to_thread(
        lambda: _resolve_mps_proxies(
            request=normalized_request,
            config=config,
            backend_manager=backend_manager,
        )
    )
    direct_included = (not bool(proxies)) or request.include_direct_connection
    return MPSProxyPreviewResponse(
        site=site_display,
        use_proxies=request.use_proxies,
        proxy_mode=request.proxy_mode,
        proxies_configured=len(getattr(config.settings.network, "proxies", []) or []),
        proxies_used=len(set(proxies)),
        direct_connection_included=direct_included,
        proxy_urls=proxies,
        proxy_capacity=capacity,
    )


@router.get("/site-bounds", response_model=MPSSiteBoundsResponse)
async def get_site_bounds_endpoint(
    site: str = "OnlyFans",
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> MPSSiteBoundsResponse:
    site_display, site_key = _normalize_site_name(site)
    db_manager = (
        backend_manager.get_site_db_manager(site_display)
        if hasattr(backend_manager, "get_site_db_manager")
        else None
    )
    db_min_id: int | None = None
    db_max_id: int | None = None
    if db_manager is not None:
        try:
            db_min_id, db_max_id = await _query_min_max(
                db_manager,
                site_key=site_key,
                include_user_bounds=True,
                include_invalid_bounds=False,
            )
        except Exception:
            logger.debug("Failed querying db min/max for site bounds", exc_info=True)

    return MPSSiteBoundsResponse(
        site=site_display,
        database_min_id=db_min_id,
        database_max_id=db_max_id,
    )


@router.post("/discover-bounds", response_model=MPSBoundaryDiscoveryResponse)
async def discover_bounds_endpoint(
    request: MPSBoundaryDiscoveryRequest,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> MPSBoundaryDiscoveryResponse:
    site_display, site_key = _normalize_site_name(request.site)
    db_manager = (
        backend_manager.get_site_db_manager(site_display)
        if hasattr(backend_manager, "get_site_db_manager")
        else None
    )
    db_min_id: int | None = None
    db_max_id: int | None = None
    if db_manager is not None:
        try:
            db_min_id, db_max_id = await _query_min_max(
                db_manager,
                site_key=site_key,
                include_user_bounds=True,
                include_invalid_bounds=False,
            )
        except Exception as exc:
            logger.debug("Failed querying db min/max for discovery: %s", exc)

    if request.min_id is not None and request.min_id > (db_min_id or 1):
        if db_max_id is not None and request.min_id >= db_max_id:
            starting_id = request.min_id
            known_max = request.min_id
            known_min = db_min_id
            search_min_id = db_min_id or 1
        else:
            starting_id = request.min_id
            known_max = None
            known_min = None
            search_min_id = request.min_id
    else:
        starting_id = db_max_id or 1
        known_max = db_max_id
        known_min = db_min_id
        search_min_id = db_min_id or 1

    cfg = BoundaryDiscoveryConfig(
        min_id=search_min_id,
        known_max_id=known_max,
        known_min_id=known_min,
        probe_multiplier=request.probe_multiplier,
        max_id_ceiling=request.max_id_ceiling,
        max_probes=request.max_probes,
        samples_per_step=request.samples_per_step,
        refinement_steps=request.refinement_steps,
        confidence_level=request.confidence_level,
        target_margin_of_error=request.target_margin_of_error,
    )
    result = await discover_user_id_boundaries(
        site=site_display,
        backend_manager=backend_manager,
        config=cfg,
    )
    return MPSBoundaryDiscoveryResponse(
        site=site_display,
        discovered_start_id=result.discovered_start_id,
        discovered_end_id=result.discovered_end_id,
        highest_observed_id=result.highest_observed_id,
        lowest_observed_id=result.lowest_observed_id,
        estimated_total_users=result.estimated_total_users,
        population_confidence_interval=list(result.population_confidence_interval),
        estimated_density=result.estimated_density,
        density_margin_of_error=result.density_margin_of_error,
        confidence_level=result.confidence_level,
        highest_id_confidence_interval=list(result.highest_id_confidence_interval),
        total_probes_used=result.total_probes_used,
        valid_probes_count=result.valid_probes_count,
        warnings=result.warnings,
        observations=result.observations,
    )


@router.get("/discover-bounds/stream")
async def discover_bounds_stream(
    site: str = "OnlyFans",
    min_id: int | None = None,
    max_id_ceiling: int = 100_000_000_000,
    max_probes: int = 50,
    probe_multiplier: float = 10.0,
    samples_per_step: int = 3,
    refinement_steps: int = 6,
    confidence_level: float = 0.95,
    target_margin_of_error: float = 0.05,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> StreamingResponse:
    site_display, site_key = _normalize_site_name(site)
    db_manager = (
        backend_manager.get_site_db_manager(site_display)
        if hasattr(backend_manager, "get_site_db_manager")
        else None
    )
    db_min_id: int | None = None
    db_max_id: int | None = None
    if db_manager is not None:
        try:
            db_min_id, db_max_id = await _query_min_max(
                db_manager,
                site_key=site_key,
                include_user_bounds=True,
                include_invalid_bounds=False,
            )
        except Exception as exc:
            logger.debug("Failed querying db min/max for discovery stream: %s", exc)

    if min_id is not None and min_id > (db_min_id or 1):
        if db_max_id is not None and min_id >= db_max_id:
            starting_id = min_id
            known_max = min_id
            known_min = db_min_id
            search_min_id = db_min_id or 1
        else:
            starting_id = min_id
            known_max = None
            known_min = None
            search_min_id = min_id
    else:
        starting_id = db_max_id or 1
        known_max = db_max_id
        known_min = db_min_id
        search_min_id = db_min_id or 1

    cfg = BoundaryDiscoveryConfig(
        min_id=search_min_id,
        known_max_id=known_max,
        known_min_id=known_min,
        probe_multiplier=probe_multiplier,
        max_id_ceiling=max_id_ceiling,
        max_probes=max_probes,
        samples_per_step=samples_per_step,
        refinement_steps=refinement_steps,
        confidence_level=confidence_level,
        target_margin_of_error=target_margin_of_error,
    )

    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

    async def _on_probe(obs: ProbeObservation, total: int, highest: int | None) -> None:
        await queue.put({
            "type": "probe",
            "user_id": obs.user_id,
            "is_valid": obs.is_valid,
            "phase": obs.phase,
            "total_probes": total,
            "highest_observed_id": highest,
        })

    async def _on_phase(phase: str, message: str) -> None:
        await queue.put({
            "type": "phase",
            "phase": phase,
            "message": message,
        })

    async def _run_discovery():
        try:
            await queue.put({
                "type": "start",
                "site": site_display,
                "starting_id": cfg.known_max_id or starting_id or cfg.min_id,
                "max_probes": cfg.max_probes,
                "max_id_ceiling": cfg.max_id_ceiling,
            })
            result = await discover_user_id_boundaries(
                site=site_display,
                backend_manager=backend_manager,
                config=cfg,
                on_probe=_on_probe,
                on_phase=_on_phase,
            )
            await queue.put({
                "type": "complete",
                "result": {
                    "site": result.site,
                    "discovered_start_id": result.discovered_start_id,
                    "discovered_end_id": result.discovered_end_id,
                    "highest_observed_id": result.highest_observed_id,
                    "lowest_observed_id": result.lowest_observed_id,
                    "estimated_total_users": result.estimated_total_users,
                    "population_confidence_interval": list(result.population_confidence_interval),
                    "estimated_density": result.estimated_density,
                    "density_margin_of_error": result.density_margin_of_error,
                    "confidence_level": result.confidence_level,
                    "highest_id_confidence_interval": list(result.highest_id_confidence_interval),
                    "total_probes_used": result.total_probes_used,
                    "valid_probes_count": result.valid_probes_count,
                    "warnings": result.warnings,
                    "observations": [
                        {
                            "user_id": o.user_id,
                            "is_valid": o.is_valid,
                            "phase": o.phase,
                        }
                        for o in result.observations
                    ],
                },
            })
        except Exception as exc:
            logger.exception("Boundary discovery streaming failed")
            await queue.put({"type": "error", "message": str(exc)})
        finally:
            await queue.put(None)

    discovery_task = asyncio.create_task(_run_discovery())

    async def event_generator():
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            if not discovery_task.done():
                discovery_task.cancel()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
