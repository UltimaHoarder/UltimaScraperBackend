"""Canonical profile-task construction shared by MPS admission and retries."""

from __future__ import annotations

import hashlib
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ultima_scraper_api import ContentType

from ..database.mps_manifest import mps_task_id
from .job_builder import build_task
from .models import Task, TaskType

# Conclusive invalid limit: testing 3 distinct geographic regions
# resolves over 99.5% of creator geoblocks while cutting latency by 40%.
MPS_MAX_ROUTE_HOPS = 3
MPS_MAX_PROXY_ROUTES_PER_PROFILE = 3
# Reserves replace a slot before lookup; they never add hops or retry routes.
MPS_MAX_PROXY_ALTERNATIVES_PER_ROUTE = 3


def mps_proxy_connection_limit(configured: object) -> int:
    """MPS keeps an eight-slot budget when network connections are unlimited."""
    return configured if isinstance(configured, int) and configured > 0 else 8

_CACHED_GEO_MAP: dict[str, str] = {}
_CACHED_GEO_MAP_AT: float = 0.0
_GEO_MAP_CACHE_TTL: float = 60.0


def get_cached_proxy_geo_map(*, db_session=None) -> dict[str, str]:
    """Return an in-memory cached mapping of proxy_url -> uppercase country_code."""
    global _CACHED_GEO_MAP, _CACHED_GEO_MAP_AT
    now = time.monotonic()
    if _CACHED_GEO_MAP_AT and (now - _CACHED_GEO_MAP_AT) < _GEO_MAP_CACHE_TTL:
        return _CACHED_GEO_MAP
    try:
        from ..helpers.ip_geolocation import get_geolocation_service
        from ..usapi_initializer import resolve_proxy_url

        service = get_geolocation_service()
        geos = (
            service.get_all_cached_geolocations(db_session=db_session)
            if db_session is not None
            else service.get_all_cached_geolocations()
        )
        geo_map: dict[str, str] = {}
        for item in geos:
            p_url = item.get("proxy_url")
            cc = item.get("country_code")
            if p_url and cc:
                p_clean = str(p_url).strip()
                cc_clean = str(cc).strip().upper()
                geo_map[p_clean] = cc_clean
                geo_map[p_clean.rstrip("/")] = cc_clean
        # Geolocation records use configured identifiers, while frozen MPS
        # pools contain resolved URLs (including separately stored credentials).
        # Preserve exact cached entries when an alias resolves to an existing key.
        # Parse the config once per rebuild; each identifier resolves once.
        from ..config import UltimaScraperCollectionConfig

        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        resolved_ids: dict[str, str | None] = {}
        for proxy_id, country in list(geo_map.items()):
            if proxy_id not in resolved_ids:
                resolved_ids[proxy_id] = resolve_proxy_url(proxy_id, config=config)
            resolved = resolved_ids[proxy_id]
            if resolved:
                geo_map.setdefault(resolved, country)
                geo_map.setdefault(resolved.rstrip("/"), country)
        _CACHED_GEO_MAP = geo_map
        _CACHED_GEO_MAP_AT = now
        return _CACHED_GEO_MAP
    except Exception:
        if db_session is not None:
            raise
        return _CACHED_GEO_MAP


def _group_mps_proxy_countries(
    proxy_pool: Sequence[str],
    geo_map: Mapping[str, str],
) -> dict[str, list[str]]:
    countries: dict[str, list[str]] = {}
    seen: set[str] = set()
    for url in proxy_pool:
        country = geo_map.get(url) or geo_map.get(url.rstrip("/"))
        if country and url not in seen:
            countries.setdefault(country, []).append(url)
            seen.add(url)
    return countries


def _weighted_country_counts(
    proxy_pool: Sequence[str], geo_map: Mapping[str, str]
) -> Counter[str]:
    return Counter(
        country for url in proxy_pool
        if (country := geo_map.get(url) or geo_map.get(url.rstrip("/")))
    )


def _mps_route_candidates(route_offset: int, slot: int, size: int):
    """Restart-stable weighted draws, followed by a bounded complete fallback."""
    start = 0
    for draw in range(16):
        digest = hashlib.blake2b(
            f"{route_offset}:{slot}:{draw}".encode(), digest_size=8
        ).digest()
        start = int.from_bytes(digest, "big") % size
        yield start
    for offset in range(size):
        yield (start + offset) % size


def _resolve_mps_task_routes(
    *,
    task_index: int,
    proxy_pool: Sequence[str],
    proxy_start_index: int,
    include_direct: bool = False,
    proxy_geo_map: Mapping[str, str] | None = None,
    max_route_hops: int = MPS_MAX_ROUTE_HOPS,
    proxy_country_groups: Mapping[str, Sequence[str]] | None = None,
    proxy_country_weights: Mapping[str, int] | None = None,
) -> tuple[str | None, list[str], int | None]:
    pool_size = len(proxy_pool)
    if pool_size == 0 or (include_direct and max_route_hops == 1):
        selected_proxy_urls: list[str] = []
    else:
        # Direct consumes one of the configured route slots.
        target_proxy_count = max_route_hops - 1 if include_direct else max_route_hops

        geo_map = (
            proxy_geo_map if proxy_geo_map is not None else get_cached_proxy_geo_map()
        )
        country_groups = (
            proxy_country_groups
            if proxy_country_groups is not None
            else _group_mps_proxy_countries(proxy_pool, geo_map)
        )
        route_offset = proxy_start_index + task_index

        # Pass 1: Select primary proxy deterministically from cyclic pool offset
        selected_proxy_urls = []
        seen_urls: set[str] = set()
        seen_countries: set[str] = set()

        primary_url = proxy_pool[route_offset % pool_size]
        selected_proxy_urls.append(primary_url)
        seen_urls.add(primary_url)
        primary_country = geo_map.get(primary_url) or geo_map.get(primary_url.rstrip("/"))
        if primary_country:
            seen_countries.add(primary_country)

        if target_proxy_count >= 3 and len(country_groups) >= target_proxy_count:
            weights = (
                proxy_country_weights if proxy_country_weights is not None
                else _weighted_country_counts(proxy_pool, geo_map)
            )
            known_weight = sum(weights.values())
            # A large country's weight can exceed one of the three required
            # regions. Include it once, then share the remaining slots across
            # the rest of the frozen weighted pool. Never repeat a country.
            dominant = sorted(
                (country for country, weight in weights.items()
                 if weight * target_proxy_count >= known_weight),
                key=lambda country: (-weights[country], country),
            )
            for required_country in [*dominant, None, None, None]:
                if len(selected_proxy_urls) >= target_proxy_count:
                    break
                if required_country in seen_countries:
                    continue
                for index in _mps_route_candidates(
                    route_offset, len(selected_proxy_urls), pool_size
                ):
                    url = proxy_pool[index]
                    country = geo_map.get(url) or geo_map.get(url.rstrip("/"))
                    if (url in seen_urls or not country or country in seen_countries
                        or (required_country is not None and country != required_country)):
                        continue
                    selected_proxy_urls.append(url)
                    seen_urls.add(url)
                    seen_countries.add(country)
                    break

        # Pass 2: Geo-diverse fallback selection
        # Discover countries cyclically, then rotate within each country. With
        # sparse geo metadata, choosing its first member concentrates nearly all
        # unknown-country primaries on the same known-country fallback.
        if geo_map and len(selected_proxy_urls) < target_proxy_count:
            for offset in range(1, pool_size):
                candidate_url = proxy_pool[
                    (proxy_start_index + task_index + offset) % pool_size
                ]
                if candidate_url in seen_urls:
                    continue
                candidate_country = geo_map.get(candidate_url) or geo_map.get(
                    candidate_url.rstrip("/")
                )
                if candidate_country and candidate_country not in seen_countries:
                    country_urls = country_groups[candidate_country]
                    fallback_url = country_urls[route_offset % len(country_urls)]
                    selected_proxy_urls.append(fallback_url)
                    seen_urls.add(fallback_url)
                    seen_countries.add(candidate_country)
                    if len(selected_proxy_urls) >= target_proxy_count:
                        break
        # Pass 3: Residual fill pass
        # If fewer than target_proxy_count proxies selected (e.g. fewer distinct
        # countries exist in pool, or proxies lack geo data), fill remaining slots
        if len(selected_proxy_urls) < target_proxy_count:
            for offset in range(1, pool_size):
                candidate_url = proxy_pool[
                    (proxy_start_index + task_index + offset) % pool_size
                ]
                if candidate_url not in seen_urls:
                    selected_proxy_urls.append(candidate_url)
                    seen_urls.add(candidate_url)
                    if len(selected_proxy_urls) >= target_proxy_count:
                        break

    if not selected_proxy_urls:
        direct_route_index = 0
    elif include_direct:
        direct_route_index = (proxy_start_index + task_index) % (
            len(selected_proxy_urls) + 1
        )
    else:
        direct_route_index = None

    return (
        selected_proxy_urls[0] if selected_proxy_urls else None,
        selected_proxy_urls[1:],
        direct_route_index,
    )


def _build_mps_route_alternatives(
    *,
    selected_proxy_urls: Sequence[str],
    route_offset: int,
    proxy_geo_map: Mapping[str, str],
    proxy_country_groups: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    """Bounded same-country substitutes from the job's frozen proxy pool."""
    assigned = set(selected_proxy_urls)
    alternatives: dict[str, list[str]] = {}
    for url in selected_proxy_urls:
        country = proxy_geo_map.get(url) or proxy_geo_map.get(url.rstrip("/"))
        country_urls = proxy_country_groups.get(country, ()) if country else ()
        reserves: list[str] = []
        for offset in range(len(country_urls)):
            reserve = country_urls[(route_offset + offset) % len(country_urls)]
            if reserve not in assigned:
                reserves.append(reserve)
                if len(reserves) >= MPS_MAX_PROXY_ALTERNATIVES_PER_ROUTE:
                    break
        if reserves:
            alternatives[url] = reserves
    return alternatives


def _build_mps_task_batch(
    *,
    job_id: str,
    site_display: str,
    target_user_ids: Sequence[int],
    proxy_pool: Sequence[str],
    candidate_offset: int,
    include_direct: bool = False,
    proxy_geo_map: Mapping[str, str] | None = None,
    max_route_hops: int = MPS_MAX_ROUTE_HOPS,
    conclusive_miss_threshold: int | None = None,
) -> list[Task]:
    """Build a bounded, restart-idempotent batch of native MPS tasks."""

    site_name = cast(Any, site_display)
    tasks: list[Task] = []
    if proxy_geo_map is None:
        proxy_geo_map = get_cached_proxy_geo_map()
    country_groups = _group_mps_proxy_countries(proxy_pool, proxy_geo_map)
    country_weights = _weighted_country_counts(proxy_pool, proxy_geo_map)

    for batch_index, target_user_id in enumerate(target_user_ids):
        absolute_index = candidate_offset + batch_index
        guest_proxy_url, fallback_urls, direct_route_index = _resolve_mps_task_routes(
            task_index=absolute_index,
            proxy_pool=proxy_pool,
            proxy_start_index=0,
            include_direct=include_direct,
            proxy_geo_map=proxy_geo_map,
            max_route_hops=max_route_hops,
            proxy_country_groups=country_groups,
            proxy_country_weights=country_weights,
        )
        task = build_task(
            TaskType.SCRAPE,
            site_name=site_name,
            user_id=target_user_id,
            user_username=str(target_user_id),
            content_type=ContentType.PROFILE,
            download_media=False,
            max_items=1,
            use_guest_auth=True,
            guest_proxy_url=guest_proxy_url,
            guest_fallback_proxy_urls=fallback_urls,
            guest_proxy_alternatives=_build_mps_route_alternatives(
                selected_proxy_urls=(
                    ([guest_proxy_url] if guest_proxy_url else []) + fallback_urls
                ),
                route_offset=absolute_index,
                proxy_geo_map=proxy_geo_map,
                proxy_country_groups=country_groups,
            ),
            guest_direct_route_index=direct_route_index,
            track_invalid_user=True,
            conclusive_miss_threshold=conclusive_miss_threshold,
            max_route_hops=max_route_hops,
        )
        task.task_id = mps_task_id(job_id, absolute_index, target_user_id)
        task.job_id = job_id
        tasks.append(task)
    return tasks
