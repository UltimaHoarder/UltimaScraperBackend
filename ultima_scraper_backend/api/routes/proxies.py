"""Proxies API endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import Counter
from typing import AsyncGenerator, Literal, TypedDict
from urllib.parse import urlparse

import aiohttp
from aiohttp_socks import ProxyConnector
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from ultima_scraper_api.config import ProxyGroupSettings
from sqlmodel import select
from ultima_scraper_db_archive import UserAuthModel

from ...config import Proxy, UltimaScraperCollectionConfig
from ...config_settings_sync import (
    ConfigVersionConflict,
    ConfigVersionRequired,
    persist_config_snapshot,
)
from ...database.factory import MultiSiteDatabaseManager
from ...helpers.ip_geolocation import (
    GeolocationData,
    get_geolocation_service,
    get_ip_geolocation,
)
from ...helpers.proxy_url import resolve_proxy_target
from ...manager import BackendManager
from ...security import validate_outbound_url
from ...services.plugin_proxies import guard_proxy_assignment
from ..dependencies import get_backend_manager, get_config
from ._auth_event_utils import publish_auth_updated

_logger = logging.getLogger(__name__)

router = APIRouter(prefix="/proxies", tags=["proxies"])

_DEFAULT_PROXY_TARGET_DOMAINS = [
    "onlyfans.com",
    "fansly.com",
    "ipwho.is",
    "checkip.amazonaws.com",
    "ipv4.download.thinkbroadband.com",
]
_MAX_PROXY_ANALYZE_TIMEOUT_SECONDS = 30.0
_MAX_PROXY_CONCURRENCY = 64
_MAX_PROXY_STRESS_REQUESTS = 512


def _mask_proxy_url(raw_proxy_url: object) -> str | None:
    if not isinstance(raw_proxy_url, str):
        return None
    normalized = raw_proxy_url.strip()
    if not normalized:
        return None
    try:
        parsed = urlparse(normalized)
    except Exception:
        return None
    if not parsed.scheme or not parsed.hostname:
        return None
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _persist_proxy_config(config: UltimaScraperCollectionConfig) -> None:
    """Persist proxy edits to canonical settings in PostgreSQL."""
    if not getattr(config, "is_configured", False) or getattr(config, "_settings_version", None) is None:
        return

    try:
        persist_config_snapshot(
            config,
            expected_version=config._settings_version,
        )
    except ConfigVersionRequired as exc:
        raise HTTPException(
            status_code=409,
            detail="Proxy configuration changed; reload the network page and retry.",
        ) from exc
    except ConfigVersionConflict as exc:
        raise HTTPException(
            status_code=409,
            detail="Proxy configuration changed; reload the network page and retry.",
        ) from exc


# -----------------------------------------------------------------------------
# Helper: Fetch real OnlyFans CDN video URLs via guest login
# -----------------------------------------------------------------------------
async def _fetch_onlyfans_cdn_urls(backend_manager: BackendManager) -> list[str]:
    """
    Login as guest to OnlyFans API and fetch all CDN video URLs from public posts.

    Returns:
        A list of CDN video URLs (720p mp4 preferred), empty list if none found.
    """
    try:
        video_urls: list[str] = []

        # Fetch two pages of public login posts through an anonymous worker session.
        for page in range(2):
            offset = page * 10
            link = f"https://onlyfans.com/api2/v2/posts/login?limit=10&offset={offset}&skip_users=all"
            response = await backend_manager.execute_worker_api_method(
                site="onlyfans",
                account_id=None,
                target={"kind": "requester"},
                method="json_request",
                args=(link,),
                auth_override={"id": 0},
                guest=True,
                timeout_seconds=30.0,
            )
            if not isinstance(response, dict):
                continue

            for item in response.get("items", []):
                for media in item.get("media", []):
                    if media.get("type") != "video":
                        continue
                    files = media.get("files", {})
                    video_url = files.get("full", {}).get("url")
                    if video_url:
                        video_urls.append(video_url)

        if video_urls:
            _logger.info(
                "[CDN Fetch] Found %s video URLs for performance analysis pool",
                len(video_urls),
            )
        else:
            _logger.warning("[CDN Fetch] No video URLs found in login posts")
        return video_urls
    except Exception as exc:
        _logger.error("[CDN Fetch] Error fetching CDN URLs: %s", exc)
        return []


class ProxyResponse(BaseModel):
    """Response model for proxy data."""

    id: str  # Use URL as unique ID
    url: str
    username: str | None = None
    enabled: bool = True
    max_connections: int = 8
    group: str | None = None
    assigned_auths: list[int] = []  # List of user_auth IDs using this proxy
    active_assigned_count: int = 0  # Subset of assigned_auths that are active
    geolocation: GeolocationData | None = None  # Cached geolocation from DB
    guest_lookup_timeout: float | None = None


class ProxyAssignmentRequest(BaseModel):
    """Request model for assigning proxy to auth."""

    auth_id: int
    proxy_url: str | None = None  # None to remove assignment


class ProxyAuthRecord(TypedDict):
    id: int
    site: str
    username: str | None
    status: str | None
    active: bool


class ProxyCreateRequest(BaseModel):
    """Request model for creating a proxy."""

    url: str
    username: str | None = None
    password: str | None = None
    enabled: bool = True
    max_connections: int = -1
    group: str | None = None
    guest_lookup_timeout: float | None = None


class ProxyTestRequest(BaseModel):
    """Request model for testing a proxy connection."""

    url: str
    username: str | None = None
    password: str | None = None
    timeout_seconds: float = 5.0


class ProxyTestResponse(BaseModel):
    """Response model for proxy test results."""

    success: bool
    message: str
    latency_ms: int | None = None


class ProxyBulkTestItem(BaseModel):
    """Single proxy item for bulk reachability testing."""

    url: str
    username: str | None = None
    password: str | None = None


class ProxyBulkTestRequest(BaseModel):
    """Request model for bulk testing multiple proxies."""

    proxies: list[ProxyBulkTestItem]
    timeout_seconds: float = 5.0
    concurrency: int = 16


class ProxyBulkTestResult(BaseModel):
    """Result for testing a single proxy in a bulk test run."""

    url: str
    success: bool
    message: str
    latency_ms: int | None = None


class ProxyBulkTestResponse(BaseModel):
    """Response model containing results of bulk testing."""

    results: list[ProxyBulkTestResult]


class ProxyBulkCreateItem(BaseModel):
    """Single proxy to create or update in bulk."""

    url: str
    username: str | None = None
    password: str | None = None
    enabled: bool = True
    max_connections: int = -1
    group: str | None = None


class ProxyBulkCreateRequest(BaseModel):
    """Request model for bulk importing proxies."""

    proxies: list[ProxyBulkCreateItem] = []
    raw_text: str | None = None
    default_group: str | None = None
    overwrite: bool = True


class ProxyBulkCreateResponse(BaseModel):
    """Response model for bulk proxy creation."""

    status: str
    added: list[ProxyResponse]
    updated: list[ProxyResponse]
    skipped: list[str]
    errors: list[str]


class ProxyBulkSyncRequest(BaseModel):
    """Request model for bulk editing / synchronizing proxies."""

    proxies: list[ProxyBulkCreateItem] = []
    raw_text: str | None = None
    default_group: str | None = None
    target_group: str | None = None


class ProxyBulkSyncResponse(BaseModel):
    """Response model for bulk editing / synchronizing proxies."""

    status: str
    added: list[ProxyResponse] = []
    kept: list[ProxyResponse] = []
    deleted: list[str] = []
    errors: list[str] = []
    message: str = ""
    target_group: str | None = None


class ProxyBulkGroupRequest(BaseModel):
    """Request model for reassigning proxies to a group."""

    proxy_urls: list[str]
    group: str | None = None


class ProxyBulkGroupResponse(BaseModel):
    """Response model for assigning proxies to a group."""

    status: str
    message: str
    updated_count: int
    group: str | None = None


class GroupSettingsUpdateRequest(BaseModel):
    """Request model for updating capacity/concurrency across all proxies in a group."""

    max_connections: int | None = None
    max_concurrency: int | None = None
    enabled: bool | None = None
    # Stored on the group, so proxies added to it later inherit it; a proxy's
    # own guest_lookup_timeout still wins. Send null to clear it.
    guest_lookup_timeout: float | None = Field(default=None, ge=0.5)


class GroupSettingsUpdateResponse(BaseModel):
    """Response model for group-wide settings updates."""

    status: str
    message: str
    updated_count: int
    group: str
    max_connections: int | None = None
    max_concurrency: int | None = None
    enabled: bool | None = None
    guest_lookup_timeout: float | None = None


class GroupSettingsResponse(BaseModel):
    """A proxy group's own settings."""

    group: str
    guest_lookup_timeout: float | None = None


class BulkProxyStateRequest(BaseModel):
    """Request model for enabling/disabling multiple proxies."""

    proxy_urls: list[str]
    enabled: bool


class BulkProxyStateResponse(BaseModel):
    """Response model for bulk enabling/disabling proxies."""

    status: str
    message: str
    updated_count: int
    enabled: bool


class BulkProxyDeleteRequest(BaseModel):
    """Request model for deleting multiple proxies."""

    proxy_urls: list[str]


class BulkProxyDeleteResponse(BaseModel):
    """Response model for deleting multiple proxies."""

    status: str
    message: str
    deleted_count: int


class BulkProxyEditRequest(BaseModel):
    """Request model for bulk-editing settings across multiple proxies."""

    proxy_urls: list[str]
    max_connections: int | None = None
    max_concurrency: int | None = None
    guest_lookup_timeout: float | None = None


class BulkProxyEditResponse(BaseModel):
    """Response model for bulk proxy edits."""

    status: str
    message: str
    updated_count: int
    max_connections: int | None = None
    max_concurrency: int | None = None
    guest_lookup_timeout: float | None = None


class ProxyAnalyzerRequest(BaseModel):
    """Request model for proxy performance analysis."""

    concurrency_levels: list[int] = [1, 2, 4, 8, 16]
    requests_per_level: int = 10
    timeout_seconds: float = 10.0
    head_url: str = "https://ipwho.is"
    # Fallback download URL when live CDN is disabled
    download_url: str = "http://ipv4.download.thinkbroadband.com/5MB.zip"
    download_bytes: int = 1024 * 1024  # 1MB partial download (Range header)
    mode: Literal["head", "range", "mixed"] = "mixed"
    auto_ramp: bool = False
    max_error_rate: float = 0.1
    max_p95_multiplier: float = 2.0
    # Advanced options
    warmup_requests: int = 5
    verify_ip: bool = True
    track_ttfb: bool = True
    # Connection pool settings (emulates download manager behavior)
    max_connections: int = 64  # Base max connection pool size
    test_connection_scaling: bool = (
        True  # Test multiple connection pool sizes per concurrency
    )
    connection_levels: list[int] = [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
    ]  # Fixed connection pool sizes (doubling pattern)
    adaptive_connections: bool = (
        True  # Use hill-climbing for connection levels (start at 8, test 16 and 4, follow best)
    )
    # Request count for fair saturation testing (should be >= max_concurrency * 4 for fair comparison)
    stress_requests: int = (
        256  # Enough to saturate x64 concurrency (256/64 = 4 requests per slot)
    )
    # Live CDN: Fetch a real OnlyFans CDN URL via guest login instead of using static URL
    use_live_cdn: bool = (
        True  # If True, fetch a live video URL from OnlyFans public posts
    )


class ProxyAnalyzerResult(BaseModel):
    concurrency: int
    max_connections: int  # Connection pool size used for this test
    requests: int
    successes: int
    errors: int
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    min_latency_ms: float
    jitter_ms: float  # Standard deviation of latencies
    avg_ttfb_ms: float  # Time to first byte
    throughput_mbps: float
    efficiency_score: float  # Throughput per second of latency
    status_counts: dict[str, int]
    head_status_counts: dict[str, int] | None = None
    download_status_counts: dict[str, int] | None = None
    proxy_ip: str | None = None
    ip_consistent: bool = True
    # Geolocation data
    geolocation: GeolocationData | None = None


class ProxyAnalyzerResponse(BaseModel):
    success: bool
    message: str
    results: list[ProxyAnalyzerResult]
    recommended_concurrency: int | None = None
    recommended_connections: int | None = None  # Optimal connection pool size
    best_configs: dict[int, dict[str, int | float]] = (
        {}
    )  # Per concurrency: {concurrency: {connections, efficiency}}
    warmup_ip: str | None = None
    warmup_latency_ms: float | None = None


def _build_proxy_url(proxy: Proxy) -> str:
    if proxy.username and proxy.password:
        parsed = urlparse(proxy.url)
        if parsed.username is None and parsed.password is None:
            userinfo = f"{proxy.username}:{proxy.password}@"
            netloc = (
                f"{userinfo}{parsed.hostname}:{parsed.port}"
                if parsed.port
                else f"{userinfo}{parsed.hostname}"
            )
            return parsed._replace(netloc=netloc).geturl()
    return proxy.url


def _get_proxy_scheme(proxy_url: str) -> str:
    return urlparse(proxy_url).scheme.lower()


def _resolve_allowed_target_domains(
    config: UltimaScraperCollectionConfig,
) -> list[str]:
    security_cfg = getattr(getattr(config, "settings", None), "security", None)
    configured = (
        list(getattr(security_cfg, "media_proxy_allowed_domains", []))
        if security_cfg is not None
        else []
    )
    return configured or _DEFAULT_PROXY_TARGET_DOMAINS


def _validate_proxy_analyzer_request(
    request: ProxyAnalyzerRequest,
    *,
    allowed_domains: list[str],
) -> None:
    if (
        request.timeout_seconds <= 0
        or request.timeout_seconds > _MAX_PROXY_ANALYZE_TIMEOUT_SECONDS
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                f"timeout_seconds must be between 0 and {_MAX_PROXY_ANALYZE_TIMEOUT_SECONDS}"
            ),
        )
    if (
        request.stress_requests <= 0
        or request.stress_requests > _MAX_PROXY_STRESS_REQUESTS
    ):
        raise HTTPException(
            status_code=400,
            detail=f"stress_requests must be between 1 and {_MAX_PROXY_STRESS_REQUESTS}",
        )
    for concurrency in request.concurrency_levels:
        if concurrency <= 0 or concurrency > _MAX_PROXY_CONCURRENCY:
            raise HTTPException(
                status_code=400,
                detail=f"concurrency_levels values must be between 1 and {_MAX_PROXY_CONCURRENCY}",
            )
    if request.max_connections <= 0 or request.max_connections > _MAX_PROXY_CONCURRENCY:
        raise HTTPException(
            status_code=400,
            detail=f"max_connections must be between 1 and {_MAX_PROXY_CONCURRENCY}",
        )
    for level in request.connection_levels:
        if level <= 0 or level > _MAX_PROXY_CONCURRENCY:
            raise HTTPException(
                status_code=400,
                detail=f"connection_levels values must be between 1 and {_MAX_PROXY_CONCURRENCY}",
            )
    validate_outbound_url(
        request.head_url,
        allowed_domains=allowed_domains,
        allow_private=False,
    )
    validate_outbound_url(
        request.download_url,
        allowed_domains=allowed_domains,
        allow_private=False,
    )


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int((len(sorted_values) - 1) * percentile)
    return sorted_values[index]


def _jitter(values: list[float]) -> float:
    """Calculate standard deviation (jitter) of latencies."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return variance**0.5


async def _run_stress_requests(
    *,
    session: aiohttp.ClientSession,
    proxy_url: str | None,
    method: Literal["HEAD", "GET"],
    url: str,
    total_requests: int,
    concurrency: int,
    timeout_seconds: float,
    headers: dict[str, str] | None = None,
    track_ttfb: bool = False,
    verify_ip: bool = False,
    url_pool: list[str] | None = None,  # Pool of URLs to cycle through
) -> tuple[list[float], list[float], int, Counter[str], int, list[str]]:
    """Run stress requests with advanced tracking.

    Emulates the download manager's behavior:
    - Semaphore is released AFTER connection acquisition
    - Data transfer happens outside the semaphore lock
    - This allows accurate measurement of concurrent download capacity

    Args:
        url_pool: Optional list of URLs to cycle through (round-robin).
                  If provided, each request picks from the pool.
                  If None, uses the single `url` parameter.

    Returns: (latencies, ttfb_list, bytes_read, status_counts, total_requests, ips_seen)
    """
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    ttfb_list: list[float] = []
    bytes_read = 0
    status_counts: Counter[str] = Counter()
    ips_seen: list[str] = []
    lock = asyncio.Lock()

    # Build URL list for round-robin assignment
    if url_pool:
        urls = url_pool
    else:
        urls = [url]

    async def _single_request(request_index: int) -> None:
        nonlocal bytes_read
        start = time.perf_counter()
        ttfb = 0.0

        # Round-robin URL selection
        request_url = urls[request_index % len(urls)]

        try:
            # Acquire connection under semaphore (emulates download_part behavior)
            async with semaphore:
                response = await session.request(
                    method,
                    request_url,
                    proxy=proxy_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout_seconds),
                )

            # Semaphore released - connection acquired
            # Data transfer happens OUTSIDE the semaphore (like optimized download_part)
            try:
                async with response:
                    # Time to first byte (connection established)
                    if track_ttfb:
                        ttfb = (time.perf_counter() - start) * 1000

                    async with lock:
                        status_counts[str(response.status)] += 1

                    if method == "GET":
                        data = await response.read()
                        async with lock:
                            bytes_read += len(data)
                            # IP verification (supports both JSON and plain text)
                            if verify_ip:
                                try:
                                    # Try JSON first (ipwho.is)
                                    import json

                                    response_data = json.loads(data.decode())
                                    if "ip" in response_data:
                                        ips_seen.append(response_data["ip"])
                                except Exception:
                                    # Fallback to plain text (checkip.amazonaws.com)
                                    try:
                                        ip = data.decode().strip()
                                        if ip and "." in ip:  # Basic IP validation
                                            ips_seen.append(ip)
                                    except Exception:
                                        pass
                    else:
                        await response.release()

                latency_ms = (time.perf_counter() - start) * 1000
                async with lock:
                    latencies.append(latency_ms)
                    if track_ttfb:
                        ttfb_list.append(ttfb)
            except Exception as exc:
                async with lock:
                    status_counts[f"error:{type(exc).__name__}"] += 1
                latency_ms = (time.perf_counter() - start) * 1000
                async with lock:
                    latencies.append(latency_ms)
        except Exception as exc:
            # Connection acquisition failed
            async with lock:
                status_counts[f"error:{type(exc).__name__}"] += 1
            latency_ms = (time.perf_counter() - start) * 1000
            async with lock:
                latencies.append(latency_ms)

    await asyncio.gather(*[_single_request(i) for i in range(total_requests)])
    return latencies, ttfb_list, bytes_read, status_counts, total_requests, ips_seen


async def _warmup_proxy(
    session: aiohttp.ClientSession,
    proxy_url: str | None,
    url: str,
    warmup_requests: int,
    timeout_seconds: float,
) -> tuple[str | None, float]:
    """Run warmup requests to establish connections and get proxy IP.

    For SOCKS proxies using ProxyConnector, proxy_url should be None since
    the connector handles the proxy. For HTTP proxies, pass the proxy URL.

    Returns: (proxy_ip, avg_latency_ms)
    """
    proxy_ip = None
    latencies: list[float] = []

    for _ in range(warmup_requests):
        start = time.perf_counter()
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            if proxy_url is not None:
                response_ctx = session.get(url, proxy=proxy_url, timeout=timeout)
            else:
                response_ctx = session.get(url, timeout=timeout)

            async with response_ctx as response:
                data = await response.read()
                latency_ms = (time.perf_counter() - start) * 1000
                latencies.append(latency_ms)

                # Try to extract IP from response
                if not proxy_ip:
                    try:
                        # Try JSON first (ipwho.is)
                        import json

                        response_data = json.loads(data.decode())
                        if "ip" in response_data:
                            proxy_ip = response_data["ip"]
                    except Exception:
                        # Fallback to plain text (checkip.amazonaws.com)
                        try:
                            ip = data.decode().strip()
                            if ip and "." in ip:  # Basic IP validation
                                proxy_ip = ip
                        except Exception:
                            pass
        except Exception:
            pass

    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    return proxy_ip, avg_latency


@router.get("/", response_model=list[ProxyResponse])
def list_proxies(
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> list[ProxyResponse]:
    """List all proxies from config with their assignments and cached geolocation."""
    proxies: list[ProxyResponse] = []
    geo_service = get_geolocation_service()

    # Get proxies from config
    if (
        hasattr(config.settings, "network")
        and config.settings.network
        and config.settings.network.proxies
    ):
        db_manager = MultiSiteDatabaseManager()

        # Batch-load all auth assignments in one query per site
        all_proxy_urls = [proxy.url for proxy in config.settings.network.proxies]
        auths_by_proxy: dict[str, list[int]] = {url: [] for url in all_proxy_urls}
        active_auths_by_proxy: dict[str, int] = {url: 0 for url in all_proxy_urls}

        for site_name in ["onlyfans", "fansly"]:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue
            try:
                with site_db.get_session() as session:
                    auths = session.exec(
                        select(
                            UserAuthModel.id,
                            UserAuthModel.proxy_id,
                            UserAuthModel.active,
                        ).where(UserAuthModel.proxy_id.in_(all_proxy_urls))
                    ).all()
                    for auth_id, proxy_id, active in auths:
                        if proxy_id in auths_by_proxy:
                            auths_by_proxy[proxy_id].append(auth_id)
                            if active:
                                active_auths_by_proxy[proxy_id] += 1
            except Exception as e:
                _logger.warning(f"Error querying auths for site {site_name}: {e}")

        for proxy in config.settings.network.proxies:
            assigned_auths = auths_by_proxy.get(proxy.url, [])
            active_assigned_count = active_auths_by_proxy.get(proxy.url, 0)

            # Look up cached geolocation from DB
            cached_geo = geo_service.get_geolocation_by_proxy_url(proxy.url)

            proxies.append(
                ProxyResponse(
                    id=proxy.url,
                    url=proxy.url,
                    username=proxy.username,
                    enabled=getattr(proxy, "enabled", True),
                    max_connections=proxy.max_connections,
                    group=getattr(proxy, "group", None),
                    assigned_auths=assigned_auths,
                    active_assigned_count=active_assigned_count,
                    geolocation=cached_geo,
                    guest_lookup_timeout=getattr(proxy, "guest_lookup_timeout", None),
                )
            )

    return proxies


@router.get("/geolocations")
async def list_proxy_geolocations() -> list[GeolocationData]:
    """Return all cached proxy geolocation records from the database."""
    service = get_geolocation_service()
    return service.get_all_cached_geolocations()


@router.post("/{proxy_url:path}/geolocation")
async def fetch_proxy_geolocation(
    proxy_url: str,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> GeolocationData:
    """Resolve the proxy's public IP and fetch its geolocation.

    Makes a single request through the proxy to ipwho.is to discover
    the public IP, then fetches + caches geolocation data in the DB.
    """
    proxies = config.settings.network.proxies or []
    proxy = next((p for p in proxies if p.url == proxy_url), None)
    if proxy is None and proxy_url != "direct":
        raise HTTPException(status_code=404, detail="Proxy not found")

    timeout = aiohttp.ClientTimeout(total=10.0)
    url = "https://ipwho.is"

    try:
        if proxy_url == "direct":
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    data = await response.json()
                    public_ip: str = data.get("ip", "")
        else:
            resolved_proxy_url = _build_proxy_url(proxy)  # type: ignore[arg-type]
            scheme = _get_proxy_scheme(resolved_proxy_url)

            if scheme.startswith("socks"):
                connector = ProxyConnector.from_url(resolved_proxy_url, limit=4)
                async with aiohttp.ClientSession(
                    connector=connector, timeout=timeout
                ) as session:
                    async with session.get(url) as response:
                        data = await response.json()
                        public_ip = data.get("ip", "")
            else:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, proxy=resolved_proxy_url) as response:
                        data = await response.json()
                        public_ip = data.get("ip", "")

        if not public_ip:
            raise HTTPException(status_code=502, detail="Could not determine proxy IP")

        geo = await get_ip_geolocation(public_ip, proxy_url=proxy_url)
        if geo is None:
            raise HTTPException(
                status_code=502,
                detail=f"Geolocation lookup failed for {public_ip}",
            )
        return geo

    except HTTPException:
        raise
    except Exception as exc:
        _logger.error(f"[Geolocation] Error fetching for proxy {proxy_url}: {exc}")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/assign", dependencies=[Depends(guard_proxy_assignment)])
async def assign_proxy_to_auth(
    request: ProxyAssignmentRequest,
    backend_manager: BackendManager = Depends(get_backend_manager),
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> dict[str, str]:
    """Assign or remove proxy from an auth account."""
    if request.proxy_url is not None:
        configured_proxy = next(
            (
                proxy
                for proxy in (config.settings.network.proxies or [])
                if proxy.url == request.proxy_url
            ),
            None,
        )
        if configured_proxy is not None and not getattr(
            configured_proxy, "enabled", True
        ):
            raise HTTPException(status_code=409, detail="Proxy is disabled")

    db_manager = MultiSiteDatabaseManager()

    # Try to find and update the auth across all sites
    for site_name in ["onlyfans", "fansly"]:
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            continue

        try:
            with site_db.get_session() as session:
                auth = session.exec(
                    select(UserAuthModel).where(UserAuthModel.id == request.auth_id)
                ).first()

                if auth:
                    auth.proxy_id = request.proxy_url
                    session.add(auth)
                    session.commit()
                    await publish_auth_updated(
                        backend_manager,
                        auth.user_id,
                        username=auth.username,
                        site=auth.site,
                    )
                    return {"status": "success", "message": "Proxy assignment updated"}

        except Exception as e:
            _logger.error(f"Error assigning proxy: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e

    raise HTTPException(status_code=404, detail="Auth not found")


@router.get("/{proxy_url:path}/auths")
def get_proxy_auths(proxy_url: str) -> list[ProxyAuthRecord]:
    """Get all auths assigned to a specific proxy."""
    db_manager = MultiSiteDatabaseManager()
    auths: list[ProxyAuthRecord] = []

    for site_name in ["onlyfans", "fansly"]:
        site_db = db_manager.get_or_create_manager(site_name)
        if not site_db:
            continue

        try:
            with site_db.get_session() as session:
                auth_list = session.exec(
                    select(UserAuthModel).where(UserAuthModel.proxy_id == proxy_url)
                ).all()

                for auth in auth_list:
                    auths.append(
                        {
                            "id": auth.id,
                            "site": auth.site,
                            "username": auth.username,
                            "status": auth.status,
                            "active": auth.active,
                        }
                    )

        except Exception as e:
            _logger.warning(f"Error querying auths for proxy {proxy_url}: {e}")

    return auths


class MediaPoolResponse(BaseModel):
    """Response model for media pool URLs."""

    success: bool
    urls: list[str]
    message: str | None = None


@router.get("/media-pool")
async def get_media_pool(
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> MediaPoolResponse:
    """Fetch OnlyFans CDN video URLs for performance analysis (via guest login)."""
    try:
        urls = await _fetch_onlyfans_cdn_urls(backend_manager)
        if urls:
            return MediaPoolResponse(
                success=True,
                urls=urls,
                message=f"Found {len(urls)} video URLs",
            )
        else:
            return MediaPoolResponse(
                success=False,
                urls=[],
                message="No video URLs found in public posts",
            )
    except Exception as exc:
        _logger.error(f"Error fetching media pool: {exc}")
        return MediaPoolResponse(
            success=False,
            urls=[],
            message=f"Error: {exc}",
        )


async def _check_proxy_tcp(
    url: str,
    timeout_seconds: float = 5.0,
) -> tuple[bool, str, int | None]:
    """Test proxy reachability via non-blocking TCP connection."""
    clean_url = url.strip()
    if not clean_url.startswith(
        ("http://", "https://", "socks4://", "socks4a://", "socks5://", "socks5h://")
    ):
        clean_url = f"http://{clean_url}"

    try:
        host, port = resolve_proxy_target(clean_url)
    except ValueError as exc:
        return False, str(exc), None

    start = time.perf_counter()
    try:
        conn = asyncio.open_connection(host, port)
        reader, writer = await asyncio.wait_for(conn, timeout=timeout_seconds)
        latency_ms = int((time.perf_counter() - start) * 1000)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, "TCP connection succeeded", latency_ms
    except asyncio.TimeoutError:
        return False, "Connection timed out", None
    except OSError as exc:
        return False, str(exc) or "Connection failed", None
    except Exception as exc:
        return False, str(exc) or "Connection failed", None


@router.post("/test", response_model=ProxyTestResponse)
async def test_proxy(request: ProxyTestRequest) -> ProxyTestResponse:
    """Test proxy reachability via non-blocking TCP connection."""
    success, message, latency_ms = await _check_proxy_tcp(
        request.url,
        timeout_seconds=request.timeout_seconds,
    )
    return ProxyTestResponse(
        success=success,
        message=message,
        latency_ms=latency_ms,
    )


@router.post("/bulk-test", response_model=ProxyBulkTestResponse)
async def bulk_test_proxies(request: ProxyBulkTestRequest) -> ProxyBulkTestResponse:
    """Test multiple proxies concurrently via non-blocking TCP connections."""
    concurrency_limit = max(1, min(request.concurrency, 32))
    sem = asyncio.Semaphore(concurrency_limit)

    async def _test_one(item: ProxyBulkTestItem) -> ProxyBulkTestResult:
        async with sem:
            success, message, latency_ms = await _check_proxy_tcp(
                item.url,
                timeout_seconds=request.timeout_seconds,
            )
            return ProxyBulkTestResult(
                url=item.url,
                success=success,
                message=message,
                latency_ms=latency_ms,
            )

    results = await asyncio.gather(*[_test_one(item) for item in request.proxies])
    return ProxyBulkTestResponse(results=list(results))


@router.post("/{proxy_url:path}/analyze", response_model=ProxyAnalyzerResponse)
async def analyze_proxy(
    proxy_url: str,
    request: ProxyAnalyzerRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> ProxyAnalyzerResponse:
    _validate_proxy_analyzer_request(
        request,
        allowed_domains=_resolve_allowed_target_domains(config),
    )

    # Fetch live CDN URLs if enabled
    cdn_url_pool: list[str] = []
    if request.use_live_cdn:
        cdn_url_pool = await _fetch_onlyfans_cdn_urls(backend_manager)
        if cdn_url_pool:
            # Keep using Range request (5MB) for realistic download testing
            request.download_bytes = 5 * 1024 * 1024  # 5MB partial download
            _logger.info(
                f"[Performance Analysis] Using {len(cdn_url_pool)} live CDN URLs for pool"
            )
        else:
            _logger.warning(
                "[Performance Analysis] Failed to fetch live CDN URLs, using default"
            )

    proxies = config.settings.network.proxies or []
    proxy = next((p for p in proxies if p.url == proxy_url), None)
    if proxy is None:
        raise HTTPException(status_code=404, detail="Proxy not found")

    resolved_proxy_url = _build_proxy_url(proxy)
    scheme = _get_proxy_scheme(resolved_proxy_url)
    timeout = aiohttp.ClientTimeout(total=request.timeout_seconds)

    results: list[ProxyAnalyzerResult] = []
    best_configs: dict[int, dict[str, int | float]] = (
        {}
    )  # {concurrency: {connections, efficiency}}
    baseline_p95: float | None = None
    recommended_concurrency: int | None = None
    recommended_connections: int | None = None
    warmup_ip: str | None = None
    warmup_latency: float | None = None
    best_overall_efficiency = 0.0

    # Phase 1: Warmup (use default connection limit)
    if request.warmup_requests > 0:
        if scheme.startswith("socks"):
            connector = ProxyConnector.from_url(
                resolved_proxy_url, limit=request.max_connections
            )
            session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            proxy_arg = None
        else:
            connector = aiohttp.TCPConnector(limit=request.max_connections)
            session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            proxy_arg = resolved_proxy_url

        try:
            warmup_ip, warmup_latency = await _warmup_proxy(
                session=session,
                proxy_url=proxy_arg,  # None for SOCKS, URL for HTTP
                url=request.head_url,
                warmup_requests=request.warmup_requests,
                timeout_seconds=request.timeout_seconds,
            )
        finally:
            await session.close()

    # Phase 2: Test each concurrency level with multiple connection pool sizes
    for concurrency in request.concurrency_levels:
        if concurrency <= 0:
            continue

        # Determine connection levels to test for this concurrency
        if request.test_connection_scaling:
            # Test all connection levels (1, 2, 4, 8, 16, 32)
            connection_levels_to_test = request.connection_levels
        else:
            # Just test with configured max_connections
            connection_levels_to_test = [request.max_connections]

        best_efficiency_for_concurrency = 0.0
        best_connections_for_concurrency = request.max_connections

        for max_connections in connection_levels_to_test:
            # Create session with specific connection limit
            if scheme.startswith("socks"):
                connector = ProxyConnector.from_url(
                    resolved_proxy_url, limit=max_connections
                )
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                proxy_arg = None
            else:
                connector = aiohttp.TCPConnector(limit=max_connections)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                proxy_arg = resolved_proxy_url

            try:
                # Configurable request count for fair saturation testing
                head_count = request.stress_requests
                range_count = request.stress_requests
                if request.mode == "head":
                    range_count = 0
                elif request.mode == "range":
                    head_count = 0

                latencies: list[float] = []
                ttfb_list: list[float] = []
                bytes_read = 0
                status_counts: Counter[str] = Counter()
                head_status: Counter[str] = Counter()
                range_status: Counter[str] = Counter()
                ips_seen: list[str] = []

                if head_count > 0:
                    head_latencies, head_ttfb, head_bytes, head_status, _, head_ips = (
                        await _run_stress_requests(
                            session=session,
                            proxy_url=proxy_arg,
                            method="GET" if request.verify_ip else "HEAD",
                            url=request.head_url,
                            total_requests=head_count,
                            concurrency=concurrency,
                            timeout_seconds=request.timeout_seconds,
                            track_ttfb=request.track_ttfb,
                            verify_ip=request.verify_ip,
                        )
                    )
                    latencies.extend(head_latencies)
                    ttfb_list.extend(head_ttfb)
                    bytes_read += head_bytes
                    status_counts.update(head_status)
                    ips_seen.extend(head_ips)

                if range_count > 0:
                    # Only use Range header if download_bytes > 0
                    headers = {}
                    if request.download_bytes > 0:
                        end_byte = request.download_bytes - 1
                        headers = {"Range": f"bytes=0-{end_byte}"}
                    range_latencies, range_ttfb, range_bytes, range_status, _, _ = (
                        await _run_stress_requests(
                            session=session,
                            proxy_url=proxy_arg,
                            method="GET",
                            url=request.download_url,
                            total_requests=range_count,
                            concurrency=concurrency,
                            timeout_seconds=request.timeout_seconds,
                            headers=headers if headers else None,
                            track_ttfb=request.track_ttfb,
                            url_pool=cdn_url_pool if cdn_url_pool else None,
                        )
                    )
                    latencies.extend(range_latencies)
                    ttfb_list.extend(range_ttfb)
                    bytes_read += range_bytes
                    status_counts.update(range_status)

                successes = sum(
                    count for key, count in status_counts.items() if key.isdigit()
                )
                errors = sum(
                    count
                    for key, count in status_counts.items()
                    if key.startswith("error:")
                )
                avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
                p50_latency = _percentile(latencies, 0.50)
                p95_latency = _percentile(latencies, 0.95)
                p99_latency = _percentile(latencies, 0.99)
                max_latency = max(latencies) if latencies else 0.0
                min_latency = min(latencies) if latencies else 0.0
                jitter = _jitter(latencies)
                avg_ttfb = sum(ttfb_list) / len(ttfb_list) if ttfb_list else 0.0

                total_seconds = sum(latencies) / 1000 if latencies else 0.0
                throughput_mbps = (
                    (bytes_read * 8) / (total_seconds * 1_000_000)
                    if total_seconds > 0
                    else 0.0
                )

                # Calculate efficiency: throughput per second of latency
                efficiency = (
                    throughput_mbps / (avg_latency / 1000) if avg_latency > 0 else 0.0
                )

                # Check IP consistency
                unique_ips = set(ips_seen)
                ip_consistent = len(unique_ips) <= 1
                proxy_ip_for_level = ips_seen[0] if ips_seen else warmup_ip

                # Fetch geolocation for the proxy IP
                geolocation_data: GeolocationData | None = None
                if proxy_ip_for_level:
                    try:
                        geolocation_data = await get_ip_geolocation(
                            proxy_ip_for_level, proxy_url=proxy_url
                        )
                    except Exception as exc:
                        _logger.warning(
                            f"[Performance Analysis] Failed to fetch geolocation for {proxy_ip_for_level}: {exc}"
                        )

                result = ProxyAnalyzerResult(
                    concurrency=concurrency,
                    max_connections=max_connections,
                    requests=head_count + range_count,
                    successes=successes,
                    errors=errors,
                    avg_latency_ms=round(avg_latency, 2),
                    p50_latency_ms=round(p50_latency, 2),
                    p95_latency_ms=round(p95_latency, 2),
                    p99_latency_ms=round(p99_latency, 2),
                    max_latency_ms=round(max_latency, 2),
                    min_latency_ms=round(min_latency, 2),
                    jitter_ms=round(jitter, 2),
                    avg_ttfb_ms=round(avg_ttfb, 2),
                    throughput_mbps=round(throughput_mbps, 2),
                    efficiency_score=round(efficiency, 4),
                    status_counts=dict(status_counts),
                    head_status_counts=dict(head_status) if head_count > 0 else None,
                    download_status_counts=(
                        dict(range_status) if range_count > 0 else None
                    ),
                    proxy_ip=proxy_ip_for_level,
                    ip_consistent=ip_consistent,
                    geolocation=geolocation_data,
                )
                results.append(result)

                # Track best configuration for this concurrency level
                success_rate = (
                    successes / (head_count + range_count)
                    if (head_count + range_count) > 0
                    else 0.0
                )
                if (
                    success_rate >= 0.95
                    and efficiency > best_efficiency_for_concurrency
                ):
                    best_efficiency_for_concurrency = efficiency
                    best_connections_for_concurrency = max_connections

                # Track best overall configuration
                if success_rate >= 0.95 and efficiency > best_overall_efficiency:
                    best_overall_efficiency = efficiency
                    recommended_concurrency = concurrency
                    recommended_connections = max_connections

                if baseline_p95 is None and p95_latency > 0:
                    baseline_p95 = p95_latency

                # Auto-ramp stopping conditions
                if request.auto_ramp and baseline_p95 is not None:
                    error_rate = (
                        errors / (head_count + range_count)
                        if (head_count + range_count) > 0
                        else 0.0
                    )
                    if error_rate > request.max_error_rate:
                        break
                    if p95_latency > baseline_p95 * request.max_p95_multiplier:
                        break

            finally:
                await session.close()

        # Store best configuration for this concurrency level
        if best_efficiency_for_concurrency > 0:
            best_configs[concurrency] = {
                "connections": best_connections_for_concurrency,
                "efficiency": round(best_efficiency_for_concurrency, 4),
            }

    return ProxyAnalyzerResponse(
        success=True,
        message="Performance analysis complete",
        results=results,
        recommended_concurrency=recommended_concurrency,
        recommended_connections=recommended_connections,
        best_configs=best_configs,
        warmup_ip=warmup_ip,
        warmup_latency_ms=round(warmup_latency, 2) if warmup_latency else None,
    )


@router.post("/{proxy_url:path}/analyze-stream")
async def analyze_proxy_stream(
    proxy_url: str,
    request: ProxyAnalyzerRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> StreamingResponse:
    """Stream performance analysis results as Server-Sent Events (SSE).

    Each connection test result is sent immediately when complete.
    """
    _validate_proxy_analyzer_request(
        request,
        allowed_domains=_resolve_allowed_target_domains(config),
    )

    # Fetch live CDN URLs if enabled (do this before streaming starts)
    cdn_url_pool: list[str] = []
    if request.use_live_cdn:
        cdn_url_pool = await _fetch_onlyfans_cdn_urls(backend_manager)
        if cdn_url_pool:
            # Keep using Range request (5MB) for realistic download testing
            request.download_bytes = 5 * 1024 * 1024  # 5MB partial download
            _logger.info(
                f"[Performance Analysis Stream] Using {len(cdn_url_pool)} live CDN URLs for pool"
            )
        else:
            _logger.warning(
                "[Performance Analysis Stream] Failed to fetch live CDN URLs, using default"
            )

    # Handle direct connection (no proxy)
    is_direct = proxy_url == "direct"
    proxy = None
    if not is_direct:
        proxies = config.settings.network.proxies or []
        proxy = next((p for p in proxies if p.url == proxy_url), None)
        if proxy is None:
            raise HTTPException(status_code=404, detail="Proxy not found")

    async def generate_results() -> AsyncGenerator[str, None]:
        current_proxy = proxy
        if is_direct:
            resolved_proxy_url: str | None = None
            scheme = ""
        else:
            if current_proxy is None:
                raise HTTPException(status_code=404, detail="Proxy not found")
            resolved_proxy_url = _build_proxy_url(current_proxy)
            scheme = _get_proxy_scheme(resolved_proxy_url)
        timeout = aiohttp.ClientTimeout(total=request.timeout_seconds)

        best_configs: dict[int, dict[str, int | float]] = {}
        baseline_p95: float | None = None
        recommended_concurrency: int | None = None
        recommended_connections: int | None = None
        warmup_ip: str | None = None
        warmup_latency: float | None = None
        best_overall_efficiency = 0.0
        cached_geolocation: GeolocationData | None = None
        geolocation_fetched = False

        # Phase 1: Warmup
        if request.warmup_requests > 0:
            if is_direct:
                # Direct connection - no proxy
                connector = aiohttp.TCPConnector(limit=request.max_connections)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                proxy_arg = None
            elif scheme.startswith("socks"):
                if resolved_proxy_url is None:
                    raise RuntimeError("SOCKS proxy URL not available")
                connector = ProxyConnector.from_url(
                    resolved_proxy_url, limit=request.max_connections
                )
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                proxy_arg = None
            else:
                connector = aiohttp.TCPConnector(limit=request.max_connections)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                proxy_arg = resolved_proxy_url

            try:
                warmup_ip, warmup_latency = await _warmup_proxy(
                    session=session,
                    proxy_url=proxy_arg,  # None for SOCKS/direct (connector handles it), URL for HTTP
                    url=request.head_url,
                    warmup_requests=request.warmup_requests,
                    timeout_seconds=request.timeout_seconds,
                )
                # Send warmup event
                yield f"data: {json.dumps({'type': 'warmup', 'ip': warmup_ip, 'latency_ms': round(warmup_latency, 2) if warmup_latency else None})}\n\n"
            finally:
                await session.close()

        # Phase 2: Test each concurrency level
        for concurrency in request.concurrency_levels:
            if concurrency <= 0:
                continue

            # Determine which connection levels to test
            if not request.test_connection_scaling:
                connection_levels_to_test = [request.max_connections]
            elif request.adaptive_connections:
                # Adaptive hill-climbing: start at middle (8), test neighbors, follow best direction
                # Levels: [1, 2, 4, 8, 16, 32, 64] - indices 0-6, middle is index 3 (value 8)
                all_levels = sorted(request.connection_levels)
                mid_index = len(all_levels) // 2  # Index 3 for 7 elements

                tested_connections: dict[int, float] = {}  # conn -> efficiency

                async def test_single_connection_level(
                    conn_level: int,
                ) -> tuple[float, ProxyAnalyzerResult | None]:
                    """Test a single connection level and return efficiency score."""
                    if is_direct:
                        # Direct connection - no proxy
                        conn = aiohttp.TCPConnector(limit=conn_level)
                        sess = aiohttp.ClientSession(connector=conn, timeout=timeout)
                        p_arg = None
                    elif scheme.startswith("socks"):
                        if resolved_proxy_url is None:
                            raise RuntimeError("SOCKS proxy URL not available")
                        conn = ProxyConnector.from_url(
                            resolved_proxy_url, limit=conn_level
                        )
                        sess = aiohttp.ClientSession(connector=conn, timeout=timeout)
                        p_arg = None
                    else:
                        conn = aiohttp.TCPConnector(limit=conn_level)
                        sess = aiohttp.ClientSession(connector=conn, timeout=timeout)
                        p_arg = resolved_proxy_url

                    try:
                        # Configurable request count for fair saturation testing
                        h_count = (
                            request.stress_requests if request.mode != "range" else 0
                        )
                        r_count = (
                            request.stress_requests if request.mode != "head" else 0
                        )
                        total_req = h_count + r_count

                        lats: list[float] = []
                        ttfbs: list[float] = []
                        b_read = 0
                        s_counts: Counter[str] = Counter()
                        h_status: Counter[str] = Counter()
                        r_status: Counter[str] = Counter()
                        ips: list[str] = []

                        if h_count > 0:
                            h_lats, h_ttfb, h_bytes, h_status, _, h_ips = (
                                await _run_stress_requests(
                                    session=sess,
                                    proxy_url=p_arg,
                                    method="GET" if request.verify_ip else "HEAD",
                                    url=request.head_url,
                                    total_requests=h_count,
                                    concurrency=concurrency,
                                    timeout_seconds=request.timeout_seconds,
                                    track_ttfb=request.track_ttfb,
                                    verify_ip=request.verify_ip,
                                )
                            )
                            lats.extend(h_lats)
                            ttfbs.extend(h_ttfb)
                            b_read += h_bytes
                            s_counts.update(h_status)
                            ips.extend(h_ips)

                        if r_count > 0:
                            # Only use Range header if download_bytes > 0
                            hdrs: dict[str, str] | None = None
                            if request.download_bytes > 0:
                                end_byte = request.download_bytes - 1
                                hdrs = {"Range": f"bytes=0-{end_byte}"}
                            r_lats, r_ttfb, r_bytes, r_status, _, _ = (
                                await _run_stress_requests(
                                    session=sess,
                                    proxy_url=p_arg,
                                    method="GET",
                                    url=request.download_url,
                                    total_requests=r_count,
                                    concurrency=concurrency,
                                    timeout_seconds=request.timeout_seconds,
                                    headers=hdrs,
                                    track_ttfb=request.track_ttfb,
                                    url_pool=cdn_url_pool if cdn_url_pool else None,
                                )
                            )
                            lats.extend(r_lats)
                            ttfbs.extend(r_ttfb)
                            b_read += r_bytes
                            s_counts.update(r_status)

                        succ = sum(c for k, c in s_counts.items() if k.isdigit())
                        errs = sum(
                            c for k, c in s_counts.items() if k.startswith("error:")
                        )
                        avg_lat = sum(lats) / len(lats) if lats else 0.0
                        p50 = _percentile(lats, 0.50)
                        p95 = _percentile(lats, 0.95)
                        p99 = _percentile(lats, 0.99)
                        max_lat = max(lats) if lats else 0.0
                        min_lat = min(lats) if lats else 0.0
                        jit = _jitter(lats)
                        avg_t = sum(ttfbs) / len(ttfbs) if ttfbs else 0.0

                        total_sec = sum(lats) / 1000 if lats else 0.0
                        tput = (
                            (b_read * 8) / (total_sec * 1_000_000)
                            if total_sec > 0
                            else 0.0
                        )
                        eff = tput / (avg_lat / 1000) if avg_lat > 0 else 0.0

                        unique_ips = set(ips)
                        ip_cons = len(unique_ips) <= 1
                        p_ip = ips[0] if ips else warmup_ip

                        # Fetch geolocation once and cache
                        nonlocal cached_geolocation, geolocation_fetched
                        if not geolocation_fetched and p_ip:
                            geolocation_fetched = True
                            try:
                                cached_geolocation = await get_ip_geolocation(
                                    p_ip, proxy_url=proxy_url
                                )
                            except Exception as exc:
                                _logger.warning(
                                    f"[Performance Analysis Stream] Geolocation failed for {p_ip}: {exc}"
                                )

                        res = ProxyAnalyzerResult(
                            concurrency=concurrency,
                            max_connections=conn_level,
                            requests=total_req,
                            successes=succ,
                            errors=errs,
                            avg_latency_ms=round(avg_lat, 2),
                            p50_latency_ms=round(p50, 2),
                            p95_latency_ms=round(p95, 2),
                            p99_latency_ms=round(p99, 2),
                            max_latency_ms=round(max_lat, 2),
                            min_latency_ms=round(min_lat, 2),
                            jitter_ms=round(jit, 2),
                            avg_ttfb_ms=round(avg_t, 2),
                            throughput_mbps=round(tput, 2),
                            efficiency_score=round(eff, 2),
                            status_counts=dict(s_counts),
                            head_status_counts=dict(h_status) if h_count > 0 else None,
                            download_status_counts=(
                                dict(r_status) if r_count > 0 else None
                            ),
                            proxy_ip=p_ip,
                            ip_consistent=ip_cons,
                            geolocation=cached_geolocation,
                        )

                        success_rate = succ / total_req if total_req > 0 else 0.0
                        return (eff if success_rate >= 0.95 else 0.0, res)
                    finally:
                        await sess.close()

                # Calculate total requests for progress messages
                progress_requests = (
                    request.stress_requests
                    if request.mode != "mixed"
                    else request.stress_requests * 2
                )

                # Hill-climbing for connections: start at mid (8), test up (16) and down (4)
                start_idx = mid_index
                start_level = all_levels[start_idx]

                # Test baseline (8 conn)
                yield f"data: {json.dumps({'type': 'progress', 'concurrency': concurrency, 'connections': start_level, 'requests': progress_requests})}\n\n"
                base_eff, base_result = await test_single_connection_level(start_level)
                tested_connections[start_level] = base_eff
                if base_result:
                    yield f"data: {json.dumps({'type': 'result', 'result': base_result.model_dump()})}\n\n"

                # Test up neighbor (16 conn) if exists
                up_idx = start_idx + 1
                up_eff = 0.0
                if up_idx < len(all_levels):
                    up_level = all_levels[up_idx]
                    yield f"data: {json.dumps({'type': 'progress', 'concurrency': concurrency, 'connections': up_level, 'requests': progress_requests})}\n\n"
                    up_eff, up_result = await test_single_connection_level(up_level)
                    tested_connections[up_level] = up_eff
                    if up_result:
                        yield f"data: {json.dumps({'type': 'result', 'result': up_result.model_dump()})}\n\n"

                # Test down neighbor (4 conn) if exists
                down_idx = start_idx - 1
                down_eff = 0.0
                if down_idx >= 0:
                    down_level = all_levels[down_idx]
                    yield f"data: {json.dumps({'type': 'progress', 'concurrency': concurrency, 'connections': down_level, 'requests': progress_requests})}\n\n"
                    down_eff, down_result = await test_single_connection_level(
                        down_level
                    )
                    tested_connections[down_level] = down_eff
                    if down_result:
                        yield f"data: {json.dumps({'type': 'result', 'result': down_result.model_dump()})}\n\n"

                # Determine best direction and follow
                best_eff = base_eff
                best_idx = start_idx
                if up_eff > best_eff:
                    best_eff = up_eff
                    best_idx = up_idx
                if down_eff > best_eff:
                    best_eff = down_eff
                    best_idx = down_idx

                if best_idx > start_idx:
                    # Going up was better - keep going up
                    current_idx = best_idx + 1
                    prev_eff = best_eff
                    while current_idx < len(all_levels):
                        level = all_levels[current_idx]
                        yield f"data: {json.dumps({'type': 'progress', 'concurrency': concurrency, 'connections': level, 'requests': progress_requests})}\n\n"
                        eff, res = await test_single_connection_level(level)
                        tested_connections[level] = eff
                        if res:
                            yield f"data: {json.dumps({'type': 'result', 'result': res.model_dump()})}\n\n"
                        if eff < prev_eff:
                            break  # Peak found, stop
                        prev_eff = eff
                        current_idx += 1
                elif best_idx < start_idx:
                    # Going down was better - keep going down
                    current_idx = best_idx - 1
                    prev_eff = best_eff
                    while current_idx >= 0:
                        level = all_levels[current_idx]
                        yield f"data: {json.dumps({'type': 'progress', 'concurrency': concurrency, 'connections': level, 'requests': progress_requests})}\n\n"
                        eff, res = await test_single_connection_level(level)
                        tested_connections[level] = eff
                        if res:
                            yield f"data: {json.dumps({'type': 'result', 'result': res.model_dump()})}\n\n"
                        if eff < prev_eff:
                            break  # Peak found, stop
                        prev_eff = eff
                        current_idx -= 1
                # else: baseline was best, no need to continue

                # Find best from tested connections
                best_conn_for_this_concurrency = max(
                    tested_connections,
                    key=lambda level: tested_connections.get(level, 0.0),
                    default=start_level,
                )
                best_eff_for_this_concurrency = tested_connections.get(
                    best_conn_for_this_concurrency, 0.0
                )

                if best_eff_for_this_concurrency > best_overall_efficiency:
                    best_overall_efficiency = best_eff_for_this_concurrency
                    recommended_concurrency = concurrency
                    recommended_connections = best_conn_for_this_concurrency

                best_configs[concurrency] = {
                    "connections": best_conn_for_this_concurrency,
                    "efficiency": round(best_eff_for_this_concurrency, 2),
                }
                continue  # Skip the old loop below
            else:
                # Test all connection levels sequentially
                connection_levels_to_test = request.connection_levels

            best_efficiency_for_concurrency = 0.0
            best_connections_for_concurrency = request.max_connections

            for max_connections in connection_levels_to_test:
                if is_direct:
                    # Direct connection - no proxy
                    connector = aiohttp.TCPConnector(limit=max_connections)
                    session = aiohttp.ClientSession(
                        connector=connector, timeout=timeout
                    )
                    proxy_arg = None
                elif scheme.startswith("socks"):
                    if resolved_proxy_url is None:
                        raise RuntimeError("SOCKS proxy URL not available")
                    connector = ProxyConnector.from_url(
                        resolved_proxy_url, limit=max_connections
                    )
                    session = aiohttp.ClientSession(
                        connector=connector, timeout=timeout
                    )
                    proxy_arg = None
                else:
                    connector = aiohttp.TCPConnector(limit=max_connections)
                    session = aiohttp.ClientSession(
                        connector=connector, timeout=timeout
                    )
                    proxy_arg = resolved_proxy_url

                try:
                    # Configurable request count for fair saturation testing
                    head_count = request.stress_requests
                    range_count = request.stress_requests
                    if request.mode == "head":
                        range_count = 0
                    elif request.mode == "range":
                        head_count = 0

                    total_requests_for_test = head_count + range_count

                    # Send progress event before starting test
                    yield f"data: {json.dumps({'type': 'progress', 'concurrency': concurrency, 'connections': max_connections, 'requests': total_requests_for_test})}\n\n"

                    latencies: list[float] = []
                    ttfb_list: list[float] = []
                    bytes_read = 0
                    status_counts: Counter[str] = Counter()
                    head_status: Counter[str] = Counter()
                    range_status: Counter[str] = Counter()
                    ips_seen: list[str] = []

                    if head_count > 0:
                        (
                            head_latencies,
                            head_ttfb,
                            head_bytes,
                            head_status,
                            _,
                            head_ips,
                        ) = await _run_stress_requests(
                            session=session,
                            proxy_url=proxy_arg,
                            method="GET" if request.verify_ip else "HEAD",
                            url=request.head_url,
                            total_requests=head_count,
                            concurrency=concurrency,
                            timeout_seconds=request.timeout_seconds,
                            track_ttfb=request.track_ttfb,
                            verify_ip=request.verify_ip,
                        )
                        latencies.extend(head_latencies)
                        ttfb_list.extend(head_ttfb)
                        bytes_read += head_bytes
                        status_counts.update(head_status)
                        ips_seen.extend(head_ips)

                    if range_count > 0:
                        # Only use Range header if download_bytes > 0
                        headers: dict[str, str] | None = None
                        if request.download_bytes > 0:
                            end_byte = request.download_bytes - 1
                            headers = {"Range": f"bytes=0-{end_byte}"}
                        (
                            range_latencies,
                            range_ttfb,
                            range_bytes,
                            range_status_result,
                            _,
                            _,
                        ) = await _run_stress_requests(
                            session=session,
                            proxy_url=proxy_arg,
                            method="GET",
                            url=request.download_url,
                            total_requests=range_count,
                            concurrency=concurrency,
                            timeout_seconds=request.timeout_seconds,
                            headers=headers,
                            track_ttfb=request.track_ttfb,
                            url_pool=cdn_url_pool if cdn_url_pool else None,
                        )
                        latencies.extend(range_latencies)
                        ttfb_list.extend(range_ttfb)
                        bytes_read += range_bytes
                        status_counts.update(range_status_result)
                        range_status = range_status_result

                    successes = sum(
                        count for key, count in status_counts.items() if key.isdigit()
                    )
                    errors = sum(
                        count
                        for key, count in status_counts.items()
                        if key.startswith("error:")
                    )
                    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
                    p50_latency = _percentile(latencies, 0.50)
                    p95_latency = _percentile(latencies, 0.95)
                    p99_latency = _percentile(latencies, 0.99)
                    max_latency = max(latencies) if latencies else 0.0
                    min_latency = min(latencies) if latencies else 0.0
                    jitter = _jitter(latencies)
                    avg_ttfb = sum(ttfb_list) / len(ttfb_list) if ttfb_list else 0.0

                    total_seconds = sum(latencies) / 1000 if latencies else 0.0
                    throughput_mbps = (
                        (bytes_read * 8) / (total_seconds * 1_000_000)
                        if total_seconds > 0
                        else 0.0
                    )
                    efficiency = (
                        throughput_mbps / (avg_latency / 1000)
                        if avg_latency > 0
                        else 0.0
                    )

                    unique_ips = set(ips_seen)
                    ip_consistent = len(unique_ips) <= 1
                    proxy_ip_for_level = ips_seen[0] if ips_seen else warmup_ip

                    # Fetch geolocation once and cache
                    if not geolocation_fetched and proxy_ip_for_level:
                        geolocation_fetched = True
                        try:
                            cached_geolocation = await get_ip_geolocation(
                                proxy_ip_for_level, proxy_url=proxy_url
                            )
                        except Exception as exc:
                            _logger.warning(
                                f"[Performance Analysis Stream] Geolocation failed for {proxy_ip_for_level}: {exc}"
                            )

                    result = ProxyAnalyzerResult(
                        concurrency=concurrency,
                        max_connections=max_connections,
                        requests=head_count + range_count,
                        successes=successes,
                        errors=errors,
                        avg_latency_ms=round(avg_latency, 2),
                        p50_latency_ms=round(p50_latency, 2),
                        p95_latency_ms=round(p95_latency, 2),
                        p99_latency_ms=round(p99_latency, 2),
                        max_latency_ms=round(max_latency, 2),
                        min_latency_ms=round(min_latency, 2),
                        jitter_ms=round(jitter, 2),
                        avg_ttfb_ms=round(avg_ttfb, 2),
                        throughput_mbps=round(throughput_mbps, 2),
                        efficiency_score=round(efficiency, 2),
                        status_counts=dict(status_counts),
                        head_status_counts=(
                            dict(head_status) if head_count > 0 else None
                        ),
                        download_status_counts=(
                            dict(range_status) if range_count > 0 else None
                        ),
                        proxy_ip=proxy_ip_for_level,
                        ip_consistent=ip_consistent,
                        geolocation=cached_geolocation,
                    )

                    # Stream result immediately
                    yield f"data: {json.dumps({'type': 'result', 'result': result.model_dump()})}\n\n"

                    success_rate = (
                        successes / (head_count + range_count)
                        if (head_count + range_count) > 0
                        else 0.0
                    )
                    if (
                        success_rate >= 0.95
                        and efficiency > best_efficiency_for_concurrency
                    ):
                        best_efficiency_for_concurrency = efficiency
                        best_connections_for_concurrency = max_connections

                    if success_rate >= 0.95 and efficiency > best_overall_efficiency:
                        best_overall_efficiency = efficiency
                        recommended_concurrency = concurrency
                        recommended_connections = max_connections

                    if baseline_p95 is None and p95_latency > 0:
                        baseline_p95 = p95_latency

                    if request.auto_ramp and baseline_p95 is not None:
                        error_rate = (
                            errors / (head_count + range_count)
                            if (head_count + range_count) > 0
                            else 0.0
                        )
                        if (
                            error_rate > request.max_error_rate
                            or p95_latency > baseline_p95 * request.max_p95_multiplier
                        ):
                            break

                finally:
                    await session.close()

            if best_efficiency_for_concurrency > 0:
                best_configs[concurrency] = {
                    "connections": best_connections_for_concurrency,
                    "efficiency": round(best_efficiency_for_concurrency, 2),
                }

        # Send final summary
        yield f"data: {json.dumps({'type': 'complete', 'recommended_concurrency': recommended_concurrency, 'recommended_connections': recommended_connections, 'best_configs': best_configs})}\n\n"

    return StreamingResponse(
        generate_results(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def parse_proxy_line(
    line: str,
    default_group: str | None = None,
) -> tuple[str, str | None, str | None, str | None]:
    """Parse a proxy line into (clean_url, username, password, group).

    Supports:
    - http://anon:anon@31.59.20.176:6754
    - socks5://anon:anon@31.59.20.176:6754
    - 31.59.20.176:6754:anon:anon
    - anon:anon@31.59.20.176:6754
    - 31.59.20.176:6754
    - http://31.59.20.176:6754
    - line # group_name
    """
    raw = line.strip()
    if not raw:
        raise ValueError("Empty line")

    group = default_group
    if "#" in raw:
        parts = raw.split("#", 1)
        raw = parts[0].strip()
        comment_group = parts[1].strip()
        if comment_group:
            group = comment_group

    if not raw:
        raise ValueError("Empty line after comment removal")

    # Check if scheme is present: http://..., socks5://...
    if "://" in raw:
        parsed = urlparse(raw)
        scheme = parsed.scheme.lower()
        host = parsed.hostname
        port = parsed.port
        username = parsed.username
        password = parsed.password
        if not host:
            raise ValueError(f"Missing host in: {raw}")
        clean_url = f"{scheme}://{host}:{port}" if port else f"{scheme}://{host}"
        return clean_url, username, password, group

    # Check if user:pass@host:port without scheme
    if "@" in raw:
        parsed = urlparse(f"http://{raw}")
        host = parsed.hostname
        port = parsed.port
        username = parsed.username
        password = parsed.password
        if not host:
            raise ValueError(f"Missing host in: {raw}")
        clean_url = f"http://{host}:{port}" if port else f"http://{host}"
        return clean_url, username, password, group

    # Check colon separated formats
    parts = raw.split(":")
    if len(parts) == 4:
        # host:port:user:pass
        host, port_str, username, password = parts
        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(f"Invalid port in: {raw}") from exc
        clean_url = f"http://{host}:{port}"
        return clean_url, username or None, password or None, group
    elif len(parts) == 2:
        # host:port
        host, port_str = parts
        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(f"Invalid port in: {raw}") from exc
        clean_url = f"http://{host}:{port}"
        return clean_url, None, None, group

    raise ValueError(f"Unrecognized proxy format: {raw}")


@router.post("/", response_model=ProxyResponse)
async def add_proxy(
    proxy_data: ProxyCreateRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> ProxyResponse:
    """Add a new proxy to configuration."""
    try:
        clean_url, user, pwd, grp = parse_proxy_line(
            proxy_data.url, default_group=proxy_data.group
        )
    except Exception:
        clean_url = proxy_data.url
        user = proxy_data.username
        pwd = proxy_data.password
        grp = proxy_data.group

    final_username = proxy_data.username or user
    final_password = proxy_data.password or pwd
    final_group = proxy_data.group or grp

    if config.settings.network.proxies and any(
        p.url == clean_url for p in config.settings.network.proxies
    ):
        raise HTTPException(status_code=400, detail="Proxy already exists")

    new_proxy = Proxy(
        url=clean_url,
        username=final_username,
        password=final_password,
        enabled=proxy_data.enabled,
        max_connections=proxy_data.max_connections,
        group=final_group,
        guest_lookup_timeout=proxy_data.guest_lookup_timeout,
    )

    if not config.settings.network.proxies:
        config.settings.network.proxies = []

    config.settings.network.proxies.append(new_proxy)
    _persist_proxy_config(config)

    return ProxyResponse(
        id=new_proxy.url,
        url=new_proxy.url,
        username=new_proxy.username,
        enabled=new_proxy.enabled,
        max_connections=new_proxy.max_connections,
        group=new_proxy.group,
        assigned_auths=[],
        guest_lookup_timeout=new_proxy.guest_lookup_timeout,
    )


@router.post("/bulk", response_model=ProxyBulkCreateResponse)
async def bulk_add_proxies(
    request: ProxyBulkCreateRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> ProxyBulkCreateResponse:
    """Bulk import proxies from parsed items or raw text lines."""
    items_to_process: list[ProxyBulkCreateItem] = list(request.proxies)

    if request.raw_text:
        for raw_line in request.raw_text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("//"):
                continue
            try:
                url, user, pwd, grp = parse_proxy_line(
                    line, default_group=request.default_group
                )
                items_to_process.append(
                    ProxyBulkCreateItem(
                        url=url,
                        username=user,
                        password=pwd,
                        enabled=True,
                        max_connections=-1,
                        group=grp,
                    )
                )
            except Exception as exc:
                _logger.warning(f"[Bulk Proxy] Failed to parse line '{line}': {exc}")

    if not config.settings.network.proxies:
        config.settings.network.proxies = []

    added: list[ProxyResponse] = []
    updated: list[ProxyResponse] = []
    skipped: list[str] = []
    errors: list[str] = []

    existing_by_url = {p.url: p for p in config.settings.network.proxies}

    for item in items_to_process:
        try:
            try:
                clean_url, user, pwd, grp = parse_proxy_line(
                    item.url, default_group=item.group or request.default_group
                )
            except Exception:
                clean_url = item.url
                user = item.username
                pwd = item.password
                grp = item.group or request.default_group

            final_username = item.username or user
            final_password = item.password or pwd
            final_group = item.group or grp or request.default_group

            existing = existing_by_url.get(clean_url)
            if existing:
                if request.overwrite:
                    if final_username is not None:
                        existing.username = final_username
                    if final_password is not None:
                        existing.password = final_password
                    if final_group is not None:
                        existing.group = final_group
                    existing.enabled = item.enabled
                    if item.max_connections != -1:
                        existing.max_connections = item.max_connections
                    updated.append(
                        ProxyResponse(
                            id=existing.url,
                            url=existing.url,
                            username=existing.username,
                            enabled=existing.enabled,
                            max_connections=existing.max_connections,
                            group=getattr(existing, "group", None),
                            assigned_auths=[],
                        )
                    )
                else:
                    skipped.append(clean_url)
            else:
                new_p = Proxy(
                    url=clean_url,
                    username=final_username,
                    password=final_password,
                    enabled=item.enabled,
                    max_connections=item.max_connections,
                    group=final_group,
                )
                config.settings.network.proxies.append(new_p)
                existing_by_url[clean_url] = new_p
                added.append(
                    ProxyResponse(
                        id=new_p.url,
                        url=new_p.url,
                        username=new_p.username,
                        enabled=new_p.enabled,
                        max_connections=new_p.max_connections,
                        group=new_p.group,
                        assigned_auths=[],
                    )
                )
        except Exception as exc:
            errors.append(f"Error processing {item.url}: {exc}")

    _persist_proxy_config(config)
    return ProxyBulkCreateResponse(
        status="success",
        added=added,
        updated=updated,
        skipped=skipped,
        errors=errors,
    )


@router.post("/bulk-sync", response_model=ProxyBulkSyncResponse)
@router.put("/bulk", response_model=ProxyBulkSyncResponse)
async def bulk_sync_proxies(
    request: ProxyBulkSyncRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> ProxyBulkSyncResponse:
    """Bulk edit proxies list:
    - Keep existing proxies that are present in the new list (do not replace them).
    - Delete existing proxies that are NOT in the new list.
    - Add new proxies that are not currently in the list.
    """
    items_to_process: list[ProxyBulkCreateItem] = list(request.proxies)

    if request.raw_text and not items_to_process:
        for raw_line in request.raw_text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("//"):
                continue
            try:
                url, user, pwd, grp = parse_proxy_line(
                    line, default_group=request.default_group
                )
                items_to_process.append(
                    ProxyBulkCreateItem(
                        url=url,
                        username=user,
                        password=pwd,
                        enabled=True,
                        max_connections=-1,
                        group=grp,
                    )
                )
            except Exception as exc:
                _logger.warning(f"[Bulk Proxy Sync] Failed to parse line '{line}': {exc}")

    if not config.settings.network.proxies:
        config.settings.network.proxies = []

    existing_by_url = {p.url: p for p in config.settings.network.proxies}

    processed_items: list[tuple[str, str | None, str | None, str | None, bool, int]] = []
    seen_clean_urls: set[str] = set()
    errors: list[str] = []

    for item in items_to_process:
        try:
            try:
                clean_url, user, pwd, grp = parse_proxy_line(
                    item.url, default_group=item.group or request.default_group
                )
            except Exception:
                clean_url = item.url
                user = item.username
                pwd = item.password
                grp = item.group or request.default_group

            final_username = item.username or user
            final_password = item.password or pwd
            final_group = item.group or grp or request.default_group

            if clean_url in seen_clean_urls:
                continue
            seen_clean_urls.add(clean_url)
            processed_items.append(
                (
                    clean_url,
                    final_username,
                    final_password,
                    final_group,
                    item.enabled,
                    item.max_connections,
                )
            )
        except Exception as exc:
            errors.append(f"Error processing {item.url}: {exc}")

    target_group: str | None = None
    if request.target_group and request.target_group.strip():
        target_group = request.target_group.strip()
    elif request.default_group and request.default_group.strip():
        target_group = request.default_group.strip()

    def is_proxy_in_scope(p: Proxy) -> bool:
        if target_group is None or target_group == "__all__":
            return True
        p_group = getattr(p, "group", None)
        p_group_str = p_group.strip() if isinstance(p_group, str) and p_group.strip() else None
        if target_group == "__ungrouped__":
            return p_group_str is None
        return p_group_str == target_group

    target_clean_urls = {p[0] for p in processed_items}

    kept_proxies: list[Proxy] = []
    untouched_proxies: list[Proxy] = []
    deleted_urls: list[str] = []

    for p in config.settings.network.proxies:
        if p.url in target_clean_urls:
            if target_group and target_group not in ("__all__", "__ungrouped__"):
                if getattr(p, "group", None) != target_group:
                    p.group = target_group
            kept_proxies.append(p)
        elif not is_proxy_in_scope(p):
            untouched_proxies.append(p)
        else:
            deleted_urls.append(p.url)

    added_proxies: list[Proxy] = []
    added_responses: list[ProxyResponse] = []
    for (
        clean_url,
        final_username,
        final_password,
        final_group,
        enabled,
        max_connections,
    ) in processed_items:
        if clean_url not in existing_by_url:
            assigned_group = final_group or (
                target_group if target_group not in ("__all__", "__ungrouped__") else None
            )
            new_p = Proxy(
                url=clean_url,
                username=final_username,
                password=final_password,
                enabled=enabled,
                max_connections=max_connections,
                group=assigned_group,
            )
            added_proxies.append(new_p)
            added_responses.append(
                ProxyResponse(
                    id=new_p.url,
                    url=new_p.url,
                    username=new_p.username,
                    enabled=new_p.enabled,
                    max_connections=new_p.max_connections,
                    group=new_p.group,
                    assigned_auths=[],
                )
            )

    config.settings.network.proxies = [
        p for p in config.settings.network.proxies if p.url not in set(deleted_urls)
    ] + added_proxies
    _persist_proxy_config(config)

    kept_responses = [
        ProxyResponse(
            id=p.url,
            url=p.url,
            username=getattr(p, "username", None),
            enabled=getattr(p, "enabled", True),
            max_connections=getattr(p, "max_connections", -1),
            group=getattr(p, "group", None),
            assigned_auths=[],
        )
        for p in kept_proxies
    ]

    scope_desc = (
        f" in group '{target_group}'"
        if target_group and target_group not in ("__all__", "__ungrouped__")
        else (" (ungrouped)" if target_group == "__ungrouped__" else "")
    )

    return ProxyBulkSyncResponse(
        status="success",
        added=added_responses,
        kept=kept_responses,
        deleted=deleted_urls,
        errors=errors,
        message=f"Bulk edit complete{scope_desc}: {len(kept_responses)} kept, {len(added_responses)} added, {len(deleted_urls)} deleted.",
        target_group=target_group,
    )


@router.put("/bulk-group", response_model=ProxyBulkGroupResponse)
async def bulk_assign_group(
    request: ProxyBulkGroupRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> ProxyBulkGroupResponse:
    """Assign multiple proxies to a specified group."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    target_urls = set(request.proxy_urls)
    updated_count = 0

    for proxy in config.settings.network.proxies:
        if proxy.url in target_urls:
            proxy.group = request.group or None
            updated_count += 1

    _persist_proxy_config(config)
    return ProxyBulkGroupResponse(
        status="success",
        message=f"Assigned {updated_count} proxies to group '{request.group or 'none'}'",
        updated_count=updated_count,
        group=request.group,
    )


@router.put("/groups/{group_name}/settings", response_model=GroupSettingsUpdateResponse)
async def update_group_settings(
    group_name: str,
    update_data: GroupSettingsUpdateRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> GroupSettingsUpdateResponse:
    """Update settings (connections, concurrency, enabled) for all proxies in a group."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    matched = [
        p for p in config.settings.network.proxies if getattr(p, "group", None) == group_name
    ]
    if not matched:
        raise HTTPException(
            status_code=404, detail=f"No proxies found in group '{group_name}'"
        )

    for proxy in matched:
        if update_data.max_connections is not None:
            proxy.max_connections = update_data.max_connections
        if update_data.max_concurrency is not None:
            proxy.downloads.max_concurrency = update_data.max_concurrency
        if update_data.enabled is not None:
            proxy.enabled = update_data.enabled

    network = config.settings.network
    if "guest_lookup_timeout" in update_data.model_fields_set:
        group_settings = network.proxy_groups.get(group_name) or ProxyGroupSettings()
        group_settings.guest_lookup_timeout = update_data.guest_lookup_timeout
        if group_settings.model_dump(exclude_none=True):
            network.proxy_groups[group_name] = group_settings
        else:
            network.proxy_groups.pop(group_name, None)

    _persist_proxy_config(config)
    group_settings = (getattr(network, "proxy_groups", None) or {}).get(group_name)
    return GroupSettingsUpdateResponse(
        status="success",
        message=f"Updated {len(matched)} proxies in group '{group_name}'",
        updated_count=len(matched),
        group=group_name,
        max_connections=update_data.max_connections,
        max_concurrency=update_data.max_concurrency,
        enabled=update_data.enabled,
        guest_lookup_timeout=group_settings.guest_lookup_timeout if group_settings else None,
    )


@router.get("/groups/{group_name}/settings", response_model=GroupSettingsResponse)
async def get_group_settings(
    group_name: str,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> GroupSettingsResponse:
    """Return a proxy group's own settings (values its proxies inherit)."""
    group_settings = (getattr(config.settings.network, "proxy_groups", None) or {}).get(group_name)
    return GroupSettingsResponse(
        group=group_name,
        guest_lookup_timeout=group_settings.guest_lookup_timeout if group_settings else None,
    )


@router.put("/bulk-state", response_model=BulkProxyStateResponse)
async def bulk_update_proxy_state(
    request: BulkProxyStateRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> BulkProxyStateResponse:
    """Bulk enable or disable multiple proxies by URL."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    target_urls = set(request.proxy_urls)
    updated_count = 0
    for proxy in config.settings.network.proxies:
        if proxy.url in target_urls:
            proxy.enabled = request.enabled
            updated_count += 1

    _persist_proxy_config(config)
    return BulkProxyStateResponse(
        status="success",
        message=f"{'Enabled' if request.enabled else 'Disabled'} {updated_count} proxies",
        updated_count=updated_count,
        enabled=request.enabled,
    )


@router.put("/bulk-edit", response_model=BulkProxyEditResponse)
async def bulk_edit_proxies(
    request: BulkProxyEditRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> BulkProxyEditResponse:
    """Bulk edit max_connections and/or max_concurrency across multiple proxies by URL."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    if (
        request.max_connections is None
        and request.max_concurrency is None
        and request.guest_lookup_timeout is None
    ):
        raise HTTPException(
            status_code=400,
            detail="At least one of max_connections, max_concurrency, or guest_lookup_timeout must be provided",
        )

    target_urls = set(request.proxy_urls)
    updated_count = 0
    for proxy in config.settings.network.proxies:
        if proxy.url in target_urls:
            if request.max_connections is not None:
                proxy.max_connections = request.max_connections
            if request.max_concurrency is not None:
                proxy.downloads.max_concurrency = request.max_concurrency
            if request.guest_lookup_timeout is not None:
                proxy.guest_lookup_timeout = request.guest_lookup_timeout
            updated_count += 1

    _persist_proxy_config(config)

    parts = []
    if request.max_connections is not None:
        parts.append(f"max_connections={request.max_connections}")
    if request.max_concurrency is not None:
        parts.append(f"max_concurrency={request.max_concurrency}")
    if request.guest_lookup_timeout is not None:
        parts.append(f"guest_lookup_timeout={request.guest_lookup_timeout}")
    detail = ", ".join(parts)

    return BulkProxyEditResponse(
        status="success",
        message=f"Updated {detail} on {updated_count} proxies",
        updated_count=updated_count,
        max_connections=request.max_connections,
        max_concurrency=request.max_concurrency,
        guest_lookup_timeout=request.guest_lookup_timeout,
    )


@router.delete("/bulk-delete", response_model=BulkProxyDeleteResponse)
async def bulk_delete_proxies(
    request: BulkProxyDeleteRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
) -> BulkProxyDeleteResponse:
    """Bulk delete multiple proxies by URL."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    target_urls = set(request.proxy_urls)
    initial_count = len(config.settings.network.proxies)
    config.settings.network.proxies = [
        p for p in config.settings.network.proxies if p.url not in target_urls
    ]
    deleted_count = initial_count - len(config.settings.network.proxies)

    _persist_proxy_config(config)
    return BulkProxyDeleteResponse(
        status="success",
        message=f"Deleted {deleted_count} proxies",
        deleted_count=deleted_count,
    )


@router.delete("/{proxy_url:path}")
async def remove_proxy(
    proxy_url: str,
    config: UltimaScraperCollectionConfig = Depends(get_config),
):
    """Remove a proxy from configuration."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    initial_count = len(config.settings.network.proxies)
    config.settings.network.proxies = [
        p for p in config.settings.network.proxies if p.url != proxy_url
    ]

    if len(config.settings.network.proxies) == initial_count:
        raise HTTPException(status_code=404, detail="Proxy not found")

    _persist_proxy_config(config)
    return {"status": "success", "message": "Proxy removed"}


class ProxyUpdateRequest(BaseModel):
    """Request model for updating proxy settings."""

    username: str | None = None
    password: str | None = None
    enabled: bool | None = None
    max_connections: int | None = None
    max_concurrency: int | None = None
    group: str | None = None
    guest_lookup_timeout: float | None = None


class ProxyUpdateResponse(BaseModel):
    """Response model for a persisted proxy settings update."""

    status: str
    message: str
    username: str | None
    password: str
    enabled: bool
    max_connections: int
    max_concurrency: int
    group: str | None = None
    guest_lookup_timeout: float | None = None


class NetworkUpdateRequest(BaseModel):
    """Request model for updating network (direct connection) settings."""

    max_connections: int | None = None
    max_concurrency: int | None = None
    guest_lookup_timeout: float | None = None
    guest_route_timeout: float | None = None


@router.put("/network")
async def update_network_settings(
    update_data: NetworkUpdateRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
):
    """Update network (direct connection) settings."""
    if update_data.max_connections is not None:
        config.settings.network.max_connections = update_data.max_connections
    if update_data.max_concurrency is not None:
        config.settings.network.downloads.max_concurrency = update_data.max_concurrency
    if update_data.guest_lookup_timeout is not None:
        config.settings.network.guest_lookup_timeout = update_data.guest_lookup_timeout
    if update_data.guest_route_timeout is not None:
        config.settings.network.guest_route_timeout = update_data.guest_route_timeout

    _persist_proxy_config(config)
    return {
        "status": "success",
        "message": "Network settings updated",
        "max_connections": config.settings.network.max_connections,
        "max_concurrency": config.settings.network.downloads.max_concurrency,
        "guest_lookup_timeout": config.settings.network.guest_lookup_timeout,
        "guest_route_timeout": getattr(config.settings.network, "guest_route_timeout", 3.0),
    }


@router.put("/{proxy_url:path}", response_model=ProxyUpdateResponse)
async def update_proxy(
    proxy_url: str,
    update_data: ProxyUpdateRequest,
    config: UltimaScraperCollectionConfig = Depends(get_config),
):
    """Update proxy settings."""
    if not config.settings.network.proxies:
        raise HTTPException(status_code=404, detail="No proxies configured")

    masked_url = _mask_proxy_url(proxy_url)
    proxy = next(
        (
            p
            for p in config.settings.network.proxies
            if p.url == proxy_url or (masked_url and _mask_proxy_url(p.url) == masked_url)
        ),
        None,
    )
    if proxy is None:
        raise HTTPException(status_code=404, detail="Proxy not found")

    if update_data.username is not None:
        proxy.username = update_data.username or None
    if update_data.password is not None:
        proxy.password = update_data.password or None
    if update_data.enabled is not None:
        proxy.enabled = update_data.enabled
    if update_data.max_connections is not None:
        proxy.max_connections = update_data.max_connections
    if update_data.max_concurrency is not None:
        proxy.downloads.max_concurrency = update_data.max_concurrency
    if "group" in update_data.model_fields_set:
        proxy.group = update_data.group or None
    if "guest_lookup_timeout" in update_data.model_fields_set:
        proxy.guest_lookup_timeout = update_data.guest_lookup_timeout

    _persist_proxy_config(config)
    return ProxyUpdateResponse(
        status="success",
        message="Proxy settings updated",
        username=proxy.username,
        password="***",
        enabled=proxy.enabled,
        max_connections=proxy.max_connections,
        max_concurrency=proxy.downloads.max_concurrency,
        group=getattr(proxy, "group", None),
        guest_lookup_timeout=getattr(proxy, "guest_lookup_timeout", None),
    )
