"""IP Geolocation helper using ipwho.is API with SQLite persistence."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from collections import OrderedDict
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict, cast

import aiohttp
from sqlmodel import select

_logger = logging.getLogger(__name__)


class FlagData(TypedDict, total=False):
    """Flag information."""

    img: str
    emoji: str
    emoji_unicode: str


class ConnectionData(TypedDict, total=False):
    """Connection/ISP information."""

    asn: int
    org: str
    isp: str
    domain: str


class TimezoneData(TypedDict, total=False):
    """Timezone information."""

    id: str
    abbr: str
    is_dst: bool
    offset: int
    utc: str
    current_time: str


class GeolocationData(TypedDict, total=False):
    """Geolocation data structure from ipwho.is."""

    ip: str
    success: bool
    type: str  # IPv4 or IPv6
    continent: str
    continent_code: str
    country: str
    country_code: str
    region: str
    region_code: str
    city: str
    latitude: float
    longitude: float
    is_eu: bool
    postal: str
    calling_code: str
    capital: str
    borders: str
    flag: FlagData
    connection: ConnectionData
    timezone: TimezoneData
    message: str  # Error message if success=False


class _CacheEntry:
    """Simple in-memory cache entry with expiry."""

    def __init__(self, data: GeolocationData, ttl_seconds: int = 3600):
        self.data = data
        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at


class IPGeolocationService:
    """Service for fetching IP geolocation data with DB persistence and in-memory caching.

    Lookup order:
      1. In-memory cache (fast, 1-hour TTL)
      2. SQLite ``ultimascraper.db`` (persistent, 24-hour TTL)
      3. ipwho.is API (remote, result saved to DB + cache)
    """

    DB_TTL_HOURS: int = 24  # Re-fetch from API after this many hours

    def __init__(
        self,
        base_url: str = "https://ipwho.is",
        cache_ttl_seconds: int = 3600,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        max_cache_entries: int = 1000,
    ):
        self.base_url = base_url
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_cache_entries = max(0, max_cache_entries)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._cache: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def get_geolocation(
        self, ip: str, *, proxy_url: str | None = None
    ) -> GeolocationData | None:
        """Fetch geolocation data for an IP address.

        Checks in-memory cache → DB → API (saves result).
        """
        if not ip or ip == "direct":
            return None

        # Skip private/local IPs
        try:
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                _logger.debug(f"[Geolocation] Skipping private/local IP: {ip}")
                return None
        except ValueError:
            _logger.warning(f"[Geolocation] Invalid IP address: {ip}")
            return None

        # 1) In-memory cache
        async with self._lock:
            if ip in self._cache:
                entry = self._cache[ip]
                if not entry.is_expired():
                    self._cache.move_to_end(ip)
                    _logger.debug(f"[Geolocation] Memory cache hit for {ip}")
                    return entry.data
                del self._cache[ip]

        # 2) Database lookup
        db_data = self._load_from_db(ip)
        if db_data is not None:
            _logger.debug(f"[Geolocation] DB cache hit for {ip}")
            async with self._lock:
                self._store_cache_entry(ip, db_data)
            return db_data

        # 3) Fetch from API
        api_data = await self._fetch_from_api(ip)
        if api_data is not None:
            # Save to DB
            self._save_to_db(api_data, proxy_url=proxy_url)
            # Save to in-memory cache
            async with self._lock:
                self._store_cache_entry(ip, api_data)
        return api_data

    def get_all_cached_geolocations(self, *, db_session=None) -> list[GeolocationData]:
        """Return all geolocation records from the database."""
        try:
            from ultima_scraper_db_archive.global_db import (
                ProxyGeolocationModel,
                get_global_session,
            )

            with (
                nullcontext(db_session)
                if db_session is not None
                else get_global_session()
            ) as session:
                rows = session.execute(select(ProxyGeolocationModel)).scalars().all()
                return [
                    cast(GeolocationData, cast(Any, row).to_geolocation_dict())
                    for row in rows
                ]
        except Exception as exc:
            if db_session is not None:
                # The caller owns this transaction; SQL failure requires its
                # rollback and must not be mistaken for an empty geo cache.
                raise
            _logger.error(f"[Geolocation] Error reading all from DB: {exc}")
            return []

    def get_geolocation_by_proxy_url(self, proxy_url: str) -> GeolocationData | None:
        """Look up cached geolocation for a proxy URL from the database."""
        try:
            from ultima_scraper_db_archive.global_db import (
                ProxyGeolocationModel,
                get_global_session,
            )

            with get_global_session() as session:
                row = session.exec(
                    select(ProxyGeolocationModel).where(
                        ProxyGeolocationModel.proxy_url == proxy_url
                    )
                ).first()
                if row is None:
                    return None
                return cast(GeolocationData, cast(Any, row).to_geolocation_dict())
        except Exception as exc:
            _logger.error(
                f"[Geolocation] Error loading by proxy_url {proxy_url}: {exc}"
            )
            return None

    # ------------------------------------------------------------------ #
    # DB helpers
    # ------------------------------------------------------------------ #

    def _load_from_db(self, ip: str) -> GeolocationData | None:
        """Load geolocation from the global database if fresh enough."""
        try:
            from ultima_scraper_db_archive.global_db import (
                ProxyGeolocationModel,
                get_global_session,
            )

            with get_global_session() as session:
                row = session.exec(
                    select(ProxyGeolocationModel).where(ProxyGeolocationModel.ip == ip)
                ).first()
                if row is None:
                    return None

                # Check DB TTL
                updated_at = row.updated_at
                if updated_at is None:
                    return None
                age = datetime.now(timezone.utc) - updated_at.replace(
                    tzinfo=timezone.utc
                )
                if age > timedelta(hours=self.DB_TTL_HOURS):
                    _logger.debug(f"[Geolocation] DB entry for {ip} expired ({age})")
                    return None

                return cast(GeolocationData, cast(Any, row).to_geolocation_dict())
        except Exception as exc:
            _logger.error(f"[Geolocation] Error loading from DB for {ip}: {exc}")
            return None

    def _save_to_db(
        self, data: GeolocationData, *, proxy_url: str | None = None
    ) -> None:
        """Upsert geolocation data into the global database."""
        try:
            from ultima_scraper_db_archive.global_db import (
                ProxyGeolocationModel,
                get_global_session,
            )

            ip_value = data.get("ip")
            if not ip_value:
                _logger.warning(
                    "[Geolocation] Refusing to save geolocation payload without IP"
                )
                return

            with get_global_session() as session:
                existing = session.exec(
                    select(ProxyGeolocationModel).where(
                        ProxyGeolocationModel.ip == ip_value
                    )
                ).first()

                if existing:
                    # Update existing record
                    connection = data.get("connection") or {}
                    tz = data.get("timezone") or {}
                    flag = data.get("flag") or {}
                    if proxy_url is not None:
                        existing.proxy_url = proxy_url
                    existing.ip_type = data.get("type")
                    existing.continent = data.get("continent")
                    existing.country = data.get("country")
                    existing.country_code = data.get("country_code")
                    existing.region = data.get("region")
                    existing.city = data.get("city")
                    existing.latitude = data.get("latitude")
                    existing.longitude = data.get("longitude")
                    existing.postal = data.get("postal")
                    existing.flag_emoji = flag.get("emoji")
                    existing.flag_img = flag.get("img")
                    existing.asn = connection.get("asn")
                    existing.org = connection.get("org")
                    existing.isp = connection.get("isp")
                    existing.isp_domain = connection.get("domain")
                    existing.timezone_id = tz.get("id")
                    existing.timezone_abbr = tz.get("abbr")
                    existing.timezone_utc = tz.get("utc")
                    existing.updated_at = datetime.now(timezone.utc)
                    session.add(existing)
                else:
                    # Insert new record
                    row = cast(Any, ProxyGeolocationModel).from_geolocation_dict(
                        cast(dict[str, Any], data), proxy_url=proxy_url
                    )
                    session.add(row)

                session.commit()
                _logger.info(
                    f"[Geolocation] Saved to DB: {data.get('ip')} → "
                    f"{data.get('city')}, {data.get('country')}"
                )
        except Exception as exc:
            _logger.error(f"[Geolocation] Error saving to DB: {exc}")

    # ------------------------------------------------------------------ #
    # API fetch
    # ------------------------------------------------------------------ #

    async def _fetch_from_api(self, ip: str) -> GeolocationData | None:
        """Fetch geolocation from ipwho.is."""
        url = f"{self.base_url}/{ip}"

        for attempt in range(self.max_retries + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()

                            if not data.get("success", True):
                                _logger.warning(
                                    f"[Geolocation] API error for {ip}: "
                                    f"{data.get('message', 'Unknown')}"
                                )
                                return None

                            connection = data.get("connection", {})
                            _logger.info(
                                f"[Geolocation] API response for {ip}: "
                                f"{data.get('city', 'N/A')}, {data.get('country', 'N/A')} "
                                f"({connection.get('isp', 'N/A')})"
                            )
                            return data

                        elif response.status == 429:
                            wait_time = 2**attempt
                            _logger.warning(
                                f"[Geolocation] Rate limited for {ip}, "
                                f"waiting {wait_time}s (attempt {attempt + 1}/{self.max_retries})"
                            )
                            await asyncio.sleep(wait_time)
                            continue

                        else:
                            _logger.error(
                                f"[Geolocation] HTTP {response.status} for {ip}"
                            )
                            return None

            except asyncio.TimeoutError:
                _logger.error(f"[Geolocation] Timeout for {ip}")
                return None
            except Exception as exc:
                _logger.error(f"[Geolocation] Error for {ip}: {exc}")
                if attempt < self.max_retries:
                    await asyncio.sleep(1)
                    continue
                return None

        return None

    # ------------------------------------------------------------------ #
    # Cache management
    # ------------------------------------------------------------------ #

    def get_cache_size(self) -> int:
        self.remove_expired_entries()
        return len(self._cache)

    def _store_cache_entry(self, ip: str, data: GeolocationData) -> None:
        self.remove_expired_entries()
        self._cache.pop(ip, None)
        if not self.max_cache_entries:
            return
        while len(self._cache) >= self.max_cache_entries:
            self._cache.popitem(last=False)
        self._cache[ip] = _CacheEntry(data, ttl_seconds=self.cache_ttl_seconds)

    def remove_expired_entries(self) -> int:
        expired_keys = [ip for ip, entry in self._cache.items() if entry.is_expired()]
        for key in expired_keys:
            del self._cache[key]
        if expired_keys:
            _logger.info(f"[Geolocation] Removed {len(expired_keys)} expired entries")
        return len(expired_keys)


# -------------------------------------------------------------------- #
# Global singleton
# -------------------------------------------------------------------- #

_geolocation_service: IPGeolocationService | None = None


def get_geolocation_service() -> IPGeolocationService:
    """Get or create the global geolocation service instance."""
    global _geolocation_service
    if _geolocation_service is None:
        _geolocation_service = IPGeolocationService()
    return _geolocation_service


async def get_ip_geolocation(
    ip: str, *, proxy_url: str | None = None
) -> GeolocationData | None:
    """Convenience function to get geolocation for an IP.

    Checks memory cache → DB → API (saves result).
    """
    service = get_geolocation_service()
    return await service.get_geolocation(ip, proxy_url=proxy_url)
