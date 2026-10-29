"""Timestamp-based account and post counts, separate from inventory deltas."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, ClassVar, Literal

from sqlalchemy import func, or_, select, text
from ultima_scraper_db_archive import PostModel, UserModel

from ..database.factory import MultiSiteDatabaseManager

logger = logging.getLogger(__name__)

ActivityMetric = Literal["accounts", "posts"]
ActivityInterval = Literal["minute", "hour", "day", "month", "year"]
ActivityRange = Literal["1h", "24h", "7d", "30d", "90d", "1y", "all", "custom"]
AccountDateField = Literal["join_date", "created_at"]

ACTIVITY_RANGES: dict[str, tuple[str, ...]] = {
    "minute": ("1h", "24h"),
    "hour": ("24h", "7d", "30d"),
    "day": ("7d", "30d", "90d", "1y"),
    "month": ("1y", "all"),
    "year": ("all",),
}


@dataclass(slots=True)
class ActivityPoint:
    bucket_started_at: str
    count: int


@dataclass(slots=True)
class ActivitySiteSeries:
    site: str
    available: bool
    availability_error: str | None
    undated_count: int | None
    points: list[ActivityPoint]
    total_count: int | None = None
    completed_count: int | None = None
    before_window_count: int | None = None


@dataclass(slots=True)
class ActivityStatistics:
    metric: ActivityMetric
    interval: ActivityInterval
    range: ActivityRange
    date_field: str
    start_at: str
    end_at: str
    calculated_at: str
    sites: list[ActivitySiteSeries]
    coverage_start_at: str | None = None
    coverage_end_at: str | None = None
    window_start_at: str | None = None
    window_end_at: str | None = None
    bucket_count: int = 0
    bucket_offset: int = 0
    completed_bucket_count: int = 0


class StatisticsActivityService:
    """Group archived records in UTC; cache bounded results and coalesce requests."""

    MAX_BUCKETS = 2_000
    # Legacy records use the minimum timestamp when the source date is unknown.
    UNDATED_TIMESTAMP = datetime.min.replace(tzinfo=UTC)
    CACHE_SECONDS = 60
    CACHE_SIZE = 64
    _cache: ClassVar[OrderedDict[tuple[Any, ...], tuple[float, ActivityStatistics]]] = (
        OrderedDict()
    )
    _inflight: ClassVar[dict[tuple[Any, ...], asyncio.Task[ActivityStatistics]]] = {}

    @staticmethod
    def floor_bucket(value: datetime, interval: ActivityInterval) -> datetime:
        value = value.astimezone(UTC).replace(second=0, microsecond=0)
        if interval != "minute":
            value = value.replace(minute=0)
        if interval in ("day", "month", "year"):
            value = value.replace(hour=0)
        if interval in ("month", "year"):
            value = value.replace(day=1)
        if interval == "year":
            value = value.replace(month=1)
        return value

    @staticmethod
    def advance_bucket(
        value: datetime, interval: ActivityInterval, count: int = 1
    ) -> datetime:
        if interval == "month":
            month = value.year * 12 + value.month - 1 + count
            return value.replace(year=month // 12, month=month % 12 + 1)
        if interval == "year":
            return value.replace(year=value.year + count)
        return value + timedelta(**{f"{interval}s": count})

    @classmethod
    def window(
        cls, interval: ActivityInterval, range_key: ActivityRange, now: datetime
    ) -> tuple[datetime | None, datetime]:
        if range_key != "all" and range_key not in ACTIVITY_RANGES[interval]:
            raise ValueError(
                f"Range '{range_key}' is not supported for {interval} buckets."
            )
        current = cls.floor_bucket(now, interval)
        end = cls.advance_bucket(current, interval)
        if range_key == "all":
            return None, end
        if interval == "month":
            return cls.advance_bucket(current, interval, -11), end
        units = {
            "1h": 60,
            "24h": 1_440,
            "7d": 10_080,
            "30d": 43_200,
            "90d": 129_600,
            "1y": 525_600,
        }
        divisor = {"minute": 1, "hour": 60, "day": 1_440}[interval]
        return cls.advance_bucket(
            current, interval, -(units[range_key] // divisor - 1)
        ), end

    @classmethod
    def date_window(
        cls, first: datetime, cutoff: datetime, interval: ActivityInterval
    ) -> tuple[datetime, datetime]:
        """Retain the requested resolution over the complete calendar span."""
        return cls.floor_bucket(first, interval), cls.advance_bucket(
            cls.floor_bucket(cutoff - timedelta(microseconds=1), interval), interval
        )

    @staticmethod
    def bucket_count(start: datetime, end: datetime, interval: ActivityInterval) -> int:
        if interval == "month":
            return (end.year - start.year) * 12 + end.month - start.month
        if interval == "year":
            return end.year - start.year
        return (
            int((end - start).total_seconds())
            // {"minute": 60, "hour": 3600, "day": 86400}[interval]
        )

    @staticmethod
    def first_timestamp_statement(
        metric: ActivityMetric, account_date: AccountDateField, cutoff: datetime
    ):
        model = UserModel if metric == "accounts" else PostModel
        timestamp = model.join_date if metric == "accounts" else model.created_at
        return (
            select(func.min(timestamp))
            .select_from(model)
            .where(
                timestamp > StatisticsActivityService.UNDATED_TIMESTAMP,
                timestamp < cutoff,
            )
        )

    @classmethod
    def _first_timestamp(
        cls,
        site: str,
        metric: ActivityMetric,
        account_date: AccountDateField,
        cutoff: datetime,
    ) -> datetime | None:
        with (
            MultiSiteDatabaseManager()
            .get_or_create_manager(site)
            .get_session() as session
        ):
            session.execute(text("SET TRANSACTION READ ONLY"))
            session.execute(text("SET LOCAL statement_timeout = '60s'"))
            first = None
            if metric == "accounts":
                fast_stmt = (
                    select(UserModel.join_date)
                    .where(
                        UserModel.id.in_(
                            select(UserModel.id)
                            .where(
                                UserModel.join_date > cls.UNDATED_TIMESTAMP,
                                UserModel.join_date < cutoff,
                            )
                            .order_by(UserModel.id.asc())
                            .limit(200)
                        )
                    )
                    .order_by(UserModel.join_date.asc())
                    .limit(1)
                )
                first = session.execute(fast_stmt).scalar_one_or_none()
            if first is None:
                first = session.execute(
                    cls.first_timestamp_statement(metric, account_date, cutoff)
                ).scalar_one_or_none()
        return first.astimezone(UTC) if first else None

    @staticmethod
    def count_statement(
        metric: ActivityMetric,
        interval: ActivityInterval,
        account_date: AccountDateField,
        start: datetime | None,
        now: datetime,
    ):
        if metric == "accounts" and account_date != "join_date":
            raise ValueError("Account creation uses join_date.")
        model = UserModel if metric == "accounts" else PostModel
        timestamp = getattr(
            model, account_date if metric == "accounts" else "created_at"
        )
        # Convert before truncation so the database/session timezone cannot change grouping.
        bucket = func.date_trunc(interval, func.timezone("UTC", timestamp)).label(
            "bucket"
        )
        statement = (
            select(bucket, func.count())
            .select_from(model)
            .where(
                timestamp > StatisticsActivityService.UNDATED_TIMESTAMP, timestamp < now
            )
        )
        if start is not None:
            statement = statement.where(timestamp >= start)
        return (
            statement.group_by(bucket)
            .order_by(bucket)
            .limit(StatisticsActivityService.MAX_BUCKETS + 1)
        )

    @classmethod
    def _load_site(
        cls,
        site: str,
        metric: ActivityMetric,
        interval: ActivityInterval,
        account_date: AccountDateField,
        start: datetime | None,
        now: datetime,
        *,
        coverage_start: datetime | None = None,
        coverage_end: datetime | None = None,
        complete_start: datetime | None = None,
    ) -> ActivitySiteSeries:
        manager = MultiSiteDatabaseManager().get_or_create_manager(site)
        with manager.get_session() as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            session.execute(text("SET LOCAL statement_timeout = '60s'"))
            rows = session.execute(
                cls.count_statement(metric, interval, account_date, start, now)
            ).all()
            span = len(rows)
            if start is None and rows:
                first_bucket = rows[0][0]
                current = cls.floor_bucket(now, interval)
                if interval == "month":
                    span = (
                        (current.year - first_bucket.year) * 12
                        + current.month
                        - first_bucket.month
                        + 1
                    )
                elif interval == "year":
                    span = current.year - first_bucket.year + 1
            if span > cls.MAX_BUCKETS:
                raise ValueError("Too many activity buckets; select a larger interval.")
            model = UserModel if metric == "accounts" else PostModel
            timestamp = model.join_date if metric == "accounts" else model.created_at
            undated = int(
                session.execute(
                    select(func.count())
                    .select_from(model)
                    .where(or_(timestamp.is_(None), timestamp <= cls.UNDATED_TIMESTAMP))
                ).scalar_one()
            )
            totals = (None, None, None)
            if coverage_end is not None:
                assert complete_start is not None and start is not None
                statement = (
                    select(
                        func.count(),
                        func.count().filter(
                            timestamp < cls.floor_bucket(coverage_end, interval),
                            timestamp >= complete_start,
                        ),
                        func.count().filter(timestamp < start),
                    )
                    .select_from(model)
                    .where(timestamp > cls.UNDATED_TIMESTAMP, timestamp < coverage_end)
                )
                if coverage_start is not None:
                    statement = statement.where(timestamp >= coverage_start)
                totals = tuple(int(value) for value in session.execute(statement).one())
        return ActivitySiteSeries(
            site=site,
            available=True,
            availability_error=None,
            undated_count=undated,
            points=[
                ActivityPoint(bucket.replace(tzinfo=UTC).isoformat(), int(count))
                for bucket, count in rows
            ],
            total_count=totals[0],
            completed_count=totals[1],
            before_window_count=totals[2],
        )

    @classmethod
    async def get_activity(
        cls,
        sites: list[str],
        metric: ActivityMetric,
        interval: ActivityInterval,
        range_key: ActivityRange,
        account_date: AccountDateField = "join_date",
        refresh: bool = False,
        start_date: date | None = None,
        end_date: date | None = None,
        bucket_offset: int | None = None,
    ) -> ActivityStatistics:
        if metric == "accounts" and interval not in ("day", "month", "year"):
            raise ValueError("Accounts support day, month, and year buckets.")
        if metric == "accounts" and account_date != "join_date":
            raise ValueError("Account creation uses join_date.")
        if bucket_offset is not None and bucket_offset < 0:
            raise ValueError("Bucket offset must be nonnegative.")
        now = datetime.now(UTC)
        coverage_start = None
        cutoff = now
        if range_key == "custom":
            if start_date is None or end_date is None or start_date > end_date:
                raise ValueError(
                    "Custom ranges require a start date on or before the end date."
                )
            coverage_start = datetime.combine(start_date, datetime.min.time(), UTC)
            if coverage_start >= now:
                raise ValueError("The start date must be in the past.")
            cutoff = min(
                datetime.combine(min(end_date, now.date()), datetime.min.time(), UTC)
                + timedelta(days=1),
                now,
            )
            start, end = cls.date_window(coverage_start, cutoff, interval)
        else:
            if start_date is not None or end_date is not None:
                raise ValueError("Date bounds require the custom range.")
            start, end = cls.window(interval, range_key, now)
        key = (
            tuple(sorted(sites)),
            metric,
            interval,
            range_key,
            account_date if metric == "accounts" else "created_at",
            start_date,
            end_date,
            bucket_offset,
        )
        cached = cls._cache.get(key)
        if not refresh and cached and cached[0] > time.monotonic():
            cls._cache.move_to_end(key)
            return cached[1]
        if key not in cls._inflight:

            async def build() -> ActivityStatistics:
                semaphore = asyncio.Semaphore(2)
                try:
                    effective_start, effective_end = start, end
                    first_errors: set[str] = set()
                    if range_key == "all":

                        async def first_for_site(site: str) -> datetime | None:
                            try:
                                async with semaphore:
                                    return await asyncio.to_thread(
                                        cls._first_timestamp,
                                        site,
                                        metric,
                                        account_date,
                                        cutoff,
                                    )
                            except Exception:
                                logger.exception(
                                    "Failed to load first %s timestamp for %s",
                                    metric,
                                    site,
                                )
                                first_errors.add(site)
                                return None

                        first_dates = await asyncio.gather(
                            *(first_for_site(site) for site in sorted(sites))
                        )
                        earliest = min(
                            (first for first in first_dates if first is not None),
                            default=cls.floor_bucket(now, interval),
                        )
                        effective_start, effective_end = cls.date_window(
                            earliest, cutoff, interval
                        )
                    assert effective_start is not None
                    total_buckets = cls.bucket_count(
                        effective_start, effective_end, interval
                    )
                    last_offset = max(0, total_buckets - cls.MAX_BUCKETS)
                    offset = (
                        last_offset
                        if bucket_offset is None
                        else min(bucket_offset, last_offset)
                    )
                    window_start = cls.advance_bucket(effective_start, interval, offset)
                    window_end = min(
                        cls.advance_bucket(
                            window_start, interval, min(cls.MAX_BUCKETS, total_buckets)
                        ),
                        effective_end,
                    )
                    full_coverage_start = coverage_start or effective_start
                    complete_start = effective_start
                    if full_coverage_start > complete_start:
                        complete_start = cls.advance_bucket(complete_start, interval)
                    complete_end = cls.floor_bucket(cutoff, interval)
                    complete_buckets = max(
                        0, cls.bucket_count(complete_start, complete_end, interval)
                    )

                    async def load(site: str) -> ActivitySiteSeries:
                        if site in first_errors:
                            return ActivitySiteSeries(
                                site,
                                False,
                                "Activity counts could not be loaded for this site.",
                                None,
                                [],
                            )
                        try:
                            async with semaphore:
                                return await asyncio.to_thread(
                                    cls._load_site,
                                    site,
                                    metric,
                                    interval,
                                    account_date,
                                    max(window_start, full_coverage_start),
                                    min(window_end, cutoff),
                                    coverage_start=full_coverage_start,
                                    coverage_end=cutoff,
                                    complete_start=complete_start,
                                )
                        except Exception:
                            logger.exception(
                                "Failed to load %s activity for %s", metric, site
                            )
                            return ActivitySiteSeries(
                                site,
                                False,
                                "Activity counts could not be loaded for this site.",
                                None,
                                [],
                            )

                    series = await asyncio.gather(
                        *(load(site) for site in sorted(sites))
                    )
                    result = ActivityStatistics(
                        metric,
                        interval,
                        range_key,
                        account_date if metric == "accounts" else "created_at",
                        effective_start.isoformat(),
                        effective_end.isoformat(),
                        now.isoformat(),
                        series,
                        coverage_start.isoformat() if coverage_start else None,
                        cutoff.isoformat(),
                        window_start.isoformat(),
                        window_end.isoformat(),
                        total_buckets,
                        offset,
                        complete_buckets,
                    )
                    if all(item.available for item in series):
                        cls._cache[key] = (time.monotonic() + cls.CACHE_SECONDS, result)
                        cls._cache.move_to_end(key)
                        while len(cls._cache) > cls.CACHE_SIZE:
                            cls._cache.popitem(last=False)
                    return result
                finally:
                    cls._inflight.pop(key, None)

            cls._inflight[key] = asyncio.create_task(build())
        return await asyncio.shield(cls._inflight[key])
