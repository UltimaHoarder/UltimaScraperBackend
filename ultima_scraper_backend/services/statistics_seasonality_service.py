"""UTC calendar profiles with exposure-normalized rates over completed days."""

from __future__ import annotations

import asyncio
import calendar
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import ClassVar, Literal

from sqlalchemy import func, select, text
from ultima_scraper_db_archive import PostModel, UserModel

from ..database.factory import MultiSiteDatabaseManager
from .statistics_activity_service import (
    AccountDateField,
    ActivityMetric,
    StatisticsActivityService,
)

logger = logging.getLogger(__name__)
SeasonalityCycle = Literal["hour", "weekday", "monthday", "month"]


@dataclass(slots=True)
class SeasonalSite:
    site: str
    available: bool
    counts: list[int]
    availability_error: str | None = None
    first_date: str | None = None


@dataclass(slots=True)
class SeasonalityStatistics:
    metric: ActivityMetric
    cycle: SeasonalityCycle
    date_field: str
    start_date: str
    end_date: str
    calculated_at: str
    exposures: list[float]
    sites: list[SeasonalSite]


class StatisticsSeasonalityService:
    _cache: ClassVar[OrderedDict[tuple, tuple[float, SeasonalityStatistics]]] = (
        OrderedDict()
    )
    _inflight: ClassVar[dict[tuple, asyncio.Task[SeasonalityStatistics]]] = {}

    @staticmethod
    def exposures(start: date, end: date, cycle: SeasonalityCycle) -> list[float]:
        """Number of observed hours, weekdays, or calendar-month equivalents per bin."""
        bins = [0.0] * {"hour": 24, "weekday": 7, "monthday": 31, "month": 12}[cycle]
        if cycle == "hour":
            return [float((end - start).days)] * 24
        day = start
        while day < end:
            index = (
                day.weekday()
                if cycle == "weekday"
                else day.day - 1
                if cycle == "monthday"
                else day.month - 1
            )
            bins[index] += (
                1
                if cycle in ("weekday", "monthday")
                else 1 / calendar.monthrange(day.year, day.month)[1]
            )
            day += timedelta(days=1)
        return bins

    @staticmethod
    def count_statement(
        metric: ActivityMetric,
        cycle: SeasonalityCycle,
        account_date: AccountDateField,
        start: datetime | None,
        end: datetime,
    ):
        if metric == "accounts" and account_date != "join_date":
            raise ValueError("Account creation uses join_date.")
        model = UserModel if metric == "accounts" else PostModel
        timestamp = getattr(
            model, account_date if metric == "accounts" else "created_at"
        )
        utc = func.timezone("UTC", timestamp)
        bucket = (
            func.extract(
                {
                    "hour": "hour",
                    "weekday": "isodow",
                    "monthday": "day",
                    "month": "month",
                }[cycle],
                utc,
            )
            - (0 if cycle == "hour" else 1)
        ).label("bucket")
        statement = (
            select(bucket, func.count())
            .select_from(model)
            .where(
                timestamp > StatisticsActivityService.UNDATED_TIMESTAMP, timestamp < end
            )
        )
        if start is not None:
            statement = statement.where(timestamp >= start)
        return statement.group_by(bucket).order_by(bucket)

    @classmethod
    def _load_site(
        cls,
        site: str,
        metric: ActivityMetric,
        cycle: SeasonalityCycle,
        account_date: AccountDateField,
        start: datetime | None,
        end: datetime,
    ) -> SeasonalSite:
        with (
            MultiSiteDatabaseManager()
            .get_or_create_manager(site)
            .get_session() as session
        ):
            session.execute(text("SET TRANSACTION READ ONLY"))
            session.execute(text("SET LOCAL statement_timeout = '60s'"))
            first = start
            if start is None:
                first = None
                if metric == "accounts":
                    fast_stmt = (
                        select(UserModel.join_date)
                        .where(
                            UserModel.id.in_(
                                select(UserModel.id)
                                .where(
                                    UserModel.join_date > StatisticsActivityService.UNDATED_TIMESTAMP,
                                    UserModel.join_date < end,
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
                        StatisticsActivityService.first_timestamp_statement(
                            metric, account_date, end
                        )
                    ).scalar_one_or_none()
            rows = session.execute(
                cls.count_statement(metric, cycle, account_date, start, end)
            ).all()
        counts = [0] * {"hour": 24, "weekday": 7, "monthday": 31, "month": 12}[cycle]
        for bucket, count in rows:
            counts[int(bucket)] = int(count)
        return SeasonalSite(
            site,
            True,
            counts,
            first_date=first.astimezone(UTC).date().isoformat() if first else None,
        )

    @classmethod
    async def get_seasonality(
        cls,
        sites: list[str],
        metric: ActivityMetric,
        cycle: SeasonalityCycle,
        account_date: AccountDateField = "join_date",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> SeasonalityStatistics:
        if metric == "accounts" and account_date != "join_date":
            raise ValueError("Account creation uses join_date.")
        now = datetime.now(UTC)
        # Exclude today's unfinished day from both counts and exposure.
        end = min(
            min(end_date, now.date()) + timedelta(days=1) if end_date else now.date(),
            now.date(),
        )
        start = start_date
        if start is not None and (start >= end or start.year < 1900):
            raise ValueError(
                "Choose at least one completed day, starting in 1900 or later."
            )
        key = (tuple(sorted(sites)), metric, cycle, account_date, start, end)
        cached = cls._cache.get(key)
        if cached and cached[0] > time.monotonic():
            cls._cache.move_to_end(key)
            return cached[1]
        if key not in cls._inflight:

            async def build() -> SeasonalityStatistics:
                semaphore = asyncio.Semaphore(2)

                async def load(site: str) -> SeasonalSite:
                    try:
                        async with semaphore:
                            return await asyncio.to_thread(
                                cls._load_site,
                                site,
                                metric,
                                cycle,
                                account_date,
                                datetime.combine(start, datetime.min.time(), UTC)
                                if start
                                else None,
                                datetime.combine(end, datetime.min.time(), UTC),
                            )
                    except Exception:
                        logger.exception("Seasonality unavailable for %s", site)
                        return SeasonalSite(
                            site,
                            False,
                            [],
                            "Seasonality counts unavailable for this site.",
                        )

                try:
                    series = await asyncio.gather(
                        *(load(site) for site in sorted(sites))
                    )
                    effective_start = start or min(
                        (
                            date.fromisoformat(site.first_date)
                            for site in series
                            if site.available and site.first_date
                        ),
                        default=end - timedelta(days=1),
                    )
                    result = SeasonalityStatistics(
                        metric,
                        cycle,
                        account_date if metric == "accounts" else "created_at",
                        effective_start.isoformat(),
                        (end - timedelta(days=1)).isoformat(),
                        now.isoformat(),
                        cls.exposures(effective_start, end, cycle),
                        series,
                    )
                    if all(site.available for site in series):
                        cls._cache[key] = (time.monotonic() + 60, result)
                        while len(cls._cache) > 64:
                            cls._cache.popitem(last=False)
                    return result
                finally:
                    cls._inflight.pop(key, None)

            cls._inflight[key] = asyncio.create_task(build())
        return await asyncio.shield(cls._inflight[key])
