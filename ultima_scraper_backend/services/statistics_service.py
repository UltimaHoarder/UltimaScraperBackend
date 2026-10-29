"""Daily site statistics and profile size estimation helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, ClassVar, Literal, cast

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session
from sqlmodel import select as sql_select
from ultima_scraper_db_archive import (
    MassMessageModel,
    MediaModel,
    MessageModel,
    PostModel,
    StatisticsModel,
    StoryModel,
    UserModel,
)

from ultima_scraper_backend.database.factory import MultiSiteDatabaseManager
from ultima_scraper_core import runtime_env_value

logger = logging.getLogger(__name__)

SizeSource = Literal["actual", "estimated", "unknown"]
SnapshotSource = Literal["current", "latest", "none"]
BuildState = Literal["building", "failed"]
StatisticsHistoryRange = Literal["7d", "30d", "90d", "all"]
_STATISTICS_AVAILABILITY_UNSET = object()


@dataclass(slots=True)
class DailyStatisticsSnapshot:
    """Serializable daily snapshot stored in PostgreSQL and Redis."""

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

    def to_cache_payload(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_cache_payload(cls, payload: str) -> "DailyStatisticsSnapshot":
        data = json.loads(payload)
        return cls(**data)


@dataclass(slots=True)
class SiteStatisticsAvailability:
    """Current availability state for a site's statistics snapshot."""

    site: str
    target_bucket_date: str
    snapshot: DailyStatisticsSnapshot | None
    snapshot_source: SnapshotSource
    is_current: bool
    is_building: bool
    build_status: StatisticsBuildStatus | None = None

    @property
    def has_snapshot(self) -> bool:
        return self.snapshot is not None


@dataclass(slots=True)
class ResolvedUserSize:
    """Resolved size values for profile responses."""

    size: int
    display_size: int
    estimated_size: int | None
    size_source: SizeSource
    statistics_bucket_date: str | None = None


@dataclass(slots=True)
class KnownUserMediaStats:
    """Known on-disk media coverage for a performer."""

    total_size: int = 0
    total_count: int = 0
    image_count: int = 0
    video_count: int = 0
    audio_count: int = 0


@dataclass(slots=True)
class StatisticsBuildStatus:
    """Ephemeral build status for background statistics generation."""

    site: str
    target_bucket_date: str
    state: BuildState
    progress_ratio: float | None = None
    processed_rows: int = 0
    total_rows_estimate: int | None = None
    min_id: int | None = None
    max_id: int | None = None
    last_processed_id: int | None = None
    started_at: str | None = None
    updated_at: str | None = None
    message: str | None = None
    error: str | None = None


class StatisticsService:
    """Computes and caches daily site-level media statistics."""

    CACHE_KEY_PREFIX = "ultimascraper:statistics"
    BUILD_LOCK_TTL_FLOOR_SECONDS = 300
    BUILD_FAILURE_RETRY_SECONDS = 900
    CHUNK_BATCH_SIZE = 25_000  # Existing fallback; explicit budgets have no floor.
    _build_tasks: ClassVar[dict[str, asyncio.Task[None]]] = {}
    _build_registry_lock: ClassVar[threading.Lock] = threading.Lock()
    _local_build_locks: ClassVar[set[str]] = set()
    _build_statuses: ClassVar[dict[str, StatisticsBuildStatus]] = {}

    @staticmethod
    def utc_day_window(
        now: datetime | None = None,
    ) -> tuple[date, datetime, datetime]:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        bucket_started_at = current.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        bucket_ends_at = bucket_started_at + timedelta(days=1)
        return bucket_started_at.date(), bucket_started_at, bucket_ends_at

    @staticmethod
    def seconds_until_next_utc_midnight(now: datetime | None = None) -> int:
        _, _, bucket_ends_at = StatisticsService.utc_day_window(now)
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        return max(1, int((bucket_ends_at - current).total_seconds()))

    @classmethod
    def history_date_window(
        cls,
        range_key: StatisticsHistoryRange,
        now: datetime | None = None,
    ) -> tuple[date | None, date]:
        end_date, _, _ = cls.utc_day_window(now)
        days = {"7d": 7, "30d": 30, "90d": 90}.get(range_key)
        start_date = end_date - timedelta(days=days - 1) if days else None
        return start_date, end_date

    @classmethod
    def _bucket_cache_key(cls, site: str, bucket_date: date) -> str:
        return f"{cls.CACHE_KEY_PREFIX}:{site.lower()}:{bucket_date.isoformat()}"

    @classmethod
    def _latest_cache_key(cls, site: str) -> str:
        return f"{cls.CACHE_KEY_PREFIX}:{site.lower()}:latest"

    @classmethod
    def _build_task_key(cls, site: str, bucket_date: date) -> str:
        return f"{site.lower()}:{bucket_date.isoformat()}"

    @classmethod
    def _build_lock_key(cls, site: str, bucket_date: date) -> str:
        return f"{cls.CACHE_KEY_PREFIX}:build:{site.lower()}:{bucket_date.isoformat()}"

    @classmethod
    def _build_lock_ttl(cls, now: datetime | None = None) -> int:
        return max(
            cls.BUILD_LOCK_TTL_FLOOR_SECONDS,
            cls.seconds_until_next_utc_midnight(now) + 300,
        )

    @classmethod
    def _get_local_build_status(
        cls,
        site: str,
        bucket_date: date,
    ) -> StatisticsBuildStatus | None:
        task_key = cls._build_task_key(site, bucket_date)
        with cls._build_registry_lock:
            return cls._build_statuses.get(task_key)

    @classmethod
    def _set_local_build_status(
        cls,
        site: str,
        bucket_date: date,
        status: StatisticsBuildStatus | None,
    ) -> None:
        task_key = cls._build_task_key(site, bucket_date)
        with cls._build_registry_lock:
            if status is None:
                cls._build_statuses.pop(task_key, None)
            else:
                cls._build_statuses[task_key] = status

    @classmethod
    def _build_failure_retry_due(
        cls,
        status: StatisticsBuildStatus,
        now: datetime | None = None,
    ) -> bool:
        if status.state != "failed":
            return True
        if not status.updated_at:
            return True
        try:
            updated_at = datetime.fromisoformat(status.updated_at)
        except ValueError:
            return True
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        return (current - updated_at).total_seconds() >= cls.BUILD_FAILURE_RETRY_SECONDS

    @staticmethod
    def _redis_client(redis_manager: object | None) -> Any | None:
        if redis_manager is None or not getattr(redis_manager, "is_connected", False):
            return None
        return getattr(redis_manager, "client", None)

    @staticmethod
    def _decode_redis_value(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        if isinstance(value, str):
            return value
        return str(value)

    @classmethod
    async def _get_cached_snapshot(
        cls,
        site: str,
        bucket_date: date,
        redis_manager: object | None,
    ) -> DailyStatisticsSnapshot | None:
        client = cls._redis_client(redis_manager)
        if client is None:
            return None

        for key in (
            cls._bucket_cache_key(site, bucket_date),
            cls._latest_cache_key(site),
        ):
            try:
                raw = await client.get(key)
            except Exception as exc:
                logger.debug("Failed reading statistics cache key %s: %s", key, exc)
                return None

            payload = cls._decode_redis_value(raw)
            if not payload:
                continue

            try:
                snapshot = DailyStatisticsSnapshot.from_cache_payload(payload)
            except Exception as exc:
                logger.debug("Failed decoding statistics cache key %s: %s", key, exc)
                continue

            if (
                key.endswith(":latest")
                or snapshot.bucket_date == bucket_date.isoformat()
            ):
                return snapshot
        return None

    @classmethod
    async def _cache_snapshot(
        cls,
        snapshot: DailyStatisticsSnapshot,
        redis_manager: object | None,
    ) -> None:
        client = cls._redis_client(redis_manager)
        if client is None:
            return

        ttl = cls.seconds_until_next_utc_midnight()
        payload = snapshot.to_cache_payload()
        bucket_date = date.fromisoformat(snapshot.bucket_date)
        keys = (
            cls._bucket_cache_key(snapshot.site, bucket_date),
            cls._latest_cache_key(snapshot.site),
        )
        for key in keys:
            try:
                await client.set(key, payload, ex=ttl)
            except Exception as exc:
                logger.debug("Failed caching statistics key %s: %s", key, exc)
                return

    @staticmethod
    def _unwrap_scalar(value: Any) -> Any:
        if value is None:
            return None
        mapping = cast(Any, getattr(value, "_mapping", None))
        if mapping:
            try:
                return next(iter(mapping.values()))
            except Exception:
                return value
        return value

    @classmethod
    def _coerce_int(cls, value: Any) -> int:
        value = cls._unwrap_scalar(value)
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _coerce_float(cls, value: Any) -> float:
        value = cls._unwrap_scalar(value)
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _coerce_optional_int(cls, value: Any) -> int | None:
        value = cls._unwrap_scalar(value)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _has_inventory_metrics(snapshot: DailyStatisticsSnapshot | None) -> bool:
        return snapshot is not None and all(
            value is not None
            for value in (
                snapshot.user_record_count,
                snapshot.performer_profile_count,
                snapshot.active_user_count,
                snapshot.archived_content_count,
                snapshot.post_count,
                snapshot.message_count,
                snapshot.story_count,
                snapshot.mass_message_count,
                snapshot.downloaded_media_count,
            )
        )

    @classmethod
    def _from_model(
        cls,
        site: str,
        row: StatisticsModel,
    ) -> DailyStatisticsSnapshot:
        return DailyStatisticsSnapshot(
            site=site.lower(),
            bucket_date=row.bucket_date.isoformat(),
            bucket_started_at=row.bucket_started_at.astimezone(
                timezone.utc
            ).isoformat(),
            bucket_ends_at=row.bucket_ends_at.astimezone(timezone.utc).isoformat(),
            calculated_at=row.calculated_at.astimezone(timezone.utc).isoformat(),
            total_media_count=cls._coerce_int(row.total_media_count),
            sized_media_count=cls._coerce_int(row.sized_media_count),
            zero_size_media_count=cls._coerce_int(row.zero_size_media_count),
            image_media_count=cls._coerce_int(row.image_media_count),
            image_sized_count=cls._coerce_int(row.image_sized_count),
            video_media_count=cls._coerce_int(row.video_media_count),
            video_sized_count=cls._coerce_int(row.video_sized_count),
            audio_media_count=cls._coerce_int(row.audio_media_count),
            audio_sized_count=cls._coerce_int(row.audio_sized_count),
            other_media_count=cls._coerce_int(row.other_media_count),
            other_sized_count=cls._coerce_int(row.other_sized_count),
            total_known_bytes=cls._coerce_int(row.total_known_bytes),
            avg_media_size_bytes=cls._coerce_int(row.avg_media_size_bytes),
            avg_image_size_bytes=cls._coerce_int(row.avg_image_size_bytes),
            avg_video_size_bytes=cls._coerce_int(row.avg_video_size_bytes),
            avg_audio_size_bytes=cls._coerce_int(row.avg_audio_size_bytes),
            avg_other_size_bytes=cls._coerce_int(row.avg_other_size_bytes),
            size_coverage_ratio=cls._coerce_float(row.size_coverage_ratio),
            user_record_count=cls._coerce_optional_int(
                getattr(row, "user_record_count", None)
            ),
            performer_profile_count=cls._coerce_optional_int(
                getattr(row, "performer_profile_count", None)
            ),
            active_user_count=cls._coerce_optional_int(
                getattr(row, "active_user_count", None)
            ),
            archived_content_count=cls._coerce_optional_int(
                getattr(row, "archived_content_count", None)
            ),
            post_count=cls._coerce_optional_int(
                getattr(row, "post_count", None)
            ),
            message_count=cls._coerce_optional_int(
                getattr(row, "message_count", None)
            ),
            story_count=cls._coerce_optional_int(
                getattr(row, "story_count", None)
            ),
            mass_message_count=cls._coerce_optional_int(
                getattr(row, "mass_message_count", None)
            ),
            downloaded_media_count=cls._coerce_optional_int(
                getattr(row, "downloaded_media_count", None)
            ),
        )

    @staticmethod
    def _unwrap_snapshot_model(value: Any) -> Any | None:
        candidate = StatisticsService._unwrap_scalar(value)
        if (
            candidate is not None
            and hasattr(candidate, "bucket_date")
            and hasattr(candidate, "calculated_at")
        ):
            return candidate
        if hasattr(value, "_mapping"):
            for mapped_value in getattr(value, "_mapping").values():
                if hasattr(mapped_value, "bucket_date") and hasattr(
                    mapped_value, "calculated_at"
                ):
                    return mapped_value
        return None

    @classmethod
    def _classify_category(cls, category: Any) -> str:
        if isinstance(category, str):
            normalized = category.strip().lower()
            if normalized in {"image", "video", "audio"}:
                return normalized
        return "other"

    @classmethod
    def _finalize_snapshot_from_totals(
        cls,
        *,
        site: str,
        bucket_date: date,
        bucket_started_at: datetime,
        bucket_ends_at: datetime,
        calculated_at: datetime,
        totals: dict[str, int],
    ) -> DailyStatisticsSnapshot:
        def _average(total_bytes: int, sized_count: int) -> int:
            if sized_count <= 0:
                return 0
            return int(round(total_bytes / sized_count))

        total_media_count = totals["total_media_count"]
        sized_media_count = totals["sized_media_count"]
        image_sized_count = totals["image_sized_count"]
        video_sized_count = totals["video_sized_count"]
        audio_sized_count = totals["audio_sized_count"]
        other_sized_count = totals["other_sized_count"]

        coverage = (
            round(sized_media_count / total_media_count, 6)
            if total_media_count > 0
            else 0.0
        )
        return DailyStatisticsSnapshot(
            site=site.lower(),
            bucket_date=bucket_date.isoformat(),
            bucket_started_at=bucket_started_at.isoformat(),
            bucket_ends_at=bucket_ends_at.isoformat(),
            calculated_at=calculated_at.isoformat(),
            total_media_count=total_media_count,
            sized_media_count=sized_media_count,
            zero_size_media_count=totals["zero_size_media_count"],
            image_media_count=totals["image_media_count"],
            image_sized_count=image_sized_count,
            video_media_count=totals["video_media_count"],
            video_sized_count=video_sized_count,
            audio_media_count=totals["audio_media_count"],
            audio_sized_count=audio_sized_count,
            other_media_count=totals["other_media_count"],
            other_sized_count=other_sized_count,
            total_known_bytes=totals["total_known_bytes"],
            avg_media_size_bytes=_average(
                totals["total_known_bytes"], sized_media_count
            ),
            avg_image_size_bytes=_average(
                totals["image_known_bytes"], image_sized_count
            ),
            avg_video_size_bytes=_average(
                totals["video_known_bytes"], video_sized_count
            ),
            avg_audio_size_bytes=_average(
                totals["audio_known_bytes"], audio_sized_count
            ),
            avg_other_size_bytes=_average(
                totals["other_known_bytes"], other_sized_count
            ),
            size_coverage_ratio=coverage,
            user_record_count=totals["user_record_count"],
            performer_profile_count=totals["performer_profile_count"],
            active_user_count=totals.get("active_user_count"),
            archived_content_count=totals["archived_content_count"],
            post_count=totals.get("post_count"),
            message_count=totals.get("message_count"),
            story_count=totals.get("story_count"),
            mass_message_count=totals.get("mass_message_count"),
            downloaded_media_count=totals["downloaded_media_count"],
        )

    @classmethod
    def _inventory_totals(cls, session: Session) -> dict[str, int]:
        user_record_count = cls._coerce_int(
            session.exec(sql_select(func.count(UserModel.id))).one()
        )
        performer_profile_count = cls._coerce_int(
            session.exec(
                sql_select(func.count(UserModel.id)).where(
                    UserModel.performer.is_(True)
                )
            ).one()
        )
        active_user_count = cls._coerce_int(
            session.exec(
                sql_select(func.count(UserModel.id)).where(
                    UserModel.active.is_(True)
                )
            ).one()
        )
        post_count = cls._coerce_int(
            session.exec(sql_select(func.count(PostModel.id))).one()
        )
        message_count = cls._coerce_int(
            session.exec(sql_select(func.count(MessageModel.id))).one()
        )
        story_count = cls._coerce_int(
            session.exec(sql_select(func.count(StoryModel.id))).one()
        )
        mass_message_count = cls._coerce_int(
            session.exec(sql_select(func.count(MassMessageModel.id))).one()
        )
        archived_content_count = (
            post_count + message_count + story_count + mass_message_count
        )
        return {
            "user_record_count": user_record_count,
            "performer_profile_count": performer_profile_count,
            "active_user_count": active_user_count,
            "archived_content_count": archived_content_count,
            "post_count": post_count,
            "message_count": message_count,
            "story_count": story_count,
            "mass_message_count": mass_message_count,
            "downloaded_media_count": 0,
        }

    @classmethod
    def _chunk_batch_size(cls) -> int:
        raw = runtime_env_value("ULTIMA_STATS_BATCH_SIZE")
        if raw is None or not raw.strip():
            value = cls.CHUNK_BATCH_SIZE
        else:
            try:
                value = int(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "ULTIMA_STATS_BATCH_SIZE must be a positive integer"
                ) from exc
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("ULTIMA_STATS_BATCH_SIZE must be a positive integer")
        return value

    @classmethod
    def _compute_snapshot(
        cls,
        session: Session,
        site: str,
        bucket_date: date,
        bucket_started_at: datetime,
        bucket_ends_at: datetime,
        progress_callback: Callable[[StatisticsBuildStatus], None] | None = None,
    ) -> DailyStatisticsSnapshot:
        batch_size = cls._chunk_batch_size()
        inventory_totals = cls._inventory_totals(session)
        range_row = session.exec(
            sql_select(func.min(MediaModel.id), func.max(MediaModel.id))
        ).one()
        min_id = cls._coerce_int(range_row[0])
        max_id = cls._coerce_int(range_row[1])
        calculated_at = datetime.now(timezone.utc)

        if min_id <= 0 or max_id <= 0:
            return cls._finalize_snapshot_from_totals(
                site=site,
                bucket_date=bucket_date,
                bucket_started_at=bucket_started_at,
                bucket_ends_at=bucket_ends_at,
                calculated_at=calculated_at,
                totals={
                    "total_media_count": 0,
                    "sized_media_count": 0,
                    "zero_size_media_count": 0,
                    "image_media_count": 0,
                    "image_sized_count": 0,
                    "video_media_count": 0,
                    "video_sized_count": 0,
                    "audio_media_count": 0,
                    "audio_sized_count": 0,
                    "other_media_count": 0,
                    "other_sized_count": 0,
                    "total_known_bytes": 0,
                    "image_known_bytes": 0,
                    "video_known_bytes": 0,
                    "audio_known_bytes": 0,
                    "other_known_bytes": 0,
                    **inventory_totals,
                },
            )

        totals = {
            "total_media_count": 0,
            "sized_media_count": 0,
            "zero_size_media_count": 0,
            "image_media_count": 0,
            "image_sized_count": 0,
            "video_media_count": 0,
            "video_sized_count": 0,
            "audio_media_count": 0,
            "audio_sized_count": 0,
            "other_media_count": 0,
            "other_sized_count": 0,
            "total_known_bytes": 0,
            "image_known_bytes": 0,
            "video_known_bytes": 0,
            "audio_known_bytes": 0,
            "other_known_bytes": 0,
            **inventory_totals,
        }

        last_id = min_id - 1
        while True:
            batch_rows = session.exec(
                sql_select(
                    MediaModel.id,
                    MediaModel.category,
                    MediaModel.size,
                    MediaModel.downloaded,
                )
                .where(MediaModel.id > last_id, MediaModel.id <= max_id)
                .order_by(MediaModel.id.asc())
                .limit(batch_size)
            ).all()
            if not batch_rows:
                break

            for batch_row in batch_rows:
                media_id = cls._coerce_int(batch_row[0])
                category = cls._classify_category(batch_row[1])
                size = cls._coerce_int(batch_row[2])
                downloaded = bool(batch_row[3])

                totals["total_media_count"] += 1
                totals[f"{category}_media_count"] += 1
                if downloaded:
                    totals["downloaded_media_count"] += 1

                if size > 0:
                    totals["sized_media_count"] += 1
                    totals["total_known_bytes"] += size
                    totals[f"{category}_sized_count"] += 1
                    totals[f"{category}_known_bytes"] += size
                else:
                    totals["zero_size_media_count"] += 1

                last_id = media_id

            if progress_callback is not None:
                span = max(1, max_id - min_id)
                progress_callback(
                    StatisticsBuildStatus(
                        site=site.lower(),
                        target_bucket_date=bucket_date.isoformat(),
                        state="building",
                        progress_ratio=min(1.0, max(0.0, (last_id - min_id) / span)),
                        processed_rows=totals["total_media_count"],
                        total_rows_estimate=None,
                        min_id=min_id,
                        max_id=max_id,
                        last_processed_id=last_id,
                        started_at=calculated_at.isoformat(),
                        updated_at=datetime.now(timezone.utc).isoformat(),
                        message=(
                            f"Processed {totals['total_media_count']:,} media rows"
                        ),
                        error=None,
                    )
                )

        return cls._finalize_snapshot_from_totals(
            site=site,
            bucket_date=bucket_date,
            bucket_started_at=bucket_started_at,
            bucket_ends_at=bucket_ends_at,
            calculated_at=datetime.now(timezone.utc),
            totals=totals,
        )

    @classmethod
    def _upsert_snapshot(
        cls,
        session: Session,
        snapshot: DailyStatisticsSnapshot,
    ) -> StatisticsModel:
        values = {
            "bucket_date": date.fromisoformat(snapshot.bucket_date),
            "bucket_started_at": datetime.fromisoformat(snapshot.bucket_started_at),
            "bucket_ends_at": datetime.fromisoformat(snapshot.bucket_ends_at),
            "calculated_at": datetime.fromisoformat(snapshot.calculated_at),
            "total_media_count": snapshot.total_media_count,
            "sized_media_count": snapshot.sized_media_count,
            "zero_size_media_count": snapshot.zero_size_media_count,
            "image_media_count": snapshot.image_media_count,
            "image_sized_count": snapshot.image_sized_count,
            "video_media_count": snapshot.video_media_count,
            "video_sized_count": snapshot.video_sized_count,
            "audio_media_count": snapshot.audio_media_count,
            "audio_sized_count": snapshot.audio_sized_count,
            "other_media_count": snapshot.other_media_count,
            "other_sized_count": snapshot.other_sized_count,
            "total_known_bytes": snapshot.total_known_bytes,
            "avg_media_size_bytes": snapshot.avg_media_size_bytes,
            "avg_image_size_bytes": snapshot.avg_image_size_bytes,
            "avg_video_size_bytes": snapshot.avg_video_size_bytes,
            "avg_audio_size_bytes": snapshot.avg_audio_size_bytes,
            "avg_other_size_bytes": snapshot.avg_other_size_bytes,
            "size_coverage_ratio": snapshot.size_coverage_ratio,
            "user_record_count": snapshot.user_record_count,
            "performer_profile_count": snapshot.performer_profile_count,
            "active_user_count": snapshot.active_user_count,
            "archived_content_count": snapshot.archived_content_count,
            "post_count": snapshot.post_count,
            "message_count": snapshot.message_count,
            "story_count": snapshot.story_count,
            "mass_message_count": snapshot.mass_message_count,
            "downloaded_media_count": snapshot.downloaded_media_count,
        }
        stmt = (
            pg_insert(StatisticsModel)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[StatisticsModel.bucket_date],
                set_=values,
            )
            .returning(StatisticsModel.id)
        )
        persisted_id = int(session.connection().execute(stmt).scalar_one())
        session.commit()
        row = session.get(StatisticsModel, persisted_id)
        if row is None:
            raise RuntimeError(
                f"Failed to load statistics row {persisted_id} after upsert"
            )
        return row

    @classmethod
    def _load_latest_snapshot_sync(
        cls,
        site: str,
    ) -> DailyStatisticsSnapshot | None:
        site_key = site.strip().lower()
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_key)
        with site_db.get_session() as session:
            latest_row = session.exec(
                sql_select(StatisticsModel).order_by(StatisticsModel.bucket_date.desc())
            ).first()
            latest = cls._unwrap_snapshot_model(latest_row)
            if latest is None:
                return None
            return cls._from_model(site_key, latest)

    @classmethod
    def _load_statistics_history_sync(
        cls,
        site: str,
        *,
        start_date: date | None,
        end_date: date,
    ) -> list[DailyStatisticsSnapshot]:
        site_key = site.strip().lower()
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_key)
        with site_db.get_session() as session:
            statement = sql_select(StatisticsModel).where(
                StatisticsModel.bucket_date <= end_date
            )
            if start_date is not None:
                statement = statement.where(StatisticsModel.bucket_date >= start_date)
            rows = session.exec(
                statement.order_by(StatisticsModel.bucket_date.asc())
            ).all()
            return [cls._from_model(site_key, row) for row in rows]

    @classmethod
    async def get_statistics_history(
        cls,
        site: str,
        *,
        range_key: StatisticsHistoryRange = "30d",
        now: datetime | None = None,
    ) -> list[DailyStatisticsSnapshot]:
        start_date, end_date = cls.history_date_window(range_key, now)
        return await asyncio.to_thread(
            cls._load_statistics_history_sync,
            site,
            start_date=start_date,
            end_date=end_date,
        )

    @classmethod
    def _load_or_compute_daily_statistics_sync(
        cls,
        site: str,
        *,
        bucket_date: date,
        bucket_started_at: datetime,
        bucket_ends_at: datetime,
        force_refresh: bool,
        progress_callback: Callable[[StatisticsBuildStatus], None] | None = None,
    ) -> DailyStatisticsSnapshot:
        site_key = site.strip().lower()
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site_key)
        with site_db.get_session() as session:
            if not force_refresh:
                existing_row = session.exec(
                    sql_select(StatisticsModel).where(
                        StatisticsModel.bucket_date == bucket_date
                    )
                ).first()
                existing = cls._unwrap_snapshot_model(existing_row)
                if existing is not None:
                    snapshot = cls._from_model(site_key, existing)
                    if cls._has_inventory_metrics(snapshot):
                        return snapshot

            snapshot = cls._compute_snapshot(
                session,
                site_key,
                bucket_date,
                bucket_started_at,
                bucket_ends_at,
                progress_callback=progress_callback,
            )
            persisted = cls._upsert_snapshot(session, snapshot)
            return cls._from_model(site_key, persisted)

    @classmethod
    async def _acquire_build_lock(
        cls,
        site: str,
        bucket_date: date,
        redis_manager: object | None,
        *,
        now: datetime | None = None,
    ) -> bool:
        site_key = site.strip().lower()
        task_key = cls._build_task_key(site_key, bucket_date)
        client = cls._redis_client(redis_manager)
        if client is not None:
            try:
                return bool(
                    await client.set(
                        cls._build_lock_key(site_key, bucket_date),
                        "1",
                        nx=True,
                        ex=cls._build_lock_ttl(now),
                    )
                )
            except Exception as exc:
                logger.debug(
                    "Failed acquiring statistics build lock for %s/%s: %s",
                    site_key,
                    bucket_date,
                    exc,
                )
                return False

        with cls._build_registry_lock:
            if task_key in cls._local_build_locks:
                return False
            cls._local_build_locks.add(task_key)
            return True

    @classmethod
    async def _release_build_lock(
        cls,
        site: str,
        bucket_date: date,
        redis_manager: object | None,
    ) -> None:
        site_key = site.strip().lower()
        task_key = cls._build_task_key(site_key, bucket_date)
        client = cls._redis_client(redis_manager)
        if client is not None:
            try:
                await client.delete(cls._build_lock_key(site_key, bucket_date))
            except Exception as exc:
                logger.debug(
                    "Failed releasing statistics build lock for %s/%s: %s",
                    site_key,
                    bucket_date,
                    exc,
                )

        with cls._build_registry_lock:
            cls._local_build_locks.discard(task_key)

    @classmethod
    async def _is_build_in_progress(
        cls,
        site: str,
        bucket_date: date,
        redis_manager: object | None,
    ) -> bool:
        site_key = site.strip().lower()
        task_key = cls._build_task_key(site_key, bucket_date)
        with cls._build_registry_lock:
            status = cls._build_statuses.get(task_key)
            if status is not None and status.state == "building":
                return True
            local_task = cls._build_tasks.get(task_key)
            if local_task is not None and not local_task.done():
                return True
            if task_key in cls._local_build_locks:
                return True

        client = cls._redis_client(redis_manager)
        if client is None:
            return False

        try:
            raw = await client.get(cls._build_lock_key(site_key, bucket_date))
        except Exception as exc:
            logger.debug(
                "Failed checking statistics build lock for %s/%s: %s",
                site_key,
                bucket_date,
                exc,
            )
            return False
        return bool(cls._decode_redis_value(raw))

    @classmethod
    async def ensure_daily_statistics_started(
        cls,
        site: str,
        *,
        redis_manager: object | None = None,
        force_refresh: bool = False,
        now: datetime | None = None,
    ) -> bool:
        site_key = site.strip().lower()
        bucket_date, bucket_started_at, bucket_ends_at = cls.utc_day_window(now)
        task_key = cls._build_task_key(site_key, bucket_date)
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        existing_status = cls._get_local_build_status(site_key, bucket_date)
        if (
            not force_refresh
            and existing_status is not None
            and existing_status.state == "failed"
            and not cls._build_failure_retry_due(existing_status, current)
        ):
            return False

        with cls._build_registry_lock:
            existing = cls._build_tasks.get(task_key)
            if existing is not None and not existing.done():
                return False

        acquired = await cls._acquire_build_lock(
            site_key,
            bucket_date,
            redis_manager,
            now=now,
        )
        if not acquired:
            return False

        started_status = StatisticsBuildStatus(
            site=site_key,
            target_bucket_date=bucket_date.isoformat(),
            state="building",
            progress_ratio=0.0,
            processed_rows=0,
            total_rows_estimate=None,
            min_id=None,
            max_id=None,
            last_processed_id=None,
            started_at=current.isoformat(),
            updated_at=current.isoformat(),
            message="Starting statistics scan",
            error=None,
        )
        cls._set_local_build_status(site_key, bucket_date, started_status)

        def _progress_callback(status: StatisticsBuildStatus) -> None:
            cls._set_local_build_status(site_key, bucket_date, status)

        async def _runner() -> None:
            try:
                snapshot = await asyncio.to_thread(
                    cls._load_or_compute_daily_statistics_sync,
                    site_key,
                    bucket_date=bucket_date,
                    bucket_started_at=bucket_started_at,
                    bucket_ends_at=bucket_ends_at,
                    force_refresh=force_refresh,
                    progress_callback=_progress_callback,
                )
                await cls._cache_snapshot(snapshot, redis_manager)
            except Exception as exc:
                failure_time = datetime.now(timezone.utc).isoformat()
                latest_status = cls._get_local_build_status(site_key, bucket_date)
                cls._set_local_build_status(
                    site_key,
                    bucket_date,
                    StatisticsBuildStatus(
                        site=site_key,
                        target_bucket_date=bucket_date.isoformat(),
                        state="failed",
                        progress_ratio=(
                            latest_status.progress_ratio if latest_status else None
                        ),
                        processed_rows=(
                            latest_status.processed_rows if latest_status else 0
                        ),
                        total_rows_estimate=(
                            latest_status.total_rows_estimate if latest_status else None
                        ),
                        min_id=latest_status.min_id if latest_status else None,
                        max_id=latest_status.max_id if latest_status else None,
                        last_processed_id=(
                            latest_status.last_processed_id if latest_status else None
                        ),
                        started_at=(
                            latest_status.started_at
                            if latest_status and latest_status.started_at
                            else current.isoformat()
                        ),
                        updated_at=failure_time,
                        message="Statistics build failed",
                        error=str(exc),
                    ),
                )
                logger.warning(
                    "Failed building daily statistics for %s/%s: %s",
                    site_key,
                    bucket_date,
                    exc,
                    exc_info=True,
                )
            else:
                cls._set_local_build_status(site_key, bucket_date, None)
            finally:
                await cls._release_build_lock(site_key, bucket_date, redis_manager)
                with cls._build_registry_lock:
                    cls._build_tasks.pop(task_key, None)

        try:
            task = asyncio.create_task(_runner())
        except Exception:
            await cls._release_build_lock(site_key, bucket_date, redis_manager)
            raise

        with cls._build_registry_lock:
            cls._build_tasks[task_key] = task
        return True

    @classmethod
    async def get_statistics_availability(
        cls,
        site: str,
        *,
        redis_manager: object | None = None,
        schedule_if_missing: bool = False,
        schedule_refresh: bool = False,
        now: datetime | None = None,
    ) -> SiteStatisticsAvailability:
        site_key = site.strip().lower()
        bucket_date, _, _ = cls.utc_day_window(now)
        target_bucket = bucket_date.isoformat()
        build_status = cls._get_local_build_status(site_key, bucket_date)
        cached = await cls._get_cached_snapshot(site_key, bucket_date, redis_manager)

        if not schedule_refresh:
            if (
                cached is not None
                and cached.bucket_date == target_bucket
                and cls._has_inventory_metrics(cached)
            ):
                return SiteStatisticsAvailability(
                    site=site_key,
                    target_bucket_date=target_bucket,
                    snapshot=cached,
                    snapshot_source="current",
                    is_current=True,
                    is_building=await cls._is_build_in_progress(
                        site_key,
                        bucket_date,
                        redis_manager,
                    ),
                    build_status=build_status,
                )

        latest = cached
        if latest is None:
            latest = await asyncio.to_thread(cls._load_latest_snapshot_sync, site_key)

        if latest is not None:
            await cls._cache_snapshot(latest, redis_manager)

        is_current = latest is not None and latest.bucket_date == target_bucket
        inventory_metrics_missing = is_current and not cls._has_inventory_metrics(
            latest
        )
        should_schedule = schedule_refresh or (
            schedule_if_missing and (not is_current or inventory_metrics_missing)
        )
        if (
            should_schedule
            and not schedule_refresh
            and build_status is not None
            and build_status.state == "failed"
            and not cls._build_failure_retry_due(build_status, now)
        ):
            should_schedule = False
        build_started = False
        if should_schedule:
            build_started = await cls.ensure_daily_statistics_started(
                site_key,
                redis_manager=redis_manager,
                force_refresh=schedule_refresh or inventory_metrics_missing,
                now=now,
            )
            build_status = cls._get_local_build_status(site_key, bucket_date)

        if latest is not None:
            snapshot_source: SnapshotSource = "current" if is_current else "latest"
        else:
            snapshot_source = "none"

        return SiteStatisticsAvailability(
            site=site_key,
            target_bucket_date=target_bucket,
            snapshot=latest,
            snapshot_source=snapshot_source,
            is_current=is_current,
            is_building=build_started
            or await cls._is_build_in_progress(site_key, bucket_date, redis_manager),
            build_status=build_status,
        )

    @classmethod
    async def get_daily_statistics(
        cls,
        site: str,
        *,
        redis_manager: object | None = None,
        force_refresh: bool = False,
        now: datetime | None = None,
    ) -> DailyStatisticsSnapshot:
        site_key = site.strip().lower()
        bucket_date, bucket_started_at, bucket_ends_at = cls.utc_day_window(now)

        if not force_refresh:
            cached = await cls._get_cached_snapshot(
                site_key, bucket_date, redis_manager
            )
            if cached is not None and cached.bucket_date == bucket_date.isoformat():
                return cached

        snapshot = await asyncio.to_thread(
            cls._load_or_compute_daily_statistics_sync,
            site_key,
            bucket_date=bucket_date,
            bucket_started_at=bucket_started_at,
            bucket_ends_at=bucket_ends_at,
            force_refresh=force_refresh,
        )
        await cls._cache_snapshot(snapshot, redis_manager)
        return snapshot

    @staticmethod
    def _pick_average(preferred: int, fallback: int) -> int:
        return preferred if preferred > 0 else fallback

    @classmethod
    def estimate_profile_size(
        cls,
        *,
        media_count: int,
        image_count: int,
        video_count: int,
        audio_count: int,
        stream_count: int,
        snapshot: DailyStatisticsSnapshot,
    ) -> int | None:
        overall_avg = snapshot.avg_media_size_bytes
        image_avg = cls._pick_average(snapshot.avg_image_size_bytes, overall_avg)
        video_avg = cls._pick_average(snapshot.avg_video_size_bytes, overall_avg)
        audio_avg = cls._pick_average(snapshot.avg_audio_size_bytes, overall_avg)
        other_avg = cls._pick_average(snapshot.avg_other_size_bytes, overall_avg)

        estimated = 0
        estimated += max(0, image_count) * image_avg
        estimated += max(0, video_count) * video_avg
        estimated += max(0, audio_count) * audio_avg
        estimated += max(0, stream_count) * video_avg

        covered_count = (
            max(0, image_count)
            + max(0, video_count)
            + max(0, audio_count)
            + max(0, stream_count)
        )
        remaining = max(0, media_count - covered_count)
        estimated += remaining * other_avg

        return estimated if estimated > 0 else None

    @staticmethod
    def get_actual_media_size(session: Session, user_id: int) -> int:
        total_row = session.exec(
            sql_select(func.coalesce(func.sum(MediaModel.size), 0)).where(
                MediaModel.user_id == user_id,
                MediaModel.size > 0,
            )
        ).one()
        return StatisticsService._coerce_int(total_row)

    @classmethod
    def get_known_media_stats(
        cls,
        session: Session,
        user_id: int,
    ) -> KnownUserMediaStats:
        stats = KnownUserMediaStats()
        rows = session.exec(
            sql_select(
                MediaModel.category,
                func.count(MediaModel.id),
                func.coalesce(func.sum(MediaModel.size), 0),
            )
            .where(
                MediaModel.user_id == user_id,
                MediaModel.size > 0,
            )
            .group_by(MediaModel.category)
        ).all()

        for row in rows:
            category = cls._classify_category(row[0])
            count = cls._coerce_int(row[1])
            size = cls._coerce_int(row[2])

            stats.total_count += count
            stats.total_size += size

            if category == "image":
                stats.image_count += count
            elif category == "video":
                stats.video_count += count
            elif category == "audio":
                stats.audio_count += count

        return stats

    @classmethod
    async def resolve_user_size(
        cls,
        *,
        session: Session,
        user_id: int,
        site: str,
        stored_size: int,
        media_count: int,
        image_count: int,
        video_count: int,
        audio_count: int,
        stream_count: int,
        redis_manager: object | None = None,
        statistics_availability: SiteStatisticsAvailability | None | object = (
            _STATISTICS_AVAILABILITY_UNSET
        ),
    ) -> ResolvedUserSize:
        if statistics_availability is _STATISTICS_AVAILABILITY_UNSET:
            try:
                availability = await cls.get_statistics_availability(
                    site,
                    redis_manager=redis_manager,
                    schedule_if_missing=True,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to load daily statistics while resolving user size for %s/%s: %s",
                    site,
                    user_id,
                    exc,
                )
                availability = None
        else:
            availability = cast(
                SiteStatisticsAvailability | None,
                statistics_availability,
            )

        return cls.resolve_user_size_from_statistics(
            session=session,
            user_id=user_id,
            site=site,
            stored_size=stored_size,
            media_count=media_count,
            image_count=image_count,
            video_count=video_count,
            audio_count=audio_count,
            stream_count=stream_count,
            statistics_availability=availability,
        )

    @classmethod
    def resolve_user_size_from_statistics(
        cls,
        *,
        session: Session,
        user_id: int,
        site: str,
        stored_size: int,
        media_count: int,
        image_count: int,
        video_count: int,
        audio_count: int,
        stream_count: int,
        statistics_availability: SiteStatisticsAvailability | None,
    ) -> ResolvedUserSize:
        """Resolve size using SQL and an already loaded statistics snapshot only."""
        actual_size = max(0, cls._coerce_int(stored_size))
        known_media = KnownUserMediaStats()
        if actual_size <= 0:
            actual_size = cls.get_actual_media_size(session, user_id)

        if actual_size > 0:
            try:
                known_media = cls.get_known_media_stats(session, user_id)
                actual_size = max(actual_size, known_media.total_size)
            except Exception as exc:
                logger.warning(
                    "Failed to load known media stats while resolving user size for %s/%s: %s",
                    site,
                    user_id,
                    exc,
                )
                known_media.total_size = actual_size

            total_media_count = max(0, cls._coerce_int(media_count))
            if total_media_count > 0 and known_media.total_count >= total_media_count:
                return ResolvedUserSize(
                    size=actual_size,
                    display_size=actual_size,
                    estimated_size=actual_size,
                    size_source="actual",
                )

        if actual_size > 0:
            resolved_actual = ResolvedUserSize(
                size=actual_size,
                display_size=actual_size,
                estimated_size=None,
                size_source="actual",
            )
        else:
            resolved_actual = None

        snapshot = (
            statistics_availability.snapshot
            if statistics_availability is not None
            else None
        )
        if snapshot is None:
            return resolved_actual or ResolvedUserSize(
                size=0,
                display_size=0,
                estimated_size=None,
                size_source="unknown",
            )

        remaining_media_count = max(
            0,
            cls._coerce_int(media_count) - known_media.total_count,
        )
        remaining_image_count = max(
            0,
            cls._coerce_int(image_count) - known_media.image_count,
        )
        remaining_video_like_count = max(
            0,
            cls._coerce_int(video_count)
            + cls._coerce_int(stream_count)
            - known_media.video_count,
        )
        remaining_audio_count = max(
            0,
            cls._coerce_int(audio_count) - known_media.audio_count,
        )
        remaining_estimate = cls.estimate_profile_size(
            media_count=remaining_media_count,
            image_count=remaining_image_count,
            video_count=remaining_video_like_count,
            audio_count=remaining_audio_count,
            stream_count=0,
            snapshot=snapshot,
        )

        if actual_size > 0:
            total_estimate = actual_size
            if remaining_estimate is not None and remaining_estimate > 0:
                total_estimate += remaining_estimate
            return ResolvedUserSize(
                size=actual_size,
                display_size=actual_size,
                estimated_size=total_estimate,
                size_source="actual",
                statistics_bucket_date=snapshot.bucket_date,
            )

        if remaining_estimate is not None and remaining_estimate > 0:
            return ResolvedUserSize(
                size=0,
                display_size=remaining_estimate,
                estimated_size=remaining_estimate,
                size_source="estimated",
                statistics_bucket_date=snapshot.bucket_date,
            )

        return ResolvedUserSize(
            size=0,
            display_size=0,
            estimated_size=None,
            size_source="unknown",
            statistics_bucket_date=snapshot.bucket_date,
        )
