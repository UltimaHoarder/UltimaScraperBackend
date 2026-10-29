"""Background reconciliation for terminal-job Redis working sets."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Collection
from typing import Any, Sequence, cast

from ..job_system.manager import JobManager
from ..job_system.models import JobState
from ..job_system.storage import JobStorage
from ..redis.client import RedisManager

logger = logging.getLogger(__name__)

# JobExecutionSettings field -> reconciler attribute.
_SETTING_ATTRIBUTES = {
    "terminal_reconcile_interval_seconds": "interval_seconds",
    "terminal_reconcile_min_age_seconds": "min_age_seconds",
    "terminal_reconcile_batch_size": "max_jobs_per_cycle",
    "completed_job_retention_seconds": "retention_age_seconds",
    "completed_job_retention_batch_size": "retention_batch_size",
}


async def load_job_execution_settings_async() -> Any | None:
    """Read this backend's live job-execution settings, or ``None`` before setup."""
    from ultima_scraper_db_archive.global_db import get_global_session, is_setup_complete

    from ..config_settings_sync import load_job_execution_settings
    from ..database.threads import run_database_call

    if not is_setup_complete():
        return None

    def load() -> Any:
        with get_global_session() as session:
            return load_job_execution_settings(session)

    return await run_database_call(load)

_TERMINAL_STATES = {
    JobState.COMPLETED.value: JobState.COMPLETED,
    JobState.FAILED.value: JobState.FAILED,
    JobState.CANCELLED.value: JobState.CANCELLED,
}


class TerminalJobReconciler:
    """Resume archive-first cleanup for terminal jobs left in Redis."""

    def __init__(
        self,
        redis_manager: RedisManager,
        *,
        interval_seconds: int = 300,
        min_age_seconds: int = 300,
        max_jobs_per_cycle: int = 2,
        scan_count: int = 100,
        retention_age_seconds: int = JobStorage.DEFAULT_COMPLETED_JOB_RETENTION_SECONDS,
        retention_batch_size: int = JobStorage.DEFAULT_WRITE_BATCH_SIZE,
        settings_loader: Callable[[], Awaitable[Any | None]] | None = None,
        overrides: Collection[str] = (),
    ) -> None:
        for name, value, minimum in (
            ("retention_age_seconds", retention_age_seconds, 0),
            ("retention_batch_size", retention_batch_size, 1),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        self.redis_manager = redis_manager
        self.manager = JobManager(redis_manager)
        self.interval_seconds = max(30, interval_seconds)
        self.min_age_seconds = max(0, min_age_seconds)
        self.max_jobs_per_cycle = max(1, max_jobs_per_cycle)
        self.scan_count = max(10, scan_count)
        self.retention_age_seconds = retention_age_seconds
        self.retention_batch_size = retention_batch_size
        self._settings_loader = settings_loader
        # Attributes fixed by deprecated environment overrides.
        self._overrides = frozenset(overrides)
        self._scan_cursor = 0
        self._task: asyncio.Task[None] | None = None
        self._deletion_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="terminal-job-reconciler")
        from .job_deletion import run_deletion_worker
        self._deletion_task = asyncio.create_task(run_deletion_worker(self.redis_manager), name="job-deletion-worker")

    async def stop(self) -> None:
        if self._deletion_task is not None:
            self._deletion_task.cancel()
            try:
                await self._deletion_task
            except asyncio.CancelledError:
                pass
            self._deletion_task = None
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def refresh_settings(self) -> None:
        """Apply current Settings values; keep the previous ones on failure."""
        if self._settings_loader is None:
            return
        try:
            settings = await self._settings_loader()
        except Exception:
            logger.warning(
                "Could not read job-execution settings; keeping current "
                "terminal-job cleanup limits",
                exc_info=True,
            )
            return
        if settings is None:
            return
        for field, attribute in _SETTING_ATTRIBUTES.items():
            if attribute not in self._overrides:
                setattr(self, attribute, getattr(settings, field))

    async def _run(self) -> None:
        while True:
            try:
                await self.refresh_settings()
                processed = await self.run_once()
                if processed:
                    logger.info(
                        "Terminal-job reconciliation processed %d job(s)", processed
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Terminal-job reconciliation cycle failed")
            await asyncio.sleep(self.interval_seconds)

    async def run_once(self) -> int:
        """Inspect a bounded Redis index window and clean eligible jobs."""
        storage = self.manager.storage
        client = cast(Any, self.redis_manager.client)
        # Completed windows have already removed their task artifacts. Retire
        # their cached headers too, after the existing archive/ownership checks.
        retired = await storage.cleanup_completed_jobs(
            self.retention_age_seconds, batch_size=self.retention_batch_size
        )
        cursor, raw_entries = await client.zscan(
            storage.JOB_INDEX_KEY,
            cursor=self._scan_cursor,
            count=self.scan_count,
        )
        self._scan_cursor = int(cursor)
        cutoff = time.time() - self.min_age_seconds
        processed = 0

        for raw_job_id, raw_score in cast(Sequence[tuple[Any, Any]], raw_entries):
            if processed >= self.max_jobs_per_cycle:
                break
            if float(raw_score) > cutoff:
                continue

            job_id = (
                raw_job_id.decode()
                if isinstance(raw_job_id, bytes)
                else str(raw_job_id)
            )
            raw_status = await client.hget(
                storage.JOB_STATE_KEY.format(job_id=job_id), "status"
            )
            status = (
                raw_status.decode() if isinstance(raw_status, bytes) else raw_status
            )
            terminal_state = _TERMINAL_STATES.get(str(status))
            if terminal_state is None:
                continue

            artifact_keys = (
                storage.TASK_INDEX_KEY.format(job_id=job_id),
                storage.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id),
                storage.TASKS_READY_KEY.format(job_id=job_id),
                storage.TASKS_BLOCKED_KEY.format(job_id=job_id),
                storage.TASKS_COMPLETED_KEY.format(job_id=job_id),
                storage.TASKS_INFLIGHT_KEY.format(job_id=job_id),
                storage.TASK_OUTPUTS_KEY.format(job_id=job_id),
            )
            if not await client.exists(*artifact_keys):
                continue

            await self.manager._finalize_terminal_job_cleanup(
                job_id, terminal_state=terminal_state
            )
            processed += 1

        # Resume any interrupted background task purges tracked in Redis
        try:
            pending_purge_ids = await client.smembers(storage.PENDING_TASK_PURGE_KEY)
            for raw_id in pending_purge_ids:
                purge_job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
                from ..api.routes.job_central_store import _purge_job_tasks_sync
                total = await asyncio.to_thread(_purge_job_tasks_sync, purge_job_id)
                await client.srem(storage.PENDING_TASK_PURGE_KEY, purge_job_id)
                if total:
                    logger.info("Terminal reconciler resumed and purged %d task(s) for %s", total, purge_job_id)
        except Exception as exc:
            logger.debug("Failed checking pending task purges: %s", exc)

        try:
            from ..database.receipt_retention import run_retention_cycle
            receipts = await run_retention_cycle(self.redis_manager)
            if receipts:
                logger.info("Expired %d unreferenced database write receipts", receipts)
        except Exception:
            logger.exception("Database receipt retention cycle failed; remaining receipts retained")
        try:
            from ..database.outbox_retention import run_outbox_retention_cycle
            events = await run_outbox_retention_cycle(self.redis_manager)
            if events:
                logger.info("Expired %d delivered terminal-job outbox events", events)
        except Exception:
            logger.exception("Outbox retention failed; remaining events retained")
        try:
            from ..database.history_retirement import run_history_retirement_cycle
            leaves = await run_history_retirement_cycle(self.redis_manager)
            if leaves:
                logger.info("Retired %d finished-job history partitions", leaves)
        except Exception:
            logger.exception("History partition retirement failed; partitions retained")
        try:
            from ..database.history_compaction import run_history_compaction_cycle
            compacted = await run_history_compaction_cycle(self.redis_manager)
            if compacted:
                logger.info("Compacted task history of %d finished jobs", compacted)
        except Exception:
            logger.exception("Task history compaction failed; full history retained")
        return processed + retired


async def log_redis_memory_guardrail(redis_manager: RedisManager) -> None:
    """Warn when Redis has no hard memory ceiling; never mutate server config."""
    try:
        client = cast(Any, redis_manager.client)
        info = await client.info("memory")
        configured_max = int(info.get("maxmemory", 0) or 0)
        used = int(info.get("used_memory", 0) or 0)
        configured_policy = str(info.get("maxmemory_policy", "unknown"))
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not inspect Redis memory configuration: %s", exc)
        return

    if configured_max <= 0:
        logger.warning(
            "Redis has no maxmemory ceiling (used=%d bytes, policy=%s). "
            "Set REDIS_MAXMEMORY for the bundled service or configure the "
            "external Redis host explicitly.",
            used,
            configured_policy,
        )
    else:
        logger.info(
            "Redis memory guardrail: used=%d max=%d policy=%s",
            used,
            configured_max,
            configured_policy,
        )
