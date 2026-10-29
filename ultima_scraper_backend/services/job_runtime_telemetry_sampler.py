"""Persist low-cost, time-based snapshots for live job diagnostics."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from ..job_system.routing import BackendRouter
from ..job_system.storage import JobStorage
from ..redis_channels import KEY_PREFIX

logger = logging.getLogger(__name__)


class JobRuntimeTelemetrySampler:
    """Capture one Redis-backed job-state sample per second across API workers."""

    INTERVAL_SECONDS = 1.0
    TICK_LOCK_TTL_SECONDS = 60
    MAX_CONCURRENT_JOB_READS = 32
    TICK_LOCK_KEY = f"{KEY_PREFIX}:jobs:runtime-sampler:tick:{{bucket}}"

    def __init__(self, redis_manager: Any, backend_id: str | None = None) -> None:
        self.redis = redis_manager
        self.storage = JobStorage(redis_manager)
        self.router = BackendRouter(redis_manager)
        self.backend_id = str(backend_id or "").strip()
        self._task: asyncio.Task[None] | None = None
        self._wake_task: asyncio.Task[None] | None = None
        self._discovery_wake = asyncio.Event()
        self._next_discovery_at = 0.0
        self._ledger_job_ids: set[str] = set()

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(
                self._run(), name="job-runtime-telemetry-sampler"
            )
            from ..job_system.idle import listen_for_wakeups

            self._wake_task = asyncio.create_task(
                listen_for_wakeups(self.redis, [self._discovery_wake]),
                name="job-runtime-telemetry-wake-hints",
            )

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if self._wake_task is not None:
            self._wake_task.cancel()
            await asyncio.gather(self._wake_task, return_exceptions=True)
            self._wake_task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        last_bucket = -1
        while True:
            now = time.time()
            bucket = int(now)
            if bucket != last_bucket:
                last_bucket = bucket
                try:
                    await self._sample_active_jobs(bucket)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.debug("Job runtime telemetry sampling failed", exc_info=True)

            until_next_tick = self.INTERVAL_SECONDS - (time.time() % self.INTERVAL_SECONDS)
            await asyncio.sleep(max(0.05, min(0.25, until_next_tick)))

    async def _sample_active_jobs(self, bucket: int) -> None:
        client = self.storage._client()
        tick_lock = self.TICK_LOCK_KEY.format(bucket=bucket)
        if not await client.set(tick_lock, "sampled", nx=True, ex=self.TICK_LOCK_TTL_SECONDS):
            return

        raw_backend_ids = await client.zrange(self.router.LOAD_KEY, 0, -1)
        backend_ids = {
            value.decode() if isinstance(value, bytes) else str(value)
            for value in raw_backend_ids
        }
        if self.backend_id:
            backend_ids.add(self.backend_id)
        pipeline = client.pipeline(transaction=False)
        ordered_backend_ids = sorted(backend_ids)
        for backend_id in ordered_backend_ids:
            pipeline.smembers(self.router.backend_jobs_key(backend_id))
        active_job_sets = await pipeline.execute()
        active_job_ids: set[str] = set()
        for job_ids in active_job_sets:
            active_job_ids.update(
                value.decode() if isinstance(value, bytes) else str(value)
                for value in job_ids
            )
        from ..database.threads import run_database_call
        from ..job_system.ledger import ExecutionLedger
        ledger = ExecutionLedger()
        if active_job_ids or self._ledger_job_ids or self._discovery_wake.is_set() or time.monotonic() >= self._next_discovery_at:
            self._discovery_wake.clear()
            self._ledger_job_ids = set(await run_database_call(ledger.active_run_ids, limit=1000))
            self._next_discovery_at = time.monotonic() + (1 if self._ledger_job_ids else 5)
        ledger_job_ids = self._ledger_job_ids
        active_job_ids.update(ledger_job_ids)
        if not active_job_ids:
            return

        stats_by_job = {}
        observed_at = {}
        ordered_ids = sorted(active_job_ids)
        for offset in range(0, len(ordered_ids), 256):
            batch = ordered_ids[offset:offset + 256]
            observed_at.update(dict.fromkeys(batch, int(time.time() * 1000)))
            stats_by_job.update(await run_database_call(ledger.stats_many, batch))

        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_JOB_READS)

        async def read_job_sample(job_id: str) -> tuple[str, dict[str, int]] | None:
            async with semaphore:
                try:
                    # Stamp before the read: the counters reflect this moment,
                    # while SQL/thread latency after it varies per tick and
                    # would otherwise compress or stretch measured intervals.
                    observed_ms = observed_at[job_id]
                    stats = stats_by_job.get(job_id)
                    if stats is None:
                        stats = await self.storage.get_job_task_stats(job_id)
                    if job_id in ledger_job_ids:
                        # This is an ephemeral diagnostic projection; SQL still
                        # owns all run/task state and cancellation decisions.
                        await client.hset(self.storage.JOB_STATE_KEY.format(job_id=job_id), mapping={
                            "total_tasks": int(stats.get("total") or 0),
                            "ready_tasks": int(stats.get("ready") or 0),
                            "inflight_tasks": int(stats.get("inflight") or 0),
                            "completed_tasks": int(stats.get("completed") or 0),
                            "failed_tasks": int(stats.get("failed") or 0),
                            "blocked_tasks": int(stats.get("blocked") or 0),
                        })
                        await client.expire(self.storage.JOB_STATE_KEY.format(job_id=job_id), 86400)
                    return job_id, {
                        "timestamp_ms": observed_ms,
                        "total": max(0, int(stats.get("total") or 0)),
                        "ready": max(0, int(stats.get("ready") or 0)),
                        "inflight": max(0, int(stats.get("inflight") or 0)),
                        "completed": max(0, int(stats.get("completed") or 0)),
                        "failed": max(0, int(stats.get("failed") or 0)),
                    }
                except Exception:
                    logger.debug(
                        "Could not sample runtime counters for job %s", job_id,
                        exc_info=True,
                    )
                    return None

        observed = await asyncio.gather(
            *(read_job_sample(job_id) for job_id in sorted(active_job_ids))
        )
        await self.storage.append_job_runtime_samples(
            (sample for sample in observed if sample is not None),
            tick_ms=bucket * 1000,
        )
