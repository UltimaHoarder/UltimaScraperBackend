"""High-level orchestration utilities for the job system."""

from __future__ import annotations

from collections.abc import AsyncIterable
from contextlib import nullcontext

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional
from uuid import uuid4

from ..database.write_queue import (
    DatabaseWriteClient,
    DatabaseWriteOperation,
    stable_command_id,
)
from ..redis.client import RedisManager
from .models import (
    Job,
    JobSchedule,
    JobSource,
    JobState,
    Task,
    TaskState,
)
from . import mps_attempt, retention
from .routing import BackendRouter
from .storage import JobStorage, TaskPayloadBudgetExceeded

logger = logging.getLogger(__name__)


class StandardJobMetadata:
    """Standard metadata structure for all jobs."""

    def __init__(
        self,
        *,
        source: JobSource,
        priority: int = 0,
        backend_id: Optional[str] = None,
        # Scraper-specific (optional)
        identifier: Optional[str] = None,
        # Additional fields
        **extra: object,
    ) -> None:
        self.source = source
        self.priority = priority
        self.backend_id = backend_id
        self.identifier = identifier
        self.extra = extra

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary for job metadata."""
        result: dict[str, object] = {
            "source": self.source.value,
            "priority": self.priority,
        }
        if self.backend_id:
            result["backend_id"] = self.backend_id
        if self.identifier:
            result["identifier"] = self.identifier
        result.update(self.extra)
        return result


class JobManager:
    """Create, persist, and execute jobs and tasks."""

    # Large MPS jobs can take hours to archive. Keep the single-writer lock
    # alive for that whole operation; successful/failed runs release it below.
    TERMINAL_CLEANUP_LOCK_TTL = 6 * 60 * 60

    def __init__(self, redis_manager: RedisManager) -> None:
        self.redis_manager = redis_manager
        self.storage = JobStorage(redis_manager)
        self.router = BackendRouter(redis_manager)
        self.database_writes = DatabaseWriteClient(
            redis_manager,
            producer_id="job-manager",
        )

    async def run_job(
        self,
        job: Job,
        *,
        timeout_per_task: Optional[timedelta] = None,
        context: Optional[dict[str, object]] = None,
    ) -> None:
        try:
            await job.run(
                storage=self.storage,
                context=context if context is not None else {},
                timeout_per_task=timeout_per_task,
            )
        finally:
            await self._release_dedup_for_job(job)

    async def cancel_job(self, job: Job) -> None:
        job.state = JobState.CANCELLED
        job.completed_at = job.completed_at or datetime.now(timezone.utc).isoformat()
        job.updated_at = job.completed_at
        await self.storage.persist_job(job)
        await self._release_dedup_for_job(job)

    async def _release_dedup_for_job(self, job: Job) -> None:
        await self.storage.release_dedup_for_job(job)

    async def ensure_connected(self) -> None:
        await self.redis_manager.connect()

    async def create_job(
        self,
        *,
        name: str,
        created_by: str,
        tasks: Iterable[Task],
        schedule: Optional[JobSchedule] = None,
        source: JobSource = JobSource.MANUAL,
        priority: int = 0,
        identifier: Optional[str] = None,
        extra_metadata: Optional[dict[str, object]] = None,
    ) -> Job:
        """
        Unified job creation method.

        Args:
            name: Job name
            created_by: Who/what created the job
            tasks: Tasks to execute
            schedule: Optional schedule configuration (if None, job is manual/immediate)
            source: Where the job originated from
            priority: Dispatch priority
            identifier: Optional user/resource identifier
            extra_metadata: Additional metadata fields

        Returns:
            Created job with standardized metadata
        """
        # Build standardized metadata - merge extra fields with standard fields
        metadata: dict[str, object] = {}
        if extra_metadata:
            metadata.update(extra_metadata)

        # Build standardized metadata structure
        std_metadata = StandardJobMetadata(
            source=source,
            priority=priority,
            identifier=identifier,
        )

        # Merge standard metadata into the dict
        metadata.update(std_metadata.to_dict())

        # Create job with unified structure
        job = Job.create(
            name=name,
            created_by=created_by,
            tasks=[],
            schedule=schedule,
            metadata=metadata,
        )

        return await self.prepare_job(job, tasks=tasks)

    async def prepare_job(
        self,
        job: Job,
        *,
        tasks: Iterable[Task] | None = None,
        priority: int = 0,
        backend_id: str | None = None,
        select_backend: bool = False,
    ) -> Job:
        """Commit producer inputs and routing before bounded runtime activation."""
        from functools import partial
        from anyio import from_thread
        from ..database.task_admission import prepare_task_plan
        from ..database.threads import run_database_call

        config = job.to_config(include_task_ids=False).to_dict()
        config["metadata"] = {**config["metadata"], "dispatch_priority": priority}
        if backend_id is not None:
            config["metadata"]["dispatch_backend_id"] = backend_id

        def choose_backend(saved):
            metadata = saved["metadata"]
            # The SQL worker owns the uncommitted definitions. Marshal only
            # compact routing inputs to Redis's owning event loop; failure
            # rolls back preparation before recovery can see runnable work.
            return from_thread.run(partial(
                self.router._select_backend,
                capabilities=metadata["required_capabilities"],
                ip_lock_key=metadata.get("ip_lock_key"),
            ))

        saved = await run_database_call(
            prepare_task_plan,
            job_id=job.job_id, job_type=job.name, site="",
            job_config=config,
            args_payload={"job_name": job.name, "created_by": job.created_by},
            tasks=job.tasks if tasks is None else tasks,
            select_backend=choose_backend if select_backend and backend_id is None else None,
        )
        return Job.from_dict(saved)

    async def activate_job(
        self,
        job: Job,
        *,
        distributed_mode: bool,
        priority: int = 0,
        ip_lock_key: Optional[str] = None,
        backend_id: str | None = None,
        reserve_dedup: bool = True,
    ) -> tuple[str | None, list[Task]]:
        """SQL admission already exposed the run; Redis never dispatches it."""
        from .ledger import owns

        if not owns(job.to_dict()):
            raise ValueError("Job must be admitted to the execution ledger before activation")
        return None, []


    async def activate_materialized_job(self, job: Job, **kwargs) -> str | None:
        """Validate admission for callers with an already prepared job."""
        await self.activate_job(job, distributed_mode=False)
        return job.metadata.get("dispatch_backend_id")


    async def cancel_job_by_id(
        self,
        job_id: str,
        *,
        reason: str = "",
        wait_for_resource_cleanup: bool = True,
    ) -> Job:
        """Cancel a job and remove it from live routing state."""

        from .ledger import ExecutionLedger
        from ..database.threads import run_database_call
        cancelled = await run_database_call(ExecutionLedger().control, job_id, "cancel")
        if cancelled is not None:
            return Job.from_dict(cancelled)

        job = await self.storage.fetch_job(job_id, include_tasks=False)
        if not job:
            from .durable import cancel_waiting_job

            cancelled = await cancel_waiting_job(self, job_id, reason=reason)
            if cancelled is not None:
                return cancelled
            job = await self.storage.fetch_job(job_id, include_tasks=False)
            if job is None:
                raise mps_attempt.StaleTaskDefinition(f"Job {job_id} changed during cancellation")
        if job.state in {JobState.COMPLETED, JobState.CANCELLED}:
            raise RuntimeError(f"Job is already {job.state.value}")

        identity = mps_attempt.current(job_id)
        if identity is None and job.metadata.get("mps_manifest"):
            identity = mps_attempt.Identity(
                job_id, "", job.metadata.get("mps_task_manifest") or ""
            )
        with mps_attempt.scope(identity):
            backend_id = await self._resolve_backend_assignment(job_id)
            cancelled = await self.storage.mark_job_cancelled(job_id, reason=reason)
            await self.router.clear_job_routing(job_id=job_id, backend_id=backend_id)
            if wait_for_resource_cleanup:
                await self.finalize_job_cancellation(job_id)
            else:
                try:
                    await self.storage.release_dedup_for_job_id(
                        job_id, default_backend_id=backend_id
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "Failed eager release of dedup for cancelled job %s: %s",
                        job_id,
                        exc,
                    )
        return cancelled or job

    async def _finalize_terminal_job_cleanup(
        self,
        job_id: str,
        *,
        terminal_state: JobState,
    ) -> None:
        """Release only the terminal definition observed by this cleanup."""
        snapshot = await self.storage.fetch_job(job_id, include_tasks=False)
        if snapshot is None or snapshot.state != terminal_state:
            return
        identity = mps_attempt.current(job_id)
        if identity is None and snapshot.metadata.get("mps_manifest"):
            identity = mps_attempt.Identity(
                job_id, "", snapshot.metadata.get("mps_task_manifest") or ""
            )
        terminal_snapshot = None
        if identity is None:
            terminal_snapshot = await retention.capture_terminal(
                self.storage, job_id, terminal_state.value
            )
            if terminal_snapshot is None:
                return
        try:
            with (
                mps_attempt.scope(identity),
                retention.scope(terminal_snapshot) if terminal_snapshot is not None else nullcontext(),
            ):
                if identity is not None:
                    await mps_attempt.assert_current(self.storage, job_id, identity.manifest_id)
                await self._cleanup_terminal_definition(job_id, terminal_state=terminal_state)
        except (mps_attempt.StaleTaskDefinition, retention.StaleRetirement):
            logger.debug("Skipped obsolete terminal cleanup for %s", job_id)

    async def _cleanup_terminal_definition(
        self, job_id: str, *, terminal_state: JobState
    ) -> None:
        cleanup_lock_key = f"ultimascraper:job:{job_id}:terminal_cleanup_lock"
        lock_owner = f"job-manager:{terminal_state.value}:{uuid4().hex}"
        lock_acquired = False
        try:
            lock_acquired = bool(
                await self.redis_manager.set(
                    cleanup_lock_key,
                    lock_owner,
                    nx=True,
                    ex=self.TERMINAL_CLEANUP_LOCK_TTL,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Failed to acquire terminal cleanup lock for %s job %s: %s",
                terminal_state.value,
                job_id,
                exc,
            )

        if not lock_acquired:
            return

        try:
            archived_tasks: int | None = None
            try:
                terminal_job = await self.storage.fetch_job(
                    job_id,
                    include_tasks=False,
                )
                if terminal_job is None or terminal_job.state != terminal_state:
                    return
                if retention.current(job_id) is not None:
                    await retention.evaluate(self.storage, job_id, "return 1", [], [])
                archive_generation = terminal_job.completed_at
                if not archive_generation:
                    raise RuntimeError(
                        f"Terminal job {job_id} has no completion timestamp"
                    )
                archive_batch_size, archive_max_bytes = (
                    self.storage.task_archive_limits()
                )
                archive_definition = (
                    None if terminal_job.metadata.get("mps_manifest")
                    else retention.definition_id(job_id, {"metadata": terminal_job.metadata})
                )
                archive_snapshot = retention.current(job_id)
                archive_signature = (
                    retention.archive_signature(archive_snapshot)
                    if archive_definition is not None and archive_snapshot is not None
                    else None
                )
                archive_result = await self.database_writes.submit(
                    DatabaseWriteOperation.ARCHIVE_JOB_TASKS,
                    {
                        "job_id": job_id,
                        "generation": archive_generation,
                        "batch_size": archive_batch_size,
                        "max_bytes": archive_max_bytes,
                        **(
                            {
                                "mps_task_manifest": mps_attempt.current(
                                    job_id
                                ).manifest_id
                            }
                            if mps_attempt.current(job_id) is not None
                            else {
                                "task_definition_id": archive_definition,
                                **({"archive_signature": archive_signature} if archive_signature is not None else {}),
                            }
                        ),
                    },
                    command_id=stable_command_id(
                        "archive-job-tasks-v4" if archive_definition is not None else "archive-job-tasks-v3",
                        job_id,
                        archive_generation,
                        archive_batch_size,
                        archive_max_bytes,
                        *([archive_definition, archive_signature] if archive_definition is not None else []),
                    ),
                )
                if not archive_result.get("stale"):
                    archived_tasks = int(archive_result.get("archived_tasks") or 0)
            except retention.StaleRetirement:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to archive task blobs for %s job %s before purge: %s",
                    terminal_state.value,
                    job_id,
                    exc,
                )

            try:
                await self.storage.release_dedup_for_job_id(job_id)
            except TaskPayloadBudgetExceeded as exc:
                logger.warning("Retaining terminal task data for %s: %s", job_id, exc)
                return

            if archived_tasks is None or archived_tasks <= 0:
                if archived_tasks == 0:
                    logger.debug(
                        "No task blobs require cleanup for %s job %s",
                        terminal_state.value,
                        job_id,
                    )
                    await self.storage.clear_task_state(job_id, reset_stats=False)
                    if terminal_job.metadata.get("mps_manifest"):
                        from . import mps_state

                        await mps_state.release_budget(self.storage._client(), job_id, storage=self.storage)
                        await mps_attempt.control_mutation(
                            self.storage, job_id, "return redis.call('EXPIRE',KEYS[1],ARGV[1])",
                            [mps_state.PLAN_KEY.format(job_id=job_id)],
                            [self.storage.completed_job_retention_seconds],
                        )
                        marker_key = self.storage.TASK_ARCHIVE_MARKER_KEY.format(
                            job_id=job_id
                        )
                        if mps_attempt.current(job_id) is not None or retention.current(job_id) is not None:
                            await mps_attempt.control_mutation(
                                self.storage,
                                job_id,
                                "return redis.call('DEL', KEYS[1])",
                                [marker_key],
                                [],
                            )
                        else:
                            await self.storage.redis.delete(marker_key)
                return

            try:
                purged = await self.storage.purge_job_task_blobs(job_id)
            except (mps_attempt.StaleTaskDefinition, retention.StaleRetirement):
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to purge task blobs for %s job %s: %s",
                    terminal_state.value,
                    job_id,
                    exc,
                )
            else:
                if purged:
                    logger.info(
                        "Purged %d task blobs from Redis for %s job %s",
                        purged,
                        terminal_state.value,
                        job_id,
                    )
        finally:
            try:
                await self.storage._client().eval(
                    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
                    1,
                    cleanup_lock_key,
                    lock_owner,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Failed to release terminal cleanup lock for %s: %s",
                    job_id,
                    exc,
                )

    async def finalize_job_cancellation(self, job_id: str) -> None:
        """Release remaining resources for a cancelled job once routing is cleared."""

        await self._finalize_terminal_job_cleanup(
            job_id,
            terminal_state=JobState.CANCELLED,
        )

    async def prioritize_job_by_id(
        self,
        job_id: str,
        *,
        priority: int = -1,
    ) -> tuple[Job, int]:
        """Raise a job's ready tasks in the owning backend queue."""

        job = await self.storage.fetch_job(job_id, include_tasks=False)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        if job.state not in {JobState.RUNNING, JobState.PENDING}:
            raise RuntimeError(
                f"Only running/pending jobs can be prioritized (current: {job.state.value})"
            )

        if job.metadata.get("execution_engine") == "ledger":
            from .ledger import ExecutionLedger
            from ..database.threads import run_database_call
            ledger = ExecutionLedger()
            updated = await run_database_call(ledger.control, job_id, "update", priority=priority)
            stats = await run_database_call(ledger.stats, job_id)
            return Job.from_dict(updated), stats["ready"]

        backend_id = await self._resolve_backend_assignment(job_id)
        if not backend_id:
            raise RuntimeError("Job has not been assigned to a backend yet")

        rescored = await self.router.prioritize_job_tasks(
            backend_id,
            job_id,
            priority=priority,
        )
        await self.storage.set_job_priority(job_id, priority)
        return job, rescored

    async def prepare_retry_job(
        self,
        job_id: str,
        *,
        auth_id: int | None = None,
        max_include_tasks: int | None = None,
    ) -> tuple[Job, Job]:
        """Load a job and build a retry clone from failed tasks or all tasks."""

        original_job = await self.storage.fetch_job_with_tasks(
            job_id,
            include_tasks=True,
            max_include_tasks=max_include_tasks,
        )
        if original_job is None:
            raise ValueError(f"Job {job_id} not found")
        if not original_job.tasks:
            if original_job.metadata.get("task_history_compaction"):
                raise RuntimeError(
                    "This job's completed task history was removed after the task "
                    "history retention period; only failed tasks are kept for retry"
                )
            if original_job.state in {JobState.COMPLETED, JobState.CANCELLED}:
                raise RuntimeError(
                    "Completed/cancelled job tasks were archived from Redis; "
                    "retry requires live task payloads"
                )
            raise RuntimeError("Job has no tasks available for retry")

        retry_failed_only = any(
            task.state == TaskState.FAILED for task in original_job.tasks
        )
        retry_tasks: list[Task] = []
        now = datetime.now(timezone.utc).isoformat()
        for task in original_job.tasks:
            if retry_failed_only and task.state != TaskState.FAILED:
                continue
            updated = task.model_copy(
                update={
                    "task_id": f"task-{uuid4().hex[:12]}",
                    "state": TaskState.PENDING,
                    "created_at": now,
                    "updated_at": now,
                    "started_at": None,
                    "completed_at": None,
                    "result": None,
                    "attempts": 0,
                    "depends_on": [],
                    "subtasks": [],
                }
            )
            if auth_id is not None:
                updated.auth_id = auth_id
            # Normalize only the selected definition, after discarding execution
            # history. The source task and its retained results stay unchanged.
            retry_tasks.append(Task.from_dict(updated.to_dict()))

        retry_prefix = "Retry: "
        base_name = original_job.name
        while base_name.startswith(retry_prefix):
            base_name = base_name[len(retry_prefix) :].lstrip()
        retry_name = f"{retry_prefix}{base_name}" if base_name else original_job.name
        retry_job = Job.create(
            name=retry_name,
            created_by=original_job.created_by or "webui",
            tasks=retry_tasks,
            metadata={
                "retry_of": original_job.job_id,
                "original_job_name": original_job.name,
                "retry_auth_id": auth_id,
            },
        )
        return original_job, retry_job

    async def activate_retry_job(
        self,
        *,
        original_job: Job,
        retry_job: Job,
        cancel_reason: str,
        distributed_mode: bool,
        priority: int = 0,
        preferred_backend_id: str | None = None,
    ) -> tuple[str | None, list[Task]]:
        """Cancel the original job and submit the retry clone."""

        assigned_backend_id = await self._resolve_backend_assignment(
            original_job.job_id
        )
        await self.router.clear_job_routing(
            job_id=original_job.job_id,
            backend_id=assigned_backend_id,
        )
        await self.storage.cancel_job_fully(original_job.job_id, reason=cancel_reason)
        return await self.activate_job(
            retry_job,
            distributed_mode=distributed_mode,
            priority=priority,
            backend_id=(
                assigned_backend_id if distributed_mode else preferred_backend_id
            ),
        )

    async def fetch_job_for_update(self, job_id: str) -> Job | None:
        """Read a runtime header or one retained durable definition for editing.

        This explicit control path does not restore Redis keys or task arrays.
        Older ordinary jobs still need their existing edit/recovery protocol.
        """
        job = await self.storage.fetch_job(job_id, include_tasks=False)
        if job is not None:
            return job

        from ultima_scraper_db_archive.global_db import global_session

        from ..database.threads import run_database_call
        from ..database.job_history import headers

        header = (await run_database_call(headers, global_session, [job_id])).get(job_id)
        if header is None or not header["config"].get("metadata", {}).get("mps_manifest"):
            return None
        job = Job.from_dict(header["config"])
        job.metadata["total_tasks_count_override"] = header["total"]
        job.completed_tasks_count = header["completed"]
        job.failed_tasks_count = header["failed"]
        terminal = {
            "complete": JobState.FAILED if header["failed"] else JobState.COMPLETED,
            "cancelled": JobState.CANCELLED,
            "failed": JobState.FAILED,
        }.get(header["status"])
        if terminal is not None:
            job.state = terminal
        return job

    async def update_job_by_id(
        self,
        job_id: str,
        *,
        name: str | None = None,
        schedule: JobSchedule | None = None,
        tasks: Iterable[Task] | AsyncIterable[Task] | None = None,
        route_immediate_job: bool = True,
        args_patch: dict[str, Any] | None = None,
    ) -> Job:
        """Update job metadata, scheduling, and optionally replace tasks."""

        job = await self.fetch_job_for_update(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        if job.metadata.get("execution_engine") == "ledger":
            from .ledger import ExecutionLedger
            from ..database.threads import run_database_call
            if tasks is not None:
                from .replacement import replace_tasks
                return await replace_tasks(self, job, tasks, name=name, schedule=schedule)
            config = await run_database_call(ExecutionLedger().control, job_id, "update",
                name=name, schedule=schedule.to_dict() if schedule is not None else None)
            return Job.from_dict(config)

        if job.metadata.get("mps_manifest"):
            if tasks is not None:
                from .replacement import replace_tasks

                return await replace_tasks(self, job, tasks, name=name, schedule=schedule)
            from .durable import update_job_config

            return await update_job_config(self, job, name=name, schedule=schedule)

        from .ordinary_edit import update

        return await update(
            self, job_id, name=name, schedule=schedule, tasks=tasks,
            route_immediate_job=route_immediate_job, args_patch=args_patch,
        )

    async def _enqueue_ready_tasks_from_storage(
        self,
        *,
        job: Job,
        backend_id: str,
        base_priority: int,
    ) -> None:
        """Stream ready tasks from Redis so routing does not duplicate large ID lists."""

        lookup = {task.task_id: task for task in job.tasks}
        async for batch_ids in self.storage.iter_ready_task_ids(
            job.job_id,
            batch_size=1000,
        ):
            items: list[tuple[str, str, int]] = []
            for task_id in batch_ids:
                task = lookup.get(task_id)
                priority = base_priority
                if task and task.payload:
                    raw_priority = task.payload_value("priority")
                    if isinstance(raw_priority, int):
                        priority = raw_priority
                    elif isinstance(raw_priority, str) and raw_priority.strip():
                        try:
                            priority = int(raw_priority)
                        except ValueError:
                            priority = base_priority
                items.append((job.job_id, task_id, priority))
            if items:
                await self.router.enqueue_ready_tasks(
                    backend_id=backend_id,
                    items=items,
                )

    async def _resolve_backend_assignment(self, job_id: str) -> str | None:
        """Resolve the backend currently responsible for a job, if any."""

        state_key = self.storage.JOB_STATE_KEY.format(job_id=job_id)
        raw_backend_id = await self.redis_manager.hget(state_key, "backend_id")
        if raw_backend_id:
            return (
                raw_backend_id.decode()
                if isinstance(raw_backend_id, bytes)
                else str(raw_backend_id)
            )

        client = getattr(self.redis_manager, "client", self.redis_manager)
        try:
            raw_backends = await client.zrange(self.router.LOAD_KEY, 0, -1)
            for b in raw_backends:
                b_str = b.decode() if isinstance(b, bytes) else str(b)
                if b_str:
                    key_str = self.router._backend_jobs_key(b_str)
                    if hasattr(self.redis_manager, "sismember"):
                        is_member = await self.redis_manager.sismember(key_str, job_id)
                    elif hasattr(client, "sismember"):
                        is_member = await client.sismember(key_str, job_id)
                    else:
                        members = await self.redis_manager.smembers(key_str)
                        is_member = job_id in members
                    if is_member:
                        return b_str
        except Exception:
            pass
        return None

    async def _route_pending_job(
        self,
        job: Job,
        *,
        backend_id: str | None,
    ) -> None:
        """Route an already-persisted immediate job back into the queueing layer."""

        priority = await self.storage.get_job_priority(job.job_id)
        if backend_id:
            await self.router.remove_ready_tasks_for_job(backend_id, job.job_id)
            await self._enqueue_ready_tasks_from_storage(
                job=job,
                backend_id=backend_id,
                base_priority=priority,
            )
        else:
            await self.router.remove_job_from_unclaimed_pool(job.job_id)
            await self.router.add_job_to_unclaimed_pool(job.job_id)
