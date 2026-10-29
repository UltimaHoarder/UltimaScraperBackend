"""Execute existing handlers against SQL attempt ownership and writer fences."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import UTC, datetime

from sqlalchemy.exc import OperationalError

from ..database.threads import run_database_call
from ..database.write_queue import stable_command_id
from .ledger import Claim, ExecutionLedger, StaleAttempt, archive_parallel_passes
from .models import Job, Task, TaskCancelled, TaskResult, TaskState
from .profile_completion import (
    PROFILE_COMPLETION_KEY,
    is_final_profile,
    profile_completion_intent,
)

logger = logging.getLogger(__name__)
_service_task: asyncio.Task[None] | None = None


def database_unavailable(error):
    """Recognize connection outages, including PgBouncer's queue timeout."""
    original = getattr(error, "orig", error)
    sqlstate = getattr(original, "sqlstate", None)
    message = str(original)
    return isinstance(error, OperationalError) and (
        sqlstate in {"57P01", "57P02", "57P03"}
        or (sqlstate is not None and sqlstate.startswith("08") and sqlstate != "08P01")
        or "server_login_retry" in message
        or (
            sqlstate == "08P01"
            and ("query_wait_timeout" in message or "server conn crashed?" in message)
        )
        or sqlstate is None
    )


class _ServiceRetry:
    """Back off unavailable SQL passes without replaying an open transaction."""

    def __init__(self, operation):
        self.operation = operation
        self.failures = 0
        self.last_warning = None

    async def failed(self, error):
        original = getattr(error, "orig", error)
        cached_login = "server_login_retry" in str(original)
        pool_wait = "query_wait_timeout" in str(original)
        unavailable = database_unavailable(error)
        if not unavailable:
            logger.exception("Execution ledger %s pass failed", self.operation)
            await asyncio.sleep(1)
            return
        self.failures += 1
        delay = min(30, (15 if cached_login or pool_wait else 1) * 2 ** min(self.failures - 1, 5))
        now = time.monotonic()
        if self.last_warning is None or now - self.last_warning >= 60:
            logger.warning(
                "Execution ledger %s is waiting for the database%s; retrying in %ss",
                self.operation,
                " (PgBouncer cached login failure)" if cached_login else " (PgBouncer pool wait timeout)" if pool_wait else "",
                delay,
            )
            self.last_warning = now
        await asyncio.sleep(delay)

    def succeeded(self):
        if self.failures:
            logger.info("Execution ledger %s database access restored", self.operation)
        self.failures = 0
        self.last_warning = None


def ledger_claim_page_size(batch, configured=None):
    """Claim page from ``worker_pool.claim_batch_size``, capped at the ledger batch."""
    from ..deprecated_env import deprecated_env_int

    size = deprecated_env_int("ULTIMA_CLAIM_BATCH_SIZE") or configured or batch
    return min(batch, size)


def worker_claim_capacity(pool, active_count, concurrency, batch):
    """Fetch slots and bounded persistence slots share one owned-attempt set."""
    persisting = len(getattr(pool, "_ledger_persisting_tasks", ()))
    extra = min(concurrency, batch)
    total_limit = concurrency + extra
    return max(
        0, min(concurrency - active_count + persisting, total_limit - active_count)
    ), total_limit


async def wait_for_worker_capacity(active_tasks, stage_changed):
    wake = asyncio.create_task(stage_changed.wait())
    try:
        await asyncio.wait(
            {*active_tasks, wake}, return_when=asyncio.FIRST_COMPLETED, timeout=1
        )
    finally:
        wake.cancel()
        await asyncio.gather(wake, return_exceptions=True)


async def start_service(backend):
    """Start the control process's SQL planner, recovery and event relay."""
    global _service_task
    if _service_task is None or _service_task.done():
        _service_task = asyncio.create_task(service(backend), name="execution-ledger")


async def stop_service():
    global _service_task
    if _service_task is not None:
        _service_task.cancel()
        await asyncio.gather(_service_task, return_exceptions=True)
        _service_task = None


class LedgerRouterAdapter:
    """Claim SQL attempts using the worker's available capacity."""

    def __init__(self, pool):
        self.pool, self.ledger = pool, ExecutionLedger()
        self.claims = {}
        pool._ledger_claims = self.claims

    async def claim_ready_tasks(self, backend_id, *, count):
        capabilities = [*self.pool.supported_sites, "api_commands"]
        claims = []
        if self.pool.worker_count > 1 and self.pool.worker_index == 0:
            # Worker zero serves interactive commands first, then fills its
            # remaining slots with batch work. Every claim already takes runs
            # in priority order, so API commands still go ahead of MPS on any
            # worker; reserving this process idled one core in every run.
            claims = await run_database_call(
                self.ledger.claim_many,
                owner=self.pool.worker_id,
                backend_id=backend_id,
                capabilities=capabilities,
                lane="interactive",
                count=count,
            )
        if len(claims) < count:
            claims = [*claims, *await run_database_call(
                self.ledger.claim_many,
                owner=self.pool.worker_id,
                backend_id=backend_id,
                capabilities=capabilities,
                lane=None,
                count=count - len(claims),
            )]
        result = []
        for claim in claims:
            identity = (claim.run_id, claim.task_id)
            self.claims[identity] = (self.ledger, claim)
            result.append(identity)
        return result

    async def renew_buffered(self, identities):
        from .ledger_batches import checkpoint_request

        requests = []
        for identity in identities:
            _, claim = self.claims[identity]
            request = checkpoint_request(self.ledger, claim, claim.task, renew=True)
            request.update(checkpoint=False, payload=None)
            requests.append(request)
        results = await run_database_call(self.ledger.checkpoint_batch, requests)
        for identity, error in zip(identities, results, strict=True):
            if error is not None:
                self.claims.pop(identity, None)
        return {
            identity
            for identity, error in zip(identities, results, strict=True)
            if error is None
        }


class LedgerTaskStorage:
    """Handler progress checkpoints; completion is committed by the runner."""

    def __init__(
        self,
        ledger: ExecutionLedger,
        claim: Claim,
        task: Task,
        redis,
        metrics=None,
        batcher=None,
    ):
        self.ledger, self.claim, self.task, self.redis = ledger, claim, task, redis
        # Claim already committed the running state, owner and attempt start.
        # Measure the interval from a completed checkpoint, not SQL admission.
        self._last_checkpoint = time.monotonic()
        self._persisting = False
        self._checkpoint_lock = asyncio.Lock()
        self.metrics = metrics
        self.batcher = batcher

    async def persist_task(self, *, job_id, task, **kwargs):
        if job_id != self.claim.run_id:
            raise ValueError("Task progress belongs to another run")
        # Nested subtasks live inside the parent's bounded task payload.
        if task.task_id != self.claim.task_id:
            return
        if task.state in {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED}:
            return
        await self.checkpoint()

    async def checkpoint(self, *, persisting=False, renew=False):
        async with self._checkpoint_lock:
            if (
                not (persisting and not self._persisting)
                and time.monotonic() - self._last_checkpoint < 1
            ):
                return
            call = self.metrics.database_call if self.metrics else run_database_call
            payload = self.task.to_dict()
            if PROFILE_COMPLETION_KEY in self.claim.task:
                payload[PROFILE_COMPLETION_KEY] = self.claim.task[PROFILE_COMPLETION_KEY]
            kwargs = (
                {"phase": "writer_prepare" if persisting else "progress"}
                if self.metrics
                else {}
            )
            if self.batcher is None:
                await call(
                    self.ledger.progress,
                    self.claim,
                    payload,
                    persisting=persisting,
                    renew=renew,
                    **kwargs,
                )
            else:
                from .ledger_batches import checkpoint_request

                await self.batcher.submit(
                    "checkpoint",
                    checkpoint_request(
                        self.ledger,
                        self.claim,
                        payload,
                        persisting=persisting,
                        renew=renew,
                    ),
                    metrics=self.metrics,
                    phase="writer_prepare" if persisting else "progress",
                )
            self._persisting |= persisting
            self._last_checkpoint = time.monotonic()

    async def get_task_output(self, job_id, task_id):
        return await run_database_call(self.ledger.output, job_id, task_id)


class FencedWriter:
    def __init__(self, writer, ledger, claim, storage):
        self.writer, self.ledger, self.claim, self.storage = (
            writer,
            ledger,
            claim,
            storage,
        )
        self.pending = set()
        self._profile_result = None
        self._writes_started = 0

    def prepare_profile_completion(self, result):
        if is_final_profile(self.storage.task):
            if self.pending or self._writes_started:
                raise ValueError("A final profile completion must precede its only write")
            self._profile_result = result

    def __getattr__(self, name):
        return getattr(self.writer, name)

    async def _prepare(self, operation, payload, kwargs):
        if self._profile_result is not None and self._writes_started:
            raise ValueError("A final profile completion allows only one write")
        # A retry uses another attempt's fence and command identity; replay of
        # one accepted attempt still gets the same committed writer receipt.
        identity = {
            "run_id": self.claim.run_id,
            "task_id": self.claim.task_id,
            "token": self.claim.token,
            "owner": self.claim.owner,
        }
        body_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()
        kwargs["execution_key"] = stable_command_id(
            "execution-mutation-v2",
            self.claim.run_id,
            self.claim.task_id,
            operation.value,
            kwargs.get("command_id") or body_hash,
        )
        kwargs["command_id"] = stable_command_id(
            "execution-write-v2", self.claim.token, kwargs["execution_key"]
        )
        kwargs["execution_attempt"] = identity
        if self._profile_result is not None:
            self.claim.task[PROFILE_COMPLETION_KEY] = profile_completion_intent(
                self.storage.task, self._profile_result,
                execution_key=kwargs["execution_key"], operation=operation.value,
                site=kwargs.get("site") or self.storage.task.site_name,
            )
        await self.storage.checkpoint(persisting=True, renew=True)
        self._writes_started += 1

    async def submit(self, operation, payload, **kwargs):
        await self._prepare(operation, payload, kwargs)
        return await self.writer.submit(operation, payload, **kwargs)

    async def enqueue(self, operation, payload, **kwargs):
        await self._prepare(operation, payload, kwargs)
        result = await self.writer.enqueue(operation, payload, **kwargs)
        self.pending.add(result[0] if isinstance(result, tuple) else result)
        return result

    async def drain(self):
        for command_id in tuple(self.pending):
            await self.wait_for_result(command_id)
        self.pending.clear()

    async def wait_for_result(self, command_id, **kwargs):
        result = await self.writer.wait_for_result(command_id, **kwargs)
        self.pending.discard(command_id)
        return result


class ExecutionTelemetry:
    """Reuse the existing bounded diagnostic aggregate for ledger attempts."""

    def __init__(self):
        self.started_ms = int(time.time() * 1000)
        self.setup = {}
        self.handler = {}
        self.routes = {}
        self.ledger = {}
        self.route_active_ms = 0.0
        self.route_attempts = 0
        self.setup_ms = self.handler_ms = self.post_ms = self.drain_ms = (
            self.release_ms
        ) = 0.0

    async def database_call(self, function, *args, phase, **kwargs):
        started = time.monotonic()
        execution_ms = 0.0
        execution_started = execution_finished = None

        def execute_call():
            nonlocal execution_ms, execution_started, execution_finished
            execution_started = time.monotonic()
            try:
                return function(*args, **kwargs)
            finally:
                execution_finished = time.monotonic()
                execution_ms = (execution_finished - execution_started) * 1000

        try:
            return await run_database_call(execute_call)
        finally:
            finished = time.monotonic()
            self.record_database_span(
                phase, started, finished, execution_started, execution_finished
            )

    def record_database_span(
        self,
        phase,
        started,
        finished,
        execution_started,
        execution_finished,
        *,
        submitted=None,
    ):
        execution_ms = (
            (execution_finished - execution_started) * 1000
            if execution_started is not None and execution_finished is not None
            else 0.0
        )
        elapsed_ms = (finished - started) * 1000
        for name, duration in (
            (phase, elapsed_ms),
            (f"{phase}_execution", execution_ms),
            (f"{phase}_admission", max(0.0, elapsed_ms - execution_ms)),
            (
                f"{phase}_thread_wait_admission",
                max(
                    0.0,
                    (
                        (
                            execution_started
                            if execution_started is not None
                            else finished
                        )
                        - (started if submitted is None else submitted)
                    )
                    * 1000,
                ),
            ),
            (
                f"{phase}_resume_admission",
                max(0.0, (finished - execution_finished) * 1000)
                if execution_finished is not None
                else 0.0,
            ),
        ):
            entry = self.ledger.setdefault(name, {"ms": 0.0, "count": 0})
            entry["ms"] += duration
            entry["count"] += 1
        if submitted is not None:
            entry = self.ledger.setdefault(
                f"{phase}_batch_wait_admission", {"ms": 0.0, "count": 0}
            )
            entry["ms"] += max(0.0, (submitted - started) * 1000)
            entry["count"] += 1

    def record_handler_timing(self, name, duration):
        entry = self.handler.setdefault(name, {"ms": 0.0, "count": 0})
        entry["ms"] += max(0.0, float(duration))
        entry["count"] += 1

    def record_route_timing(self, duration):
        self.route_active_ms += max(0.0, float(duration))
        self.route_attempts += 1

    def _route(self, route_id):
        if route_id != "direct" and not (
            isinstance(route_id, str)
            and len(route_id) == 22
            and route_id.startswith("route-")
            and all(c in "0123456789abcdef" for c in route_id[6:])
        ):
            return None
        return self.routes.setdefault(
            route_id,
            {
                **{
                    f"{name}_count": 0
                    for name in (
                        "lookup_found",
                        "lookup_not_found",
                        "lookup_error",
                        "lookup_timeout",
                        "lookup_cancelled",
                        "guest_init_failure",
                        "guest_init_cancelled",
                    )
                },
                "route_attempts": 0,
                "lookup_observations": 0,
                "route_active_ms": 0.0,
                "guest_init_ms": 0.0,
                "lookup_ms": 0.0,
            },
        )

    def record_route_observation(
        self,
        route_id,
        route_active_ms,
        guest_init_ms,
        lookup_ms,
        lookup_observed,
        lookup_outcome,
    ):
        entry = self._route(route_id)
        if entry is None:
            return
        entry["route_attempts"] += 1
        entry["route_active_ms"] += max(0.0, float(route_active_ms))
        entry["guest_init_ms"] += max(0.0, float(guest_init_ms))
        if lookup_observed:
            entry["lookup_observations"] += 1
            entry["lookup_ms"] += max(0.0, float(lookup_ms))
        field = {
            "found": "lookup_found_count",
            "not_found": "lookup_not_found_count",
        }.get(lookup_outcome, f"{lookup_outcome}_count")
        if field in entry:
            entry[field] += 1

    def record_route_selection(self, route_id, role, deferred, retry):
        entry = self._route(route_id)
        if entry is None or role not in {"primary", "hedge", "fallback"}:
            return
        for field in (
            f"{role}_selected_count",
            "deferred_selected_count" if deferred else "healthy_selected_count",
        ):
            entry[field] = entry.get(field, 0) + 1
        if retry:
            entry["retry_selected_count"] = entry.get("retry_selected_count", 0) + 1

    def snapshot(self, timed_writer):
        setup = {
            **self.setup,
            "residual": {
                "ms": max(
                    0.0,
                    self.setup_ms - sum(entry["ms"] for entry in self.setup.values()),
                ),
                "count": 1,
            },
        }
        result = {
            "setup_ms": self.setup_ms,
            "handler_ms": self.handler_ms,
            "post_ms": self.post_ms,
            "release_ms": self.release_ms,
            "route_active_ms": self.route_active_ms,
            "route_attempts": self.route_attempts,
            "setup_subphases_json": json.dumps(setup),
            "route_lookup_stats_json": json.dumps(self.routes),
            "guest_auth_stats_json": json.dumps(
                {
                    key.removeprefix("guest_auth_"): value
                    for key, value in self.handler.items()
                    if key.startswith("guest_auth_")
                }
            ),
            "terminal_stats_json": json.dumps(
                {"counter_transition": {"ms": self.post_ms, "count": 1}}
            ),
            "ledger_stats_json": json.dumps(self.ledger),
        }
        for name in (
            "guest_init",
            "initial_lookup",
            "refresh_api",
            "fallback_retry",
            "serialization",
        ):
            entry = self.handler.get(name, {})
            result[f"handler_{name}_ms"] = entry.get("ms", 0.0)
            result[f"handler_{name}_count"] = entry.get("count", 0)
        result["writer_ms"] = self.drain_ms
        if timed_writer:
            result["writer_ms"] += timed_writer.elapsed_ms
            for target, source in {
                "admission_ms": "admission_ms",
                "queue_ms": "queue_ms",
                "execution_ms": "execution_ms",
                "commit_ms": "commit_ms",
                "overhead_ms": "writer_overhead_ms",
                "delivery_ms": "delivery_ms",
                "timed_submits": "timed_submit_count",
                "timing_coverage": "timing_coverage_count",
            }.items():
                result[f"writer_{target}"] = getattr(timed_writer, source)
            result.update(timed_writer.profile_telemetry_kwargs())
        return result


def _transient(exc):
    import aiohttp

    visited = set()
    while exc is not None and id(exc) not in visited:
        visited.add(id(exc))
        # The next attempt uses a fresh transaction and the existing bounded
        # SQL retry schedule, ownership fence, and logical mutation identity.
        if database_unavailable(exc) or isinstance(
            exc, (TimeoutError, ConnectionError, aiohttp.ClientConnectionError)
        ):
            return True
        exc = exc.__cause__
    return False


async def execute(
    pool,
    ledger: ExecutionLedger,
    claim: Claim,
    *,
    claim_wait_ms: float = 0.0,
    effective_worker_concurrency: int = 0,
):
    """Return the existing worker-loop result without Redis lifecycle writes."""
    job, task = Job.from_dict(claim.job), Task.from_dict(claim.task)
    from .ledger_batches import completion_request
    from .lifecycle_batcher import worker_batcher

    batcher = worker_batcher(pool, ledger)
    metrics = ExecutionTelemetry()
    storage = LedgerTaskStorage(ledger, claim, task, pool.redis, metrics, batcher)
    started = time.monotonic()
    retry = False
    writer = FencedWriter(pool.database_writes, ledger, claim, storage)
    from ..workers.worker_pool import _TimedDatabaseWrites

    timed_writer = (
        _TimedDatabaseWrites(writer) if pool.database_writes is not None else None
    )
    registered = committed = False
    identity = (claim.run_id, claim.task_id)
    if not hasattr(pool, "_ledger_persisting_tasks"):
        pool._ledger_persisting_tasks = set()

    def release_fetch_capacity():
        # Only the independent profile handler's final write releases a slot.
        if is_final_profile(task):
            pool._ledger_persisting_tasks.add(identity)
            changed = getattr(pool, "_ledger_stage_changed", None)
            if changed is not None:
                changed.set()

    async def publish_metrics():
        telemetry_storage = getattr(pool, "task_job_storage", None)
        if telemetry_storage is not None:
            try:
                created = datetime.fromisoformat(task.created_at)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=UTC)
                await telemetry_storage.record_task_runtime_telemetry(
                    job.job_id,
                    started_ms=metrics.started_ms,
                    finished_ms=int(time.time() * 1000),
                    queue_wait_ms=max(
                        0.0, metrics.started_ms - created.timestamp() * 1000
                    ),
                    success=task.state == TaskState.COMPLETED,
                    completed_count=None,
                    total_tasks=job.total_tasks,
                    claim_wait_ms=claim_wait_ms,
                    worker_id=pool.worker_id,
                    # Every worker, including the interactive-first worker
                    # zero, now takes batch work.
                    expected_workers=max(1, getattr(pool, "worker_count", 1)),
                    effective_worker_concurrency=effective_worker_concurrency,
                    **metrics.snapshot(timed_writer),
                )
            except Exception:
                logger.debug(
                    "Could not publish ledger runtime telemetry", exc_info=True
                )

    async def run_handler():
        nonlocal registered
        setup_started = time.monotonic()
        if claim.number > 1 and PROFILE_COMPLETION_KEY in claim.task:
            recovered = await metrics.database_call(
                ledger.profile_completion, claim, task=task, phase="writer_completion_recovery"
            )
            if recovered is not None:
                task.result, task.state = recovered, TaskState.COMPLETED
                task.attempts = claim.number
                task.completed_at = task.updated_at = recovered.completed_at
                metrics.setup_ms = (time.monotonic() - setup_started) * 1000
                return recovered
        phase_started = time.monotonic()
        await pool._refresh_storage_config_before_task()
        metrics.setup["config_refresh"] = {
            "ms": (time.monotonic() - phase_started) * 1000,
            "count": 1,
        }
        phase_started = time.monotonic()
        await pool._ensure_auth_loaded_for_task(task, task.site_name)
        metrics.setup["auth_load"] = {
            "ms": (time.monotonic() - phase_started) * 1000,
            "count": 1,
        }
        previous = None
        if task.depends_on:
            outputs = {
                key: await storage.get_task_output(job.job_id, key)
                for key in task.depends_on
            }
            previous = (
                outputs[task.depends_on[0]] if len(task.depends_on) == 1 else outputs
            )
        registry = getattr(pool, "worker_registry", None)
        if registry is not None:
            try:
                await registry.start_task(
                    task_id=task.task_id,
                    task_type=task.task_type.value,
                    metadata={"job_id": job.job_id, "job_name": job.name},
                )
                registered = True
            except Exception:
                logger.debug("Could not register ledger attempt", exc_info=True)
        context = {
            "api": pool.apis[task.site_name.lower()],
            "worker_id": pool.worker_id,
            "site": task.site_name,
            "db_managers": pool.db_managers,
            "user_manager": pool.user_manager,
            "config": pool.config,
            "redis": pool.redis,
            "database_writes": timed_writer or writer,
            "record_route_timing": metrics.record_route_timing,
            "record_route_observation": metrics.record_route_observation,
            "record_route_selection": metrics.record_route_selection,
            "record_handler_timing": metrics.record_handler_timing,
            "release_fetch_capacity": release_fetch_capacity,
            "prepare_profile_completion": writer.prepare_profile_completion,
        }
        metrics.setup_ms = (time.monotonic() - setup_started) * 1000
        phase_started = time.monotonic()
        try:
            result = await task.run(
                job=job, storage=storage, context=context, previous_output=previous
            )
            drain_started = time.monotonic()
            try:
                await writer.drain()
            finally:
                metrics.drain_ms = (time.monotonic() - drain_started) * 1000
        finally:
            metrics.handler_ms = (time.monotonic() - phase_started) * 1000
        return result

    async def heartbeat():
        recovery = _ServiceRetry("attempt renewal")
        while True:
            await asyncio.sleep(max(0.1, ledger.limits.lease_seconds / 3))
            while True:
                try:
                    await metrics.database_call(ledger.renew, claim, phase="renew")
                except OperationalError as exc:
                    if not database_unavailable(exc):
                        raise
                    # Retry on a fresh connection. SQL still checks the owner,
                    # token and database clock; an expired lease is never revived.
                    await recovery.failed(exc)
                else:
                    recovery.succeeded()
                    break

    handler = asyncio.create_task(run_handler())
    keeper = asyncio.create_task(heartbeat())
    try:
        done, _ = await asyncio.wait(
            {handler, keeper}, return_when=asyncio.FIRST_COMPLETED
        )
        if keeper in done:
            await keeper  # Lease loss or a database failure cancels execution.
            raise StaleAttempt("Attempt heartbeat stopped")
        try:
            await handler
        except TaskCancelled:
            raise StaleAttempt("Task stopped before completing its attempt") from None
        except Exception as exc:  # noqa: BLE001 - persist handler/setup failures as attempt evidence
            retry = _transient(exc)
            if task.result is None:
                task.result = TaskResult(
                    success=False, message=str(exc), errors=[str(exc)]
                )
            task.state = TaskState.FAILED
            task.completed_at = task.result.completed_at
            if PROFILE_COMPLETION_KEY in claim.task:
                recovered = await metrics.database_call(
                    ledger.profile_completion, claim, task=task, phase="writer_completion_recovery"
                )
                if recovered is not None:
                    task.result, task.state = recovered, TaskState.COMPLETED
                    task.completed_at = task.updated_at = recovered.completed_at
                    retry = False
        finish_started = time.monotonic()
        await batcher.submit(
            "finish",
            completion_request(
                ledger,
                claim,
                task,
                retry=retry,
                timings={
                    "execution_ms": (time.monotonic() - started) * 1000,
                    "claim_wait_ms": claim_wait_ms,
                    **metrics.snapshot(timed_writer),
                },
            ),
            metrics=metrics,
            phase="finish",
        )
        metrics.post_ms = (time.monotonic() - finish_started) * 1000
        committed = True
        return True, False, None  # The planner owns overall run finalization.
    except StaleAttempt:
        logger.info(
            "Discarded stale execution attempt %s/%s", claim.run_id, claim.task_id
        )
        return True, False, None
    finally:
        pool._ledger_persisting_tasks.discard(identity)
        cleanup_started = time.monotonic()
        handler.cancel()
        keeper.cancel()
        await asyncio.gather(handler, keeper, return_exceptions=True)
        if registered:
            try:
                await pool.worker_registry.complete_task(
                    success=committed and task.state == TaskState.COMPLETED,
                    task_id=task.task_id,
                )
            except Exception:
                logger.debug("Could not complete ledger registry task", exc_info=True)

        metrics.release_ms = (time.monotonic() - cleanup_started) * 1000
        if committed:
            await publish_metrics()


async def publish_refill_diagnostics(client, observations):
    """Publish committed planner measurements in one bounded Redis round trip."""
    from collections import defaultdict

    from . import mps_state

    by_job = defaultdict(list)
    for observation in observations:
        payload = dict(observation)
        job_id = payload.pop("job_id")
        by_job[job_id].append(json.dumps(payload))
    pipeline = client.pipeline(transaction=False)
    for job_id, values in by_job.items():
        key = mps_state.REFILL_TRACE_KEY.format(job_id=job_id)
        pipeline.rpush(key, *values)
        pipeline.ltrim(key, -256, -1)
        pipeline.expire(key, mps_state.REFILL_TRACE_TTL_SECONDS)
    if by_job:
        await pipeline.execute()


# Durable job_progress summaries replace per-task outbox rows for durable readers.
JOB_PROGRESS_INTERVAL_SECONDS = 30.0
# How often the leader checks queue-table index sizes for an online rebuild.
QUEUE_INDEX_MAINTENANCE_SECONDS = 60.0


async def maintain_queue_indexes(ledger, coordination, *, interval=None, passes=None):
    """Leader loop: rebuild bloated queue indexes online, one per pass.

    ``passes`` bounds the loop for tests; the service runs it forever.
    """
    retry = _ServiceRetry("queue index maintenance")
    completed = 0
    while passes is None or completed < passes:
        await coordination.wait()
        try:
            rebuilt = await run_database_call(ledger.reindex_bloated_queue_indexes)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await retry.failed(error)
            completed += 1
            continue
        retry.succeeded()
        for item in rebuilt:
            logger.info(
                "Rebuilt queue index %s online: %.1f MB -> %.1f MB in %.0f ms",
                item["index"], item["before_bytes"] / 1048576,
                item["after_bytes"] / 1048576, item["ms"],
            )
        completed += 1
        if passes is None or completed < passes:
            await asyncio.sleep(QUEUE_INDEX_MAINTENANCE_SECONDS if interval is None else interval)


PLANNER_BATCH_TICKS = 8
_ARCHIVE_PROCESS_ON = {"1", "true", "on", "yes"}


def archive_process_enabled() -> bool:
    """ULTIMA_ARCHIVE_PROCESS=1: archive passes run in a dedicated process.

    Default off. The launcher and the control plane read the same setting, so
    exactly one of them runs the archiver loops.
    """
    from ultima_scraper_core import runtime_env_value

    value = runtime_env_value("ULTIMA_ARCHIVE_PROCESS")
    return value is not None and value.strip().lower() in _ARCHIVE_PROCESS_ON


def _queue_diagnostics(diagnostic_queue, observations):
    if diagnostic_queue.full():
        diagnostic_queue.get_nowait()
        diagnostic_queue.task_done()
    diagnostic_queue.put_nowait(observations)


async def archive_loop(ledger, coordination, wake, diagnostic_queue):
    """One archive pass after another while this process holds its lease."""
    from .idle import IdleDelay

    retry = _ServiceRetry("archive")
    idle = IdleDelay()
    while True:
        await coordination.wait()
        try:
            observations = []
            outcome = ["deferred"]
            archived = await run_database_call(
                ledger.archive_tick, diagnostics=observations, outcome=outcome
            )
            if observations:
                _queue_diagnostics(diagnostic_queue, observations)
            retry.succeeded()
            if archived:
                idle.reset()
                await asyncio.sleep(0)
            else:
                await idle.wait(wake, idle=outcome == ["idle"])
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await retry.failed(error)


async def service(backend):
    """Keep SQL refill independent of event transport and diagnostic publication."""
    from anyio import from_thread

    from ..redis_channels import HOOKS_CHANNEL

    from .idle import IdleDelay, SchedulingLease, listen_for_wakeups

    ledger = ExecutionLedger()
    coordination = SchedulingLease(backend.redis)
    wakes = {name: asyncio.Event() for name in ("planner", "archive", "relay")}
    redis_config = getattr(backend.redis, "config", None)
    if redis_config is not None:
        from . import live_events

        live_events.install_redis(host=redis_config.host, port=redis_config.port,
                                  db=redis_config.db, password=redis_config.password)
    diagnostic_queue = asyncio.Queue(maxsize=16)

    async def planner():
        retry = _ServiceRetry("planner")
        idle = IdleDelay()
        while True:
            await coordination.wait()
            try:
                observations = []
                outcome = ["deferred"]
                progressed = False
                productive_ticks = 0
                for _ in range(PLANNER_BATCH_TICKS):
                    if not coordination.allowed:
                        break
                    if (
                        await run_database_call(
                            ledger.tick, diagnostics=observations, refill_only=True, outcome=outcome
                        )
                        is None
                    ):
                        break
                    progressed = True
                    productive_ticks += 1
                if observations:
                    if diagnostic_queue.full():
                        diagnostic_queue.get_nowait()
                        diagnostic_queue.task_done()
                    diagnostic_queue.put_nowait(observations)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await retry.failed(error)
                continue
            retry.succeeded()
            if productive_ticks == PLANNER_BATCH_TICKS:
                # Every tick admitted work, so more is waiting: plan again now.
                # A short or empty batch (window full, deferred, idle) still
                # waits, so a busy window is not polled in a tight loop.
                idle.reset()
                await asyncio.sleep(0)
                continue
            await idle.wait(wakes["planner"], idle=not progressed and outcome == ["idle"])

    async def recovery():
        retry = _ServiceRetry("recovery")
        while True:
            await coordination.wait()
            try:
                await run_database_call(ledger.recover_expired)
                retry.succeeded()
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await retry.failed(error)

    async def archiver():
        await archive_loop(ledger, coordination, wakes["archive"], diagnostic_queue)

    async def publish_async(payloads):
        if backend.redis is None or not backend.redis.is_connected:
            raise ConnectionError("Event transport is disconnected")
        pipeline = backend.redis.client.pipeline(transaction=False)
        for payload in payloads:
            pipeline.publish(HOOKS_CHANNEL, json.dumps(payload))
        await pipeline.execute()

    def publish(payloads):
        from_thread.run(publish_async, payloads)

    async def relay():
        retry = _ServiceRetry("event relay")
        idle = IdleDelay()
        while True:
            await coordination.wait()
            try:
                outcome = ["deferred"]
                delivered = await run_database_call(ledger.relay, publish_many=publish, outcome=outcome)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await retry.failed(error)
            else:
                retry.succeeded()
                # Drain backlog in bounded transactions without gating refill.
                if delivered == ledger.limits.batch or outcome == ["backlog"]:
                    idle.reset()
                    await asyncio.sleep(0)
                else:
                    await idle.wait(wakes["relay"], idle=outcome == ["idle"])

    async def progress():
        retry = _ServiceRetry("job progress")
        previous = {}
        while True:
            await coordination.wait()
            try:
                previous = await run_database_call(ledger.record_progress, previous=previous)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await retry.failed(error)
                continue
            retry.succeeded()
            await asyncio.sleep(JOB_PROGRESS_INTERVAL_SECONDS)

    async def queue_maintenance():
        await maintain_queue_indexes(ledger, coordination)

    async def diagnostics():
        while True:
            observations = await diagnostic_queue.get()
            try:
                if backend.redis is not None and backend.redis.is_connected:
                    await publish_refill_diagnostics(backend.redis.client, observations)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug(
                    "Could not publish ledger refill diagnostics", exc_info=True
                )
            finally:
                diagnostic_queue.task_done()

    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(coordination.run(), name="ledger-scheduling-coordination")
            tasks.create_task(listen_for_wakeups(backend.redis, wakes.values()), name="ledger-wake-hints")
            tasks.create_task(planner(), name="ledger-planner")
            tasks.create_task(recovery(), name="ledger-recovery")
            # ULTIMA_ARCHIVE_PARALLEL_PASSES > 1 runs concurrent passes on
            # disjoint terminal rows; read at service start. With
            # ULTIMA_ARCHIVE_PROCESS the launcher runs them in their own
            # process instead, off this process's interpreter lock.
            if not archive_process_enabled():
                for index in range(archive_parallel_passes()):
                    tasks.create_task(
                        archiver(), name="ledger-archiver" if index == 0 else f"ledger-archiver-{index}"
                    )
            tasks.create_task(relay(), name="ledger-event-relay")
            tasks.create_task(progress(), name="ledger-job-progress")
            tasks.create_task(queue_maintenance(), name="ledger-queue-maintenance")
            tasks.create_task(diagnostics(), name="ledger-refill-diagnostics")
    finally:
        await coordination.close()
