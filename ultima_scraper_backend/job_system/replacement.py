"""Stage edited MPS tasks in SQL and reconcile their bounded Redis window."""

from __future__ import annotations

import json
from ..redis_channels import KEY_PREFIX

from collections.abc import AsyncIterable
from types import SimpleNamespace
from typing import Any, Iterable
from uuid import uuid4

from ..database.write_queue import (
    DatabaseWriteCommand,
    DatabaseWriteOperation,
    stable_command_id,
)
from . import Job, JobManager, JobSchedule, JobState, JobStorage, Task
from . import mps_state, runtime_admission


async def replace_tasks(
    manager: JobManager,
    job: Job,
    tasks: Iterable[Task] | AsyncIterable[Task],
    *,
    name: str | None,
    schedule: JobSchedule | None,
) -> Job:
    from . import durable as mps_durable

    if name is not None and not name.strip():
        raise ValueError("Job name cannot be empty")
    if job.state == JobState.RUNNING:
        raise RuntimeError("Cannot update tasks while job is running")
    if job.metadata.get("execution_engine") == "ledger" and job.started_at is not None:
        raise ValueError("A started ledger run keeps its inputs; create a retry run to change tasks")
    async def values():
        if isinstance(tasks, AsyncIterable):
            async for task in tasks:
                yield task
        else:
            for task in tasks:
                yield task

    iterator = values()
    first = await anext(iterator, None)
    if first is None:
        raise ValueError("No valid tasks provided")
    manifest_id = f"definition-{uuid4().hex}"
    writes = manager.database_writes
    await writes.submit(
        DatabaseWriteOperation.BEGIN_MPS_REPLACEMENT,
        {
            "job_id": job.job_id,
            "manifest_id": manifest_id,
            "name": name.strip() if name is not None else None,
            "schedule": schedule.to_dict() if schedule is not None else None,
        },
        command_id=stable_command_id("mps-edit-begin-v1", manifest_id),
    )
    from .resources import window_policy
    from .storage import TaskPayloadBudgetExceeded

    policy = window_policy(job.metadata, manager.storage)
    batch_limit = policy.replacement_rows
    byte_limit = min(writes.max_command_bytes, writes.max_queue_bytes, policy.replacement_bytes)
    ordinary = policy.input_snapshot
    input_max_bytes = policy.input_bytes
    offset = 0
    batch: list[dict[str, Any]] = []
    batch_bytes = 0

    def envelope_size() -> int:
        # The timestamp has the maximum ISO precision emitted by the writer.
        # Include the actual producer, IDs and offset, rather than assuming
        # that a payload-sized batch also fits the transport envelope.
        return len(
            DatabaseWriteCommand(
                command_id=stable_command_id("mps-edit-chunk-v1", manifest_id, offset),
                operation=DatabaseWriteOperation.APPEND_MPS_REPLACEMENT,
                site=None,
                payload={
                    "job_id": job.job_id,
                    "manifest_id": manifest_id,
                    "offset": offset,
                    "tasks": [],
                },
                producer_id=writes.producer_id,
                created_at="9999-12-31T23:59:59.999999+00:00",
            ).to_json()
        )

    async def flush() -> None:
        nonlocal offset, batch, batch_bytes
        await writes.submit(
            DatabaseWriteOperation.APPEND_MPS_REPLACEMENT,
            {
                "job_id": job.job_id,
                "manifest_id": manifest_id,
                "offset": offset,
                "tasks": batch,
            },
            command_id=stable_command_id("mps-edit-chunk-v1", manifest_id, offset),
        )
        offset += len(batch)
        batch, batch_bytes = [], 0

    overhead = envelope_size()
    async def with_first():
        yield first
        async for task in iterator:
            yield task

    async for task in with_first():
        task_dict = task.to_dict(include_result=not ordinary)
        if ordinary:
            task_dict["mps_task_manifest"] = manifest_id
            input_bytes = len(json.dumps(task_dict).encode("utf-8"))
            if input_bytes > input_max_bytes:
                raise TaskPayloadBudgetExceeded(task.task_id, input_bytes, input_max_bytes, phase="SQL input read")
        size = len(json.dumps(task_dict, separators=(",", ":"), sort_keys=True))
        if batch and (
            len(batch) >= batch_limit or overhead + batch_bytes + size + 1 > byte_limit
        ):
            await flush()
            overhead = envelope_size()
        if overhead + size > byte_limit:
            if ordinary:
                raise TaskPayloadBudgetExceeded(task.task_id, overhead + size, byte_limit, phase="SQL input insert")
            raise ValueError(
                "One replacement task exceeds the configured writer byte budget"
            )
        batch_bytes += size + bool(batch)
        batch.append(task_dict)
    await flush()

    async with mps_durable._schedule_edit_lock(manager.storage, job.job_id) as (
        key,
        owner,
    ):
        current = await manager.storage.fetch_job(job.job_id, include_tasks=False)
        if current is not None and current.state == JobState.RUNNING:
            raise RuntimeError("Cannot update tasks while job is running")
        await writes.submit(
            DatabaseWriteOperation.COMMIT_MPS_REPLACEMENT,
            {"job_id": job.job_id, "manifest_id": manifest_id, "total": offset},
            command_id=stable_command_id("mps-edit-commit-v1", manifest_id),
        )
        if job.metadata.get("execution_engine") == "ledger":
            updated = await manager.storage.fetch_job(job.job_id, include_tasks=False)
            if updated is None:
                raise ValueError("Job was deleted during replacement")
            return updated
        # Use the ordinary publication/budget path without dispatching from an
        # API request that may not own this job's backend assignment. The
        # background materializer performs dispatch once the edit lock ends.
        await mps_durable.advance(
            SimpleNamespace(redis=manager.redis_manager),
            job.job_id,
            lock_key=key,
            lock_owner=owner,
            activate=False,
        )
        if await manager.storage._client().exists(manager.storage.JOB_CONFIG_KEY.format(job_id=job.job_id)):
            await mps_state.schedule(manager.storage._client(), job.job_id)
    updated = await manager.storage.fetch_job(job.job_id, include_tasks=False)
    if updated is None:
        from ..database.threads import run_database_call

        plan = await run_database_call(mps_durable.MPSManifestStore().get, job.job_id)
        assert plan is not None
        updated = mps_durable.job_from_plan(plan)
    return updated


async def reconcile_definition(
    storage: JobStorage,
    plan: dict[str, Any],
    *,
    lock_key: str | None = None,
    lock_owner: str | None = None,
) -> None:
    """Retire an old bounded window after SQL has committed its replacement.

    Deletion is repeatable after interruption. Only the final transaction removes
    the old header and releases its admission budget; recovery can repeat any
    partially finished cleanup using the authoritative SQL definition.
    """
    from . import durable as mps_durable

    job_id = plan["job_id"]
    incoming = plan["job_config"].get("metadata", {})
    manifest_id = incoming.get("mps_task_manifest")
    client = storage._client()
    config_key = storage.JOB_CONFIG_KEY.format(job_id=job_id)
    raw = await client.get(config_key)
    if raw is None:
        return
    config = json.loads(raw)
    previous = config.get("metadata", {})
    if previous.get("mps_task_manifest") == manifest_id:
        return
    if int(previous.get("mps_config_revision", 0)) >= int(
        incoming.get("mps_config_revision", 0)
    ):
        return
    if lock_key is None:
        async with mps_durable._schedule_edit_lock(storage, job_id) as (key, owner):
            await reconcile_definition(storage, plan, lock_key=key, lock_owner=owner)
        return
    await mps_durable._check_owner(client, lock_key, lock_owner)
    manager = JobManager(storage.redis)
    await manager.router.clear_job_routing(
        job_id=job_id,
        backend_id=await manager._resolve_backend_assignment(job_id),
    )
    index_key = storage.TASK_INDEX_KEY.format(job_id=job_id)
    while True:
        ids = await storage.list_task_ids_window(
            job_id, offset=0, limit=storage.DEFAULT_WRITE_BATCH_SIZE
        )
        if not ids:
            break
        tasks = await storage.fetch_tasks_by_ids(job_id, ids)
        async with client.pipeline(transaction=True) as pipe:
            await pipe.watch(config_key, index_key, lock_key)
            await mps_durable._check_owner(pipe, lock_key, lock_owner)
            if await pipe.get(config_key) != raw:
                return
            pipe.multi()
            for task_id, task in zip(ids, tasks, strict=True):
                pipe.delete(
                    storage.TASK_KEY.format(job_id=job_id, task_id=task_id),
                    storage.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id=task_id),
                f"{KEY_PREFIX}:view:task:{task_id}:state",
                )
                if task is not None:
                    from .recovery_scripts import COMPARE_DELETE

                    pipe.eval(
                        COMPARE_DELETE,
                        1,
                        storage.DEDUPE_KEY.format(dedup=task.dedup_key),
                        task_id,
                    )
                    if task.deferred_dedup_key:
                        pipe.srem(
                            storage.DEFERRED_TASKS_KEY.format(
                                dedup_key=task.deferred_dedup_key
                            ),
                            f"{job_id}:{task_id}",
                        )
            pipe.zrem(index_key, *ids)
            await pipe.execute()
    async with client.pipeline(transaction=True) as pipe:
        await pipe.watch(config_key, lock_key)
        await mps_durable._check_owner(pipe, lock_key, lock_owner)
        if await pipe.get(config_key) != raw:
            return
        pipe.multi()
        await mps_state.release_budget(client, job_id, pipe=pipe)
        runtime_admission.release(pipe, job_id)
        pipe.zrem(storage.JOB_INDEX_KEY, job_id)
        pipe.delete(
            config_key,
            storage.JOB_STATE_KEY.format(job_id=job_id),
            mps_state.PLAN_KEY.format(job_id=job_id),
            index_key,
            storage.TASKS_READY_KEY.format(job_id=job_id),
            storage.TASKS_BLOCKED_KEY.format(job_id=job_id),
            storage.TASKS_INFLIGHT_KEY.format(job_id=job_id),
            storage.TASKS_COMPLETED_KEY.format(job_id=job_id),
            mps_state.FAILED_KEY.format(job_id=job_id),
            storage.TASK_OUTPUTS_KEY.format(job_id=job_id),
            storage.RECENT_TASK_FAILURES_KEY.format(job_id=job_id),
        )
        pipe.zrem(storage.SCHEDULED_KEY, job_id)
        await pipe.execute()
