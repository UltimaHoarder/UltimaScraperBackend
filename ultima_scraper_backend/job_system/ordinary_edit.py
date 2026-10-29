"""Commit ordinary edits before publishing runnable tasks; resume interrupted publication."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from ..database.threads import run_database_call
from ..redis_channels import KEY_PREFIX
from . import retention
from .models import Job, JobConfig, JobRuntimeState, JobState

logger = logging.getLogger(__name__)
PENDING_KEY = f"{KEY_PREFIX}:jobs:edits"


class EditInProgress(RuntimeError):
    """Another owner is changing this job."""


class EditPersistenceError(RuntimeError):
    """The durable edit or its recoverable publication could not finish."""


@dataclass(frozen=True)
class Owner:
    job_id: str
    token: str


_owner: ContextVar[Owner | None] = ContextVar("ordinary_job_edit", default=None)
_task_definition: ContextVar[tuple[str, str] | None] = ContextVar(
    "ordinary_task_definition", default=None
)


@contextmanager
def task_scope(job: Job):
    value = (
        None
        if job.metadata.get("mps_manifest")
        else (
            job.job_id,
            retention.definition_id(job.job_id, {"metadata": job.metadata}),
        )
    )
    token = _task_definition.set(value)
    try:
        yield
    finally:
        _task_definition.reset(token)


def task_command_identity(job_id: str) -> dict[str, str]:
    value = _task_definition.get()
    return (
        {"task_definition_id": value[1]}
        if value is not None and value[0] == job_id
        else {}
    )


async def release_reservation(storage: Any, key: str, expected_owner: str) -> bool:
    """Let stale worker cleanup finish without releasing a replacement's lock."""
    from .recovery_scripts import COMPARE_DELETE

    definition = _task_definition.get()
    if definition is None:
        return bool(
            await storage._client().eval(COMPARE_DELETE, 1, key, expected_owner)
        )
    job_id, identity = definition
    return bool(
        await storage._client().eval(
            """
local raw = redis.call('GET',KEYS[1])
if not raw then return 0 end
local config = cjson.decode(raw)
if ((config.metadata or {}).task_definition_id or ARGV[3]) ~= ARGV[1] then return 0 end
if redis.call('GET',KEYS[2]) ~= ARGV[2] then return 0 end
return redis.call('DEL',KEYS[2])
""",
            2,
            storage.JOB_CONFIG_KEY.format(job_id=job_id),
            key,
            identity,
            expected_owner,
            job_id,
        )
    )


def sql_definition_expression():
    from sqlalchemy import cast, func
    from sqlalchemy.dialects.postgresql import JSONB
    from ultima_scraper_db_archive.global_db import CentralJobModel

    return func.coalesce(
        cast(CentralJobModel.job_config_json, JSONB)["metadata"][
            "task_definition_id"
        ].astext,
        CentralJobModel.job_id,
    )


def sql_definitions(session: Any, payloads: Any) -> dict[str, str]:
    from sqlalchemy import select
    from ultima_scraper_db_archive.global_db import CentralJobModel

    ids = sorted(
        {
            str(payload["job_id"])
            for payload in payloads
            if "mps_task_manifest" not in payload
        }
    )
    if not ids:
        return {}
    return dict(
        session.execute(
            select(CentralJobModel.job_id, sql_definition_expression())
            .where(CentralJobModel.job_id.in_(ids))
            .order_by(CentralJobModel.job_id)
            .with_for_update()
        ).all()
    )


def current(job_id: str | None = None) -> Owner | None:
    owner = _owner.get()
    return (
        owner
        if owner is not None and (job_id is None or owner.job_id == job_id)
        else None
    )


@contextmanager
def scope(owner: Owner):
    token = _owner.set(owner)
    try:
        yield owner
    finally:
        _owner.reset(token)


def lock_key(job_id: str) -> str:
    return f"{KEY_PREFIX}:job:{job_id}:edit_lock"


def record_key(job_id: str) -> str:
    return f"{KEY_PREFIX}:job:{job_id}:edit"


def revision(job_id: str, config: dict[str, Any]) -> str:
    return str(config.get("metadata", {}).get("config_revision") or job_id)


@asynccontextmanager
async def lease(manager: Any, job_id: str, *, wait: bool = False):
    client = manager.storage._client()
    owner = Owner(job_id, uuid4().hex)
    key = lock_key(job_id)
    seconds = manager.TERMINAL_CLEANUP_LOCK_TTL
    while not await client.set(key, owner.token, nx=True, ex=seconds):
        if not wait:
            raise EditInProgress(f"Job {job_id} is being edited")
        await asyncio.sleep(0.05)

    async def renew() -> None:
        while True:
            await asyncio.sleep(seconds / 3)
            if not await client.eval(
                "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('EXPIRE',KEYS[1],ARGV[2]) end return 0",
                1,
                key,
                owner.token,
                seconds,
            ):
                return

    heartbeat = asyncio.create_task(renew())
    try:
        yield owner
    finally:
        from ultima_scraper_core.processes import finish_owned

        async def release() -> None:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await client.eval(
                "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0",
                1,
                key,
                owner.token,
            )

        await finish_owned(asyncio.create_task(release()))


async def assert_owner(storage: Any, owner: Owner) -> None:
    if not await storage._client().eval(
        "return redis.call('GET',KEYS[1]) == ARGV[1] and 1 or 0",
        1,
        lock_key(owner.job_id),
        owner.token,
    ):
        raise EditInProgress(f"Edit ownership for {owner.job_id} changed")


def sql_owner_check(storage: Any, owner: Owner):
    """Verify Redis ownership while the SQL header row lock is held.

    Recovery takes the same SQL lock before clearing an abandoned edit. This
    single bounded Redis check prevents a delayed SQL call from committing after
    recovery released its gate. The API owns and drains this thread on cancel.
    """
    loop = asyncio.get_running_loop()

    def check() -> None:
        asyncio.run_coroutine_threadsafe(assert_owner(storage, owner), loop).result()

    return check


async def control_mutation(
    storage: Any, job_id: str, script: str, keys: list[str], args: list[Any]
) -> Any:
    from redis.exceptions import ResponseError

    if retention.current(job_id) is not None:
        return await retention.evaluate(storage, job_id, script, keys, args)
    owner = current(job_id)
    expected_definition = task_command_identity(job_id).get("task_definition_id", "")
    prefix = """
local editor = redis.call('HGET', KEYS[1], 'edit_pending')
if ARGV[1] ~= '' then
    if redis.call('GET',KEYS[2]) ~= ARGV[1] then return redis.error_reply('USR_EDIT_OWNER_CHANGED') end
    if editor and editor ~= ARGV[1] then return redis.error_reply('USR_EDIT_OWNER_CHANGED') end
elseif editor then return {false, 'editing'} end
if ARGV[1] == '' and ARGV[2] ~= '' then
    local raw = redis.call('GET',KEYS[3])
    if not raw then return redis.error_reply('USR_TASK_DEFINITION_CHANGED') end
    local config = cjson.decode(raw)
    local definition = (config.metadata or {}).task_definition_id or config.job_id
    if definition ~= ARGV[2] then return redis.error_reply('USR_TASK_DEFINITION_CHANGED') end
end
local operation_keys = {}
for i=4,#KEYS do operation_keys[i-3] = KEYS[i] end
local operation_args = {}
for i=3,#ARGV do operation_args[i-2] = ARGV[i] end
local KEYS, ARGV = operation_keys, operation_args
"""
    while True:
        try:
            result = await storage._client().eval(
                prefix + script,
                len(keys) + 3,
                storage.JOB_STATE_KEY.format(job_id=job_id),
                lock_key(job_id),
                storage.JOB_CONFIG_KEY.format(job_id=job_id),
                *keys,
                owner.token if owner is not None else "",
                expected_definition,
                *args,
            )
        except ResponseError as exc:
            if "USR_TASK_DEFINITION_CHANGED" in str(exc) or (
                expected_definition and "USR_EDIT_OWNER_CHANGED" in str(exc)
            ):
                from .mps_attempt import StaleTaskDefinition

                raise StaleTaskDefinition(
                    f"Ordinary task definition for {job_id} changed"
                ) from exc
            if "USR_EDIT_OWNER_CHANGED" in str(exc):
                raise EditInProgress(f"Edit ownership for {job_id} changed") from exc
            raise
        if result not in ([None, b"editing"], [None, "editing"]):
            return result
        if not await storage._client().exists(lock_key(job_id)):
            from .manager import JobManager

            manager = JobManager(storage.redis)
            manager.storage = storage
            try:
                await recover(manager, job_id)
            except EditInProgress:
                pass
        await asyncio.sleep(0.05)


async def _begin(
    storage: Any, owner: Owner, job: Job, descriptor: dict[str, Any]
) -> None:
    result = await storage._client().eval(
        """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('EXISTS', KEYS[2]) == 1 then return -2 end
if ARGV[4] == '1' and (redis.call('HGET',KEYS[3],'status') == 'running'
    or redis.call('SCARD',KEYS[4]) > 0) then return -3 end
redis.call('SET',KEYS[2],ARGV[2])
redis.call('HSET',KEYS[3],'edit_pending',ARGV[1])
redis.call('ZADD',KEYS[5],ARGV[3],ARGV[5])
redis.call('ZADD',KEYS[6],ARGV[3],ARGV[5])
return 1
""",
        6,
        lock_key(job.job_id),
        record_key(job.job_id),
        storage.JOB_STATE_KEY.format(job_id=job.job_id),
        storage.TASKS_INFLIGHT_KEY.format(job_id=job.job_id),
        PENDING_KEY,
        storage.JOB_INDEX_KEY,
        owner.token,
        json.dumps(descriptor),
        datetime.now(timezone.utc).timestamp(),
        int(descriptor["tasks_changed"]),
        job.job_id,
    )
    if result == -3:
        raise EditInProgress("Cannot replace tasks while the job has running work")
    if result != 1:
        raise EditInProgress(f"Job {job.job_id} has another pending edit")


def _commit_sync(
    job: Job,
    descriptor: dict[str, Any],
    args_patch: dict[str, Any],
    check_owner: Any,
    input_max_bytes: int,
    staged: Any,
) -> None:
    from sqlalchemy import Text, cast, delete, func, insert, literal, select, update
    from sqlalchemy.dialects.postgresql import JSONB
    from ultima_scraper_db_archive.global_db import (
        CentralJobModel,
        CentralTaskModel,
        global_session,
    )
    from .storage import TaskPayloadBudgetExceeded

    with global_session() as session:
        try:
            row = session.execute(
                select(CentralJobModel.job_config_json)
                .where(CentralJobModel.job_id == job.job_id)
                .with_for_update()
            ).one_or_none()
            check_owner()
            if row is None:
                raise EditPersistenceError(
                    f"Durable job header for {job.job_id} is missing"
                )
            config = json.loads(row[0] or "{}")
            if revision(job.job_id, config) != descriptor["previous_revision"]:
                raise EditInProgress(f"Stored configuration for {job.job_id} changed")
            args = {**args_patch, "job_name": job.name}
            values = {}
            if descriptor["tasks_changed"]:
                args["task_count"] = staged.count
                session.execute(
                    delete(CentralTaskModel).where(
                        CentralTaskModel.job_id == job.job_id
                    )
                )
                for batch in staged.batches():
                    for task_row in batch:
                        size = len(task_row["definition_json"].encode("utf-8"))
                        if size > input_max_bytes:
                            raise TaskPayloadBudgetExceeded(
                                task_row["task_id"],
                                size,
                                input_max_bytes,
                                phase="SQL input read",
                            )
                    session.execute(insert(CentralTaskModel), batch)
                values["status"] = "pending"
                for field in (
                    "started_at",
                    "completed_at",
                    "last_event_at",
                    "result_success",
                    "result_error",
                    "result_completed_at",
                ):
                    values[field] = None
            summary = cast(
                func.coalesce(func.nullif(CentralJobModel.args_json, ""), "{}"), JSONB
            )
            if descriptor["tasks_changed"]:
                # Complete inputs live in task rows; list/detail reads need only
                # this small summary, including after a previously eager edit.
                summary = summary.op("-")("tasks")
            values["args_json"] = cast(
                summary.op("||")(cast(literal(json.dumps(args)), JSONB)), Text
            )
            values["job_config_json"] = json.dumps(
                job.to_config(include_task_ids=False).to_dict()
            )
            session.execute(
                update(CentralJobModel)
                .where(CentralJobModel.job_id == job.job_id)
                .values(**values)
            )
            session.commit()
        except BaseException:
            session.rollback()
            raise


def _read_committed_sync(job_id: str, check_owner: Any) -> dict[str, Any] | None:
    from sqlalchemy import select
    from ultima_scraper_db_archive.global_db import CentralJobModel, global_session

    with global_session() as session:
        row = session.execute(
            select(CentralJobModel.job_config_json)
            .where(CentralJobModel.job_id == job_id)
            .with_for_update()
        ).one_or_none()
        check_owner()
        if row is None:
            raise EditPersistenceError(f"Durable job header for {job_id} is missing")
        return json.loads(row[0]) if row[0] else {}


async def _clear_runtime(storage: Any, owner: Owner) -> None:
    job_id = owner.job_id
    await storage.release_dedup_for_job_id(job_id)
    await storage.clear_task_state(job_id)
    batch_size, _ = storage.task_archive_limits()
    while ids := await storage.list_task_ids_window(job_id, offset=0, limit=batch_size):
        keys = [storage.TASK_INDEX_KEY.format(job_id=job_id)]
        for task_id in ids:
            keys.extend(
                (
                    storage.TASK_KEY.format(job_id=job_id, task_id=task_id),
                    storage.TASK_DEPENDENTS_KEY.format(job_id=job_id, task_id=task_id),
                    storage.TASK_EXEC_LOCK_KEY.format(task_id=task_id),
                )
            )
        await control_mutation(
            storage,
            job_id,
            """
for i=2,#KEYS do redis.call('DEL',KEYS[i]) end
for i=1,#ARGV do redis.call('ZREM',KEYS[1],ARGV[i]) end
return #ARGV
""",
            keys,
            ids,
        )
    await control_mutation(
        storage,
        job_id,
        "return redis.call('DEL',KEYS[1])",
        [storage.TASK_ARCHIVE_MARKER_KEY.format(job_id=job_id)],
        [],
    )


async def _publish(
    storage: Any, owner: Owner, descriptor: dict[str, Any], config: dict[str, Any]
) -> Job:
    job_id = owner.job_id
    current_job = await storage.fetch_job(job_id, include_tasks=False)
    state = current_job.to_state() if current_job is not None else JobRuntimeState()
    if descriptor["tasks_changed"]:
        await _clear_runtime(storage, owner)
        state.status = JobState.PENDING
        state.started_at = state.completed_at = None
        state.total_tasks = state.completed_tasks = state.failed_tasks = 0
        cursor = None
        while True:
            page = await storage.fetch_archived_task_page(job_id, after_task_id=cursor)
            await storage.persist_tasks(job_id, page.tasks)
            state.total_tasks += len(page.tasks)
            if not page.has_more:
                break
            cursor = page.next_cursor
        state.updated_at = datetime.now(timezone.utc).isoformat()
    job = Job.from_split_data(JobConfig.from_dict(config), state)
    await storage.persist_job(
        job,
        config_only=not descriptor["tasks_changed"],
        reset_state=descriptor["tasks_changed"],
    )
    if descriptor["tasks_changed"] or (
        descriptor["schedule_changed"] and job.state != JobState.RUNNING
    ):
        if job.is_scheduled:
            await storage.clear_task_state(job_id)
        else:
            batch_size, max_bytes = storage.task_input_limits()
            await storage.initialize_task_state_from_storage(
                job_id,
                return_ready_task_ids=False,
                batch_size=batch_size,
                max_bytes=max_bytes,
            )
    return job


async def _finish(
    manager: Any, owner: Owner, descriptor: dict[str, Any], job: Job
) -> None:
    storage, job_id = manager.storage, owner.job_id
    descriptor["phase"] = "published"
    await control_mutation(
        storage,
        job_id,
        """
redis.call('SET',KEYS[1],ARGV[1])
redis.call('HDEL',KEYS[2],'edit_pending')
return 1
""",
        [record_key(job_id), storage.JOB_STATE_KEY.format(job_id=job_id)],
        [json.dumps(descriptor)],
    )
    if (
        (descriptor["route"] or descriptor["backend_id"])
        and not job.is_scheduled
        and job.state in {JobState.PENDING, JobState.RUNNING}
    ):
        await manager._route_pending_job(job, backend_id=descriptor["backend_id"])
    elif job.is_scheduled and descriptor["backend_id"]:
        await manager.router.remove_ready_tasks_for_job(
            descriptor["backend_id"], job_id
        )
    await control_mutation(
        storage,
        job_id,
        """
redis.call('DEL',KEYS[1])
redis.call('ZREM',KEYS[2],ARGV[1])
return 1
""",
        [record_key(job_id), PENDING_KEY],
        [job_id],
    )


async def _recover_owned(
    manager: Any, owner: Owner, descriptor: dict[str, Any]
) -> Job | None:
    storage, job_id = manager.storage, owner.job_id
    if descriptor["phase"] != "published":
        if not await storage._client().eval(
            """
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('HSET',KEYS[2],'edit_pending',ARGV[1]); return 1
""",
            2,
            lock_key(job_id),
            storage.JOB_STATE_KEY.format(job_id=job_id),
            owner.token,
        ):
            raise EditInProgress(f"Edit ownership for {job_id} changed")
    config = await run_database_call(
        _read_committed_sync, job_id, sql_owner_check(storage, owner)
    )
    committed = (
        config is not None and revision(job_id, config) == descriptor["revision"]
    )
    if committed:
        if descriptor["phase"] == "published":
            job = await storage.fetch_job(job_id, include_tasks=False)
            if job is None:
                raise EditPersistenceError(f"Published header for {job_id} is missing")
        else:
            job = await _publish(storage, owner, descriptor, config)
    else:
        # The old SQL call has completed or lost this lease: clearing the gate
        # cannot let that delayed call commit after recovery's header-row check.
        job = await storage.fetch_job(job_id, include_tasks=False)
        if job is None:
            raise EditPersistenceError(f"Original header for {job_id} is missing")
    await _finish(manager, owner, descriptor, job)
    return job if committed else None


async def recover(manager: Any, job_id: str) -> Job | None:
    async with lease(manager, job_id) as owner:
        with scope(owner):
            raw = await manager.storage._client().get(record_key(job_id))
            if raw is None:
                if await manager.storage._client().hget(
                    manager.storage.JOB_STATE_KEY.format(job_id=job_id), "edit_pending"
                ):
                    raise EditPersistenceError(
                        f"Pending edit for {job_id} has no recovery record"
                    )
                await control_mutation(
                    manager.storage,
                    job_id,
                    "if not redis.call('HGET',KEYS[1],'edit_pending') then return redis.call('ZREM',KEYS[2],ARGV[1]) end return 0",
                    [manager.storage.JOB_STATE_KEY.format(job_id=job_id), PENDING_KEY],
                    [job_id],
                )
                return None
            return await _recover_owned(manager, owner, json.loads(raw))


@asynccontextmanager
async def finish(storage: Any, job: Job):
    """Serialize post-handler bookkeeping with edits of the same job."""
    from .manager import JobManager
    from .mps_attempt import StaleTaskDefinition

    manager = JobManager(storage.redis)
    manager.storage = storage
    async with lease(manager, job.job_id, wait=True) as owner:
        with scope(owner):
            raw = await storage._client().get(record_key(job.job_id))
            if raw is not None:
                await _recover_owned(manager, owner, json.loads(raw))
            header = await storage.fetch_job(job.job_id, include_tasks=False)
            if header is None or retention.definition_id(
                job.job_id, {"metadata": header.metadata}
            ) != retention.definition_id(job.job_id, {"metadata": job.metadata}):
                raise StaleTaskDefinition(
                    f"Ordinary task definition for {job.job_id} changed"
                )
            yield


async def _update_staged(
    manager: Any,
    job_id: str,
    *,
    name: str | None,
    schedule: Any,
    tasks: Any,
    route_immediate_job: bool,
    args_patch: dict[str, Any] | None,
) -> Job:
    storage = manager.storage
    async with lease(manager, job_id) as owner:
        with scope(owner):
            pending = await storage._client().get(record_key(job_id))
            if pending is not None:
                await _recover_owned(manager, owner, json.loads(pending))
            job = await storage.fetch_job(job_id, include_tasks=False)
            if job is None:
                raise ValueError(f"Job {job_id} not found")
            if name is not None and not name.strip():
                raise ValueError("Job name cannot be empty")
            old_config = job.to_config(include_task_ids=False).to_dict()
            descriptor = {
                "phase": "prepared",
                "revision": uuid4().hex,
                "previous_revision": revision(job_id, old_config),
                "tasks_changed": tasks is not None,
                "schedule_changed": schedule is not None,
                "route": route_immediate_job,
                "backend_id": await manager._resolve_backend_assignment(job_id),
            }
            if name is not None:
                job.name = name.strip()
            if schedule is not None:
                job.schedule = schedule
                job.metadata["source"] = "scheduled" if job.is_scheduled else "manual"
            if tasks is not None:
                job.tasks = []
                job.task_ids = []
                job.metadata["total_tasks_count_override"] = tasks.count
                job.metadata["task_definition_id"] = uuid4().hex
                job.metadata.pop("failure_reason", None)
                job.metadata.pop("worker_id", None)
                job.state = JobState.PENDING
                job.started_at = job.completed_at = None
                job.completed_tasks_count = job.failed_tasks_count = 0
            job.metadata["config_revision"] = descriptor["revision"]
            await _begin(storage, owner, job, descriptor)
            try:
                _, input_max_bytes = storage.task_input_limits()
                await run_database_call(
                    _commit_sync,
                    job,
                    descriptor,
                    args_patch or {},
                    sql_owner_check(storage, owner),
                    input_max_bytes,
                    tasks,
                )
            except Exception as exc:
                recovered = None
                try:
                    recovered = await _recover_owned(manager, owner, descriptor)
                except Exception:
                    logger.exception("Pending edit retained for recovery: %s", job_id)
                from .storage import TaskLoadLimitExceeded

                if recovered is not None:
                    return recovered
                if isinstance(exc, (EditInProgress, TaskLoadLimitExceeded)):
                    raise
                raise EditPersistenceError(
                    f"Job edit {job_id} could not commit; any pending edit is retained for recovery"
                ) from exc
            # Even the first publication reads the committed pages. Recovery
            # uses exactly this path and never collects the whole input archive.
            try:
                result = await _publish(
                    storage,
                    owner,
                    descriptor,
                    job.to_config(include_task_ids=False).to_dict(),
                )
                await _finish(manager, owner, descriptor, result)
            except Exception as exc:
                raise EditPersistenceError(
                    f"Job edit {job_id} committed; publication is pending recovery: {exc}"
                ) from exc
            return result


async def update(
    manager: Any,
    job_id: str,
    *,
    name: str | None,
    schedule: Any,
    tasks: Any,
    route_immediate_job: bool,
    args_patch: dict[str, Any] | None,
) -> Job:
    from .task_inputs import stage_task_inputs

    _, input_max_bytes = manager.storage.task_input_limits()
    async with stage_task_inputs(
        tasks, job_id=job_id, input_max_bytes=input_max_bytes
    ) as staged:
        return await _update_staged(
            manager,
            job_id,
            name=name,
            schedule=schedule,
            tasks=staged,
            route_immediate_job=route_immediate_job,
            args_patch={
                key: value
                for key, value in (args_patch or {}).items()
                if key != "tasks"
            },
        )
