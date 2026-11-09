"""SQL-owned job execution with bounded windows and fenced worker attempts.

The existing manifest tables are immutable input/selection sources. The ledger
owns executable work, attempt leases and controls. Redis is a projection and a
content-write transport, never evidence that a ledger attempt owns a task.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from itertools import groupby
from typing import Any
from uuid import uuid4

from sqlalchemy import case, delete, false, func, literal_column, select, text, true, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import defer, undefer
from ultima_scraper_db_archive.execution_models import (
    ExecutionAttemptModel as Attempt,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionDefinitionModel as Definition,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionFeedModel as Feed,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionOutboxModel as Outbox,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionResourceModel as Resource,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionRunModel as Run,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionWorkModel as Work,
)
from ultima_scraper_db_archive.execution_models import (
    ExecutionWorkPayloadModel,
)
from ultima_scraper_db_archive.global_db import (
    CentralJobModel,
    CentralTaskModel,
    MPSPlanModel,
    global_session,
)

from ultima_scraper_core import runtime_env_value
from ultima_scraper_core.environment import read_env

from ..database.mps_definitions import MPSDefinitionBudgetExceeded
from ..database.mps_manifest import (
    MPSManifestStore,
    PhaseTimer,
    trusted_mps_identity_enabled,
)
from . import run_turnstile
from .host_capacity import DOWNLOAD_CAPABILITY
from .api_command_policy import WORKER_LEASE_ERROR_CODE, WORKER_LEASE_EXHAUSTED
from .models import Job, Task, TaskResult, TaskState
from .profile_completion import PROFILE_COMPLETION_KEY, read_committed_profile_completion
from .resources import terminal_job_state

ENGINE = "ledger"
TERMINAL = {"completed", "failed", "cancelled"}
ACTIVE_WORK = {"running", "persisting"}
# Fixed states must remain SQL literals so generic prepared plans can prove
# the terminal partial-index predicate as well as custom plans can.
_TERMINAL_WORK = Work.state.in_((literal_column("'completed'"), literal_column("'failed'")))
# Resident work never exceeds a plan's unarchived remainder. Count it only
# when that remainder is small; a large one always has unadmitted tasks.
_ADMISSION_PROBE_LIMIT = 4096


_INSERT_ADMITTED_WORK = text("""
INSERT INTO management.execution_work
    (run_id, task_id, state, lane, site, capabilities, dedup_key,
     payload_bytes, payload_size, candidate)
SELECT CAST(:run_id AS varchar), w.task_id, w.state, w.lane, w.site,
    ARRAY(SELECT jsonb_array_elements_text(w.capabilities)),
    w.dedup_key, w.payload_bytes, w.payload_size, w.candidate
FROM unnest(
    CAST(:task_ids AS varchar[]), CAST(:states AS varchar[]), CAST(:lanes AS varchar[]),
    CAST(:sites AS varchar[]), CAST(:capabilities AS jsonb[]), CAST(:dedup_keys AS text[]),
    CAST(:payload_bytes AS bigint[]), CAST(:payload_sizes AS bigint[]),
    CAST(:candidates AS bigint[])
) AS w(task_id, state, lane, site, capabilities, dedup_key,
       payload_bytes, payload_size, candidate)
""")
_INSERT_ADMITTED_PAYLOADS = text("""
INSERT INTO management.execution_work_payloads (run_id, task_id, payload_json)
SELECT CAST(:run_id AS varchar), p.task_id, p.payload_json
FROM unnest(CAST(:task_ids AS varchar[]), CAST(:payloads AS text[])) AS p(task_id, payload_json)
""")


def _work_lane(task) -> str:
    kind = task.task_type.value
    if kind == "plugin":
        return "plugin"
    if kind == "api_command":
        return "interactive"
    return "download" if kind == "download" else "scrape"


def _work_capabilities(task) -> list[str]:
    """A download also needs a host that can store it right now."""
    capabilities = list(task.capabilities)
    if _work_lane(task) == "download" and DOWNLOAD_CAPABILITY not in capabilities:
        capabilities.append(DOWNLOAD_CAPABILITY)
    return capabilities


def _insert_admitted_work(session, run_id, rows, candidates):
    """Insert admitted tasks and their payloads with two set-based statements.

    Produces the same rows as constructing ``Work(..., payload_json=...)`` per
    task (attempt, available_at and created_at keep their server defaults), at
    a fraction of the ORM unit-of-work cost. ``rows`` holds
    ``(task, encoded_payload, size)`` in admission order.
    """
    if not rows:
        return
    task_ids = [task.task_id for task, _, _ in rows]
    session.execute(_INSERT_ADMITTED_WORK, {
        "run_id": run_id,
        "task_ids": task_ids,
        "states": ["failed" if task.state == TaskState.FAILED else "queued" for task, _, _ in rows],
        "lanes": [_work_lane(task) for task, _, _ in rows],
        "sites": [task.site_name.lower() for task, _, _ in rows],
        "capabilities": [json.dumps(_work_capabilities(task)) for task, _, _ in rows],
        "dedup_keys": [task.dedup_key for task, _, _ in rows],
        "payload_bytes": [size for _, _, size in rows],
        "payload_sizes": [len(encoded.encode("utf-8")) for _, encoded, _ in rows],
        "candidates": [candidates.get(task.task_id) for task, _, _ in rows],
    })
    session.execute(_INSERT_ADMITTED_PAYLOADS, {
        "run_id": run_id,
        "task_ids": task_ids,
        "payloads": [encoded for _, encoded, _ in rows],
    })


# Queue tables churn every task through several indexed state changes (never
# HOT) and delete it on archive. Vacuum reuses heap space but never shrinks
# indexes, so a long MPS run bloated them to hundreds of MB for <=60k rows.
_QUEUE_TABLES = ("execution_work", "execution_work_payloads", "execution_resources")
_DEFAULT_QUEUE_REINDEX_BYTES = 64 * 1024 * 1024


logger = logging.getLogger(__name__)
_OWNER_ENGINES: dict[str, Any] = {}
_REINDEX_PRIVILEGE_WARNED: list[bool] = []


def owner_engine():
    """An engine for ULTIMA_ARCHIVE_OWNER_DATABASE_URL, or None when unset.

    REINDEX needs the table owner. When the application runs as a DML-only
    role (transaction-pooled PgBouncer), maintenance connects as the owner.
    """
    url = runtime_env_value("ULTIMA_ARCHIVE_OWNER_DATABASE_URL")
    if not url or not url.strip():
        return None
    url = url.strip()
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg2://", "postgresql://"):
        if url.startswith(prefix):
            url = "postgresql+psycopg://" + url[len(prefix):]
            break
    if url not in _OWNER_ENGINES:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        _OWNER_ENGINES[url] = create_engine(
            url, poolclass=NullPool, connect_args={"prepare_threshold": None}
        )
    return _OWNER_ENGINES[url]


def queue_reindex_threshold() -> int:
    """ULTIMA_LEDGER_QUEUE_REINDEX_BYTES: rebuild a queue index above this size.

    Default 64 MiB; 0 disables online rebuilds. Read per maintenance pass.
    """
    raw = runtime_env_value("ULTIMA_LEDGER_QUEUE_REINDEX_BYTES")
    value = _DEFAULT_QUEUE_REINDEX_BYTES if raw is None or not raw.strip() else int(raw)
    if value < 0:
        raise ValueError("ULTIMA_LEDGER_QUEUE_REINDEX_BYTES must not be negative")
    return value


def _admission_pending():
    """Whether refill can still admit work for the outer Run/plan row.

    Admitted tasks stay resident until the archiver advances the plan cursor.
    Fully admitted runs only await the archiver; visiting them in the refill
    rotation costs a full accounting pass and an idle backoff each, which
    starved a large run's refill behind dozens of single-task API commands.
    """
    remaining = MPSPlanModel.total - MPSPlanModel.cursor
    resident = (
        select(func.count())
        .select_from(Work)
        .where(Work.run_id == Run.run_id)
        .scalar_subquery()
    )
    return (MPSPlanModel.status == "preparing") | case(
        (remaining > _ADMISSION_PROBE_LIMIT, true()),
        else_=resident < remaining,
    )

# Dependencies, not CTE text order, enforce run -> work -> database clock.
# Check the lease only after potentially blocking row locks have completed.
_ATTEMPT_FENCE_SQL = """
WITH run_fence AS MATERIALIZED (
    SELECT run_id FROM management.execution_runs
    WHERE run_id=:run_id AND state NOT IN ('completed','failed','cancelled')
    FOR SHARE
), work_fence AS MATERIALIZED (
    SELECT w.run_id,w.task_id,w.state,w.attempt,w.dedup_key,w.lease_until
    FROM management.execution_work w JOIN run_fence r ON r.run_id=w.run_id
    WHERE w.run_id=:run_id AND w.task_id=:task_id
        AND w.state IN ('running','persisting')
        AND w.token=:token AND w.owner=:owner
    FOR UPDATE OF w
), owned AS MATERIALIZED (
    SELECT work_fence.*,clock_timestamp() AS now FROM work_fence
)
"""

_CHECKPOINT_ATTEMPT_SQL = text(
    _ATTEMPT_FENCE_SQL
    + """, updated_work AS (
    UPDATE management.execution_work w SET
        payload_size=CASE WHEN :checkpoint THEN octet_length(CAST(:payload AS text)) ELSE w.payload_size END,
        state=CASE WHEN :persisting THEN 'persisting' ELSE w.state END,
        lease_until=CASE WHEN :renew
            THEN f.now+make_interval(secs => :lease_seconds) ELSE w.lease_until END
    FROM owned f WHERE w.run_id=f.run_id AND w.task_id=f.task_id
        AND f.lease_until>f.now
    RETURNING w.run_id,w.task_id,w.dedup_key,w.lease_until
), updated_payload AS (
    UPDATE management.execution_work_payloads p SET payload_json=:payload
    FROM updated_work w WHERE :checkpoint AND p.run_id=w.run_id AND p.task_id=w.task_id
    RETURNING p.task_id
), renewed_resource AS (
    UPDATE management.execution_resources r SET lease_until=w.lease_until
    FROM updated_work w WHERE :renew AND r.resource_key=w.dedup_key AND r.token=:token
    RETURNING r.resource_key
)
SELECT task_id FROM updated_work
"""
)

_FINISH_ATTEMPT_SQL = text(
    _ATTEMPT_FENCE_SQL
    + """, finished_work AS (
    UPDATE management.execution_work w SET
        owner=NULL,token=NULL,lease_until=NULL,
        state=CASE WHEN :retry AND NOT :success AND f.attempt<:max_attempts
            THEN 'queued' WHEN :success THEN 'completed' ELSE 'failed' END,
        available_at=CASE WHEN :retry AND NOT :success AND f.attempt<:max_attempts
            THEN f.now+make_interval(secs => LEAST(60,power(2,LEAST(6,f.attempt))))
            ELSE w.available_at END,
        payload_size=octet_length(CASE WHEN :retry AND NOT :success AND f.attempt<:max_attempts
            THEN CAST(:retry_payload AS text) ELSE CAST(:payload AS text) END)
    FROM owned f WHERE w.run_id=f.run_id AND w.task_id=f.task_id
        AND f.lease_until>f.now
    RETURNING w.run_id,w.task_id,w.state,w.available_at,w.dedup_key,f.now
), finished_payload AS (
    UPDATE management.execution_work_payloads p SET payload_json=
        CASE WHEN w.state='queued' THEN :retry_payload ELSE :payload END
    FROM finished_work w WHERE p.run_id=w.run_id AND p.task_id=w.task_id
    RETURNING p.task_id
), finished_attempt AS (
    UPDATE management.execution_attempts a SET
        state=CASE WHEN :success THEN 'completed' ELSE 'failed' END,
        completed_at=w.now,error=:error,timings_json=:timings
    FROM finished_work w
    -- Bind the partition key directly: the materialized work CTE cannot
    -- prune attempt partitions while PostgreSQL plans this UPDATE.
    WHERE a.run_id=:run_id AND a.run_id=w.run_id
        AND a.task_id=w.task_id AND a.number=:number
    RETURNING a.run_id,a.task_id
), released_resource AS (
    DELETE FROM management.execution_resources r USING finished_work w
    WHERE r.resource_key=w.dedup_key AND r.token=:token
    RETURNING r.resource_key
)
SELECT w.state,w.available_at FROM finished_work w
JOIN finished_attempt a ON a.run_id=w.run_id AND a.task_id=w.task_id
"""
)


class StaleAttempt(RuntimeError):
    """The attempt expired, was cancelled, or was replaced."""


def owns(config: dict[str, Any]) -> bool:
    return config.get("metadata", {}).get("execution_engine") == ENGINE


def _number(name: str, default: int, *, resolve=runtime_env_value) -> int:
    raw = resolve(name)
    value = default if raw is None or not raw.strip() else int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


@dataclass(frozen=True)
class Limits:
    tasks: int = 256
    bytes: int = 4 * 1024 * 1024
    global_tasks: int = 4096
    global_bytes: int = 64 * 1024 * 1024
    batch: int = 128
    lease_seconds: int = 120
    result_bytes: int = 16 * 1024 * 1024
    # Explicit fixture/control limits retain their batch size unless overridden.
    # Runtime planning can amortize pages without enlarging worker claims.
    planner_batch: int | None = None
    archive_batch: int | None = None
    # Total resident count ceiling, including the independently drained results.
    # Bytes are a terminal high-water mark: existing attempts may still finish.
    terminal_tasks: int = 8192
    terminal_bytes: int = 64 * 1024 * 1024

    @classmethod
    def load(cls):
        # Parse missing file-backed settings once per construction. A new
        # load still observes file edits; process values, including blanks,
        # retain precedence and all existing validation and defaults.
        file_values: dict[str, str] | None = None

        def value(name: str) -> str | None:
            nonlocal file_values
            process_value = os.environ.get(name)
            if process_value is not None:
                return process_value
            if file_values is None:
                file_values = read_env()
            return file_values.get(name)

        def number(name: str, default: int) -> int:
            return _number(name, default, resolve=value)

        # Ledger-specific overrides remain available. Otherwise retain the
        # established window environment controls; persisted controls are read
        # by the planner on its own connection, never in this constructor.
        defaults = dict(cls().__dict__)
        defaults.pop("planner_batch")
        defaults.pop("archive_batch")
        defaults.pop("terminal_tasks")
        defaults.pop("terminal_bytes")
        defaults["tasks"] = number("ULTIMA_JOB_WINDOW_MAX_TASKS", 1000)
        defaults["bytes"] = number(
            "ULTIMA_JOB_WINDOW_MAX_BYTES",
            number("ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", 64 * 1024 * 1024),
        )
        defaults["global_tasks"] = number(
            "ULTIMA_JOB_GLOBAL_WINDOW_MAX_TASKS", defaults["tasks"]
        )
        defaults["global_bytes"] = number(
            "ULTIMA_JOB_GLOBAL_WINDOW_MAX_BYTES", defaults["bytes"]
        )
        resolved = {
            name: number(f"ULTIMA_LEDGER_{name.upper()}", default)
            for name, default in defaults.items()
        }
        # Retain an explicitly configured batch cap. Otherwise use a
        # larger bounded page only for archive/refill, not claim or control work.
        configured_batch = value("ULTIMA_LEDGER_BATCH")
        planner_setting = None
        try:
            from .resources import _persisted_job_execution_setting

            planner_setting = _persisted_job_execution_setting("ledger_planner_batch")
        except Exception:
            pass
        planner_default = (
            resolved["batch"]
            if configured_batch and configured_batch.strip()
            else (planner_setting or 512)
        )
        return cls(
            **resolved,
            planner_batch=number("ULTIMA_LEDGER_PLANNER_BATCH", planner_default),
            archive_batch=number("ULTIMA_LEDGER_ARCHIVE_BATCH", 2048),
            terminal_tasks=number(
                "ULTIMA_LEDGER_TERMINAL_TASKS", 2 * resolved["global_tasks"]
            ),
            terminal_bytes=number(
                "ULTIMA_LEDGER_TERMINAL_BYTES", resolved["global_bytes"]
            ),
        )


@dataclass(frozen=True)
class Claim:
    run_id: str
    task_id: str
    token: str
    owner: str
    number: int
    job: dict[str, Any]
    task: dict[str, Any]


_ARCHIVE_SQL_ROWS_DISABLED = {"0", "false", "off", "no"}

# Locks one archive batch and builds its large text column in SQL. ``json``
# (not ``jsonb``) keeps each value's original text, so ``definition`` is the
# stored payload with only its top-level ``result`` replaced by null: the same
# bytes as ``_dump(task.to_dict(include_result=False))`` for a payload written
# by ``_dump(task.to_dict())``. ``light`` drops the task's own ``payload``,
# which no history column reads. Subtasks would also need their results
# removed, so such rows (and any without a ``result`` key) return the full
# payload for the Python path instead.
_LOCK_ARCHIVE_ROWS = text("""
WITH locked AS MATERIALIZED (
    SELECT w.task_id, w.candidate
    FROM management.execution_work w
    WHERE w.run_id = :run_id AND w.task_id = ANY(CAST(:ids AS varchar[]))
    ORDER BY w.task_id
    FOR UPDATE
)
SELECT l.task_id, l.candidate, e.light,
    CASE WHEN e.simple THEN e.definition END AS definition,
    CASE WHEN e.simple THEN NULL ELSE p.payload_json END AS payload_json
FROM locked l
-- One primary-key probe per locked row. A plain join (or a LATERAL the
-- planner can flatten into one) hashed the batch against a scan of the whole
-- payload table, which bloats during a run: 8-43 s passes in production.
-- LIMIT keeps this subquery from being flattened.
CROSS JOIN LATERAL (
    SELECT payload_json FROM management.execution_work_payloads
    WHERE run_id = :run_id AND task_id = l.task_id
    LIMIT 1
) p
CROSS JOIN LATERAL (
    SELECT
        '{' || coalesce(string_agg(
            to_json(f.key)::text || ':' || f.value::text, ',' ORDER BY f.n
        ) FILTER (WHERE f.key <> 'payload'), '') || '}' AS light,
        '{' || string_agg(
            to_json(f.key)::text || ':'
                || CASE WHEN f.key = 'result' THEN 'null' ELSE f.value::text END,
            ',' ORDER BY f.n
        ) || '}' AS definition,
        count(*) FILTER (WHERE f.key = 'result') = 1
            AND count(*) FILTER (WHERE f.key = 'subtasks') = 1
            AND bool_and(f.key <> 'subtasks' OR f.value::text = '[]') AS simple
    FROM json_each(p.payload_json::json) WITH ORDINALITY AS f(key, value, n)
) e
""")


def archive_sql_rows_enabled() -> bool:
    """ULTIMA_ARCHIVE_SQL_ROWS=0/false/off/no restores the ORM + Python row build."""
    value = runtime_env_value("ULTIMA_ARCHIVE_SQL_ROWS")
    return value is None or value.strip().lower() not in _ARCHIVE_SQL_ROWS_DISABLED


_MAX_ARCHIVE_PARALLEL_PASSES = 8


def archive_parallel_passes() -> int:
    """ULTIMA_ARCHIVE_PARALLEL_PASSES: concurrent archive passes (default 1, max 8).

    1 keeps one pass per run, holding the run's plan row for the whole pass.
    Above 1, passes for one run lock disjoint queue rows with SKIP LOCKED and
    take the plan row only to record progress (see ``archive_tick``). The
    service reads this at start to size its archiver pool; ``archive_tick``
    and ``tick`` read it per pass for their locking mode.
    """
    value = runtime_env_value("ULTIMA_ARCHIVE_PARALLEL_PASSES")
    try:
        passes = int(value.strip()) if value is not None and value.strip() else 1
    except ValueError:
        return 1
    return max(1, min(passes, _MAX_ARCHIVE_PARALLEL_PASSES))


def _python_history_row(payload_json: str, run_id: str) -> dict[str, Any]:
    from .storage import _task_to_central_row

    task = Task.from_dict(json.loads(payload_json))
    return {
        **_task_to_central_row(task, run_id),
        "definition_json": _dump(task.to_dict(include_result=False)),
    }


def _archive_history_row(row: Any, run_id: str) -> dict[str, Any]:
    """History row from ``_LOCK_ARCHIVE_ROWS``: Python derives only the small columns."""
    from .storage import _task_history_columns

    if row.definition is None:
        return _python_history_row(row.payload_json, run_id)
    task = Task.from_dict({**json.loads(row.light), "payload": None})
    return {**_task_history_columns(task, run_id), "definition_json": row.definition}


_SET_CLAIMS_DISABLED = {"0", "false", "off", "no"}


def set_claims_enabled() -> bool:
    """ULTIMA_LEDGER_SET_CLAIMS=1/true/on/yes claims with fixed-text array SQL.

    Default off: claims build ORM objects as before. The set-based path writes
    the same work, payload and attempt rows with three statements per run.
    """
    value = runtime_env_value("ULTIMA_LEDGER_SET_CLAIMS")
    return value is not None and value.strip().lower() not in _SET_CLAIMS_DISABLED | {""}


_CLAIM_WORK = text("""
UPDATE management.execution_work w SET
    state = 'running', owner = :owner, token = c.token, attempt = w.attempt + 1,
    lease_until = :until, payload_size = c.size
FROM unnest(CAST(:task_ids AS varchar[]), CAST(:tokens AS varchar[]), CAST(:sizes AS bigint[]))
    AS c(task_id, token, size)
WHERE w.run_id = :run_id AND w.task_id = c.task_id
""")
_CLAIM_PAYLOADS = text("""
UPDATE management.execution_work_payloads p SET payload_json = c.payload
FROM unnest(CAST(:task_ids AS varchar[]), CAST(:payloads AS text[])) AS c(task_id, payload)
WHERE p.run_id = :run_id AND p.task_id = c.task_id
""")
_CLAIM_ATTEMPTS = text("""
INSERT INTO management.execution_attempts (run_id, task_id, number, token, owner, state, started_at)
SELECT CAST(:run_id AS varchar), c.task_id, c.number, c.token, :owner, 'running', CAST(:now AS timestamptz)
FROM unnest(CAST(:task_ids AS varchar[]), CAST(:numbers AS integer[]), CAST(:tokens AS varchar[]))
    AS c(task_id, number, token)
""")


def _acquire_resources(session, run_id, selected, until, now) -> set[str]:
    """Lease each selected task's dedup resource; returns the tokens that won."""
    resources = sorted(
        [
            {
                "resource_key": dedup_key,
                "token": token,
                "run_id": run_id,
                "task_id": task_id,
                "lease_until": until,
            }
            for task_id, dedup_key, token in selected
            if dedup_key
        ],
        key=lambda row: row["resource_key"],
    )
    if not resources:
        return set()
    statement = insert(Resource).values(resources)
    return set(
        session.execute(
            statement.on_conflict_do_update(
                index_elements=[Resource.resource_key],
                set_={
                    key: getattr(statement.excluded, key)
                    for key in ("token", "run_id", "task_id", "lease_until")
                },
                where=Resource.lease_until <= now,
            ).returning(Resource.token)
        ).scalars()
    )


def _dump(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _attempt_timings(timings: dict[str, Any] | None) -> str:
    """Serialize attempt timings without the nested ``*_json`` maps.

    Route, phase and writer maps already reach job diagnostics through the
    Redis runtime telemetry; repeating them on every attempt row made them
    about half of the attempt history's size.
    """
    return _dump(
        {key: value for key, value in (timings or {}).items() if not key.endswith("_json")}
    )


def _now(session) -> datetime:
    return session.execute(select(func.clock_timestamp())).scalar_one()


def _event(session, run_id: str, event: str, **data):
    session.add(
        Outbox(
            run_id=run_id,
            payload_json=_dump(
                {
                    "type": "job_event",
                    "event": event,
                    "job_id": run_id,
                    "execution_engine": ENGINE,
                    **data,
                }
            ),
        )
    )


# Per-task lifecycle events are live hints, not durable history; see live_events.
TASK_EVENTS = frozenset({
    "task_started", "task_completed", "task_failed", "task_retry_scheduled", "task_recovered",
})


# Task events stored before they became live-only. A bulk job's backlog of them
# has no durable consumer; marking pages delivered without a sequence keeps the
# job-level events queued behind them from waiting hours for the relay.
LEGACY_TASK_EVENT_PREFIX = '{"type":"job_event","event":"task_'
LEGACY_SKIP_BATCH = 10000
_SKIP_LEGACY_TASK_EVENTS = text("""UPDATE management.execution_outbox o SET delivered=true
FROM (SELECT event_id, run_id FROM management.execution_outbox
      WHERE delivered=false AND starts_with(payload_json, :prefix)
      ORDER BY event_id LIMIT :limit FOR UPDATE SKIP LOCKED) legacy
WHERE o.event_id=legacy.event_id AND o.run_id=legacy.run_id
RETURNING o.event_id""")


def _task_event(session, run_id: str, event: str, **data):
    from . import live_events

    live_events.defer(session, {
        "type": "job_event", "event": event, "job_id": run_id,
        "execution_engine": ENGINE, **data,
    })


def _scheduled(config: dict, session) -> datetime:
    schedule = config.get("schedule") or {}
    if schedule.get("interval_seconds") or schedule.get("cron"):
        raise ValueError(
            "Recurring schedules are not supported; use an immediate or one-time schedule"
        )
    value = schedule.get("scheduled_for")
    result = datetime.fromisoformat(value) if value else _now(session)
    if result.tzinfo is None:
        raise ValueError("Scheduled execution requires an explicit timezone")
    return result


def revise(session, run, plan):
    """Retain previous definition revisions; a run points at its current input."""
    validate_inputs(session, plan, Limits.load())
    revision = (
        session.scalar(
            select(func.max(Definition.revision)).where(
                Definition.definition_id == run.definition_id
            )
        )
        + 1
    )
    config = plan["job_config"]
    config.setdefault("metadata", {}).update(
        execution_engine=ENGINE,
        execution_definition_id=run.definition_id,
        execution_revision=revision,
    )
    session.add(
        Definition(
            definition_id=run.definition_id,
            revision=revision,
            config_json=_dump({"job": config, "selection": plan["selection"]}),
        )
    )
    session.flush()
    run.revision, run.config_json = revision, _dump(config)
    run.backend_id = config["metadata"].get("dispatch_backend_id")
    run.next_at = _scheduled(config, session)
    session.get(MPSPlanModel, run.run_id).job_config_json = run.config_json
    session.get(CentralJobModel, run.run_id).job_config_json = run.config_json
    return config


def validate_inputs(session, plan, limits):
    from ultima_scraper_db_archive.global_db import MPSTaskDefinitionModel

    from .storage import TaskPayloadBudgetExceeded

    manifest = plan["selection"].get("task_manifest_id") or plan["selection"].get(
        "spawned_manifest_id"
    )
    budget = min(limits.bytes, limits.global_bytes)
    oversized = session.execute(
        select(
            MPSTaskDefinitionModel.task_id,
            func.octet_length(MPSTaskDefinitionModel.task_json),
        )
        .where(
            MPSTaskDefinitionModel.manifest_id == manifest,
            func.octet_length(MPSTaskDefinitionModel.task_json) > budget,
        )
        .limit(1)
    ).first()
    if oversized:
        raise TaskPayloadBudgetExceeded(
            oversized[0], oversized[1], budget, phase="execution input"
        )


def admit(
    session,
    plan: dict[str, Any],
    *,
    definition_id: str | None = None,
    revision: int = 1,
) -> dict[str, Any]:
    """Called in the input transaction, before any runtime is exposed."""
    config = plan["job_config"]
    existing = session.get(Run, plan["job_id"])
    if existing is not None:
        return plan
    validate_inputs(session, plan, Limits.load())
    # ATTACH installs foreign keys with SHARE ROW EXCLUSIVE on execution_runs.
    # Take the DDL fence before INSERT's ROW EXCLUSIVE lock on that table:
    # otherwise another admission can hold the fence while waiting for our
    # insert transaction, leaving us waiting for its fence in a deadlock.
    from ..database.job_partitions import ensure_history_partitions

    ensure_history_partitions(session, plan["job_id"])
    definition_id = definition_id or f"definition-{uuid4().hex}"
    metadata = {
        **config.get("metadata", {}),
        "execution_engine": ENGINE,
        "execution_definition_id": definition_id,
        "execution_revision": revision,
        "total_tasks_count_override": plan["total"],
    }
    config = {**config, "metadata": metadata}
    next_at = _scheduled(config, session)
    # Keep the complete frozen selection in the definition, with compressed
    # ranges left in SQL. A run references its own source plan and task IDs.
    session.add(
        Definition(
            definition_id=definition_id,
            revision=revision,
            config_json=_dump({"job": config, "selection": plan["selection"]}),
        )
    )
    session.flush()
    session.add(
        Run(
            run_id=plan["job_id"],
            definition_id=definition_id,
            revision=revision,
            config_json=_dump(config),
            next_at=next_at,
            backend_id=metadata.get("dispatch_backend_id"),
            priority=int(
                metadata.get("priority", metadata.get("dispatch_priority", 0))
            ),
        )
    )
    session.flush()
    source = session.get(MPSPlanModel, plan["job_id"])
    source.job_config_json = _dump(config)
    central = session.get(CentralJobModel, plan["job_id"])
    if central is not None:
        central.job_config_json = _dump(config)
    _event(
        session,
        plan["job_id"],
        "job_created",
        job_name=config.get("name"),
        status="pending",
        task_count=plan["total"],
    )
    return {**plan, "job_config": config}


def validate_write_attempt(session, identity: dict[str, str]):
    """Fence mutations in the same SQL transaction as their write receipt."""
    validate_write_attempts(session, [identity])


def validate_write_attempts(session, identities: list[dict[str, str]]):
    """Lock a bounded writer batch's fences before admitting any mutation.

    Controls lock runs before work. Keep that order, then read the database
    clock after all potentially blocking locks. No ownership evidence is cached
    outside this transaction; committed receipts bypass this check as before.
    """
    if not identities:
        return
    run_ids = sorted({identity["run_id"] for identity in identities})
    run_turnstile.lock_shared(session, run_ids)
    runs = dict(
        session.execute(
            select(Run.run_id, Run.state)
            .where(Run.run_id.in_(run_ids))
            .order_by(Run.run_id)
            .with_for_update(read=True)
        ).all()
    )
    task_ids = sorted(
        {(identity["run_id"], identity["task_id"]) for identity in identities}
    )
    work = {
        (row.run_id, row.task_id): row
        for row in session.execute(
            select(
                Work.run_id,
                Work.task_id,
                Work.state,
                Work.token,
                Work.owner,
                Work.lease_until,
            )
            .where(tuple_(Work.run_id, Work.task_id).in_(task_ids))
            .order_by(Work.run_id, Work.task_id)
            .with_for_update(read=True)
        )
    }
    now = _now(session)
    for identity in identities:
        row = work.get((identity["run_id"], identity["task_id"]))
        if (
            identity["run_id"] not in runs
            or runs[identity["run_id"]] in TERMINAL
            or row is None
            or row.state not in ACTIVE_WORK
            or row.token != identity["token"]
            or row.owner != identity["owner"]
            or row.lease_until is None
            or row.lease_until <= now
        ):
            raise StaleAttempt(
                "Database mutation belongs to an expired or cancelled attempt"
            )


def _work_payload(payload_json: str, state: str, owner: str | None, attempt: int) -> dict:
    """Task payload with the live execution state of its work row."""
    payload = json.loads(payload_json)
    payload.update(
        state="pending" if state == "queued" else state,
        worker_id=owner,
        attempts=attempt,
    )
    if state == "persisting":
        payload["state"] = "saving_db"
    return payload


class ExecutionLedger:
    def __init__(
        self,
        session_factory: Callable = global_session,
        *,
        limits: Limits | None = None,
    ):
        self.session_factory = session_factory
        self._configured_windows = limits is None
        self.limits = limits or Limits.load()
        self.manifests = MPSManifestStore(session_factory)
        self._last_planned_run_id = ""
        self._last_archived_run_id = ""
        # job_id -> (md5 of proxy_pool_json, decoded pool) for refill passes.
        self._proxy_pools: dict[str, tuple[str, tuple[Any, ...]]] = {}

    def checkpoint_batch(self, requests):
        from .ledger_batches import checkpoint_batch

        return checkpoint_batch(self, requests)

    def finish_batch(self, requests):
        from .ledger_batches import finish_batch

        return finish_batch(self, requests)

    def _locked_run(self, session, run_id: str, *, shared: bool = False):
        if shared:
            run_turnstile.lock_shared(session, [run_id])
        else:
            run_turnstile.lock_exclusive(session, run_id)
        return session.execute(
            select(Run)
            .where(Run.run_id == run_id)
            .with_for_update(read=shared, key_share=not shared)
        ).scalar_one_or_none()

    def _attempt(self, session, claim: Claim):
        # Shared run locks let independent workers finish concurrently, while
        # cancellation obtains an exclusive run lock before revoking work.
        run = self._locked_run(session, claim.run_id, shared=True)
        work = session.execute(
            select(Work)
            .where(Work.run_id == claim.run_id, Work.task_id == claim.task_id)
            .with_for_update()
        ).scalar_one_or_none()
        now = _now(session)
        if (
            run is None
            or run.state in TERMINAL
            or work is None
            or work.token != claim.token
            or work.owner != claim.owner
            or work.state not in ACTIVE_WORK
            or work.lease_until <= now
        ):
            raise StaleAttempt(f"Attempt no longer owns {claim.run_id}/{claim.task_id}")
        return run, work, now

    def claim(
        self,
        *,
        owner: str,
        backend_id: str,
        capabilities: list[str],
        lane: str | None = None,
    ) -> Claim | None:
        claims = self.claim_many(
            owner=owner,
            backend_id=backend_id,
            capabilities=capabilities,
            lane=lane,
            count=1,
        )
        return claims[0] if claims else None

    def _planner_snapshot(self, session, source, pool_digest) -> dict[str, Any]:
        """``_snapshot`` for refill, with the decoded proxy pool cached by digest.

        The pool is returned as a tuple: callers only read it, and a shared
        cached value must not be mutated.
        """
        cached = self._proxy_pools.get(source.job_id)
        if cached is None or cached[0] != pool_digest:
            raw = session.scalar(
                select(MPSPlanModel.proxy_pool_json).where(MPSPlanModel.job_id == source.job_id)
            )
            if len(self._proxy_pools) >= 64:
                self._proxy_pools.clear()
            cached = self._proxy_pools[source.job_id] = (pool_digest, tuple(json.loads(raw)))
        return {
            "job_id": source.job_id,
            "site": source.site,
            "job_config": json.loads(source.job_config_json),
            "selection": json.loads(source.selection_json),
            "proxy_pool": cached[1],
            "total": source.total,
            "cursor": source.cursor,
            "completed": source.completed,
            "failed": source.failed,
            "status": source.status,
            "summary": json.loads(source.summary_json),
        }

    def _claim_run_set_based(
        self, session, run, run_id, total, starting, ready, wanted, owner, until, now,
    ) -> list[Claim]:
        """claim_many for one locked run, without per-task ORM objects.

        Writes the same rows as the ORM path: work (state, owner, token,
        attempt + 1, lease, payload_size), the rewritten payload and one
        running attempt per task, in three fixed-text array statements.
        """
        candidates = session.execute(
            select(Work.task_id, Work.dedup_key, Work.attempt)
            .where(Work.run_id == run_id, *ready)
            .order_by(Work.created_at, Work.task_id)
            .limit(max(8, wanted * 2))
            .with_for_update(skip_locked=True)
        ).all()
        selected, keys = [], set()
        for task_id, dedup_key, attempt in candidates:
            if dedup_key and dedup_key in keys:
                continue
            if dedup_key:
                keys.add(dedup_key)
            selected.append((task_id, dedup_key, attempt, uuid4().hex))
            if len(selected) == wanted:
                break
        acquired = _acquire_resources(
            session,
            run_id,
            [(task_id, dedup_key, token) for task_id, dedup_key, _, token in selected],
            until,
            now,
        )
        claimed = [
            (task_id, attempt, token)
            for task_id, dedup_key, attempt, token in selected
            if not dedup_key or token in acquired
        ]
        if not claimed:
            return []
        if starting:
            run.state = "running"
            run.started_at = run.started_at or now
            central = session.get(CentralJobModel, run_id)
            central.status, central.started_at = "running", run.started_at
        payloads = dict(session.execute(
            select(ExecutionWorkPayloadModel.task_id, ExecutionWorkPayloadModel.payload_json).where(
                ExecutionWorkPayloadModel.run_id == run_id,
                ExecutionWorkPayloadModel.task_id.in_([task_id for task_id, _, _ in claimed]),
            )
        ).all())
        stamp = now.isoformat()
        tasks, stored = [], []
        for task_id, attempt, token in claimed:
            task = json.loads(payloads[task_id])
            task.update(state="running", attempts=attempt, worker_id=owner)
            tasks.append(task)
            stored.append(_dump({
                **task, "attempts": attempt + 1, "started_at": stamp, "updated_at": stamp,
            }))
        task_ids = [task_id for task_id, _, _ in claimed]
        tokens = [token for _, _, token in claimed]
        session.execute(_CLAIM_WORK, {
            "run_id": run_id, "owner": owner, "until": until, "task_ids": task_ids,
            "tokens": tokens, "sizes": [len(value.encode("utf-8")) for value in stored],
        })
        session.execute(_CLAIM_PAYLOADS, {"run_id": run_id, "task_ids": task_ids, "payloads": stored})
        session.execute(_CLAIM_ATTEMPTS, {
            "run_id": run_id, "owner": owner, "now": now, "task_ids": task_ids,
            "numbers": [attempt + 1 for _, attempt, _ in claimed], "tokens": tokens,
        })
        result = []
        for (task_id, attempt, token), task in zip(claimed, tasks):
            _task_event(
                session, run_id, "task_started", task_id=task_id, attempt=attempt + 1,
                worker_id=owner, status="running",
            )
            job = json.loads(run.config_json)
            job.setdefault("metadata", {})["total_tasks_count_override"] = total
            result.append(Claim(run_id, task_id, token, owner, attempt + 1, job, task))
        return result

    def claim_many(
        self,
        *,
        owner: str,
        backend_id: str,
        capabilities: list[str],
        count: int,
        lane: str | None = None,
    ) -> list[Claim]:
        """Claim a bounded batch in one transaction, fencing runs before work.

        Running jobs take shared run locks, compatible with other claimers,
        finishes and writer fences. Only the first start changes the run row.
        Controls and planner transactions still acquire exclusive run locks.
        """
        if count <= 0:
            return []
        count = min(count, self.limits.batch)
        with self.session_factory() as session:
            # These bounded claims are too short to amortize JIT compilation
            # when changing window statistics inflate the planner's estimate.
            # LOCAL resets at commit/rollback, including through PgBouncer.
            session.execute(text("SET LOCAL jit=off"))
            now = _now(session)
            ready = [
                Work.state == "queued",
                Work.available_at <= now,
                Work.capabilities.contained_by(capabilities),
                Work.site.in_(capabilities),
                # Keep resource availability as a keyed candidate probe.
                # NOT EXISTS can become a hash anti join that scans/sorts the
                # whole window before LIMIT, even with the claim-order index.
                select(Resource.resource_key)
                .where(
                    Resource.resource_key == Work.dedup_key,
                    Resource.lease_until > now,
                )
                .exists()
                .is_(False),
            ]
            if lane:
                ready.append(Work.lane == lane)
            run_rows = session.execute(
                select(Run.run_id, MPSPlanModel.total)
                .join(MPSPlanModel, MPSPlanModel.job_id == Run.run_id)
                .where(
                    Run.state.in_(["pending", "running"]),
                    Run.next_at <= now,
                    (Run.backend_id.is_(None) | (Run.backend_id == backend_id)),
                    select(Work.task_id)
                    .where(Work.run_id == Run.run_id, *ready)
                    .exists(),
                )
                .order_by(Run.priority, Run.run_id)
                .limit(min(count, 32))
            ).all()
            result = []
            until = now + timedelta(seconds=self.limits.lease_seconds)
            set_claims = set_claims_enabled() and session.get_bind().dialect.name == "postgresql"
            # Collect ORM changes until the batch ends. Resource SQL must not
            # autoflush one attempt, work update and outbox row per task.
            with session.no_autoflush:
                for run_id, total in run_rows:
                    if len(result) >= count:
                        break
                    run = session.execute(
                        select(Run)
                        .where(
                            Run.run_id == run_id,
                            Run.state == "running",
                            Run.next_at <= now,
                            Run.backend_id.is_(None) | (Run.backend_id == backend_id),
                            run_turnstile.try_shared(Run.run_id),
                        )
                        .with_for_update(read=True, skip_locked=True)
                    ).scalar_one_or_none()
                    starting = run is None
                    if starting:
                        run = session.execute(
                            select(Run)
                            .where(
                                Run.run_id == run_id,
                                Run.state == "pending",
                                Run.next_at <= now,
                                Run.backend_id.is_(None)
                                | (Run.backend_id == backend_id),
                            )
                            .with_for_update(key_share=True, skip_locked=True)
                        ).scalar_one_or_none()
                    if run is None:
                        continue
                    if set_claims:
                        result.extend(self._claim_run_set_based(
                            session, run, run_id, total, starting, ready,
                            count - len(result), owner, until, now,
                        ))
                        continue
                    candidates = (
                        session.execute(
                            select(Work)
                            .where(Work.run_id == run_id, *ready)
                            .order_by(Work.created_at, Work.task_id)
                            .limit(max(8, (count - len(result)) * 2))
                            .with_for_update(skip_locked=True)
                        )
                        .scalars()
                        .all()
                    )
                    selected, keys = [], set()
                    for work in candidates:
                        if work.dedup_key and work.dedup_key in keys:
                            continue
                        if work.dedup_key:
                            keys.add(work.dedup_key)
                        selected.append((work, uuid4().hex))
                        if len(selected) == count - len(result):
                            break
                    acquired = _acquire_resources(
                        session,
                        run_id,
                        [(work.task_id, work.dedup_key, token) for work, token in selected],
                        until,
                        now,
                    )
                    for work, token in selected:
                        if work.dedup_key and token not in acquired:
                            continue
                        work.state, work.owner, work.token = "running", owner, token
                        work.attempt += 1
                        work.lease_until = until
                        if starting:
                            run.state = "running"
                            run.started_at = run.started_at or now
                            central = session.get(CentralJobModel, run_id)
                            central.status, central.started_at = (
                                "running",
                                run.started_at,
                            )
                            starting = False
                        task = json.loads(work.payload_json)
                        task.update(
                            state="running", attempts=work.attempt - 1, worker_id=owner
                        )
                        work.payload_json = _dump(
                            {
                                **task,
                                "attempts": work.attempt,
                                "started_at": now.isoformat(),
                                "updated_at": now.isoformat(),
                            }
                        )
                        session.add(
                            Attempt(
                                run_id=run_id,
                                task_id=work.task_id,
                                number=work.attempt,
                                token=token,
                                owner=owner,
                                state="running",
                                started_at=now,
                            )
                        )
                        _task_event(
                            session,
                            run_id,
                            "task_started",
                            task_id=work.task_id,
                            attempt=work.attempt,
                            worker_id=owner,
                            status="running",
                        )
                        job = json.loads(run.config_json)
                        job.setdefault("metadata", {})["total_tasks_count_override"] = (
                            total
                        )
                        result.append(
                            Claim(
                                run_id,
                                work.task_id,
                                token,
                                owner,
                                work.attempt,
                                job,
                                task,
                            )
                        )
            session.commit()
            return result

    def renew(self, claim: Claim) -> bool:
        self._checkpoint(claim, renew=True)
        return True

    def _checkpoint(self, claim, *, payload=None, persisting=False, renew=False):
        with self.session_factory() as session:
            run_turnstile.lock_shared(session, [claim.run_id])
            changed = session.execute(
                _CHECKPOINT_ATTEMPT_SQL,
                {
                    "run_id": claim.run_id,
                    "task_id": claim.task_id,
                    "token": claim.token,
                    "owner": claim.owner,
                    "checkpoint": payload is not None,
                    "payload": payload,
                    "persisting": persisting,
                    "renew": renew,
                    "lease_seconds": self.limits.lease_seconds,
                },
            ).scalar_one_or_none()
            if changed is None:
                raise StaleAttempt(
                    f"Attempt no longer owns {claim.run_id}/{claim.task_id}"
                )
            session.commit()

    def progress(
        self,
        claim: Claim,
        payload: dict[str, Any],
        *,
        persisting: bool = False,
        renew: bool = False,
    ):
        encoded = _dump(payload)
        if len(encoded) > self.limits.result_bytes:
            raise ValueError("Task progress exceeds the ledger payload budget")
        self._checkpoint(claim, payload=encoded, persisting=persisting, renew=renew)

    def finish(
        self,
        claim: Claim,
        task: Task,
        *,
        retry: bool = False,
        timings: dict | None = None,
    ):
        if task.result and any(
            len(_dump(child.to_dict()))
            > min(self.limits.bytes, self.limits.global_bytes)
            for child in task.result.new_tasks
        ):
            task.state = TaskState.FAILED
            task.result = TaskResult(
                success=False,
                message="A spawned task exceeds the execution input byte budget",
            )
            task.completed_at = task.result.completed_at
            retry = False
        payload = task.to_dict()
        encoded = _dump(payload)
        if len(encoded) > self.limits.result_bytes:
            task.state = TaskState.FAILED
            task.result = TaskResult(
                success=False,
                message="Task result exceeds the execution result byte budget",
            )
            task.completed_at = task.result.completed_at
            task.subtasks = []
            payload, retry = task.to_dict(), False
            encoded = _dump(payload)
            if len(encoded) > self.limits.result_bytes:
                raise ValueError("Task input exceeds the result page byte budget")
        with self.session_factory() as session:
            prior = session.execute(
                select(Attempt.token, Attempt.state).where(
                    Attempt.run_id == claim.run_id,
                    Attempt.task_id == claim.task_id,
                    Attempt.number == claim.number,
                )
            ).one_or_none()
            if (
                prior is not None
                and prior.token == claim.token
                and prior.state in {"completed", "failed"}
            ):
                return  # Acknowledgement loss never repeats a committed finish.
            success = (
                task.state == TaskState.COMPLETED
                and task.result is not None
                and task.result.success
            )
            error = (
                None
                if success
                else (task.result.message if task.result else "Task failed")
            )
            final_state = "completed" if success else "failed"
            if payload.get("state") != final_state:
                payload["state"] = final_state
                encoded = _dump(payload)
            retry_payload = None
            if retry and not success:
                pending_payload = {**payload}
                pending_payload.update(
                    state="pending", result=None, completed_at=None, worker_id=None
                )
                if PROFILE_COMPLETION_KEY in claim.task:
                    pending_payload[PROFILE_COMPLETION_KEY] = claim.task[PROFILE_COMPLETION_KEY]
                retry_payload = _dump(pending_payload)
                if len(retry_payload) > self.limits.result_bytes:
                    raise ValueError("Retry checkpoint exceeds its payload byte budget")
            run_turnstile.lock_shared(session, [claim.run_id])
            changed = session.execute(
                _FINISH_ATTEMPT_SQL,
                {
                    "run_id": claim.run_id,
                    "task_id": claim.task_id,
                    "token": claim.token,
                    "owner": claim.owner,
                    "number": claim.number,
                    "success": success,
                    "retry": retry,
                    "max_attempts": task.max_attempts,
                    "payload": encoded,
                    "retry_payload": retry_payload,
                    "error": error,
                    "timings": _attempt_timings(timings),
                },
            ).one_or_none()
            if changed is None:
                raise StaleAttempt(
                    f"Attempt no longer owns {claim.run_id}/{claim.task_id}"
                )
            if changed.state == "queued":
                _task_event(
                    session,
                    claim.run_id,
                    "task_retry_scheduled",
                    task_id=claim.task_id,
                    retry_at=changed.available_at.isoformat(),
                    error=error,
                )
            else:
                _task_event(
                    session,
                    claim.run_id,
                    "task_completed" if success else "task_failed",
                    task_id=claim.task_id,
                    status=changed.state,
                    attempt=claim.number,
                    message=error or (task.result.message if task.result else ""),
                )
            session.commit()

    def profile_completion(self, claim: Claim, *, task: Task | None = None):
        with self.session_factory() as session:
            return read_committed_profile_completion(
                session, claim.run_id, task if task is not None else Task.from_dict(claim.task),
                claim.task.get(PROFILE_COMPLETION_KEY),
            )

    def recover_expired(self, *, database_outage: bool = False) -> int:
        recovered = 0
        with self.session_factory() as session:
            now = _now(session)
            candidates = session.execute(
                select(Work.run_id, Work.task_id)
                .where(Work.state.in_(ACTIVE_WORK), Work.lease_until <= now)
                .order_by(Work.lease_until)
                .limit(self.limits.batch)
            ).all()
            if not candidates:
                return 0
            # Controls acquire run then work locks. Keep that ordering while
            # reconciling receipts so cancellation cannot be bypassed.
            active_runs = session.execute(
                select(Run.run_id)
                .where(Run.run_id.in_(sorted({r.run_id for r in candidates})),
                       Run.state.not_in(TERMINAL),
                       run_turnstile.try_shared(Run.run_id))
                .order_by(Run.run_id)
                .with_for_update(read=True, skip_locked=True)
            ).scalars().all()
            if not active_runs:
                return 0
            rows = (
                session.execute(
                    select(Work)
                    .where(Work.state.in_(ACTIVE_WORK), Work.lease_until <= now,
                           Work.run_id.in_(active_runs),
                           tuple_(Work.run_id, Work.task_id).in_([tuple(r) for r in candidates]))
                    .order_by(Work.lease_until)
                    .limit(self.limits.batch)
                    .with_for_update(skip_locked=True)
                )
                .scalars()
                .all()
            )
            for work in rows:
                attempt = session.get(
                    Attempt, (work.run_id, work.task_id, work.attempt)
                )
                attempt.state, attempt.completed_at, attempt.error = (
                    "expired",
                    now,
                    "Worker lease expired",
                )
                session.execute(
                    delete(Resource).where(
                        Resource.resource_key == work.dedup_key,
                        Resource.token == work.token,
                    )
                )
                payload = json.loads(work.payload_json)
                task = Task.from_dict(payload)
                completed = read_committed_profile_completion(
                    session, work.run_id, task, payload.get(PROFILE_COMPLETION_KEY)
                )
                work.owner = work.token = None
                work.lease_until = None
                if database_outage and completed is None:
                    task.forgive_database_outage()
                    payload.update(
                        max_attempts=task.max_attempts,
                        database_outage_retries=task.database_outage_retries,
                    )
                    work.payload_json = _dump(payload)
                if completed is not None:
                    task.result, task.state = completed, TaskState.COMPLETED
                    task.completed_at = task.updated_at = completed.completed_at
                    task.attempts = work.attempt
                    work.state, work.payload_json = "completed", _dump(task.to_dict())
                    attempt.state, attempt.error = "completed", None
                    timings = json.loads(attempt.timings_json)
                    timings["recovered_committed_profile"] = True
                    attempt.timings_json = _dump(timings)
                    _task_event(session, work.run_id, "task_completed",
                           task_id=work.task_id, status="completed", attempt=work.attempt,
                           message=completed.message, recovered_committed_profile=True)
                elif work.attempt >= task.max_attempts:
                    task.state = TaskState.FAILED
                    task.result = TaskResult(
                        success=False,
                        message=WORKER_LEASE_EXHAUSTED,
                        output={"error_code": WORKER_LEASE_ERROR_CODE},
                    )
                    task.completed_at = task.result.completed_at
                    work.state, work.payload_json = "failed", _dump(task.to_dict())
                else:
                    work.state = "queued"
                    work.available_at = now
                _task_event(
                    session,
                    work.run_id,
                    "task_recovered",
                    task_id=work.task_id,
                    status=work.state,
                )
                recovered += 1
            session.commit()
        return recovered

    def _source_page(
        self,
        session,
        plan,
        resident_candidates,
        limit,
        byte_limit,
        *,
        resident_task_ids=(),
    ):
        from .mps_tasks import _build_mps_task_batch, get_cached_proxy_geo_map

        selected = plan["selection"]
        source_id = selected.get("task_manifest_id")
        tasks, ordinals = [], {}
        if source_id is None:
            total = selected.get("candidate_total", plan["total"])
            checkpoint = selected.get(
                "candidate_checkpoint",
                {"next": min(plan["cursor"], total), "pending": []},
            )
            active = set(resident_candidates)
            positions = []
            for first, last in checkpoint["pending"]:
                for value in range(first, last):
                    if value not in active:
                        positions.append(value)
                    if len(positions) == limit:
                        break
                if len(positions) == limit:
                    break
            cursor = checkpoint["next"]
            while len(positions) < limit and cursor < total:
                if cursor not in active:
                    positions.append(cursor)
                cursor += 1
            ids = self.manifests.candidate_ids(
                plan["job_id"], positions, db_session=session
            )
            if len(ids) != len(positions):
                raise ValueError("Execution selection differs from its durable ranges")
            # The planner holds an exclusive run lock. A second checkout here
            # can wait behind workers that need that lock and fill PgBouncer.
            # Reuse this transaction and resolve routes once for the whole page.
            geo_map = (
                get_cached_proxy_geo_map(db_session=session)
                if positions and plan["proxy_pool"]
                else {}
            )
            sites = {
                "onlyfans": "OnlyFans",
                "fansly": "Fansly",
                "loyalfans": "LoyalFans",
            }
            # The builder groups the route pool once per call. Build contiguous
            # candidate runs together, retaining exact offsets through holes.
            for _, group in groupby(
                enumerate(positions), key=lambda indexed: indexed[1] - indexed[0]
            ):
                page_positions = [position for _, position in group]
                page_tasks = _build_mps_task_batch(
                    job_id=plan["job_id"],
                    site_display=sites[plan["site"]],
                    target_user_ids=[ids[position] for position in page_positions],
                    proxy_pool=plan["proxy_pool"],
                    proxy_geo_map=geo_map,
                    candidate_offset=page_positions[0],
                    include_direct=bool(
                        selected.get("include_direct_connection", False)
                    ),
                    **{
                        key: int(selected[key])
                        for key in ("max_route_hops", "conclusive_miss_threshold")
                        if selected.get(key) is not None
                    },
                )
                tasks.extend(page_tasks)
                ordinals.update(
                    (task.task_id, position)
                    for task, position in zip(page_tasks, page_positions, strict=True)
                )
            if cursor >= total:
                source_id = selected.get("spawned_manifest_id")
        if len(tasks) < limit and source_id is not None:
            rows = self.manifests.definitions.ready_page(
                source_id,
                limit=limit - len(tasks),
                byte_limit=byte_limit,
                exclude_task_ids=list(resident_task_ids),
                db_session=session,
            )
            for row in rows:
                task = Task.from_dict(row["task"])
                if row["dependency_failed"]:
                    task.state = TaskState.FAILED
                    task.result = TaskResult(
                        success=False, message="Skipped because a dependency failed"
                    )
                    task.completed_at = task.result.completed_at
                tasks.append(task)
        from .payloads import ScrapeTaskPayload

        for task in tasks:
            task.mps_task_manifest = selected.get("task_manifest_id") or ""
            if selected.get("retry_auth_id") is not None:
                task.auth_id = int(selected["retry_auth_id"])
                if isinstance(task.payload, ScrapeTaskPayload):
                    task.payload.use_guest_auth = False
        return tasks, ordinals

    def _archive_finished(
        self, session, run, plan, timings=None, *, parallel=False, plan_locked=True
    ):
        """Archive one batch of the run's terminal queue rows.

        ``parallel`` locks the batch with SKIP LOCKED, so concurrent passes
        own disjoint rows and never wait for each other's rows. Without
        ``plan_locked`` (parallel ``archive_tick``) the plan row is locked only
        after history is written, for candidate-only batches, or before
        ``archive_window`` for mixed ones.
        """
        lap = PhaseTimer(timings)
        # Read lengths before payloads. Result growth cannot make one archive
        # pass hydrate batch * result_bytes into application memory.
        query = (
            select(Work.task_id, Work.payload_size)
            .where(Work.run_id == run.run_id, _TERMINAL_WORK)
            .order_by(Work.task_id)
            .limit(
                self.limits.archive_batch
                or self.limits.planner_batch
                or self.limits.batch
            )
        )
        if parallel:
            # Rows past the byte budget stay locked until commit; another
            # pass takes them next time.
            query = query.with_for_update(skip_locked=True)
        candidates = session.execute(query).all()
        ids, size = [], 0
        for task_id, payload_size in candidates:
            if ids and size + payload_size > self.limits.result_bytes:
                break
            ids.append(task_id)
            size += payload_size
        lap("select_terminal")
        if not ids:
            return plan, 0
        if archive_sql_rows_enabled() and session.get_bind().dialect.name == "postgresql":
            # Locks are taken in task order inside the statement; sorting the
            # wide result in SQL would spill, so order it here instead.
            rows = sorted(
                session.execute(
                    _LOCK_ARCHIVE_ROWS, {"run_id": run.run_id, "ids": ids}
                ).all(),
                key=lambda row: row.task_id,
            )
            lap("lock_rows")
            history = [_archive_history_row(row, run.run_id) for row in rows]
        else:
            rows = (
                session.execute(
                    select(Work)
                    .where(Work.run_id == run.run_id, Work.task_id.in_(ids))
                    .order_by(Work.task_id)
                    .with_for_update()
                )
                .scalars()
                .all()
            )
            lap("lock_rows")
            history = [
                _python_history_row(work.payload_json, run.run_id) for work in rows
            ]
        candidate_ordinals = {
            row.task_id: row.candidate for row in rows if row.candidate is not None
        }
        lap("build_rows")
        if not plan_locked and len(candidate_ordinals) == len(history):
            archive = self.manifests.archive_concurrent(
                run.run_id,
                rows=history,
                manifest_id=plan["selection"].get("task_manifest_id"),
                candidate_ordinals=candidate_ordinals,
                db_session=session,
                timings=timings,
                trusted_identity=trusted_mps_identity_enabled(),
            )
            lap.skip()
            session.execute(
                delete(Work).where(Work.run_id == run.run_id, Work.task_id.in_(ids))
            )
            lap("delete_work")
            if archive is not None:
                plan = self.manifests.archive_checkpoint(
                    run.run_id, archive, db_session=session, timings=timings
                )
                lap.skip()
                # Flush the checkpoint inside the timed phase, as the serial
                # path's DELETE does.
                session.flush()
                lap("checkpoint_flush")
            return plan, len(history)
        if not plan_locked:
            # Explicit or spawned rows keep the serial path: plan lock first.
            plan = self.manifests._snapshot(self._lock_plan(session, run.run_id))
            lap("plan_lock")
        plan = self.manifests.archive_window(
            run.run_id,
            start=plan["cursor"],
            end=plan["cursor"] + len(history),
            rows=history,
            manifest_id=plan["selection"].get("task_manifest_id"),
            db_session=session,
            candidate_ordinals=candidate_ordinals,
            timings=timings,
            trusted_identity=trusted_mps_identity_enabled(),
        )
        lap.skip()
        # Executing the DELETE also autoflushes the plan's pending checkpoint.
        session.execute(
            delete(Work).where(Work.run_id == run.run_id, Work.task_id.in_(ids))
        )
        lap("delete_work")
        return plan, len(history)

    def _window_limits(self, session, metadata) -> Limits:
        if not self._configured_windows:
            return self.limits
        from .resources import window_policy
        from .storage import JobStorage

        policy = window_policy(metadata, JobStorage(None), db_session=session)
        windows = {
            name: _number(f"ULTIMA_LEDGER_{name.upper()}", getattr(policy, name))
            for name in ("tasks", "bytes", "global_tasks", "global_bytes")
        }
        return replace(
            self.limits,
            **windows,
            terminal_tasks=_number(
                "ULTIMA_LEDGER_TERMINAL_TASKS", 2 * windows["global_tasks"]
            ),
            terminal_bytes=_number(
                "ULTIMA_LEDGER_TERMINAL_BYTES", windows["global_bytes"]
            ),
        )

    def reindex_bloated_queue_indexes(
        self, *, threshold_bytes: int | None = None, limit: int = 1
    ) -> list[dict[str, Any]]:
        """Rebuild oversized queue-table indexes online, largest first.

        ``REINDEX INDEX CONCURRENTLY`` keeps reads and writes running; the
        live queue is small, so each rebuild is short. It cannot run inside a
        transaction, so it uses its own autocommit connection. A failed
        concurrent rebuild leaves an invalid ``*_ccnew`` copy, which is dropped.
        """
        threshold = queue_reindex_threshold() if threshold_bytes is None else threshold_bytes
        if threshold <= 0:
            return []
        with self.session_factory() as session:
            bind = session.get_bind()
            if bind.dialect.name != "postgresql":
                return []
            candidates = session.execute(
                text("""
                    SELECT i.relname, pg_relation_size(i.oid)
                    FROM pg_index x
                    JOIN pg_class i ON i.oid = x.indexrelid
                    JOIN pg_class t ON t.oid = x.indrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = 'management' AND t.relname = ANY(:tables)
                      AND x.indisvalid AND pg_relation_size(i.oid) > :threshold
                    ORDER BY 2 DESC LIMIT :limit
                """),
                {"tables": list(_QUEUE_TABLES), "threshold": threshold, "limit": limit},
            ).all()
        engine = owner_engine() or getattr(bind, "engine", bind)
        rebuilt = []
        for name, before in candidates:
            started = time.perf_counter()
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                try:
                    connection.exec_driver_sql(f'REINDEX INDEX CONCURRENTLY management."{name}"')
                except Exception as error:
                    if getattr(getattr(error, "orig", None), "sqlstate", None) == "42501":
                        # Not the table owner (a DML-only runtime role): nothing
                        # was started, so there is nothing to clean up.
                        if not _REINDEX_PRIVILEGE_WARNED:
                            _REINDEX_PRIVILEGE_WARNED.append(True)
                            logger.warning(
                                "Queue index rebuilds need the table owner; set "
                                "ULTIMA_ARCHIVE_OWNER_DATABASE_URL to enable them"
                            )
                        return rebuilt
                    pattern = name.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
                    leftovers = connection.execute(
                        text("""
                            SELECT i.relname FROM pg_index x
                            JOIN pg_class i ON i.oid = x.indexrelid
                            JOIN pg_namespace n ON n.oid = i.relnamespace
                            WHERE n.nspname = 'management' AND NOT x.indisvalid
                              AND i.relname LIKE :pattern
                        """),
                        {"pattern": pattern + "\\_ccnew%"},
                    ).scalars().all()
                    for leftover in leftovers:
                        connection.exec_driver_sql(
                            f'DROP INDEX CONCURRENTLY IF EXISTS management."{leftover}"'
                        )
                    raise
                after = connection.scalar(
                    text("SELECT pg_relation_size(to_regclass(:name))"),
                    {"name": f'management."{name}"'},
                )
            rebuilt.append({
                "index": name, "before_bytes": int(before), "after_bytes": int(after),
                "ms": round((time.perf_counter() - started) * 1000, 1),
            })
        return rebuilt

    def archive_tick(self, *, diagnostics: list[dict] | None = None, outcome=None):
        """Drain durable results independently of execution admission."""
        if outcome is not None:
            outcome[:] = ["deferred"]
        with self.session_factory() as session:
            started = time.perf_counter()
            run = session.execute(
                select(Run)
                .where(
                    Run.state.in_(["pending", "running", "paused"]),
                    select(Work.task_id)
                    .where(
                        Work.run_id == Run.run_id,
                        _TERMINAL_WORK,
                    )
                    .exists(),
                    run_turnstile.try_shared(Run.run_id),
                )
                .order_by(
                    case((Run.run_id > self._last_archived_run_id, 0), else_=1),
                    Run.run_id,
                )
                .limit(1)
                .with_for_update(read=True, skip_locked=True)
            ).scalar_one_or_none()
            if run is None:
                if outcome is not None and not session.scalar(
                    select(
                        select(Work.task_id)
                        .join(Run, Run.run_id == Work.run_id)
                        .where(
                            Run.state.in_(["pending", "running", "paused"]),
                            _TERMINAL_WORK,
                        )
                        .exists()
                    )
                ):
                    outcome[:] = ["idle"]
                return None
            self._last_archived_run_id = run.run_id
            parallel = archive_parallel_passes() > 1
            source_query = select(MPSPlanModel).where(MPSPlanModel.job_id == run.run_id)
            # Parallel passes read the plan unlocked; the plan row is locked
            # only to record progress, after the batch's rows are owned.
            locked_query = source_query.with_for_update(skip_locked=True)
            source = session.execute(
                source_query if parallel else locked_query
            ).scalar_one_or_none()
            plan_locked = not parallel
            if source is not None and source.status == "preparing" and parallel:
                # Retry preparation rewrites the checkpoint under the plan lock,
                # then updates the run: hold the plan for the whole pass, and
                # skip it while preparation holds it, as a serial pass does.
                source = session.execute(
                    locked_query.execution_options(populate_existing=True)
                ).scalar_one_or_none()
                plan_locked = True
            if source is None:
                return None
            phases = {} if diagnostics is not None else None
            lap = PhaseTimer(phases, start=started)
            lap("select_run")
            plan, archived = self._archive_finished(
                session, run, self.manifests._snapshot(source), timings=phases,
                parallel=parallel, plan_locked=plan_locked,
            )
            lap.skip()
            if plan["cursor"] >= plan["total"]:
                if not plan_locked:
                    source = self._lock_plan(session, run.run_id)
                    plan = self.manifests._snapshot(source)
                if plan["cursor"] >= plan["total"]:
                    self._complete_run(session, run, source, plan, _now(session))
            session.commit()
            lap("commit")
            if outcome is not None and archived:
                outcome[:] = ["progress"]
            if diagnostics is not None:
                diagnostics.append(
                    {
                        "job_id": run.run_id,
                        "stage": "ledger_archive",
                        "timestamp_ms": int(time.time() * 1000),
                        "archived_tasks": archived,
                        "archive_ms": round((time.perf_counter() - started) * 1000, 3),
                        # Milliseconds per archive step; their sum approximates archive_ms.
                        "phases_ms": phases,
                        "controls": {"materializer_version": "ledger-archive-v3"},
                    }
                )
            return run.run_id if archived else None

    @staticmethod
    def _lock_plan(session, run_id):
        """Lock the run's plan row, refreshing any older unlocked copy."""
        return session.execute(
            select(MPSPlanModel)
            .where(MPSPlanModel.job_id == run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).scalar_one()

    def _complete_run(self, session, run, source, plan, now):
        run.state = terminal_job_state(plan["job_config"], plan["completed"], plan["failed"])
        run.completed_at = now
        config = plan["job_config"]
        config.update(state=run.state, completed_at=now.isoformat())
        run.config_json = _dump(config)
        source.job_config_json = _dump(config)
        central = session.get(CentralJobModel, run.run_id)
        central.status, central.completed_at = run.state, now
        _event(
            session,
            run.run_id,
            "job_completed",
            status=run.state,
            completed_tasks=plan["completed"],
            failed_tasks=plan["failed"],
            total_tasks=plan["total"],
        )

    def tick(
        self, *, diagnostics: list[dict] | None = None, refill_only=False, outcome=None
    ) -> str | None:
        """Archive and refill one run, under count and byte budgets, atomically."""
        if outcome is not None:
            outcome[:] = ["deferred"]
        with self.session_factory() as session:
            started = time.perf_counter()
            phases = {} if diagnostics is not None else None
            lap = PhaseTimer(phases, start=started)
            now = _now(session)
            # Only the planner owns window accounting. A shared SQL lock makes
            # the global cap exact across multiple control processes.
            if not session.execute(
                text(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended('execution-window-budget',0))"
                )
            ).scalar_one():
                return None
            lap("budget_lock")
            plannable = [
                Run.state.in_(["pending", "running"]),
                Run.next_at <= now,
                run_turnstile.try_shared(Run.run_id),
            ]
            if refill_only:
                plannable.append(_admission_pending())
            run = session.execute(
                select(Run)
                .join(MPSPlanModel, MPSPlanModel.job_id == Run.run_id)
                .where(*plannable)
                .order_by(
                    case((Run.run_id > self._last_planned_run_id, 0), else_=1)
                    if refill_only
                    else MPSPlanModel.updated_at,
                    Run.run_id,
                )
                .limit(1)
                # Window accounting is serialized by the advisory lock and
                # plan row. A shared run fence allows active writers/finishes
                # to proceed; controls still wait before revoking the run.
                .with_for_update(skip_locked=True, read=True, of=Run)
            ).scalar_one_or_none()
            if run is None:
                if outcome is not None and not session.scalar(
                    select(
                        select(Run.run_id)
                        .join(MPSPlanModel, MPSPlanModel.job_id == Run.run_id)
                        .where(*plannable)
                        .exists()
                    )
                ):
                    outcome[:] = ["idle"]
                return None
            run_id = run.run_id
            self._last_planned_run_id = run_id
            lap("select_run")
            source_query = select(MPSPlanModel).where(MPSPlanModel.job_id == run.run_id)
            # Parallel archive passes hold the shared run lock while they wait
            # for the plan, and the one finishing a run then updates the run
            # row. Waiting for the plan here, under this pass's own shared run
            # lock, would deadlock with it; skip the run until the next pass.
            parallel_archive = archive_parallel_passes() > 1
            locked_query = source_query.with_for_update(skip_locked=parallel_archive)
            # The proxy pool is most of the plan row (~67 KB for 1,300 routes)
            # and rarely changes: fetch its digest, not the pool, every pass.
            pooled = (
                select(MPSPlanModel, func.md5(MPSPlanModel.proxy_pool_json))
                .options(defer(MPSPlanModel.proxy_pool_json))
                .where(MPSPlanModel.job_id == run.run_id)
            )
            if not refill_only:
                pooled = pooled.with_for_update(skip_locked=parallel_archive)
            result = session.execute(pooled)
            row = result.one_or_none() if parallel_archive else result.one()
            if row is None:
                return None
            source, pool_digest = row
            plan = self._planner_snapshot(session, source, pool_digest)
            lap("snapshot")
            limits = self._window_limits(
                session, plan["job_config"].get("metadata", {})
            )
            lap("limits")
            if plan["status"] == "preparing":
                if refill_only:
                    result = session.execute(locked_query)
                    source = (
                        result.scalar_one_or_none() if parallel_archive else result.scalar_one()
                    )
                    if source is None:
                        return None
                    session.refresh(source)
                    plan = self.manifests._snapshot(source)
                plan = self.manifests.advance_retry(
                    run.run_id,
                    after=plan["selection"]["retry_preparation"]["after"],
                    db_session=session,
                )
                if plan["status"] != "preparing":
                    run.config_json = _dump(plan["job_config"])
                source.updated_at = now
                session.commit()
                if outcome is not None:
                    outcome[:] = ["progress"]
                return run_id
            archive_started = time.perf_counter()
            archived = 0
            if not refill_only:
                # This pass holds the plan; with parallel archive passes it
                # must not wait for rows they hold while they wait for the plan.
                plan, archived = self._archive_finished(
                    session, run, plan, parallel=parallel_archive
                )
            archive_ms = (time.perf_counter() - archive_started) * 1000
            session.flush()
            if plan["cursor"] >= plan["total"]:
                if refill_only:
                    return None  # The independent archiver finalizes the run.
                run.state = terminal_job_state(
                    plan["job_config"], plan["completed"], plan["failed"]
                )
                run.completed_at = now
                config = plan["job_config"]
                config.update(state=run.state, completed_at=now.isoformat())
                run.config_json = _dump(config)
                source.job_config_json = _dump(config)
                central = session.get(CentralJobModel, run.run_id)
                central.status, central.completed_at = run.state, now
                _event(
                    session,
                    run.run_id,
                    "job_completed",
                    status=run.state,
                    completed_tasks=plan["completed"],
                    failed_tasks=plan["failed"],
                    total_tasks=plan["total"],
                )
                session.commit()
                if outcome is not None:
                    outcome[:] = ["progress"]
                return run_id
            # Counts, byte budgets and outstanding candidate positions share
            # one snapshot. Avoid copying every resident's task ID/state/size
            # into Python and then scanning the same window for global totals.
            # Keep all rows in one statement snapshot for exact global budgets
            # and the archive/checkpoint ordering below. Candidate and task-ID
            # arrays are membership sets; sorting the wide resident rows can
            # spill to disk and prevents partial aggregation under load.
            resident = (
                select(
                    Work.run_id,
                    Work.task_id,
                    Work.state,
                    Work.payload_bytes,
                    Work.payload_size,
                    Work.candidate,
                )
                .subquery("resident_window")
            ).c
            own = resident.run_id == run.run_id
            executable = resident.state.in_(["queued", "running", "persisting"])
            budgeted = executable if refill_only else true()
            terminal = resident.state.in_(["completed", "failed"])
            lap("archive")
            accounting_started = time.perf_counter()
            explicit_source = bool(
                plan["selection"].get("task_manifest_id")
                or plan["selection"].get("spawned_manifest_id")
            )
            window = session.execute(
                select(
                    func.count().filter(own & budgeted).label("count"),
                    func.count().filter(own).label("resident_count"),
                    func.coalesce(
                        func.sum(resident.payload_bytes).filter(own & budgeted), 0
                    ).label("bytes"),
                    func.array_agg(resident.candidate)
                    .filter(own & resident.candidate.is_not(None))
                    .label("candidates"),
                    func.array_agg(resident.task_id)
                    .filter(own & (explicit_source or refill_only))
                    .label("task_ids"),
                    func.count().filter(own & (resident.state == "queued")).label("ready"),
                    func.count()
                    .filter(own & resident.state.in_(ACTIVE_WORK))
                    .label("inflight"),
                    func.count()
                    .filter(own & resident.state.in_(["completed", "failed"]))
                    .label("terminal"),
                    func.count().filter(budgeted).label("global_count"),
                    func.coalesce(
                        func.sum(resident.payload_bytes).filter(budgeted), 0
                    ).label("global_bytes"),
                    func.count().filter(terminal).label("global_terminal_count"),
                    func.coalesce(
                        func.sum(resident.payload_size).filter(terminal),
                        0,
                    ).label("terminal_bytes"),
                )
            ).one()
            accounting_ms = (time.perf_counter() - accounting_started) * 1000
            lap("accounting")
            if refill_only:
                # Resident evidence MUST precede the plan snapshot. An archive
                # deletes work and advances its checkpoint atomically; reading
                # the checkpoint first could re-admit a concurrently deleted ID.
                source, pool_digest = session.execute(
                    select(MPSPlanModel, func.md5(MPSPlanModel.proxy_pool_json))
                    .options(defer(MPSPlanModel.proxy_pool_json))
                    .where(MPSPlanModel.job_id == run.run_id)
                    .execution_options(populate_existing=True)
                ).one()
                plan = self._planner_snapshot(session, source, pool_digest)
            lap("plan_refresh")
            total_count, total_bytes = window.global_count, window.global_bytes
            # PostgreSQL SUM(bigint) yields numeric/Decimal. Normalize the
            # exact integral byte count before computing integer page budgets
            # or projecting JSON diagnostics.
            total_bytes = int(total_bytes)
            planner_batch = self.limits.planner_batch or self.limits.batch
            count = min(
                planner_batch,
                limits.tasks - window.count,
                limits.global_tasks - total_count,
            )
            if refill_only:
                count = min(
                    count,
                    limits.terminal_tasks - total_count - window.global_terminal_count,
                )
                if int(window.terminal_bytes) >= limits.terminal_bytes:
                    count = 0
            byte_limit = min(
                limits.bytes - int(window.bytes),
                limits.global_bytes - total_bytes,
            )
            admitted = admitted_bytes = ready_added = 0
            refill_started = time.perf_counter()
            if count > 0 and byte_limit > 0:
                try:
                    tasks, candidates = self._source_page(
                        session,
                        plan,
                        window.candidates or (),
                        count,
                        byte_limit,
                        resident_task_ids=window.task_ids or (),
                    )
                except MPSDefinitionBudgetExceeded as exc:
                    if exc.required_bytes > min(limits.bytes, limits.global_bytes):
                        raise
                    # Existing work can temporarily leave less than one task
                    # of free space. Preserve the input and commit any archive
                    # progress; a later pass can refill after budget release.
                    tasks, candidates = [], {}
                lap("source_page")
                rows = []
                for task in tasks:
                    encoded = _dump(task.to_dict())
                    size = len(encoded)
                    if size > min(limits.bytes, limits.global_bytes):
                        raise ValueError(
                            f"Task {task.task_id} exceeds the execution window byte budget"
                        )
                    if size > byte_limit:
                        break
                    byte_limit -= size
                    admitted += 1
                    admitted_bytes += size
                    ready_added += task.state != TaskState.FAILED
                    rows.append((task, encoded, size))
                lap("encode")
                _insert_admitted_work(session, run.run_id, rows, candidates)
                lap("insert")
            # Bookkeeping must not upgrade the shared run fence: writers own
            # that row while committing unrelated task mutations. The plan row
            # is already exclusively owned and supplies fair planner rotation.
            if not refill_only:
                source.updated_at = now
            session.commit()
            lap("commit")
            if diagnostics is not None:
                diagnostics.append(
                    {
                        "job_id": run_id,
                        "stage": "ledger_refill",
                        "timestamp_ms": int(time.time() * 1000),
                        "ready": window.ready + ready_added,
                        "inflight": window.inflight,
                        "archived_tasks": archived,
                        "admitted_tasks": admitted,
                        "archive_ms": round(archive_ms, 3),
                        "window_accounting_ms": round(accounting_ms, 3),
                        "refill_ms": round(
                            (time.perf_counter() - refill_started) * 1000, 3
                        ),
                        "pass_ms": round((time.perf_counter() - started) * 1000, 3),
                        # Milliseconds per refill step; their sum approximates pass_ms.
                        "phases_ms": phases,
                        "controls": {
                            "materializer_version": "ledger-refill-v3"
                            if refill_only
                            else "ledger-refill-v2",
                            "window_tasks": limits.tasks,
                            "window_bytes": limits.bytes,
                            "global_tasks": limits.global_tasks,
                            "global_bytes": limits.global_bytes,
                            "archive_batch_tasks": self.limits.archive_batch
                            or planner_batch,
                            "refill_batch_tasks": planner_batch,
                            "resident_tasks": window.resident_count + admitted,
                            "execution_tasks": window.count + admitted,
                            "resident_bytes": int(window.bytes) + admitted_bytes,
                            "terminal_resident_tasks": window.terminal,
                            "terminal_global_tasks": window.global_terminal_count,
                            "terminal_global_bytes": int(window.terminal_bytes),
                            "terminal_task_limit": limits.terminal_tasks,
                            "terminal_byte_high_water": limits.terminal_bytes,
                            "global_used_tasks": total_count + admitted,
                            "global_used_bytes": total_bytes + admitted_bytes,
                        },
                    }
                )
            return run_id if archived or admitted else None

    def _cancel_locked(self, session, run, now):
        from .storage import _task_to_central_row

        source = session.get(MPSPlanModel, run.run_id)
        plan = self.manifests._snapshot(source)
        while True:
            plan, archived = self._archive_finished(session, run, plan)
            if not archived:
                break
        # Running progress never contains final result graphs. Iterate the
        # bounded input window without collecting all payloads in a list.
        ids = (
            session.execute(select(Work.task_id).where(Work.run_id == run.run_id))
            .scalars()
            .all()
        )
        for task_id in ids:
            work = session.get(Work, (run.run_id, task_id))
            task = Task.from_dict(json.loads(work.payload_json))
            task.state = TaskState.CANCELLED
            task.result = TaskResult(success=False, message="Job cancelled")
            task.completed_at = now.isoformat()
            session.execute(
                insert(CentralTaskModel)
                .values(
                    **{
                        **_task_to_central_row(task, run.run_id),
                        "definition_json": _dump(task.to_dict(include_result=False)),
                    }
                )
                .on_conflict_do_nothing()
            )
            session.expunge(work)
        session.execute(
            update(Attempt)
            .where(Attempt.run_id == run.run_id, Attempt.state == "running")
            .values(state="cancelled", completed_at=now, error="Job cancelled")
        )
        session.execute(delete(Resource).where(Resource.run_id == run.run_id))
        session.execute(delete(Work).where(Work.run_id == run.run_id))
        self.manifests.terminate(run.run_id, status="cancelled", db_session=session)
        run.state, run.completed_at = "cancelled", now

    def control(
        self,
        run_id: str,
        action: str,
        *,
        name: str | None = None,
        priority: int | None = None,
        schedule: dict | None = None,
    ) -> dict | None:
        with self.session_factory() as session:
            run = self._locked_run(session, run_id)
            if run is None:
                return None
            if json.loads(run.config_json).get("metadata", {}).get("deletion_pending"):
                raise ValueError("Job deletion is in progress; retry deletion to finish cleanup")
            if run.state in TERMINAL:
                # Every action records an event, and history retirement may
                # have dropped this finished run's outbox leaf.
                from ..database.job_partitions import restore_retired_history

                restore_retired_history(session, run_id)
            now = _now(session)
            source = session.get(MPSPlanModel, run_id)
            config = json.loads(source.job_config_json)
            if action == "cancel":
                if run.state not in TERMINAL:
                    self._cancel_locked(session, run, now)
            elif action in {"pause", "resume"}:
                if run.state in TERMINAL:
                    raise ValueError("A terminal run cannot be paused or resumed")
                run.state = "paused" if action == "pause" else "pending"
            elif action == "update":
                if name is not None:
                    if not name.strip():
                        raise ValueError("Job name cannot be empty")
                    config["name"] = name.strip()
                if priority is not None:
                    run.priority = priority
                    config.setdefault("metadata", {})["priority"] = priority
                if schedule is not None:
                    if run.started_at is not None:
                        raise ValueError(
                            "A started run cannot be rescheduled; create a retry run"
                        )
                    config["schedule"] = schedule
                plan = self.manifests._snapshot(source)
                plan["job_config"] = config
                config = revise(session, run, plan)
            else:
                raise ValueError(f"Unknown execution control {action}")
            config.update(state=run.state, updated_at=now.isoformat())
            if run.completed_at:
                config["completed_at"] = run.completed_at.isoformat()
            run.config_json = source.job_config_json = _dump(config)
            central = session.get(CentralJobModel, run_id)
            central.status, central.job_config_json = run.state, _dump(config)
            central.completed_at = run.completed_at
            central.priority = run.priority
            central.last_event_at = now
            args = json.loads(central.args_json or "{}")
            args.update(job_name=config["name"], schedule=config.get("schedule"))
            central.args_json = _dump(args)
            _event(
                session,
                run_id,
                {
                    "cancel": "job_cancelled",
                    "pause": "job_paused",
                    "resume": "job_resumed",
                }.get(action, "job_updated"),
                status=run.state,
            )
            session.commit()
        return self.job(run_id)

    def job(self, run_id: str) -> dict | None:
        return self.jobs_many([run_id]).get(run_id)

    def jobs_many(self, run_ids, *, db_session=None, statistics=None) -> dict[str, dict]:
        """Hydrate at most 256 headers without one checkout per job.

        A caller can share its request-scoped transaction and statistics batch.
        No connection is retained beyond that unit of work, including NullPool.
        """
        run_ids = list(dict.fromkeys(run_ids))
        if not run_ids:
            return {}
        if len(run_ids) > 256:
            raise ValueError("At most 256 jobs can be hydrated per header batch")
        with (
            self.session_factory() if db_session is None else nullcontext(db_session)
        ) as session:
            statistics = (
                self.stats_many(run_ids, db_session=session)
                if statistics is None else statistics
            )
            rows = session.execute(
                select(Run, MPSPlanModel)
                .join(MPSPlanModel, MPSPlanModel.job_id == Run.run_id)
                .where(Run.run_id.in_(run_ids))
            ).all()
            result = {}
            for run, source in rows:
                stats = statistics.get(run.run_id)
                if stats is None:
                    continue
                config = json.loads(source.job_config_json)
                config.update(
                    state=stats["state"],
                    started_at=run.started_at.isoformat() if run.started_at else None,
                    completed_at=run.completed_at.isoformat() if run.completed_at else None,
                )
                config.setdefault("metadata", {}).update(
                    total_tasks_count_override=stats["total"],
                    completed_tasks_count_override=stats["completed"],
                    failed_tasks_count_override=stats["failed"],
                    mps_archived_completed=source.completed,
                    mps_archived_failed=source.failed,
                )
                result[run.run_id] = config
            return result

    def task(self, run_id: str, task_id: str) -> dict | None:
        with self.session_factory() as session:
            return self._task_payloads(session, run_id, [task_id])[task_id]

    def _task_payloads(
        self, session, run_id: str, task_ids: Sequence[str]
    ) -> dict[str, dict | None]:
        """Hydrate tasks of one run with a fixed number of set-based reads."""
        payloads: dict[str, dict | None] = dict.fromkeys(task_ids)
        if not task_ids:
            return payloads
        for row in session.execute(
            select(Work.task_id, Work.payload_json, Work.state, Work.owner, Work.attempt)
            .where(Work.run_id == run_id, Work.task_id.in_(task_ids))
        ):
            payloads[row.task_id] = _work_payload(
                row.payload_json, row.state, row.owner, row.attempt
            )
        rest = [task_id for task_id in task_ids if payloads[task_id] is None]
        if not rest:
            return payloads
        # job_id is the partition key; task_id alone probes every job leaf.
        history = {
            row.task_id: row
            for row in session.execute(
                select(CentralTaskModel)
                .options(undefer(CentralTaskModel.definition_json))
                .where(
                    CentralTaskModel.job_id == run_id,
                    CentralTaskModel.task_id.in_(rest),
                )
            ).scalars()
        }
        run = session.get(Run, run_id)
        if run is None:
            return payloads
        # SQL history keeps the fully typed task result after window purge.
        undefined = [
            task_id
            for task_id in rest
            if not (task_id in history and history[task_id].definition_json)
        ]
        definitions: dict[str, str] = {}
        if undefined:
            source = session.get(MPSPlanModel, run_id)
            selected = json.loads(source.selection_json)
            manifest = selected.get("task_manifest_id") or selected.get(
                "spawned_manifest_id"
            )
            from ultima_scraper_db_archive.global_db import MPSTaskDefinitionModel

            definitions = dict(
                session.execute(
                    select(
                        MPSTaskDefinitionModel.task_id, MPSTaskDefinitionModel.task_json
                    ).where(
                        MPSTaskDefinitionModel.manifest_id == manifest,
                        MPSTaskDefinitionModel.task_id.in_(undefined),
                    )
                ).all()
            )
        workers = (
            dict(
                session.execute(
                    select(Attempt.task_id, Attempt.owner)
                    .where(Attempt.run_id == run_id, Attempt.task_id.in_(list(history)))
                    .order_by(Attempt.task_id, Attempt.number.desc())
                    .distinct(Attempt.task_id)
                ).all()
            )
            if history
            else {}
        )
        for task_id in rest:
            row = history.get(task_id)
            if row is not None and row.definition_json:
                payload = json.loads(row.definition_json)
            elif task_id in definitions and definitions[task_id]:
                payload = json.loads(definitions[task_id])
            elif row is not None:
                payload = {
                    "task_id": task_id,
                    "job_id": run_id,
                    "user_id": row.user_id,
                    "task_type": row.task_type,
                    "site_name": row.site,
                }
            else:
                continue
            if row is not None:
                payload.update(
                    state=row.status,
                    result=json.loads(row.result_json) if row.result_json else None,
                    worker_id=workers.get(task_id),
                    started_at=row.started_at.isoformat() if row.started_at else None,
                    completed_at=row.completed_at.isoformat()
                    if row.completed_at
                    else None,
                )
            elif run.state == "cancelled":
                payload.update(state="cancelled", result=None)
            payloads[task_id] = payload
        return payloads

    def output(self, run_id: str, task_id: str):
        payload = self.task(run_id, task_id)
        return (
            (payload or {}).get("result", {}).get("output")
            if (payload or {}).get("result")
            else None
        )

    def stats(self, run_id: str, *, db_session=None) -> dict | None:
        return self.stats_many([run_id], db_session=db_session).get(run_id)

    def stats_many(self, run_ids, *, db_session=None) -> dict[str, dict]:
        """Read a bounded group of diagnostic counters on one connection."""
        run_ids = list(dict.fromkeys(run_ids))
        if not run_ids:
            return {}
        if len(run_ids) > 256:
            raise ValueError("At most 256 runs can be sampled per statistics batch")
        with (
            self.session_factory() if db_session is None else nullcontext(db_session)
        ) as session:
            counts = (
                select(
                    Work.run_id,
                    *[
                        func.count().filter(Work.state == state).label(state)
                        for state in (
                            "queued",
                            "running",
                            "persisting",
                            "completed",
                            "failed",
                        )
                    ],
                )
                .where(Work.run_id.in_(run_ids))
                .group_by(Work.run_id)
                .subquery()
            )
            rows = session.execute(
                select(
                    Run.run_id,
                    MPSPlanModel.job_config_json,
                    MPSPlanModel.total,
                    MPSPlanModel.completed,
                    MPSPlanModel.failed,
                    Run.state,
                    counts.c.queued,
                    counts.c.running,
                    counts.c.persisting,
                    counts.c.completed,
                    counts.c.failed,
                )
                .select_from(MPSPlanModel)
                .join(Run, Run.run_id == MPSPlanModel.job_id)
                .outerjoin(counts, counts.c.run_id == Run.run_id)
                .where(Run.run_id.in_(run_ids))
            ).all()
            result = {}
            for row in rows:
                (
                    run_id,
                    job_config_json,
                    total,
                    completed,
                    failed,
                    state,
                    queued,
                    running,
                    persisting,
                    live_completed,
                    live_failed,
                ) = row
                completed, failed = (
                    completed + (live_completed or 0),
                    failed + (live_failed or 0),
                )
                inflight = (running or 0) + (persisting or 0)
                result[run_id] = {
                    "total": total,
                    "completed": completed,
                    "failed": failed,
                    "ready": queued or 0,
                    "inflight": inflight,
                    "blocked": max(0, total - completed - failed - inflight - (queued or 0))
                    if state not in TERMINAL
                    else 0,
                    "deferred": 0,
                    "cancelled": max(0, total - completed - failed)
                    if state == "cancelled"
                    else 0,
                    "task_type_counts": json.loads(job_config_json)
                    .get("metadata", {})
                    .get("mps_task_type_counts", {}),
                    "execution_engine": ENGINE,
                    "state": state,
                }

            return result

    def retry(self, run_id: str, *, auth_id: int | None = None) -> dict | None:
        """Freeze a retry from SQL evidence and revoke its source atomically."""
        with self.session_factory() as session:
            run = self._locked_run(session, run_id)
            if run is None:
                return None
            if json.loads(run.config_json).get("metadata", {}).get("deletion_pending"):
                raise ValueError("Job deletion is in progress; retry deletion to finish cleanup")
            source = session.get(MPSPlanModel, run_id)
            if auth_id is not None:
                from ultima_scraper_db_archive import UserAuthModel

                if isinstance(auth_id, bool) or auth_id <= 0:
                    raise ValueError("Retry auth_id must be a positive integer")
                session.execute(
                    text("SELECT set_config('search_path', :path, true)"),
                    {"path": f"{source.site},management,public"},
                )
                if (
                    session.scalar(
                        select(UserAuthModel.user_id)
                        .where(
                            UserAuthModel.user_id == auth_id,
                            UserAuthModel.active.is_(True),
                        )
                        .limit(1)
                    )
                    is None
                ):
                    raise ValueError(f"Auth {auth_id} is not active for {source.site}")
            plan = self.manifests._snapshot(source)
            while True:
                plan, archived = self._archive_finished(session, run, plan)
                if not archived:
                    break
            original = json.loads(run.config_json)
            name = original["name"]
            while name.startswith("Retry: "):
                name = name.removeprefix("Retry: ").lstrip()
            retry = Job.create(
                name=f"Retry: {name}",
                created_by=original["created_by"],
                tasks=[],
                metadata={
                    "mps_manifest": True,
                    "retry_of": run_id,
                    "retry_auth_id": auth_id,
                    "dispatch_backend_id": run.backend_id,
                    "priority": run.priority,
                    "site": source.site,
                    "durable_job_kind": original.get("metadata", {}).get(
                        "durable_job_kind", "mps"
                    ),
                },
            )
            prepared = self.manifests.clone_for_retry(
                source_job_id=run_id,
                job_id=retry.job_id,
                job_config=retry.to_dict(),
                site=source.site,
                live_failed_user_ids=[],
                live_failed_tasks=[],
                expected_manifest_id=plan["selection"].get("task_manifest_id"),
                auth_id=auth_id,
                db_session=session,
                incremental=True,
            )
            prepared = admit(session, prepared)
            if run.state not in TERMINAL:
                self._cancel_locked(session, run, _now(session))
                config = json.loads(run.config_json)
                config.update(
                    state=run.state, completed_at=run.completed_at.isoformat()
                )
                source.job_config_json = run.config_json = _dump(config)
                _event(
                    session,
                    run_id,
                    "job_cancelled",
                    status="cancelled",
                    retry_of=retry.job_id,
                )
            session.commit()
        return self.job(retry.job_id)

    def begin_delete(self, run_id: str, batch_size: int = 25000, progress=None) -> bool:
        """Fence writers first, then purge history with bounded transactions.

        A cancelled run stays durable until cleanup finishes, so interrupted
        deletion can be retried without leaving executable work behind.
        """

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        with self.session_factory() as session:
            run = self._locked_run(session, run_id)
            if run is None:
                return False
            if run.state not in TERMINAL:
                self._cancel_locked(session, run, _now(session))
            config = json.loads(run.config_json)
            metadata = config.setdefault("metadata", {})
            metadata["deletion_pending"] = True
            # The background worker finishes deletions in request order.
            metadata.setdefault("deletion_requested_at", _now(session).isoformat())
            config["state"] = run.state
            config["completed_at"] = run.completed_at.isoformat() if run.completed_at else None
            run.config_json = _dump(config)
            source = session.get(MPSPlanModel, run_id)
            if source is not None:
                source.job_config_json = run.config_json
            session.execute(update(CentralJobModel).where(CentralJobModel.job_id == run_id).values(
                status="deleting", completed_at=run.completed_at,
                job_config_json=run.config_json, last_event_at=_now(session),
            ))
            state = run.state
            session.commit()
        if progress and state == "cancelled":
            progress("cancelled", 0)

        return True

    def delete(self, run_id: str, batch_size: int = 25000, progress=None, *, time_budget: float | None = None, receipt_queue=None) -> bool | None:
        """Dispose of a fenced run using a complete accepted-writer snapshot.

        None means inventory is unavailable and cleanup must wait; an empty
        ReceiptQueueSnapshot explicitly confirms no queued or in-flight writes.
        """
        from ultima_scraper_db_archive.execution_models import ExecutionMutationModel
        from ultima_scraper_db_archive.global_db import TaskMediaPlanModel, TaskMediaSelectionModel, TaskMediaEvidenceModel
        if not self.begin_delete(run_id, batch_size, progress):
            return False
        started = time.monotonic()
        with self.session_factory() as session:
            saved = json.loads(session.get(Run, run_id).config_json).get("metadata", {}).get("deletion_progress", {})
        removed = int(saved.get("rows", 0))
        partitions_dropped = int(saved.get("partitions_dropped", 0))
        if receipt_queue is None or run_id in receipt_queue.run_ids:
            # Logical retries need mutation-to-receipt mappings too. Drain
            # accepted writes before disposing of either kind of recovery row.
            self._save_deletion_progress(run_id, {
                **saved, "rows": removed, "partitions_dropped": partitions_dropped,
                "table": saved.get("table", "database_write_receipts"),
                "completed_stages": int(saved.get("completed_stages", 0)), "total_stages": 10,
                "error": ("Waiting for a complete database writer queue inventory."
                          if receipt_queue is None else
                          "Waiting for this job's queued database writes to drain."),
            })
            if progress:
                progress("database_write_receipts", removed)
            return None
        for stage, (model, owner_column) in enumerate((
            (Outbox, Outbox.run_id),
            (ExecutionMutationModel, ExecutionMutationModel.run_id),
            (Attempt, Attempt.run_id),
            (Resource, Resource.run_id),
            (Work, Work.run_id),
            (CentralTaskModel, CentralTaskModel.job_id),
            (TaskMediaSelectionModel, TaskMediaSelectionModel.selection_id),
            (TaskMediaEvidenceModel, TaskMediaEvidenceModel.selection_id),
            (TaskMediaPlanModel, TaskMediaPlanModel.job_id),
        )):
            if stage < int(saved.get("completed_stages", 0)):
                continue
            identity_cursor = saved.get("identity_cursor") if saved.get("table") == model.__tablename__ else None
            identity_sweep_cursor = saved.get("identity_sweep_cursor") if saved.get("table") == model.__tablename__ else None
            identity_sweep = identity_sweep_cursor is not None
            if not identity_sweep:
                from ..database.job_partitions import identity_cleanup_uses_sweep
                with self.session_factory() as session:
                    identity_sweep = identity_cleanup_uses_sweep(session, model.__tablename__, run_id)
                if identity_sweep:
                    # An old key cursor can resume safely as a physical pass;
                    # rows already deleted simply leave empty ranges to advance.
                    identity_sweep_cursor = "(0,0)"
            if model.__tablename__ in {"execution_mutations", "execution_attempts", "execution_outbox", "job_tasks"}:
                from ..database.job_partitions import drop_history_partition
                try:
                    with self.session_factory() as session:
                        if self._locked_run(session, run_id) is None:
                            return True
                        if drop_history_partition(session, run_id, model.__tablename__):
                            self._save_deletion_progress(run_id, {"rows": removed, "table": model.__tablename__,
                                "completed_stages": stage, "total_stages": 10, "partitions_dropped": partitions_dropped + 1}, db_session=session)
                            session.commit()
                            partitions_dropped += 1
                            if progress:
                                progress(model.__tablename__, removed)
                except DBAPIError as exc:
                    if getattr(exc.orig, "sqlstate", None) not in {"55P03", "40P01", "40001"}:
                        raise
                    # Roll back the failed DDL transaction before checkpointing.
                    # Leave the stage pending; the background worker retries it
                    # on its next cycle rather than reporting a failed deletion.
                    self._save_deletion_progress(run_id, {
                        "rows": removed, "table": model.__tablename__,
                        "completed_stages": stage, "total_stages": 10,
                        "partitions_dropped": partitions_dropped,
                        "error": f"Waiting for database lock on {model.__tablename__}; retrying automatically.",
                    })
                    if progress:
                        progress(model.__tablename__, removed)
                    return None
            keys = list(model.__table__.primary_key.columns)
            locations = [literal_column(f"{model.__tablename__}.ctid").label("row_location")]
            if model.__tablename__ in {"execution_mutations", "execution_attempts", "execution_outbox", "job_tasks"}:
                locations.insert(0, literal_column(f"{model.__tablename__}.tableoid").label("relation_id"))
            predicate = (owner_column.in_(select(TaskMediaPlanModel.selection_id).where(
                TaskMediaPlanModel.job_id == run_id
            )) if model in {TaskMediaSelectionModel, TaskMediaEvidenceModel}
                         else owner_column == run_id)
            transient_failures = 0
            while True:
                try:
                    with self.session_factory() as session:
                        if self._locked_run(session, run_id) is None:
                            return True
                        # Lock in primary-key order before deleting. Relays and
                        # overlapping purges may own rows; never wait on those
                        # rows while holding a different history-row lock.
                        batch = (
                            select(*locations).select_from(model).where(predicate)
                            .order_by(*keys).limit(batch_size)
                            .with_for_update(skip_locked=True)
                            .cte("deletion_batch").prefix_with("MATERIALIZED")
                        )
                        result = session.execute(
                            delete(model).where(predicate, (tuple_(*locations).in_(select(*batch.c)) if len(locations) > 1 else locations[0].in_(select(batch.c.row_location))))
                            .execution_options(synchronize_session=False)
                        )
                        count = result.rowcount
                        # A short batch may have skipped locked rows. Do not
                        # reach the final cascading delete until they are gone.
                        remaining = count < batch_size and session.scalar(
                            select(owner_column).where(predicate).limit(1)
                        ) is not None
                        stage_finished = count < batch_size and not remaining
                        next_identity_cursor = identity_cursor
                        next_identity_sweep_cursor = identity_sweep_cursor
                        if count < batch_size and not remaining:
                            from ..database.job_partitions import purge_identity_page, purge_identity_sweep_page
                            if identity_sweep:
                                identity_count, remaining, next_identity_sweep_cursor = purge_identity_sweep_page(
                                    session, model.__tablename__, run_id, batch_size, after=identity_sweep_cursor,
                                )
                                next_identity_cursor = None
                            else:
                                identity_count, remaining, next_identity_cursor = purge_identity_page(
                                    session, model.__tablename__, run_id, batch_size, after=identity_cursor,
                                )
                            count += identity_count
                            stage_finished = not remaining
                        self._save_deletion_progress(run_id, {"rows": removed + count, "table": model.__tablename__,
                            "completed_stages": stage + int(stage_finished), "total_stages": 10,
                            "partitions_dropped": partitions_dropped, "identity_cursor": next_identity_cursor,
                            "identity_sweep_cursor": next_identity_sweep_cursor}, db_session=session)
                        session.commit()
                    identity_cursor = next_identity_cursor
                    identity_sweep_cursor = next_identity_sweep_cursor
                    transient_failures = 0
                except DBAPIError as exc:
                    # The failed session closes/rolls back before retrying.
                    if getattr(exc.orig, "sqlstate", None) not in {"40P01", "40001"}:
                        raise
                    transient_failures += 1
                    if transient_failures >= 5:
                        raise
                    time.sleep(0.05 * (2 ** (transient_failures - 1)))
                    continue
                removed += count
                if time_budget is not None and time.monotonic() - started >= time_budget:
                    return None
                if progress:
                    progress(model.__tablename__, removed)
                if stage_finished:
                    break
                if not count and not identity_sweep:
                    time.sleep(0.05)

        # Append the receipt stage so existing nine-stage checkpoints retain
        # their meaning. Fencing and history disposal precede receipt removal.
        from ..database.job_receipts import (
            JobReceiptCleanupDeferred, plan_receipt_sweep, purge_job_receipts, purge_job_receipts_sweep,
        )
        from ..database.write_receipts import DatabaseWriteReceiptBusy
        in_stage = saved.get("table") == "database_write_receipts"
        receipt_cursor = saved.get("receipt_cursor") if in_stage else None
        # None: not chosen yet; False: command-ordered pages; dict: physical sweep.
        receipt_sweep = saved.get("receipt_sweep") if in_stage else None
        while True:
            checkpoint = {"rows": removed, "table": "database_write_receipts",
                          "completed_stages": 9, "total_stages": 10,
                          "partitions_dropped": partitions_dropped, "receipt_cursor": receipt_cursor,
                          "receipt_sweep": receipt_sweep}
            try:
                with self.session_factory() as session:
                    if self._locked_run(session, run_id) is None:
                        return True
                    if receipt_sweep is None:
                        receipt_sweep = plan_receipt_sweep(session, run_id, after=receipt_cursor) or False
                    if receipt_sweep:
                        page = purge_job_receipts_sweep(session, run_id, sweep=receipt_sweep,
                                                        limit=min(batch_size, 512), queue=receipt_queue)
                    else:
                        page = purge_job_receipts(session, run_id, limit=min(batch_size, 512),
                                                  after=receipt_cursor, queue=receipt_queue)
                    self._save_deletion_progress(run_id, {
                        **checkpoint, "rows": removed + page.deleted,
                        "completed_stages": 9 + int(page.finished), "receipt_cursor": page.cursor,
                        "receipt_sweep": page.sweep or receipt_sweep,
                    }, db_session=session)
                    session.commit()
            except (JobReceiptCleanupDeferred, DatabaseWriteReceiptBusy) as exc:
                # The page session rolls back first, releasing any partial
                # writer locks. The committed cursor never passes that page.
                self._save_deletion_progress(run_id, {**checkpoint, "error": str(exc)})
                if progress:
                    progress("database_write_receipts", removed)
                return None
            removed += page.deleted
            receipt_cursor = page.cursor
            receipt_sweep = page.sweep or receipt_sweep
            if progress:
                progress("database_write_receipts", removed)
            if page.finished:
                break
            if time_budget is not None and time.monotonic() - started >= time_budget:
                return None
            if not page.deleted and not page.advanced:
                # SKIP LOCKED left rows for a subsequent worker pass.
                return None

        with self.session_factory() as session:
            run = self._locked_run(session, run_id)
            if run is None:
                return True  # Another deletion finished the same fenced run.
            definition_id = run.definition_id
            session.execute(delete(Run).where(Run.run_id == run_id))
            session.execute(delete(MPSPlanModel).where(MPSPlanModel.job_id == run_id))
            session.execute(delete(CentralJobModel).where(CentralJobModel.job_id == run_id))
            if session.scalar(select(Run.run_id).where(
                Run.definition_id == definition_id
            ).limit(1)) is None:
                session.execute(delete(Definition).where(Definition.definition_id == definition_id))
            session.commit()
        return True

    def _save_deletion_progress(self, run_id, value, *, db_session=None):
        value = {**value, "updated_at": datetime.now().isoformat()}
        with (nullcontext(db_session) if db_session is not None else self.session_factory()) as session:
            run = self._locked_run(session, run_id)
            if run is None:
                return
            config = json.loads(run.config_json)
            config.setdefault("metadata", {})["deletion_progress"] = value
            run.config_json = _dump(config)
            source = session.get(MPSPlanModel, run_id)
            if source is not None:
                source.job_config_json = run.config_json
            session.execute(update(CentralJobModel).where(CentralJobModel.job_id == run_id).values(job_config_json=run.config_json))
            if db_session is None:
                session.commit()

    def task_page(
        self,
        run_id: str,
        *,
        after: str = "",
        limit: int = 100,
        offset: int = 0,
        max_bytes: int | None = None,
    ):
        limit = max(1, min(limit, 500))
        budget = min(self.limits.result_bytes, max_bytes or self.limits.result_bytes)
        with self.session_factory() as session:
            from ultima_scraper_db_archive.global_db import MPSTaskDefinitionModel

            source = session.get(MPSPlanModel, run_id)
            if source is None:
                return {
                    "tasks": [],
                    "next_cursor": None,
                    "has_more": False,
                    "scanned_count": 0,
                }
            selection = json.loads(source.selection_json)
            manifest = selection.get("task_manifest_id") or selection.get(
                "spawned_manifest_id"
            )
            # Each source can contribute at most this many IDs to the merged
            # page. Limiting after UNION would scan/deduplicate the entire job
            # history before returning even its first page.
            offset = max(0, offset)
            page_size = offset + limit + 1
            sources = [
                (Work.task_id, Work.run_id == run_id),
                (CentralTaskModel.task_id, CentralTaskModel.job_id == run_id),
            ]
            if manifest is not None:
                sources.append((
                    MPSTaskDefinitionModel.task_id,
                    MPSTaskDefinitionModel.manifest_id == manifest,
                ))
            pages = []
            for task_id_column, predicate in sources:
                page = (
                    select(task_id_column.label("task_id"))
                    .where(predicate, task_id_column > after)
                    .order_by(task_id_column)
                    .limit(page_size)
                    .subquery()
                )
                pages.append(select(page.c.task_id))
            ids = pages[0].union(*pages[1:]).subquery()
            task_ids = list(
                session.execute(
                    select(ids.c.task_id)
                    .where(ids.c.task_id > after)
                    .order_by(ids.c.task_id)
                    .offset(offset)
                    .limit(limit + 1)
                ).scalars()
            )
            hydrated = self._task_payloads(session, run_id, task_ids[:limit])
        payloads, occupied = [], 0
        for task_id in task_ids[:limit]:
            task = hydrated[task_id]
            if task is None:
                continue
            size = len(_dump(task))
            if payloads and occupied + size > budget:
                break
            if size > budget:
                raise ValueError("One task exceeds the ledger page byte budget")
            occupied += size
            payloads.append(task)
        last = payloads[-1]["task_id"] if payloads else after
        return {
            "tasks": payloads,
            "next_cursor": "ledger:" + last if len(task_ids) > len(payloads) else None,
            "has_more": len(task_ids) > len(payloads),
            "scanned_count": len(payloads),
        }

    def attempts(
        self,
        run_id: str,
        *,
        after_task: str = "",
        after_attempt: int = 0,
        limit: int = 100,
    ):
        with self.session_factory() as session:
            rows = session.execute(
                select(Attempt)
                .where(
                    Attempt.run_id == run_id,
                    tuple_(Attempt.task_id, Attempt.number)
                    > tuple_(after_task, after_attempt),
                )
                .order_by(Attempt.task_id, Attempt.number)
                .limit(min(limit, 500))
            ).scalars()
            return [
                {
                    "task_id": row.task_id,
                    "attempt": row.number,
                    "worker_id": row.owner,
                    "state": row.state,
                    "started_at": row.started_at.isoformat(),
                    "completed_at": row.completed_at.isoformat()
                    if row.completed_at
                    else None,
                    "error": row.error,
                    "timings": json.loads(row.timings_json),
                }
                for row in rows
            ]

    def events(
        self,
        *,
        after: int = 0,
        run_id: str | None = None,
        limit: int = 100,
        include_api_commands: bool = False,
        through: int | None = None,
    ):
        with self.session_factory() as session:
            query = select(Outbox).where(Outbox.sequence > after)
            if through is not None:
                query = query.where(Outbox.sequence <= through)
            if run_id:
                query = query.where(Outbox.run_id == run_id)
            if not include_api_commands:
                query = query.where(
                    ~Outbox.run_id.startswith("api-"),
                    ~Outbox.run_id.startswith("test-"),
                )
            rows = session.execute(
                query.order_by(Outbox.sequence).limit(min(limit, 500))
            ).scalars()
            return [
                {
                    **json.loads(row.payload_json),
                    "sequence": row.sequence,
                    "event_id": row.event_id,
                }
                for row in rows
            ]

    def event_head(self) -> int:
        with self.session_factory() as session:
            return (
                session.scalar(select(Feed.next_sequence).where(Feed.feed_id == 1)) - 1
            )

    def active(self, *, limit: int = 250):
        with self.session_factory() as session:
            rows = session.execute(
                select(
                    Work.run_id,
                    Work.payload_json,
                    Work.state,
                    Work.owner,
                    Work.attempt,
                    Run.config_json,
                    Run.state,
                    MPSPlanModel.total,
                )
                .join(Run, Run.run_id == Work.run_id)
                .join(MPSPlanModel, MPSPlanModel.job_id == Run.run_id)
                .where(
                    Work.state.in_(["queued", "running", "persisting"]),
                    Run.state.in_(["pending", "running", "paused"]),
                    Run.next_at <= func.clock_timestamp(),
                    Work.lane != "interactive",
                    ~Run.run_id.startswith("api-"),
                    ~Run.run_id.startswith("test-"),
                )
                .order_by(Work.state == "queued", Work.created_at, Work.task_id)
                .limit(min(limit, 250))
            ).all()
        result, occupied = [], 0
        # The work row is the live task; hydrate it from this snapshot rather
        # than issuing one session and primary-key read per listed task.
        for run_id, payload_json, work_state, owner, attempt, config, state, total in rows:
            payload = _work_payload(payload_json, work_state, owner, attempt)
            size = len(_dump(payload))
            if result and occupied + size > self.limits.result_bytes:
                break
            occupied += size
            result.append(
                {
                    "job_id": run_id,
                    "job_name": json.loads(config)["name"],
                    "job_status": state,
                    "job_total_tasks": total,
                    "task": payload,
                }
            )
        return result

    def active_run_ids(self, *, limit: int = 1000) -> list[str]:
        """Find diagnostic runs without hydrating any task payloads."""
        with self.session_factory() as session:
            return list(
                session.scalars(
                    select(Run.run_id)
                    .where(
                        Run.state.in_(["pending", "running", "paused"]),
                        Run.next_at <= func.clock_timestamp(),
                        ~Run.run_id.startswith("api-"),
                        ~Run.run_id.startswith("test-"),
                    )
                    .order_by(Run.run_id)
                    .limit(min(limit, 1000))
                )
            )

    def relay(
        self,
        publish: Callable[[dict], None] | None = None,
        *,
        publish_many: Callable[[list[dict]], None] | None = None,
        outcome=None,
    ) -> int:
        """Publish committed events at least once, with a replayable commit order.

        Only relays serialize on the feed row. The callback must finish before
        marking delivery; on failure the complete batch remains replayable.
        Consumers deduplicate by event_id. No worker completion takes this lock.
        """
        if outcome is not None:
            outcome[:] = ["deferred"]
        with self.session_factory() as session:
            feed = session.execute(
                select(Feed).where(Feed.feed_id == 1).with_for_update(skip_locked=True)
            ).scalar_one_or_none()
            if feed is None:
                return 0
            skipped_ids = []
            if session.get_bind().dialect.name == "postgresql":
                skipped_ids = list(session.scalars(_SKIP_LEGACY_TASK_EVENTS, {
                    "prefix": LEGACY_TASK_EVENT_PREFIX, "limit": LEGACY_SKIP_BATCH,
                }))
            skipped = len(skipped_ids)
            pending = Outbox.delivered == false()
            if skipped == LEGACY_SKIP_BATCH:
                # More legacy rows follow this page. Deliver only events it has
                # passed, so none of them is published late, then continue.
                pending = pending & (Outbox.event_id < max(skipped_ids))
            rows = (
                session.execute(
                    select(Outbox)
                    # Match the existing partial index predicate. IS FALSE
                    # cannot use PostgreSQL's WHERE delivered = false index.
                    .where(pending)
                    .order_by(Outbox.event_id)
                    .limit(self.limits.batch)
                    # Deletion locks history before purging it. Claim rows
                    # before publishing so delivery cannot later wait on a
                    # purge holding another row from this relay batch.
                    .with_for_update(skip_locked=True)
                )
                .scalars()
                .all()
            )
            payloads = []
            for row in rows:
                row.sequence = feed.next_sequence
                feed.next_sequence += 1
                payloads.append(
                    {
                        **json.loads(row.payload_json),
                        "sequence": row.sequence,
                        "event_id": row.event_id,
                    }
                )
            if payloads:
                if publish_many is not None:
                    publish_many(payloads)
                elif publish is not None:
                    for payload in payloads:
                        publish(payload)
                else:
                    raise ValueError("An event publisher is required")
            for row in rows:
                row.delivered = True
            if outcome is not None:
                if skipped == LEGACY_SKIP_BATCH:
                    outcome[:] = ["backlog"]
                elif rows or skipped:
                    outcome[:] = ["progress"]
                elif not session.scalar(select(select(Outbox.event_id).where(Outbox.delivered == false()).exists())):
                    outcome[:] = ["idle"]
            session.commit()
            return len(rows)

    def record_progress(self, *, previous=None, limit: int = 256) -> dict[str, tuple]:
        """Append a durable ``job_progress`` event for each running run that moved.

        Task events are live-only. Durable consumers, such as remote job
        adapters reading the plugin feed, follow progress through these bounded
        summaries. Pass the returned counts back to skip unchanged runs.
        """
        previous = previous or {}
        current = {}
        with self.session_factory() as session:
            run_ids = list(session.scalars(
                select(Run.run_id).where(
                    Run.state == "running",
                    ~Run.run_id.startswith("api-"),
                    ~Run.run_id.startswith("test-"),
                ).order_by(Run.run_id).limit(limit)
            ))
            for run_id, stats in self.stats_many(run_ids, db_session=session).items():
                counts = (stats["completed"], stats["failed"], stats["total"])
                current[run_id] = counts
                if previous.get(run_id) == counts:
                    continue
                _event(
                    session, run_id, "job_progress",
                    completed_tasks=stats["completed"], failed_tasks=stats["failed"],
                    total_tasks=stats["total"], inflight_tasks=stats["inflight"],
                    ready_tasks=stats["ready"], status=stats["state"],
                )
            session.commit()
        return current
