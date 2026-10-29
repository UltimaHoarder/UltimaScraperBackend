"""Bounded lifecycle transactions; one stale attempt cannot poison its peers."""

from sqlalchemy import select, text, tuple_

from . import run_turnstile
from .ledger import Attempt, StaleAttempt, _attempt_timings, _dump, _task_event
from .models import TaskResult, TaskState
from .profile_completion import PROFILE_COMPLETION_KEY


def checkpoint_request(ledger, claim, payload, *, persisting=False, renew=False):
    encoded = _dump(payload)
    if len(encoded) > ledger.limits.result_bytes:
        raise ValueError("Task progress exceeds the ledger payload budget")
    return {
        **{key: getattr(claim, key) for key in ("run_id", "task_id", "token", "owner")},
        "payload": encoded,
        "checkpoint": True,
        "persisting": persisting,
        "renew": renew,
    }


def completion_request(ledger, claim, task, *, retry=False, timings=None):
    if task.result and any(
        len(_dump(child.to_dict()))
        > min(ledger.limits.bytes, ledger.limits.global_bytes)
        for child in task.result.new_tasks
    ):
        task.state = TaskState.FAILED
        task.result = TaskResult(
            success=False,
            message="A spawned task exceeds the execution input byte budget",
        )
        task.completed_at, retry = task.result.completed_at, False
    payload = task.to_dict()
    encoded = _dump(payload)
    if len(encoded) > ledger.limits.result_bytes:
        task.state = TaskState.FAILED
        task.result = TaskResult(
            success=False,
            message="Task result exceeds the execution result byte budget",
        )
        task.completed_at, task.subtasks, retry = task.result.completed_at, [], False
        payload = task.to_dict()
        encoded = _dump(payload)
        if len(encoded) > ledger.limits.result_bytes:
            raise ValueError("Task input exceeds the result page byte budget")
    success = (
        task.state == TaskState.COMPLETED
        and task.result is not None
        and task.result.success
    )
    error = None if success else (task.result.message if task.result else "Task failed")
    final_state = "completed" if success else "failed"
    if payload.get("state") != final_state:
        payload["state"] = final_state
        encoded = _dump(payload)
    retry_payload = None
    if retry and not success:
        retry_payload = _dump(
            {
                **payload,
                **({PROFILE_COMPLETION_KEY: claim.task[PROFILE_COMPLETION_KEY]}
                   if PROFILE_COMPLETION_KEY in claim.task else {}),
                "state": "pending",
                "result": None,
                "completed_at": None,
                "worker_id": None,
            }
        )
    if retry_payload is not None and len(retry_payload) > ledger.limits.result_bytes:
        raise ValueError("Retry checkpoint exceeds its payload byte budget")
    return {
        **{
            key: getattr(claim, key)
            for key in ("run_id", "task_id", "token", "owner", "number")
        },
        "success": success,
        "retry": retry,
        "max_attempts": task.max_attempts,
        "payload": encoded,
        "retry_payload": retry_payload,
        "error": error,
        "timings": _attempt_timings(timings),
        "message": error or (task.result.message if task.result else ""),
    }


_FENCE = """
WITH requested AS MATERIALIZED (
    SELECT * FROM json_to_recordset(CAST(:requests AS json)) AS r(
        ordinal integer, run_id text, task_id text, token text, owner text,
        number integer, payload text, persisting boolean, renew boolean, checkpoint boolean,
        success boolean, retry boolean, max_attempts integer,
        retry_payload text, error text, timings text, message text)
), run_fence AS MATERIALIZED (
    SELECT run_id FROM management.execution_runs
    WHERE run_id IN (SELECT run_id FROM requested)
      AND state NOT IN ('completed','failed','cancelled')
    ORDER BY run_id FOR SHARE
), work_fence AS MATERIALIZED (
    SELECT r.*, w.attempt, w.dedup_key, w.lease_until
    FROM requested r JOIN run_fence j ON j.run_id=r.run_id
    JOIN management.execution_work w ON w.run_id=r.run_id AND w.task_id=r.task_id
    WHERE w.state IN ('running','persisting') AND w.token=r.token AND w.owner=r.owner
    ORDER BY w.run_id,w.task_id FOR UPDATE OF w
), after_locks AS MATERIALIZED (
    SELECT clock_timestamp() AS now FROM (SELECT count(*) FROM work_fence) barrier
), owned AS MATERIALIZED (
    SELECT f.*, c.now FROM work_fence f CROSS JOIN after_locks c
    WHERE f.lease_until>c.now
)
"""

_CHECKPOINT = text(
    _FENCE
    + """, updated_work AS (
    UPDATE management.execution_work w SET payload_size=CASE WHEN f.checkpoint THEN octet_length(f.payload) ELSE w.payload_size END,
      state=CASE WHEN f.persisting THEN 'persisting' ELSE w.state END,
      lease_until=CASE WHEN f.renew THEN f.now+make_interval(secs=>:lease_seconds) ELSE w.lease_until END
    FROM owned f WHERE w.run_id=f.run_id AND w.task_id=f.task_id
    RETURNING f.ordinal,f.token,f.renew,f.checkpoint,f.payload,w.run_id,w.task_id,w.dedup_key,w.lease_until
), updated_payload AS (
    UPDATE management.execution_work_payloads p SET payload_json=w.payload
    FROM updated_work w WHERE w.checkpoint AND p.run_id=w.run_id AND p.task_id=w.task_id
    RETURNING p.task_id
), renewed_resource AS (
    UPDATE management.execution_resources r SET lease_until=w.lease_until
    FROM updated_work w WHERE w.renew AND r.resource_key=w.dedup_key AND r.token=w.token
    RETURNING r.resource_key
)
SELECT ordinal FROM updated_work
"""
)

_FINISH = text(
    _FENCE
    + """, finished_work AS (
    UPDATE management.execution_work w SET owner=NULL,token=NULL,lease_until=NULL,
      state=CASE WHEN f.retry AND NOT f.success AND f.attempt<f.max_attempts
        THEN 'queued' WHEN f.success THEN 'completed' ELSE 'failed' END,
      available_at=CASE WHEN f.retry AND NOT f.success AND f.attempt<f.max_attempts
        THEN f.now+make_interval(secs=>LEAST(60,power(2,LEAST(6,f.attempt)))) ELSE w.available_at END,
      payload_size=octet_length(CASE WHEN f.retry AND NOT f.success AND f.attempt<f.max_attempts
        THEN f.retry_payload ELSE f.payload END)
    FROM owned f WHERE w.run_id=f.run_id AND w.task_id=f.task_id
    RETURNING w.run_id,w.task_id,w.state,w.available_at,w.dedup_key,
      f.ordinal,f.token,f.number,f.success,f.error,f.timings,f.message,f.now,
      CASE WHEN f.retry AND NOT f.success AND f.attempt<f.max_attempts
        THEN f.retry_payload ELSE f.payload END AS final_payload
), finished_payload AS (
    UPDATE management.execution_work_payloads p SET payload_json=w.final_payload
    FROM finished_work w WHERE p.run_id=w.run_id AND p.task_id=w.task_id
    RETURNING p.task_id
), finished_attempt AS (
    UPDATE management.execution_attempts a SET
      state=CASE WHEN w.success THEN 'completed' ELSE 'failed' END,
      completed_at=w.now,error=w.error,timings_json=w.timings
    -- Prune history partitions at planning time, before the CTE runs.
    FROM finished_work w WHERE a.run_id=ANY(CAST(:history_run_ids AS text[]))
      AND a.run_id=w.run_id AND a.task_id=w.task_id AND a.number=w.number
    RETURNING a.run_id,a.task_id
), released_resource AS (
    DELETE FROM management.execution_resources r USING finished_work w
    WHERE r.resource_key=w.dedup_key AND r.token=w.token RETURNING r.resource_key
)
SELECT w.* FROM finished_work w JOIN finished_attempt a
  ON a.run_id=w.run_id AND a.task_id=w.task_id
"""
)
# Every row of a run's attempt partition shares its run_id. A merge join on
# run_id looks cheap for a one-row CTE estimate, but it reads the run's whole
# partition for each batch, so finish cost grew with run progress (~860k index
# tuples per batch at 50% of a 1.4M-task MPS run). Keep primary-key probes.
# LOCAL settings end with this transaction, including through PgBouncer.
_FINISH_PLANNER = text("SET LOCAL enable_mergejoin=off")


def _encoded_requests(ledger, requests):
    if not requests or len(requests) > ledger.limits.batch:
        raise ValueError("Lifecycle batch exceeds its task budget")
    if len({(r["run_id"], r["task_id"]) for r in requests}) != len(requests):
        raise ValueError("Lifecycle batch contains duplicate tasks")
    # Budget actual JSON transport bytes, including escaping and retry payloads.
    rows = [{**request, "ordinal": i} for i, request in enumerate(requests)]
    if any(len(r["payload"] or "") > ledger.limits.result_bytes for r in rows):
        raise ValueError("Lifecycle request exceeds its payload byte budget")
    encoded = _dump(rows)
    if len(requests) > 1 and len(encoded) > ledger.limits.result_bytes:
        raise ValueError("Lifecycle batch exceeds its byte budget")
    return rows, encoded


def _requests(ledger, requests):
    # Preserve the private rows-only helper for existing callers.
    return _encoded_requests(ledger, requests)[0]


def checkpoint_batch(ledger, requests):
    rows, encoded = _encoded_requests(ledger, requests)
    with ledger.session_factory() as session:
        run_turnstile.lock_shared(session, (r["run_id"] for r in rows))
        changed = set(
            session.execute(
                _CHECKPOINT,
                {
                    "requests": encoded,
                    "lease_seconds": ledger.limits.lease_seconds,
                },
            ).scalars()
        )
        session.commit()
    return [
        None if i in changed else StaleAttempt("Attempt no longer owns checkpoint")
        for i in range(len(rows))
    ]


def _replayed(session, requests):
    keys = [(r["run_id"], r["task_id"], r["number"]) for r in requests]
    if not keys:
        return set()
    rows = session.execute(
        select(Attempt.run_id, Attempt.task_id, Attempt.number, Attempt.token).where(
            tuple_(Attempt.run_id, Attempt.task_id, Attempt.number).in_(keys),
            Attempt.state.in_(["completed", "failed"]),
        )
    ).all()
    committed = {(r.run_id, r.task_id, r.number, r.token) for r in rows}
    return {
        r["ordinal"]
        for r in requests
        if (r["run_id"], r["task_id"], r["number"], r["token"]) in committed
    }


def finish_batch(ledger, requests):
    rows, encoded = _encoded_requests(ledger, requests)
    with ledger.session_factory() as session:
        accepted = _replayed(session, rows)
        pending = [r for r in rows if r["ordinal"] not in accepted]
        if pending:
            run_turnstile.lock_shared(session, (r["run_id"] for r in pending))
            session.execute(_FINISH_PLANNER)
            changed = session.execute(
                _FINISH,
                {
                    "requests": encoded if len(pending) == len(rows) else _dump(pending),
                    "history_run_ids": sorted({r["run_id"] for r in pending}),
                },
            ).all()
            accepted.update(row.ordinal for row in changed)
            # A concurrent finish can commit while this transaction waits on
            # its fence. Re-read missing attempts after those locks complete.
            missing = [r for r in pending if r["ordinal"] not in accepted]
            accepted.update(_replayed(session, missing))
            for row in changed:
                if row.state == "queued":
                    _task_event(
                        session,
                        row.run_id,
                        "task_retry_scheduled",
                        task_id=row.task_id,
                        retry_at=row.available_at.isoformat(),
                        error=row.error,
                    )
                else:
                    _task_event(
                        session,
                        row.run_id,
                        "task_completed" if row.success else "task_failed",
                        task_id=row.task_id,
                        status=row.state,
                        attempt=row.number,
                        message=row.message,
                    )
        session.commit()
    return [
        None if i in accepted else StaleAttempt("Attempt no longer owns completion")
        for i in range(len(rows))
    ]
