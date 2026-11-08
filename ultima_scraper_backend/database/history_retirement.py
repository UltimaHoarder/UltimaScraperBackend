"""Retire finished runs' event and mutation partitions.

Every leaf of a partitioned parent is a relation that a backend must open, lock
and cache whenever a query cannot prune to one run: the outbox relay, event
replay, outbox retention and receipt checks by command ID. Keeping one leaf
per finished run makes each of those queries, and each backend's relation
cache, grow with job history. Finished runs keep task and attempt history for
job views; their outbox and mutation leaves are dropped once the run has been
terminal for the grace period, all its events are delivered and every
event-reading plugin has consumed them.

Worker API commands are the exception: one page of profiles starts dozens, and
nothing reads their task rows once the caller has its answer. All of a
successful command's leaves go shortly after it finishes. A failed command's
task and attempt leaves last as long as other jobs' full task history.
"""

import logging
import time

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from ultima_scraper_core import runtime_env_value

from .history_compaction import retention_days
from .job_partitions import (
    RETIRED_TABLES,
    _drop_partition,
    _owned_partition,
    partition_name,
    purge_identity_page,
)

DEFAULT_RETIRE_AFTER_SECONDS = 3600
IDENTITY_BATCH = 5000
CYCLE_LIMIT = 64
CYCLE_BUDGET_SECONDS = 10.0
# Well past the two minutes a caller waits for a command's result.
API_COMMAND_GRACE_SECONDS = 600
API_COMMAND_CYCLE_LIMIT = 300
API_COMMAND_CYCLE_BUDGET_SECONDS = 30.0
TASK_HISTORY_TABLES = ("job_tasks", "execution_attempts")
STATUS_KEY = "ultimascraper:database:history-retirement:status"
CURSOR_KEY = "ultimascraper:database:history-retirement:identity-cursors"
TERMINAL = ("completed", "failed", "cancelled")
# Lock waits, not row counts, bound each retirement transaction. Dropping a
# leaf takes ACCESS EXCLUSIVE on its parent, which queues behind relays and
# writers; give up quickly and retry next cycle rather than stall them.
LOCK_TIMEOUT = "500ms"
# 42P01: every API process runs this cycle, and another one dropped the leaf
# between the ownership check and reading it.
RETRYABLE_SQLSTATES = {"55P03", "40P01", "40001", "42P01"}

logger = logging.getLogger(__name__)

_CANDIDATES = text(r"""
SELECT r.run_id, p.relname AS history_table
FROM pg_inherits i
JOIN pg_class c ON c.oid=i.inhrelid
JOIN pg_class p ON p.oid=i.inhparent
JOIN management.execution_runs r
  ON r.run_id=replace(
       substring(pg_get_expr(c.relpartbound, c.oid) FROM '^FOR VALUES IN \(''(.*)''\)$'),
       '''''', '''')
WHERE i.inhparent IN (to_regclass('management.execution_outbox'),
                      to_regclass('management.execution_mutations'))
  AND c.relname ~ '_job_[0-9a-f]{32}$'
  AND r.state IN ('completed','failed','cancelled')
  AND r.completed_at < clock_timestamp() - make_interval(secs => :grace)
  AND coalesce(r.config_json::jsonb->'metadata'->>'deletion_pending', '') <> 'true'
  AND NOT (r.run_id = ANY(CAST(:protected AS text[])))
-- Mutation leaves need no delivery check. Release them first, then eligible
-- event leaves, before spending the remaining budget on directory cleanup.
ORDER BY p.relname = 'execution_outbox', r.completed_at, r.run_id
LIMIT :limit
""")

_API_COMMAND_CANDIDATES = text(r"""
SELECT r.run_id, p.relname AS history_table, r.state = 'completed' AS succeeded
FROM pg_inherits i
JOIN pg_class c ON c.oid=i.inhrelid
JOIN pg_class p ON p.oid=i.inhparent
JOIN management.execution_runs r
  ON r.run_id=replace(
       substring(pg_get_expr(c.relpartbound, c.oid) FROM '^FOR VALUES IN \(''(.*)''\)$'),
       '''''', '''')
WHERE i.inhparent IN (to_regclass('management.execution_outbox'),
                      to_regclass('management.execution_mutations'),
                      to_regclass('management.job_tasks'),
                      to_regclass('management.execution_attempts'))
  AND c.relname ~ '_job_[0-9a-f]{32}$'
  AND r.run_id LIKE 'api-%'
  AND r.state IN ('completed','failed','cancelled')
  AND (
    (r.state = 'completed'
     AND r.completed_at < clock_timestamp() - make_interval(secs => :grace))
    -- A NULL grace keeps failures' task history for as long as the setting does.
    OR (p.relname IN ('job_tasks','execution_attempts')
        AND r.completed_at < clock_timestamp()
            - make_interval(secs => CAST(:failed_grace AS double precision)))
  )
  AND coalesce(r.config_json::jsonb->'metadata'->>'deletion_pending', '') <> 'true'
  AND NOT (r.run_id = ANY(CAST(:protected AS text[])))
ORDER BY r.completed_at, r.run_id, p.relname
LIMIT :limit
""")

# A committed DROP is also a durable cleanup checkpoint. Find its remaining
# directory rows after restart without opening/scanning every history leaf.
# Seek the next composite owner key, then check equality outside the LIMIT.
# An equality filter inside LIMIT can choose the global event_id index and scan
# its entirety for a missing owner, especially after statistics/rebuild changes.
_ORPHAN_CANDIDATES = text("""
SELECT r.run_id,ids.event_id FROM management.execution_runs r
JOIN LATERAL (
    SELECT run_id,event_id FROM management.execution_outbox_identities owned
    WHERE (owned.run_id,owned.event_id) >= (r.run_id,CAST('-9223372036854775808' AS bigint))
    ORDER BY owned.run_id,owned.event_id LIMIT 1 OFFSET 0
) ids ON ids.run_id=r.run_id
WHERE r.state IN ('completed','failed','cancelled')
  AND r.completed_at < clock_timestamp() - make_interval(secs => :grace)
  AND coalesce(r.config_json::jsonb->'metadata'->>'deletion_pending','') <> 'true'
  AND NOT (r.run_id = ANY(CAST(:protected AS text[])))
  AND to_regclass('management.execution_outbox_job_' ||
      substr(encode(sha256(convert_to(r.run_id,'UTF8')),'hex'),1,32)) IS NULL
ORDER BY r.completed_at,r.run_id LIMIT :limit
""")


def retire_after_seconds() -> int:
    value = int(
        runtime_env_value("ULTIMA_HISTORY_RETIRE_AFTER_SECONDS")
        or DEFAULT_RETIRE_AFTER_SECONDS
    )
    if value < 0:
        raise ValueError("History retirement grace must not be negative")
    return value


def _terminal_run_locked(session, run_id, grace):
    """Lock the run against reopening and check every retirement condition.

    The shared run lock conflicts with control and replacement paths, which
    lock the run for update before recording events. Writers lock it shared
    and reject terminal runs, so no row can enter these leaves meanwhile.
    """
    session.execute(text(f"SET LOCAL lock_timeout='{LOCK_TIMEOUT}'"))
    run = session.execute(
        text("""SELECT state,
          completed_at < clock_timestamp() - make_interval(secs => :grace) AS aged,
          coalesce(config_json::jsonb->'metadata'->>'deletion_pending', '') = 'true' AS deleting
        FROM management.execution_runs WHERE run_id=:run FOR SHARE"""),
        {"run": run_id, "grace": grace},
    ).one_or_none()
    return run is not None and run.state in TERMINAL and run.aged and not run.deleting


def _eligible(session, run_id, table, grace, floor):
    if not _terminal_run_locked(session, run_id, grace):
        return False
    if not _owned_partition(session, run_id, table):
        return False
    if table != "execution_outbox":
        return True
    # Undelivered events still need the relay; events past the plugin floor
    # still need their plugin's replay. The leaf is named, so this is pruned.
    return not session.scalar(
        text(f"""SELECT EXISTS(SELECT 1 FROM management.{partition_name(run_id, table)}
          WHERE delivered=false
             OR (CAST(:floor AS bigint) IS NOT NULL AND sequence > CAST(:floor AS bigint)))"""),
        {"floor": floor},
    )


def _purge_event_identities(factory, run_id, grace, deadline, cursors):
    """Resume bounded directory cleanup after the consumed leaf is gone.

    Stale directory entries reserve their identities and join to no event;
    they never authorize a write. A reopened/recreated leaf stops cleanup.
    """
    while time.monotonic() < deadline:
        with factory() as session:
            if not _terminal_run_locked(session, run_id, grace):
                return False
            session.execute(text(
                "SELECT pg_advisory_xact_lock(hashtextextended('mutation-partition-ddl',0))"
            ))
            if session.scalar(text("SELECT to_regclass(:leaf)"), {
                "leaf": "management." + partition_name(run_id, "execution_outbox")
            }) is not None:
                return False
            _, remaining, cursor = purge_identity_page(
                session, "execution_outbox", run_id, IDENTITY_BATCH,
                after=cursors.get(run_id),
            )
            session.commit()
            # Redis may lag a commit after an outage; repeating a page is safe.
            # Never advance the external cursor before the SQL commit.
            if remaining and cursor is not None:
                cursors[run_id] = cursor
            else:
                cursors.pop(run_id, None)
        if not remaining:
            return True
    return False


def _drop_leaf(factory, run_id, table, grace, floor):
    with factory() as session:
        if not _eligible(session, run_id, table, grace, floor):
            return False
        # Same fence as provisioning and deletion, after the run lock as there.
        session.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended('mutation-partition-ddl',0))"
            )
        )
        if not _owned_partition(session, run_id, table):
            return False
        dropped = _drop_partition(session, run_id, table)
        if dropped and table in TASK_HISTORY_TABLES:
            # DROP fires no row triggers. A command's handful of rows always
            # fits one page, so its directory entries go with the leaf.
            purge_identity_page(session, table, run_id, IDENTITY_BATCH)
        session.commit()
    return dropped


def retire_finished_history(
    *,
    protected_run_ids=frozenset(),
    grace_seconds=None,
    session_factory=None,
    limit=CYCLE_LIMIT,
    budget_seconds=CYCLE_BUDGET_SECONDS,
    identity_cursors=None,
):
    """Drop eligible leaves, oldest finished runs first, within a time budget.

    Busy leaves are left for a later cycle. Returns per-outcome counts.
    """
    from ultima_scraper_db_archive.global_db import global_session

    from .outbox_retention import _plugin_event_floor

    factory = session_factory or global_session
    grace = retire_after_seconds() if grace_seconds is None else grace_seconds
    deadline = time.monotonic() + budget_seconds
    cursors = {} if identity_cursors is None else identity_cursors
    with factory() as session:
        if not session.scalar(
            text(
                "SELECT relkind='p' FROM pg_class WHERE oid=to_regclass('management.execution_outbox')"
            )
        ):
            return {"retired": 0, "busy": 0, "waiting": 0}
        floor = _plugin_event_floor(session)
        candidates = session.execute(
            _CANDIDATES,
            {"grace": grace, "protected": sorted(protected_run_ids), "limit": limit},
        ).all()
        session.commit()
    counts = {"retired": 0, "busy": 0, "waiting": 0}
    for run_id, table in candidates:
        if table not in RETIRED_TABLES:
            continue
        if time.monotonic() >= deadline:
            break
        try:
            if _drop_leaf(factory, run_id, table, grace, floor):
                counts["retired"] += 1
            else:
                counts["waiting"] += 1
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) not in RETRYABLE_SQLSTATES:
                raise
            counts["busy"] += 1
    # Release eligible leaf files before spending the remaining budget deleting
    # millions of directory rows. Missing leaves are rediscovered after restart.
    if time.monotonic() < deadline:
        try:
            with factory() as session:
                # A poor plan must not turn an idle background check into an
                # unbounded scan competing with active database work.
                remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
                session.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                                {"timeout": str(remaining_ms)})
                orphan_runs = session.scalars(_ORPHAN_CANDIDATES, {
                    "grace": grace, "protected": sorted(protected_run_ids), "limit": limit,
                }).all()
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != "57014":
                raise
            counts["busy"] += 1
            return counts
        for run_id in orphan_runs:
            if time.monotonic() >= deadline:
                break
            try:
                if not _purge_event_identities(factory, run_id, grace, deadline, cursors):
                    counts["waiting"] += 1
            except DBAPIError as exc:
                if getattr(exc.orig, "sqlstate", None) not in RETRYABLE_SQLSTATES:
                    raise
                counts["busy"] += 1
    return counts


def retire_finished_api_commands(
    *,
    protected_run_ids=frozenset(),
    grace_seconds=API_COMMAND_GRACE_SECONDS,
    failed_days=None,
    session_factory=None,
    limit=API_COMMAND_CYCLE_LIMIT,
    budget_seconds=API_COMMAND_CYCLE_BUDGET_SECONDS,
):
    """Drop finished API commands' leaves, oldest first, within a time budget."""
    from ultima_scraper_db_archive.global_db import global_session

    from .outbox_retention import _plugin_event_floor

    factory = session_factory or global_session
    deadline = time.monotonic() + budget_seconds
    with factory() as session:
        days = retention_days(session) if failed_days is None else failed_days
        failed_grace = days * 86400 if days > 0 else None
        floor = _plugin_event_floor(session)
        candidates = session.execute(
            _API_COMMAND_CANDIDATES,
            {
                "grace": grace_seconds,
                "failed_grace": failed_grace,
                "protected": sorted(protected_run_ids),
                "limit": limit,
            },
        ).all()
        session.commit()
    counts = {"retired": 0, "busy": 0, "waiting": 0}
    for run_id, table, succeeded in candidates:
        if time.monotonic() >= deadline:
            break
        grace = grace_seconds if succeeded else failed_grace
        try:
            if _drop_leaf(factory, run_id, table, grace, floor):
                counts["retired"] += 1
            else:
                counts["waiting"] += 1
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) not in RETRYABLE_SQLSTATES:
                raise
            counts["busy"] += 1
    return counts


async def run_history_retirement_cycle(redis_manager):
    from .receipt_retention import pending_run_ids, retention_mode
    from .threads import run_database_call

    if retention_mode() == "paused":
        return 0
    # Late replays of a committed mutation resolve through its leaf. Keep runs
    # with queued writer commands until those commands have been consumed.
    protected = await pending_run_ids(redis_manager)
    previous = await redis_manager.client.hgetall(CURSOR_KEY)
    cursors = {
        key.decode() if isinstance(key, bytes) else key: int(value)
        for key, value in previous.items()
    }
    previous_keys = set(cursors)
    counts = await run_database_call(
        retire_finished_history, protected_run_ids=protected, identity_cursors=cursors
    )
    api_commands = await run_database_call(
        retire_finished_api_commands, protected_run_ids=protected
    )
    counts = {key: counts[key] + api_commands[key] for key in counts}
    stale = previous_keys - set(cursors)
    if stale:
        await redis_manager.client.hdel(CURSOR_KEY, *stale)
    if cursors:
        await redis_manager.client.hset(CURSOR_KEY, mapping=cursors)
    await redis_manager.client.hset(
        STATUS_KEY,
        mapping={**{key: str(value) for key, value in counts.items()},
                 "updated_at": str(time.time())},
    )
    return counts["retired"]
