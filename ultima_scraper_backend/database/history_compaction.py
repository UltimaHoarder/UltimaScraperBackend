"""Compact finished job history down to its failures.

A finished job keeps every completed per-task and per-attempt row, millions for
a profile probe, although nothing needs them once it has settled: job summaries
come from ``mps_plans`` and the live work counters, and a retry rebuilds only
failures and the children that completed producers spawned. After
``job_execution.task_history_retention_days`` each run's two dedicated history
leaves are rewritten to keep failed, cancelled or errored tasks, completed tasks
whose result spawned children, and every attempt of a kept task. A job without
failures therefore can no longer be re-run as a whole from its history.

The kept rows wait in temporary tables while the leaves are truncated and
refilled in one transaction, so a failure rolls back to the full history.
TRUNCATE releases the leaf files immediately; a DELETE would rewrite every row
to WAL and leave the files their original size. It locks only the run's own
leaves, never the partitioned parents. TRUNCATE fires no row triggers, so the
global identity directories are cleaned afterwards in committed pages.

TRUNCATE needs the table owner. A DML-only runtime role truncates through
migration c8a3e5f71d26's SECURITY DEFINER function instead, when it exists.
"""

import json
import logging
import time
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from .job_partitions import partition_name

SETTING = "task_history_retention_days"
DEFAULT_RETENTION_DAYS = 3
METADATA_KEY = "task_history_compaction"
TERMINAL = ("completed", "failed", "cancelled")
STATUS_KEY = "ultimascraper:database:history-compaction:status"
# One large run reads tens of GB; compact at most this many per cycle.
RUN_LIMIT = 1
IDENTITY_PAGE = 50_000
CYCLE_BUDGET_SECONDS = 30.0
# Copying a twelve-million-task leaf takes minutes; lock waits must not.
COPY_TIMEOUT = "30min"
LOCK_TIMEOUT = "500ms"
RETRYABLE_SQLSTATES = {"55P03", "40P01", "40001"}

logger = logging.getLogger(__name__)

_ELIGIBLE = """
  r.state IN ('completed','failed','cancelled')
  AND r.run_id NOT LIKE 'api-%'
  AND r.completed_at < clock_timestamp() - make_interval(days => :days)
  AND coalesce(r.config_json::jsonb->'metadata'->>'deletion_pending', '') <> 'true'
  AND NOT (r.run_id = ANY(CAST(:protected AS text[])))
"""

_CANDIDATES = text(f"""
SELECT r.run_id FROM management.execution_runs r
WHERE {_ELIGIBLE}
  AND r.config_json::jsonb->'metadata'->'{METADATA_KEY}' IS NULL
ORDER BY r.completed_at, r.run_id
LIMIT :limit
""")

_IDENTITY_PENDING = text(f"""
SELECT r.run_id FROM management.execution_runs r
WHERE coalesce(r.config_json::jsonb->'metadata'->'{METADATA_KEY}'->>'identities_pending', '')
      = 'true'
ORDER BY r.completed_at, r.run_id
LIMIT :limit
""")

# Completed producers stay because a retry rebuilds their spawned children
# from ``new_tasks``; the regex skips parsing results with an empty list.
_KEEP_TASK = """
  t.status <> 'completed' OR t.error IS NOT NULL
  OR (t.result_json ~ '"new_tasks": *\\[ *[^] ]'
      AND jsonb_array_length(coalesce(t.result_json::jsonb->'new_tasks', '[]'::jsonb)) > 0)
"""


def retention_days(session) -> int:
    """job_execution.task_history_retention_days, read live; ``0`` disables."""
    from ..job_system.resources import _persisted_job_execution_setting

    try:
        # A savepoint keeps a failed settings read from aborting the cycle.
        with session.begin_nested():
            days = _persisted_job_execution_setting(SETTING, db_session=session)
    except Exception:
        days = None
    return DEFAULT_RETENTION_DAYS if days is None or days < 0 else days


def _leaves(run_id):
    return (
        "management." + partition_name(run_id, "job_tasks"),
        "management." + partition_name(run_id, "execution_attempts"),
    )


def _owns_leaves(session, run_id):
    """Both leaves exist and are this run's dedicated partitions."""

    tasks_leaf, attempts_leaf = _leaves(run_id)
    return session.scalar(
        text("""
        SELECT count(*) = 2 FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE (i.inhrelid, i.inhparent) IN (
            (to_regclass(:tasks), to_regclass('management.job_tasks')),
            (to_regclass(:attempts), to_regclass('management.execution_attempts')))
          AND pg_get_expr(c.relpartbound, c.oid) = 'FOR VALUES IN (' || quote_literal(:run) || ')'
        """),
        {"tasks": tasks_leaf, "attempts": attempts_leaf, "run": run_id},
    )


def _truncate_function(session):
    """Whether migration c8a3e5f71d26's owner truncate function exists."""
    info = getattr(session, "info", None)
    if info is not None and "history_truncate_function" in info:
        return info["history_truncate_function"]
    available = bool(session.scalar(text(
        "SELECT to_regprocedure('management.usr_truncate_history_partitions(text)') IS NOT NULL"
    )))
    if info is not None:
        info["history_truncate_function"] = available
    return available


def _set_metadata(session, run_id, value):
    session.execute(
        text("""
        UPDATE management.execution_runs
        SET config_json = jsonb_set(
            jsonb_set(config_json::jsonb, '{metadata}',
                      coalesce(config_json::jsonb->'metadata', '{}'::jsonb)),
            CAST(:path AS text[]), CAST(:value AS jsonb))::text
        WHERE run_id = :run
        """),
        {"run": run_id, "path": ["metadata", METADATA_KEY], "value": json.dumps(value)},
    )


def compact_run(session, run_id, *, days, protected_run_ids=frozenset()):
    """Rewrite one run's leaves to its kept rows. Returns counts, or ``None``.

    ``None`` means the run is no longer eligible. The caller commits; a lock
    timeout raises and rolls everything back, leaving the full history.
    """

    session.execute(text("SELECT set_config('statement_timeout', :t, true)"), {"t": COPY_TIMEOUT})
    # The shared run lock conflicts with reopening and replacement paths.
    eligible = session.scalar(
        text(f"""
        SELECT true FROM management.execution_runs r
        WHERE r.run_id = :run AND {_ELIGIBLE}
          AND r.config_json::jsonb->'metadata'->'{METADATA_KEY}' IS NULL
        FOR SHARE
        """),
        {"run": run_id, "days": days, "protected": sorted(protected_run_ids)},
    )
    if not eligible or not _owns_leaves(session, run_id):
        return None
    tasks_leaf, attempts_leaf = _leaves(run_id)
    total_tasks = session.scalar(text(f"SELECT count(*) FROM {tasks_leaf}"))
    total_attempts = session.scalar(text(f"SELECT count(*) FROM {attempts_leaf}"))
    session.execute(text(f"""
        CREATE TEMP TABLE usr_compaction_tasks ON COMMIT DROP AS
        SELECT t.* FROM {tasks_leaf} t WHERE {_KEEP_TASK}
    """))
    session.execute(text(f"""
        CREATE TEMP TABLE usr_compaction_attempts ON COMMIT DROP AS
        SELECT a.* FROM {attempts_leaf} a
        WHERE a.task_id IN (SELECT task_id FROM usr_compaction_tasks)
    """))
    kept_tasks = session.scalar(text("SELECT count(*) FROM usr_compaction_tasks"))
    kept_attempts = session.scalar(text("SELECT count(*) FROM usr_compaction_attempts"))
    # Serialize with leaf creation, retirement and job deletion, then recheck.
    session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended('mutation-partition-ddl',0))")
    )
    if not _owns_leaves(session, run_id):
        return None
    session.execute(text("SELECT set_config('lock_timeout', :t, true)"), {"t": LOCK_TIMEOUT})
    if _truncate_function(session):
        # The same statement, run with the table owner's rights.
        session.execute(
            text("SELECT management.usr_truncate_history_partitions(:run)"), {"run": run_id}
        )
    else:
        session.execute(text(f"TRUNCATE {attempts_leaf}, {tasks_leaf}"))
    session.execute(text(f"INSERT INTO {tasks_leaf} SELECT * FROM usr_compaction_tasks"))
    session.execute(text(f"INSERT INTO {attempts_leaf} SELECT * FROM usr_compaction_attempts"))
    counts = {
        "compacted_at": datetime.now(timezone.utc).isoformat(),
        "retention_days": days,
        "kept_tasks": kept_tasks,
        "removed_tasks": total_tasks - kept_tasks,
        "kept_attempts": kept_attempts,
        "removed_attempts": total_attempts - kept_attempts,
        "identities_pending": True,
    }
    _set_metadata(session, run_id, counts)
    return counts


def purge_identity_page(session, run_id, kind, after, limit=None):
    """Delete one keyset page of directory rows whose history row is gone.

    Returns ``(deleted, next_cursor)``; ``next_cursor`` is ``None`` when the
    run has no directory rows past ``after``. Checks only the run's own leaf:
    checking through the parent locks every partition of the table.
    """

    limit = IDENTITY_PAGE if limit is None else limit
    tasks_leaf, attempts_leaf = _leaves(run_id)
    if kind == "task":
        directory, owner, key, leaf = "job_task_identities", "job_id", "task_id", tasks_leaf
    else:
        directory, owner, key, leaf = "execution_attempt_identities", "run_id", "token", attempts_leaf
    page = session.execute(
        text(f"""
        SELECT max(d.{key}) AS last, count(*) AS scanned FROM (
            SELECT {key} FROM management.{directory}
            WHERE {owner} = :run AND {key} > :after
            ORDER BY {owner}, {key} LIMIT :limit
        ) d({key})
        """),
        {"run": run_id, "after": after, "limit": limit},
    ).one()
    if not page.scanned:
        return 0, None
    deleted = session.execute(
        text(f"""
        DELETE FROM management.{directory} d
        WHERE d.{owner} = :run AND d.{key} > :after AND d.{key} <= :last
          AND NOT EXISTS (SELECT 1 FROM {leaf} h WHERE h.{key} = d.{key})
        """),
        {"run": run_id, "after": after, "last": page.last},
    ).rowcount
    return deleted, page.last


def _purge_identities(factory, run_id, deadline):
    """Clean both directories for one compacted run; True when finished."""

    for kind in ("task", "attempt"):
        cursor = ""
        while cursor is not None:
            if time.monotonic() >= deadline:
                return False
            with factory() as session:
                if not _owns_leaves(session, run_id):
                    # Retirement or deletion now owns the run's directory rows.
                    _set_metadata(session, run_id, {
                        **_metadata(session, run_id), "identities_pending": False,
                    })
                    session.commit()
                    return True
                _, cursor = purge_identity_page(session, run_id, kind, cursor)
                session.commit()
    with factory() as session:
        _set_metadata(session, run_id, {**_metadata(session, run_id), "identities_pending": False})
        session.commit()
    return True


def _metadata(session, run_id):
    raw = session.scalar(
        text(
            "SELECT config_json::jsonb->'metadata'->:key FROM management.execution_runs "
            "WHERE run_id = :run"
        ),
        {"run": run_id, "key": METADATA_KEY},
    )
    return dict(raw or {})


def compact_finished_history(
    *,
    protected_run_ids=frozenset(),
    days=None,
    session_factory=None,
    run_limit=RUN_LIMIT,
    budget_seconds=CYCLE_BUDGET_SECONDS,
):
    """Compact the oldest eligible runs, then continue directory cleanup."""

    from ultima_scraper_db_archive.global_db import global_session

    factory = session_factory or global_session
    if days is None:
        with factory() as session:
            days = retention_days(session)
            session.commit()
    counts = {"compacted": 0, "removed_tasks": 0, "busy": 0, "identities_pending": 0}
    if days <= 0:
        return counts
    deadline = time.monotonic() + budget_seconds
    with factory() as session:
        candidates = session.scalars(_CANDIDATES, {
            "days": days, "protected": sorted(protected_run_ids), "limit": run_limit,
        }).all()
        session.commit()
    for run_id in candidates:
        try:
            with factory() as session:
                result = compact_run(
                    session, run_id, days=days, protected_run_ids=protected_run_ids
                )
                session.commit()
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) not in RETRYABLE_SQLSTATES:
                raise
            counts["busy"] += 1
            continue
        if result is not None:
            counts["compacted"] += 1
            counts["removed_tasks"] += result["removed_tasks"]
            logger.info(
                "Compacted task history for %s: kept %d of %d tasks",
                run_id, result["kept_tasks"], result["kept_tasks"] + result["removed_tasks"],
            )
    with factory() as session:
        pending = session.scalars(_IDENTITY_PENDING, {"limit": 64}).all()
        session.commit()
    for run_id in pending:
        try:
            if not _purge_identities(factory, run_id, deadline):
                counts["identities_pending"] += 1
                break
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) not in RETRYABLE_SQLSTATES:
                raise
            counts["busy"] += 1
            break
    return counts


async def run_history_compaction_cycle(redis_manager):
    from .receipt_retention import pending_run_ids, retention_mode
    from .threads import run_database_call

    if retention_mode() == "paused":
        return 0
    # Runs with queued writer commands may still resolve through their history.
    protected = await pending_run_ids(redis_manager)
    counts = await run_database_call(compact_finished_history, protected_run_ids=protected)
    await redis_manager.client.hset(
        STATUS_KEY,
        mapping={**{key: str(value) for key, value in counts.items()},
                 "updated_at": str(time.time())},
    )
    return counts["compacted"]
