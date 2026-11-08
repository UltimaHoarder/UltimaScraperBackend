"""Provision and retire job history partitions only after SQL fencing."""

import hashlib
import re

from sqlalchemy import text

HISTORY_TABLES = {
    "execution_mutations": "run_id",
    "execution_attempts": "run_id",
    "execution_outbox": "run_id",
    "job_tasks": "job_id",
}
IDENTITY_TABLES = {
    "execution_attempts": ("execution_attempt_identities", "run_id", "token"),
    "execution_outbox": ("execution_outbox_identities", "run_id", "event_id"),
    "job_tasks": ("job_task_identities", "job_id", "task_id"),
}
IDENTITY_SWEEP_PAGES = 128
IDENTITY_SWEEP_MIN_ROWS = 1_000_000
IDENTITY_SWEEP_MIN_SHARE = 0.1
PARENT = "management.execution_mutations"
# Finished runs keep task and attempt history for job views, except API
# commands. Their delivered events and idempotency mutations are retired (see
# history_retirement).
RETIRED_TABLES = ("execution_outbox", "execution_mutations")


def partition_name(run_id, table="execution_mutations"):
    if table not in HISTORY_TABLES:
        raise ValueError("Unknown history table")
    return table + "_job_" + hashlib.sha256(run_id.encode()).hexdigest()[:32]


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _owner_ddl(session):
    """Whether migration b7c41e9a2d58's SECURITY DEFINER partition functions exist.

    With them, a DML-only runtime role (transaction-pooled PgBouncer) can
    attach and drop job partitions. Without them (older schema), the caller
    must own the tables, as before.
    """
    info = getattr(session, "info", None)
    if info is not None and "partition_ddl_functions" in info:
        return info["partition_ddl_functions"]
    available = bool(session.scalar(text(
        "SELECT to_regprocedure('management.usr_attach_history_partition(text,text)') IS NOT NULL"
    )))
    if info is not None:
        info["partition_ddl_functions"] = available
    return available


def _drop_partition(session, run_id, table):
    """Drop a run's own leaf; the caller holds the DDL fence and checked ownership."""
    if _owner_ddl(session):
        return bool(session.scalar(
            text("SELECT management.usr_drop_history_partition(:table, :run)"),
            {"table": table, "run": run_id},
        ))
    session.execute(text(f"DROP TABLE management.{partition_name(run_id, table)}"))
    return True


def ensure_history_partition(session, run_id, table):
    return _ensure_partitions(session, run_id, (table,))[table]


def _ensure_partitions(session, run_id, tables):
    """Provision leaves under one DDL fence after one catalog read.

    Returns, per table, whether the run owns a dedicated leaf.
    """
    for table in tables:
        if table not in HISTORY_TABLES:
            raise ValueError("Unknown history table")
    rows = session.execute(
        text(
            """SELECT t.name,
              (SELECT relkind FROM pg_class
               WHERE oid=to_regclass('management.' || t.name)) AS relkind,
              (SELECT pg_get_expr(relpartbound,oid) FROM pg_class
               WHERE oid=to_regclass('management.' || t.name || '_legacy')) AS legacy,
              to_regclass('management.' || t.leaf) IS NOT NULL AS leaf_exists
            FROM unnest(CAST(:tables AS text[]), CAST(:leaves AS text[])) AS t(name, leaf)"""
        ),
        {
            "tables": list(tables),
            "leaves": [partition_name(run_id, table) for table in tables],
        },
    ).all()
    owned = {table: False for table in tables}
    # Existing jobs remain in their explicitly bounded legacy partition.
    pending = [
        (table, leaf_exists)
        for table, relkind, legacy, leaf_exists in rows
        if relkind == "p" and not (legacy and _literal(run_id) in legacy)
    ]
    if not pending:
        return owned
    session.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended('mutation-partition-ddl',0))"
        )
    )
    for table, leaf_existed in pending:
        parent = "management." + table
        name = partition_name(run_id, table)
        # Re-check under the fence: the catalog read preceded the lock.
        if leaf_existed or session.scalar(
            text("SELECT to_regclass(:name)"), {"name": "management." + name}
        ):
            if not _owned_partition(session, run_id, table):
                raise RuntimeError(
                    "History partition name is already owned by another relation"
                )
            owned[table] = True
            continue
        # CREATE TABLE ... PARTITION OF needs ACCESS EXCLUSIVE on the parent,
        # which cannot coexist with a backup's long-lived ACCESS SHARE lock.
        # Attach a new empty table instead: the parent needs only SHARE UPDATE
        # EXCLUSIVE. PostgreSQL supplies its indexes, foreign keys and row triggers
        # during attachment. Both steps share this transaction and the DDL fence.
        if _owner_ddl(session):
            # The same two statements, run with the table owner's rights.
            session.execute(
                text("SELECT management.usr_attach_history_partition(:table, :run)"),
                {"table": table, "run": run_id},
            )
            owned[table] = True
            continue
        session.execute(
            text(
                f"CREATE TABLE management.{name} (LIKE {parent} "
                "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED "
                "INCLUDING STORAGE INCLUDING COMMENTS)"
            )
        )
        session.execute(
            text(
                f"ALTER TABLE {parent} ATTACH PARTITION management.{name} "
                f"FOR VALUES IN ({_literal(run_id)})"
            )
        )
        owned[table] = True
    return owned


def _owned_partition(session, run_id, table="execution_mutations"):
    return (
        session.scalar(
            text("""SELECT pg_get_expr(c.relpartbound,c.oid)
        FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
        WHERE c.oid=to_regclass(:name) AND i.inhparent=to_regclass(:parent)"""),
            {
                "name": "management." + partition_name(run_id, table),
                "parent": "management." + table,
            },
        )
        == f"FOR VALUES IN ({_literal(run_id)})"
    )


def drop_history_partition(session, run_id, table, *, allow_orphan=False):
    if table not in HISTORY_TABLES:
        raise ValueError("Unknown history table")
    # Call with the run locked before DDL; preserve fencing until the leaf is gone.
    state = session.scalar(
        text(
            "SELECT state FROM management.execution_runs WHERE run_id=:run AND config_json::jsonb->'metadata'->>'deletion_pending'='true'"
        ),
        {"run": run_id},
    )
    if (
        state not in {"completed", "failed", "cancelled"}
        and allow_orphan
        and table == "job_tasks"
    ):
        session.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended('mutation-partition-ddl',0))"
            )
        )
        absent = session.scalar(
            text("""SELECT NOT EXISTS(SELECT 1 FROM management.execution_runs WHERE run_id=:run)
            AND NOT EXISTS(SELECT 1 FROM management.jobs WHERE job_id=:run)
            AND NOT EXISTS(SELECT 1 FROM management.mps_plans WHERE job_id=:run)"""),
            {"run": run_id},
        )
        if absent:
            state = "cancelled"
    if state not in {"completed", "failed", "cancelled"}:
        raise ValueError("History partition disposal requires a terminal fenced run")
    if not _owned_partition(session, run_id, table):
        return False
    # Give short relay/replay transactions time to drain without an unbounded
    # DDL wait. Restore the caller's timeout after successful disposal.
    previous_timeout = session.scalar(text("SHOW lock_timeout"))
    session.execute(text("SET LOCAL lock_timeout='2s'"))
    dropped = _drop_partition(session, run_id, table)
    session.execute(
        text("SELECT set_config('lock_timeout', :timeout, true)"),
        {"timeout": previous_timeout},
    )
    return dropped


def ensure_history_partitions(session, run_id):
    _ensure_partitions(session, run_id, tuple(HISTORY_TABLES))


def restore_retired_history(session, run_id):
    """Recreate a finished run's retired leaves before it records events again.

    Call with the run row locked and before this transaction writes
    execution_runs: ATTACH validates foreign keys with SHARE ROW EXCLUSIVE on it.
    """
    _ensure_partitions(session, run_id, RETIRED_TABLES)


def ensure_mutation_partition(session, run_id):
    return ensure_history_partition(session, run_id, "execution_mutations")


def drop_mutation_partition(session, run_id):
    return drop_history_partition(session, run_id, "execution_mutations")


def task_conflict_columns(session):
    from ultima_scraper_db_archive.global_db import CentralTaskModel

    if "task_history_partitioned" not in session.info:
        session.info["task_history_partitioned"] = session.scalar(
            text(
                "SELECT relkind='p' FROM pg_class WHERE oid=to_regclass('management.job_tasks')"
            )
        )
    return (
        [CentralTaskModel.job_id, CentralTaskModel.task_id]
        if session.info["task_history_partitioned"]
        else [CentralTaskModel.task_id]
    )


def task_identity_model(session):
    """Use the migrated global directory only with all identity guards active."""
    from ultima_scraper_db_archive.execution_models import JobTaskIdentityModel
    from ultima_scraper_db_archive.global_db import CentralTaskModel

    if session.get_bind().dialect.name != "postgresql":
        return CentralTaskModel
    if "task_identity_directory_guarded" not in session.info:
        # pg_partition_tree opens every leaf and retains AccessShare locks.
        # Traverse catalog inheritance instead so guard verification itself
        # does not consume the partition locks this directory avoids.
        state = session.execute(text("""
            WITH RECURSIVE history_relations(relid) AS (
              SELECT oid FROM pg_class
              WHERE oid=to_regclass('management.job_tasks')
              UNION ALL
              SELECT child.inhrelid FROM pg_inherits child
              JOIN history_relations parent ON parent.relid=child.inhparent
            )
            SELECT relkind='p' AS partitioned,
              relkind='p'
              AND to_regclass('management.job_task_identities') IS NOT NULL
              AND current_setting('session_replication_role') IN ('origin','local')
              AND NOT EXISTS (
                SELECT 1 FROM history_relations leaf
                WHERE NOT EXISTS (
                  SELECT 1 FROM pg_trigger guard
                  WHERE guard.tgrelid=leaf.relid
                    AND guard.tgname='history_identity_guard'
                    AND guard.tgtype=31 AND guard.tgenabled IN ('O','A')
                    AND guard.tgfoid=to_regprocedure('management.history_identity_guard()')
                )
              ) AS guarded
            FROM pg_class WHERE oid=to_regclass('management.job_tasks')
        """)).one()
        session.info["task_history_partitioned"] = state.partitioned
        session.info["task_identity_directory_guarded"] = bool(state.guarded)
    return (
        JobTaskIdentityModel
        if session.info["task_identity_directory_guarded"]
        else CentralTaskModel
    )


def purge_identity_batch(session, table, run_id, limit):
    count, remaining, _ = purge_identity_page(session, table, run_id, limit)
    return count, remaining


def identity_cleanup_uses_sweep(session, table, run_id):
    """Prefer sequential heap reads for large owners of a global directory.

    Token/task keys are random relative to heap order. On a small shared-buffer
    budget an ordered key scan fetches nearly one heap page for every row. A
    dense owner's physical sweep reads the directory once instead. Estimates
    only choose the strategy; they never decide which rows can be removed.
    """
    if table not in IDENTITY_TABLES:
        return False
    directory, owner, key = IDENTITY_TABLES[table]
    total = session.scalar(text("""SELECT reltuples FROM pg_class
        WHERE oid=to_regclass(:name) AND relkind='r'"""), {"name": "management." + directory})
    if total is None or total < IDENTITY_SWEEP_MIN_ROWS:
        return False
    plan = session.scalar(text(f"""EXPLAIN (FORMAT JSON) SELECT {key}
        FROM management.{directory} WHERE {owner}=:run"""), {"run": run_id})
    estimated = plan[0]["Plan"]["Plan Rows"]
    return estimated >= IDENTITY_SWEEP_MIN_ROWS and estimated >= total * IDENTITY_SWEEP_MIN_SHARE


def purge_identity_sweep_page(session, table, run_id, limit, *, after="(0,0)"):
    """Delete at most one bounded heap range of a fenced owner's identities.

    Checkpoint the returned physical cursor with the commit. Empty and foreign
    ranges advance too. A final owner-wide check rewinds for skipped row locks
    or rows moved behind the cursor by a heap rewrite, before completion.
    """
    if limit <= 0:
        raise ValueError("limit must be positive")
    if table not in IDENTITY_TABLES:
        return 0, False, None
    match = re.fullmatch(r"\((\d+),(\d+)\)", after)
    if not match:
        raise ValueError("Invalid identity sweep cursor")
    directory, owner, _ = IDENTITY_TABLES[table]
    relation = session.execute(text("""SELECT relkind,
        pg_relation_size(oid)/current_setting('block_size')::bigint AS blocks
        FROM pg_class WHERE oid=to_regclass(:name)"""), {"name": "management." + directory}).one_or_none()
    if relation is None:
        return 0, False, None
    if relation.relkind != 'r':
        raise ValueError("Identity sweep requires an unpartitioned directory")
    blocks = relation.blocks
    block = int(match[1])
    high = f"({block + IDENTITY_SWEEP_PAGES},0)"
    count, last = session.execute(text(f"""WITH batch AS MATERIALIZED (
        SELECT ctid AS row_location FROM management.{directory}
        WHERE {owner}=:run AND ctid>CAST(:after AS tid) AND ctid<CAST(:high AS tid)
        ORDER BY ctid LIMIT :limit FOR UPDATE SKIP LOCKED), deleted AS (
        DELETE FROM management.{directory} WHERE {owner}=:run
        AND ctid IN (SELECT row_location FROM batch) RETURNING 1)
        SELECT count(*), (SELECT batch.row_location::text FROM batch
                         ORDER BY batch.row_location DESC LIMIT 1) FROM deleted"""),
        {"run": run_id, "after": after, "high": high, "limit": limit}).one()
    if count == limit:
        return count, True, last
    if block + IDENTITY_SWEEP_PAGES < blocks:
        return count, True, high
    remaining = session.scalar(text(f"""SELECT EXISTS(SELECT 1
        FROM management.{directory} WHERE {owner}=:run)"""), {"run": run_id})
    return count, bool(remaining), "(0,0)" if remaining else None


def purge_identity_page(session, table, run_id, limit, *, after=None):
    """Seek through an owner's directory, returning a commit-safe next cursor.

    Repeatedly starting at the owner's first key revisits every deleted index
    entry until vacuum catches up. Persist the cursor with the deletion's
    checkpoint in the same transaction. At the end of a pass, check the whole
    owner again and rewind if SKIP LOCKED left any rows behind.
    """
    if limit <= 0:
        raise ValueError("limit must be positive")
    if table not in IDENTITY_TABLES:
        return 0, False, None
    directory, owner, key = IDENTITY_TABLES[table]
    if (
        session.scalar(
            text("SELECT to_regclass(:table)"), {"table": "management." + directory}
        )
        is None
    ):
        return 0, False, None
    seek = f" AND {key}>:after" if after is not None else ""
    count, cursor = session.execute(
        text(f"""WITH batch AS MATERIALIZED (
        SELECT ctid FROM management.{directory} WHERE {owner}=:run{seek} ORDER BY {key}
        LIMIT :limit FOR UPDATE SKIP LOCKED), deleted AS (
        DELETE FROM management.{directory} WHERE {owner}=:run
        AND ctid IN (SELECT ctid FROM batch) RETURNING {key})
        SELECT count(*), max({key}) FROM deleted"""),
        {"run": run_id, "limit": limit, "after": after},
    ).one()
    # A full page always has another bounded pass. Avoid rescanning the dead
    # prefix just to check EXISTS after every batch of a multi-million-row job.
    if count == limit:
        return count, True, cursor
    remaining = session.scalar(
        text(f"SELECT EXISTS(SELECT 1 FROM management.{directory} WHERE {owner}=:run)"),
        {"run": run_id},
    )
    return count, bool(remaining), None
