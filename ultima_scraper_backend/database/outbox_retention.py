"""Bound delivered terminal-job event history while yielding to execution."""

import logging
import re
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.exc import OperationalError


from .receipt_retention import ReceiptPurgeResult, pending_command_ids, retention_mode

logger = logging.getLogger(__name__)

CURSOR_KEY = "ultimascraper:database:outbox-retention:cursor"
IDLE_BATCH = 512
EXECUTING_BATCH = 128
DEFAULT_RETENTION_HOURS = 168
STATUS_KEY = "ultimascraper:database:outbox-retention:status"


def purge_outbox(
    *, cursor="(0,0)", mode="idle", protected_ids=frozenset(), session_factory=None
):
    from ultima_scraper_db_archive.global_db import global_session

    if mode not in {"idle", "paused", "always"}:
        raise ValueError("Invalid outbox cleanup mode")
    if mode == "paused" or (mode == "idle" and protected_ids):
        return ReceiptPurgeResult(
            0,
            0,
            paused_reason="operator_paused"
            if mode == "paused"
            else "write_queue_active",
        )
    match = re.fullmatch(r"\((\d+),(\d+)\)", cursor)
    event_cursor = re.fullmatch(r"event:(\d+)", cursor)
    if not match and not event_cursor:
        raise ValueError("Invalid outbox sweep cursor")
    block = int(match[1]) if match else 0
    high = f"({block + 128},0)"
    with (session_factory or global_session)() as session:
        if not session.scalar(
            text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('execution-outbox-retention',0))"
            )
        ):
            return ReceiptPurgeResult(0, 0, busy=True)
        session.execute(text("SET LOCAL statement_timeout='5s'"))
        session.execute(text("SET LOCAL lock_timeout='250ms'"))
        # Execution no longer pauses cleanup (a long job would keep every
        # event); it only shrinks each batch so purging stays in the background.
        executing = mode == "idle" and session.scalar(
            text("""SELECT EXISTS(
            SELECT 1 FROM management.execution_runs r WHERE r.state='running'
            OR (r.state='pending' AND r.next_at<=clock_timestamp())
            OR (r.state='paused' AND EXISTS(SELECT 1 FROM management.execution_work w
                WHERE w.run_id=r.run_id AND w.state!='cancelled')))""")
        )
        batch = EXECUTING_BATCH if executing else IDLE_BATCH
        cutoff = datetime.now(UTC) - timedelta(hours=_retention_hours(session))
        floor = _plugin_event_floor(session)
        if session.scalar(
            text(
                "SELECT relkind='p' FROM pg_class WHERE oid=to_regclass('management.execution_outbox')"
            )
        ) and session.scalar(
            text("SELECT to_regclass('management.execution_outbox_identities')")
        ):
            try:
                return _purge_partitioned_outbox(
                    session,
                    int(event_cursor[1]) if event_cursor else 0,
                    cutoff,
                    floor=floor,
                    batch=batch,
                )
            except OperationalError as exc:
                # Bulk identity purges (job deletion, history retirement)
                # leave dead index entries ahead of the cursor until
                # autovacuum removes them; stepping over millions can exceed
                # the timeout. Keep the cursor and retry on a later cycle.
                if getattr(exc.orig, "sqlstate", None) != "57014":
                    raise
                session.rollback()
                logger.warning(
                    "Outbox retention scan timed out; retrying after autovacuum"
                )
                return ReceiptPurgeResult(0, 0, busy=True)
        blocks = session.scalar(
            text(
                "SELECT pg_relation_size('management.execution_outbox')/current_setting('block_size')::bigint"
            )
        )
        rows = session.execute(
            text("""SELECT o.event_id,o.ctid::text AS location
            FROM management.execution_outbox o
            WHERE o.ctid>CAST(:cursor AS tid) AND o.ctid<CAST(:high AS tid)
            AND o.delivered=true AND o.created_at<:cutoff
            -- No sequence: a legacy task event skipped by the relay, never replayable.
            AND (o.sequence IS NULL OR CAST(:floor AS bigint) IS NULL OR o.sequence<=CAST(:floor AS bigint))
            ORDER BY o.ctid LIMIT :batch FOR UPDATE OF o SKIP LOCKED"""),
            {
                "cursor": cursor if match else "(0,0)",
                "high": high,
                "cutoff": cutoff,
                "floor": floor,
                "batch": batch,
            },
        ).all()
        ids = [r.event_id for r in rows]
        if ids:
            session.execute(
                text(
                    "DELETE FROM management.execution_outbox WHERE event_id=ANY(CAST(:ids AS bigint[])) AND delivered=true"
                ),
                {"ids": ids},
            )
        session.commit()
        return ReceiptPurgeResult(
            len(ids),
            len(ids),
            next_cursor=rows[-1].location if len(rows) == batch else high,
            finished=len(rows) < batch and block + 128 >= blocks,
        )


def _retention_hours(session) -> int:
    """job_execution.event_retention_hours, read live from shared settings."""
    from ..job_system.resources import _persisted_job_execution_setting

    try:
        # A savepoint keeps a failed settings read from aborting the purge.
        with session.begin_nested():
            hours = _persisted_job_execution_setting("event_retention_hours", db_session=session)
    except Exception:
        hours = None
    return hours if hours and hours > 0 else DEFAULT_RETENTION_HOURS


def _plugin_event_floor(session) -> int | None:
    """Highest event sequence every enabled event-reading plugin has consumed.

    None when no such plugin exists. Events after it are kept regardless of age:
    plugins replay the outbox from their own durable cursors.
    """
    if not session.scalar(text("SELECT to_regclass('management.plugin_event_cursors') IS NOT NULL")):
        return None
    return session.scalar(text("""SELECT min(c.sequence) FROM management.plugin_event_cursors c
        JOIN management.plugin_installations i ON i.plugin_id=c.plugin_id AND i.enabled=1
        JOIN management.plugin_capability_grants g ON g.plugin_id=c.plugin_id AND g.capability='events.read'"""))


def _purge_partitioned_outbox(session, after, cutoff, *, floor=None, batch=IDLE_BATCH):
    # Sweep the compact global event index. A partitioned parent's physical
    # size is zero, and ctid alone is not unique across its leaves.
    candidates = session.execute(
        text(
            "SELECT event_id,run_id FROM management.execution_outbox_identities WHERE event_id>:after ORDER BY event_id LIMIT :batch"
        ),
        {"after": after, "batch": batch},
    ).all()
    if not candidates:
        return ReceiptPurgeResult(0, 0, next_cursor="event:0", finished=True)
    parameters = {
        "events": [r.event_id for r in candidates],
        "runs": [r.run_id for r in candidates],
        "cutoff": cutoff,
        "floor": floor,
    }
    rows = session.execute(
        text("""SELECT o.event_id,o.run_id FROM management.execution_outbox o
        JOIN unnest(CAST(:events AS bigint[]),CAST(:runs AS text[])) AS requested(event_id,run_id)
          ON o.event_id=requested.event_id AND o.run_id=requested.run_id
        WHERE o.delivered=true AND o.created_at<:cutoff
        -- No sequence: a legacy task event skipped by the relay, never replayable.
        AND (o.sequence IS NULL OR CAST(:floor AS bigint) IS NULL OR o.sequence<=CAST(:floor AS bigint))
        ORDER BY o.event_id FOR UPDATE OF o SKIP LOCKED"""),
        parameters,
    ).all()
    if rows:
        session.execute(
            text("""DELETE FROM management.execution_outbox o USING
            unnest(CAST(:events AS bigint[]),CAST(:runs AS text[])) AS requested(event_id,run_id)
            WHERE o.event_id=requested.event_id AND o.run_id=requested.run_id AND o.delivered=true"""),
            {"events": [r.event_id for r in rows], "runs": [r.run_id for r in rows]},
        )
    session.commit()
    return ReceiptPurgeResult(
        len(rows),
        len(rows),
        next_cursor=f"event:{candidates[-1].event_id}",
        finished=len(candidates) < batch,
    )


def reclaim_empty_outbox_partitions(*, mode="idle", session_factory=None, limit=16):
    """Return disk held by event partitions of finished runs that are now empty.

    Deleting a run's delivered events leaves its partition's pages allocated,
    and vacuum cannot always truncate them. A finished run's partition with no
    rows is truncated whole. Each partition is checked again under an exclusive
    lock in its own short transaction, so readers and running jobs are never
    blocked for long; busy partitions are left for a later cycle.
    """
    from sqlalchemy.exc import OperationalError, ProgrammingError
    from ultima_scraper_db_archive.global_db import global_session

    from .job_partitions import _owner_ddl

    if mode == "paused":
        return 0
    factory = session_factory or global_session
    with factory() as session:
        if not session.scalar(text("SELECT relkind='p' FROM pg_class WHERE oid=to_regclass('management.execution_outbox')")):
            return 0
        leaves = session.execute(
            text("""SELECT c.relname FROM pg_inherits i
            JOIN pg_class c ON c.oid=i.inhrelid
            JOIN management.execution_runs r
              ON pg_get_expr(c.relpartbound,c.oid)=format('FOR VALUES IN (%L)', r.run_id)
            WHERE i.inhparent='management.execution_outbox'::regclass
              AND r.state IN ('completed','failed','cancelled')
              AND pg_relation_size(c.oid) > 0
            ORDER BY pg_relation_size(c.oid) DESC LIMIT :limit"""),
            {"limit": limit},
        ).scalars().all()
        session.commit()
    reclaimed = 0
    for leaf in leaves:
        if not re.fullmatch(r"execution_outbox_job_[0-9a-f]{32}", leaf):
            continue
        with factory() as session:
            try:
                if not session.scalar(text(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended('execution-outbox-retention',0))"
                )):
                    return reclaimed
                session.execute(text("SET LOCAL lock_timeout='250ms'"))
                session.execute(text(f"LOCK TABLE management.{leaf} IN ACCESS EXCLUSIVE MODE"))
                if session.scalar(text(f"SELECT NOT EXISTS(SELECT 1 FROM management.{leaf})")):
                    if _owner_ddl(session):
                        # TRUNCATE needs the table owner; the runtime role may not be.
                        session.execute(
                            text("SELECT management.usr_truncate_outbox_partition(:leaf)"),
                            {"leaf": leaf},
                        )
                    else:
                        session.execute(text(f"TRUNCATE management.{leaf}"))
                    reclaimed += 1
                session.commit()
            except OperationalError:
                session.rollback()  # Busy: a reader or writer holds it now.
            except ProgrammingError as exc:
                # History retirement dropped the leaf after it was listed.
                if getattr(exc.orig, "sqlstate", None) != "42P01":
                    raise
                session.rollback()
    return reclaimed


async def run_outbox_retention_cycle(redis_manager, *, max_batches=32):
    from .threads import run_database_call

    mode = retention_mode()
    started = time.monotonic()
    reclaimed = await run_database_call(reclaim_empty_outbox_partitions, mode=mode)
    total = 0
    status = "budget_exhausted"
    reason = ""
    for _ in range(max_batches):
        protected = await pending_command_ids(redis_manager)
        raw = await redis_manager.client.get(CURSOR_KEY)
        cursor = raw.decode() if isinstance(raw, bytes) else raw
        result = await run_database_call(
            purge_outbox, cursor=cursor or "(0,0)", mode=mode, protected_ids=protected
        )
        total += result.deleted
        if result.paused_reason or result.busy:
            status = "paused" if result.paused_reason else "busy"
            reason = result.paused_reason or ""
            break
        await redis_manager.client.set(
            CURSOR_KEY, "(0,0)" if result.finished else result.next_cursor
        )
        if result.finished or time.monotonic() - started >= 5:
            status = "completed" if result.finished else "budget_exhausted"
            break
    await redis_manager.client.hset(
        STATUS_KEY,
        mapping={
            "mode": mode,
            "status": status,
            "paused_reason": reason,
            "deleted": total,
            "reclaimed_partitions": reclaimed,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            "updated_at": datetime.now(UTC).isoformat(),
        },
    )
    await redis_manager.client.expire(STATUS_KEY, 86400)
    return total
