"""Bound SQL receipt history without dropping queued or retained recovery work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from ultima_scraper_core import runtime_env_value

REPLAYABLE_UPSERTS = frozenset({"upsert_user", "upsert_invalid_user"})
# A receipt matters only briefly: a producer waits at most 15 minutes for its
# result, and a writer that crashed after committing replays its still-queued
# commands. Older envelopes cannot repeat a mutation without one: upserts are
# idempotent, and run-owned commands fail their attempt fence once the run or
# task is no longer active. Queued commands are protected separately.
RETENTION_SETTING = "receipt_retention_minutes"
DEFAULT_RETENTION_MINUTES = 60
MINIMUM_RETENTION_MINUTES = 15
RECEIPT_BATCH_SIZE = 512
RETENTION_STATUS_KEY = "ultimascraper:database:receipt-retention:status"


def retention_mode() -> str:
    value = runtime_env_value("ULTIMA_DATABASE_WRITE_RECEIPT_CLEANUP_MODE") or "idle"
    if value not in {"idle", "paused", "always"}:
        raise ValueError("Receipt cleanup mode must be idle, paused, or always")
    return value


def retention_minutes(session) -> int:
    """job_execution.receipt_retention_minutes, read live from shared settings."""
    from ..job_system.resources import _persisted_job_execution_setting

    try:
        # A savepoint keeps a failed settings read from aborting the purge.
        with session.begin_nested():
            minutes = _persisted_job_execution_setting(RETENTION_SETTING, db_session=session)
    except Exception:
        minutes = None
    if minutes is None:
        return DEFAULT_RETENTION_MINUTES
    return max(MINIMUM_RETENTION_MINUTES, minutes)


async def pending_command_ids(redis_manager, max_entries: int = 10000) -> set[str]:
    """Inspect the complete bounded accepted stream, including its pending list.

    Incomplete or malformed inventory aborts cleanup instead of treating it as
    an empty queue. Producers can enqueue during the inventory; old envelopes
    without receipts are rejected by the writer, and live runs stay protected.
    """
    commands = await _pending_commands(redis_manager, max_entries)
    return {command.command_id for command in commands}


async def pending_run_ids(redis_manager, max_entries: int = 10000) -> set[str]:
    """Runs that still own queued writer commands, under the same inventory rules."""
    commands = await _pending_commands(redis_manager, max_entries)
    return {
        command.execution_attempt["run_id"]
        for command in commands
        if command.execution_attempt
    }


@dataclass(frozen=True)
class ReceiptQueueSnapshot:
    command_ids: frozenset[str] = frozenset()
    run_ids: frozenset[str] = frozenset()


async def pending_write_inventory(redis_manager, max_entries: int = 10000) -> ReceiptQueueSnapshot:
    """Inspect accepted and in-flight writes once for job-deletion protection."""
    commands = await _pending_commands(redis_manager, max_entries)
    return ReceiptQueueSnapshot(
        frozenset(command.command_id for command in commands),
        frozenset(command.execution_attempt["run_id"] for command in commands
                  if command.execution_attempt),
    )


async def _pending_commands(redis_manager, max_entries: int):
    from .write_queue import (
        DatabaseWriteClient,
        DatabaseWriterService,
        parse_partition_layout,
    )

    client = redis_manager.client
    if client is None:
        raise RuntimeError("Receipt cleanup requires Redis queue inspection")
    current, target = parse_partition_layout(
        await client.get(DatabaseWriteClient.LAYOUT_KEY)
    )
    rows = []
    for stream in (
        *(
            DatabaseWriteClient.partition_keys(partition)[0]
            for partition in range(max(current, target or 1))
        ),
        DatabaseWriteClient.PRIORITY_STREAM_KEY,
    ):
        rows.extend(await client.xrange(stream, count=max_entries + 1 - len(rows)))
        if len(rows) > max_entries:
            raise RuntimeError(
                "Receipt cleanup queue inventory exceeds its safety budget"
            )
    return [DatabaseWriterService._parse_entry(row)[1] for row in rows]


# A finished run's commands fail their attempt fence, so its receipts go once
# its idempotency mutations are retired; live runs keep theirs.
_ELIGIBLE = """
 r.committed_at < :cutoff
 AND r.failed_command_json IS NULL
 AND r.result_json::jsonb->>'success' = 'true'
 AND NOT (r.command_id = ANY(CAST(:protected AS text[])))
 AND ((r.run_id IS NULL AND r.operation IN ('upsert_user','upsert_invalid_user'))
      OR (r.run_id IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM management.execution_runs live WHERE live.run_id=r.run_id
            AND NOT (live.state IN ('completed','failed','cancelled')
                     AND live.completed_at < :cutoff))))
 AND NOT EXISTS (SELECT 1 FROM management.execution_mutations m WHERE m.command_id=r.command_id)
"""


@dataclass(frozen=True)
class ReceiptPurgeResult:
    selected: int
    deleted: int
    busy: bool = False
    next_cursor: str | None = None
    finished: bool = False
    paused_reason: str | None = None


def purge_receipts(
    *,
    protected_ids: set[str],
    batch_size: int = RECEIPT_BATCH_SIZE,
    cutoff: datetime | None = None,
    dry_run: bool = False,
    session_factory=None,
    cursor: str | None = None,
    mode: str = "always",
) -> ReceiptPurgeResult:
    """Purge one bounded batch, rechecking references after writer-lock admission."""
    from ultima_scraper_db_archive.global_db import global_session

    from .write_receipts import DatabaseWriteReceiptBusy, _lock

    # Each command needs a writer advisory lock. Large row-delete batches
    # exhaust PostgreSQL's shared lock table even when row locks would fit.
    if batch_size < 1 or batch_size > RECEIPT_BATCH_SIZE:
        raise ValueError("Receipt purge batch_size must be between 1 and 512")
    if mode not in {"idle", "paused", "always"}:
        raise ValueError("Receipt cleanup mode must be idle, paused, or always")
    if not dry_run and (mode == "paused" or (mode == "idle" and protected_ids)):
        return ReceiptPurgeResult(
            0,
            0,
            paused_reason="operator_paused"
            if mode == "paused"
            else "write_queue_active",
        )
    if cutoff is not None and (
        cutoff.tzinfo is None
        or cutoff > datetime.now(UTC) - timedelta(minutes=MINIMUM_RETENTION_MINUTES)
    ):
        raise ValueError("Receipt cutoff must preserve the configured recovery window")
    with (session_factory or global_session)() as session:
        ceiling = datetime.now(UTC) - timedelta(minutes=retention_minutes(session))
        cutoff = cutoff or ceiling
        if cutoff.tzinfo is None or cutoff > ceiling:
            raise ValueError("Receipt cutoff must preserve the configured recovery window")
        parameters = {
            "cutoff": cutoff,
            "protected": sorted(protected_ids),
            "limit": batch_size,
        }
        # Multiple APIs/operators must not purge overlapping receipt batches.
        if not session.scalar(
            text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('database-receipt-retention',0))"
            )
        ):
            return ReceiptPurgeResult(0, 0, True)
        session.execute(text("SET LOCAL statement_timeout='30s'"))
        # A paused run executes nothing once its in-flight tasks have finished,
        # so it does not hold cleanup back while it waits to be resumed.
        if (
            mode == "idle"
            and not dry_run
            and session.scalar(
                text("""SELECT
          EXISTS(SELECT 1 FROM management.execution_runs r
          WHERE r.state='running' OR (r.state='pending' AND r.next_at<=clock_timestamp())
          OR (r.state='paused' AND EXISTS(SELECT 1 FROM management.execution_work w WHERE w.run_id=r.run_id AND w.state IN ('running','persisting'))))""")
            )
        ):
            return ReceiptPurgeResult(0, 0, paused_reason="execution_active")
        next_cursor = None
        finished = False
        if cursor is None:
            ids = list(
                session.scalars(
                    text(
                        "SELECT r.command_id FROM management.database_write_receipts r WHERE "
                        + _ELIGIBLE
                        + " LIMIT :limit"
                    ),
                    parameters,
                )
            )
        else:
            import re

            match = re.fullmatch(r"\((\d+),(\d+)\)", cursor)
            if not match:
                raise ValueError("Invalid receipt sweep cursor")
            block = int(match[1])
            blocks = session.scalar(
                text(
                    "SELECT pg_relation_size('management.database_write_receipts')/current_setting('block_size')::bigint"
                )
            )
            high = f"({block + 128},0)"
            rows = session.execute(
                text(
                    "SELECT r.command_id,r.ctid::text AS location FROM management.database_write_receipts r WHERE "
                    + _ELIGIBLE
                    + " AND r.ctid>CAST(:cursor AS tid) AND r.ctid<CAST(:high AS tid) ORDER BY r.ctid LIMIT :limit"
                ),
                {**parameters, "cursor": cursor, "high": high},
            ).all()
            ids = [row.command_id for row in rows]
            # Advance through empty/protected ranges too. A later sweep starts
            # from zero so newly eligible or concurrently moved rows are revisited.
            next_cursor = rows[-1].location if len(rows) == batch_size else high
            finished = len(rows) < batch_size and block + 128 >= blocks
        if not ids or dry_run:
            return ReceiptPurgeResult(
                len(ids), 0, next_cursor=next_cursor, finished=finished
            )
        try:
            _lock(session, ids, defer_if_busy=True)
        except DatabaseWriteReceiptBusy:
            return ReceiptPurgeResult(len(ids), 0, True)
        result = session.execute(
            text(
                "DELETE FROM management.database_write_receipts r WHERE r.command_id=ANY(CAST(:ids AS text[])) AND "
                + _ELIGIBLE
            ),
            {**parameters, "ids": ids},
        )
        session.commit()
        return ReceiptPurgeResult(
            len(ids), result.rowcount, next_cursor=next_cursor, finished=finished
        )


async def run_retention_cycle(redis_manager, *, max_batches: int = 128) -> int:
    import time

    from .threads import run_database_call

    total = 0
    mode = retention_mode()
    status = "idle"
    paused_reason = ""
    batches = 0
    started = time.monotonic()
    cursor_key = "ultimascraper:database:receipt-retention:cursor"
    for _ in range(max_batches):
        protected = await pending_command_ids(redis_manager)
        raw_cursor = await redis_manager.client.get(cursor_key)
        cursor = raw_cursor.decode() if isinstance(raw_cursor, bytes) else raw_cursor
        result = await run_database_call(
            purge_receipts, protected_ids=protected, cursor=cursor or "(0,0)", mode=mode
        )
        total += result.deleted
        if result.paused_reason:
            status, paused_reason = "paused", result.paused_reason
            break
        if result.busy:
            status = "busy"
            break
        batches += 1
        status = "running"
        await redis_manager.client.set(
            cursor_key, "(0,0)" if result.finished else result.next_cursor
        )
        if result.finished or time.monotonic() - started >= 15:
            status = "completed" if result.finished else "budget_exhausted"
            break
    if status == "running":
        status = "budget_exhausted"
    await redis_manager.client.hset(
        RETENTION_STATUS_KEY,
        mapping={
            "mode": mode,
            "status": status,
            "paused_reason": paused_reason,
            "batches": batches,
            "deleted": total,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            "updated_at": datetime.now(UTC).isoformat(),
        },
    )
    await redis_manager.client.expire(RETENTION_STATUS_KEY, 86400)
    return total
