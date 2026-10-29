"""SQL-checkpointed deletion, independent of the initiating HTTP connection."""

import asyncio
import json
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text

from ..database.threads import run_database_call
from ..database.receipt_retention import pending_write_inventory
from ..job_system.ledger import ExecutionLedger
from ..job_system.ledger_runtime import _ServiceRetry, database_unavailable
from ..job_system.storage import JobStorage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _DeletionPass:
    finished: tuple[str, ...] = ()
    advanced: bool = False


def _position(progress):
    # Ignore heartbeat/error timestamps: a blocked job must retain its backoff.
    progress = progress or {}
    return tuple(progress.get(key) for key in (
        "rows", "completed_stages", "partitions_dropped", "identity_cursor",
        "identity_sweep_cursor", "receipt_cursor", "receipt_sweep",
    ))


_PROGRESS = text("""SELECT config_json::jsonb->'metadata'->'deletion_progress'
    FROM management.execution_runs WHERE run_id=:id""")

# Finish one job before starting the next, so bulk deletions disappear one
# by one. A job that reported a blocker within BLOCKED_RETRY_SECONDS yields to
# the next request; checkpoint timestamps are local naive times.
_NEXT = text("""SELECT r.run_id FROM management.execution_runs r,
    LATERAL (SELECT r.config_json::jsonb->'metadata' AS meta) m
    WHERE m.meta->>'deletion_pending'='true'
    ORDER BY (m.meta->'deletion_progress'->>'error' IS NOT NULL
              AND (m.meta->'deletion_progress'->>'updated_at')::timestamp
                  > CAST(:blocked_since AS timestamp)),
             (m.meta->>'deletion_requested_at')::timestamptz NULLS LAST,
             r.completed_at, r.run_id
    LIMIT 1""")
BLOCKED_RETRY_SECONDS = 30

# Well below PgBouncer's idle_transaction_timeout (120s in production).
LEASE_HEARTBEAT_SECONDS = 30.0


@contextmanager
def _lease_heartbeat(lease):
    """Keep the lease transaction active while a pass works in other sessions.

    One budgeted pass can outlast the pooler's idle-transaction timeout. A
    killed lease releases the cluster lock mid-pass and admits a second
    deleter for the same job. The lease is not touched by the caller while
    this runs, so the heartbeat thread is its only user.
    """
    stop = threading.Event()

    def beat():
        while not stop.wait(LEASE_HEARTBEAT_SECONDS):
            try:
                lease.execute(text("SELECT 1"))
            except Exception:  # noqa: BLE001 - the caller's next lease read reports it
                logger.warning("Job deletion lease heartbeat failed", exc_info=True)
                return

    thread = threading.Thread(target=beat, name="job-deletion-lease", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


def _cleanup_pass(session_factory=None, *, receipt_queue=None):
    from ultima_scraper_db_archive.global_db import global_session

    factory = session_factory or global_session
    with factory() as lease:
        if not lease.scalar(
            text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('job-background-deletion',0))"
            )
        ):
            return _DeletionPass()
        blocked_since = datetime.now() - timedelta(seconds=BLOCKED_RETRY_SECONDS)
        ids = list(lease.scalars(_NEXT, {"blocked_since": blocked_since.isoformat()}))
        finished = []
        advanced = False
        ledger = ExecutionLedger(factory)
        for job_id in ids:
            try:
                before = _position(lease.scalar(_PROGRESS, {"id": job_id}))
                with _lease_heartbeat(lease):
                    deleted = ledger.delete(job_id, time_budget=5, receipt_queue=receipt_queue)
                if deleted:
                    finished.append(job_id)
                    advanced = True
                else:
                    after = _position(lease.scalar(_PROGRESS, {"id": job_id}))
                    advanced |= before != after
            except Exception as exc:
                # Do not open more database sessions to report a pool outage.
                # Already committed checkpoints remain authoritative.
                if database_unavailable(exc):
                    raise
                with factory() as session:
                    raw = session.scalar(
                        text(
                            "SELECT config_json FROM management.execution_runs WHERE run_id=:id"
                        ),
                        {"id": job_id},
                    )
                if raw:
                    saved = (
                        json.loads(raw).get("metadata", {}).get("deletion_progress", {})
                    )
                    ledger._save_deletion_progress(
                        job_id, {**saved, "error": str(exc)[:500]}
                    )
                logger.exception(
                    "Background deletion failed for %s; checkpoint retained", job_id
                )
        return _DeletionPass(tuple(finished), advanced)


def cleanup_pending(session_factory=None, *, receipt_queue=None):
    """Dispose one bounded pass and return completed job IDs."""
    return list(_cleanup_pass(session_factory, receipt_queue=receipt_queue).finished)


async def run_deletion_worker(redis_manager):
    retry = _ServiceRetry("job deletion")
    while True:
        try:
            inventory = None
            if redis_manager is not None:
                try:
                    inventory = await pending_write_inventory(redis_manager)
                except Exception:
                    # Keep the missing snapshot explicit: history and receipt
                    # disposal fail closed until queue inspection succeeds.
                    logger.warning("Job receipt cleanup waiting for writer queue inventory", exc_info=True)
            result = await run_database_call(_cleanup_pass, receipt_queue=inventory)
            retry.succeeded()
            for job_id in result.finished:
                await JobStorage(redis_manager).delete_job(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - service preserves retry/shutdown
            await retry.failed(exc)
            continue
        # Refresh the queue inventory and reacquire the cluster lease between
        # passes, but do not spend two seconds idle while cleanup advances.
        await asyncio.sleep(0 if result.advanced else 2)
