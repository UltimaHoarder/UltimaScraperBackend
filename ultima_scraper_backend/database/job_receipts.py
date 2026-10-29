"""Bounded disposal of receipts exclusively owned by a fenced, deleting job."""

import re
from dataclasses import dataclass

from sqlalchemy import text

from .receipt_retention import RECEIPT_BATCH_SIZE, ReceiptQueueSnapshot
from .write_receipts import _lock


class JobReceiptCleanupDeferred(Exception):
    """Keep the job and checkpoint until a receipt-cleanup prerequisite clears."""


@dataclass(frozen=True)
class JobReceiptPage:
    deleted: int
    cursor: str | None
    finished: bool
    # Physical sweeps only: the next checkpointed sweep state, and whether the
    # page moved it (an empty heap window still advances the sweep).
    sweep: dict | None = None
    advanced: bool = False


# Command IDs are content hashes, so an owner's command-ordered page touches one
# random heap page per receipt. Large owners are swept in physical order instead
# when their receipts are concentrated in a heap range (they are written while
# the job runs). Estimates only choose the strategy and the range; every page
# keeps the same eligibility, row locks, writer locks and completion check.
RECEIPT_SWEEP_MIN_ROWS = 50_000
RECEIPT_SWEEP_SAMPLE = 4096
RECEIPT_SWEEP_PAGES = 1024
# Widen the sampled range; receipts outside it are removed by the tail pass.
RECEIPT_SWEEP_MARGIN = 0.01


_READ_PLANNER = text("""SELECT
    current_setting('enable_indexscan') AS indexscan,
    current_setting('enable_indexonlyscan') AS indexonlyscan,
    current_setting('enable_seqscan') AS seqscan,
    current_setting('enable_bitmapscan') AS bitmapscan""")
_SET_PLANNER = text("""SELECT
    set_config('enable_indexscan',:indexscan,true),
    set_config('enable_indexonlyscan',:indexonlyscan,true),
    set_config('enable_seqscan',:seqscan,true),
    set_config('enable_bitmapscan',:bitmapscan,true)""")


def _receipts_remaining(session, eligible, params):
    # A completion check starts at the owner's first key, which can mean
    # millions of dead index entries pointing to randomly ordered heap pages.
    # EXISTS underestimates this startup cost after a large purge. A bitmap
    # scan groups those reads by heap page. Scope this choice to this check;
    # ordinary pages retain their indexed command seeks and reference checks.
    original = dict(session.execute(_READ_PLANNER).mappings().one())
    session.execute(_SET_PLANNER, {
        "indexscan": "off", "indexonlyscan": "off",
        "seqscan": "off", "bitmapscan": "on",
    })
    remaining = session.scalar(text("""SELECT EXISTS(SELECT 1
        FROM management.database_write_receipts r WHERE """ + eligible + ")"), params)
    # On an error the caller rolls back this page and its transaction-local
    # settings together. Restore successful checks before saving progress.
    session.execute(_SET_PLANNER, original)
    return remaining


def _eligibility(session, run_id, limit, queue):
    """Check every precondition and return the shared eligibility predicate."""
    if not 1 <= limit <= RECEIPT_BATCH_SIZE:
        raise ValueError("Receipt deletion batch must be between 1 and 512")
    fenced = session.scalar(text("""SELECT state IN ('completed','failed','cancelled')
        AND config_json::jsonb->'metadata'->>'deletion_pending'='true'
        FROM management.execution_runs WHERE run_id=:run"""), {"run": run_id})
    if not fenced:
        raise ValueError("Receipt disposal requires a terminal fenced run")
    if queue is None:
        raise JobReceiptCleanupDeferred("Waiting for a complete database writer queue inventory.")
    if run_id in queue.run_ids:
        raise JobReceiptCleanupDeferred("Waiting for this job's queued database writes to drain.")
    if not session.scalar(text("""SELECT EXISTS(SELECT 1 FROM pg_index
        WHERE indexrelid=to_regclass('management.idx_write_receipts_run_command')
          AND indisvalid AND indisready)""")):
        raise JobReceiptCleanupDeferred(
            "Receipt cleanup requires database migration 6f4b2e9a17c0 (job receipt index)."
        )
    # Keep the reference lookup correlated. A merge anti-join otherwise walks
    # the legacy mutation index from its beginning for every receipt page,
    # even though the receipt cursor itself seeks forward correctly.
    eligible = """r.run_id=:run
        AND NOT (r.command_id=ANY(CAST(:protected AS text[])))
        AND NOT EXISTS (SELECT 1 FROM management.execution_mutations m
                        WHERE m.command_id=r.command_id OFFSET 0)"""
    params = {"run": run_id, "limit": limit,
              "protected": sorted(queue.command_ids)}
    return eligible, params


def purge_job_receipts(session, run_id, *, limit=RECEIPT_BATCH_SIZE, after=None,
                       queue: ReceiptQueueSnapshot | None = None):
    """Caller holds the run FOR UPDATE; commit this page with its checkpoint.

    Explicit deletion removes recent and failed receipts too. Queued writes for
    this run drain first. Other commands' receipts and shared mutation references
    remain protected. Try writer locks rather than waiting in reverse lock order.
    """
    eligible, params = _eligibility(session, run_id, limit, queue)
    seek = " AND r.command_id>:after" if after is not None else ""
    ids = list(session.scalars(text("""SELECT r.command_id
        FROM management.database_write_receipts r WHERE """ + eligible + seek + """
        ORDER BY r.command_id LIMIT :limit FOR UPDATE OF r SKIP LOCKED"""),
        {**params, "after": after}))
    deleted = 0
    if ids:
        _lock(session, ids, defer_if_busy=True)
        deleted = session.execute(text("""DELETE FROM management.database_write_receipts r
            WHERE r.command_id=ANY(CAST(:ids AS text[])) AND """ + eligible),
            {**params, "ids": ids}).rowcount
    if len(ids) == limit:
        return JobReceiptPage(deleted, ids[-1], False)
    # A short page can have skipped row locks or protected references that
    # changed while acquiring writer locks. Rewind before marking it complete.
    remaining = _receipts_remaining(session, eligible, params)
    return JobReceiptPage(deleted, None, not remaining)


def plan_receipt_sweep(session, run_id, *, after=None):
    """Return a physical sweep state for a large owner, or None for key pages.

    Rows after a command cursor are a uniform sample of the remaining receipts
    (command IDs are hashes), so their heap blocks bound where the rest live.
    """
    plan = session.scalar(text("""EXPLAIN (FORMAT JSON) SELECT 1
        FROM management.database_write_receipts r WHERE r.run_id=:run"""
        + (" AND r.command_id>:after" if after is not None else "")),
        {"run": run_id, "after": after})
    if plan[0]["Plan"]["Plan Rows"] < RECEIPT_SWEEP_MIN_ROWS:
        return None
    low, high = session.execute(text("""SELECT min(block), max(block) FROM (
        SELECT (r.ctid::text::point)[0]::bigint AS block
        FROM management.database_write_receipts r WHERE r.run_id=:run"""
        + (" AND r.command_id>:after" if after is not None else "")
        + " ORDER BY r.command_id LIMIT :sample) sample"),
        {"run": run_id, "after": after, "sample": RECEIPT_SWEEP_SAMPLE}).one()
    if low is None:
        return None
    margin = int((high - low) * RECEIPT_SWEEP_MARGIN) + 1
    return {"cursor": f"({max(low - margin, 0)},0)", "end": high + margin + 1}


def _delete_located(session, eligible, params, locations):
    """Lock eligible candidates by location, then delete them under writer locks."""
    if not locations:
        return 0
    rows = session.execute(text("""SELECT r.command_id, r.ctid::text
        FROM management.database_write_receipts r
        WHERE r.ctid=ANY(CAST(:locations AS tid[])) AND """ + eligible + """
        ORDER BY r.command_id FOR UPDATE OF r SKIP LOCKED"""),
        {**params, "locations": locations}).all()
    if not rows:
        return 0
    ids = [command_id for command_id, _ in rows]
    _lock(session, ids, defer_if_busy=True)
    # Row locks pin these locations to exactly the writer-locked commands until
    # commit. Delete by location alone: an indexable command-ID list lets the
    # planner pick a random index leaf per hashed command ID over the heap
    # pages just read.
    return session.execute(text("""DELETE FROM management.database_write_receipts r
        WHERE r.ctid=ANY(CAST(:locations AS tid[])) AND """ + eligible),
        {**params, "locations": [location for _, location in rows]}).rowcount


def purge_job_receipts_sweep(session, run_id, *, sweep, limit=RECEIPT_BATCH_SIZE,
                             queue: ReceiptQueueSnapshot | None = None):
    """Delete one physically ordered page; checkpoint the returned sweep state.

    The range pass reads bounded heap windows in order. A tail pass then finds
    receipts outside the sampled range, or skipped by row locks, with a bitmap
    scan. Completion still requires the owner-wide eligibility check.
    """
    eligible, params = _eligibility(session, run_id, limit, queue)
    if not sweep.get("tail"):
        match = re.fullmatch(r"\((\d+),(\d+)\)", sweep["cursor"])
        if not match:
            raise ValueError("Invalid receipt sweep cursor")
        high = min(int(match[1]) + RECEIPT_SWEEP_PAGES, int(sweep["end"]))
        # Select candidates before the reference checks: in heap order their
        # command IDs are random, so checking a whole window costs a random
        # mutation-index read for every row the LIMIT would discard.
        locations = list(session.scalars(text("""SELECT r.ctid::text
            FROM management.database_write_receipts r WHERE r.run_id=:run
              AND r.ctid>CAST(:after AS tid) AND r.ctid<CAST(:high AS tid)
            ORDER BY r.ctid LIMIT :limit"""),
            {"run": run_id, "after": sweep["cursor"], "high": f"({high},0)", "limit": limit}))
        deleted = _delete_located(session, eligible, params, locations)
        if len(locations) == limit:
            following = {**sweep, "cursor": locations[-1]}
        elif high < int(sweep["end"]):
            following = {**sweep, "cursor": f"({high},0)"}
        else:
            following = {"tail": True}
        return JobReceiptPage(deleted, None, False, following, True)
    original = dict(session.execute(_READ_PLANNER).mappings().one())
    session.execute(_SET_PLANNER, {
        "indexscan": "off", "indexonlyscan": "off",
        "seqscan": "off", "bitmapscan": "on",
    })
    locations = list(session.scalars(text("""SELECT r.ctid::text
        FROM management.database_write_receipts r WHERE """ + eligible + """
        LIMIT :limit"""), params))
    session.execute(_SET_PLANNER, original)
    deleted = _delete_located(session, eligible, params, locations)
    if len(locations) == limit:
        return JobReceiptPage(deleted, None, False, sweep, False)
    remaining = _receipts_remaining(session, eligible, params)
    return JobReceiptPage(deleted, None, not remaining, sweep, False)
