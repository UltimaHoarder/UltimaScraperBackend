"""Fair queueing between run controls and the run's hot shared fences.

Controls (pause, resume, cancel, delete stages, admission) lock a run row
exclusively. Writers, checkpoints, finishes, the planner and the archiver lock
the same row in share mode, back to back. PostgreSQL row locks have no queue:
a share locker may join while an exclusive request waits, so on a busy run a
control can wait indefinitely.

With ULTIMA_LEDGER_RUN_TURNSTILE=1 every hot share locker first takes a shared
transaction advisory lock for the run, and a control first takes the exclusive
one. Heavyweight locks are granted in order: once a control waits, new share
lockers queue behind it (blocking sites) or skip the run for this pass
(SKIP LOCKED sites), the current holders finish, and the control gets in.
Both locks end with their transaction.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

from sqlalchemy import func, literal, text, true

TURNSTILE_ENV = "ULTIMA_LEDGER_RUN_TURNSTILE"
_PREFIX = "usr-run-turnstile:"

_LOCK_SHARED = text(
    "SELECT count(pg_advisory_xact_lock_shared(hashtextextended(:prefix || r, 0))) "
    "FROM (SELECT DISTINCT r FROM unnest(CAST(:runs AS text[])) AS u(r) ORDER BY r) s"
)
_LOCK_EXCLUSIVE = text("SELECT pg_advisory_xact_lock(hashtextextended(:prefix || :run, 0))")


def enabled() -> bool:
    return os.environ.get(TURNSTILE_ENV, "").strip() == "1"


def lock_shared(session: Any, run_ids: Iterable[str]) -> None:
    """Pass the turnstile before share-locking these runs' rows (blocking)."""
    runs = sorted({str(run_id) for run_id in run_ids})
    if runs and enabled():
        session.execute(_LOCK_SHARED, {"prefix": _PREFIX, "runs": runs})


def lock_exclusive(session: Any, run_id: str) -> None:
    """Close the turnstile before locking the run row exclusively."""
    if enabled():
        session.execute(_LOCK_EXCLUSIVE, {"prefix": _PREFIX, "run": str(run_id)})


def try_shared(run_id_column: Any) -> Any:
    """WHERE clause for SKIP LOCKED share lockers: skip a run a control waits for."""
    if not enabled():
        return true()
    return func.pg_try_advisory_xact_lock_shared(
        func.hashtextextended(literal(_PREFIX).op("||")(run_id_column), 0)
    )
