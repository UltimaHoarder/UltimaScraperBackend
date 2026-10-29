"""PostgreSQL selection snapshots and checkpoints for bounded MPS execution.

Full ID sets are never materialized in Python. Sparse selections remain
disk-backed, and every reader uses a keyset and limit.
"""

from __future__ import annotations

import json
import logging
import time
from bisect import bisect_left
from collections.abc import Callable, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from itertools import groupby
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import func, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from ultima_scraper_core import runtime_env_value
from ultima_scraper_db_archive.global_db import (
    CentralJobModel,
    CentralTaskModel,
    MPSCandidateRangeModel,
    MPSPlanModel,
    MPSTaskDefinitionModel,
    global_session,
)


@lru_cache(maxsize=64)
def _archive_insert_statement(columns: tuple[str, ...], conflict: tuple[str, ...]):
    """One stable typed-array INSERT for each bounded archive column shape."""
    table = CentralTaskModel.__table__
    dialect = postgres_dialect()
    quote = dialect.identifier_preparer.quote
    names = ", ".join(quote(name) for name in columns)
    arrays = ", ".join(
        f"CAST(:rows_{name} AS {table.c[name].type.compile(dialect=dialect)}[])"
        for name in columns
    )
    conflicts = ", ".join(quote(name) for name in conflict)
    updates = ", ".join(
        f"{quote(column.name)}=EXCLUDED.{quote(column.name)}"
        for column in table.columns
        if column.name != "task_id"
    )
    return text(
        f"INSERT INTO management.job_tasks ({names}) "
        f"SELECT * FROM unnest({arrays}) AS incoming WHERE true "
        f"ON CONFLICT ({conflicts}) DO UPDATE SET {updates}"
    )


_SET_TRUSTED_IDENTITY = text(
    "SELECT set_config('ultima.trusted_mps_identity', :value, true)"
)
_ENABLED = {"1", "true", "on", "yes"}


def trusted_mps_identity_enabled() -> bool:
    """ULTIMA_ARCHIVE_TRUSTED_MPS_IDENTITY=1/true/on/yes enables the bypass.

    Off by default. Read per archive batch from the process environment, then
    the shared env file, so a file change applies without a restart unless the
    process environment already sets the variable.
    """
    value = runtime_env_value("ULTIMA_ARCHIVE_TRUSTED_MPS_IDENTITY")
    return value is not None and value.strip().lower() in _ENABLED


class PhaseTimer:
    """Accumulate elapsed milliseconds per named phase into an optional dict.

    Each call closes the phase that began at the previous call (or creation).
    Without a dict it only reads the clock, so callers need no branches.
    """

    def __init__(self, timings: dict[str, float] | None, start: float | None = None):
        self.timings = timings
        self.mark = time.perf_counter() if start is None else start

    def __call__(self, phase: str) -> None:
        now = time.perf_counter()
        if self.timings is not None:
            elapsed = (now - self.mark) * 1000
            self.timings[phase] = round(self.timings.get(phase, 0.0) + elapsed, 3)
        self.mark = now

    def skip(self) -> None:
        """Start the next phase now, excluding time a callee recorded itself."""
        self.mark = time.perf_counter()


@dataclass(frozen=True)
class ConcurrentArchive:
    """History inserted by ``archive_concurrent``, awaiting ``archive_checkpoint``."""

    rows: list[dict[str, Any]]
    positions: dict[str, int]
    manifest_id: str | None
    source_manifest: str | None


def task_source_manifest(selection: dict[str, Any], offset: int) -> str | None:
    """Find the explicit source after an optional compressed candidate prefix."""
    manifest_id = selection.get("task_manifest_id")
    if manifest_id is not None:
        return manifest_id
    if offset >= int(selection.get("candidate_total", 0)):
        return selection.get("spawned_manifest_id")
    return None


# Executed as one statement: the header, counts, offset/limit and candidate
# membership all observe the same PostgreSQL snapshot.
_SELECT_MANIFEST = """
WITH existing AS (
    SELECT id, last_checked_at FROM users WHERE id BETWEEN :start_id AND :end_id
), invalid AS (
    SELECT user_id, COALESCE(updated_at, last_checked_at) AS checked_at
    FROM invalid_users WHERE site = :site AND user_id BETWEEN :start_id AND :end_id
), known AS (
    -- Duplicate IDs are safe: LAG produces no positive gap for equal IDs.
    SELECT CAST(:start_id AS numeric) - 1 AS id
    UNION ALL SELECT id FROM existing
    UNION ALL SELECT user_id FROM invalid
    UNION ALL SELECT CAST(:end_id AS numeric) + 1
), raw_gaps AS (
    SELECT LAG(id) OVER (ORDER BY id) + 1 AS start_id, id - 1 AS end_id FROM known
), missing AS (
    SELECT CAST(start_id AS bigint) AS start_id, CAST(end_id AS bigint) AS end_id
    FROM raw_gaps WHERE start_id <= end_id AND :include_missing
), eligible_invalid AS (
    SELECT user_id FROM invalid
    WHERE :include_invalid
      AND NOT EXISTS (SELECT 1 FROM existing WHERE existing.id = invalid.user_id)
      AND (CAST(:invalid_cutoff AS timestamptz) IS NULL OR checked_at IS NULL OR checked_at <= :invalid_cutoff)
), eligible_existing AS (
    SELECT id FROM existing WHERE :include_existing
      AND (CAST(:existing_cutoff AS timestamptz) IS NULL OR last_checked_at IS NULL OR last_checked_at <= :existing_cutoff)
), segments AS (
    SELECT start_id, end_id FROM missing
    UNION ALL SELECT user_id, user_id FROM eligible_invalid
    UNION ALL SELECT id, id FROM eligible_existing
), ordered AS (
    SELECT start_id, end_id,
        SUM(end_id - start_id + 1) OVER (ORDER BY start_id ROWS UNBOUNDED PRECEDING)
            - (end_id - start_id + 1) AS first_ordinal
    FROM segments
), summary AS (
    SELECT
        COALESCE((SELECT SUM(end_id - start_id + 1) FROM missing), 0) AS missing_count,
        (SELECT COUNT(*) FROM eligible_invalid) AS invalid_eligible_count,
        (SELECT COUNT(*) FROM eligible_existing) AS existing_eligible_count,
        COALESCE((SELECT SUM(end_id - start_id + 1) FROM segments), 0) AS candidate_count
), bounds AS (
    SELECT *, GREATEST(LEAST(candidate_count - :candidate_offset,
                            COALESCE(CAST(:max_profiles AS bigint), candidate_count)), 0) AS total
    FROM summary
), plan_insert AS (
    INSERT INTO management.mps_plans
        (job_id, site, job_config_json, selection_json, proxy_pool_json, total, summary_json)
    SELECT :job_id, :site, :job_config, :selection, :proxy_pool, total,
        json_build_object('missing_count', missing_count,
            'invalid_eligible_count', invalid_eligible_count,
            'existing_eligible_count', existing_eligible_count,
            'candidate_count', candidate_count)::text
    FROM bounds WHERE total > 0
    RETURNING job_id, total, summary_json
), clipped AS (
    SELECT ordered.*, GREATEST(first_ordinal, :candidate_offset) AS lo,
        LEAST(first_ordinal + end_id - start_id + 1,
              :candidate_offset + plan_insert.total) AS hi
    FROM ordered CROSS JOIN plan_insert
), ranges_insert AS (
    INSERT INTO management.mps_candidate_ranges (job_id, first_ordinal, start_id, end_id)
    SELECT :job_id, lo - :candidate_offset,
        start_id + lo - first_ordinal, start_id + hi - first_ordinal - 1
    FROM clipped WHERE hi > lo
    RETURNING first_ordinal
)
SELECT total, summary_json FROM plan_insert
"""


# An offset-0 missing-only plan proves that every ID between its selection
# start and its first candidate was known when it was frozen. Users and invalid
# rows accumulate (no runtime path deletes them), so that prefix stays known
# and is skipped instead of recounted; on a large archive the dense low-ID
# prefix is most of the count. Retries keep their source selection but hold
# failed-ID ranges, so they never supply a hint.
_SELECT_KNOWN_PREFIX_END = """
SELECT max(r.start_id) FROM management.mps_plans p
JOIN management.mps_candidate_ranges r ON r.job_id = p.job_id AND r.first_ordinal = 0
WHERE p.site = :site AND r.start_id > :start_id
  AND NOT COALESCE(p.job_config_json::jsonb -> 'metadata' ? 'retry_of', false)
  AND NOT (p.selection_json::jsonb ?| array['task_manifest_id', 'spawned_manifest_id'])
  AND (p.selection_json::jsonb ->> 'start_id')::bigint <= :start_id
  AND COALESCE((p.selection_json::jsonb ->> 'candidate_offset')::bigint, 0) = 0
  AND NOT COALESCE((p.selection_json::jsonb ->> 'ignore_missing_ids')::boolean, false)
  AND NOT COALESCE((p.selection_json::jsonb ->> 'include_invalid')::boolean, false)
  AND NOT COALESCE((p.selection_json::jsonb ->> 'include_existing_users')::boolean, false)
"""

_MPS_COVERAGE_ID_SPAN = 1_000_000
# Coarse blocks are counted first; only a coarse block that overlaps the
# selected window is subdivided into fine blocks. Selection outside the window
# never transfers IDs, so work follows the page size, not the ID range.
_MPS_COVERAGE_FANOUT = 16
_MPS_ID_STREAM_PAGE_SIZE = 16384
_MPS_RANGE_INSERT_BATCH_SIZE = 1000


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


_UNSPECIFIED = object()


class NoMPSCandidates(ValueError):
    """The snapshot contains no candidates at the requested offset."""


def mps_task_id(job_id: str, ordinal: int, user_id: int) -> str:
    return f"task-{uuid5(NAMESPACE_URL, f'ultimascraper:mps:{job_id}:{ordinal}:{user_id}').hex}"


class _WindowFilled(Exception):
    """The finite window is selected and at least one candidate follows it."""


def _create_missing_only_plan(
    session: Any, params: dict[str, Any]
) -> tuple[int, str] | None:
    """Create exact ranges using current coverage in the transaction snapshot.

    Count known IDs per block. A block outside the requested ordinal window
    contributes only its count. A partly populated block inside the window is
    subdivided, and only fine blocks inside the window stream their known IDs.
    Overlapping identities count once.

    A finite window stops once it is selected and one later candidate is
    proven. Counting the rest of a large range reads every index entry in it
    (minutes on a cold archive), so its total is reported from the caller's
    analysis hint and marked inexact. Unlimited selections count everything.
    """
    start_id = int(params["start_id"])
    end_id = int(params["end_id"])
    candidate_offset = int(params["candidate_offset"] or 0)
    max_profiles = params["max_profiles"]
    range_size = max(end_id - start_id + 1, 0)
    if range_size <= 0:
        return None

    started = time.monotonic()
    coverage_ms = scan_ms = 0.0
    count_queries = streamed_blocks = streamed_ids = 0
    page_end = (
        candidate_offset + int(max_profiles)
        if max_profiles is not None
        else None
    )
    fine_span = _MPS_COVERAGE_ID_SPAN
    missing_count = 0  # Missing IDs before the block being visited.
    known_count = 0
    selected = 0
    plan: MPSPlanModel | None = None
    pending_ranges: list[dict[str, int | str]] = []

    def ensure_plan() -> None:
        nonlocal plan
        if plan is None:
            plan = MPSPlanModel(
                job_id=params["job_id"],
                site=params["site"],
                job_config_json=params["job_config"],
                selection_json=params["selection"],
                proxy_pool_json=params["proxy_pool"],
                status="ready",
                total=0,
                cursor=0,
                completed=0,
                failed=0,
                summary_json="{}",
            )
            session.add(plan)
            session.flush()

    def flush_ranges() -> None:
        if pending_ranges:
            session.execute(insert(MPSCandidateRangeModel), pending_ranges)
            pending_ranges.clear()

    def overlaps_window(first_ordinal: int, count: int) -> bool:
        after_last = first_ordinal + count
        if page_end is not None:
            after_last = min(after_last, page_end)
        return after_last > max(first_ordinal, candidate_offset)

    def emit_gap(gap_start: int, gap_end: int, ordinal: int) -> None:
        """Select the window's share of a missing run starting at ``ordinal``."""
        nonlocal selected
        first = max(ordinal, candidate_offset)
        after_last = ordinal + gap_end - gap_start + 1
        if page_end is not None:
            after_last = min(after_last, page_end)
        if after_last <= first:
            return
        ensure_plan()
        pending_ranges.append(
            {
                "job_id": params["job_id"],
                "first_ordinal": first - candidate_offset,
                "start_id": gap_start + first - ordinal,
                "end_id": gap_start + after_last - ordinal - 1,
            }
        )
        selected += after_last - first
        if len(pending_ranges) >= _MPS_RANGE_INSERT_BATCH_SIZE:
            flush_ranges()

    def count_known(lower: int, upper: int) -> int:
        nonlocal coverage_ms, count_queries
        began = time.monotonic()
        # invalid_users is unique on (site, user_id), so a plain count is exact.
        known = int(session.execute(text(
            "SELECT (SELECT count(*) FROM users WHERE id BETWEEN :lower AND :upper) + "
            "(SELECT count(*) FROM invalid_users i "
            "WHERE i.site=:site AND i.user_id BETWEEN :lower AND :upper "
            # The redundant range lets PostgreSQL merge two ordered range
            # scans instead of probing the users index once per invalid row.
            "AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id=i.user_id "
            "AND u.id BETWEEN :lower AND :upper))"
        ), {"lower": lower, "upper": upper, "site": params["site"]}).scalar_one())
        coverage_ms += (time.monotonic() - began) * 1000
        count_queries += 1
        return known

    def stream_block(lower: int, upper: int, ordinal: int, block_missing: int) -> None:
        nonlocal scan_ms, streamed_blocks, streamed_ids
        began = time.monotonic()
        streamed_blocks += 1
        previous_id = lower - 1
        result = session.execute(text(
            "SELECT id AS user_id FROM users WHERE id BETWEEN :lower AND :upper "
            "UNION SELECT user_id FROM invalid_users WHERE site=:site "
            "AND user_id BETWEEN :lower AND :upper ORDER BY user_id"
        ).execution_options(yield_per=_MPS_ID_STREAM_PAGE_SIZE),
            {"lower": lower, "upper": upper, "site": params["site"]})
        try:
            for row in result:
                known_id = int(row[0])
                streamed_ids += 1
                if known_id <= previous_id:
                    continue
                if known_id > previous_id + 1:
                    emit_gap(previous_id + 1, known_id - 1, ordinal)
                    ordinal += known_id - previous_id - 1
                    if page_end is not None and ordinal >= page_end:
                        # The block count already supplies the remaining total.
                        return
                previous_id = known_id
            if upper > previous_id:
                emit_gap(previous_id + 1, upper, ordinal)
                ordinal += upper - previous_id
            if ordinal != missing_count + block_missing:
                raise RuntimeError(
                    "MPS missing-only ID stream count differs from its coverage count"
                )
        finally:
            result.close()
            scan_ms += (time.monotonic() - began) * 1000

    def visit_block(block_lower: int, block_upper: int) -> bool:
        """Account for one block; return whether it overlapped the window."""
        nonlocal missing_count, known_count
        known = count_known(block_lower, block_upper)
        block_missing = block_upper - block_lower + 1 - known
        overlapped = overlaps_window(missing_count, block_missing)
        if overlapped and known and block_upper - block_lower + 1 > fine_span:
            for fine_lower in range(block_lower, block_upper + 1, fine_span):
                visit_block(fine_lower, min(block_upper, fine_lower + fine_span - 1))
            return True
        if overlapped:
            if known == 0:
                emit_gap(block_lower, block_upper, missing_count)
            else:
                stream_block(block_lower, block_upper, missing_count, block_missing)
        known_count += known
        missing_count += block_missing
        if page_end is not None and missing_count > page_end:
            raise _WindowFilled
        return overlapped

    def visit(lower: int, upper: int) -> None:
        # Start fine and double while blocks only contribute counts: the
        # window is usually near the start, and a coarse block inside it would
        # be counted twice. Large offsets still gallop to coarse blocks.
        span = fine_span
        block_lower = lower
        while block_lower <= upper:
            block_upper = min(upper, block_lower + span - 1)
            overlapped = visit_block(block_lower, block_upper)
            block_lower = block_upper + 1
            span = (
                fine_span
                if overlapped
                else min(span * 2, fine_span * _MPS_COVERAGE_FANOUT)
            )

    known_prefix_end = session.execute(
        text(_SELECT_KNOWN_PREFIX_END),
        {"site": params["site"], "start_id": start_id},
    ).scalar_one_or_none()
    scan_start = start_id
    if known_prefix_end is not None:
        scan_start = min(int(known_prefix_end), end_id + 1)
        known_count = scan_start - start_id

    complete = True
    try:
        visit(scan_start, end_id)
    except _WindowFilled:
        complete = False
    finally:
        logging.getLogger(__name__).info(
            "MPS manifest job=%s known_prefix_end=%s coverage_count_ms=%.1f sparse_scan_ms=%.1f elapsed_ms=%.1f count_queries=%s streamed_blocks=%s streamed_ids=%s selected=%s",
            params["job_id"], known_prefix_end, coverage_ms, scan_ms, (time.monotonic() - started) * 1000, count_queries, streamed_blocks, streamed_ids, selected,
        )

    if complete:
        candidate_count = max(range_size - known_count, 0)
        if missing_count != candidate_count:
            raise RuntimeError("MPS missing-only coverage count differs from the numeric range")
        available = max(candidate_count - candidate_offset, 0)
        total = min(available, int(max_profiles)) if max_profiles is not None else available
    else:
        # The window is full and a later candidate exists; the scanned prefix
        # is a lower bound for the range total.
        total = int(max_profiles)
        hint = params.get("candidate_count_hint")
        candidate_count = max(int(hint or 0), missing_count)
    if selected != total:
        raise RuntimeError(
            f"MPS missing-only snapshot selected {selected} IDs; expected {total}"
        )
    if plan is None:
        return None

    flush_ranges()
    summary: dict[str, Any] = {
        "missing_count": candidate_count,
        "invalid_eligible_count": 0,
        "existing_eligible_count": 0,
        "candidate_count": candidate_count,
    }
    if not complete:
        summary["candidate_count_exact"] = False
    summary_json = json.dumps(summary)
    plan.total = total
    plan.summary_json = summary_json
    session.flush()
    return total, summary_json


def _create_non_missing_plan(
    session: Any, params: dict[str, Any]
) -> tuple[int, str] | None:
    """Create exact ranges for non-missing candidates (invalid and/or existing users).

    Bypasses gap detection and sort across the full numeric ID space.
    Uses index scans and relational islands (user_id - ROW_NUMBER())
    to stream and insert coalesced candidate ranges in a single pass.
    """
    start_id = int(params["start_id"])
    end_id = int(params["end_id"])
    if end_id < start_id:
        return None

    include_invalid = bool(params.get("include_invalid"))
    include_existing = bool(params.get("include_existing"))
    if not include_invalid and not include_existing:
        return None

    candidate_offset = int(params.get("candidate_offset") or 0)
    max_profiles = params.get("max_profiles")
    fetch_limit = int(max_profiles) + 1 if max_profiles is not None else None
    slice_limit = int(max_profiles) if max_profiles is not None else None

    bind = session.get_bind() if hasattr(session, "get_bind") else None
    dialect_name = getattr(getattr(bind, "dialect", None), "name", None)
    if dialect_name == "postgresql" and hasattr(session, "connection"):
        session.connection().exec_driver_sql(
            "SET LOCAL enable_seqscan = off; SET LOCAL enable_hashjoin = off; SET LOCAL enable_bitmapscan = off"
        )

    if include_invalid and not include_existing:
        candidates_sql = """
            SELECT i.user_id, 'invalid' AS src
            FROM invalid_users i
            WHERE i.site = :site
              AND i.user_id BETWEEN :start_id AND :end_id
              AND (CAST(:invalid_cutoff AS timestamptz) IS NULL
                   OR COALESCE(i.updated_at, i.last_checked_at) IS NULL
                   OR COALESCE(i.updated_at, i.last_checked_at) <= :invalid_cutoff)
              AND NOT EXISTS (
                  SELECT 1 FROM users u
                  WHERE u.id = i.user_id
                    AND u.id BETWEEN :start_id AND :end_id
              )
            ORDER BY i.user_id
        """
    elif include_existing and not include_invalid:
        candidates_sql = """
            SELECT u.id AS user_id, 'existing' AS src
            FROM users u
            WHERE u.id BETWEEN :start_id AND :end_id
              AND (CAST(:existing_cutoff AS timestamptz) IS NULL
                   OR u.last_checked_at IS NULL
                   OR u.last_checked_at <= :existing_cutoff)
            ORDER BY u.id
        """
    else:
        candidates_sql = """
            (
                SELECT i.user_id, 'invalid' AS src
                FROM invalid_users i
                WHERE i.site = :site
                  AND i.user_id BETWEEN :start_id AND :end_id
                  AND (CAST(:invalid_cutoff AS timestamptz) IS NULL
                       OR COALESCE(i.updated_at, i.last_checked_at) IS NULL
                       OR COALESCE(i.updated_at, i.last_checked_at) <= :invalid_cutoff)
                  AND NOT EXISTS (
                      SELECT 1 FROM users u
                      WHERE u.id = i.user_id
                        AND u.id BETWEEN :start_id AND :end_id
                  )
                ORDER BY i.user_id
            )
            UNION ALL
            (
                SELECT u.id AS user_id, 'existing' AS src
                FROM users u
                WHERE u.id BETWEEN :start_id AND :end_id
                  AND (CAST(:existing_cutoff AS timestamptz) IS NULL
                       OR u.last_checked_at IS NULL
                       OR u.last_checked_at <= :existing_cutoff)
                ORDER BY u.id
            )
            ORDER BY user_id
        """

    plan = MPSPlanModel(
        job_id=params["job_id"],
        site=params["site"],
        job_config_json=params["job_config"],
        selection_json=params["selection"],
        proxy_pool_json=params["proxy_pool"],
        status="ready",
        total=0,
        cursor=0,
        completed=0,
        failed=0,
        summary_json="{}",
    )
    session.add(plan)
    session.flush()

    query_str = f"""
        WITH fetched AS (
            {candidates_sql}
            OFFSET :candidate_offset
            LIMIT :fetch_limit
        ), sliced AS (
            SELECT user_id, src FROM fetched
            ORDER BY user_id
            LIMIT :slice_limit
        ), numbered AS (
            SELECT user_id,
                   user_id - ROW_NUMBER() OVER (ORDER BY user_id) AS grp,
                   ROW_NUMBER() OVER (ORDER BY user_id) - 1 AS ord
            FROM sliced
        ), ranges AS (
            SELECT MIN(ord) AS first_ordinal,
                   MIN(user_id) AS start_id,
                   MAX(user_id) AS end_id
            FROM numbered
            GROUP BY grp
        ), ranges_insert AS (
            INSERT INTO management.mps_candidate_ranges (job_id, first_ordinal, start_id, end_id)
            SELECT :job_id, first_ordinal, start_id, end_id
            FROM ranges
            RETURNING first_ordinal
        )
        SELECT (SELECT COUNT(*) FROM fetched) AS fetched_count,
               (SELECT COUNT(*) FROM sliced) AS sliced_count,
               (SELECT COUNT(*) FROM ranges_insert) AS ranges_count,
               (SELECT COUNT(*) FILTER (WHERE src = 'invalid') FROM fetched) AS fetched_invalid,
               (SELECT COUNT(*) FILTER (WHERE src = 'existing') FROM fetched) AS fetched_existing;
    """

    res = session.execute(
        text(query_str),
        {
            "job_id": params["job_id"],
            "site": params["site"],
            "start_id": start_id,
            "end_id": end_id,
            "candidate_offset": candidate_offset,
            "fetch_limit": fetch_limit,
            "slice_limit": slice_limit,
            "invalid_cutoff": params["invalid_cutoff"],
            "existing_cutoff": params["existing_cutoff"],
        },
    ).first()

    if res is None or res[1] == 0:
        session.delete(plan)
        session.flush()
        return None

    fetched_count = int(res[0])
    sliced_count = int(res[1])
    fetched_invalid = int(res[3] or 0)
    fetched_existing = int(res[4] or 0)

    complete = (max_profiles is None) or (fetched_count <= sliced_count)
    if complete:
        candidate_count = candidate_offset + sliced_count
    else:
        hint = params.get("candidate_count_hint")
        candidate_count = max(int(hint or 0), candidate_offset + fetched_count)

    if include_invalid and not include_existing:
        invalid_eligible_count = candidate_count
        existing_eligible_count = 0
    elif include_existing and not include_invalid:
        invalid_eligible_count = 0
        existing_eligible_count = candidate_count
    else:
        invalid_eligible_count = fetched_invalid
        existing_eligible_count = fetched_existing

    summary: dict[str, Any] = {
        "missing_count": 0,
        "invalid_eligible_count": invalid_eligible_count,
        "existing_eligible_count": existing_eligible_count,
        "candidate_count": candidate_count,
    }
    if not complete:
        summary["candidate_count_exact"] = False

    total = sliced_count
    summary_json = json.dumps(summary)
    plan.total = total
    plan.summary_json = summary_json
    session.flush()
    return total, summary_json


class MPSManifestStore:
    def __init__(self, session_factory: Callable[..., Any] = global_session) -> None:
        self.session_factory = session_factory

    @property
    def definitions(self):
        from .mps_definitions import MPSTaskDefinitions

        return MPSTaskDefinitions(self)

    def create(
        self,
        *,
        job_id: str,
        site: str,
        job_config: dict[str, Any],
        selection: dict[str, Any],
        proxy_pool: Sequence[str],
        site_session_factory: Callable[..., Any],
        db_session: Any | None = None,
        candidate_count_hint: int | None = None,
    ) -> dict[str, Any]:
        """Freeze selection atomically; retries return the original manifest.

        Called by the database writer with a session scoped to the chosen site.
        The same archive database owns its management and site schemas.
        """
        now = datetime.now(timezone.utc)

        def cutoff(key: str) -> datetime | None:
            hours = selection.get(key)
            return now - timedelta(hours=float(hours)) if hours is not None else None

        params = {
            "job_id": job_id,
            "site": site.lower(),
            "job_config": json.dumps(job_config),
            "selection": json.dumps(selection, sort_keys=True),
            "proxy_pool": json.dumps(list(proxy_pool)),
            "start_id": _positive_int(selection["start_id"], "start_id"),
            "end_id": _positive_int(selection["end_id"], "end_id"),
            "candidate_offset": selection.get("candidate_offset", 0),
            "max_profiles": selection.get("max_profiles"),
            "include_missing": not selection.get("ignore_missing_ids", False),
            "include_invalid": selection.get("include_invalid", False),
            "include_existing": selection.get("include_existing_users", False),
            "invalid_cutoff": cutoff("invalid_last_checked_older_than_hours"),
            "existing_cutoff": cutoff("existing_last_checked_older_than_hours"),
            "candidate_count_hint": candidate_count_hint,
        }
        with (
            nullcontext(db_session)
            if db_session is not None
            else site_session_factory()
        ) as session:
            if db_session is None:
                session.connection().exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                )
            session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:job_id, 0))"),
                {"job_id": job_id},
            )
            existing = session.get(MPSPlanModel, job_id)
            if existing is not None:
                return self._snapshot(existing)
            missing_only = (
                params["include_missing"]
                and not params["include_invalid"]
                and not params["include_existing"]
            )
            non_missing = (
                not params["include_missing"]
                and (params["include_invalid"] or params["include_existing"])
            )
            if missing_only:
                row = _create_missing_only_plan(session, params)
            elif non_missing:
                row = _create_non_missing_plan(session, params)
            else:
                row = session.execute(
                    text(_SELECT_MANIFEST),
                    {k: v for k, v in params.items() if k != "candidate_count_hint"},
                ).first()
            if row is None:
                raise NoMPSCandidates(
                    "No profiles selected for the requested candidate offset and filters"
                )
            session.execute(
                insert(CentralJobModel)
                .values(
                    job_id=job_id,
                    job_type="mass_profile_scrape",
                    site=site.lower(),
                    args_json=json.dumps(
                        {
                            "job_name": job_config.get("name", "MPS"),
                            "created_by": job_config.get("created_by"),
                            "task_count": int(row[0]),
                            "task_types": ["scrape_user"],
                            "schedule": None,
                            "mps_manifest": True,
                        }
                    ),
                )
                .on_conflict_do_nothing(index_elements=[CentralJobModel.job_id])
            )
            result = session.get(MPSPlanModel, job_id)
            assert result is not None
            snapshot = self._snapshot(result)
            from ..job_system.runtime_admission import validate_durable_configuration

            validate_durable_configuration(job_id, snapshot["job_config"])
            from ..job_system.ledger import admit

            snapshot = admit(session, snapshot)
            if db_session is None:
                session.commit()
            return snapshot

    def clone_for_retry(
        self,
        *,
        source_job_id: str,
        job_id: str,
        job_config: dict[str, Any],
        site: str,
        live_failed_user_ids: Sequence[int],
        live_failed_tasks: Sequence[dict[str, Any]] = (),
        expected_manifest_id: Any = _UNSPECIFIED,
        auth_id: int | None,
        db_session: Any,
        incremental: bool = False,
    ) -> dict[str, Any]:
        """Clone failed candidates, or the immutable selection when none failed.

        Only the live Redis window crosses the process boundary. Archived
        failures and full compressed ranges are selected/copied inside SQL.
        """
        session = db_session
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:id, 0))"), {"id": job_id}
        )
        existing = session.get(MPSPlanModel, job_id)
        if existing is not None:
            return self._snapshot(existing)
        source = session.execute(
            select(MPSPlanModel)
            .where(MPSPlanModel.job_id == source_job_id)
            .with_for_update()
        ).scalar_one()
        if "retry_preparation" in json.loads(source.selection_json):
            raise ValueError("MPS retry preparation is still in progress")
        if source.site != site:
            raise ValueError("Retry source belongs to another site")
        live_ids = [
            _positive_int(identifier, "live failed user ID")
            for identifier in live_failed_user_ids
        ]
        source_metadata = json.loads(source.job_config_json).get("metadata", {})
        job_config = {**job_config, "metadata": dict(job_config.get("metadata", {}))}
        if "durable_job_kind" in source_metadata:
            job_config["metadata"]["durable_job_kind"] = source_metadata["durable_job_kind"]
        else:
            job_config["metadata"].pop("durable_job_kind", None)
        selection = json.loads(source.selection_json)
        selection.pop("candidate_checkpoint", None)
        if expected_manifest_id is not _UNSPECIFIED and selection.get("task_manifest_id") != expected_manifest_id:
            raise ValueError("MPS task definition changed before retry")
        graph = bool(selection.get("task_manifest_id") or selection.get("spawned_manifest_id"))
        if auth_id is not None:
            selection["retry_auth_id"] = _positive_int(auth_id, "auth_id")
        source_failed = session.scalar(
            text("""
            SELECT count(*) FROM management.job_tasks
            WHERE job_id=:source AND status='failed' AND user_id IS NOT NULL
        """),
            {"source": source_job_id},
        )
        if not graph and int(source_failed) < source.failed:
            raise ValueError("Archived MPS failures lack canonical profile identity")
        retry_failures = bool(source_failed or live_ids)
        plan = MPSPlanModel(
            job_id=job_id,
            site=site,
            job_config_json=json.dumps(job_config),
            selection_json=json.dumps(selection),
            proxy_pool_json=source.proxy_pool_json,
            total=0,
            summary_json="{}",
        )
        session.add(plan)
        session.flush()
        type_counts = {"scrape": source.total}
        if graph:
            from .mps_retry import clone_graph, freeze_graph

            retry_auth_id = selection.get("retry_auth_id")
            if incremental:
                total, retry_failures = freeze_graph(
                    session, source, plan, live_tasks=list(live_failed_tasks), auth_id=retry_auth_id,
                )
                type_counts = {}
            else:
                total, retry_failures, type_counts = clone_graph(
                    session, source, plan, live_tasks=list(live_failed_tasks), auth_id=retry_auth_id,
                )
            selection = {"task_manifest_id": job_id,
                         **({"retry_auth_id": retry_auth_id} if retry_auth_id is not None else {})}
            if incremental:
                plan.status = "preparing"
                selection["retry_preparation"] = {
                    "source_job_id": source_job_id,
                    "source_created_at": source.created_at.isoformat(),
                    "source_manifest": json.loads(source.selection_json).get("task_manifest_id"),
                    "after": -1, "copied": 0, "type_counts": {},
                    "failed_only": retry_failures, "auth_id": retry_auth_id,
                }
        elif retry_failures:
            total = session.scalar(
                text("""
                WITH candidates AS (
                    SELECT user_id FROM management.job_tasks
                    WHERE job_id=:source AND status='failed' AND user_id IS NOT NULL
                    UNION SELECT unnest(CAST(:live_ids AS bigint[]))
                ), inserted AS (
                    INSERT INTO management.mps_candidate_ranges
                        (job_id, first_ordinal, start_id, end_id)
                    SELECT :target, row_number() OVER (ORDER BY user_id) - 1, user_id, user_id
                    FROM candidates RETURNING 1
                ) SELECT count(*) FROM inserted
            """),
                {"source": source_job_id, "target": job_id, "live_ids": live_ids},
            )
        else:
            session.execute(
                text("""
                INSERT INTO management.mps_candidate_ranges (job_id,first_ordinal,start_id,end_id)
                SELECT :target,first_ordinal,start_id,end_id
                FROM management.mps_candidate_ranges WHERE job_id=:source
            """),
                {"source": source_job_id, "target": job_id},
            )
            total = source.total
        plan.total = int(total)
        plan.selection_json = json.dumps(selection)
        if not graph:
            type_counts = {"scrape": plan.total}
        self._retry_metadata(plan, type_counts, graph=graph, session=session)
        plan.summary_json = json.dumps(
            {
                "candidate_count": plan.total,
                "retry_of": source_job_id,
                "failed_candidates_only": retry_failures,
            }
        )
        if plan.status != "preparing":
            self._retire_retry_source(session, source, plan)
        session.execute(
            insert(CentralJobModel).values(
                job_id=job_id,
                job_type="retry" if source_metadata.get("durable_job_kind") == "ordinary" else "mass_profile_scrape",
                site=site,
                args_json=json.dumps(
                    {
                        "job_name": job_config.get("name", "MPS retry"),
                        "created_by": job_config.get("created_by"),
                        "task_count": plan.total,
                        "mps_manifest": True,
                        "retry_of": source_job_id,
                        "task_types": list(type_counts),
                    }
                ),
            )
        )
        session.flush()
        return self._snapshot(plan)

    @staticmethod
    def _retry_metadata(plan: Any, type_counts: dict[str, int], *, graph: bool, session: Any) -> None:
        config = json.loads(plan.job_config_json)
        config.update(total_tasks=plan.total, tasks=[], task_ids=[], completed_tasks=0, failed_tasks=0)
        config.setdefault("metadata", {}).update(
            mps_task_type_counts=type_counts, total_tasks_count_override=plan.total,
            **({"mps_task_manifest": plan.job_id} if graph else {}),
        )
        if graph:
            from .mps_definitions import definition_routing

            config["metadata"].update(definition_routing(session, plan.job_id))
        from ..job_system.runtime_admission import validate_durable_configuration

        validate_durable_configuration(plan.job_id, config)
        plan.job_config_json = json.dumps(config)

    @staticmethod
    def _retire_retry_source(session: Any, source: Any, plan: Any) -> None:
        now = datetime.now(timezone.utc)
        source.status = "cancelled"
        source.updated_at = now
        session.execute(
            update(CentralJobModel)
            .where(CentralJobModel.job_id == source.job_id)
            .values(
                status="cancelled",
                completed_at=now,
                last_event_at=now,
                result_message=f"Superseded by retry job {plan.job_id}",
            )
        )

    def advance_retry(
        self, job_id: str, *, after: int, db_session: Any,
    ) -> dict[str, Any]:
        """Commit one frozen graph page; a stale command never repeats a page."""
        from types import SimpleNamespace
        from .mps_retry import copy_graph_page

        session = db_session
        plan = session.execute(
            select(MPSPlanModel).where(MPSPlanModel.job_id == job_id).with_for_update()
        ).scalar_one_or_none()
        if plan is None:
            return {"deleted": True}
        if plan.status != "preparing":
            return self._snapshot(plan)
        selection = json.loads(plan.selection_json)
        state = selection["retry_preparation"]
        if after != state["after"]:
            return self._snapshot(plan)
        source_snapshot = SimpleNamespace(
            job_id=state["source_job_id"], site=plan.site, proxy_pool_json=plan.proxy_pool_json,
        )
        complete = copy_graph_page(session, source_snapshot, plan, state)
        if complete:
            source = session.execute(
                select(MPSPlanModel).where(
                    MPSPlanModel.job_id == state["source_job_id"]
                ).with_for_update()
            ).scalar_one_or_none()
            if (source is None
                or source.created_at.isoformat() != state["source_created_at"]
                or json.loads(source.selection_json).get("task_manifest_id") != state["source_manifest"]):
                raise ValueError("MPS retry source was deleted or replaced during preparation")
            self._retry_metadata(plan, state["type_counts"], graph=True, session=session)
            config = json.loads(plan.job_config_json)
            metadata = config.setdefault("metadata", {})
            metadata["mps_config_revision"] = int(metadata.get("mps_config_revision", 0)) + 1
            plan.job_config_json = json.dumps(config)
            plan.status = "ready"
            selection.pop("retry_preparation")
            self._retire_retry_source(session, source, plan)
            history = session.get(CentralJobModel, job_id)
            if history is not None:
                args = json.loads(history.args_json)
                args["task_types"] = list(state["type_counts"])
                history.args_json = json.dumps(args)
        plan.selection_json = json.dumps(selection)
        plan.updated_at = datetime.now(timezone.utc)
        session.flush()
        return self._snapshot(plan)

    @staticmethod
    def _snapshot(plan: MPSPlanModel) -> dict[str, Any]:
        return {
            "job_id": plan.job_id,
            "site": plan.site,
            "job_config": json.loads(plan.job_config_json),
            "selection": json.loads(plan.selection_json),
            "proxy_pool": json.loads(plan.proxy_pool_json),
            "total": plan.total,
            "cursor": plan.cursor,
            "completed": plan.completed,
            "failed": plan.failed,
            "status": plan.status,
            "summary": json.loads(plan.summary_json),
        }

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self.session_factory() as session:
            plan = session.get(MPSPlanModel, job_id)
            return self._snapshot(plan) if plan is not None else None

    def update_config(
        self,
        job_id: str,
        *,
        name: str | None = None,
        schedule: dict[str, Any] | None = None,
        start_schedule_revision: int | None = None,
        db_session: Any | None = None,
    ) -> dict[str, Any]:
        """Edit the compact definition without loading or resetting its work.

        Revisions order concurrent API edits and delayed Redis publication.
        A scheduled dispatch releases only the revision that became due, so a
        delayed scheduler cannot start a newly postponed job.
        """
        from ..job_system.models import JobSchedule

        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise ValueError("Job name cannot be empty")
        with (
            nullcontext(db_session)
            if db_session is not None
            else self.session_factory()
        ) as session:
            plan = session.execute(
                select(MPSPlanModel)
                .where(MPSPlanModel.job_id == job_id)
                .with_for_update()
            ).scalar_one_or_none()
            if plan is None:
                raise ValueError(f"Job {job_id} not found")
            config = json.loads(plan.job_config_json)
            metadata = config.setdefault("metadata", {})
            revision = int(metadata.get("mps_config_revision", 0))
            history_changes: dict[str, Any] = {}
            if start_schedule_revision is not None:
                if name is not None or schedule is not None:
                    raise ValueError("A schedule trigger cannot also edit the job")
                when = config.get("schedule", {}).get("scheduled_for")
                if (
                    revision != start_schedule_revision
                    or not metadata.get("mps_schedule_pending")
                    or plan.status != "ready"
                    or not when
                ):
                    return self._snapshot(plan)
                due = datetime.fromisoformat(when)
                if due.tzinfo is None:
                    due = due.replace(tzinfo=timezone.utc)
                if due > datetime.now(timezone.utc):
                    return self._snapshot(plan)
                metadata["mps_schedule_pending"] = False
            else:
                if name is None and schedule is None:
                    return self._snapshot(plan)
                if name is not None:
                    config["name"] = name.strip()
                    history_changes["job_name"] = config["name"]
                if schedule is not None:
                    normalized = JobSchedule.from_dict(schedule).to_dict()
                    if normalized["scheduled_for"]:
                        datetime.fromisoformat(normalized["scheduled_for"])
                    config["schedule"] = normalized
                    pending = any(normalized.values())
                    metadata["source"] = "scheduled" if pending else "manual"
                    metadata["mps_schedule_pending"] = pending
                    history_changes["schedule"] = normalized
            metadata["mps_config_revision"] = revision + 1
            from ..job_system.runtime_admission import validate_durable_configuration

            validate_durable_configuration(job_id, config)
            plan.job_config_json = json.dumps(config)
            plan.updated_at = datetime.now(timezone.utc)
            if history_changes:
                # Merge at the row update, preserving concurrent status/history
                # fields instead of writing a stale copy of the whole document.
                session.execute(
                    text(
                        "UPDATE management.jobs SET args_json = "
                        "(args_json::jsonb || CAST(:changes AS jsonb))::text "
                        "WHERE job_id = :job_id"
                    ),
                    {"job_id": job_id, "changes": json.dumps(history_changes)},
                )
            result = self._snapshot(plan)
            if db_session is None:
                session.commit()
            return result

    def candidate_page(
        self, job_id: str, *, offset: int, limit: int, db_session: Any | None = None
    ) -> list[int]:
        """Expand at most ``limit`` candidates from an indexed ordinal seek."""
        _positive_int(limit, "limit")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        model = MPSCandidateRangeModel
        with (
            nullcontext(db_session)
            if db_session is not None
            else self.session_factory()
        ) as session:
            anchor = session.execute(
                select(model.first_ordinal)
                .where(model.job_id == job_id, model.first_ordinal <= offset)
                .order_by(model.first_ordinal.desc())
                .limit(1)
            ).scalar_one_or_none()
            if anchor is None:
                return []
            rows = session.execute(
                select(model.first_ordinal, model.start_id, model.end_id)
                .where(
                    model.job_id == job_id,
                    model.first_ordinal >= anchor,
                    model.first_ordinal < offset + limit,
                )
                .order_by(model.first_ordinal)
                .limit(limit)
            )
            values: list[int] = []
            for ordinal, first, last in rows:
                start = first + max(offset - ordinal, 0)
                stop = min(last + 1, start + limit - len(values))
                values.extend(range(start, stop))
            return values

    def candidate_ids(
        self, job_id: str, ordinals: Sequence[int], *, db_session: Any | None = None
    ) -> dict[int, int]:
        """Resolve a bounded set of candidate positions through indexed seeks."""
        if not ordinals:
            return {}
        with nullcontext(db_session) if db_session is not None else self.session_factory() as session:
            rows = session.execute(text("""
                SELECT position.ordinal,
                    ranges.start_id + position.ordinal - ranges.first_ordinal AS user_id
                FROM unnest(CAST(:ordinals AS bigint[])) AS position(ordinal)
                JOIN LATERAL (
                    SELECT first_ordinal, start_id, end_id
                    FROM management.mps_candidate_ranges
                    WHERE job_id=:job AND first_ordinal<=position.ordinal
                    ORDER BY first_ordinal DESC LIMIT 1
                ) ranges ON position.ordinal-ranges.first_ordinal
                    <= ranges.end_id-ranges.start_id
            """), {"job": job_id, "ordinals": list(ordinals)})
            return {int(row.ordinal): int(row.user_id) for row in rows}

    def archived_task_ids(
        self, job_id: str, task_ids: Sequence[str], manifest_id: str | None,
        candidate_ordinals: dict[str, int],
    ) -> set[str]:
        """Read terminal proof for only the resident tasks of this definition."""
        if not task_ids:
            return set()
        with self.session_factory() as session:
            plan = session.execute(select(MPSPlanModel).where(
                MPSPlanModel.job_id == job_id
            ).with_for_update(read=True)).scalar_one_or_none()
            if plan is None or json.loads(plan.selection_json).get("task_manifest_id") != manifest_id:
                return set()
            return self._committed_task_ids(session, plan, task_ids, candidate_ordinals)

    @staticmethod
    def _committed_task_ids(session, plan, task_ids, candidate_ordinals):
        selection = json.loads(plan.selection_json)
        checkpoint = selection.get("candidate_checkpoint", {
            "next": min(plan.cursor, selection.get("candidate_total", plan.total)),
            "pending": [],
        })
        pending = checkpoint["pending"]
        committed = set()
        for task_id, position in candidate_ordinals.items():
            index = bisect_left(pending, [position + 1]) - 1
            outstanding = index >= 0 and pending[index][0] <= position < pending[index][1]
            if position < checkpoint["next"] and not outstanding:
                committed.add(task_id)
        explicit = set(task_ids) - candidate_ordinals.keys()
        definition = selection.get("task_manifest_id") or selection.get("spawned_manifest_id")
        if explicit and definition is not None:
            model = MPSTaskDefinitionModel
            committed.update(session.execute(select(model.task_id).where(
                model.manifest_id == definition, model.task_id.in_(explicit),
                model.status.in_(["completed", "failed"]),
            )).scalars())
        if committed:
            evidence = set(session.execute(select(CentralTaskModel.task_id).where(
                CentralTaskModel.job_id == plan.job_id,
                CentralTaskModel.task_id.in_(committed),
                CentralTaskModel.status.in_(["completed", "failed"]),
            )).scalars())
            if evidence != committed:
                raise ValueError("Durable checkpoint is missing terminal task evidence")
        return committed

    def _rolling_archive_evidence(self, session, plan, rows, candidate_ordinals):
        """Validate arbitrary completed tasks and retain only unfinished positions."""
        rows, identities, source_manifest, positions = self._rolling_archive_checks(
            session, plan, rows, candidate_ordinals
        )
        self._merge_candidate_checkpoint(plan, positions)
        return rows, identities, source_manifest

    def _rolling_archive_checks(self, session, plan, rows, candidate_ordinals):
        """Read-only part of ``_rolling_archive_evidence``; ``plan`` is not changed.

        Returns the not-yet-archived rows, their proven identities, the source
        definition manifest and the candidate positions of the returned rows.
        """
        ids = {row["task_id"] for row in rows}
        if len(ids) != len(rows) or not rows or any(
            row["job_id"] != plan.job_id or row["status"] not in {"completed", "failed"}
            for row in rows
        ):
            raise ValueError("Rolling archive requires distinct terminal tasks from this job")
        if not candidate_ordinals.keys() <= ids:
            raise ValueError("Candidate positions do not match the archived tasks")
        from .job_partitions import task_identity_model

        # Guarded history inserts enforce ownership atomically. Probing the
        # directory here repeats the trigger's random index reads for every
        # new task. Legacy/unguarded schemas still require the history check.
        identity = task_identity_model(session)
        if identity is CentralTaskModel:
            existing = session.execute(select(
                identity.task_id, identity.job_id
            ).where(identity.task_id.in_(ids))).all()
            if any(row.job_id != plan.job_id for row in existing):
                raise ValueError("Archived task belongs to another job")
        archived = self._committed_task_ids(session, plan, ids, candidate_ordinals)
        rows = [row for row in rows if row["task_id"] not in archived]
        ids -= archived
        selection = json.loads(plan.selection_json)
        source_manifest = selection.get("task_manifest_id") or selection.get("spawned_manifest_id")
        positions = {key: value for key, value in candidate_ordinals.items() if key in ids}
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in positions.values()):
            raise ValueError("Candidate positions must be non-negative integers")
        candidates = self.candidate_ids(plan.job_id, list(positions.values()), db_session=session)
        identities: dict[str, int | None] = {}
        for task_id, ordinal in positions.items():
            user_id = candidates.get(ordinal)
            if user_id is None or mps_task_id(plan.job_id, ordinal, user_id) != task_id:
                raise ValueError("Rolling archive differs from its selected candidate")
            identities[task_id] = user_id
        explicit = ids - positions.keys()
        if explicit:
            if source_manifest is None:
                raise ValueError("Rolling archive has no matching task definition")
            definitions = self.definitions.ready_page(
                source_manifest, limit=len(explicit), only_task_ids=list(explicit), db_session=session
            )
            by_id = {row["task_id"]: row for row in rows}
            for definition in definitions:
                task_id = definition["task_id"]
                evidence = by_id[task_id]
                if (evidence["task_type"] != definition["task"]["task_type"]
                        or (definition["dependency_failed"] and evidence["status"] != "failed")):
                    raise ValueError("Rolling archive differs from its explicit task definition")
                try:
                    user_id = int(definition["task"].get("user_id"))
                except (TypeError, ValueError):
                    user_id = 0
                identities[task_id] = user_id if user_id > 0 else None
        if set(identities) != ids:
            raise ValueError("Rolling archive lacks ready task evidence")
        return rows, identities, source_manifest, positions

    @staticmethod
    def _merge_candidate_checkpoint(plan, positions):
        """Mark ``positions`` archived in the plan's candidate checkpoint."""
        if positions:
            selection = json.loads(plan.selection_json)
            checkpoint = selection.get("candidate_checkpoint", {
                "next": min(plan.cursor, selection.get("candidate_total", plan.total)),
                "pending": [],
            })
            completed = sorted(positions.values())
            ranges = list(checkpoint["pending"])
            frontier = max(checkpoint["next"], completed[-1] + 1)
            if frontier > checkpoint["next"]:
                ranges.append([checkpoint["next"], frontier])
            pending = []
            for first, last in ranges:
                begin = first
                index = bisect_left(completed, first)
                while index < len(completed) and completed[index] < last:
                    position = completed[index]
                    if begin < position:
                        pending.append([begin, position])
                    begin = position + 1
                    index += 1
                if begin < last:
                    pending.append([begin, last])
            # These ranges describe outstanding work, so they remain bounded
            # by the resident task limit even when an early task is very slow.
            selection["candidate_checkpoint"] = {"next": frontier, "pending": pending}
            plan.selection_json = json.dumps(selection)

    def archive_window(
        self,
        job_id: str,
        *,
        start: int,
        end: int,
        rows: list[dict[str, Any]],
        manifest_id: str | None = None,
        db_session: Any | None = None,
        runtime: dict[str, Any] | None = None,
        candidate_ordinals: dict[str, int] | None = None,
        timings: dict[str, float] | None = None,
        trusted_identity: bool = False,
    ) -> dict[str, Any]:
        """Commit terminal evidence and its progress in the same transaction.

        ``timings`` optionally receives elapsed milliseconds per phase.
        ``trusted_identity`` lets proven candidate rows skip the global task
        directory (migration d5e1a7c39b42); other rows are always guarded.
        """
        lap = PhaseTimer(timings)
        with (
            nullcontext(db_session) if db_session is not None else self.session_factory()
        ) as session:
            plan = session.execute(
                select(MPSPlanModel).where(MPSPlanModel.job_id == job_id).with_for_update()
            ).scalar_one()
            lap("plan_lock")
            if json.loads(plan.selection_json).get("task_manifest_id") != manifest_id:
                raise ValueError("MPS task definition changed before window archive")
            source_manifest = task_source_manifest(json.loads(plan.selection_json), start)
            identities: dict[str, int | None]
            if candidate_ordinals is not None:
                rows, identities, source_manifest = self._rolling_archive_evidence(
                    session, plan, rows, candidate_ordinals
                )
                if not rows:
                    return self._snapshot(plan)
                start, end = plan.cursor, plan.cursor + len(rows)
            elif source_manifest is None:
                ids = self.candidate_page(
                    job_id, offset=start, limit=end - start, db_session=session
                )
                identities = {
                    mps_task_id(job_id, start + index, user_id): user_id
                    for index, user_id in enumerate(ids)
                }
            else:
                if plan.cursor < end:
                    definitions = self.definitions.ready_page(
                        source_manifest, limit=end - start, db_session=session
                    )
                else:
                    model = MPSTaskDefinitionModel
                    definitions = [
                        {"task_id": row.task_id, "task": json.loads(row.task_json)}
                        for row in session.execute(select(model.task_id, model.task_json).where(
                            model.manifest_id == source_manifest,
                            model.task_id.in_([row["task_id"] for row in rows]),
                            model.status != "pending",
                        ))
                    ]
                identities = {}
                for definition in definitions:
                    try:
                        user_id = int(definition["task"]["user_id"])
                    except (TypeError, ValueError):
                        user_id = 0
                    # Explicit username tasks can start unresolved. Preserve
                    # the canonical ID obtained by their worker; known positive
                    # IDs still require an exact identity match.
                    identities[definition["task_id"]] = user_id if user_id > 0 else None
                by_id = {row["task_id"]: row for row in rows}
                for definition in definitions:
                    evidence = by_id.get(definition["task_id"])
                    if evidence is not None and (
                        evidence["task_type"] != definition["task"]["task_type"]
                        or (definition.get("dependency_failed") and evidence["status"] != "failed")
                    ):
                        raise ValueError("MPS archive differs from the explicit task definition")
            if set(identities) != {row["task_id"] for row in rows}:
                raise ValueError(
                    "MPS archive window has incomplete or different candidate evidence"
                )
            if plan.cursor >= end:
                return self._snapshot(plan)
            if plan.cursor != start or not start < end <= plan.total:
                raise ValueError("MPS archive window does not match the durable checkpoint")
            if len(rows) != end - start or len({row["task_id"] for row in rows}) != len(
                rows
            ):
                raise ValueError(
                    "MPS archive window is incomplete or contains duplicate tasks"
                )
            if any(
                row["job_id"] != job_id or row["status"] not in {"completed", "failed"}
                for row in rows
            ):
                raise ValueError("MPS archive requires terminal tasks from this job")
            self._apply_archive_identities(rows, identities)
            lap("evidence")
            # Candidate rows reaching here passed _rolling_archive_evidence:
            # each ID equals mps_task_id(job, ordinal, user), so it cannot
            # belong to another job. Explicit/spawned rows are never trusted.
            trusted_ids = (
                {row["task_id"] for row in rows if row["task_id"] in candidate_ordinals}
                if trusted_identity and candidate_ordinals is not None
                else set()
            )
            self._insert_archive_history(
                session, job_id, rows, trusted_ids,
                prefetch=candidate_ordinals is not None, lap=lap,
            )
            snapshot = self._advance_archive_progress(
                session, plan, job_id, rows, source_manifest, end, runtime, lap
            )
            if db_session is None:
                session.commit()
            return snapshot

    # Parallel archive passes (ULTIMA_ARCHIVE_PARALLEL_PASSES > 1) split
    # archive_window for candidate-only batches into two calls in one
    # transaction. The caller must hold the run FOR SHARE and FOR UPDATE locks
    # on every row's queue entry, taken with SKIP LOCKED, so concurrent passes
    # own disjoint tasks. archive_concurrent validates and inserts history
    # without the plan lock; archive_checkpoint then locks the plan and records
    # progress. Lock order: run (shared) -> queue rows -> plan.

    def archive_concurrent(
        self,
        job_id: str,
        *,
        rows: list[dict[str, Any]],
        manifest_id: str | None,
        candidate_ordinals: dict[str, int],
        db_session: Any,
        timings: dict[str, float] | None = None,
        trusted_identity: bool = False,
    ) -> ConcurrentArchive | None:
        """Validate and insert a candidate-only batch without locking the plan.

        Evidence is read from an unlocked plan snapshot. Archived positions
        only grow, so a task committed in the snapshot stays committed;
        ``archive_checkpoint`` re-checks the rest under the plan lock. Returns
        ``None`` when every row was already archived.
        """
        if candidate_ordinals.keys() != {row["task_id"] for row in rows}:
            raise ValueError("Parallel archive accepts only candidate rows")
        lap = PhaseTimer(timings)
        session = db_session
        plan = session.execute(
            select(MPSPlanModel)
            .where(MPSPlanModel.job_id == job_id)
            .execution_options(populate_existing=True)
        ).scalar_one()
        lap("plan_snapshot")
        if json.loads(plan.selection_json).get("task_manifest_id") != manifest_id:
            raise ValueError("MPS task definition changed before window archive")
        rows, identities, source_manifest, positions = self._rolling_archive_checks(
            session, plan, rows, candidate_ordinals
        )
        if not rows:
            return None
        self._apply_archive_identities(rows, identities)
        lap("evidence")
        self._insert_archive_history(
            session, job_id, rows,
            {row["task_id"] for row in rows} if trusted_identity else set(),
            prefetch=True, lap=lap,
        )
        return ConcurrentArchive(rows, positions, manifest_id, source_manifest)

    def archive_checkpoint(
        self,
        job_id: str,
        archive: ConcurrentArchive,
        *,
        db_session: Any,
        timings: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """Lock the plan and record an ``archive_concurrent`` batch's progress."""
        lap = PhaseTimer(timings)
        session = db_session
        # populate_existing: the session may hold an older unlocked copy.
        plan = session.execute(
            select(MPSPlanModel)
            .where(MPSPlanModel.job_id == job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).scalar_one()
        lap("plan_lock")
        selection = json.loads(plan.selection_json)
        if (
            selection.get("task_manifest_id") != archive.manifest_id
            or (selection.get("task_manifest_id") or selection.get("spawned_manifest_id"))
            != archive.source_manifest
        ):
            raise ValueError("MPS task definition changed during a parallel archive")
        if self._committed_task_ids(
            session, plan, list(archive.positions), archive.positions
        ):
            raise ValueError("Parallel archive overlaps an already archived task")
        start = plan.cursor
        end = start + len(archive.rows)
        if end > plan.total:
            raise ValueError("MPS archive window does not match the durable checkpoint")
        self._merge_candidate_checkpoint(plan, archive.positions)
        lap("evidence")
        return self._advance_archive_progress(
            session, plan, job_id, archive.rows, archive.source_manifest, end, None, lap
        )

    @staticmethod
    def _apply_archive_identities(rows, identities):
        """Reject rows whose profile differs from their proven identity."""
        for row in rows:
            user_id = identities[row["task_id"]]
            if user_id is not None and row.get("user_id") not in (None, user_id):
                raise ValueError("MPS task identity differs from its selected profile")
            if user_id is not None:
                row["user_id"] = user_id

    def _insert_archive_history(self, session, job_id, rows, trusted_ids, *, prefetch, lap):
        """Insert history rows; only ``trusted_ids`` may skip the task directory."""
        if prefetch and len(rows) >= 1024:
            from .archive_prefetch import prefetch_archive_keys
            from .job_partitions import task_identity_model

            if task_identity_model(session) is not CentralTaskModel:
                prefetch_archive_keys(
                    session, job_id, (row["task_id"] for row in rows),
                    directory=len(trusted_ids) < len(rows),
                )
        lap("prefetch")
        from .job_partitions import task_conflict_columns
        conflict = tuple(column.name for column in task_conflict_columns(session))
        # ORM executemany both sends individual INSERTs and fragments mixed
        # outcomes when nullable fields differ. Explicit NULLs are equivalent
        # for columns without defaults. Keep omission semantics for defaults,
        # input order and the existing archive/checkpoint transaction.
        null_fields = {
            column.name: None
            for column in CentralTaskModel.__table__.columns
            if column.nullable
            and column.default is None
            and column.server_default is None
        }
        # Untrusted rows first, so the transaction-local trust flag covers
        # only the trusted tail and is cleared before anything else runs.
        parts = [(False, rows)]
        if trusted_ids:
            parts = [
                (False, [row for row in rows if row["task_id"] not in trusted_ids]),
                (True, [row for row in rows if row["task_id"] in trusted_ids]),
            ]
        for trusted, part in parts:
            if not part:
                continue
            if trusted:
                session.execute(_SET_TRUSTED_IDENTITY, {"value": "on"})
            normalized = (
                {**null_fields, **{key: value for key, value in row.items() if value is not None}}
                for row in part
            )
            for columns, group in groupby(normalized, key=lambda row: tuple(sorted(row))):
                records = list(group)
                statement = _archive_insert_statement(columns, conflict)
                # Arrays keep the SQL text and bind count independent of page
                # size, avoiding per-row compilation and query conversion.
                for offset in range(0, len(records), 1000):
                    page = records[offset:offset + 1000]
                    try:
                        session.execute(statement, {
                            f"rows_{name}": [row[name] for row in page]
                            for name in columns
                        })
                    except IntegrityError as exc:
                        # Keep the archive API's ownership error while relying
                        # on the trigger for races with another history writer.
                        diagnostic = getattr(exc.orig, "diag", None)
                        if (
                            getattr(exc.orig, "sqlstate", None) == "23505"
                            and getattr(diagnostic, "message_primary", None)
                            == "Task ID already belongs to another job"
                        ):
                            raise ValueError("Archived task belongs to another job") from exc
                        raise
            if trusted:
                session.execute(_SET_TRUSTED_IDENTITY, {"value": "off"})
        lap("history_insert")

    def _advance_archive_progress(
        self, session, plan, job_id, rows, source_manifest, end, runtime, lap
    ):
        """Record archived ``rows`` on the locked ``plan``: definition statuses,
        spawned children, counters, cursor ``end`` and completion."""
        if source_manifest is not None:
            session.execute(text("""
                UPDATE management.mps_task_definitions SET status=:status
                WHERE manifest_id=:manifest AND task_id=:task_id
            """), [
                {"manifest": source_manifest, "task_id": row["task_id"], "status": row["status"]}
                for row in rows
            ])
        self.definitions.append_spawned(plan, rows, session=session)
        lap("definitions")
        plan.cursor = end
        plan.completed += sum(row["status"] == "completed" for row in rows)
        plan.failed += sum(row["status"] == "failed" for row in rows)
        plan.updated_at = datetime.now(timezone.utc)
        config = json.loads(plan.job_config_json)
        if runtime is not None:
            # Preserve compact execution metadata in the same transaction
            # as the checkpoint, before a cache-pressure yield can remove it.
            # Definition controls and authoritative counters remain SQL-owned.
            for field in ("created_at", "updated_at", "started_at"):
                if runtime.get(field) is not None:
                    config[field] = runtime[field]
            if runtime.get("state") in {"pending", "running", "paused"}:
                config["state"] = runtime["state"]
            for field in (
                "run_count",
                "last_ran_at",
                "last_error",
                "result_message",
                "runtime_telemetry",
            ):
                if field in runtime.get("metadata", {}):
                    config.setdefault("metadata", {})[field] = runtime["metadata"][field]
            from ..job_system.storage import _iso_to_datetime

            session.execute(update(CentralJobModel).where(CentralJobModel.job_id == job_id).values(
                started_at=func.coalesce(CentralJobModel.started_at, _iso_to_datetime(runtime.get("started_at"))),
            ))
        if end == plan.total:
            plan.status = "complete"
            status = "failed" if plan.failed else "completed"
            config.update(state=status, completed_at=plan.updated_at.isoformat(),
                          updated_at=plan.updated_at.isoformat(),
                          completed_tasks=plan.completed, failed_tasks=plan.failed)
            session.execute(update(CentralJobModel).where(CentralJobModel.job_id == job_id).values(
                status=status, completed_at=plan.updated_at,
                result_success=not bool(plan.failed), result_completed_at=plan.updated_at,
                result_message=f"Processed {plan.total} tasks with {plan.failed} failed task(s)",
                result_error=None,
                result_data_json=json.dumps({"completed_tasks": plan.completed, "failed_tasks": plan.failed, "total_tasks": plan.total}),
            ))
        plan.job_config_json = json.dumps(config)
        snapshot = self._snapshot(plan)
        lap("checkpoint")
        return snapshot

    def active_page(self, *, after_job_id: str = "", limit: int, oldest_first: bool = False, due_only: bool = False) -> list[str]:
        _positive_int(limit, "limit")
        with self.session_factory() as session:
            query = select(MPSPlanModel.job_id).where(MPSPlanModel.status.in_(["ready", "preparing"]))
            if due_only:
                # Match the scheduler's UTC interpretation of naive ISO values.
                # Future schedules stay durable without occupying runtime slots.
                session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
                query = query.where(text("""(
                    NOT COALESCE((job_config_json::jsonb #>> '{metadata,mps_schedule_pending}')::boolean,
                        NULLIF(job_config_json::jsonb #>> '{schedule,scheduled_for}', '') IS NOT NULL)
                    OR NULLIF(job_config_json::jsonb #>> '{schedule,scheduled_for}', '') IS NULL
                    OR (job_config_json::jsonb #>> '{schedule,scheduled_for}')::timestamptz <= now()
                )"""))
            if oldest_first:
                if after_job_id:
                    after_time = session.scalar(select(MPSPlanModel.updated_at).where(MPSPlanModel.job_id == after_job_id))
                    if after_time is not None:
                        query = query.where(tuple_(MPSPlanModel.updated_at, MPSPlanModel.job_id) > tuple_(after_time, after_job_id))
                query = query.order_by(MPSPlanModel.updated_at, MPSPlanModel.job_id)
            else:
                query = query.where(MPSPlanModel.job_id > after_job_id).order_by(MPSPlanModel.job_id)
            return list(session.execute(query.limit(limit)).scalars())

    def terminate(
        self, job_id: str, *, status: str, db_session: Any | None = None
    ) -> None:
        if status not in {"cancelled", "failed"}:
            raise ValueError("MPS terminal status must be cancelled or failed")
        with (
            nullcontext(db_session)
            if db_session is not None
            else self.session_factory()
        ) as session:
            session.execute(
                update(MPSPlanModel)
                .where(
                    MPSPlanModel.job_id == job_id,
                    MPSPlanModel.status.in_(["ready", "preparing"]),
                )
                .values(status=status, updated_at=datetime.now(timezone.utc))
            )
            session.execute(text(
                "DELETE FROM management.mps_retry_tasks WHERE job_id=:job_id"
            ), {"job_id": job_id})
            if db_session is None:
                session.commit()

    def archived_output_sizes(self, job_id: str, task_ids: list[str]) -> dict[str, int]:
        """Inspect output byte lengths without transferring full task results."""
        if not task_ids:
            return {}
        with self.session_factory() as session:
            rows = session.execute(text("""
                SELECT task_id, octet_length(COALESCE(
                    (NULLIF(result_json, '')::json -> 'output')::text, 'null'
                )) AS size
                FROM management.job_tasks
                WHERE job_id=:job_id AND task_id=ANY(CAST(:task_ids AS text[]))
            """), {"job_id": job_id, "task_ids": task_ids})
            return {row.task_id: int(row.size) for row in rows}

    def archived_outputs(
        self, job_id: str, task_ids: list[str], *, byte_limit: int
    ) -> dict[str, str]:
        """Transfer only output JSON whose complete page fits the byte budget.

        The SQL-side check also covers a row growing after the size preflight.
        Non-output result fields never cross the database connection here.
        """
        if not task_ids:
            return {}
        with self.session_factory() as session:
            rows = session.execute(text("""
                WITH selected AS MATERIALIZED (
                    SELECT task_id, COALESCE(
                        (NULLIF(result_json, '')::json -> 'output')::text, 'null'
                    ) AS output
                    FROM management.job_tasks
                    WHERE job_id=:job_id AND task_id=ANY(CAST(:task_ids AS text[]))
                )
                SELECT task_id, CASE WHEN
                    sum(octet_length(task_id) + octet_length(output)) OVER () <= :byte_limit
                    THEN output END AS output
                FROM selected
            """), {"job_id": job_id, "task_ids": task_ids, "byte_limit": byte_limit})
            result = {}
            for row in rows:
                if row.output is None:
                    raise RuntimeError("MPS dependency output changed beyond the admission byte budget")
                result[row.task_id] = row.output
            return result
