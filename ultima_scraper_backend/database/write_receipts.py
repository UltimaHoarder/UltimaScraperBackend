"""PostgreSQL writer receipts, committed with their database mutations."""

from __future__ import annotations

import json
import time

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from sqlalchemy import String, insert, select, text
from ultima_scraper_db_archive.global_db import DatabaseWriteReceiptModel

if TYPE_CHECKING:
    from .write_queue import DatabaseWriteCommand, DatabaseWriteResult


class DatabaseWriteReceiptBusy(Exception):
    """Another SQL transaction owns this command; defer without failing it."""


@dataclass(frozen=True, slots=True)
class _ReceiptSnapshot:
    command_id: str
    operation: str
    site: str | None
    result_json: str
    post_commit_json: str | None


_EXECUTION_RECEIPTS = text("""
SELECT mutation.run_id, mutation.task_id, mutation.mutation_key,
       mutation.command_id AS mutation_command_id,
       direct_receipt.command_id AS direct_command_id,
       direct_receipt.operation AS direct_operation,
       direct_receipt.site AS direct_site,
       direct_receipt.result_json AS direct_result_json,
       direct_receipt.post_commit_json AS direct_post_commit_json,
       logical_receipt.command_id AS logical_command_id,
       logical_receipt.operation AS logical_operation,
       logical_receipt.site AS logical_site,
       logical_receipt.result_json AS logical_result_json,
       logical_receipt.post_commit_json AS logical_post_commit_json
FROM unnest(CAST(:command_ids AS text[]), CAST(:run_ids AS text[]),
            CAST(:task_ids AS text[]), CAST(:mutation_keys AS text[]))
     AS requested(command_id, run_id, task_id, mutation_key)
LEFT JOIN management.execution_mutations AS mutation
  ON mutation.run_id=requested.run_id AND mutation.task_id=requested.task_id
 AND mutation.mutation_key=requested.mutation_key
 AND mutation.run_id=ANY(CAST(:history_run_ids AS text[]))
LEFT JOIN management.database_write_receipts AS direct_receipt
  ON direct_receipt.command_id=requested.command_id
LEFT JOIN management.database_write_receipts AS logical_receipt
  ON logical_receipt.command_id=NULLIF(mutation.command_id, requested.command_id)
WHERE mutation.command_id IS NOT NULL OR direct_receipt.command_id IS NOT NULL
""")


def _read_execution_receipts(
    session: Any,
    command_ids: Sequence[str],
    identities: Mapping[str, tuple[str, str, str] | None],
) -> tuple[dict[tuple[str, str, str], str], dict[str, Any]]:
    """Read direct and logical outcomes together, after receipt locks are held.

    All three joins use primary keys. Keep the lock statement separate so this
    read takes a fresh READ COMMITTED snapshot after acquiring those locks.
    Missing logical receipts and existing direct receipts remain distinguishable.
    """
    ids = sorted(set(command_ids))
    keys = [identities[command_id] or (None, None, None) for command_id in ids]
    committed = {}
    existing = {}
    result = session.execute(_EXECUTION_RECEIPTS, {
        "command_ids": ids,
        "run_ids": [key[0] for key in keys],
        "task_ids": [key[1] for key in keys],
        "mutation_keys": [key[2] for key in keys],
        # A join-derived key alone prevents planning-time partition pruning.
        "history_run_ids": sorted({key[0] for key in keys if key[0] is not None}),
    })
    try:
        for row in result.mappings():
            if row["mutation_command_id"] is not None:
                committed[(row["run_id"], row["task_id"], row["mutation_key"])] = row["mutation_command_id"]
            for prefix in ("direct", "logical"):
                if (
                    (command_id := row[f"{prefix}_command_id"]) is not None
                    and command_id not in existing
                ):
                    existing[command_id] = _ReceiptSnapshot(
                        command_id,
                        row[f"{prefix}_operation"],
                        row[f"{prefix}_site"],
                        row[f"{prefix}_result_json"],
                        row[f"{prefix}_post_commit_json"],
                    )
    finally:
        result.close()
    return committed, existing


def _lock(session: Any, command_ids: Sequence[str], *, defer_if_busy: bool = False) -> None:
    # Consistent order also protects against a replacement writer overlapping
    # a still-running SQL transaction after losing its Redis leader lease.
    if defer_if_busy:
        # Acquire in explicit deterministic order. The caller rolls the transaction
        # back on contention, releasing any earlier locks from this batch.
        acquired = session.execute(
            text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(value, 0)) "
                "FROM (SELECT value FROM unnest(CAST(:ids AS text[])) AS value ORDER BY value) ordered_ids"
            ),
            {"ids": ["database-write:" + value for value in sorted(set(command_ids))]},
        ).scalars().all()
        if not all(acquired):
            raise DatabaseWriteReceiptBusy("Database write receipt is owned by another transaction")
        return
    session.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended(value, 0)) "
            "FROM unnest(CAST(:ids AS text[])) AS value"
        ),
        {"ids": ["database-write:" + value for value in sorted(set(command_ids))]},
    )


@lru_cache(maxsize=32)
def _unnest_receipt_insert(table: Any, dialect: Any, columns: tuple[str, ...]) -> Any:
    """One fixed-text INSERT per table and column set, fed by typed arrays.

    A multi-row VALUES statement changes text with every page size and carries
    one bind per row x column; psycopg re-split and re-converted it on every
    batch (~13% of writer CPU). This text never changes, so its parse caches.
    """
    quote = dialect.identifier_preparer.quote
    names = ", ".join(quote(column) for column in columns)

    def element(column: str) -> str:
        # An explicit CAST to varchar(n) truncates silently; casting to the
        # unbounded type leaves the column's assignment check to reject overlong
        # values exactly as the multi-row VALUES insert did.
        type_ = table.c[column].type
        if isinstance(type_, String) and type_.length:
            type_ = String()
        return type_.compile(dialect=dialect)

    arrays = ", ".join(
        f"CAST(:receipt_{column} AS {element(column)}[])" for column in columns
    )
    return text(
        f"INSERT INTO {dialect.identifier_preparer.format_table(table)} ({names}) "
        f"SELECT * FROM unnest({arrays})"
    )


def _insert_receipt_rows(session: Any, table: Any, records: list[dict[str, Any]]) -> None:
    """Insert receipt rows within the caller's SQL transaction, one statement per page."""
    connection = session.connection(bind_arguments={"clause": table})
    for offset in range(0, len(records), 1000):
        page = records[offset:offset + 1000]
        columns = tuple(page[0])
        if connection.dialect.name != "postgresql":
            session.execute(insert(table).values(page))
            continue
        statement = _unnest_receipt_insert(table, connection.dialect, columns)
        connection.execute(statement, {
            f"receipt_{column}": [row[column] for row in page] for column in columns
        })


def _read(session: Any, command_ids: Sequence[str]) -> dict[str, Any]:
    if not command_ids:
        return {}
    return {
        row.command_id: row
        for row in session.execute(
            select(DatabaseWriteReceiptModel).where(
                DatabaseWriteReceiptModel.command_id.in_(command_ids)
            )
        ).scalars()
    }


def _receipt_owner_runs(session: Any, run_ids: Sequence[str]) -> set[str]:
    """Prevent late failure/replay receipts from surviving explicit deletion.

    Receipt writers lock commands before runs. Deletion uses nonblocking command
    locks, so it can yield while this shared fence drains before disposal.
    """
    from ..job_system import run_turnstile

    run_turnstile.lock_shared(session, run_ids)
    return set(session.scalars(text("""SELECT run_id FROM management.execution_runs
        WHERE run_id=ANY(CAST(:runs AS text[]))
          AND coalesce(config_json::jsonb->'metadata'->>'deletion_pending','')<>'true'
        ORDER BY run_id FOR SHARE"""), {"runs": sorted(set(run_ids))}))


def _stored_result_json(result: DatabaseWriteResult) -> str:
    """Serialize a receipt without the per-command writer timing.

    Producers read timing from the Redis result cache; a receipt only has to
    prove the outcome, and the timing was most of its size.
    """
    from .write_queue import WRITE_TIMING_KEY

    if WRITE_TIMING_KEY not in result.payload:
        return result.to_json()
    payload = {
        key: value for key, value in result.payload.items() if key != WRITE_TIMING_KEY
    }
    return replace(result, payload=payload).to_json()


@contextmanager
def receipt_batch(
    session: Any,
    commands: Sequence[DatabaseWriteCommand],
    results: dict[str, dict[str, Any]],
    *, timing: dict[str, float] | None = None,
) -> Iterator[list[DatabaseWriteCommand]]:
    """Return unapplied commands and stage their receipts before caller commit.

    The caller owns this transaction and must not commit inside the block.
    Neither mutation nor receipt survives a rollback. Existing receipts skip
    mutation and return the original payload, including counts from that write.
    """
    from .write_queue import DatabaseWriteRejected, DatabaseWriteResult

    from ultima_scraper_db_archive.execution_models import ExecutionMutationModel

    def mutation_identity(command):
        identity = command.execution_attempt
        return (identity["run_id"], identity["task_id"], command.execution_key) if identity and command.execution_key else None

    identities = {command.command_id: mutation_identity(command) for command in commands}
    logical = ["execution-mutation:" + json.dumps(identity) for identity in identities.values() if identity]
    started = time.perf_counter() if timing is not None else 0.0
    _lock(session, [command.command_id for command in commands] + logical, defer_if_busy=True)
    if timing is not None:
        timing['receipt_lock_ms'] = (time.perf_counter()-started)*1000.0
        started = time.perf_counter()
    committed_mutations = {}
    if logical:
        committed_mutations, existing = _read_execution_receipts(
            session, [command.command_id for command in commands], identities
        )
    else:
        existing = _read(session, [command.command_id for command in commands])
    if timing is not None:
        timing['receipt_read_ms'] = (time.perf_counter()-started)*1000.0
    pending: list[DatabaseWriteCommand] = []
    replayed: list[DatabaseWriteCommand] = []
    seen: set[str] = set()
    logical_pending = {}
    aliases = {}
    for command in commands:
        identity = identities[command.command_id]
        row = existing.get(committed_mutations.get(identity, command.command_id))
        if row is not None:
            if row.operation != command.operation.value or row.site != command.site:
                raise ValueError(
                    "Database write command ID was reused for a different operation or site"
                )
            result = DatabaseWriteResult.from_json(row.result_json)
            if not result.success:
                raise DatabaseWriteRejected(
                    result.error or "Database write was rejected"
                )
            results[command.command_id] = result.payload
            if command.command_id not in existing:
                replayed.append(command)
        elif command.command_id not in seen:
            if identity is not None and identity in logical_pending:
                aliases[command.command_id] = logical_pending[identity]
            else:
                pending.append(command)
                if identity is not None:
                    logical_pending[identity] = command.command_id
            seen.add(command.command_id)
    session.info["database_writer_receipt_batch"] = True
    try:
        started = time.perf_counter() if timing is not None else 0.0
        attempts = [
            command.execution_attempt
            for command in pending
            if command.execution_attempt is not None
        ]
        if attempts:
            from ..job_system.ledger import validate_write_attempts

            validate_write_attempts(session, attempts)
        if timing is not None:
            timing['write_fence_ms'] = (time.perf_counter()-started)*1000.0
        yield pending
    finally:
        session.info.pop("database_writer_receipt_batch", None)
    for alias, original in aliases.items():
        results[alias] = results[original]
    completed_at = datetime.now(timezone.utc).isoformat()
    rows = []
    mutation_rows = []
    pending_ids = {command.command_id for command in pending}
    by_id = {command.command_id: command for command in commands}
    applied = {command.command_id: command for command in [*pending, *replayed]}
    applied.update({alias: by_id[alias] for alias in aliases})
    replay_run_ids = {command.execution_attempt["run_id"] for command in replayed
                      if command.execution_attempt}
    replay_owners = _receipt_owner_runs(session, list(replay_run_ids)) if replay_run_ids else set()
    replayed_ids = {command.command_id for command in replayed}
    for command in applied.values():
        if (command.command_id in replayed_ids and command.execution_attempt
                and command.execution_attempt["run_id"] not in replay_owners):
            continue  # Return the original outcome without recreating deleted history.
        result = DatabaseWriteResult(
            command_id=command.command_id,
            success=True,
            payload=results[command.command_id],
            error=None,
            completed_at=completed_at,
        )
        rows.append(
            {
                "command_id": command.command_id,
                "run_id": command.execution_attempt["run_id"] if command.execution_attempt else None,
                "operation": command.operation.value,
                "site": command.site,
                "result_json": _stored_result_json(result),
                "post_commit_json": None,
            }
        )
        identity = identities[command.command_id]
        if identity is not None and command.command_id in pending_ids:
            mutation_rows.append({
                "run_id": identity[0], "task_id": identity[1],
                "mutation_key": identity[2], "command_id": command.command_id,
            })
    if rows:
        # Preserve the old ORM autoflush boundary, then use one SQL statement
        # per bounded page. DBAPI executemany sends a separate INSERT for every
        # row, even when it is pipelined under one driver call.
        session.flush()
        for table, records in (
            (ExecutionMutationModel.__table__, mutation_rows),
            (DatabaseWriteReceiptModel.__table__, rows),
        ):
            if records:
                _insert_receipt_rows(session, table, records)


def lookup(command_ids: Sequence[str]) -> dict[str, DatabaseWriteResult]:
    """Read only the requested outcomes; callers must supply bounded batches."""
    from ultima_scraper_db_archive.global_db import global_session
    from .write_queue import DatabaseWriteResult

    if not command_ids:
        return {}
    with global_session() as session:
        return {
            key: DatabaseWriteResult.from_json(row.result_json)
            for key, row in _read(session, command_ids).items()
        }


def record_failure(
    command: DatabaseWriteCommand,
    result: DatabaseWriteResult,
    *,
    raw_fields: Mapping[str, str] | None = None,
) -> DatabaseWriteResult:
    """Retain failed input before Redis acknowledges it; preserve any prior commit."""
    import json
    from ultima_scraper_db_archive.global_db import global_session
    from .write_queue import DatabaseWriteResult

    with global_session() as session:
        _lock(session, [command.command_id])
        existing = _read(session, [command.command_id]).get(command.command_id)
        if existing is not None:
            return DatabaseWriteResult.from_json(existing.result_json)
        if command.execution_attempt and command.execution_attempt["run_id"] not in _receipt_owner_runs(
            session, [command.execution_attempt["run_id"]]
        ):
            # A deliberately deleted job has abandoned SQL recovery history.
            # The writer still publishes the rejection and acknowledges Redis.
            return result
        session.execute(
            insert(DatabaseWriteReceiptModel).values(
                command_id=command.command_id,
                run_id=command.execution_attempt["run_id"] if command.execution_attempt else None,
                operation=command.operation.value,
                site=command.site,
                result_json=_stored_result_json(result),
                failed_command_json=json.dumps(dict(raw_fields), sort_keys=True)
                if raw_fields is not None
                else (command.to_json() if not result.success else None),
            )
        )
        session.commit()
    return result


def record_success(
    command: DatabaseWriteCommand, payload: dict[str, Any]
) -> dict[str, Any]:
    """Complete a resumable maintenance command after its verified SQL batches."""
    from ultima_scraper_db_archive.global_db import global_session

    results: dict[str, dict[str, Any]] = {}
    with global_session() as session:
        with receipt_batch(session, [command], results) as pending:
            if pending:
                results[command.command_id] = payload
        session.commit()
    return results[command.command_id]


def record_invalid(entry_id: str, fields: Mapping[str, str], error: str) -> None:
    """Archive an unparseable stream entry before releasing its Redis capacity."""
    import json
    from ultima_scraper_db_archive.global_db import global_session
    from .write_queue import DatabaseWriteResult, stable_command_id

    command_id = stable_command_id("invalid-writer-entry", entry_id)
    with global_session() as session:
        _lock(session, [command_id])
        if command_id in _read(session, [command_id]):
            return
        result = DatabaseWriteResult(
            command_id=command_id,
            success=False,
            payload={"stream_id": entry_id},
            error=error,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        session.execute(
            insert(DatabaseWriteReceiptModel).values(
                command_id=command_id,
                operation="invalid_entry",
                site=None,
                result_json=result.to_json(),
                failed_command_json=json.dumps(dict(fields), sort_keys=True),
            )
        )
        session.commit()
