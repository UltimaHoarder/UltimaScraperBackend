"""Stage ordinary task inputs on configured temporary storage with bounded buffers."""

from __future__ import annotations

import asyncio
import json
import tempfile
from collections.abc import AsyncIterable, Iterable, Iterator
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from ultima_scraper_core.processes import finish_owned

from ..api.job_update_body import _file_call
from .models import Task
from .storage import TaskPayloadBudgetExceeded


def _row_size(row: dict[str, Any]) -> int:
    return sum(
        len(str(value).encode("utf-8")) for value in row.values() if value is not None
    )


@dataclass
class StagedTaskInputs:
    source: BinaryIO
    batch_size: int
    max_bytes: int
    count: int = 0
    max_record_bytes: int = 0
    sites: list[str] = field(default_factory=list)

    def tasks_by_site(self) -> Iterator[Task]:
        """Replay supported sites in first-appearance order without a task index in RAM."""
        for site in self.sites:
            for batch in self.batches():
                for row in batch:
                    if str(row["site"]).lower() == site:
                        yield Task.from_dict(json.loads(row["definition_json"]))

    def batches(self) -> Iterator[list[dict[str, Any]]]:
        """Iterate only locally generated rows, in insertion order, on the SQL thread."""
        self.source.seek(0)
        rows: list[dict[str, Any]] = []
        occupied = 0
        read = 0
        while raw := self.source.readline(self.max_record_bytes + 1):
            if len(raw) > self.max_record_bytes:
                raise RuntimeError("Staged task input record exceeds its recorded size")
            row = json.loads(raw)
            size = _row_size(row)
            if size > self.max_bytes:
                raise TaskPayloadBudgetExceeded(
                    row["task_id"], size, self.max_bytes, phase="SQL input insert"
                )
            if rows and (
                len(rows) == self.batch_size or occupied + size > self.max_bytes
            ):
                yield rows
                rows, occupied = [], 0
            rows.append(row)
            occupied += size
            read += 1
        if read != self.count:
            raise RuntimeError("Staged task input count changed")
        if rows:
            yield rows


async def _tasks(source: Iterable[Task] | AsyncIterable[Task]):
    asynchronous = isinstance(source, AsyncIterable)
    iterator = aiter(source) if asynchronous else iter(source)
    try:
        if asynchronous:
            async for task in iterator:
                yield task
        else:
            for task in iterator:
                yield task
    finally:
        # Staging owns the iterator it consumes, including early rejection and
        # cancellation. Drain async finalizers before their temporary inputs close.
        if asynchronous:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                cleanup = asyncio.create_task(close())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await finish_owned(cleanup)
                    raise
        else:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


@asynccontextmanager
async def stage_task_inputs(
    source: Iterable[Task] | AsyncIterable[Task] | None,
    *,
    job_id: str,
    input_max_bytes: int,
):
    """Finish input building before the edit lease or a database session is held."""
    if source is None:
        yield None
        return
    from ..api.routes.job_central_store import (
        _central_task_insert_batch_size,
        _task_to_central_row,
    )
    from ..api.routes.jobs import _central_task_insert_max_bytes

    # TemporaryFile honors TMPDIR; it does not keep an expanding in-memory spool.
    # Every owned write drains before cancellation closes this file.
    with tempfile.TemporaryFile(mode="w+b", prefix="usr-task-inputs-") as stream:
        staged = StagedTaskInputs(
            stream, _central_task_insert_batch_size(), _central_task_insert_max_bytes()
        )
        records: list[bytes] = []
        occupied = 0
        async with aclosing(_tasks(source)) as tasks:
            async for task in tasks:
                task.job_id = job_id
                row = _task_to_central_row(job_id, task)
                site = str(task.site_name).lower()
                if site not in staged.sites:
                    staged.sites.append(site)
                if row["definition_json"] is None:
                    raise ValueError(
                        "Ordinary task inputs cannot use an MPS definition"
                    )
                size = _row_size(row)
                if size > staged.max_bytes:
                    raise TaskPayloadBudgetExceeded(
                        task.task_id, size, staged.max_bytes, phase="SQL input insert"
                    )
                definition_bytes = len(row["definition_json"].encode("utf-8"))
                if definition_bytes > input_max_bytes:
                    raise TaskPayloadBudgetExceeded(
                        task.task_id,
                        definition_bytes,
                        input_max_bytes,
                        phase="SQL input read",
                    )
                encoded = json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\n"
                if records and (
                    len(records) == staged.batch_size
                    or occupied + len(encoded) > staged.max_bytes
                ):
                    await _file_call(stream.write, b"".join(records))
                    records, occupied = [], 0
                records.append(encoded)
                occupied += len(encoded)
                staged.max_record_bytes = max(staged.max_record_bytes, len(encoded))
                staged.count += 1
        if records:
            await _file_call(stream.write, b"".join(records))
        del records
        if not staged.count:
            raise ValueError("No valid tasks provided")
        yield staged
