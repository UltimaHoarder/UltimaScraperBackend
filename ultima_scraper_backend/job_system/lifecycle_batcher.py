"""Worker-local, count/byte bounded preparation and completion queues."""

import asyncio
import logging
import time
from collections import deque

from anyio import CancelScope
from ultima_scraper_core.processes import finish_owned

from ..database.threads import run_database_call
from .ledger import _dump

logger = logging.getLogger(__name__)


class LifecycleBatcher:
    def __init__(self, ledger, *, flush_seconds=0.005):
        self.ledger = ledger
        self.flush_seconds = flush_seconds
        self.max_tasks = ledger.limits.batch
        self.max_bytes = ledger.limits.result_bytes
        self.capacity_tasks = 2 * self.max_tasks
        # A single valid result can include a retry copy and JSON escaping.
        self.capacity_bytes = 4 * self.max_bytes + 65536
        self._queues = {kind: deque() for kind in ("checkpoint", "finish")}
        self._condition = asyncio.Condition()
        self._tasks = self._bytes = 0
        self._runners = {}
        self._closed = False

    async def submit(self, kind, request, *, metrics=None, phase=None):
        if kind not in {"checkpoint", "finish"}:
            raise ValueError("Unknown lifecycle operation")
        started = time.monotonic()
        size = len(_dump({**request, "ordinal": 0})) + 32
        if size > self.capacity_bytes:
            raise ValueError("Lifecycle request exceeds its queue byte budget")
        future = asyncio.get_running_loop().create_future()
        async with self._condition:
            await self._condition.wait_for(
                lambda: (
                    self._closed
                    or (
                        self._tasks < self.capacity_tasks
                        and self._bytes + size <= self.capacity_bytes
                    )
                )
            )
            if self._closed:
                raise RuntimeError("Lifecycle queue is closed")
            self._tasks += 1
            self._bytes += size
            self._queues[kind].append((request, future, size, metrics, phase, started))
            if kind not in self._runners:
                self._runners[kind] = asyncio.create_task(
                    self._drain(kind), name=f"ledger-{kind}-batches"
                )

        async def wait():
            return await future

        owned = asyncio.create_task(wait())
        try:
            return await asyncio.shield(owned)
        except asyncio.CancelledError:
            # Once queued, SQL ownership remains ours through commit/rollback.
            with CancelScope(shield=True):
                try:
                    await finish_owned(owned)
                except Exception:
                    logger.debug(
                        "Cancelled lifecycle request rolled back", exc_info=True
                    )
            raise

    async def _drain(self, kind):
        # Preparation must not sit behind a completion transaction, or vice
        # versa. Both runners share the original count/byte queue budget and
        # existing SQL admission; each phase still has one ordered runner.
        queue = self._queues[kind]
        try:
            while queue:
                await asyncio.sleep(self.flush_seconds)
                entries, size = [], 0
                while queue and len(entries) < self.max_tasks:
                    next_size = queue[0][2]
                    if entries and size + next_size > self.max_bytes:
                        break
                    entries.append(queue.popleft())
                    size += next_size
                submitted = time.monotonic()
                execution_started = execution_finished = None

                def call(entries=entries):
                    nonlocal execution_started, execution_finished
                    execution_started = time.monotonic()
                    try:
                        method = (
                            self.ledger.checkpoint_batch
                            if kind == "checkpoint"
                            else self.ledger.finish_batch
                        )
                        return method([entry[0] for entry in entries])
                    finally:
                        execution_finished = time.monotonic()

                try:
                    results = await run_database_call(call)
                except Exception as error:  # noqa: BLE001 - transaction failure belongs to every request in this batch
                    results = [error] * len(entries)
                finished = time.monotonic()
                for entry, error in zip(entries, results, strict=True):
                    _, future, _, metrics, phase, started = entry
                    if metrics is not None:
                        metrics.record_database_span(
                            phase,
                            started,
                            finished,
                            execution_started,
                            execution_finished,
                            submitted=submitted,
                        )
                    if error is None:
                        future.set_result(None)
                    else:
                        future.set_exception(error)
                async with self._condition:
                    self._tasks -= len(entries)
                    self._bytes -= size
                    self._condition.notify_all()
        finally:
            self._runners.pop(kind, None)

    async def close(self):
        async with self._condition:
            self._closed = True
            self._condition.notify_all()
        if self._runners:
            owned = asyncio.gather(*self._runners.values())
            try:
                await asyncio.shield(owned)
            except asyncio.CancelledError:
                with CancelScope(shield=True):
                    await finish_owned(owned)
                raise


def worker_batcher(pool, ledger):
    batcher = getattr(pool, "_ledger_lifecycle_batcher", None)
    if batcher is None:
        batcher = pool._ledger_lifecycle_batcher = LifecycleBatcher(ledger)
    elif batcher.ledger is not ledger:
        raise ValueError("Worker lifecycle queue belongs to another ledger")
    return batcher
