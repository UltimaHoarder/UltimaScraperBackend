"""Bounded producer-local polling of committed writer results."""

import asyncio
import time
from collections import deque


class WriteResultWaiter:
    def __init__(self, client, keys, *, capacity, poll_seconds):
        self.client = client
        self.keys = keys
        self.slots = asyncio.Semaphore(capacity)
        self.poll_seconds = poll_seconds
        self.batch_size = min(128, capacity)
        self.pending = {}
        self.runner = None

    async def snapshot(self, command_id):
        # Caller deadlines include admission to this bounded registry. A
        # cancelled observer never cancels the accepted durable command.
        async with self.slots:
            future = asyncio.get_running_loop().create_future()
            self.pending[future] = command_id
            if self.runner is None:
                self.runner = asyncio.create_task(
                    self._poll(), name="database-write-results"
                )
            try:
                result, available, submitted, observed_at, received_at = await future
                if observed_at is not None:
                    # Include delivery back to this producer's coroutine, using
                    # elapsed monotonic time on the same Redis clock basis.
                    observed_at += max(0, time.monotonic() - received_at) * 1000
                return result, available, submitted, observed_at
            finally:
                self.pending.pop(future, None)

    async def _poll(self):
        try:
            # Coalesce observers registered together before the first read.
            await asyncio.sleep(0)
            while self.pending:
                observers = deque(self.pending.items())
                while observers:
                    page = [
                        observers.popleft()
                        for _ in range(min(len(observers), self.batch_size))
                    ]
                    ids = list(dict.fromkeys(command_id for _, command_id in page))
                    async with self.client.pipeline(transaction=False) as pipe:
                        await pipe.mget(
                            [key for command_id in ids for key in self.keys(command_id)]
                        )
                        await pipe.time()
                        values, clock = await pipe.execute(raise_on_error=False)
                    received_at = time.monotonic()
                    if isinstance(values, Exception):
                        raise values
                    observed_at = None
                    try:
                        observed_at = float(clock[0]) * 1000 + float(clock[1]) / 1000
                    except TypeError, ValueError, IndexError:
                        pass  # Profiling does not determine commit authority.
                    snapshots = {
                        command_id: (
                            *values[index * 3 : index * 3 + 3],
                            observed_at,
                            received_at,
                        )
                        for index, command_id in enumerate(ids)
                    }
                    for future, command_id in page:
                        if future.done():
                            continue
                        result = snapshots[command_id]
                        if result[0] is not None or result[2] is None:
                            self.pending.pop(future, None)
                            future.set_result(result)
                    # Do not retain a page of results across the next sleep.
                    del snapshots, values, page
                if self.pending:
                    await asyncio.sleep(self.poll_seconds)
        except Exception as error:  # noqa: BLE001 - A failed snapshot belongs to every pending observer.
            for future in tuple(self.pending):
                if not future.done():
                    future.set_exception(error)
            self.pending.clear()
        except asyncio.CancelledError:
            for future in tuple(self.pending):
                future.cancel()
            self.pending.clear()
            raise
        finally:
            self.runner = None
