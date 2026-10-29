"""Dedicated ledger archiver process (ULTIMA_ARCHIVE_PROCESS=1).

Archive passes are CPU-heavy Python (history rows, evidence checks). In the
control plane they share one interpreter lock with refill planning, the event
relay and recovery, so a busy archiver slows admission. Here they run in
their own process, under their own scheduling lease, with the same loop and
the same ULTIMA_ARCHIVE_PARALLEL_PASSES setting.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import uvloop

logger = logging.getLogger(__name__)

LEASE_ROLE = "archiver"


class _Stop(Exception):
    """Raised inside the task group when the process is asked to stop."""


async def run_archiver(redis_manager, *, ledger=None, stop: asyncio.Event | None = None) -> None:
    """Run archive loops until ``stop`` is set (or the task is cancelled)."""
    from .idle import SchedulingLease, listen_for_wakeups
    from .ledger import ExecutionLedger, archive_parallel_passes
    from .ledger_runtime import archive_loop, publish_refill_diagnostics

    ledger = ledger or ExecutionLedger()
    coordination = SchedulingLease(redis_manager, role=LEASE_ROLE)
    wake = asyncio.Event()
    diagnostic_queue: asyncio.Queue = asyncio.Queue(maxsize=16)
    stop = stop or asyncio.Event()

    async def diagnostics():
        while True:
            observations = await diagnostic_queue.get()
            try:
                if redis_manager is not None and redis_manager.is_connected:
                    await publish_refill_diagnostics(redis_manager.client, observations)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Could not publish archive diagnostics", exc_info=True)
            finally:
                diagnostic_queue.task_done()

    passes = archive_parallel_passes()
    logger.info("Ledger archiver process running %d archive pass(es)", passes)
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(coordination.run(), name="archiver-scheduling-coordination")
            tasks.create_task(listen_for_wakeups(redis_manager, [wake]), name="archiver-wake-hints")
            tasks.create_task(diagnostics(), name="archiver-diagnostics")
            for index in range(passes):
                tasks.create_task(
                    archive_loop(ledger, coordination, wake, diagnostic_queue),
                    name=f"archiver-pass-{index}",
                )

            async def stopper():
                await stop.wait()
                raise _Stop  # Ends the task group, cancelling the loops.

            tasks.create_task(stopper(), name="archiver-stop")
    except* _Stop:
        pass
    finally:
        await coordination.close()


def ledger_archiver_main() -> None:
    """Process entry point used by the launcher."""
    os.environ["ULTIMA_DATABASE_PROCESS_ROLE"] = "application"

    async def main() -> None:
        from ..config import UltimaScraperCollectionConfig
        from ..redis.client import RedisManager

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop.set)
        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        redis_manager = RedisManager(config=config.settings.redis)
        if not await redis_manager.connect():
            # Without Redis the lease falls back to bounded polling; archive
            # SQL stays correct either way (row locks own each batch).
            logger.warning("Ledger archiver could not connect to Redis; using SQL fallback polling")
        try:
            await run_archiver(redis_manager, stop=stop)
        finally:
            client = getattr(redis_manager, "_client", None) or getattr(redis_manager, "client", None)
            if client is not None:
                try:
                    await client.aclose()
                except Exception:  # noqa: BLE001 - shutting down
                    logger.debug("Archiver Redis close failed", exc_info=True)

    asyncio.run(main(), loop_factory=uvloop.new_event_loop)
