"""Dedicated plugin service; the API never owns background plugin loops."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

from ultima_scraper_core import load_env, runtime_env_value

from ..database.threads import run_database_call
from .recovery import database_interrupted, error_summary
from .runtime_owner import PluginRuntimeOwnershipLost

logger = logging.getLogger(__name__)


async def serve(stop: asyncio.Event):
    from ultima_scraper_db_archive.global_db import global_session

    from ..api.dependencies import get_backend_instance, shutdown_backend
    from .execution import PluginTaskRunner
    from .platform import PluginPlatform
    from .postgres_repository import PostgresPluginRepository
    from .runtime_owner import PluginRuntimeCoordinator

    configured = runtime_env_value("USR_PLUGINS_DIR")
    root = (
        Path(configured).expanduser().resolve()
        if configured
        else Path(runtime_env_value("USR_REPOSITORY_ROOT") or Path.cwd()) / "plugins"
    )

    def bootstrap():
        repository = PostgresPluginRepository(
            global_session, legacy_runtime_dir=root / ".runtime"
        )
        platform = PluginPlatform(root, repository=repository)
        platform.load_all()
        return platform

    coordinator = None
    runner = None
    try:
        platform = await run_database_call(bootstrap)
        platform.attach_backend(await get_backend_instance())
        if stop.is_set():
            return
        coordinator = PluginRuntimeCoordinator(platform, root)
        coordinator.start()
        while not stop.is_set():
            if coordinator._task is not None and coordinator._task.done():
                coordinator._task.result()
            for task in platform.runtime_tasks():
                if task.done():
                    task.result()
            if (
                coordinator.is_owner
                and platform._runtime_started
                and (runner is None or runner.done())
            ):
                if runner is not None:
                    runner.result()
                runner = asyncio.create_task(PluginTaskRunner(platform).loop())
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except TimeoutError:
                pass
    finally:
        # Preserve the primary failure, while still closing every owned
        # resource. Cleanup may itself encounter the same database outage.
        if runner is not None:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        cleanup = []
        if coordinator is not None:
            cleanup.extend(
                await asyncio.gather(coordinator.stop(), return_exceptions=True)
            )
        cleanup.extend(await asyncio.gather(shutdown_backend(), return_exceptions=True))
        for error in cleanup:
            if isinstance(error, Exception):
                logger.warning(
                    "Plugin supervisor cleanup encountered %s", type(error).__name__
                )


async def supervise():
    """Re-elect after interruptions; programming and schema errors stay fatal."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    failures = 0
    while not stop.is_set():
        try:
            await serve(stop)
            return
        except Exception as error:
            if not isinstance(
                error, PluginRuntimeOwnershipLost
            ) and not database_interrupted(error):
                raise
            failures += 1
            original = getattr(error, "orig", error)
            pool_unavailable = any(
                message in str(original)
                for message in ("server_login_retry", "query_wait_timeout")
            )
            delay = min(30, (15 if pool_unavailable else 1) * 2 ** min(failures - 1, 5))
            logger.warning(
                "Plugin runtime paused after %s; reinitializing and re-electing in %ss",
                error_summary(error),
                delay,
            )
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass


def main():
    load_env()
    from ..logging_utils import configure_runtime_logging

    configure_runtime_logging("plugin-supervisor")
    os.environ["ULTIMA_DATABASE_PROCESS_ROLE"] = "application"
    from .postgres_repository import PluginSchemaNotReady

    try:
        asyncio.run(supervise())
    except PluginSchemaNotReady as exc:
        logging.getLogger(__name__).critical("%s", exc)
        raise SystemExit(78) from exc


if __name__ == "__main__":
    main()
