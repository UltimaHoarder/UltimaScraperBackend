"""Dedicated database-writer process entry point."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import signal
import socket
import threading
import time

import uvloop
from collections.abc import Sequence
from types import FrameType
from typing import Any
from uuid import uuid4

import redis as sync_redis
from redis import exceptions as redis_errors

from ..redis_channels import KEY_PREFIX
from .write_queue import (
    DatabaseWriteClient,
    DatabaseWriterService,
    parse_partition_layout,
    writer_process_count,
)

logger = logging.getLogger(__name__)

WRITER_LEADER_KEY = f"{KEY_PREFIX}:database:writer:leader"
WRITER_LEADER_TTL_SECONDS = 30
WRITER_LEADER_RENEW_SECONDS = 5

_RELEASE_LEADER_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

_RENEW_LEASES_SCRIPT = """
local renewed = 0
for i = 1, #KEYS do
    if redis.call('GET', KEYS[i]) == ARGV[1] then
        redis.call('EXPIRE', KEYS[i], ARGV[2])
        renewed = renewed + 1
    end
end
return renewed
"""

# Compare-and-set the partition layout. Producers stop admitting bulk work
# while it reads "current>target".
_SET_LAYOUT_SCRIPT = """
if (redis.call('GET', KEYS[1]) or '1') ~= ARGV[1] then return 0 end
if ARGV[2] == '1' then redis.call('DEL', KEYS[1]) else redis.call('SET', KEYS[1], ARGV[2]) end
return 1
"""

# Finish a switch only once every old and new partition stream is empty
# (entries leave a stream only when acknowledged), atomically with respect to
# producers' layout-checked enqueues. Records then change partition with
# nothing of theirs still queued, so per-record order survives the switch.
_FINISH_LAYOUT_SWITCH_SCRIPT = """
if (redis.call('GET', KEYS[1]) or '1') ~= ARGV[1] then return 0 end
for i = 2, #KEYS do
    if redis.call('XLEN', KEYS[i]) > 0 then return 0 end
end
if ARGV[2] == '1' then redis.call('DEL', KEYS[1]) else redis.call('SET', KEYS[1], ARGV[2]) end
return 1
"""


def partition_lease_key(partition: int) -> str:
    """Partition 0's lease is the original single-writer lease."""
    return WRITER_LEADER_KEY if partition == 0 else f"{WRITER_LEADER_KEY}:{partition}"


async def acquire_partitions(
    client: object, token: str, ttl: int, processes: int
) -> tuple[int, list[int]]:
    """Take up to this process's share of the active layout's partition leases."""
    redis_client: Any = client
    layout, _ = parse_partition_layout(await redis_client.get(DatabaseWriteClient.LAYOUT_KEY))
    quota = math.ceil(layout / processes)
    held: list[int] = []
    try:
        for partition in range(layout):
            if len(held) >= quota:
                break
            if await redis_client.set(partition_lease_key(partition), token, nx=True, ex=ttl):
                held.append(partition)
    except BaseException:
        for partition in held:
            try:
                await redis_client.eval(
                    _RELEASE_LEADER_SCRIPT, 1, partition_lease_key(partition), token
                )
            except Exception:  # noqa: BLE001 - the lease expires on its own
                pass
        raise
    return layout, held


def coordinate_partitions(
    client: Any, *, held: Sequence[int], layout: int, processes: int
) -> bool:
    """Drive layout changes; return True when this process must re-acquire.

    Runs on the synchronous lease heartbeat. Only the partition-0 owner
    changes the layout, and only after every partition stream has drained.
    """
    raw = client.get(DatabaseWriteClient.LAYOUT_KEY)
    raw_value = (raw.decode() if isinstance(raw, bytes) else raw) or "1"
    current, target = parse_partition_layout(raw)
    if current != layout:
        return True
    if 0 in held:
        if current == processes:
            if target is not None:
                # A stale switch toward another configuration blocks producers.
                client.eval(_SET_LAYOUT_SCRIPT, 1, DatabaseWriteClient.LAYOUT_KEY,
                            raw_value, str(current))
        else:
            return _switch_layout(client, raw_value, current, processes)
    if len(held) < math.ceil(current / processes):
        # A partition nobody holds would never drain; take a fair share again.
        return any(
            partition not in held and not client.exists(partition_lease_key(partition))
            for partition in range(current)
        )
    return False


def _switch_layout(client: Any, raw_value: str, current: int, processes: int) -> bool:
    switching = f"{current}>{processes}"
    if raw_value != switching:
        if client.eval(_SET_LAYOUT_SCRIPT, 1, DatabaseWriteClient.LAYOUT_KEY,
                       raw_value, switching):
            logger.info(
                "Database writer layout switching from %d to %d partition(s); "
                "bulk producers wait while the queue drains",
                current,
                processes,
            )
    streams = [
        DatabaseWriteClient.partition_keys(partition)[0]
        for partition in range(max(current, processes))
    ]
    if client.eval(_FINISH_LAYOUT_SWITCH_SCRIPT, 1 + len(streams),
                   DatabaseWriteClient.LAYOUT_KEY, *streams, switching, str(processes)):
        logger.info("Database writer layout switched to %d partition(s)", processes)
        return True
    return False


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _redis_restart_error(exc: redis_errors.RedisError) -> bool:
    """Retry transport/restart failures without hiding credentials or OOM errors."""
    if isinstance(
        exc, (redis_errors.AuthenticationError, redis_errors.AuthorizationError)
    ):
        return False
    return isinstance(
        exc, (redis_errors.ConnectionError, redis_errors.TimeoutError)
    ) or (
        isinstance(exc, redis_errors.ResponseError) and str(exc).startswith("NOGROUP ")
    )


def database_writer_main() -> None:
    """Run the sole write-pool owner for this USR runtime."""
    from ..logging_utils import configure_runtime_logging

    configure_runtime_logging("database-writer")

    # This is a separate process, so the role is fixed before any PostgreSQL
    # engine is constructed.  Pool construction uses it to select the bounded
    # writer pool instead of request/worker-reader pooling.
    os.environ["ULTIMA_DATABASE_PROCESS_ROLE"] = "writer"
    shutdown_requested = threading.Event()

    def handle_signal(_signum: int, _frame: FrameType | None) -> None:
        shutdown_requested.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    async def run() -> None:
        from ..config import UltimaScraperCollectionConfig
        from ..redis.client import RedisManager
        from .write_executor import PostgreSQLWriteExecutor

        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        redis_manager = RedisManager(
            config=config.settings.redis,
        )
        connected = await redis_manager.connect()
        if not connected:
            raise RuntimeError("Database writer could not connect to Redis")
        try:
            client = redis_manager.client
            leader_ttl = _positive_int_env(
                "ULTIMA_DATABASE_WRITER_LEADER_TTL_SECONDS",
                WRITER_LEADER_TTL_SECONDS,
            )
            renew_seconds = min(
                _positive_int_env(
                    "ULTIMA_DATABASE_WRITER_LEADER_RENEW_SECONDS",
                    WRITER_LEADER_RENEW_SECONDS,
                ),
                max(1, leader_ttl // 3),
            )

            processes = writer_process_count()
            # Several processes keep one consumer name each, so their own
            # pending entries stay theirs across partition re-assignment.
            consumer_name = (
                None
                if processes == 1
                else f"writer-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
            )
            while not shutdown_requested.is_set():
                leader_token = uuid4().hex
                try:
                    layout, held = await acquire_partitions(
                        client, leader_token, leader_ttl, processes
                    )
                except redis_errors.RedisError as exc:
                    if not _redis_restart_error(exc):
                        raise
                    logger.warning("Database writer waiting for Redis: %s", exc)
                    await asyncio.sleep(1)
                    continue
                if not held:
                    await asyncio.sleep(1)
                    continue

                if processes == 1 and layout == 1:
                    logger.info(
                        "Database writer candidate acquired the global writer lease"
                    )
                else:
                    logger.info(
                        "Database writer acquired partition lease(s) %s of %d "
                        "(%d writer process(es) configured)",
                        ",".join(str(partition) for partition in held),
                        layout,
                        processes,
                    )
                lease_keys = [partition_lease_key(partition) for partition in held]
                heartbeat_stop = threading.Event()
                leader_lost = threading.Event()
                reassign = threading.Event()

                def renew_leader_lease() -> None:
                    redis_config = redis_manager.config
                    socket_timeout = max(
                        1.0,
                        min(float(renew_seconds), float(leader_ttl) / 6.0),
                    )
                    heartbeat_client = sync_redis.Redis(
                        host=redis_config.host,
                        port=redis_config.port,
                        db=redis_config.db,
                        password=redis_config.password or None,
                        socket_timeout=socket_timeout,
                        socket_connect_timeout=socket_timeout,
                        decode_responses=True,
                    )
                    consecutive_errors = 0
                    last_confirmed_at = time.monotonic()
                    loss_after_seconds = max(
                        float(renew_seconds),
                        float(leader_ttl - (2 * renew_seconds)),
                    )
                    try:
                        while not heartbeat_stop.wait(renew_seconds):
                            try:
                                renewed = heartbeat_client.eval(
                                    _RENEW_LEASES_SCRIPT,
                                    len(lease_keys),
                                    *lease_keys,
                                    leader_token,
                                    str(leader_ttl),
                                ) == len(lease_keys)
                            except Exception as exc:  # noqa: BLE001
                                consecutive_errors += 1
                                unconfirmed_seconds = (
                                    time.monotonic() - last_confirmed_at
                                )
                                logger.warning(
                                    "Database writer lease renewal failed "
                                    "(%d error(s), %.1fs unconfirmed): %s",
                                    consecutive_errors,
                                    unconfirmed_seconds,
                                    exc,
                                )
                                if unconfirmed_seconds >= loss_after_seconds:
                                    leader_lost.set()
                                    return
                                continue
                            consecutive_errors = 0
                            if not renewed:
                                leader_lost.set()
                                return
                            last_confirmed_at = time.monotonic()
                            try:
                                # Keep renewing after a reassignment request:
                                # the service still owns its partitions while
                                # its lanes drain.
                                if not reassign.is_set() and coordinate_partitions(
                                    heartbeat_client,
                                    held=held,
                                    layout=layout,
                                    processes=processes,
                                ):
                                    reassign.set()
                            except Exception as exc:  # noqa: BLE001
                                logger.warning(
                                    "Database writer partition check failed: %s", exc
                                )
                    finally:
                        heartbeat_client.close()

                heartbeat = threading.Thread(
                    target=renew_leader_lease,
                    name="database-writer-leader-heartbeat",
                    daemon=True,
                )
                heartbeat.start()
                try:
                    executor = PostgreSQLWriteExecutor(redis_manager)
                    writer = DatabaseWriterService(
                        redis_manager,
                        executor,
                        consumer_name=consumer_name,
                        partitions=held,
                    )
                    await writer.run(
                        stop_requested=lambda: (
                            shutdown_requested.is_set()
                            or leader_lost.is_set()
                            or reassign.is_set()
                        )
                    )
                except redis_errors.RedisError as exc:
                    if not _redis_restart_error(exc):
                        raise
                    # Service shutdown drains owned SQL work and closes the
                    # executor. Re-election constructs a new service/group and
                    # replays durable receipts before accepting further writes.
                    logger.warning(
                        "Database writer returning to standby after Redis loss: %s", exc
                    )
                finally:
                    heartbeat_stop.set()
                    heartbeat.join(timeout=max(renew_seconds * 2, 2))
                    for lease_key in lease_keys:
                        try:
                            await client.eval(
                                _RELEASE_LEADER_SCRIPT,
                                1,
                                lease_key,
                                leader_token,
                            )
                        except Exception as exc:  # noqa: BLE001
                            logger.warning(
                                "Could not release database writer lease cleanly: %s",
                                exc,
                            )

                if reassign.is_set() and not shutdown_requested.is_set():
                    # The layout or partition ownership changed; take a fresh
                    # share of the partitions at once.
                    continue

                if leader_lost.is_set() and not shutdown_requested.is_set():
                    logger.warning(
                        "Database writer lost its lease; returning to standby"
                    )
                if not shutdown_requested.is_set():
                    await asyncio.sleep(1)
        finally:
            await redis_manager.disconnect()

    try:
        asyncio.run(run(), loop_factory=uvloop.new_event_loop)
    except KeyboardInterrupt:
        shutdown_requested.set()
    except Exception:
        logger.exception("Database writer runtime crashed")
        raise
