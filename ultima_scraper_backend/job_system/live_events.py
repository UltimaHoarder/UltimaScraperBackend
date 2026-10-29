"""Best-effort live delivery of per-task lifecycle events.

Task events drive the WebUI's live job view and idle wake-up hints. Durable
consumers (plugin feeds, event replay and retention) read job-level events only:
plugins normalize task events away. Storing two outbox rows per task made bulk
jobs outrun the relay by hours and held tens of gigabytes per run. These events
are published to Redis after their transaction commits and are never stored. A
lost one delays a live refresh; durable ``job_progress`` summaries and the UI's
own polling still converge.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from collections.abc import Callable
from typing import Any

from sqlalchemy import event as sa_event

logger = logging.getLogger(__name__)

_PENDING = "live_task_events"
_HOOKED = "live_task_events_hooked"
# Batches, not events: one per committed transaction. A full queue drops new
# batches rather than holding memory or stalling the committing thread.
MAX_QUEUED_BATCHES = 1024
DRAIN_BATCHES = 64

Publisher = Callable[[list[dict[str, Any]]], None]
_publisher: Publisher | None = None


def set_publisher(publisher: Publisher | None) -> None:
    """Install this process's publisher; None disables live task events."""
    global _publisher
    _publisher = publisher


def install_redis(*, url: str | None = None, host: str | None = None, port: int = 6379,
                  db: int = 0, password: str | None = None) -> None:
    """Publish this process's task events through its own Redis connection."""
    if isinstance(_publisher, RedisPublisher) and _publisher.pid == os.getpid():
        return
    set_publisher(RedisPublisher(url=url, host=host, port=port, db=db, password=password))


def defer(session, payload: dict[str, Any]) -> None:
    """Publish ``payload`` only if ``session``'s current transaction commits."""
    if not session.info.get(_HOOKED):
        sa_event.listen(session, "after_commit", _after_commit)
        sa_event.listen(session, "after_rollback", _after_rollback)
        session.info[_HOOKED] = True
    session.info.setdefault(_PENDING, []).append(payload)


def _after_commit(session) -> None:
    payloads = session.info.pop(_PENDING, None)
    publisher = _publisher
    if not payloads or publisher is None:
        return
    try:
        publisher(payloads)
    except Exception:  # noqa: BLE001 - live hints never fail a committed write
        logger.debug("Could not queue live task events", exc_info=True)


def _after_rollback(session) -> None:
    session.info.pop(_PENDING, None)


class RedisPublisher:
    """Hand committed batches to one background thread that pipelines them."""

    def __init__(self, *, url=None, host=None, port=6379, db=0, password=None):
        self.pid = os.getpid()
        self._connection = {"url": url, "host": host, "port": port, "db": db, "password": password}
        self._queue: queue.Queue[list[dict[str, Any]]] = queue.Queue(MAX_QUEUED_BATCHES)
        self.dropped = 0
        self._thread = threading.Thread(target=self._run, name="live-task-events", daemon=True)
        self._thread.start()

    def __call__(self, payloads: list[dict[str, Any]]) -> None:
        try:
            self._queue.put_nowait(payloads)
        except queue.Full:
            self.dropped += len(payloads)

    def _client(self):
        import redis as sync_redis

        options = {"socket_timeout": 2.0, "socket_connect_timeout": 2.0}
        if self._connection["url"]:
            return sync_redis.Redis.from_url(self._connection["url"], **options)
        return sync_redis.Redis(
            host=self._connection["host"], port=self._connection["port"],
            db=self._connection["db"], password=self._connection["password"] or None, **options,
        )

    def _run(self) -> None:
        from ..redis_channels import HOOKS_CHANNEL

        client = None
        warned_at = 0.0
        while True:
            batches = [self._queue.get()]
            while len(batches) < DRAIN_BATCHES:
                try:
                    batches.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            try:
                client = client or self._client()
                pipeline = client.pipeline(transaction=False)
                for payloads in batches:
                    for payload in payloads:
                        pipeline.publish(HOOKS_CHANNEL, json.dumps(payload))
                pipeline.execute()
            except Exception:  # noqa: BLE001 - drop this batch; reconnect next time
                client = None
                self.dropped += sum(len(payloads) for payloads in batches)
                if time.monotonic() - warned_at > 60:
                    warned_at = time.monotonic()
                    logger.warning("Live task events are not reaching Redis; dropped %d so far",
                                   self.dropped, exc_info=True)
                time.sleep(1.0)
