"""Durable Redis command transport for PostgreSQL mutations.

Worker processes are producers only.  A bounded database-writer runtime owns
the PostgreSQL write pools and acknowledges stream entries only after their
transaction commits.  The transport is deliberately small and operation
agnostic; database-specific execution lives in :mod:`write_executor`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import socket
import time
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol, TypeAlias, cast
from uuid import uuid4

from redis.exceptions import RedisError
from ultima_scraper_core import runtime_env_value

from ..redis_channels import KEY_PREFIX
from . import write_queue_scripts, write_result_cache
from .write_receipts import DatabaseWriteReceiptBusy

logger = logging.getLogger(__name__)
WRITE_TIMING_KEY = "__database_write_timing__"

RedisFields: TypeAlias = Mapping[str | bytes, Any]
StreamEntry: TypeAlias = tuple[str | bytes, RedisFields]


@dataclass(slots=True)
class _OpenLaneBatch:
    pending: list[tuple[str, "DatabaseWriteCommand"]]
    encoded_bytes: int
    refill_wait_ms: float
    opened_at: float = field(default_factory=time.monotonic)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None


class _WriterSlots:
    """SQL slots that wake interactive control lanes before bulk lanes.

    ``asyncio.Semaphore`` wakes waiters strictly in arrival order, so a job
    submission could wait behind every bulk lane already queued for a slot.
    """

    def __init__(self, size: int) -> None:
        self._value = size
        self._waiters: tuple[deque[asyncio.Future[None]], deque[asyncio.Future[None]]] = (
            deque(),
            deque(),
        )

    async def acquire(self, *, priority: bool = False) -> bool:
        if self._value > 0:
            self._value -= 1
            return True
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters[0 if priority else 1].append(future)
        try:
            await future
        except asyncio.CancelledError:
            # A slot handed over just before cancellation must not leak.
            if future.done() and not future.cancelled():
                self.release()
            raise
        return True

    def release(self) -> None:
        for waiters in self._waiters:
            while waiters:
                future = waiters.popleft()
                if not future.done():
                    future.set_result(None)
                    return
        self._value += 1

    def locked(self) -> bool:
        return self._value == 0

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(self, *_exc: object) -> None:
        self.release()

    def priority(self) -> "_PrioritySlot":
        return _PrioritySlot(self)


class _PrioritySlot:
    def __init__(self, slots: _WriterSlots) -> None:
        self._slots = slots

    async def __aenter__(self) -> None:
        await self._slots.acquire(priority=True)

    async def __aexit__(self, *_exc: object) -> None:
        self._slots.release()


class DatabaseWriteOperation(str, Enum):
    """Database mutations accepted by the canonical writer."""

    UPSERT_CONTENT_BATCH = "upsert_content_batch"
    UPDATE_MEDIA_SELECTION = "update_media_selection"
    UPSERT_USER = "upsert_user"
    UPSERT_SUBSCRIPTIONS = "upsert_subscriptions"
    UPSERT_INVALID_USER = "upsert_invalid_user"
    FINALIZE_DOWNLOAD = "finalize_download"
    UPDATE_DRM_STREAMS = "update_drm_streams"
    SYNC_JOB_STATUS = "sync_job_status"
    SYNC_TASK_STATUS = "sync_task_status"
    ARCHIVE_JOB_TASKS = "archive_job_tasks"
    PREPARE_MPS_JOB = "prepare_mps_job"
    RETRY_MPS_JOB = "retry_mps_job"
    ADVANCE_MPS_RETRY = "advance_mps_retry"
    UPDATE_MPS_CONFIG = "update_mps_config"
    BEGIN_MPS_REPLACEMENT = "begin_mps_replacement"
    APPEND_MPS_REPLACEMENT = "append_mps_replacement"
    COMMIT_MPS_REPLACEMENT = "commit_mps_replacement"
    ARCHIVE_MPS_WINDOW = "archive_mps_window"
    TERMINATE_MPS_JOB = "terminate_mps_job"
    RECONCILE_CENTRAL_JOBS = "reconcile_central_jobs"


type WriterLaneKey = tuple[str | None, str, DatabaseWriteOperation, int]
type WriterIdentityKey = tuple[str | None, str, str]

# Interactive commands that create new work use a small priority stream, so a
# submission does not wait behind the bulk backlog. Each owns a fresh identity;
# no earlier bulk mutation can target it, so the separate stream cannot reorder
# mutations for one record.
PRIORITY_OPERATIONS = frozenset({DatabaseWriteOperation.PREPARE_MPS_JOB})


class DatabaseWriteError(RuntimeError):
    """Base class for writer transport and execution failures."""


class DatabaseWriteTimeout(DatabaseWriteError):
    """Raised when a caller did not receive a writer result in time."""


class DatabaseWriteRejected(DatabaseWriteError):
    """Raised when the writer permanently rejects or dead-letters a command."""


@dataclass(frozen=True, slots=True)
class DatabaseWriteCommand:
    """Versioned, JSON-safe database mutation envelope."""

    command_id: str
    operation: DatabaseWriteOperation
    site: str | None
    payload: dict[str, Any]
    producer_id: str
    created_at: str
    version: int = 1
    execution_attempt: dict[str, str] | None = None
    execution_key: str | None = None

    def _json_value(self) -> dict[str, Any]:
        value = {
            "version": self.version,
            "command_id": self.command_id,
            "operation": self.operation.value,
            "site": self.site,
            "payload": self.payload,
            "producer_id": self.producer_id,
            "created_at": self.created_at,
        }
        if self.execution_attempt is not None:
            value["execution_attempt"] = self.execution_attempt
        if self.execution_key is not None:
            value["execution_key"] = self.execution_key
        return value

    def to_json(self) -> str:
        return json.dumps(self._json_value(), separators=(",", ":"), sort_keys=True)

    def to_bytes(self, *, max_bytes: int) -> bytes:
        # Use the native JSON encoder. Producer batches bound the input graph;
        # a Python token-by-token encoder was substantially slower in profiling.
        # to_json uses ensure_ascii, so len is also the eventual byte count.
        encoded = self.to_json()
        if len(encoded) > max_bytes:
            raise DatabaseWriteRejected(
                f"Database write command {self.command_id} exceeds "
                f"the maximum of {max_bytes} bytes"
            )
        return encoded.encode("ascii")

    @classmethod
    def from_json(cls, raw: str | bytes) -> "DatabaseWriteCommand":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("Database write command must be a JSON object")
        version = int(value.get("version", 0))
        if version not in {1, 2}:
            raise ValueError(f"Unsupported database write command version: {version}")
        execution_attempt = value.get("execution_attempt")
        execution_key = value.get("execution_key")
        if execution_key is not None and (version != 2 or not isinstance(execution_key, str)
                or not execution_key or len(execution_key) > 128):
            raise ValueError("Execution mutation keys require version 2 and at most 128 characters")
        if version == 2:
            if (not isinstance(execution_attempt, dict)
                    or set(execution_attempt) != {"run_id", "task_id", "token", "owner"}
                    or any(not isinstance(item, str) or not item for item in execution_attempt.values())):
                raise ValueError("Version 2 writer commands require a complete execution attempt")
        elif execution_attempt is not None:
            raise ValueError("Execution attempts require writer command version 2")
        payload = value.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("Database write command payload must be an object")
        site_value = value.get("site")
        return cls(
            command_id=str(value["command_id"]),
            operation=DatabaseWriteOperation(str(value["operation"])),
            site=(str(site_value).strip().lower() if site_value else None),
            payload=cast(dict[str, Any], payload),
            producer_id=str(value.get("producer_id") or "unknown"),
            created_at=str(value["created_at"]),
            version=version,
            execution_attempt=execution_attempt,
            execution_key=execution_key,
        )


@dataclass(frozen=True, slots=True)
class DatabaseWriteResult:
    """Terminal command result cached in Redis for producers and retries."""

    command_id: str
    success: bool
    payload: dict[str, Any]
    error: str | None
    completed_at: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "command_id": self.command_id,
                "success": self.success,
                "payload": self.payload,
                "error": self.error,
                "completed_at": self.completed_at,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "DatabaseWriteResult":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("Database write result must be a JSON object")
        payload = value.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValueError("Database write result payload must be an object")
        return cls(
            command_id=str(value["command_id"]),
            success=bool(value["success"]),
            payload=cast(dict[str, Any], payload),
            error=(str(value["error"]) if value.get("error") else None),
            completed_at=str(value["completed_at"]),
        )


class DatabaseWriteExecutor(Protocol):
    """Business executor owned by :class:`DatabaseWriterService`."""

    async def execute_batch(
        self, commands: Sequence[DatabaseWriteCommand]
    ) -> Mapping[str, dict[str, Any]]:
        """Commit commands and return a payload for every command id."""

    async def persist_outcome(self, command: DatabaseWriteCommand, result: DatabaseWriteResult) -> DatabaseWriteResult:
        """Retain a terminal outcome durably before acknowledging Redis work."""

    async def persist_invalid(self, entry_id: str, fields: Mapping[str, str], error: str) -> None:
        """Retain malformed input durably before acknowledging it."""

    async def close(self) -> None:
        """Release writer-owned database resources."""


def stable_command_id(*parts: object) -> str:
    """Build a deterministic idempotency key from canonical command parts."""

    digest = hashlib.sha256()
    for part in parts:
        encoded = str(part).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _archive_redis_fields(fields: RedisFields) -> dict[str, str]:
    # JSON's ASCII escaping preserves surrogate-escaped bytes reversibly, even
    # when the malformed stream value is not UTF-8.
    def retained(value: Any) -> str:
        return value.decode("utf-8", errors="surrogateescape") if isinstance(value, bytes) else str(value)

    return {retained(key): retained(value) for key, value in fields.items()}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _positive_float_env(
    name: str, default: float, *, value: float | None = None
) -> float:
    raw = runtime_env_value(name) if value is None else value
    if raw is None or (value is None and isinstance(raw, str) and not raw.strip()):
        return default
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive finite number") from exc
    if isinstance(raw, bool) or not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return parsed


def _positive_int_env(name: str, default: int, *, value: int | None = None) -> int:
    raw = runtime_env_value(name) if value is None else value
    if raw is None or (value is None and isinstance(raw, str) and not raw.strip()):
        return default
    try:
        parsed = int(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if isinstance(raw, bool) or not isinstance(raw, (str, int)) or parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


WRITER_PROCESSES_ENV = "ULTIMA_DATABASE_WRITER_PROCESSES"


def writer_process_count() -> int:
    """Configured database-writer processes; 1 keeps the single-stream writer."""
    return _positive_int_env(WRITER_PROCESSES_ENV, 1)


def parse_partition_layout(raw: object) -> tuple[int, int | None]:
    """Return the active bulk partition count and a pending target, if any."""
    if raw is None:
        return 1, None
    current, _, target = _decode(raw).partition(">")
    return int(current), (int(target) if target else None)


def _lookup_write_receipt(command_id: str) -> DatabaseWriteResult | None:
    from .write_receipts import lookup

    return lookup([command_id]).get(command_id)


def _skip_enqueue_result_precheck() -> bool:
    """ULTIMA_DATABASE_ENQUEUE_SKIP_RESULT_PRECHECK=1 drops the read before enqueue."""
    return os.environ.get("ULTIMA_DATABASE_ENQUEUE_SKIP_RESULT_PRECHECK", "").strip() == "1"


def _stale_attempt(exc: BaseException | None) -> BaseException | None:
    """The fence failure behind a batch error, if a command's attempt lost it."""
    from ..job_system.ledger import StaleAttempt

    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, StaleAttempt):
            return exc
        exc = exc.__cause__ or exc.__context__
    return None


class DatabaseWriteClient:
    """Submit durable commands and await their committed result."""

    STREAM_KEY = f"{KEY_PREFIX}:database:writes"
    RESULT_KEY = f"{KEY_PREFIX}:database:write:result:{{command_id}}"
    RESULT_AVAILABLE_KEY = f"{KEY_PREFIX}:database:write:available:{{command_id}}"
    SUBMITTED_KEY = f"{KEY_PREFIX}:database:write:submitted:{{command_id}}"
    RESULT_TTL_SECONDS = 7 * 24 * 60 * 60
    DEFAULT_TIMEOUT_SECONDS = 15 * 60.0
    MAX_COMMAND_BYTES = 64 * 1024 * 1024
    QUEUED_BYTES_KEY = f"{KEY_PREFIX}:database:writes:bytes"
    NOTIFY_KEY = f"{KEY_PREFIX}:database:writes:notify"
    PRIORITY_STREAM_KEY = f"{KEY_PREFIX}:database:writes:priority"
    PRIORITY_QUEUED_BYTES_KEY = f"{KEY_PREFIX}:database:writes:priority:bytes"
    PRIORITY_NOTIFY_KEY = f"{KEY_PREFIX}:database:writes:priority:notify"
    # Writer-local entry IDs carry their stream; Redis never sees this prefix.
    PRIORITY_ENTRY_PREFIX = "priority:"
    # With several writer processes, bulk commands are split into partitions
    # by record identity, so one record's mutations stay on one stream and
    # one process. Partition 0 is the original stream: a one-partition layout
    # uses exactly the original keys. Absent means one partition; "L>N" means
    # the writers are draining L partitions before switching to N.
    LAYOUT_KEY = f"{KEY_PREFIX}:database:writes:layout"

    def __init__(
        self,
        redis_manager: object,
        *,
        producer_id: str | None = None,
        timeout_seconds: float | None = None,
        max_command_bytes: int | None = None,
        max_queue_bytes: int | None = None,
        max_queue_commands: int | None = None,
        enqueue_timeout_seconds: float | None = None,
        enqueue_poll_max_seconds: float | None = None,
        result_poll_max_seconds: float | None = None,
        result_waiter_poll_seconds: float | None = None,
    ) -> None:
        self.redis = redis_manager
        self.producer_id = (
            producer_id or f"producer-{socket.gethostname()}-{os.getpid()}"
        )
        self.timeout_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITE_TIMEOUT_SECONDS",
            self.DEFAULT_TIMEOUT_SECONDS,
            value=timeout_seconds,
        )
        self.max_command_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES",
            self.MAX_COMMAND_BYTES,
            value=max_command_bytes,
        )
        writer_concurrency = _positive_int_env("ULTIMA_DATABASE_WRITER_CONCURRENCY", 4)
        writer_batch_size = _positive_int_env("ULTIMA_DATABASE_WRITER_BATCH_SIZE", 100)
        # Derive omitted controls from existing configured writer budgets.
        # Explicit queue settings remain independent of those derivations.
        self.max_queue_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_QUEUE_MAX_BYTES",
            self.max_command_bytes * writer_concurrency,
            value=max_queue_bytes,
        )
        self.max_queue_commands = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_QUEUE_MAX_COMMANDS",
            writer_batch_size * writer_concurrency,
            value=max_queue_commands,
        )
        # The priority stream holds only control commands; it needs room for
        # one maximum command, not a bulk window.
        self.max_priority_queue_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_PRIORITY_QUEUE_MAX_BYTES",
            self.max_command_bytes,
        )
        self.max_priority_queue_commands = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_PRIORITY_QUEUE_MAX_COMMANDS",
            writer_concurrency * 4,
        )
        self.enqueue_timeout_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITE_ENQUEUE_TIMEOUT_SECONDS",
            self.timeout_seconds,
            value=enqueue_timeout_seconds,
        )
        self.enqueue_poll_max_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITE_ENQUEUE_POLL_MAX_SECONDS",
            0.1,
            value=enqueue_poll_max_seconds,
        )
        self.result_poll_max_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITE_RESULT_POLL_MAX_SECONDS",
            0.1,
            value=result_poll_max_seconds,
        )
        # How often this producer's waiter re-reads all of its pending results
        # (one MGET pipeline per interval). 5 ms keeps delivery latency low; a
        # longer interval adds that much to each write but costs far fewer
        # Redis round trips when hundreds of writes are pending.
        self.result_waiter_poll_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITE_RESULT_WAITER_POLL_SECONDS",
            min(0.005, self.result_poll_max_seconds),
            value=result_waiter_poll_seconds,
        )
        self._enqueue_script: Any = None
        self._result_waiter: Any = None
        self._partitions: int | None = None

    def _client(self) -> Any:
        client = getattr(self.redis, "client", None)
        if client is None:
            raise DatabaseWriteError("Redis is unavailable for database writes")
        return client

    @classmethod
    def result_key(cls, command_id: str) -> str:
        return cls.RESULT_KEY.format(command_id=command_id)

    @classmethod
    def partition_keys(cls, partition: int) -> tuple[str, str, str]:
        """Return the stream, byte counter and notify list of a bulk partition."""
        if partition == 0:
            return cls.STREAM_KEY, cls.QUEUED_BYTES_KEY, cls.NOTIFY_KEY
        stream = f"{cls.STREAM_KEY}:p{partition}"
        return stream, f"{stream}:bytes", f"{stream}:notify"

    @classmethod
    def entry_prefix(cls, stream: str) -> str:
        """Return the writer-local entry ID prefix of a stream."""
        if stream == cls.PRIORITY_STREAM_KEY:
            return cls.PRIORITY_ENTRY_PREFIX
        if stream.startswith(cls.STREAM_KEY + ":p"):
            return stream.removeprefix(cls.STREAM_KEY + ":") + ":"
        return ""

    @staticmethod
    def command_partition(command: "DatabaseWriteCommand", partitions: int) -> int:
        """Route a command by the identity the writer orders it on."""
        if partitions <= 1:
            return 0
        site, domain, target = DatabaseWriterService._writer_identity_key(command)
        digest = hashlib.sha256(f"{site}\0{domain}\0{target}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % partitions

    @classmethod
    def entry_stream(cls, entry_id: str) -> tuple[str, str, str]:
        """Return the stream, byte counter and Redis ID of a writer entry."""
        if entry_id.startswith(cls.PRIORITY_ENTRY_PREFIX):
            return (
                cls.PRIORITY_STREAM_KEY,
                cls.PRIORITY_QUEUED_BYTES_KEY,
                entry_id.removeprefix(cls.PRIORITY_ENTRY_PREFIX),
            )
        if entry_id.startswith("p"):
            # Redis stream IDs are numeric; only partition entries start with "p".
            label, raw_id = entry_id.split(":", 1)
            stream, bytes_key, _ = cls.partition_keys(int(label[1:]))
            return stream, bytes_key, raw_id
        return cls.STREAM_KEY, cls.QUEUED_BYTES_KEY, entry_id

    @classmethod
    def submitted_key(cls, command_id: str) -> str:
        return cls.SUBMITTED_KEY.format(command_id=command_id)

    @classmethod
    def result_available_key(cls, command_id: str) -> str:
        return cls.RESULT_AVAILABLE_KEY.format(command_id=command_id)

    async def submit(
        self,
        operation: DatabaseWriteOperation,
        payload: Mapping[str, Any],
        *,
        site: str | None = None,
        command_id: str | None = None,
        timeout_seconds: float | None = None,
        execution_attempt: dict[str, str] | None = None,
        execution_key: str | None = None,
    ) -> dict[str, Any]:
        # A per-call timeout bounds the entire submit, including admission.
        # Omitted timeouts keep the independently configured stage budgets.
        deadline = (
            time.monotonic()
            + _positive_float_env(
                "timeout_seconds", self.timeout_seconds, value=timeout_seconds
            )
            if timeout_seconds is not None
            else None
        )
        queued = await self.enqueue(
            operation,
            payload,
            site=site,
            command_id=command_id,
            timeout_seconds=timeout_seconds,
            execution_attempt=execution_attempt,
            execution_key=execution_key,
        )
        queued_command_id = queued[0] if isinstance(queued, tuple) else queued
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise DatabaseWriteTimeout(
                f"Timed out waiting for database write {queued_command_id}; "
                "the accepted command remains queued"
            )
        return await self.wait_for_result(
            queued_command_id,
            timeout_seconds=remaining,
        )

    async def enqueue(
        self,
        operation: DatabaseWriteOperation,
        payload: Mapping[str, Any],
        *,
        site: str | None = None,
        command_id: str | None = None,
        timeout_seconds: float | None = None,
        _include_timing: bool = False,
        execution_attempt: dict[str, str] | None = None,
        execution_key: str | None = None,
    ) -> str | tuple[str, dict[str, float]]:
        """Durably enqueue a mutation without waiting for its SQL commit.

        Producers wait for admission capacity, but do not wait for SQL commit.
        Data-producing tasks use :meth:`submit` when task completion must wait
        for the committed mutation.
        """

        admission_started = time.monotonic()
        serialization_started = time.monotonic()
        normalized_site = site.strip().lower() if site else None
        command = DatabaseWriteCommand(
            command_id=command_id or uuid4().hex,
            operation=operation,
            site=normalized_site,
            payload=dict(payload),
            producer_id=self.producer_id,
            created_at=_utc_now(),
            version=2 if execution_attempt else 1,
            execution_attempt=execution_attempt,
            execution_key=execution_key,
        )
        command_build_ms = (time.monotonic() - serialization_started) * 1000.0
        timeout = (
            self.enqueue_timeout_seconds
            if timeout_seconds is None
            else _positive_float_env(
                "timeout_seconds", self.enqueue_timeout_seconds, value=timeout_seconds
            )
        )
        deadline = time.monotonic() + timeout
        enqueue_timing = {
            "producer_serialization_ms": command_build_ms,
            "producer_capacity_wait_ms": 0.0,
            "producer_redis_enqueue_ms": 0.0,
            "producer_admission_overhead_ms": 0.0,
        }

        def finish(command_id_value: str) -> str | tuple[str, dict[str, float]]:
            admission_ms = (time.monotonic() - admission_started) * 1000.0
            enqueue_timing["producer_admission_overhead_ms"] = max(
                0.0,
                admission_ms
                - enqueue_timing["producer_serialization_ms"]
                - enqueue_timing["producer_capacity_wait_ms"]
                - enqueue_timing["producer_redis_enqueue_ms"],
            )
            return (
                (command_id_value, enqueue_timing)
                if _include_timing
                else command_id_value
            )

        client = self._client()

        async def published_result() -> bool:
            try:
                existing_result = await client.get(self.result_key(command.command_id))
            except RedisError as exc:
                raise DatabaseWriteError(
                    f"Redis could not read database write {command.command_id}: {exc}"
                ) from exc
            if existing_result is None:
                return False
            result = DatabaseWriteResult.from_json(existing_result)
            if not result.success:
                self._unwrap_result(result)
            return True

        # The enqueue script returns a published result itself, so the separate
        # read only matters for a command that is too large to encode.
        skip_precheck = _skip_enqueue_result_precheck()
        if not skip_precheck and await published_result():
            return finish(command.command_id)

        priority = operation in PRIORITY_OPERATIONS
        queue_max_bytes = (
            self.max_priority_queue_bytes if priority else self.max_queue_bytes
        )
        queue_max_commands = (
            self.max_priority_queue_commands if priority else self.max_queue_commands
        )
        encoding_started = time.monotonic()
        try:
            encoded = command.to_bytes(
                max_bytes=min(self.max_command_bytes, queue_max_bytes)
            )
            enqueue_timing["producer_serialization_ms"] += (
                time.monotonic() - encoding_started
            ) * 1000.0
        except DatabaseWriteRejected:
            enqueue_timing["producer_serialization_ms"] += (
                time.monotonic() - encoding_started
            ) * 1000.0
            if skip_precheck and await published_result():
                return finish(command.command_id)
            # A retry may arrive after the original result was evicted and the
            # operator lowered the input budget. Its committed receipt still wins.
            receipt = await self._receipt_until(command.command_id, deadline)
            if receipt is None:
                raise
            self._unwrap_result(receipt)
            return finish(command.command_id)

        if self._enqueue_script is None:
            self._enqueue_script = client.register_script(write_queue_scripts.ENQUEUE)
        delay = 0.05
        try:
            while True:
                redis_started = time.monotonic()
                if priority:
                    # Priority commands own fresh identities; never partitioned.
                    layout = "*"
                    stream_keys = (
                        self.PRIORITY_STREAM_KEY,
                        self.PRIORITY_QUEUED_BYTES_KEY,
                        self.PRIORITY_NOTIFY_KEY,
                    )
                else:
                    if self._partitions is None:
                        self._partitions = parse_partition_layout(
                            await client.get(self.LAYOUT_KEY)
                        )[0]
                    layout = str(self._partitions)
                    stream_keys = self.partition_keys(
                        self.command_partition(command, self._partitions)
                    )
                response = await self._enqueue_script(
                    keys=[
                        stream_keys[0],
                        self.submitted_key(command.command_id),
                        self.result_key(command.command_id),
                        stream_keys[1],
                        stream_keys[2],
                        self.LAYOUT_KEY,
                    ],
                    args=[encoded, queue_max_bytes, queue_max_commands, layout],
                    client=client,
                )
                enqueue_timing["producer_redis_enqueue_ms"] += (
                    time.monotonic() - redis_started
                ) * 1000.0
                status = _decode(response[0])
                if status == "completed":
                    self._unwrap_result(DatabaseWriteResult.from_json(response[1]))
                    return finish(command.command_id)
                if status == "queued":
                    return finish(command.command_id)
                if status == "layout":
                    self._partitions = parse_partition_layout(response[1])[0]
                    continue
                if status == "switching":
                    # Writers are draining the old layout; read it again later.
                    self._partitions = None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DatabaseWriteTimeout(
                        f"Database write {command.command_id} was not enqueued within "
                        f"{timeout:.1f}s: queue admission blocked ({status}, "
                        f"{response[1]} bytes, {response[2]} commands); "
                        "accepted commands remain queued"
                    )
                sleep_seconds = min(delay, remaining)
                wait_started = time.monotonic()
                await asyncio.sleep(sleep_seconds)
                enqueue_timing["producer_capacity_wait_ms"] += (
                    time.monotonic() - wait_started
                ) * 1000.0
                delay = min(delay * 1.5, self.enqueue_poll_max_seconds)
        except RedisError as exc:
            raise DatabaseWriteError(
                f"Redis could not enqueue database write {command.command_id}: {exc}"
            ) from exc

    async def _receipt_until(self, command_id: str, deadline: float) -> DatabaseWriteResult | None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DatabaseWriteTimeout(f"Timed out waiting for database write {command_id}")
        try:
            async with asyncio.timeout(remaining):
                return await asyncio.to_thread(_lookup_write_receipt, command_id)
        except TimeoutError as exc:
            raise DatabaseWriteTimeout(f"Timed out reading the durable receipt for database write {command_id}") from exc
        except Exception as exc:
            raise DatabaseWriteError(f"PostgreSQL could not read the durable receipt for database write {command_id}") from exc

    async def wait_for_result(
        self,
        command_id: str,
        *,
        timeout_seconds: float | None = None,
        include_timing: bool = False,
    ) -> dict[str, Any]:
        timeout = (
            _positive_float_env(
                "timeout_seconds", self.timeout_seconds, value=timeout_seconds
            )
            if timeout_seconds is not None
            else self.timeout_seconds
        )
        deadline = time.monotonic() + timeout
        # Short commits should not pay a 50 ms delivery floor. Retain the
        # existing capped backoff and absolute deadline for a busy writer.
        delay = 0.005
        client = self._client()
        if self._result_waiter is None:
            from .write_result_waiter import WriteResultWaiter

            self._result_waiter = WriteResultWaiter(
                client,
                lambda value: (
                    self.result_key(value),
                    self.result_available_key(value),
                    self.submitted_key(value),
                ),
                capacity=self.max_queue_commands,
                poll_seconds=self.result_waiter_poll_seconds,
            )
        while True:
            try:
                # Read delivery and queued state in one Redis snapshot. A
                # separate EXISTS can run after publication/ACK and mistake
                # a just-published cache result for an evicted result.
                async with asyncio.timeout(max(0, deadline - time.monotonic())):
                    raw_result, raw_available_at, raw_submitted, observed_at = (
                        await self._result_waiter.snapshot(command_id)
                    )
            except TimeoutError as exc:
                raise DatabaseWriteTimeout(
                    f"Timed out after {timeout:.1f}s waiting for database write "
                    f"{command_id}; the durable command remains queued"
                ) from exc
            except RedisError as exc:
                raise DatabaseWriteError(
                    f"Redis could not read database write result {command_id}: {exc}"
                ) from exc
            if raw_result is not None:
                payload = self._unwrap_result(DatabaseWriteResult.from_json(raw_result))
                if include_timing:
                    timing = payload.setdefault(WRITE_TIMING_KEY, {})
                    if isinstance(timing, dict):
                        timing["writer_result_source"] = "cache"
                        if raw_available_at is None:
                            timing["writer_result_split_missing_reason"] = (
                                "availability_timestamp_missing"
                            )
                        else:
                            timing["writer_result_available_at_ms"] = float(
                                _decode(raw_available_at)
                            )
                            if observed_at is not None:
                                timing["writer_result_observed_at_ms"] = observed_at
                            else:
                                # The result remains authoritative when optional
                                # profiling cannot read Redis's clock.
                                timing["writer_result_split_missing_reason"] = (
                                    "observation_timestamp_unavailable"
                                )
                if not include_timing:
                    payload.pop(WRITE_TIMING_KEY, None)
                return payload
            if raw_submitted is None:
                receipt = await self._receipt_until(command_id, deadline)
                if receipt is not None:
                    payload = self._unwrap_result(receipt)
                    if include_timing:
                        timing = payload.get(WRITE_TIMING_KEY)
                        if not isinstance(timing, dict) or not timing:
                            # Receipts are stored without writer timing.
                            timing = payload[WRITE_TIMING_KEY] = {
                                "writer_timing_unavailable": True
                            }
                        timing["writer_result_source"] = "receipt"
                        timing["writer_result_split_missing_reason"] = (
                            "result_cache_miss"
                        )
                    if not include_timing:
                        payload.pop(WRITE_TIMING_KEY, None)
                    return payload
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DatabaseWriteTimeout(
                    f"Timed out after {timeout:.1f}s waiting for database write "
                    f"{command_id}; the durable command remains queued"
                )
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 1.5, self.result_poll_max_seconds)

    @staticmethod
    def _unwrap_result(result: DatabaseWriteResult) -> dict[str, Any]:
        if not result.success:
            raise DatabaseWriteRejected(
                result.error or f"Database write {result.command_id} was rejected"
            )
        return result.payload


class DatabaseWriterService:
    """Crash-recoverable Redis Stream consumer for owned bulk partitions.

    One service runs per writer process. It consumes only the partitions the
    runtime holds leases for (by default the single original stream), so no
    two live processes read or reclaim the same stream. The partition-0 owner
    also consumes the priority stream.
    """

    CONSUMER_GROUP = "database_writers_v1"
    DEAD_LETTER_STREAM = f"{KEY_PREFIX}:database:writes:dead"
    READY_KEY = f"{KEY_PREFIX}:database:writer:ready:{{consumer_name}}"
    READY_TTL_SECONDS = 10
    READY_REFRESH_SECONDS = 5.0
    ATTEMPTS_KEY = f"{KEY_PREFIX}:database:write:attempts"
    DEFAULT_RETRY_DELAY_SECONDS = 5.0
    DEFAULT_DEAD_LETTER_MAXLEN = 10_000

    def __init__(
        self,
        redis_manager: object,
        executor: DatabaseWriteExecutor,
        *,
        consumer_name: str | None = None,
        batch_size: int | None = None,
        batch_max_bytes: int | None = None,
        concurrency: int | None = None,
        max_attempts: int | None = None,
        reclaim_idle_ms: int | None = None,
        retry_delay_seconds: float | None = None,
        dead_letter_maxlen: int | None = None,
        dead_letter_max_bytes: int | None = None,
        result_cache_max_entries: int | None = None,
        result_cache_max_bytes: int | None = None,
        block_ms: int = 1000,
        partitions: Sequence[int] = (0,),
        consume_priority: bool | None = None,
    ) -> None:
        self.redis = redis_manager
        self.executor = executor
        self.partitions = tuple(sorted(set(int(partition) for partition in partitions)))
        if not self.partitions or self.partitions[0] < 0:
            raise ValueError("A database writer needs at least one partition")
        self.consume_priority = (
            0 in self.partitions if consume_priority is None else consume_priority
        )
        self._read_cursor = 0
        self.consumer_name = consumer_name or (
            f"writer-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
        )
        self.batch_size = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_BATCH_SIZE", 100, value=batch_size
        )
        self.concurrency = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_CONCURRENCY", 4, value=concurrency
        )
        if self.batch_size <= self.concurrency * 2:
            default_refill_min = 1
        elif self.batch_size < 32:
            default_refill_min = max(self.concurrency, self.batch_size // 2)
        else:
            # The continuous-refill experiment produced denser but materially
            # slower UPSERT_USER transactions. Refill at half-window capacity;
            # operation-aware compaction still protects homogeneous batches.
            default_refill_min = max(1, (self.batch_size + 1) // 2)
        self.refill_min_commands_per_lane = min(
            self.batch_size,
            _positive_int_env(
                "ULTIMA_DATABASE_WRITER_REFILL_MIN_COMMANDS",
                default_refill_min,
            ),
        )
        # ``batch_size`` is a SQL batch target for each execution lane, not a
        # global window shared by every lane and operation type. A global
        # 100-command window split across eight shards and interleaved task/job
        # status writes produced ~4-command UPSERT_USER transactions.
        self.window_commands = self.batch_size * self.concurrency
        self.refill_min_commands = min(
            self.window_commands,
            self.refill_min_commands_per_lane * self.concurrency,
        )
        self.batch_max_wait_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITER_BATCH_MAX_WAIT_MS",
            50.0,
        ) / 1000.0
        self._batch_slots = _WriterSlots(self.concurrency)
        self._lane_tails: dict[WriterLaneKey, asyncio.Task[None]] = {}
        self._identity_tails: dict[WriterIdentityKey, asyncio.Task[None]] = {}
        self._task_identity_keys: dict[
            asyncio.Task[None], set[WriterIdentityKey]
        ] = {}
        self._open_lane_batches: dict[
            WriterLaneKey, _OpenLaneBatch
        ] = {}
        self._lane_last_finished: dict[WriterLaneKey, float] = {}
        self.batch_max_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_BATCH_MAX_BYTES",
            _positive_int_env(
                "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES",
                DatabaseWriteClient.MAX_COMMAND_BYTES,
            )
            * self.concurrency,
            value=batch_max_bytes,
        )
        self._read_script: Any = None
        self._claim_script: Any = None
        self._ack_script: Any = None

        self.max_attempts = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_MAX_ATTEMPTS", 8, value=max_attempts
        )
        self.reclaim_idle_ms = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_RECLAIM_IDLE_MS", 30_000, value=reclaim_idle_ms
        )
        self.retry_delay_seconds = _positive_float_env(
            "ULTIMA_DATABASE_WRITER_RETRY_DELAY_SECONDS",
            self.DEFAULT_RETRY_DELAY_SECONDS,
            value=retry_delay_seconds,
        )
        self.dead_letter_maxlen = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_DEAD_LETTER_MAXLEN",
            self.DEFAULT_DEAD_LETTER_MAXLEN,
            value=dead_letter_maxlen,
        )
        queue_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_QUEUE_MAX_BYTES",
            _positive_int_env(
                "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES",
                DatabaseWriteClient.MAX_COMMAND_BYTES,
            )
            * self.concurrency,
        )
        queue_commands = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_QUEUE_MAX_COMMANDS",
            self.batch_size * self.concurrency,
        )
        self.queue_max_bytes = queue_bytes
        self.queue_max_commands = queue_commands
        # Keep results from more than one bounded writer window available so
        # producers can observe completed writes even while the queue refills.
        # The cache is shared in Redis, so it holds every process's windows.
        # The explicit environment override remains authoritative.
        processes = writer_process_count()
        default_result_cache_entries = 2 * processes * max(
            queue_commands, self.batch_size * self.concurrency
        )
        self.result_cache_max_entries = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_RESULT_CACHE_MAX_ENTRIES",
            default_result_cache_entries,
            value=result_cache_max_entries,
        )
        self.result_cache_max_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_RESULT_CACHE_MAX_BYTES",
            queue_bytes * processes,
            value=result_cache_max_bytes,
        )
        self.dead_letter_max_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITER_DEAD_LETTER_MAX_BYTES",
            queue_bytes,
            value=dead_letter_max_bytes,
        )
        self.block_ms = _positive_int_env("block_ms", 1000, value=block_ms)
        self._running = False
        self._last_reclaim_at = 0.0
        self._last_priority_reclaim_at = 0.0
        self._active_batches = 0

    def _client(self) -> Any:
        client = getattr(self.redis, "client", None)
        if client is None:
            raise DatabaseWriteError("Redis is unavailable for database writer")
        return client

    async def run(self, *, stop_requested: Callable[[], bool] | None = None) -> None:
        """Refill a bounded working window as independent lanes finish."""

        client = self._client()
        self._running = True
        stopping = asyncio.Event()
        active: dict[asyncio.Task[None], tuple[int, int]] = {}
        # Priority lanes stay outside the bulk window so a submission never
        # waits for bulk refill capacity.
        priority_active: dict[asyncio.Task[None], tuple[int, int]] = {}
        notification: asyncio.Task[Any] | None = None
        recovering = True
        priority_recovering = True
        priority_due = True
        next_priority_poll = 0.0
        refill_blocked_since: float | None = None
        poll_seconds = min(self.block_ms / 1000.0, 1.0)

        def should_stop() -> bool:
            return not self._running or bool(stop_requested and stop_requested())

        ready_key = self.READY_KEY.format(consumer_name=self.consumer_name)
        logger.info(
            "Database writer %s started (batch_size=%d, concurrency=%d, "
            "max_attempts=%d, partitions=%s)",
            self.consumer_name,
            self.batch_size,
            self.concurrency,
            self.max_attempts,
            ",".join(str(partition) for partition in self.partitions),
        )
        heartbeat: asyncio.Task[None] | None = None
        try:
            await client.set(ready_key, _utc_now(), ex=self.READY_TTL_SECONDS)
            # The ready key is this consumer's liveness for peers' reclaim. It
            # stays fresh until owned lanes have drained, not just while the
            # main loop runs, so a stopping writer's entries are not stolen.
            heartbeat = asyncio.create_task(self._refresh_ready_key(ready_key))
            await self._ensure_group()
            while not should_stop():
                if notification is not None and notification.done():
                    popped = notification.result()
                    notification = None
                    if popped and _decode(popped[0]) == (
                        DatabaseWriteClient.PRIORITY_NOTIFY_KEY
                    ):
                        priority_due = True
                now = time.monotonic()
                if heartbeat.done():
                    heartbeat.result()  # surfaces an unexpected heartbeat crash
                for lanes in (active, priority_active):
                    for task in tuple(lanes):
                        if task.done():
                            del lanes[task]
                            task.result()
                if now >= next_priority_poll:
                    # Fallback for a notification that arrived while the bulk
                    # loop could not block on Redis.
                    priority_due = True
                if (
                    self.consume_priority
                    and priority_due
                    and len(priority_active) < self.concurrency
                ):
                    priority_due = False
                    next_priority_poll = now + poll_seconds
                    priority_entries: list[StreamEntry] = []
                    if priority_recovering:
                        # Same recovery barrier as the bulk stream, scoped to
                        # the independent priority stream.
                        if not priority_active:
                            if await self._pending_entry_count(
                                stream=DatabaseWriteClient.PRIORITY_STREAM_KEY
                            ):
                                priority_entries = await self._claim_stale_priority_entries()
                            else:
                                priority_recovering = False
                    if not priority_recovering:
                        priority_entries = await self._read_stream_entries(
                            DatabaseWriteClient.PRIORITY_STREAM_KEY,
                            max_count=self.concurrency - len(priority_active),
                            max_bytes=self.batch_max_bytes,
                        )
                    if priority_entries:
                        pending, sizes = await self._run_owned(
                            self._prepare_entries(priority_entries)
                        )
                        priority_active.update(
                            self._start_lanes(
                                pending, sizes, stopping, should_stop, priority=True
                            )
                        )
                        del priority_entries, pending, sizes
                        priority_due = True
                        continue
                remaining_count = self.window_commands - sum(
                    size[0] for size in active.values()
                )
                remaining_bytes = self.batch_max_bytes - sum(
                    size[1] for size in active.values()
                )
                entries: list[StreamEntry] = []
                if not active and self.consume_priority:
                    # One owner archives the shared legacy diagnostic cache.
                    await self._run_owned(self._archive_legacy_dead_letters())
                if recovering:
                    # Unknown older work is a recovery barrier. Do not reclaim
                    # this process's in-flight entries or overtake another owner.
                    if not active:
                        if await self._pending_entry_count():
                            entries = await self._claim_stale_entries()
                        else:
                            recovering = False
                refill_ready = (
                    not active or remaining_count >= self.refill_min_commands
                )
                if active and not refill_ready:
                    refill_blocked_since = refill_blocked_since or time.monotonic()
                if (
                    not recovering
                    and refill_ready
                    and remaining_count > 0
                    and remaining_bytes > 0
                ):
                    entries = await self._read_new_entries(
                        max_count=remaining_count,
                        max_bytes=remaining_bytes,
                        block=False,
                    )
                if entries:
                    refill_wait_ms = (
                        (time.monotonic() - refill_blocked_since) * 1000.0
                        if refill_blocked_since is not None else 0.0
                    )
                    refill_blocked_since = None
                    pending, sizes = await self._run_owned(
                        self._prepare_entries(entries)
                    )
                    active.update(
                        self._start_lanes(
                            pending,
                            sizes,
                            stopping,
                            should_stop,
                            refill_wait_ms=refill_wait_ms,
                        )
                    )
                    # Raw fields and parsed work belong only to the admitted
                    # lane tasks, not to the lifetime of the main loop.
                    del entries, pending, sizes
                    continue

                waiting: set[asyncio.Task[Any]] = set(active) | set(priority_active)
                notify_keys = []
                if (
                    not recovering
                    and refill_ready
                    and remaining_count > 0
                    and remaining_bytes > 0
                ):
                    notify_keys.extend(
                        DatabaseWriteClient.partition_keys(partition)[2]
                        for partition in self.partitions
                    )
                if (
                    self.consume_priority
                    and not priority_recovering
                    and len(priority_active) < self.concurrency
                ):
                    notify_keys.append(DatabaseWriteClient.PRIORITY_NOTIFY_KEY)
                if (
                    notification is None
                    and notify_keys
                    and (
                        not (active or priority_active)
                        or client.connection_pool.max_connections > 1
                    )
                ):
                    # A blocking notification must not occupy the sole Redis
                    # connection while an active write needs to publish/ACK.
                    notification = asyncio.create_task(
                        client.blpop(notify_keys, timeout=poll_seconds)
                    )
                if notification is not None:
                    waiting.add(notification)
                if waiting:
                    await asyncio.wait(
                        waiting,
                        timeout=poll_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                else:
                    await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            raise
        finally:
            self._running = False
            stopping.set()
            # asyncio.to_thread cannot stop a SQL transaction when its awaiting
            # task is cancelled. Own those tasks through completion before
            # closing pools or returning the writer lease to the supervisor.
            try:
                remaining = [*active, *priority_active]
                if notification is not None:
                    remaining.append(notification)
                await self._drain_tasks(remaining)
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                try:
                    await client.delete(ready_key)
                finally:
                    await self.executor.close()
            logger.info("Database writer %s stopped", self.consumer_name)

    def stop(self) -> None:
        self._running = False

    async def _refresh_ready_key(self, ready_key: str) -> None:
        client = self._client()
        while True:
            await asyncio.sleep(self.READY_REFRESH_SECONDS)
            try:
                await client.set(ready_key, _utc_now(), ex=self.READY_TTL_SECONDS)
            except RedisError as exc:
                # The main loop reports Redis loss; a missed refresh only lets
                # peers treat this consumer as gone sooner.
                logger.warning("Database writer heartbeat failed: %s", exc)

    async def _ensure_group(self) -> None:
        client = self._client()
        for stream in (
            *(DatabaseWriteClient.partition_keys(partition)[0] for partition in self.partitions),
            DatabaseWriteClient.PRIORITY_STREAM_KEY,
        ):
            try:
                await client.xgroup_create(
                    stream,
                    self.CONSUMER_GROUP,
                    id="0",
                    mkstream=True,
                )
            except Exception as exc:  # redis-py exposes ResponseError across versions
                if "BUSYGROUP" not in str(exc):
                    raise

    @staticmethod
    def _script_entries(rows: Sequence[Any]) -> list[StreamEntry]:
        return [
            (row[0], dict(zip(row[1][::2], row[1][1::2], strict=True))) for row in rows
        ]

    def _stream_entries(self, stream: str, rows: Sequence[Any]) -> list[StreamEntry]:
        entries = self._script_entries(rows)
        prefix = DatabaseWriteClient.entry_prefix(stream)
        if prefix:
            return [(prefix + _decode(entry_id), fields) for entry_id, fields in entries]
        return entries

    async def _read_stream_entries(
        self,
        stream: str,
        *,
        max_count: int | None = None,
        max_bytes: int | None = None,
    ) -> list[StreamEntry]:
        client = self._client()
        if self._read_script is None:
            self._read_script = client.register_script(write_queue_scripts.READ_NEW)
        return self._stream_entries(
            stream,
            await self._read_script(
                keys=[stream],
                args=[
                    self.CONSUMER_GROUP,
                    self.consumer_name,
                    self.batch_size if max_count is None else max_count,
                    self.batch_max_bytes if max_bytes is None else max_bytes,
                    self.batch_max_bytes,
                ],
                client=client,
            ),
        )

    async def _read_new_entries(
        self,
        *,
        max_count: int | None = None,
        max_bytes: int | None = None,
        block: bool = True,
    ) -> list[StreamEntry]:
        client = self._client()

        async def read() -> list[StreamEntry]:
            count = self.batch_size if max_count is None else max_count
            budget = self.batch_max_bytes if max_bytes is None else max_bytes
            # Rotate the first partition so one busy stream cannot starve another.
            offset = self._read_cursor % len(self.partitions)
            self._read_cursor = offset + 1
            entries: list[StreamEntry] = []
            for partition in (*self.partitions[offset:], *self.partitions[:offset]):
                if count <= 0 or budget <= 0:
                    break
                rows = await self._read_stream_entries(
                    DatabaseWriteClient.partition_keys(partition)[0],
                    max_count=count,
                    max_bytes=budget,
                )
                entries.extend(rows)
                count -= len(rows)
                budget -= sum(
                    len(value) if isinstance(value, bytes) else len(str(value).encode("utf-8"))
                    for _, fields in rows
                    for value in fields.values()
                )
            return entries

        entries = await read()
        if not entries and block:
            # A coalesced notification avoids polling or missing the enqueue
            # between an empty read and blocking. Work lives only in the stream.
            await client.blpop(
                [DatabaseWriteClient.partition_keys(partition)[2] for partition in self.partitions],
                timeout=self.block_ms / 1000.0,
            )
            entries = await read()
        return entries

    async def _pending_entry_count(self, *, stream: str | None = None) -> int:
        """Pending entries in one stream, or in every owned bulk partition."""
        if stream is None:
            return sum([
                await self._pending_entry_count(
                    stream=DatabaseWriteClient.partition_keys(partition)[0]
                )
                for partition in self.partitions
            ])
        summary = await self._client().xpending(stream, self.CONSUMER_GROUP)
        if isinstance(summary, Mapping):
            return int(summary.get("pending") or 0)
        if isinstance(summary, Sequence) and summary:
            return int(summary[0])
        return 0

    async def _claim_stream_entries(self, stream: str) -> list[StreamEntry]:
        client = self._client()
        if self._claim_script is None:
            self._claim_script = client.register_script(write_queue_scripts.CLAIM_STALE)
        return self._stream_entries(
            stream,
            await self._claim_script(
                keys=[stream],
                args=[
                    self.CONSUMER_GROUP,
                    self.consumer_name,
                    self.reclaim_idle_ms,
                    self.batch_size,
                    self.batch_max_bytes,
                    self.READY_KEY.format(consumer_name=""),
                ],
                client=client,
            ),
        )

    async def _claim_stale_entries(self) -> list[StreamEntry]:
        now = time.monotonic()
        if now - self._last_reclaim_at < max(self.reclaim_idle_ms / 2000.0, 1.0):
            return []
        entries: list[StreamEntry] = []
        for partition in self.partitions:
            stream = DatabaseWriteClient.partition_keys(partition)[0]
            # Each partition orders independent records, so recover them in
            # turn; a partition is skipped only when it has nothing pending.
            if len(self.partitions) == 1 or await self._pending_entry_count(stream=stream):
                entries = await self._claim_stream_entries(stream)
                if entries:
                    break
        # Drain eligible recovery batches immediately; throttle only empty scans.
        self._last_reclaim_at = 0.0 if entries else now
        return entries

    async def _claim_stale_priority_entries(self) -> list[StreamEntry]:
        now = time.monotonic()
        if now - self._last_priority_reclaim_at < max(
            self.reclaim_idle_ms / 2000.0, 1.0
        ):
            return []
        entries = await self._claim_stream_entries(
            DatabaseWriteClient.PRIORITY_STREAM_KEY
        )
        self._last_priority_reclaim_at = 0.0 if entries else now
        return entries

    async def _queue_ack(self, pipe: Any, entry_ids: Sequence[str]) -> None:
        client = self._client()
        if self._ack_script is None:
            self._ack_script = client.register_script(write_queue_scripts.ACKNOWLEDGE)
        by_stream: dict[tuple[str, str], list[str]] = {}
        for entry_id in entry_ids:
            stream, bytes_key, raw_id = DatabaseWriteClient.entry_stream(entry_id)
            by_stream.setdefault((stream, bytes_key), []).append(raw_id)
        for (stream, bytes_key), raw_ids in by_stream.items():
            await self._ack_script(
                keys=[stream, bytes_key],
                args=[self.CONSUMER_GROUP, *raw_ids],
                client=pipe,
            )

    @staticmethod
    def _parse_entry(entry: StreamEntry) -> tuple[str, DatabaseWriteCommand]:
        raw_id, fields = entry
        normalized = {_decode(key): value for key, value in fields.items()}
        raw_command = normalized.get("command")
        if raw_command is None:
            raise ValueError("Database write stream entry has no command field")
        return _decode(raw_id), DatabaseWriteCommand.from_json(raw_command)

    async def _process_entries(self, entries: Sequence[StreamEntry]) -> None:
        """Process a finite recovery/test batch with the same ordered scheduler."""
        stopping = asyncio.Event()
        tasks: dict[asyncio.Task[None], tuple[int, int]] = {}
        try:
            pending, sizes = await self._run_owned(self._prepare_entries(entries))
            tasks = self._start_lanes(pending, sizes, stopping)
            if tasks:
                # wait does not cancel the children when its caller is cancelled.
                await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                for task in tasks:
                    if task.done():
                        task.result()
        finally:
            stopping.set()
            await self._drain_tasks(list(tasks))

    @staticmethod
    async def _drain_tasks(tasks: Sequence[asyncio.Task[Any]]) -> None:
        if not tasks:
            return
        drained = asyncio.gather(*tasks, return_exceptions=True)
        cancelled = False
        while not drained.done():
            try:
                await asyncio.shield(drained)
            except asyncio.CancelledError:
                # Repeated cancellation must not detach database threads.
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    async def _run_owned[T](self, operation: Coroutine[Any, Any, T]) -> T:
        preparation = asyncio.create_task(operation)
        try:
            return await asyncio.shield(preparation)
        finally:
            await self._drain_tasks([preparation])

    async def _prepare_entries(
        self,
        entries: Sequence[StreamEntry],
    ) -> tuple[list[tuple[str, DatabaseWriteCommand]], dict[str, int]]:
        parsed: list[tuple[str, DatabaseWriteCommand]] = []
        sizes: dict[str, int] = {}
        for entry in entries:
            try:
                item = self._parse_entry(entry)
                sizes[item[0]] = sum(
                    len(value)
                    if isinstance(value, bytes)
                    else len(str(value).encode("utf-8"))
                    for value in entry[1].values()
                )
                parsed.append(item)
            except Exception as exc:
                entry_id = _decode(entry[0])
                async with self._batch_slots:
                    await self._dead_letter_unparseable(entry_id, entry[1], exc)
        if not parsed:
            return [], {}

        client = self._client()
        raw_results = await client.mget(
            [
                DatabaseWriteClient.result_key(command.command_id)
                for _, command in parsed
            ]
        )
        pending: list[tuple[str, DatabaseWriteCommand]] = []
        completed: list[tuple[str, str]] = []
        for (entry_id, command), raw_result in zip(parsed, raw_results, strict=True):
            if raw_result is not None:
                # Bridge cached outcomes created before receipt-backed delivery.
                async with self._batch_slots:
                    await self.executor.persist_outcome(
                        command, DatabaseWriteResult.from_json(raw_result)
                    )
                completed.append((entry_id, command.command_id))
            else:
                pending.append((entry_id, command))
        if completed:
            await self._ack_and_delete_many(completed)
        return pending, sizes

    def _start_lanes(
        self,
        pending: Sequence[tuple[str, DatabaseWriteCommand]],
        sizes: Mapping[str, int],
        stopping: asyncio.Event,
        should_stop: Callable[[], bool] | None = None,
        *,
        refill_wait_ms: float = 0.0,
        priority: bool = False,
    ) -> dict[asyncio.Task[None], tuple[int, int]]:
        # Compact available work before assigning execution slots. Identity
        # tails below retain record ordering even when its next batch rotates
        # to a different slot; fixed hash shards needlessly fragment waves.
        lane_inputs: dict[tuple[str | None, str], list[tuple[str, DatabaseWriteCommand]]] = {}
        for entry_id, command in pending:
            domain, _ = self._command_identity(command)
            lane_inputs.setdefault((command.site, domain), []).append((entry_id, command))
        lanes: list[tuple[WriterLaneKey, list[tuple[str, DatabaseWriteCommand]]]] = []
        for (site, domain), lane_input in lane_inputs.items():
            while lane_input:
                lane = self._take_operation_batch(lane_input, max_size=max(1, math.ceil(len(pending) / self.concurrency)))
                cursor = getattr(self, "_dense_lane_cursor", 0)
                self._dense_lane_cursor = (cursor + 1) % self.concurrency
                lanes.append(((site, domain, lane[0][1].operation, cursor), lane))
        tasks = {}
        for key, lane in lanes:
            lane_bytes = sum(sizes[entry_id] for entry_id, _ in lane)
            open_batch = self._open_lane_batches.get(key)
            identity_keys = tuple(
                self._writer_identity_key(command) for _, command in lane
            )
            if (
                open_batch is not None
                and open_batch.task is not None
                and not open_batch.task.done()
                and all(
                    (tail := self._identity_tails.get(identity_key)) is None
                    or tail.done()
                    or tail is open_batch.task
                    for identity_key in identity_keys
                )
            ):
                open_batch.pending.extend(lane)
                open_batch.encoded_bytes += lane_bytes
                open_batch.refill_wait_ms = max(
                    open_batch.refill_wait_ms,
                    refill_wait_ms,
                )
                open_batch.changed.set()
                tasks[open_batch.task] = (
                    len(open_batch.pending),
                    open_batch.encoded_bytes,
                )
                for identity_key in identity_keys:
                    self._identity_tails[identity_key] = open_batch.task
                self._task_identity_keys.setdefault(open_batch.task, set()).update(
                    identity_keys
                )
                continue

            # A rotating execution label is not a record-ordering constraint.
            # Only actual identity tails must finish first; the shared semaphore
            # lets unrelated batches use whichever SQL slot becomes available.
            predecessors = {
                predecessor
                for identity_key in identity_keys
                if (predecessor := self._identity_tails.get(identity_key)) is not None
                and not predecessor.done()
            }
            open_batch = _OpenLaneBatch(
                pending=list(lane),
                encoded_bytes=lane_bytes,
                refill_wait_ms=refill_wait_ms,
            )
            task = asyncio.create_task(
                self._process_lane(
                    open_batch.pending,
                    predecessors=tuple(predecessors),
                    stopping=stopping,
                    should_stop=should_stop,
                    lane_key=key,
                    refill_wait_ms=refill_wait_ms,
                    open_batch=open_batch,
                    priority=priority,
                )
            )
            open_batch.task = task
            self._open_lane_batches[key] = open_batch
            self._lane_tails[key] = task
            for identity_key in identity_keys:
                self._identity_tails[identity_key] = task
            self._task_identity_keys[task] = set(identity_keys)
            tasks[task] = (len(lane), lane_bytes)

            def release_tail(
                done,
                key=key,
                batch=open_batch,
            ):
                if self._lane_tails.get(key) is done:
                    del self._lane_tails[key]
                for identity_key in self._task_identity_keys.pop(done, ()):
                    if self._identity_tails.get(identity_key) is done:
                        del self._identity_tails[identity_key]
                current_open_batch = self._open_lane_batches.get(key)
                if current_open_batch is batch and batch.task is done:
                    del self._open_lane_batches[key]

            task.add_done_callback(release_tail)
        return tasks

    @staticmethod
    def _command_identity(command: DatabaseWriteCommand) -> tuple[str, object]:
        """Return an ordering domain and identity for a durable mutation."""
        payload = command.payload
        target: object = command.command_id
        for key in ("user_id", "subscriber_id", "media_id", "task_id", "job_id"):
            if payload.get(key) is not None:
                domain = (
                    "user"
                    if key in {"user_id", "subscriber_id"}
                    else key.removesuffix("_id")
                )
                return domain, payload[key]
        if command.operation == DatabaseWriteOperation.UPSERT_USER:
            user_payload = payload.get("user")
            if isinstance(user_payload, Mapping) and user_payload.get("id") is not None:
                return "user", user_payload["id"]
        return command.operation.value, target

    def _command_shard(self, command: DatabaseWriteCommand) -> int:
        """Keep mutations for one logical record ordered on a fixed lane."""

        _, target = self._command_identity(command)
        digest = hashlib.sha256(str(target).encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % self.concurrency

    @classmethod
    def _writer_identity_key(cls, command: DatabaseWriteCommand) -> WriterIdentityKey:
        domain, target = cls._command_identity(command)
        return command.site, domain, str(target)

    async def _process_lane(
        self,
        pending: Sequence[tuple[str, DatabaseWriteCommand]],
        *,
        predecessors: Sequence[asyncio.Task[None]] = (),
        stopping: asyncio.Event,
        should_stop: Callable[[], bool] | None = None,
        lane_key: WriterLaneKey | None = None,
        refill_wait_ms: float = 0.0,
        open_batch: _OpenLaneBatch | None = None,
        priority: bool = False,
    ) -> None:
        """Execute one ordered record lane in homogeneous transaction batches."""

        def is_stopping() -> bool:
            if should_stop is not None and should_stop():
                stopping.set()
            return stopping.is_set()

        predecessor_wait_ms = 0.0
        if predecessors:
            predecessor_started = time.monotonic()
            await asyncio.gather(
                *(asyncio.shield(predecessor) for predecessor in predecessors)
            )
            predecessor_wait_ms = (time.monotonic() - predecessor_started) * 1000.0
        remaining: list[tuple[str, DatabaseWriteCommand]] | None = None
        retry_pending: list[tuple[str, DatabaseWriteCommand]] = []
        retry_delay = self.retry_delay_seconds
        while not is_stopping():
            async with (
                self._batch_slots.priority() if priority else self._batch_slots
            ):
                if is_stopping():
                    return
                if remaining is None:
                    # Keep accumulating while waiting for a SQL slot. Closing
                    # on age alone used to freeze tiny batches behind busy
                    # slots even when later reads filled the working window.
                    # An available slot still respects the original maximum
                    # age; queueing does not start another collection delay.
                    # Control commands are latency-sensitive and rarely
                    # coalesce; run them as soon as they hold a slot.
                    if open_batch is not None and not priority:
                        while not is_stopping():
                            if len(open_batch.pending) >= self.batch_size:
                                break
                            collection_seconds = (
                                open_batch.opened_at
                                + self.batch_max_wait_seconds
                                - time.monotonic()
                            )
                            if collection_seconds <= 0:
                                break
                            open_batch.changed.clear()
                            try:
                                await asyncio.wait_for(
                                    open_batch.changed.wait(),
                                    timeout=collection_seconds,
                                )
                            except TimeoutError:
                                break
                    if lane_key is not None and open_batch is not None:
                        current_task = asyncio.current_task()
                        if (
                            self._open_lane_batches.get(lane_key) is open_batch
                            and open_batch.task is current_task
                        ):
                            del self._open_lane_batches[lane_key]
                        refill_wait_ms = max(
                            refill_wait_ms, open_batch.refill_wait_ms
                        )
                    # No await between closing and copying the accumulator.
                    remaining = list(pending)
                if not retry_pending:
                    if not remaining or is_stopping():
                        return
                    retry_pending = self._take_operation_batch(remaining)
                    retry_delay = self.retry_delay_seconds
                self._active_batches += 1
                try:
                    retry_pending = await self._execute_and_ack_batch(
                        retry_pending,
                        lane_key=lane_key,
                        predecessor_wait_ms=predecessor_wait_ms,
                        refill_wait_ms=refill_wait_ms,
                    )
                    predecessor_wait_ms = 0.0
                    refill_wait_ms = 0.0
                finally:
                    self._active_batches -= 1
            if retry_pending and not is_stopping():
                # The lane stays ordered while other lanes can use the SQL
                # slot. Shutdown interrupts backoff without another attempt.
                try:
                    await asyncio.wait_for(stopping.wait(), timeout=retry_delay)
                except TimeoutError:
                    pass
                retry_delay = min(retry_delay * 2, 30.0)

    def _take_operation_batch(
        self,
        pending: list[tuple[str, DatabaseWriteCommand]],
        *, max_size: int | None = None,
    ) -> list[tuple[str, DatabaseWriteCommand]]:
        """Remove one dense homogeneous batch without reordering an identity.

        Commands for different logical records may be regrouped by operation.
        For each record, only its leading same-operation run is eligible, so a
        later mutation can never overtake an earlier mutation for that record.
        """

        leading_operation: dict[tuple[str, str], DatabaseWriteOperation] = {}
        blocked_identities: set[tuple[str, str]] = set()
        eligible: dict[DatabaseWriteOperation, list[int]] = {}
        for index, (_, command) in enumerate(pending):
            domain, target = self._command_identity(command)
            identity = (domain, str(target))
            if identity in blocked_identities:
                continue
            operation = leading_operation.setdefault(identity, command.operation)
            if command.operation != operation:
                blocked_identities.add(identity)
                continue
            eligible.setdefault(operation, []).append(index)

        selected_indices = min(
            eligible.values(),
            key=lambda indices: (-len(indices), indices[0]),
        )[: min(self.batch_size, max_size or self.batch_size)]
        selected = set(selected_indices)
        batch = [pending[index] for index in selected_indices]
        pending[:] = [item for index, item in enumerate(pending) if index not in selected]
        return batch

    async def _execute_and_ack_batch(
        self,
        pending: Sequence[tuple[str, DatabaseWriteCommand]],
        *,
        lane_key: WriterLaneKey | None = None,
        predecessor_wait_ms: float = 0.0,
        refill_wait_ms: float = 0.0,
    ) -> list[tuple[str, DatabaseWriteCommand]]:
        commands = [command for _, command in pending]
        client = self._client()
        queue_commands_at_start = 0
        queue_bytes_at_start = 0
        try:
            async with client.pipeline(transaction=False) as pipe:
                for partition in self.partitions:
                    stream, bytes_key, _ = DatabaseWriteClient.partition_keys(partition)
                    await pipe.xlen(stream)
                    await pipe.get(bytes_key)
                depth = await pipe.execute()
            queue_commands_at_start = sum(int(value or 0) for value in depth[::2])
            queue_bytes_at_start = sum(int(value or 0) for value in depth[1::2])
        except (RedisError, TypeError, ValueError):
            # Profiling is best-effort and must never reject a durable write.
            pass
        writer_started_wall = datetime.now(timezone.utc)
        writer_started = time.monotonic()
        lane_idle_ms = 0.0
        if lane_key is not None and lane_key in self._lane_last_finished:
            lane_idle_ms = max(
                0.0,
                (writer_started - self._lane_last_finished[lane_key]) * 1000.0,
            )
        try:
            payloads = await self.executor.execute_batch(commands)
            missing = [
                command.command_id
                for command in commands
                if command.command_id not in payloads
            ]
            if missing:
                raise RuntimeError(
                    "Database writer executor omitted command results: "
                    + ", ".join(missing)
                )
        except DatabaseWriteReceiptBusy:
            logger.info("Database writer deferred %d command(s): receipt transaction still active", len(commands))
            return list(pending)
        except Exception as exc:
            stale = None if isinstance(exc, DatabaseWriteRejected) else _stale_attempt(exc)
            if stale is not None:
                # An expired or replaced attempt's fence fails the whole batch
                # transaction, but only that command can never apply. Retrying
                # the batch would dead-letter its valid siblings with it.
                exc = DatabaseWriteRejected(str(stale))
            if not isinstance(exc, DatabaseWriteRejected):
                return await self._retry_failed_batch(commands, pending, exc)
            if len(pending) > 1:
                # A rejection belongs to one command, not every command in
                # its transaction batch. Receipts make committed siblings safe
                # to replay; keep lane ordering if a sibling needs a retry.
                for index, item in enumerate(pending):
                    retry = await self._execute_and_ack_batch(
                        [item],
                        lane_key=lane_key,
                        predecessor_wait_ms=predecessor_wait_ms,
                        refill_wait_ms=refill_wait_ms,
                    )
                    if retry:
                        return [*retry, *pending[index + 1:]]
                return []
            entry_id, command = pending[0]
            await self._record_failure(entry_id, command, exc)
            logger.warning("Database write %s rejected: %s", command.command_id, exc)
            return []

        writer_total_ms = (time.monotonic() - writer_started) * 1000.0
        if lane_key is not None:
            self._lane_last_finished[lane_key] = time.monotonic()
        writer_finished_wall = datetime.now(timezone.utc)
        try:
            redis_publish_started = await client.time()
            writer_publish_started_at_ms = (
                float(redis_publish_started[0]) * 1000.0
                + float(redis_publish_started[1]) / 1000.0
            )
        except (RedisError, TypeError, ValueError, IndexError):
            writer_publish_started_at_ms = 0.0
        entry_by_command = {
            command.command_id: entry_id for entry_id, command in pending
        }
        for command in commands:
            payload = payloads[command.command_id]
            timing = payload.setdefault(WRITE_TIMING_KEY, {})
            try:
                enqueued_ms = int(
                    DatabaseWriteClient.entry_stream(
                        _decode(entry_by_command[command.command_id])
                    )[2].split("-", 1)[0]
                )
                timing["writer_queue_ms"] = max(
                    0.0,
                    writer_started_wall.timestamp() * 1000.0 - enqueued_ms,
                )
            except (KeyError, TypeError, ValueError):
                timing["writer_queue_ms"] = 0.0
            timing["writer_total_ms"] = writer_total_ms
            timing["writer_started_at_ms"] = writer_started_wall.timestamp() * 1000.0
            timing["writer_finished_at_ms"] = writer_finished_wall.timestamp() * 1000.0
            timing["writer_result_publish_started_at_ms"] = writer_publish_started_at_ms
            timing["writer_configured_concurrency"] = self.concurrency
            timing["writer_configured_batch_size"] = self.batch_size
            timing["writer_refill_min_commands"] = self.refill_min_commands
            timing["writer_refill_min_commands_per_lane"] = self.refill_min_commands_per_lane
            timing["writer_window_commands"] = self.window_commands
            timing["writer_queue_max_commands"] = self.queue_max_commands
            timing["writer_queue_max_bytes"] = self.queue_max_bytes
            timing["writer_queue_commands_at_start"] = queue_commands_at_start
            timing["writer_queue_bytes_at_start"] = queue_bytes_at_start
            timing["writer_active_lanes"] = self._active_batches
            timing["writer_batch_commands"] = len(commands)
            timing["writer_batch_fill_ratio"] = len(commands) / self.batch_size
            timing["writer_lane_domain"] = lane_key[1] if lane_key is not None else "unknown"
            timing["writer_operation"] = command.operation.value
            timing["writer_lane_shard"] = lane_key[3] if lane_key is not None else -1
            timing["writer_lane_idle_ms"] = lane_idle_ms
            timing["writer_lane_predecessor_wait_ms"] = predecessor_wait_ms
            timing["writer_refill_wait_ms"] = refill_wait_ms

        batch_execution_ms = sum(
            max(
                0.0,
                float(
                    payloads[command.command_id]
                    .get(WRITE_TIMING_KEY, {})
                    .get("execution_ms", 0.0)
                ),
            )
            for command in commands
        )
        batch_commit_ms = sum(
            max(
                0.0,
                float(
                    payloads[command.command_id]
                    .get(WRITE_TIMING_KEY, {})
                    .get("commit_ms", 0.0)
                ),
            )
            for command in commands
        )
        batch_overhead_ms = max(
            0.0,
            writer_total_ms - batch_execution_ms - batch_commit_ms,
        )
        for command in commands:
            timing = payloads[command.command_id][WRITE_TIMING_KEY]
            own_work_ms = max(0.0, float(timing.get("execution_ms") or 0.0)) + max(
                0.0, float(timing.get("commit_ms") or 0.0)
            )
            timing["writer_batch_peer_work_ms"] = max(
                0.0,
                writer_total_ms - own_work_ms - batch_overhead_ms,
            )
            timing["writer_batch_overhead_ms"] = batch_overhead_ms

        completed_at = _utc_now()
        completed_entries: list[tuple[str, str]] = []
        results: list[DatabaseWriteResult] = []
        for entry_id, command in pending:
            results.append(
                DatabaseWriteResult(
                    command_id=command.command_id,
                    success=True,
                    payload=dict(payloads[command.command_id]),
                    error=None,
                    completed_at=completed_at,
                )
            )
            completed_entries.append((entry_id, command.command_id))

        # SQL receipts already committed with the mutations. Publish the
        # bounded delivery cache before ACK; an evicted result is recovered
        # from PostgreSQL without repeating the transaction.
        async with client.pipeline(transaction=True) as pipe:
            for result in results:
                await self._cache_result(pipe, result)
            await pipe.execute()
        await self._ack_and_delete_many(completed_entries)
        return []

    async def _archive_legacy_dead_letters(self) -> None:
        """Copy an old, unaccounted diagnostic cache to SQL within one read budget."""
        client = self._client()
        remaining = self.batch_max_bytes
        for _ in range(self.batch_size):
            row = await write_result_cache.legacy_dead_letter(
                client, self.DEAD_LETTER_STREAM, remaining, self.batch_max_bytes)
            if not row:
                return
            entry, size = row
            entry_id = _decode(entry[0])
            normalized = _archive_redis_fields(dict(zip(entry[1][::2], entry[1][1::2], strict=True)))
            await self.executor.persist_invalid("legacy-dead-letter:" + entry_id,
                normalized, normalized.get("error", "Legacy writer dead letter"))
            await client.xdel(self.DEAD_LETTER_STREAM, entry_id)
            remaining -= int(size)

    async def _cache_result(self, pipe: Any, result: DatabaseWriteResult) -> None:
        await write_result_cache.cache_result(self._client(), pipe, result,
            max_entries=self.result_cache_max_entries,
            max_bytes=self.result_cache_max_bytes, eviction_batch_size=self.batch_size)

    async def _cache_dead_letter(self, pipe: Any, fields: dict[str, str]) -> None:
        await write_result_cache.cache_dead_letter(self._client(), pipe,
            self.DEAD_LETTER_STREAM, fields, max_entries=self.dead_letter_maxlen,
            max_bytes=self.dead_letter_max_bytes, eviction_batch_size=self.batch_size)

    async def _retry_failed_batch(self, commands, pending, exc: Exception):
        """Count a failed attempt for every command; return those to retry."""
        run_ids = sorted({
            str(value)
            for command in commands
            if (value := (command.execution_attempt or {}).get("run_id") or command.payload.get("job_id"))
        })
        logger.error(
            "Database writer batch failed for %d command(s) operations=%s sites=%s jobs=%s: %s",
            len(commands),
            sorted({command.operation.value for command in commands}),
            sorted({command.site or "management" for command in commands}),
            run_ids,
            exc,
            exc_info=exc,
        )
        retry = []
        for entry_id, command in pending:
            if not await self._record_failure(entry_id, command, exc):
                retry.append((entry_id, command))
        return retry

    async def _record_failure(
        self,
        entry_id: str,
        command: DatabaseWriteCommand,
        exc: Exception,
    ) -> bool:
        client = self._client()
        attempts = int(await client.hincrby(self.ATTEMPTS_KEY, command.command_id, 1))
        rejected = isinstance(exc, DatabaseWriteRejected)
        if not rejected and attempts < self.max_attempts:
            return False
        error = (
            f"Database write {command.command_id} rejected: {exc}"
            if rejected else
            f"Database write {command.command_id} failed after {attempts} attempts: "
            f"{type(exc).__name__}: {exc}"
        )
        result = DatabaseWriteResult(
            command_id=command.command_id,
            success=False,
            payload={},
            error=error,
            completed_at=_utc_now(),
        )
        # A PostgreSQL outage leaves accepted work pending. It must never be
        # dropped just because the Redis diagnostic cache is full.
        result = await self.executor.persist_outcome(command, result)
        async with client.pipeline(transaction=True) as pipe:
            if not result.success:
                await self._cache_dead_letter(pipe, {
                    "command": command.to_json(), "error": result.error or error,
                    "attempts": str(attempts), "failed_at": result.completed_at,
                })
            await self._cache_result(pipe, result)
            await self._queue_ack(pipe, [entry_id])
            pipe.hdel(self.ATTEMPTS_KEY, command.command_id)
            await pipe.execute()
        return True

    async def _dead_letter_unparseable(
        self,
        entry_id: str,
        fields: RedisFields,
        exc: Exception,
    ) -> None:
        error = f"Invalid database write entry: {type(exc).__name__}: {exc}"
        client = self._client()
        normalized = _archive_redis_fields(fields)
        await self.executor.persist_invalid(entry_id, normalized, error)
        async with client.pipeline(transaction=True) as pipe:
            await self._cache_dead_letter(pipe, {
                "stream_id": entry_id,
                "fields": json.dumps(normalized, sort_keys=True),
                "error": error, "failed_at": _utc_now(),
            })
            await self._queue_ack(pipe, [entry_id])
            await pipe.execute()

    async def _ack_and_delete_many(
        self,
        entries: Sequence[tuple[str, str]],
    ) -> None:
        if not entries:
            return
        client = self._client()
        async with client.pipeline(transaction=True) as pipe:
            for entry_id, command_id in entries:
                pipe.hdel(self.ATTEMPTS_KEY, command_id)
            await self._queue_ack(pipe, [entry_id for entry_id, _ in entries])
            await pipe.execute()


async def run_writer_until_stopped(
    redis_manager: object,
    executor_factory: Callable[
        [], Awaitable[DatabaseWriteExecutor] | DatabaseWriteExecutor
    ],
    *,
    stop_requested: Callable[[], bool],
) -> None:
    """Construct and run a writer executor after Redis is connected."""

    executor = executor_factory()
    if asyncio.iscoroutine(executor):
        executor = await executor
    service = DatabaseWriterService(
        redis_manager, cast(DatabaseWriteExecutor, executor)
    )
    await service.run(stop_requested=stop_requested)
