"""Event-sourced job system using Redis Streams.

This module implements the event log layer for the job system, providing:
- Append-only event storage (source of truth)
- Event replay for state reconstruction
- Audit trail for debugging

Events are stored in Redis Streams with format:
    {KEY_PREFIX}:job:{job_id}:events

All state changes must go through events - no direct state updates allowed.
This eliminates race conditions and provides complete observability.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, cast

from ..redis.client import RedisManager

from ..redis_channels import JOB_EVENTS_CHANNEL, KEY_PREFIX

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """All possible event types in the job system.

    State Machine Events:
    - JOB_CREATED: New job entered the system
    - TASK_CLAIMED: Worker claimed a task for execution
    - TASK_STARTED: Task execution began
    - TASK_PROGRESS: Progress update during task execution
    - TASK_SAVING_DB: Task is saving results to database
    - TASK_COMPLETED: Task finished successfully
    - TASK_FAILED: Task execution failed
    - TASK_CANCELLED: Task was cancelled
    - JOB_COMPLETED: All tasks finished
    - JOB_FAILED: Job failed due to task failure
    - JOB_CANCELLED: Job was cancelled by user

    Recovery Events:
    - TASK_ORPHANED: Task detected as orphaned (worker crashed)
    - TASK_RECOVERED: Orphaned task was recovered
    - LOCK_ACQUIRED: Execution lock acquired
    - LOCK_RELEASED: Execution lock released
    - LOCK_EXPIRED: Lock expired (worker timeout)
    """

    # Job lifecycle
    JOB_CREATED = "job_created"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"

    # Task lifecycle
    TASK_CLAIMED = "task_claimed"
    TASK_STARTED = "task_started"
    TASK_PROGRESS = "task_progress"
    TASK_SAVING_DB = "task_saving_db"
    TASK_DB_SAVED = "task_db_saved"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"

    # Recovery
    TASK_ORPHANED = "task_orphaned"
    TASK_RECOVERED = "task_recovered"

    # Locks
    LOCK_ACQUIRED = "lock_acquired"
    LOCK_RELEASED = "lock_released"
    LOCK_EXPIRED = "lock_expired"


@dataclass
class JobEvent:
    """Immutable event representing a state change in the job system.

    Attributes:
        event_type: Type of event (from EventType enum)
        job_id: Job identifier
        task_id: Task identifier (None for job-level events)
        timestamp: ISO timestamp when event occurred
        worker_id: Worker that emitted the event (None for system events)
        data: Event-specific payload data
        event_id: Redis Stream ID (assigned by Redis, format: timestamp-sequence)
    """

    event_type: EventType
    job_id: str
    task_id: Optional[str] = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    worker_id: Optional[str] = None
    data: dict[str, Any] = field(default_factory=lambda: {})
    event_id: Optional[str] = None  # Assigned by Redis Stream

    def to_dict(self) -> dict[str, Any]:
        """Serialize event to dictionary for Redis storage."""
        return {
            "event_type": self.event_type.value,
            "job_id": self.job_id,
            "task_id": self.task_id or "",
            "timestamp": self.timestamp,
            "worker_id": self.worker_id or "",
            "data": json.dumps(self.data),
        }

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], event_id: Optional[str] = None
    ) -> "JobEvent":
        """Deserialize event from Redis."""

        # Redis returns bytes, need to decode
        def decode_value(v: Any) -> str:
            if isinstance(v, bytes):
                return v.decode("utf-8")
            return str(v)

        normalized = {decode_value(key): value for key, value in data.items()}

        event_type_str = decode_value(normalized.get("event_type", ""))
        task_id_str = decode_value(normalized.get("task_id", ""))
        worker_id_str = decode_value(normalized.get("worker_id", ""))
        data_str = decode_value(normalized.get("data", "{}"))

        try:
            event_data = cast(dict[str, Any], json.loads(data_str) if data_str else {})
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse event data: {data_str}")
            event_data = {}

        return cls(
            event_type=EventType(event_type_str),
            job_id=decode_value(normalized["job_id"]),
            task_id=task_id_str if task_id_str else None,
            timestamp=decode_value(normalized["timestamp"]),
            worker_id=worker_id_str if worker_id_str else None,
            data=event_data,
            event_id=event_id,
        )


class EventLog:
    """Append-only event log using Redis Streams.

    This is the single source of truth for all job/task state changes.
    All state mutations must be recorded as events.

    Redis Streams provide:
    - Atomic append (XADD)
    - Ordered by timestamp
    - Consumer groups for processing
    - Persistence and replication
    """

    # Stream key format: {KEY_PREFIX}:job:{job_id}:events
    STREAM_KEY_FORMAT = f"{KEY_PREFIX}:job:{{job_id}}:events"

    # Global event stream for all jobs (for monitoring/debugging)
    GLOBAL_STREAM = f"{KEY_PREFIX}:job_events:global"

    # Maximum events to keep per job (MAXLEN ~10000)
    MAX_EVENTS_PER_JOB = 10000

    # TTL for job event streams (24 hours)
    STREAM_TTL = 24 * 60 * 60

    def __init__(self, redis_manager: RedisManager) -> None:
        """Initialize event log with Redis connection."""
        self.redis = redis_manager

    def _stream_key(self, job_id: str) -> str:
        """Generate Redis Stream key for a job."""
        return self.STREAM_KEY_FORMAT.format(job_id=job_id)

    async def append_job_started_once(
        self,
        event: JobEvent,
        *,
        started_at: str,
        backend_id: str | None,
    ) -> tuple[bool, str]:
        """Atomically transition an MPS job to running and append JOB_STARTED.

        Workers load job snapshots independently, so many first tasks can see
        an empty ``started_at`` before any one of them persists it. Claim the
        transition in the same Lua operation that appends the event; only the
        winner publishes the event and runtime fields.
        """
        from .mps_attempt import StaleTaskDefinition, current, mutate
        from .storage import JobStorage

        if event.event_type != EventType.JOB_STARTED:
            raise ValueError("append_job_started_once requires a JOB_STARTED event")

        identity = current(event.job_id)
        if identity is None:
            raise RuntimeError("Atomic MPS job start requires a bound task attempt")

        event.data["mps_task_manifest"] = identity.manifest_id
        event_dict = event.to_dict()
        stream_key = self._stream_key(event.job_id)
        storage = JobStorage(self.redis)
        published = {
            "event": event.event_type.value,
            "job_id": event.job_id,
            "task_id": event.task_id,
            "worker_id": event.worker_id,
            "timestamp": event.timestamp,
            **event.data,
        }
        fields = [part for item in event_dict.items() for part in item]
        result = await mutate(
            storage,
            event.job_id,
            identity.manifest_id,
            """
            local status = redis.call('HGET', KEYS[1], 'status') or ''
            local current_started_at = redis.call('HGET', KEYS[1], 'started_at')
            if status == 'completed' or status == 'failed' or status == 'cancelled' then
                return {-3, current_started_at or '', ''}
            end
            if current_started_at and current_started_at ~= '' then
                return {0, current_started_at, ''}
            end
            if ARGV[8] ~= '' and not redis.call('ZSCORE', KEYS[6], ARGV[8]) then
                return {-4, '', ''}
            end

            redis.call('HSET', KEYS[1],
                'status', 'running',
                'started_at', ARGV[2],
                'updated_at', ARGV[2])
            if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'worker_id', ARGV[3]) end
            if ARGV[4] ~= '' then redis.call('HSET', KEYS[1], 'backend_id', ARGV[4]) end

            local id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[6], '*', unpack(ARGV, 9))
            redis.call('EXPIRE', KEYS[3], ARGV[5])
            redis.call('XADD', KEYS[4], 'MAXLEN', '~', 100000,
                '*', 'stream_key', KEYS[3], unpack(ARGV, 9))
            redis.call('PUBLISH', KEYS[5], ARGV[7])
            return {1, ARGV[2], id}
            """,
            [
                stream_key,
                self.GLOBAL_STREAM,
                JOB_EVENTS_CHANNEL,
                storage.TASK_INDEX_KEY.format(job_id=event.job_id),
            ],
            [
                started_at,
                event.worker_id or "",
                backend_id or "",
                self.STREAM_TTL,
                self.MAX_EVENTS_PER_JOB,
                json.dumps(published),
                event.task_id or "",
                *fields,
            ],
        )

        if not isinstance(result, (list, tuple)) or len(result) < 2:
            raise RuntimeError("Redis returned an invalid atomic job-start result")
        outcome = int(result[0])
        actual_started_at = result[1]
        if isinstance(actual_started_at, bytes):
            actual_started_at = actual_started_at.decode("utf-8")
        actual_started_at = str(actual_started_at or "")
        if outcome == -3:
            raise StaleTaskDefinition(
                f"Cannot start terminal MPS job {event.job_id}"
            )
        if outcome == -4:
            raise StaleTaskDefinition(
                f"MPS task for job {event.job_id} is no longer resident"
            )
        if outcome not in {0, 1} or not actual_started_at:
            raise RuntimeError("Redis could not establish the MPS job start time")
        return outcome == 1, actual_started_at

    async def read_events(
        self,
        job_id: str,
        start_id: str = "0",
        count: Optional[int] = None,
    ) -> list[JobEvent]:
        """Read events from a job's event stream.

        Args:
            job_id: Job to read events for
            start_id: Stream ID to start from (0 = beginning, - = end)
            count: Maximum number of events to read (None = all)

        Returns:
            List of events in chronological order
        """
        stream_key = self._stream_key(job_id)

        try:
            # XRANGE returns [(id, {field: value, ...}), ...]
            results = await self.redis.client.xrange(
                stream_key,
                min=start_id,
                max="+",  # + means end of stream
                count=count,
            )

            events: list[JobEvent] = []
            for event_id, event_data in results:
                # Decode event_id if bytes
                if isinstance(event_id, bytes):
                    event_id_str = event_id.decode("utf-8")
                else:
                    event_id_str = str(event_id)

                event = JobEvent.from_dict(event_data, event_id=event_id_str)
                events.append(event)

            return events

        except Exception as exc:
            logger.error(f"Failed to read events for job {job_id}: {exc}")
            return []
