"""Core job and task data models for the UltimaScraper job system.

This module defines the UNIFIED Job and Task models used throughout the system:
- BackendManager creates Jobs with Tasks
- Jobs are persisted to Redis via JobStorage
- BackendRouter dispatches Jobs to worker queues
- Worker Pool executes Jobs via registered task handlers
- WebUI displays Job/Task progress in real-time

Architecture Flow:
    Job Creation → Redis Storage → BackendRouter → Worker Pool → Task Handlers → Results

Key Classes:
    Job: A collection of tasks with scheduling, state tracking, and execution logic
    Task: An individual unit of work (scrape, download, etc.)
    TaskExecutionContext: Context passed to task handlers during execution

IMPORTANT: This is the ONLY Job class used for Redis persistence. The legacy
Job class from worker/job_types.py has been removed. All code should import
Job from this module.

See: documentation/JOB_ARCHITECTURE.md for detailed architecture documentation.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Optional,
    cast,
)
from uuid import uuid4

from ultima_scraper_api import ContentType

from ..redis_channels import JOB_EVENTS_CHANNEL
from .payloads import APICommandTaskPayload, DownloadTaskPayload, ScrapeTaskPayload

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from ..config import UltimaScraperCollectionConfig
    from ..database.base_manager import BaseDatabaseManager
    from ..database.write_queue import DatabaseWriteClient
    from ..redis.client import RedisManager
    from .storage import JobStorage


def _empty_str_list() -> list[str]:
    return []


def _empty_dict() -> dict[str, Any]:
    return {}


def _empty_task_list() -> list["Task"]:
    return []


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


MAX_DATABASE_OUTAGE_RETRIES = 10

_TASK_PAYLOAD_COMPATIBILITY_KEYS = frozenset(
    {"dedupe_override", "template_content_type", "template_task"}
)


def _sanitize_task_payload_dict(payload: dict[object, Any]) -> dict[str, Any]:
    sanitized = {str(key): value for key, value in payload.items()}
    for key in _TASK_PAYLOAD_COMPATIBILITY_KEYS:
        sanitized.pop(key, None)
    return sanitized


def _serialize_task_payload(payload: Any) -> Any:
    if payload is None or isinstance(payload, (str, int, float, bool)):
        return payload
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, dict):
        return {str(key): _serialize_task_payload(value) for key, value in payload.items()}
    if isinstance(payload, (list, tuple)):
        return [_serialize_task_payload(value) for value in payload]
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        return _serialize_task_payload(model_dump())
    return payload


def _deserialize_task_payload(task_type: "TaskType", payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(
        payload,
        (ScrapeTaskPayload, DownloadTaskPayload, APICommandTaskPayload),
    ):
        return payload
    if not isinstance(payload, dict):
        return payload

    payload_dict = _sanitize_task_payload_dict(cast(dict[object, Any], payload))

    try:
        if task_type == TaskType.SCRAPE:
            return ScrapeTaskPayload.model_validate(payload_dict)
        if task_type == TaskType.DOWNLOAD:
            return DownloadTaskPayload.model_validate(payload_dict)
        if task_type == TaskType.API_COMMAND:
            return APICommandTaskPayload.model_validate(payload_dict)
    except Exception as exc:
        logger.debug(
            "Falling back to raw payload dict for task type %s: %s",
            task_type.value,
            exc,
        )
    return payload_dict


class TaskType(str, Enum):
    """Supported task types for Phase 1."""

    SCRAPE = "scrape"
    DOWNLOAD = "download"
    API_COMMAND = "api_command"
    PLUGIN = "plugin"


class TaskState(str, Enum):
    """Lifecycle states for a task."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    SAVING_DB = "saving_db"
    DEFERRED = "deferred"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"
    RECOVERED = "recovered"


class JobState(str, Enum):
    """Lifecycle states for a job."""

    DELETING = "deleting"

    STAGING = "staging"
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


JobStatus = JobState
TaskStatus = TaskState


class JobSource(str, Enum):
    """Standardized job sources - where the job originated from."""

    MANUAL = "manual"
    SCRAPER_TAB = "scraper_tab"
    CUSTOM_JOB = "custom_job"
    SCHEDULED = "scheduled"
    API = "api"
    TEMPLATE = "template"


@dataclass
class TaskResult:
    """Outcome of a task execution.

    Attributes:
        success: Whether the task succeeded
        output: Task output data (passed to dependent tasks)
        message: Human-readable result message
        errors: List of error messages encountered
        metrics: Numerical metrics for monitoring
        continue_job_on_failure: Whether this failed task may consume its
            failure budget while independent tasks continue
        completed_at: ISO timestamp of completion
        new_tasks: Tasks to add to the job dynamically (e.g., scrape spawns downloads)
    """

    success: bool
    output: Any = None
    message: str = ""
    errors: list[str] = field(default_factory=_empty_str_list)
    metrics: dict[str, Any] = field(default_factory=_empty_dict)
    continue_job_on_failure: bool = False
    completed_at: str = field(default_factory=_utc_now_iso)
    new_tasks: list["Task"] = field(default_factory=_empty_task_list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "output": self.output,
            "message": self.message,
            "errors": self.errors,
            "metrics": self.metrics,
            "continue_job_on_failure": self.continue_job_on_failure,
            "completed_at": self.completed_at,
            "new_tasks": [t.to_dict() for t in self.new_tasks],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskResult:
        raw_new_tasks = data.get("new_tasks")
        new_tasks_data: list[dict[str, Any]] = []
        if isinstance(raw_new_tasks, list):
            for item in cast(list[object], raw_new_tasks):
                if isinstance(item, dict):
                    new_tasks_data.append(cast(dict[str, Any], item))
        new_tasks = [Task.from_dict(task_data) for task_data in new_tasks_data]
        return cls(
            success=data["success"],
            output=data.get("output"),
            message=data.get("message", ""),
            errors=list(data.get("errors") or []),
            metrics=dict(data.get("metrics") or {}),
            continue_job_on_failure=bool(data.get("continue_job_on_failure", False)),
            completed_at=data.get("completed_at", _utc_now_iso()),
            new_tasks=new_tasks,
        )


class TaskExecutionError(RuntimeError):
    """Raised when a task execution fails unexpectedly."""


class TaskCancelled(RuntimeError):
    """Raised when a task is cancelled mid-flight."""


TaskHandler = Callable[["Task", "TaskRunContext"], Awaitable[TaskResult]]


class TaskRegistry:
    """Registry for task execution handlers keyed by :class:`TaskType`."""

    _handlers: Dict[TaskType, TaskHandler] = {}

    @classmethod
    def register(cls, task_type: TaskType, handler: TaskHandler) -> None:
        if task_type in cls._handlers:
            # Changed to debug level to avoid console spam during module loading
            logger.debug("Overwriting handler for task type %s", task_type.value)
        cls._handlers[task_type] = handler

    @classmethod
    def get(cls, task_type: TaskType) -> Optional[TaskHandler]:
        return cls._handlers.get(task_type)

    @classmethod
    def registered_types(cls) -> Iterable[TaskType]:
        return list(cls._handlers.keys())


register_task_handler = TaskRegistry.register


@dataclass
class Task:
    """Represents an individual unit of work within a job.

    A Task can contain SubTasks, creating a hierarchical structure:
        Task (parent)
        └── SubTask (child tasks with parent_task_id set)

    Example:
        Task: "Scrape User: onlyfans"
        ├── SubTask: "Scrape Stories"
        ├── SubTask: "Scrape Posts"
        └── SubTask: "Scrape Messages"
    """

    user_id: int | str
    task_type: TaskType
    content_type: str | None = None
    site_name: str = ""
    user_username: str | None = None
    payload: Any = field(default_factory=_empty_dict)
    auth_id: int | None = None
    job_id: str | None = None
    mps_task_manifest: str | None = None
    capabilities: list[str] = field(default_factory=_empty_str_list)
    ip_lock_key: Optional[str] = None
    deferred_dedup_key: Optional[str] = None
    task_id: str = field(default_factory=lambda: f"task-{uuid4().hex[:12]}")
    state: TaskState = TaskState.PENDING
    worker_id: str | None = None
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    depends_on: list[str] = field(default_factory=_empty_str_list)
    result: Optional[TaskResult] = None
    attempts: int = 0
    max_attempts: int = 3
    database_outage_retries: int = 0
    # Hierarchical structure
    parent_task_id: Optional[str] = None  # Set if this is a subtask
    subtasks: list["Task"] = field(default_factory=_empty_task_list)  # Child tasks

    def forgive_database_outage(self) -> None:
        """Give back the try an outage took, up to a limit in case this task caused it."""
        if self.database_outage_retries < MAX_DATABASE_OUTAGE_RETRIES:
            self.database_outage_retries += 1
            self.max_attempts += 1

    @property
    def dedup_key(self) -> str:
        # JSON turns str-backed enums into their value. Reservation and worker
        # release must use the same key on either side of that round trip.
        content_type = (
            self.content_type.value
            if isinstance(self.content_type, Enum)
            else self.content_type
        )
        return (
            f"task:{self.task_type.value}:{self.site_name}:{self.user_id}:"
            f"{content_type or ''}:{self.auth_id or 'any'}"
        )

    @property
    def is_subtask(self) -> bool:
        """Check if this task is a subtask of another task."""
        return self.parent_task_id is not None

    @property
    def has_subtasks(self) -> bool:
        """Check if this task has child subtasks."""
        return len(self.subtasks) > 0

    @property
    def total_subtasks(self) -> int:
        """Count total number of subtasks."""
        return len(self.subtasks)

    @property
    def completed_subtasks(self) -> int:
        """Count completed subtasks."""
        return sum(1 for st in self.subtasks if st.state == TaskState.COMPLETED)

    @property
    def failed_subtasks(self) -> int:
        """Count failed subtasks."""
        return sum(1 for st in self.subtasks if st.state == TaskState.FAILED)

    def model_copy(
        self,
        *,
        update: dict[str, Any] | None = None,
        deep: bool = False,
    ) -> Task:
        """Pydantic-style compatibility helper for legacy call sites."""

        cloned = copy.deepcopy(self) if deep else replace(self)
        if update:
            for key, value in update.items():
                setattr(cloned, key, value)
        return cloned

    def payload_value(self, key: str, default: Any = None) -> Any:
        if isinstance(self.payload, dict):
            return self.payload.get(key, default)
        return getattr(self.payload, key, default)

    def to_dict(self, *, include_result: bool = True) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type.value,
            "site_name": self.site_name,
            "user_id": self.user_id,
            "user_username": self.user_username,
            "content_type": self.content_type,
            "payload": _serialize_task_payload(self.payload),
            "auth_id": self.auth_id,
            "job_id": self.job_id,
            "capabilities": self.capabilities,
            "ip_lock_key": self.ip_lock_key,
            "deferred_dedup_key": self.deferred_dedup_key,
            "state": self.state.value,
            "worker_id": self.worker_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "depends_on": self.depends_on,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            **({"database_outage_retries": self.database_outage_retries} if self.database_outage_retries else {}),
            "result": self.result.to_dict() if include_result and self.result else None,
            "parent_task_id": self.parent_task_id,
            "subtasks": [st.to_dict(include_result=include_result) for st in self.subtasks],
            **({"mps_task_manifest": self.mps_task_manifest} if self.mps_task_manifest is not None else {}),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Task:
        result_data = data.get("result")
        result = TaskResult.from_dict(result_data) if result_data else None
        task_type = TaskType(data.get("task_type", TaskType.API_COMMAND.value))

        # Recursively build subtasks
        raw_subtasks = data.get("subtasks")
        subtasks_data: list[dict[str, Any]] = []
        if isinstance(raw_subtasks, list):
            for item in cast(list[object], raw_subtasks):
                if isinstance(item, dict):
                    subtasks_data.append(cast(dict[str, Any], item))
        subtasks = [
            cls.from_dict(st_data)
            for st_data in subtasks_data
        ]

        return cls(
            task_id=str(data.get("task_id", f"task-{uuid4().hex[:12]}")),
            task_type=task_type,
            site_name=str(data.get("site_name", "")),
            user_id=data.get("user_id", 0),
            user_username=data.get("user_username"),
            content_type=data.get("content_type"),
            payload=_deserialize_task_payload(task_type, data.get("payload") or {}),
            auth_id=data.get("auth_id"),
            job_id=data.get("job_id"),
            mps_task_manifest=data.get("mps_task_manifest"),
            capabilities=list(data.get("capabilities") or []),
            ip_lock_key=data.get("ip_lock_key"),
            deferred_dedup_key=data.get("deferred_dedup_key"),
            state=TaskState(data.get("state", TaskState.PENDING.value)),
            worker_id=(
                str(data["worker_id"])
                if data.get("worker_id") is not None
                else None
            ),
            created_at=data.get("created_at", _utc_now_iso()),
            updated_at=data.get("updated_at", _utc_now_iso()),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            depends_on=list(data.get("depends_on") or []),
            attempts=data.get("attempts", 0),
            max_attempts=data.get("max_attempts", 3),
            database_outage_retries=data.get("database_outage_retries", 0),
            result=result,
            parent_task_id=data.get("parent_task_id"),
            subtasks=subtasks,
        )

    async def _publish_subtask_progress(
        self, job: "Job", context: Optional[dict[str, Any]]
    ) -> None:
        """Publish subtask progress event to Redis."""
        if not context or "redis" not in context:
            return

        try:
            redis = context["redis"]

            event = {
                "type": "subtask_progress",
                "job_id": self.job_id,
                "task_id": self.task_id,
                "total_subtasks": self.total_subtasks,
                "completed_subtasks": self.completed_subtasks,
                "failed_subtasks": self.failed_subtasks,
                "timestamp": _utc_now_iso(),
            }

            await redis.client.publish(JOB_EVENTS_CHANNEL, json.dumps(event))
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Failed to publish subtask progress: {exc}")

    async def run(
        self,
        *,
        job: "Job",
        storage: "JobStorage",
        context: Optional[dict[str, Any]] = None,
        previous_output: Any = None,
        timeout: Optional[timedelta] = None,
    ) -> TaskResult:
        """Execute the task and its subtasks using registered handlers.

        Execution flow:
        1. If task has subtasks, execute them sequentially first
        2. Once all subtasks complete, execute the parent task handler
        3. Parent task can aggregate subtask results from context

        Args:
            job: Parent job executing this task.
            storage: Storage adapter used for persisting state updates.
            context: Optional execution context data.
            previous_output: Output from the preceding task in the chain.
            timeout: Optional timeout for task execution.

        Returns:
            The :class:`TaskResult` produced by the handler.
        """

        handler = TaskRegistry.get(self.task_type)
        if handler is None:
            raise TaskExecutionError(
                f"No handler registered for task type {self.task_type.value}"
            )

        if job.metadata.get("mps_manifest") and self.mps_task_manifest is None:
            self.mps_task_manifest = job.metadata.get("mps_task_manifest") or ""
        self.attempts += 1
        self.state = TaskState.RUNNING
        worker_id = (context or {}).get("worker_id")
        if worker_id is not None:
            self.worker_id = str(worker_id)
        self.started_at = _utc_now_iso()
        self.updated_at = self.started_at
        await storage.persist_task(job_id=job.job_id, task=self)

        run_context = TaskRunContext(
            job=job,
            storage=storage,
            previous_output=previous_output,
            context=context or {},
        )

        try:
            # Execute subtasks first if they exist
            if self.has_subtasks:
                logger.info(
                    f"Task {self.task_id} executing {len(self.subtasks)} subtasks"
                )
                subtask_results: list[TaskResult] = []

                for subtask in self.subtasks:
                    logger.info(
                        f"Executing subtask {subtask.task_id}: {subtask.content_type}"
                    )
                    subtask_result = await subtask.run(
                        job=job,
                        storage=storage,
                        context=context,
                        previous_output=previous_output,
                        timeout=timeout,
                    )
                    subtask_results.append(subtask_result)

                    # Publish progress after each subtask
                    await self._publish_subtask_progress(job, context)

                # Store subtask results in context for parent handler
                run_context.context["subtask_results"] = subtask_results
                logger.info(
                    f"Task {self.task_id} completed all subtasks, "
                    f"aggregating results"
                )

            # Execute the task handler (may aggregate subtask results)
            async def execute_handler():
                if timeout:
                    return await asyncio.wait_for(
                        handler(self, run_context), timeout=timeout.total_seconds()
                    )
                return await handler(self, run_context)

            # Profile lookups only persist database records. Restoration reserves
            # archive files, so it must not block these tasks before API lookup.
            # A paid feed still follows the content path even when its task is
            # labelled Profile; each subtask acquires its own appropriate lease.
            profile_lookup = (
                self.task_type == TaskType.SCRAPE
                and self.content_type == ContentType.PROFILE
                and not getattr(self.payload, "paid_content_feed", False)
            )
            if self.task_type == TaskType.DOWNLOAD or (
                self.task_type == TaskType.SCRAPE and not profile_lookup
            ):
                from ..services.archive_write_guard import writer_guard

                async with writer_guard(run_context.context.get("config")):
                    result = await execute_handler()
            else:
                result = await execute_handler()

        except asyncio.CancelledError as exc:
            logger.info("Task %s cancelled", self.task_id)
            self.state = TaskState.CANCELLED
            self.completed_at = _utc_now_iso()
            self.updated_at = self.completed_at
            self.result = TaskResult(success=False, message="Task cancelled")
            await storage.persist_task(job_id=job.job_id, task=self)
            raise TaskCancelled("Task execution cancelled") from exc
        except Exception as exc:  # noqa: BLE001
            logger.exception("Task %s failed: %s", self.task_id, exc)
            self.state = TaskState.FAILED
            self.completed_at = _utc_now_iso()
            self.updated_at = self.completed_at
            self.result = TaskResult(
                success=False,
                message=str(exc),
                errors=[str(exc)],
            )
            await storage.persist_task(job_id=job.job_id, task=self)
            raise TaskExecutionError(str(exc)) from exc

        self.result = result
        self.completed_at = result.completed_at
        self.updated_at = result.completed_at
        self.state = TaskState.COMPLETED if result.success else TaskState.FAILED
        await storage.persist_task(job_id=job.job_id, task=self)

        if not result.success and self.state == TaskState.FAILED:
            raise TaskExecutionError(result.message or "Task failed")

        return result


@dataclass
class JobSchedule:
    """Represents the scheduling metadata for a job."""

    scheduled_for: Optional[str] = None
    interval_seconds: Optional[int] = None
    cron: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scheduled_for": self.scheduled_for,
            "interval_seconds": self.interval_seconds,
            "cron": self.cron,
        }

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> JobSchedule:
        if not data:
            return cls()
        return cls(
            scheduled_for=data.get("scheduled_for"),
            interval_seconds=data.get("interval_seconds"),
            cron=data.get("cron"),
        )


@dataclass
class JobConfig:
    """Immutable job configuration stored separately from runtime state."""

    job_id: str
    name: str
    created_by: str
    schedule: JobSchedule = field(default_factory=JobSchedule)
    metadata: dict[str, Any] = field(default_factory=_empty_dict)
    task_ids: list[str] = field(default_factory=_empty_str_list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "created_by": self.created_by,
            "schedule": self.schedule.to_dict(),
            "metadata": self.metadata,
            "task_ids": list(self.task_ids),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobConfig":
        raw_task_ids = data.get("task_ids")
        task_ids = raw_task_ids if isinstance(raw_task_ids, list) else []
        return cls(
            job_id=str(data["job_id"]),
            name=str(data["name"]),
            created_by=str(data["created_by"]),
            schedule=JobSchedule.from_dict(
                cast(dict[str, Any] | None, data.get("schedule"))
            ),
            metadata=dict(cast(dict[str, Any] | None, data.get("metadata")) or {}),
            task_ids=[str(task_id) for task_id in task_ids],
        )


@dataclass
class JobRuntimeState:
    """Mutable job runtime state stored in the Redis state hash."""

    status: JobState = JobState.PENDING
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobRuntimeState":
        def _as_int(key: str) -> int:
            raw_value = data.get(key, 0)
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return 0

        return cls(
            status=JobState(str(data.get("status", JobState.PENDING.value))),
            created_at=str(data.get("created_at", _utc_now_iso())),
            updated_at=str(data.get("updated_at", _utc_now_iso())),
            started_at=(
                str(data["started_at"]) if data.get("started_at") is not None else None
            ),
            completed_at=(
                str(data["completed_at"])
                if data.get("completed_at") is not None
                else None
            ),
            total_tasks=_as_int("total_tasks"),
            completed_tasks=_as_int("completed_tasks"),
            failed_tasks=_as_int("failed_tasks"),
        )


@dataclass
class Job:
    """Aggregates multiple tasks into a unit of work."""

    name: str
    created_by: str
    tasks: list[Task]
    job_id: str = field(default_factory=lambda: f"job-{uuid4().hex[:12]}")
    state: JobState = JobState.PENDING
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    schedule: JobSchedule = field(default_factory=JobSchedule)
    metadata: dict[str, Any] = field(default_factory=_empty_dict)
    task_ids: list[str] = field(default_factory=_empty_str_list)

    def __post_init__(self) -> None:
        if not self.task_ids:
            self.task_ids = [task.task_id for task in self.tasks]

    def to_dict(self) -> dict[str, Any]:
        """Serialize job to dictionary with standardized metadata fields.

        IMPORTANT: Always includes total_tasks, completed_tasks, failed_tasks
        as per JOB_TASK_STANDARDIZATION.md to ensure consistent tracking.
        """
        return {
            "job_id": self.job_id,
            "name": self.name,
            "created_by": self.created_by,
            "state": self.state.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "schedule": self.schedule.to_dict(),
            "metadata": self.metadata,
            "task_ids": list(self.task_ids),
            # Metadata fields - always included for standardization
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "tasks": [task.to_dict() for task in self.tasks],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        raw_tasks = data.get("tasks")
        task_payloads: list[dict[str, Any]] = []
        if isinstance(raw_tasks, list):
            for item in cast(list[object], raw_tasks):
                if isinstance(item, dict):
                    task_payloads.append(cast(dict[str, Any], item))
        return cls(
            job_id=data["job_id"],
            name=data["name"],
            created_by=data["created_by"],
            state=JobState(data.get("state", JobState.PENDING.value)),
            created_at=data.get("created_at", _utc_now_iso()),
            updated_at=data.get("updated_at", _utc_now_iso()),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            schedule=JobSchedule.from_dict(data.get("schedule")),
            metadata=dict(data.get("metadata") or {}),
            task_ids=list(data.get("task_ids") or []),
            tasks=[Task.from_dict(task_data) for task_data in task_payloads],
        )

    @property
    def is_scheduled(self) -> bool:
        """Check if job has any schedule configuration."""
        return bool(
            self.schedule.scheduled_for
            or self.schedule.interval_seconds
            or self.schedule.cron
        )

    @property
    def source(self) -> Optional[JobSource]:
        """Get the job source from metadata."""
        source_value = self.metadata.get("source")
        if source_value and isinstance(source_value, str):
            try:
                return JobSource(source_value)
            except ValueError:
                return None
        return None

    @property
    def total_tasks(self) -> int:
        """Count total number of top-level tasks."""
        if self.tasks:
            return len(self.tasks)
        if self.task_ids:
            return len(self.task_ids)
        override = self.metadata.get("total_tasks_count_override")
        if override is not None:
            return int(override)
        return 0

    @property
    def total_subtasks(self) -> int:
        """Count total number of subtasks across all tasks."""
        return sum(task.total_subtasks for task in self.tasks)

    @property
    def completed_tasks(self) -> int:
        """Count completed top-level tasks."""
        if self.tasks:
            return sum(1 for task in self.tasks if task.state == TaskState.COMPLETED)
        override = self.metadata.get("completed_tasks_count_override")
        return int(override) if override is not None else 0

    @property
    def failed_tasks(self) -> int:
        """Count failed top-level tasks."""
        if self.tasks:
            return sum(1 for task in self.tasks if task.state == TaskState.FAILED)
        override = self.metadata.get("failed_tasks_count_override")
        return int(override) if override is not None else 0

    @property
    def completed_tasks_count(self) -> int:
        """Compatibility alias for legacy job-count callers."""
        return self.completed_tasks

    @completed_tasks_count.setter
    def completed_tasks_count(self, value: int) -> None:
        self.metadata["completed_tasks_count_override"] = value

    @property
    def failed_tasks_count(self) -> int:
        """Compatibility alias for legacy job-count callers."""
        return self.failed_tasks

    @failed_tasks_count.setter
    def failed_tasks_count(self, value: int) -> None:
        self.metadata["failed_tasks_count_override"] = value

    @classmethod
    def create(
        cls,
        *,
        name: str,
        created_by: str,
        tasks: Iterable[Task],
        schedule: Optional[JobSchedule] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Job:
        """Unified job creation method."""
        return cls(
            name=name,
            created_by=created_by,
            tasks=list(tasks),
            schedule=schedule or JobSchedule(),
            metadata=metadata or {},
        )

    def add_task(self, task: Task) -> None:
        self.tasks.append(task)
        self.task_ids.append(task.task_id)
        self.updated_at = _utc_now_iso()

    def to_config(self, *, include_task_ids: bool = True) -> JobConfig:
        return JobConfig.from_dict(
            {
                "job_id": self.job_id,
                "name": self.name,
                "created_by": self.created_by,
                "schedule": self.schedule.to_dict(),
                "metadata": self.metadata,
                "task_ids": (
                    (list(self.task_ids) if self.task_ids else [t.task_id for t in self.tasks])
                    if include_task_ids else []
                ),
            }
        )

    def to_state(self) -> JobRuntimeState:
        return JobRuntimeState.from_dict(
            {
                "status": self.state.value,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
            }
        )

    @classmethod
    def from_split_data(cls, config: JobConfig, state: JobRuntimeState) -> "Job":
        return cls(
            job_id=config.job_id,
            name=config.name,
            created_by=config.created_by,
            tasks=[],
            task_ids=list(config.task_ids),
            schedule=JobSchedule.from_dict(config.schedule.to_dict()),
            metadata={
                **config.metadata,
                "total_tasks_count_override": state.total_tasks,
                "completed_tasks_count_override": state.completed_tasks,
                "failed_tasks_count_override": state.failed_tasks,
            },
            state=JobState(state.status.value),
            created_at=state.created_at,
            updated_at=state.updated_at,
            started_at=state.started_at,
            completed_at=state.completed_at,
        )

    async def publish_progress(self, context: Optional[dict[str, Any]] = None) -> None:
        """Compatibility helper for callers that publish progress outside run()."""

        if not context or "redis" not in context:
            return

        try:
            redis = context["redis"]
            event = {
                "type": "job_progress",
                "job_id": self.job_id,
                "job_name": self.name,
                "status": self.state.value,
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "total_subtasks": self.total_subtasks,
                "timestamp": _utc_now_iso(),
            }
            await redis.client.publish(JOB_EVENTS_CHANNEL, json.dumps(event))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Job %s: Failed to publish job progress: %s", self.job_id, exc)

    async def run(
        self,
        *,
        storage: "JobStorage",
        context: Optional[dict[str, Any]] = None,
        timeout_per_task: Optional[timedelta] = None,
    ) -> Any:
        """Execute job tasks sequentially with chaining support."""

        self.state = JobState.RUNNING
        self.started_at = _utc_now_iso()
        self.updated_at = self.started_at
        await storage.persist_job(self, runtime_only=True)

        # Helper to publish progress events
        async def publish_progress() -> None:
            """Publish job progress event to Redis."""
            if not context or "redis" not in context:
                logger.warning(
                    f"Job {self.job_id}: Cannot publish progress - no Redis context available"
                )
                return

            try:
                redis = context["redis"]

                event = {
                    "type": "job_progress",
                    "job_id": self.job_id,
                    "job_name": self.name,
                    "status": self.state.value,
                    "total_tasks": self.total_tasks,
                    "completed_tasks": self.completed_tasks,
                    "failed_tasks": self.failed_tasks,
                    "total_subtasks": self.total_subtasks,
                    "timestamp": _utc_now_iso(),
                }

                await redis.client.publish(JOB_EVENTS_CHANNEL, json.dumps(event))
                logger.info(
                    f"Job {self.job_id}: Published progress event - "
                    f"completed_tasks={self.completed_tasks}/{self.total_tasks}, "
                    f"status={self.state.value}"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"Job {self.job_id}: Failed to publish job progress: {exc}"
                )

        last_output: Any = None
        for task in self.tasks:
            if task.depends_on:
                unmet = [
                    dep for dep in task.depends_on if not self._task_completed(dep)
                ]
                if unmet:
                    raise TaskExecutionError(
                        f"Task {task.task_id} has unmet dependencies: {", ".join(unmet)}"
                    )

            try:
                result = await task.run(
                    job=self,
                    storage=storage,
                    context=context,
                    previous_output=last_output,
                    timeout=timeout_per_task,
                )
            except TaskCancelled:
                logger.info("Job %s cancelled via task %s", self.job_id, task.task_id)
                self.state = JobState.CANCELLED
                self.completed_at = _utc_now_iso()
                self.updated_at = self.completed_at
                await storage.persist_job(self, runtime_only=True)
                raise
            except TaskExecutionError as exc:
                logger.error(
                    "Job %s failed on task %s: %s", self.job_id, task.task_id, exc
                )
                self.state = JobState.FAILED
                self.completed_at = _utc_now_iso()
                self.updated_at = self.completed_at
                await storage.persist_job(self, runtime_only=True)
                raise

            last_output = result.output

            # Publish progress after each task completes
            await publish_progress()

        self.state = JobState.COMPLETED
        self.completed_at = _utc_now_iso()
        self.updated_at = self.completed_at
        await storage.persist_job(self, runtime_only=True)
        return last_output

    def _task_completed(self, task_id: str) -> bool:
        for task in self.tasks:
            if task.task_id == task_id:
                return task.state == TaskState.COMPLETED
        return False


@dataclass
class JobDispatchEnvelope:
    """Payload stored in backend queues when dispatching jobs to workers."""

    job: Job
    capabilities: list[str] = field(default_factory=_empty_str_list)
    ip_lock_key: Optional[str] = None
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=_empty_dict)
    backend_id: Optional[str] = None
    dispatched_at: str = field(default_factory=_utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job.to_dict(),
            "capabilities": self.capabilities,
            "ip_lock_key": self.ip_lock_key,
            "priority": self.priority,
            "metadata": self.metadata,
            "backend_id": self.backend_id,
            "dispatched_at": self.dispatched_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobDispatchEnvelope":
        return cls(
            job=Job.from_dict(data["job"]),
            capabilities=list(data.get("capabilities") or []),
            ip_lock_key=data.get("ip_lock_key"),
            priority=data.get("priority", 0),
            metadata=dict(data.get("metadata") or {}),
            backend_id=data.get("backend_id"),
            dispatched_at=data.get("dispatched_at", _utc_now_iso()),
        )


@dataclass
class TaskRunContext:
    """Runtime context passed to task handlers."""

    job: Job
    storage: "JobStorage"
    previous_output: Any
    context: dict[str, Any]
    site_api: Any = None
    user_manager: Any = None
    db_managers: dict[str, "BaseDatabaseManager"] | None = None
    config: "UltimaScraperCollectionConfig | None" = None
    site: str | None = None
    worker_id: str | None = None
    redis: "RedisManager | None" = None
    database_writes: "DatabaseWriteClient | None" = None

    def __post_init__(self) -> None:
        runtime_context = self.context or {}
        self.site_api = runtime_context.get("site_api", runtime_context.get("api"))
        self.user_manager = runtime_context.get("user_manager")
        db_managers = runtime_context.get("db_managers")
        if isinstance(db_managers, dict):
            self.db_managers = cast(dict[str, "BaseDatabaseManager"], db_managers)
        self.config = cast(
            "UltimaScraperCollectionConfig | None",
            runtime_context.get("config"),
        )
        site_value = runtime_context.get("site")
        self.site = str(site_value) if site_value is not None else None
        worker_value = runtime_context.get("worker_id")
        self.worker_id = str(worker_value) if worker_value is not None else None
        self.redis = cast("RedisManager | None", runtime_context.get("redis"))
        self.database_writes = cast(
            "DatabaseWriteClient | None", runtime_context.get("database_writes")
        )


async def _default_passthrough_handler(
    task: Task, context: TaskRunContext
) -> TaskResult:
    """Fallback handler that simply returns the payload output."""

    message = task.payload_value("message", "Task completed")
    output = task.payload_value("output")
    metrics = task.payload_value("metrics", {})
    return TaskResult(
        success=True,
        output=output,
        message=message,
        metrics=dict(metrics),
    )


def _ensure_default_handlers_registered() -> None:
    registered = set(TaskRegistry.registered_types())
    if TaskType.SCRAPE not in registered:
        register_task_handler(TaskType.SCRAPE, _default_passthrough_handler)
    if TaskType.DOWNLOAD not in registered:
        register_task_handler(TaskType.DOWNLOAD, _default_passthrough_handler)
    if TaskType.API_COMMAND not in registered:
        register_task_handler(TaskType.API_COMMAND, _default_passthrough_handler)


_ensure_default_handlers_registered()
