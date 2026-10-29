"""Job system primitives for UltimaScraperVerse.

This package provides the data models, storage helpers, and manager layer
for the new asynchronous job/task pipeline that powers scraping and download
workflows across distributed backends.
"""

import importlib

from .drive_selection import DriveSelectionError, DriveSelectionResult, DriveSelector
from .job_builder import build_job, build_task
from .manager import JobManager
from .models import (
    Job,
    JobDispatchEnvelope,
    JobSchedule,
    JobSource,
    JobState,
    Task,
    TaskExecutionError,
    TaskResult,
    TaskRunContext,
    TaskState,
    TaskType,
    register_task_handler,
)
from .routing import BackendRouter
from .storage import JobStorage
from .subscription_validation import (
    SubscriptionInfo,
    get_effective_auth_for_performer,
    get_subscribed_auths,
    split_tasks_by_auth,
    validate_subscription,
)
from .templates import (
    BackendStrategy,
    JobTemplate,
    JobTemplateInstantiation,
    JobTemplateStore,
    RetryPolicy,
    TemplateOverrides,
    TemplateParameterDefinition,
)

JobStatus = JobState
TaskStatus = TaskState

# Ensure task handlers register during package import.
importlib.import_module(f"{__name__}.handlers")

__all__ = [
    "BackendRouter",
    "BackendStrategy",
    "build_job",
    "build_task",
    "DriveSelector",
    "DriveSelectionError",
    "DriveSelectionResult",
    "Job",
    "JobDispatchEnvelope",
    "JobManager",
    "JobSchedule",
    "JobSource",
    "JobState",
    "JobStatus",
    "JobStorage",
    "JobTemplate",
    "JobTemplateInstantiation",
    "JobTemplateStore",
    "RetryPolicy",
    "SubscriptionInfo",
    "Task",
    "TaskExecutionError",
    "TaskResult",
    "TaskRunContext",
    "TaskState",
    "TaskStatus",
    "TaskType",
    "TemplateOverrides",
    "TemplateParameterDefinition",
    "get_effective_auth_for_performer",
    "get_subscribed_auths",
    "register_task_handler",
    "split_tasks_by_auth",
    "validate_subscription",
]
