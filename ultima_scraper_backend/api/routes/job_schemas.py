"""Request and response models for the job routes."""

from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel, Field

from ...job_system import JobStatus

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | dict[str, JsonValue] | list[JsonValue]


class JobScheduleRequest(BaseModel):
    """Job scheduling configuration."""

    scheduled_for: str | None = None
    interval_seconds: int | None = None
    cron: str | None = None
    start_immediately: bool | None = None


class TaskPayload(BaseModel):
    """Task payload definition from the API."""

    task_type: str
    params: dict[str, Any]


class CreateJobRequest(BaseModel):
    """Request to create a job."""

    job_name: str
    tasks: list[TaskPayload]
    created_by: str
    schedule: JobScheduleRequest | None = None
    ignore_space_requirements: bool = False
    save_as_template: bool | None = None
    template_name: str | None = None
    template_description: str | None = None
    # Run only on this backend's workers (otherwise any backend may claim its tasks).
    backend_id: str | None = None


class CreateJobTemplateRequest(BaseModel):
    """Request to save a job as a template."""

    name: str
    description: str | None = None
    created_by: str = "webui"


class UpdateJobRequest(BaseModel):
    """Request to update job metadata or schedule."""

    job_name: str | None = None
    schedule: JobScheduleRequest | None = None
    tasks: list[TaskPayload] | None = None


class RetryJobRequest(BaseModel):
    """Request to retry a job, optionally overriding the auth account.

    When ``auth_id`` is omitted (or ``None``), each cloned task keeps its
    original ``auth_id`` so the retry uses the same auth that created the
    original job.
    """

    auth_id: int | None = None


class DeleteJobsRequest(BaseModel):
    """Request to remove selected jobs or jobs older than a cutoff."""

    job_ids: list[str] = Field(default_factory=list, max_length=200)
    older_than_hours: int | None = Field(default=None, gt=0)


class DeleteJobFailure(BaseModel):
    job_id: str
    error: str


class DeleteJobsResponse(BaseModel):
    queued: int = 0
    requested: int
    deleted: int
    failed: int
    failures: list[DeleteJobFailure] = Field(default_factory=list)


class TaskSummary(BaseModel):
    """Brief task summary for job list view."""

    task_type: str
    content_type: str | None = None
    status: str
    username: str | None = None
    site: str | None = None
    auth_id: int | None = None
    items_processed: int = 0
    items_total: int | None = None


class TaskStats(BaseModel):
    """Aggregate task counters for list/detail views."""

    total: int = 0
    ready: int = 0
    blocked: int = 0
    inflight: int = 0
    deferred: int = 0
    completed: int = 0
    failed: int = 0
    task_type_counts: dict[str, int] = Field(default_factory=dict)


class JobErrorStats(BaseModel):
    """Aggregated error counts grouped by normalized message."""

    total: int = 0
    groups: dict[str, int] = Field(default_factory=dict)


class JobResponse(BaseModel):
    """Job response model."""

    job_id: str
    execution_engine: str | None = None
    deletion_progress: dict[str, Any] | None = None
    job_name: str
    job_type: str | None = None
    site: str | None = None
    status: JobStatus
    priority: int = 0
    worker_id: str | None = None
    created_at: str
    updated_at: str | None
    started_at: str | None = None
    completed_at: str | None = None
    task_count: int
    completed_tasks: int = 0
    failed_tasks: int = 0
    tasks: List[TaskSummary] = []
    task_stats: TaskStats = Field(default_factory=TaskStats)
    error_stats: JobErrorStats = Field(default_factory=JobErrorStats)
    created_by: str | None = None
    is_scheduled: bool = False
    scheduled_for: str | None = None
    interval_seconds: int | None = None
    cron: str | None = None
    run_count: int = 0
    last_ran_at: str | None = None
    last_activity_at: str | None = None
    materialization_status: str | None = None
    materialized_tasks: int | None = None
    materialization_total: int | None = None
    materialization_error: str | None = None


class TaskDetail(BaseModel):
    """Detailed task information."""

    task_id: str | None = None
    task_type: str
    status: str
    site: str | None = None
    username: str | None = None
    content_type: str | None = None
    payload: dict[str, JsonValue] | None = None
    worker_id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    progress: float = 0.0
    items_processed: int = 0
    items_total: int | None = None
    error: str | None = None
    result: dict[str, Any] | None = None
    proxy_url: str | None = None
    proxy_routes: list[str | None] | None = None
    proxy_hop_used: int | None = None


class ActiveTaskDetail(BaseModel):
    """Active task detail for dashboard."""

    job_id: str
    job_name: str
    job_status: str
    task_id: str | None = None
    task_type: str
    status: str
    job_total_tasks: int | None = None
    site: str | None = None
    username: str | None = None
    content_type: str | None = None
    worker_id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    progress: float = 0.0
    items_processed: int = 0
    items_total: int | None = None
    items_scraped: int | None = None
    items_saved: int | None = None
    bytes_written: int | None = None
    total_bytes: int | None = None
    speed_bps: float | None = None
    avg_speed_bps: float | None = None
    error: str | None = None


class ActiveTaskListResponse(BaseModel):
    """Response payload for active tasks."""

    tasks: list[ActiveTaskDetail]
    total: int


class JobRuntimeTelemetry(BaseModel):
    """Bounded, full-job execution telemetry aggregated by workers."""

    version: int = 13
    scope: str = "full_job_aggregate"
    task_count: int = 0
    succeeded_tasks: int = 0
    failed_tasks: int = 0
    first_started_at: str | None = None
    last_finished_at: str | None = None
    active_window_ms: int = 0
    task_duration_sum_ms: float = 0.0
    mean_task_duration_ms: float | None = None
    mean_queue_wait_ms: float | None = None
    mean_setup_ms: float | None = None
    setup_subphases: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    mean_handler_ms: float | None = None
    mean_handler_exclusive_ms: float | None = None
    guest_auth_stats: dict[str, dict[str, float]] = Field(default_factory=dict)
    terminal_stats: dict[str, dict[str, float]] = Field(default_factory=dict)
    ledger_stats: dict[str, dict[str, float]] = Field(default_factory=dict)
    mean_handler_guest_init_ms: float | None = None
    handler_guest_init_count: int = 0
    mean_handler_initial_lookup_ms: float | None = None
    handler_initial_lookup_count: int = 0
    mean_handler_refresh_api_ms: float | None = None
    handler_refresh_api_count: int = 0
    mean_handler_fallback_retry_ms: float | None = None
    handler_fallback_retry_count: int = 0
    mean_handler_serialization_ms: float | None = None
    handler_serialization_count: int = 0
    mean_writer_ms: float | None = None
    mean_post_ms: float | None = None
    mean_release_ms: float | None = None
    mean_claim_wait_ms: float | None = None
    mean_writer_admission_ms: float | None = None
    mean_writer_admission_serialization_ms: float | None = None
    mean_writer_admission_capacity_wait_ms: float | None = None
    mean_writer_admission_redis_ms: float | None = None
    mean_writer_admission_overhead_detail_ms: float | None = None
    mean_writer_queue_ms: float | None = None
    mean_writer_execution_ms: float | None = None
    mean_writer_commit_ms: float | None = None
    mean_writer_overhead_ms: float | None = None
    mean_writer_batch_peer_work_ms: float | None = None
    mean_writer_delivery_ms: float | None = None
    mean_writer_result_publication_ms: float | None = None
    mean_writer_result_observation_ms: float | None = None
    mean_writer_result_delivery_residual_ms: float | None = None
    writer_result_delivery_split_coverage_ratio: float | None = None
    writer_result_cache_hits: int = 0
    writer_receipt_fallbacks: int = 0
    writer_result_cache_hit_ratio: float | None = None
    writer_split_missing_reasons: dict[str, int] = Field(default_factory=dict)
    writer_operation_stats: dict[str, dict[str, float | int]] = Field(
        default_factory=dict
    )
    writer_domain_stats: dict[str, dict[str, float | int]] = Field(
        default_factory=dict
    )
    writer_timed_submits: int = 0
    writer_timing_coverage_ratio: float | None = None
    mean_writer_connection_checkout_ms: float | None = None
    mean_writer_receipt_lookup_ms: float | None = None
    mean_writer_receipt_finalize_ms: float | None = None
    mean_writer_payload_ms: float | None = None
    mean_writer_user_row_ms: float | None = None
    mean_writer_alias_ms: float | None = None
    mean_writer_authority_ms: float | None = None
    writer_authority_rows_examined: int = 0
    writer_authority_rows_skipped: int = 0
    writer_authority_rows_upserted: int = 0
    mean_writer_profile_ms: float | None = None
    mean_writer_social_ms: float | None = None
    mean_writer_flush_ms: float | None = None
    mean_writer_queue_commands_at_start: float | None = None
    max_writer_queue_commands_at_start: int = 0
    mean_writer_queue_bytes_at_start: float | None = None
    max_writer_queue_bytes_at_start: int = 0
    mean_writer_active_lanes: float | None = None
    max_writer_active_lanes: int = 0
    mean_writer_batch_commands: float | None = None
    mean_writer_batch_fill_ratio: float | None = None
    mean_writer_lane_idle_ms: float | None = None
    mean_writer_lane_predecessor_wait_ms: float | None = None
    mean_writer_refill_wait_ms: float | None = None
    writer_configured_concurrency: int = 0
    writer_configured_batch_size: int = 0
    writer_window_commands: int = 0
    writer_queue_max_commands: int = 0
    writer_queue_max_bytes: int = 0
    writer_refill_min_commands: int = 0
    writer_refill_min_commands_per_lane: int = 0
    max_worker_completion_gap_ms: float = 0.0
    ready_queue_recovery_count: int = 0
    ready_queue_recovered_tasks: int = 0
    time_weighted_inflight: float | None = None
    route_attempts: int = 0
    route_active_sum_ms: float = 0.0
    route_lookup_stats: dict[str, dict[str, float | int | None]] = Field(
        default_factory=dict
    )
    time_weighted_active_routes: float | None = None
    steady_task_count: int = 0
    steady_window_ms: int = 0
    steady_throughput: float | None = None
    steady_mean_task_duration_ms: float | None = None
    ramp_window_ms: int | None = None
    drain_window_ms: int | None = None
    ready_nonempty_ratio: float | None = None
    ready_nonempty_samples: int = 0
    ready_sample_count: int = 0
    mean_ready_depth: float | None = None
    max_ready_depth: int = 0
    workers_seen: int = 0
    expected_workers: int = 0
    time_weighted_participating_workers: float | None = None
    time_weighted_effective_worker_capacity: float | None = None


class MPSRefillTraceEvent(BaseModel):
    """Timestamped queue/refill transitions for a durable MPS job."""

    stage: str
    cycle: int = 0
    timestamp_ms: int
    ready: int = 0
    inflight: int = 0
    completed: int = 0
    failed: int = 0
    window_end: int = 0
    task_count: int | None = None
    ready_added: int | None = None
    archived_tasks: int | None = None
    admitted_tasks: int | None = None
    archive_ms: float | None = None
    window_accounting_ms: float | None = None
    refill_ms: float | None = None
    pass_ms: float | None = None
    window_start: int | None = None
    reason: str | None = None
    # Archive passes: elapsed milliseconds per archive step.
    phases_ms: dict[str, float] | None = None
    controls: dict[str, int | str | bool | None] | None = None


class JobRuntimeSample(BaseModel):
    """One timestamped Redis task-state observation."""

    cursor: str
    timestamp_ms: int
    total: int
    ready: int
    inflight: int
    completed: int
    failed: int


class JobRuntimeSamplesPage(BaseModel):
    """A page of persisted job timeline samples."""

    samples: list[JobRuntimeSample]
    next_after_id: str | None = None
    has_more: bool = False


class JobDetailResponse(BaseModel):
    """Detailed job response with task information."""

    job_id: str
    execution_engine: str | None = None
    job_name: str | None = None
    job_type: str | None = None
    status: str
    site: str | None = None
    priority: int = 0
    worker_id: str | None = None
    retries: int = 0
    max_retries: int = 3
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    result_success: bool | None = None
    result_message: str | None = None
    result_error: str | None = None
    args: dict[str, Any] | None = None
    tasks: List[TaskDetail] = []
    task_stats: TaskStats = Field(default_factory=TaskStats)
    task_stats_observed_at_ms: int | None = None
    mps_refill_trace: list[MPSRefillTraceEvent] = Field(default_factory=list)
    error_stats: JobErrorStats = Field(default_factory=JobErrorStats)
    run_count: int = 0
    last_ran_at: str | None = None
    last_activity_at: str | None = None
    materialization_status: str | None = None
    materialized_tasks: int | None = None
    materialization_total: int | None = None
    materialization_error: str | None = None
    runtime_telemetry: JobRuntimeTelemetry | None = None


class TaskPageResponse(BaseModel):
    """Cursor page of tasks for aggregate-first drilldown views."""

    tasks: list[TaskDetail]
    next_cursor: str | None = None
    has_more: bool = False
    scanned_count: int = 0
    offset: int = 0
    limit: int = 100


class TemplateParameterPayload(BaseModel):
    name: str
    type: str = "string"
    required: bool = True
    default: JsonValue | None = None
    description: str = ""
    choices: list[JsonValue] | None = None


class RetryPolicyPayload(BaseModel):
    attempts: int = 3
    backoff_seconds: float = 5.0
    max_backoff_seconds: float = 60.0


class BackendStrategyPayload(BaseModel):
    mode: str = "auto"
    backend_id: str | None = None


class JobTemplateTaskPayload(BaseModel):
    name: str | None
    task_type: str
    site_name_template: str
    user_id_template: str
    content_type_template: str
    payload_template: dict[str, JsonValue] = {}
    capabilities: list[str] = []
    depends_on: list[str] = []
    ip_lock_key_template: str | None = None
    max_attempts: int | None = None
    dedupe_override: bool = False


class JobTemplateResponse(BaseModel):
    template_id: str
    name: str
    description: str
    created_by: str
    created_at: str
    updated_at: str
    default_priority: int
    retry_policy: RetryPolicyPayload
    backend_strategy: BackendStrategyPayload
    job_name_template: str | None
    default_schedule_at: str | None
    metadata: dict[str, JsonValue]
    parameters: list[TemplateParameterPayload]
    tasks: list[JobTemplateTaskPayload]


class JobTemplateCreateBase(BaseModel):
    template_name: str
    description: str = ""
    created_by: str = "webui"
    parameters: list[TemplateParameterPayload] = []
    default_priority: int = 0
    retry_policy: RetryPolicyPayload | None = None
    backend_strategy: BackendStrategyPayload | None = None
    job_name_template: str | None = None
    default_schedule_at: str | None = None
    metadata: dict[str, JsonValue] | None = None


class CreateJobTemplateFromTasksRequest(JobTemplateCreateBase):
    tasks: List[TaskPayload]


class CreateJobTemplateFromJobRequest(JobTemplateCreateBase):
    job_id: str


class TemplateEntry(BaseModel):
    """A single (username, auth_id) pair for batch template instantiation."""

    username: str
    auth_id: int


class InstantiateTemplateRequest(BaseModel):
    """Request to instantiate a job template with given parameter values.

    Use ``entries`` to batch multiple (username, auth_id) pairs into one job.
    Falls back to legacy single-entry mode when ``entries`` is omitted.
    """

    parameter_values: dict[str, Any]
    job_name: str | None = None
    created_by: str = "webui"
    auth_id: int | None = None  # legacy single-entry fallback
    entries: list[TemplateEntry] | None = None
    schedule: JobScheduleRequest | None = None
    ignore_space_requirements: bool = False


class JobListResponse(BaseModel):
    """Job list response."""

    jobs: List[JobResponse]
    total: int
    page: int
    page_size: int
    available_types: list[str] = []
    available_owners: list[str] = []
