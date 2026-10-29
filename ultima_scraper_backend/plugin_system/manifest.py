"""Plugin manifest schema.

Every plugin is defined by a single YAML manifest with strict sections:
    identity, capabilities, task_types, event_subscriptions, callbacks,
    outbound_deliveries, and operational_policy.

Phase 1 uses a fixed Pydantic schema — no arbitrary extension structures.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from packaging.specifiers import InvalidSpecifier, SpecifierSet

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .permissions import PluginCapability
from .sdk import SDKContract, TaskHandler, reject_external_refs


# ── Regex for identifiers ────────────────────────────────────────────────

_PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,62}[a-z0-9]$")
_HANDLER_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,62}[a-z0-9]$")
_TASK_TYPE_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,62}[a-z0-9]$")
_EVENT_FAMILY_RE = re.compile(r"^[a-z][a-z0-9_.]{0,62}[a-z0-9]$")
_EVENT_TYPE_RE = re.compile(r"^[a-z][a-z0-9_.]{0,62}[a-z0-9]$")
_INTEGRATION_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,62}[a-z0-9]$")
_RUNTIME_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}[a-z0-9]$")
_DOMAIN_RE = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$")
_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$"
)


# ── Allowed event families ───────────────────────────────────────────────

ALLOWED_EVENT_FAMILIES: frozenset[str] = frozenset(
    {
        "job.lifecycle",
        "worker.health",
        "delivery.lifecycle",
        "plugin.lifecycle",
        "system.alerts",
    }
)


# ── Shared manifest base ───────────────────────────────────────────────────


class ManifestModel(BaseModel):
    """Base model for all manifest sections.

    Declarative plugins may not provide arbitrary extension/code fields.
    """

    model_config = ConfigDict(extra="forbid")


# ── Task-type field model (Pydantic-like, not raw JSON Schema) ────────────

class TaskFieldDefinition(ManifestModel):
    """A single field in a plugin-declared task schema."""

    name: str = Field(..., min_length=1, max_length=128)
    field_type: Literal[
        "string", "integer", "float", "boolean", "list", "object"
    ]
    required: bool = True
    description: str = ""
    allowed_values: list[str | int | float | bool] | None = None
    min_value: float | None = None
    max_value: float | None = None
    min_length: int | None = None
    max_length: int | None = None


class TaskResultFieldDefinition(ManifestModel):
    """A single field in a task-result schema."""

    name: str = Field(..., min_length=1, max_length=128)
    field_type: Literal[
        "string", "integer", "float", "boolean", "list", "object"
    ]
    description: str = ""


class PluginTaskType(ManifestModel):
    """A task type declared by a plugin.

    Maps to a real USR task kind.  Payloads are validated against
    ``payload_fields`` before submission.  Results are shaped by
    ``result_fields``.
    """

    task_type: str = Field(
        ...,
        min_length=3,
        max_length=64,
        validation_alias=AliasChoices("task_type", "job_type"),
        serialization_alias="task_type",
    )
    description: str = ""
    payload_fields: list[TaskFieldDefinition] = Field(default_factory=list)
    result_fields: list[TaskResultFieldDefinition] = Field(default_factory=list)
    max_priority: int = Field(default=10, ge=0, le=100)
    handler: TaskHandler | None = None
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None

    @field_validator("input_schema", "output_schema")
    @classmethod
    def _validate_schema(cls, value):
        if value is not None:
            from jsonschema import Draft202012Validator
            Draft202012Validator.check_schema(value)
            reject_external_refs(value)
        return value

    @field_validator("task_type")
    @classmethod
    def _validate_task_type(cls, v: str) -> str:
        if not _TASK_TYPE_RE.match(v):
            raise ValueError(
                f"task_type must match {_TASK_TYPE_RE.pattern!r}, got {v!r}"
            )
        return v

    @property
    def job_type(self) -> str:
        """Compatibility alias for older callers."""
        return self.task_type


# ── Event subscription ───────────────────────────────────────────────────

class EventSubscription(ManifestModel):
    """An event subscription declared by a plugin."""

    event_family: str
    event_types: list[str] = Field(default_factory=list)
    delivery_target: str = Field(
        ...,
        description="Name of an outbound delivery target to send matched events to.",
    )

    @field_validator("event_family")
    @classmethod
    def _validate_family(cls, v: str) -> str:
        if v not in ALLOWED_EVENT_FAMILIES:
            raise ValueError(
                f"event_family must be one of {sorted(ALLOWED_EVENT_FAMILIES)}, got {v!r}"
            )
        return v

    @field_validator("event_types")
    @classmethod
    def _validate_types(cls, v: list[str]) -> list[str]:
        for t in v:
            if not _EVENT_TYPE_RE.match(t):
                raise ValueError(
                    f"event_type must match {_EVENT_TYPE_RE.pattern!r}, got {t!r}"
                )
        return v


# ── Callback endpoint ────────────────────────────────────────────────────

class CallbackEndpoint(ManifestModel):
    """An inbound webhook/callback handler declared by a plugin.

    The platform assigns the actual route; the plugin declares a semantic
    handler id and a payload schema.
    """

    handler_id: str = Field(..., min_length=3, max_length=64)
    description: str = ""
    auth_method: Literal["hmac", "bearer"] = "hmac"
    payload_fields: list[TaskFieldDefinition] = Field(default_factory=list)

    @field_validator("handler_id")
    @classmethod
    def _validate_handler_id(cls, v: str) -> str:
        if not _HANDLER_ID_RE.match(v):
            raise ValueError(
                f"handler_id must match {_HANDLER_ID_RE.pattern!r}, got {v!r}"
            )
        return v


# ── Outbound delivery target ─────────────────────────────────────────────

class OutboundDeliveryTarget(ManifestModel):
    """An outbound delivery target declared by a plugin.

    Events matching a subscription are delivered here via signed HTTP POST.
    """

    target_name: str = Field(..., min_length=1, max_length=128)
    url_setting: str | None = None
    url: str = Field(..., min_length=1)
    description: str = ""

    @field_validator("url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        if not v.startswith(("https://", "http://")):
            raise ValueError("Outbound delivery URL must start with https:// or http://")
        return v


# ── Remote delegation ─────────────────────────────────────────────────────

REMOTE_DELEGATION_ROLES: frozenset[str] = frozenset(
    {"submitter", "executor", "bidirectional"}
)

REMOTE_LIFECYCLE_EVENTS: frozenset[str] = frozenset(
    {"claimed", "progress", "completed", "failed", "cancelled"}
)

REMOTE_TRANSPORTS: frozenset[str] = frozenset({"http", "websocket"})


class RemoteAuthConfig(ManifestModel):
    """Authentication configuration for a remote integration."""

    method: Literal["api_key", "bearer", "hmac"] = "api_key"
    header_name: str = Field(
        default="X-API-Key",
        min_length=1,
        max_length=128,
    )


class RemoteCorrelationConfig(ManifestModel):
    """Describes which correlation fields the integration uses."""

    local_id_field: str = Field(default="usr_submission_id")
    remote_id_field: str = Field(default="remote_job_id")
    submitter_id_field: str = Field(default="submitted_by")


class RemoteResponseMapping(ManifestModel):
    """How to extract correlation/status fields from remote responses."""

    external_job_id_path: str = Field(default="job.id", min_length=1)
    remote_status_path: str = Field(default="job.status", min_length=1)
    error_path: str | None = Field(default=None, min_length=1)


class DeclarativeValueMapping(ManifestModel):
    """One declarative mapping from context data to a target field."""

    source_path: str | None = Field(default=None, min_length=1)
    literal: Any = None

    @model_validator(mode="after")
    def _validate_exactly_one_source(self) -> DeclarativeValueMapping:
        provided = sum(
            1
            for field_name in ("source_path", "literal")
            if field_name in self.model_fields_set
        )
        if provided != 1:
            raise ValueError(
                "Declarative value mappings must set exactly one of "
                "'source_path' or 'literal'"
            )
        return self


_MAPPING_OBJECT_KEYS = frozenset({"source_path", "literal"})
_REQUEST_SOURCE_PREFIXES = ("payload.", "runtime.", "secret.")
_CALLBACK_SOURCE_PREFIXES = (
    "callback.body.",
    "callback.headers.",
    "runtime.",
    "secret.",
)


def _validate_source_path_prefix(
    source_path: str,
    *,
    allowed_prefixes: tuple[str, ...] | None,
    field_name: str,
) -> None:
    if allowed_prefixes is None:
        return
    if source_path in {"payload", "runtime", "secret", "callback", "callback.body", "callback.headers"}:
        return
    if any(source_path.startswith(prefix) for prefix in allowed_prefixes):
        return
    raise ValueError(
        f"{field_name} source_path {source_path!r} must start with one of "
        f"{sorted(allowed_prefixes)}"
    )


def _validate_mapping_tree(
    value: Any,
    *,
    allowed_prefixes: tuple[str, ...] | None,
    field_name: str,
) -> None:
    if isinstance(value, dict):
        keys = set(value.keys())
        if keys & _MAPPING_OBJECT_KEYS:
            if keys not in ({"source_path"}, {"literal"}):
                raise ValueError(
                    f"{field_name} mapping objects may only declare exactly one of "
                    "'source_path' or 'literal'"
                )
            if "source_path" in value:
                source_path = value["source_path"]
                if not isinstance(source_path, str) or not source_path:
                    raise ValueError(
                        f"{field_name} source_path must be a non-empty string"
                    )
                _validate_source_path_prefix(
                    source_path,
                    allowed_prefixes=allowed_prefixes,
                    field_name=field_name,
                )
            return
        for key, item in value.items():
            _validate_mapping_tree(
                item,
                allowed_prefixes=allowed_prefixes,
                field_name=f"{field_name}.{key}",
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_mapping_tree(
                item,
                allowed_prefixes=allowed_prefixes,
                field_name=f"{field_name}[{index}]",
            )
        return


class RemoteRequestMapping(ManifestModel):
    """Declarative request mapping for one remote HTTP operation."""

    path_params: dict[str, Any] = Field(default_factory=dict)
    query: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, Any] = Field(default_factory=dict)
    body: dict[str, Any] = Field(default_factory=dict)

    @field_validator("path_params", "query", "headers", "body")
    @classmethod
    def _validate_request_mapping(cls, v: dict[str, Any], info: Any) -> dict[str, Any]:
        _validate_mapping_tree(
            v,
            allowed_prefixes=_REQUEST_SOURCE_PREFIXES,
            field_name=f"request_mapping.{info.field_name}",
        )
        return v


class RemoteHTTPOperation(ManifestModel):
    """One HTTP operation in the remote delegation contract."""

    transport: Literal["http"] = "http"
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    endpoint: str = Field(..., min_length=1)
    auth_scope: str | None = Field(default=None, min_length=1)
    request_mapping: RemoteRequestMapping = Field(default_factory=RemoteRequestMapping)
    response_mapping: RemoteResponseMapping = Field(default_factory=RemoteResponseMapping)

    @field_validator("endpoint")
    @classmethod
    def _validate_http_endpoint(cls, v: str) -> str:
        if not v.startswith(("https://", "http://", "/")):
            raise ValueError(
                "operation endpoint values must start with https://, http://, or /"
            )
        return v


class RemoteOperationSet(ManifestModel):
    """Named remote HTTP operations."""

    submission: RemoteHTTPOperation | None = None
    claim: RemoteHTTPOperation | None = None
    heartbeat: RemoteHTTPOperation | None = None
    progress: RemoteHTTPOperation | None = None
    complete: RemoteHTTPOperation | None = None
    fail: RemoteHTTPOperation | None = None
    cancel: RemoteHTTPOperation | None = None
    poll: RemoteHTTPOperation | None = None


class RemoteTaskMatch(ManifestModel):
    """Declarative selector for an inbound remote work item."""

    job_type: str | None = Field(default=None, min_length=1, max_length=128)
    kind: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def _validate_match_selector(self) -> RemoteTaskMatch:
        if self.job_type is None and self.kind is None:
            raise ValueError(
                "remote_match must declare at least one of 'job_type' or 'kind'"
            )
        return self


class ExecutorTaskBinding(ManifestModel):
    """Maps one inbound work kind to one native USR task."""

    remote_match: RemoteTaskMatch
    local_task_type: str = Field(..., min_length=1, max_length=64)
    payload_mapping: dict[str, Any] = Field(default_factory=dict)
    defaults: dict[str, Any] = Field(default_factory=dict)
    overrides: dict[str, Any] = Field(default_factory=dict)
    task_alias: Any | None = None
    depends_on_aliases: Any | None = None

    @field_validator("local_task_type")
    @classmethod
    def _validate_local_task_type(cls, v: str) -> str:
        if not _TASK_TYPE_RE.match(v):
            raise ValueError(
                f"local_task_type must match {_TASK_TYPE_RE.pattern!r}, got {v!r}"
            )
        return v

    @field_validator("payload_mapping")
    @classmethod
    def _validate_payload_mapping(cls, v: dict[str, Any]) -> dict[str, Any]:
        _validate_mapping_tree(
            v,
            allowed_prefixes=None,
            field_name="executor_mapping.payload_mapping",
        )
        return v

    @model_validator(mode="after")
    def _validate_alias_mappings(self) -> ExecutorTaskBinding:
        for field_name in ("task_alias", "depends_on_aliases"):
            value = getattr(self, field_name)
            if value is None:
                continue
            _validate_mapping_tree(
                value,
                allowed_prefixes=None,
                field_name=f"executor_mapping.{field_name}",
            )
        return self


class ExecutorMapping(ManifestModel):
    """Declaratively translate remote work into native USR jobs/tasks."""

    remote_job_type_path: str = Field(default="job_type", min_length=1)
    remote_payload_path: str = Field(default="payload", min_length=1)
    remote_tasks_path: str | None = Field(default=None, min_length=1)
    local_job_name_template: str = Field(
        default="Remote {{ remote_job_type }}",
        min_length=1,
        max_length=256,
    )
    task_bindings: list[ExecutorTaskBinding] = Field(default_factory=list)


class AuthCredentialExportConfig(ManifestModel):
    """Resolve a selected local auth into a normalized remote auth bundle."""

    site_path: str = Field(default="payload.site", min_length=1)
    auth_id_path: str = Field(default="payload.auth_id", min_length=1)
    runtime_key: str = Field(default="auth_bundle", min_length=1, max_length=64)

    @field_validator("site_path", "auth_id_path")
    @classmethod
    def _validate_export_paths(cls, v: str, info: Any) -> str:
        _validate_source_path_prefix(
            v,
            allowed_prefixes=("payload.", "runtime."),
            field_name=f"auth_export.{info.field_name}",
        )
        return v

    @field_validator("runtime_key")
    @classmethod
    def _validate_runtime_key(cls, v: str) -> str:
        if not _RUNTIME_KEY_RE.match(v):
            raise ValueError(
                f"runtime_key must match {_RUNTIME_KEY_RE.pattern!r}, got {v!r}"
            )
        return v


class AuthCredentialImportConfig(ManifestModel):
    """Import a claimed remote auth bundle into the local auth/session store."""

    bundle_path: str = Field(default="payload.auth_bundle", min_length=1)
    site_path: str = Field(default="payload.site", min_length=1)
    username_path: str | None = Field(
        default="payload.auth_bundle.username",
        min_length=1,
    )
    user_id_path: str | None = Field(default="payload.auth_bundle.id", min_length=1)
    runtime_key: str = Field(default="imported_auth_id", min_length=1, max_length=64)

    @field_validator("runtime_key")
    @classmethod
    def _validate_runtime_import_key(cls, v: str) -> str:
        if not _RUNTIME_KEY_RE.match(v):
            raise ValueError(
                f"runtime_key must match {_RUNTIME_KEY_RE.pattern!r}, got {v!r}"
            )
        return v


class CallbackRegistration(ManifestModel):
    """Declarative auto-registration of callback/webhook endpoints."""

    endpoint: str = Field(default="", min_length=0)
    method: Literal["POST", "PUT"] = "POST"
    auth_scope: str | None = Field(default=None, min_length=1)
    lifecycle_events: list[str] = Field(default_factory=list)
    generated_callback_url_source: Literal["platform_callback_url"] = (
        "platform_callback_url"
    )
    generated_secret_source: Literal["callback_secret"] = "callback_secret"
    payload_mapping: dict[str, Any] = Field(default_factory=dict)

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, v: str) -> str:
        if v and not v.startswith(("https://", "http://", "/")):
            raise ValueError(
                "callback registration endpoint must start with https://, "
                "http://, or /"
            )
        return v

    @field_validator("lifecycle_events")
    @classmethod
    def _validate_events(cls, v: list[str]) -> list[str]:
        for evt in v:
            if evt not in REMOTE_LIFECYCLE_EVENTS:
                raise ValueError(
                    f"lifecycle_event must be one of "
                    f"{sorted(REMOTE_LIFECYCLE_EVENTS)}, got {evt!r}"
                )
        return v

    @field_validator("payload_mapping")
    @classmethod
    def _validate_payload_mapping(cls, v: dict[str, Any]) -> dict[str, Any]:
        _validate_mapping_tree(
            v,
            allowed_prefixes=None,
            field_name="callback_registration.payload_mapping",
        )
        return v


class InboundCallbackExtraction(ManifestModel):
    """How to normalize one inbound callback payload."""

    lifecycle_event: Any
    correlation_id: Any | None = None
    external_job_id: Any | None = None
    remote_status: Any | None = None
    progress: Any | None = None
    result: Any | None = None
    error: Any | None = None

    @model_validator(mode="after")
    def _validate_extract_mappings(self) -> InboundCallbackExtraction:
        for field_name in (
            "lifecycle_event",
            "correlation_id",
            "external_job_id",
            "remote_status",
            "progress",
            "result",
            "error",
        ):
            value = getattr(self, field_name)
            if value is None:
                continue
            _validate_mapping_tree(
                value,
                allowed_prefixes=_CALLBACK_SOURCE_PREFIXES,
                field_name=f"callbacks.inbound.extract.{field_name}",
            )
        return self


class InboundCallbackMapping(ManifestModel):
    """One inbound callback handler mapping."""

    handler_id: str = Field(..., min_length=3, max_length=64)
    extract: InboundCallbackExtraction

    @field_validator("handler_id")
    @classmethod
    def _validate_handler_id(cls, v: str) -> str:
        if not _HANDLER_ID_RE.match(v):
            raise ValueError(
                f"handler_id must match {_HANDLER_ID_RE.pattern!r}, got {v!r}"
            )
        return v


class RemoteCallbackConfig(ManifestModel):
    """Inbound callback mappings for remote delegation."""

    inbound: list[InboundCallbackMapping] = Field(default_factory=list)


class RemoteWebSocketReconnectPolicy(ManifestModel):
    """Reconnect policy for outbound remote websocket sessions."""

    initial_backoff_seconds: int = Field(default=1, ge=1, le=300)
    max_backoff_seconds: int = Field(default=60, ge=1, le=3600)
    max_retries: int = Field(default=0, ge=0, le=10000)


class RemoteWebSocketEnvelopeMapping(ManifestModel):
    """Field names used when serializing/deserializing WS envelopes."""

    message_id_field: str = Field(default="message_id", min_length=1)
    message_type_field: str = Field(default="message_type", min_length=1)
    plugin_id_field: str = Field(default="plugin_id", min_length=1)
    correlation_id_field: str = Field(default="correlation_id", min_length=1)
    external_job_id_field: str = Field(default="external_job_id", min_length=1)
    action_field: str = Field(default="action", min_length=1)
    payload_field: str = Field(default="payload", min_length=1)
    timestamp_field: str = Field(default="timestamp", min_length=1)


class RemoteWebSocketConfig(ManifestModel):
    """Config for a full-duplex remote websocket session."""

    url: str = Field(..., min_length=1)
    auth: RemoteAuthConfig = Field(default_factory=RemoteAuthConfig)
    reconnect_policy: RemoteWebSocketReconnectPolicy = Field(
        default_factory=RemoteWebSocketReconnectPolicy
    )
    heartbeat_interval_seconds: int = Field(default=30, ge=1, le=3600)
    message_envelope_mapping: RemoteWebSocketEnvelopeMapping = Field(
        default_factory=RemoteWebSocketEnvelopeMapping
    )

    @field_validator("url")
    @classmethod
    def _validate_websocket_url(cls, v: str) -> str:
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("websocket.url must start with ws:// or wss://")
        return v


class RemoteDelegation(ManifestModel):
    """Configuration for delegated remote execution.

    Describes how a plugin integrates with an external control plane
    for remote job submission, execution callbacks, or both.
    """

    integration_id: str = Field(..., min_length=3, max_length=64)
    role: Literal["submitter", "executor", "bidirectional"]
    description: str = ""
    external_system_url: str = Field(default="", min_length=0)
    external_system_url_setting: str | None = None
    auth: RemoteAuthConfig = Field(default_factory=RemoteAuthConfig)
    auth_scopes: dict[str, RemoteAuthConfig] = Field(default_factory=dict)
    correlation: RemoteCorrelationConfig = Field(
        default_factory=RemoteCorrelationConfig,
    )
    operations: RemoteOperationSet = Field(default_factory=RemoteOperationSet)
    auth_export: AuthCredentialExportConfig | None = None
    auth_import: AuthCredentialImportConfig | None = None
    executor_mapping: ExecutorMapping | None = None
    callback_registration: CallbackRegistration | None = None
    websocket: RemoteWebSocketConfig | None = None
    callbacks: RemoteCallbackConfig = Field(default_factory=RemoteCallbackConfig)

    @field_validator("integration_id")
    @classmethod
    def _validate_integration_id(cls, v: str) -> str:
        if not _INTEGRATION_ID_RE.match(v):
            raise ValueError(
                f"integration_id must match {_INTEGRATION_ID_RE.pattern!r}, got {v!r}"
            )
        return v

    @field_validator("external_system_url")
    @classmethod
    def _validate_external_url(cls, v: str) -> str:
        if v and not v.startswith(("https://", "http://")):
            raise ValueError(
                "external_system_url must start with https:// or http://"
            )
        return v

    @model_validator(mode="after")
    def _validate_role_requirements(self) -> RemoteDelegation:
        role = self.role
        declared_auth_scopes = set(self.auth_scopes.keys())

        for scope_name in declared_auth_scopes:
            if not _INTEGRATION_ID_RE.match(scope_name):
                raise ValueError(
                    f"auth scope names must match {_INTEGRATION_ID_RE.pattern!r}, got {scope_name!r}"
                )

        for operation_name in (
            "submission",
            "claim",
            "heartbeat",
            "progress",
            "complete",
            "fail",
            "cancel",
            "poll",
        ):
            operation = getattr(self.operations, operation_name)
            if operation is None or operation.auth_scope is None:
                continue
            if operation.auth_scope not in declared_auth_scopes:
                raise ValueError(
                    f"operations.{operation_name}.auth_scope references unknown auth scope "
                    f"{operation.auth_scope!r}; declared: {sorted(declared_auth_scopes)}"
                )
        if (
            self.callback_registration is not None
            and self.callback_registration.auth_scope is not None
            and self.callback_registration.auth_scope not in declared_auth_scopes
        ):
            raise ValueError(
                "callback_registration.auth_scope references unknown auth scope "
                f"{self.callback_registration.auth_scope!r}; declared: {sorted(declared_auth_scopes)}"
            )

        if role in ("submitter", "bidirectional"):
            if not self.external_system_url:
                raise ValueError(
                    f"Role {role!r} requires external_system_url"
                )
            if self.operations.submission is None:
                raise ValueError(
                    f"Role {role!r} requires operations.submission"
                )
        elif self.auth_export is not None:
            raise ValueError(
                "auth_export requires a submitter or bidirectional remote_delegation role"
            )
        if role in ("executor", "bidirectional"):
            if self.operations.claim is None:
                raise ValueError(
                    f"Role {role!r} requires operations.claim"
                )
            if not self.callbacks.inbound:
                raise ValueError(
                    f"Role {role!r} requires at least one callbacks.inbound mapping"
                )
        elif self.auth_import is not None:
            raise ValueError(
                "auth_import requires an executor or bidirectional remote_delegation role"
            )
        if (
            self.callback_registration is not None
            and not self.callback_registration.endpoint
        ):
            raise ValueError(
                "callback_registration must declare an endpoint when configured"
            )
        return self


# ── Operational policy ────────────────────────────────────────────────────

class DeliveryRetryPolicy(ManifestModel):
    """Retry policy for outbound event deliveries."""

    max_retries: int = Field(default=10, ge=0, le=100)
    initial_backoff_seconds: int = Field(default=5, ge=1, le=300)
    max_backoff_seconds: int = Field(default=3600, ge=1, le=86400)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)


class OperationalPolicy(ManifestModel):
    """Plugin operational policy."""

    retry_policy: DeliveryRetryPolicy = Field(
        default_factory=DeliveryRetryPolicy,
    )
    max_pending_deliveries: int = Field(default=1000, ge=1, le=100000)
    delivery_concurrency: int = Field(default=4, ge=1, le=16)
    task_concurrency: int = Field(default=2, ge=1, le=8)
    state_quota_bytes: int = Field(
        default=65536,
        ge=0,
        le=1_048_576,
        description="Maximum total size in bytes for plugin state KV store.",
    )
    replay_on_enable: bool = Field(
        default=True,
        description=(
            "Replay retained ledger events on first enable. Existing cursors "
            "always resume after restart; disabling pauses consumption."
        ),
    )
    allow_private_networks: bool = Field(
        default=False,
        description=(
            "Allow outbound HTTP and websocket targets that resolve to private, "
            "loopback, link-local, or otherwise non-public addresses."
        ),
    )
    allow_insecure_http: bool = Field(
        default=False,
        description=(
            "Allow cleartext http:// and ws:// outbound targets. Keep disabled "
            "unless a trusted local integration cannot provide TLS."
        ),
    )
    allowed_outbound_domains: list[str] = Field(
        default_factory=list,
        description=(
            "Optional exact-domain and subdomain allowlist for outbound targets. "
            "An empty list permits any domain that passes IP safety checks."
        ),
    )

    @field_validator("allowed_outbound_domains")
    @classmethod
    def _validate_allowed_outbound_domains(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            domain = value.strip().lower().rstrip(".")
            if (
                not domain
                or "://" in domain
                or "/" in domain
                or "*" in domain
                or any(part == "" for part in domain.split("."))
                or _DOMAIN_RE.fullmatch(domain) is None
            ):
                raise ValueError(
                    "allowed_outbound_domains entries must be plain domain names"
                )
            if domain not in normalized:
                normalized.append(domain)
        return normalized


# ── WebUI extension ───────────────────────────────────────────────────────

class PluginWebUI(ManifestModel):
    """Declarative WebUI extension configuration."""

    mode: Literal["native", "sandbox"] = "native"
    entry: str = Field(
        default="",
        max_length=255,
        description="Path to JavaScript entry file relative to plugin folder.",
    )
    styles: list[str] = Field(
        default_factory=list,
        description="Optional CSS stylesheet files relative to plugin folder.",
    )


# ── Top-level manifest ────────────────────────────────────────────────────

class PluginManifest(ManifestModel):
    """The complete plugin manifest — one YAML file per plugin."""

    # Identity
    manifest_schema_version: Literal[1, 2] = 1
    sdk: SDKContract | None = None
    configuration_schema: dict[str, Any] | None = None
    plugin_id: str = Field(..., min_length=3, max_length=64)
    plugin_version: str = Field(..., min_length=5, max_length=32)
    display_name: str = Field(..., min_length=1, max_length=128)
    description: str = ""
    usr_compatibility: str = Field(
        default=">=0.1.0",
        description="SemVer range the plugin targets.",
    )

    # Capabilities
    capabilities: list[PluginCapability] = Field(default_factory=list)

    # Task types
    task_types: list[PluginTaskType] = Field(
        default_factory=list,
        validation_alias=AliasChoices("task_types", "job_types"),
        serialization_alias="task_types",
    )

    # Event subscriptions
    event_subscriptions: list[EventSubscription] = Field(default_factory=list)

    # Inbound callbacks
    callbacks: list[CallbackEndpoint] = Field(default_factory=list)

    # Outbound delivery targets
    outbound_deliveries: list[OutboundDeliveryTarget] = Field(default_factory=list)

    # Operational policy
    operational_policy: OperationalPolicy = Field(
        default_factory=OperationalPolicy,
    )

    # Remote delegation (optional)
    remote_delegation: RemoteDelegation | None = Field(default=None)

    # WebUI extension (optional)
    webui: PluginWebUI | None = Field(default=None)

    # ── Validators ────────────────────────────────────────────────────────

    @field_validator("configuration_schema")
    @classmethod
    def _validate_configuration_schema(cls, value):
        return SDKContract.valid_schema(value) if value is not None else None

    @field_validator("plugin_id")
    @classmethod
    def _validate_plugin_id(cls, v: str) -> str:
        if not _PLUGIN_ID_RE.match(v):
            raise ValueError(
                f"plugin_id must match {_PLUGIN_ID_RE.pattern!r}, got {v!r}"
            )
        return v

    @field_validator("plugin_version")
    @classmethod
    def _validate_version(cls, v: str) -> str:
        if not _SEMVER_RE.match(v):
            raise ValueError(
                f"plugin_version must be valid semver, got {v!r}"
            )
        return v

    @field_validator("usr_compatibility")
    @classmethod
    def _validate_usr_compatibility(cls, v: str) -> str:
        try:
            SpecifierSet(v)
        except InvalidSpecifier as exc:
            raise ValueError(f"usr_compatibility must be a valid version range: {v!r}") from exc
        return v

    @model_validator(mode="after")
    def _cross_validate(self) -> PluginManifest:
        """Ensure referential integrity across manifest sections."""
        # Every event subscription must reference an existing delivery target
        target_names = {t.target_name for t in self.outbound_deliveries}
        for sub in self.event_subscriptions:
            if sub.delivery_target not in target_names:
                raise ValueError(
                    f"Event subscription references unknown delivery target "
                    f"{sub.delivery_target!r}; available: {sorted(target_names)}"
                )

        # Capability consistency checks
        cap_values = {c.value for c in self.capabilities}
        schema = self.sdk.configuration_schema if self.sdk else self.configuration_schema
        settings = (schema or {}).get("properties", {})
        references = [target.url_setting for target in self.outbound_deliveries]
        if self.remote_delegation:
            references.append(self.remote_delegation.external_system_url_setting)
        for reference in references:
            if reference and reference not in settings:
                raise ValueError(f"URL setting {reference!r} must name a configuration property")
        if self.manifest_schema_version == 2 and self.webui and self.webui.mode == "native" and "webui.native" not in cap_values:
            raise ValueError("Native SDK UI requires explicit webui.native permission")
        if self.manifest_schema_version == 2 and self.sdk is None:
            raise ValueError("Manifest schema v2 requires an SDK contract")
        if self.sdk:
            declared = {task.task_type for task in self.task_types}
            services = {service.service_id: service for service in self.sdk.http_services}
            if len(services) != len(self.sdk.http_services):
                raise ValueError("Duplicate HTTP service ids")
            if set(self.sdk.access.http_services) != set(services):
                raise ValueError("HTTP services must have matching access scopes")
            if services and "http.request" not in cap_values:
                raise ValueError("HTTP services require http.request")
            if self.sdk.access.proxy_sources and "proxies.sync" not in cap_values:
                raise ValueError("Proxy sources require proxies.sync")
            credentials = {item.name for item in self.sdk.setup.credentials} if self.sdk.setup else set()
            if self.sdk.setup and len(credentials) != len(self.sdk.setup.credentials):
                raise ValueError("Duplicate setup credential names")
            for service in services.values():
                if service.credential and service.credential not in credentials:
                    raise ValueError("HTTP service references an undeclared setup credential")
            if self.sdk.setup and self.sdk.setup.validation_service:
                service = services.get(self.sdk.setup.validation_service)
                if service is None:
                    raise ValueError("Setup validation references an unknown HTTP service")
                from .sdk import validate_schema
                validate_schema(service.query_schema, self.sdk.setup.validation_query, name="validation query")
            schedule_ids = [item.schedule_id for item in self.sdk.schedules]
            if len(schedule_ids) != len(set(schedule_ids)):
                raise ValueError("Duplicate schedule ids")
            for schedule in self.sdk.schedules:
                if schedule.task_type not in self.sdk.access.task_types:
                    raise ValueError("Scheduled tasks require approved task access")
                field = settings.get(schedule.interval_setting, {})
                if field.get("type") != "integer" or field.get("minimum", 0) < 60 or field.get("maximum", float("inf")) > 604800:
                    raise ValueError("Schedule intervals require an integer setting bounded from 60 to 604800 seconds")
            if self.sdk.schedules and "jobs.schedule" not in cap_values:
                raise ValueError("Scheduled tasks require jobs.schedule")
            if not set(self.sdk.access.task_types) <= declared:
                raise ValueError("Task access scopes must reference declared task types")
            ids = [item.contribution_id for item in self.sdk.ui]
            if len(ids) != len(set(ids)):
                raise ValueError("Duplicate UI contribution ids")
            for contribution in self.sdk.ui:
                if contribution.task_type and contribution.task_type not in declared:
                    raise ValueError("UI contribution references an unknown task type")
            if self.sdk.ui and "webui.extend" not in cap_values:
                raise ValueError("UI contributions require the webui.extend capability")
            if self.sdk.access.profile_fields and "profiles.read" not in cap_values:
                raise ValueError("Profile field access requires profiles.read")
            if self.sdk.access.propose_profile_changes and "profiles.propose" not in cap_values:
                raise ValueError("Profile proposals require profiles.propose")
            if self.sdk.access.state_keys and not ({"state.read", "state.write"} & cap_values):
                raise ValueError("State key access requires state.read or state.write")
            if self.manifest_schema_version == 2:
                for task in self.task_types:
                    if not task.task_type.startswith(self.plugin_id + ".") or task.handler is None:
                        raise ValueError("SDK task types require a plugin namespace and an execution handler")
        if any(task.handler and task.handler.kind == "process" for task in self.task_types) and "runtime.process" not in cap_values:
            raise ValueError("Process adapters require runtime.process")
        if self.task_types and "jobs.submit" not in cap_values:
            raise ValueError(
                "Plugin declares task_types but does not request 'jobs.submit' capability"
            )
        if self.event_subscriptions and "events.read" not in cap_values:
            raise ValueError(
                "Plugin declares event_subscriptions but does not request "
                "'events.read' capability"
            )
        if self.callbacks and "webhooks.register" not in cap_values:
            raise ValueError(
                "Plugin declares callbacks but does not request "
                "'webhooks.register' capability"
            )

        # Handler id uniqueness
        handler_ids = [cb.handler_id for cb in self.callbacks]
        if len(handler_ids) != len(set(handler_ids)):
            raise ValueError("Duplicate callback handler_id values in manifest")

        # Target name uniqueness
        delivery_names = [t.target_name for t in self.outbound_deliveries]
        if len(delivery_names) != len(set(delivery_names)):
            raise ValueError("Duplicate outbound delivery target_name values in manifest")

        # Task type uniqueness
        task_type_names = [jt.task_type for jt in self.task_types]
        if len(task_type_names) != len(set(task_type_names)):
            raise ValueError("Duplicate task_type values in manifest")

        # Remote delegation capability consistency
        if self.remote_delegation is not None:
            rd = self.remote_delegation
            if rd.role in ("submitter", "bidirectional"):
                if "remote.jobs.submit" not in cap_values:
                    raise ValueError(
                        "Plugin declares remote_delegation with submitter/bidirectional "
                        "role but does not request 'remote.jobs.submit' capability"
                    )
                if (
                    rd.auth_export is not None
                    and PluginCapability.AUTH_CREDENTIALS_EXPORT.value not in cap_values
                ):
                    raise ValueError(
                        "Plugin declares remote_delegation.auth_export but does not request "
                        "'auth.credentials.export' capability"
                    )
            if rd.role in ("executor", "bidirectional"):
                if "remote.jobs.execute" not in cap_values:
                    raise ValueError(
                        "Plugin declares remote_delegation with executor/bidirectional "
                        "role but does not request 'remote.jobs.execute' capability"
                    )
                if "remote.callbacks.receive" not in cap_values:
                    raise ValueError(
                        "Plugin declares remote_delegation with executor/bidirectional "
                        "role but does not request 'remote.callbacks.receive' capability"
                    )
                if (
                    rd.auth_import is not None
                    and PluginCapability.AUTH_CREDENTIALS_IMPORT.value not in cap_values
                ):
                    raise ValueError(
                        "Plugin declares remote_delegation.auth_import but does not request "
                        "'auth.credentials.import' capability"
                    )
            # Validate inbound callback mappings reference declared callback handler_ids
            declared_handlers = {cb.handler_id for cb in self.callbacks}
            for mapping in rd.callbacks.inbound:
                if mapping.handler_id not in declared_handlers:
                    raise ValueError(
                        f"Remote inbound callback mapping references unknown handler_id "
                        f"{mapping.handler_id!r}; declared: {sorted(declared_handlers)}"
                    )

        return self

    @property
    def job_types(self) -> list[PluginTaskType]:
        """Compatibility alias for older callers."""
        return self.task_types


def load_manifest_from_dict(data: dict[str, Any]) -> PluginManifest:
    """Parse and validate a manifest from a raw dictionary (e.g. YAML-loaded)."""
    return PluginManifest.model_validate(data)
