"""Versioned plugin contracts shared by runtimes, administration, and UI."""

from __future__ import annotations

import re
from typing import Any, Literal

from packaging.specifiers import SpecifierSet
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SDK_VERSION = "1.1.0"
SLOTS = {
    "users.card.badges": ("item",),
    "users.table.identity_extra": ("user", "onEditUser"),
    "users.edit_modal.top": ("user", "formValues", "setFieldValue"),
    "user.profile.panels": ("user",),
    "navigation.pages": (),
}


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")


def validate_schema(schema: dict[str, Any], value: Any, *, name="payload") -> None:
    from jsonschema import Draft202012Validator, FormatChecker

    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(value), key=lambda error: str(error.path))
    if errors:
        error = errors[0]
        path = ".".join(str(part) for part in error.path)
        raise ValueError(f"Invalid {name}{'.' + path if path else ''}: {error.message}")


class AccessScopes(Contract):
    profile_fields: list[
        Literal["name", "username", "description", "site", "user_id"]
    ] = Field(default_factory=list)
    task_types: list[str] = Field(default_factory=list)
    outbound_domains: list[str] = Field(default_factory=list)
    state_keys: list[str] = Field(default_factory=list, max_length=128)
    http_services: list[str] = Field(default_factory=list)
    proxy_sources: list[str] = Field(default_factory=list)
    propose_profile_changes: bool = False

    @field_validator("outbound_domains")
    @classmethod
    def domains(cls, values):
        domains = [value.strip().lower().rstrip(".") for value in values]
        if any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", value)
            or ".." in value
            for value in domains
        ):
            raise ValueError("Outbound scopes must be plain domain names")
        return sorted(set(domains))

    @field_validator("state_keys")
    @classmethod
    def keys(cls, values):
        if any(not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", value) for value in values):
            raise ValueError("State scopes must name exact state keys")
        return sorted(set(values))


class TaskHandler(Contract):
    kind: Literal["http", "process"] = "http"
    url: str | None = Field(default=None, max_length=2048)
    auth_scope: str | None = None
    command: list[str] = Field(default_factory=list, max_length=32)
    timeout_seconds: int = Field(default=60, ge=1, le=3600)
    response_limit_bytes: int = Field(default=1_048_576, ge=1024, le=16_777_216)

    @model_validator(mode="after")
    def check_transport(self):
        if self.kind == "http" and (not self.url or self.command):
            raise ValueError("HTTP handlers require a URL and cannot declare a command")
        if self.kind == "process" and (not self.command or self.url):
            raise ValueError(
                "Process handlers require a command and cannot declare a URL"
            )
        return self


class UIContribution(Contract):
    contribution_id: str = Field(
        min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$"
    )
    slot: str
    kind: Literal["badge", "action", "panel", "page"]
    title: str = Field(min_length=1, max_length=128)
    text: str = Field(default="", max_length=4096)
    order: int = Field(default=0, ge=-1000, le=1000)
    task_type: str | None = None

    @field_validator("slot")
    @classmethod
    def valid_slot(cls, value):
        if value not in SLOTS:
            raise ValueError(f"Unknown UI slot {value!r}")
        return value


class HTTPService(Contract):
    service_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")
    url: str = Field(max_length=2048)
    credential: str | None = None
    auth_scheme: Literal["Token", "Bearer"] = "Bearer"
    query_schema: dict[str, Any] = Field(
        default_factory=lambda: {"type": "object", "additionalProperties": False}
    )
    timeout_seconds: int = Field(default=30, ge=1, le=60)
    response_limit_bytes: int = Field(default=1_048_576, ge=1024, le=2_097_152)

    @field_validator("url")
    @classmethod
    def fixed_url(cls, value):
        from urllib.parse import urlsplit

        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
        ):
            raise ValueError(
                "HTTP services require a fixed HTTPS URL without credentials, query or fragment"
            )
        return value

    @field_validator("query_schema")
    @classmethod
    def query_contract(cls, value):
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(value)
        reject_external_refs(value)
        return value


class SetupCredential(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")
    title: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=1024)
    required: bool = True


class PluginSetup(Contract):
    credentials: list[SetupCredential] = Field(default_factory=list)
    validation_service: str | None = None
    validation_query: dict[str, Any] = Field(default_factory=dict)


class ScheduledTask(Contract):
    schedule_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")
    task_type: str
    interval_setting: str


class SDKContract(Contract):
    compatibility: str = ">=1.0.0,<2"
    configuration_schema: dict[str, Any] = Field(
        default_factory=lambda: {"type": "object", "additionalProperties": False}
    )
    access: AccessScopes = Field(default_factory=AccessScopes)
    ui: list[UIContribution] = Field(default_factory=list)
    setup: PluginSetup | None = None
    http_services: list[HTTPService] = Field(default_factory=list)
    schedules: list[ScheduledTask] = Field(default_factory=list)

    @field_validator("compatibility")
    @classmethod
    def valid_compatibility(cls, value):
        SpecifierSet(value)
        return value

    @field_validator("configuration_schema")
    @classmethod
    def valid_schema(cls, value):
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(value)
        # Runtime contracts cannot fetch schemas or transmit data over a $ref.
        reject_external_refs(value)
        return value


def reject_external_refs(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if (
                key in {"$ref", "$dynamicRef"}
                and isinstance(item, str)
                and not item.startswith("#")
            ):
                raise ValueError("Plugin schemas may only use local references")
            reject_external_refs(item)
    elif isinstance(value, list):
        for item in value:
            reject_external_refs(item)
