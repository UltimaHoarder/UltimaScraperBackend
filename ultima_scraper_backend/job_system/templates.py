"""Job template management for the UltimaScraper job system."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, cast

from ultima_scraper_api import ContentType

from ultima_scraper_backend.redis_channels import KEY_PREFIX

from ..redis.client import RedisManager
from .models import Task, TaskType

_TEMPLATE_SET_KEY = f"{KEY_PREFIX}:job_templates"
_TEMPLATE_PREFIX = f"{KEY_PREFIX}:job_template:"
_TEMPLATE_NAME_PREFIX = f"{KEY_PREFIX}:job_template:name:"

ParameterDict = Mapping[str, Any]

_TEMPLATE_PAYLOAD_METADATA_KEYS = frozenset(
    {"dedupe_override", "template_content_type", "template_task"}
)


def _default_str_list() -> List[str]:
    return []


def _default_any_dict() -> Dict[str, Any]:
    return {}


def _default_payload() -> Dict[str, Any]:
    return {}


def _default_parameter_list() -> List["TemplateParameterDefinition"]:
    return []


def _strip_template_payload_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[str, Any], value)
        return {
            str(key): _strip_template_payload_metadata(item)
            for key, item in mapping.items()
            if str(key) not in _TEMPLATE_PAYLOAD_METADATA_KEYS
        }
    if isinstance(value, list):
        sequence = cast(List[Any], value)
        return [_strip_template_payload_metadata(item) for item in sequence]
    return value


def _normalize_template_content_type(
    value: str | ContentType | None,
) -> str | ContentType | None:
    if value in (None, "", "None"):
        return None
    if isinstance(value, ContentType):
        return value

    trimmed = str(value).strip()
    compact = trimmed.replace(" ", "").replace("_", "").replace("-", "").lower()
    for content_type in ContentType:
        normalized_value = (
            str(content_type.value)
            .replace(" ", "")
            .replace("_", "")
            .replace("-", "")
            .lower()
        )
        if compact == normalized_value:
            return content_type

    return trimmed


@dataclass
class RetryPolicy:
    attempts: int = 3
    backoff_seconds: float = 5.0
    max_backoff_seconds: float = 60.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempts": self.attempts,
            "backoff_seconds": self.backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "RetryPolicy":
        if not data:
            return cls()
        return cls(
            attempts=int(data.get("attempts", 3)),
            backoff_seconds=float(data.get("backoff_seconds", 5.0)),
            max_backoff_seconds=float(data.get("max_backoff_seconds", 60.0)),
        )


@dataclass
class BackendStrategy:
    mode: str = "auto"  # auto, fixed, prompt
    backend_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "backend_id": self.backend_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "BackendStrategy":
        if not data:
            return cls()
        return cls(
            mode=str(data.get("mode", "auto")), backend_id=data.get("backend_id")
        )


@dataclass
class TemplateParameterDefinition:
    name: str
    type: str = "string"  # string, int, float, bool, datetime, json
    required: bool = True
    default: Any = None
    description: str = ""
    choices: Optional[List[Any]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "required": self.required,
            "default": self.default,
            "description": self.description,
            "choices": self.choices,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TemplateParameterDefinition":
        return cls(
            name=str(data["name"]),
            type=str(data.get("type", "string")),
            required=bool(data.get("required", True)),
            default=data.get("default"),
            description=str(data.get("description", "")),
            choices=list(data["choices"]) if data.get("choices") is not None else None,
        )

    def coerce(self, provided: Any) -> Any:
        value = provided
        if value is None:
            value = self.default
        if value is None and self.required:
            raise ValueError(f"Parameter '{self.name}' is required")
        if value is None:
            return None
        try:
            if self.type == "string":
                return str(value)
            if self.type == "int":
                return int(value)
            if self.type == "float":
                return float(value)
            if self.type == "bool":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"1", "true", "yes", "on"}:
                        return True
                    if lowered in {"0", "false", "no", "off"}:
                        return False
                    raise ValueError
                return bool(value)
            if self.type == "datetime":
                if isinstance(value, datetime):
                    return value.isoformat()
                return str(value)
            if self.type == "json":
                if isinstance(value, str):
                    return json.loads(value)
                return value
        except (
            ValueError,
            TypeError,
            json.JSONDecodeError,
        ) as exc:  # pragma: no cover - defensive
            raise ValueError(
                f"Invalid value for parameter '{self.name}': {value!r}"
            ) from exc
        if self.choices is not None and value not in self.choices:
            raise ValueError(
                f"Parameter '{self.name}' must be one of {self.choices}, got {value!r}"
            )
        return value


@dataclass
class JobTemplateTaskDefinition:
    name: Optional[str]
    task_type: TaskType
    site_name_template: str
    user_id_template: str
    content_type_template: str
    user_username_template: Optional[str] = None
    auth_id_template: Optional[str] = None
    payload_template: Mapping[str, Any] = field(default_factory=_default_payload)
    capabilities: List[str] = field(default_factory=_default_str_list)
    depends_on: List[str] = field(default_factory=_default_str_list)
    ip_lock_key_template: Optional[str] = None
    max_attempts: Optional[int] = None
    dedupe_override: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "task_type": self.task_type.value,
            "site_name_template": self.site_name_template,
            "user_id_template": self.user_id_template,
            "user_username_template": self.user_username_template,
            "content_type_template": self.content_type_template,
            "auth_id_template": self.auth_id_template,
            "payload_template": self._serialize_payload(self.payload_template),
            "capabilities": list(self.capabilities),
            "depends_on": list(self.depends_on),
            "ip_lock_key_template": self.ip_lock_key_template,
            "max_attempts": self.max_attempts,
            "dedupe_override": self.dedupe_override,
        }

    @staticmethod
    def _serialize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
        def _convert(obj: Any) -> Any:
            if isinstance(obj, Mapping):
                mapping = cast(Mapping[str, Any], obj)
                return {str(key): _convert(value) for key, value in mapping.items()}
            if isinstance(obj, list):
                sequence = cast(List[Any], obj)
                return [_convert(item) for item in sequence]
            return obj

        converted = _convert(payload)
        return cast(dict[str, Any], _strip_template_payload_metadata(converted))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "JobTemplateTaskDefinition":
        return cls(
            name=data.get("name"),
            task_type=TaskType(data["task_type"]),
            site_name_template=str(data.get("site_name_template", "")),
            user_id_template=str(data["user_id_template"]),
            user_username_template=(
                str(data["user_username_template"])
                if data.get("user_username_template") is not None
                else None
            ),
            content_type_template=str(data["content_type_template"]),
            auth_id_template=(
                str(data["auth_id_template"])
                if data.get("auth_id_template") is not None
                else None
            ),
            payload_template=data.get("payload_template", {}),
            capabilities=list(data.get("capabilities", [])),
            depends_on=list(data.get("depends_on", [])),
            ip_lock_key_template=data.get("ip_lock_key_template"),
            max_attempts=data.get("max_attempts"),
            dedupe_override=bool(data.get("dedupe_override", False)),
        )


@dataclass
class TemplateOverrides:
    priority: Optional[int] = None
    backend_strategy: Optional[BackendStrategy] = None
    retry_policy: Optional[RetryPolicy] = None
    schedule_at: Optional[str] = None
    job_name: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


@dataclass
class JobTemplateInstantiation:
    template_id: str
    template_name: str
    job_name: str
    tasks: List[Task]
    priority: int
    retry_policy: RetryPolicy
    backend_strategy: BackendStrategy
    schedule_at: Optional[str]
    metadata: dict[str, Any]
    parameter_values: dict[str, Any]


@dataclass
class JobTemplate:
    name: str
    description: str
    created_by: str
    tasks: List[JobTemplateTaskDefinition]
    parameters: List[TemplateParameterDefinition] = field(
        default_factory=_default_parameter_list
    )
    template_id: str = field(
        default_factory=lambda: f"template-{uuid.uuid4().hex[:10]}"
    )
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    default_priority: int = 0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    backend_strategy: BackendStrategy = field(default_factory=BackendStrategy)
    job_name_template: Optional[str] = None
    default_schedule_at: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=_default_any_dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "description": self.description,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "default_priority": self.default_priority,
            "retry_policy": self.retry_policy.to_dict(),
            "backend_strategy": self.backend_strategy.to_dict(),
            "job_name_template": self.job_name_template,
            "default_schedule_at": self.default_schedule_at,
            "metadata": self.metadata,
            "parameters": [param.to_dict() for param in self.parameters],
            "tasks": [task.to_dict() for task in self.tasks],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "JobTemplate":
        return cls(
            template_id=str(data["template_id"]),
            name=str(data["name"]),
            description=str(data.get("description", "")),
            created_by=str(data.get("created_by", "system")),
            created_at=str(
                data.get("created_at", datetime.now(timezone.utc).isoformat())
            ),
            updated_at=str(
                data.get("updated_at", datetime.now(timezone.utc).isoformat())
            ),
            default_priority=int(data.get("default_priority", 0)),
            retry_policy=RetryPolicy.from_dict(data.get("retry_policy")),
            backend_strategy=BackendStrategy.from_dict(data.get("backend_strategy")),
            job_name_template=data.get("job_name_template"),
            default_schedule_at=data.get("default_schedule_at"),
            metadata=dict(data.get("metadata", {})),
            parameters=[
                TemplateParameterDefinition.from_dict(item)
                for item in data.get("parameters", [])
            ],
            tasks=[
                JobTemplateTaskDefinition.from_dict(item)
                for item in data.get("tasks", [])
            ],
        )

    def instantiate(
        self,
        parameter_values: ParameterDict,
        *,
        created_by: str,
        overrides: Optional[TemplateOverrides] = None,
    ) -> JobTemplateInstantiation:
        usable_params = self._prepare_parameters(parameter_values)
        tokens = self._build_tokens(usable_params, created_by)

        job_name = (
            overrides.job_name
            if overrides and overrides.job_name
            else self._render(self.job_name_template or self.name, tokens)
        )
        priority = (
            overrides.priority
            if overrides and overrides.priority is not None
            else self.default_priority
        )
        retry_policy = (
            overrides.retry_policy
            if overrides and overrides.retry_policy
            else self.retry_policy
        )
        backend_strategy = (
            overrides.backend_strategy
            if overrides and overrides.backend_strategy
            else self.backend_strategy
        )
        schedule_at = (
            overrides.schedule_at
            if overrides and overrides.schedule_at
            else self.default_schedule_at
        )

        tasks = self._instantiate_tasks(tokens)

        metadata: dict[str, Any] = {
            "template_id": self.template_id,
            "template_name": self.name,
            "template_parameters": usable_params,
        }
        metadata.update(self.metadata)
        if overrides and overrides.metadata:
            metadata.update(overrides.metadata)

        return JobTemplateInstantiation(
            template_id=self.template_id,
            template_name=self.name,
            job_name=job_name,
            tasks=tasks,
            priority=priority,
            retry_policy=retry_policy,
            backend_strategy=backend_strategy,
            schedule_at=schedule_at,
            metadata=metadata,
            parameter_values=usable_params,
        )

    def _prepare_parameters(self, provided: ParameterDict) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for definition in self.parameters:
            raw_value = provided.get(definition.name)
            result[definition.name] = definition.coerce(raw_value)
        for key, value in provided.items():
            if key not in result:
                result[key] = value
        return result

    def _build_tokens(
        self, parameters: Mapping[str, Any], created_by: str
    ) -> dict[str, Any]:
        tokens: dict[str, Any] = {
            **parameters,
            "template_id": self.template_id,
            "template_name": self.name,
            "created_by": created_by,
            "now": datetime.now(timezone.utc).isoformat(),
        }
        return tokens

    def _instantiate_tasks(self, tokens: Mapping[str, Any]) -> List[Task]:
        instantiated: List[Task] = []
        alias_map: dict[str, str] = {}

        for definition in self.tasks:
            site_name = str(self._render(definition.site_name_template, tokens) or "")
            user_id = str(self._render(definition.user_id_template, tokens))
            user_username = (
                str(self._render(definition.user_username_template, tokens))
                if definition.user_username_template
                else None
            )
            rendered_content_type = self._render(
                definition.content_type_template, tokens
            )
            content_type = _normalize_template_content_type(
                None
                if rendered_content_type in (None, "", "None")
                else str(rendered_content_type)
            )
            payload = _strip_template_payload_metadata(
                self._render_value(definition.payload_template, tokens)
            )
            rendered_auth_id = (
                self._render(definition.auth_id_template, tokens)
                if definition.auth_id_template
                else None
            )
            auth_id: int | None = None
            if rendered_auth_id not in (None, "", "None"):
                try:
                    auth_id = int(rendered_auth_id)
                except (TypeError, ValueError):
                    auth_id = None
            ip_lock = (
                str(self._render(definition.ip_lock_key_template, tokens))
                if definition.ip_lock_key_template
                else None
            )

            depends_on_ids: List[str] = []
            for dependency in definition.depends_on:
                if dependency not in alias_map:
                    raise ValueError(
                        f"Unknown task dependency '{dependency}' in template '{self.template_id}'"
                    )
                depends_on_ids.append(alias_map[dependency])

            task = Task(
                site_name=site_name,
                user_id=user_id,
                user_username=user_username,
                content_type=content_type,
                task_type=definition.task_type,
                payload=payload,
                auth_id=auth_id,
                capabilities=list(definition.capabilities),
                ip_lock_key=ip_lock,
                depends_on=depends_on_ids,
            )
            task = Task.from_dict(task.to_dict())

            if definition.max_attempts is not None:
                task.max_attempts = int(definition.max_attempts)
            if definition.name:
                alias_map[definition.name] = task.task_id

            instantiated.append(task)

        return instantiated

    def _render(self, template: Optional[str], tokens: Mapping[str, Any]) -> Any:
        if template is None:
            return None
        try:
            return template.format_map(_StrictMapping(tokens))
        except KeyError as exc:
            missing = exc.args[0]
            raise ValueError(
                f"Missing template parameter '{missing}' for template '{self.template_id}'"
            ) from exc

    def _render_value(self, value: Any, tokens: Mapping[str, Any]) -> Any:
        if isinstance(value, str):
            return self._render(value, tokens)
        if isinstance(value, Mapping):
            mapping = cast(Mapping[str, Any], value)
            return {
                key: self._render_value(val, tokens) for key, val in mapping.items()
            }
        if isinstance(value, list):
            sequence = cast(List[Any], value)
            return [self._render_value(item, tokens) for item in sequence]
        return value


class _StrictMapping(dict[str, Any]):
    def __init__(self, source: Mapping[str, Any]) -> None:
        super().__init__(source)

    def __missing__(self, key: str) -> Any:  # pragma: no cover - defensive
        raise KeyError(key)


class JobTemplateStore:
    def __init__(self, redis: RedisManager) -> None:
        self.redis = redis

    async def create_template(self, template: JobTemplate) -> JobTemplate:
        name_key = self._name_key(template.name)
        existing = await self.redis.get(name_key)
        if existing:
            raise ValueError(f"Template name '{template.name}' already exists")

        now = datetime.now(timezone.utc).isoformat()
        template.created_at = template.created_at or now
        template.updated_at = now

        await self.redis.set(name_key, template.template_id)
        await self.redis.set(
            self._template_key(template.template_id), json.dumps(template.to_dict())
        )
        await self.redis.zadd(
            _TEMPLATE_SET_KEY,
            {template.template_id: datetime.now(timezone.utc).timestamp()},
        )
        return template

    async def get_template(self, template_id: str) -> Optional[JobTemplate]:
        raw = await self.redis.get(self._template_key(template_id))
        if not raw:
            return None
        data = json.loads(raw)
        return JobTemplate.from_dict(data)

    async def get_template_by_name(self, name: str) -> Optional[JobTemplate]:
        template_id = await self.redis.get(self._name_key(name))
        if not template_id:
            return None
        return await self.get_template(str(template_id))

    async def list_templates(self) -> List[JobTemplate]:
        template_ids = await self.redis.zrange(_TEMPLATE_SET_KEY, 0, -1)
        results: List[JobTemplate] = []
        for template_id in template_ids:
            if isinstance(template_id, tuple):
                template_id = template_id[0]
            template = await self.get_template(str(template_id))
            if template:
                results.append(template)
        return results

    async def instantiate_template(
        self,
        template_id: str,
        parameter_values: ParameterDict,
        *,
        created_by: str,
        overrides: Optional[TemplateOverrides] = None,
    ) -> JobTemplateInstantiation:
        template = await self.get_template(template_id)
        if not template:
            raise KeyError(f"Template '{template_id}' not found")
        return template.instantiate(
            parameter_values, created_by=created_by, overrides=overrides
        )

    def _template_key(self, template_id: str) -> str:
        return f"{_TEMPLATE_PREFIX}{template_id}"

    def _name_key(self, name: str) -> str:
        normalized = name.strip().lower()
        return f"{_TEMPLATE_NAME_PREFIX}{normalized}"


async def seed_builtin_templates(store: JobTemplateStore) -> list[JobTemplate]:
    created: list[JobTemplate] = []
    existing = await store.get_template_by_name("Scrape + Download")
    if existing is not None:
        return [existing]

    content_types = (
        "Profile",
        "Posts",
        "Stories",
        "Highlights",
        "Messages",
        "Archived Posts",
    )
    task_definitions: list[JobTemplateTaskDefinition] = []
    for index, content_type in enumerate(content_types, start=1):
        scrape_name = f"scrape_{index}"
        task_definitions.append(
            JobTemplateTaskDefinition(
                name=scrape_name,
                task_type=TaskType.SCRAPE,
                site_name_template="{site}",
                user_id_template="{user_id}",
                user_username_template="{username}",
                content_type_template=content_type,
                payload_template={
                    "download_media": True,
                    "save_to_db": True,
                },
            )
        )
        task_definitions.append(
            JobTemplateTaskDefinition(
                name=f"download_{index}",
                task_type=TaskType.DOWNLOAD,
                site_name_template="{site}",
                user_id_template="{user_id}",
                user_username_template="{username}",
                content_type_template=content_type,
                depends_on=[scrape_name],
                payload_template={},
            )
        )

    template = JobTemplate(
        name="Scrape + Download",
        description="Built-in full performer scrape and download workflow.",
        created_by="system",
        job_name_template="Scrape + Download: {username}",
        parameters=[
            TemplateParameterDefinition(name="site"),
            TemplateParameterDefinition(name="user_id", type="int"),
            TemplateParameterDefinition(name="username", required=False, default=""),
        ],
        tasks=task_definitions,
        metadata={"builtin": True},
    )
    created.append(await store.create_template(template))
    return created
