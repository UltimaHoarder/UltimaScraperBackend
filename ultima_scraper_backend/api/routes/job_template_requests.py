"""Normalize job template, payload and schedule requests."""

from __future__ import annotations

from typing import TYPE_CHECKING
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Any, Literal, Mapping, Optional, Sequence, cast
from ultima_scraper_api import ContentType
from ...job_system import JobSchedule
from ...job_system.templates import JobTemplate
from .job_schemas import BackendStrategyPayload, JobScheduleRequest, JobTemplateResponse, JobTemplateTaskPayload, JsonValue, RetryPolicyPayload, TemplateParameterPayload
from .job_views import _stringify_mapping

if TYPE_CHECKING:
    from ...job_system.templates import TemplateParameterDefinition


CONTENT_TYPE_MAP: dict[str, ContentType] = {
    "Posts": ContentType.POSTS,
    "ArchivedPosts": ContentType.ARCHIVED_POSTS,
    "Archived": ContentType.ARCHIVED_POSTS,
    "Messages": ContentType.MESSAGES,
    "Stories": ContentType.STORIES,
    "Highlights": ContentType.HIGHLIGHTS,
    "Profile": ContentType.PROFILE,
    "Chats": ContentType.CHATS,
    "MassMessages": ContentType.MASS_MESSAGES,
}


type SupportedSiteName = Literal["OnlyFans", "Fansly", "LoyalFans"]


def _coerce_json_value(value: object) -> JsonValue:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[str, object], value)
        return {str(key): _coerce_json_value(val) for key, val in mapping.items()}
    if isinstance(value, list):
        sequence = cast(list[object], value)
        return [_coerce_json_value(item) for item in sequence]
    if isinstance(value, tuple):
        sequence = cast(tuple[object, ...], value)
        return [_coerce_json_value(item) for item in sequence]
    return str(value)


def _coerce_json_mapping(value: Mapping[str, object] | None) -> dict[str, JsonValue]:
    if not value:
        return {}
    return {str(key): _coerce_json_value(val) for key, val in value.items()}


def _payload_to_template_payload(payload: object | None) -> dict[str, JsonValue]:
    if payload is None:
        return {}
    if hasattr(payload, "model_dump"):
        model_dump = getattr(payload, "model_dump")
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, Mapping):
                return _coerce_json_mapping(
                    _stringify_mapping(cast(Mapping[Any, Any], dumped))
                )
    if isinstance(payload, Mapping):
        return _coerce_json_mapping(
            _stringify_mapping(cast(Mapping[Any, Any], payload))
        )
    if isinstance(payload, list):
        sequence = cast(list[object], payload)
        return {"items": _coerce_json_value(sequence)}
    return {"value": _coerce_json_value(payload)}


def _parameter_definitions_from_payloads(
    parameters: Sequence[TemplateParameterPayload],
) -> list["TemplateParameterDefinition"]:
    from ...job_system.templates import TemplateParameterDefinition

    return [
        TemplateParameterDefinition(
            name=param.name,
            type=param.type,
            required=param.required,
            default=param.default,
            description=param.description,
            choices=param.choices,
        )
        for param in parameters
    ]


def _template_to_response(template: "JobTemplate") -> JobTemplateResponse:
    parameters = [
        TemplateParameterPayload(
            name=param.name,
            type=param.type,
            required=param.required,
            default=param.default,
            description=param.description,
            choices=param.choices,
        )
        for param in template.parameters
    ]

    tasks: list[JobTemplateTaskPayload] = []
    for task in template.tasks:
        payload_template = _coerce_json_mapping(
            cast(Mapping[str, object], task.payload_template)
        )
        tasks.append(
            JobTemplateTaskPayload(
                name=task.name,
                task_type=task.task_type.value,
                site_name_template=task.site_name_template,
                user_id_template=task.user_id_template,
                content_type_template=task.content_type_template,
                payload_template=payload_template,
                capabilities=list(task.capabilities),
                depends_on=list(task.depends_on),
                ip_lock_key_template=task.ip_lock_key_template,
                max_attempts=task.max_attempts,
                dedupe_override=task.dedupe_override,
            )
        )

    return JobTemplateResponse(
        template_id=template.template_id,
        name=template.name,
        description=template.description,
        created_by=template.created_by,
        created_at=template.created_at,
        updated_at=template.updated_at,
        default_priority=template.default_priority,
        retry_policy=RetryPolicyPayload(
            attempts=template.retry_policy.attempts,
            backoff_seconds=template.retry_policy.backoff_seconds,
            max_backoff_seconds=template.retry_policy.max_backoff_seconds,
        ),
        backend_strategy=BackendStrategyPayload(
            mode=template.backend_strategy.mode,
            backend_id=template.backend_strategy.backend_id,
        ),
        job_name_template=template.job_name_template,
        default_schedule_at=template.default_schedule_at,
        metadata=_coerce_json_mapping(cast(Mapping[str, object], template.metadata)),
        parameters=parameters,
        tasks=tasks,
    )


# All content types available for scraping
ALL_SCRAPABLE_TYPES = [
    "Posts",
    "ArchivedPosts",
    "Messages",
    "Stories",
    "Highlights",
    "Profile",
]


def _normalize_site_name(site: str) -> str:
    """Normalize site name to proper case format.

    Args:
        site: Site name in any case (e.g., 'onlyfans', 'OnlyFans', 'ONLYFANS')

    Returns:
        Properly formatted site name ('OnlyFans', 'Fansly', 'LoyalFans')
    """
    site_lower = site.lower()
    if site_lower == "onlyfans":
        return "OnlyFans"
    elif site_lower == "fansly":
        return "Fansly"
    elif site_lower == "loyalfans":
        return "LoyalFans"
    else:
        # Default to original if unknown
        return site


def _normalize_max_items(raw_value: object) -> int | None:
    """Normalize max_items to an int or None (unlimited).

    Treats None, empty strings, "all", and non-positive values as unlimited.
    """
    if raw_value is None:
        return None

    if isinstance(raw_value, bool):
        return None

    if isinstance(raw_value, str):
        value = raw_value.strip().lower()
        if value in {"", "all", "none"}:
            return None
        try:
            parsed = int(value)
        except ValueError:
            return None
        return parsed if parsed > 0 else None

    if isinstance(raw_value, int):
        return raw_value if raw_value > 0 else None

    return None


def _normalize_nonnegative_int(raw_value: object) -> int | None:
    """Normalize a non-negative integer task parameter."""

    if raw_value is None or isinstance(raw_value, bool):
        return None
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _normalize_content_type(raw_value: object) -> str:
    """Normalize a content type string to canonical values."""
    if not isinstance(raw_value, str):
        return "Posts"

    trimmed = raw_value.strip()
    compact = trimmed.replace(" ", "").replace("_", "").lower()
    content_type_map = {
        "posts": "Posts",
        "archivedposts": "ArchivedPosts",
        "archived": "ArchivedPosts",
        "messages": "Messages",
        "stories": "Stories",
        "highlights": "Highlights",
        "chats": "Chats",
        "massmessages": "MassMessages",
        "profile": "Profile",
        "all": "all",
    }
    return content_type_map.get(compact, trimmed)


def _normalize_bool(raw_value: object, *, default: bool = False) -> bool:
    """Normalize common boolean-like inputs."""
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        value = raw_value.strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        return default
    if isinstance(raw_value, (int, float)):
        return bool(raw_value)
    return default


def _normalize_paid_feed_content_types(raw_value: object) -> list[str]:
    """Normalize paid-feed content types to the supported subset."""
    if isinstance(raw_value, list):
        normalized = [
            _normalize_content_type(item)
            for item in cast(list[object], raw_value)
            if _normalize_content_type(item) in {"Posts", "Messages"}
        ]
    elif raw_value is None:
        normalized = []
    else:
        content_type = _normalize_content_type(raw_value)
        normalized = [content_type] if content_type in {"Posts", "Messages"} else []

    deduped: list[str] = []
    for content_type in normalized:
        if content_type not in deduped:
            deduped.append(content_type)

    return deduped or ["Posts", "Messages"]


def _build_schedule_from_request(request: JobScheduleRequest) -> JobSchedule:
    schedule = JobSchedule()
    now = datetime.now(timezone.utc)

    has_interval = request.interval_seconds is not None
    has_cron = bool(request.cron)
    has_scheduled_for = bool(request.scheduled_for)
    has_start_immediately = bool(request.start_immediately)

    if has_interval and request.interval_seconds is not None:
        if request.interval_seconds <= 0:
            raise HTTPException(
                status_code=400,
                detail="interval_seconds must be greater than 0",
            )

    if has_interval and has_cron:
        raise HTTPException(
            status_code=400,
            detail="Provide either interval_seconds or cron, not both",
        )

    if has_start_immediately and has_cron:
        raise HTTPException(
            status_code=400,
            detail="start_immediately is only supported for interval schedules",
        )

    if has_start_immediately and has_scheduled_for:
        raise HTTPException(
            status_code=400,
            detail="Provide either start_immediately or scheduled_for, not both",
        )

    scheduled_at: Optional[datetime] = None

    def _normalize_datetime(value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="scheduled_for must be an ISO-8601 timestamp",
            ) from exc

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    if has_scheduled_for and request.scheduled_for:
        scheduled_at = _normalize_datetime(request.scheduled_for)
        if not has_interval and not has_cron and scheduled_at <= now:
            raise HTTPException(
                status_code=400,
                detail="scheduled_for must be in the future",
            )

    if has_interval and request.interval_seconds is not None:
        schedule.interval_seconds = request.interval_seconds
        if request.start_immediately:
            scheduled_at = now
        elif scheduled_at is None or scheduled_at <= now:
            scheduled_at = now + timedelta(seconds=request.interval_seconds)

    if has_cron and request.cron:
        schedule.cron = request.cron
        try:
            trigger = cast(Any, CronTrigger).from_crontab(
                request.cron,
                timezone=timezone.utc,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="Invalid cron expression",
            ) from exc
        next_time_raw = trigger.get_next_fire_time(None, now)
        next_time = next_time_raw if isinstance(next_time_raw, datetime) else None
        if not next_time:
            raise HTTPException(
                status_code=400,
                detail="Cron expression did not produce a next run time",
            )
        scheduled_at = next_time

    if scheduled_at:
        schedule.scheduled_for = scheduled_at.isoformat()

    return schedule


def _payload_to_template(payload: object | None) -> dict[str, object]:
    compatibility_keys = {"dedupe_override", "template_content_type", "template_task"}

    def _strip_compatibility_keys(data: Mapping[str, object]) -> dict[str, object]:
        return {
            str(key): value
            for key, value in data.items()
            if str(key) not in compatibility_keys
        }

    if payload is None:
        return {}
    if isinstance(payload, BaseModel):
        return _strip_compatibility_keys(payload.model_dump())
    if isinstance(payload, Mapping):
        return _strip_compatibility_keys(
            _stringify_mapping(cast(Mapping[Any, Any], payload))
        )
    return {}
