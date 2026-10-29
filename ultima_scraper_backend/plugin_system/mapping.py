"""Declarative mapping helpers for plugin runtime orchestration."""

from __future__ import annotations

import re
from typing import Any

from ultima_scraper_api import ContentType

from ultima_scraper_backend.job_system import TaskType, build_job, build_task

from .manifest import ExecutorMapping, ExecutorTaskBinding

_TEMPLATE_RE = re.compile(r"{{\s*([a-zA-Z0-9_.-]+)\s*}}")
_MAPPING_OBJECT_KEYS = frozenset({"source_path", "literal"})


def resolve_path(payload: Any, path: str, default: Any = None) -> Any:
    """Resolve a dotted path from dict/list data."""
    current = payload
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return default
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            idx = int(part)
            if idx >= len(current):
                return default
            current = current[idx]
            continue
        return default
    return current


def render_template(template: str, context: dict[str, Any]) -> str:
    """Render a tiny `{{ path }}` template against context data."""

    def _replace(match: re.Match[str]) -> str:
        value = resolve_path(context, match.group(1), "")
        return "" if value is None else str(value)

    return _TEMPLATE_RE.sub(_replace, template)


def _is_mapping_object(value: Any) -> bool:
    return isinstance(value, dict) and bool(set(value.keys()) & _MAPPING_OBJECT_KEYS)


def evaluate_mapping_node(
    mapping: Any,
    context: dict[str, Any],
) -> Any:
    if _is_mapping_object(mapping):
        if "literal" in mapping:
            return mapping["literal"]
        return resolve_path(context, str(mapping.get("source_path", "")))
    if isinstance(mapping, dict):
        return {
            field_name: evaluate_mapping_node(field_mapping, context)
            for field_name, field_mapping in mapping.items()
        }
    if isinstance(mapping, list):
        return [evaluate_mapping_node(item, context) for item in mapping]
    return mapping


def apply_field_mappings(
    mappings: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field_name, mapping in mappings.items():
        result[field_name] = evaluate_mapping_node(mapping, context)
    return result


def match_task_binding(
    binding: ExecutorTaskBinding,
    *,
    remote_job_type: str | None,
    remote_message: dict[str, Any],
) -> bool:
    remote_match = binding.remote_match
    if remote_match.job_type is not None and remote_match.job_type != remote_job_type:
        return False
    if remote_match.kind is not None:
        remote_kind = resolve_path(remote_message, "kind")
        if remote_kind != remote_match.kind:
            return False
    return True


def _coerce_content_type(raw_value: Any) -> Any:
    if raw_value is None or isinstance(raw_value, ContentType):
        return raw_value
    if isinstance(raw_value, str):
        candidate = raw_value.strip().upper()
        for content_type in ContentType:
            if content_type.name == candidate or str(content_type.value).upper() == candidate:
                return content_type
    return raw_value


def _coerce_task_kwargs(task_type: TaskType, kwargs: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(kwargs)
    if "content_type" in normalized:
        normalized["content_type"] = _coerce_content_type(normalized["content_type"])
    if task_type in {TaskType.SCRAPE, TaskType.DOWNLOAD} and "site_name" in normalized:
        normalized["site_name"] = str(normalized["site_name"])
    if task_type == TaskType.API_COMMAND and "site_name" in normalized:
        normalized["site_name"] = str(normalized["site_name"])
    return normalized


def build_native_job_from_remote(
    *,
    plugin_id: str,
    correlation_id: str,
    external_job_id: str | None,
    executor_mapping: ExecutorMapping,
    remote_message: dict[str, Any],
    runtime_context: dict[str, Any] | None = None,
) -> tuple[Any, list[Any]]:
    """Translate remote work into one native USR job with one or more tasks."""
    remote_job_type_value = resolve_path(
        remote_message, executor_mapping.remote_job_type_path
    )
    remote_job_type = (
        str(remote_job_type_value) if remote_job_type_value is not None else None
    )
    remote_payload_raw = resolve_path(remote_message, executor_mapping.remote_payload_path)
    remote_payload = remote_payload_raw if isinstance(remote_payload_raw, dict) else {}
    runtime = dict(runtime_context or {})
    context: dict[str, Any] = {
        "plugin_id": plugin_id,
        "correlation_id": correlation_id,
        "external_job_id": external_job_id,
        "remote_job_type": remote_job_type,
        "remote_payload": remote_payload,
        "remote": remote_message,
        "runtime": runtime,
    }
    context.update(runtime)

    remote_task_items: list[dict[str, Any]]
    if executor_mapping.remote_tasks_path:
        remote_tasks_value = resolve_path(remote_message, executor_mapping.remote_tasks_path)
        if isinstance(remote_tasks_value, list):
            remote_task_items = [
                item for item in remote_tasks_value if isinstance(item, dict)
            ]
        elif isinstance(remote_tasks_value, dict):
            remote_task_items = [remote_tasks_value]
        else:
            remote_task_items = []
    else:
        remote_task_items = [remote_message]

    tasks: list[Any] = []
    task_aliases: dict[str, str] = {}
    pending_dependencies: list[tuple[Any, list[str]]] = []

    for index, remote_task in enumerate(remote_task_items):
        matched_for_item = False
        item_context = {
            **context,
            "remote_task": remote_task,
            "remote_task_index": index,
        }
        for binding in executor_mapping.task_bindings:
            if not match_task_binding(
                binding,
                remote_job_type=remote_job_type,
                remote_message=remote_task,
            ):
                continue
            matched_for_item = True
            task_type = TaskType(binding.local_task_type)
            kwargs: dict[str, Any] = {}
            kwargs.update(binding.defaults)
            kwargs.update(apply_field_mappings(binding.payload_mapping, item_context))
            kwargs.update(binding.overrides)
            kwargs = _coerce_task_kwargs(task_type, kwargs)
            task = build_task(task_type, **kwargs)
            tasks.append(task)

            alias_value = (
                evaluate_mapping_node(binding.task_alias, item_context)
                if binding.task_alias is not None
                else None
            )
            if alias_value not in (None, ""):
                task_aliases[str(alias_value)] = task.task_id

            dependency_value = (
                evaluate_mapping_node(binding.depends_on_aliases, item_context)
                if binding.depends_on_aliases is not None
                else None
            )
            if dependency_value in (None, ""):
                continue
            if isinstance(dependency_value, list):
                dependency_aliases = [
                    str(alias)
                    for alias in dependency_value
                    if alias not in (None, "")
                ]
            else:
                dependency_aliases = [str(dependency_value)]
            if dependency_aliases:
                pending_dependencies.append((task, dependency_aliases))

        if executor_mapping.remote_tasks_path and not matched_for_item:
            remote_kind = resolve_path(remote_task, "kind")
            raise ValueError(
                "No executor task binding matched remote task "
                f"at index {index} with kind {remote_kind!r}"
            )

    if not tasks:
        raise ValueError(
            f"No executor task bindings matched remote job type {remote_job_type!r}"
        )

    for task, dependency_aliases in pending_dependencies:
        missing_aliases = [
            alias for alias in dependency_aliases if alias not in task_aliases
        ]
        if missing_aliases:
            raise ValueError(
                "Executor task binding references unknown dependency aliases: "
                f"{missing_aliases}"
            )
        combined_dependencies = list(task.depends_on)
        for alias in dependency_aliases:
            dependency_task_id = task_aliases[alias]
            if dependency_task_id not in combined_dependencies:
                combined_dependencies.append(dependency_task_id)
        task.depends_on = combined_dependencies

    job_name = render_template(executor_mapping.local_job_name_template, context)
    job_result = build_job(
        tasks,
        job_name=job_name or f"Remote {correlation_id}",
        created_by=f"plugin:{plugin_id}",
    )
    return job_result.job, tasks
