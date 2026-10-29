"""Resolve shared execution windows and producer-specific input settings.

Stored MPS keys and ordinary settings remain compatible. New submissions name
their resource scope explicitly; old definitions resolve their historical scope
here, rather than teaching dispatch, attempts and checkpoints producer names.
"""

from dataclasses import dataclass

from ultima_scraper_core.environment import runtime_env_value

from ..database.write_queue import DatabaseWriteClient, _positive_int_env
from . import mps_state


def _persisted_job_execution_setting(name: str, *, db_session=None) -> int | None:
    """Read a live persisted setting, falling back safely to environment config."""
    if db_session is not None:
        from ..config_settings_sync import load_job_execution_settings

        value = getattr(load_job_execution_settings(db_session), name, None)
        return int(value) if value is not None else None
    try:
        from ..config import UltimaScraperCollectionConfig

        value = getattr(
            UltimaScraperCollectionConfig.load_or_create_default_config().settings.job_execution,
            name,
            None,
        )
        return int(value) if value is not None else None
    except Exception:
        return None


def task_insert_max_bytes(value: int | None = None) -> int:
    configured = runtime_env_value("ULTIMA_JOB_TASK_INSERT_MAX_BYTES")
    if value is not None or (configured is not None and str(configured).strip()):
        return _positive_int_env(
            "ULTIMA_JOB_TASK_INSERT_MAX_BYTES",
            DatabaseWriteClient.MAX_COMMAND_BYTES,
            value=value,
        )
    return _positive_int_env(
        "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
    )


@dataclass(frozen=True, slots=True)
class WindowPolicy:
    scope: str
    tasks: int
    bytes: int
    global_tasks: int
    global_bytes: int
    input_rows: int
    input_bytes: int
    budget_key: str
    waiting_key: str
    waiting_set_key: str
    replacement_rows: int
    replacement_bytes: int
    input_snapshot: bool


def window_policy(metadata, storage, *, db_session=None) -> WindowPolicy:
    scope = metadata.get("job_resource_scope") or (
        "ordinary" if metadata.get("durable_job_kind") == "ordinary" else "mps"
    )
    if scope == "ordinary":
        input_rows, input_bytes = storage.task_input_limits()
        replacement_rows = _positive_int_env(
            "ULTIMA_JOB_TASK_INSERT_BATCH_SIZE", storage.DEFAULT_WRITE_BATCH_SIZE
        )
        replacement_bytes = task_insert_max_bytes()
        input_snapshot = True
    elif scope == "mps":
        replacement_rows = 0  # Scaled to the window below.
        replacement_bytes = _positive_int_env(
            "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES",
            DatabaseWriteClient.MAX_COMMAND_BYTES,
        )
        input_rows = input_bytes = None
        input_snapshot = False
    else:
        raise ValueError(f"Unknown job resource scope: {scope}")
    env_tasks = runtime_env_value("ULTIMA_JOB_WINDOW_MAX_TASKS")
    if env_tasks is not None and str(env_tasks).strip():
        tasks = _positive_int_env(
            "ULTIMA_JOB_WINDOW_MAX_TASKS", storage.DEFAULT_WRITE_BATCH_SIZE
        )
    else:
        configured_tasks = _persisted_job_execution_setting(
            "window_max_tasks", db_session=db_session
        )
        tasks = configured_tasks or _positive_int_env(
            "ULTIMA_JOB_WINDOW_MAX_TASKS", storage.DEFAULT_WRITE_BATCH_SIZE
        )
    if scope == "mps":
        raw_cap = metadata.get("proxy_capacity")
        if isinstance(raw_cap, (int, float)) and raw_cap > 0:
            # Peg window to proxy capacity with 20% pipeline headroom so proxies are never starved
            tasks = max(tasks, int(raw_cap * 1.2))
        # Replacement and retry pages move one window's worth of tasks at a
        # time, still bounded by replacement_bytes, so they scale with the job.
        replacement_rows = tasks

    size = _positive_int_env(
        "ULTIMA_JOB_WINDOW_MAX_BYTES",
        _positive_int_env(
            "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES",
            DatabaseWriteClient.MAX_COMMAND_BYTES,
        ),
    )
    return WindowPolicy(
        scope,
        tasks,
        size,
        _positive_int_env("ULTIMA_JOB_GLOBAL_WINDOW_MAX_TASKS", tasks),
        _positive_int_env("ULTIMA_JOB_GLOBAL_WINDOW_MAX_BYTES", size),
        tasks if input_rows is None else input_rows,
        size if input_bytes is None else input_bytes,
        mps_state.BUDGET_KEY,
        mps_state.WAITING_KEY,
        mps_state.WAITING_SET_KEY,
        replacement_rows,
        replacement_bytes,
        input_snapshot,
    )


def recovery_page_size(default: int) -> int:
    return _positive_int_env(
        "ULTIMA_JOB_RECOVERY_BATCH_SIZE",
        _positive_int_env("ULTIMA_MPS_RECOVERY_BATCH_SIZE", default),
    )


def terminal_job_state(job_config, completed: int, failed: int) -> str:
    """Final state of a run whose every task has finished.

    MPS sweeps run many independent profile tasks under a failure budget, so
    any completed task makes the sweep complete. An ordinary job's tasks form
    one request (a scrape and its download), so any failed task fails it.
    """
    metadata = (job_config or {}).get("metadata") or {}
    if metadata.get("durable_job_kind") == "ordinary":
        return "failed" if failed else "completed"
    return "completed" if completed or not failed else "failed"
