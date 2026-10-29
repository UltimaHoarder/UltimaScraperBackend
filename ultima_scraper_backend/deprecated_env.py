"""Environment overrides kept for one release after moving into Settings.

Each name here has a database-owned setting. A set variable still wins so
existing deployments keep their behavior, and a warning names the setting
that replaces it. Invalid values are reported and ignored.
"""

from __future__ import annotations

import logging

from ultima_scraper_core import runtime_env_value

logger = logging.getLogger(__name__)

# Deprecated environment name -> replacing settings path.
DEPRECATED_SETTINGS_ENV = {
    "ULTIMA_WORKER_CONCURRENCY": "worker_pool.task_concurrency",
    "ULTIMA_CLAIM_BATCH_SIZE": "worker_pool.claim_batch_size",
    "USR_TERMINAL_JOB_RECONCILE_INTERVAL_SECONDS": "job_execution.terminal_reconcile_interval_seconds",
    "USR_TERMINAL_JOB_RECONCILE_MIN_AGE_SECONDS": "job_execution.terminal_reconcile_min_age_seconds",
    "USR_TERMINAL_JOB_RECONCILE_BATCH_SIZE": "job_execution.terminal_reconcile_batch_size",
    "USR_COMPLETED_JOB_RETENTION_SECONDS": "job_execution.completed_job_retention_seconds",
    "USR_COMPLETED_JOB_RETENTION_BATCH_SIZE": "job_execution.completed_job_retention_batch_size",
    "ULTIMA_JOB_TASK_ARCHIVE_BATCH_SIZE": "job_execution.task_archive_batch_size",
    "ULTIMA_MPS_REPLACEMENT_BATCH_SIZE": "job_execution.mps_replacement_batch_size",
    "ULTIMA_LEDGER_PLANNER_BATCH": "job_execution.ledger_planner_batch",
    "ULTIMA_MPS_GUEST_ROUTE_TIMEOUT": "network.guest_route_timeout",
}

_reported: set[str] = set()


def _report_once(name: str, message: str, *args: object) -> None:
    if name not in _reported:
        _reported.add(name)
        logger.warning(message, *args)


def deprecated_env_int(name: str, *, minimum: int = 1) -> int | None:
    """Return a deprecated integer override, or ``None`` when unset/invalid."""
    raw = runtime_env_value(name)
    if raw is None or not raw.strip():
        return None
    setting = DEPRECATED_SETTINGS_ENV[name]
    try:
        value = int(raw.strip())
    except ValueError:
        value = minimum - 1
    if value < minimum:
        _report_once(
            name,
            "Ignoring %s=%r: expected an integer >= %d. Remove it and set %s in Settings.",
            name, raw, minimum, setting,
        )
        return None
    _report_once(
        name,
        "%s is deprecated and overrides the %s setting; remove it and edit Settings instead.",
        name, setting,
    )
    return value
