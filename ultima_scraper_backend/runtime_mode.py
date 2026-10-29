"""Authoritative runtime-mode parsing for backend processes."""

from __future__ import annotations

from ultima_scraper_core import runtime_env_value

VALID_RUNTIME_MODES = frozenset({"api", "webui", "worker"})


def get_runtime_mode(default: str = "api") -> str:
    """Return the normalized runtime mode or fail before starting services."""

    raw_mode = (runtime_env_value("RUN_MODE") or default).strip().lower()
    if raw_mode not in VALID_RUNTIME_MODES:
        choices = ", ".join(sorted(VALID_RUNTIME_MODES))
        raise RuntimeError(f"Invalid RUN_MODE={raw_mode!r}; expected one of: {choices}")
    return raw_mode


def is_api_only_runtime() -> bool:
    """Return whether this process must obey the strict API-only contract."""

    return get_runtime_mode() == "api"
