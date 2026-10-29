"""Plugin capability and permission model.

Phase 1 keeps broad capability-based permissions.  Every plugin action is
gated by one of the declared capabilities.  No action succeeds without an
explicit grant.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .manifest import PluginManifest


class PluginCapability(str, enum.Enum):
    """Named capabilities a plugin may request."""

    JOBS_SUBMIT = "jobs.submit"
    JOBS_SCHEDULE = "jobs.schedule"
    HTTP_REQUEST = "http.request"
    PROXIES_SYNC = "proxies.sync"
    EVENTS_READ = "events.read"
    WEBHOOKS_REGISTER = "webhooks.register"
    WORKERS_STATUS_READ = "workers.status.read"
    AUTH_CREDENTIALS_EXPORT = "auth.credentials.export"
    AUTH_CREDENTIALS_IMPORT = "auth.credentials.import"
    REMOTE_JOBS_SUBMIT = "remote.jobs.submit"
    REMOTE_JOBS_EXECUTE = "remote.jobs.execute"
    REMOTE_CALLBACKS_RECEIVE = "remote.callbacks.receive"
    REMOTE_SESSIONS_CONNECT = "remote.sessions.connect"
    WEBUI_EXTEND = "webui.extend"
    WEBUI_NATIVE = "webui.native"
    PROFILES_READ = "profiles.read"
    PROFILES_PROPOSE = "profiles.propose"
    RUNTIME_PROCESS = "runtime.process"
    STATE_READ = "state.read"
    STATE_WRITE = "state.write"


# Exhaustive allowlist — nothing outside this set is valid in phase 1.
ALL_CAPABILITIES: frozenset[PluginCapability] = frozenset(PluginCapability)


def validate_capabilities(
    requested: list[str],
) -> tuple[list[PluginCapability], list[str]]:
    """Validate a list of raw capability strings.

    Returns a tuple of (valid_capabilities, invalid_strings).
    """
    valid: list[PluginCapability] = []
    invalid: list[str] = []
    for raw in requested:
        try:
            valid.append(PluginCapability(raw))
        except ValueError:
            invalid.append(raw)
    return valid, invalid


def check_capability(manifest: PluginManifest, capability: PluginCapability) -> bool:
    """Return *True* if *manifest* declares *capability*."""
    return capability.value in [c.value for c in manifest.capabilities]
