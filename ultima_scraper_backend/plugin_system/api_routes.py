"""Plugin admin API routes.

Provides operator-facing endpoints for managing plugins:
- List / inspect plugins
- Enable / disable plugins
- Manage secrets
- Manage plugin state
- View delivery status
- Submit jobs as a plugin (capability-gated)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse
from httpx import RequestError, TimeoutException
from pydantic import AliasChoices, BaseModel, Field

from .manifest import ALLOWED_EVENT_FAMILIES
from .permissions import PluginCapability
from .sdk import SDK_VERSION, SLOTS
from .installation import InstallationConflict
from ..database.threads import run_database_call

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/plugins", tags=["plugins"])
MAX_PLUGIN_CALLBACK_BODY_BYTES = 1_048_576


async def _read_bounded_callback_body(request: Request) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_PLUGIN_CALLBACK_BODY_BYTES:
            raise HTTPException(status_code=413, detail="Callback payload is too large")
        chunks.append(chunk)
    return b"".join(chunks)


# ── Shared dependency ─────────────────────────────────────────────────────


def resolve_plugin_platform(app: Any) -> Any | None:
    """Return the platform, creating it when setup migrated the schema after startup."""
    platform = getattr(app.state, "plugin_platform", None)
    ensure = getattr(app.state, "ensure_plugin_platform", None)
    if platform is None and ensure is not None:
        platform = ensure()
    return platform


def _get_platform(request: Request) -> Any:
    """Resolve the worker-local facade from FastAPI application state."""
    platform = resolve_plugin_platform(request.app)
    if platform is None:
        raise HTTPException(status_code=503, detail="Plugin platform not initialized")
    return platform


# ── Request / response models ─────────────────────────────────────────────


class PluginStateSetRequest(BaseModel):
    key: str = Field(min_length=1, max_length=128)
    value: Any
    reason: str = ""


class PluginJobSubmitRequest(BaseModel):
    plugin_id: str | None = None
    task_type: str = Field(
        ...,
        validation_alias=AliasChoices("task_type", "job_type"),
        serialization_alias="task_type",
    )
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class PluginActivationRequest(BaseModel):
    package_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    approve_permissions: bool = False


def _get_runtime_identity(request: Request, platform: Any = Depends(_get_platform)) -> str:
    authorization = request.headers.get("authorization", "")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Plugin token required")
    identity = platform.repository.authenticate_api_token(authorization[7:])
    if identity is None:
        raise HTTPException(status_code=401, detail="Plugin token is expired or revoked")
    platform._require_enabled(identity)
    return identity


class PluginSetupRequest(BaseModel):
    configuration: dict[str, Any] = Field(default_factory=dict)
    credentials: dict[str, str] = Field(default_factory=dict, repr=False)
    enable: bool = False


class PluginInstallSource(BaseModel):
    kind: Literal['git', 'local']
    source_id: str | None = Field(default=None, max_length=128)
    url: str | None = Field(default=None, max_length=2048)
    revision: str | None = Field(default=None, max_length=200)


class PluginInstallConfirmation(BaseModel):
    plugin_id: str = Field(pattern=r'^[a-z][a-z0-9_-]{1,62}[a-z0-9]$')
    preview_token: str = Field(pattern=r'^[a-f0-9]{32}$')


class PluginUninstallRequest(BaseModel):
    delete_data: bool = False
    confirmation: str = ''


def _installation_error(error):
    return HTTPException(status_code=409 if isinstance(error, InstallationConflict) else 400, detail=str(error))


@router.get('/sources')
async def plugin_sources(platform: Any = Depends(_get_platform)):
    return await run_database_call(platform.list_plugin_sources)


@router.post('/install/prepare')
async def prepare_plugin_install(request: PluginInstallSource, platform: Any = Depends(_get_platform)):
    try:
        return await run_database_call(platform.prepare_plugin_install, request.model_dump(exclude_none=True))
    except (ValueError, OSError) as error:
        raise _installation_error(error) from error


@router.post('/install/confirm', status_code=201)
async def confirm_plugin_install(request: PluginInstallConfirmation, platform: Any = Depends(_get_platform)):
    try:
        return await run_database_call(platform.confirm_plugin_install, request.plugin_id, request.preview_token)
    except (ValueError, OSError) as error:
        raise _installation_error(error) from error


@router.post('/install/cancel')
async def cancel_plugin_install(request: PluginInstallConfirmation, platform: Any = Depends(_get_platform)):
    try:
        await run_database_call(platform.cancel_plugin_install, request.plugin_id, request.preview_token)
        return {'status': 'canceled'}
    except (ValueError, OSError) as error:
        raise _installation_error(error) from error


@router.post('/{plugin_id}/uninstall')
async def uninstall_plugin(plugin_id: str, request: PluginUninstallRequest, platform: Any = Depends(_get_platform)):
    try:
        result = await run_database_call(platform.uninstall_plugin, plugin_id, delete_data=request.delete_data, confirmation=request.confirmation)
        await platform._stop_plugin_supervisors(plugin_id)
        return result
    except (ValueError, OSError, KeyError) as error:
        raise _installation_error(error) from error


class PluginHTTPQuery(BaseModel):
    query: dict[str, Any] = Field(default_factory=dict)


@router.post("/runtime/http/{service_id}")
async def runtime_http_get(service_id: str, request: PluginHTTPQuery, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    try:
        return await platform.scoped_http_get(plugin_id, service_id, request.query)
    except (PermissionError, ValueError) as error:
        raise HTTPException(status_code=403 if isinstance(error, PermissionError) else 400, detail=str(error)) from error


@router.post("/runtime/proxies/sync")
async def runtime_sync_proxies(request: Request, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    from ..api.routes._auth_event_utils import publish_auth_updated
    from ..api.routes.settings import _publish_settings_updated_event
    from ..database.threads import run_database_call
    from ..services.plugin_proxies import ProxySyncRequest, sync_plugin_proxies
    try:
        # Proxy passwords must not appear in Pydantic's request-error payload.
        raw = await _read_bounded_callback_body(request)
        try:
            snapshot = ProxySyncRequest.model_validate_json(raw)
        except ValueError:
            raise ValueError("Invalid or empty provider proxy snapshot") from None
        config, result, accounts, _reused = await run_database_call(sync_plugin_proxies, platform, plugin_id, snapshot)
        record = platform._require_known(plugin_id)
        for schedule in record.manifest.sdk.schedules:
            await run_database_call(platform.repository.update_host_document, plugin_id, "schedule:" + schedule.schedule_id, lambda state: {**state, "last_result": {**result, "completed_at": platform.repository.now().isoformat()}})
        backend = platform._backend
        # Re-publish on receipt replay too: the process may have died after the
        # canonical commit but before notifying workers.
        await _publish_settings_updated_event(backend, settings_id=config._settings_id, settings_scope=config.config_write_backend(), version=config._settings_version, updated_at=config._settings_updated_at, changed_fields=["network"], actor_backend_id=None, request_id=snapshot.idempotency_key)
        if accounts:
            from ..api.dependencies import get_backend_manager
            manager = await get_backend_manager()
            for account in accounts:
                await publish_auth_updated(manager, account["user_id"], username=account["username"], site=account["site"])
        return result
    except (PermissionError, ValueError) as error:
        raise HTTPException(status_code=403 if isinstance(error, PermissionError) else 400, detail=str(error)) from error


@router.get("/{plugin_id}/setup")
def get_plugin_setup(plugin_id: str, platform: Any = Depends(_get_platform)):
    return platform.setup_description(plugin_id)


@router.post("/{plugin_id}/setup")
async def save_plugin_setup(plugin_id: str, request: Request, platform: Any = Depends(_get_platform)):
    try:
        raw = await _read_bounded_callback_body(request)
        try:
            setup = PluginSetupRequest.model_validate_json(raw)
        except ValueError:
            # Request validation must never echo a submitted credential.
            raise ValueError("Invalid plugin setup request") from None
        return await platform.save_plugin_setup(plugin_id, setup.configuration, setup.credentials, enable=setup.enable)
    except (PermissionError, ValueError) as error:
        raise HTTPException(status_code=403 if isinstance(error, PermissionError) else 400, detail=str(error)) from error


@router.post("/{plugin_id}/schedules/{schedule_id}/run")
async def run_plugin_schedule(plugin_id: str, schedule_id: str, platform: Any = Depends(_get_platform)):
    from ..database.threads import run_database_call
    try:
        return await run_database_call(platform.submit_schedule, plugin_id, schedule_id, force=True)
    except (PermissionError, ValueError, RuntimeError) as error:
        raise HTTPException(status_code=403 if isinstance(error, PermissionError) else 400, detail=str(error)) from error


@router.post("/runtime/invoke")
async def invoke_runtime_operation(request: PluginJobSubmitRequest, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    if request.plugin_id is not None and request.plugin_id != plugin_id:
        raise HTTPException(status_code=403, detail="Plugin identity does not match its token")
    try:
        return await platform.invoke_operation(plugin_id, request.task_type, request.payload)
    except (TimeoutError, TimeoutException):
        raise HTTPException(status_code=504, detail="Plugin service did not respond before the operation deadline") from None
    except RequestError:
        raise HTTPException(status_code=502, detail="Plugin service request failed") from None
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=403 if isinstance(exc, PermissionError) else 400, detail=str(exc)) from exc


@router.post("/runtime/jobs")
async def submit_runtime_job(request: PluginJobSubmitRequest, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    from ..database.threads import run_database_call
    if request.plugin_id is not None and request.plugin_id != plugin_id:
        raise HTTPException(status_code=403, detail="Plugin identity does not match its token")
    try:
        return await run_database_call(platform.submit_execution, plugin_id, request.task_type, request.payload, idempotency_key=request.idempotency_key)
    except (PermissionError, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=403 if isinstance(exc, PermissionError) else 400, detail=str(exc)) from exc


@router.get("/runtime/state/{key}")
def read_runtime_state(key: str, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    try:
        return platform.scoped_state(plugin_id, key)
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=403 if isinstance(exc, PermissionError) else 400, detail=str(exc)) from exc


@router.put("/runtime/state")
def write_runtime_state(request: PluginStateSetRequest, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    try:
        if len(json.dumps(request.value).encode()) > 65536:
            raise ValueError("State value exceeds 64 KiB")
        return platform.scoped_state(plugin_id, request.key, value=request.value, write=True)
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=403 if isinstance(exc, PermissionError) else 400, detail=str(exc)) from exc


@router.get("/runtime/profiles/{site}/{user_id}")
def read_runtime_profile(site: str, user_id: int, plugin_id: str = Depends(_get_runtime_identity), platform: Any = Depends(_get_platform)):
    try:
        return platform.scoped_profile(plugin_id, site, user_id)
    except (KeyError, PermissionError, ValueError) as exc:
        status = 404 if isinstance(exc, KeyError) else 403 if isinstance(exc, PermissionError) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc


class SecretCreateRequest(BaseModel):
    plugin_id: str
    scope_kind: str
    scope_name: str


class SecretRotateRequest(BaseModel):
    plugin_id: str
    scope_kind: str
    scope_name: str


class SecretSetRequest(BaseModel):
    plugin_id: str
    scope_kind: str
    scope_name: str
    raw_secret: str = Field(..., min_length=1)


class PluginCreateRequest(BaseModel):
    configuration_schema: dict[str, Any] | None = None
    """Request body for creating a new plugin from a manifest definition."""

    plugin_id: str = Field(..., min_length=3, max_length=64)
    manifest_schema_version: int = Field(default=1, ge=1, le=2)
    sdk: dict[str, Any] | None = None
    plugin_version: str = Field(..., min_length=5, max_length=32)
    display_name: str = Field(..., min_length=1, max_length=128)
    description: str = ""
    usr_compatibility: str = Field(default=">=0.1.0")
    capabilities: list[str] = Field(default_factory=list)
    task_types: list[dict[str, Any]] = Field(
        default_factory=list,
        validation_alias=AliasChoices("task_types", "job_types"),
        serialization_alias="task_types",
    )
    event_subscriptions: list[dict[str, Any]] = Field(default_factory=list)
    callbacks: list[dict[str, Any]] = Field(default_factory=list)
    outbound_deliveries: list[dict[str, Any]] = Field(default_factory=list)
    operational_policy: dict[str, Any] = Field(default_factory=dict)
    remote_delegation: dict[str, Any] | None = Field(default=None)
    webui: dict[str, Any] | None = Field(default=None)


class PluginManifestUpdateRequest(BaseModel):
    manifest_yaml: str = Field(..., min_length=1)


class PluginPurgeRequest(BaseModel):
    confirmation: str = Field(..., min_length=1)


class DelegatedJobSubmitRequest(BaseModel):
    """Request body for submitting a delegated job to a remote system."""

    plugin_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class RemoteExecutorActionRequest(BaseModel):
    """Request body for executor actions on the remote control plane."""

    action: str = Field(
        ...,
        description="One of: claim, heartbeat, progress, complete, fail, cancel",
    )
    remote_job_id: str = Field(default="")
    payload: dict[str, Any] = Field(default_factory=dict)


class SetExternalJobIdRequest(BaseModel):
    """Associate an external job ID with a submission correlation."""

    external_job_id: str = Field(..., min_length=1)


# ── Plugin listing and inspection ─────────────────────────────────────────


@router.get("")
async def list_plugins(
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.list_plugins()


@router.post("", status_code=201)
async def create_plugin(
    request: PluginCreateRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    manifest_data = request.model_dump(exclude_none=True)
    try:
        return platform.create_plugin(manifest_data)
    except (ValueError, OSError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/metadata")
async def get_plugin_metadata() -> dict[str, Any]:
    """Return backend-owned manifest choices used by operator clients."""
    return {
        "manifest_schema_version": 2,
        "supported_manifest_schema_versions": [1, 2],
        "sdk_version": SDK_VERSION,
        "slots": list(SLOTS),
        "capabilities": sorted(capability.value for capability in PluginCapability),
        "event_families": sorted(ALLOWED_EVENT_FAMILIES),
    }


@router.get("/remote-status")
async def get_remote_delegation_overview(
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    """Aggregate status across all remote delegation activity."""
    return platform.get_remote_delegation_overview()


@router.get("/active-webui")
async def list_active_webui(
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    """Return active plugins providing WebUI extensions."""
    return platform.list_active_webui()


@router.get("/{plugin_id}/assets/{asset_path:path}")
async def get_plugin_asset(
    plugin_id: str,
    asset_path: str,
    platform: Any = Depends(_get_platform),
    package: str | None = Query(default=None, pattern=r"^[a-f0-9]{64}$"),
) -> FileResponse:
    """Serve a static WebUI asset from a plugin directory."""
    import mimetypes

    try:
        file_path = platform.get_plugin_asset_path(plugin_id, asset_path, package=package)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    media_type, _ = mimetypes.guess_type(str(file_path))
    if file_path.suffix == ".js":
        media_type = "application/javascript"
    elif file_path.suffix == ".css":
        media_type = "text/css"

    return FileResponse(file_path, media_type=media_type, headers={
        "X-Content-Type-Options": "nosniff", "Cache-Control": "private, no-store",
        "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src data:; connect-src 'none'; frame-ancestors *",
    })


@router.get("/{plugin_id}")
async def get_plugin(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    result = platform.get_plugin(plugin_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Plugin {plugin_id!r} not found")
    return result


@router.get("/{plugin_id}/manifest")
async def get_plugin_manifest(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.get_plugin_manifest_yaml(plugin_id)
    except (KeyError, FileNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.put("/{plugin_id}/manifest")
async def update_plugin_manifest(
    plugin_id: str,
    request: PluginManifestUpdateRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.update_plugin_manifest_yaml(plugin_id, request.manifest_yaml)
    except (KeyError, FileNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ── Lifecycle management ──────────────────────────────────────────────────


@router.post("/{plugin_id}/enable")
async def enable_plugin(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.enable_plugin(plugin_id)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{plugin_id}/preflight")
async def preflight_plugin(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.preflight_plugin(plugin_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{plugin_id}/capabilities/approve")
async def approve_plugin_capabilities(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.approve_capabilities(plugin_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{plugin_id}/disable")
async def disable_plugin(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.disable_plugin(plugin_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/{plugin_id}/data")
async def purge_plugin_data(
    plugin_id: str,
    request: PluginPurgeRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.purge_plugin_data(plugin_id, request.confirmation)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/{plugin_id}/reload")
async def reload_plugin(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    return platform.reload_plugin(plugin_id)


# ── Job submission (capability-gated) ─────────────────────────────────────


@router.post("/{plugin_id}/jobs/submit")
async def submit_job(
    plugin_id: str,
    request: PluginJobSubmitRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        from ..database.threads import run_database_call
        return await run_database_call(platform.submit_execution, plugin_id, request.task_type, request.payload, idempotency_key=request.idempotency_key)
    except (KeyError, ValueError, PermissionError, RuntimeError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc


@router.post("/{plugin_id}/token")
async def create_plugin_token(plugin_id: str, platform: Any = Depends(_get_platform)):
    platform._require_enabled(plugin_id)
    return {"token": platform.repository.issue_api_token(plugin_id), "expires_in": 300}


@router.put("/{plugin_id}/configuration")
async def configure_plugin(plugin_id: str, configuration: dict[str, Any], platform: Any = Depends(_get_platform)):
    try:
        return platform.set_configuration(plugin_id, configuration)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{plugin_id}/configuration")
async def read_plugin_configuration(plugin_id: str, platform: Any = Depends(_get_platform)):
    record = platform._require_known(plugin_id)
    return platform.configuration(record.manifest) if record.manifest else {}


@router.get("/{plugin_id}/settings")
async def read_plugin_settings(plugin_id: str, platform: Any = Depends(_get_platform)):
    try:
        return platform.plugin_settings(plugin_id)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class PluginConnectionTestRequest(BaseModel):
    target: str
    configuration: dict[str, Any] = Field(default_factory=dict)


@router.post("/{plugin_id}/settings/test")
async def test_plugin_connection(plugin_id: str, request: PluginConnectionTestRequest, platform: Any = Depends(_get_platform)):
    try:
        return await platform.test_plugin_connection(plugin_id, request.target, request.configuration)
    except Exception as exc:
        # Avoid exposing transport URLs or connection credentials in the API.
        if isinstance(exc, (ValueError, KeyError, PermissionError)):
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail="Connection failed or timed out") from exc


@router.post("/{plugin_id}/packages/stage")
async def stage_plugin_package(plugin_id: str, request: PluginManifestUpdateRequest, platform: Any = Depends(_get_platform)):
    import yaml
    try:
        manifest = yaml.safe_load(request.manifest_yaml)
        if not isinstance(manifest, dict):
            raise ValueError("Manifest must be a mapping")
        return platform.stage_upgrade(plugin_id, manifest)
    except (ValueError, KeyError, yaml.YAMLError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{plugin_id}/packages/activate")
async def activate_plugin_package(plugin_id: str, request: PluginActivationRequest, platform: Any = Depends(_get_platform)):
    try:
        return platform.activate_upgrade(plugin_id, request.package_digest, approve_permissions=request.approve_permissions)
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=403 if isinstance(exc, PermissionError) else 400, detail=str(exc)) from exc


@router.post("/{plugin_id}/packages/rollback")
async def rollback_plugin_package(plugin_id: str, platform: Any = Depends(_get_platform)):
    try:
        return platform.rollback_package(plugin_id)
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=403 if isinstance(exc, PermissionError) else 400, detail=str(exc)) from exc


# ── Secrets ───────────────────────────────────────────────────────────────


@router.get("/{plugin_id}/secrets")
async def list_secrets(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.list_secrets(plugin_id)


@router.post("/{plugin_id}/secrets/create")
async def create_secret(
    plugin_id: str,
    request: SecretCreateRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, str]:
    try:
        raw = platform.create_secret(plugin_id, request.scope_kind, request.scope_name)
        return {"raw_secret": raw, "note": "This value will not be shown again."}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{plugin_id}/secrets/rotate")
async def rotate_secret(
    plugin_id: str,
    request: SecretRotateRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, str]:
    try:
        raw = platform.rotate_secret(plugin_id, request.scope_kind, request.scope_name)
        return {"raw_secret": raw, "note": "This value will not be shown again."}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{plugin_id}/secrets/set")
async def set_secret(
    plugin_id: str,
    request: SecretSetRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, str]:
    try:
        platform.set_secret(
            plugin_id,
            request.scope_kind,
            request.scope_name,
            request.raw_secret,
        )
        return {"status": "ok", "note": "Secret stored successfully."}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ── State management ──────────────────────────────────────────────────────


@router.get("/{plugin_id}/state")
async def list_state(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.list_state(plugin_id)


@router.get("/{plugin_id}/state/{key}")
async def get_state(
    plugin_id: str,
    key: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    value = platform.get_state(plugin_id, key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"State key {key!r} not found")
    return {"key": key, "value": value}


@router.put("/{plugin_id}/state")
async def set_state(
    plugin_id: str,
    request: PluginStateSetRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, str]:
    try:
        platform.set_state(
            plugin_id,
            request.key,
            request.value,
            actor="operator",
            reason=request.reason,
        )
        return {"status": "ok"}
    except (KeyError, ValueError) as exc:
        status = 404 if isinstance(exc, KeyError) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc


@router.delete("/{plugin_id}/state/{key}")
async def delete_state(
    plugin_id: str,
    key: str,
    reason: str = Query(default=""),
    platform: Any = Depends(_get_platform),
) -> dict[str, str]:
    try:
        deleted = platform.delete_state(plugin_id, key, actor="operator", reason=reason)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"State key {key!r} not found")
        return {"status": "deleted"}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{plugin_id}/state-audit")
async def state_audit(
    plugin_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.get_state_audit(plugin_id, limit=limit)


# ── Delivery observability ────────────────────────────────────────────────


@router.get("/{plugin_id}/delivery-status")
async def delivery_status(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.delivery_statuses(plugin_id)


@router.get("/{plugin_id}/deliveries")
async def recent_deliveries(
    plugin_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.recent_deliveries(plugin_id, limit=limit)


# ── Worker status (capability-gated) ─────────────────────────────────────


@router.get("/{plugin_id}/workers")
async def read_worker_status(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return platform.read_worker_status(plugin_id)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ── Remote delegation ─────────────────────────────────────────────────────


@router.get("/{plugin_id}/remote-delegation")
async def get_remote_delegation(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    info = platform.get_remote_delegation_info(plugin_id)
    if info is None:
        raise HTTPException(
            status_code=404,
            detail=f"Plugin {plugin_id!r} has no remote delegation configuration",
        )
    return info


@router.post("/{plugin_id}/remote-delegation/submit")
async def submit_delegated_job(
    plugin_id: str,
    request: DelegatedJobSubmitRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    """Validate and execute a delegated job submission to the remote system."""
    try:
        submission = platform.submit_delegated_job(plugin_id, request.payload)
        result = await platform.execute_remote_submission(
            submission,
            idempotency_key=request.idempotency_key,
        )
        return result
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc


@router.post("/{plugin_id}/remote-delegation/callbacks/{handler_id}")
async def receive_remote_callback(
    plugin_id: str,
    handler_id: str,
    request: Request,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                declared_size = int(content_length)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid Content-Length") from exc
            if declared_size > MAX_PLUGIN_CALLBACK_BODY_BYTES:
                raise HTTPException(status_code=413, detail="Callback payload is too large")
        raw_body = await _read_bounded_callback_body(request)
        platform.verify_remote_callback_request(
            plugin_id,
            handler_id,
            raw_body=raw_body,
            headers={k: v for k, v in request.headers.items()},
        )
        payload: dict[str, Any] = {}
        if raw_body:
            try:
                parsed = json.loads(raw_body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise HTTPException(
                    status_code=400, detail=f"Invalid JSON callback payload: {exc}"
                ) from exc
            if not isinstance(parsed, dict):
                raise HTTPException(
                    status_code=400, detail="Callback payload must be a JSON object"
                )
            payload = parsed
        return platform.receive_remote_callback(
            plugin_id,
            handler_id,
            payload,
            headers={k: v for k, v in request.headers.items()},
        )
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc


# ── Remote executor actions ───────────────────────────────────────────────


@router.post("/{plugin_id}/remote-delegation/executor-action")
async def execute_remote_action(
    plugin_id: str,
    request: RemoteExecutorActionRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    """Execute a worker action (claim/heartbeat/progress/complete/fail) on the remote control plane."""
    try:
        return await platform.execute_remote_worker_action(
            plugin_id, request.action, request.remote_job_id, request.payload
        )
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc


# ── Remote submission correlation ─────────────────────────────────────────


@router.post("/{plugin_id}/remote-delegation/submissions/{correlation_id}/external-id")
async def set_external_job_id(
    plugin_id: str,
    correlation_id: str,
    request: SetExternalJobIdRequest,
    platform: Any = Depends(_get_platform),
) -> dict[str, str]:
    """Associate an external job ID (from the control plane) with a tracked submission."""
    try:
        existing = platform.get_remote_submission(correlation_id)
        if existing is None or existing.get("plugin_id") != plugin_id:
            raise KeyError(
                f"No submission found for plugin {plugin_id!r} and "
                f"correlation_id {correlation_id!r}"
            )
        platform.set_external_job_id(correlation_id, request.external_job_id)
        return {
            "status": "ok",
            "correlation_id": correlation_id,
            "external_job_id": request.external_job_id,
        }
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{plugin_id}/remote-delegation/submissions/{correlation_id}/poll")
async def poll_remote_status(
    plugin_id: str,
    correlation_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    """Poll the remote control plane for the current status of a submitted job."""
    try:
        return await platform.poll_remote_job_status(plugin_id, correlation_id)
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc


# ── Remote submission observability ───────────────────────────────────────


@router.get("/{plugin_id}/remote-delegation/submissions")
async def list_remote_submissions(
    plugin_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    """List all tracked remote submissions for a plugin."""
    return platform.list_remote_submissions(plugin_id, limit=limit)


@router.get("/{plugin_id}/remote-delegation/submissions/{correlation_id}")
async def get_remote_submission(
    plugin_id: str,
    correlation_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    """Get details for a single remote submission by correlation ID."""
    result = platform.get_remote_submission(correlation_id)
    if result is None or result.get("plugin_id") != plugin_id:
        raise HTTPException(
            status_code=404,
            detail=f"No submission found for correlation_id {correlation_id!r}",
        )
    return result


@router.get("/{plugin_id}/remote-delegation/runtime/correlations")
async def list_runtime_correlations(
    plugin_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    platform: Any = Depends(_get_platform),
) -> list[dict[str, Any]]:
    return platform.list_runtime_correlations(plugin_id, limit=limit)


@router.get("/{plugin_id}/remote-delegation/runtime/correlations/{correlation_id}")
async def get_runtime_correlation(
    plugin_id: str,
    correlation_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    result = platform.get_runtime_correlation(correlation_id)
    if result is None or result.get("plugin_id") != plugin_id:
        raise HTTPException(
            status_code=404,
            detail=f"No runtime correlation found for {correlation_id!r}",
        )
    return result


@router.get("/{plugin_id}/remote-delegation/runtime/health")
async def get_runtime_health(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    return platform.get_supervisor_health(plugin_id)


@router.get("/{plugin_id}/remote-delegation/runtime/websocket")
async def get_websocket_session(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    result = platform.get_websocket_session(plugin_id)
    if result is None:
        raise HTTPException(status_code=404, detail="No websocket session record found")
    return result


@router.get("/{plugin_id}/remote-delegation/runtime/callback-registration")
async def get_callback_registration(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    result = platform.get_callback_registration_status(plugin_id)
    if result is None:
        raise HTTPException(
            status_code=404, detail="No callback registration record found"
        )
    return result


@router.post("/{plugin_id}/remote-delegation/runtime/callback-registration/re-register")
async def re_register_callback_registration(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return await platform.re_register_callbacks(plugin_id)
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc


@router.post(
    "/{plugin_id}/remote-delegation/runtime/correlations/{correlation_id}/resync"
)
async def resync_runtime_correlation(
    plugin_id: str,
    correlation_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return await platform.resync_runtime_correlation(plugin_id, correlation_id)
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc


@router.post("/{plugin_id}/remote-delegation/runtime/websocket/replay")
async def replay_websocket_backlog(
    plugin_id: str,
    platform: Any = Depends(_get_platform),
) -> dict[str, Any]:
    try:
        return await platform.replay_websocket_backlog(plugin_id)
    except (KeyError, ValueError, PermissionError) as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        if isinstance(exc, KeyError):
            status = 404
        raise HTTPException(status_code=status, detail=str(exc)) from exc
