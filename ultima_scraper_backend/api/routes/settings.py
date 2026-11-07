"""Settings API routes.

Provides endpoints for managing application configuration,
including remote config editing for other backends via Redis.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import update as sa_update
from sqlalchemy.orm import selectinload
from sqlmodel import or_, select
from ultima_scraper_core import archive_database_url
from ultima_scraper_db_archive.crypto import (
    decrypt_secret_value,
    encrypt_secret_value,
)

from ultima_scraper_backend.config import (
    BackendSettings,
    DownloadPathSetup,
    GlobalXPathSetup,
    JobExecutionSettings,
    Network as NetworkConfig,
    Redis as RedisConfig,
    SecuritySettings,
    UltimaScraperCollectionConfig,
    WorkerPool,
    decrypt_config_secrets,
)
from ultima_scraper_backend.config_settings_sync import (
    ConfigVersionConflict,
    ConfigVersionRequired,
    persist_config_snapshot,
)
from ultima_scraper_backend.models.backend import BackendInstance
from ultima_scraper_backend.redis_channels import (
    CONFIG_EVENTS_CHANNEL,
    CONFIG_KEY_PREFIX,
)
from ultima_scraper_backend.redis_events import JsonValue, publish_ws_event
from ultima_scraper_backend.storage_config_propagation import (
    StorageConfigPropagation,
    propagate_storage_config,
)

from ...job_system.path_formatter import invalid_format_tokens
from ...services.settings_service import SECRET_MASK, SettingsService
from ..dependencies import get_backend_instance

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])


class SiteApiConfig(BaseModel):
    """Site-specific API configuration."""

    download_directories: list[str] = []
    blacklists: list[str] = []


class SitesConfig(BaseModel):
    """All sites configuration."""

    onlyfans: SiteApiConfig = SiteApiConfig()
    fansly: SiteApiConfig = SiteApiConfig()


class DRMConfig(BaseModel):
    """DRM configuration."""

    model_config = {"extra": "forbid"}

    device_client_blob_filepath: str | None = None
    device_private_key_filepath: str | None = None


class SettingsResponse(BaseModel):
    """Full settings response - mirrors UltimaScraperCollectionConfig.Settings."""

    # General
    infinite_loop: bool = False
    exit_on_completion: bool = True
    hostname: str

    # Network / Backend
    worker_pool: WorkerPool
    backend: BackendSettings
    job_execution: JobExecutionSettings = JobExecutionSettings()
    redis: RedisConfig  # Top-level Redis config
    network: NetworkConfig
    security: SecuritySettings

    # Paths
    download_setup: DownloadPathSetup
    metadata_setup: GlobalXPathSetup
    previews_directory: str | None = None
    subtitles_directory: str | None = None

    # DRM
    drm: DRMConfig = DRMConfig()

    # Sites
    sites: SitesConfig

    # Config path info
    config_path: str | None = None

    # Setup status
    is_configured: bool = False
    config_scope: str | None = None
    config_version: int | None = None
    config_updated_at: str | None = None
    storage_propagation: StorageConfigPropagation | None = None
    configuration_source: str = "database"
    runtime_readonly_fields: list[str] = ["hostname", "security", "backend.backend_id"]
    runtime_restart_required: bool = True


class SettingsUpdateRequest(BaseModel):
    """Settings update request (partial updates allowed)."""

    model_config = {"extra": "forbid"}

    infinite_loop: bool | None = None
    exit_on_completion: bool | None = None
    hostname: str | None = None
    worker_pool: WorkerPool | None = None
    backend: BackendSettings | None = None
    job_execution: JobExecutionSettings | None = None
    redis: RedisConfig | None = None
    network: NetworkConfig | None = None
    security: SecuritySettings | None = None
    download_setup: DownloadPathSetup | None = None
    metadata_setup: GlobalXPathSetup | None = None
    drm: DRMConfig | None = None
    previews_directory: str | None = None
    subtitles_directory: str | None = None
    sites: SitesConfig | None = None
    is_configured: bool | None = None
    expected_version: int | None = None
    actor_backend_id: int | None = None
    request_id: str | None = None


def _current_config_scope(config: UltimaScraperCollectionConfig) -> str:
    return config.config_write_backend()


def _load_config_row(scope: str):
    from ultima_scraper_db_archive.global_db import (
        SettingsModel,
        get_global_session,
    )

    with get_global_session() as session:
        row = session.exec(
            select(SettingsModel).where(
                SettingsModel.backend == scope,
                SettingsModel.key == "config",
            )
        ).first()
        if row is not None:
            session.expunge(row)
        return row


def _extract_attempted_patch(update: SettingsUpdateRequest) -> dict[str, JsonValue]:
    data = update.model_dump(exclude_none=True)
    for reserved_key in ("expected_version", "actor_backend_id", "request_id"):
        data.pop(reserved_key, None)
    return data


def _merge_model_update(
    target: BaseModel,
    patch: BaseModel,
    raw_patch: dict[str, Any],
) -> None:
    for field_name, raw_value in raw_patch.items():
        if field_name in {"plugin_sync_receipts", "managed_by"}:
            continue
        value = getattr(patch, field_name)
        current_value = getattr(target, field_name, None)
        if (
            isinstance(raw_value, dict)
            and isinstance(value, BaseModel)
            and isinstance(current_value, BaseModel)
        ):
            _merge_model_update(current_value, value, raw_value)
            continue
        if field_name == "password" and value in (SECRET_MASK, "***"):
            continue
        if field_name == "proxies" and isinstance(value, list):
            existing = {(proxy.url, proxy.username): proxy for proxy in current_value or []}
            owners = {proxy.url: proxy.managed_by for proxy in current_value or []}
            for proxy in value:
                proxy.managed_by = owners.get(proxy.url)
                if proxy.password in (SECRET_MASK, "***"):
                    prior = existing.get((proxy.url, proxy.username))
                    if prior is None:
                        raise HTTPException(status_code=422, detail="A new proxy requires its password")
                    proxy.password = prior.password
        setattr(target, field_name, value)


async def _publish_settings_updated_event(
    backend: BackendInstance | None,
    *,
    settings_id: int,
    settings_scope: str,
    version: int,
    updated_at: str | None,
    changed_fields: list[str],
    actor_backend_id: int | None,
    request_id: str | None,
    target_backend_id: str | None = None,
) -> None:
    if not backend or not backend.redis or not backend.redis.is_connected:
        return
    payload = {
        "type": "settings_updated",
        "settings_id": settings_id,
        "settings_scope": settings_scope,
        "version": version,
        "updated_at": updated_at,
        "changed_fields": changed_fields,
        "actor_backend_id": actor_backend_id,
        "request_id": request_id,
    }
    if target_backend_id:
        payload["target_backend_id"] = target_backend_id
        payload["backend_id"] = target_backend_id
    await backend.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(payload))


async def _publish_worker_pool_runtime_events(
    backend: BackendInstance | None,
    *,
    settings_scope: str,
    old_size: int,
    new_size: int,
    old_max_claimed_jobs: int,
    new_max_claimed_jobs: int,
    target_backend_id: str | None = None,
) -> None:
    if not backend or not backend.redis or not backend.redis.is_connected:
        return

    target = target_backend_id or (backend.instance_id if backend else None)
    if new_size != old_size:
        scale_event: dict[str, Any] = {
            "type": "worker_pool_scale",
            "target_size": new_size,
            "backend_id": target,
            "target_backend_id": target,
        }
        if not target_backend_id:
            scale_event["settings_scope"] = settings_scope
        await backend.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(scale_event))
        logger.info(
            "Published worker_pool_scale event for target %s (scope %s): %s → %s",
            target,
            settings_scope,
            old_size,
            new_size,
        )

    if new_max_claimed_jobs != old_max_claimed_jobs:
        claimed_event: dict[str, Any] = {
            "type": "max_claimed_jobs_update",
            "max_claimed_jobs": new_max_claimed_jobs,
            "backend_id": target,
            "target_backend_id": target,
        }
        if not target_backend_id:
            claimed_event["settings_scope"] = settings_scope
        await backend.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(claimed_event))
        logger.info(
            "Published max_claimed_jobs_update event for target %s (scope %s): %s → %s",
            target,
            settings_scope,
            old_max_claimed_jobs,
            new_max_claimed_jobs,
        )


def _config_to_response(config: UltimaScraperCollectionConfig) -> SettingsResponse:
    """Convert config object to response model."""
    settings = config.settings
    redis = settings.redis.model_copy(deep=True)
    network = settings.network.model_copy(deep=True)
    security = settings.security.model_copy(deep=True)

    if redis.password:
        redis.password = SECRET_MASK
    if getattr(network, "proxies", None):
        for proxy in network.proxies:
            if getattr(proxy, "password", None):
                proxy.password = SECRET_MASK

    # Get blacklists safely using getattr
    onlyfans_blacklists = getattr(config.site_apis.onlyfans, "blacklists", [])
    fansly_blacklists = getattr(config.site_apis.fansly, "blacklists", [])

    config_scope = _current_config_scope(config)
    config_path = None

    return SettingsResponse(
        hostname=settings.hostname,
        infinite_loop=settings.infinite_loop,
        exit_on_completion=settings.exit_on_completion,
        worker_pool=settings.worker_pool,
        backend=settings.backend,
        job_execution=settings.job_execution,
        redis=redis,
        network=network,
        security=security,
        download_setup=settings.download_setup,
        metadata_setup=settings.metadata_setup,
        previews_directory=(
            str(settings.previews_directory) if settings.previews_directory else None
        ),
        subtitles_directory=(
            str(settings.subtitles_directory) if settings.subtitles_directory else None
        ),
        sites=SitesConfig(
            onlyfans=SiteApiConfig(
                blacklists=onlyfans_blacklists,
            ),
            fansly=SiteApiConfig(
                blacklists=fansly_blacklists,
            ),
        ),
        drm=DRMConfig(
            device_client_blob_filepath=(
                str(settings.drm.device_client_blob_filepath)
                if settings.drm.device_client_blob_filepath
                else None
            ),
            device_private_key_filepath=(
                str(settings.drm.device_private_key_filepath)
                if settings.drm.device_private_key_filepath
                else None
            ),
        ),
        config_path=config_path,
        is_configured=getattr(config, "is_configured", False),
        config_scope=config_scope,
        config_version=config._settings_version,
        config_updated_at=config._settings_updated_at,
        configuration_source="database" if config._settings_id is not None else "defaults",
    )


_REDIS_ENVIRONMENT_KEYS = (
    "REDIS_URL",
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_DB",
    "REDIS_PASSWORD",
    "REDIS_ENABLED",
)


def _redis_environment_values(redis: RedisConfig) -> dict[str, object | None]:
    """Translate editable Redis settings into the shared runtime environment."""
    host = redis.host.strip()
    if not host:
        raise HTTPException(status_code=422, detail="Redis host must not be empty.")
    if not 1 <= redis.port <= 65535:
        raise HTTPException(
            status_code=422,
            detail="Redis port must be between 1 and 65535.",
        )
    if redis.db < 0:
        raise HTTPException(status_code=422, detail="Redis database must not be negative.")

    password_is_masked = redis.password in (SECRET_MASK, "***")
    values: dict[str, object | None] = {
        "REDIS_HOST": host,
        "REDIS_PORT": redis.port,
        "REDIS_DB": redis.db,
        "REDIS_ENABLED": str(redis.enabled).lower(),
    }
    # The API masks existing passwords. A masked value means "keep it";
    # null/empty explicitly removes the shared password.
    if not password_is_masked:
        # Individual values become authoritative for the next process start.
        values["REDIS_URL"] = None
        values["REDIS_PASSWORD"] = redis.password or None
    return values


def _process_redis_environment_overrides() -> list[str]:
    """Return process-owned Redis keys that take precedence over shared .env."""
    return [name for name in _REDIS_ENVIRONMENT_KEYS if name in os.environ]


@router.get("/", response_model=SettingsResponse)
async def get_settings() -> SettingsResponse:
    """Get current application settings."""
    try:
        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        return _config_to_response(config)
    except Exception as e:
        logger.exception("Failed to load settings")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/runtime/redis", response_model=SettingsResponse)
async def update_runtime_redis_settings(redis: RedisConfig) -> SettingsResponse:
    """Update Redis bootstrap values in the shared environment file.

    Redis is deployment-owned rather than part of the canonical application
    settings row. The running clients are intentionally not rebuilt here;
    callers must restart the affected backend/worker processes.
    """
    process_overrides = _process_redis_environment_overrides()
    if process_overrides:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    "Redis is supplied by the running process environment. "
                    "Change the deployment environment and restart instead."
                ),
                "fields": process_overrides,
            },
        )

    try:
        from ultima_scraper_core import update_env

        update_env(_redis_environment_values(redis))
        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        return _config_to_response(config)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to update Redis runtime settings")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _validate_format_tokens(raw_update: dict[str, Any]) -> None:
    """Reject path formats that name a token the formatter cannot render."""
    problems: list[str] = []
    fields: list[str] = []
    for section in ("download_setup", "metadata_setup"):
        values = cast(dict[str, Any], raw_update.get(section) or {})
        templates = [
            (f"{section}.{key}", values[key])
            for key in ("directory_format", "filename_format")
            if values.get(key)
        ]
        templates += [
            (f"{section}.alt_directory_formats", template)
            for template in values.get("alt_directory_formats") or []
        ]
        for field, template in templates:
            for token in invalid_format_tokens(str(template)):
                problems.append(f"{{{token}}} is not a valid token in {field}")
                if field not in fields:
                    fields.append(field)
    if problems:
        raise HTTPException(
            status_code=422,
            detail={"message": "; ".join(problems) + ".", "format_fields": fields},
        )


def _apply_settings_update_to_config(
    config: UltimaScraperCollectionConfig,
    update: SettingsUpdateRequest,
) -> list[str]:
    settings = config.settings
    raw_update = update.model_dump(mode="json", exclude_unset=True)
    _validate_format_tokens(raw_update)

    changed_sections: list[str] = []

    for field in ("infinite_loop", "exit_on_completion"):
        value = getattr(update, field)
        if value is not None:
            setattr(settings, field, value)
            changed_sections.append(field)

    # Update worker pool
    if update.worker_pool is not None:
        _merge_model_update(
            settings.worker_pool,
            update.worker_pool,
            cast(dict[str, Any], raw_update.get("worker_pool", {})),
        )
        changed_sections.append("worker_pool")

    # Update backend
    if update.backend is not None:
        _merge_model_update(
            settings.backend,
            update.backend,
            cast(dict[str, Any], raw_update.get("backend", {})),
        )
        changed_sections.append("backend")

    if update.job_execution is not None:
        _merge_model_update(
            settings.job_execution,
            update.job_execution,
            cast(dict[str, Any], raw_update.get("job_execution", {})),
        )
        changed_sections.append("job_execution")

    # Update network
    if update.network is not None:
        _merge_model_update(
            settings.network,
            update.network,
            cast(dict[str, Any], raw_update.get("network", {})),
        )
        changed_sections.append("network")

    # Update download setup
    if update.download_setup is not None:
        _merge_model_update(
            settings.download_setup,
            update.download_setup,
            cast(dict[str, Any], raw_update.get("download_setup", {})),
        )
        changed_sections.append("download_setup")

    # Update metadata setup
    if update.metadata_setup is not None:
        _merge_model_update(
            settings.metadata_setup,
            update.metadata_setup,
            cast(dict[str, Any], raw_update.get("metadata_setup", {})),
        )
        changed_sections.append("metadata_setup")

    # Update DRM
    if update.drm is not None:
        from pathlib import Path

        settings.drm.device_client_blob_filepath = (
            Path(update.drm.device_client_blob_filepath)
            if update.drm.device_client_blob_filepath
            else None
        )
        settings.drm.device_private_key_filepath = (
            Path(update.drm.device_private_key_filepath)
            if update.drm.device_private_key_filepath
            else None
        )
        changed_sections.append("drm")

    # Update preview/subtitle directories
    if update.previews_directory is not None:
        from pathlib import Path

        settings.previews_directory = (
            Path(update.previews_directory) if update.previews_directory else None
        )
        changed_sections.append("paths")
    if update.subtitles_directory is not None:
        from pathlib import Path

        settings.subtitles_directory = (
            Path(update.subtitles_directory) if update.subtitles_directory else None
        )
        changed_sections.append("paths")

    # Update sites
    if update.sites is not None:
        _merge_model_update(
            config.site_apis,
            update.sites,
            cast(dict[str, Any], raw_update.get("sites", {})),
        )
        changed_sections.append("sites")

    return changed_sections


@router.put("/", response_model=SettingsResponse)
async def update_settings(
    update: SettingsUpdateRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> SettingsResponse:
    """Update application settings.

    Only provided fields will be updated.
    """
    try:
        runtime_fields = {"hostname", "redis", "security", "is_configured"} & update.model_fields_set
        if update.backend is not None and "backend_id" in update.backend.model_fields_set:
            runtime_fields.add("backend.backend_id")
        if runtime_fields:
            raise HTTPException(status_code=422, detail={
                "message": "Deployment settings are read-only here; update the environment and restart.",
                "fields": sorted(runtime_fields),
            })
        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        settings = config.settings
        old_worker_pool_size = settings.worker_pool.size
        old_max_claimed_jobs = settings.worker_pool.max_claimed_jobs
        attempted_patch = _extract_attempted_patch(update)

        changed_sections = _apply_settings_update_to_config(config, update)

        # Nothing to persist.
        if not changed_sections:
            return _config_to_response(config)

        target_scope = _current_config_scope(config)
        try:
            persisted_snapshot = persist_config_snapshot(
                config,
                expected_version=update.expected_version,
            )
        except ConfigVersionRequired as exc:
            raise HTTPException(
                status_code=400,
                detail="expected_version is required for settings updates.",
            ) from exc
        except ConfigVersionConflict as exc:
            latest_row = _load_config_row(target_scope)
            current_state: dict[str, JsonValue] = {}
            current_updated_at: str | None = None

            if latest_row is not None:
                current_updated_at = latest_row.updated_at.isoformat()
                try:
                    latest_payload = json.loads(latest_row.value)
                    decrypt_config_secrets(latest_payload)
                    latest_config = UltimaScraperCollectionConfig(**latest_payload)
                    setattr(latest_config, "_settings_backend", latest_row.backend)
                    current_state = _config_to_response(latest_config).model_dump(
                        mode="json"
                    )
                except Exception:
                    current_state = {"error": "Stored configuration cannot be decoded"}

            conflicting_fields = sorted(
                key
                for key, value in attempted_patch.items()
                if current_state.get(key) != value
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Settings have changed since you loaded them.",
                    "current": current_state,
                    "current_version": exc.current_version,
                    "current_updated_at": current_updated_at,
                    "attempted_patch": attempted_patch,
                    "conflicting_fields": conflicting_fields,
                },
            ) from exc

        if "job_execution" in changed_sections and backend.redis:
            from ...job_system import mps_state

            await mps_state.sync_runtime_settings(backend.redis.client, settings)

        storage_propagation: StorageConfigPropagation | None = None
        storage_changed = {"download_setup", "metadata_setup"} & set(changed_sections)
        if storage_changed:
            storage_propagation = await propagate_storage_config(
                backend,
                config,
                changed_sections=storage_changed,
                config_version=persisted_snapshot.version,
            )

        # Handle worker pool scaling if size or max_claimed_jobs changed
        if "worker_pool" in changed_sections:
            new_size = settings.worker_pool.size
            new_max_claimed = settings.worker_pool.max_claimed_jobs
            if backend.redis:
                await _publish_worker_pool_runtime_events(
                    backend,
                    settings_scope=target_scope,
                    old_size=old_worker_pool_size,
                    new_size=new_size,
                    old_max_claimed_jobs=old_max_claimed_jobs,
                    new_max_claimed_jobs=new_max_claimed,
                )
            # Sync backend registry max_workers if registry is active
            registry = getattr(backend, "_registry", None)
            if registry is not None and hasattr(registry, "update_max_workers"):
                await registry.update_max_workers(new_size)

        sections: list[str] = sorted(set(changed_sections))
        sections_payload: list[JsonValue] = [section for section in sections]
        await publish_ws_event(
            backend.redis,
            {
                "type": "config_updated",
                "sections": sections_payload,
            },
        )

        await _publish_settings_updated_event(
            backend,
            settings_id=persisted_snapshot.settings_id,
            settings_scope=persisted_snapshot.settings_scope,
            version=persisted_snapshot.version,
            updated_at=persisted_snapshot.updated_at,
            changed_fields=sections,
            actor_backend_id=update.actor_backend_id,
            request_id=update.request_id,
            target_backend_id=backend.instance_id,
        )

        response = _config_to_response(config)
        response.storage_propagation = storage_propagation
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to update settings")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/config-path")
async def get_config_path() -> dict[str, str]:
    """Get the path to the configuration database."""
    try:
        from ultima_scraper_db_archive.global_db import (
            get_env_path,
            get_postgres_engine,
        )

        if get_postgres_engine() is not None:
            return {"path": str(get_env_path())}
        return {"path": ""}
    except Exception as e:
        logger.exception("Failed to get config path")
        raise HTTPException(status_code=500, detail=str(e)) from e


class TestRedisRequest(BaseModel):
    """Request to test Redis connection."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None


class TestRedisResponse(BaseModel):
    """Response from Redis connection test."""

    success: bool
    message: str
    latency_ms: float | None = None


@router.post("/test-redis", response_model=TestRedisResponse)
async def test_redis_connection(request: TestRedisRequest) -> TestRedisResponse:
    """Test Redis connection with provided settings.

    Tests connectivity to Redis server without modifying any configuration.
    """
    import asyncio
    import time

    try:
        import redis.asyncio as aioredis
    except ImportError:
        return TestRedisResponse(
            success=False,
            message="Redis library not installed (pip install redis)",
        )

    try:
        start = time.perf_counter()

        # Create connection with short timeout
        client = aioredis.Redis(
            host=request.host,
            port=request.port,
            db=request.db,
            password=request.password,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
        )

        # Test connection with PING
        try:
            result = await asyncio.wait_for(cast(Any, client).ping(), timeout=5.0)
            latency_ms = (time.perf_counter() - start) * 1000

            if result:
                # Get some info
                info = cast(dict[str, Any], await cast(Any, client).info("server"))
                redis_version = info.get("redis_version", "unknown")

                await client.aclose()

                return TestRedisResponse(
                    success=True,
                    message=f"Connected to Redis {redis_version}",
                    latency_ms=round(latency_ms, 2),
                )
            else:
                await client.aclose()
                return TestRedisResponse(
                    success=False,
                    message="PING failed - unexpected response",
                )
        finally:
            await client.aclose()

    except asyncio.TimeoutError:
        return TestRedisResponse(
            success=False,
            message="Connection timeout (5s)",
        )
    except Exception as e:
        error_msg = str(e)
        # Clean up common error messages
        if "Connection refused" in error_msg:
            error_msg = f"Connection refused - is Redis running on {request.host}:{request.port}?"
        elif "Authentication" in error_msg.lower() or "NOAUTH" in error_msg:
            error_msg = "Authentication failed - check password"

        return TestRedisResponse(
            success=False,
            message=error_msg,
        )


# ============================================================================
# Raw DB Settings Endpoints
# ============================================================================


class DBSettingRecord(BaseModel):
    id: int
    backend: str
    key: str
    value: str
    is_secret: bool
    description: str | None = None
    version: int
    created_at: str | None = None
    updated_at: str | None = None


class DBSettingUpsertRequest(BaseModel):
    backend: str
    key: str
    value: str
    is_secret: bool = False
    description: str | None = None
    expected_version: int | None = None
    actor_backend_id: int | None = None
    request_id: str | None = None


class DBSettingPatchRequest(BaseModel):
    backend: str | None = None
    key: str | None = None
    value: str | None = None
    is_secret: bool | None = None
    description: str | None = None
    expected_version: int | None = None
    actor_backend_id: int | None = None
    request_id: str | None = None


class ScopeUpdateRequest(BaseModel):
    scope: str


def _serialize_db_setting(row: object, include_secrets: bool) -> DBSettingRecord:
    from ultima_scraper_db_archive.global_db import SettingsModel

    setting = row
    if not isinstance(setting, SettingsModel):
        raise TypeError("Expected SettingsModel")
    value = setting.value
    if setting.key == "config" or setting.key.startswith(("db.", "redis.")) or setting.key == "network.config":
        value = SECRET_MASK
    elif setting.is_secret and not include_secrets:
        value = SECRET_MASK
    elif setting.is_secret and include_secrets:
        value = decrypt_secret_value(setting.value) or ""
    return DBSettingRecord(
        id=setting.id,
        backend=setting.backend,
        key=setting.key,
        value=value,
        is_secret=setting.is_secret,
        description=setting.description,
        version=setting.version,
        created_at=setting.created_at.isoformat() if setting.created_at else None,
        updated_at=setting.updated_at.isoformat() if setting.updated_at else None,
    )


@router.get("/db", response_model=list[DBSettingRecord])
async def list_db_settings(
    backend: str | None = Query(None, description="Filter by backend namespace"),
    key: str | None = Query(None, description="Filter by key"),
    include_secrets: bool = Query(
        False, description="Return unmasked values for secret entries"
    ),
) -> list[DBSettingRecord]:
    """List raw settings table rows for direct DB inspection/editing."""
    from sqlmodel import select
    from ultima_scraper_db_archive.global_db import (
        SettingsModel,
        get_global_session,
    )

    with get_global_session() as session:
        stmt = select(SettingsModel)
        if backend:
            stmt = stmt.where(SettingsModel.backend == backend)
        if key:
            stmt = stmt.where(SettingsModel.key == key)
        stmt = stmt.order_by(SettingsModel.backend.asc(), SettingsModel.key.asc())
        rows = session.exec(stmt).all()
        for row in rows:
            session.expunge(row)

    return [_serialize_db_setting(row, include_secrets) for row in rows]


def _ensure_raw_setting_editable(key: str) -> None:
    if key in {"config", "config_scope"} or key.startswith(("db.", "redis.", "download.")) or key in {"network.config", "drm.config", "hostname"}:
        raise HTTPException(status_code=422, detail="Use the application settings API or configuration migration for this key.")


@router.put("/db", response_model=DBSettingRecord)
async def upsert_db_setting(
    request: DBSettingUpsertRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> DBSettingRecord:
    """Create or update any setting row in the DB by backend+key."""
    from ultima_scraper_db_archive.global_db import (
        SettingsModel,
        get_global_session,
    )

    _ensure_raw_setting_editable(request.key.strip())
    service = SettingsService()
    row = None
    with get_global_session() as session:
        existing = session.exec(
            select(SettingsModel).where(
                SettingsModel.backend == request.backend.strip(),
                SettingsModel.key == request.key.strip(),
            )
        ).first()

        if existing is None:
            persisted_value = (
                encrypt_secret_value(request.value)
                if request.is_secret
                else request.value
            )
            row = SettingsModel(
                backend=request.backend.strip(),
                key=request.key.strip(),
                value=persisted_value or "",
                is_secret=request.is_secret,
                description=request.description,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
        else:
            if request.expected_version is None:
                raise HTTPException(
                    status_code=400,
                    detail="expected_version is required when updating an existing row.",
                )
            updated_id = cast(
                int | None,
                cast(Any, session)
                .exec(
                    sa_update(SettingsModel)
                    .where(
                        SettingsModel.id == existing.id,
                        SettingsModel.version == request.expected_version,
                    )
                    .values(
                        backend=request.backend.strip(),
                        key=request.key.strip(),
                        value=(
                            encrypt_secret_value(request.value)
                            if request.is_secret
                            else request.value
                        ),
                        is_secret=request.is_secret,
                        description=request.description,
                    )
                    .returning(SettingsModel.id)
                )
                .scalar_one_or_none(),
            )
            if updated_id is None:
                latest = session.exec(
                    select(SettingsModel).where(SettingsModel.id == existing.id)
                ).first()
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Setting row changed since it was read.",
                        "current": (
                            _serialize_db_setting(
                                latest, include_secrets=False
                            ).model_dump(mode="json")
                            if latest is not None
                            else None
                        ),
                        "attempted_patch": request.model_dump(exclude_none=True),
                    },
                )
            session.commit()
            row = session.exec(
                select(SettingsModel).where(SettingsModel.id == existing.id)
            ).first()

        if row is None:
            raise HTTPException(status_code=500, detail="Failed to persist setting row")
        session.expunge(row)

    service.invalidate()
    await _publish_settings_updated_event(
        backend,
        settings_id=row.id,
        settings_scope=row.backend,
        version=row.version,
        updated_at=row.updated_at.isoformat(),
        changed_fields=[row.key],
        actor_backend_id=request.actor_backend_id,
        request_id=request.request_id,
    )
    return _serialize_db_setting(row, include_secrets=False)


@router.patch("/db/{setting_id}", response_model=DBSettingRecord)
async def patch_db_setting(
    setting_id: int,
    request: DBSettingPatchRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> DBSettingRecord:
    """Patch a setting row by primary key (supports backend/key rename)."""
    from sqlmodel import select
    from ultima_scraper_db_archive.global_db import (
        SettingsModel,
        get_global_session,
    )

    service = SettingsService()

    with get_global_session() as session:
        row = session.exec(
            select(SettingsModel).where(SettingsModel.id == setting_id)
        ).first()
        if row is None:
            raise HTTPException(status_code=404, detail="Setting row not found")

        _ensure_raw_setting_editable(row.key)
        if request.key is not None:
            _ensure_raw_setting_editable(request.key.strip())

        if request.expected_version is None:
            raise HTTPException(
                status_code=400,
                detail="expected_version is required for patch updates.",
            )

        values: dict[str, Any] = {}
        if request.backend is not None:
            values["backend"] = request.backend.strip()
        if request.key is not None:
            values["key"] = request.key.strip()
        if request.value is not None:
            target_secret = (
                request.is_secret if request.is_secret is not None else row.is_secret
            )
            values["value"] = (
                encrypt_secret_value(request.value) if target_secret else request.value
            )
        if request.is_secret is not None:
            values["is_secret"] = request.is_secret
            if request.value is None and request.is_secret and row.value:
                values["value"] = encrypt_secret_value(row.value) or row.value
        if request.description is not None:
            values["description"] = request.description

        if not values:
            session.expunge(row)
            return _serialize_db_setting(row, include_secrets=False)

        updated_id = cast(
            int | None,
            cast(Any, session)
            .exec(
                sa_update(SettingsModel)
                .where(
                    SettingsModel.id == setting_id,
                    SettingsModel.version == request.expected_version,
                )
                .values(**values)
                .returning(SettingsModel.id)
            )
            .scalar_one_or_none(),
        )
        if updated_id is None:
            latest = session.exec(
                select(SettingsModel).where(SettingsModel.id == setting_id)
            ).first()
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Setting row changed since it was read.",
                    "current": (
                        _serialize_db_setting(latest, include_secrets=False).model_dump(
                            mode="json"
                        )
                        if latest is not None
                        else None
                    ),
                    "attempted_patch": request.model_dump(exclude_none=True),
                },
            )

        session.commit()
        row = session.exec(
            select(SettingsModel).where(SettingsModel.id == setting_id)
        ).first()
        if row is None:
            raise HTTPException(status_code=500, detail="Updated setting row missing")
        session.expunge(row)

    service.invalidate()
    await _publish_settings_updated_event(
        backend,
        settings_id=row.id,
        settings_scope=row.backend,
        version=row.version,
        updated_at=row.updated_at.isoformat(),
        changed_fields=[row.key],
        actor_backend_id=request.actor_backend_id,
        request_id=request.request_id,
    )
    return _serialize_db_setting(row, include_secrets=False)


@router.delete("/db/{setting_id}")
async def delete_db_setting(
    setting_id: int,
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, bool | int]:
    """Delete a setting row by primary key."""
    from sqlmodel import select
    from ultima_scraper_db_archive.global_db import (
        SettingsModel,
        get_global_session,
    )

    service = SettingsService()
    deleted = False
    deleted_scope: str | None = None
    deleted_key: str | None = None
    with get_global_session() as session:
        row = session.exec(
            select(SettingsModel).where(SettingsModel.id == setting_id)
        ).first()
        if row is not None:
            _ensure_raw_setting_editable(row.key)
            deleted_scope = row.backend
            deleted_key = row.key
            session.delete(row)
            session.commit()
            deleted = True

    service.invalidate()
    if deleted and deleted_scope and deleted_key:
        await _publish_settings_updated_event(
            backend,
            settings_id=setting_id,
            settings_scope=deleted_scope,
            version=0,
            updated_at=None,
            changed_fields=[deleted_key],
            actor_backend_id=None,
            request_id=None,
        )
    return {"success": deleted, "id": setting_id}


@router.get("/db/scopes", response_model=dict[str, str])
async def list_config_scopes() -> dict[str, str]:
    """Read the same typed mapping used by runtime and the inventory editor."""
    return {row.hostname: row.settings_scope for row in await list_backend_inventory(hostname=None) if row.settings_scope}


@router.put("/db/scopes/{backend_id}", response_model=dict[str, str])
async def set_config_scope(backend_id: str, request: ScopeUpdateRequest) -> dict[str, str]:
    scope = request.scope.strip()
    row = _load_config_row(scope)
    if row is None:
        raise HTTPException(status_code=404, detail="Canonical config scope not found")
    await upsert_backend_inventory(BackendInventoryUpsertRequest(backend_name=backend_id, hostname=backend_id, settings_id=row.id))
    return {"backend_id": backend_id, "scope": scope}


@router.delete("/db/scopes/{backend_id}", response_model=dict[str, bool | str])
async def delete_config_scope(backend_id: str) -> dict[str, bool | str]:
    rows = await list_backend_inventory(hostname=None)
    for row in rows:
        if row.hostname == backend_id:
            await delete_backend_inventory(row.id)
            return {"backend_id": backend_id, "deleted": True}
    return {"backend_id": backend_id, "deleted": False}


class BackendInventoryRecord(BaseModel):
    id: int
    backend_name: str
    hostname: str
    local_ip: str | None = None
    settings_id: int
    settings_scope: str | None = None
    settings_key: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class BackendInventoryUpsertRequest(BaseModel):
    backend_name: str
    hostname: str
    local_ip: str | None = None
    settings_id: int


class BackendInventoryPatchRequest(BaseModel):
    backend_name: str | None = None
    hostname: str | None = None
    local_ip: str | None = None
    settings_id: int | None = None


def _serialize_backend_inventory(
    backend_row: object,
    settings_row: object | None = None,
) -> BackendInventoryRecord:
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        SettingsModel,
    )

    if not isinstance(backend_row, BackendInventoryModel):
        raise TypeError("Expected BackendInventoryModel")
    setting_scope: str | None = None
    setting_key: str | None = None
    if isinstance(settings_row, SettingsModel):
        setting_scope = settings_row.backend
        setting_key = settings_row.key
    return BackendInventoryRecord(
        id=backend_row.id,
        backend_name=backend_row.backend_name,
        hostname=backend_row.hostname,
        local_ip=str(backend_row.local_ip) if backend_row.local_ip is not None else None,
        settings_id=backend_row.settings_id,
        settings_scope=setting_scope,
        settings_key=setting_key,
        created_at=(
            backend_row.created_at.isoformat() if backend_row.created_at else None
        ),
        updated_at=(
            backend_row.updated_at.isoformat() if backend_row.updated_at else None
        ),
    )


@router.get("/db/backends", response_model=list[BackendInventoryRecord])
async def list_backend_inventory(
    hostname: str | None = Query(None, description="Filter by hostname"),
) -> list[BackendInventoryRecord]:
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        get_global_session,
    )

    records: list[BackendInventoryRecord] = []
    with get_global_session() as session:
        stmt = select(BackendInventoryModel).options(
            selectinload(BackendInventoryModel.settings)
        )
        if hostname:
            stmt = stmt.where(BackendInventoryModel.hostname == hostname)
        stmt = stmt.order_by(BackendInventoryModel.hostname.asc())
        rows = session.exec(stmt).all()
        # Serialised while the session is open. Backends share settings rows,
        # so detaching one row per backend would detach a shared row twice.
        for row in rows:
            records.append(_serialize_backend_inventory(row, row.settings))
    return records


@router.put("/db/backends", response_model=BackendInventoryRecord)
async def upsert_backend_inventory(
    request: BackendInventoryUpsertRequest,
) -> BackendInventoryRecord:
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        SettingsModel,
        get_global_session,
    )

    with get_global_session() as session:
        settings_row = session.exec(
            select(SettingsModel).where(SettingsModel.id == request.settings_id)
        ).first()
        if settings_row is None:
            raise HTTPException(
                status_code=404, detail="Referenced settings_id not found"
            )

        if settings_row.key != "config":
            raise HTTPException(status_code=422, detail="Backend mappings must reference a canonical config row")

        row = session.exec(
            select(BackendInventoryModel).where(
                BackendInventoryModel.hostname == request.hostname.strip()
            )
        ).first()
        if row is None:
            row = BackendInventoryModel(
                backend_name=request.backend_name.strip(),
                hostname=request.hostname.strip(),
                local_ip=request.local_ip,
                settings_id=request.settings_id,
            )
            session.add(row)
        else:
            row.backend_name = request.backend_name.strip()
            row.hostname = request.hostname.strip()
            row.local_ip = request.local_ip
            row.settings_id = request.settings_id
            session.add(row)

        session.commit()
        session.refresh(row)
        session.refresh(settings_row)
        return _serialize_backend_inventory(row, settings_row)


@router.patch("/db/backends/{backend_id}", response_model=BackendInventoryRecord)
async def patch_backend_inventory(
    backend_id: int,
    request: BackendInventoryPatchRequest,
) -> BackendInventoryRecord:
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        SettingsModel,
        get_global_session,
    )

    with get_global_session() as session:
        row = session.exec(
            select(BackendInventoryModel).where(BackendInventoryModel.id == backend_id)
        ).first()
        if row is None:
            raise HTTPException(
                status_code=404, detail="Backend inventory row not found"
            )

        if request.settings_id is not None:
            settings_check = session.exec(
                select(SettingsModel).where(SettingsModel.id == request.settings_id)
            ).first()
            if settings_check is None:
                raise HTTPException(
                    status_code=404,
                    detail="Referenced settings_id not found",
                )
            if settings_check.key != "config":
                raise HTTPException(status_code=422, detail="Backend mappings must reference a canonical config row")
            row.settings_id = request.settings_id
        if request.backend_name is not None:
            row.backend_name = request.backend_name.strip()
        if request.hostname is not None:
            row.hostname = request.hostname.strip()
        if request.local_ip is not None:
            row.local_ip = request.local_ip

        session.add(row)
        session.commit()
        session.refresh(row)
        linked_settings = session.exec(
            select(SettingsModel).where(SettingsModel.id == row.settings_id)
        ).first()
        return _serialize_backend_inventory(row, linked_settings)


@router.delete("/db/backends/{backend_id}")
async def delete_backend_inventory(backend_id: int) -> dict[str, bool | int]:
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        get_global_session,
    )

    deleted = False
    with get_global_session() as session:
        row = session.exec(
            select(BackendInventoryModel).where(BackendInventoryModel.id == backend_id)
        ).first()
        if row is not None:
            session.delete(row)
            session.commit()
            deleted = True
    return {"success": deleted, "id": backend_id}


# ============================================================================
# Remote Backend Config Endpoints
# ============================================================================


class RemoteSettingsResponse(BaseModel):
    """Response containing a remote backend's settings."""

    backend_id: str
    settings: SettingsResponse | None = None
    error: str | None = None


class RemoteUpdateResponse(BaseModel):
    """Response from remote config update."""

    backend_id: str
    success: bool
    message: str


class SyncAllResponse(BaseModel):
    """Response from syncing settings to all backends."""

    success: bool
    message: str
    backends_updated: list[str]
    backends_failed: list[str]


def _load_backend_config(
    backend_id: str,
) -> tuple[UltimaScraperCollectionConfig | None, Any | None, Any | None]:
    from ultima_scraper_core import archive_database_url
    from ultima_scraper_db_archive.global_db import (
        BackendInventoryModel,
        SettingsModel,
        get_global_session,
    )

    if not archive_database_url():
        return None, None, None

    with get_global_session() as session:
        inv = session.exec(
            select(BackendInventoryModel).where(
                or_(
                    BackendInventoryModel.hostname == backend_id,
                    BackendInventoryModel.backend_name == backend_id,
                )
            )
        ).first()

        row: SettingsModel | None = None
        if inv is not None and inv.settings_id:
            row = session.exec(
                select(SettingsModel).where(
                    SettingsModel.id == inv.settings_id,
                    SettingsModel.key == "config",
                )
            ).first()

        if row is None:
            row = session.exec(
                select(SettingsModel).where(
                    SettingsModel.backend == backend_id,
                    SettingsModel.key == "config",
                )
            ).first()

        if row is None:
            # Fall back to global or first available config row
            row = session.exec(
                select(SettingsModel).where(
                    SettingsModel.backend == "global",
                    SettingsModel.key == "config",
                )
            ).first()

        if row is None:
            row = session.exec(
                select(SettingsModel).where(SettingsModel.key == "config")
            ).first()

        if row is not None:
            session.expunge(row)
            if inv is not None:
                session.expunge(inv)
            payload = json.loads(row.value)
            decrypt_config_secrets(payload)
            config = UltimaScraperCollectionConfig(**payload)
            config.settings.hostname = backend_id
            config.settings.backend.backend_id = backend_id
            config._settings_backend = row.backend
            config._settings_id = row.id
            config._settings_version = row.version
            config._settings_updated_at = (
                row.updated_at.isoformat() if row.updated_at else None
            )
            config.is_configured = True
            return config, row, inv

        return None, None, inv


@router.get("/{backend_id}", response_model=RemoteSettingsResponse)
async def get_remote_settings(
    backend_id: str,
    backend: BackendInstance = Depends(get_backend_instance),
) -> RemoteSettingsResponse:
    """Get settings for a remote backend.

    Retrieves canonical config from the database for the specified backend,
    falling back to cached config in Redis.
    """
    # If requesting our own config, return it directly
    if backend_id == backend.instance_id:
        try:
            config = UltimaScraperCollectionConfig.load_or_create_default_config()
            return RemoteSettingsResponse(
                backend_id=backend_id,
                settings=_config_to_response(config),
            )
        except Exception as e:
            return RemoteSettingsResponse(
                backend_id=backend_id,
                error=str(e),
            )

    try:
        # 1. Canonical source of truth: PostgreSQL database
        config, row, _ = _load_backend_config(backend_id)
        if config is not None:
            settings = _config_to_response(config)
            if settings.redis.password:
                settings.redis.password = SECRET_MASK
            for proxy in settings.network.proxies:
                if proxy.password:
                    proxy.password = SECRET_MASK
            settings.config_path = None
            return RemoteSettingsResponse(
                backend_id=backend_id,
                settings=settings,
            )

        # 2. Fallback: cached config from Redis (e.g. standalone remote backends)
        if backend.redis and backend.redis.is_connected:
            key = f"{CONFIG_KEY_PREFIX}{backend_id}"
            raw = await backend.redis.client.get(key)
            if raw is not None:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                config_data = json.loads(raw)
                settings = SettingsResponse.model_validate(config_data)
                if settings.redis.password:
                    settings.redis.password = SECRET_MASK
                for proxy in settings.network.proxies:
                    if proxy.password:
                        proxy.password = SECRET_MASK
                settings.config_path = None
                return RemoteSettingsResponse(
                    backend_id=backend_id,
                    settings=settings,
                )

        return RemoteSettingsResponse(
            backend_id=backend_id,
            error=f"No config found for backend '{backend_id}' - backend may be offline",
        )

    except json.JSONDecodeError:
        return RemoteSettingsResponse(
            backend_id=backend_id,
            error="Invalid config data in Redis",
        )
    except Exception as e:
        logger.exception(f"Failed to get remote settings for {backend_id}")
        return RemoteSettingsResponse(
            backend_id=backend_id,
            error=str(e),
        )


def _http_exception_message(exc: HTTPException) -> str:
    """Flatten a structured error detail into one readable sentence."""
    detail = exc.detail
    if not isinstance(detail, dict):
        return str(detail)
    detail = cast(dict[str, Any], detail)
    message = str(detail.get("message") or "Settings update failed")
    if detail.get("current_version") is not None:
        message += (
            f" Reload to get version {detail['current_version']}, then save again."
        )
    if detail.get("fields"):
        message += f" ({', '.join(map(str, detail['fields']))})"
    return message


@router.put("/{backend_id}", response_model=RemoteUpdateResponse)
async def update_remote_settings(
    backend_id: str,
    update: SettingsUpdateRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> RemoteUpdateResponse:
    """Update settings for a remote backend.

    Applies settings to the canonical database for the specified backend scope,
    creating a dedicated scoped settings row if needed, and notifies the cluster.
    """
    # If updating our own config, do it directly
    if backend_id == backend.instance_id:
        try:
            local_update = update
            if local_update.expected_version is None:
                local_config = (
                    UltimaScraperCollectionConfig.load_or_create_default_config()
                )
                local_response = _config_to_response(local_config)
                local_update = local_update.model_copy(
                    update={"expected_version": local_response.config_version}
                )
            await update_settings(local_update, backend)
            return RemoteUpdateResponse(
                backend_id=backend_id,
                success=True,
                message="Settings updated successfully",
            )
        except HTTPException as e:
            return RemoteUpdateResponse(
                backend_id=backend_id,
                success=False,
                message=_http_exception_message(e),
            )
        except Exception as e:
            return RemoteUpdateResponse(
                backend_id=backend_id,
                success=False,
                message=str(e),
            )

    try:
        config, current_row, inv = _load_backend_config(backend_id)
        if config is None:
            config = UltimaScraperCollectionConfig.load_or_create_default_config()

        target_scope = backend_id
        config.settings.hostname = backend_id
        config.settings.backend.backend_id = backend_id

        old_worker_pool_size = config.settings.worker_pool.size
        old_max_claimed_jobs = config.settings.worker_pool.max_claimed_jobs

        changed_sections = _apply_settings_update_to_config(config, update)

        if not changed_sections:
            return RemoteUpdateResponse(
                backend_id=backend_id,
                success=True,
                message="No settings changed",
            )

        persisted = None
        # If current_row belongs directly to this backend scope, update it
        if current_row is not None and current_row.backend == target_scope:
            config._settings_backend = target_scope
            exp_ver = (
                update.expected_version
                if update.expected_version is not None
                else current_row.version
            )
            persisted = persist_config_snapshot(config, expected_version=exp_ver)
        else:
            # Fork or create a dedicated scoped row for this backend
            config._settings_backend = target_scope
            config._settings_id = None
            persisted = persist_config_snapshot(config, expected_version=None)

            # Update or create BackendInventoryModel to link to this new dedicated row
            from ultima_scraper_db_archive.global_db import (
                BackendInventoryModel,
                get_global_session,
            )

            with get_global_session() as session:
                if inv is not None:
                    inv_row = session.get(BackendInventoryModel, inv.id)
                    if inv_row is not None:
                        inv_row.settings_id = persisted.settings_id
                        session.add(inv_row)
                        session.commit()
                else:
                    new_inv = BackendInventoryModel(
                        backend_name=backend_id,
                        hostname=backend_id,
                        settings_id=persisted.settings_id,
                    )
                    session.add(new_inv)
                    session.commit()

        SettingsService().invalidate()

        # Notify via Redis if connected
        if backend.redis and backend.redis.is_connected:
            request_id = str(uuid4())
            try:
                await _publish_settings_updated_event(
                    backend,
                    settings_id=persisted.settings_id,
                    settings_scope=target_scope,
                    version=persisted.version,
                    updated_at=persisted.updated_at,
                    changed_fields=changed_sections,
                    actor_backend_id=None,
                    request_id=request_id,
                    target_backend_id=backend_id,
                )
            except Exception as exc:
                logger.warning("Could not publish settings_updated event: %s", exc)

            if update.worker_pool is not None:
                try:
                    await _publish_worker_pool_runtime_events(
                        backend,
                        settings_scope=target_scope,
                        old_size=old_worker_pool_size,
                        new_size=config.settings.worker_pool.size,
                        old_max_claimed_jobs=old_max_claimed_jobs,
                        new_max_claimed_jobs=config.settings.worker_pool.max_claimed_jobs,
                        target_backend_id=backend_id,
                    )
                except Exception as exc:
                    logger.warning("Could not publish worker pool events: %s", exc)

            updates = update.model_dump(
                mode="json",
                exclude_unset=True,
                exclude={"expected_version", "actor_backend_id", "request_id"},
            )
            if updates:
                try:
                    req_event = {
                        "type": "config_update_request",
                        "target_backend_id": backend_id,
                        "expected_version": update.expected_version,
                        "source_backend_id": backend.instance_id,
                        "request_id": request_id,
                        "updates": updates,
                    }
                    await backend.redis.publish(
                        CONFIG_EVENTS_CHANNEL, json.dumps(req_event)
                    )
                except Exception as exc:
                    logger.warning("Could not publish config_update_request event: %s", exc)

        return RemoteUpdateResponse(
            backend_id=backend_id,
            success=True,
            message=f"Settings updated successfully for backend '{backend_id}'",
        )
    except HTTPException as exc:
        return RemoteUpdateResponse(
            backend_id=backend_id,
            success=False,
            message=_http_exception_message(exc),
        )
    except ConfigVersionConflict as exc:
        return RemoteUpdateResponse(
            backend_id=backend_id,
            success=False,
            message=f"Config version conflict (version {exc.current_version})",
        )
    except Exception as e:
        logger.exception(f"Failed to update remote settings for {backend_id}")
        return RemoteUpdateResponse(
            backend_id=backend_id,
            success=False,
            message=str(e),
        )


@router.put("/sync-all", response_model=SyncAllResponse)
async def sync_settings_to_all(
    update: SettingsUpdateRequest,
    backend: BackendInstance = Depends(get_backend_instance),
) -> SyncAllResponse:
    """Sync settings to all registered backends.

    Broadcasts config updates to all backends via Redis pub/sub.
    Also applies to the local backend.
    """
    # First apply to local backend
    try:
        local_update = update
        if local_update.expected_version is None:
            local_config = UltimaScraperCollectionConfig.load_or_create_default_config()
            local_response = _config_to_response(local_config)
            local_update = local_update.model_copy(
                update={"expected_version": local_response.config_version}
            )
        await update_settings(local_update, backend)
        local_updated = True
    except Exception as e:
        logger.error(f"Failed to update local settings: {e}")
        local_updated = False

    backends_updated: list[str] = []
    backends_failed: list[str] = []

    if local_updated:
        backends_updated.append(backend.instance_id)
    else:
        backends_failed.append(backend.instance_id)

    # Check Redis availability for broadcasting
    if not backend.redis or not backend.redis.is_connected:
        return SyncAllResponse(
            success=local_updated,
            message="Local updated but Redis not available for sync",
            backends_updated=backends_updated,
            backends_failed=backends_failed,
        )

    try:
        updates = update.model_dump(mode="json", exclude_unset=True, exclude={"expected_version", "actor_backend_id", "request_id"})
        if not updates:
            return SyncAllResponse(
                success=local_updated,
                message="No updates to sync",
                backends_updated=backends_updated,
                backends_failed=backends_failed,
            )

        # Broadcast to all backends (target_backend_id = None means all)
        request_id = str(uuid4())
        event = {
            "type": "config_update_request",
            "target_backend_id": None,  # Broadcast to all
            "source_backend_id": backend.instance_id,
            "request_id": request_id,
            "updates": updates,
        }

        await backend.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(event))
        logger.info(f"Broadcast config sync to all backends: {request_id}")

        return SyncAllResponse(
            success=True,
            message="Settings sync broadcast sent to all backends",
            backends_updated=backends_updated,
            backends_failed=backends_failed,
        )

    except Exception as e:
        logger.exception("Failed to broadcast settings sync")
        return SyncAllResponse(
            success=False,
            message=str(e),
            backends_updated=backends_updated,
            backends_failed=backends_failed,
        )
