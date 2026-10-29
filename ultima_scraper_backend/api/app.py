"""Control plane module (moved from fastapi_control.py)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, cast

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from starlette.middleware.base import RequestResponseEndpoint
from starlette.middleware.trustedhost import TrustedHostMiddleware
from ultima_scraper_api import get_enabled_sites
from ultima_scraper_core import load_env, runtime_env_value

load_env()

from ultima_scraper_db_archive.crypto import (
    validate_secret_crypto_configuration,
)

from ..logging_utils import (
    configure_runtime_logging,
    get_daily_log_path,
    prune_old_logs,
)

configure_runtime_logging("api")

from ..models.backend import BackendInstance
from ..runtime_mode import get_runtime_mode, is_api_only_runtime
from .. import cluster_clients
from ..security import (
    SECURITY_METRICS,
    admin_token_required,
    allow_docs,
    allow_remote_bind,
    build_request_id,
    configure_log_redaction,
    enforce_rate_limit,
    ensure_http_admin_auth,
    extract_admin_token_from_request,
    get_allowed_hosts,
    get_allowed_origin_regex,
    get_allowed_origins,
    get_default_bind_host,
    get_security_enforcement_mode,
    is_origin_allowed,
    log_security_startup_report,
    validate_security_configuration,
)
from ..services.statistics_service import StatisticsService
from ..services.terminal_job_reconciler import (
    TerminalJobReconciler,
    log_redis_memory_guardrail,
)
from .dependencies import get_backend_instance, shutdown_backend

# Import new route modules
from .routes import (
    account_purchases,
    admin,
    api_requests,
    auths,
    chats,
    cluster,
    curl_parser,
    database_browser,
    files,
    server_restore,
    jobs,
    media,
    mps,
    orders,
    proxies,
    search_loadouts,
    favourites,
    settings,
    setup,
    statistics,
    status,
    subscriptions,
    system,
    tasks,
    users,
    watch,
    webhooks,
    websocket,
)
from .routes.websocket import (
    setup_event_broadcasting,
    setup_status_broadcasting,
    stop_event_broadcasting,
    stop_status_broadcasting,
    subscribe_to_redis_logs,
)

# Logging configuration is handled by the runtime launcher before processes start.
logger = logging.getLogger("fastapi_control")

_statistics_refresh_task: asyncio.Task[None] | None = None
_terminal_job_reconciler: TerminalJobReconciler | None = None
_STATISTICS_REFRESH_POLL_SECONDS = 300
_STATISTICS_SETUP_RETRY_SECONDS = 30
_BACKEND_INIT_LOG_TYPE = "backend_init"
_BACKEND_INIT_LOG_BASENAME = "debug"

# /cluster/join takes a one-time invite code instead of a token.
_PUBLIC_UNAUTH_PATHS = {"/api/v1/admin/health", "/api/v1/cluster/join"}
_SECURITY_PROTECTED_PREFIX = "/api/v1"
_PLUGIN_CALLBACK_PATH_RE = re.compile(
    r"^/api/v1/plugins/[a-z][a-z0-9_-]{1,62}[a-z0-9]/"
    r"remote-delegation/callbacks/[a-z][a-z0-9_-]{1,62}[a-z0-9]$"
)


def _resolve_plugins_dir() -> Path:
    configured = runtime_env_value("USR_PLUGINS_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    repository_root = runtime_env_value("USR_REPOSITORY_ROOT")
    if repository_root:
        return Path(repository_root).expanduser().resolve() / "plugins"
    return Path.cwd().resolve() / "plugins"


def _create_plugin_platform() -> Any:
    """Build the control-process plugin facade over the migrated plugin schema."""
    from ultima_scraper_backend.plugin_system.platform import PluginPlatform
    from ultima_scraper_backend.plugin_system.postgres_repository import PostgresPluginRepository
    from ultima_scraper_db_archive.global_db import global_session

    plugins_dir = _resolve_plugins_dir()
    plugins_dir.mkdir(parents=True, exist_ok=True)
    plugin_platform = PluginPlatform(plugins_dir, repository=PostgresPluginRepository(
        global_session, legacy_runtime_dir=plugins_dir / ".runtime",
    ))
    loaded_plugins = plugin_platform.load_all()
    logger.info(
        "Plugin platform initialized in control process with %d plugin(s)",
        len(loaded_plugins),
    )
    # Background plugin work belongs to the dedicated supervisor process.
    return plugin_platform


@dataclass(slots=True)
class RuntimeSecurityConfig:
    allowed_origins: list[str]
    allowed_origin_regex: str | None
    allowed_hosts: list[str]
    ws_allowed_origins: list[str]
    docs_enabled: bool
    admin_token_required: bool


def _load_runtime_security_config() -> RuntimeSecurityConfig:
    # Trust boundaries are bootstrap settings and do not query the DB at import.
    from ..security import get_ws_allowed_origins

    return RuntimeSecurityConfig(
        allowed_origins=get_allowed_origins(),
        allowed_origin_regex=get_allowed_origin_regex(),
        allowed_hosts=get_allowed_hosts(),
        ws_allowed_origins=get_ws_allowed_origins(),
        docs_enabled=allow_docs(),
        admin_token_required=admin_token_required(),
    )


def _is_protected_path(path: str) -> bool:
    return path.startswith(_SECURITY_PROTECTED_PREFIX)


def _uses_plugin_callback_auth(path: str) -> bool:
    """Return true only for the exact HMAC/bearer callback route shape."""
    return _PLUGIN_CALLBACK_PATH_RE.fullmatch(path) is not None


def _normalize_origin(origin: str) -> str:
    return origin.strip().lower().rstrip("/")


def _is_allowed_origin(origin: str, allowed_origins: list[str]) -> bool:
    return is_origin_allowed(
        _normalize_origin(origin),
        [_normalize_origin(x) for x in allowed_origins],
        allowed_origin_regex=_runtime_security.allowed_origin_regex,
    )


def _env_int(name: str, default: int, *, strict: bool = False) -> int:
    raw = runtime_env_value(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        if strict:
            raise ValueError(f"{name} must be an integer") from None
        return default


def _enforce_security_rate_limits(request: Request) -> None:
    path = request.url.path
    if _uses_plugin_callback_auth(path):
        enforce_rate_limit(
            request,
            bucket="plugin_callback",
            limit=_env_int("USR_RATE_LIMIT_PLUGIN_CALLBACK", 300),
            window_seconds=60,
        )
        return

    if path.startswith("/api/v1/setup/"):
        enforce_rate_limit(
            request,
            bucket="setup",
            limit=_env_int("USR_RATE_LIMIT_SETUP", 30),
            window_seconds=60,
        )
        return

    if path == "/api/v1/media/proxy":
        enforce_rate_limit(
            request,
            bucket="media_proxy",
            limit=_env_int("USR_RATE_LIMIT_MEDIA_PROXY", 60),
            window_seconds=60,
        )
        return

    if "/analyze" in path and path.startswith("/api/v1/proxies/"):
        enforce_rate_limit(
            request,
            bucket="proxy_analyze",
            limit=_env_int("USR_RATE_LIMIT_PROXY_ANALYZE", 10),
            window_seconds=60,
        )
        return


def _forbidden_response(
    request_id: str,
    *,
    status_code: int,
    detail: str,
    extra_headers: dict[str, str] | None = None,
) -> JSONResponse:
    headers = {"X-Request-ID": request_id}
    if extra_headers:
        headers.update(extra_headers)
    return JSONResponse(
        status_code=status_code, content={"detail": detail}, headers=headers
    )


_runtime_security = _load_runtime_security_config()


def _append_backend_init_debug_line(message: str, truncate: bool = False) -> None:
    log_path = get_daily_log_path(
        log_type=_BACKEND_INIT_LOG_TYPE,
        base_name=_BACKEND_INIT_LOG_BASENAME,
    )
    mode = "w" if truncate else "a"
    with log_path.open(mode, encoding="utf-8") as log_file:
        log_file.write(f"{message}\n")
        log_file.flush()


async def _refresh_daily_statistics_once(
    backend: BackendInstance,
) -> bool:
    from ultima_scraper_db_archive.global_db import is_setup_complete

    if not is_setup_complete():
        return False

    for site in (site_name.lower() for site_name in get_enabled_sites()):
        try:
            await StatisticsService.get_statistics_availability(
                site,
                redis_manager=backend.redis,
                schedule_if_missing=True,
            )
        except Exception as exc:
            logger.warning("Failed refreshing statistics for %s: %s", site, exc)
    return True


def _statistics_refresh_delay_seconds(*, setup_complete: bool) -> int:
    if not setup_complete:
        return _STATISTICS_SETUP_RETRY_SECONDS
    return min(
        StatisticsService.seconds_until_next_utc_midnight() + 1,
        _STATISTICS_REFRESH_POLL_SECONDS,
    )


async def _statistics_refresh_loop(backend: BackendInstance) -> None:
    while True:
        try:
            setup_complete = await _refresh_daily_statistics_once(backend)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Daily statistics refresh cycle failed")
            setup_complete = False
        await asyncio.sleep(
            _statistics_refresh_delay_seconds(setup_complete=setup_complete)
        )


# Lifespan context manager must be defined BEFORE FastAPI app creation
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _statistics_refresh_task
    global _terminal_job_reconciler
    # Write to debug file FIRST to verify this is being called.
    prune_old_logs(_BACKEND_INIT_LOG_TYPE)
    _append_backend_init_debug_line(
        "[LIFESPAN] *** FUNCTION CALLED ***",
        truncate=True,
    )

    logger.info("Starting up FastAPI application...")
    configure_log_redaction()
    validate_security_configuration()
    validate_secret_crypto_configuration()
    try:
        from ..config import UltimaScraperCollectionConfig
        from ..job_system.guest_sessions import configure_guest_route_rate_limit

        # Guest lookups made here share each proxy's budget with the workers.
        network = UltimaScraperCollectionConfig.load_or_create_default_config().settings.network
        configure_guest_route_rate_limit(network.guest_route_rate_limit, worker_count=1)
    except Exception:
        logger.exception("Could not apply the guest request rate limit in the API process")
    api_only = is_api_only_runtime()
    logger.info("FastAPI runtime contract: %s", "api-only" if api_only else "full")
    await admin.ensure_api_postgres_dependency()
    from ultima_scraper_backend.plugin_system.postgres_repository import PluginSchemaNotReady

    plugin_platform: Any = None
    plugin_runtime_coordinator: Any = None
    backend: BackendInstance | None = None
    plugin_platform_lock = threading.Lock()

    def ensure_plugin_platform() -> Any:
        """Create the platform on first use when setup migrated the schema after startup."""
        with plugin_platform_lock:
            if getattr(app.state, "plugin_platform", None) is None:
                try:
                    created = _create_plugin_platform()
                except PluginSchemaNotReady:
                    return None
                if backend is not None:
                    created.attach_backend(backend)
                app.state.plugin_platform = created
            return app.state.plugin_platform

    try:
        plugin_platform = _create_plugin_platform()
        app.state.plugin_platform = plugin_platform
    except PluginSchemaNotReady:
        # A new database is migrated by the setup wizard; first use retries.
        logger.info("Plugin platform will start once setup has migrated the database")
    except Exception:
        logger.exception("Plugin platform failed to initialize in control process")
    app.state.ensure_plugin_platform = ensure_plugin_platform

    _append_backend_init_debug_line(
        "[LIFESPAN] About to call get_backend_instance()..."
    )

    backend = await get_backend_instance()
    runtime_telemetry_sampler: Any | None = None

    await admin.ensure_api_runtime_dependencies(backend)

    _append_backend_init_debug_line(
        f"[LIFESPAN] Backend instance obtained: {backend.instance_id}"
    )

    await setup_event_broadcasting(backend)
    await setup_status_broadcasting(backend)

    webhook_delivery = None
    if backend.redis and backend.redis.is_connected:
        from ..services.webhook_delivery import WebhookDeliveryService
        from .dependencies import get_config
        try:
            webhook_delivery = WebhookDeliveryService(backend.redis, get_config)
            await webhook_delivery.start()
        except Exception:
            if webhook_delivery is not None:
                await webhook_delivery.stop()
            webhook_delivery = None
            logger.exception("Webhook delivery service failed to start")

    try:
        if plugin_platform is not None:
            plugin_platform.attach_backend(backend)
        if not api_only and plugin_runtime_coordinator is not None:
            plugin_runtime_coordinator.start()
    except Exception:
        logger.exception("Plugin runtime failed to start")

    # Seed built-in job templates
    if backend.redis and backend.redis.is_connected:
        try:
            from ..job_system.templates import JobTemplateStore, seed_builtin_templates

            _template_store = JobTemplateStore(cast(Any, backend.redis))
            await seed_builtin_templates(_template_store)
        except Exception as exc:
            logger.warning("Failed to seed built-in templates: %s", exc)

        await log_redis_memory_guardrail(cast(Any, backend.redis))

        from ..deprecated_env import deprecated_env_int
        from ..services.terminal_job_reconciler import (
            load_job_execution_settings_async,
        )

        # Limits come from job_execution settings; deprecated env vars still
        # pin individual values for one release.
        reconciler_overrides = {
            attribute: value
            for name, attribute, minimum in (
                ("USR_TERMINAL_JOB_RECONCILE_INTERVAL_SECONDS", "interval_seconds", 30),
                ("USR_TERMINAL_JOB_RECONCILE_MIN_AGE_SECONDS", "min_age_seconds", 0),
                ("USR_TERMINAL_JOB_RECONCILE_BATCH_SIZE", "max_jobs_per_cycle", 1),
                ("USR_COMPLETED_JOB_RETENTION_SECONDS", "retention_age_seconds", 0),
                ("USR_COMPLETED_JOB_RETENTION_BATCH_SIZE", "retention_batch_size", 1),
            )
            if (value := deprecated_env_int(name, minimum=minimum)) is not None
        }
        _terminal_job_reconciler = TerminalJobReconciler(
            cast(Any, backend.redis),
            settings_loader=load_job_execution_settings_async,
            overrides=reconciler_overrides.keys(),
            **reconciler_overrides,
        )
        await _terminal_job_reconciler.start()
        logger.info("Terminal-job Redis reconciler started")

    # Start Redis log subscriber background task
    log_subscriber_task: asyncio.Task[None] | None = None
    if backend.redis and backend.redis.is_connected:
        log_subscriber_task = asyncio.create_task(subscribe_to_redis_logs(backend))
        logger.info("Redis log subscriber task started")
    else:
        logger.warning("Redis not available, WebUI logs will not be available")

    from ..job_system import ledger_runtime, mps_state

    from ..config import UltimaScraperCollectionConfig
    if backend.redis and backend.redis.is_connected:
        await mps_state.sync_runtime_settings(
            backend.redis.client,
            UltimaScraperCollectionConfig.load_or_create_default_config().settings,
        )

    await ledger_runtime.start_service(backend)

    if backend.redis and backend.redis.is_connected:
        try:
            from ..services.job_runtime_telemetry_sampler import (
                JobRuntimeTelemetrySampler,
            )

            runtime_telemetry_sampler = JobRuntimeTelemetrySampler(
                cast(Any, backend.redis), backend_id=backend.instance_id
            )
            await runtime_telemetry_sampler.start()
            logger.info("Job runtime telemetry sampler started")
        except Exception:
            runtime_telemetry_sampler = None
            logger.exception("Job runtime telemetry sampler failed to start")

    _statistics_refresh_task = asyncio.create_task(_statistics_refresh_loop(backend))
    cluster_client_task = asyncio.create_task(cluster_clients.refresh_loop())
    logger.info("Daily statistics refresh monitor started")

    logger.info("Backend initialization complete")
    logger.info("fastapi_control lifespan started")
    try:
        yield
    finally:
        _append_backend_init_debug_line("[LIFESPAN] Shutting down...")

        if webhook_delivery is not None:
            await webhook_delivery.stop()

        if runtime_telemetry_sampler is not None:
            await runtime_telemetry_sampler.stop()
            logger.info("Job runtime telemetry sampler stopped")

        await ledger_runtime.stop_service()

        if plugin_runtime_coordinator is not None:
            try:
                await plugin_runtime_coordinator.stop()
            except Exception:
                logger.exception("Plugin runtime failed to stop cleanly")
        app.state.plugin_platform = None
        app.state.ensure_plugin_platform = None

        # Cancel event broadcasting task
        await stop_event_broadcasting()
        await stop_status_broadcasting()


        cluster_client_task.cancel()

        if _statistics_refresh_task:
            _statistics_refresh_task.cancel()
            try:
                await _statistics_refresh_task
            except asyncio.CancelledError:
                pass
            _statistics_refresh_task = None
            logger.info("Daily statistics refresh task stopped")

        if _terminal_job_reconciler:
            await _terminal_job_reconciler.stop()
            _terminal_job_reconciler = None
            logger.info("Terminal-job Redis reconciler stopped")

        # Cancel Redis log subscriber task
        if log_subscriber_task:
            log_subscriber_task.cancel()
            try:
                await log_subscriber_task
            except asyncio.CancelledError:
                pass
            logger.info("Redis log subscriber task stopped")

        logger.info("Shutting down FastAPI application...")
        await shutdown_backend()
        logger.info("Backend shutdown complete")
        logger.info("fastapi_control shutting down")


# Create FastAPI app with lifespan parameter
app = FastAPI(
    title="UltimaScraper Backend API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if _runtime_security.docs_enabled else None,
    redoc_url="/redoc" if _runtime_security.docs_enabled else None,
    openapi_url="/openapi.json" if _runtime_security.docs_enabled else None,
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=_runtime_security.allowed_hosts,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_runtime_security.allowed_origins,
    allow_origin_regex=_runtime_security.allowed_origin_regex,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def security_http_middleware(
    request: Request,
    call_next: RequestResponseEndpoint,
) -> Response:
    request_id = build_request_id(request.headers.get("x-request-id"))
    request.state.request_id = request_id

    if request.method.upper() == "OPTIONS":
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    origin = request.headers.get("origin")
    if origin and not _is_allowed_origin(origin, _runtime_security.allowed_origins):
        SECURITY_METRICS["origin_blocked_total"] += 1
        logger.warning(
            "[SECURITY] Origin blocked: origin=%s method=%s path=%s request_id=%s",
            origin,
            request.method,
            request.url.path,
            request_id,
        )
        if get_security_enforcement_mode() != "report_only":
            return _forbidden_response(
                request_id,
                status_code=403,
                detail="Forbidden origin",
            )

    try:
        _enforce_security_rate_limits(request)
        if (request.url.path in {"/api/v1/plugins/runtime/invoke", "/api/v1/plugins/runtime/jobs", "/api/v1/plugins/runtime/state", "/api/v1/plugins/runtime/proxies/sync"}
            or request.url.path.startswith("/api/v1/plugins/runtime/http/")
            or request.url.path.startswith("/api/v1/plugins/runtime/state/")
            or request.url.path.startswith("/api/v1/plugins/runtime/profiles/")):
            from ..plugin_system.api_routes import _get_runtime_identity
            from ..plugin_system.api_routes import resolve_plugin_platform
            platform = await asyncio.to_thread(resolve_plugin_platform, request.app)
            if platform is None:
                raise HTTPException(status_code=503, detail="Plugin platform not initialized")
            _get_runtime_identity(request, platform)
        elif _is_protected_path(request.url.path) and not _uses_plugin_callback_auth(
            request.url.path
        ):
            # A client that just joined through another member is not loaded yet.
            await cluster_clients.refresh_for(extract_admin_token_from_request(request))
            ensure_http_admin_auth(
                request,
                request_id=request_id,
                bypass_paths=_PUBLIC_UNAUTH_PATHS,
                token_required=_runtime_security.admin_token_required,
            )
    except HTTPException as exc:
        return _forbidden_response(
            request_id,
            status_code=exc.status_code,
            detail=str(exc.detail),
            extra_headers=cast(dict[str, str] | None, exc.headers),
        )

    try:
        response: Response = await call_next(request)
    except Exception as exc:
        # Uvicorn persists the stack; attach route context to that same record.
        exc.add_note(f"request_id={request_id} method={request.method} path={request.url.path}")
        raise
    if response.status_code >= 500:
        logger.error(
            "HTTP response status=%s method=%s path=%s request_id=%s",
            response.status_code, request.method, request.url.path, request_id,
        )
    response.headers["X-Request-ID"] = request_id
    return response


# Include routers
app.include_router(admin.router, prefix="/api/v1")
app.include_router(account_purchases.router, prefix="/api/v1")
app.include_router(api_requests.router, prefix="/api/v1")
app.include_router(jobs.router, prefix="/api/v1")
app.include_router(tasks.router, prefix="/api/v1")
app.include_router(users.router, prefix="/api/v1")
app.include_router(auths.router, prefix="/api/v1")
app.include_router(chats.router, prefix="/api/v1")
app.include_router(orders.router, prefix="/api/v1")
app.include_router(proxies.router, prefix="/api/v1")
app.include_router(search_loadouts.router, prefix="/api/v1")
app.include_router(favourites.router, prefix="/api/v1")
app.include_router(settings.router, prefix="/api/v1")
app.include_router(setup.router, prefix="/api/v1")
app.include_router(status.router, prefix="/api/v1")
app.include_router(statistics.router, prefix="/api/v1")
app.include_router(subscriptions.router, prefix="/api/v1")
app.include_router(curl_parser.router, prefix="/api/v1")
app.include_router(mps.router, prefix="/api/v1")
app.include_router(database_browser.router, prefix="/api/v1")
app.include_router(files.router, prefix="/api/v1")
app.include_router(server_restore.router, prefix="/api/v1")
app.include_router(media.router, prefix="/api/v1")
app.include_router(system.router, prefix="/api/v1")
app.include_router(cluster.router, prefix="/api/v1")
app.include_router(webhooks.router, prefix="/api/v1")
app.include_router(watch.router, prefix="/api/v1")
app.include_router(websocket.router)

# Plugin system routes
try:
    from ultima_scraper_backend.plugin_system.api_routes import router as plugin_router

    app.include_router(plugin_router, prefix="/api/v1")
except Exception:  # noqa: BLE001
    logger.exception("Plugin API routes could not be registered")


def _find_available_port(start_port: int, max_attempts: int = 10) -> int:
    """Find an available port starting from start_port."""
    import socket

    permission_blocked = False
    for offset in range(max_attempts):
        port = start_port + offset
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("127.0.0.1", port))
                # Also check if we can actually listen
                s.listen(1)
                return port
        except PermissionError:
            permission_blocked = True
            break
        except OSError:
            continue

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.1", 0))
            s.listen(1)
            fallback_port = cast(int, s.getsockname()[1])
            logger.warning(
                "Could not find available port in range %s-%s; falling back to ephemeral port %s",
                start_port,
                start_port + max_attempts - 1,
                fallback_port,
            )
            return fallback_port
    except PermissionError:
        permission_blocked = True
    except OSError as exc:
        raise RuntimeError(
            f"Could not find available port in range {start_port}-{start_port + max_attempts - 1}"
        ) from exc

    if permission_blocked:
        logger.warning(
            "Port availability probe for %s was blocked by the runtime environment; "
            "using configured port without probing",
            start_port,
        )
        return start_port

    raise RuntimeError(
        f"Could not find available port in range {start_port}-{start_port + max_attempts - 1}"
    )


def _get_worker_count() -> int:
    """Return the number of uvicorn worker processes.

    Reads ``CONTROL_WORKERS`` from the process environment first, then the
    persisted UltimaScraper environment file.  Accepted values:
      - A positive integer  → use exactly that many workers.
      - ``"auto"``          → ``(cpu_count // 2) + 1``, clamped to [1, 8].
      - Absent / ``"1"``    → single-process mode (default).
    """
    raw = (runtime_env_value("CONTROL_WORKERS") or "1").strip().lower()
    if raw == "auto":
        cpu = os.cpu_count() or 1
        return max(1, min((cpu // 2) + 1, 8))
    try:
        count = int(raw)
        return max(1, count)
    except (TypeError, ValueError):
        logger.warning("Invalid CONTROL_WORKERS value %r; defaulting to 1", raw)
        return 1


def _get_worker_healthcheck_timeout() -> int:
    """Return the Uvicorn worker health timeout used during spawn bootstrap."""
    raw = (runtime_env_value("CONTROL_WORKER_HEALTHCHECK_TIMEOUT") or "60").strip()
    try:
        timeout = int(raw)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid CONTROL_WORKER_HEALTHCHECK_TIMEOUT value %r; defaulting to 60",
            raw,
        )
        return 60
    if timeout <= 0:
        logger.warning(
            "Invalid CONTROL_WORKER_HEALTHCHECK_TIMEOUT value %r; defaulting to 60",
            raw,
        )
        return 60
    return timeout


def _https_listener(workers: int):
    """Where the HTTPS listener binds, and its certificate; None without HTTPS.

    It binds the LAN only with USR_ALLOW_REMOTE_BIND, like the plain one. Each
    API worker serves both listeners.
    """
    from ..backend_tls import TLSConfigurationError, tls_enabled, tls_files, tls_port

    if not tls_enabled():
        return None
    try:
        files = tls_files()
    except TLSConfigurationError:
        logger.exception("HTTPS is misconfigured; serving plain HTTP only")
        return None
    if files is None:
        return None
    return ("0.0.0.0" if allow_remote_bind(False) else "127.0.0.1"), tls_port(), files


async def _serve_together(servers: list[uvicorn.Server], sockets: list | None = None) -> None:
    """Run the listeners until one stops, then stop the others.

    With sockets (one per server, bound by the parent), each serves its own.
    """
    tasks = [
        asyncio.create_task(server.serve(sockets=[sockets[index]] if sockets else None))
        for index, server in enumerate(servers)
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for server in servers:
        server.should_exit = True
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        task.result()


class _BothListeners:
    """One API worker process serving the plain and the HTTPS socket.

    uvicorn's supervisor hands every worker the sockets the parent bound; this
    is its target, so it must pickle: it holds only the two configs.
    """

    def __init__(self, plain: uvicorn.Config, https: uvicorn.Config) -> None:
        self.plain = plain
        self.https = https

    def __call__(self, sockets: list | None = None) -> None:
        # As uvicorn.Server.run does: the loop uvicorn is configured for (uvloop).
        asyncio.run(
            _serve_together([uvicorn.Server(self.plain), uvicorn.Server(self.https)], sockets),
            loop_factory=self.plain.get_loop_factory(),
        )


def run_control():
    try:
        run_mode = get_runtime_mode()
        # Uvicorn imports the application independently in each worker. Publish
        # one token before spawning them so bootstrap polling does not mistake
        # a request handled by another worker for a backend restart.
        os.environ.setdefault(
            "USR_BACKEND_STARTUP_ID",
            setup.get_backend_startup_id(),
        )
        default_port = int(os.environ.get("CONTROL_PORT", "21121"))
        host = os.environ.get("CONTROL_HOST", get_default_bind_host("127.0.0.1"))
        if host in {"0.0.0.0", "::"} and not allow_remote_bind(False):
            logger.warning(
                "Remote bind requested but USR_ALLOW_REMOTE_BIND is not enabled; forcing loopback bind"
            )
            host = "127.0.0.1"
        port = (
            _find_available_port(default_port) if run_mode == "webui" else default_port
        )
        if port != default_port:
            logger.info(f"Port {default_port} in use, using port {port}")

        workers = _get_worker_count()
        worker_healthcheck_timeout = _get_worker_healthcheck_timeout()
        if workers > 1:
            logger.info(
                "Starting uvicorn with %d workers (health timeout: %ds)",
                workers,
                worker_healthcheck_timeout,
            )

        log_security_startup_report(
            bind_host=host,
            cors_origins=_runtime_security.allowed_origins,
            trusted_hosts=_runtime_security.allowed_hosts,
            docs_enabled=_runtime_security.docs_enabled,
        )

        original_stdin = sys.stdin
        try:
            # ``run_control`` is normally itself a multiprocessing child. A
            # terminal descriptor inherited by that child may not survive
            # uvicorn's spawn boundary, which makes every worker crash while
            # reopening stdin. The API workers do not consume stdin.
            if workers > 1:
                sys.stdin = None

            tls = _https_listener(workers)
            if tls is None:
                uvicorn.run(
                    "ultima_scraper_backend.api.app:app",
                    host=host,
                    port=port,
                    workers=workers,
                    timeout_worker_healthcheck=worker_healthcheck_timeout,
                    log_config=None,
                )
            elif workers > 1:
                from uvicorn.supervisors import Multiprocess

                tls_host, tls_port, files = tls
                logger.info(
                    "Serving HTTPS for other machines on %s:%s from each of %d workers", tls_host, tls_port, workers
                )
                plain = uvicorn.Config(
                    "ultima_scraper_backend.api.app:app",
                    host=host,
                    port=port,
                    workers=workers,
                    timeout_worker_healthcheck=worker_healthcheck_timeout,
                    log_config=None,
                )
                https = uvicorn.Config(
                    "ultima_scraper_backend.api.app:app",
                    host=tls_host,
                    port=tls_port,
                    ssl_certfile=str(files.cert_file),
                    ssl_keyfile=str(files.key_file),
                    # The worker's plain listener runs the app's startup.
                    lifespan="off",
                    log_config=None,
                )
                Multiprocess(
                    plain, target=_BothListeners(plain, https), sockets=[plain.bind_socket(), https.bind_socket()]
                ).run()
            else:
                tls_host, tls_port, files = tls
                logger.info("Serving HTTPS for other machines on %s:%s", tls_host, tls_port)
                asyncio.run(
                    _serve_together(
                        [
                            uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_config=None)),
                            # Same app object: its startup already ran for the plain listener.
                            uvicorn.Server(
                                uvicorn.Config(
                                    app,
                                    host=tls_host,
                                    port=tls_port,
                                    ssl_certfile=str(files.cert_file),
                                    ssl_keyfile=str(files.key_file),
                                    lifespan="off",
                                    log_config=None,
                                )
                            ),
                        ]
                    )
                )
        finally:
            sys.stdin = original_stdin
    except Exception as exc:
        logger.exception("Control plane startup failed: %s", exc)
        raise


if __name__ == "__main__":
    run_control()
