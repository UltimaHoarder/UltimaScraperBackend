"""Plugin platform service.

This module is the single entry point for the declarative plugin runtime.
Route handlers, startup code, and the file watcher all delegate here instead
of reaching into registry / secrets / state / delivery directly.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import tempfile
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import websockets
import yaml
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from ultima_scraper_backend.job_system.manager import JobManager
from ultima_scraper_backend.redis_channels import JOB_EVENTS_CHANNEL

from .delivery import DeliveryAttempt, EventDeliveryEngine
from .grants import PluginCapabilityGrantStore
from .http_transport import HTTPResponse, default_http_request
from .manifest import PluginManifest, load_manifest_from_dict
from .mapping import (
    apply_field_mappings,
    build_native_job_from_remote,
    evaluate_mapping_node,
    render_template,
    resolve_path,
)
from .outbound_policy import validate_plugin_http_url, validate_plugin_websocket_url
from .permissions import PluginCapability, check_capability
from .registry import PluginRecord, PluginRegistry, PluginStatus
from .repository import PluginRepository
from .runtime_store import (
    CallbackRegistrationRecord,
    PluginRuntimeStore,
    WebSocketMessageRecord,
    WebSocketSessionRecord,
)
from .sdk import SDK_VERSION
from .secrets import SecretManager
from .state_store import PluginStateStore
from .submission import RemoteSubmissionRecord

logger = logging.getLogger(__name__)


def _next_correlation_id() -> str:
    import uuid

    return uuid.uuid4().hex


_TERMINAL_SUBMISSION_STATES = frozenset({"completed", "failed", "cancelled"})
_WS_INBOUND_REMOTE_ACTIONS = frozenset({"claim", "dispatch", "execute", "job"})


def _bounded_env_int(
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %d", name, raw, default)
        return default
    return min(maximum, max(minimum, parsed))


def _exception_summary(exc: BaseException) -> str:
    """Return a single-line exception summary suitable for supervisor logs."""
    detail = " ".join(str(exc).split())
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


async def _default_websocket_connect(url: str, headers: dict[str, str]) -> Any:
    """Open a websocket connection using the project's default client."""
    parameters = inspect.signature(websockets.connect).parameters
    kwargs: dict[str, Any] = {
        (
            "additional_headers"
            if "additional_headers" in parameters
            else "extra_headers"
        ): headers
    }
    if "proxy" in parameters:
        # The validated destination must remain the actual destination; an
        # ambient process proxy would create an undeclared network hop.
        kwargs["proxy"] = None
    return await websockets.connect(url, **kwargs)


from .services import PluginServices


class PluginPlatform(PluginServices):
    """Unified plugin platform facade."""

    def __init__(
        self,
        plugins_dir: Path,
        *,
        http_post: Any = None,
        http_request: Any = None,
        websocket_connect: Any = None,
        public_base_url: str | None = None,
        repository: Any = None,
    ) -> None:
        runtime_dir = plugins_dir / ".runtime"
        self.repository = repository or PluginRepository(
            runtime_dir / "plugin-runtime.sqlite3",
            legacy_runtime_dir=runtime_dir,
        )
        self._catalog_revision = self.repository.get_catalog_revision()
        self.registry = PluginRegistry(plugins_dir, manifest_loader=self._load_active_manifest, is_uninstalled=self.is_uninstalled)
        self.secrets = SecretManager(repository=self.repository)
        self.state = PluginStateStore(repository=self.repository)
        self.runtime_store = PluginRuntimeStore(repository=self.repository)
        self.capability_grants = PluginCapabilityGrantStore(
            repository=self.repository
        )
        self._uses_default_http_transport = http_post is None and http_request is None
        self._uses_default_websocket_transport = websocket_connect is None
        self.delivery = EventDeliveryEngine(
            http_post=http_post,
            repository=self.repository,
            url_resolver=self.configured_url,
            url_validator=(
                validate_plugin_http_url
                if self._uses_default_http_transport
                else None
            ),
        )
        self._plugins_dir = plugins_dir
        self._http_request = http_request or (
            self._wrap_http_post(http_post)
            if http_post is not None
            else default_http_request
        )
        self._websocket_connect = websocket_connect or _default_websocket_connect
        self._public_base_url = (
            public_base_url
            or os.environ.get(
                "USR_PLUGIN_PUBLIC_BASE_URL", "http://127.0.0.1:8000/api/v1"
            )
        ).rstrip("/")
        self._backend: Any = None
        self._runtime_started = False
        self._supervisor_tasks: dict[str, list[asyncio.Task[None]]] = {}
        self._job_event_task: asyncio.Task[None] | None = None
        self._reconcile_task: asyncio.Task[None] | None = None
        self._configuration_task: asyncio.Task[None] | None = None
        self._schedule_task: asyncio.Task[None] | None = None
        delivery_queue_limit = _bounded_env_int(
            "USR_PLUGIN_DELIVERY_QUEUE_LIMIT",
            1000,
            minimum=1,
            maximum=100_000,
        )
        self._delivery_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=delivery_queue_limit
        )
        self._delivery_workers: list[asyncio.Task[None]] = []
        self._ws_live_sessions: dict[str, Any] = {}
        self._ws_send_locks: dict[str, asyncio.Lock] = {}
        self._local_job_index: dict[str, str] = {}
        try:
            self._usr_version = package_version("ultimascraper")
        except PackageNotFoundError:
            self._usr_version = "0.1.0"
        except Exception:
            logger.debug("Failed to resolve USR package version", exc_info=True)
            self._usr_version = "0.1.0"
        # In-memory submission tracking (correlation state)
        self._submissions: dict[str, RemoteSubmissionRecord] = {}
        # Index: external_job_id → correlation_id  (for callback correlation)
        self._external_id_index: dict[str, str] = {}
        self._restore_runtime_state()
        self._durable_delivery = None
        if self.repository.storage_kind == "postgresql":
            from .durable_delivery import DurableDeliveryRuntime
            self._durable_delivery = DurableDeliveryRuntime(self)

    @staticmethod
    def _wrap_http_post(http_post: Any) -> Any:
        async def _wrapped_request(
            method: str,
            url: str,
            payload: bytes | None,
            headers: dict[str, str],
            *,
            timeout: float = 30.0,
        ) -> HTTPResponse:
            if method.upper() == "POST":
                status_code = await http_post(
                    url,
                    payload or b"",
                    headers,
                    timeout=timeout,
                )
                return HTTPResponse(status_code=int(status_code))
            return await default_http_request(
                method,
                url,
                payload,
                headers,
                timeout=timeout,
            )

        return _wrapped_request

    async def _validate_http_target(
        self,
        manifest: PluginManifest,
        url: str,
        *,
        require_enabled: bool = True,
    ) -> None:
        if manifest.sdk:
            self.require_outbound_scope(manifest.plugin_id, url, require_enabled=require_enabled)
        if self._uses_default_http_transport:
            await asyncio.to_thread(validate_plugin_http_url, manifest, url)

    async def _validate_websocket_target(
        self,
        manifest: PluginManifest,
        url: str,
    ) -> None:
        if manifest.sdk:
            self.require_outbound_scope(manifest.plugin_id, url)
        if self._uses_default_websocket_transport:
            await asyncio.to_thread(validate_plugin_websocket_url, manifest, url)

    def _restore_runtime_state(self) -> None:
        self._refresh_runtime_state()

    def _refresh_runtime_state(self, *, preserve_unpersisted: bool = False) -> None:
        """Refresh durable correlations for cross-worker API consistency."""
        loaded: dict[str, RemoteSubmissionRecord] = {}
        external_index: dict[str, str] = {}
        local_index: dict[str, str] = {}
        for record in self.runtime_store.list_correlations():
            submission = RemoteSubmissionRecord.from_runtime_record(record)
            loaded[submission.correlation_id] = submission
            if submission.external_job_id:
                external_index[submission.external_job_id] = submission.correlation_id
            if submission.local_job_id:
                local_index[submission.local_job_id] = submission.correlation_id
        if preserve_unpersisted:
            for correlation_id, submission in self._submissions.items():
                if correlation_id in loaded:
                    continue
                loaded[correlation_id] = submission
                if submission.external_job_id:
                    external_index[submission.external_job_id] = correlation_id
                if submission.local_job_id:
                    local_index[submission.local_job_id] = correlation_id
        self._submissions = loaded
        self._external_id_index = external_index
        self._local_job_index = local_index

    # ── Bootstrap ─────────────────────────────────────────────────────────

    def load_all(self) -> list[str]:
        """Scan the plugins directory and load all manifests.

        Previously-enabled plugins are automatically re-enabled so they
        survive restarts.
        """
        scan_revision = self.repository.get_catalog_revision()
        loaded = self.registry.scan_plugins_dir()
        present_plugin_ids = self._present_manifest_ids()
        installed_plugin_ids = self.repository.list_installed_plugins()
        previously_missing_ids = self.repository.list_missing_plugins()
        startup_catalog_changed = bool(
            (present_plugin_ids - installed_plugin_ids)
            | (present_plugin_ids & previously_missing_ids)
            | (
                (installed_plugin_ids - present_plugin_ids)
                - previously_missing_ids
            )
        )
        for plugin_id in present_plugin_ids:
            self.repository.set_plugin_missing(plugin_id, False)
        for plugin_id in installed_plugin_ids - present_plugin_ids:
            self.repository.set_plugin_missing(plugin_id, True)
        if startup_catalog_changed:
            self._publish_catalog_change()
        enabled_ids = self._load_enabled_set()
        for plugin_id in loaded:
            record = self.registry.get(plugin_id)
            if record and record.manifest:
                self._init_secrets_for_plugin(record)
                self.state.set_quota(
                    plugin_id,
                    record.manifest.operational_policy.state_quota_bytes,
                )
                if plugin_id in enabled_ids:
                    if not self.capability_grants.has_record(plugin_id):
                        self.capability_grants.grant(
                            plugin_id,
                            {cap.value for cap in record.manifest.capabilities},
                        )
                    try:
                        preflight = self.preflight_plugin(plugin_id)
                        if preflight["ready"]:
                            self.registry.enable_plugin(plugin_id)
                        else:
                            logger.warning(
                                "Plugin %s was not re-enabled: %s",
                                plugin_id,
                                preflight["summary"],
                            )
                    except ValueError:
                        pass
        for plugin_id in self.repository.list_missing_plugins():
            if plugin_id not in present_plugin_ids and not self.is_uninstalled(plugin_id):
                self.registry.mark_missing(plugin_id)
        # Capture the revision from before the scan, then perform a normal
        # cross-worker refresh. A catalog mutation racing startup is therefore
        # observed instead of being accidentally acknowledged without a scan.
        self._catalog_revision = scan_revision
        self._sync_catalog_from_repository()
        return loaded

    def attach_backend(self, backend: Any) -> None:
        """Attach the active control-plane backend for native execution/runtime."""
        self._backend = backend

    async def start_runtime(self) -> None:
        """Start plugin runtime supervisors for all enabled plugins."""
        if self._runtime_started:
            return
        self._runtime_started = True
        self._sync_persisted_configuration()
        await self._reconcile_runtime()
        self._job_event_task = asyncio.create_task(
            self._durable_delivery.consume_loop() if self._durable_delivery else self._job_event_loop()
        )
        self._configuration_task = asyncio.create_task(
            self._configuration_sync_loop(),
            name="plugin-configuration-sync",
        )
        self._schedule_task = asyncio.create_task(self.schedule_loop(), name="plugin-schedules")
        delivery_worker_count = _bounded_env_int(
            "USR_PLUGIN_DELIVERY_WORKERS",
            4,
            minimum=1,
            maximum=16,
        )
        self._delivery_workers = [
            asyncio.create_task(
                self._delivery_worker_loop(),
                name=f"plugin-delivery-{index + 1}",
            )
            for index in range(delivery_worker_count)
        ]

    async def stop_runtime(self) -> None:
        """Stop all runtime supervisors and websocket/event listeners."""
        self._runtime_started = False
        plugin_ids = set(self._supervisor_tasks) | set(self._ws_live_sessions)
        tasks = self.runtime_tasks()
        tasks.extend(
            task for group in self._supervisor_tasks.values() for task in group
        )
        self._configuration_task = None
        self._schedule_task = None
        self._reconcile_task = None
        self._job_event_task = None
        self._delivery_workers.clear()
        self._supervisor_tasks.clear()
        for task in tasks:
            task.cancel()
        # A failed listener must not prevent the remaining work from stopping
        # before this supervisor relinquishes its database lease.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results, strict=True):
            if isinstance(result, Exception):
                logger.warning(
                    "Plugin task %s stopped after %s",
                    task.get_name(), type(result).__name__,
                )
        for plugin_id in plugin_ids:
            try:
                await self._stop_plugin_supervisors(plugin_id)
            except Exception as error:
                logger.warning(
                    "Plugin %s shutdown state could not be saved: %s",
                    plugin_id, type(error).__name__,
                )

    def runtime_tasks(self) -> list[asyncio.Task[None]]:
        """Core loops that must remain alive for the runtime to be healthy."""
        return [
            task
            for task in (
                self._configuration_task,
                self._schedule_task,
                self._reconcile_task,
                self._job_event_task,
                *self._delivery_workers,
            )
            if task is not None
        ]

    def _sync_persisted_configuration(self) -> None:
        """Apply desired enabled state written by any API worker."""
        self._sync_catalog_from_repository()
        enabled_ids = self.repository.list_enabled_plugins()
        for record in self.registry.list_all():
            plugin_id = record.plugin_id
            should_enable = plugin_id in enabled_ids and not self.is_uninstalled(plugin_id)
            if should_enable and not record.is_active:
                if record.status in {PluginStatus.INVALID, PluginStatus.MISSING}:
                    continue
                try:
                    if self.preflight_plugin(plugin_id)["ready"]:
                        self.registry.enable_plugin(plugin_id)
                except (KeyError, ValueError):
                    logger.exception(
                        "Failed applying desired enabled state for %s", plugin_id
                    )
            elif not should_enable and record.is_active:
                self.registry.disable_plugin(plugin_id)

    def _sync_catalog_from_repository(self) -> None:
        """Refresh worker-local manifests after another worker handles a change."""
        revision = self.repository.get_catalog_revision()
        if revision == self._catalog_revision:
            return
        loaded = self.registry.scan_plugins_dir()
        changed_records: list[PluginRecord] = []
        for plugin_id in loaded:
            record = self.registry.get(plugin_id)
            if record is None or record.manifest is None:
                continue
            self.repository.set_plugin_missing(plugin_id, False)
            self._init_secrets_for_plugin(record)
            self.state.set_quota(
                plugin_id,
                record.manifest.operational_policy.state_quota_bytes,
            )
            self._revoke_removed_capabilities(record)
            changed_records.append(record)
        for plugin_id in self.repository.list_missing_plugins():
            if plugin_id not in self._present_manifest_ids() and not self.is_uninstalled(plugin_id):
                self.registry.mark_missing(plugin_id)
        for record in self.registry.list_all():
            if self.is_uninstalled(record.plugin_id):
                self.registry.remove_plugin(record.plugin_id)
        self._catalog_revision = revision
        # Set the local revision before preflight calls, which themselves use
        # _require_known(), to avoid recursively scanning this same revision.
        for record in changed_records:
            self._disable_if_preflight_failed(record)

    def _publish_catalog_change(self) -> None:
        self._catalog_revision = self.repository.bump_catalog_revision()

    async def _configuration_sync_loop(self) -> None:
        while self._runtime_started:
            await asyncio.sleep(1.0)
            self._sync_persisted_configuration()
            await self._reconcile_runtime()

    async def _delivery_worker_loop(self) -> None:
        if self._durable_delivery:
            await self._durable_delivery.delivery_loop()
            return
        while self._runtime_started:
            event = await self._delivery_queue.get()
            try:
                await self.dispatch_event(**event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Queued plugin delivery failed for %s/%s",
                    event.get("event_family"),
                    event.get("event_type"),
                )
            finally:
                self._delivery_queue.task_done()

    async def _emit_event(self, **event: Any) -> list[DeliveryAttempt]:
        """Queue runtime events without blocking the job-event control path."""
        if self._durable_delivery or not self._runtime_started or not self._delivery_workers:
            return await self.dispatch_event(**event)
        await self._delivery_queue.put(event)
        return []

    def _schedule_runtime_reconcile(self) -> None:
        if not self._runtime_started:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._reconcile_task is not None and not self._reconcile_task.done():
            return
        self._reconcile_task = loop.create_task(self._reconcile_runtime())

    async def _reconcile_runtime(self) -> None:
        enabled_ids = {record.plugin_id for record in self.registry.list_enabled()}
        for plugin_id in list(self._supervisor_tasks):
            if plugin_id not in enabled_ids:
                await self._stop_plugin_supervisors(plugin_id)
        for plugin_id in enabled_ids:
            if plugin_id not in self._supervisor_tasks:
                await self._start_plugin_supervisors(plugin_id)
        self._reconcile_task = None

    async def _start_plugin_supervisors(self, plugin_id: str) -> None:
        record = self.registry.get(plugin_id)
        if record is None or not record.is_active or record.manifest is None:
            return

        manifest = record.manifest
        tasks: list[asyncio.Task[None]] = []
        if manifest.remote_delegation is not None:
            tasks.append(
                asyncio.create_task(
                    self._run_plugin_reconcile_loop(plugin_id),
                    name=f"plugin-reconcile:{plugin_id}",
                )
            )
            if manifest.remote_delegation.role in ("executor", "bidirectional"):
                if manifest.remote_delegation.operations.claim is not None:
                    tasks.append(
                        asyncio.create_task(
                            self._run_plugin_claim_loop(plugin_id),
                            name=f"plugin-claim:{plugin_id}",
                        )
                    )
            if self._plugin_uses_websocket(manifest):
                tasks.append(
                    asyncio.create_task(
                        self._run_plugin_websocket_loop(plugin_id),
                        name=f"plugin-websocket:{plugin_id}",
                    )
                )

        self._supervisor_tasks[plugin_id] = tasks

    async def _stop_plugin_supervisors(self, plugin_id: str) -> None:
        tasks = self._supervisor_tasks.pop(plugin_id, [])
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

        session = self._ws_live_sessions.pop(plugin_id, None)
        if session is not None:
            with suppress(Exception):
                await session.close()

        session_record = self.runtime_store.get_session(plugin_id)
        if session_record is not None:
            session_record.connected = False
            session_record.last_error = None
            self.runtime_store.save_session(session_record)

    def _plugin_uses_websocket(self, manifest: PluginManifest) -> bool:
        _ = manifest
        return False

    def _plugin_runtime_enabled(self, plugin_id: str) -> bool:
        record = self.registry.get(plugin_id)
        return bool(self._runtime_started and record is not None and record.is_active)

    async def _run_plugin_reconcile_loop(self, plugin_id: str) -> None:
        backoff = 5
        consecutive_failures = 0
        while self._plugin_runtime_enabled(plugin_id):
            try:
                await self._ensure_callback_registration(plugin_id)
                await self._reconcile_plugin_submissions(plugin_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures == 1:
                    logger.warning(
                        "Plugin reconcile loop for %s is unavailable (%s), "
                        "retrying in %ds",
                        plugin_id,
                        _exception_summary(exc),
                        backoff,
                    )
                else:
                    logger.debug(
                        "Plugin reconcile retry %d for %s failed; retrying in %ds",
                        consecutive_failures,
                        plugin_id,
                        backoff,
                        exc_info=True,
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if consecutive_failures:
                logger.info(
                    "Plugin reconcile loop for %s recovered after %d failed attempt(s)",
                    plugin_id,
                    consecutive_failures,
                )
            consecutive_failures = 0
            backoff = 5
            await asyncio.sleep(30)

    async def _run_plugin_claim_loop(self, plugin_id: str) -> None:
        backoff = 5
        consecutive_failures = 0
        connected_once = False
        while self._plugin_runtime_enabled(plugin_id):
            try:
                claimed = await self._claim_remote_work(plugin_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures == 1:
                    logger.warning(
                        "Plugin claim loop for %s: remote unavailable (%s), "
                        "retrying in %ds",
                        plugin_id,
                        _exception_summary(exc),
                        backoff,
                    )
                else:
                    logger.debug(
                        "Plugin claim loop retry %d for %s failed; retrying in %ds",
                        consecutive_failures,
                        plugin_id,
                        backoff,
                        exc_info=True,
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if consecutive_failures:
                logger.info(
                    "Plugin claim loop for %s: remote connectivity restored after "
                    "%d failed attempt(s)",
                    plugin_id,
                    consecutive_failures,
                )
            elif not connected_once:
                logger.info(
                    "Plugin claim loop for %s: remote connectivity verified",
                    plugin_id,
                )
            connected_once = True
            consecutive_failures = 0
            backoff = 5
            await asyncio.sleep(1 if claimed else 5)

    async def _run_plugin_websocket_loop(self, plugin_id: str) -> None:
        backoff_seconds = 1
        while self._plugin_runtime_enabled(plugin_id):
            record = self.registry.get(plugin_id)
            manifest = record.manifest if record is not None else None
            rd = manifest.remote_delegation if manifest is not None else None
            if rd is None or rd.websocket is None:
                return

            session_record = self.runtime_store.get_session(
                plugin_id
            ) or WebSocketSessionRecord(
                plugin_id=plugin_id,
                url=rd.websocket.url,
            )
            session_record.url = rd.websocket.url
            self.runtime_store.save_session(session_record)

            websocket_client: Any = None
            try:
                headers = self._build_websocket_headers(plugin_id, rd)
                await self._validate_websocket_target(manifest, rd.websocket.url)
                websocket_client = await self._websocket_connect(
                    rd.websocket.url, headers
                )
                self._ws_live_sessions[plugin_id] = websocket_client
                session_record.connected = True
                session_record.last_connected_at = datetime.now(UTC).isoformat()
                session_record.last_error = None
                session_record.reconnect_attempts = 0
                self.runtime_store.save_session(session_record)

                await self._flush_websocket_backlog(plugin_id, rd)
                backoff_seconds = rd.websocket.reconnect_policy.initial_backoff_seconds

                while self._plugin_runtime_enabled(plugin_id):
                    try:
                        raw_message = await asyncio.wait_for(
                            websocket_client.recv(),
                            timeout=float(rd.websocket.heartbeat_interval_seconds),
                        )
                    except asyncio.TimeoutError:
                        await self._send_session_heartbeat(plugin_id, rd)
                        continue

                    session_record.last_heartbeat_at = datetime.now(UTC).isoformat()
                    self.runtime_store.save_session(session_record)
                    envelope = self._parse_websocket_envelope(rd, raw_message)
                    if envelope is None:
                        continue
                    await self._handle_websocket_envelope(plugin_id, rd, envelope)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                session_record.connected = False
                session_record.last_error = str(exc)
                session_record.reconnect_attempts += 1
                self.runtime_store.save_session(session_record)

                max_retries = rd.websocket.reconnect_policy.max_retries
                if max_retries and session_record.reconnect_attempts > max_retries:
                    logger.warning(
                        "Websocket supervisor for %s exceeded max retries (%s)",
                        plugin_id,
                        max_retries,
                    )
                    return
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(
                    backoff_seconds * 2,
                    rd.websocket.reconnect_policy.max_backoff_seconds,
                )
            finally:
                self._ws_live_sessions.pop(plugin_id, None)
                session_record.connected = False
                self.runtime_store.save_session(session_record)
                if websocket_client is not None:
                    with suppress(Exception):
                        await websocket_client.close()

    async def _job_event_loop(self) -> None:
        backend = self._backend
        redis = getattr(backend, "redis", None)
        if redis is None or not getattr(redis, "is_connected", False):
            logger.info("Plugin runtime job-event loop not started (Redis unavailable)")
            return

        pubsub = None
        try:
            pubsub = cast(Any, redis.client).pubsub()
            await pubsub.subscribe(JOB_EVENTS_CHANNEL)
            async for raw_message in pubsub.listen():
                message = self._normalize_pubsub_message(raw_message)
                if message is None or message.get("type") != "message":
                    continue
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                if not isinstance(data, str):
                    continue
                try:
                    event = cast(dict[str, Any], json.loads(data))
                except json.JSONDecodeError:
                    continue
                await self._process_local_job_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Plugin runtime job-event loop failed")
        finally:
            if pubsub is not None:
                with suppress(Exception):
                    await pubsub.unsubscribe(JOB_EVENTS_CHANNEL)
                with suppress(Exception):
                    await pubsub.close()

    # ── Plugin creation ──────────────────────────────────────────────────

    def create_plugin(self, manifest_data: dict[str, Any]) -> dict[str, Any]:
        """Create a new plugin from a manifest dictionary.

        Validates the manifest, writes ``plugins/<plugin_id>/manifest.yaml``
        atomically, reloads it into the registry, and returns the new
        plugin record in ``disabled`` state.
        """
        self._sync_catalog_from_repository()

        # Validate manifest first — fail before any filesystem mutation
        manifest = load_manifest_from_dict(manifest_data)
        plugin_id = manifest.plugin_id
        normalized_manifest_data = manifest.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )

        # Reject duplicates
        if self.registry.get(plugin_id) is not None:
            raise ValueError(f"Plugin {plugin_id!r} already exists")

        # Path safety: ensure the plugin dir resolves inside _plugins_dir
        plugin_dir = (self._plugins_dir / plugin_id).resolve()
        if not plugin_dir.is_relative_to(self._plugins_dir.resolve()):
            raise ValueError("Invalid plugin_id — path escapes plugins directory")

        # Create directory
        try:
            plugin_dir.mkdir(parents=True, exist_ok=False)
        except FileExistsError as exc:
            # A different API worker may have won a concurrent create after
            # this worker refreshed its local registry.
            raise ValueError(f"Plugin {plugin_id!r} already exists") from exc

        manifest_path = plugin_dir / "manifest.yaml"
        try:
            self._write_manifest_atomically(manifest_path, normalized_manifest_data)
        except BaseException:
            # Clean up the created directory on failure
            try:
                if manifest_path.exists():
                    manifest_path.unlink()
                plugin_dir.rmdir()
            except OSError:
                pass
            raise

        # Reload into registry
        reload_ok = self.registry.reload_plugin(plugin_id)
        record = self.registry.get(plugin_id)
        if reload_ok:
            self.repository.set_plugin_missing(plugin_id, False)
        self._publish_catalog_change()

        if record is None:
            return {
                "plugin_id": plugin_id,
                "status": "created",
                "reload_succeeded": False,
                "error": "Plugin was written but failed to load into registry",
            }

        # Initialize secrets for callbacks/deliveries
        if record.manifest:
            self._init_secrets_for_plugin(record)
            self.state.set_quota(
                plugin_id,
                record.manifest.operational_policy.state_quota_bytes,
            )

        result = self._plugin_view(record)
        result["creation_status"] = "created"
        result["reload_succeeded"] = reload_ok
        return result

    def get_plugin_manifest_yaml(self, plugin_id: str) -> dict[str, Any]:
        record = self._require_known(plugin_id)
        manifest_path = record.manifest_path
        if record.manifest and record.manifest.manifest_schema_version == 2:
            manifest_yaml = yaml.safe_dump(record.manifest.model_dump(mode="json"), sort_keys=False)
        elif manifest_path.is_file():
            manifest_yaml = manifest_path.read_text(encoding="utf-8")
        else:
            raise FileNotFoundError(f"Manifest not found for plugin {plugin_id!r}")
        return {
            "plugin_id": plugin_id,
            "manifest_yaml": manifest_yaml,
            "status": record.status.value,
            "revision": record.revision,
        }

    def update_plugin_manifest_yaml(
        self,
        plugin_id: str,
        manifest_yaml: str,
    ) -> dict[str, Any]:
        record = self._require_known(plugin_id)
        manifest_path = record.manifest_path
        if not manifest_path.parent.exists():
            raise FileNotFoundError(f"Plugin directory missing for {plugin_id!r}")
        try:
            parsed = yaml.safe_load(manifest_yaml)
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Manifest must be a YAML mapping")

        manifest = load_manifest_from_dict(parsed)
        if manifest.plugin_id != plugin_id:
            raise ValueError(
                f"Edited manifest plugin_id {manifest.plugin_id!r} must match {plugin_id!r}"
            )

        normalized_manifest_data = manifest.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
        if record.manifest and record.manifest.manifest_schema_version == 2:
            staged = self.stage_upgrade(plugin_id, normalized_manifest_data)
            return {**self._plugin_view(record), "reload_succeeded": True, "manifest_yaml": manifest_yaml, "staged_package": staged}
        self._write_manifest_atomically(manifest_path, normalized_manifest_data)

        reload_ok = self.registry.reload_plugin(plugin_id)
        updated = self.registry.get(plugin_id)
        if updated is None:
            self._publish_catalog_change()
            raise RuntimeError(
                f"Plugin {plugin_id!r} disappeared after manifest update"
            )

        if reload_ok and updated.manifest:
            self.repository.set_plugin_missing(plugin_id, False)
            self._init_secrets_for_plugin(updated)
            self.state.set_quota(
                plugin_id,
                updated.manifest.operational_policy.state_quota_bytes,
            )
            self._revoke_removed_capabilities(updated)
            self._disable_if_preflight_failed(updated)
        self._publish_catalog_change()
        self._schedule_runtime_reconcile()

        result = self._plugin_view(updated)
        result["reload_succeeded"] = reload_ok
        result["manifest_yaml"] = manifest_path.read_text(encoding="utf-8")
        return result

    # ── Plugin lifecycle ──────────────────────────────────────────────────

    def approve_plugin(self, plugin_id: str) -> dict[str, Any]:
        """Deprecated: approval is no longer required. Alias for enable."""
        return self.enable_plugin(plugin_id)

    def enable_plugin(self, plugin_id: str) -> dict[str, Any]:
        if self.is_uninstalled(plugin_id):
            raise ValueError('Plugin is uninstalled; reinstall it before enabling')
        installation = self.repository.get_host_document(plugin_id, 'installation')
        if installation.get('source') and not installation.get('setup_completed'):
            raise ValueError('Complete plugin setup before enabling')
        record = self._require_known(plugin_id)
        self._init_secrets_for_plugin(record)
        if record.manifest is None:
            raise ValueError(f"Plugin {plugin_id!r} has no valid manifest")
        if not self.capability_grants.has_record(plugin_id) and not (record.manifest.sdk and record.manifest.sdk.setup):
            # Backward-compatible first activation: the Enable action is the
            # operator's approval of the initial requested capability set.
            self.capability_grants.grant(
                plugin_id,
                {cap.value for cap in record.manifest.capabilities},
            )
            self.approve_scopes(record)
        preflight = self.preflight_plugin(plugin_id)
        if not preflight["ready"]:
            raise ValueError(preflight["summary"])
        record = self.registry.enable_plugin(plugin_id)
        record.last_error = None
        self.repository.set_plugin_enabled(plugin_id, True)
        if record.manifest.manifest_schema_version == 2 and not self.repository.active_package(plugin_id):
            self.repository.activate_package(plugin_id, self.package_digest(record))
        self._schedule_runtime_reconcile()
        return self._plugin_view(record)

    def disable_plugin(self, plugin_id: str) -> dict[str, Any]:
        current = self._require_known(plugin_id)
        self.repository.set_plugin_enabled(plugin_id, False)
        self.repository.revoke_api_tokens(plugin_id)
        if current.status in {PluginStatus.INVALID, PluginStatus.MISSING}:
            return self._plugin_view(current)
        record = self.registry.disable_plugin(plugin_id)
        self._schedule_runtime_reconcile()
        return self._plugin_view(record)

    def purge_plugin_data(
        self,
        plugin_id: str,
        confirmation: str,
    ) -> dict[str, Any]:
        """Irreversibly purge retained data for an already-disabled plugin."""
        record = self._require_known(plugin_id)
        expected_confirmation = f"PURGE {plugin_id}"
        if confirmation != expected_confirmation:
            raise ValueError(
                f"Confirmation must exactly match {expected_confirmation!r}"
            )
        if record.is_active or plugin_id in self.repository.list_enabled_plugins():
            raise ValueError("Disable the plugin before purging its retained data")

        counts = self.repository.purge_plugin_data(plugin_id)
        self._refresh_runtime_state()
        return {
            "plugin_id": plugin_id,
            "status": "purged",
            "deleted_records": sum(counts.values()),
            "deleted_by_store": counts,
            "manifest_retained": record.manifest_path.is_file(),
        }

    def reload_plugin(self, plugin_id: str) -> dict[str, Any]:
        self._require_known(plugin_id)
        if plugin_id not in self._present_manifest_ids():
            record = self.registry.mark_missing(plugin_id)
            self.repository.set_plugin_missing(plugin_id, True)
            self._publish_catalog_change()
            self._schedule_runtime_reconcile()
            return self._plugin_view(record)
        ok = self.registry.reload_plugin(plugin_id)
        record = self.registry.get(plugin_id)
        if record is None:
            self._publish_catalog_change()
            return {"plugin_id": plugin_id, "status": "not_found"}
        if ok and record.manifest:
            self.repository.set_plugin_missing(plugin_id, False)
            self._init_secrets_for_plugin(record)
            self.state.set_quota(
                plugin_id,
                record.manifest.operational_policy.state_quota_bytes,
            )
            self._revoke_removed_capabilities(record)
            self._disable_if_preflight_failed(record)
        self._publish_catalog_change()
        self._schedule_runtime_reconcile()
        return self._plugin_view(record)

    def approve_capabilities(self, plugin_id: str) -> dict[str, Any]:
        """Approve the manifest's exact requested capability set."""
        record = self._require_known(plugin_id)
        if record.manifest is None:
            raise ValueError(f"Plugin {plugin_id!r} has no valid manifest")
        self.approve_scopes(record)
        self.capability_grants.grant(
            plugin_id,
            {cap.value for cap in record.manifest.capabilities},
        )
        return self._plugin_view(record)

    def handle_filesystem_changes(
        self,
        added: list[str],
        modified: list[str],
        removed: list[str],
    ) -> dict[str, list[str]]:
        """Apply watcher-detected plugin directory changes to live state."""
        loaded: list[str] = []
        deleted: list[str] = []
        enabled_ids = self._load_enabled_set()

        for plugin_id in sorted(set(added + modified)):
            if self.is_uninstalled(plugin_id):
                continue
            ok = self.registry.reload_plugin(plugin_id)
            record = self.registry.get(plugin_id)
            if not ok or record is None or record.manifest is None:
                continue
            self.repository.set_plugin_missing(plugin_id, False)
            self._init_secrets_for_plugin(record)
            self.state.set_quota(
                plugin_id,
                record.manifest.operational_policy.state_quota_bytes,
            )
            self._revoke_removed_capabilities(record)
            if plugin_id in enabled_ids:
                if not self.capability_grants.has_record(plugin_id):
                    self.capability_grants.grant(
                        plugin_id,
                        {cap.value for cap in record.manifest.capabilities},
                    )
                try:
                    if self.preflight_plugin(plugin_id)["ready"]:
                        self.registry.enable_plugin(plugin_id)
                except ValueError:
                    pass
            self._disable_if_preflight_failed(record)
            loaded.append(plugin_id)

        for plugin_id in sorted(set(removed)):
            if self.is_uninstalled(plugin_id):
                self.registry.remove_plugin(plugin_id)
                continue
            self.registry.mark_missing(plugin_id)
            self.repository.set_plugin_missing(plugin_id, True)
            deleted.append(plugin_id)

        if added or modified or removed:
            self._publish_catalog_change()
        self._schedule_runtime_reconcile()
        return {"loaded": loaded, "removed": deleted}

    # ── Plugin queries ────────────────────────────────────────────────────

    def list_plugins(self) -> list[dict[str, Any]]:
        self._sync_persisted_configuration()
        return [self._plugin_view(r) for r in self.registry.list_all() if not self.is_uninstalled(r.plugin_id)]

    def list_active_webui(self) -> list[dict[str, Any]]:
        """Return enabled plugins that provide WebUI extensions."""
        self._sync_persisted_configuration()
        results: list[dict[str, Any]] = []
        for record in self.registry.list_all():
            if not record.is_active or record.manifest is None:
                continue
            if record.manifest.webui is None and not (record.manifest.sdk and record.manifest.sdk.ui):
                continue
            if not self._has_capability(record, PluginCapability.WEBUI_EXTEND):
                continue
            if record.manifest.manifest_schema_version == 2 and record.manifest.webui and record.manifest.webui.mode == "native" and not self._has_capability(record, PluginCapability.WEBUI_NATIVE):
                continue
            digest = self.package_digest(record)
            webui = record.manifest.webui
            entry_path = webui.entry.lstrip("/") if webui else ""
            results.append(
                {
                    "plugin_id": record.manifest.plugin_id,
                    "display_name": record.manifest.display_name,
                    "plugin_version": record.manifest.plugin_version,
                    "package_digest": digest,
                    "sdk_version": SDK_VERSION,
                    "mode": webui.mode if webui else "declarative",
                    "access": self.approved_access(record) if record.manifest.sdk else None,
                    "contributions": [item.model_dump(mode="json") for item in record.manifest.sdk.ui] if record.manifest.sdk else [],
                    "entry": f"/api/v1/plugins/{record.manifest.plugin_id}/assets/{entry_path}?package={digest}" if entry_path else "",
                    "styles": [
                        f"/api/v1/plugins/{record.manifest.plugin_id}/assets/{s.lstrip('/')}?package={digest}"
                        for s in webui.styles
                    ] if webui else [],
                }
            )
        return results

    def get_plugin_asset_path(self, plugin_id: str, asset_path: str, *, package: str | None = None) -> Path:
        """Resolve and validate a static asset path inside a plugin directory."""
        self._sync_persisted_configuration()
        record = self.registry.get(plugin_id)
        if record is None or record.manifest_path is None:
            raise KeyError(f"Plugin {plugin_id!r} not found")
        if not record.is_active or not self._has_capability(record, PluginCapability.WEBUI_EXTEND):
            raise PermissionError("Plugin WebUI access is disabled or revoked")
        if record.manifest.manifest_schema_version == 2 and record.manifest.webui and record.manifest.webui.mode == "native" and not self._has_capability(record, PluginCapability.WEBUI_NATIVE):
            raise PermissionError("Trusted native UI permission was revoked")
        digest = self.package_digest(record)
        if package is not None and package != digest:
            raise PermissionError("Plugin asset belongs to an inactive package")
        plugin_dir = self.package_directory(plugin_id, digest).resolve()
        target = (plugin_dir / asset_path).resolve()
        try:
            target.relative_to(plugin_dir)
        except ValueError as exc:
            raise PermissionError("Access denied: path traversal detected") from exc
        if not target.is_file():
            raise FileNotFoundError(f"Asset {asset_path!r} not found")
        declared = {record.manifest.webui.entry, *record.manifest.webui.styles} if record.manifest.webui else set()
        # Only declared browser assets are public; backend code and package
        # manifests can contain configuration and are never served here.
        if asset_path not in declared:
            raise PermissionError("Asset is not declared by the WebUI contract")
        return target

    def get_plugin(self, plugin_id: str) -> dict[str, Any] | None:
        self._sync_persisted_configuration()
        record = self.registry.get(plugin_id)
        return self._plugin_view(record) if record else None

    def preflight_plugin(self, plugin_id: str) -> dict[str, Any]:
        """Validate whether a plugin can be safely activated."""
        record = self._require_known(plugin_id)
        manifest = record.manifest
        if record.status == PluginStatus.MISSING:
            return {
                "plugin_id": plugin_id,
                "ready": False,
                "summary": record.last_error or "Plugin manifest is missing",
                "checks": [
                    {
                        "name": "manifest",
                        "status": "failed",
                        "message": record.last_error or "Plugin manifest is missing",
                    }
                ],
            }
        if manifest is None:
            return {
                "plugin_id": plugin_id,
                "ready": False,
                "summary": record.last_error or "Plugin manifest is invalid",
                "checks": [
                    {
                        "name": "manifest",
                        "status": "failed",
                        "message": record.last_error or "Manifest is invalid",
                    }
                ],
            }

        requested = {cap.value for cap in manifest.capabilities}
        granted = self.capability_grants.get(plugin_id)
        pending = sorted(requested - granted)
        compatibility = SpecifierSet(manifest.usr_compatibility)
        compatible = Version(self._usr_version) in compatibility
        secrets_count = len(self.secrets.list_for_plugin(plugin_id))

        checks = [
            {
                "name": "manifest",
                "status": "passed",
                "message": f"Manifest schema v{manifest.manifest_schema_version} is valid",
            },
            {
                "name": "compatibility",
                "status": "passed" if compatible else "failed",
                "message": (
                    f"USR {self._usr_version} satisfies {manifest.usr_compatibility}"
                    if compatible
                    else f"USR {self._usr_version} does not satisfy {manifest.usr_compatibility}"
                ),
            },
            {
                "name": "capabilities",
                "status": "approval_required" if pending else "passed",
                "message": (
                    f"Approval required for: {', '.join(pending)}"
                    if pending
                    else f"{len(granted)} capability grant(s) approved"
                ),
            },
            {
                "name": "secrets",
                "status": "passed",
                "message": f"{secrets_count} managed secret scope(s) initialized",
            },
        ]
        sdk_checks, pending_access = self.sdk_checks(manifest)
        checks.extend(sdk_checks)
        ready = compatible and not pending and not pending_access and all(check["status"] == "passed" for check in sdk_checks)
        failed = next(
            (check["message"] for check in checks if check["status"] == "failed"),
            None,
        )
        summary = (
            str(failed)
            if failed
            else (
                f"Capability approval required: {', '.join(pending)}"
                if pending
                else next((check["message"] for check in sdk_checks if check["status"] != "passed"), "Plugin passed activation preflight")
            )
        )
        return {
            "plugin_id": plugin_id,
            "ready": ready,
            "summary": summary,
            "usr_version": self._usr_version,
            "compatibility": manifest.usr_compatibility,
            "requested_capabilities": sorted(requested),
            "granted_capabilities": sorted(granted),
            "pending_capabilities": pending,
            "pending_scopes": pending_access,
            "checks": checks,
        }

    def _disable_if_preflight_failed(self, record: PluginRecord) -> None:
        if not record.is_active:
            return
        preflight = self.preflight_plugin(record.plugin_id)
        if preflight["ready"]:
            return
        self.registry.disable_plugin(record.plugin_id)
        record.last_error = preflight["summary"]
        self.repository.set_plugin_enabled(record.plugin_id, False)

    def _revoke_removed_capabilities(self, record: PluginRecord) -> None:
        """Drop grants removed from a newer manifest revision."""
        if (
            record.manifest is None
            or not self.capability_grants.has_record(record.plugin_id)
        ):
            return
        requested = {cap.value for cap in record.manifest.capabilities}
        granted = self.capability_grants.get(record.plugin_id)
        retained = granted & requested
        if retained != granted:
            self.capability_grants.grant(record.plugin_id, retained)

    def _runtime_owner_status(self) -> dict[str, Any] | None:
        return self.repository.get_runtime_owner()

    def _runtime_available(self) -> bool:
        if self._runtime_started:
            return True
        owner = self._runtime_owner_status()
        return bool(owner and owner["active"])

    def _plugin_view(self, record: PluginRecord) -> dict[str, Any]:
        plugin_id = (
            record.manifest.plugin_id
            if record.manifest is not None
            else record.manifest_path.parent.name
        )
        if record.manifest is not None:
            result = record.to_dict()
        else:
            result = {
                "plugin_id": plugin_id,
                "display_name": plugin_id,
                "plugin_version": "unknown",
                "status": record.status.value,
                "capabilities": [],
                "loaded_at": record.loaded_at.isoformat(),
                "enabled_at": None,
                "disabled_at": None,
                "last_error": record.last_error,
                "revision": record.revision,
            }
        preflight = self.preflight_plugin(plugin_id)
        pending = preflight.get("pending_capabilities", [])
        if record.status == PluginStatus.MISSING:
            health_status = "missing"
            health_message = record.last_error or "Plugin manifest is missing"
        elif record.status.value == "invalid":
            health_status = "invalid"
            health_message = record.last_error or "Manifest is invalid"
        elif pending or preflight.get("pending_scopes"):
            health_status = "approval_required"
            health_message = preflight["summary"]
        elif not preflight["ready"]:
            health_status = "incompatible"
            health_message = preflight["summary"]
        elif not record.is_active:
            health_status = "disabled"
            health_message = "Plugin is disabled"
        elif not self._runtime_available():
            health_status = "starting"
            health_message = "Waiting for plugin runtime"
        else:
            manifest = record.manifest
            needs_supervisor = bool(manifest and manifest.remote_delegation is not None)
            supervisor_count = len(self._supervisor_tasks.get(record.plugin_id, []))
            if self._runtime_started and needs_supervisor and supervisor_count == 0:
                health_status = "starting"
                health_message = "Runtime supervisor is starting"
            else:
                health_status = "healthy"
                health_message = "Activation checks passed"

        result.update(
            {
                "manifest_schema_version": (
                    record.manifest.manifest_schema_version if record.manifest else None
                ),
                "usr_compatibility": (
                    record.manifest.usr_compatibility if record.manifest else None
                ),
                "requested_capabilities": preflight.get("requested_capabilities", []),
                "granted_capabilities": preflight.get("granted_capabilities", []),
                "pending_capabilities": pending,
                "pending_scopes": preflight.get("pending_scopes", []),
                "runtime_storage": self.repository.storage_kind,
                "sdk_version": SDK_VERSION,
                "setup_required": self.setup_description(plugin_id)["required"],
                "setup_ready": self.setup_description(plugin_id)["ready"],
                "schedules": self.schedule_status(plugin_id),
                "has_settings": bool(record.manifest and self.configuration_schema(record.manifest) is not None),
                "delivery_queue": self.repository.queue_health(plugin_id),
                "active_package": self.repository.active_package(plugin_id),
                "ui_mode": record.manifest.webui.mode if record.manifest and record.manifest.webui else "declarative",
                "preflight_ready": bool(preflight.get("ready", False)),
                "health_status": health_status,
                "health_message": health_message,
            }
        )
        return result

    def _has_capability(
        self,
        record: PluginRecord,
        capability: PluginCapability,
    ) -> bool:
        return (
            record.manifest is not None
            and check_capability(record.manifest, capability)
            and capability.value in self.capability_grants.get(record.plugin_id)
        )

    # ── Capability-gated operations ───────────────────────────────────────

    def submit_job(
        self,
        plugin_id: str,
        task_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate and return a normalized job submission request.

        Actual job activation is handled by the caller using USR's
        JobManager — this method only validates the plugin has permission
        and the payload matches the declared schema.
        """
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        if not self._has_capability(record, PluginCapability.JOBS_SUBMIT):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have {PluginCapability.JOBS_SUBMIT.value}"
            )

        declared_types = {jt.task_type for jt in manifest.task_types}
        if task_type not in declared_types:
            raise ValueError(
                f"Plugin {plugin_id!r} does not declare task_type {task_type!r}; "
                f"available: {sorted(declared_types)}"
            )

        # Validate payload against declared fields
        jt_def = next(jt for jt in manifest.task_types if jt.task_type == task_type)
        errors = self._validate_job_payload(jt_def, payload) if jt_def.input_schema is None else []
        if jt_def.input_schema is not None:
            from .sdk import validate_schema
            validate_schema(jt_def.input_schema, payload)
        if errors:
            raise ValueError(f"Task payload validation failed: {'; '.join(errors)}")

        return {
            "plugin_id": plugin_id,
            "task_type": task_type,
            "job_type": task_type,
            "payload": payload,
            "created_by": f"plugin:{plugin_id}",
        }

    def read_worker_status(self, plugin_id: str) -> dict[str, Any]:
        """Placeholder for worker-status reads.

        Actual worker data comes from the WorkerRegistry; this method
        only validates the capability gate.
        """
        record = self._require_enabled(plugin_id)
        if not self._has_capability(record, PluginCapability.WORKERS_STATUS_READ):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.WORKERS_STATUS_READ.value}"
            )
        return {"plugin_id": plugin_id, "allowed": True}

    # ── Remote delegation operations ──────────────────────────────────────

    def submit_delegated_job(
        self,
        plugin_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate a delegated job submission and return normalized config.

        This performs capability/config validation and returns a submission
        descriptor.  To actually send the HTTP request, call
        ``execute_remote_submission()`` with the returned descriptor.
        """
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        if not self._has_capability(record, PluginCapability.REMOTE_JOBS_SUBMIT):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.REMOTE_JOBS_SUBMIT.value}"
            )

        rd = manifest.remote_delegation
        if rd is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no remote_delegation configuration"
            )
        if rd.role not in ("submitter", "bidirectional"):
            raise ValueError(
                f"Plugin {plugin_id!r} remote_delegation role is {rd.role!r}, "
                f"not 'submitter' or 'bidirectional'"
            )
        if rd.operations.submission is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no operations.submission configuration"
            )

        correlation_id = _next_correlation_id()
        runtime_context: dict[str, Any] = {}
        if rd.auth_export is not None:
            runtime_context[rd.auth_export.runtime_key] = self._export_auth_bundle(
                plugin_id,
                manifest,
                payload,
            )
        return {
            "plugin_id": plugin_id,
            "integration_id": rd.integration_id,
            "correlation_id": correlation_id,
            "external_system_url": rd.external_system_url,
            "operation": rd.operations.submission.model_dump(
                mode="json", exclude_none=True
            ),
            "correlation": {
                "local_id_field": rd.correlation.local_id_field,
                "remote_id_field": rd.correlation.remote_id_field,
                "submitter_id_field": rd.correlation.submitter_id_field,
            },
            "payload": payload,
            "runtime_context": runtime_context,
            "created_by": f"plugin:{plugin_id}",
        }

    async def execute_remote_submission(
        self,
        submission: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Perform the actual HTTP POST to a remote control plane.

        Takes the output of ``submit_delegated_job()`` and:
        1. Resolves the target URL from plugin config
        2. Gets the auth secret from the secret store
        3. Builds the HTTP request from the configured operation mapping
        4. Sends the request with retry/backoff
        5. Persists a ``RemoteSubmissionRecord`` for correlation tracking
        6. Returns a normalized acknowledgement
        """
        plugin_id = submission["plugin_id"]
        integration_id = submission["integration_id"]
        correlation_id = submission["correlation_id"]
        payload = submission["payload"]
        extra_runtime_context = cast(
            dict[str, Any],
            submission.get("runtime_context") or {},
        )
        manifest_record = self.registry.get(plugin_id)
        rd = (
            manifest_record.manifest.remote_delegation
            if manifest_record is not None and manifest_record.manifest is not None
            else None
        )
        if rd is None or rd.operations.submission is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no operations.submission configuration"
            )
        manifest = cast(PluginManifest, manifest_record.manifest)
        operation = rd.operations.submission

        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:16]
        record = RemoteSubmissionRecord(
            correlation_id=correlation_id,
            plugin_id=plugin_id,
            integration_id=integration_id,
            payload_hash=payload_hash,
        )
        (
            target_url,
            headers,
            payload_bytes,
            outbound_payload,
        ) = self._build_http_operation_request(
            plugin_id,
            rd,
            operation,
            payload=payload,
            runtime_context={
                "plugin_id": plugin_id,
                "integration_id": integration_id,
                "correlation_id": correlation_id,
                "external_job_id": None,
                "action": "submission",
                "idempotency_key": idempotency_key,
                **extra_runtime_context,
            },
        )
        record.last_outbound_command = {
            "action": "submit",
            "transport": "http",
            "target": target_url,
            "payload": outbound_payload,
        }

        if manifest_record and manifest_record.manifest:
            retry_policy = manifest_record.manifest.operational_policy.retry_policy
            max_retries = retry_policy.max_retries
            initial_backoff = retry_policy.initial_backoff_seconds
            max_backoff = retry_policy.max_backoff_seconds
            backoff_multiplier = retry_policy.backoff_multiplier
        else:
            max_retries = 3
            initial_backoff = 2
            max_backoff = 30
            backoff_multiplier = 2.0

        last_error: str | None = None
        status_code: int | None = None
        response_body: dict[str, Any] | None = None
        message_id: str | None = None
        backoff = initial_backoff

        for attempt in range(max_retries + 1):
            record.last_attempt_at = datetime.now(UTC).isoformat()
            record.retry_count = attempt
            record.touch()
            try:
                await self._validate_http_target(manifest, target_url)
                response = await self._http_request(
                    operation.method,
                    target_url,
                    payload_bytes,
                    headers,
                )
                status_code = response.status_code
                parsed_body = self._parse_response_body(response.body)
                response_body = (
                    cast(dict[str, Any], parsed_body)
                    if isinstance(parsed_body, dict)
                    else None
                )
                if 200 <= status_code < 300:
                    record.status = "submitted"
                    last_error = None
                else:
                    last_error = f"HTTP {status_code}"
                    record.last_error = last_error

                self._apply_response_mapping(
                    record,
                    response_body,
                    operation.response_mapping,
                )
                if 200 <= status_code < 300:
                    break
            except Exception as exc:
                last_error = str(exc)
                record.last_error = last_error

            if attempt < max_retries:
                await asyncio.sleep(min(backoff, max_backoff))
                backoff = min(backoff * backoff_multiplier, max_backoff)

        if last_error:
            record.status = "failed"

        self._submissions[correlation_id] = record
        self._persist_submission_record(record)

        logger.info(
            "Remote submission %s for plugin %s: status=%s external_job_id=%s",
            correlation_id,
            plugin_id,
            record.status,
            record.external_job_id,
        )

        return {
            "correlation_id": correlation_id,
            "plugin_id": plugin_id,
            "integration_id": integration_id,
            "status": record.status,
            "external_job_id": record.external_job_id,
            "http_status_code": status_code,
            "response_body": response_body,
            "last_error": last_error,
            "retry_count": record.retry_count,
            "payload_hash": payload_hash,
            "message_id": message_id,
        }

    def set_external_job_id(
        self,
        correlation_id: str,
        external_job_id: str,
    ) -> None:
        """Associate a remote-side job ID with a submission correlation."""
        self._refresh_runtime_state()
        rec = self._submissions.get(correlation_id)
        if rec is None:
            raise KeyError(f"Unknown correlation_id {correlation_id!r}")
        rec.external_job_id = external_job_id
        self._external_id_index[external_job_id] = correlation_id
        rec.touch()
        self._persist_submission_record(rec)

    # ── Executor operations (USWebAdmin-style plugins) ────────────────────

    async def execute_remote_worker_action(
        self,
        plugin_id: str,
        action: str,
        remote_job_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Perform an executor action on the remote control plane.

        Actions: 'claim', 'heartbeat', 'progress', 'complete', 'fail', 'cancel'.
        The executor plugin must have ``remote.jobs.execute`` capability.
        """
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        if not self._has_capability(record, PluginCapability.REMOTE_JOBS_EXECUTE):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.REMOTE_JOBS_EXECUTE.value}"
            )

        rd = manifest.remote_delegation
        if rd is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no remote_delegation configuration"
            )
        if rd.role not in ("executor", "bidirectional"):
            raise ValueError(
                f"Plugin {plugin_id!r} remote_delegation role is {rd.role!r}, "
                f"not 'executor' or 'bidirectional'"
            )

        valid_actions = ("claim", "heartbeat", "progress", "complete", "fail", "cancel")
        if action not in valid_actions:
            raise ValueError(
                f"Invalid executor action {action!r}; valid: {valid_actions}"
            )

        try:
            operation = self._operation_for_action(rd, action)
            if operation is None:
                raise ValueError(
                    f"Plugin {plugin_id!r} does not declare operations.{action}"
                )
            correlation_id = self._correlation_for_external_job(remote_job_id)
            existing = self._submissions.get(correlation_id) if correlation_id else None
            worker_id, worker_name = self._plugin_worker_identity(plugin_id)
            url, headers, payload_bytes, outbound_payload = (
                self._build_http_operation_request(
                    plugin_id,
                    rd,
                    operation,
                    payload=payload,
                    runtime_context={
                        "plugin_id": plugin_id,
                        "integration_id": rd.integration_id,
                        "correlation_id": correlation_id,
                        "external_job_id": remote_job_id or None,
                        "action": action,
                        "remote_response": (
                            existing.remote_response if existing else None
                        ),
                        "worker_id": worker_id,
                        "worker_name": worker_name,
                        "local_job_id": existing.local_job_id if existing else None,
                        "local_job_name": existing.local_job_name if existing else None,
                    },
                )
            )
            await self._validate_http_target(manifest, url)
            response = await self._http_request(
                operation.method,
                url,
                payload_bytes,
                headers,
            )
            status_code = response.status_code
            parsed_body = self._parse_response_body(response.body)
            response_body = (
                cast(dict[str, Any], parsed_body)
                if isinstance(parsed_body, dict)
                else None
            )
            message_id = None
            if existing is not None:
                existing.last_outbound_command = {
                    "action": action,
                    "external_job_id": remote_job_id,
                    "payload": outbound_payload,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                self._apply_response_mapping(
                    existing, response_body, operation.response_mapping
                )
                self._persist_submission_record(existing)

            self._record_outbound_action(
                plugin_id=plugin_id,
                external_job_id=remote_job_id,
                action=action,
                payload=outbound_payload,
            )
            return {
                "plugin_id": plugin_id,
                "action": action,
                "remote_job_id": remote_job_id,
                "http_status_code": status_code,
                "success": 200 <= status_code < 300,
                "response_body": response_body,
                "message_id": message_id,
            }
        except Exception as exc:
            return {
                "plugin_id": plugin_id,
                "action": action,
                "remote_job_id": remote_job_id,
                "http_status_code": None,
                "success": False,
                "error": str(exc),
            }

    # ── Status return path (polling + callback) ───────────────────────────

    async def poll_remote_job_status(
        self,
        plugin_id: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Poll the remote control plane for a job's current status.

        Uses the submission record to find the external job ID, then
        queries the configured HTTP poll endpoint.
        """
        self._refresh_runtime_state()
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        if not self._has_capability(record, PluginCapability.REMOTE_JOBS_SUBMIT):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.REMOTE_JOBS_SUBMIT.value}"
            )

        submission = self._submissions.get(correlation_id)
        if submission is None:
            raise KeyError(f"No submission found for correlation_id {correlation_id!r}")

        if not submission.external_job_id:
            return {
                "correlation_id": correlation_id,
                "status": submission.status,
                "remote_status": None,
                "error": "No external job ID recorded — cannot poll",
            }

        rd = manifest.remote_delegation
        if rd is None:
            raise ValueError("Plugin has no remote_delegation configuration")
        if rd.operations.poll is None:
            raise ValueError("Plugin has no operations.poll configuration")
        operation = rd.operations.poll
        poll_url, headers, payload_bytes, _outbound_payload = (
            self._build_http_operation_request(
                plugin_id,
                rd,
                operation,
                payload={},
                runtime_context={
                    "plugin_id": plugin_id,
                    "integration_id": rd.integration_id,
                    "correlation_id": correlation_id,
                    "external_job_id": submission.external_job_id,
                    "action": "poll",
                    "remote_response": submission.remote_response,
                    "local_job_id": submission.local_job_id,
                    "local_job_name": submission.local_job_name,
                },
            )
        )

        try:
            await self._validate_http_target(manifest, poll_url)
            response = await self._http_request(
                operation.method,
                poll_url,
                payload_bytes,
                headers,
            )
            status_code = response.status_code
            remote_data = self._parse_response_body(response.body)

            # Update submission record with remote status
            if 200 <= status_code < 300:
                extracted = self._apply_response_mapping(
                    submission,
                    (
                        cast(dict[str, Any], remote_data)
                        if isinstance(remote_data, dict)
                        else None
                    ),
                    operation.response_mapping,
                )
                remote_status = extracted["remote_status"]
                if remote_status:
                    self._update_submission_from_remote_status(
                        submission, str(remote_status)
                    )

                return {
                    "correlation_id": correlation_id,
                    "external_job_id": submission.external_job_id,
                    "status": submission.status,
                    "remote_status": remote_status,
                    "remote_data": remote_data,
                    "http_status_code": status_code,
                }
            else:
                return {
                    "correlation_id": correlation_id,
                    "external_job_id": submission.external_job_id,
                    "status": submission.status,
                    "remote_status": None,
                    "error": f"HTTP {status_code}",
                    "http_status_code": status_code,
                }
        except Exception as exc:
            return {
                "correlation_id": correlation_id,
                "external_job_id": submission.external_job_id,
                "status": submission.status,
                "remote_status": None,
                "error": str(exc),
            }

    def receive_remote_callback(
        self,
        plugin_id: str,
        handler_id: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Validate, route, and correlate an inbound remote callback.

        Checks the plugin has `remote.callbacks.receive` capability,
        validates the handler_id, correlates via external_job_id or
        correlation_id in the payload, and updates the submission record.
        """
        self._refresh_runtime_state()
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        if not self._has_capability(record, PluginCapability.REMOTE_CALLBACKS_RECEIVE):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.REMOTE_CALLBACKS_RECEIVE.value}"
            )

        # Find the declared callback handler
        handler = next(
            (cb for cb in manifest.callbacks if cb.handler_id == handler_id),
            None,
        )
        if handler is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no callback handler {handler_id!r}"
            )

        rd = manifest.remote_delegation
        mapping = None
        if rd is not None:
            mapping = next(
                (
                    item
                    for item in rd.callbacks.inbound
                    if item.handler_id == handler_id
                ),
                None,
            )
            if mapping is None:
                raise ValueError(
                    f"Plugin {plugin_id!r} has no inbound callback mapping for {handler_id!r}"
                )

        normalized_headers = {k.lower(): v for k, v in (headers or {}).items()}
        extracted: dict[str, Any] = {
            "lifecycle_event": None,
            "correlation_id": None,
            "external_job_id": None,
            "remote_status": None,
            "progress": None,
            "result": None,
            "error": None,
        }
        if mapping is not None:
            callback_context = {
                "callback": {
                    "body": payload,
                    "headers": normalized_headers,
                },
                "runtime": {
                    "plugin_id": plugin_id,
                    "handler_id": handler_id,
                },
                "secret": (
                    self._secret_context(plugin_id, rd) if rd is not None else {}
                ),
            }
            extracted = {
                "lifecycle_event": evaluate_mapping_node(
                    mapping.extract.lifecycle_event,
                    callback_context,
                ),
                "correlation_id": (
                    evaluate_mapping_node(
                        mapping.extract.correlation_id, callback_context
                    )
                    if mapping.extract.correlation_id is not None
                    else None
                ),
                "external_job_id": (
                    evaluate_mapping_node(
                        mapping.extract.external_job_id, callback_context
                    )
                    if mapping.extract.external_job_id is not None
                    else None
                ),
                "remote_status": (
                    evaluate_mapping_node(
                        mapping.extract.remote_status, callback_context
                    )
                    if mapping.extract.remote_status is not None
                    else None
                ),
                "progress": (
                    evaluate_mapping_node(mapping.extract.progress, callback_context)
                    if mapping.extract.progress is not None
                    else None
                ),
                "result": (
                    evaluate_mapping_node(mapping.extract.result, callback_context)
                    if mapping.extract.result is not None
                    else None
                ),
                "error": (
                    evaluate_mapping_node(mapping.extract.error, callback_context)
                    if mapping.extract.error is not None
                    else None
                ),
            }

        lifecycle_event = (
            str(extracted["lifecycle_event"])
            if extracted["lifecycle_event"] not in (None, "")
            else None
        )
        if lifecycle_event is None:
            raise ValueError(
                f"Inbound callback mapping for {handler_id!r} did not extract a lifecycle_event"
            )

        # Correlate callback with stored submission record
        correlated_submission: RemoteSubmissionRecord | None = None
        correlation_id = extracted["correlation_id"]
        external_job_id = extracted["external_job_id"]

        if correlation_id and correlation_id in self._submissions:
            correlated_submission = self._submissions[str(correlation_id)]
        elif external_job_id:
            ext_id = str(external_job_id)
            corr_id = self._external_id_index.get(ext_id)
            if corr_id:
                correlated_submission = self._submissions.get(corr_id)

        # Update submission status based on lifecycle event
        if correlated_submission and lifecycle_event:
            if correlated_submission.external_job_id is None and external_job_id:
                correlated_submission.external_job_id = str(external_job_id)
            normalized_event = {
                "handler_id": handler_id,
                "lifecycle_event": lifecycle_event,
                "correlation_id": correlation_id,
                "external_job_id": external_job_id,
                "remote_status": extracted["remote_status"],
                "progress": extracted["progress"],
                "result": extracted["result"],
                "error": extracted["error"],
                "payload": payload,
            }
            self._update_submission_from_lifecycle_event(
                correlated_submission, lifecycle_event, normalized_event
            )
            if extracted["error"] not in (None, ""):
                correlated_submission.last_error = str(extracted["error"])
                self._persist_submission_record(correlated_submission)

        return {
            "plugin_id": plugin_id,
            "handler_id": handler_id,
            "lifecycle_event": lifecycle_event,
            "payload": payload,
            "accepted": True,
            "correlated": correlated_submission is not None,
            "correlation_id": (
                correlated_submission.correlation_id if correlated_submission else None
            ),
        }

    def get_remote_delegation_info(self, plugin_id: str) -> dict[str, Any] | None:
        """Return the remote delegation configuration for a plugin."""
        record = self._require_known(plugin_id)
        manifest = record.manifest
        if manifest is None or manifest.remote_delegation is None:
            return None
        return manifest.remote_delegation.model_dump(
            mode="json", by_alias=True, exclude_none=True
        )

    # ── Remote submission observability ────────────────────────────────────

    def list_remote_submissions(
        self,
        plugin_id: str | None = None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return recent remote submission records for operator visibility."""
        self._refresh_runtime_state()
        items = list(self._submissions.values())
        if plugin_id is not None:
            items = [s for s in items if s.plugin_id == plugin_id]
        items.sort(key=lambda s: s.created_at, reverse=True)
        return [s.to_dict() for s in items[:limit]]

    def get_remote_submission(self, correlation_id: str) -> dict[str, Any] | None:
        """Get a single submission record by correlation ID."""
        self._refresh_runtime_state()
        rec = self._submissions.get(correlation_id)
        return rec.to_dict() if rec else None

    # ── Event dispatch ────────────────────────────────────────────────────

    async def dispatch_event(
        self,
        *,
        event_family: str,
        event_type: str,
        body: dict[str, Any],
        job_id: str | None = None,
        worker_id: str | None = None,
        event_id: str | None = None,
    ) -> list[DeliveryAttempt]:
        if self._durable_delivery:
            await self._durable_delivery.enqueue({
                "event_family": event_family, "event_type": event_type, "body": body,
                "job_id": job_id, "worker_id": worker_id, "event_id": event_id,
            })
            return []
        return await self.delivery.dispatch_event(
            event_family=event_family,
            event_type=event_type,
            body=body,
            job_id=job_id,
            worker_id=worker_id,
            registry=self.registry,
            secret_manager=self.secrets,
        )

    # ── Secret operations (operator-facing) ───────────────────────────────

    def create_secret(
        self,
        plugin_id: str,
        scope_kind: str,
        scope_name: str,
    ) -> str:
        self._require_known(plugin_id)
        return self.secrets.create_secret(plugin_id, scope_kind, scope_name)

    def rotate_secret(
        self,
        plugin_id: str,
        scope_kind: str,
        scope_name: str,
    ) -> str:
        self._require_known(plugin_id)
        return self.secrets.rotate_secret(plugin_id, scope_kind, scope_name)

    def set_secret(
        self,
        plugin_id: str,
        scope_kind: str,
        scope_name: str,
        raw_secret: str,
    ) -> None:
        self._require_known(plugin_id)
        self.secrets.set_secret(plugin_id, scope_kind, scope_name, raw_secret)

    def list_secrets(self, plugin_id: str) -> list[dict[str, Any]]:
        return [s.to_dict() for s in self.secrets.list_for_plugin(plugin_id)]

    # ── State operations ──────────────────────────────────────────────────

    def get_state(self, plugin_id: str, key: str) -> Any | None:
        self._require_known(plugin_id)
        return self.state.get(plugin_id, key)

    def set_state(
        self,
        plugin_id: str,
        key: str,
        value: Any,
        *,
        actor: str = "plugin",
        reason: str = "",
    ) -> None:
        self._require_known(plugin_id)
        self.state.set(plugin_id, key, value, actor=actor, reason=reason)

    def delete_state(
        self,
        plugin_id: str,
        key: str,
        *,
        actor: str = "plugin",
        reason: str = "",
    ) -> bool:
        self._require_known(plugin_id)
        return self.state.delete(plugin_id, key, actor=actor, reason=reason)

    def list_state(self, plugin_id: str) -> list[dict[str, Any]]:
        return self.state.list_entries(plugin_id)

    def get_state_audit(
        self, plugin_id: str, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        return self.state.get_audit_log(plugin_id, limit=limit)

    # ── Delivery observability ────────────────────────────────────────────

    def delivery_statuses(self, plugin_id: str | None = None) -> list[dict[str, Any]]:
        return self.delivery.list_endpoint_statuses(plugin_id)

    def recent_deliveries(
        self, plugin_id: str | None = None, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        return self.delivery.recent_deliveries(plugin_id, limit=limit)

    # ── Runtime/operator observability ───────────────────────────────────

    def list_runtime_correlations(
        self,
        plugin_id: str | None = None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        items = self.runtime_store.list_correlations(plugin_id)
        return [item.to_dict() for item in items[:limit]]

    def get_runtime_correlation(
        self,
        correlation_id: str,
    ) -> dict[str, Any] | None:
        record = self.runtime_store.get_correlation(correlation_id)
        return record.to_dict() if record is not None else None

    def get_supervisor_health(
        self,
        plugin_id: str,
    ) -> dict[str, Any]:
        self._require_known(plugin_id)
        session = self.runtime_store.get_session(plugin_id)
        registration = self.runtime_store.get_registration(plugin_id)
        owner = self._runtime_owner_status()
        return {
            "plugin_id": plugin_id,
            "runtime_started": self._runtime_available(),
            "runtime_owned_here": self._runtime_started,
            "runtime_owner": owner,
            "supervisor_count": len(self._supervisor_tasks.get(plugin_id, [])),
            "websocket_connected": bool(session.connected) if session else False,
            "websocket_backlog": (
                len([item for item in session.backlog if not item.acknowledged])
                if session
                else 0
            ),
            "callback_registered": (
                bool(registration.registered) if registration else False
            ),
        }

    def get_remote_delegation_overview(self) -> dict[str, Any]:
        """Aggregate status across all remote delegation activity."""
        self._sync_persisted_configuration()
        self._refresh_runtime_state(preserve_unpersisted=True)
        total = 0
        active = 0
        terminal = 0
        errored = 0
        per_plugin: dict[str, dict[str, int]] = {}

        for rec in self._submissions.values():
            total += 1
            pid = rec.plugin_id
            if pid not in per_plugin:
                per_plugin[pid] = {"active": 0, "completed": 0, "failed": 0, "total": 0}
            per_plugin[pid]["total"] += 1

            if rec.status in _TERMINAL_SUBMISSION_STATES:
                terminal += 1
                if rec.status == "failed":
                    per_plugin[pid]["failed"] += 1
                else:
                    per_plugin[pid]["completed"] += 1
            else:
                active += 1
                per_plugin[pid]["active"] += 1

            if rec.last_error:
                errored += 1

        if self._runtime_started:
            supervised_plugins = list(self._supervisor_tasks.keys())
        elif self._runtime_available():
            supervised_plugins = [
                record.plugin_id
                for record in self.registry.list_enabled()
                if record.manifest.remote_delegation is not None
            ]
        else:
            supervised_plugins = []

        return {
            "runtime_started": self._runtime_available(),
            "runtime_owned_here": self._runtime_started,
            "runtime_owner": self._runtime_owner_status(),
            "supervised_plugins": supervised_plugins,
            "submissions": {
                "total": total,
                "active": active,
                "terminal": terminal,
                "errored": errored,
            },
            "per_plugin": per_plugin,
        }

    def get_websocket_session(self, plugin_id: str) -> dict[str, Any] | None:
        self._require_known(plugin_id)
        session = self.runtime_store.get_session(plugin_id)
        return session.to_dict() if session is not None else None

    def get_callback_registration_status(self, plugin_id: str) -> dict[str, Any] | None:
        self._require_known(plugin_id)
        registration = self.runtime_store.get_registration(plugin_id)
        return registration.to_dict() if registration is not None else None

    async def re_register_callbacks(self, plugin_id: str) -> dict[str, Any]:
        self._require_known(plugin_id)
        record = await self._ensure_callback_registration(plugin_id, force=True)
        return (
            record.to_dict()
            if record is not None
            else {
                "plugin_id": plugin_id,
                "registered": False,
            }
        )

    async def resync_runtime_correlation(
        self,
        plugin_id: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        self._require_known(plugin_id)
        return await self.poll_remote_job_status(plugin_id, correlation_id)

    async def replay_websocket_backlog(self, plugin_id: str) -> dict[str, Any]:
        record = self._require_known(plugin_id)
        manifest = record.manifest
        if manifest is None or manifest.remote_delegation is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no remote_delegation configuration"
            )
        rd = manifest.remote_delegation
        if rd.websocket is None:
            raise ValueError(f"Plugin {plugin_id!r} has no websocket configuration")
        flushed = await self._flush_websocket_backlog(plugin_id, rd)
        session = self.runtime_store.get_session(plugin_id)
        return {
            "plugin_id": plugin_id,
            "flushed": flushed,
            "remaining_backlog": (
                len([item for item in session.backlog if not item.acknowledged])
                if session is not None
                else 0
            ),
        }

    # ── Internal helpers ──────────────────────────────────────────────────

    def _persist_submission_record(self, rec: RemoteSubmissionRecord) -> None:
        rec.touch()
        self._submissions[rec.correlation_id] = rec
        if rec.external_job_id:
            self._external_id_index[rec.external_job_id] = rec.correlation_id
        if rec.local_job_id:
            self._local_job_index[rec.local_job_id] = rec.correlation_id
        self.runtime_store.upsert_correlation(rec.to_runtime_record())
        try:
            self.state.set(
                rec.plugin_id,
                f"_remote:submission:{rec.correlation_id}",
                rec.to_dict(),
                actor="platform",
                reason="remote_submission_tracking",
            )
        except ValueError:
            logger.warning(
                "Could not mirror submission %s into plugin state store: quota exceeded",
                rec.correlation_id,
            )

    @staticmethod
    def _operation_for_action(rd: Any, action: str) -> Any | None:
        return getattr(rd.operations, action, None)

    def _plugin_worker_identity(self, plugin_id: str) -> tuple[str, str]:
        instance_id = str(getattr(self._backend, "instance_id", "plugin-runtime"))
        worker_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{plugin_id}:{instance_id}"))
        worker_name = f"{plugin_id}@{instance_id}"[:120]
        return worker_id, worker_name

    @staticmethod
    def _normalize_int(value: Any, *, field_name: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be an integer, got {value!r}") from exc

    def _export_auth_bundle(
        self,
        plugin_id: str,
        manifest: PluginManifest,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        rd = manifest.remote_delegation
        if rd is None or rd.auth_export is None:
            return {}
        record = self._require_enabled(plugin_id)
        if not self._has_capability(record, PluginCapability.AUTH_CREDENTIALS_EXPORT):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.AUTH_CREDENTIALS_EXPORT.value}"
            )

        export_cfg = rd.auth_export
        context = {"payload": payload, "runtime": {}}
        site_value = resolve_path(context, export_cfg.site_path)
        auth_id_value = resolve_path(context, export_cfg.auth_id_path)
        site = str(site_value or "").strip().lower()
        if not site:
            raise ValueError("Delegated auth export requires a non-empty site")
        auth_id = self._normalize_int(auth_id_value, field_name="auth_id")

        from ultima_scraper_backend.api.dependencies import get_backend_manager

        backend_manager = get_backend_manager()
        account = backend_manager._find_account(site, auth_id)
        if account is None:
            raise ValueError(
                f"Delegated auth export could not find auth_id={auth_id} for site {site!r}"
            )
        return backend_manager._build_auth_payload(account, site)

    async def _import_auth_bundle(
        self,
        plugin_id: str,
        manifest: PluginManifest,
        remote_message: dict[str, Any],
    ) -> dict[str, Any]:
        rd = manifest.remote_delegation
        if rd is None or rd.auth_import is None:
            return {}
        record = self._require_enabled(plugin_id)
        if not self._has_capability(record, PluginCapability.AUTH_CREDENTIALS_IMPORT):
            raise PermissionError(
                f"Plugin {plugin_id!r} does not have "
                f"{PluginCapability.AUTH_CREDENTIALS_IMPORT.value}"
            )

        import_cfg = rd.auth_import
        bundle_value = resolve_path(remote_message, import_cfg.bundle_path)
        auth_bundle = (
            cast(dict[str, Any], bundle_value)
            if isinstance(bundle_value, dict)
            else None
        )
        if not auth_bundle:
            raise ValueError("Claimed remote work did not include a valid auth bundle")

        site_value = resolve_path(remote_message, import_cfg.site_path)
        site = str(site_value or "").strip().lower()
        if not site:
            raise ValueError("Claimed remote work did not include a valid site")

        username_value = (
            resolve_path(remote_message, import_cfg.username_path)
            if import_cfg.username_path
            else auth_bundle.get("username")
        )
        user_id_value = (
            resolve_path(remote_message, import_cfg.user_id_path)
            if import_cfg.user_id_path
            else auth_bundle.get("id")
        )
        user_id = self._normalize_int(user_id_value, field_name="auth_bundle.id")
        username = (
            str(username_value).strip() if username_value not in (None, "") else None
        )

        from ultima_scraper_backend.api.dependencies import get_backend_manager

        backend_manager = get_backend_manager()
        account = await backend_manager._store_account(
            site,
            username,
            user_id,
            dict(auth_bundle),
        )
        return {import_cfg.runtime_key: account.id}

    def _resolve_remote_auth_config(
        self,
        rd: Any,
        auth_scope: str | None = None,
    ) -> Any:
        if auth_scope and auth_scope in rd.auth_scopes:
            return rd.auth_scopes[auth_scope]
        return rd.auth

    def _resolve_remote_auth_secret(
        self,
        plugin_id: str,
        rd: Any,
        auth_scope: str | None = None,
    ) -> str:
        if auth_scope:
            return (
                self.secrets.get_raw_secret(plugin_id, "remote_auth", auth_scope) or ""
            )
        if rd.auth_scopes:
            for scope_name in rd.auth_scopes.keys():
                scoped_secret = self.secrets.get_raw_secret(
                    plugin_id,
                    "remote_auth",
                    scope_name,
                )
                if scoped_secret:
                    return scoped_secret
        return self.secrets.get_raw_secret(plugin_id, "remote_auth", "default") or ""

    def _sign_remote_auth_payload(
        self,
        plugin_id: str,
        rd: Any,
        payload: bytes,
        auth_scope: str | None = None,
    ) -> str | None:
        scope_name = auth_scope
        if scope_name is None:
            if rd.auth_scopes:
                scope_name = next(iter(rd.auth_scopes.keys()), None)
            else:
                scope_name = "default"
        if scope_name is None:
            return None
        return self.secrets.sign_payload(
            plugin_id,
            "remote_auth",
            scope_name,
            payload,
        )

    def _secret_context(
        self,
        plugin_id: str,
        rd: Any,
        *,
        preferred_scope: str | None = None,
    ) -> dict[str, Any]:
        default_secret = (
            self.secrets.get_raw_secret(plugin_id, "remote_auth", "default") or ""
        )
        remote_auth_scopes: dict[str, str] = {}
        for scope_name in rd.auth_scopes.keys():
            remote_auth_scopes[scope_name] = (
                self.secrets.get_raw_secret(plugin_id, "remote_auth", scope_name) or ""
            )

        secret_value = (
            remote_auth_scopes.get(preferred_scope or "")
            or self._resolve_remote_auth_secret(plugin_id, rd, preferred_scope)
            or default_secret
        )

        context: dict[str, Any] = {
            "value": secret_value,
            "remote_auth": remote_auth_scopes,
        }
        if default_secret:
            context["default"] = default_secret
        for scope_name, scope_secret in remote_auth_scopes.items():
            context[scope_name] = scope_secret
        return context

    @staticmethod
    def _stringify_mapping_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"))
        return str(value)

    def _append_query_params(self, url: str, query_params: dict[str, Any]) -> str:
        if not query_params:
            return url
        parts = urlsplit(url)
        existing = list(parse_qsl(parts.query, keep_blank_values=True))
        for key, value in query_params.items():
            if value is None:
                continue
            if isinstance(value, list):
                for item in value:
                    existing.append((key, self._stringify_mapping_value(item)))
            else:
                existing.append((key, self._stringify_mapping_value(value)))
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(existing, doseq=True),
                parts.fragment,
            )
        )

    def _build_http_operation_request(
        self,
        plugin_id: str,
        rd: Any,
        operation: Any,
        *,
        payload: dict[str, Any],
        runtime_context: dict[str, Any],
    ) -> tuple[str, dict[str, str], bytes | None, Any]:
        context = {
            "payload": payload,
            "runtime": runtime_context,
            "secret": self._secret_context(
                plugin_id,
                rd,
                preferred_scope=operation.auth_scope,
            ),
        }
        path_params_raw = evaluate_mapping_node(
            operation.request_mapping.path_params,
            context,
        )
        path_params = (
            cast(dict[str, Any], path_params_raw)
            if isinstance(path_params_raw, dict)
            else {}
        )
        query_raw = evaluate_mapping_node(operation.request_mapping.query, context)
        query_params = (
            cast(dict[str, Any], query_raw) if isinstance(query_raw, dict) else {}
        )
        body_mapping = operation.request_mapping.body
        body_payload = (
            evaluate_mapping_node(body_mapping, context) if body_mapping else None
        )
        payload_bytes = (
            json.dumps(body_payload, separators=(",", ":")).encode()
            if body_payload is not None
            else None
        )
        headers = self._build_remote_headers(
            plugin_id,
            rd,
            payload_bytes,
            content_type=payload_bytes is not None,
            auth_scope=operation.auth_scope,
        )
        mapped_headers_raw = evaluate_mapping_node(
            operation.request_mapping.headers,
            context,
        )
        if isinstance(mapped_headers_raw, dict):
            for header_name, header_value in mapped_headers_raw.items():
                if header_value is None:
                    continue
                headers[str(header_name)] = self._stringify_mapping_value(header_value)
        url = self._resolve_remote_endpoint(
            self.configured_url(self.registry.get(plugin_id).manifest, rd.external_system_url, rd.external_system_url_setting).rstrip("/") if rd.external_system_url else "",
            operation.endpoint,
            path_params,
        )
        return (
            self._append_query_params(url, query_params),
            headers,
            payload_bytes,
            body_payload,
        )

    def _extract_response_mapping(
        self,
        response_body: dict[str, Any] | None,
        response_mapping: Any,
    ) -> dict[str, Any]:
        if response_body is None:
            return {
                "external_job_id": None,
                "remote_status": None,
                "error": None,
            }
        error_value = None
        if response_mapping.error_path:
            error_value = resolve_path(response_body, response_mapping.error_path)
        return {
            "external_job_id": resolve_path(
                response_body,
                response_mapping.external_job_id_path,
            ),
            "remote_status": resolve_path(
                response_body,
                response_mapping.remote_status_path,
            ),
            "error": error_value,
        }

    def _apply_response_mapping(
        self,
        rec: RemoteSubmissionRecord,
        response_body: dict[str, Any] | None,
        response_mapping: Any,
    ) -> dict[str, Any]:
        extracted = self._extract_response_mapping(response_body, response_mapping)
        if response_body is not None:
            rec.remote_response = response_body
        external_job_id = extracted["external_job_id"]
        if external_job_id not in (None, ""):
            rec.external_job_id = str(external_job_id)
            self._external_id_index[rec.external_job_id] = rec.correlation_id
        remote_status = extracted["remote_status"]
        if remote_status not in (None, ""):
            rec.remote_status = str(remote_status)
        error_value = extracted["error"]
        if error_value not in (None, ""):
            rec.last_error = str(error_value)
        return extracted

    @staticmethod
    def _literal_mapping_value(mapping_value: Any) -> Any:
        if isinstance(mapping_value, dict) and set(mapping_value.keys()) == {"literal"}:
            return mapping_value["literal"]
        return mapping_value if not isinstance(mapping_value, dict) else None

    def _resolve_remote_endpoint(
        self,
        base_url: str,
        endpoint: str,
        variables: dict[str, Any],
    ) -> str:
        rendered = endpoint or ""
        for key, value in variables.items():
            rendered = rendered.replace(
                f"{{{key}}}", "" if value is None else str(value)
            )
        if "{{" in rendered and "}}" in rendered:
            rendered = render_template(rendered, variables)
        if rendered.startswith(("https://", "http://", "ws://", "wss://")):
            return rendered
        if not base_url:
            return rendered
        return (
            f"{base_url.rstrip('/')}/{rendered.lstrip('/')}"
            if rendered
            else base_url.rstrip("/")
        )

    @staticmethod
    def _parse_response_body(body: bytes) -> Any:
        if not body:
            return None
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return body
        text = text.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    def _build_remote_headers(
        self,
        plugin_id: str,
        rd: Any,
        payload: bytes | None = None,
        *,
        content_type: bool = True,
        auth_scope: str | None = None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = "application/json"

        auth_config = self._resolve_remote_auth_config(rd, auth_scope)
        secret = self._resolve_remote_auth_secret(plugin_id, rd, auth_scope)
        if not secret:
            return headers

        if auth_config.method == "bearer":
            headers[auth_config.header_name] = f"Bearer {secret}"
        elif auth_config.method == "hmac":
            signature = self._sign_remote_auth_payload(
                plugin_id,
                rd,
                payload or b"",
                auth_scope=auth_scope,
            )
            if signature:
                headers[auth_config.header_name] = f"sha256={signature}"
        else:
            headers[auth_config.header_name] = secret
        return headers

    def _build_websocket_headers(
        self,
        plugin_id: str,
        rd: Any,
    ) -> dict[str, str]:
        websocket_cfg = rd.websocket
        if websocket_cfg is None:
            return {}
        secret = self._resolve_remote_auth_secret(plugin_id, rd)
        if not secret:
            return {}
        auth_config = websocket_cfg.auth
        if auth_config.method == "bearer":
            return {auth_config.header_name: f"Bearer {secret}"}
        if auth_config.method == "hmac":
            signature = self._sign_remote_auth_payload(plugin_id, rd, b"")
            return {auth_config.header_name: f"sha256={signature}"} if signature else {}
        return {auth_config.header_name: secret}

    def _correlation_for_external_job(self, external_job_id: str) -> str | None:
        if not external_job_id:
            return None
        return self._external_id_index.get(external_job_id)

    def _record_outbound_action(
        self,
        *,
        plugin_id: str,
        external_job_id: str,
        action: str,
        payload: dict[str, Any],
    ) -> None:
        correlation_id = self._correlation_for_external_job(external_job_id)
        if correlation_id is None:
            return
        rec = self._submissions.get(correlation_id)
        if rec is None or rec.plugin_id != plugin_id:
            return
        rec.last_outbound_command = {
            "action": action,
            "external_job_id": external_job_id,
            "payload": payload,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        self._persist_submission_record(rec)

    def _callback_url(self, plugin_id: str, handler_id: str) -> str:
        return (
            f"{self._public_base_url}/plugins/{plugin_id}"
            f"/remote-delegation/callbacks/{handler_id}"
        )

    def _build_callback_registration_payload(
        self,
        plugin_id: str,
        rd: Any,
        *,
        auth_scope: str | None = None,
    ) -> dict[str, Any]:
        registration = rd.callback_registration
        if registration is None:
            return {}

        callback_entries: list[dict[str, Any]] = []
        callback_urls: dict[str, str] = {}
        callback_secrets: dict[str, str] = {}
        for mapping in rd.callbacks.inbound:
            lifecycle_event = self._literal_mapping_value(
                mapping.extract.lifecycle_event
            )
            if lifecycle_event in (None, ""):
                lifecycle_event = ""
            url = self._callback_url(plugin_id, mapping.handler_id)
            secret = (
                self.secrets.get_raw_secret(plugin_id, "callback", mapping.handler_id)
                or ""
            )
            callback_entries.append(
                {
                    "lifecycle_event": lifecycle_event,
                    "handler_id": mapping.handler_id,
                    "url": url,
                    "secret": secret,
                }
            )
            if lifecycle_event:
                callback_urls[str(lifecycle_event)] = url
            callback_secrets[mapping.handler_id] = secret

        subscribed_events = registration.lifecycle_events or [
            item["lifecycle_event"]
            for item in callback_entries
            if item["lifecycle_event"]
        ]
        context: dict[str, Any] = {
            "plugin_id": plugin_id,
            "lifecycle_events": subscribed_events,
            "callbacks": callback_entries,
            "callback_urls": callback_urls,
            "callback_secrets": callback_secrets,
            "callback_url": callback_entries[0]["url"] if callback_entries else "",
            "callback_secret": (
                callback_entries[0]["secret"] if callback_entries else ""
            ),
            "secret": self._secret_context(
                plugin_id,
                rd,
                preferred_scope=auth_scope,
            ),
        }
        if registration.payload_mapping:
            return apply_field_mappings(registration.payload_mapping, context)
        return context

    async def _ensure_callback_registration(
        self,
        plugin_id: str,
        *,
        force: bool = False,
    ) -> CallbackRegistrationRecord | None:
        record = self.registry.get(plugin_id)
        manifest = record.manifest if record is not None else None
        rd = manifest.remote_delegation if manifest is not None else None
        if manifest is None or rd is None or rd.callback_registration is None:
            return None

        existing = self.runtime_store.get_registration(plugin_id)
        if existing is None:
            existing = CallbackRegistrationRecord(
                plugin_id=plugin_id,
                endpoint="",
                lifecycle_events=[],
            )
        cfg = rd.callback_registration
        endpoint = self._resolve_remote_endpoint(
            self.configured_url(manifest, rd.external_system_url, rd.external_system_url_setting).rstrip("/") if rd.external_system_url else "",
            cfg.endpoint,
            {},
        )
        if existing.registered and existing.endpoint == endpoint and not force:
            return existing
        payload_dict = self._build_callback_registration_payload(
            plugin_id,
            rd,
            auth_scope=cfg.auth_scope,
        )
        payload_bytes = json.dumps(payload_dict, separators=(",", ":")).encode()

        existing.endpoint = endpoint
        existing.lifecycle_events = cfg.lifecycle_events or list(
            callback["lifecycle_event"]
            for callback in payload_dict.get("callbacks", [])
        )
        existing.last_attempt_at = datetime.now(UTC).isoformat()
        existing.last_error = None

        try:
            headers = self._build_remote_headers(
                plugin_id,
                rd,
                payload_bytes,
                content_type=True,
                auth_scope=cfg.auth_scope,
            )
            await self._validate_http_target(manifest, endpoint)
            response = await self._http_request(
                cfg.method,
                endpoint,
                payload_bytes,
                headers,
            )
            response_body = self._parse_response_body(response.body)
            success = 200 <= response.status_code < 300
            if not success:
                existing.last_error = f"HTTP {response.status_code}"

            if success:
                existing.registered = True
                existing.last_success_at = datetime.now(UTC).isoformat()
                existing.last_error = None
            else:
                existing.registered = False
                if existing.last_error is None:
                    existing.last_error = str(response_body)
        except Exception as exc:
            existing.registered = False
            existing.last_error = str(exc)

        self.runtime_store.save_registration(existing)
        return existing

    async def _reconcile_plugin_submissions(self, plugin_id: str) -> None:
        record = self.registry.get(plugin_id)
        manifest = record.manifest if record is not None else None
        rd = manifest.remote_delegation if manifest is not None else None
        if manifest is None or rd is None:
            return

        if record is None or not self._has_capability(
            record, PluginCapability.REMOTE_JOBS_SUBMIT
        ):
            return
        if rd.operations.poll is None:
            return

        for correlation in self.runtime_store.list_correlations(plugin_id):
            if correlation.status in _TERMINAL_SUBMISSION_STATES:
                continue
            if not correlation.external_job_id:
                continue
            try:
                await self.poll_remote_job_status(plugin_id, correlation.correlation_id)
            except Exception as exc:
                logger.warning(
                    "Reconcile poll failed for plugin=%s correlation=%s (%s)",
                    plugin_id,
                    correlation.correlation_id,
                    _exception_summary(exc),
                )
                logger.debug(
                    "Reconcile poll failure detail for plugin=%s correlation=%s",
                    plugin_id,
                    correlation.correlation_id,
                    exc_info=True,
                )

        await self._detect_stale_submissions(plugin_id)

    async def _detect_stale_submissions(
        self,
        plugin_id: str,
        *,
        stale_threshold_seconds: float = 3600,
    ) -> list[str]:
        """Detect submissions stuck in non-terminal states past the threshold.

        Returns a list of correlation IDs that were marked as stale.
        """
        now = datetime.now(UTC)
        stale_ids: list[str] = []

        for rec in list(self._submissions.values()):
            if rec.plugin_id != plugin_id:
                continue
            if rec.status in _TERMINAL_SUBMISSION_STATES:
                continue
            try:
                updated = datetime.fromisoformat(rec.updated_at)
            except (ValueError, TypeError):
                continue
            elapsed = (now - updated).total_seconds()
            if elapsed < stale_threshold_seconds:
                continue

            stale_ids.append(rec.correlation_id)
            rec.status = "failed"
            rec.last_error = (
                f"Stale submission detected: no progress for "
                f"{elapsed:.0f}s (threshold={stale_threshold_seconds:.0f}s)"
            )
            self._persist_submission_record(rec)
            logger.warning(
                "Stale submission plugin=%s correlation=%s external_job=%s "
                "elapsed=%.0fs — marked failed",
                plugin_id,
                rec.correlation_id,
                rec.external_job_id,
                elapsed,
            )

            if rec.external_job_id:
                try:
                    await self.execute_remote_worker_action(
                        plugin_id,
                        "fail",
                        rec.external_job_id,
                        {"failure_message": rec.last_error},
                    )
                except Exception:
                    logger.warning(
                        "Could not report stale failure to remote for correlation=%s",
                        rec.correlation_id,
                        exc_info=True,
                    )

        return stale_ids

    def _iter_remote_work_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            direct_job = payload.get("job")
            if isinstance(direct_job, dict):
                return [direct_job]
            if payload.get("claimed") is False and payload.get("job") is None:
                return []
            for key in ("jobs", "items", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
            return [payload]
        return []

    async def _claim_remote_work(self, plugin_id: str) -> bool:
        record = self.registry.get(plugin_id)
        manifest = record.manifest if record is not None else None
        rd = manifest.remote_delegation if manifest is not None else None
        if manifest is None or rd is None:
            return False
        if rd.role not in ("executor", "bidirectional"):
            return False
        if rd.executor_mapping is None:
            return False
        if rd.operations.claim is None:
            return False
        worker_id, worker_name = self._plugin_worker_identity(plugin_id)
        request_payload = {
            "worker_id": worker_id,
            "worker_name": worker_name,
            "capability_tags": [plugin_id],
            "heartbeat_timeout_seconds": 60,
            "plugin_id": plugin_id,
        }
        url, headers, payload_bytes, _outbound_payload = (
            self._build_http_operation_request(
                plugin_id,
                rd,
                rd.operations.claim,
                payload=request_payload,
                runtime_context={
                    "plugin_id": plugin_id,
                    "integration_id": rd.integration_id,
                    "correlation_id": None,
                    "external_job_id": None,
                    "action": "claim",
                    "worker_id": worker_id,
                    "worker_name": worker_name,
                },
            )
        )
        await self._validate_http_target(manifest, url)
        response = await self._http_request(
            rd.operations.claim.method,
            url,
            payload_bytes,
            headers,
        )
        if response.status_code == 204:
            return False
        if not (200 <= response.status_code < 300):
            logger.warning(
                "Remote claim failed for %s with HTTP %s",
                plugin_id,
                response.status_code,
            )
            return False

        response_body = self._parse_response_body(response.body)
        claimed_any = False
        for work_item in self._iter_remote_work_items(response_body):
            try:
                await self._bridge_remote_work_item(
                    plugin_id,
                    work_item,
                    response_mapping=rd.operations.claim.response_mapping,
                )
                claimed_any = True
            except Exception:
                logger.exception(
                    "Failed to process claimed remote work for %s", plugin_id
                )
        return claimed_any

    async def _bridge_remote_work_item(
        self,
        plugin_id: str,
        remote_message: dict[str, Any],
        *,
        external_job_id: str | None = None,
        correlation_id: str | None = None,
        response_mapping: Any | None = None,
    ) -> dict[str, Any]:
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        rd = manifest.remote_delegation
        if rd is None or rd.executor_mapping is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no executor_mapping for remote execution"
            )
        if self._backend is None or getattr(self._backend, "redis", None) is None:
            raise RuntimeError(
                "Native execution bridge requires an attached backend with Redis"
            )

        if external_job_id is None:
            mapping_to_use = response_mapping or (
                rd.operations.claim.response_mapping
                if rd.operations.claim is not None
                else None
            )
            extracted_external = (
                resolve_path(
                    remote_message,
                    mapping_to_use.external_job_id_path,
                )
                if mapping_to_use is not None
                else None
            )
            if extracted_external is not None:
                external_job_id = str(extracted_external)
            else:
                fallback_external = remote_message.get(
                    "external_job_id"
                ) or remote_message.get("job_id")
                if fallback_external is not None:
                    external_job_id = str(fallback_external)

        if correlation_id is None:
            correlation_candidate = resolve_path(
                remote_message,
                rd.correlation.local_id_field,
            )
            if correlation_candidate is None:
                correlation_candidate = remote_message.get("correlation_id")
            if correlation_candidate is not None:
                correlation_id = str(correlation_candidate)
        if correlation_id is None:
            correlation_id = _next_correlation_id()

        existing: RemoteSubmissionRecord | None = self._submissions.get(correlation_id)
        if existing is None and external_job_id:
            existing_corr = self._correlation_for_external_job(external_job_id)
            if existing_corr:
                existing = self._submissions.get(existing_corr)

        if existing is not None and existing.local_job_id:
            return existing.to_dict()

        payload_hash = hashlib.sha256(
            json.dumps(remote_message, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:16]
        correlation = existing or RemoteSubmissionRecord(
            correlation_id=correlation_id,
            plugin_id=plugin_id,
            integration_id=rd.integration_id,
            payload_hash=payload_hash,
        )
        correlation.external_job_id = external_job_id
        correlation.remote_response = remote_message
        correlation.last_inbound_event = remote_message
        self._persist_submission_record(correlation)

        try:
            bridge_runtime_context = await self._import_auth_bundle(
                plugin_id,
                manifest,
                remote_message,
            )
            native_job, tasks = build_native_job_from_remote(
                plugin_id=plugin_id,
                correlation_id=correlation_id,
                external_job_id=external_job_id,
                executor_mapping=rd.executor_mapping,
                remote_message=remote_message,
                runtime_context=bridge_runtime_context,
            )
            native_job.metadata.setdefault("plugin_id", plugin_id)
            native_job.metadata.setdefault("plugin_correlation_id", correlation_id)
            if external_job_id:
                native_job.metadata.setdefault("external_job_id", external_job_id)
            native_job.metadata.setdefault("remote_integration_id", rd.integration_id)

            job_manager = JobManager(self._backend.redis)
            backend_id = (
                None
                if getattr(self._backend, "distributed_mode", False)
                else getattr(
                    self._backend,
                    "instance_id",
                    None,
                )
            )
            native_job = await job_manager.prepare_job(native_job, backend_id=backend_id)
            # Recovery owns the committed plan even if runtime publication is
            # interrupted. Persist its identity before activation so a duplicate
            # remote delivery cannot create a second local job in that gap.
            correlation.local_job_id = native_job.job_id
            correlation.local_job_name = native_job.name
            correlation.local_status = "pending"
            correlation.remote_status = correlation.status = "claimed"
            self._persist_submission_record(correlation)
            await job_manager.activate_job(
                native_job,
                distributed_mode=bool(
                    getattr(self._backend, "distributed_mode", False)
                ),
                backend_id=backend_id,
            )

            correlation.local_job_id = native_job.job_id
            correlation.local_job_name = native_job.name
            correlation.local_status = "claimed"
            correlation.remote_status = "claimed"
            correlation.status = "claimed"
            correlation.last_error = None
            self._persist_submission_record(correlation)

            await self._emit_event(
                event_family="job.lifecycle",
                event_type="job.claimed",
                body={
                    "job_id": native_job.job_id,
                    "job_name": native_job.name,
                    "plugin_id": plugin_id,
                    "correlation_id": correlation_id,
                    "external_job_id": external_job_id,
                    "task_count": len(tasks),
                },
                job_id=native_job.job_id,
            )
        except Exception as exc:
            if correlation.local_job_id:
                # The committed local job remains recoverable. A failed runtime
                # hint or event delivery must not report that work as failed or
                # invite another local job on a repeated remote delivery.
                correlation.last_error = str(exc)
                self._persist_submission_record(correlation)
                logger.exception("Prepared plugin job %s awaits runtime recovery", correlation.local_job_id)
                return correlation.to_dict()
            correlation.local_status = "failed"
            correlation.status = "failed"
            correlation.last_error = str(exc)
            correlation.last_inbound_event = {
                "event": "bridge_failed",
                "error": str(exc),
                "remote_message": remote_message,
            }
            self._persist_submission_record(correlation)
            logger.exception(
                "Failed to bridge remote work item for %s external_job_id=%s",
                plugin_id,
                external_job_id,
            )
            if external_job_id and rd.operations.fail is not None:
                fail_result = await self.execute_remote_worker_action(
                    plugin_id,
                    "fail",
                    external_job_id,
                    {"failure_message": str(exc)},
                )
                if fail_result.get("success"):
                    correlation.remote_status = "failed"
                    self._persist_submission_record(correlation)
        return correlation.to_dict()

    def verify_remote_callback_request(
        self,
        plugin_id: str,
        handler_id: str,
        *,
        raw_body: bytes,
        headers: dict[str, str],
    ) -> None:
        record = self._require_enabled(plugin_id)
        manifest = record.manifest
        handler = next(
            (cb for cb in manifest.callbacks if cb.handler_id == handler_id), None
        )
        if handler is None:
            raise ValueError(
                f"Plugin {plugin_id!r} has no callback handler {handler_id!r}"
            )

        normalized_headers = {k.lower(): v for k, v in headers.items()}
        if handler.auth_method == "bearer":
            auth_header = normalized_headers.get("authorization", "")
            token = (
                auth_header.split(" ", 1)[1].strip()
                if auth_header.lower().startswith("bearer ")
                else auth_header.strip()
            )
            if not token or not self.secrets.verify(
                plugin_id, "callback", handler_id, token
            ):
                raise PermissionError("Invalid callback bearer token")
            return

        signature = (
            normalized_headers.get("x-usr-signature")
            or normalized_headers.get("x-plugin-signature")
            or normalized_headers.get("x-signature")
            or normalized_headers.get("x-hub-signature-256")
            or ""
        )
        if not signature or not self.secrets.verify_signature(
            plugin_id,
            "callback",
            handler_id,
            raw_body,
            signature,
        ):
            raise PermissionError("Invalid callback signature")

    def _normalize_pubsub_message(self, message: object) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return None
        return cast(dict[str, Any], message)

    def _normalize_local_job_event(
        self, event: dict[str, Any]
    ) -> dict[str, Any] | None:
        raw_event = event.get("event") or event.get("type")
        if raw_event is None:
            return None
        event_name = str(raw_event).replace(".", "_")
        lifecycle_event: str | None = None
        if event_name == "job_created":
            lifecycle_event = "created"
        elif event_name == "task_claimed":
            lifecycle_event = "claimed"
        elif event_name == "job_started":
            lifecycle_event = "running"
        elif event_name == "job_progress":
            completed_tasks = int(event.get("completed_tasks") or 0)
            lifecycle_event = "progress" if completed_tasks > 0 else "running"
        elif event_name == "job_completed":
            lifecycle_event = "completed"
        elif event_name == "job_failed":
            lifecycle_event = "failed"
        elif event_name == "job_cancelled":
            lifecycle_event = "cancelled"
        if lifecycle_event is None:
            return None
        normalized = dict(event)
        normalized["lifecycle_event"] = lifecycle_event
        return normalized

    async def _process_local_job_event(self, event: dict[str, Any]) -> None:
        normalized = self._normalize_local_job_event(event)
        if normalized is None:
            return

        job_id = normalized.get("job_id")
        worker_id = normalized.get("worker_id")
        lifecycle_event = str(normalized["lifecycle_event"])

        await self._emit_event(
            event_family="job.lifecycle",
            event_type=f"job.{lifecycle_event}",
            body=normalized,
            job_id=str(job_id) if job_id else None,
            worker_id=str(worker_id) if worker_id else None,
        )

        if not isinstance(job_id, str):
            return
        correlation_id = self._local_job_index.get(job_id)
        if correlation_id is None:
            return
        rec = self._submissions.get(correlation_id)
        if rec is None:
            return
        await self._apply_local_job_event(rec, normalized)

    async def _apply_local_job_event(
        self,
        rec: RemoteSubmissionRecord,
        event: dict[str, Any],
    ) -> None:
        lifecycle_event = str(event["lifecycle_event"])
        rec.local_status = lifecycle_event
        rec.last_inbound_event = event
        if lifecycle_event == "claimed":
            rec.status = "claimed"
        elif lifecycle_event in {"running", "progress"}:
            rec.status = "in_progress"
        elif lifecycle_event in {"completed", "failed", "cancelled"}:
            rec.status = lifecycle_event
        self._persist_submission_record(rec)
        await self._mirror_local_lifecycle_event(rec, event)

    async def _mirror_local_lifecycle_event(
        self,
        rec: RemoteSubmissionRecord,
        event: dict[str, Any],
    ) -> None:
        if not rec.external_job_id:
            return
        record = self.registry.get(rec.plugin_id)
        manifest = record.manifest if record is not None else None
        rd = manifest.remote_delegation if manifest is not None else None
        if (
            manifest is None
            or rd is None
            or rd.role not in ("executor", "bidirectional")
            or record is None
            or not self._has_capability(record, PluginCapability.REMOTE_JOBS_EXECUTE)
        ):
            return

        lifecycle_event = str(event["lifecycle_event"])
        action_map = {
            "running": "heartbeat",
            "progress": "progress",
            "completed": "complete",
            "failed": "fail",
            "cancelled": "cancel",
        }
        action = action_map.get(lifecycle_event)
        if action is None:
            return
        payload = dict(event)
        payload.pop("lifecycle_event", None)
        try:
            result = await self.execute_remote_worker_action(
                rec.plugin_id,
                action,
                rec.external_job_id,
                payload,
            )
            if isinstance(result, dict) and not result.get("success", True):
                error_detail = result.get("error", "remote action returned failure")
                rec.last_error = f"mirror {action}: {error_detail}"
                self._persist_submission_record(rec)
                logger.warning(
                    "Mirror %s for plugin=%s external_job_id=%s returned failure: %s",
                    action,
                    rec.plugin_id,
                    rec.external_job_id,
                    error_detail,
                )
        except Exception:
            rec.last_error = f"mirror {action}: unhandled exception"
            self._persist_submission_record(rec)
            logger.exception(
                "Mirror %s failed for plugin=%s external_job_id=%s correlation=%s",
                action,
                rec.plugin_id,
                rec.external_job_id,
                rec.correlation_id,
            )

    def _build_websocket_envelope(
        self,
        rd: Any,
        record: WebSocketMessageRecord,
        plugin_id: str,
    ) -> dict[str, Any]:
        assert rd.websocket is not None
        mapping = rd.websocket.message_envelope_mapping
        return {
            mapping.message_id_field: record.message_id,
            mapping.message_type_field: record.message_type,
            mapping.plugin_id_field: plugin_id,
            mapping.correlation_id_field: record.correlation_id,
            mapping.external_job_id_field: record.external_job_id,
            mapping.action_field: record.action,
            mapping.payload_field: record.payload,
            mapping.timestamp_field: record.timestamp,
        }

    def _parse_websocket_envelope(
        self,
        rd: Any,
        raw_message: Any,
    ) -> dict[str, Any] | None:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        if isinstance(raw_message, str):
            try:
                raw_message = json.loads(raw_message)
            except json.JSONDecodeError:
                return None
        if not isinstance(raw_message, dict):
            return None
        assert rd.websocket is not None
        mapping = rd.websocket.message_envelope_mapping
        return {
            "message_id": raw_message.get(mapping.message_id_field),
            "message_type": raw_message.get(mapping.message_type_field),
            "plugin_id": raw_message.get(mapping.plugin_id_field),
            "correlation_id": raw_message.get(mapping.correlation_id_field),
            "external_job_id": raw_message.get(mapping.external_job_id_field),
            "action": raw_message.get(mapping.action_field),
            "payload": raw_message.get(mapping.payload_field),
            "timestamp": raw_message.get(mapping.timestamp_field),
            "raw": raw_message,
        }

    def _mark_websocket_message_acknowledged(
        self,
        plugin_id: str,
        message_id: str,
    ) -> None:
        session = self.runtime_store.get_session(plugin_id)
        if session is None:
            return
        changed = False
        for item in session.backlog:
            if item.message_id == message_id and not item.acknowledged:
                item.acknowledged = True
                changed = True
                break
        if changed:
            self.runtime_store.save_session(session)

    async def _send_websocket_record(
        self,
        plugin_id: str,
        rd: Any,
        message_record: WebSocketMessageRecord,
    ) -> bool:
        websocket_client = self._ws_live_sessions.get(plugin_id)
        if websocket_client is None:
            return False
        lock = self._ws_send_locks.setdefault(plugin_id, asyncio.Lock())
        envelope = self._build_websocket_envelope(rd, message_record, plugin_id)
        async with lock:
            await websocket_client.send(json.dumps(envelope, separators=(",", ":")))
        return True

    async def _flush_websocket_backlog(
        self,
        plugin_id: str,
        rd: Any,
    ) -> int:
        session = self.runtime_store.get_session(plugin_id)
        if session is None:
            return 0
        flushed = 0
        for item in session.backlog:
            if item.acknowledged:
                continue
            if await self._send_websocket_record(plugin_id, rd, item):
                flushed += 1
        return flushed

    async def _send_session_heartbeat(self, plugin_id: str, rd: Any) -> None:
        if plugin_id not in self._ws_live_sessions:
            return
        heartbeat = WebSocketMessageRecord(
            message_id=_next_correlation_id(),
            message_type="command",
            action="heartbeat",
            payload={"scope": "session"},
        )
        await self._send_websocket_record(plugin_id, rd, heartbeat)

    async def _handle_websocket_envelope(
        self,
        plugin_id: str,
        rd: Any,
        envelope: dict[str, Any],
    ) -> None:
        message_type = str(envelope.get("message_type") or "")
        message_id = envelope.get("message_id")
        action = str(envelope.get("action") or "")
        payload = envelope.get("payload")

        if isinstance(message_id, str) and message_type in {"ack", "error"}:
            self._mark_websocket_message_acknowledged(plugin_id, message_id)
            return

        if (
            rd.role in ("executor", "bidirectional")
            and rd.executor_mapping is not None
            and action in _WS_INBOUND_REMOTE_ACTIONS
            and isinstance(payload, dict)
        ):
            await self._bridge_remote_work_item(
                plugin_id,
                payload,
                external_job_id=(
                    str(envelope["external_job_id"])
                    if envelope.get("external_job_id") is not None
                    else None
                ),
                correlation_id=(
                    str(envelope["correlation_id"])
                    if envelope.get("correlation_id") is not None
                    else None
                ),
            )
            return

        if action in self._LIFECYCLE_STATUS_MAP and isinstance(payload, dict):
            mapping = next(
                (
                    item
                    for item in rd.callbacks.inbound
                    if self._literal_mapping_value(item.extract.lifecycle_event)
                    == action
                ),
                None,
            )
            if mapping is not None:
                self.receive_remote_callback(plugin_id, mapping.handler_id, payload)

    def _require_enabled(self, plugin_id: str) -> PluginRecord:
        if self.is_uninstalled(plugin_id):
            raise PermissionError('Plugin is uninstalled')
        self._sync_persisted_configuration()
        record = self.registry.get(plugin_id)
        if record is None:
            raise KeyError(f"Unknown plugin {plugin_id!r}")
        if not record.is_active:
            raise PermissionError(
                f"Plugin {plugin_id!r} is not enabled (status={record.status.value})"
            )
        return record

    def _require_known(self, plugin_id: str) -> PluginRecord:
        self._sync_catalog_from_repository()
        record = self.registry.get(plugin_id)
        if record is None:
            raise KeyError(f"Unknown plugin {plugin_id!r}")
        return record

    def _load_enabled_set(self) -> set[str]:
        """Load the set of plugin IDs that were previously enabled."""
        return self.repository.list_enabled_plugins()

    def _present_manifest_ids(self) -> set[str]:
        """Return plugin directories that currently contain a manifest file."""
        if not self._plugins_dir.is_dir():
            return set()
        present: set[str] = set()
        try:
            children = list(self._plugins_dir.iterdir())
        except OSError:
            logger.warning(
                "Could not inspect plugin catalog directory %s",
                self._plugins_dir,
                exc_info=True,
            )
            return present
        for child in children:
            if child.name.startswith('.') or not child.is_dir() or self.is_uninstalled(child.name):
                continue
            if (child / "manifest.yaml").is_file() or (
                child / "manifest.yml"
            ).is_file():
                present.add(child.name)
        return present

    def _write_manifest_atomically(
        self,
        manifest_path: Path,
        manifest_data: dict[str, Any],
    ) -> None:
        fd, tmp_path = tempfile.mkstemp(
            dir=str(manifest_path.parent),
            suffix=".yaml.tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                yaml.safe_dump(
                    manifest_data,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    allow_unicode=True,
                )
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, str(manifest_path))
            try:
                directory_fd = os.open(manifest_path.parent, os.O_RDONLY)
            except OSError:
                directory_fd = None
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except BaseException:
            with suppress(OSError):
                os.unlink(tmp_path)
            raise

    def _init_secrets_for_plugin(self, record: PluginRecord) -> None:
        """Create secrets for all endpoints/targets that don't already have one."""
        manifest = record.manifest
        if manifest is None:
            return
        for cb in manifest.callbacks:
            self.secrets.ensure_secret(
                manifest.plugin_id,
                "callback",
                cb.handler_id,
            )
        for target in manifest.outbound_deliveries:
            self.secrets.ensure_secret(
                manifest.plugin_id,
                "outbound",
                target.target_name,
            )
        for task in manifest.task_types:
            if task.handler and task.handler.auth_scope:
                self.secrets.ensure_secret(manifest.plugin_id, "task", task.handler.auth_scope)
        # Create secret for remote delegation integration if present
        if manifest.remote_delegation is not None:
            if self.repository.get_host_document(manifest.plugin_id, 'installation').get('source'):
                # Source-installed remote authentication is supplied in Setup,
                # never replaced by a generated placeholder credential.
                return
            rd = manifest.remote_delegation
            legacy_secret = (
                self.secrets.get_raw_secret(
                    manifest.plugin_id, "remote_integration", rd.integration_id
                )
                or ""
            )
            if rd.auth_scopes:
                copy_legacy_to_scope = len(rd.auth_scopes) == 1 and legacy_secret
                for scope_name in rd.auth_scopes.keys():
                    self.secrets.ensure_secret(
                        manifest.plugin_id,
                        "remote_auth",
                        scope_name,
                        initial_value=(
                            legacy_secret if copy_legacy_to_scope else None
                        ),
                    )
            else:
                self.secrets.ensure_secret(
                    manifest.plugin_id,
                    "remote_auth",
                    "default",
                    initial_value=legacy_secret or None,
                )

    # ── Remote submission state helpers ────────────────────────────────────

    # Lifecycle event → submission status mapping
    _LIFECYCLE_STATUS_MAP: dict[str, str] = {
        "claimed": "claimed",
        "progress": "in_progress",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
    }

    # Remote job status (from polling) → submission status mapping
    _REMOTE_STATUS_MAP: dict[str, str] = {
        "pending": "submitted",
        "claimed": "claimed",
        "in_progress": "in_progress",
        "running": "in_progress",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
    }

    def _update_submission_from_lifecycle_event(
        self,
        rec: RemoteSubmissionRecord,
        lifecycle_event: str,
        payload: dict[str, Any],
    ) -> None:
        """Update a submission record based on a lifecycle callback event."""
        new_status = self._LIFECYCLE_STATUS_MAP.get(lifecycle_event)
        rec.last_inbound_event = payload
        rec.remote_response = payload
        remote_status = payload.get("remote_status")
        rec.remote_status = (
            str(remote_status) if remote_status not in (None, "") else lifecycle_event
        )
        error_value = payload.get("error")
        if error_value not in (None, ""):
            rec.last_error = str(error_value)
        if new_status:
            rec.status = new_status
        self._persist_submission_record(rec)

    def _update_submission_from_remote_status(
        self,
        rec: RemoteSubmissionRecord,
        remote_status: str,
    ) -> None:
        """Update a submission record based on polled remote status."""
        new_status = self._REMOTE_STATUS_MAP.get(remote_status)
        rec.remote_status = remote_status
        if new_status:
            rec.status = new_status
        self._persist_submission_record(rec)

    @staticmethod
    def _validate_job_payload(
        jt_def: Any,
        payload: dict[str, Any],
    ) -> list[str]:
        """Validate a payload dict against declared job fields."""
        errors: list[str] = []
        declared_names = {f.name for f in jt_def.payload_fields}
        for field_def in jt_def.payload_fields:
            if field_def.required and field_def.name not in payload:
                errors.append(f"Missing required field: {field_def.name}")
        for key in payload:
            if key not in declared_names:
                errors.append(f"Unknown field: {key}")
        for field_def in jt_def.payload_fields:
            if field_def.name not in payload:
                continue
            value = payload[field_def.name]
            type_ok = _check_field_type(value, field_def.field_type)
            if not type_ok:
                errors.append(
                    f"Field {field_def.name}: expected {field_def.field_type}, "
                    f"got {type(value).__name__}"
                )
            if (
                field_def.allowed_values is not None
                and value not in field_def.allowed_values
            ):
                errors.append(
                    f"Field {field_def.name}: value {value!r} not in "
                    f"allowed_values {field_def.allowed_values}"
                )
        return errors


def _check_field_type(value: Any, expected: str) -> bool:
    type_map = {
        "string": str,
        "integer": int,
        "float": (int, float),
        "boolean": bool,
        "list": list,
        "object": dict,
    }
    expected_types = type_map.get(expected)
    if expected_types is None:
        return True
    # bool is a subclass of int in Python — reject bools for integer fields
    if expected == "integer" and isinstance(value, bool):
        return False
    return isinstance(value, expected_types)
