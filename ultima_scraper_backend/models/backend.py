"""Backend instance with mode switching capability.

Each backend instance can independently enable/disable distributed mode.
When distributed mode is enabled, local jobs migrate to Redis for sharing.
When distributed mode is disabled, Redis jobs stay for other backends.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import socket
from importlib.metadata import version as package_version
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ..config import ExecutionMode, ProcessRole
from ..ultima_control.system_stats_reporter import SystemStatsReporter
from .event_bus import EventBus, InMemoryEventBus, RedisEventBus
from .queue import InMemoryQueue, JobQueue, RedisQueue

if TYPE_CHECKING:
    from ultima_scraper_backend.ultima_control.backend_registry import (
        BackendRegistry as BackendRegistryType,
    )

    from ..redis.client import RedisManager

logger = logging.getLogger(__name__)


# Import BackendRegistry at runtime for auto-registration in distributed mode
_BackendRegistry: type[Any] | None = None
try:
    from ultima_scraper_backend.ultima_control.backend_registry import (
        BackendRegistry as _BackendRegistryImport,
    )

    _BackendRegistry = _BackendRegistryImport
except ImportError:
    pass


def _redis_client(redis: RedisManager) -> Any:
    """Return the underlying async Redis client with an explicit dynamic boundary."""
    return cast(Any, redis.client)


def _decode_pubsub_message(message: object) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None
    return cast(dict[str, Any], message)


async def _publish_worker_pool_runtime_events(
    redis: RedisManager | None,
    *,
    backend_id: str,
    settings_scope: str,
    old_size: int,
    new_size: int,
    old_max_claimed_jobs: int,
    new_max_claimed_jobs: int,
) -> None:
    if redis is None:
        return

    from ..redis_channels import CONFIG_EVENTS_CHANNEL

    if new_size != old_size:
        scale_event = {
            "type": "worker_pool_scale",
            "target_size": new_size,
            "backend_id": backend_id,
            "settings_scope": settings_scope,
        }
        await redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(scale_event))
        logger.info(
            "Published worker_pool_scale event for scope %s: %s → %s",
            settings_scope,
            old_size,
            new_size,
        )

    if new_max_claimed_jobs != old_max_claimed_jobs:
        claimed_event = {
            "type": "max_claimed_jobs_update",
            "max_claimed_jobs": new_max_claimed_jobs,
            "backend_id": backend_id,
            "settings_scope": settings_scope,
        }
        await redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(claimed_event))
        logger.info(
            "Published max_claimed_jobs_update event for scope %s: %s → %s",
            settings_scope,
            old_max_claimed_jobs,
            new_max_claimed_jobs,
        )


class BackendInstance:
    """Represents a single backend instance with mode switching capability.

    Each backend can independently:
    - Switch between local (InMemoryQueue) and distributed (RedisQueue)
    - Migrate pending jobs when enabling distributed mode
    - Leave Redis jobs for other backends when disabling
    - Auto-register with Redis for multi-backend visibility
    """

    def __init__(
        self,
        instance_id: str,
        redis: RedisManager | None = None,
        distributed_mode: bool = False,
        execution_mode: ExecutionMode = ExecutionMode.DISTRIBUTED,
        process_role: ProcessRole = ProcessRole.CONTROL,
    ) -> None:
        """Initialize backend instance.

        Args:
            instance_id: Unique identifier for this backend (e.g., "backend-1")
            redis: Redis client (required for distributed mode)
            distributed_mode: Start in distributed mode?
            execution_mode: Execution mode (local, distributed, redis_metrics)
            process_role: Role of this process (control or worker)
        """
        self.instance_id = instance_id
        self.redis = redis
        self._distributed_mode = False
        self._transitioning = False
        self._execution_mode = execution_mode
        self._process_role = process_role

        # Initialize queues
        self._local_queue = InMemoryQueue()
        self._redis_queue: RedisQueue | None = None

        # Initialize event buses
        self._local_event_bus = InMemoryEventBus()
        self._redis_event_bus: RedisEventBus | None = None

        # Backend registry for multi-backend visibility
        self._registry: BackendRegistryType | None = None
        self._stats_reporter: SystemStatsReporter | None = None


        # Config event subscriber task
        self._config_subscriber_task: asyncio.Task[None] | None = None
        self._settings_reload_tasks: dict[str, asyncio.Task[None]] = {}
        self._settings_reload_latest_events: dict[str, dict[str, Any]] = {}
        self._settings_last_applied_versions: dict[str, int] = {}

        # Set initial mode
        if distributed_mode and redis:
            # Note: In real usage, you'd await this, but for init we mark it pending
            self._init_distributed_mode = True
        else:
            self._init_distributed_mode = False

        logger.info(
            f"BackendInstance '{instance_id}' initialized "
            f"(queue_mode={'distributed' if distributed_mode else 'local'}, "
            f"execution_mode={execution_mode.value}, "
            f"process_role={process_role.value})"
        )

    async def initialize(self) -> None:
        """Complete async initialization (call after __init__)."""
        print(
            f"[BACKEND {self.instance_id}] Initializing... "
            f"distributed_mode={self._distributed_mode}, "
            f"execution_mode={self._execution_mode.value}, "
            f"process_role={self._process_role.value}"
        )
        if self._init_distributed_mode:
            await self.enable_distributed_mode()

        # Set up backend registry for multi-backend visibility
        # Only WORKER processes register - CONTROL plane doesn't need to be visible
        should_register = (
            self._distributed_mode
            and self.redis is not None
            and _BackendRegistry is not None
            and self._process_role == ProcessRole.WORKER
        )
        if should_register:
            await self._setup_registry()

        # Host telemetry belongs to the control process, not individual workers.
        # Use the same backend identity that /system/backend reads from Redis.
        if (
            self.redis is not None
            and self._process_role == ProcessRole.CONTROL
            and self._stats_reporter is None
        ):
            try:
                reporter = SystemStatsReporter(self.redis, self.instance_id)
                await reporter.start()
                self._stats_reporter = reporter
            except Exception:
                logger.exception("Could not start system stats reporter")

        # Keep management.backends aligned with active backend identity/settings scope.
        await self._sync_backend_inventory()

        # Start config event subscriber for remote config updates
        # Must be AFTER _setup_registry so instance_id is resolved
        if self._distributed_mode and self.redis is not None:
            await self._start_config_subscriber()
            # Publish current config to Redis for remote access
            # Uses resolved instance_id (with suffix if needed)
            await self._publish_config_to_redis()

    @staticmethod
    def _resolve_local_ip() -> str | None:
        """Best-effort local IP discovery for backend inventory rows."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(("8.8.8.8", 80))
                resolved = sock.getsockname()[0]
                if resolved:
                    return resolved
        except Exception:
            pass

        try:
            resolved = socket.gethostbyname(socket.gethostname())
            return resolved or None
        except Exception:
            return None

    async def _sync_backend_inventory(self) -> None:
        """Upsert this backend into management.backends.

        This keeps DB inventory in sync with the running backend(s) that
        register in Redis/system status.
        """
        try:
            await asyncio.to_thread(self._sync_backend_inventory_sync)
        except Exception as exc:
            logger.debug(
                "Skipping backend inventory sync for '%s': %s",
                self.instance_id,
                exc,
            )

    def _sync_backend_inventory_sync(self) -> None:
        from sqlmodel import select
        from ultima_scraper_db_archive.global_db import (
            BackendInventoryModel,
            SettingsModel,
            get_global_session,
            is_setup_complete,
        )

        from ..config import UltimaScraperCollectionConfig

        if not is_setup_complete():
            return

        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        target_scope = getattr(config, "_settings_backend", None)
        if not isinstance(target_scope, str) or not target_scope.strip():
            target_scope = config.config_write_backend()

        hostname = str(config.settings.hostname or socket.gethostname()).strip()
        if not hostname:
            hostname = socket.gethostname()
        backend_name = str(self.instance_id or hostname).strip()
        local_ip = self._resolve_local_ip()

        with get_global_session() as session:
            settings_row = session.exec(
                select(SettingsModel).where(
                    SettingsModel.backend == target_scope,
                    SettingsModel.key == "config",
                )
            ).first()
            if settings_row is None:
                return

            row = session.exec(
                select(BackendInventoryModel).where(
                    BackendInventoryModel.hostname == hostname
                )
            ).first()
            if row is None:
                row = BackendInventoryModel(
                    backend_name=backend_name,
                    hostname=hostname,
                    local_ip=local_ip,
                    settings_id=settings_row.id,
                )
                session.add(row)
            else:
                row.backend_name = backend_name
                row.local_ip = local_ip
                row.settings_id = settings_row.id
                session.add(row)

            session.commit()

    async def _start_config_subscriber(self) -> None:
        """Start listening for remote config update requests."""
        if self._config_subscriber_task is not None:
            return

        self._config_subscriber_task = asyncio.create_task(
            self._config_subscriber_loop()
        )
        logger.info(f"Config subscriber started for backend '{self.instance_id}'")
        print(f"[BACKEND {self.instance_id}] 📡 Config subscriber started")

    async def _stop_config_subscriber(self) -> None:
        """Stop config event subscriber."""
        if self._config_subscriber_task:
            self._config_subscriber_task.cancel()
            try:
                await self._config_subscriber_task
            except asyncio.CancelledError:
                pass
            self._config_subscriber_task = None

    async def _config_subscriber_loop(self) -> None:
        """Subscribe to config events and handle remote updates."""
        from ..redis_channels import CONFIG_EVENTS_CHANNEL

        if not self.redis:
            return

        try:
            pubsub = _redis_client(self.redis).pubsub()
            await pubsub.subscribe(CONFIG_EVENTS_CHANNEL)
            logger.info(f"Subscribed to {CONFIG_EVENTS_CHANNEL}")

            async for raw_message in pubsub.listen():
                message = _decode_pubsub_message(raw_message)
                if message is None or message.get("type") != "message":
                    continue

                try:
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    event = json.loads(data)

                    event_type = event.get("type")

                    # Filter by target backend for other event types
                    target_id = event.get("target_backend_id")
                    if target_id and target_id != self.instance_id:
                        continue

                    if event_type == "config_update_request":
                        await self._handle_config_update_request(event)
                    elif event_type == "config_get_request":
                        await self._handle_config_get_request(event)
                    elif event_type == "settings_updated":
                        await self._handle_settings_updated_event(event)

                except json.JSONDecodeError:
                    logger.warning("Invalid JSON in config event")
                except Exception as e:
                    logger.error(f"Error handling config event: {e}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Config subscriber error: {e}")

    def _get_local_settings_scope(self) -> str:
        from ..config import UltimaScraperCollectionConfig

        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        return config.config_write_backend()

    async def _handle_settings_updated_event(self, event: dict[str, Any]) -> None:
        """Handle settings update broadcast and debounce runtime reload."""
        scope = str(event.get("settings_scope") or "").strip()
        if not scope:
            return

        # Ignore updates for scopes this backend does not use.
        try:
            local_scope = self._get_local_settings_scope()
        except Exception:
            return
        if scope != local_scope:
            return

        try:
            version = int(event.get("version") or 0)
        except (TypeError, ValueError):
            return

        if version <= self._settings_last_applied_versions.get(scope, 0):
            return

        self._settings_reload_latest_events[scope] = event
        existing_task = self._settings_reload_tasks.get(scope)
        if existing_task is not None and not existing_task.done():
            return

        self._settings_reload_tasks[scope] = asyncio.create_task(
            self._debounced_reload_settings_scope(scope)
        )

    async def _debounced_reload_settings_scope(self, scope: str) -> None:
        """Reload settings for a scope after a short coalescing window."""
        try:
            await asyncio.sleep(0.35)
            latest_event = self._settings_reload_latest_events.get(scope, {})
            try:
                latest_version = int(latest_event.get("version") or 0)
            except (TypeError, ValueError):
                latest_version = 0

            from ..config import UltimaScraperCollectionConfig

            # Pull latest DB-backed settings and refresh Redis config cache.
            UltimaScraperCollectionConfig.load_or_create_default_config()
            await self._publish_config_to_redis()

            if latest_version > 0:
                self._settings_last_applied_versions[scope] = latest_version
            logger.info(
                "Applied settings_updated event for scope=%s version=%s",
                scope,
                latest_version,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Failed to reload settings for scope %s: %s", scope, exc)
        finally:
            self._settings_reload_tasks.pop(scope, None)

    async def _handle_config_update_request(self, event: dict[str, Any]) -> None:
        """Handle a remote config update request."""
        from ..config import UltimaScraperCollectionConfig

        logger.info(f"Received config update request: {event}")

        try:
            updates = event.get("updates", {})
            if not updates:
                return

            old_worker_pool_size: int | None = None
            new_worker_pool_size: int | None = None
            old_max_claimed_jobs: int | None = None
            new_max_claimed_jobs: int | None = None

            # Load current config
            runtime_fields = {
                "hostname",
                "redis",
                "security",
                "is_configured",
            } & updates.keys()
            if "backend_id" in updates.get("backend", {}):
                runtime_fields.add("backend.backend_id")
            if runtime_fields:
                raise ValueError(
                    "Deployment settings cannot be changed through config broadcasts"
                )
            config = UltimaScraperCollectionConfig.load_or_create_default_config()
            settings = config.settings

            from ..job_system import mps_state
            await mps_state.sync_runtime_settings(self.redis.client, settings)

            from ..api.routes.settings import SettingsUpdateRequest, _merge_model_update

            patch = SettingsUpdateRequest.model_validate(updates)
            old_max_claimed_jobs = settings.worker_pool.max_claimed_jobs
            old_worker_pool_size = settings.worker_pool.size
            for field in ("worker_pool", "backend", "network", "download_setup", "metadata_setup", "drm"):
                if field in updates and updates[field] is not None:
                    current = getattr(settings, field)
                    parsed = type(current).model_validate(updates[field])
                    _merge_model_update(current, parsed, updates[field])
            if patch.sites is not None:
                _merge_model_update(config.site_apis, patch.sites, updates["sites"])
            for field in ("infinite_loop", "exit_on_completion"):
                if field in updates:
                    setattr(settings, field, getattr(patch, field))
            for field in ("previews_directory", "subtitles_directory"):
                if field in updates:
                    from pathlib import Path
                    setattr(settings, field, Path(updates[field]) if updates[field] else None)
            new_max_claimed_jobs = settings.worker_pool.max_claimed_jobs
            new_worker_pool_size = settings.worker_pool.size

            from ..config_settings_sync import persist_config_snapshot

            target_scope = config.config_write_backend()
            snapshot = persist_config_snapshot(
                config, expected_version=event.get("expected_version") if event.get("expected_version") is not None else config._settings_version
            )
            persisted_settings_id = snapshot.settings_id
            persisted_version = snapshot.version
            persisted_updated_at = snapshot.updated_at
            logger.info(f"Config updated and saved for backend '{self.instance_id}'")

            # Republish config to Redis
            await self._publish_config_to_redis()

            if (
                old_worker_pool_size is not None
                and new_worker_pool_size is not None
                and old_max_claimed_jobs is not None
                and new_max_claimed_jobs is not None
            ):
                await _publish_worker_pool_runtime_events(
                    self.redis,
                    backend_id=self.instance_id,
                    settings_scope=target_scope,
                    old_size=old_worker_pool_size,
                    new_size=new_worker_pool_size,
                    old_max_claimed_jobs=old_max_claimed_jobs,
                    new_max_claimed_jobs=new_max_claimed_jobs,
                )

            if (
                self.redis
                and persisted_settings_id is not None
                and persisted_version is not None
            ):
                from ..redis_channels import CONFIG_EVENTS_CHANNEL

                settings_updated = {
                    "type": "settings_updated",
                    "target_backend_id": self.instance_id,
                    "settings_id": persisted_settings_id,
                    "settings_scope": target_scope,
                    "version": persisted_version,
                    "updated_at": persisted_updated_at,
                    "changed_fields": sorted(str(key) for key in updates.keys()),
                    "actor_backend_id": None,
                    "request_id": event.get("request_id"),
                }
                await self.redis.publish(
                    CONFIG_EVENTS_CHANNEL, json.dumps(settings_updated)
                )

            # Publish success response
            if self.redis:
                from ..redis_channels import CONFIG_EVENTS_CHANNEL

                response = {
                    "type": "config_update_response",
                    "source_backend_id": self.instance_id,
                    "request_id": event.get("request_id"),
                    "success": True,
                }
                await self.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(response))

        except Exception as e:
            logger.error(f"Failed to apply config update: {e}")
            # Publish error response
            if self.redis:
                from ..redis_channels import CONFIG_EVENTS_CHANNEL

                response = {
                    "type": "config_update_response",
                    "source_backend_id": self.instance_id,
                    "request_id": event.get("request_id"),
                    "success": False,
                    "error": str(e),
                }
                await self.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(response))

    async def _handle_config_get_request(self, event: dict[str, Any]) -> None:
        """Handle a request to get this backend's config."""
        # Just republish config to Redis
        await self._publish_config_to_redis()

    async def _publish_config_to_redis(self) -> None:
        """Publish current config to Redis for remote access."""
        if not self.redis:
            return

        from ..config import UltimaScraperCollectionConfig
        from ..redis_channels import CONFIG_KEY_PREFIX

        try:
            config = UltimaScraperCollectionConfig.load_or_create_default_config()
            from ..api.routes.settings import _config_to_response

            config_data = _config_to_response(config).model_dump(mode="json")

            # Store in Redis with TTL
            key = f"{CONFIG_KEY_PREFIX}{self.instance_id}"
            await self.redis.set(key, json.dumps(config_data), ex=120)
            logger.debug(f"Published config to Redis: {key}")

        except Exception as e:
            logger.error(f"Failed to publish config to Redis: {e}")

    async def _setup_registry(self) -> None:
        """Set up and register with the backend registry."""
        if _BackendRegistry is None or self.redis is None:
            return

        from ..config import UltimaScraperCollectionConfig

        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        registry_class = cast(Any, _BackendRegistry)
        registry = cast(
            BackendRegistryType,
            registry_class(
                redis_manager=self.redis,
                backend_id=self.instance_id,
                max_workers=config.settings.worker_pool.size,
                version=package_version("ultima-scraper-backend"),
                capabilities=["scraping", "downloading"],
                status="online",
                heartbeat_interval=10.0,
                ttl=60,
                status_provider=self._get_registry_status,
            ),
        )
        self._registry = registry

        # Register and start heartbeat
        await registry.register()
        await registry.start_heartbeat()

        # Update our instance_id to match the resolved unique ID
        if registry.backend_id:
            self.instance_id = registry.backend_id

        logger.info(f"Backend '{self.instance_id}' registered with Redis registry")


    def _get_registry_status(self) -> dict[str, object]:
        """Provide current status for registry heartbeat."""
        drives: list[dict[str, object]] = []

        # Get drive information using psutil for accurate drive detection
        try:
            import psutil

            excluded_prefixes = (
                "/boot",
                "/snap",
                "/proc",
                "/sys",
                "/dev",
                "/run",
                "/tmp",
                "/var/lib/docker",
                "/var/lib/snapd",
            )
            excluded_fstypes = {
                "squashfs",
                "tmpfs",
                "devtmpfs",
                "overlay",
                "fuse.portal",
            }
            seen_mounts: set[str] = set()

            for partition in psutil.disk_partitions(all=False):
                mount_point = partition.mountpoint

                # Skip if already seen or excluded
                if mount_point in seen_mounts:
                    continue
                if any(mount_point.startswith(p) for p in excluded_prefixes):
                    continue
                if partition.fstype in excluded_fstypes:
                    continue

                seen_mounts.add(mount_point)

                try:
                    usage = psutil.disk_usage(mount_point)
                    drives.append(
                        {
                            "path": mount_point,
                            "device": partition.device,
                            "total_bytes": usage.total,
                            "used_bytes": usage.used,
                            "free_bytes": usage.free,
                            "percent_used": round(usage.percent, 1),
                            "configured_directories": [],
                        }
                    )
                except (OSError, PermissionError):
                    continue
        except ImportError:
            # Fallback without psutil
            try:
                for mount_point in ["/", "/home"]:
                    mp = Path(mount_point)
                    if not mp.exists():
                        continue
                    usage = shutil.disk_usage(mount_point)
                    drives.append(
                        {
                            "path": mount_point,
                            "device": mount_point,
                            "total_bytes": usage.total,
                            "used_bytes": usage.used,
                            "free_bytes": usage.free,
                            "percent_used": (
                                round(usage.used / usage.total * 100, 1)
                                if usage.total > 0
                                else 0
                            ),
                            "configured_directories": [],
                        }
                    )
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Failed to get drive status: {e}")

        return {"drives": drives}

    @property
    def distributed_mode(self) -> bool:
        """Current mode: True = distributed (Redis), False = local (memory)."""
        return self._distributed_mode

    @property
    def transitioning(self) -> bool:
        """Is mode switch in progress?"""
        return self._transitioning

    @property
    def active_queue(self) -> JobQueue:
        """Queue that workers should pull from (respects current mode)."""
        if self._distributed_mode and self._redis_queue:
            return self._redis_queue
        else:
            return self._local_queue

    @property
    def job_queue(self) -> JobQueue:
        """Compatibility alias for callers that still expect `job_queue`."""
        return self.active_queue

    @property
    def active_event_bus(self) -> EventBus:
        """Event bus to use (respects current mode)."""
        if self._distributed_mode and self._redis_event_bus:
            return self._redis_event_bus
        else:
            return self._local_event_bus

    async def enable_distributed_mode(self) -> None:
        """Enable distributed mode and migrate pending local jobs to Redis.

        Migration strategy:
        1. Initialize Redis queue/event bus
        2. Migrate all pending local jobs to Redis
        3. Switch to distributed mode

        This ensures other backends can help with existing work.
        """
        if self._distributed_mode:
            logger.warning(f"[{self.instance_id}] Already in distributed mode")
            return

        if not self.redis:
            logger.error(
                f"[{self.instance_id}] Cannot enable distributed mode: no Redis client"
            )
            return

        self._transitioning = True

        try:
            logger.info(f"[{self.instance_id}] Enabling distributed mode...")

            # Initialize Redis queue and event bus
            self._redis_queue = RedisQueue(
                self.redis, queue_key=f"ultimascraper:jobs:{self.instance_id}"
            )
            self._redis_event_bus = RedisEventBus(self.redis)

            # Migrate all pending local jobs to Redis
            migrated = 0
            while True:
                try:
                    # Non-blocking get from local queue
                    job = await asyncio.wait_for(
                        self._local_queue.dequeue(timeout=0), timeout=0.1
                    )
                    if job is None:
                        break
                    await self._redis_queue.enqueue(job)
                    migrated += 1
                except asyncio.TimeoutError:
                    break

            # Enable distributed mode
            self._distributed_mode = True

            logger.info(
                f"[{self.instance_id}] ✅ Distributed mode ENABLED "
                f"(migrated {migrated} jobs to Redis)"
            )

        except Exception as e:
            logger.error(
                f"[{self.instance_id}] Failed to enable distributed mode: {e}",
                exc_info=True,
            )
            raise

        finally:
            self._transitioning = False

    async def disable_distributed_mode(self) -> None:
        """Disable distributed mode (leave Redis jobs for other backends).

        Migration strategy:
        1. Switch to local mode
        2. DO NOT migrate Redis jobs to local

        Rationale: Other backends may still be using Redis queue.
        Migrating jobs to local would steal work from them.
        """
        if not self._distributed_mode:
            logger.warning(f"[{self.instance_id}] Already in local mode")
            return

        self._transitioning = True

        try:
            logger.info(f"[{self.instance_id}] Disabling distributed mode...")


            # Get Redis queue size before switching
            redis_jobs = await self._redis_queue.size() if self._redis_queue else 0

            # Switch to local mode (do NOT migrate Redis jobs)
            self._distributed_mode = False

            # Close Redis event bus
            if self._redis_event_bus:
                await self._redis_event_bus.close()
                self._redis_event_bus = None

            logger.info(
                f"[{self.instance_id}] ✅ Distributed mode DISABLED "
                f"(left {redis_jobs} jobs in Redis for other backends)"
            )

        except Exception as e:
            logger.error(
                f"[{self.instance_id}] Failed to disable distributed mode: {e}",
                exc_info=True,
            )
            raise

        finally:
            self._transitioning = False

    async def get_queue_stats(self) -> dict[str, object]:
        """Get current queue statistics."""
        # Get unclaimed pool size (the actual pending jobs queue)
        unclaimed_size = 0
        if self.redis:
            try:
                from ..job_system.routing import BackendRouter

                unclaimed_size = await self.redis.llen(BackendRouter.UNCLAIMED_JOBS_KEY)
            except Exception:
                pass

        # Read recovery countdown from Redis
        recovery_interval = 30  # must match RECOVERY_CHECK_INTERVAL in worker_pool
        next_recovery_seconds: int | None = None
        last_recovery_at: str | None = None
        if self.redis:
            try:
                from datetime import datetime, timezone

                rc_key = f"ultimascraper:backend:recovery_ts:{self.instance_id}"
                ts_raw = await self.redis.get(rc_key)
                if ts_raw:
                    ts_str = str(ts_raw)
                    last_dt = datetime.fromisoformat(ts_str)
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
                    remaining = max(0, int(recovery_interval - elapsed))
                    next_recovery_seconds = remaining
                    last_recovery_at = ts_str
            except Exception:
                pass

        return {
            "instance_id": self.instance_id,
            "distributed_mode": self._distributed_mode,
            "transitioning": self._transitioning,
            "local_queue_size": await self._local_queue.size(),
            "redis_queue_size": unclaimed_size,
            "active_queue_type": "redis" if self._distributed_mode else "memory",
            "recovery_interval": recovery_interval,
            "next_recovery_seconds": next_recovery_seconds,
            "last_recovery_at": last_recovery_at,
        }

    async def close(self) -> None:
        """Release backend resources (event buses, Redis connections)."""
        if self._stats_reporter is not None:
            try:
                await self._stats_reporter.stop()
            except Exception:
                logger.exception("Could not stop system stats reporter")
            self._stats_reporter = None


        # Stop config subscriber
        await self._stop_config_subscriber()
        for task in self._settings_reload_tasks.values():
            task.cancel()
        if self._settings_reload_tasks:
            await asyncio.gather(
                *self._settings_reload_tasks.values(), return_exceptions=True
            )
        self._settings_reload_tasks.clear()
        self._settings_reload_latest_events.clear()

        # Unregister from backend registry
        if self._registry:
            try:
                await self._registry.stop_heartbeat()
                await self._registry.unregister()
                logger.info(f"Backend '{self.instance_id}' unregistered from registry")
            except Exception as e:
                logger.warning(f"Failed to unregister backend: {e}")
            self._registry = None

        if self._redis_event_bus:
            await self._redis_event_bus.close()
            self._redis_event_bus = None
        if self.redis:
            await self.redis.disconnect()
