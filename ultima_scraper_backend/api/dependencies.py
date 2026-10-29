"""FastAPI dependency injection for backend services.

Provides singleton access to:
- BackendInstance: Queue and event bus management
- Config: Application configuration
- Database sessions: SQLite access
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING


from ..config import ExecutionMode, ProcessRole, UltimaScraperCollectionConfig
from ..manager import BackendManager
from ..models.backend import BackendInstance
from ..redis.client import RedisManager

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Global singleton instances
_backend_instance: BackendInstance | None = None
_backend_manager: BackendManager | None = None


def get_config() -> UltimaScraperCollectionConfig:
    """Read current canonical values and revision together across control workers."""
    return UltimaScraperCollectionConfig.load_or_create_default_config()


async def get_backend_instance() -> BackendInstance:
    """Get backend instance (singleton per process).

    The backend instance manages:
    - Job queue (local or Redis)
    - Event bus (local or Redis)
    - Mode switching (enable/disable distributed)
    """
    global _backend_instance

    if _backend_instance is None:
        print("[DEPS] Creating new BackendInstance...")
        config = get_config()
        backend_id = config.settings.get_backend_id()
        print(f"[DEPS] Backend ID: {backend_id}")

        # Initialize Redis if enabled (optional - gracefully handle failures)
        redis = None
        if config.settings.redis.enabled and config.settings.redis.host:
            try:
                print(
                    f"[DEPS] Connecting to Redis {config.settings.redis.host}:{config.settings.redis.port}..."
                )
                redis = RedisManager(
                    host=config.settings.redis.host,
                    port=config.settings.redis.port,
                    db=config.settings.redis.db,
                    password=config.settings.redis.password,
                )
                await redis.connect()
                from ultima_scraper_api.managers.redis import attach_redis

                attach_redis(redis)
                logger.info(
                    "Redis connected: %s:%s",
                    redis.config.host,
                    redis.config.port,
                )
                print("[DEPS] Redis connected")
            except Exception as e:
                logger.warning(
                    "Redis connection failed (running in local mode): %s", str(e)
                )
                print(f"[DEPS ERROR] Redis connection failed: {e}")
                redis = None

        # Determine execution mode and whether distributed queue is used
        execution_mode = config.settings.backend.execution_mode

        # distributed_mode determines if we use Redis queue (not just Redis connection)
        # In REDIS_METRICS mode, we connect to Redis but use local queue
        distributed_mode = (
            redis is not None and execution_mode == ExecutionMode.DISTRIBUTED
        )

        print(
            f"[DEPS] Creating BackendInstance "
            f"(distributed={distributed_mode}, execution_mode={execution_mode.value}, role=control)"
        )
        _backend_instance = BackendInstance(
            instance_id=backend_id,
            redis=redis,
            distributed_mode=distributed_mode,
            execution_mode=execution_mode,
            process_role=ProcessRole.CONTROL,  # FastAPI = control plane
        )

        # Complete async initialization
        print("[DEPS] Calling initialize()...")
        await _backend_instance.initialize()
        print("[DEPS] BackendInstance initialized")

        logger.info(
            f"BackendInstance '{backend_id}' initialized (distributed={_backend_instance.distributed_mode})"
        )

    return _backend_instance


def get_backend_manager() -> BackendManager:
    """Get backend manager (singleton per process).

    The backend manager provides access to:
    - Database managers for each site
    - Authentication management
    - API clients
    """
    global _backend_manager

    if _backend_manager is None:
        # Initialize BackendManager with default config
        _backend_manager = BackendManager()
        logger.info("BackendManager initialized")

    return _backend_manager


async def shutdown_backend() -> None:
    """Cleanup backend resources on shutdown."""
    global _backend_instance
    global _backend_manager

    if _backend_instance:
        logger.info(
            "Shutting down backend instance '%s'", _backend_instance.instance_id
        )
        redis_manager = _backend_instance.redis
        await _backend_instance.close()
        if redis_manager is not None:
            from ultima_scraper_api.managers.redis import detach_redis

            detach_redis(redis_manager)
        _backend_instance = None
        logger.info("Backend instance shutdown complete")

    if _backend_manager:
        logger.info("Shutting down backend manager resources")
        try:
            await _backend_manager.close_live_sessions()
        except Exception as exc:
            logger.warning("Failed to close backend live sessions: %s", exc)

        try:
            if _backend_manager.multi_site_db_manager:
                _backend_manager.multi_site_db_manager.disconnect_all()
        except Exception as exc:
            logger.warning("Failed to disconnect backend manager DB pools: %s", exc)

        _backend_manager = None

    try:
        from ..database.factory import MultiSiteDatabaseManager

        MultiSiteDatabaseManager().disconnect_all()
    except Exception as exc:
        logger.warning("Failed to disconnect shared DB pools on shutdown: %s", exc)
