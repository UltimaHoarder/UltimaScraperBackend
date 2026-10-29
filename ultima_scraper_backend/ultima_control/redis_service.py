"""Centralized Redis service facade (Tier 1).

Provides a single point of access to Redis operations with:
- High-level business operations (jobs, backend status, etc.)
- Managed pubsub subscriptions via EventSubscriber
- Consistent error handling and logging
- Type-safe operations

Architecture:
    RedisService (this) → RedisManager (USAPI) → redis.asyncio

Usage:
    service = RedisService.get_instance()
    job = await service.fetch_job(job_id)
    await service.publish_event("download_progress", event_data)
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, TypeGuard

from ultima_scraper_api.managers.redis import (
    get_redis,
)
from ultima_scraper_backend.redis_channels import HOOKS_CHANNEL

if TYPE_CHECKING:
    from .event_subscriber import EventSubscriber

from ultima_scraper_backend.redis.client import RedisManager

logger = logging.getLogger(__name__)

class RedisService:
    """Centralized Redis service facade.

    Singleton pattern - use get_instance() to access.
    Provides high-level operations on top of USAPI's RedisManager.
    """

    _instance: RedisService | None = None
    _event_subscriber: EventSubscriber | None = None

    def __init__(self) -> None:
        """Initialize Redis service.

        Note: Use get_instance() instead of direct instantiation.
        """
        self._redis: RedisManager | None = None

    def attach_manager(self, manager: RedisManager) -> None:
        """Attach an externally managed Redis connection for shared use.

        Args:
            manager: Connected Redis manager instance to reuse throughout the app.
        """
        if not manager.is_connected:
            logger.warning(
                "Attempted to attach a Redis manager that isn't connected; ignoring"
            )
            return

        self._redis = manager

        # Ensure ultima_scraper_api's global helpers also reuse this connection
        try:
            from ultima_scraper_api.managers.redis import connection as usapi_connection

            usapi_connection._redis_manager = manager  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.debug("Failed to sync USAPI Redis manager: %s", exc)

    @classmethod
    def get_instance(cls) -> RedisService:
        """Get or create the singleton instance.

        Returns:
            RedisService singleton
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def set_event_subscriber(cls, subscriber: EventSubscriber) -> None:
        """Set the centralized event subscriber (Tier 2).

        Args:
            subscriber: EventSubscriber instance to manage pubsub
        """
        cls._event_subscriber = subscriber

    @property
    def redis(self) -> RedisManager | None:
        """Get Redis manager instance.

        Returns:
            RedisManager if available and connected, None otherwise
        """
        if self._redis is None:
            base_redis = get_redis()
            if isinstance(base_redis, RedisManager):
                self._redis = base_redis
            elif base_redis is not None:
                logger.debug(
                    "Using base RedisManager instance without extended queue helpers"
                )

        if self._redis and self._redis.is_connected:
            return self._redis

        return None

    @staticmethod
    def _is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
        return isinstance(value, dict)

    @staticmethod
    def _load_json_dict(data: str) -> dict[str, object] | None:
        parsed = json.loads(data)
        if not RedisService._is_object_dict(parsed):
            return None
        return {str(key): value for key, value in parsed.items()}

    @property
    def is_available(self) -> bool:
        """Check if Redis is available and connected.

        Returns:
            True if Redis is connected
        """
        return self.redis is not None

    # =========================================================================
    # Publishing Operations
    # =========================================================================

    async def publish_event(
        self,
        event_type: str,
        data: dict[str, Any],
        channel: str = HOOKS_CHANNEL,
    ) -> bool:
        """Publish event to Redis channel.

        Args:
            event_type: Event type identifier
            data: Event payload data
            channel: Redis channel to publish to

        Returns:
            True if published successfully
        """
        if not self.redis:
            logger.debug(f"Redis unavailable, skipping event: {event_type}")
            return False

        try:
            event = {"event_type": event_type, **data}
            return await self.redis.publish(channel, json.dumps(event))
        except Exception as e:
            logger.debug(f"Failed to publish {event_type} event: {e}")
            return False

    # =========================================================================
    # Job Operations (High-Level)
    # =========================================================================

    async def fetch_job(self, job_id: str) -> dict[str, Any] | None:
        """Fetch job data from Redis.

        Args:
            job_id: Job identifier

        Returns:
            Job data dict if found, None otherwise
        """
        if not self.redis:
            return None

        try:
            key = f"ultimascraper:jobs:{job_id}"
            data = await self.redis.get(key)
            if data:
                return self._load_json_dict(data)
            return None
        except Exception as e:
            logger.debug(f"Failed to fetch job {job_id}: {e}")
            return None

    async def list_jobs(self, pattern: str = "ultimascraper:jobs:*") -> list[str]:
        """List all job IDs matching pattern.

        Args:
            pattern: Redis key pattern

        Returns:
            List of job IDs
        """
        if not self.redis:
            return []

        try:
            keys = await self.redis.keys(pattern)
            # Extract job IDs from keys (ultimascraper:jobs:job-123 → job-123)
            return [key.split(":")[-1] for key in keys]
        except Exception as e:
            logger.debug(f"Failed to list jobs: {e}")
            return []

    async def delete_job(self, job_id: str) -> bool:
        """Delete job data from Redis.

        Args:
            job_id: Job identifier

        Returns:
            True if deleted successfully
        """
        if not self.redis:
            return False

        try:
            key = f"ultimascraper:jobs:{job_id}"
            return await self.redis.delete(key)
        except Exception as e:
            logger.debug(f"Failed to delete job {job_id}: {e}")
            return False

    # =========================================================================
    # Backend/Worker Status Operations
    # =========================================================================

    async def get_backend_status(self) -> dict[str, Any] | None:
        """Get backend manager status.

        Returns:
            Backend status dict if available
        """
        if not self.redis:
            return None

        try:
            key = "ultimascraper:backend:status"
            data = await self.redis.get(key)
            if data:
                return self._load_json_dict(data)
            return None
        except Exception as e:
            logger.debug(f"Failed to get backend status: {e}")
            return None

    async def get_worker_status(self, worker_id: str) -> dict[str, Any] | None:
        """Get worker status.

        Args:
            worker_id: Worker identifier

        Returns:
            Worker status dict if available
        """
        if not self.redis:
            return None

        try:
            key = f"ultimascraper:workers:{worker_id}:status"
            data = await self.redis.get(key)
            if data:
                return self._load_json_dict(data)
            return None
        except Exception as e:
            logger.debug(f"Failed to get worker {worker_id} status: {e}")
            return None

    # =========================================================================
    # Queue Operations
    # =========================================================================

    # =========================================================================
    # Health Check
    # =========================================================================

    async def health_check(self) -> dict[str, Any]:
        """Check Redis health and connectivity.

        Returns:
            Health status dict with connection info
        """
        if not self.redis:
            return {
                "connected": False,
                "error": "Redis not initialized",
            }

        try:
            await self.redis.client.ping()  # type: ignore[misc]
            return {
                "connected": True,
                "host": self.redis.config.host,
                "port": self.redis.config.port,
                "db": self.redis.config.db,
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
            }


# =============================================================================
# Convenience Functions (Shortcuts)
# =============================================================================


def get_redis_service() -> RedisService:
    """Get the singleton RedisService instance.

    Returns:
        RedisService singleton
    """
    return RedisService.get_instance()


async def publish_download_progress(event: dict[str, Any]) -> bool:
    """Publish download progress event.

    Args:
        event: Download progress event data

    Returns:
        True if published successfully
    """
    service = get_redis_service()
    return await service.publish_event("download_progress", event)


async def publish_api_command_result(event: dict[str, Any]) -> bool:
    """Publish API command result event.

    Args:
        event: API command result data

    Returns:
        True if published successfully
    """
    service = get_redis_service()
    return await service.publish_event("api_command_result", event)
