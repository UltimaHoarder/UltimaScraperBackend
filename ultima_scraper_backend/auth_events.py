"""
Auth Management Event System

Provides Redis pub/sub channels for synchronizing auth changes across all workers.
When auths are added, removed, or updated via the API, events are published so workers
can update their auth collections appropriately.

**Channels:**
- `ultimascraper:auth_events` - All auth management events

**Event Types:**
- `auth_added` - New auth added to system
- `auth_removed` - Auth removed from system
- `auth_updated` - Auth credentials updated

**Shard Behavior:**
- Shard owner: Full login with WebSocket
- Non-shard workers: Do nothing (lazy load on demand)
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, cast

from .redis_channels import AUTH_EVENTS_CHANNEL

if TYPE_CHECKING:
    from ultima_scraper_backend.redis.client import RedisManager

logger = logging.getLogger(__name__)


class AuthEventType(str, Enum):
    """Types of auth management events."""

    ADDED = "auth_added"
    REMOVED = "auth_removed"
    UPDATED = "auth_updated"


class AuthEventPublisher:
    """Publishes auth management events to Redis."""

    CHANNEL = AUTH_EVENTS_CHANNEL

    def __init__(self, redis: RedisManager) -> None:
        """
        Initialize publisher.

        Args:
            redis: RedisManager instance for publishing
        """
        self.redis = redis

    async def publish_auth_added(
        self,
        auth_id: int,
        username: Optional[str] = None,
        site: str = "onlyfans",
    ) -> None:
        """
        Publish auth_added event.

        Args:
            auth_id: OnlyFans account ID (user_id)
            username: Username (optional)
            site: Site name (default: onlyfans)
        """
        await self._publish_event(
            event_type=AuthEventType.ADDED,
            auth_id=auth_id,
            username=username,
            site=site,
        )

    async def publish_auth_updated(
        self,
        auth_id: int,
        username: Optional[str] = None,
        site: str = "onlyfans",
    ) -> None:
        """
        Publish auth_updated event.

        Args:
            auth_id: OnlyFans account ID (user_id)
            username: Username (optional)
            site: Site name (default: onlyfans)
        """
        await self._publish_event(
            event_type=AuthEventType.UPDATED,
            auth_id=auth_id,
            username=username,
            site=site,
        )

    async def _publish_event(
        self,
        event_type: AuthEventType,
        auth_id: int,
        site: str,
        username: Optional[str] = None,
    ) -> None:
        """
        Publish an auth management event.

        Args:
            event_type: Type of event
            auth_id: OnlyFans account ID
            site: Site name
            username: Username (optional)
        """
        try:
            from datetime import datetime, timezone

            event = {
                "event": event_type.value,
                "auth_id": auth_id,
                "site": site,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            if username:
                event["username"] = username

            message = json.dumps(event)
            await self.redis.publish(self.CHANNEL, message)

            logger.info(f"Published {event_type.value} event for auth {auth_id}")
        except Exception as exc:
            logger.error(f"Failed to publish auth event: {exc}", exc_info=True)


class AuthEventHandler:
    """
    Handles auth management events received by workers.

    Workers subscribe to auth events and update their auth collections:
    - Shard owner: Full login with WebSocket
    - Non-shard: Do nothing (lazy load on demand)
    """

    def __init__(
        self,
        worker_index: int,
        worker_count: int,
        api: Any,
        engine: Any,
        site: str = "onlyfans",
    ) -> None:
        """
        Initialize event handler.

        Args:
            worker_index: This worker's index (0-based)
            worker_count: Total number of workers
            api: USAPI instance
            engine: SQLAlchemy engine
            site: Site name (default: onlyfans)
        """
        self.worker_index = worker_index
        self.worker_count = worker_count
        self.api = api
        self.engine = engine
        self.site = site

    def is_shard_owner(self, auth_id: int) -> bool:
        """Check if this worker owns the shard for given auth."""
        return (auth_id % self.worker_count) == self.worker_index

    async def handle_event(self, event: dict[str, Any]) -> None:
        """
        Handle an auth management event.

        Args:
            event: Event data from Redis pub/sub
        """
        event_type = event.get("event")
        auth_id_raw = event.get("auth_id")
        site = event.get("site", "onlyfans")

        if auth_id_raw is None:
            logger.warning(f"Auth event missing auth_id: {event}")
            return
        try:
            auth_id = int(auth_id_raw)
        except (TypeError, ValueError):
            logger.warning(f"Auth event has invalid auth_id: {event}")
            return

        if site.lower() != self.site.lower():
            logger.debug(f"Ignoring auth event for different site: {site}")
            return

        if event_type == AuthEventType.ADDED.value:
            await self._handle_auth_added(auth_id, event)
        elif event_type == AuthEventType.REMOVED.value:
            await self._handle_auth_removed(auth_id)
        elif event_type == AuthEventType.UPDATED.value:
            await self._handle_auth_updated(auth_id, event)
        else:
            logger.warning(f"Unknown auth event type: {event_type}")

    async def _handle_auth_added(self, auth_id: int, event: dict[str, Any]) -> None:
        """
        Handle auth_added event.

        Shard owner: Full login with WebSocket
        Non-shard: Do nothing (lazy load on demand)
        """
        if not self.is_shard_owner(auth_id):
            logger.info(
                f"Auth {auth_id} added, but not our shard "
                f"(belongs to worker {auth_id % self.worker_count}). "
                "Will lazy-load if needed."
            )
            return

        logger.info(
            f"Auth {auth_id} added and belongs to our shard. Loading with WebSocket..."
        )

        try:
            from ultima_scraper_backend.usapi_initializer import (
                USAPIInitializer,
                apply_assigned_proxy_to_auth_json,
            )

            # Check if already loaded
            if hasattr(self.api, "find_auth"):
                existing = self.api.find_auth(auth_id)
                if existing:
                    logger.info(f"Auth {auth_id} already loaded, skipping")
                    return

            # Load from database with full initialization
            initializer = USAPIInitializer(self.engine)

            # Build auth_json from database
            from sqlmodel import Session, select

            from ultima_scraper_db_archive import UserAuthModel

            with Session(self.engine) as session:
                stmt = select(UserAuthModel).where(
                    UserAuthModel.user_id == auth_id,
                    UserAuthModel.site == self.site.lower(),
                )
                account = session.exec(stmt).first()

                if not account:
                    logger.warning(f"Auth {auth_id} not found in database")
                    return

                session.expunge(account)

            # Build auth_json
            if self.site.lower() == "onlyfans":
                auth_json = {
                    "id": account.user_id,
                    "username": account.username or "",
                    "cookie": account.cookie or "",
                    "x_bc": account.x_bc or "",
                    "user_agent": account.user_agent or "",
                    "email": account.email or "",
                    "password": account.password or "",
                }
            else:
                logger.warning(f"Site {self.site} not supported for auth events")
                return

            auth_json = apply_assigned_proxy_to_auth_json(
                auth_json, getattr(account, "proxy_id", None)
            )

            # Full login with WebSocket
            authed = await initializer.initialize_auth(
                site=self.site,
                account_id=auth_id,
                api_instance=self.api,
                auth_json=auth_json,
                skip_api_refresh=False,  # Do API refresh for new auth
            )

            if authed:
                logger.info(
                    f"Successfully loaded auth {auth_id} with WebSocket "
                    f"(username: {account.username or 'unknown'})"
                )
            else:
                logger.warning(f"Failed to load auth {auth_id}")

        except Exception as exc:
            logger.error(f"Error handling auth_added event: {exc}", exc_info=True)

    async def _handle_auth_removed(self, auth_id: int) -> None:
        """
        Handle auth_removed event.

        Remove auth from API instance if loaded.
        """
        logger.info(f"Auth {auth_id} removed event received")

        try:
            auths = cast(dict[int, Any], self.api.auths)
            if auth_id in auths:
                await self.api.remove_auth(auths[auth_id])
                logger.info(f"Removed auth {auth_id} from API instance")
            else:
                logger.debug(f"Auth {auth_id} not loaded, nothing to remove")

        except Exception as exc:
            logger.error(f"Error handling auth_removed event: {exc}", exc_info=True)

    async def _handle_auth_updated(self, auth_id: int, event: dict[str, Any]) -> None:
        """
        Handle auth_updated event.

        Reload auth credentials from database.
        """
        logger.info(f"Auth {auth_id} updated event received")

        # Remove existing and reload
        await self._handle_auth_removed(auth_id)

        # If shard owner, reload with new credentials
        if self.is_shard_owner(auth_id):
            await self._handle_auth_added(auth_id, event)
        else:
            logger.info(
                f"Auth {auth_id} updated but not our shard. "
                "Will lazy-load with new credentials if needed."
            )
