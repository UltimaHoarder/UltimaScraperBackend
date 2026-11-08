"""WebSocket action handler for processing WebSocket events.

Handles real-time events from OnlyFans API:
- 'new_message' with subType='new_subscriber' (subscription detection)
- 'toasts' events (subscription notifications)
- 'online' events (user activity)
- 'api2_chat_message' events (chat messages, queued for DB persistence)
- 'post_published' events (post fetched from the API and queued for DB persistence)
- 'post_updated' notifications
"""

from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, Union, cast

from sqlmodel import Session, select
from ultima_scraper_api import get_site_title
from ultima_scraper_api.apis.onlyfans.classes.websocket.messages.toast import (
    ToastCollectionModel,
    ToastTypeEnum,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from .manager import BackendManager
    from ultima_scraper_backend.workers.worker_pool import Worker
    from ultima_scraper_api import UltimaScraperAPI

from ultima_scraper_core.typings.guards import (
    has_auth_id,
    has_auths,
    has_engine,
    has_find_auth,
    has_redis_manager,
    has_subscriptions,
    has_ultima_scraper_api,
)
from ultima_scraper_backend.helpers.background_tasks import spawn_background_task
from ultima_scraper_backend.redis.keys import WATCHES_PAUSED_KEY
from ultima_scraper_backend.redis_channels import HOOKS_CHANNEL, KEY_PREFIX
from ultima_scraper_db_archive.site import SubscriptionModel, UserModel

logger = logging.getLogger(__name__)

# Constants
DEFAULT_LOCK_TTL = 30
DEFAULT_SUBSCRIPTION_DAYS = 30


def _renew_lapsed_subscription(subscription: SubscriptionModel) -> bool:
    """Reactivate a saved subscription that a new subscribed event shows has lapsed."""
    now = datetime.now(timezone.utc)
    expires_at = subscription.expires_at
    if expires_at is not None and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if subscription.active and (expires_at is None or expires_at > now):
        return False
    subscription.active = True
    subscription.expires_at = now + timedelta(days=DEFAULT_SUBSCRIPTION_DAYS)
    return True


class UserInfoProtocol(Protocol):
    username: str | None
    credit_balance: float | None

    def is_performer(self) -> bool: ...


def _extract_source_id(source_id: Optional[Union[str, int]]) -> Optional[int]:
    """Extract and convert source_id to integer.

    Handles formats like:
    - "onlyfans:ws:15585607" -> 15585607
    - "15585607" -> 15585607
    - 15585607 -> 15585607

    Args:
        source_id: Source ID in various formats

    Returns:
        Integer source ID or None if invalid
    """
    if not source_id:
        return None

    if isinstance(source_id, int):
        return source_id

    # Remaining supported branch is str.
    if ":" in source_id:
        source_id = source_id.split(":")[-1]

    try:
        return int(source_id)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert source_id to int: {source_id}")
        return None

    return None


def _extract_site_name(source_id: Optional[Union[str, int]]) -> str:
    """Extract site name from source_id.

    Handles formats like:
    - "onlyfans:ws:15585607" -> "onlyfans"
    - "fansly:ws:123456" -> "fansly"
    - "loyalfans:ws:789012" -> "loyalfans"

    Args:
        source_id: Source ID in format "site:ws:account_id"

    Returns:
        Site name (lowercase)

    Raises:
        ValueError: If source_id is missing, malformed, or contains invalid site name
    """
    if not source_id:
        raise ValueError("source_id is required and cannot be None or empty")

    if isinstance(source_id, str) and ":" in source_id:
        site_name = source_id.split(":")[0].lower()
        if site_name in ["onlyfans", "fansly", "loyalfans"]:
            return site_name
        raise ValueError(
            f"Invalid site name '{site_name}' in source_id '{source_id}'. "
            f"Expected: onlyfans, fansly, or loyalfans"
        )

    # Malformed source_id
    raise ValueError(
        f"Invalid source_id format '{source_id}'. "
        f"Expected format: 'site:ws:account_id' (e.g., 'onlyfans:ws:15585607')"
    )


class WebSocketActionHandler:
    """Handles WebSocket events and triggers appropriate actions.

    Features:
    - Multi-site support (OnlyFans, Fansly, LoyalFans)
    - Distributed message deduplication using Redis locks
    - Automatic subscription detection and database persistence
    - Support for multiple event types (subscriptions, posts, messages, etc.)
    - Dynamic site routing based on message source

    Architecture:
    - Expects backend to have `ultima_scraper_api: UltimaScraperAPI` attribute
    - UltimaScraperAPI provides access to all site APIs via `api_instances.{SiteName}`
    - Messages are routed to correct site API based on source_id prefix
    """

    def __init__(self, backend_or_worker: Union[BackendManager, Worker]) -> None:
        """Initialize the WebSocket action handler.

        Args:
            backend_or_worker: Reference to backend manager or worker
                Must have `ultima_scraper_api` attribute (UltimaScraperAPI instance)
        """
        self.backend = backend_or_worker
        self._processed_message_ids: deque[str] = deque(maxlen=1000)

        # Resolve UltimaScraperAPI instance (provides access to all site APIs)
        self._ultima_scraper_api = self._resolve_ultima_scraper_api()

        if self._ultima_scraper_api:
            logger.info(
                f"✅ WebSocketActionHandler initialized with UltimaScraperAPI from {type(backend_or_worker).__name__}"
            )
            logger.info(
                f"   Supported sites: {[name for name in vars(self._ultima_scraper_api.api_instances).keys()]}"
            )
        else:
            logger.warning(
                f"No UltimaScraperAPI instance found in backend type: {type(backend_or_worker).__name__}"
            )

        # Get database engine for persistence
        self.engine: Optional[Engine] = (
            backend_or_worker.engine if has_engine(backend_or_worker) else None
        )

        # Get Redis instance for distributed locking
        self.redis: Optional[Any] = None
        if has_redis_manager(backend_or_worker):
            self.redis = backend_or_worker.get_redis()
        elif hasattr(backend_or_worker, "redis"):
            redis_attr = getattr(backend_or_worker, "redis")
            self.redis = redis_attr() if callable(redis_attr) else redis_attr

        self._database_writes: Optional[Any] = None

        if self.redis is not None:
            logger.info("✅ Redis locking enabled for WebSocket message deduplication")
        else:
            logger.warning("No Redis available - using local deduplication only")

    def _resolve_ultima_scraper_api(self) -> Optional[UltimaScraperAPI]:
        """Resolve the UltimaScraperAPI instance from backend object.

        Naming convention:
        - ultima_scraper_api = UltimaScraperAPI() instance (wrapper/manager)
        - site_api = OnlyFans/Fansly/etc API instance (accessed via api_instances.OnlyFans)

        Expected:
        - backend.ultima_scraper_api (UltimaScraperAPI instance)

        Returns:
            UltimaScraperAPI instance or None if not found
        """
        # Use type guard for type-safe access
        if has_ultima_scraper_api(self.backend):
            logger.debug("Using backend.ultima_scraper_api")
            return self.backend.ultima_scraper_api

        logger.debug(
            f"No UltimaScraperAPI found. Backend type: {type(self.backend).__name__}, "
            f"attributes: {[a for a in dir(self.backend) if not a.startswith('_')][:20]}"
        )
        return None

    def _get_site_api(self, site_name: str) -> Optional[Any]:
        """Get site API instance by site name.

        Args:
            site_name: Site name ("onlyfans", "fansly", "loyalfans")

        Returns:
            Site API instance or None if not found
        """
        if not self._ultima_scraper_api:
            return None

        # Map lowercase site name to proper case attribute name
        try:
            site_title = get_site_title(site_name)
            site_api = getattr(self._ultima_scraper_api.api_instances, site_title, None)
            if site_api:
                logger.debug(f"✓ Found {site_title} site API")
                return site_api
            else:
                logger.warning(f"✗ {site_title} site API not found in api_instances")
                return None
        except Exception as exc:
            logger.warning(f"Error getting site API for {site_name}: {exc}")
            return None

    async def _acquire_message_lock(
        self, message_id: str, ttl: int = DEFAULT_LOCK_TTL
    ) -> bool:
        """Try to acquire a distributed lock for processing a message.

        Args:
            message_id: Unique message ID
            ttl: Lock timeout in seconds (default 30)

        Returns:
            True if lock acquired, False if another worker already has it
        """
        if not self.redis:
            # No Redis - fallback to local deduplication only
            return True

        try:
            lock_key = f"{KEY_PREFIX}:ws_message_lock:{message_id}"
            # Use SET NX (set if not exists) with expiration
            acquired = await self.redis.set(lock_key, b"1", nx=True, ex=ttl)
            return bool(acquired)
        except Exception as exc:
            logger.warning(f"Redis lock failed, allowing message through: {exc}")
            return True  # Fail open - better to process twice than not at all

    async def process_message(self, message: dict[str, Any]) -> None:
        """Process a WebSocket message and take appropriate actions.

        Args:
            message: WebSocket message from Redis storage
                Format: {
                    "toasts": [...],  # Event data at top level
                    "id": "...",
                    "stored_at": "...",
                    "source_id": "onlyfans:ws:15585607"
                }
        """
        try:
            # Get message ID to avoid duplicate processing
            message_id = message.get("id")

            # Try to acquire distributed lock for this message
            if message_id:
                if message_id in self._processed_message_ids:
                    logger.debug(
                        f"Message {message_id} already processed locally, skipping"
                    )
                    return

                # Try to claim this message across all workers
                if not await self._acquire_message_lock(message_id):
                    logger.debug(
                        f"Message {message_id} already claimed by another worker, skipping"
                    )
                    return

            # Extract and convert source account info and determine site
            source_id_raw = message.get("source_id")

            # Skip messages without source_id (legacy or malformed messages)
            if not source_id_raw:
                logger.warning(
                    f"⚠️ Skipping WebSocket message without source_id. "
                    f"Message keys: {list(message.keys())}"
                )
                # Mark as processed to avoid reprocessing
                if message_id:
                    self._processed_message_ids.append(message_id)
                return

            source_id = _extract_source_id(source_id_raw)
            site_name = _extract_site_name(source_id_raw)
            username = message.get("username")

            # Debug: Log received event types (exclude metadata fields)
            event_keys = [
                k
                for k in message.keys()
                if k not in ["id", "stored_at", "source_id", "username"]
            ]
            logger.debug(
                f"📨 WebSocket event from {source_id} ({site_name}): keys={event_keys}"
            )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Full WebSocket message: {json.dumps(message, indent=2, default=str)}"
                )

            # Route event to appropriate handler (with site_name for multi-site support)
            if "new_message" in message:
                logger.info(f"🔔 Processing new_message event for {site_name}")
                await self._handle_new_message(
                    message["new_message"], source_id, username, site_name
                )
            elif "toasts" in message:
                logger.info(f"🍞 Processing toasts event for {site_name}")
                toast_collection = ToastCollectionModel(message["toasts"])
                await self._handle_toasts_event(
                    toast_collection, source_id, username, site_name
                )
            elif "online" in message:
                logger.info(f"🟢 Processing online event for {site_name}")
                await self._handle_online_event(
                    message["online"], source_id, username, site_name
                )
            elif "post_published" in message:
                spawn_background_task(
                    self._handle_post_published(
                        message["post_published"], source_id, username, site_name
                    ),
                    name="ws-post-published",
                )
            elif "post_updated" in message:
                post_id = message["post_updated"]
                logger.debug(
                    f"📝 Post {post_id} updated (account: {source_id}, site: {site_name})"
                )
            elif "api2_chat_message" in message:
                logger.debug(
                    f"💬 Chat message received from {source_id} on {site_name}"
                )
                spawn_background_task(
                    self._handle_chat_message(
                        message["api2_chat_message"], source_id, site_name
                    ),
                    name="ws-chat-message",
                )
            else:
                logger.debug(f"❓ Unknown WebSocket event type: {event_keys}")

            # Mark as processed locally
            if message_id:
                self._processed_message_ids.append(message_id)

        except Exception as exc:
            logger.error(f"Error processing WebSocket message: {exc}", exc_info=True)

    async def _handle_new_message(
        self,
        message_data: dict[str, Any],
        source_id: Optional[int],
        username: Optional[str],
        site_name: str,
    ) -> None:
        """Handle 'new_message' event - includes new subscriber notifications.

        Args:
            message_data: Message data from the event
            source_id: Account ID that received the notification
            username: Username of the account
            site_name: Site name for routing to correct API (required: "onlyfans", "fansly", "loyalfans")
        """
        sub_type = message_data.get("subType")
        message_type = message_data.get("type")

        if sub_type == "new_subscriber" or message_type == "subscribed":
            user_data = message_data.get("user", {})
            user_id = user_data.get("id")
            subscriber_username = user_data.get("username")
            subscriber_name = user_data.get("name")

            if not user_id:
                logger.warning("New subscriber notification missing user ID")
                return

            logger.info(
                f"🎉 NEW SUBSCRIBER: @{subscriber_username} ({subscriber_name}) "
                f"subscribed to account {username} on {site_name}"
            )

            authed = await self._get_authed_for_account(source_id, site_name)
            if not authed:
                logger.warning(
                    f"Could not find authed instance for account {source_id} on {site_name}"
                )
                return

            is_new = await self._check_and_add_user(
                authed,
                user_id,
                user_data,
                site_name=site_name,
            )
            if is_new:
                logger.info(
                    f"✓ New subscriber @{subscriber_username} added to subscriptions"
                )
            else:
                logger.debug(
                    f"Subscriber @{subscriber_username} already in subscription list"
                )
        else:
            logger.debug(
                f"Message notification: type={message_type}, subType={sub_type}"
            )

    async def _are_watches_paused(self) -> bool:
        """Return True when the global watch pause flag is present."""

        if self.redis is None:
            return False

        try:
            return bool(await self.redis.get(WATCHES_PAUSED_KEY))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to read watches pause key: %s", exc)
            return False

    async def _handle_toasts_event(
        self,
        toast_collection: ToastCollectionModel,
        source_id: Optional[int],
        username: Optional[str],
        site_name: str,
    ) -> None:
        """Handle 'toasts' event - includes toast notifications.

        Args:
            toasts_data: Toast data from the event
            source_id: Account ID that received the notification
            username: Username of the account
            site_name: Site name for routing to correct API (required: "onlyfans", "fansly", "loyalfans")
        """
        # Find all subscription-related toasts
        toasts = toast_collection.find(toast_type=ToastTypeEnum.SUBSCRIBED)

        logger.info(
            f"🍞 Toast handler called with {len(toasts)} SUBSCRIBED toasts (source={source_id}, site={site_name})"
        )

        for toast in toasts:
            # Access toast properties directly from the ToastModel
            related_user = toast.data.related_user
            subscriber_id = related_user.get("id")
            subscriber_username = related_user.get("username")
            subscriber_name = related_user.get("name")

            if not subscriber_id:
                logger.warning(f"Toast notification missing relatedUser.id: {toast.id}")
                continue

            logger.info(
                f"🎉 NEW SUBSCRIBER (toast): @{subscriber_username} ({subscriber_name}) "
                f"subscribed to account {username} on {site_name}"
            )

            # Get the authed API instance
            logger.debug(
                f"Looking up authed for source_id={source_id}, site={site_name}, type={type(source_id)}"
            )
            authed = await self._get_authed_for_account(source_id, site_name)
            if not authed:
                logger.warning(
                    f"Could not find authed instance for account {source_id} on {site_name}"
                )
                continue

            logger.debug(
                f"✓ Found authed instance, id={authed.id if has_auth_id(authed) else None}"
            )

            # Check subscription status from relatedUser data
            subscribed_by = related_user.get("subscribedBy", False)
            subscribed_on = related_user.get("subscribedOn", False)

            logger.info(
                f"💾 Attempting to save subscription: subscribedBy={subscribed_by}, subscribedOn={subscribed_on}"
            )

            # Save to database
            if self.engine and has_auth_id(authed):
                await self._save_toast_subscription_to_db(
                    authed_id=authed.id,
                    related_user=related_user,
                    subscribed_by=subscribed_by,
                    subscribed_on=subscribed_on,
                    site_name=site_name,
                )
            else:
                logger.warning(
                    f"Cannot save to DB: engine={self.engine is not None}, authed.id={has_auth_id(authed)}"
                )

    async def _handle_online_event(
        self,
        online_data: dict[str, Any],
        source_id: Optional[int],
        username: Optional[str],
        site_name: str,
    ) -> None:
        """Handle 'online' event - user coming online.

        Args:
            online_data: Online data from the event
            source_id: Account ID that received the notification
            username: Username of the account
            site_name: Site name for routing to correct API (required: "onlyfans", "fansly", "loyalfans")
        """

    def _get_database_writes(self) -> Optional[Any]:
        """Return the durable write client, reusing the worker's when it has one."""
        existing = getattr(self.backend, "database_writes", None)
        if existing is not None:
            return existing
        if self._database_writes is None and self.redis is not None:
            from ultima_scraper_backend.database.write_queue import (
                DatabaseWriteClient,
            )

            self._database_writes = DatabaseWriteClient(self.redis)
        return self._database_writes

    async def _queue_content_save(
        self,
        content: Any,
        *,
        user_id: int,
        content_type: str,
        auth_user_id: int,
        site_name: str,
    ) -> None:
        """Queue one USAPI content object through the scraper's durable write path."""
        from ultima_scraper_backend.job_system.handlers.scrape.content import (
            save_content_to_db,
        )

        database_writes = self._get_database_writes()
        if database_writes is None:
            logger.warning(
                f"Cannot save {content_type} {content.id}: no database write queue"
            )
            return

        saved, errors = await save_content_to_db(
            [content],
            user_id,
            content_type,
            worker_id="websocket",
            auth_user_id=auth_user_id,
            database_writes=database_writes,
            site=site_name,
        )
        if errors:
            logger.warning(
                f"Saving {content_type} {content.id} from WebSocket failed: {errors}"
            )
        else:
            logger.info(
                f"💾 Saved {content_type} {content.id} from WebSocket (saved={saved})"
            )

    async def _handle_chat_message(
        self,
        message_data: dict[str, Any],
        source_id: Optional[int],
        site_name: str,
    ) -> None:
        """Handle 'api2_chat_message' event by saving the message to the DB.

        Args:
            message_data: Raw chat message from the event (same shape as the chats API)
            source_id: Account that received the message
            site_name: Site name for routing to correct API
        """
        try:
            if site_name != "onlyfans":
                return
            from_user = message_data.get("fromUser") or {}
            if not message_data.get("id") or not from_user.get("id"):
                logger.debug("Chat message without id/fromUser, not saving")
                return

            authed = await self._get_authed_for_account(source_id, site_name)
            if not authed or not has_auth_id(authed):
                return
            auth_user_id = int(authed.id)

            # The chat partner is the sender, unless the account sent it itself.
            partner_data = from_user
            if int(from_user["id"]) == auth_user_id:
                partner_data = message_data.get("toUser") or message_data.get(
                    "withUser"
                )
                if not partner_data or not partner_data.get("id"):
                    logger.debug(
                        f"Outgoing chat message {message_data['id']} has no recipient, not saving"
                    )
                    return

            from ultima_scraper_api.apis.onlyfans.classes.message_model import (
                MessageModel as APIMessageModel,
            )

            partner = authed.resolve_user(partner_data)
            message = APIMessageModel(message_data, partner, cache=False)
            await self._queue_content_save(
                message,
                user_id=int(partner.id),
                content_type="messages",
                auth_user_id=auth_user_id,
                site_name=site_name,
            )
        except Exception as exc:
            logger.error(
                f"Error saving WebSocket chat message {message_data.get('id')}: {exc}",
                exc_info=True,
            )

    async def _handle_post_published(
        self,
        post_data: dict[str, Any],
        source_id: Optional[int],
        username: Optional[str],
        site_name: str,
    ) -> None:
        """Handle 'post_published' event by fetching the post and saving it to the DB.

        Args:
            post_data: Post data from the event (only ids, so the post is fetched)
            source_id: Account that received the notification
            username: Username of the account
            site_name: Site name for routing to correct API
        """
        post_id = post_data.get("id")
        user_id = post_data.get("user_id")
        logger.debug(
            f"📝 Post {post_id} published by user {user_id} (account: {username})"
        )
        try:
            if site_name != "onlyfans" or not post_id or not user_id:
                return
            authed = await self._get_authed_for_account(source_id, site_name)
            if not authed or not has_auth_id(authed):
                return

            author = authed.find_user(int(user_id)) or await authed.get_user(
                int(user_id)
            )
            if not author:
                logger.warning(f"Author {user_id} of published post {post_id} not found")
                return
            post = await author.get_post(int(post_id))
            await self._queue_content_save(
                post,
                user_id=int(author.id),
                content_type="posts",
                auth_user_id=int(authed.id),
                site_name=site_name,
            )
        except Exception as exc:
            logger.error(
                f"Error saving published post {post_id}: {exc}", exc_info=True
            )

    async def _get_authed_for_account(
        self, account_id: Optional[Union[int, str]], site_name: str
    ) -> Optional[
        Any
    ]:  # Returns auth_types (OnlyFansAuthModel | FanslyAuthModel | etc)
        """Get the authenticated API instance for an account on a specific site.

        Args:
            account_id: Account ID to look up (int or string)
            site_name: Site name (required: "onlyfans", "fansly", "loyalfans")

        Returns:
            auth_types instance or None if not found
        """
        if not account_id:
            logger.debug("No account_id provided")
            return None

        # Convert to int
        try:
            account_id_int = int(account_id)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert account_id to int: {account_id}")
            return None

        # Get the site API for this site
        site_api = self._get_site_api(site_name)
        if not site_api:
            logger.warning(
                f"No {site_name} site API available to lookup account {account_id_int}"
            )
            return None

        # Try site API's find_auth method first
        if has_find_auth(site_api):
            result = site_api.find_auth(account_id_int)
            if result:
                logger.debug(
                    f"✓ Found auth for account {account_id_int} on {site_name}"
                )
                return result

        # Fallback: Manual search through auths
        if has_auths(site_api):
            for auth in site_api.auths:
                if has_auth_id(auth) and int(auth.id) == account_id_int:
                    logger.debug(
                        f"✓ Found auth for account {account_id_int} on {site_name} (manual)"
                    )
                    return auth

        logger.warning(f"✗ Auth not found for account {account_id_int} on {site_name}")
        return None

    async def _check_and_add_user(
        self,
        authed: Any,  # Changed from auth_types to Any to fix type error
        user_id: int,
        user_data: Optional[
            Union[dict[str, Any], Any]
        ] = None,  # Changed user_types to Any
        *,
        site_name: str,
    ) -> bool:
        """Check if user exists in database and add if new.

        Args:
            authed: Authenticated API instance
            user_id: User ID to check
            user_data: Optional user data from notification or API (avoids API call)
            site_name: External platform that resolved the user identity

        Returns:
            True if user was new and added, False if already existed
        """
        try:
            # Check if user exists in the authed instance's user list
            if has_subscriptions(authed) and authed.subscriptions:
                existing_users = authed.subscriptions
                # Check if it's a list or dict
                if isinstance(existing_users, dict):
                    if user_id in existing_users or str(user_id) in existing_users:
                        logger.debug(
                            f"User {user_id} already exists in subscriptions (dict)"
                        )
                        return False
                else:
                    # It's a list - check using has_auth_id type guard
                    if any(
                        has_auth_id(u) and str(u.id) == str(user_id)
                        for u in existing_users
                    ):
                        logger.debug(
                            f"User {user_id} already exists in subscriptions (list)"
                        )
                        return False

            # Fetch user info from API - check for get_user method
            if not (hasattr(authed, "get_user") and callable(authed.get_user)):
                logger.warning(
                    f"authed instance has no get_user method, cannot add user {user_id}"
                )
                return False

            logger.info(f"Fetching info for new user {user_id}")
            get_user = cast(Callable[[int], Any], authed.get_user)
            user_info = cast(UserInfoProtocol | None, await get_user(user_id))

            if not user_info:
                logger.warning(f"Could not fetch user info for {user_id}")
                return False

            # Add to authed instance's subscription list (in-memory)
            if has_subscriptions(authed):
                if isinstance(authed.subscriptions, dict):
                    # If it's a dict, use ID as key
                    authed.subscriptions[user_id] = user_info  # type: ignore[index]
                    logger.info(
                        f"✓ Added user {user_info.username} ({user_id}) to subscriptions dict"
                    )
                else:
                    # If it's a list, append
                    authed.subscriptions.append(user_info)  # type: ignore[arg-type]
                    logger.info(
                        f"✓ Added user {user_info.username} ({user_id}) to subscriptions list"
                    )
            else:
                logger.warning("authed instance has no subscriptions attribute")

            # Save to database if we have an engine and authed has id
            if self.engine and has_auth_id(authed):
                await self._save_subscription_to_db(
                    authed_id=authed.id,
                    subscriber_id=user_id,
                    user_info=user_info,
                    site_name=site_name,
                )
            else:
                logger.warning(
                    f"Cannot save to DB: engine={self.engine is not None}, authed.id={has_auth_id(authed)}"
                )

            return True

        except Exception as exc:
            logger.error(f"Error adding user {user_id}: {exc}", exc_info=True)

        return False

    async def _save_subscription_to_db(
        self,
        authed_id: int,
        subscriber_id: int,
        user_info: UserInfoProtocol,
        *,
        site_name: str,
    ) -> None:
        """Save subscription to database when receiving WebSocket 'new_subscriber' event.

        Args:
            authed_id: ID of your authenticated creator account (receiving the subscription)
            subscriber_id: ID of the fan/user who just subscribed to you
            user_info: User info object from API for the subscriber
            site_name: External platform that resolved the user identity

        Schema mapping (when someone subscribes to YOUR creator account):
            SubscriptionModel.user_id = Your creator account ID (authed_id)
            SubscriptionModel.subscriber_id = The fan subscribing to you (subscriber_id)
        """
        try:
            if not self.engine:
                logger.warning("No database engine available")
                return

            # Extract username early for event publishing
            username = user_info.username or ""

            with Session(self.engine) as session:
                # First, ensure the creator exists in users table
                existing_user = session.exec(
                    select(UserModel).where(UserModel.id == subscriber_id)
                ).first()

                if not existing_user:
                    # Extract user data with proper USAPI types
                    credit_balance = user_info.credit_balance
                    balance = 0.0 if credit_balance is None else float(credit_balance)

                    # 'performer' field: call is_performer() method
                    performer = user_info.is_performer()

                    # Create user record for the creator
                    new_user = UserModel(
                        id=subscriber_id,
                        username=username,
                        balance=float(balance),
                        spend=False,
                        performer=performer,
                        favorite=False,
                    )
                    session.add(new_user)
                    logger.debug(f"Created user record for creator {subscriber_id}")

                # Check if subscription already exists
                existing_sub = session.exec(
                    select(SubscriptionModel).where(
                        SubscriptionModel.user_id == authed_id,  # Your creator account
                        SubscriptionModel.subscriber_id == subscriber_id,  # The fan
                    )
                ).first()

                if existing_sub:
                    # Update existing subscription
                    existing_sub.active = True
                    existing_sub.expires_at = datetime.now(timezone.utc) + timedelta(
                        days=DEFAULT_SUBSCRIPTION_DAYS
                    )
                    logger.debug(
                        f"Updated existing subscription for fan {subscriber_id} -> creator {authed_id}"
                    )
                else:
                    # Create new subscription
                    # ✓ FIXED: Correct mapping for WebSocket new_subscriber event
                    # - user_id = Your creator account (authed_id)
                    # - subscriber_id = The fan subscribing to you (subscriber_id)
                    new_sub = SubscriptionModel(
                        user_id=authed_id,  # Your creator account (being subscribed to)
                        subscriber_id=subscriber_id,  # The fan subscribing to you
                        paid_content=True,  # Assume paid unless we know otherwise
                        active=True,
                        expires_at=datetime.now(timezone.utc)
                        + timedelta(days=DEFAULT_SUBSCRIPTION_DAYS),
                    )
                    session.add(new_sub)
                    logger.info(
                        f"💾 Saved WebSocket subscription to DB: fan {subscriber_id} -> creator {authed_id}"
                    )

                session.commit()

                # Notify subscribed clients that subscriptions changed.
                if self.redis:
                    try:
                        event = {
                            "event": "subscription_added",
                            "auth_id": authed_id,
                            "subscriber_id": subscriber_id,
                            "username": username,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        await self.redis.publish(HOOKS_CHANNEL, json.dumps(event))
                        logger.debug(
                            f"Published subscription_added event for {username}"
                        )
                    except Exception as pub_exc:
                        logger.warning(
                            f"Failed to publish subscription_added event: {pub_exc}"
                        )

        except Exception as exc:
            logger.error(f"Error saving subscription to database: {exc}", exc_info=True)

    async def _save_toast_subscription_to_db(
        self,
        authed_id: int,
        related_user: dict[str, Any],
        subscribed_by: bool,
        subscribed_on: bool,
        *,
        site_name: str,
    ) -> None:
        """Save subscription from toast notification to database.

        Toast events have bidirectional subscription info in relatedUser:
        - subscribedBy: true = YOU (authed) are subscribed TO them (they're a performer)
        - subscribedOn: true = THEY are subscribed TO you (you're a performer)

        Args:
            authed_id: Your authenticated account ID
            related_user: relatedUser object from toast data
            subscribed_by: Whether authed is subscribed to this user
            subscribed_on: Whether this user is subscribed to authed
            site_name: External platform that emitted the toast
        """
        try:
            if not self.engine:
                logger.warning("No database engine available")
                return

            subscriber_id = related_user.get("id")
            username = related_user.get("username")

            if not subscriber_id:
                logger.warning("Toast relatedUser missing id")
                return

            with Session(self.engine) as session:
                # Ensure user exists in users table
                existing_user = session.exec(
                    select(UserModel).where(UserModel.id == subscriber_id)
                ).first()

                if not existing_user:
                    # Create user record
                    new_user = UserModel(
                        id=subscriber_id,
                        username=username,
                        balance=0.0,
                        spend=False,
                        performer=True,  # If they show up in toast, they're probably a performer
                        favorite=False,
                    )
                    session.add(new_user)
                    logger.debug(
                        f"Created user record for {username} ({subscriber_id})"
                    )

                # Handle subscribedBy: YOU subscribed TO them
                if subscribed_by:
                    existing_sub = session.exec(
                        select(SubscriptionModel).where(
                            SubscriptionModel.user_id
                            == subscriber_id,  # They're the performer
                            SubscriptionModel.subscriber_id
                            == authed_id,  # You're the subscriber
                        )
                    ).first()

                    if not existing_sub:
                        new_sub = SubscriptionModel(
                            user_id=subscriber_id,  # The performer you're subscribed to
                            subscriber_id=authed_id,  # You (the subscriber)
                            paid_content=True,
                            active=True,
                            expires_at=datetime.now(timezone.utc)
                            + timedelta(days=DEFAULT_SUBSCRIPTION_DAYS),
                        )
                        session.add(new_sub)
                        logger.info(
                            f"💾 Saved toast subscription: authed {authed_id} -> performer {username} ({subscriber_id})"
                        )
                    elif _renew_lapsed_subscription(existing_sub):
                        logger.info(
                            f"💾 Renewed lapsed toast subscription: authed -> {username}"
                        )

                # Handle subscribedOn: THEY subscribed TO you
                if subscribed_on:
                    existing_sub = session.exec(
                        select(SubscriptionModel).where(
                            SubscriptionModel.user_id
                            == authed_id,  # You're the performer
                            SubscriptionModel.subscriber_id
                            == subscriber_id,  # They're the subscriber
                        )
                    ).first()

                    if not existing_sub:
                        new_sub = SubscriptionModel(
                            user_id=authed_id,  # You (the performer)
                            subscriber_id=subscriber_id,  # They (the subscriber)
                            paid_content=True,
                            active=True,
                            expires_at=datetime.now(timezone.utc)
                            + timedelta(days=DEFAULT_SUBSCRIPTION_DAYS),
                        )
                        session.add(new_sub)
                        logger.info(
                            f"💾 Saved toast subscription: {username} ({subscriber_id}) -> authed (performer) {authed_id}"
                        )
                    elif _renew_lapsed_subscription(existing_sub):
                        logger.info(
                            f"💾 Renewed lapsed toast subscription: {username} -> authed"
                        )

                session.commit()

                # Notify subscribed clients.
                if self.redis and (subscribed_by or subscribed_on):
                    try:
                        event = {
                            "event": "subscription_added",
                            "auth_id": authed_id,
                            "subscriber_id": subscriber_id,
                            "username": username,
                            "subscribed_by": subscribed_by,
                            "subscribed_on": subscribed_on,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        await self.redis.publish(HOOKS_CHANNEL, json.dumps(event))
                        logger.debug(
                            f"Published subscription_added event for {username}"
                        )
                    except Exception as pub_exc:
                        logger.warning(
                            f"Failed to publish subscription_added event: {pub_exc}"
                        )

        except Exception as exc:
            logger.error(
                f"Error saving toast subscription to database: {exc}", exc_info=True
            )


async def create_websocket_action_handler(
    backend_or_worker: Union[BackendManager, Worker],
) -> WebSocketActionHandler:
    """Create and initialize a WebSocket action handler.

    Args:
        backend_or_worker: Backend manager or worker instance

    Returns:
        Initialized WebSocketActionHandler
    """
    return WebSocketActionHandler(backend_or_worker)
