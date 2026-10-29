"""
Backend USAPI Initialization with Database Models

This module handles the initialization of USAPI (UltimaScraper API) instances
on backend systems by loading existing database data into API models. This allows
backends to use cached data without repeated database queries and keeps
subscription counts synchronized with the API.

Key Responsibilities:
1. Load database records (auths, users, subscriptions) into USAPI models
2. Attach populated models to USAPI instances
3. Contact the API to refresh subscriber counts
4. Update subscription counts in the database if changes are detected
5. Ensure thread-safe and consistent updates
6. Register WebSocket action handler for real-time event processing
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import quote, urlparse, urlunparse

from sqlalchemy import Engine
from sqlmodel import Session, select
from ultima_scraper_api.apis.onlyfans.classes.auth_model import OnlyFansAuthModel
from ultima_scraper_api.apis.onlyfans.classes.subscription_model import (
    SubscriptionModel as APISubscriptionModel,
)
from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    UserModel as APIUserModel,
)
from ultima_scraper_api.apis.onlyfans.onlyfans import OnlyFansAPI

from ultima_scraper_db_archive import (
    SubscriptionModel as DBSubscriptionModel,
)
from ultima_scraper_db_archive import UserInfoModel, UserModel

if TYPE_CHECKING:
    from ultima_scraper_backend.redis.client import RedisManager
    from ultima_scraper_backend.websocket_actions import WebSocketActionHandler

logger = logging.getLogger(__name__)


class AssignedProxyResolutionError(ValueError):
    """Raised when an assigned proxy identifier cannot be resolved."""


def resolve_proxy_url(proxy_id: str | None, *, config=None) -> str | None:
    """Resolve a configured proxy identifier to a usable proxy URL.

    The current config stores proxies under ``settings.network.proxies`` and the
    persisted auth records often already use the proxy URL as the identifier.
    This helper accepts either form:
    - direct proxy URL: returned as-is
    - configured proxy key/url match: returns the configured URL
    """

    raw = (proxy_id or "").strip()
    if not raw:
        return None

    try:
        from .config import UltimaScraperCollectionConfig

        if config is None:
            config = UltimaScraperCollectionConfig.load_or_create_default_config()
        for proxy in getattr(config.settings.network, "proxies", []):
            proxy_url = str(getattr(proxy, "url", "") or "").strip()
            if not proxy_url:
                continue
            if raw == proxy_url:
                if not getattr(proxy, "enabled", True):
                    return None
                parsed_proxy = urlparse(proxy_url)
                username = getattr(proxy, "username", None)
                password = getattr(proxy, "password", None)
                if not username:
                    return proxy_url
                host = parsed_proxy.hostname or ""
                if ":" in host and not host.startswith("["):
                    host = f"[{host}]"
                port = f":{parsed_proxy.port}" if parsed_proxy.port else ""
                credentials = quote(str(username), safe="")
                if password is not None:
                    credentials += f":{quote(str(password), safe='')}"
                return urlunparse(
                    (
                        parsed_proxy.scheme,
                        f"{credentials}@{host}{port}",
                        parsed_proxy.path,
                        parsed_proxy.params,
                        parsed_proxy.query,
                        parsed_proxy.fragment,
                    )
                )
    except Exception:
        logger.debug(
            "Failed to resolve proxy identifier '%s' from config", raw, exc_info=True
        )

    parsed = urlparse(raw)
    return raw if parsed.scheme and parsed.netloc else None


def resolve_assigned_proxy_url(proxy_id: str | None) -> str | None:
    """Resolve an assigned proxy identifier or raise when the config is stale."""

    raw = (proxy_id or "").strip()
    if not raw:
        return None

    resolved = resolve_proxy_url(raw)
    if resolved:
        return resolved

    raise AssignedProxyResolutionError(
        f"Assigned proxy '{raw}' is disabled or was not found in the current configuration"
    )


def apply_assigned_proxy_to_auth_json(
    auth_json: dict[str, Any], proxy_id: str | None
) -> dict[str, Any]:
    """Attach the resolved assigned proxy URL to an auth payload when configured."""

    proxy_url = resolve_assigned_proxy_url(proxy_id)
    if proxy_url:
        auth_json["proxy_url"] = proxy_url
    return auth_json


def is_auth_in_shard(auth_id: int, worker_index: int, worker_count: int) -> bool:
    """
    Determine if an auth belongs to a worker's shard.

    Uses modulo-based distribution: auth_id % worker_count == worker_index
    This ensures even distribution and consistent assignment.

    Args:
        auth_id: OnlyFans account ID (user_id from auth_json)
        worker_index: Worker index (0-based)
        worker_count: Total number of workers

    Returns:
        True if this auth belongs to the worker's shard

    Example:
        With 5 workers (0-4) and auth_id=12345:
        12345 % 5 = 0  → belongs to worker 0
    """
    # Ensure all parameters are integers to avoid string formatting errors
    auth_id = int(auth_id)
    worker_count = int(worker_count)
    worker_index = int(worker_index)
    return (auth_id % worker_count) == worker_index


class USAPIInitializer:
    """
    Manages USAPI initialization with database models.

    Thread Safety:
    - Uses asyncio.Lock to prevent concurrent initialization attempts
    - Lock is held for the entire initialize_auth() workflow
    - Internal methods (_sync_subscriptions_to_db) do NOT acquire locks
      to avoid deadlocks, as they're only called from within locked contexts
    """

    def __init__(
        self,
        engine: Engine,
        ws_action_handler: Optional[WebSocketActionHandler] = None,
        redis_manager: Optional[RedisManager] = None,
    ) -> None:
        """
        Initialize the USAPI loader with a database engine.

        Args:
            engine: SQLModel database engine for querying records
            ws_action_handler: Optional WebSocket action handler for event processing
            redis_manager: Optional RedisManager for publishing events
        """
        self.engine = engine
        self._lock = asyncio.Lock()
        self._ws_action_handler = ws_action_handler
        self._redis_manager = redis_manager

    async def initialize_auth(
        self,
        site: str,
        account_id: int,
        api_instance: OnlyFansAPI,
        auth_json: dict[str, Any],
        skip_api_refresh: bool = False,
    ) -> Optional[OnlyFansAuthModel]:
        """
        Initialize an OnlyFansAuthModel with database data and optionally refresh subscriptions.

        This method:
        1. Logs into the API using provided credentials
        2. Loads existing subscriptions from the database
        3. Attaches them to the OnlyFansAuthModel
        4. Optionally contacts the API to get current subscriber counts (if skip_api_refresh=False)
        5. Updates the database if counts have changed

        Args:
            site: Site name (e.g., 'onlyfans')
            account_id: User account ID
            api_instance: OnlyFans API instance to use for login
            auth_json: Authentication credentials dictionary
            skip_api_refresh: If True, skip API subscription count check (default: False)

        Returns:
            Initialized OnlyFansAuthModel with database data loaded, or None if login fails
        """
        if site.lower() != "onlyfans":
            logger.warning("USAPIInitializer only supports OnlyFans currently")
            return None

        async with self._lock:
            try:
                # Step 1: Login to API
                authed = await api_instance.login(auth_json=auth_json)
                if not authed:
                    logger.error(
                        f"Failed to login account {account_id} for USAPI initialization"
                    )
                    return None

                # Register WebSocket action handler if available
                if (
                    self._ws_action_handler
                    and hasattr(authed, "websocket_connection")
                    and authed.websocket_connection
                ):
                    try:
                        ws_action_handler = self._ws_action_handler
                        # Create callback wrapper for this authed instance
                        # Capture account_id, username, and site in closure
                        _account_id = account_id
                        _username = authed.username
                        _site = site.lower()

                        async def ws_callback(message: dict[str, Any]) -> None:
                            """Process WebSocket message through action handler."""
                            try:
                                # Add source_id and username for routing
                                # Format: "{site}:ws:{account_id}" to match WebSocketStorage format
                                enriched_message = {
                                    **message,
                                    "source_id": f"{_site}:ws:{_account_id}",
                                    "username": _username,
                                }
                                await ws_action_handler.process_message(
                                    enriched_message
                                )
                            except Exception as exc:
                                logger.error(
                                    f"Error processing WebSocket message: {exc}",
                                    exc_info=True,
                                )

                        # Use new .on() method instead of register_callback()
                        authed.websocket_connection.on("message", ws_callback)
                        logger.info(
                            f"Registered WebSocket action handler for account {account_id}"
                        )
                    except Exception as ws_exc:
                        logger.warning(
                            f"Failed to register WebSocket handler: {ws_exc}"
                        )

                # Start WebSocket connection after callbacks are attached so
                # the background listen loop cannot emit early messages before
                # the handler is registered.
                await authed.websocket_connection.start()

                logger.info(
                    f"Logged in account {account_id} ({authed.username}) for initialization"
                )

                # Step 2: Load database subscriptions
                db_subscriptions = self._load_db_subscriptions(account_id)
                logger.info(
                    f"Loaded {len(db_subscriptions)} subscriptions from database for account {account_id}"
                )

                # Step 3: Convert DB subscriptions to API models and attach to authed
                api_subscriptions: list[APISubscriptionModel] = []
                for db_sub in db_subscriptions:
                    try:
                        api_sub = self._convert_db_to_api_subscription(
                            db_sub, authed, authed
                        )
                        if api_sub:
                            api_subscriptions.append(api_sub)
                    except Exception as exc:
                        logger.warning(
                            f"Failed to convert subscription {db_sub.id}: {exc}"
                        )
                        continue

                # Attach subscriptions to the auth model
                authed.subscriptions = api_subscriptions
                logger.info(
                    f"Attached {len(api_subscriptions)} subscriptions to OnlyFansAuthModel"
                )

                return authed

            except Exception as exc:
                logger.exception(
                    f"Failed to initialize USAPI for account {account_id}: {exc}"
                )
                return None

    def _load_db_subscriptions(self, subscriber_id: int) -> list[DBSubscriptionModel]:
        """
        Load all active subscriptions from the database for a given subscriber.

        Args:
            subscriber_id: ID of the subscriber account

        Returns:
            List of database subscription models
        """
        with Session(self.engine) as session:
            subscriptions = list(
                session.exec(
                    select(DBSubscriptionModel).where(
                        DBSubscriptionModel.subscriber_id == subscriber_id
                    )
                )
            )
            return subscriptions

    def _convert_db_to_api_subscription(
        self,
        db_sub: DBSubscriptionModel,
        authed: OnlyFansAuthModel,
        subscriber: OnlyFansAuthModel,
    ) -> Optional[APISubscriptionModel]:
        """
        Convert a database subscription model to an API subscription model.

        This creates the necessary mock API response data to instantiate
        an API subscription model from database records.

        Args:
            db_sub: Database subscription model
            authed: Authenticated OnlyFans auth model
            subscriber: Subscriber auth model (typically same as authed)

        Returns:
            API subscription model or None if conversion fails
        """
        try:
            # Load user data from database
            with Session(self.engine) as session:
                user = session.get(UserModel, db_sub.user_id)
                if not user:
                    logger.warning(
                        f"User {db_sub.user_id} not found for subscription {db_sub.id}"
                    )
                    return None

                user_info = session.exec(
                    select(UserInfoModel).where(
                        UserInfoModel.user_id == db_sub.user_id
                    )
                ).first()

                # Create mock API response data for subscription
                # This mimics what the OnlyFans API would return
                expires_at = db_sub.expires_at
                subscription_data: dict[str, Any] = {
                    "id": db_sub.user_id,
                    "username": user.username or f"u{db_sub.user_id}",
                    "name": user.username or f"u{db_sub.user_id}",
                    "postsCount": (
                        max(0, int(user_info.post_count or 0))
                        if user_info is not None
                        else 0
                    ),
                    "subscribedBy": db_sub.active,
                    "subscribedByData": {},
                    "subscribedByExpire": True,
                    "subscribedByExpireDate": (
                        expires_at.isoformat()
                        if expires_at is not None
                        else datetime.now(timezone.utc).isoformat()
                    ),
                    "subscribedByAutoprolong": False,
                    "subscribedIsExpiredNow": not db_sub.active,
                    "currentSubscribePrice": 0,
                    "subscribePrice": 0,
                    "subscribedOn": False,
                    "subscribedOnData": {},
                    "subscribedOnExpiredNow": True,
                    "subscribedOnDuration": "P0Y0M0DT0H0M",  # ISO 8601 duration format
                    "canPromotion": False,
                    "canReport": True,
                }

                # Create API user model
                api_user = APIUserModel(subscription_data, authed)

                # Create API subscription model
                api_subscription = APISubscriptionModel(
                    subscription_data, api_user, subscriber
                )

                return api_subscription

        except Exception as exc:
            logger.exception(f"Failed to convert subscription {db_sub.id}: {exc}")
            return None

    async def create_cookie_auth(
        self,
        site: str,
        account_id: int,
        api_instance: OnlyFansAPI,
    ) -> Optional[OnlyFansAuthModel]:
        """
        Create an auth model from database cookies WITHOUT calling the API login endpoint.

        **Use Case: Lazy Auth Loading for Non-Shard Jobs**
        When a worker receives a job for an auth that doesn't belong to its shard,
        it can use this method to create an auth instance using stored cookies.
        This avoids unnecessary API login calls while still allowing job execution.

        **Key Differences from initialize_auth():**
        - Does NOT call api_instance.login() (no API request)
        - Does NOT enable WebSockets
        - Does NOT load subscriptions from database
        - Uses cookies directly to construct auth model
        - Much faster (no network I/O)

        **When to Use:**
        - Job arrives for non-shard auth (not loaded during worker init)
        - Need to execute job with minimal overhead
        - Don't need subscriber lists or WebSocket events

        Args:
            site: Site name (e.g., 'onlyfans')
            account_id: OnlyFans account ID (user_id)
            api_instance: OnlyFans API instance to attach auth to

        Returns:
            OnlyFansAuthModel if auth was created successfully, None otherwise

        Example:
            ```python
            # Job arrives for auth 67890, but worker shard only has 12345
            auth = api.find_auth(67890)
            if not auth:
                # Lazy load with cookie - no API call!
                auth = await initializer.create_cookie_auth(
                    site="onlyfans",
                    account_id=67890,
                    api_instance=api
                )

            # Now execute job with the auth
            await execute_job_task(job, auth)
            ```
        """
        if site.lower() != "onlyfans":
            logger.warning(f"Cookie auth only supports OnlyFans, got: {site}")
            return None

        from ultima_scraper_db_archive import UserAuthModel

        # Load auth data from database
        with Session(self.engine) as session:
            stmt = select(UserAuthModel).where(
                UserAuthModel.user_id == account_id, UserAuthModel.site == site.lower()
            )
            account = session.exec(stmt).first()

            if not account:
                logger.warning(
                    f"No auth found in database for account_id={account_id}, site={site}"
                )
                return None

            # Detach from session
            session.expunge(account)

        # Build auth_json from database cookies
        auth_json = {
            "id": account.user_id,
            "username": account.username or "",
            "cookie": account.cookie or "",
            "x_bc": account.x_bc or "",
            "user_agent": account.user_agent or "",
            "email": account.email or "",
            "password": account.password or "",
        }
        auth_json = apply_assigned_proxy_to_auth_json(auth_json, account.proxy_id)

        # Use the API's login method to create/register the auth, but bypass
        # live auth validation for lazy download tasks that only need stored cookies.
        try:
            authed = await api_instance.login(
                auth_json=auth_json,
                ignore_auth_errors=True,
            )

            if not authed:
                logger.warning(
                    f"Failed to create auth for account {account_id} using stored cookies"
                )
                return None

            logger.info(
                f"Created cookie-based auth for account {account_id} "
                f"({account.username}) - Using stored cookies"
            )
            return authed

        except Exception as exc:
            logger.error(
                f"Failed to create cookie auth for account {account_id}: {exc}",
                exc_info=True,
            )
            return None

    async def _sync_subscriptions_to_db(
        self,
        subscriber_id: int,
        api_subscriptions: list[APISubscriptionModel],
        authed: OnlyFansAuthModel,
    ) -> None:
        """
        Synchronize API subscriptions to the database.

        This method ensures the database reflects the current state from the API.
        It creates new subscription records, updates existing ones, and marks
        inactive subscriptions.

        Note: This method should only be called from within initialize_auth(),
        which already holds self._lock. Do not call directly without locking.

        Args:
            subscriber_id: ID of the subscriber account
            api_subscriptions: List of subscriptions from the API
            authed: Authenticated OnlyFans auth model
        """
        with Session(self.engine) as session:
            # Load existing subscriptions from database
            existing_subs = {
                sub.user_id: sub
                for sub in session.exec(
                    select(DBSubscriptionModel).where(
                        DBSubscriptionModel.subscriber_id == subscriber_id
                    )
                )
            }

            # Track which subscriptions we've seen from API
            seen_user_ids: set[int] = set()

            # Update or create subscriptions from API
            for api_sub in api_subscriptions:
                user_id = int(api_sub.user.id)
                seen_user_ids.add(user_id)

                # Get or create user record
                user = session.get(UserModel, user_id)
                if not user:
                    user = UserModel(
                        id=user_id,
                    )
                    if api_sub.user.username:
                        user.username = api_sub.user.username
                    user.active = True
                    session.add(user)
                else:
                    # Update username if changed
                    if api_sub.user.username and user.username != api_sub.user.username:
                        user.username = api_sub.user.username
                        session.add(user)

                # Get or create subscription record
                db_sub = existing_subs.get(user_id)
                expires_at = api_sub.resolve_expires_at()

                if db_sub:
                    # Update existing subscription
                    changed = False
                    if db_sub.active != api_sub.is_active():
                        db_sub.active = api_sub.is_active()
                        changed = True
                    if db_sub.expires_at != expires_at:
                        db_sub.expires_at = expires_at
                        changed = True

                    if changed:
                        session.add(db_sub)
                        logger.debug(
                            f"Updated subscription for user {user_id} (subscriber {subscriber_id})"
                        )
                else:
                    # Create new subscription - id will be auto-generated by database
                    new_sub = DBSubscriptionModel(
                        user_id=user_id,
                        subscriber_id=subscriber_id,
                        active=api_sub.is_active(),
                        expires_at=expires_at,
                    )
                    session.add(new_sub)
                    # Note: ID will be generated on commit, no need to flush for each record
                    logger.debug(
                        f"Queued new subscription for user {user_id} (subscriber {subscriber_id})"
                    )

            # Mark subscriptions not in API response as inactive
            inactive_count = 0
            for user_id, db_sub in existing_subs.items():
                if user_id not in seen_user_ids and db_sub.active:
                    db_sub.active = False
                    session.add(db_sub)
                    inactive_count += 1
                    logger.debug(
                        f"Marked subscription inactive for user {user_id} (subscriber {subscriber_id})"
                    )

            session.commit()
            logger.info(
                f"Successfully synced {len(api_subscriptions)} subscriptions to database "
                f"for subscriber {subscriber_id} ({inactive_count} marked inactive)"
            )

            # Publish completion for subscribed clients.
            if self._redis_manager:
                try:
                    from .redis_channels import HOOKS_CHANNEL

                    await self._redis_manager.publish(
                        HOOKS_CHANNEL,
                        json.dumps(
                            {
                                "event": "subscription_sync_complete",
                                "auth_id": subscriber_id,
                                "username": authed.username,
                                "total": len(api_subscriptions),
                                "inactive_count": inactive_count,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                        ),
                    )
                    logger.debug(
                        f"Published subscription_sync_complete event for auth {subscriber_id}"
                    )
                except Exception as e:
                    logger.debug(f"Could not publish sync completion event: {e}")


async def initialize_usapi_with_db(
    engine: Engine,
    site: str,
    account_id: int,
    api_instance: OnlyFansAPI,
    auth_json: dict[str, Any],
    skip_api_refresh: bool = False,
    ws_action_handler: Optional[WebSocketActionHandler] = None,
    redis_manager: Optional[RedisManager] = None,
) -> Optional[OnlyFansAuthModel]:
    """
    Convenience function to initialize USAPI with database models.

    This is the main entry point for backend initialization. It handles the complete
    workflow of loading database data into USAPI models and optionally synchronizing with the API.

    Args:
        engine: SQLModel database engine
        site: Site name (e.g., 'onlyfans')
        account_id: User account ID
        api_instance: OnlyFans API instance
        auth_json: Authentication credentials dictionary
        skip_api_refresh: If True, skip API subscription count check (default: False)
        ws_action_handler: Optional WebSocket action handler for event processing
        redis_manager: Optional RedisManager for publishing events

    Returns:
        Initialized OnlyFansAuthModel or None if initialization fails

    Example:
        ```python
        from sqlmodel import create_engine
        import ultima_scraper_api
        from ultima_scraper_backend.usapi_initializer import initialize_usapi_with_db

        engine = create_engine("sqlite:///path/to/db.db")
        api = ultima_scraper_api.select_api("onlyfans", UltimaScraperAPIConfig())

        auth_json = {
            "id": 12345,
            "username": "user",
            "cookie": "...",
            "x_bc": "...",
            "user_agent": "...",
        }

        # During worker init - skip API refresh
        authed = await initialize_usapi_with_db(
            engine=engine,
            site="onlyfans",
            account_id=12345,
            api_instance=api,
            auth_json=auth_json,
            skip_api_refresh=True,
        )

        if authed:
            # Use authed.subscriptions without additional DB queries
            for sub in authed.subscriptions:
                logger.debug(f"Subscriber: {sub.user.username}")
        ```
    """
    initializer = USAPIInitializer(
        engine, ws_action_handler=ws_action_handler, redis_manager=redis_manager
    )
    return await initializer.initialize_auth(
        site, account_id, api_instance, auth_json, skip_api_refresh=skip_api_refresh
    )


async def load_all_accounts_to_api(
    api_instance: OnlyFansAPI,
    engine: Engine,
    site: str,
    skip_api_refresh: bool = True,
    ws_action_handler: Optional[WebSocketActionHandler] = None,
    redis_manager: Optional[RedisManager] = None,
    worker_index: Optional[int] = None,
    worker_count: Optional[int] = None,
) -> list[OnlyFansAuthModel]:
    """
    Load authenticated accounts from the database into the API instance.

    **Shard-Based Loading (Option B - Hybrid Architecture):**
    When worker_index and worker_count are provided, this function only loads
    accounts that belong to this worker's shard (auth_id % worker_count == worker_index).
    These shard accounts get FULL initialization with WebSockets enabled for real-time events.

    Non-shard accounts are NOT loaded here - they will be lazy-loaded on-demand when
    jobs arrive using cookie-based auth (no API login call, no WebSocket).

    This function queries the database for active user_auths records for the given
    site and initializes each one into the API's auths collection.

    Args:
        api_instance: OnlyFans API instance to populate with auths
        engine: SQLModel database engine
        site: Site name (e.g., 'onlyfans')
        skip_api_refresh: If True, skip API subscription count check (default: False)
        ws_action_handler: Optional WebSocket action handler for real-time events
        redis_manager: Optional RedisManager for publishing events
        worker_index: Worker index (0-based) for shard calculation. If provided with worker_count,
                      only loads accounts in this worker's shard
        worker_count: Total number of workers. Required if worker_index is provided

    Returns:
        List of initialized OnlyFansAuthModel instances (shard only if sharding enabled)

    Example (Full Load - Legacy):
        ```python
        from sqlmodel import create_engine
        import ultima_scraper_api
        from ultima_scraper_backend.usapi_initializer import load_all_accounts_to_api

        engine = create_engine("sqlite:///path/to/db.db")
        api = ultima_scraper_api.select_api("onlyfans", UltimaScraperAPIConfig())

        # During worker init - skip API refresh
        auths = await load_all_accounts_to_api(api, engine, skip_api_refresh=True)
        print(f"Loaded {len(auths)} authenticated accounts")
        ```
    """
    from ultima_scraper_db_archive import UserAuthModel

    # Validate shard parameters
    if (worker_index is not None) != (worker_count is not None):
        raise ValueError(
            "worker_index and worker_count must both be provided or both be None"
        )

    if worker_index is not None and worker_count is not None:
        if worker_index < 0 or worker_index >= worker_count:
            raise ValueError(f"worker_index must be between 0 and {worker_count - 1}")
        if worker_count <= 0:
            raise ValueError("worker_count must be positive")

    with Session(engine) as session:
        # Query active auths only and let the database apply shard filtering.
        stmt = select(UserAuthModel).where(
            UserAuthModel.site == site.lower(),
            UserAuthModel.active.is_(True),
        )
        if worker_index is not None and worker_count is not None:
            stmt = stmt.where((UserAuthModel.user_id % worker_count) == worker_index)
        accounts = list(session.exec(stmt))

        # Detach from session to avoid lazy loading issues
        for acc in accounts:
            session.expunge(acc)

    if not accounts:
        if worker_index is not None and worker_count is not None:
            logger.info(
                f"Shard mode: Worker {worker_index}/{worker_count - 1} found no active accounts"
            )
        else:
            logger.warning(
                f"No active authenticated accounts found in database for site '{site}'"
            )
        return []

    if worker_index is not None and worker_count is not None:
        logger.info(
            f"Shard mode: Worker {worker_index}/{worker_count - 1} loading "
            f"{len(accounts)} active account(s) (shard only)"
        )
    else:
        logger.info(f"Legacy mode: Loading all {len(accounts)} active account(s)")

    if not accounts:
        logger.info("No accounts in this worker's shard")
        return []

    logger.info(f"Found {len(accounts)} account(s) to initialize for site '{site}'")

    # Initialize each account with WebSocket handler if provided
    authed_models: list[OnlyFansAuthModel] = []
    initializer = USAPIInitializer(
        engine, ws_action_handler=ws_action_handler, redis_manager=redis_manager
    )

    for account in accounts:
        try:
            # Build site-specific auth_json
            # OnlyFans uses: cookie, x_bc
            # Fansly uses: authorization
            # LoyalFans uses: similar to OnlyFans

            if site.lower() == "onlyfans":
                auth_json = {
                    "id": account.user_id,  # Use user_id, not account.id
                    "username": account.username or "",
                    "cookie": account.cookie or "",
                    "x_bc": account.x_bc or "",
                    "user_agent": account.user_agent or "",
                    "email": account.email or "",
                    "password": account.password or "",
                }
            elif site.lower() == "fansly":
                auth_json = {
                    "id": account.user_id,  # Use user_id, not account.id
                    "username": account.username or "",
                    "authorization": account.authorization or "",
                    "user_agent": account.user_agent or "",
                    "email": account.email or "",
                    "password": account.password or "",
                }
            else:
                # Default/LoyalFans format (similar to OnlyFans)
                auth_json = {
                    "id": account.user_id,
                    "username": account.username or "",
                    "cookie": account.cookie or "",
                    "x_bc": account.x_bc or "",
                    "user_agent": account.user_agent or "",
                    "email": account.email or "",
                    "password": account.password or "",
                }

            auth_json = apply_assigned_proxy_to_auth_json(
                auth_json, getattr(account, "proxy_id", None)
            )

            # Initialize this auth
            authed = await initializer.initialize_auth(
                site=site,
                account_id=account.user_id,  # Use user_id (OF account ID), not DB PK
                api_instance=api_instance,
                auth_json=auth_json,
                skip_api_refresh=skip_api_refresh,
            )

            if authed:
                authed_models.append(authed)
                logger.info(f"Initialized account: {account.username or account.id}")
            else:
                logger.warning(
                    f"Failed to initialize account: {account.username or account.id}"
                )

        except Exception as exc:
            logger.error(
                f"Error initializing account {account.username or account.id}: {exc}",
                exc_info=True,
            )
            continue

    logger.info(
        f"Successfully initialized {len(authed_models)}/{len(accounts)} account(s)"
    )
    return authed_models
