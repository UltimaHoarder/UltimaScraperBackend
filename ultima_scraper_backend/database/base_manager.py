"""Base Database Manager - Abstract interface for database operations.

This module defines the base class for all database managers, providing a common
interface for operations across different database backends (SQLite, PostgreSQL).
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlalchemy import func, select as sa_select
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from ultima_scraper_db_archive.site import utcnow

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from ultima_scraper_db_archive import (
        SubscriptionModel,
        UserAuthModel,
        UserModel,
    )

logger = logging.getLogger(__name__)


class BaseDatabaseManager(ABC):
    """Abstract base class for database managers.

    This class provides a common interface for managing site-specific databases
    across different backend systems (SQLite, PostgreSQL). Each implementation
    handles connection management, session lifecycle, and CRUD operations for
    user data.

    Attributes:
        site: The site name (e.g., 'onlyfans', 'fansly')
        engine: The SQLAlchemy engine for database connections
        db_path: Optional path to the database file (mainly for SQLite)
    """

    # Mapping of database table names to content type names
    # This serves as the single source of truth for table-to-content-type conversion
    TABLENAME_TO_CONTENT_TYPE: dict[str, str] = {
        "x_posts": "Posts",
        "x_messages": "Messages",
        "x_stories": "Stories",
        "x_mass_messages": "MassMessages",
        "user_profile_media": "Profile",
    }

    def __init__(self, site: str, engine: Optional[Engine] = None):
        """Initialize the database manager.

        Args:
            site: The site name (e.g., 'onlyfans', 'fansly', 'loyalfans')
            engine: Optional pre-configured SQLAlchemy engine
        """
        self.site = site.lower()
        self.engine = engine
        self.db_path: Optional[Path] = None
        self._is_connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish database connection.

        Creates the engine and initializes tables if needed.
        Should be called before any database operations.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection and cleanup resources."""
        pass

    def get_database_stats(self) -> dict[str, Any]:
        """Return basic database stats for compatibility callers."""
        return {"site": self.site}

    @abstractmethod
    def get_session(self) -> Session:
        """Get a new database session.

        Returns:
            A SQLModel Session instance for database operations.
        """
        pass

    def ensure_connected(self) -> None:
        """Ensure database connection is established.

        Raises:
            RuntimeError: If not connected to database.
        """
        if not self.is_connected:
            raise RuntimeError(
                f"Database manager for '{self.site}' is not connected. "
                "Call connect() first."
            )

    @property
    def is_connected(self) -> bool:
        """Return whether the manager has a usable initialized engine."""

        return bool(self._is_connected and self.engine is not None)

    @staticmethod
    def _is_postgres_session(session: Session) -> bool:
        """Return True when the session is bound to PostgreSQL."""
        bind = session.get_bind()
        return bool(bind and bind.dialect.name == "postgresql")

    @staticmethod
    def _commit_or_flush(session: Session, owns_session: bool) -> None:
        """Commit when owning the session, otherwise just flush."""
        if owns_session:
            session.commit()
        else:
            session.flush()

    @staticmethod
    def _rollback_if_owned(session: Session, owns_session: bool) -> None:
        """Roll back only when this method created the session."""
        if owns_session:
            session.rollback()

    @staticmethod
    def _refresh_safe(session: Session, instance: Any) -> None:
        """Refresh instance when present, swallowing detached-instance errors."""
        if instance is None:
            return
        try:
            session.refresh(instance)
        except Exception:
            return

    @staticmethod
    def _duration_ms(start: float) -> float:
        return (time.perf_counter() - start) * 1000

    # ========================
    # User Operations
    # ========================

    def get_user(
        self, identifier: int | str, session: Optional[Session] = None
    ) -> Optional[UserModel]:
        """Get a user by ID.

        Args:
            user_id: The user ID to retrieve
            session: Optional session to use (creates new one if not provided)

        Returns:
            UserModel instance or None if not found
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import UserModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            if isinstance(identifier, int):
                stmt = select(UserModel).where(UserModel.id == identifier)
            else:
                # The lower() term lets idx_users_username_lower find the row.
                stmt = select(UserModel).where(
                    func.lower(UserModel.username) == func.lower(identifier),
                    UserModel.username == identifier,
                )
            stmt = stmt.options(selectinload(UserModel.aliases))
            result = session.exec(stmt).first()
            if result is not None:
                # Materialize alias usernames so callers can read them after the
                # session is closed (e.g. drive selection across asyncio.to_thread).
                _ = [alias.username for alias in (result.aliases or [])]
            return result
        finally:
            if close_session:
                session.close()

    def get_users_by_username_or_alias(
        self,
        username: str,
        session: Optional[Session] = None,
    ) -> list[UserModel]:
        """Return every case-insensitive canonical or alias username match."""
        self.ensure_connected()
        from ultima_scraper_db_archive import UserAliasModel, UserModel

        identifier = username.strip().lstrip("@").casefold()
        if not identifier:
            return []

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            canonical_users = session.exec(
                select(UserModel)
                .where(func.lower(UserModel.username) == identifier)
                .options(selectinload(UserModel.aliases))
            ).all()
            alias_users = session.exec(
                select(UserModel)
                .join(UserAliasModel, UserAliasModel.user_id == UserModel.id)
                .where(func.lower(UserAliasModel.username) == identifier)
                .options(selectinload(UserModel.aliases))
            ).all()
            users_by_id = {
                int(user.id): user
                for user in [*canonical_users, *alias_users]
            }
            for user in users_by_id.values():
                _ = [alias.username for alias in (user.aliases or [])]
            return [users_by_id[user_id] for user_id in sorted(users_by_id)]
        finally:
            if close_session:
                session.close()

    def get_user_identity_matches(
        self,
        user_ids,
        source_usernames,
        *,
        fetch_size: int,
        strip_stored_at: bool = True,
    ) -> dict[int, dict[str, Any]]:
        """Read one caller-bounded ID batch as scalar identity decisions.

        Alias rows stream from the driver; no ORM user graph or per-owner query
        is retained. The result has at most one small decision per requested ID.
        """
        from ultima_scraper_db_archive import UserAliasModel, UserModel

        if (
            isinstance(fetch_size, bool)
            or not isinstance(fetch_size, int)
            or fetch_size <= 0
        ):
            raise ValueError("Identity fetch size must be a positive integer")
        self.ensure_connected()
        identifiers = tuple(dict.fromkeys(user_ids))
        if not identifiers:
            return {}
        result: dict[int, dict[str, Any]] = {}

        def normalized(value):
            value = str(value or "").strip().casefold()
            return value.lstrip("@") if strip_stored_at else value

        with self.get_session() as session:
            rows = session.exec(
                select(UserModel.id, UserModel.username, UserAliasModel.username)
                .outerjoin(UserAliasModel, UserAliasModel.user_id == UserModel.id)
                .where(UserModel.id.in_(identifiers))
                .execution_options(yield_per=fetch_size)
            )
            try:
                for user_id, username, alias in rows:
                    identity = result.get(int(user_id))
                    if identity is None:
                        identity = result[int(user_id)] = {
                            "id": int(user_id),
                            "username": username,
                            "matches_source_identity": normalized(username)
                            in source_usernames,
                        }
                    if not identity["matches_source_identity"]:
                        identity["matches_source_identity"] = (
                            normalized(alias) in source_usernames
                        )
            finally:
                rows.close()
        return result

    @staticmethod
    def _username_candidate_ids(username: str):
        from ultima_scraper_db_archive import UserAliasModel, UserModel

        identifier = username.strip().lstrip("@").casefold()
        if not identifier:
            return None
        return (
            select(UserModel.id)
            .where(func.lower(UserModel.username) == identifier)
            .union(
                select(UserModel.id)
                .join(UserAliasModel, UserAliasModel.user_id == UserModel.id)
                .where(func.lower(UserAliasModel.username) == identifier)
            )
        )

    def iter_user_ids_by_username(self, username: str, *, fetch_size: int):
        """Stream canonical/alias candidates without hydrating user objects."""
        if (
            isinstance(fetch_size, bool)
            or not isinstance(fetch_size, int)
            or fetch_size <= 0
        ):
            raise ValueError("Identity fetch size must be a positive integer")
        self.ensure_connected()
        statement = self._username_candidate_ids(username)
        if statement is None:
            return
        statement = statement.order_by("id").execution_options(yield_per=fetch_size)
        with self.get_session() as session:
            rows = session.exec(statement)
            try:
                for row in rows:
                    yield int(row[0])
            finally:
                rows.close()

    def iter_user_identities_by_username(self, username: str, *, fetch_size: int):
        """Stream complete candidate names/IDs without unrelated alias graphs."""
        from ultima_scraper_db_archive import UserModel

        if (
            isinstance(fetch_size, bool)
            or not isinstance(fetch_size, int)
            or fetch_size <= 0
        ):
            raise ValueError("Identity fetch size must be a positive integer")
        self.ensure_connected()
        candidates = self._username_candidate_ids(username)
        if candidates is None:
            return
        statement = (
            select(UserModel.id, UserModel.username)
            .where(UserModel.id.in_(candidates))
            .order_by(UserModel.id)
            .execution_options(yield_per=fetch_size)
        )
        with self.get_session() as session:
            rows = session.exec(statement)
            try:
                for identity, username in rows:
                    yield {"id": int(identity), "username": username}
            finally:
                rows.close()


    def create_or_update_user(
        self, user_data: dict[str, Any], session: Optional[Session] = None
    ) -> UserModel:
        """Create or update a user using only ``user_id``.

        Args:
            user_data: Dictionary containing user attributes (must include id)
            session: Optional session to use

        Returns:
            Created or updated UserModel instance
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import UserModel

        if "id" not in user_data:
            raise ValueError("create_or_update_user requires user_data['id']")

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            user_id = int(user_data["id"])
            user = self.get_user(user_id, session=session)
            if user is None:
                user = UserModel(**user_data)
                session.add(user)
                self._commit_or_flush(session, close_session)
                self._refresh_safe(session, user)
                logger.info(
                    f"Created user: {user.id} ({user.username}) for site '{self.site}'"
                )
                return user

            for key, value in user_data.items():
                if key != "id" and hasattr(user, key):
                    setattr(user, key, value)

            user.touch()
            session.add(user)
            self._commit_or_flush(session, close_session)
            self._refresh_safe(session, user)
            logger.debug(f"Updated user {user_id} for site '{self.site}'")
            return user
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(f"Failed to create_or_update user: {e}")
            raise
        finally:
            if close_session:
                session.close()

    def update_user(
        self,
        user_id: int,
        updates: dict[str, Any],
        session: Optional[Session] = None,
    ) -> Optional[UserModel]:
        """Update an existing user.

        Args:
            user_id: The user ID to update
            updates: Dictionary of fields to update
            session: Optional session to use

        Returns:
            Updated UserModel instance or None if not found
        """
        self.ensure_connected()
        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            user = self.get_user(user_id, session=session)
            if not user:
                logger.warning(f"User {user_id} not found for update")
                return None

            # Update fields
            for key, value in updates.items():
                if hasattr(user, key):
                    setattr(user, key, value)

            user.touch()  # Update timestamp
            session.add(user)
            self._commit_or_flush(session, close_session)
            self._refresh_safe(session, user)
            logger.debug(f"Updated user {user_id} for site '{self.site}'")
            return user
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(f"Failed to update user {user_id}: {e}")
            raise
        finally:
            if close_session:
                session.close()

    def list_users(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
        active_only: bool = False,
        session: Optional[Session] = None,
    ) -> list[UserModel]:
        """List users with optional filtering.

        Args:
            limit: Maximum number of users to return
            offset: Number of users to skip
            active_only: If True, only return active users
            session: Optional session to use

        Returns:
            List of UserModel instances
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import UserModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            stmt = select(UserModel)
            if active_only:
                stmt = stmt.where(UserModel.active.is_(True))

            stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)

            results = session.exec(stmt).all()
            return list(results)
        finally:
            if close_session:
                session.close()

    # ========================
    # User Auth Operations
    # ========================

    def get_user_auth(
        self, user_id: int, session: Optional[Session] = None
    ) -> Optional[UserAuthModel]:
        """Get user authentication details.

        Searches by ``user_id`` first, then falls back to the record's
        primary-key ``id`` so that callers can pass either identifier.

        Args:
            user_id: The user ID (or record ID) to get auth for
            session: Optional session to use

        Returns:
            UserAuthModel instance or None if not found
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import UserAuthModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            # Primary lookup: match on the platform user_id
            stmt = select(UserAuthModel).where(
                UserAuthModel.user_id == user_id,
                UserAuthModel.site == self.site,
            )
            result = session.exec(stmt).first()
            if result is not None:
                return result

            # Fallback: the caller may have passed the DB record id
            stmt_by_id = select(UserAuthModel).where(
                UserAuthModel.id == user_id,
                UserAuthModel.site == self.site,
            )
            return session.exec(stmt_by_id).first()
        finally:
            if close_session:
                session.close()

    # ========================
    # Subscription Operations
    # ========================

    def get_subscriptions(
        self, user_id: int, session: Optional[Session] = None
    ) -> list[SubscriptionModel]:
        """Get all subscriptions for a user.

        Args:
            user_id: The user ID to get subscriptions for
            session: Optional session to use

        Returns:
            List of SubscriptionModel instances
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import SubscriptionModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            stmt = select(SubscriptionModel).where(SubscriptionModel.user_id == user_id)
            results = session.exec(stmt).all()
            return list(results)
        finally:
            if close_session:
                session.close()

    def create_subscription(
        self, subscription_data: dict[str, Any], session: Optional[Session] = None
    ) -> SubscriptionModel:
        """Create a new subscription.

        Args:
            subscription_data: Dictionary containing subscription attributes
            session: Optional session to use

        Returns:
            The created SubscriptionModel instance
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import SubscriptionModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            subscription = SubscriptionModel(**subscription_data)
            session.add(subscription)
            self._commit_or_flush(session, close_session)
            self._refresh_safe(session, subscription)
            logger.debug(f"Created subscription for site '{self.site}'")
            return subscription
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(f"Failed to create subscription: {e}")
            raise
        finally:
            if close_session:
                session.close()

    def create_or_update_subscription(
        self,
        user_id: int,
        subscriber_id: int,
        data: Optional[dict[str, Any]] = None,
        session: Optional[Session] = None,
    ) -> tuple[SubscriptionModel, bool]:
        """Create or update a subscription by user and subscriber IDs.

        Args:
            user_id: The subscribed user's ID
            subscriber_id: The subscriber's user ID
            data: Optional fields to set on create or update
            session: Optional session to use

        Returns:
            Tuple of (SubscriptionModel, created: bool)
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import SubscriptionModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            stmt = select(SubscriptionModel).where(
                SubscriptionModel.user_id == user_id,
                SubscriptionModel.subscriber_id == subscriber_id,
            )
            existing = session.exec(stmt).first()

            if existing is None:
                subscription_data = {
                    "user_id": user_id,
                    "subscriber_id": subscriber_id,
                }
                if data:
                    subscription_data.update(data)
                sub = SubscriptionModel(**subscription_data)
                session.add(sub)
                self._commit_or_flush(session, close_session)
                self._refresh_safe(session, sub)
                return sub, True

            if data:
                for key, value in data.items():
                    if hasattr(existing, key):
                        if key in {"paid_content", "paid"} and getattr(existing, key, None) is True:
                            continue
                        setattr(existing, key, value)

            session.add(existing)
            self._commit_or_flush(session, close_session)
            self._refresh_safe(session, existing)
            return existing, False
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(f"Failed to create_or_update subscription: {e}")
            raise
        finally:
            if close_session:
                session.close()

    # ========================
    # Advanced Query Methods
    # ========================

    # ========================
    # Content Operations
    # ========================

    def get_content(
        self,
        user_id: int,
        content_type: str,
        limit: Optional[int] = None,
        content_ids: Optional[list[int]] = None,
        session: Optional[Session] = None,
    ) -> list[Any]:
        """Get content items for a user by content type.

        Args:
            user_id: The user ID who owns the content
            content_type: Type of content ('posts', 'messages', 'stories', etc.)
            limit: Optional maximum number of items to return
            content_ids: Optional list of specific content IDs to fetch
            session: Optional session to use

        Returns:
            List of content model instances (PostModel, MessageModel, etc.)

        Raises:
            ValueError: If content_type is not supported
        """
        self.ensure_connected()

        # Map content_type to model class
        content_type_lower = content_type.lower()

        # Handle profile content type separately
        if content_type_lower == "profile":
            return self._get_profile_content(user_id, limit, session)

        model_map = {
            "posts": "PostModel",
            "archivedposts": "PostModel",  # Archived posts are in the same table
            "archived_posts": "PostModel",
            "messages": "MessageModel",
            "stories": "StoryModel",
            "highlights": "StoryModel",  # Highlights are stored as stories
            "mass_messages": "MassMessageModel",
            "massmessages": "MassMessageModel",
        }

        model_name = model_map.get(content_type_lower)
        if not model_name:
            raise ValueError(
                f"Unsupported content_type: {content_type}. "
                f"Supported types: {', '.join(model_map.keys())}"
            )

        # Import the model dynamically from the package-level postgres models.
        try:
            import ultima_scraper_db_archive as db_models

            model_class = getattr(db_models, model_name)
        except AttributeError as exc:
            raise ValueError(f"Model {model_name} not found in database schema") from exc

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            # Build query
            stmt = sa_select(model_class).where(model_class.user_id == user_id)

            # Filter by archived status for posts
            if content_type_lower in ("archivedposts", "archived_posts"):
                stmt = stmt.where(model_class.archived.is_(True))
            elif content_type_lower == "posts":
                stmt = stmt.where(model_class.archived.is_(False))

            # Filter by specific content IDs if provided
            if content_ids:
                stmt = stmt.where(model_class.id.in_(content_ids))

            # Eager load media relationships and nested drm_manifest
            # Chain selectinload to load the drm_manifest for each media item
            import ultima_scraper_db_archive as db_models

            MediaModel = db_models.MediaModel

            stmt = stmt.options(
                selectinload(model_class.media).selectinload(MediaModel.drm_manifest),
                selectinload(model_class.media).selectinload(MediaModel.media_urls),
            )

            # Order by created_at descending (newest first)
            stmt = stmt.order_by(model_class.created_at.desc())

            # Apply limit if provided
            if limit and limit > 0:
                stmt = stmt.limit(limit)

            results = session.execute(stmt).scalars().all()  # type: ignore[deprecated]
            logger.info(
                f"[get_content] Fetched {len(results)} {content_type} items for user {user_id}"
            )
            return list(results)
        except Exception as e:
            logger.error(
                f"[get_content] Failed to fetch {content_type} for user {user_id}: {e}",
                exc_info=True,
            )
            raise
        finally:
            if close_session:
                session.close()

    def _get_profile_content(
        self,
        user_id: int,
        limit: int | None = None,
        session: Session | None = None,
    ) -> list[Any]:
        """Get profile media (avatar and header) for a user.

        Args:
            user_id: The user ID who owns the profile media
            limit: Optional maximum number of items to return
            session: Optional session to use

        Returns:
            List of UserProfileMediaModel instances
        """
        from ultima_scraper_db_archive import UserProfileMediaModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            stmt = sa_select(UserProfileMediaModel).where(
                UserProfileMediaModel.user_id == user_id
            )
            stmt = stmt.order_by(UserProfileMediaModel.created_at.desc())

            if limit and limit > 0:
                stmt = stmt.limit(limit)

            results = session.execute(stmt).scalars().all()  # type: ignore[deprecated]
            logger.info(
                f"[get_content] Fetched {len(results)} profile media items for user {user_id}"
            )
            return list(results)
        except Exception as e:
            logger.error(
                f"[get_content] Failed to fetch profile for user {user_id}: {e}",
                exc_info=True,
            )
            raise
        finally:
            if close_session:
                session.close()

    def get_media_for_content(
        self,
        content_id: int,
        content_type: str,
        session: Optional[Session] = None,
    ) -> list[Any]:
        """Get all media items for a content item.

        Args:
            content_id: The content ID
            content_type: Type of content ('posts', 'messages', 'stories', etc.)
            session: Optional session to use

        Returns:
            List of MediaModel instances

        Raises:
            ValueError: If content_type is not supported
        """
        self.ensure_connected()

        # Map content_type to foreign key column in ContentMediaAssoModel
        content_type_lower = content_type.lower()
        fk_column_map = {
            "posts": "post_id",
            "messages": "message_id",
            "stories": "story_id",
            "mass_messages": "mass_message_id",
            "massmessages": "mass_message_id",
        }

        fk_column_name = fk_column_map.get(content_type_lower)
        if not fk_column_name:
            raise ValueError(
                f"Unsupported content_type: {content_type}. "
                f"Supported types: {', '.join(fk_column_map.keys())}"
            )

        import ultima_scraper_db_archive as db_models

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            # Query: MediaModel JOIN ContentMediaAssoModel ON media_id
            # WHERE ContentMediaAssoModel.<fk_column> = content_id
            ContentMediaAssoModel = db_models.ContentMediaAssoModel
            MediaModel = db_models.MediaModel

            # Build the join query with eager loading of drm_manifest
            stmt = (
                select(MediaModel)
                .join(
                    ContentMediaAssoModel,
                    MediaModel.id == ContentMediaAssoModel.media_id,
                )
                .where(getattr(ContentMediaAssoModel, fk_column_name) == content_id)
                .options(selectinload(MediaModel.drm_manifest))
            )

            results = session.exec(stmt).all()
            logger.debug(
                f"[get_media_for_content] Fetched {len(results)} media items for {content_type} {content_id}"
            )
            return list(results)
        except Exception as e:
            logger.error(
                f"[get_media_for_content] Failed to fetch media for {content_type} {content_id}: {e}",
                exc_info=True,
            )
            raise
        finally:
            if close_session:
                session.close()

    # ========================
    # Media Operations
    # ========================

    def find_media_by_id(
        self, media_id: int, session: Optional[Session] = None
    ) -> Optional[Any]:  # MediaModel
        """Find a media item by ID.

        Args:
            media_id: The media ID
            session: Optional session to use

        Returns:
            MediaModel instance or None if not found
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import MediaModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            stmt = select(MediaModel).where(MediaModel.id == media_id)
            result = session.exec(stmt).first()
            return result
        finally:
            if close_session:
                session.close()

    def create_or_update_media(
        self, media_data: dict[str, Any], session: Optional[Session] = None
    ) -> Any:
        """Create or update a media record using only ``media_id``.

        Args:
            media_data: Dictionary containing media attributes (must include id)
            session: Optional session to use

        Returns:
            Created or updated MediaModel instance
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import MediaModel

        if "id" not in media_data:
            raise ValueError("create_or_update_media requires media_data['id']")

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            media_columns = set(MediaModel.__table__.columns.keys())
            payload = {k: v for k, v in media_data.items() if k in media_columns}
            media_id = int(payload["id"])

            if self._is_postgres_session(session):
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                update_values = {
                    key: value for key, value in payload.items() if key != "id"
                }
                stmt = (
                    pg_insert(MediaModel)
                    .values(**payload)
                    .on_conflict_do_update(
                        index_elements=[MediaModel.id],
                        set_=update_values,
                    )
                    .returning(MediaModel.id)
                )
                persisted_media_id = int(cast(Any, session).execute(stmt).scalar_one())
                self._commit_or_flush(session, close_session)
                media = session.get(MediaModel, persisted_media_id)
                if media is None:
                    raise RuntimeError(
                        f"Failed to load media {persisted_media_id} after upsert"
                    )
                self._refresh_safe(session, media)
                return media

            media = session.get(MediaModel, media_id)
            if media is None:
                media = MediaModel(**payload)
                session.add(media)
                self._commit_or_flush(session, close_session)
                self._refresh_safe(session, media)
                return media

            for key, value in payload.items():
                if key != "id" and hasattr(media, key):
                    setattr(media, key, value)

            session.add(media)
            self._commit_or_flush(session, close_session)
            self._refresh_safe(session, media)
            return media
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(f"Failed to create_or_update media: {e}")
            raise
        finally:
            if close_session:
                session.close()

    def batch_record_filepaths(
        self,
        rows: list[tuple[int, str, bool, bool]],
        session: Optional[Session] = None,
    ) -> int:
        """Create or update FilePathModel rows in a single transaction.

        Args:
            rows: List of (media_id, filepath, preview, downloaded) tuples
            session: Optional session to use

        Returns:
            Number of rows processed
        """
        if not rows:
            return 0

        self.ensure_connected()
        from ultima_scraper_db_archive import FilePathModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            start = time.perf_counter()
            merged_rows: dict[tuple[int, str], tuple[bool, bool]] = {}
            for media_id, filepath, preview, downloaded in rows:
                key = (int(media_id), filepath)
                existing = merged_rows.get(key)
                if existing is None:
                    merged_rows[key] = (bool(preview), bool(downloaded))
                else:
                    merged_rows[key] = (
                        existing[0] or bool(preview),
                        existing[1] or bool(downloaded),
                    )

            media_ids = sorted({media_id for media_id, _ in merged_rows.keys()})
            filepaths = sorted({filepath for _, filepath in merged_rows.keys()})
            existing_rows = session.exec(
                select(FilePathModel).where(
                    FilePathModel.media_id.in_(media_ids),
                    FilePathModel.filepath.in_(filepaths),
                )
            ).all()
            existing_by_key = {
                (int(row.media_id), row.filepath): row for row in existing_rows
            }
            for (media_id, filepath), (preview, downloaded) in merged_rows.items():
                existing = existing_by_key.get((media_id, filepath))
                if existing is None:
                    session.add(
                        FilePathModel(
                            media_id=media_id,
                            filepath=filepath,
                            preview=preview,
                            downloaded=downloaded,
                        )
                    )
                    continue

                if downloaded and not existing.downloaded:
                    existing.downloaded = True
                if preview and not existing.preview:
                    existing.preview = True
                session.add(existing)

            self._commit_or_flush(session, close_session)
            logger.debug(
                "Batch recorded %d filepath rows for site '%s' in %.1fms",
                len(merged_rows),
                self.site,
                self._duration_ms(start),
            )
            return len(merged_rows)
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(f"Failed to batch record filepaths: {e}")
            raise
        finally:
            if close_session:
                session.close()

    def mark_media_downloaded(
        self,
        media_ids: list[int],
        session: Optional[Session] = None,
    ) -> int:
        """Mark media items as downloaded in the database.

        Args:
            media_ids: List of media IDs to mark as downloaded
            session: Optional session to use

        Returns:
            Number of media items updated
        """
        if not media_ids:
            return 0

        self.ensure_connected()
        from ultima_scraper_db_archive import MediaModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        updated_count = 0
        try:
            # Bulk update using SQL UPDATE with IN clause for efficiency
            from sqlalchemy import update
            from sqlalchemy.engine import CursorResult

            stmt = (
                update(MediaModel)
                .where(MediaModel.id.in_(media_ids))
                .values(downloaded=True)
            )
            result: CursorResult[tuple[()]] = session.execute(stmt)  # type: ignore[assignment]
            self._commit_or_flush(session, close_session)
            updated_count = result.rowcount or 0
            logger.info(
                f"[mark_media_downloaded] Marked {updated_count}/{len(media_ids)} "
                f"media items as downloaded for site '{self.site}'"
            )
            return updated_count
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(
                f"[mark_media_downloaded] Failed to mark media as downloaded: {e}"
            )
            raise
        finally:
            if close_session:
                session.close()

    def update_media_size(
        self,
        media_id: int,
        file_size: int,
        session: Optional[Session] = None,
    ) -> bool:
        """Update the size of a downloaded media item.

        Persists the actual file size to the database so that future downloads
        can skip files with matching sizes without needing HEAD requests.

        Args:
            media_id: ID of the media to update
            file_size: File size in bytes
            session: Optional session to use

        Returns:
            True if update was successful, False otherwise
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import MediaModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            # Update the size field for the media
            from sqlalchemy import update

            stmt = (
                update(MediaModel)
                .where(MediaModel.id == media_id)
                .values(size=file_size)
            )
            result = cast(Any, session).execute(stmt)
            self._commit_or_flush(session, close_session)

            rowcount = int(result.rowcount or 0)
            if rowcount > 0:
                logger.debug(
                    f"[update_media_size] Updated media {media_id} size to {file_size:,} bytes"
                )
                return True
            else:
                logger.warning(
                    f"[update_media_size] No media found with id {media_id} to update size"
                )
                return False
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(
                f"[update_media_size] Failed to update media {media_id} size: {e}"
            )
            raise
        finally:
            if close_session:
                session.close()

    def update_media_sizes_bulk(
        self,
        media_sizes: dict[int, int],
        session: Optional[Session] = None,
    ) -> int:
        """Update many media sizes in a single DB transaction.

        Args:
            media_sizes: Mapping of media_id -> file_size
            session: Optional session to use

        Returns:
            Number of rows updated
        """
        if not media_sizes:
            return 0

        self.ensure_connected()
        from sqlalchemy import bindparam, update

        from ultima_scraper_db_archive import MediaModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            media_table = cast(Any, MediaModel.__table__)
            stmt = (
                update(media_table)
                .where(media_table.c.id == bindparam("media_id"))
                .values(size=bindparam("file_size"))
            )
            params = [
                {"media_id": media_id, "file_size": file_size}
                for media_id, file_size in media_sizes.items()
            ]
            result = cast(Any, session).execute(stmt, params)
            self._commit_or_flush(session, close_session)
            updated = int(result.rowcount or 0)
            if updated == 0:
                logger.debug(
                    "[update_media_sizes_bulk] No media rows updated "
                    f"for {len(media_sizes)} candidate items"
                )
            return updated
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(
                "[update_media_sizes_bulk] Failed to update "
                f"{len(media_sizes)} media sizes: {e}"
            )
            raise
        finally:
            if close_session:
                session.close()

    def update_drm_stream_urls(
        self,
        media_id: int,
        video_url: str | None,
        audio_url: str | None,
        video_size: int | None = None,
        audio_size: int | None = None,
        session: Session | None = None,
    ) -> bool:
        """Update the resolved video/audio stream URLs on a DRM manifest.

        These are the actual CDN stream URLs obtained from ``resolve_drm`` and
        can be used later for streaming playback without re-resolving.

        Args:
            media_id: ID of the media whose DRM manifest to update
            video_url: Resolved video stream URL (None for audio-only DRM)
            audio_url: Resolved audio stream URL
            video_size: Resolved video stream size in bytes
            audio_size: Resolved audio stream size in bytes
            session: Optional session to use

        Returns:
            True if update was successful, False otherwise
        """
        self.ensure_connected()
        from ultima_scraper_db_archive import DRMManifestModel

        close_session = session is None
        if session is None:
            session = self.get_session()

        try:
            from sqlalchemy import update

            stmt = (
                update(DRMManifestModel)
                .where(DRMManifestModel.media_id == media_id)
                .values(
                    video_url=video_url,
                    audio_url=audio_url,
                    video_size=video_size,
                    audio_size=audio_size,
                    updated_at=utcnow(),
                )
            )
            result = cast(Any, session).execute(stmt)
            self._commit_or_flush(session, close_session)

            rowcount = int(result.rowcount or 0)
            if rowcount > 0:
                logger.debug(
                    f"[update_drm_stream_urls] Updated DRM stream URLs for media {media_id}"
                )
                return True
            else:
                logger.warning(
                    f"[update_drm_stream_urls] No DRM manifest found for media {media_id}"
                )
                return False
        except Exception as e:
            self._rollback_if_owned(session, close_session)
            logger.error(
                f"[update_drm_stream_urls] Failed to update DRM stream URLs for media {media_id}: {e}"
            )
            raise
        finally:
            if close_session:
                session.close()

    # ========================
    # Transaction Support
    # ========================

    def begin_transaction(self) -> Session:
        """Begin a new transaction and return the session.

        Returns:
            A new session with an active transaction

        Example:
            session = manager.begin_transaction()
            try:
                # Do multiple operations
                manager.create_or_update_user({...}, session=session)
                manager.create_subscription({...}, session=session)
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        """
        self.ensure_connected()
        session = self.get_session()
        return session

    # ========================
    # Utility Methods
    # ========================

    def __repr__(self) -> str:
        """String representation of the database manager."""
        return f"{self.__class__.__name__}(site='{self.site}', connected={self._is_connected})"
