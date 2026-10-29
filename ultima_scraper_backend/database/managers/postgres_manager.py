"""PostgreSQL Database Manager - Implementation for PostgreSQL backend.

This module provides a concrete implementation of the database manager for PostgreSQL,
handling site-specific databases via connection strings.

Connection string format:
  postgresql+asyncpg://user:password@host:port/database
  postgresql://user:password@host:port/database
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Generator
from typing import Any, Literal, Optional, Protocol, cast
from urllib.parse import parse_qs, urlparse

from sqlalchemy import Engine, event, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool, QueuePool
from sqlmodel import Session, create_engine
from ultima_scraper_core import runtime_env_value

from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates.postgres_site import (
    metadata_obj as db_metadata,
)

from ..base_manager import BaseDatabaseManager

logger = logging.getLogger(__name__)


class _DBAPICursor(Protocol):
    def execute(self, operation: str) -> Any: ...
    def close(self) -> None: ...


class _DBAPIConnection(Protocol):
    autocommit: bool

    def cursor(self) -> _DBAPICursor: ...


def _runtime_env_int(key: str, default: int, *, minimum: int | None = None) -> int:
    """Parse integer setting from runtime env with fallback/default."""
    raw = runtime_env_value(key)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _is_pgbouncer_connection_url(url: str) -> bool:
    """Best-effort detection for PgBouncer connection strings.

    Detection sources (any true => PgBouncer mode):
    - Explicit env toggle: ``ULTIMA_USE_PGBOUNCER=true``
    - URL query: ``pgbouncer=true`` or ``pooler=pgbouncer``
    - Common PgBouncer endpoint: host ``pgbouncer`` or port ``6432``
    """
    env_flag = (runtime_env_value("ULTIMA_USE_PGBOUNCER") or "").strip().lower()
    if env_flag in {"1", "true", "yes", "on"}:
        return True

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query_flag = query.get("pgbouncer", [""])[0].strip().lower()
    pooler = query.get("pooler", [""])[0].strip().lower()
    if query_flag in {"1", "true", "yes", "on"} or pooler == "pgbouncer":
        return True

    if (parsed.hostname or "").strip().lower() == "pgbouncer":
        return True
    if parsed.port == 6432:
        return True
    return False


class PostgreSQLDatabaseManager(BaseDatabaseManager):
    """PostgreSQL implementation of the database manager.

    This class manages site-specific PostgreSQL databases using connection strings.

    Attributes:
        site: The site name (e.g., 'onlyfans', 'fansly')
        connection_string: PostgreSQL connection string
        engine: SQLAlchemy engine for the database
        async_engine: Optional async engine for async operations
    """

    def __init__(
        self,
        site: str,
        connection_string: str,
        auto_create: bool = False,
        read_only: bool = False,
        engine: Optional[Engine] = None,
        use_async: bool = False,
        pool_size: int = 8,
        max_overflow: int = 8,
        pool_role: Literal["application", "worker_reader", "writer"] | None = None,
    ):
        """Initialize the PostgreSQL database manager.

        Args:
            site: The site name (e.g., 'onlyfans', 'fansly', 'loyalfans')
            connection_string: PostgreSQL connection string
                              e.g., postgresql://user:pass@localhost:5432/onlyfans
            auto_create: Whether to bootstrap schemas/tables on connect. Runtime
                callers should leave this disabled and use Alembic instead.
            read_only: If True, only connect to existing databases
            engine: Optional pre-configured engine
            use_async: Whether to use async connections (requires asyncpg)
            pool_size: Connection pool size
            max_overflow: Max overflow connections beyond pool_size
            pool_role: Connection ownership role. Worker readers use short-lived
                PgBouncer clients; only the database writer owns a write pool.
        """
        logger.info(
            f"[DB_INIT] PostgreSQLDatabaseManager.__init__ called for site='{site}', "
            f"auto_create={auto_create}, read_only={read_only}, "
            f"use_async={use_async}"
        )

        super().__init__(site, engine)

        # The regular ``Session``/``create_engine`` path below is synchronous.
        # Setup currently persists URLs as ``postgresql+asyncpg://`` because
        # migrations and async helpers use that driver, so normalize every
        # supported PostgreSQL URL to psycopg3 before creating the sync engine.
        for async_scheme in (
            "postgresql+asyncpg://",
            "postgresql+psycopg2://",
            "postgresql://",
        ):
            if connection_string.startswith(async_scheme):
                connection_string = (
                    "postgresql+psycopg://" + connection_string[len(async_scheme) :]
                )
                break

        self.connection_string = connection_string
        self.auto_create = auto_create
        self.read_only = read_only
        self.use_async = use_async
        configured_role = (
            (
                pool_role
                or runtime_env_value("ULTIMA_DATABASE_PROCESS_ROLE")
                or "application"
            )
            .strip()
            .lower()
        )
        if configured_role not in {"application", "worker_reader", "writer"}:
            raise ValueError(f"Unsupported database pool role: {configured_role}")
        self.pool_role = cast(
            Literal["application", "worker_reader", "writer"], configured_role
        )
        if self.pool_role == "worker_reader" and not self.read_only:
            raise ValueError("worker_reader database managers must be read-only")
        if self.pool_role == "writer" and self.read_only:
            raise ValueError("writer database managers cannot be read-only")
        self.pool_size = _runtime_env_int("ULTIMA_DB_POOL_SIZE", pool_size, minimum=1)
        self.max_overflow = _runtime_env_int(
            "ULTIMA_DB_MAX_OVERFLOW", max_overflow, minimum=0
        )
        self.pool_timeout = _runtime_env_int("ULTIMA_DB_POOL_TIMEOUT", 5, minimum=1)
        self.pool_recycle = _runtime_env_int("ULTIMA_DB_POOL_RECYCLE", 1800, minimum=30)
        self.statement_timeout_ms = _runtime_env_int(
            "ULTIMA_DB_STATEMENT_TIMEOUT_MS",
            90_000,
            minimum=1_000,
        )
        self.lock_timeout_ms = _runtime_env_int(
            "ULTIMA_DB_LOCK_TIMEOUT_MS",
            10_000,
            minimum=0,
        )
        self.idle_in_transaction_timeout_ms = _runtime_env_int(
            "ULTIMA_DB_IDLE_IN_TX_TIMEOUT_MS",
            120_000,
            minimum=0,
        )
        self.isolation_level = (
            (runtime_env_value("ULTIMA_DB_ISOLATION_LEVEL") or "READ COMMITTED")
            .strip()
            .upper()
        )
        self._pgbouncer_mode = _is_pgbouncer_connection_url(self.connection_string)
        self._short_lived_clients = self.pool_role == "worker_reader" or (
            self._pgbouncer_mode and self.pool_role == "application"
        )
        if self.pool_role == "writer":
            # This is the one scale-independent SQL concurrency budget. It is
            # enforced for both direct PostgreSQL and PgBouncer deployments.
            self.pool_size = _runtime_env_int(
                "ULTIMA_DATABASE_WRITER_CONCURRENCY", 4, minimum=1
            )
            self.max_overflow = 0
        elif self._pgbouncer_mode and self.pool_role != "worker_reader":
            self.pool_size = _runtime_env_int(
                "ULTIMA_PGBOUNCER_CLIENT_POOL_SIZE",
                self.pool_size,
                minimum=1,
            )
            self.max_overflow = _runtime_env_int(
                "ULTIMA_PGBOUNCER_CLIENT_MAX_OVERFLOW",
                self.max_overflow,
                minimum=0,
            )
        self.async_engine = None
        self._session_factory: sessionmaker[Session] | None = None

        logger.info(
            f"[DB_INIT] Initialized PostgreSQL manager for '{self.site}' "
            f"(read_only={read_only}, auto_create={auto_create}, async={use_async}, "
            f"pool_role={self.pool_role}, pgbouncer_mode={self._pgbouncer_mode}, "
            f"isolation_level={self.isolation_level}, "
            f"statement_timeout_ms={self.statement_timeout_ms}, lock_timeout_ms={self.lock_timeout_ms})"
        )

    def connect(self) -> None:
        """Establish database connection and create tables if needed.

        For PostgreSQL, this connects to an existing database. The database
        and schema must already exist.
        """
        start = time.perf_counter()
        if self.engine:
            if self._is_connected:
                logger.info(
                    f"[DB_CONNECT] Using existing connected engine for '{self.site}'"
                )
                return
            logger.warning(
                f"[DB_CONNECT] Disposing incomplete engine before retry for '{self.site}'"
            )
            self.close()

        # Create synchronous engine
        pool_description = (
            "short-lived connections"
            if self._short_lived_clients
            else (
                f"pool_size={self.pool_size}, max_overflow={self.max_overflow}, "
                f"pool_timeout={self.pool_timeout}s"
            )
        )
        logger.info(
            "[DB_CONNECT] Creating PostgreSQL engine for '%s' with %s",
            self.site,
            pool_description,
        )

        if self._short_lived_clients:
            # Retained clients pin server backends in session pooling, even
            # after a transaction ends. API/site pools must release clients
            # just like worker readers; the writer keeps its fixed budget.
            pool_config: dict[str, Any] = {"poolclass": NullPool}
        else:
            pool_config = {
                "poolclass": QueuePool,
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
                "pool_timeout": self.pool_timeout,
                "pool_recycle": self.pool_recycle,
                "pool_pre_ping": True,
                "pool_use_lifo": True,
            }

        connect_args: dict[str, Any] = {}
        if self._pgbouncer_mode:
            # Prevent server-side prepared statement churn across pooled links.
            connect_args["prepare_threshold"] = None

        # Set search_path to site-specific schema so unqualified table names
        # (e.g. "users", "user_auths") resolve to <site>.users instead of
        # public.users.  The per-site schemas were created during the
        # SQLite → PostgreSQL migration.
        site_schema = self.site.lower()

        self.engine = create_engine(
            self.connection_string,
            echo=False,
            isolation_level=self.isolation_level,
            connect_args=connect_args,
            **pool_config,
        )

        # Direct/session-pooled connections retain session state, so configure
        # it once when psycopg opens the connection. Transaction-pooled
        # PgBouncer can assign a different PostgreSQL backend to every
        # transaction; those settings are applied with SET LOCAL below.
        def _set_search_path(
            dbapi_connection: _DBAPIConnection,
            _connection_record: Any,
        ) -> None:
            old_autocommit = dbapi_connection.autocommit
            dbapi_connection.autocommit = True
            cursor = dbapi_connection.cursor()
            cursor.execute(f"SET search_path TO {site_schema}, public")
            cursor.execute(f"SET statement_timeout TO {int(self.statement_timeout_ms)}")
            cursor.execute(f"SET lock_timeout TO {int(self.lock_timeout_ms)}")
            if self.idle_in_transaction_timeout_ms > 0:
                cursor.execute(
                    "SET idle_in_transaction_session_timeout TO "
                    f"{int(self.idle_in_transaction_timeout_ms)}"
                )
            cursor.close()
            dbapi_connection.autocommit = old_autocommit

        def _set_transaction_settings(connection: Any) -> None:
            statements = [
                f"SET LOCAL search_path TO {site_schema}, public",
                f"SET LOCAL statement_timeout TO {int(self.statement_timeout_ms)}",
                f"SET LOCAL lock_timeout TO {int(self.lock_timeout_ms)}",
            ]
            if self.idle_in_transaction_timeout_ms > 0:
                statements.append(
                    "SET LOCAL idle_in_transaction_session_timeout TO "
                    f"{int(self.idle_in_transaction_timeout_ms)}"
                )
            if self.read_only:
                statements.append("SET TRANSACTION READ ONLY")
            # Psycopg supports multiple statements without bound parameters.
            # Keep transaction-local settings and read-only enforcement in the
            # same order, while paying for one PgBouncer round trip per begin.
            connection.exec_driver_sql("; ".join(statements)).close()

        if self._pgbouncer_mode:
            event.listen(self.engine, "begin", _set_transaction_settings)
        else:
            event.listen(self.engine, "connect", _set_search_path)
            if self.read_only:
                event.listen(
                    self.engine,
                    "begin",
                    lambda connection: connection.exec_driver_sql(
                        "SET TRANSACTION READ ONLY"
                    ),
                )

        # Only bootstrap schema/extension/tables when auto_create is enabled.
        # Worker readers never run DDL.
        if self.auto_create:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {site_schema}"))
                conn.commit()
            self._enable_postgres_extensions()
            self.create_tables()
            logger.info(
                f"[DB_CONNECT] Bootstrapped schema/tables for '{self.site}' "
                f"(search_path='{site_schema}, public')"
            )
        else:
            logger.info(
                f"[DB_CONNECT] Connected without schema bootstrap for '{self.site}' "
                f"(search_path='{site_schema}, public')"
            )

        self._is_connected = True
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            f"[DB_CONNECT] Connected to PostgreSQL for '{self.site}' in {elapsed_ms:.1f}ms"
        )

    def create_async_engine(self) -> None:
        """Create async engine for async/await operations.

        Requires asyncpg to be installed: pip install asyncpg
        """
        if self.async_engine:
            return

        # Convert connection string to async format
        async_conn_string = self.connection_string
        if async_conn_string.startswith("postgresql+psycopg://"):
            async_conn_string = async_conn_string.replace(
                "postgresql+psycopg://", "postgresql+asyncpg://", 1
            )
        elif async_conn_string.startswith("postgresql://"):
            async_conn_string = async_conn_string.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )

        logger.info(f"[DB_CONNECT] Creating async PostgreSQL engine for '{self.site}'")

        connect_args: dict[str, Any] = {}
        if self._pgbouncer_mode:
            connect_args["prepared_statement_cache_size"] = 0
        if self._short_lived_clients:
            pool_kwargs: dict[str, Any] = {"poolclass": NullPool}
        else:
            pool_kwargs = {
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
                "pool_timeout": self.pool_timeout,
                "pool_recycle": self.pool_recycle,
                "pool_pre_ping": True,
                "pool_use_lifo": True,
            }

        self.async_engine = create_async_engine(
            async_conn_string,
            echo=False,
            connect_args=connect_args,
            **pool_kwargs,
        )

    def create_tables(self) -> None:
        """Create all tables in the site-specific PostgreSQL schema.

        Temporarily narrows search_path to *only* the site schema so that
        ``CREATE TABLE IF NOT EXISTS`` never matches (or creates) tables in
        ``public``.  The normal search_path (``<site>, public``) is restored
        when the transaction ends.
        """
        if not self.engine:
            logger.info(
                f"[DB_CREATE] Engine not yet initialized for '{self.site}', "
                "calling connect() lazily..."
            )
            self.connect()

        site_schema = self.site.lower()
        logger.info(f"[DB_CREATE] Creating tables in schema '{site_schema}'")
        start = time.perf_counter()

        try:
            engine = self.engine
            if engine is None:
                raise RuntimeError("Engine not initialized")
            with engine.begin() as conn:
                # Narrow search_path so IF NOT EXISTS ignores public tables.
                # LOCAL: it must not outlive this transaction on a pooled link.
                conn.execute(text(f"SET LOCAL search_path TO {site_schema}"))
                db_metadata.create_all(conn)
            logger.info(
                f"[DB_CREATE] Tables created successfully in '{site_schema}' "
                f"in {(time.perf_counter() - start) * 1000:.1f}ms"
            )
        except Exception as e:
            logger.error(f"[DB_CREATE] Failed to create tables: {e}")
            raise

    def _enable_postgres_extensions(self) -> None:
        """Enable shared PostgreSQL extensions in the public schema."""
        if not self.engine:
            raise RuntimeError("Engine not initialized")

        required_extensions = {"uuid-ossp", "pg_trgm"}
        extension_names = ("uuid-ossp", "pg_trgm", "vector")
        start = time.perf_counter()
        try:
            with self.engine.begin() as conn:
                for extension_name in extension_names:
                    try:
                        with conn.begin_nested():
                            conn.execute(
                                text(
                                    f'CREATE EXTENSION IF NOT EXISTS "{extension_name}" '
                                    "WITH SCHEMA public"
                                )
                            )
                            extension_schema = conn.scalar(
                                text(
                                    "SELECT namespace.nspname "
                                    "FROM pg_extension AS extension "
                                    "JOIN pg_namespace AS namespace "
                                    "ON namespace.oid = extension.extnamespace "
                                    "WHERE extension.extname = :extension_name"
                                ),
                                {"extension_name": extension_name},
                            )
                            if extension_schema is None:
                                raise RuntimeError(
                                    f"PostgreSQL extension {extension_name!r} "
                                    "was not installed"
                                )
                            if extension_schema != "public":
                                conn.execute(
                                    text(
                                        f'ALTER EXTENSION "{extension_name}" '
                                        "SET SCHEMA public"
                                    )
                                )
                    except Exception as extension_exc:
                        if extension_name in required_extensions:
                            raise RuntimeError(
                                f"Required PostgreSQL extension {extension_name!r} "
                                "could not be installed in schema 'public'"
                            ) from extension_exc
                        logger.debug(
                            "[DB_INIT] Optional extension %s is unavailable: %s",
                            extension_name,
                            extension_exc,
                        )

            logger.info(
                f"[DB_INIT] PostgreSQL extensions enabled for '{self.site}' "
                f"in {(time.perf_counter() - start) * 1000:.1f}ms"
            )
        except Exception as e:
            logger.error(f"[DB_INIT] Could not enable required extensions: {e}")
            raise

    def get_session(self) -> Session:
        """Get a database session.

        Lazily calls connect() if the engine has not been initialized yet.

        Returns:
            SQLAlchemy Session instance
        """
        if not self.engine:
            logger.info(
                f"[DB_SESSION] Engine not yet initialized for '{self.site}', "
                "calling connect() lazily..."
            )
            self.connect()

        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                class_=Session,
                expire_on_commit=False,
                autoflush=False,
            )

        return self._session_factory()

    @contextlib.contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Yield a session wrapped in ``Session.begin()`` with timing logs."""
        session = self.get_session()
        start = time.perf_counter()
        try:
            with session.begin():
                yield session
            logger.debug(
                "[DB_TXN] site=%s status=committed duration_ms=%.2f",
                self.site,
                (time.perf_counter() - start) * 1000,
            )
        except Exception:
            logger.exception(
                "[DB_TXN] site=%s status=rolled_back duration_ms=%.2f",
                self.site,
                (time.perf_counter() - start) * 1000,
            )
            raise
        finally:
            session.close()

    async def get_async_session(self) -> AsyncSession:
        """Get an async database session.

        Requires async engine. Call create_async_engine() first.

        Returns:
            SQLAlchemy AsyncSession instance
        """
        if not self.async_engine:
            self.create_async_engine()

        from sqlalchemy.ext.asyncio import AsyncSession as AsyncSessionType

        return AsyncSessionType(self.async_engine)

    def disconnect(self) -> None:
        """Disconnect database connection and cleanup resources."""
        self.close()

    def close(self) -> None:
        """Close database connection and cleanup resources."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self._session_factory = None
            logger.info(f"[DB_CLOSE] Database connection closed for '{self.site}'")

        self._is_connected = False

        if self.async_engine:
            # For async cleanup, this is a sync method limitation
            logger.info(f"[DB_CLOSE] Async engine cleanup scheduled for '{self.site}'")
