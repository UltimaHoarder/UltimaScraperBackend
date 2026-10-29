"""PostgreSQL database manager factory and coordinator."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, Literal, Optional, cast

from sqlalchemy import Engine

from ultima_scraper_core import archive_database_url

from .base_manager import BaseDatabaseManager
from .managers.postgres_manager import PostgreSQLDatabaseManager
from .usr_factory import get_default_data_directory

logger = logging.getLogger(__name__)


class DatabaseManagerFactory:
    """Factory for creating database managers for different sites and backends."""

    @staticmethod
    def create_postgres_manager(
        site: str,
        connection_string: Optional[str] = None,
        auto_create: bool = False,
        auto_connect: bool = True,
        engine: Optional[Engine] = None,
        pool_size: int = 8,
        max_overflow: int = 8,
        read_only: bool = False,
        pool_role: Literal["application", "worker_reader", "writer"] | None = None,
    ) -> PostgreSQLDatabaseManager:
        """Create a PostgreSQL database manager for a specific site.

        Args:
            site: The site name (e.g., 'onlyfans', 'fansly', 'loyalfans')
            connection_string: PostgreSQL connection string
                              e.g., postgresql://user:pass@localhost:5432/onlyfans
                              If not provided, reads from environment variable
                              {SITE_UPPER}_DATABASE_URL (or generic
                              DATABASE_URL).  As a convenience the method will
                              also check the application's ``.env`` file for
                              the same keys so that values written during
                              setup can be used without exporting them
                              manually.
            auto_create: Whether to bootstrap schemas/tables on connect. This is
                disabled by default; schema changes belong to Alembic migrations.
            auto_connect: Whether to automatically connect after creation
            engine: Optional pre-configured engine
            pool_size: Connection pool size
            max_overflow: Max overflow connections
            read_only: Enforce read-only PostgreSQL transactions.
            pool_role: Connection ownership role.

        Returns:
            Configured PostgreSQLDatabaseManager instance

        Raises:
            ValueError: If connection_string is not provided and env var not found
        """
        # Resolve connection string from multiple sources
        if not connection_string:
            env_var = f"{site.upper()}_DATABASE_URL"
            connection_string = archive_database_url(site)

            if not connection_string:
                raise ValueError(
                    f"PostgreSQL connection string not provided. "
                    f"Set either {env_var} or ULTIMA_ARCHIVE_DATABASE_URL environment variable"
                )

        manager = PostgreSQLDatabaseManager(
            site=site,
            connection_string=connection_string,
            auto_create=auto_create,
            engine=engine,
            pool_size=pool_size,
            max_overflow=max_overflow,
            read_only=read_only,
            pool_role=pool_role,
        )

        if auto_connect:
            manager.connect()

        return manager

    @staticmethod
    def create_manager_by_backend(
        site: str,
        backend: Optional[str] = None,
        **kwargs: Any,
    ) -> BaseDatabaseManager:
        """Create a database manager based on configured backend.

        Args:
            site: The site name
            backend: Database backend ('postgresql')
            **kwargs: Arguments passed to specific manager factory method

        Returns:
            Appropriate database manager instance

        Example:
            ```python
            # Uses PostgreSQL by default
            manager = DatabaseManagerFactory.create_manager_by_backend('onlyfans')

            # Or explicitly specify PostgreSQL
            manager = DatabaseManagerFactory.create_manager_by_backend(
                'onlyfans',
                backend='postgresql',
                connection_string='postgresql://user:pass@localhost/onlyfans'
            )
            ```
        """
        backend = (backend or "postgresql").lower()

        if backend in ("postgres", "postgresql", ""):
            return DatabaseManagerFactory.create_postgres_manager(site, **kwargs)
        else:
            raise ValueError(
                f"Unsupported database backend: {backend}. PostgreSQL is required."
            )


class MultiSiteDatabaseManager:
    """Coordinator for managing databases across multiple sites.

    This class is a **singleton**: every call to ``MultiSiteDatabaseManager()``
    returns the same instance so that connection pools are shared across all
    API routes instead of being duplicated per request.

    Example:
        ```python
        # Create manager for multiple sites
        multi_manager = MultiSiteDatabaseManager()

        # Get or create managers for specific sites
        of_db = multi_manager.get_or_create_manager("onlyfans")
        fansly_db = multi_manager.get_or_create_manager("fansly")

        # Access site-specific operations
        user = of_db.get_user(12345)
        subscriptions = fansly_db.get_subscriptions(67890)

        # Get stats for all sites
        all_stats = multi_manager.get_all_stats()
        ```
    """

    _instance: Optional[MultiSiteDatabaseManager] = None
    base_directory: Path
    _managers: dict[str, BaseDatabaseManager]
    _manager_lock: threading.RLock

    def __new__(cls, base_directory: Optional[Path] = None) -> MultiSiteDatabaseManager:
        if cls._instance is None:
            inst = cast(MultiSiteDatabaseManager, super().__new__(cls))
            inst.base_directory = base_directory or get_default_data_directory()
            inst._managers = {}
            inst._manager_lock = threading.RLock()
            logger.info(
                f"Initialized MultiSiteDatabaseManager singleton "
                f"with base: {inst.base_directory}"
            )
            cls._instance = inst
        return cls._instance

    def __init__(self, base_directory: Optional[Path] = None):
        # All init is handled in __new__ to guarantee singleton semantics.
        pass

    def get_manager(self, site: str) -> Optional[BaseDatabaseManager]:
        """Get the database manager for a specific site.

        Args:
            site: The site name (e.g., 'onlyfans', 'fansly')

        Returns:
            The database manager if it exists, None otherwise
        """
        return self._managers.get(site.lower())

    def has_manager(self, site: str) -> bool:
        """Check if a manager exists for a specific site.

        Args:
            site: The site name to check

        Returns:
            True if manager exists, False otherwise
        """
        return site.lower() in self._managers

    def get_or_create_manager(
        self,
        site: str,
        backend: Optional[str] = None,
        auto_connect: bool = True,
        **kwargs: Any,
    ) -> BaseDatabaseManager:
        """Get or create a PostgreSQL manager for a site.

        Args:
            site: The site name (e.g., 'onlyfans', 'fansly')
            backend: Optional backend override ('postgresql').
            auto_connect: Whether to automatically connect after creation
            **kwargs: Passed through to the underlying factory.

        Returns:
            The database manager for the site.

        Raises:
            ValueError: if an unsupported backend is requested.
        """
        site = site.lower()

        with self._manager_lock:
            if site in self._managers:
                manager = self._managers[site]
                if auto_connect and not manager.is_connected:
                    logger.info(
                        "Reconnecting existing database manager for '%s'",
                        site,
                    )
                    manager.connect()
                logger.debug(f"Returning existing manager for '{site}'")
                return manager

            backend = (backend or "postgresql").lower()

            if backend in ("postgres", "postgresql", ""):
                manager = DatabaseManagerFactory.create_postgres_manager(
                    site=site,
                    auto_connect=auto_connect,
                    **kwargs,
                )
            else:
                raise ValueError(
                    f"Unsupported database backend: {backend}. PostgreSQL is required."
                )

            self._managers[site] = manager
            logger.info(f"Created new {backend} manager for '{site}'")
            return manager

    def add_manager(self, site: str, manager: BaseDatabaseManager) -> None:
        """Add a pre-configured manager for a site.

        Args:
            site: The site name
            manager: The database manager instance
        """
        site = site.lower()
        if site in self._managers:
            logger.warning(f"Manager for '{site}' already exists, replacing")
        self._managers[site] = manager
        logger.info(f"Added manager for '{site}'")

    def get_all_sites(self) -> list[str]:
        """Get list of all sites with active managers.

        Returns:
            List of site names
        """
        return list(self._managers.keys())

    def disconnect_all(self) -> None:
        """Disconnect all database managers."""
        for site, manager in self._managers.items():
            try:
                manager.disconnect()
                logger.info(f"Disconnected manager for '{site}'")
            except Exception as e:
                logger.error(f"Failed to disconnect manager for '{site}': {e}")

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all managed databases.

        Returns:
            Dictionary mapping site names to their database stats
        """
        stats: dict[str, dict[str, Any]] = {}
        for site, manager in self._managers.items():
            try:
                if hasattr(manager, "get_database_stats"):
                    stats[site] = cast(Any, manager).get_database_stats()
                else:
                    stats[site] = {"site": site, "type": "postgresql"}
            except Exception as e:
                logger.error(f"Failed to get stats for '{site}': {e}")
                stats[site] = {"site": site, "error": str(e)}

        return stats

    def __repr__(self) -> str:
        """String representation of the multi-site manager."""
        sites = ", ".join(self._managers.keys())
        return f"MultiSiteDatabaseManager(sites=[{sites}], base={self.base_directory})"

    def __enter__(self) -> MultiSiteDatabaseManager:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - disconnect all managers."""
        self.disconnect_all()
