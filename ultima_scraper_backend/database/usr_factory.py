"""USR-specific PostgreSQL database manager factory and helpers."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Literal, Optional

from platformdirs import user_data_dir

from ultima_scraper_core import archive_database_url

from .base_manager import BaseDatabaseManager

logger = logging.getLogger(__name__)

# Application constants for platformdirs
APP_NAME = "UltimaScraper"
APP_AUTHOR = "UltimaScraper"


def get_default_data_directory() -> Path:
    """Get the default data directory for UltimaScraper.

    Uses XDG-compliant directories via platformdirs, with optional
    environment variable override.

    Returns:
        Path to the data directory (databases, user data)

    Environment Variables:
        ULTIMA_SCRAPER_DATA_DIR: Override default data directory
    """
    env_override = os.getenv("ULTIMA_SCRAPER_DATA_DIR")
    if env_override:
        logger.debug(
            f"Using data directory from ULTIMA_SCRAPER_DATA_DIR: {env_override}"
        )
        return Path(env_override)

    # Use platformdirs for XDG-compliant path
    default_dir = Path(user_data_dir(APP_NAME, APP_AUTHOR))
    logger.debug(f"Using default XDG data directory: {default_dir}")
    return default_dir


def create_usr_database_manager(
    site: str,
    base_directory: Optional[Path] = None,
    read_only: bool = False,
    auto_create: Optional[bool] = None,
    pool_role: Literal["application", "worker_reader", "writer"] | None = None,
) -> BaseDatabaseManager:
    """Create a database manager for USR.

    This is the standard way to create database managers in USR with
    worker-safe initialization.

    Args:
        site: The site name (e.g., 'onlyfans', 'fansly', 'loyalfans')
        base_directory: Base directory for user data (default: XDG user_data_dir)
                       Can be overridden with ULTIMA_SCRAPER_DATA_DIR env var
        read_only: If True, create in read-only mode (for workers)
        auto_create: Whether to run schema/table bootstrap on connect.
                    Defaults to ``False``; migrations own schema changes.
        pool_role: Connection ownership role for pooling policy.

    Returns:
        Configured and connected BaseDatabaseManager.

    Example:
        # Runtime backend initialization (DML only)
        manager = create_usr_database_manager("onlyfans", read_only=False)

        # Worker initialization (read-only, won't create)
        manager = create_usr_database_manager("onlyfans", read_only=True)

        # Custom directory
        manager = create_usr_database_manager("onlyfans", base_directory=Path("./data"))

    Environment Variables:
        ULTIMA_SCRAPER_DATA_DIR: Override default data directory
    """
    site_env = f"{site.upper()}_DATABASE_URL"
    env_conn = archive_database_url(site)

    if auto_create is None:
        auto_create = False

    if not env_conn:
        raise RuntimeError(
            f"No PostgreSQL connection configured for site '{site}'. "
            f"Set {site_env} or ULTIMA_ARCHIVE_DATABASE_URL."
        )

    from .factory import DatabaseManagerFactory

    logger.info(f"Connecting to PostgreSQL for site {site}")
    manager = DatabaseManagerFactory.create_postgres_manager(
        site=site,
        connection_string=env_conn,
        auto_create=auto_create,
        auto_connect=True,
        read_only=read_only,
        pool_role=pool_role,
    )
    return manager


def create_backend_database_manager(
    site: str, base_directory: Optional[Path] = None
) -> BaseDatabaseManager:
    """Create an explicit owner/bootstrap database manager.

    This legacy helper opts into schema bootstrap and must only be used by
    owner-run setup tooling. Application runtimes should use
    :func:`create_usr_database_manager`.

    Args:
        site: The site name
        base_directory: Base directory for user data

    Returns:
        Database manager configured for backend use
    """
    return create_usr_database_manager(
        site=site,
        base_directory=base_directory,
        read_only=False,
        auto_create=True,
    )


def create_worker_database_manager(
    site: str, base_directory: Optional[Path] = None
) -> BaseDatabaseManager:
    """Create a database manager for worker use.

    Workers receive short-lived, read-only connections. They publish every
    mutation to the dedicated database writer instead of owning write pools.

    Args:
        site: The site name
        base_directory: Base directory for user data (default: XDG user_data_dir)

    Returns:
        Database manager configured for read-only worker queries.

    Raises:
        FileNotFoundError: If database doesn't exist (backend must create it)

    Environment Variables:
        ULTIMA_SCRAPER_DATA_DIR: Override default data directory
    """
    return create_usr_database_manager(
        site=site,
        base_directory=base_directory,
        read_only=True,
        auto_create=False,  # Avoid per-worker DDL/extension storms
        pool_role="worker_reader",
    )
