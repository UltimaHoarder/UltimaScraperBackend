"""PostgreSQL database management package for UltimaScraper Backend."""

from .base_manager import BaseDatabaseManager
from .factory import DatabaseManagerFactory, MultiSiteDatabaseManager
from .managers.postgres_manager import PostgreSQLDatabaseManager
from .usr_factory import (
    create_backend_database_manager,
    create_usr_database_manager,
    create_worker_database_manager,
    get_default_data_directory,
)

__all__ = [
    "BaseDatabaseManager",
    "PostgreSQLDatabaseManager",
    "DatabaseManagerFactory",
    "MultiSiteDatabaseManager",
    # USR-specific factories
    "create_usr_database_manager",
    "create_backend_database_manager",
    "create_worker_database_manager",
    "get_default_data_directory",
]
