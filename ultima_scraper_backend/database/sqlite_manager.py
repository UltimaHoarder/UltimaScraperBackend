"""Removed SQLite compatibility shim."""

raise ImportError(
    "SQLite support has been removed; use "
    "ultima_scraper_backend.database.managers.postgres_manager instead."
)
