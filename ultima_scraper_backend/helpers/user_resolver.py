"""User identifier resolution utilities.

This module provides utilities to resolve usernames to numeric user IDs from the database.
All task creation should use numeric user_id (int) only, never usernames.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..manager import BackendManager

logger = logging.getLogger(__name__)


async def resolve_user_identifier(
    identifier: str | int | None, backend: BackendManager | None, site: str = "onlyfans"
) -> int | None:
    """
    Resolve a username or user ID to a numeric user ID from the database.

    This function should be called by all UI components before creating tasks
    to ensure only numeric user IDs are used in the job system.

    Args:
        identifier: Username (str) or user ID (int) to resolve
        backend: BackendManager instance for database access

    Returns:
        Numeric user ID if found, None otherwise

    Examples:
        >>> # User ID is passed through
        >>> await resolve_user_identifier(12345, backend)
        12345

        >>> # Username is looked up in database
        >>> await resolve_user_identifier("phoenixrebirthed", backend)
        15585607  # Returns the actual user_id from database

        >>> # Unknown username returns None
        >>> await resolve_user_identifier("nonexistent_user", backend)
        None
    """
    if identifier is None or backend is None:
        return None

    # If already an int, return it directly
    if isinstance(identifier, int):
        if identifier <= 0:
            logger.warning(
                f"Invalid user_id: {identifier}. User IDs must be positive integers."
            )
            return None
        return identifier

    identifier = identifier.strip()
    if not identifier:
        logger.warning("Empty identifier string provided")
        return None

    try:
        user_id = int(identifier)
        if user_id <= 0:
            logger.warning(
                f"Invalid user_id: {user_id}. User IDs must be positive integers."
            )
            return None
        return user_id
    except ValueError:
        pass

    try:
        db_manager = backend.db_managers.get(site.lower())
        if not db_manager:
            logger.error("Database manager for site '%s' not available", site.lower())
            return None

        db_user = await asyncio.to_thread(db_manager.get_user, identifier)
        if db_user and hasattr(db_user, "id") and db_user.id:
            if db_user.id <= 0:
                logger.warning(
                    f"Database returned invalid user_id {db_user.id} for username '{identifier}'"
                )
                return None
            logger.info(f"Resolved username '{identifier}' to user_id {db_user.id}")
            return db_user.id

        logger.warning(
            f"Username '{identifier}' not found in database. "
            f"Please ensure the user has been scraped/added to the database first."
        )
        return None
    except Exception as exc:
        logger.exception(f"Error looking up username '{identifier}' in database: {exc}")
        return None


__all__ = ["resolve_user_identifier"]
