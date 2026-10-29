from __future__ import annotations

import logging

from ...auth_events import AuthEventPublisher
from ...manager import BackendManager

logger = logging.getLogger(__name__)


def _get_auth_event_publisher(
    backend_manager: BackendManager,
) -> AuthEventPublisher | None:
    redis = getattr(backend_manager, "redis", None)
    if redis is None:
        return None

    publisher = getattr(backend_manager, "_auth_event_publisher", None)
    if isinstance(publisher, AuthEventPublisher):
        return publisher

    try:
        publisher = AuthEventPublisher(redis)
        setattr(backend_manager, "_auth_event_publisher", publisher)
        return publisher
    except Exception as exc:
        logger.warning("Failed to initialize auth event publisher: %s", exc)
        return None


async def publish_auth_added(
    backend_manager: BackendManager,
    auth_id: int,
    *,
    username: str | None = None,
    site: str = "onlyfans",
) -> None:
    publisher = _get_auth_event_publisher(backend_manager)
    if publisher is None:
        return
    try:
        await publisher.publish_auth_added(auth_id, username=username, site=site)
    except Exception as exc:
        logger.warning("Failed to publish auth_added for %s: %s", auth_id, exc)


async def publish_auth_updated(
    backend_manager: BackendManager,
    auth_id: int,
    *,
    username: str | None = None,
    site: str = "onlyfans",
) -> None:
    publisher = _get_auth_event_publisher(backend_manager)
    if publisher is None:
        return
    try:
        await publisher.publish_auth_updated(auth_id, username=username, site=site)
    except Exception as exc:
        logger.warning("Failed to publish auth_updated for %s: %s", auth_id, exc)
