from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from .redis_channels import HOOKS_CHANNEL

logger = logging.getLogger(__name__)

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list[JsonValue] | dict[str, JsonValue]


def _is_connected(redis_manager: object) -> bool:
    return bool(getattr(redis_manager, "is_connected", False))


def _get_client(redis_manager: object) -> object | None:
    return getattr(redis_manager, "client", None)


def _ensure_timestamp(event: dict[str, JsonValue]) -> None:
    if "timestamp" not in event:
        event["timestamp"] = datetime.now(timezone.utc).isoformat()


async def publish_ws_event(
    redis_manager: object | None,
    event: dict[str, JsonValue],
) -> None:
    if redis_manager is None or not _is_connected(redis_manager):
        return

    _ensure_timestamp(event)

    try:
        client = _get_client(redis_manager)
        if client is None:
            return
        publish = getattr(client, "publish", None)
        if not callable(publish):
            return
        payload = json.dumps(event)
        result = publish(HOOKS_CHANNEL, payload)
        if asyncio.iscoroutine(result):
            await result
    except Exception as exc:
        logger.debug("Failed to publish websocket event: %s", exc)
