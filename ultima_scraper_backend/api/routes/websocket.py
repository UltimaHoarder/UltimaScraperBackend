"""WebSocket endpoint for real-time event streaming.

Provides WebSocket connection for:
- Job status updates
- Task progress
- Media download events
- System notifications
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Any, Mapping, Set, cast

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from ultima_scraper_api.managers.redis.connection import RedisManager as ApiRedisManager

from ...config import UltimaScraperCollectionConfig
from ...models.backend import BackendInstance
from ...security import (
    ensure_ws_admin_auth,
    extract_admin_token,
    get_allowed_origin_regex,
    get_ws_allowed_origins,
    redact_sensitive_text,
)
from ...redis_channels import (
    AUTH_EVENTS_CHANNEL,
    HOOKS_CHANNEL,
    JOB_EVENTS_CHANNEL,
    KEY_PREFIX,
    WEBSOCKET_STREAM_CHANNEL,
)
from ...database.factory import MultiSiteDatabaseManager
from ...database.threads import run_database_call
from ..dependencies import get_backend_instance, get_config
from ...redis_events import JsonValue, publish_ws_event
from . import websocket_topics as topics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["websocket"])

# Active WebSocket connections
active_connections: Set[WebSocket] = set()
# Topics each connection subscribed to; absent means it receives everything.
connection_topics: dict[WebSocket, frozenset[str]] = {}
_job_changes = topics.JobChangeCoalescer()

# Track background tasks for cleanup
_event_broadcasting_task: asyncio.Task[None] | None = None
_event_broadcasting_started: bool = False
_job_change_task: asyncio.Task[None] | None = None
_status_broadcasting_task: asyncio.Task[None] | None = None
_status_broadcasting_started: bool = False

STATUS_BROADCAST_INTERVAL = 15.0
LOG_HISTORY_SEND_LIMIT = 1000


def _redis_pubsub(redis_manager: object) -> Any:
    client = getattr(redis_manager, "client", None)
    return cast(Any, client).pubsub()


def _normalize_pubsub_message(message: object) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None
    return cast(dict[str, Any], message)


def _resolve_ws_security() -> tuple[list[str], str | None, bool]:
    defaults = [
        "http://127.0.0.1:21120",
        "http://localhost:21120",
        "http://127.0.0.1:2112",
        "http://localhost:2112",
        "http://127.0.0.1:21121",
        "http://localhost:21121",
    ]
    token_required = True
    try:
        config = get_config()
        security_cfg = getattr(getattr(config, "settings", None), "security", None)
        if security_cfg and security_cfg.ws_allowed_origins:
            defaults = list(security_cfg.ws_allowed_origins)
        elif security_cfg and security_cfg.allowed_origins:
            defaults = list(security_cfg.allowed_origins)
        if security_cfg is not None:
            token_required = bool(getattr(security_cfg, "admin_token_required", True))
    except Exception as exc:
        logger.warning("Failed to load websocket allowed origins from config: %s", exc)
    return (
        get_ws_allowed_origins(defaults),
        get_allowed_origin_regex(None),
        token_required,
    )


def _extract_ws_admin_token(websocket: WebSocket) -> str | None:
    header_token = extract_admin_token(websocket.headers)
    if header_token:
        return header_token
    for key in ("token", "admin_token", "x_usr_admin_token"):
        candidate = websocket.query_params.get(key)
        if candidate and candidate.strip():
            return candidate.strip()
    return None


async def broadcast_event(event_data: dict[str, JsonValue]) -> None:
    """Broadcast event to all connected WebSocket clients.

    Args:
        event_data: Event data to broadcast
    """
    if not active_connections:
        return

    event_topics = topics.event_topics(event_data)
    recipients = [
        websocket for websocket in list(active_connections)
        if topics.wants(connection_topics.get(websocket), event_topics)
    ]
    if not recipients:
        return
    message = json.dumps(event_data)
    disconnected: set[WebSocket] = set()

    for websocket in recipients:
        try:
            # Check if websocket is in a valid state before sending
            # FastAPI WebSocket client_state: WebSocketState.CONNECTED = ready to send
            if hasattr(websocket, "client_state"):
                from starlette.websockets import WebSocketState

                if websocket.client_state != WebSocketState.CONNECTED:
                    disconnected.add(websocket)
                    continue

            await websocket.send_text(message)
        except Exception as e:
            logger.debug(f"Failed to send to websocket: {e}")
            disconnected.add(websocket)

    # Remove disconnected clients
    for ws in disconnected:
        active_connections.discard(ws)
        connection_topics.pop(ws, None)


@router.websocket("/events")
async def websocket_events(
    websocket: WebSocket,
    # Note: Depends() doesn't work well with WebSocket, so we get backend manually
) -> None:
    """WebSocket endpoint for real-time event streaming.

    Clients connect here to receive:
    - Job status changes
    - Task progress updates
    - Download events
    - System notifications

    Events are broadcast from:
    - Local event bus (single-machine mode)
    - Redis pub/sub (distributed mode)
    """
    origin = websocket.headers.get("origin")
    token = _extract_ws_admin_token(websocket)
    allowed_origins, allowed_origin_regex, token_required = _resolve_ws_security()
    from ... import cluster_clients

    await cluster_clients.refresh_for(token)
    try:
        ensure_ws_admin_auth(
            origin=origin,
            token=token,
            allowed_origins=allowed_origins,
            allowed_origin_regex=allowed_origin_regex,
            token_required=token_required,
        )
    except HTTPException as exc:
        logger.warning("WebSocket auth/origin rejected: %s", exc.detail)
        await websocket.close(code=1008, reason=str(exc.detail))
        return

    await websocket.accept()
    active_connections.add(websocket)

    try:
        backend = await get_backend_instance()
        await _send_log_history(websocket, backend)

        logger.info(f"WebSocket client connected (total: {len(active_connections)})")

        # Keep connection alive and handle incoming messages
        while True:
            # Receive messages (clients can send ping/subscription requests)
            data = await websocket.receive_text()

            try:
                parsed = json.loads(data)
                msg_type = parsed.get("type", "")

                # Handle ping messages - respond with pong to keep connection alive
                # This is critical for mobile browsers that aggressively close idle connections
                if msg_type == "ping":
                    await websocket.send_json(
                        {
                            "type": "pong",
                            "timestamp": parsed.get("timestamp"),
                            "server_time": asyncio.get_event_loop().time(),
                        }
                    )
                    continue

                if msg_type == "subscribe":
                    connection_topics[websocket] = topics.valid_topics(parsed.get("topics"))
                    await websocket.send_json(
                        {"type": "subscribed", "topics": sorted(connection_topics[websocket])}
                    )
                    continue

                # Echo back other messages as acknowledgment
                await websocket.send_json(
                    {
                        "type": "ack",
                        "data": data,
                    }
                )
            except json.JSONDecodeError:
                # Not JSON, echo raw data as ack
                await websocket.send_json(
                    {
                        "type": "ack",
                        "data": data,
                    }
                )

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")

    except RuntimeError as e:
        # Starlette raises RuntimeError when sending after close frame
        logger.debug(f"WebSocket closed during send: {e}")

    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)

    finally:
        active_connections.discard(websocket)
        connection_topics.pop(websocket, None)
        logger.info(f"WebSocket client removed (remaining: {len(active_connections)})")


def detect_log_category(message: str, module: str = "", logger_name: str = "") -> str:
    """Detect log category based on message content, module, and logger name.

    Args:
        message: Log message text
        module: Module name where log originated
        logger_name: Logger name

    Returns:
        Category string (jobs, tasks, workers, scraping, downloads, system)
    """
    msg_lower = message.lower()
    module_lower = module.lower()
    logger_lower = logger_name.lower()

    # Job-related logs
    if any(kw in msg_lower for kw in ["job", "claimed job", "routing", "unclaimed"]):
        return "jobs"

    # Task-related logs
    if any(kw in msg_lower for kw in ["task", "claimed task", "executing", "inflight"]):
        return "tasks"

    # Worker-related logs
    if any(kw in msg_lower for kw in ["worker", "heartbeat", "pool"]):
        return "workers"

    # Scraping-related logs
    if any(
        kw in msg_lower
        for kw in ["scrape", "scraping", "fetching", "api request", "metadata"]
    ):
        return "scraping"

    # Download-related logs
    if any(
        kw in msg_lower
        for kw in ["download", "downloading", "media", "file saved", "drm"]
    ):
        return "downloads"

    # Check module/logger names
    if "job" in module_lower or "job" in logger_lower:
        return "jobs"
    if "worker" in module_lower or "worker" in logger_lower:
        return "workers"
    if "scraper" in module_lower or "scraper" in logger_lower:
        return "scraping"
    if "download" in module_lower or "download" in logger_lower:
        return "downloads"

    # Default category
    return "system"


def _build_ws_log_event(event_data: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    msg_text = redact_sensitive_text(str(event_data.get("message", "")))
    module = str(event_data.get("module", ""))
    logger_name = str(event_data.get("logger", ""))

    category = str(
        event_data.get("category") or detect_log_category(msg_text, module, logger_name)
    )

    return {
        "type": "log",
        "level": str(event_data.get("level", "info")).lower(),
        "message": msg_text,
        "source": str(event_data.get("process", "unknown")),
        "category": category,
        "timestamp": str(event_data.get("timestamp", "")),
        "logger": logger_name,
        "module": module,
        "function": str(event_data.get("function", "")),
    }


async def _send_log_history(
    websocket: WebSocket,
    backend: BackendInstance,
) -> None:
    if backend.redis is None or not backend.redis.is_connected:
        return

    limit = min(ApiRedisManager.LOGS_HISTORY_MAX, LOG_HISTORY_SEND_LIMIT)
    if limit <= 0:
        return

    try:
        raw_entries = await backend.redis.lrange(
            ApiRedisManager.LOGS_HISTORY_KEY, 0, limit - 1
        )
    except Exception as exc:
        logger.debug("Failed to fetch log history: %s", exc)
        return

    if not raw_entries:
        return

    for raw in reversed(raw_entries):
        try:
            event_payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(event_payload, dict):
            continue
        event_data = cast(dict[str, JsonValue], event_payload)
        await websocket.send_json(_build_ws_log_event(event_data))


async def subscribe_to_redis_logs(backend: BackendInstance) -> None:
    """Background task that subscribes to Redis logs and broadcasts to WebSocket clients.

    This task runs continuously, listening to the Redis 'ultimascraper:logs' channel
    and forwarding log messages to all connected WebSocket clients.

    Args:
        backend: Backend instance with Redis client
    """
    if not backend.redis or not backend.redis.is_connected:
        logger.warning("Redis not connected, cannot subscribe to logs")
        return

    try:
        # Subscribe to Redis logs channel
        pubsub = await backend.redis.subscribe("ultimascraper:logs")
        if not pubsub:
            logger.error("Failed to create Redis pubsub for logs")
            return

        logger.info("📡 Subscribed to Redis logs channel (ultimascraper:logs)")

        # Listen for messages
        while True:
            try:
                raw_message = await cast(Any, pubsub).get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                message = _normalize_pubsub_message(raw_message)
                if message is not None and message.get("type") == "message":
                    # Parse log event
                    payload = message.get("data")
                    if not isinstance(payload, (str, bytes, bytearray)):
                        continue
                    data = cast(dict[str, JsonValue], json.loads(payload))

                    msg_text = str(data.get("message", ""))
                    module = str(data.get("module", ""))
                    logger_name = str(data.get("logger", ""))

                    # Detect category
                    category_raw = data.get("category")
                    category = (
                        str(category_raw)
                        if isinstance(category_raw, str) and category_raw
                        else detect_log_category(msg_text, module, logger_name)
                    )

                    # Forward to WebSocket clients with 'log' event type
                    await broadcast_event(
                        {
                            "type": "log",
                            "level": str(data.get("level", "info")).lower(),
                            "message": msg_text,
                            "source": str(data.get("process", "unknown")),
                            "category": category,
                            "timestamp": str(data.get("timestamp", "")),
                            "logger": logger_name,
                            "module": module,
                            "function": str(data.get("function", "")),
                        }
                    )
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing Redis log message: {e}", exc_info=True)
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        logger.info("Redis log subscriber task cancelled")
        raise
    except Exception as e:
        logger.error(f"Redis log subscriber error: {e}", exc_info=True)


async def subscribe_to_redis_job_events(backend: BackendInstance) -> None:
    """Background task that subscribes to Redis job events and broadcasts to WebSocket clients.

    This task runs continuously, listening to the Redis job_events and hooks channels
    and forwarding events to all connected WebSocket clients.

    Args:
        backend: Backend instance with Redis client
    """
    if not backend.redis or not backend.redis.is_connected:
        logger.warning("Redis not connected, cannot subscribe to job events")
        return

    pubsub = None
    try:
        # Subscribe to job events and hooks channels
        pubsub = _redis_pubsub(backend.redis)
        await pubsub.subscribe(
            AUTH_EVENTS_CHANNEL,
            JOB_EVENTS_CHANNEL,
            HOOKS_CHANNEL,
            WEBSOCKET_STREAM_CHANNEL,
        )

        logger.info(
            "📡 Subscribed to Redis events (%s, %s, %s, %s)",
            AUTH_EVENTS_CHANNEL,
            JOB_EVENTS_CHANNEL,
            HOOKS_CHANNEL,
            WEBSOCKET_STREAM_CHANNEL,
        )

        # Listen for messages
        async for raw_message in pubsub.listen():
            # Pub/sub stays subscribed so a newly connected client needs no
            # resubscription. With no local recipients, broadcast_event would
            # discard this event anyway; avoid decoding and filtering it first.
            # There is no await between this check and broadcast_event's check.
            if not active_connections:
                continue
            try:
                message = _normalize_pubsub_message(raw_message)
                if message is not None and message.get("type") == "message":
                    channel = message.get("channel")
                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8")
                    elif not isinstance(channel, str):
                        continue

                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    if not isinstance(data, str):
                        continue

                    event_data = cast(dict[str, JsonValue], json.loads(data))

                    # Filter out internal API command jobs from WebSocket stream
                    # This prevents them from appearing in the dashboard UI
                    # - "api" (literal): Fallback when USAPI is used without job context
                    # - "api-*": Published by backend API command wrapper (e.g., API Explorer)
                    # - "test-*": Test jobs
                    # Note: USAPI now accepts job_id parameter, so real jobs pass their ID through
                    job_id = event_data.get("job_id") or event_data.get("id")
                    if job_id and isinstance(job_id, str):
                        job_id_clean = job_id.lower().strip()
                        if (
                            job_id_clean == "api"
                            or job_id_clean.startswith("api-")
                            or job_id_clean.startswith("test-")
                        ):
                            continue

                    logger.debug(
                        f"Job event received on {channel}: {event_data.get('event', 'unknown')}"
                    )

                    if topics.is_task_activity(event_data):
                        _job_changes.add(event_data)
                    # Forward to WebSocket clients (per-task events reach job:<id> only)
                    await broadcast_event(event_data)

            except Exception as e:
                logger.error(f"Error processing Redis job event: {e}", exc_info=True)

    except asyncio.CancelledError:
        logger.info("Redis job events subscriber task cancelled")
        raise
    except Exception as e:
        logger.error(f"Redis job events subscriber error: {e}", exc_info=True)
    finally:
        # Clean up pubsub connection
        if pubsub:
            try:
                await pubsub.unsubscribe()
                await pubsub.close()
            except Exception:
                pass
        logger.info("Redis job events pubsub connection closed")


async def flush_job_changes(backend: BackendInstance) -> None:
    """Send coalesced job changes, and tell workers whether anyone watches API hooks."""
    watched = False
    refreshed_at = 0.0
    while True:
        await asyncio.sleep(topics.JOB_CHANGE_INTERVAL_SECONDS)
        try:
            for change in _job_changes.drain():
                await broadcast_event(change)
            now = asyncio.get_running_loop().time()
            watching = any("api_hooks" in subscribed for subscribed in connection_topics.values())
            redis = backend.redis
            if redis is None or not redis.is_connected:
                continue
            if watching and (not watched or now - refreshed_at >= topics.API_HOOK_WATCHERS_TTL_SECONDS / 3):
                await redis.client.set(
                    topics.API_HOOK_WATCHERS_KEY, "1", ex=topics.API_HOOK_WATCHERS_TTL_SECONDS
                )
                refreshed_at = now
            elif watched and not watching:
                await redis.client.delete(topics.API_HOOK_WATCHERS_KEY)
            watched = watching
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Could not flush coalesced job changes", exc_info=True)


async def setup_event_broadcasting(
    backend: BackendInstance,
) -> asyncio.Task[None] | None:
    """Setup event broadcasting from Redis to WebSocket clients.

    Starts background tasks that subscribe to Redis channels and forward events.
    Only starts once - subsequent calls are ignored.

    Args:
        backend: Backend instance

    Returns:
        The background task, or None if already started
    """
    global _event_broadcasting_task, _event_broadcasting_started

    if _event_broadcasting_started:
        logger.debug("Event broadcasting already started, skipping")
        return None

    _event_broadcasting_started = True

    # Start Redis job events subscription as a background task
    _event_broadcasting_task = asyncio.create_task(
        subscribe_to_redis_job_events(backend)
    )
    global _job_change_task
    _job_change_task = asyncio.create_task(flush_job_changes(backend))

    logger.info(
        "Event broadcasting setup complete (job_events + hooks via Redis pub/sub)"
    )
    return _event_broadcasting_task


async def _build_queue_snapshot(backend: BackendInstance) -> dict[str, JsonValue]:
    event: dict[str, JsonValue] = {
        "type": "queue_depth",
        "backend_id": backend.instance_id,
    }

    if backend.redis is None or not backend.redis.is_connected:
        event["redis_connected"] = False
        return event

    unclaimed_key = f"{KEY_PREFIX}:jobs:unclaimed"
    unclaimed = await backend.redis.llen(unclaimed_key)

    claimed_keys = await backend.redis.keys(f"{KEY_PREFIX}:jobs:claimed:*")
    claimed_counts: dict[str, JsonValue] = {}
    for key in claimed_keys:
        key_str = str(key)
        backend_id = key_str.split(":")[-1]
        claimed_counts[backend_id] = await backend.redis.llen(key_str)

    ready_keys = await backend.redis.keys(f"{KEY_PREFIX}:ready:*")
    ready_counts: dict[str, JsonValue] = {}
    for key in ready_keys:
        key_str = str(key)
        if ":notify" in key_str:
            continue
        backend_id = key_str.split(":")[-1]
        ready_counts[backend_id] = await cast(Any, backend.redis.client).zcard(key_str)

    event.update(
        {
            "redis_connected": True,
            "queues": {
                "unclaimed": unclaimed,
                "claimed": claimed_counts,
                "ready_tasks": ready_counts,
            },
        }
    )
    return event


def _collect_drive_usage(paths: list[Path]) -> list[dict[str, JsonValue]]:
    results: list[dict[str, JsonValue]] = []
    seen: set[str] = set()
    for raw_path in paths:
        path = raw_path.expanduser()
        try:
            path = path.resolve()
        except Exception:
            path = path.absolute()
        if path.as_posix() in seen:
            continue
        seen.add(path.as_posix())

        if not path.exists():
            continue

        usage = shutil.disk_usage(path)
        percent_used = 0.0
        if usage.total:
            percent_used = round((usage.used / usage.total) * 100, 2)
        results.append(
            {
                "path": str(path),
                "total_bytes": usage.total,
                "used_bytes": usage.used,
                "free_bytes": usage.free,
                "percent_used": percent_used,
            }
        )
    return results


async def _build_system_health_snapshot(
    backend: BackendInstance,
) -> dict[str, JsonValue]:
    config = await run_database_call(
        UltimaScraperCollectionConfig.load_or_create_default_config
    )
    drive_paths: list[Path] = []
    for drive in config.settings.download_setup.drives:
        drive_paths.extend(drive.directories)
    for drive in config.settings.metadata_setup.drives:
        drive_paths.extend(drive.directories)

    # Check DB connectivity in a thread to avoid blocking the event loop.
    # auto_connect=False avoids synchronous SQLite PRAGMA calls that can hold
    # a write lock for up to busy_timeout (30s) when workers are writing.
    def _check_db_status() -> dict[str, JsonValue]:
        db_manager = MultiSiteDatabaseManager()
        result: dict[str, JsonValue] = {}
        for site_name in config.settings.worker_pool.sites:
            site_db = db_manager.get_or_create_manager(site_name, auto_connect=False)
            result[site_name] = bool(site_db)
        return result

    db_status = await asyncio.to_thread(_check_db_status)

    drives = await asyncio.to_thread(_collect_drive_usage, drive_paths)

    drives_payload: list[JsonValue] = [cast(JsonValue, drive) for drive in drives]
    return {
        "type": "system_health",
        "backend_id": backend.instance_id,
        "redis_connected": bool(backend.redis and backend.redis.is_connected),
        "drives": drives_payload,
        "db_connected": db_status,
    }


async def _status_broadcast_loop(backend: BackendInstance) -> None:
    while True:
        try:
            queue_event = await _build_queue_snapshot(backend)
            await publish_ws_event(backend.redis, queue_event)

            system_event = await _build_system_health_snapshot(backend)
            await publish_ws_event(backend.redis, system_event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Status broadcast loop error: %s", exc)
        await asyncio.sleep(STATUS_BROADCAST_INTERVAL)


async def setup_status_broadcasting(
    backend: BackendInstance,
) -> asyncio.Task[None] | None:
    global _status_broadcasting_task, _status_broadcasting_started

    if _status_broadcasting_started:
        logger.debug("Status broadcasting already started, skipping")
        return None

    _status_broadcasting_started = True
    _status_broadcasting_task = asyncio.create_task(_status_broadcast_loop(backend))
    logger.info("Status broadcasting setup complete (queue + system health)")
    return _status_broadcasting_task


async def stop_event_broadcasting() -> None:
    """Stop the event broadcasting background task."""
    global _event_broadcasting_task, _event_broadcasting_started

    if _event_broadcasting_task:
        _event_broadcasting_task.cancel()
        try:
            await _event_broadcasting_task
        except asyncio.CancelledError:
            pass
        _event_broadcasting_task = None

    _event_broadcasting_started = False


async def stop_status_broadcasting() -> None:
    global _status_broadcasting_task, _status_broadcasting_started

    if _status_broadcasting_task:
        _status_broadcasting_task.cancel()
        try:
            await _status_broadcasting_task
        except asyncio.CancelledError:
            pass
        _status_broadcasting_task = None

    _status_broadcasting_started = False
    logger.info("Event broadcasting stopped")
