"""Event bus abstractions for pub/sub messaging.

Provides interface for event buses that work with or without Redis:
- InMemoryEventBus: For single-machine deployments (callbacks only)
- RedisEventBus: For distributed deployments (Redis pub/sub)
"""

from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, cast

if TYPE_CHECKING:
    from ..redis.client import RedisManager
    from .events import BaseJobEvent

logger = logging.getLogger(__name__)

EventPayload = dict[str, Any]
EventCallback = Callable[[EventPayload], Any]


def _event_to_dict(event: BaseJobEvent | EventPayload) -> EventPayload:
    if isinstance(event, dict):
        return event
    return event.model_dump()


class EventBus(ABC):
    """Abstract base class for event buses."""

    @abstractmethod
    async def publish(self, channel: str, event: BaseJobEvent | EventPayload) -> None:
        """Publish an event to a channel.
        
        Args:
            channel: Channel name
            event: Event to publish (Pydantic model or dict)
        """
        ...

    @abstractmethod
    async def subscribe(self, channel: str, callback: EventCallback) -> None:
        """Subscribe to a channel with a callback.
        
        Args:
            channel: Channel name
            callback: Function to call when event received
        """
        ...

    @abstractmethod
    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel.
        
        Args:
            channel: Channel name
        """
        ...


class InMemoryEventBus(EventBus):
    """In-memory event bus for single-machine deployments.
    
    Directly calls subscriber callbacks when events published.
    No network overhead, but events only visible to local process.
    """

    def __init__(self) -> None:
        """Initialize empty event bus."""
        self._subscribers: dict[str, list[EventCallback]] = {}
        logger.info("InMemoryEventBus initialized (single-machine mode)")

    async def publish(self, channel: str, event: BaseJobEvent | EventPayload) -> None:
        """Publish event to all local subscribers."""
        event_dict = _event_to_dict(event)

        subscribers = self._subscribers.get(channel, [])
        if subscribers:
            logger.debug(f"Publishing to {len(subscribers)} local subscribers on {channel}")
            for callback in subscribers:
                try:
                    # Call callback (may be sync or async)
                    result = callback(event_dict)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.error(f"Error in event callback for {channel}: {e}", exc_info=True)

    async def subscribe(self, channel: str, callback: EventCallback) -> None:
        """Add subscriber to channel."""
        if channel not in self._subscribers:
            self._subscribers[channel] = []
        self._subscribers[channel].append(callback)
        logger.debug(f"Subscribed to channel {channel} (total: {len(self._subscribers[channel])})")

    async def unsubscribe(self, channel: str) -> None:
        """Remove all subscribers from channel."""
        if channel in self._subscribers:
            count = len(self._subscribers[channel])
            del self._subscribers[channel]
            logger.debug(f"Unsubscribed {count} callbacks from {channel}")


class RedisEventBus(EventBus):
    """Redis-backed event bus for distributed deployments.
    
    Uses Redis pub/sub for event distribution.
    Events are broadcast to all backends connected to same Redis instance.
    """

    def __init__(self, redis: RedisManager) -> None:
        """Initialize Redis event bus.
        
        Args:
            redis: Redis client manager
        """
        self.redis = redis
        self._pubsub: Any | None = None
        self._subscribers: dict[str, list[EventCallback]] = {}
        self._listener_task: asyncio.Task[None] | None = None
        logger.info("RedisEventBus initialized (distributed mode)")

    async def _start_listener(self) -> None:
        """Start Redis pubsub listener task."""
        if self._listener_task is not None:
            return

        redis_client: Any = self.redis.client
        self._pubsub = redis_client.pubsub()
        
        async def listener() -> None:
            """Background task that receives Redis messages."""
            pubsub = self._pubsub
            if pubsub is None:
                return
            async for raw_message in pubsub.listen():
                message: EventPayload = raw_message
                if message["type"] == "message":
                    channel_raw = message.get("channel")
                    if isinstance(channel_raw, bytes):
                        channel = channel_raw.decode()
                    elif isinstance(channel_raw, str):
                        channel = channel_raw
                    else:
                        continue

                    data_raw = message.get("data")
                    if isinstance(data_raw, bytes):
                        payload = data_raw.decode()
                    elif isinstance(data_raw, str):
                        payload = data_raw
                    else:
                        continue

                    data = cast(EventPayload, json.loads(payload))
                    
                    # Call all local subscribers for this channel
                    callbacks = self._subscribers.get(channel, [])
                    for callback in callbacks:
                        try:
                            result = callback(data)
                            if asyncio.iscoroutine(result):
                                await result
                        except Exception as e:
                            logger.error(f"Error in Redis event callback for {channel}: {e}", exc_info=True)
        
        self._listener_task = asyncio.create_task(listener())
        logger.info("Redis pubsub listener started")

    async def publish(self, channel: str, event: BaseJobEvent | EventPayload) -> None:
        """Publish event to Redis channel."""
        event_dict = _event_to_dict(event)
        event_json = json.dumps(event_dict)
        await self.redis.publish(channel, event_json)
        logger.debug(f"Published event to Redis channel {channel}")

    async def subscribe(self, channel: str, callback: EventCallback) -> None:
        """Subscribe to Redis channel."""
        # Start listener if not already running
        await self._start_listener()
        pubsub = self._pubsub
        if pubsub is None:
            return

        # Add to local subscribers
        if channel not in self._subscribers:
            self._subscribers[channel] = []
            # Subscribe in Redis
            await pubsub.subscribe(channel)
            logger.debug(f"Subscribed to Redis channel {channel}")
        
        self._subscribers[channel].append(callback)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from Redis channel."""
        if channel in self._subscribers:
            del self._subscribers[channel]
            pubsub = self._pubsub
            if pubsub is not None:
                await pubsub.unsubscribe(channel)
            logger.debug(f"Unsubscribed from Redis channel {channel}")

    async def close(self) -> None:
        """Close Redis pubsub connection."""
        listener_task = self._listener_task
        if listener_task is not None:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass
        
        if self._pubsub:
            await self._pubsub.close()
        
        logger.info("RedisEventBus closed")
