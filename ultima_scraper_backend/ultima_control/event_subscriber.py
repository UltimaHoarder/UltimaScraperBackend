"""Centralized event subscriber (Tier 2).

Manages a single shared pubsub connection for all event consumers.
Provides event routing, handler registration, and automatic reconnection.

Architecture:
    EventSubscriber (this) → RedisManager.subscribe() → redis.asyncio.PubSub

Usage:
    subscriber = EventSubscriber.get_instance()
    await subscriber.start()

    # Register handlers
    subscriber.register_handler("ultimascraper:hooks", my_hook_handler)
    subscriber.register_handler("ultimascraper:logs", my_log_handler)

    # Handlers receive: async def handler(channel: str, event: BaseJobEvent) -> None
    # Events are validated Pydantic models, not raw dicts!
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import (
    Any,
    Awaitable,
    Callable,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from ultima_scraper_api.managers.redis import get_redis

from ultima_scraper_backend.job_system.events import (
    APICommandEvent,
    BaseJobEvent,
    JobProgressEvent,
    LogMessageEvent,
    MediaStatusEvent,
    OperationProgressEvent,
    ReloadUserDataEvent,
    SubscriptionSyncCompleteEvent,
    TaskProgressEvent,
    TaskStatusEvent,
    WorkerStatusEvent,
)

logger = logging.getLogger(__name__)

# Generic type for specific event handlers
T = TypeVar("T", bound=BaseJobEvent)

# Type alias for event handlers - receives typed event, not raw dict!
EventHandler = Callable[[str, BaseJobEvent], Awaitable[None]]


def _extract_event_type_from_handler(
    handler: EventHandler,
) -> type[BaseJobEvent] | None:
    """Extract event type from handler's type annotation.

    Inspects the handler function signature to determine what event type
    it expects. Returns None if handler accepts generic BaseJobEvent.

    Args:
        handler: Handler function to inspect

    Returns:
        Event type class if handler is type-specific, None if generic

    Examples:
        async def handle_downloads(channel: str, event: OperationProgressEvent):
            ...
        # Returns: OperationProgressEvent

        async def handle_all(channel: str, event: BaseJobEvent):
            ...
        # Returns: None (accepts all events)
    """
    try:
        # Get type hints from the handler function
        hints = get_type_hints(handler)

        # Look for 'event' parameter (second parameter after 'channel')
        event_type = hints.get("event")

        if event_type is None:
            return None

        # Check if it's a Union type (e.g., BaseJobEvent | None)
        origin = get_origin(event_type)
        if origin is not None:
            # Get the actual types from Union
            args = get_args(event_type)
            # Find the BaseJobEvent subclass (ignore None)
            for arg in args:
                if (
                    arg is not type(None)
                    and isinstance(arg, type)
                    and issubclass(arg, BaseJobEvent)
                ):
                    event_type = arg
                    break

        # If it's BaseJobEvent itself, return None (no filtering needed)
        if event_type is BaseJobEvent:
            return None

        # If it's a specific subclass, return it for filtering
        if isinstance(event_type, type) and issubclass(event_type, BaseJobEvent):
            return event_type  # type: ignore[return-value]

        return None

    except Exception as e:
        logger.debug(f"Could not extract event type from handler: {e}")
        return None


# Internal wrapper that stores handler + optional event type filter
class _HandlerWrapper:
    """Wraps a handler function with optional event type filtering."""

    def __init__(
        self,
        handler: EventHandler,
        event_type: type[BaseJobEvent] | None = None,
    ):
        self.handler = handler
        self.event_type = event_type

    async def __call__(self, channel: str, event: BaseJobEvent) -> None:
        """Call handler only if event matches the specified type."""
        # If no type filter, call handler for all events
        if self.event_type is None:
            await self.handler(channel, event)
            return

        # If type filter specified, only call if event matches
        if isinstance(event, self.event_type):
            await self.handler(channel, event)  # type: ignore[arg-type]


# Map event_type strings to Pydantic model classes
# This is auto-generated from the event class definitions to avoid redundancy
def _build_event_type_map() -> dict[str, type[BaseJobEvent]]:
    """Build EVENT_TYPE_MAP from event class Literal annotations.

    For consolidated events like TaskStatusEvent with Literal["task_started", "task_completed", "task_failed"],
    this automatically creates entries for all three strings pointing to the same class.
    """
    from typing import Literal, get_args, get_origin

    event_map: dict[str, type[BaseJobEvent]] = {}

    # All event classes to process
    event_classes = [
        TaskProgressEvent,
        OperationProgressEvent,
        APICommandEvent,
        TaskStatusEvent,
        JobProgressEvent,
        WorkerStatusEvent,
        MediaStatusEvent,
        SubscriptionSyncCompleteEvent,
        LogMessageEvent,
        ReloadUserDataEvent,
    ]

    for event_class in event_classes:
        # Get the 'event' field annotation
        event_field = event_class.model_fields.get("event")
        if not event_field or not event_field.annotation:
            continue

        # Extract Literal values
        origin = get_origin(event_field.annotation)
        if origin is Literal:
            # Multiple event strings for consolidated classes
            event_strings = get_args(event_field.annotation)
            for event_str in event_strings:
                event_map[event_str] = event_class
        else:
            # Single event string - extract from default value
            default = event_field.default
            if isinstance(default, str):
                event_map[default] = event_class

    return event_map


EVENT_TYPE_MAP: dict[str, type[BaseJobEvent]] = _build_event_type_map()


class EventSubscriber:
    """Centralized event subscriber with handler registry.

    Singleton pattern - manages a single pubsub connection shared
    by all event consumers in the application.
    """

    _instance: EventSubscriber | None = None

    def __init__(self) -> None:
        """Initialize event subscriber.

        Note: Use get_instance() instead of direct instantiation.
        """
        self._handlers: dict[str, list[_HandlerWrapper]] = {}
        self._pubsub: Any = None  # redis.asyncio.client.PubSub
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._reconnect_delay = 1.0  # Start with 1 second
        self._max_reconnect_delay = 60.0  # Max 1 minute

    @classmethod
    def get_instance(cls) -> EventSubscriber:
        """Get or create the singleton instance.

        Returns:
            EventSubscriber singleton
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register_handler(
        self,
        channel: str,
        handler: EventHandler,
        event_type: type[BaseJobEvent] | None = None,
    ) -> None:
        """Register an event handler for a specific channel.

        Automatically detects the event type from the handler's type annotation.
        If handler signature is `async def handler(channel: str, event: OperationProgressEvent)`,
        it will automatically filter to only pass OperationProgressEvent instances.

        Args:
            channel: Redis channel name (e.g., "ultimascraper:hooks")
            handler: Async function that receives (channel, event: EventType)
                    Event is a validated Pydantic model, not a raw dict!
            event_type: Optional explicit event type override (rarely needed).
                       If omitted, will be auto-detected from handler signature.

        Examples:
            # Auto-detection from signature (RECOMMENDED) ✅
            async def handle_downloads(channel: str, event: OperationProgressEvent):
                ...
            subscriber.register_handler(HOOKS_CHANNEL, handle_downloads)
            # 🎯 Automatically filters for OperationProgressEvent!

            # Handle all events (generic BaseJobEvent in signature)
            async def handle_all(channel: str, event: BaseJobEvent):
                ...
            subscriber.register_handler(HOOKS_CHANNEL, handle_all)
            # No filtering - receives all events

            # Explicit override (rarely needed)
            subscriber.register_handler(
                HOOKS_CHANNEL,
                handle_downloads,
                event_type=OperationProgressEvent  # Explicit override
            )
        """
        if channel not in self._handlers:
            self._handlers[channel] = []

        # Auto-detect event type from handler signature if not explicitly provided
        if event_type is None:
            event_type = _extract_event_type_from_handler(handler)

        # Wrap handler with type filter
        wrapper = _HandlerWrapper(handler, event_type)

        # Check if this exact handler is already registered (compare unwrapped handler)
        if not any(w.handler == handler for w in self._handlers[channel]):
            self._handlers[channel].append(wrapper)
            type_info = (
                f" (auto-filtering for {event_type.__name__})"
                if event_type
                else " (no filtering - all events)"
            )
            logger.debug(f"Registered handler for channel: {channel}{type_info}")

    def get_subscribed_channels(self) -> list[str]:
        """Get list of channels with registered handlers.

        Returns:
            List of channel names
        """
        return list(self._handlers.keys())

    async def start(self) -> bool:
        """Start the event subscriber loop.

        Returns:
            True if started successfully
        """
        if self._running:
            logger.warning("EventSubscriber already running")
            return True

        self._running = True
        self._task = asyncio.create_task(self._subscribe_loop())
        logger.info("EventSubscriber started")
        return True

    async def stop(self) -> None:
        """Stop the event subscriber loop."""
        if not self._running:
            return

        self._running = False

        if self._pubsub:
            try:
                await self._pubsub.unsubscribe()  # type: ignore
                await self._pubsub.close()
            except Exception as e:
                logger.debug(f"Error closing pubsub: {e}")
            self._pubsub = None

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("EventSubscriber stopped")

    async def _subscribe_loop(self) -> None:
        """Main subscription loop with automatic reconnection."""
        while self._running:
            try:
                await self._subscribe_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"EventSubscriber error: {e}", exc_info=True)

            if self._running:
                logger.info(
                    f"EventSubscriber reconnecting in {self._reconnect_delay}s..."
                )
                await asyncio.sleep(self._reconnect_delay)

                # Exponential backoff with max cap
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

    async def _subscribe_once(self) -> None:
        """Single subscription attempt - subscribes and processes messages."""
        redis = get_redis()
        if not redis or not redis.is_connected:
            logger.warning("Redis not available for EventSubscriber")
            await asyncio.sleep(5)
            return

        # Get channels to subscribe to
        channels = self.get_subscribed_channels()
        if not channels:
            logger.debug("No channels registered, EventSubscriber idle")
            await asyncio.sleep(5)
            return

        # Subscribe to all registered channels
        self._pubsub = await redis.subscribe(*channels)
        if not self._pubsub:
            logger.error("Failed to create pubsub subscription")
            return

        logger.info(f"EventSubscriber connected to channels: {channels}")

        # Reset reconnect delay on successful connection
        self._reconnect_delay = 1.0

        # Process messages
        while self._running:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )

                if message is None:
                    continue

                channel = message.get("channel")
                data = message.get("data")

                if not channel or not data:
                    continue

                # Decode channel if bytes
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8")

                # Parse JSON data
                try:
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    event_data = json.loads(data) if isinstance(data, str) else data
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse event JSON: {e}")
                    continue

                # Route to handlers
                await self._route_event(channel, event_data)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                break  # Break to trigger reconnect

    async def _route_event(self, channel: str, event_data: dict[str, Any]) -> None:
        """Route event to registered handlers.

        Parses raw dict into typed Pydantic event models before dispatching.

        Args:
            channel: Channel the event came from
            event_data: Parsed event data (raw dict from Redis)
        """
        handlers = self._handlers.get(channel, [])

        if not handlers:
            logger.debug(f"No handlers registered for channel: {channel}")
            return

        # Parse dict into typed event model
        try:
            event = self._parse_event(event_data)
        except Exception as e:
            logger.error(
                f"Failed to parse event from channel {channel}: {e}\n"
                f"Event data: {event_data}",
                exc_info=True,
            )
            return

        # Call all handlers concurrently with typed event
        tasks = [handler(channel, event) for handler in handlers]

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error dispatching event to handlers: {e}", exc_info=True)

    def _parse_event(self, event_data: dict[str, Any]) -> BaseJobEvent:
        """Parse raw event dict into typed Pydantic model.

        Args:
            event_data: Raw event data from Redis

        Returns:
            Validated event instance

        Raises:
            ValueError: If event type is unknown or validation fails
        """
        event_type = event_data.get("event")

        if not event_type:
            raise ValueError(f"Event missing 'event' field: {event_data}")

        event_class = EVENT_TYPE_MAP.get(event_type)

        if not event_class:
            raise ValueError(
                f"Unknown event type '{event_type}'. "
                f"Known types: {list(EVENT_TYPE_MAP.keys())}"
            )

        # Pydantic will validate the data
        try:
            return event_class.model_validate(event_data)
        except Exception as e:
            raise ValueError(
                f"Failed to validate {event_type} event: {e}\n"
                f"Event data: {event_data}"
            ) from e

    @property
    def is_running(self) -> bool:
        """Check if subscriber is running.

        Returns:
            True if running
        """
        return self._running

    @property
    def handler_count(self) -> int:
        """Get total number of registered handlers.

        Returns:
            Handler count
        """
        return sum(len(handlers) for handlers in self._handlers.values())


# =============================================================================
# Convenience Functions
# =============================================================================
