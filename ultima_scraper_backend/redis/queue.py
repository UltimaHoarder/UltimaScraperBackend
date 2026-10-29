"""Redis queue operations for job dispatch and task execution."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class RedisQueue:
    """Simple Redis queue/stream operations for job dispatch."""

    def __init__(self, redis_client: Any, key_prefix: str = "ultimascraper"):
        """Initialize queue with Redis client.
        
        Args:
            redis_client: Redis async client instance
            key_prefix: Key prefix for queue keys
        """
        self.client = redis_client
        self.key_prefix = key_prefix

    async def enqueue(self, queue_name: str, data: dict[str, Any]) -> None:
        """Push a job/task to a queue (LPUSH).
        
        Args:
            queue_name: Name of the queue
            data: Data to enqueue (will be JSON serialized)
        """
        key = f"{self.key_prefix}:{queue_name}"
        payload = json.dumps(data)
        await self.client.lpush(key, payload)
        logger.debug(f"Enqueued to {key}: {data.get('job_id', 'unknown')}")

    async def dequeue(self, queue_name: str, timeout: int = 0) -> Optional[dict[str, Any]]:
        """Pop an item from a queue (BRPOP).
        
        Args:
            queue_name: Name of the queue
            timeout: Blocking timeout in seconds (0 = block forever)
            
        Returns:
            Dequeued data or None if timeout
        """
        key = f"{self.key_prefix}:{queue_name}"
        result = await self.client.brpop(key, timeout=timeout)
        if result:
            _, payload = result
            return json.loads(payload)
        return None

    async def queue_length(self, queue_name: str) -> int:
        """Get the length of a queue.
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            Number of items in queue
        """
        key = f"{self.key_prefix}:{queue_name}"
        return await self.client.llen(key)
