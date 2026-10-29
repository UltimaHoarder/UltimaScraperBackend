"""Queue abstractions for job distribution.

Provides interface for job queues that work with or without Redis:
- InMemoryQueue: For single-machine deployments (no Redis needed)
- RedisQueue: For distributed deployments (multiple backends sharing work)
"""

from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..job_system.models import Job
    from ..redis.client import RedisManager

logger = logging.getLogger(__name__)


class JobQueue(ABC):
    """Abstract base class for job queues."""

    @abstractmethod
    async def enqueue(self, job: Job) -> None:
        """Add a job to the queue.
        
        Args:
            job: Job to enqueue
        """
        ...

    @abstractmethod
    async def dequeue(self, timeout: int = 5) -> Optional[Job]:
        """Remove and return a job from the queue.
        
        Args:
            timeout: How long to wait for a job (seconds)
            
        Returns:
            Job if available, None if timeout
        """
        ...

    @abstractmethod
    async def size(self) -> int:
        """Get the current queue size.
        
        Returns:
            Number of jobs in queue
        """
        ...

    @abstractmethod
    async def clear(self) -> int:
        """Remove all jobs from queue.
        
        Returns:
            Number of jobs cleared
        """
        ...


class InMemoryQueue(JobQueue):
    """In-memory job queue for single-machine deployments.
    
    Uses asyncio.Queue internally. Jobs stay in memory only.
    Fast and simple, but not shared across processes/machines.
    """

    def __init__(self) -> None:
        """Initialize empty in-memory queue."""
        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        logger.info("InMemoryQueue initialized (single-machine mode)")

    async def enqueue(self, job: Job) -> None:
        """Add job to in-memory queue."""
        await self._queue.put(job)
        logger.debug(f"Enqueued job {job.job_id} to memory queue")

    async def dequeue(self, timeout: int = 5) -> Optional[Job]:
        """Get next job from memory queue."""
        try:
            job = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            logger.debug(f"Dequeued job {job.job_id} from memory queue")
            return job
        except asyncio.TimeoutError:
            return None

    async def size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()

    async def clear(self) -> int:
        """Clear all jobs from queue."""
        count = 0
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                count += 1
            except asyncio.QueueEmpty:
                break
        logger.info(f"Cleared {count} jobs from memory queue")
        return count


class RedisQueue(JobQueue):
    """Redis-backed job queue for distributed deployments.
    
    Uses Redis list (LPUSH/BRPOP) for job distribution.
    Jobs are shared across all backends connected to same Redis instance.
    """

    def __init__(self, redis: RedisManager, queue_key: str = "ultimascraper:jobs") -> None:
        """Initialize Redis queue.
        
        Args:
            redis: Redis client manager
            queue_key: Redis key for job queue
        """
        self.redis = redis
        self.queue_key = queue_key
        logger.info(f"RedisQueue initialized (distributed mode, key={queue_key})")

    async def enqueue(self, job: Job) -> None:
        """Add job to Redis queue."""
        job_json = json.dumps(job.to_dict())
        await self.redis.lpush(self.queue_key, job_json)
        logger.debug(f"Enqueued job {job.job_id} to Redis queue")

    async def dequeue(self, timeout: int = 5) -> Optional[Job]:
        """Get next job from Redis queue."""
        from ..job_system.models import Job as JobModel
        
        result = await self.redis.brpop([self.queue_key], timeout=timeout)
        if result is None:
            return None
        
        _, job_json = result
        job_dict = json.loads(job_json)
        job = JobModel.from_dict(job_dict)
        logger.debug(f"Dequeued job {job.job_id} from Redis queue")
        return job

    async def size(self) -> int:
        """Get current Redis queue size."""
        return await self.redis.llen(self.queue_key)

    async def clear(self) -> int:
        """Clear all jobs from Redis queue."""
        count = await self.size()
        if count > 0:
            await self.redis.delete(self.queue_key)
            logger.info(f"Cleared {count} jobs from Redis queue")
        return count
