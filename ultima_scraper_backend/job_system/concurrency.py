"""
Concurrency control for the job system.

This module provides lock management for preventing concurrent operations
that could corrupt data or cause race conditions.

Key Concepts:
- Download Lock: Prevents multiple auths from downloading the same performer's content type
- Scrape Lock: Prevents concurrent scraping of the same performer (future use)

Lock Key Format:
- Download: "{performer_id}:{content_type}" (e.g., "12345:posts")
- Scrape: "{performer_id}:scrape" (e.g., "12345:scrape")

Why This Matters:
- Multiple auths may have access to the same performer
- Without locking, concurrent downloads could write to the same files
- Concurrent writes cause corruption
- Lock at performer:content_type level, not file level (too granular)
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import AsyncIterator

__all__ = [
    "DownloadLockManager",
    "LockError",
    "LockInfo",
    "LockType",
]

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


class LockType(str, Enum):
    """Types of locks in the system."""

    DOWNLOAD = "download"
    SCRAPE = "scrape"


class LockError(Exception):
    """Raised when a lock cannot be acquired."""

    def __init__(self, key: str, holder: str | None = None, message: str | None = None):
        self.key = key
        self.holder = holder
        self.message = message or f"Lock already held for key: {key}"
        super().__init__(self.message)


@dataclass
class LockInfo:
    """Information about an active lock."""

    key: str
    holder: str  # Worker/Auth ID that holds the lock
    acquired_at: datetime
    lock_type: LockType

    @property
    def duration_seconds(self) -> float:
        """How long this lock has been held."""
        return (utcnow() - self.acquired_at).total_seconds()


@dataclass
class DownloadLockManager:
    """
    Manages locks for download operations.

    Prevents concurrent downloads of the same performer's content type
    by different auth accounts. This is critical because:

    1. Multiple auths may subscribe to the same performer
    2. Download paths are based on performer, not auth
    3. Concurrent writes to the same file = corruption

    Usage:
        async with lock_manager.acquire("12345:posts", "auth_1"):
            # Download posts for performer 12345
            ...

    Lock Key Format: "{performer_id}:{content_type}"

    Thread Safety:
    - Uses asyncio.Lock for each key
    - Keys are created on-demand
    - Lock info tracked for debugging/monitoring
    """

    # Lock storage: key -> (asyncio.Lock, LockInfo | None)
    _locks: dict[str, asyncio.Lock] = field(default_factory=lambda: {})
    _lock_info: dict[str, LockInfo] = field(default_factory=lambda: {})
    _meta_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @staticmethod
    def make_key(performer_id: int, content_type: str) -> str:
        """
        Create a lock key from performer ID and content type.

        Args:
            performer_id: The performer's database ID
            content_type: Content type (posts, messages, stories, etc.)

        Returns:
            Lock key string in format "performer_id:content_type"
        """
        return f"{performer_id}:{content_type.lower()}"

    async def _get_or_create_lock(self, key: str) -> asyncio.Lock:
        """Get existing lock or create new one for key."""
        async with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
                logger.debug(f"Created new lock for key: {key}")
            return self._locks[key]

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        holder: str,
        lock_type: LockType = LockType.DOWNLOAD,
        timeout: float | None = None,
    ) -> AsyncIterator[LockInfo]:
        """
        Acquire a lock for the given key.

        Args:
            key: Lock key (e.g., "12345:posts")
            holder: Identifier of who is acquiring the lock (e.g., "auth_123")
            lock_type: Type of lock being acquired
            timeout: Optional timeout in seconds (None = wait forever)

        Yields:
            LockInfo: Information about the acquired lock

        Raises:
            LockError: If lock cannot be acquired within timeout

        Example:
            async with lock_manager.acquire("12345:posts", "auth_1"):
                # Download posts...
        """
        lock = await self._get_or_create_lock(key)

        acquired = False
        try:
            if timeout is not None:
                try:
                    # Use asyncio.wait_for for timeout
                    await asyncio.wait_for(lock.acquire(), timeout=timeout)
                    acquired = True
                except asyncio.TimeoutError:
                    # Check who holds the lock
                    current_holder = self._lock_info.get(key)
                    holder_id = current_holder.holder if current_holder else "unknown"
                    raise LockError(
                        key=key,
                        holder=holder_id,
                        message=f"Timeout acquiring lock for {key} after {timeout}s (held by {holder_id})",
                    ) from None
            else:
                await lock.acquire()
                acquired = True

            # Record lock info
            lock_info = LockInfo(
                key=key,
                holder=holder,
                acquired_at=utcnow(),
                lock_type=lock_type,
            )
            self._lock_info[key] = lock_info
            logger.debug(f"Lock acquired: {key} by {holder}")

            yield lock_info

        finally:
            if acquired:
                # Clear lock info before release
                self._lock_info.pop(key, None)
                lock.release()
                logger.debug(f"Lock released: {key} by {holder}")

    async def try_acquire(
        self,
        key: str,
        holder: str,
        lock_type: LockType = LockType.DOWNLOAD,
    ) -> LockInfo | None:
        """
        Try to acquire a lock without waiting.

        Returns:
            LockInfo if acquired, None if lock is already held

        Note:
            This method does NOT auto-release the lock. You must call
            release() manually when done. Prefer using acquire() context
            manager for automatic cleanup.
        """
        lock = await self._get_or_create_lock(key)

        if lock.locked():
            return None

        # Try to acquire without blocking
        acquired = lock.locked() is False
        if acquired:
            await lock.acquire()
            lock_info = LockInfo(
                key=key,
                holder=holder,
                acquired_at=utcnow(),
                lock_type=lock_type,
            )
            self._lock_info[key] = lock_info
            logger.debug(f"Lock acquired (non-blocking): {key} by {holder}")
            return lock_info

        return None

    async def release(self, key: str) -> bool:
        """
        Manually release a lock acquired via try_acquire().

        Returns:
            True if lock was released, False if lock didn't exist
        """
        if key not in self._locks:
            return False

        lock = self._locks[key]
        if lock.locked():
            self._lock_info.pop(key, None)
            lock.release()
            logger.debug(f"Lock released (manual): {key}")
            return True

        return False

# Global lock manager instance
_download_lock_manager: DownloadLockManager | None = None
