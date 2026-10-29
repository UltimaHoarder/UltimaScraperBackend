"""Production-grade Redis manager extending USAPI base.

This module provides a production Redis manager with connection pooling,
retry logic, and advanced operations for distributed job systems.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional, cast

from redis.exceptions import (
    BusyLoadingError,
    ConnectionError,
    RedisError,
    TimeoutError,
)
from ultima_scraper_api.managers.redis import RedisManager as BaseRedisManager

if TYPE_CHECKING:
    from ultima_scraper_api.config import Redis as RedisConfig

logger = logging.getLogger(__name__)


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


class RedisManager(BaseRedisManager):
    """Production Redis manager with retry logic and advanced operations.

    Extends USAPI's RedisManager with:
    - Longer socket timeout (30s) for blocking operations like BRPOP
    - Retry logic with exponential backoff
    - Advanced Redis operations (sorted sets, lists, hashes, pipelines)
    - Health checks
    - Production-grade error handling

    This class is designed for distributed job systems that require:
    - Job queuing (BRPOP/LPUSH)
    - Distributed locking
    - Worker coordination
    - High availability
    """

    def __init__(
        self,
        config: Optional[RedisConfig] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        password: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        socket_timeout: float = 30.0,
        max_connections: int | None = None,
        pool_wait_timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a role-sized, process-local Redis manager."""
        # Build config from individual parameters if config not provided
        if not config:
            from ultima_scraper_api.config import Redis as RedisConfig

            config = RedisConfig(
                enabled=True,
                host=host or "localhost",
                port=port or 6379,
                db=db if db is not None else 0,
                password=password,
            )


        process_role = (
            os.getenv("ULTIMA_DATABASE_PROCESS_ROLE", "application").strip().lower()
        )
        if process_role not in {"application", "worker_reader", "writer"}:
            raise ValueError(f"Unsupported process connection role: {process_role}")
        if max_connections is None:
            if process_role == "worker_reader":
                max_connections = _positive_int_env(
                    "ULTIMA_REDIS_WORKER_MAX_CONNECTIONS", 32
                )
            elif process_role == "writer":
                max_connections = _positive_int_env(
                    "ULTIMA_REDIS_WRITER_MAX_CONNECTIONS", 8
                )
            else:
                max_connections = _positive_int_env(
                    "ULTIMA_REDIS_APPLICATION_MAX_CONNECTIONS", 256
                )
        if pool_wait_timeout is None:
            pool_wait_timeout = _positive_float_env(
                "ULTIMA_REDIS_POOL_WAIT_TIMEOUT_SECONDS", 30.0
            )

        # Initialize parent with production settings
        super().__init__(
            config=config,
            max_connections=max_connections,
            pool_wait_timeout=pool_wait_timeout,
            socket_timeout=socket_timeout,
            socket_connect_timeout=5.0,
            socket_keepalive=True,
            retry_on_timeout=True,
            **kwargs,
        )

        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Runtime-tunable safety valves for disconnected Redis scenarios.
        # When Redis is down we fail fast for a short cooldown window and
        # rate-limit warning/error logs to avoid overwhelming the event loop.
        self.max_retries = max(
            1, int(os.getenv("ULTIMA_REDIS_MAX_RETRIES", str(self.max_retries)))
        )
        self.retry_delay = max(
            0.0, float(os.getenv("ULTIMA_REDIS_RETRY_DELAY", str(self.retry_delay)))
        )
        self.failure_cooldown = max(
            0.0, float(os.getenv("ULTIMA_REDIS_FAILURE_COOLDOWN", "3.0"))
        )
        self.error_log_interval = max(
            0.5, float(os.getenv("ULTIMA_REDIS_ERROR_LOG_INTERVAL", "5.0"))
        )
        self._circuit_open_until = 0.0
        self._last_error_log_at: dict[str, float] = {}

    async def connect(self) -> bool:
        """Connect to Redis with production logging."""
        result = await super().connect()
        if result:
            logger.info(
                f"Production Redis connected at {self.config.host}:{self.config.port} "
                f"(pooled, max_retries={self.max_retries}, timeout={self.pool_config['socket_timeout']}s)"
            )
        return result

    async def _execute_with_retry(
        self,
        operation: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute Redis operation with retry logic."""
        if not self.is_connected or not self.client:
            raise RuntimeError("Redis client not connected. Call connect() first.")

        now = time.monotonic()
        if now < self._circuit_open_until:
            remaining = self._circuit_open_until - now
            raise RedisError(
                f"Redis temporarily unavailable ({remaining:.1f}s cooldown remaining)"
            )

        last_error = None

        for attempt in range(self.max_retries):
            try:
                method = getattr(self.client, operation)
                result = await method(*args, **kwargs)
                # Reset cooldown state after a successful call.
                self._circuit_open_until = 0.0
                return result
            except (ConnectionError, TimeoutError) as exc:
                last_error = exc
                self._circuit_open_until = max(
                    self._circuit_open_until,
                    time.monotonic() + self.failure_cooldown,
                )
                self._log_connection_issue(
                    operation=operation,
                    error=exc,
                    final=(attempt >= self.max_retries - 1),
                )
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (attempt + 1)
                    if delay > 0:
                        await asyncio.sleep(delay)
            except RedisError as exc:
                logger.error(f"Redis {operation} error: {exc}")
                raise

        raise RedisError(
            f"Operation {operation} failed after {self.max_retries} retries: {last_error}"
        )

    def _log_connection_issue(
        self,
        *,
        operation: str,
        error: BaseException,
        final: bool,
    ) -> None:
        """Rate-limit noisy connection failure logs per operation/error type."""
        key = f"{operation}:{type(error).__name__}"
        now = time.monotonic()
        last = self._last_error_log_at.get(key, 0.0)
        if now - last < self.error_log_interval:
            return

        self._last_error_log_at[key] = now

        # Redis LOADING state (warmup after restart/AOF replay) is expected
        # and transient. Downgrade to INFO/debug to avoid alerting operators
        # every time Redis cycles.
        is_loading = isinstance(error, BusyLoadingError) or (
            "LOADING" in str(error) or "loading the dataset" in str(error).lower()
        )

        if is_loading:
            message = (
                f"Redis {operation} waiting for LOADING to finish "
                f"(attempt {self.max_retries}): {error}"
                if final
                else f"Redis {operation} deferred while server is loading: {error}"
            )
            logger.info(message)
            return

        message = (
            f"Redis {operation} failed after {self.max_retries} attempts: {error}"
            if final
            else (
                f"Redis {operation} failed "
                f"(attempt retry in progress): {error}"
            )
        )
        if final:
            logger.error(message)
        else:
            logger.warning(message)

    # Override parent methods to add retry logic
    async def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        try:
            return await self._execute_with_retry(
                "set", key, value, ex=ex, px=px, nx=nx, xx=xx
            )
        except Exception as exc:
            logger.debug(f"Failed to set Redis key '{key}': {exc}")
            return False

    async def get(self, key: str) -> Optional[str]:
        try:
            return await self._execute_with_retry("get", key)
        except Exception as exc:
            logger.debug(f"Failed to get Redis key '{key}': {exc}")
            return None

    async def delete(self, key: str, *extra_keys: str) -> bool:
        try:
            deleted = await self._execute_with_retry("delete", key, *extra_keys)
            return bool(deleted)
        except Exception as exc:
            logger.debug(f"Failed to delete Redis keys: {exc}")
            return False

    async def exists(self, key: str, *extra_keys: str) -> bool:
        try:
            existing = await self._execute_with_retry("exists", key, *extra_keys)
            return bool(existing)
        except Exception as exc:
            logger.debug(f"Failed to check Redis key existence: {exc}")
            return False

    async def expire(self, key: str, seconds: int) -> bool:
        try:
            return await self._execute_with_retry("expire", key, seconds)
        except Exception as exc:
            logger.debug(f"Failed to set Redis key expiration: {exc}")
            return False

    async def publish(self, channel: str, message: str) -> bool:
        try:
            await self._execute_with_retry("publish", channel, message)
            return True
        except Exception as exc:
            logger.debug(f"Failed to publish to Redis channel '{channel}': {exc}")
            return False

    # Advanced operations
    async def ttl(self, key: str) -> int:
        return await self._execute_with_retry("ttl", key)

    async def keys(self, pattern: str = "*") -> list[str]:
        return await self._execute_with_retry("keys", pattern)

    # List operations
    async def lpush(self, name: str, *values: str) -> int:
        return await self._execute_with_retry("lpush", name, *values)

    async def rpush(self, name: str, *values: str) -> int:
        return await self._execute_with_retry("rpush", name, *values)

    async def brpop(
        self, keys: list[str], timeout: int = 0
    ) -> Optional[tuple[str, str]]:
        return await self._execute_with_retry("brpop", keys, timeout)

    async def lrange(self, name: str, start: int, end: int) -> list[str]:
        return await self._execute_with_retry("lrange", name, start, end)

    async def lrem(self, name: str, count: int, value: str) -> int:
        return await self._execute_with_retry("lrem", name, count, value)

    async def llen(self, name: str) -> int:
        return await self._execute_with_retry("llen", name)

    async def rpop(self, name: str) -> str | None:
        result = await self._execute_with_retry("rpop", name)
        return None if result is None else str(result)

    # Hash operations
    async def hset(
        self,
        name: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        mapping: Optional[dict[str, Any]] = None,
    ) -> int:
        return await self._execute_with_retry("hset", name, key, value, mapping=mapping)

    async def hget(self, name: str, key: str) -> Optional[str]:
        return await self._execute_with_retry("hget", name, key)

    async def hgetall(self, name: str) -> dict[str, str]:
        return await self._execute_with_retry("hgetall", name)

    async def hdel(self, name: str, *keys: str) -> int:
        return await self._execute_with_retry("hdel", name, *keys)

    # Set operations
    async def sadd(self, name: str, *values: str) -> int:
        return await self._execute_with_retry("sadd", name, *values)

    async def smembers(self, name: str) -> set[str]:
        return await self._execute_with_retry("smembers", name)

    async def srem(self, name: str, *values: str) -> int:
        return await self._execute_with_retry("srem", name, *values)

    async def scard(self, name: str) -> int:
        return await self._execute_with_retry("scard", name)

    # Sorted set operations
    async def zadd(
        self,
        name: str,
        mapping: dict[str, float],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        return await self._execute_with_retry(
            "zadd", name, mapping, nx=nx, xx=xx, gt=gt, lt=lt
        )

    async def zrange(
        self, name: str, start: int, end: int, withscores: bool = False
    ) -> list[str | tuple[str, float]]:
        return await self._execute_with_retry(
            "zrange", name, start, end, withscores=withscores
        )

    async def zrevrange(
        self, name: str, start: int, end: int, withscores: bool = False
    ) -> list[str | tuple[str, float]]:
        return await self._execute_with_retry(
            "zrevrange", name, start, end, withscores=withscores
        )

    async def zrem(self, name: str, *values: str) -> int:
        return await self._execute_with_retry("zrem", name, *values)

    async def incr(self, name: str, amount: int = 1) -> int:
        return await self._execute_with_retry("incr", name, amount)

    async def type(self, name: str) -> str:
        result = await self._execute_with_retry("type", name)
        return str(result)

    @asynccontextmanager
    async def pipeline(self) -> AsyncIterator[Any]:
        """Create a Redis pipeline for batched operations."""
        if not self.is_connected or not self.client:
            raise RuntimeError("Redis client not connected")

        pipe = self.client.pipeline()
        try:
            yield pipe
            await pipe.execute()
        finally:
            await pipe.reset()

    async def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            if not self.is_connected or not self.client:
                return False
            await cast(Any, self.client).ping()
            return True
        except Exception as exc:
            logger.error(f"Redis health check failed: {exc}")
            return False
