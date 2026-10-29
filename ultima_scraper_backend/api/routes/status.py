"""Real-time status endpoint showing job/task progress."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Any, cast

from fastapi import APIRouter, Depends

from ...models.backend import BackendInstance
from ..dependencies import get_backend_instance

router = APIRouter(tags=["status"])


def _decode_redis_text(value: object, default: str = "") -> str:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, str):
        return value
    return default


async def _await_redis_call(result: object) -> Any:
    if isinstance(result, Awaitable):
        return await cast(Awaitable[Any], result)
    return result


async def _redis_keys(redis: Any, pattern: str) -> list[str]:
    raw_keys = await _await_redis_call(redis.keys(pattern))
    if not isinstance(raw_keys, list):
        return []
    return [_decode_redis_text(key) for key in cast(list[object], raw_keys)]


async def _redis_hgetall(redis: Any, key: str) -> dict[object, object]:
    raw_hash = await _await_redis_call(redis.hgetall(key))
    if isinstance(raw_hash, Mapping):
        return {
            mapping_key: mapping_value
            for mapping_key, mapping_value in cast(
                Mapping[object, object], raw_hash
            ).items()
        }
    return {}


async def _redis_smembers(redis: Any, key: str) -> set[object]:
    raw_members = await _await_redis_call(redis.smembers(key))
    if isinstance(raw_members, set):
        return cast(set[object], raw_members)
    return set()


async def _redis_lrange(redis: Any, key: str, start: int, end: int) -> list[object]:
    raw_items = await _await_redis_call(redis.lrange(key, start, end))
    if isinstance(raw_items, list):
        return cast(list[object], raw_items)
    return []


async def _redis_zcard(redis: Any, key: str) -> int:
    raw_size = await _await_redis_call(redis.zcard(key))
    return int(raw_size) if isinstance(raw_size, int) else 0


@router.get("/status/jobs")
async def get_job_status(
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, Any]:
    """Get current status of all jobs and tasks."""

    redis_manager = backend.redis
    assert redis_manager is not None and redis_manager.client is not None
    redis = redis_manager.client

    # Get all job keys
    job_keys = await _redis_keys(redis, "ultimascraper:jobs:job-*")
    jobs_data: list[dict[str, Any]] = []

    for key in job_keys:
        key_str = _decode_redis_text(key)
        # Skip task-specific keys
        if ":task:" in key_str or ":tasks:" in key_str or ":config" in key_str:
            continue

        # Extract job_id
        parts = key_str.split(":")
        if len(parts) < 3:
            continue
        job_id = parts[2]

        # Get job data
        job_hash = await _redis_hgetall(redis, f"ultimascraper:jobs:{job_id}")
        if not job_hash:
            continue

        # Get task counts
        ready_tasks = await _redis_smembers(
            redis, f"ultimascraper:jobs:{job_id}:tasks:ready"
        )
        inflight_tasks = await _redis_smembers(
            redis, f"ultimascraper:jobs:{job_id}:tasks:inflight"
        )
        completed_tasks = await _redis_smembers(
            redis, f"ultimascraper:jobs:{job_id}:tasks:completed"
        )

        jobs_data.append(
            {
                "job_id": job_id,
                "status": _decode_redis_text(job_hash.get(b"status", b"")),
                "tasks": {
                    "ready": len(ready_tasks),
                    "inflight": len(inflight_tasks),
                    "completed": len(completed_tasks),
                    "total": len(ready_tasks)
                    + len(inflight_tasks)
                    + len(completed_tasks),
                },
            }
    )

    # Get unclaimed jobs
    unclaimed = await _redis_lrange(redis, "ultimascraper:jobs:unclaimed", 0, -1)

    # Get claimed jobs per backend
    claimed_keys = await _redis_keys(redis, "ultimascraper:jobs:claimed:*")
    claimed_data: dict[str, list[str]] = {}
    for key in claimed_keys:
        key_str = _decode_redis_text(key)
        backend_id = key_str.split(":")[-1]
        claimed = await _redis_lrange(redis, key_str, 0, -1)
        claimed_data[backend_id] = [
            _decode_redis_text(job) for job in claimed
        ]

    # Get ready queue size per backend
    ready_keys = await _redis_keys(redis, "ultimascraper:ready:*")
    ready_queues: dict[str, int] = {}
    for key in ready_keys:
        key_str = _decode_redis_text(key)
        if ":notify" in key_str:
            continue
        backend_id = key_str.split(":")[-1]
        size = await _redis_zcard(redis, key_str)
        ready_queues[backend_id] = size

    return {
        "jobs": jobs_data,
        "queues": {
            "unclaimed": len(unclaimed),
            "claimed": claimed_data,
            "ready_tasks": ready_queues,
        },
    }


@router.get("/status/workers")
async def get_worker_status(
    backend: BackendInstance = Depends(get_backend_instance),
) -> dict[str, Any]:
    """Get current status of all workers."""

    redis_manager = backend.redis
    assert redis_manager is not None and redis_manager.client is not None
    redis = redis_manager.client

    worker_keys = await _redis_keys(redis, "ultimascraper:workers:worker-*")
    workers_data: list[dict[str, Any]] = []

    for key in worker_keys:
        key_str = _decode_redis_text(key)
        worker_hash = await _redis_hgetall(redis, key_str)
        if worker_hash:
            workers_data.append(
                {
                    "worker_id": key_str.split(":")[-1],
                    "status": _decode_redis_text(
                        worker_hash.get(b"status", b"unknown"), "unknown"
                    ),
                    "sites": _decode_redis_text(worker_hash.get(b"sites", b"[]"), "[]"),
                    "jobs_completed": int(
                        _decode_redis_text(worker_hash.get(b"jobs_completed", b"0"), "0")
                    ),
                    "last_heartbeat": _decode_redis_text(
                        worker_hash.get(b"last_heartbeat", b"")
                    ),
                }
            )

    return {"workers": workers_data, "total": len(workers_data)}
