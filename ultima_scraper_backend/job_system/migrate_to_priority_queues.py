"""Migration utility to convert FIFO queues to priority queues.

This script migrates existing job queues from Redis LIST (FIFO) to Redis ZSET (priority-based).
Run this once when upgrading to the priority queue system.

Usage:
    python -m ultima_scraper_backend.job_system.migrate_to_priority_queues
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, cast

from ultima_scraper_backend.redis.client import RedisManager

from .models import JobDispatchEnvelope
from .routing import BackendRouter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate_backend_queue(
    redis_manager: RedisManager,
    backend_id: str,
    default_priority: int = 5,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Migrate a single backend's queue from LIST to ZSET.

    Args:
        redis_manager: Redis connection manager
        backend_id: Backend ID to migrate
        default_priority: Default priority for existing jobs (default 5)
        dry_run: If True, only report what would be done without making changes

    Returns:
        Dict with migration statistics
    """
    client: Any = redis_manager.client

    old_queue_key = f"{BackendRouter.QUEUE_PREFIX}{backend_id}"

    # Check if queue exists and is a LIST
    queue_type = await redis_manager.type(old_queue_key)

    if queue_type == "none":
        logger.info(f"Backend {backend_id}: Queue does not exist, skipping")
        return {
            "backend_id": backend_id,
            "status": "skipped",
            "reason": "Queue does not exist",
            "jobs_migrated": 0,
        }

    if queue_type == "zset":
        logger.info(f"Backend {backend_id}: Already using priority queue (ZSET)")
        return {
            "backend_id": backend_id,
            "status": "already_migrated",
            "jobs_in_queue": cast(int, await client.zcard(old_queue_key)),
        }

    if queue_type != "list":
        logger.error(f"Backend {backend_id}: Unexpected queue type: {queue_type}")
        return {
            "backend_id": backend_id,
            "status": "error",
            "reason": f"Unexpected queue type: {queue_type}",
            "jobs_migrated": 0,
        }

    # Get all jobs from the LIST (preserving FIFO order)
    queue_length = await redis_manager.llen(old_queue_key)
    logger.info(f"Backend {backend_id}: Found {queue_length} jobs in FIFO queue")

    if queue_length == 0:
        if not dry_run:
            # Delete the empty LIST so it can be recreated as ZSET
            await redis_manager.delete(old_queue_key)
        return {
            "backend_id": backend_id,
            "status": "completed",
            "jobs_migrated": 0,
        }

    # Get all jobs
    jobs: list[str] = await redis_manager.lrange(old_queue_key, 0, -1)

    migrated_jobs: list[dict[str, Any]] = []
    for idx, job_payload in enumerate(jobs):
        try:
            # Parse the job envelope
            envelope_data = json.loads(job_payload)
            envelope = JobDispatchEnvelope.from_dict(envelope_data)

            # Use existing priority if present, otherwise use default
            priority = envelope.priority if envelope.priority != 0 else default_priority

            job_info = {
                "job_id": envelope.job.job_id,
                "job_name": envelope.job.name,
                "original_position": idx,
                "priority": priority,
            }

            migrated_jobs.append(job_info)
            logger.debug(
                f"Backend {backend_id}: Will migrate job {envelope.job.job_id} "
                f"(pos {idx}) with priority {priority}"
            )

        except Exception as exc:
            logger.error(
                f"Backend {backend_id}: Failed to parse job at position {idx}: {exc}"
            )
            continue

    if dry_run:
        logger.info(
            f"Backend {backend_id}: DRY RUN - Would migrate {len(migrated_jobs)} jobs"
        )
        return {
            "backend_id": backend_id,
            "status": "dry_run",
            "jobs_to_migrate": len(migrated_jobs),
            "jobs": migrated_jobs,
        }

    # Perform the migration
    # Step 1: Rename the old LIST to a temporary key
    temp_key = f"{old_queue_key}:migrating"
    await client.rename(old_queue_key, temp_key)
    logger.info(f"Backend {backend_id}: Renamed queue to temporary key")

    # Step 2: Create new ZSET with priority-based ordering
    try:
        for job_payload in jobs:
            envelope_data = json.loads(job_payload)
            envelope = JobDispatchEnvelope.from_dict(envelope_data)

            # Use existing priority if present, otherwise use default
            priority = envelope.priority if envelope.priority != 0 else default_priority

            # Update the envelope with the priority
            envelope.priority = priority

            # Add to ZSET with negative priority (for high-priority-first)
            score = -priority
            new_payload = json.dumps(envelope.to_dict())
            await redis_manager.zadd(old_queue_key, {new_payload: float(score)})

        # Step 3: Delete the temporary LIST
        await redis_manager.delete(temp_key)

        logger.info(
            f"Backend {backend_id}: Successfully migrated {len(migrated_jobs)} jobs to priority queue"
        )

        return {
            "backend_id": backend_id,
            "status": "completed",
            "jobs_migrated": len(migrated_jobs),
            "jobs": migrated_jobs,
        }

    except Exception as exc:
        # Rollback: restore the original LIST
        logger.error(
            f"Backend {backend_id}: Migration failed, attempting rollback: {exc}"
        )
        try:
            await redis_manager.delete(old_queue_key)  # Delete partial ZSET
            await client.rename(temp_key, old_queue_key)  # Restore original LIST
            logger.info(f"Backend {backend_id}: Rollback successful")
        except Exception as rollback_exc:
            logger.error(
                f"Backend {backend_id}: ROLLBACK FAILED - Manual intervention required: {rollback_exc}"
            )

        return {
            "backend_id": backend_id,
            "status": "error",
            "reason": str(exc),
            "jobs_migrated": 0,
        }


async def migrate_all_queues(
    redis_manager: RedisManager,
    backend_ids: list[str] | None = None,
    default_priority: int = 5,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Migrate all backend queues from FIFO to priority-based.

    Args:
        redis_manager: Redis connection manager
        backend_ids: List of backend IDs to migrate (None = discover all)
        default_priority: Default priority for existing jobs
        dry_run: If True, only report what would be done

    Returns:
        Dict with overall migration results
    """
    if backend_ids is None:
        # Discover all backend queues
        pattern = f"{BackendRouter.QUEUE_PREFIX}*"
        keys = await redis_manager.keys(pattern)
        backend_ids = [str(key) for key in keys]
        backend_ids = [
            key.replace(BackendRouter.QUEUE_PREFIX, "") for key in backend_ids
        ]

        if not backend_ids:
            logger.warning("No backend queues found")
            return {
                "status": "completed",
                "backends_migrated": 0,
                "results": [],
            }

    logger.info(f"Migrating {len(backend_ids)} backend queues...")

    results: list[dict[str, Any]] = []
    for backend_id in backend_ids:
        result = await migrate_backend_queue(
            redis_manager,
            backend_id,
            default_priority,
            dry_run,
        )
        results.append(result)

    # Summarize results
    completed = sum(1 for r in results if r["status"] == "completed")
    errors = sum(1 for r in results if r["status"] == "error")
    skipped = sum(1 for r in results if r["status"] in ["skipped", "already_migrated"])
    total_jobs = sum(r.get("jobs_migrated", 0) for r in results)

    summary: dict[str, Any] = {
        "status": "completed" if errors == 0 else "completed_with_errors",
        "backends_processed": len(results),
        "backends_migrated": completed,
        "backends_skipped": skipped,
        "backends_with_errors": errors,
        "total_jobs_migrated": total_jobs,
        "results": results,
    }

    logger.info(
        f"Migration {'DRY RUN ' if dry_run else ''}complete: "
        f"{completed} succeeded, {errors} failed, {skipped} skipped"
    )

    return summary


async def main() -> None:
    """Main entry point for migration script."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate job queues from FIFO to priority-based"
    )
    parser.add_argument(
        "--backend-id",
        help="Specific backend ID to migrate (omit to migrate all)",
    )
    parser.add_argument(
        "--default-priority",
        type=int,
        default=5,
        help="Default priority for existing jobs (default: 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)",
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=0,
        help="Redis database number (default: 0)",
    )

    args = parser.parse_args()

    # Connect to Redis
    redis_manager = RedisManager(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
    )
    await redis_manager.connect()

    try:
        if args.backend_id:
            # Migrate single backend
            result = await migrate_backend_queue(
                redis_manager,
                args.backend_id,
                args.default_priority,
                args.dry_run,
            )
            print(json.dumps(result, indent=2))
        else:
            # Migrate all backends
            result = await migrate_all_queues(
                redis_manager,
                default_priority=args.default_priority,
                dry_run=args.dry_run,
            )
            print(json.dumps(result, indent=2))
    finally:
        await redis_manager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
