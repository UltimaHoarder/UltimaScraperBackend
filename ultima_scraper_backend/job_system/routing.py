"""Backend routing utilities for the UltimaScraper job system."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional, Sequence, cast

from ultima_scraper_backend.ultima_control.backend_registry import (
    BackendInfo,
    BackendRegistry,
)

from ..redis.client import RedisManager
from ..redis_channels import (
    BACKEND_ACTIVE_JOBS_PREFIX,
    BACKEND_CAPABILITIES_PREFIX,
    BACKEND_LOAD_KEY,
    BACKEND_QUEUE_PREFIX,
    IP_LOCK_PREFIX,
    JOB_ENVELOPE_PREFIX,
    JOB_EVENTS_CHANNEL,
    KEY_PREFIX,
    READY_SEQUENCE_PREFIX,
    TASK_OWNER_PREFIX,
)
from .models import Job, JobDispatchEnvelope

logger = logging.getLogger(__name__)


class BackendRouter:
    """Selects backends and manages per-backend task queues."""

    QUEUE_PREFIX = BACKEND_QUEUE_PREFIX
    LOAD_KEY = BACKEND_LOAD_KEY
    IP_LOCK_PREFIX = IP_LOCK_PREFIX
    BACKEND_CAPABILITIES_PREFIX = BACKEND_CAPABILITIES_PREFIX
    TASK_OWNER_PREFIX = TASK_OWNER_PREFIX
    EVENTS_CHANNEL = JOB_EVENTS_CHANNEL
    READY_SEQUENCE_PREFIX = READY_SEQUENCE_PREFIX
    BACKEND_ACTIVE_JOBS_PREFIX = BACKEND_ACTIVE_JOBS_PREFIX
    JOB_ENVELOPE_PREFIX = JOB_ENVELOPE_PREFIX
    UNCLAIMED_JOBS_KEY = f"{KEY_PREFIX}:jobs:unclaimed"
    CLAIMED_JOBS_PREFIX = f"{KEY_PREFIX}:jobs:claimed:"
    READY_CLAIMS_PREFIX = f"{KEY_PREFIX}:backend:ready-claims:"
    READY_CLAIM_OWNERS_PREFIX = f"{KEY_PREFIX}:backend:ready-claim-owners:"
    READY_CLAIM_TTL_SECONDS = 30
    READY_SCORE_SCALE = 1_000_000

    def __init__(
        self,
        redis_manager: RedisManager,
        *,
        ip_lock_ttl: int = 6 * 60 * 60,
        task_owner_ttl: int = 24 * 60 * 60,
    ) -> None:
        self.redis = redis_manager
        self.ip_lock_ttl = ip_lock_ttl
        self.task_owner_ttl = task_owner_ttl

    async def enqueue(
        self, envelope: JobDispatchEnvelope, backend_id: Optional[str] = None
    ) -> str:
        """Select an appropriate backend and enqueue the job bundle."""

        capabilities = envelope.capabilities
        backend_choice = backend_id or envelope.backend_id
        backend_id = backend_choice or await self._select_backend(
            capabilities=capabilities,
            ip_lock_key=envelope.ip_lock_key,
        )
        envelope.backend_id = backend_id

        payload = json.dumps(envelope.to_dict())
        await self.redis.set(
            self._job_envelope_key(envelope.job.job_id),
            payload,
            ex=self.task_owner_ttl,
        )

        await self._client().sadd(
            self._backend_jobs_key(backend_id), envelope.job.job_id
        )
        await self._client().expire(
            self._backend_jobs_key(backend_id), self.task_owner_ttl
        )

        new_load = await self._client().zincrby(self.LOAD_KEY, 1.0, backend_id)

        await self._cache_backend_capabilities(backend_id, capabilities, new_load)
        await self._record_task_owners(envelope.job, backend_id)

        if envelope.ip_lock_key:
            lock_key = self._ip_lock_key(envelope.ip_lock_key)
            await self.redis.set(lock_key, backend_id, ex=self.ip_lock_ttl)

        await self._publish_event(
            {
                "event": "job_dispatched",
                "job_id": envelope.job.job_id,
                "job_name": envelope.job.name,
                "backend_id": backend_id,
                "capabilities": capabilities,
                "priority": envelope.priority,
                "status": envelope.job.state.value,
                "total_tasks": envelope.job.total_tasks,
                "completed_tasks": envelope.job.completed_tasks,
                "failed_tasks": envelope.job.failed_tasks,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return backend_id

    async def enqueue_ready_task(
        self,
        *,
        backend_id: str,
        job_id: str,
        task_id: str,
        priority: int = 0,
    ) -> None:
        """Place a ready task onto the backend's priority queue."""

        member = self._encode_ready_member(job_id, task_id)
        queue_key = self._queue_key(backend_id)
        sequence = await self._client().incr(self._ready_sequence_key(backend_id))
        score = priority * self.READY_SCORE_SCALE + sequence
        from .mps_attempt import control_mutation
        from .storage import JobStorage

        await control_mutation(JobStorage(self.redis), job_id, "return redis.call('ZADD',KEYS[1],ARGV[1],ARGV[2])", [queue_key], [score, member])

    async def enqueue_ready_tasks(
        self,
        *,
        backend_id: str,
        items: Sequence[tuple[str, str, int]],
    ) -> None:
        if not items:
            return

        client = self._client()
        sequence_end = int(
            await client.incrby(self._ready_sequence_key(backend_id), len(items))
        )
        sequence_start = sequence_end - len(items) + 1
        queue_members = {
            self._encode_ready_member(job_id, task_id): (
                priority * self.READY_SCORE_SCALE + sequence_start + index
            )
            for index, (job_id, task_id, priority) in enumerate(items)
        }
        from . import ordinary_edit
        owner = ordinary_edit.current()
        if owner is not None:
            from .storage import JobStorage
            if any(job_id != owner.job_id for job_id, _task, _priority in items):
                raise RuntimeError("An edit cannot route a different job")
            await ordinary_edit.control_mutation(JobStorage(self.redis), owner.job_id,
                "for i=1,#ARGV,2 do redis.call('ZADD',KEYS[1],ARGV[i+1],ARGV[i]) end return 1",
                [self._queue_key(backend_id)], [part for pair in queue_members.items() for part in pair])
        else:
            await client.zadd(self._queue_key(backend_id), queue_members)

    async def claim_ready_task(
        self,
        backend_id: str,
        *,
        timeout: int = 5,
        claimant_id: str = "legacy-single-claimant",
    ) -> Optional[tuple[str, str]]:
        """Reserve the highest-priority ready task without removing its route."""

        deadline = time.monotonic() + max(0, timeout)
        while True:
            claims = await self.claim_ready_tasks(
                backend_id,
                count=1,
                claimant_id=claimant_id,
            )
            if claims:
                return claims[0]
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

    async def claim_ready_tasks(
        self,
        backend_id: str,
        *,
        count: int = 8,
        claimant_id: str = "legacy-batch-claimant",
    ) -> list[tuple[str, str]]:
        """Reserve ready routes; dequeue happens with ready→in-flight transition.

        Keeping the sorted-set member until task state is claimed closes the
        crash window where ZPOPMIN could strand a task in the ready set. The
        short reservation prevents duplicate workers from selecting the same
        member and expires automatically if a worker disappears before start.
        """

        if count <= 0:
            return []

        queue_key = self._queue_key(backend_id)
        reservations_key, owners_key = self._ready_claim_keys(backend_id)
        scan_limit = 10_000
        results = await self._client().eval(
            """
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local expires_ms = now_ms + tonumber(ARGV[1]) * 1000
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, ARGV[4])
for _, member in ipairs(expired) do
  redis.call('ZREM', KEYS[2], member)
  redis.call('HDEL', KEYS[3], member)
end
local reserved_count = redis.call('ZCARD', KEYS[2])
local candidate_limit = math.min(tonumber(ARGV[4]), reserved_count + tonumber(ARGV[3]) * 2)
local candidates = redis.call('ZRANGE', KEYS[1], 0, candidate_limit - 1)
local claimed = {}
for _, member in ipairs(candidates) do
  if redis.call('ZSCORE', KEYS[2], member) == false then
    redis.call('ZADD', KEYS[2], expires_ms, member)
    redis.call('HSET', KEYS[3], member, ARGV[2])
    table.insert(claimed, member)
    if #claimed >= tonumber(ARGV[3]) then break end
  end
end
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('EXPIRE', KEYS[3], ARGV[5])
return claimed
""",
            3,
            queue_key,
            reservations_key,
            owners_key,
            self.READY_CLAIM_TTL_SECONDS,
            claimant_id,
            count,
            scan_limit,
            self.task_owner_ttl,
        )
        return [self._decode_ready_member(member) for member in results]

    async def remove_ready_tasks_for_job(
        self,
        backend_id: str,
        job_id: str,
        *,
        batch_size: int = 2000,
        mps_task_manifest: str | None = None,
    ) -> int:
        """Remove ready queue entries for a job using bounded prefix scans."""

        queue_key = self._queue_key(backend_id)
        cursor = 0
        removed = 0
        pattern = f"{job_id}:*"

        while True:
            cursor, members_with_scores = await self._client().zscan(
                queue_key,
                cursor=cursor,
                match=pattern,
                count=batch_size,
            )
            members = [
                member.decode() if isinstance(member, bytes) else str(member)
                for member, _score in cast(
                    Sequence[tuple[Any, Any]], members_with_scores
                )
            ]
            if members:
                if mps_task_manifest is not None:
                    from .mps_attempt import mutate
                    from .storage import JobStorage

                    removed += int(await mutate(JobStorage(self.redis), job_id, mps_task_manifest, """
                        local count = 0
                        for i=2,#ARGV do count = count + redis.call('ZREM', KEYS[3], ARGV[i]) end
                        return count
                    """, [queue_key], members))
                else:
                    from . import ordinary_edit
                    if ordinary_edit.current(job_id) is not None:
                        from .storage import JobStorage
                        removed += int(await ordinary_edit.control_mutation(JobStorage(self.redis), job_id,
                            "local n=0; for i=1,#ARGV do n=n+redis.call('ZREM',KEYS[1],ARGV[i]) end return n",
                            [queue_key], members))
                    else:
                        removed += int(await self._client().zrem(queue_key, *members))
            if cursor == 0:
                break

        return removed

    async def prioritize_job_tasks(
        self,
        backend_id: str,
        job_id: str,
        *,
        priority: int = -1,
    ) -> int:
        """Re-score a job's ready tasks so they are claimed earlier."""

        queue_key = self._queue_key(backend_id)
        members = await self._client().zrange(queue_key, 0, -1)
        if not members:
            return 0

        matching_members = [
            member
            for member in members
            if self._decode_ready_member(member)[0] == job_id
        ]
        if not matching_members:
            return 0

        rescored = 0
        for member in matching_members:
            sequence = await self._client().incr(self._ready_sequence_key(backend_id))
            score = priority * self.READY_SCORE_SCALE + sequence
            await self._client().zadd(queue_key, {member: score})
            rescored += 1
        return rescored

    async def clear_job_assignment(self, backend_id: str, job_id: str) -> None:
        from .mps_attempt import control_mutation
        from .storage import JobStorage

        await control_mutation(JobStorage(self.redis), job_id, "return redis.call('SREM',KEYS[1],ARGV[1])", [self._backend_jobs_key(backend_id)], [job_id])
        await self.remove_ready_tasks_for_job(backend_id, job_id)

    async def add_job_to_unclaimed_pool(self, job_id: str) -> None:
        from .ordinary_edit import control_mutation
        from .storage import JobStorage
        await control_mutation(JobStorage(self.redis), job_id, "return redis.call('RPUSH',KEYS[1],ARGV[1])", [self.UNCLAIMED_JOBS_KEY], [job_id])

    async def remove_job_from_unclaimed_pool(self, job_id: str) -> int:
        """Remove a job from the unclaimed queue."""

        from .ordinary_edit import control_mutation
        from .storage import JobStorage
        return int(await control_mutation(JobStorage(self.redis), job_id, "return redis.call('LREM',KEYS[1],0,ARGV[1])", [self.UNCLAIMED_JOBS_KEY], [job_id]))

    async def remove_claimed_job(self, backend_id: str, job_id: str) -> int:
        """Remove a job from a backend's claimed-jobs queue."""

        from .mps_attempt import control_mutation
        from .storage import JobStorage

        return int(await control_mutation(JobStorage(self.redis), job_id, "return redis.call('LREM',KEYS[1],0,ARGV[1])", [self._claimed_jobs_key(backend_id)], [job_id]))

    async def delete_job_envelope(self, job_id: str) -> int:
        """Delete the stored dispatch envelope for a job."""

        from .mps_attempt import control_mutation
        from .storage import JobStorage

        return int(await control_mutation(JobStorage(self.redis), job_id, "return redis.call('DEL',KEYS[1])", [self._job_envelope_key(job_id)], []))

    async def clear_job_routing(
        self,
        *,
        job_id: str,
        backend_id: str | None = None,
        mps_task_manifest: str | None = None,
    ) -> None:
        """Remove a job from unclaimed, claimed, active, and queued routing state."""
        from . import mps_attempt, retention
        from .storage import JobStorage

        attempt = mps_attempt.current(job_id)
        if mps_task_manifest is None and attempt is not None:
            mps_task_manifest = attempt.manifest_id
        if mps_task_manifest is not None or retention.current(job_id) is not None:
            keys = [self.UNCLAIMED_JOBS_KEY, self._job_envelope_key(job_id)]
            if backend_id:
                keys.extend([self._backend_jobs_key(backend_id), self._claimed_jobs_key(backend_id)])
            identity = (
                mps_attempt.Identity(job_id, "", mps_task_manifest)
                if mps_task_manifest is not None else None
            )
            with mps_attempt.scope(identity):
                await mps_attempt.control_mutation(
                    JobStorage(self.redis), job_id,
                    """
                    redis.call('LREM', KEYS[1], 0, ARGV[1])
                    redis.call('DEL', KEYS[2])
                    if #KEYS > 2 then
                        redis.call('SREM', KEYS[3], ARGV[1])
                        redis.call('LREM', KEYS[4], 0, ARGV[1])
                    end
                    return 1
                    """, keys, [job_id],
                )
            if backend_id and mps_task_manifest is not None:
                await self.remove_ready_tasks_for_job(backend_id, job_id, mps_task_manifest=mps_task_manifest)
            return
        await self.remove_job_from_unclaimed_pool(job_id)
        if backend_id:
            await self.clear_job_assignment(backend_id, job_id)
            await self.remove_claimed_job(backend_id, job_id)
        await self.delete_job_envelope(job_id)

    async def mark_job_complete(
        self,
        *,
        backend_id: str,
        job: Job,
        success: bool,
        message: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Record completion and emit an event for UI consumers."""

        if job.metadata.get("mps_manifest"):
            from .mps_attempt import current, mutate
            from .storage import JobStorage

            attempt = current(job.job_id)
            manifest = attempt.manifest_id if attempt else job.metadata.get("mps_task_manifest") or ""
            storage = JobStorage(self.redis)
            # Only an actually assigned job decrements backend load. Replays
            # of terminal publication must not repeatedly charge other jobs.
            await mutate(storage, job.job_id, manifest, """
                if redis.call('SREM', KEYS[3], ARGV[2]) == 1 then
                    local score = math.max(0, tonumber(redis.call('ZSCORE', KEYS[4], ARGV[3]) or '0') - 1)
                    redis.call('ZADD', KEYS[4], score, ARGV[3])
                end
                return 1
            """, [self._backend_jobs_key(backend_id), self.LOAD_KEY], [job.job_id, backend_id])
            await self.clear_job_routing(job_id=job.job_id, backend_id=backend_id, mps_task_manifest=manifest)
            payload = {"event": "job_completed", "backend_id": backend_id,
                       "job_id": job.job_id, "job_name": job.name, "success": success,
                       "message": message, "status": job.state.value,
                       "mps_task_manifest": manifest, "total_tasks": job.total_tasks,
                       "completed_tasks": job.completed_tasks, "failed_tasks": job.failed_tasks,
                       "timestamp": datetime.now(timezone.utc).isoformat()}
            if metadata:
                payload["metadata"] = metadata
            from ..redis_channels import JOB_EVENTS_CHANNEL
            await mutate(storage, job.job_id, manifest, "redis.call('PUBLISH', KEYS[3], ARGV[2]); return 1",
                         [JOB_EVENTS_CHANNEL], [json.dumps(payload)])
            return
        from .ordinary_edit import control_mutation
        from .storage import JobStorage

        new_score = float(await control_mutation(JobStorage(self.redis), job.job_id, """
local score = math.max(0, tonumber(redis.call('ZSCORE',KEYS[1],ARGV[1]) or '0') - 1)
redis.call('ZADD',KEYS[1],score,ARGV[1])
return tostring(score)
""", [self.LOAD_KEY], [backend_id]))

        capabilities = sorted({cap for task in job.tasks for cap in task.capabilities})
        await self._cache_backend_capabilities(backend_id, capabilities, new_score)
        await self._finalize_task_owners(job, backend_id, success)
        await self.clear_job_routing(job_id=job.job_id, backend_id=backend_id)

        event_payload: dict[str, Any] = {
            "event": "job_completed",
            "backend_id": backend_id,
            "job_id": job.job_id,
            "job_name": job.name,
            "success": success,
            "message": message,
            "status": job.state.value,
            "total_tasks": job.total_tasks,
            "completed_tasks": job.completed_tasks,
            "failed_tasks": job.failed_tasks,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if metadata:
            event_payload["metadata"] = metadata
        from ..redis_channels import JOB_EVENTS_CHANNEL

        await control_mutation(JobStorage(self.redis), job.job_id, "return redis.call('PUBLISH',KEYS[1],ARGV[1])", [JOB_EVENTS_CHANNEL], [json.dumps(event_payload)])

    async def _select_backend(
        self,
        *,
        capabilities: Sequence[str],
        ip_lock_key: Optional[str],
    ) -> str:
        """Choose the best backend for a job given constraints."""

        backends = await BackendRegistry.get_all_backends(cast(Any, self.redis))
        if not backends:
            raise RuntimeError("No backends registered in Redis")

        locked_backend = None
        if ip_lock_key:
            lock_key = self._ip_lock_key(ip_lock_key)
            locked_backend = await self.redis.get(lock_key)
            if locked_backend and locked_backend not in backends:
                await self.redis.delete(lock_key)
                locked_backend = None

        if locked_backend:
            info = backends.get(locked_backend)
            if (
                info
                and info.status in {"online", "idle"}
                and self._capabilities_satisfied(info, capabilities)
            ):
                return locked_backend

        load_entries = await self._client().zrange(
            self.LOAD_KEY, 0, -1, withscores=True
        )
        load_map = {backend_id: float(score) for backend_id, score in load_entries}

        eligible: list[tuple[float, str, BackendInfo]] = []
        for backend_id, info in backends.items():
            if info.status not in {"online", "idle"}:
                continue
            if not self._capabilities_satisfied(info, capabilities):
                continue
            load = load_map.get(backend_id, float(info.load or 0.0))
            eligible.append((load, backend_id, info))

        if not eligible:
            raise RuntimeError(
                f"No eligible backends identified for capabilities {list(capabilities)}"
            )

        eligible.sort(key=lambda entry: (entry[0], entry[1]))
        return eligible[0][1]

    @staticmethod
    def _capabilities_satisfied(info: BackendInfo, capabilities: Sequence[str]) -> bool:
        if not capabilities:
            return True
        backend_caps = set(info.capabilities or [])
        required = set(capabilities)
        return required.issubset(backend_caps)

    def _queue_key(self, backend_id: str) -> str:
        return f"{self.QUEUE_PREFIX}{backend_id}"

    def _ready_sequence_key(self, backend_id: str) -> str:
        return f"{self.READY_SEQUENCE_PREFIX}{backend_id}"

    def _backend_jobs_key(self, backend_id: str) -> str:
        return f"{self.BACKEND_ACTIVE_JOBS_PREFIX}{backend_id}"

    def _claimed_jobs_key(self, backend_id: str) -> str:
        return f"{self.CLAIMED_JOBS_PREFIX}{backend_id}"

    def backend_jobs_key(self, backend_id: str) -> str:
        """Public helper for consumers that need the active-jobs set key."""

        return self._backend_jobs_key(backend_id)

    def _job_envelope_key(self, job_id: str) -> str:
        return f"{self.JOB_ENVELOPE_PREFIX}{job_id}"

    def _encode_ready_member(self, job_id: str, task_id: str) -> str:
        return f"{job_id}:{task_id}"

    def _ready_claim_keys(self, backend_id: str) -> tuple[str, str]:
        return (
            f"{self.READY_CLAIMS_PREFIX}{backend_id}",
            f"{self.READY_CLAIM_OWNERS_PREFIX}{backend_id}",
        )

    def _decode_ready_member(self, raw: Any) -> tuple[str, str]:
        value = raw.decode() if isinstance(raw, bytes) else str(raw)
        job_id, task_id = value.split(":", 1)
        return job_id, task_id

    def _ip_lock_key(self, lock_key: str) -> str:
        return f"{self.IP_LOCK_PREFIX}{lock_key}"

    async def _publish_event(self, payload: dict[str, Any]) -> None:
        try:
            await self._client().publish(self.EVENTS_CHANNEL, json.dumps(payload))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to publish router event: %s", exc)

    def _client(self) -> Any:
        return cast(Any, self.redis.client)

    async def _cache_backend_capabilities(
        self,
        backend_id: str,
        capabilities: Sequence[str],
        load_score: float,
    ) -> None:
        payload = {
            "backend_id": backend_id,
            "capabilities": list(capabilities),
            "load": float(load_score),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.redis.set(
            f"{self.BACKEND_CAPABILITIES_PREFIX}{backend_id}",
            json.dumps(payload),
            ex=self.ip_lock_ttl,
        )

    async def _record_task_owners(self, job: Job, backend_id: str) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        client = self._client()
        batch_size = 1000
        for offset in range(0, len(job.tasks), batch_size):
            pipe = client.pipeline(transaction=False)
            for task in job.tasks[offset : offset + batch_size]:
                owner_payload = {
                    "backend_id": backend_id,
                    "job_id": job.job_id,
                    "task_id": task.task_id,
                    "status": "assigned",
                    "assigned_at": timestamp,
                }
                pipe.set(
                    f"{self.TASK_OWNER_PREFIX}{task.task_id}",
                    json.dumps(owner_payload),
                    ex=self.task_owner_ttl,
                )
            await pipe.execute()

    async def _finalize_task_owners(
        self, job: Job, backend_id: str, success: bool
    ) -> None:
        completed_at = datetime.now(timezone.utc).isoformat()
        status = "completed" if success else "failed"
        for task in job.tasks:
            key = f"{self.TASK_OWNER_PREFIX}{task.task_id}"
            payload: dict[str, Any] = {
                "backend_id": backend_id,
                "job_id": job.job_id,
                "task_id": task.task_id,
                "status": status,
                "completed_at": completed_at,
            }
            cached = await self.redis.get(key)
            if cached:
                try:
                    cached_payload = json.loads(cached)
                except json.JSONDecodeError:
                    cached_payload = {}
                payload = {**cached_payload, **payload}
            await self.redis.set(key, json.dumps(payload), ex=self.task_owner_ttl)
