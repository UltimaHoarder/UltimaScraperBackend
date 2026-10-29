"""Decode bounded durable source pages into executable tasks."""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .models import Task, TaskResult, TaskState
from .mps_tasks import _build_mps_task_batch

_PROFILE_SITES = {"onlyfans": "OnlyFans", "fansly": "Fansly", "loyalfans": "LoyalFans"}


@dataclass
class SourcePage:
    tasks: list[Task]
    candidate_ordinals: dict[str, int]


async def ready_tasks(
    store, plan, *, offset, limit, byte_limit,
    exclude_task_ids: Sequence[str] = (),
    active_candidate_ordinals: Sequence[int] = (),
    candidate_cursor: int | None = None,
):
    selection = plan["selection"]
    tasks = []
    candidate_ordinals = {}
    manifest_id = selection.get("task_manifest_id")
    if manifest_id is None:
        total = selection.get("candidate_total", plan["total"])
        checkpoint = selection.get("candidate_checkpoint", {
            "next": min(offset, total), "pending": [],
        })
        excluded = set(active_candidate_ordinals)
        positions = []
        for first, last in checkpoint["pending"]:
            for position in range(first, last):
                if position not in excluded:
                    positions.append(position)
                if len(positions) >= limit:
                    break
            if len(positions) >= limit:
                break
        cursor = max(checkpoint["next"], candidate_cursor or 0)
        while len(positions) < limit and cursor < total:
            if cursor not in excluded:
                positions.append(cursor)
            cursor += 1
        ids = await asyncio.to_thread(store.candidate_ids, plan["job_id"], positions)
        if len(ids) != len(positions):
            raise RuntimeError("Candidate checkpoint differs from its durable selection")
        include_direct = bool(selection.get("include_direct_connection", False))
        max_route_hops = selection.get("max_route_hops")
        conclusive_miss_threshold = selection.get("conclusive_miss_threshold")
        mps_batch_kwargs: dict[str, Any] = {}
        if max_route_hops is not None:
            mps_batch_kwargs["max_route_hops"] = int(max_route_hops)
        if conclusive_miss_threshold is not None:
            mps_batch_kwargs["conclusive_miss_threshold"] = int(conclusive_miss_threshold)
        for position in positions:
            task = _build_mps_task_batch(
                job_id=plan["job_id"],
                site_display=_PROFILE_SITES[plan["site"]],
                target_user_ids=[ids[position]],
                proxy_pool=plan["proxy_pool"],
                candidate_offset=position,
                include_direct=include_direct,
                **mps_batch_kwargs,
            )[0]
            tasks.append(task)
            candidate_ordinals[task.task_id] = position
        manifest_id = selection.get("spawned_manifest_id") if cursor >= total else None
    if len(tasks) >= limit or manifest_id is None:
        return SourcePage(tasks, candidate_ordinals)
    definitions = await asyncio.to_thread(
        store.definitions.ready_page,
        manifest_id,
        limit=limit - len(tasks),
        byte_limit=byte_limit,
        exclude_task_ids=exclude_task_ids,
    )
    for definition in definitions:
        task = Task.from_dict(definition["task"])
        # SQL checked predecessors across windows. Preserve IDs for the saved
        # outputs handed to workers and for retry/dependency evidence.
        if definition["dependency_failed"]:
            task.state = TaskState.FAILED
            task.result = TaskResult(
                success=False,
                message=(
                    "Retry producer did not emit this task"
                    if definition.get("retry_missing_child")
                    else "Skipped because a dependency failed"
                ),
            )
            task.completed_at = task.result.completed_at
        tasks.append(task)
    return SourcePage(tasks, candidate_ordinals)
