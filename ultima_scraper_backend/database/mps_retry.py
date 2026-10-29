"""Retry-owned MPS graphs, selected in SQL and copied in bounded pages.

Indexed SQL tables select the dependency closure without transferring the full
graph. The frozen selection then supports durable, bounded copy checkpoints.
Publication and source retirement happen only after the copy is complete.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from ultima_scraper_db_archive.global_db import MPSTaskDefinitionModel

from ..job_system.models import Task, TaskState


def retry_task_id(job_id: str, source_id: str) -> str:
    # Job IDs are generated UUIDs; JSON also makes the two components unambiguous.
    value = json.dumps([job_id, source_id], ensure_ascii=False).encode()
    return "task-" + hashlib.md5(value, usedforsecurity=False).hexdigest()


def reset_task(task: Task, job_id: str, auth_id: int | None) -> None:
    from ..job_system.payloads import ScrapeTaskPayload

    task.job_id = task.mps_task_manifest = job_id
    task.state = TaskState.PENDING
    task.worker_id = task.started_at = task.completed_at = task.result = None
    task.deferred_dedup_key = None
    task.created_at = task.updated_at = datetime.now(timezone.utc).isoformat()
    task.attempts = 0
    if auth_id is not None:
        task.auth_id = auth_id
        if isinstance(task.payload, ScrapeTaskPayload):
            task.payload.use_guest_auth = False


def freeze_graph(
    session: Any,
    source: Any,
    target: Any,
    *,
    live_tasks: list[dict[str, Any]],
    auth_id: int | None,
) -> tuple[int, bool]:
    selection = json.loads(source.selection_json)
    manifest = selection.get("task_manifest_id") or selection["spawned_manifest_id"]
    params = {
        "source": source.job_id,
        "manifest": manifest,
        "live": [task["task_id"] for task in live_tasks],
        "live_tasks": json.dumps(live_tasks),
    }
    # Session-local, transaction-owned workspaces. A writer may clone several
    # jobs in one receipt batch, so explicitly drop them on successful return.
    session.execute(
        text("""
        CREATE TEMP TABLE usr_mps_retry_nodes (
          id text PRIMARY KEY, ordinal bigint UNIQUE NOT NULL, task jsonb NOT NULL,
          dependencies text[] NOT NULL, range_ordinal bigint, failed boolean NOT NULL,
          children jsonb NOT NULL DEFAULT '[]', producer text
        ) ON COMMIT DROP
    """)
    )
    session.execute(
        text("""
        INSERT INTO usr_mps_retry_nodes(id, ordinal, task, dependencies, failed)
        SELECT node.task_id, node.ordinal + :prefix, node.task_json::jsonb-'result',
               node.dependency_ids,
               node.status='failed' OR node.task_id=ANY(CAST(:live AS text[]))
        FROM management.mps_task_definitions node WHERE node.manifest_id=:manifest
    """),
        {
            **params,
            "prefix": 0
            if selection.get("task_manifest_id")
            else selection["candidate_total"],
        },
    )
    if not selection.get("task_manifest_id"):
        # Only hybrid graphs need profile rows. The ordinary million-profile
        # range-only retry continues to use the existing compressed SQL copy.
        session.execute(
            text("""
            WITH history AS MATERIALIZED (
              SELECT task_id, user_id, status FROM management.job_tasks history
              WHERE job_id=:source AND NOT EXISTS (
                SELECT 1 FROM management.mps_task_definitions node
                WHERE node.manifest_id=:manifest AND node.task_id=history.task_id)
            )
            INSERT INTO usr_mps_retry_nodes
                (id, ordinal, task, dependencies, range_ordinal, failed)
            SELECT COALESCE(history.task_id, live.task_id, 'profile:' || candidate.id),
                   ranges.first_ordinal + candidate.id - ranges.start_id,
                   jsonb_build_object('user_id', candidate.id), ARRAY[]::text[],
                   ranges.first_ordinal + candidate.id - ranges.start_id,
                   COALESCE(history.status='failed', false) OR
                     COALESCE(history.task_id, live.task_id, 'profile:' || candidate.id)=ANY(CAST(:live AS text[]))
            FROM management.mps_candidate_ranges ranges
            CROSS JOIN LATERAL generate_series(ranges.start_id, ranges.end_id) candidate(id)
            LEFT JOIN history ON history.user_id=candidate.id
            LEFT JOIN jsonb_to_recordset(CAST(:live_tasks AS jsonb)) live(task_id text, user_id text)
              ON live.user_id=candidate.id::text AND NOT EXISTS (
                SELECT 1 FROM management.mps_task_definitions child
                WHERE child.manifest_id=:manifest AND child.task_id=live.task_id)
            WHERE ranges.job_id=:source
        """),
            params,
        )
    if live_tasks and not selection.get("task_manifest_id"):
        from .mps_manifest import mps_task_id

        for row in session.execute(
            text("""
            SELECT id, range_ordinal, task->>'user_id' AS user_id FROM usr_mps_retry_nodes
            WHERE range_ordinal IS NOT NULL AND id=ANY(:live)
        """),
            params,
        ):
            if row.id != mps_task_id(
                source.job_id, row.range_ordinal, int(row.user_id)
            ):
                raise ValueError(
                    "Live MPS failure has a different profile-task identity"
                )
    if session.scalar(
        text("""
        SELECT count(*) FROM unnest(CAST(:live AS text[])) failure(id)
        WHERE NOT EXISTS (SELECT 1 FROM usr_mps_retry_nodes node WHERE node.id=failure.id)
    """),
        params,
    ):
        raise ValueError(
            "Live MPS failure does not belong to the current task definition"
        )
    failures = int(
        session.scalar(text("SELECT count(*) FROM usr_mps_retry_nodes WHERE failed"))
    )
    if failures < source.failed:
        raise ValueError("Archived MPS failures lack task definition evidence")

    # Saved producer slots survive chained retries even before the producer has
    # run. Original jobs obtain the same provenance from the archived result.
    session.execute(
        text("""
        UPDATE usr_mps_retry_nodes node SET children=COALESCE(
          node.task->'_mps_retry_children', history.result_json::jsonb->'new_tasks', '[]')
        FROM management.job_tasks history WHERE history.job_id=:source AND history.task_id=node.id
    """),
        params,
    )
    session.execute(
        text("""
        UPDATE usr_mps_retry_nodes SET children=task->'_mps_retry_children'
        WHERE task ? '_mps_retry_children'
    """)
    )
    # Child discovery needs only identity/type metadata. Historical new_tasks
    # may contain complete media payloads, and saved slots can also occur in task.
    session.execute(text("""
        UPDATE usr_mps_retry_nodes node SET children=COALESCE((
          SELECT jsonb_agg(jsonb_build_object(
            'task_id', slot->'task_id', 'task_type', slot->'task_type',
            'site_name', slot->'site_name', 'user_id', slot->'user_id',
            'content_type', slot->'content_type',
            'paid_content_feed', slot->'paid_content_feed',
            'payload', jsonb_build_object('paid_content_feed', slot->'payload'->'paid_content_feed')
          ) ORDER BY slot_index)
          FROM jsonb_array_elements(node.children) WITH ORDINALITY entry(slot, slot_index)
        ), '[]'), task=task-'_mps_retry_children'
        WHERE children<>'[]'::jsonb OR task ? '_mps_retry_children'
    """))
    session.execute(
        text("""
        UPDATE usr_mps_retry_nodes child SET producer=parent.id
        FROM usr_mps_retry_nodes parent,
             LATERAL jsonb_array_elements(parent.children) slot
        WHERE child.id=slot->>'task_id'
    """)
    )
    session.execute(
        text("""
        UPDATE usr_mps_retry_nodes SET dependencies=array_append(dependencies, producer)
        WHERE producer IS NOT NULL AND NOT producer=ANY(dependencies)
    """)
    )
    session.execute(
        text(
            "CREATE TEMP TABLE usr_mps_retry_selected (id text PRIMARY KEY, ordinal bigint, read_bytes bigint) ON COMMIT DROP"
        )
    )
    session.execute(
        text(
            "INSERT INTO usr_mps_retry_selected(id) SELECT id FROM usr_mps_retry_nodes WHERE NOT :failed_only OR failed"
        ),
        {"failed_only": bool(failures)},
    )
    if failures:
        # A recursive UNION keeps a visited hash proportional to graph size.
        # Indexed SQL frontiers instead retain visited membership on disk.
        session.execute(
            text(
                "CREATE TEMP TABLE usr_mps_retry_frontier (id text PRIMARY KEY) ON COMMIT DROP"
            )
        )
        session.execute(
            text(
                "CREATE TEMP TABLE usr_mps_retry_next (id text PRIMARY KEY) ON COMMIT DROP"
            )
        )
        session.execute(
            text(
                "INSERT INTO usr_mps_retry_frontier SELECT id FROM usr_mps_retry_selected"
            )
        )
        while True:
            result = session.execute(
                text("""
                WITH added AS (
                  INSERT INTO usr_mps_retry_selected(id)
                  SELECT DISTINCT dependency.id FROM usr_mps_retry_frontier frontier
                  JOIN usr_mps_retry_nodes node ON node.id=frontier.id
                  CROSS JOIN LATERAL unnest(node.dependencies) dependency(id)
                  ON CONFLICT DO NOTHING RETURNING id
                ) INSERT INTO usr_mps_retry_next SELECT id FROM added
            """)
            )
            if not result.rowcount:
                break
            session.execute(text("TRUNCATE usr_mps_retry_frontier"))
            session.execute(
                text(
                    "INSERT INTO usr_mps_retry_frontier SELECT id FROM usr_mps_retry_next"
                )
            )
            session.execute(text("TRUNCATE usr_mps_retry_next"))
        session.execute(text("DROP TABLE usr_mps_retry_frontier, usr_mps_retry_next"))
    missing = session.execute(
        text("""
        SELECT chosen.id FROM usr_mps_retry_selected chosen
        LEFT JOIN usr_mps_retry_nodes node ON node.id=chosen.id WHERE node.id IS NULL
        UNION ALL
        SELECT dependency.id FROM usr_mps_retry_selected chosen
        JOIN usr_mps_retry_nodes node ON node.id=chosen.id
        CROSS JOIN LATERAL unnest(node.dependencies) dependency(id)
        LEFT JOIN usr_mps_retry_selected target ON target.id=dependency.id
        WHERE target.id IS NULL LIMIT 1
    """)
    ).first()
    if missing:
        raise ValueError(f"Retry depends on missing task {missing[0]}")

    # Size each selected value once. Recomputing sizes for every count-limited
    # lookahead would repeatedly detoast wide rows that do not fit this page.
    session.execute(text("""
        UPDATE usr_mps_retry_selected chosen SET ordinal=node.ordinal,
          read_bytes=octet_length((to_jsonb(node)-'children')::text) +
            octet_length(jsonb_build_object('selected_children',node.children)::text)
        FROM usr_mps_retry_nodes node WHERE node.id=chosen.id
    """))
    session.execute(text("""
        CREATE UNIQUE INDEX usr_mps_retry_selected_ordinal
        ON usr_mps_retry_selected(ordinal)
    """))

    # The selected graph is frozen in PostgreSQL. Subsequent writer commands
    # consume this snapshot; they never need to reread a changing source.
    result = session.execute(text("""
        INSERT INTO management.mps_retry_tasks(job_id, ordinal, task_id, read_bytes, payload)
        SELECT :job_id, chosen.ordinal, chosen.id, chosen.read_bytes,
          (to_jsonb(node)-'children') || jsonb_build_object('selected_children',COALESCE((
            SELECT jsonb_agg(slot || jsonb_build_object('task_id',child.id) ORDER BY slot_index)
            FROM jsonb_array_elements(node.children) WITH ORDINALITY entry(slot,slot_index)
            LEFT JOIN usr_mps_retry_selected child ON child.id=slot->>'task_id'
          ), '[]'))
        FROM usr_mps_retry_selected chosen
        JOIN usr_mps_retry_nodes node ON node.id=chosen.id
    """), {"job_id": target.job_id})
    session.execute(text("DROP TABLE usr_mps_retry_selected, usr_mps_retry_nodes"))
    return result.rowcount, bool(failures)


def retry_batch_limits(job_config: dict[str, Any] | None = None) -> tuple[int, int]:
    from .write_queue import DatabaseWriteClient, _positive_int_env

    if job_config and job_config.get("metadata", {}).get("durable_job_kind") == "ordinary":
        from ..api.routes.job_central_store import _central_task_insert_batch_size
        from ..api.routes.jobs import _central_task_insert_max_bytes

        return _central_task_insert_batch_size(), _positive_int_env(
            "ULTIMA_JOB_RETRY_BATCH_MAX_BYTES", 0
        ) or _central_task_insert_max_bytes()

    from ..job_system.resources import window_policy
    from ..job_system.storage import JobStorage

    metadata = (job_config or {}).get("metadata") or {}
    return (
        window_policy(metadata, JobStorage).replacement_rows,
        _positive_int_env(
            "ULTIMA_MPS_RETRY_BATCH_MAX_BYTES",
            _positive_int_env("ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES),
        ),
    )


def copy_graph_page(session: Any, source: Any, target: Any, state: dict[str, Any]) -> bool:
    """Copy and release one count/byte-bounded page in the caller's transaction."""
    from ..job_system.mps_tasks import _build_mps_task_batch, get_cached_proxy_geo_map
    from .mps_definitions import MPSDefinitionBudgetExceeded

    target_config = json.loads(target.job_config_json)
    batch_size, byte_limit = retry_batch_limits(target_config)
    total, after = state["copied"], state["after"]
    failures, auth_id = state["failed_only"], state.get("auth_id")
    counts: Counter[str] = Counter(state["type_counts"])
    proxy_pool = json.loads(source.proxy_pool_json)
    source_selection = (
        json.loads(source.selection_json)
        if getattr(source, "selection_json", None)
        else {}
    )
    include_direct = bool(source_selection.get("include_direct_connection", False))
    max_route_hops = source_selection.get("max_route_hops")
    conclusive_miss_threshold = source_selection.get("conclusive_miss_threshold")
    mps_batch_kwargs: dict[str, Any] = {}
    if max_route_hops is not None:
        mps_batch_kwargs["max_route_hops"] = int(max_route_hops)
    if conclusive_miss_threshold is not None:
        mps_batch_kwargs["conclusive_miss_threshold"] = int(conclusive_miss_threshold)
    site_display = {
        "onlyfans": "OnlyFans",
        "fansly": "Fansly",
        "loyalfans": "LoyalFans",
    }[source.site]
    # Seek by the complete primary key before filtering this job. Sparse
    # single-job estimates otherwise favor a full bitmap scan and sort.
    # Neighboring jobs contribute at most one page of metadata, never payloads.
    rows = session.execute(text("""
        WITH candidates AS MATERIALIZED (
          SELECT ordinal, task_id AS id, read_bytes FROM (
            SELECT job_id, ordinal, task_id, read_bytes FROM management.mps_retry_tasks
            WHERE (job_id, ordinal) > (:job_id, :after)
            ORDER BY job_id, ordinal LIMIT :limit
          ) page WHERE job_id=:job_id
        ), sized AS (
          SELECT *, sum(read_bytes) OVER (ORDER BY ordinal ROWS UNBOUNDED PRECEDING) AS bytes_through_row
          FROM candidates
        )
        SELECT id, ordinal, read_bytes,
          CASE WHEN bytes_through_row<=:byte_limit THEN (
            SELECT payload FROM management.mps_retry_tasks task
            WHERE task.job_id=:job_id AND task.ordinal=sized.ordinal
          ) END AS payload
        FROM sized ORDER BY ordinal
    """), {"job_id": target.job_id, "after": after, "limit": batch_size, "byte_limit": byte_limit})
    page = []
    for row in rows:
        if row.payload is None:
            if not page:
                raise MPSDefinitionBudgetExceeded(row.id, int(row.read_bytes), byte_limit)
            break
        page.append(row.payload)
    if not page:
        return True
    # Retry preparation also owns plan/run locks. Resolve geolocation with the
    # caller's connection instead of borrowing one while those locks are held.
    mps_batch_kwargs["proxy_geo_map"] = (
        get_cached_proxy_geo_map(db_session=session)
        if proxy_pool and any(item["range_ordinal"] is not None for item in page)
        else {}
    )
    definitions = []
    definition_bytes = 0
    for item in page:
        if item["range_ordinal"] is not None:
            task = _build_mps_task_batch(
                job_id=source.job_id,
                site_display=site_display,
                target_user_ids=[item["task"]["user_id"]],
                proxy_pool=proxy_pool,
                candidate_offset=item["range_ordinal"],
                include_direct=include_direct,
                **mps_batch_kwargs,
            )[0]
        else:
            task = Task.from_dict(item["task"])

        def remap(node: Task) -> None:
            node.task_id = retry_task_id(target.job_id, node.task_id)
            node.depends_on = [
                retry_task_id(target.job_id, old) for old in node.depends_on
            ]
            if node.parent_task_id:
                node.parent_task_id = retry_task_id(
                    target.job_id, node.parent_task_id
                )
            reset_task(node, target.job_id, auth_id)
            for subtask in node.subtasks:
                remap(subtask)

        task.task_id = item["id"]
        task.depends_on = item["dependencies"]
        remap(task)
        payload = task.to_dict()
        slots = []
        for child in item["selected_children"]:
            slots.append(
                {
                    key: child.get(key)
                    for key in ("task_type", "site_name", "user_id", "content_type")
                }
            )
            slots[-1]["task_id"] = (
                retry_task_id(target.job_id, child["task_id"])
                if child["task_id"]
                else None
            )
            slots[-1]["paid_content_feed"] = bool(
                child.get("paid_content_feed")
                or child.get("payload", {}).get("paid_content_feed")
            )
        payload["_mps_retry_children"] = slots
        payload["_mps_retry_prerequisite"] = bool(failures and not item["failed"])
        if item["producer"]:
            payload["_mps_retry_waiting_for"] = retry_task_id(
                target.job_id, item["producer"]
            )
        task_json = json.dumps(payload)
        task_bytes = len(task_json.encode("utf-8"))
        if target_config.get("metadata", {}).get("durable_job_kind") == "ordinary":
            from .write_queue import DatabaseWriteClient, _positive_int_env

            input_limit = _positive_int_env("ULTIMA_JOB_TASK_INPUT_MAX_BYTES", 0) or _positive_int_env(
                "ULTIMA_DATABASE_WRITE_MAX_COMMAND_BYTES", DatabaseWriteClient.MAX_COMMAND_BYTES
            )
            if task_bytes > input_limit:
                raise MPSDefinitionBudgetExceeded(task.task_id, task_bytes, input_limit)
        size = task_bytes + len(json.dumps(task.depends_on).encode("utf-8"))
        if size > byte_limit:
            raise MPSDefinitionBudgetExceeded(task.task_id, size, byte_limit)
        if definitions and definition_bytes + size > byte_limit:
            session.execute(insert(MPSTaskDefinitionModel), definitions)
            definitions = []
            definition_bytes = 0
        definitions.append(
            {
                "manifest_id": target.job_id,
                "ordinal": total,
                "task_id": task.task_id,
                "task_json": task_json,
                "dependency_ids": task.depends_on,
                "status": "pending",
            }
        )
        definition_bytes += size
        total += 1
        counts[task.task_type.value] += 1
    session.execute(insert(MPSTaskDefinitionModel), definitions)
    after = page[-1]["ordinal"]
    session.execute(text("""
        DELETE FROM management.mps_retry_tasks
        WHERE job_id=:job_id AND ordinal>:before AND ordinal<=:after
    """), {"job_id": target.job_id, "before": state["after"], "after": after})
    state.update(after=after, copied=total, type_counts=dict(counts))
    return total == target.total


def clone_graph(
    session: Any, source: Any, target: Any, *,
    live_tasks: list[dict[str, Any]], auth_id: int | None,
) -> tuple[int, bool, dict[str, int]]:
    """Atomic compatibility entry point for callers that own one transaction."""
    total, failures = freeze_graph(session, source, target, live_tasks=live_tasks, auth_id=auth_id)
    target.total = total
    state = {"after": -1, "copied": 0, "type_counts": {}, "failed_only": failures, "auth_id": auth_id}
    while not copy_graph_page(session, source, target, state):
        pass
    return total, failures, state["type_counts"]


def reconcile_children(plan: Any, rows: list[dict[str, Any]], session: Any) -> None:
    """Refresh cloned children from their rerun producer without admitting twice.

    The ordered emission slots are durable provenance, not a task-type guess.
    A changed slot fails atomically; an absent slot becomes explicit failed
    evidence when its producer completes, rather than executing a stale payload.
    """
    manifest = json.loads(plan.selection_json).get("task_manifest_id")
    if manifest is None:
        return
    definitions = dict(
        session.execute(
            text("""
        SELECT task_id, task_json::jsonb FROM management.mps_task_definitions
        WHERE manifest_id=:manifest AND task_id=ANY(:ids)
    """),
            {"manifest": manifest, "ids": [row["task_id"] for row in rows]},
        ).all()
    )
    for row in rows:
        source = definitions.get(row["task_id"], {})
        if row["status"] != "completed" or "_mps_retry_children" not in source:
            continue
        slots = source["_mps_retry_children"]
        result = json.loads(row.get("result_json") or "{}")
        emitted = result.get("new_tasks", [])
        if not emitted:
            continue
        if len({child["task_id"] for child in emitted}) != len(emitted):
            raise ValueError("Retry producer emitted duplicate task IDs")
        mappings, selected = {}, []
        for index, raw in enumerate(emitted):
            target_id = raw["task_id"]
            if index < len(slots):
                slot = slots[index]
                paid_feed = slot.get("paid_content_feed") and raw.get(
                    "payload", {}
                ).get("paid_content_feed")
                fields = ("task_type", "site_name", "content_type")
                if not paid_feed:
                    fields += ("user_id",)
                elif raw.get("user_id") != raw.get("auth_id"):
                    raise ValueError(
                        "Paid-feed retry child does not belong to its auth"
                    )
                if any(slot.get(key) != raw.get(key) for key in fields):
                    raise ValueError(
                        "Retry producer changed the identity of an emitted task slot"
                    )
                target_id = slot["task_id"]
            elif source["_mps_retry_prerequisite"]:
                target_id = None
            mappings[raw["task_id"]] = target_id
            selected.append(target_id)
        retained = {
            entry.task_id: entry
            for entry in session.execute(
                text("""
                SELECT task_id, task_json::jsonb AS task, dependency_ids, status
                FROM management.mps_task_definitions
                WHERE manifest_id=:manifest AND task_id=ANY(:ids)
            """),
                {
                    "manifest": manifest,
                    "ids": [identifier for identifier in selected if identifier],
                },
            )
        }
        admitted, evidence = [], []
        updates = []
        for raw, target_id in zip(emitted, selected):
            if target_id is None:
                continue
            task = Task.from_dict(raw)
            task.task_id = target_id
            dependencies = [mappings.get(dep, dep) for dep in task.depends_on]
            if None in dependencies:
                raise ValueError("Retry child depends on an excluded emission")
            task.depends_on = dependencies
            reset_task(
                task, plan.job_id, json.loads(plan.selection_json).get("retry_auth_id")
            )
            existing = retained.get(target_id)
            if existing:
                if (
                    existing.status != "pending"
                    or existing.task.get("_mps_retry_waiting_for") != row["task_id"]
                ):
                    raise ValueError("Retry child no longer awaits its producer")
                if not set(task.depends_on) <= set(existing.dependency_ids):
                    raise ValueError(
                        "Retry producer changed a retained child's dependencies"
                    )
                task.depends_on = list(
                    dict.fromkeys([*existing.dependency_ids, *task.depends_on])
                )
                payload = {**existing.task, **task.to_dict()}
                payload.pop("_mps_retry_waiting_for", None)
                updates.append(
                    {
                        "task": json.dumps(payload),
                        "deps": task.depends_on,
                        "manifest": manifest,
                        "id": target_id,
                    }
                )
            else:
                admitted.append(task.to_dict())
            evidence.append(task.to_dict())
        # Fresh dependency edges within this emission must remain acyclic,
        # including nodes which already have a cloned definition.
        identities = {task["task_id"] for task in evidence}
        remaining = {
            task["task_id"]: len(set(task["depends_on"]) & identities)
            for task in evidence
        }
        dependents = {}
        for task in evidence:
            for dependency in set(task["depends_on"]) & identities:
                dependents.setdefault(dependency, []).append(task["task_id"])
        ready = deque(
            identifier for identifier, count in remaining.items() if not count
        )
        visited = 0
        while ready:
            visited += 1
            for dependent in dependents.get(ready.popleft(), ()):
                remaining[dependent] -= 1
                if not remaining[dependent]:
                    ready.append(dependent)
        if visited != len(identities):
            raise ValueError("Retry producer emitted cyclic task dependencies")
        if updates:
            session.execute(
                text("""
                UPDATE management.mps_task_definitions SET task_json=:task, dependency_ids=:deps
                WHERE manifest_id=:manifest AND task_id=:id
            """),
                updates,
            )
            missing = session.execute(
                text("""
                SELECT dependency.id FROM management.mps_task_definitions node
                CROSS JOIN LATERAL unnest(node.dependency_ids) dependency(id)
                LEFT JOIN management.mps_task_definitions target
                  ON target.manifest_id=node.manifest_id AND target.task_id=dependency.id
                WHERE node.manifest_id=:manifest AND node.task_id=ANY(:ids)
                  AND target.task_id IS NULL AND NOT dependency.id=ANY(:admitted)
                LIMIT 1
            """),
                {
                    "manifest": manifest,
                    "ids": list(retained),
                    "admitted": [task["task_id"] for task in admitted],
                },
            ).first()
            if missing:
                raise ValueError(f"Retry child depends on missing task {missing[0]}")
        # Keep all emission slots (including excluded siblings) for another
        # retry. A newly discovered child must not become an independent task
        # alongside the next producer's duplicate emission.
        refreshed_slots = list(slots)
        for index, (raw, identifier) in enumerate(zip(emitted, selected)):
            slot = {
                key: raw.get(key)
                for key in ("task_type", "site_name", "user_id", "content_type")
            }
            slot.update(
                task_id=identifier,
                paid_content_feed=bool(raw.get("payload", {}).get("paid_content_feed")),
            )
            if index < len(refreshed_slots):
                refreshed_slots[index] = slot
            else:
                refreshed_slots.append(slot)
        source["_mps_retry_children"] = refreshed_slots
        session.execute(
            text(
                "UPDATE management.mps_task_definitions SET task_json=:task WHERE manifest_id=:manifest AND task_id=:id"
            ),
            {"task": json.dumps(source), "manifest": manifest, "id": row["task_id"]},
        )
        # append_spawned sees only new rows; retained history must still describe
        # the actual child identities, including refreshed already-owned nodes.
        result["new_tasks"] = admitted
        row["result_json"] = json.dumps(result)
        result["new_tasks"] = evidence
        session.execute(
            text(
                "UPDATE management.job_tasks SET result_json=:result WHERE job_id=:job AND task_id=:id"
            ),
            {"result": json.dumps(result), "job": plan.job_id, "id": row["task_id"]},
        )
