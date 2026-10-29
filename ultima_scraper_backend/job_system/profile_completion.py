"""Recover one final MPS profile outcome from its committed SQL write proof."""

from __future__ import annotations

from sqlalchemy import text

from .models import Task, TaskResult, TaskType

PROFILE_COMPLETION_KEY = "_profile_completion"


def is_final_profile(task: Task) -> bool:
    return (
        task.task_type == TaskType.SCRAPE
        and task.mps_task_manifest is not None
        and str(getattr(task.content_type, "value", task.content_type)).casefold()
        == "profile"
        and not task.subtasks
        and not getattr(task.payload, "download_media", True)
        and not getattr(task.payload, "paid_content_feed", False)
    )


def profile_completion_intent(task, result, *, execution_key, operation, site):
    if not is_final_profile(task):
        raise ValueError("Completion proof is restricted to final MPS profile writes")
    output = result.output
    invalid = operation == "upsert_invalid_user"
    if (
        result.success is not True
        or result.new_tasks
        or result.errors
        or operation not in {"upsert_user", "upsert_invalid_user"}
        or not isinstance(output, dict)
        or str(output.get("user_id")) != str(task.user_id)
        or (invalid and output.get("invalid_user") is not True)
        or (
            not invalid
            and (
                str(
                    getattr(
                        output.get("content_type"), "value", output.get("content_type")
                    )
                ).casefold()
                != "profile"
                or output.get("items_saved") != 1
                or output.get("media_urls")
                or output.get("media_selection") is not None
            )
        )
        or not isinstance(execution_key, str)
        or not execution_key
        or not isinstance(site, str)
        or site.lower() != task.site_name.lower()
    ):
        raise ValueError("Invalid final profile completion intent")
    return {
        "version": 1,
        "execution_key": execution_key,
        "operation": operation,
        "site": site.lower(),
        "result": result.to_dict(),
    }


def read_committed_profile_completion(session, run_id, task, intent):
    if intent is None or not is_final_profile(task):
        return None
    if not isinstance(intent, dict) or intent.get("version") != 1:
        raise ValueError("Invalid profile completion checkpoint")
    result = TaskResult.from_dict(intent["result"])
    expected = profile_completion_intent(
        task,
        result,
        execution_key=intent["execution_key"],
        operation=intent["operation"],
        site=intent["site"],
    )
    if expected != intent:
        raise ValueError("Invalid profile completion checkpoint")
    row = session.execute(
        text("""
        SELECT r.operation,r.site,r.result_json
        FROM management.execution_mutations m
        JOIN management.database_write_receipts r
          ON r.command_id=m.command_id AND r.run_id=m.run_id
        WHERE m.run_id=:run_id AND m.task_id=:task_id
          AND m.mutation_key=:execution_key
    """),
        {
            "run_id": run_id,
            "task_id": task.task_id,
            "execution_key": intent["execution_key"],
        },
    ).one_or_none()
    if row is None:
        return None
    from ..database.write_queue import DatabaseWriteResult

    receipt = DatabaseWriteResult.from_json(row.result_json)
    if (
        row.operation != intent["operation"]
        or row.site != intent["site"]
        or not receipt.success
    ):
        raise ValueError("Profile completion has no matching successful write receipt")
    # A prepared result is not terminal evidence. Its completion time comes
    # from the successful receipt, never from another upstream lookup.
    result.completed_at = receipt.completed_at
    return result
