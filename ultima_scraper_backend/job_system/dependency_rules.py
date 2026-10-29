"""Task dependency rules.

This module defines rules for automatically adding dependencies between tasks.
Rules are applied during job building to ensure proper task execution order.
"""

from collections.abc import Sequence

from .models import Task, TaskType


def apply_dependency_rules(tasks: Sequence[Task]) -> list[Task]:
    """Apply all dependency rules to a list of tasks.

    Rules are applied in order:
    1. Downloads depend on scrapes (for matching site/auth combinations)

    Args:
        tasks: List of tasks to process

    Returns:
        List of tasks with dependencies updated
    """
    # For large lists, we avoid deep copying EVERYTHING.
    # We only copy tasks that are actually modified.
    modified_tasks = list(tasks)

    # Apply rules in order
    modified_tasks = _apply_download_depends_on_scrape(modified_tasks)

    return modified_tasks


def _apply_download_depends_on_scrape(tasks: list[Task]) -> list[Task]:
    """Make download tasks depend on scrape tasks."""
    # Index scrape tasks by site and user_id for fast lookup
    scrape_tasks_by_site: dict[str, list[str]] = {} # site -> [task_id]
    scrape_tasks_by_user: dict[tuple[str, int], str] = {}  # (site, user_id) -> task_id

    # We also need to map auth_id for site-wide match logic
    scrape_tasks_by_site_auth: dict[tuple[str, str | None], list[str]] = {}  # (site, auth_id) -> [task_id]

    for task in tasks:
        if task.task_type == TaskType.SCRAPE:
            site = str(task.site_name).lower()
            tid = task.task_id
            
            if site not in scrape_tasks_by_site:
                scrape_tasks_by_site[site] = []
            scrape_tasks_by_site[site].append(tid)

            if task.user_id:
                scrape_tasks_by_user[(site, int(task.user_id))] = tid
            
            auth_key = (site, None if task.auth_id is None else str(task.auth_id))
            if auth_key not in scrape_tasks_by_site_auth:
                scrape_tasks_by_site_auth[auth_key] = []
            scrape_tasks_by_site_auth[auth_key].append(tid)

    # No scrape tasks? Nothing to do
    if not scrape_tasks_by_site:
        return tasks

    # Add dependencies to download tasks
    for i, task in enumerate(tasks):
        if task.task_type != TaskType.DOWNLOAD:
            continue

        site = str(task.site_name).lower()
        new_deps = set(task.depends_on)
        matched = False

        # Priority 1: Match by user_id (most specific)
        if task.user_id:
            user_key = (site, int(task.user_id))
            if user_key in scrape_tasks_by_user:
                scrape_tid = scrape_tasks_by_user[user_key]
                if scrape_tid not in new_deps:
                    new_deps.add(scrape_tid)
                matched = True

        # Priority 2: Fall back to site/auth matching
        if not matched:
            # Match matching auth_id
            auth_key = (site, None if task.auth_id is None else str(task.auth_id))
            for scrape_tid in scrape_tasks_by_site_auth.get(auth_key, []):
                new_deps.add(scrape_tid)
            
            # Also match wildcards (auth_id is None in SCRAPE)
            wildcard_key = (site, None)
            if task.auth_id is not None: # only if we haven't already checked it
                for scrape_tid in scrape_tasks_by_site_auth.get(wildcard_key, []):
                    new_deps.add(scrape_tid)

        # Update task's depends_on if changed
        if len(new_deps) != len(task.depends_on):
            # We must copy only if we modify
            # But the 'tasks' list passed in is already a copy of the input Sequence
            # However, the Task objects themselves are references.
            # We use model_copy(update=...) to be safe and efficient.
            tasks[i] = task.model_copy(update={"depends_on": sorted(new_deps)})

    return tasks


# Future rules can be added here:
#
# def _apply_some_other_rule(tasks: list[Task]) -> list[Task]:
#     """Description of rule."""
#     ...
#     return tasks
#
# Then add to apply_dependency_rules():
#     modified_tasks = _apply_some_other_rule(modified_tasks)
