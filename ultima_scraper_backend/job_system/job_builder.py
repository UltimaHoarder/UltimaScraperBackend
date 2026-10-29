"""Job and task builder for creating jobs with expanded tasks.

This module provides a clean, composable interface for building tasks and jobs:

1. `build_task()` - Creates a task with the appropriate payload
2. `build_job()` - Takes tasks, applies auth expansion, creates the job

Usage:
    from ultima_scraper_backend.job_system import (
        build_task, build_job, TaskType
    )
    from ultima_scraper_api import ContentType

    # Build a scrape task
    task = build_task(
        TaskType.SCRAPE,
        site_name="OnlyFans",
        user_id=123456,
        user_username="creator_name",
        content_type=ContentType.POSTS,
        download_media=True,  # Creates download task during auth expansion
    )

    # Build job (auth expansion happens automatically for auth_id=None)
    result = build_job(tasks=[task], job_name="My scrape job")

    # Or for API commands
    api_task = build_task(
        TaskType.API_COMMAND,
        site_name="OnlyFans",
        command="get_authed_user",
    )
    result = build_job(tasks=[api_task], job_name="API call")
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Sequence, cast
from uuid import uuid4

from ultima_scraper_api import CONTENT_VALUES, SUPPORTED_SITES_LITERALS, ContentType

from ..database import create_usr_database_manager
from .auth_expansion import (
    _create_download_task_for_scrape,
    _should_create_download_task,
    iter_scrape_tasks_by_db_subscriptions,
)
from .dependency_rules import apply_dependency_rules
from .models import Job, Task, TaskType
from .payloads import APICommandTaskPayload, DownloadTaskPayload, ScrapeTaskPayload

logger = logging.getLogger(__name__)


def _remove_dangling_dependencies(tasks: Sequence[Task]) -> list[Task]:
    """Remove dependency IDs that don't exist in the provided task graph.

    This prevents permanently blocked tasks when a previous build step rewrites
    task IDs (for example auth expansion of scrape tasks) while dependent tasks
    still reference the old IDs.
    """

    existing_task_ids = {task.task_id for task in tasks}
    normalized: list[Task] = []

    for task in tasks:
        if not task.depends_on:
            normalized.append(task)
            continue

        filtered_depends_on = [
            dep_id for dep_id in task.depends_on if dep_id in existing_task_ids
        ]
        if filtered_depends_on == task.depends_on:
            normalized.append(task)
            continue

        normalized.append(
            task.model_copy(update={"depends_on": filtered_depends_on}, deep=True)
        )

    return normalized


def _assign_download_auth_ids_from_scrapes(tasks: Sequence[Task]) -> list[Task]:
    """Assign missing download auth_id values from matching scrape tasks."""

    scrape_auth_by_content: dict[tuple[str, int, ContentType | None], set[int]] = {}
    scrape_auth_by_user: dict[tuple[str, int], set[int]] = {}

    for task in tasks:
        if task.task_type != TaskType.SCRAPE or task.auth_id is None:
            continue

        site_key = str(task.site_name).lower()
        user_id = int(task.user_id)
        content_type = cast(ContentType | None, task.content_type)
        user_key = (site_key, user_id)
        content_key = (site_key, user_id, content_type)

        if user_key not in scrape_auth_by_user:
            scrape_auth_by_user[user_key] = set()
        scrape_auth_by_user[user_key].add(task.auth_id)

        if content_key not in scrape_auth_by_content:
            scrape_auth_by_content[content_key] = set()
        scrape_auth_by_content[content_key].add(task.auth_id)

    updated: list[Task] = []
    for task in tasks:
        if task.task_type != TaskType.DOWNLOAD or task.auth_id is not None:
            updated.append(task)
            continue

        site_key = str(task.site_name).lower()
        user_id = int(task.user_id)
        content_type = cast(ContentType | None, task.content_type)
        content_key = (site_key, user_id, content_type)
        user_key = (site_key, user_id)

        auth_candidates = scrape_auth_by_content.get(content_key) or set()
        if len(auth_candidates) != 1:
            auth_candidates = scrape_auth_by_user.get(user_key) or set()

        if len(auth_candidates) == 1:
            assigned_auth_id = next(iter(auth_candidates))
            updated.append(task.model_copy(update={"auth_id": assigned_auth_id}))
        else:
            updated.append(task)

    return updated


# =============================================================================
# Task Builder
# =============================================================================


def build_task(
    task_type: TaskType,
    *,
    site_name: SUPPORTED_SITES_LITERALS,
    # Common fields for scrape/download
    user_id: int | None = None,
    user_username: str | None = None,
    content_type: ContentType | None = None,
    auth_id: int | None = None,
    # Scrape-specific
    download_media: bool = False,
    content_value: CONTENT_VALUES = "All",
    max_items: int | None = None,
    content_ids: list[int] | None = None,
    save_to_db: bool = True,
    rescrape_existing: bool = True,
    use_guest_auth: bool = False,
    guest_proxy_url: str | None = None,
    guest_fallback_proxy_urls: list[str] | None = None,
    guest_proxy_alternatives: dict[str, list[str]] | None = None,
    guest_direct_route_index: int | None = None,
    track_invalid_user: bool = False,
    paid_content_feed: bool = False,
    paid_content_performer: str | None = None,
    paid_content_types: list[str] | None = None,
    conclusive_miss_threshold: int | None = None,
    max_route_hops: int | None = None,
    # Download-specific
    media_ids: list[int] | None = None,
    ignore_space_requirements: bool = False,
    # API command-specific
    command: str | None = None,
    args: dict[str, Any] | None = None,
) -> Task:
    """Build a task with the appropriate payload.

    Args:
        task_type: Type of task (TaskType.SCRAPE, TaskType.DOWNLOAD, TaskType.API_COMMAND)
        site_name: Site to target (e.g., "OnlyFans", "Fansly")
        user_id: Target user/creator ID (required for scrape/download)
        user_username: Target username (required for scrape/download)
        content_type: Content type (required for scrape/download)
        auth_id: Specific auth ID to use (optional, expanded later if None)
        download_media: Create download task after scrape (scrape only)
        content_value: Content filter - "All", "Free", "Paid" (scrape only)
        max_items: Max items to process (scrape/download)
        content_ids: Specific content IDs to scrape (scrape only)
        save_to_db: Save scraped data to database (scrape only)
        rescrape_existing: Flush API cache to re-fetch all content (scrape only)
        media_ids: Specific media IDs to download (download only)
        command: API command name (api_command only)
        args: API command arguments (api_command only)

    Returns:
        Task with the appropriate payload configured
    """
    match task_type:
        case TaskType.SCRAPE:
            return _build_scrape_task(
                site_name=site_name,
                user_id=user_id,
                user_username=user_username,
                content_type=content_type,
                auth_id=auth_id,
                download_media=download_media,
                content_value=content_value,
                max_items=max_items,
                content_ids=content_ids,
                save_to_db=save_to_db,
                rescrape_existing=rescrape_existing,
                use_guest_auth=use_guest_auth,
                guest_proxy_url=guest_proxy_url,
                guest_fallback_proxy_urls=guest_fallback_proxy_urls,
                guest_proxy_alternatives=guest_proxy_alternatives,
                guest_direct_route_index=guest_direct_route_index,
                track_invalid_user=track_invalid_user,
                paid_content_feed=paid_content_feed,
                paid_content_performer=paid_content_performer,
                paid_content_types=paid_content_types,
                conclusive_miss_threshold=conclusive_miss_threshold,
                max_route_hops=max_route_hops,
                ignore_space_requirements=ignore_space_requirements,
            )
        case TaskType.DOWNLOAD:
            return _build_download_task(
                site_name=site_name,
                user_id=user_id,
                user_username=user_username,
                content_type=content_type,
                auth_id=auth_id,
                content_value=content_value,
                max_items=max_items,
                media_ids=media_ids,
                ignore_space_requirements=ignore_space_requirements,
            )
        case TaskType.API_COMMAND:
            return _build_api_command_task(
                site_name=site_name,
                command=command,
                args=args,
                auth_id=auth_id,
            )
        case _:
            raise ValueError(f"Unknown task type: {task_type}")


def _build_scrape_task(
    *,
    site_name: SUPPORTED_SITES_LITERALS,
    user_id: int | None,
    user_username: str | None,
    content_type: ContentType | None,
    auth_id: int | None,
    download_media: bool,
    content_value: CONTENT_VALUES,
    max_items: int | None,
    content_ids: list[int] | None,
    save_to_db: bool,
    rescrape_existing: bool,
    use_guest_auth: bool,
    guest_proxy_url: str | None,
    guest_fallback_proxy_urls: list[str] | None = None,
    guest_proxy_alternatives: dict[str, list[str]] | None = None,
    guest_direct_route_index: int | None = None,
    track_invalid_user: bool,
    paid_content_feed: bool,
    paid_content_performer: str | None,
    paid_content_types: list[str] | None,
    conclusive_miss_threshold: int | None = None,
    max_route_hops: int | None = None,
    ignore_space_requirements: bool,
) -> Task:
    """Build a scrape task."""
    if user_id is None:
        raise ValueError("user_id is required for scrape tasks")
    if user_username is None and not paid_content_feed:
        raise ValueError("user_username is required for scrape tasks")
    if content_type is None and not paid_content_feed:
        raise ValueError("content_type is required for scrape tasks")

    payload = ScrapeTaskPayload(
        user_username=user_username,
        content_value=content_value,
        max_items=max_items,
        content_ids=content_ids,
        download_media=download_media,
        save_to_db=save_to_db,
        rescrape_existing=rescrape_existing,
        use_guest_auth=use_guest_auth,
        guest_proxy_url=guest_proxy_url,
        guest_fallback_proxy_urls=list(guest_fallback_proxy_urls or []),
        guest_proxy_alternatives=guest_proxy_alternatives or {},
        guest_direct_route_index=guest_direct_route_index,
        track_invalid_user=track_invalid_user,
        paid_content_feed=paid_content_feed,
        paid_content_performer=paid_content_performer,
        paid_content_types=paid_content_types,
        conclusive_miss_threshold=conclusive_miss_threshold,
        max_route_hops=max_route_hops,
        ignore_space_requirements=ignore_space_requirements,
    )

    return Task(
        site_name=site_name,
        user_id=user_id,
        user_username=user_username,
        content_type=content_type,
        task_type=TaskType.SCRAPE,
        payload=payload,
        auth_id=auth_id,
    )


def _build_download_task(
    *,
    site_name: SUPPORTED_SITES_LITERALS,
    user_id: int | None,
    user_username: str | None,
    content_type: ContentType | None,
    auth_id: int | None,
    content_value: CONTENT_VALUES,
    max_items: int | None,
    media_ids: list[int] | None,
    ignore_space_requirements: bool,
) -> Task:
    """Build a download task."""
    if user_id is None:
        raise ValueError("user_id is required for download tasks")
    if user_username is None:
        raise ValueError("user_username is required for download tasks")
    if content_type is None:
        raise ValueError("content_type is required for download tasks")

    payload = DownloadTaskPayload(
        user_username=user_username,
        content_value=content_value,
        max_items=max_items,
        media_ids=media_ids,
        ignore_space_requirements=ignore_space_requirements,
    )

    return Task(
        site_name=site_name,
        user_id=user_id,
        user_username=user_username,
        content_type=content_type,
        task_type=TaskType.DOWNLOAD,
        payload=payload,
        auth_id=auth_id,
    )


def _build_api_command_task(
    *,
    site_name: SUPPORTED_SITES_LITERALS,
    command: str | None,
    args: dict[str, Any] | None,
    auth_id: int | None,
) -> Task:
    """Build an API command task."""
    if command is None:
        raise ValueError("command is required for api_command tasks")

    payload = APICommandTaskPayload(
        command=command,
        args=args or {},
    )

    return Task(
        site_name=site_name,
        user_id=0,  # Not applicable for API commands
        user_username="",  # Not applicable
        content_type=None,
        task_type=TaskType.API_COMMAND,
        payload=payload,
        auth_id=auth_id,
    )


# =============================================================================
# Job Builder
# =============================================================================


@dataclass
class JobBuildResult:
    """Result of building a job with task breakdown."""

    job: Job
    scrape_tasks: list[Task] = field(  # pyright: ignore[reportUnknownVariableType]
        default_factory=list
    )
    download_tasks: list[Task] = field(  # pyright: ignore[reportUnknownVariableType]
        default_factory=list
    )
    api_command_tasks: list[Task] = field(  # pyright: ignore[reportUnknownVariableType]
        default_factory=list
    )
    auth_ids_used: list[int] = field(  # pyright: ignore[reportUnknownVariableType]
        default_factory=list
    )

    @property
    def total_tasks(self) -> int:
        return len(self.job.tasks)

    def summary(self) -> str:
        """Return a human-readable summary of the job."""
        lines = [
            f"Job: {self.job.name} ({self.job.job_id})",
            f"  Total tasks: {self.total_tasks}",
            f"  Scrape tasks: {len(self.scrape_tasks)}",
            f"  Download tasks: {len(self.download_tasks)}",
            f"  API command tasks: {len(self.api_command_tasks)}",
            f"  Auth accounts: {len(self.auth_ids_used)}",
        ]
        return "\n".join(lines)


def iter_expanded_tasks(
    tasks: Iterable[Task], *, include_inactive_subscriptions: bool = False,
) -> Iterator[Task]:
    """Expand one source task at a time; release every owned site manager."""
    managers: dict[str, Any] = {}
    with ExitStack() as cleanup:
        for task in tasks:
            site = str(task.site_name).lower()
            if site not in managers:
                try:
                    managers[site] = create_usr_database_manager(site=site, read_only=True)
                    cleanup.callback(managers[site].disconnect)
                except FileNotFoundError:
                    logger.warning("Database not found for site=%s, using tasks without expansion", site)
                    managers[site] = None
            manager = managers[site]
            if manager is None:
                yield task
            elif task.task_type == TaskType.SCRAPE:
                payload = task.payload
                if isinstance(payload, ScrapeTaskPayload) and payload.use_guest_auth:
                    yield task
                    if _should_create_download_task(task):
                        yield _create_download_task_for_scrape(task)
                else:
                    yield from iter_scrape_tasks_by_db_subscriptions(
                        db_manager=manager, task=task,
                        include_inactive_subscriptions=include_inactive_subscriptions,
                    )
            elif task.task_type == TaskType.DOWNLOAD:
                payload = task.payload
                media_ids = (
                    payload.media_ids if isinstance(payload, DownloadTaskPayload)
                    else payload.get("media_ids") if isinstance(payload, dict) else None
                )
                if isinstance(media_ids, list) and len(media_ids) > 1:
                    for media_id in media_ids:
                        if isinstance(payload, DownloadTaskPayload):
                            new_payload = payload.model_copy(update={"media_ids": [media_id]})
                        else:
                            new_payload = {**payload, "media_ids": [media_id]}
                        yield task.model_copy(update={
                            "task_id": f"task-{uuid4().hex[:12]}", "payload": new_payload,
                        })
                else:
                    yield task
            else:
                yield task


def build_job(
    tasks: Sequence[Task],
    *,
    job_name: str = "Job",
    created_by: str = "job_builder",
    include_inactive_subscriptions: bool = False,
) -> JobBuildResult:
    """Build a job from a list of tasks.

    Tasks with auth_id=None are automatically expanded to one task per
    eligible auth account. Tasks with explicit auth_id are kept as-is.

    Args:
        tasks: List of tasks to include in the job
        job_name: Name for the job
        created_by: Creator identifier
        include_inactive_subscriptions: Include inactive subscription auths

    Returns:
        JobBuildResult with the built job and task breakdown
    """
    if not tasks:
        raise ValueError("At least one task is required")

    final_tasks: list[Task] = []
    all_auth_ids: set[int] = set()

    # Group tasks by site for efficient DB access
    tasks_by_site: dict[str, list[Task]] = {}
    for task in tasks:
        site = str(task.site_name).lower()
        if site not in tasks_by_site:
            tasks_by_site[site] = []
        tasks_by_site[site].append(task)

    # Keep site grouping and the same expansion rules for in-memory callers.
    for site_tasks in tasks_by_site.values():
        final_tasks.extend(iter_expanded_tasks(
            site_tasks,
            include_inactive_subscriptions=include_inactive_subscriptions,
        ))

    # Assign missing download auth_id values from scrape tasks when the
    # mapping is unambiguous.
    final_tasks = _assign_download_auth_ids_from_scrapes(final_tasks)

    # Remove stale dependency references caused by task ID rewrites during
    # auth expansion, then re-apply dependency rules (downloads -> scrapes).
    final_tasks = _remove_dangling_dependencies(final_tasks)

    # Apply dependency rules (e.g., downloads depend on scrapes)
    final_tasks = apply_dependency_rules(final_tasks)

    # Categorize tasks and collect auth IDs in single pass
    scrape_tasks: list[Task] = []
    download_tasks: list[Task] = []
    api_command_tasks: list[Task] = []
    for task in final_tasks:
        if task.auth_id is not None:
            all_auth_ids.add(task.auth_id)
        match task.task_type:
            case TaskType.SCRAPE:
                scrape_tasks.append(task)
            case TaskType.DOWNLOAD:
                download_tasks.append(task)
            case TaskType.API_COMMAND:
                api_command_tasks.append(task)

    # Build job
    job = Job(
        name=job_name,
        created_by=created_by,
        tasks=final_tasks,
    )

    return JobBuildResult(
        job=job,
        scrape_tasks=scrape_tasks,
        download_tasks=download_tasks,
        api_command_tasks=api_command_tasks,
        auth_ids_used=sorted(all_auth_ids),
    )
