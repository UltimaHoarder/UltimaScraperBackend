"""Task types API routes.

Provides task definition metadata for UI job creation.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])


class TaskParamType(str, Enum):
    """Parameter input types for task configuration."""

    TEXT = "text"
    NUMBER = "number"
    BOOLEAN = "boolean"
    SELECT = "select"
    MULTISELECT = "multiselect"
    USERNAME = "username"
    USER_ID = "user_id"
    PATH = "path"


class TaskParameterOption(BaseModel):
    """Option for select/multiselect parameters."""

    label: str
    value: str


class TaskParameter(BaseModel):
    """Definition of a single task parameter."""

    name: str
    label: str
    param_type: TaskParamType
    required: bool = False
    default: Any = None
    description: str = ""
    options: List[TaskParameterOption] = []
    placeholder: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None


class TaskDefinition(BaseModel):
    """Definition of an available task type."""

    task_id: str
    name: str
    category: str
    description: str
    parameters: List[TaskParameter] = []
    keywords: List[str] = []


# Task definitions exposed by the control-plane API.
TASK_DEFINITIONS: List[TaskDefinition] = [
    TaskDefinition(
        task_id="scrape_user",
        name="Scrape User Profile",
        category="Scraping",
        description="Scrape a user's profile, posts, messages, stories, and media",
        keywords=["download", "profile", "performer", "creator", "posts", "messages"],
        parameters=[
            TaskParameter(
                name="site",
                label="Site",
                param_type=TaskParamType.SELECT,
                required=True,
                default="OnlyFans",
                description="Which site to scrape from",
                options=[
                    TaskParameterOption(label="OnlyFans", value="OnlyFans"),
                    TaskParameterOption(label="Fansly", value="Fansly"),
                ],
            ),
            TaskParameter(
                name="username",
                label="Username",
                param_type=TaskParamType.USERNAME,
                required=True,
                description="Username of the performer",
                placeholder="creator_name",
            ),
            TaskParameter(
                name="content_types",
                label="Content Types",
                param_type=TaskParamType.MULTISELECT,
                required=False,
                default=[
                    "Posts",
                    "Messages",
                    "Stories",
                    "Highlights",
                    "ArchivedPosts",
                    "Profile",
                ],
                description="Types of content to scrape",
                options=[
                    TaskParameterOption(label="Posts", value="Posts"),
                    TaskParameterOption(label="Messages", value="Messages"),
                    TaskParameterOption(label="Stories", value="Stories"),
                    TaskParameterOption(label="Highlights", value="Highlights"),
                    TaskParameterOption(label="Archived Posts", value="ArchivedPosts"),
                    TaskParameterOption(label="Profile", value="Profile"),
                ],
            ),
            TaskParameter(
                name="download_media",
                label="Download Media",
                param_type=TaskParamType.BOOLEAN,
                default=True,
                description="Download media files after scraping",
            ),
            TaskParameter(
                name="max_items",
                label="Max Items",
                param_type=TaskParamType.NUMBER,
                default=None,
                description="Maximum number of items to scrape (leave empty for all)",
                placeholder="100",
                min_value=1,
            ),
        ],
    ),
    TaskDefinition(
        task_id="scrape_paid_content",
        name="Scrape Paid Content",
        category="Scraping",
        description="Scrape purchased posts and messages from an auth account's paid feed",
        keywords=["paid", "purchases", "ppv", "messages", "posts"],
        parameters=[
            TaskParameter(
                name="site",
                label="Site",
                param_type=TaskParamType.SELECT,
                required=True,
                default="OnlyFans",
                description="Which site to scrape from",
                options=[
                    TaskParameterOption(label="OnlyFans", value="OnlyFans"),
                    TaskParameterOption(label="Fansly", value="Fansly"),
                ],
            ),
            TaskParameter(
                name="performer",
                label="Performer Filter",
                param_type=TaskParamType.TEXT,
                required=False,
                description="Optional username or numeric user ID to limit the paid feed",
                placeholder="creator_name or 123456",
            ),
            TaskParameter(
                name="content_types",
                label="Content Types",
                param_type=TaskParamType.MULTISELECT,
                required=False,
                default=["Posts", "Messages"],
                description="Paid feed types to keep",
                options=[
                    TaskParameterOption(label="Posts", value="Posts"),
                    TaskParameterOption(label="Messages", value="Messages"),
                ],
            ),
            TaskParameter(
                name="download_media",
                label="Download Media",
                param_type=TaskParamType.BOOLEAN,
                default=True,
                description="Spawn download tasks for the purchased content after scraping",
            ),
            TaskParameter(
                name="max_items",
                label="Max Items",
                param_type=TaskParamType.NUMBER,
                default=None,
                description="Maximum number of paid feed items to fetch (leave empty for all)",
                placeholder="100",
                min_value=1,
            ),
        ],
    ),
    TaskDefinition(
        task_id="download_media",
        name="Download Media",
        category="Download",
        description="Download media files for a user",
        keywords=["images", "videos", "files"],
        parameters=[
            TaskParameter(
                name="site",
                label="Site",
                param_type=TaskParamType.SELECT,
                required=True,
                default="OnlyFans",
                description="Which site to download from",
                options=[
                    TaskParameterOption(label="OnlyFans", value="OnlyFans"),
                    TaskParameterOption(label="Fansly", value="Fansly"),
                ],
            ),
            TaskParameter(
                name="username",
                label="Username",
                param_type=TaskParamType.USERNAME,
                required=True,
                placeholder="creator_name",
            ),
            TaskParameter(
                name="content_types",
                label="Content Types",
                param_type=TaskParamType.MULTISELECT,
                required=False,
                default=[
                    "Posts",
                    "Messages",
                    "Stories",
                    "Highlights",
                    "ArchivedPosts",
                    "Profile",
                ],
                description="Types of content to download media from",
                options=[
                    TaskParameterOption(label="Posts", value="Posts"),
                    TaskParameterOption(label="Messages", value="Messages"),
                    TaskParameterOption(label="Stories", value="Stories"),
                    TaskParameterOption(label="Highlights", value="Highlights"),
                    TaskParameterOption(label="Archived Posts", value="ArchivedPosts"),
                    TaskParameterOption(label="Profile", value="Profile"),
                ],
            ),
            TaskParameter(
                name="media_type",
                label="Media Type",
                param_type=TaskParamType.SELECT,
                default="all",
                description="Filter by media type",
                options=[
                    TaskParameterOption(label="All Media", value="all"),
                    TaskParameterOption(label="Images Only", value="images"),
                    TaskParameterOption(label="Videos Only", value="videos"),
                ],
            ),
        ],
    ),
    TaskDefinition(
        task_id="refresh_subscriptions",
        name="Refresh Subscriptions",
        category="Account",
        description="Update the list of active subscriptions for all accounts",
        keywords=["subs", "following", "update"],
        parameters=[
            TaskParameter(
                name="site",
                label="Site",
                param_type=TaskParamType.SELECT,
                required=True,
                default="OnlyFans",
                description="Which site to refresh",
                options=[
                    TaskParameterOption(label="OnlyFans", value="OnlyFans"),
                    TaskParameterOption(label="Fansly", value="Fansly"),
                ],
            ),
        ],
    ),
]

TASK_CATEGORIES = ["Scraping", "Download", "Account", "Advanced"]


@router.get("/types", response_model=List[TaskDefinition])
async def list_task_types() -> List[TaskDefinition]:
    """List all available task types for job creation.

    Returns:
        List of task definitions with parameters
    """
    return TASK_DEFINITIONS


@router.get("/types/{task_id}", response_model=TaskDefinition)
async def get_task_type(task_id: str) -> TaskDefinition:
    """Get a specific task type definition.

    Args:
        task_id: Task type ID

    Returns:
        Task definition
    """
    for task_def in TASK_DEFINITIONS:
        if task_def.task_id == task_id:
            return task_def

    from fastapi import HTTPException

    raise HTTPException(status_code=404, detail=f"Task type '{task_id}' not found")


@router.get("/categories", response_model=List[str])
async def list_task_categories() -> List[str]:
    """List available task categories.

    Returns:
        List of category names
    """
    return TASK_CATEGORIES
