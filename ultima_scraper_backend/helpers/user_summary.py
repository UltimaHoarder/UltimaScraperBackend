"""Shared response shaping for API user objects."""

from typing import Any

_SUMMARY_FIELDS = (
    "id",
    "username",
    "name",
    "display_name",
    "about",
    "joining",
    "join_date",
    "created_at",
)


def summarize_user(user: object) -> dict[str, Any]:
    """Extract the common user fields exposed by backend responses."""

    summary = {
        field: getattr(user, field, None)
        for field in _SUMMARY_FIELDS
        if hasattr(user, field)
    }
    if hasattr(user, "raw"):
        summary["raw"] = getattr(user, "raw")
    return summary
