"""Schedule stale cached profiles only after the cached HTTP response is sent."""
from datetime import datetime
from collections.abc import Iterable
from typing import Any
from fastapi import BackgroundTasks
from ..helpers.user_refresh import needs_user_refresh


def queue_cached_profile_refresh(background_tasks: BackgroundTasks | None, users: Iterable[Any]) -> None:
    if background_tasks is None:
        return
    keys = set()
    for user in users:
        site = user.get("site") if isinstance(user, dict) else user.site
        user_id = user.get("user_id") if isinstance(user, dict) else user.user_id
        checked = user.get("last_checked_at") if isinstance(user, dict) else user.last_checked_at
        if isinstance(checked, str):
            checked = datetime.fromisoformat(checked)
        if needs_user_refresh(checked):
            keys.add((site.lower(), int(user_id)))
    if keys:
        background_tasks.add_task(refresh_cached_profiles, sorted(keys))


async def refresh_cached_profiles(keys: list[tuple[str, int]]) -> None:
    from ..api.routes.users import _ensure_profile_refresh_started
    from ..database.factory import MultiSiteDatabaseManager
    managers = MultiSiteDatabaseManager()
    for site, user_id in keys:
        site_db = managers.get_or_create_manager(site)
        if site_db is not None:
            _ensure_profile_refresh_started(site, user_id, site_db)
