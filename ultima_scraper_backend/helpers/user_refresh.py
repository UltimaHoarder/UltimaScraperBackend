"""Cache refresh policy for site user records."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

USER_REFRESH_MAX_AGE = timedelta(days=7)


def normalize_utc(dt: datetime | None) -> datetime | None:
    """Normalize database timestamps to timezone-aware UTC."""

    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def needs_user_refresh(
    last_checked_at: datetime | None, *, now: datetime | None = None
) -> bool:
    """Return ``True`` when cached user data should be refreshed from the API."""

    normalized = normalize_utc(last_checked_at)
    if normalized is None:
        return True
    current_time = now or datetime.now(timezone.utc)
    return current_time - normalized > USER_REFRESH_MAX_AGE
