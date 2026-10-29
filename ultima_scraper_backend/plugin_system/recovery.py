"""Retry only unavailable or interrupted database transactions."""

from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from ..security import redact_sensitive_text


def error_summary(error: BaseException) -> str:
    """Exclude SQL bind parameters and redact credentials in service errors."""
    original = getattr(error, "orig", error)
    sqlstate = getattr(original, "sqlstate", None)
    state = f" [{sqlstate}]" if sqlstate else ""
    return redact_sensitive_text(
        f"{type(error).__name__}{state}: {' '.join(str(original).split())}"
    )[:2000]


def database_interrupted(error: BaseException) -> bool:
    if isinstance(error, PoolTimeout):
        return True
    if not isinstance(error, DBAPIError):
        return False
    if error.connection_invalidated:
        return True
    original = error.orig
    sqlstate = getattr(original, "sqlstate", None)
    if sqlstate in {"40001", "40P01", "57P01", "57P02", "57P03"}:
        return True
    if sqlstate and sqlstate.startswith("08") and sqlstate != "08P01":
        return True
    if sqlstate == "08P01" and isinstance(error, OperationalError):
        return any(
            message in str(original)
            for message in ("query_wait_timeout", "server_login_retry", "server conn crashed?")
        )
    return isinstance(error, OperationalError) and (
        sqlstate is None or "server_login_retry" in str(original)
    )
