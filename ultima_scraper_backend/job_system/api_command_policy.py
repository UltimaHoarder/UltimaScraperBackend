"""Retry policy for anonymous profile reads, separate from other API commands."""

PROFILE_REFRESH_MAX_ATTEMPTS = 3
WORKER_LEASE_EXHAUSTED = "Worker lease expired; attempt budget exhausted"
WORKER_LEASE_ERROR_CODE = "worker_lease_expired"


def expired_worker_result(result) -> bool:
    """Recognize structured failures and previously archived lease failures."""
    payload = result.payload or {}
    return not result.success and (
        payload.get("error_code") == WORKER_LEASE_ERROR_CODE
        or result.message == WORKER_LEASE_EXHAUSTED
    )
