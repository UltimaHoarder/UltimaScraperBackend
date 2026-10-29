"""Bounded read-only index warming; archive ownership remains trigger-enforced."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, wait
from itertools import islice

from sqlalchemy import text
from ultima_scraper_core import runtime_env_value

logger = logging.getLogger(__name__)
_DISABLED = {"0", "false", "off", "no"}
_readers = ThreadPoolExecutor(max_workers=3, thread_name_prefix="archive-prefetch")
_MAX_TASKS = 2048
_MIN_TASKS = 1024
_READS = (
    (
        "SELECT task_id FROM management.job_task_identities "
        "WHERE task_id=ANY(CAST(%(ids)s AS text[]))"
    ),
    (
        "SELECT job_id,task_id FROM management.job_task_identities "
        "WHERE job_id=%(job)s AND task_id=ANY(CAST(%(ids)s AS text[]))"
    ),
    (
        "SELECT job_id,task_id FROM management.job_tasks "
        "WHERE job_id=%(job)s AND task_id=ANY(CAST(%(ids)s AS text[]))"
    ),
)


def _read(dbapi, arguments, parameters, query, job_id, task_ids):
    """Each reader owns and closes one connection; it returns no task data."""
    try:
        with dbapi.connect(*arguments, **parameters) as connection:
            connection.execute("SET TRANSACTION READ ONLY", prepare=False)
            connection.execute("SET LOCAL statement_timeout='3s'", prepare=False)
            connection.execute("SET LOCAL lock_timeout='100ms'", prepare=False)
            with connection.cursor() as cursor:
                cursor.execute(query, {"job": job_id, "ids": task_ids}, prepare=False)
        return True
    except Exception as error:  # noqa: BLE001 - an optional hint must fall back
        # A cache hint never supplies ownership evidence or fails the write.
        # The canonical archive still validates/inserts under its own locks.
        logger.debug("Archive index prefetch skipped (%s)", type(error).__name__)
        return False


def prefetch_enabled():
    """ULTIMA_ARCHIVE_PREFETCH=0/false/off/no disables warming; default on.

    Read per archive batch, so an env-file change applies without a restart
    unless the process environment already sets the variable.
    """
    value = runtime_env_value("ULTIMA_ARCHIVE_PREFETCH")
    return value is None or value.strip().lower() not in _DISABLED


def _index_is_cold(session):
    # Restrict extra reads to a directory whose primary index alone exceeds
    # PostgreSQL's shared cache. Small/hot installations keep the normal path.
    return bool(session.scalar(text(
        "SELECT pg_relation_size(to_regclass("
        "'management.job_task_identities_pkey')) > "
        "pg_size_bytes(current_setting('shared_buffers'))"
    )))


def prefetch_archive_keys(session, job_id, task_ids, *, directory=True):
    """Overlap cold index reads without changing archive data or transaction.

    The caller has already verified the history guards. Its transaction owns
    the global try-lock through commit, so at most one three-reader batch can
    run across API processes. Busy callers use the ordinary archive path.
    Connections are short-lived even behind session/unknown-mode PgBouncer;
    no application or writer pool is resized or retained.

    ``directory=False`` skips the two global-directory reads when every row
    bypasses the directory, warming only the run's own job_tasks keys.
    """
    if not prefetch_enabled():
        return
    ids = list(islice(task_ids, _MAX_TASKS))
    if len(ids) < _MIN_TASKS:
        return
    bind = session.get_bind()
    engine = getattr(bind, "engine", bind)
    dialect = engine.dialect
    if dialect.name != "postgresql" or dialect.driver != "psycopg":
        return
    if not _index_is_cold(session):
        return
    if not session.scalar(text(
        "SELECT pg_try_advisory_xact_lock("
        "hashtextextended('mps-archive-index-prefetch',0))"
    )):
        return
    arguments, parameters = dialect.create_connect_args(engine.url)
    parameters = {
        **parameters,
        "connect_timeout": 3,
        "prepare_threshold": None,
        "keepalives_idle": 5,
        "keepalives_interval": 1,
        "keepalives_count": 2,
        "tcp_user_timeout": 5000,
    }
    futures = []
    try:
        for query in _READS if directory else _READS[2:]:
            futures.append(_readers.submit(
                _read, dialect.loaded_dbapi, arguments, parameters, query, job_id, ids
            ))
        for future in futures:
            future.result()
    except Exception as error:  # noqa: BLE001 - preserve the canonical write path
        logger.debug("Archive index prefetch unavailable (%s)", type(error).__name__)
    finally:
        # Own submitted readers even if another submit or worker fails. The
        # caller's global transaction lock outlives every read connection.
        wait(futures)
