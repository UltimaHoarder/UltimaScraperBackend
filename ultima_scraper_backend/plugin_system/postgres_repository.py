"""PostgreSQL authority for plugin installations and runtime state.

The legacy repository's parameterized statements are shared with the SQLite
import/test adapter. Connections and transactions are never shared between
processes, and no session-level search path survives a pooled transaction.
"""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable
from contextvars import ContextVar
from datetime import UTC
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from .repository import SCHEMA_VERSION, PluginRepository


class PluginSchemaNotReady(RuntimeError):
    """Operator action is required before the plugin service can start."""


_active_connection: ContextVar[_Connection | None] = ContextVar(
    "plugin_postgres_connection", default=None
)


def _statement(sql: str, parameters: tuple[Any, ...]) -> tuple[Any, dict[str, Any]]:
    parts = sql.split("?")
    if len(parts) != len(parameters) + 1:
        raise ValueError("Plugin SQL parameter count does not match")
    bound = parts[0]
    values = {}
    for index, (value, suffix) in enumerate(zip(parameters, parts[1:], strict=True)):
        name = f"p{index}"
        bound += f":{name}" + suffix
        values[name] = value
    # These are fixed legacy SQL forms, never user-provided statements.
    if "INSERT OR IGNORE INTO" in bound:
        bound = (
            bound.replace("INSERT OR IGNORE INTO", "INSERT INTO").rstrip().rstrip(";")
        )
        bound += " ON CONFLICT DO NOTHING"
    bound = bound.replace("LIMIT -1 OFFSET 5000", "OFFSET 5000")
    return text(bound), values


class _Connection:
    def __init__(self, factory: Callable, *, serializable: bool = False):
        self.factory = factory
        self.context = factory()
        self.session = self.context.__enter__()
        try:
            if serializable:
                self.session.connection(
                    execution_options={"isolation_level": "SERIALIZABLE"}
                )
            self.session.execute(text("SET LOCAL search_path = management, public"))
        except BaseException:
            self.context.__exit__(*sys.exc_info())
            raise
        self._context_token = _active_connection.set(self)

    def execute(self, sql: str, parameters: tuple[Any, ...] = ()):
        statement, values = _statement(sql, parameters)
        result = self.session.execute(statement, values)
        return result.mappings() if result.returns_rows else result

    def executemany(self, sql: str, parameters):
        parameters = list(parameters)
        if not parameters:
            return None
        statement, _ = _statement(sql, parameters[0])
        return self.session.execute(
            statement,
            [{f"p{i}": value for i, value in enumerate(row)} for row in parameters],
        )

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def close(self):
        self._finish_context(None, None, None)

    def _finish_context(self, kind, value, traceback):
        try:
            self.context.__exit__(kind, value, traceback)
        finally:
            _active_connection.reset(self._context_token)

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        try:
            self.session.rollback() if kind else self.session.commit()
        finally:
            self._finish_context(kind, value, traceback)


class PostgresPluginRepository(PluginRepository):
    """Shared plugin authority; SQLite is only a retained migration source."""

    storage_kind = "postgresql"

    def __init__(
        self, session_factory: Callable, *, legacy_runtime_dir: Path | None = None
    ):
        self.session_factory = session_factory
        self.database_path = None
        with self._connect() as connection:
            exists = connection.execute(
                "SELECT to_regclass('management.plugin_repository_meta') IS NOT NULL AS present"
            ).fetchone()
            if not exists["present"]:
                raise PluginSchemaNotReady(
                    "Plugin database schema is missing. Apply plugin migration d2a70c16f493 through the database migration workflow before starting the plugin supervisor."
                )
            row = connection.execute(
                "SELECT value FROM plugin_repository_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None or int(row["value"]) != SCHEMA_VERSION:
                raise PluginSchemaNotReady(
                    "Plugin database schema version is incompatible. Apply plugin migration d2a70c16f493 before startup."
                )
        if legacy_runtime_dir is not None:
            self._import_sqlite_once(legacy_runtime_dir / "plugin-runtime.sqlite3")
            self._import_legacy_json_once(legacy_runtime_dir)

    def _connect(self):
        return _Connection(self.session_factory)

    def now(self):
        connection = _active_connection.get()
        if connection is not None and connection.factory is self.session_factory:
            return connection.session.scalar(text("SELECT clock_timestamp()")).astimezone(UTC)
        with self.session_factory() as session:
            return session.scalar(text("SELECT clock_timestamp()")).astimezone(UTC)

    def _transaction(self, callback: Callable):
        # Legacy quota/import operations need serializable read-modify-write
        # semantics. Queue claims use their own short row-lock transactions.
        for attempt in range(4):
            connection = _Connection(self.session_factory, serializable=True)
            try:
                result = callback(connection)
                connection.commit()
                return result
            except DBAPIError as exc:
                connection.rollback()
                if (
                    getattr(exc.orig, "sqlstate", None) not in {"40001", "40P01"}
                    or attempt == 3
                ):
                    raise
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()

    def _import_sqlite_once(self, path: Path) -> None:
        if not path.is_file():
            return
        from ultima_scraper_db_archive.plugin_models import PLUGIN_TABLES

        def migrate(connection):
            if connection.execute(
                "SELECT value FROM plugin_repository_meta WHERE key = 'legacy_sqlite_imported'"
            ).fetchone():
                return
            # An existing PostgreSQL installation wins. Never overwrite grants
            # or enable a plugin from a stale local copy on another host.
            if connection.execute(
                "SELECT 1 FROM plugin_installations LIMIT 1"
            ).fetchone():
                connection.execute(
                    "INSERT INTO plugin_repository_meta(key,value) VALUES ('legacy_sqlite_imported','skipped-existing') ON CONFLICT DO NOTHING"
                )
                return
            source = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                source.row_factory = sqlite3.Row
                names = {
                    row[0]
                    for row in source.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                for table in PLUGIN_TABLES:
                    name = table.name
                    if name not in names or name == "plugin_runtime_owner":
                        continue
                    columns = [column.name for column in table.columns]
                    if name == "plugin_repository_meta":
                        rows = source.execute(
                            "SELECT * FROM plugin_repository_meta WHERE key != 'schema_version'"
                        )
                    else:
                        rows = source.execute(f"SELECT * FROM {name}")
                    sql = f"INSERT INTO {name} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) ON CONFLICT DO NOTHING"
                    for row in rows:
                        connection.execute(
                            sql, tuple(row[column] for column in columns)
                        )
                connection.execute(
                    "SELECT setval(pg_get_serial_sequence('management.plugin_state_audit','id'), COALESCE((SELECT MAX(id) FROM plugin_state_audit),0)+1, false)"
                )
                connection.execute(
                    "INSERT INTO plugin_repository_meta(key,value) VALUES ('legacy_sqlite_imported','complete') ON CONFLICT DO NOTHING"
                )
            finally:
                source.close()

        self._transaction(migrate)
