"""Transactional persistence for the local plugin control plane.

The plugin catalog is local to one USR installation, so SQLite is the
authoritative durable store while Redis remains the job/event transport.  A
short-lived connection is used per operation: this keeps the repository safe
across Uvicorn worker processes without sharing connection objects after a
fork.

Legacy ``plugins/.runtime/*.json`` files are imported once.  They are never
deleted or renamed by the importer, which makes rollback to the legacy runtime
possible until an operator explicitly removes those backups.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

from .durable_repository import DURABLE_SCHEMA, DurablePluginRepository

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 4
_T = TypeVar("_T")


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class PluginRepository(DurablePluginRepository):
    """Versioned, process-safe repository for plugin runtime state."""

    def __init__(self, database_path: Path, *, legacy_runtime_dir: Path | None = None) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.database_path.parent, 0o700)
        self._initialize_schema()
        if legacy_runtime_dir is not None:
            self._import_legacy_json_once(legacy_runtime_dir)

    # ------------------------------------------------------------------
    # Connection / transaction helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=10.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize_schema(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS plugin_repository_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_installations (
                    plugin_id TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    missing_since TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_capability_grants (
                    plugin_id TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    granted_at TEXT NOT NULL,
                    PRIMARY KEY (plugin_id, capability)
                );

                CREATE TABLE IF NOT EXISTS plugin_capability_grant_sets (
                    plugin_id TEXT PRIMARY KEY,
                    approved_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_secrets (
                    plugin_id TEXT NOT NULL,
                    scope_kind TEXT NOT NULL,
                    scope_name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (plugin_id, scope_kind, scope_name)
                );

                CREATE TABLE IF NOT EXISTS plugin_quotas (
                    plugin_id TEXT PRIMARY KEY,
                    quota_bytes INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_state (
                    plugin_id TEXT NOT NULL,
                    state_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (plugin_id, state_key)
                );

                CREATE TABLE IF NOT EXISTS plugin_state_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plugin_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_plugin_state_audit_plugin_id_id
                    ON plugin_state_audit(plugin_id, id DESC);

                CREATE TABLE IF NOT EXISTS plugin_runtime_correlations (
                    correlation_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_plugin_runtime_correlations_plugin_updated
                    ON plugin_runtime_correlations(plugin_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS plugin_websocket_sessions (
                    plugin_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_callback_registrations (
                    plugin_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_delivery_attempts (
                    delivery_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL,
                    target_name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    next_retry_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_plugin_delivery_attempts_due
                    ON plugin_delivery_attempts(status, next_retry_at);
                CREATE INDEX IF NOT EXISTS ix_plugin_delivery_attempts_plugin_updated
                    ON plugin_delivery_attempts(plugin_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS plugin_runtime_owner (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    owner_id TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL
                );
                """
            )
            connection.executescript(DURABLE_SCHEMA)
            version_row = connection.execute(
                """
                SELECT value FROM plugin_repository_meta
                WHERE key = 'schema_version'
                """
            ).fetchone()
            if version_row is not None:
                try:
                    existing_version = int(version_row["value"])
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "Plugin repository has an invalid schema version"
                    ) from exc
                if existing_version > SCHEMA_VERSION:
                    raise RuntimeError(
                        "Plugin repository schema "
                        f"{existing_version} is newer than supported "
                        f"schema {SCHEMA_VERSION}"
                    )
            connection.execute(
                """
                INSERT INTO plugin_repository_meta(key, value)
                VALUES ('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(SCHEMA_VERSION),),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO plugin_capability_grant_sets(plugin_id, approved_at)
                SELECT plugin_id, MIN(granted_at)
                FROM plugin_capability_grants
                GROUP BY plugin_id
                """
            )
        os.chmod(self.database_path, 0o600)

    def _transaction(self, callback: Callable[[sqlite3.Connection], _T]) -> _T:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = callback(connection)
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _encode(payload: dict[str, Any]) -> str:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode(raw: str) -> dict[str, Any]:
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise ValueError("Stored plugin payload must be a JSON object")
        return decoded

    # ------------------------------------------------------------------
    # Installation lifecycle / grants
    # ------------------------------------------------------------------

    def get_host_document(self, plugin_id: str, name: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM plugin_repository_meta WHERE key=?",
                (f"plugin:{plugin_id}:host:{name}",),
            ).fetchone()
        return self._decode(row["value"]) if row else {}

    def update_host_document(self, plugin_id: str, name: str, update) -> dict[str, Any]:
        def apply(connection):
            key = f"plugin:{plugin_id}:host:{name}"
            connection.execute("INSERT INTO plugin_repository_meta(key,value) VALUES (?,?) ON CONFLICT DO NOTHING", (key, "{}"))
            row = connection.execute("SELECT value FROM plugin_repository_meta WHERE key=?", (key,)).fetchone()
            payload = update(self._decode(row["value"]))
            connection.execute("UPDATE plugin_repository_meta SET value=? WHERE key=?", (self._encode(payload), key))
            return payload
        return self._transaction(apply)

    def save_setup(self, plugin_id, configuration, secrets):
        def save(connection):
            connection.execute(
                "INSERT INTO plugin_configuration(plugin_id,payload_json,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (plugin_id, self._encode(configuration), _utcnow_iso()),
            )
            for payload in secrets:
                connection.execute(
                    "INSERT INTO plugin_secrets(plugin_id,scope_kind,scope_name,payload_json,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(plugin_id,scope_kind,scope_name) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                    (plugin_id, payload["scope_kind"], payload["scope_name"], self._encode(payload), _utcnow_iso()),
                )
        self._transaction(save)

    def list_enabled_plugins(self) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT plugin_id FROM plugin_installations WHERE enabled = 1"
            ).fetchall()
        return {str(row["plugin_id"]) for row in rows}

    def list_installed_plugins(self) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT plugin_id FROM plugin_installations"
            ).fetchall()
        return {str(row["plugin_id"]) for row in rows}

    def set_plugin_enabled(self, plugin_id: str, enabled: bool) -> None:
        now = _utcnow_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_installations(plugin_id, enabled, missing_since, updated_at)
                VALUES (?, ?, NULL, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (plugin_id, int(enabled), now),
            )

    def set_plugin_missing(self, plugin_id: str, missing: bool) -> None:
        now = _utcnow_iso()
        missing_since = now if missing else None
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_installations(plugin_id, enabled, missing_since, updated_at)
                VALUES (?, 0, ?, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET
                    enabled = CASE WHEN excluded.missing_since IS NULL
                                   THEN plugin_installations.enabled ELSE 0 END,
                    missing_since = CASE
                        WHEN excluded.missing_since IS NULL THEN NULL
                        ELSE COALESCE(
                            plugin_installations.missing_since,
                            excluded.missing_since
                        )
                    END,
                    updated_at = excluded.updated_at
                """,
                (plugin_id, missing_since, now),
            )

    def list_missing_plugins(self) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT plugin_id FROM plugin_installations
                WHERE missing_since IS NOT NULL
                """
            ).fetchall()
        return {str(row["plugin_id"]) for row in rows}

    def get_catalog_revision(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT value FROM plugin_repository_meta
                WHERE key = 'catalog_revision'
                """
            ).fetchone()
        return int(row["value"]) if row is not None else 0

    def bump_catalog_revision(self) -> int:
        def _bump(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                """
                SELECT value FROM plugin_repository_meta
                WHERE key = 'catalog_revision'
                """
            ).fetchone()
            revision = (int(row["value"]) if row is not None else 0) + 1
            connection.execute(
                """
                INSERT INTO plugin_repository_meta(key, value)
                VALUES ('catalog_revision', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(revision),),
            )
            return revision

        return self._transaction(_bump)

    def has_grant_record(self, plugin_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM plugin_capability_grant_sets WHERE plugin_id = ? LIMIT 1",
                (plugin_id,),
            ).fetchone()
        return row is not None

    def get_grants(self, plugin_id: str) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT capability FROM plugin_capability_grants WHERE plugin_id = ?",
                (plugin_id,),
            ).fetchall()
        return {str(row["capability"]) for row in rows}

    def replace_grants(self, plugin_id: str, capabilities: set[str]) -> set[str]:
        now = _utcnow_iso()

        def _replace(connection: sqlite3.Connection) -> set[str]:
            connection.execute(
                "DELETE FROM plugin_capability_grants WHERE plugin_id = ?",
                (plugin_id,),
            )
            connection.executemany(
                """
                INSERT INTO plugin_capability_grants(plugin_id, capability, granted_at)
                VALUES (?, ?, ?)
                """,
                [(plugin_id, capability, now) for capability in sorted(capabilities)],
            )
            connection.execute(
                """
                INSERT INTO plugin_capability_grant_sets(plugin_id, approved_at)
                VALUES (?, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET approved_at = excluded.approved_at
                """,
                (plugin_id, now),
            )
            connection.execute(
                """
                INSERT INTO plugin_installations(plugin_id, enabled, missing_since, updated_at)
                VALUES (?, 0, NULL, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (plugin_id, now),
            )
            return set(capabilities)

        return self._transaction(_replace)

    # ------------------------------------------------------------------
    # Secrets
    # ------------------------------------------------------------------

    def list_secret_payloads(self, plugin_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT payload_json FROM plugin_secrets"
        params: tuple[Any, ...] = ()
        if plugin_id is not None:
            query += " WHERE plugin_id = ?"
            params = (plugin_id,)
        query += " ORDER BY plugin_id, scope_kind, scope_name"
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._decode(str(row["payload_json"])) for row in rows]

    def upsert_secret_payload(self, payload: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_secrets(
                    plugin_id, scope_kind, scope_name, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(plugin_id, scope_kind, scope_name) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    str(payload["plugin_id"]),
                    str(payload["scope_kind"]),
                    str(payload["scope_name"]),
                    self._encode(payload),
                    _utcnow_iso(),
                ),
            )

    def insert_secret_payload_if_absent(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Atomically initialize one secret and return the authoritative value."""

        def _insert(connection: sqlite3.Connection) -> dict[str, Any]:
            plugin_id = str(payload["plugin_id"])
            scope_kind = str(payload["scope_kind"])
            scope_name = str(payload["scope_name"])
            connection.execute(
                """
                INSERT OR IGNORE INTO plugin_secrets(
                    plugin_id, scope_kind, scope_name, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    plugin_id,
                    scope_kind,
                    scope_name,
                    self._encode(payload),
                    _utcnow_iso(),
                ),
            )
            row = connection.execute(
                """
                SELECT payload_json FROM plugin_secrets
                WHERE plugin_id = ? AND scope_kind = ? AND scope_name = ?
                """,
                (plugin_id, scope_kind, scope_name),
            ).fetchone()
            if row is None:
                raise RuntimeError("Plugin secret initialization did not persist")
            return self._decode(str(row["payload_json"]))

        return self._transaction(_insert)

    def delete_plugin_secrets(self, plugin_id: str) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM plugin_secrets WHERE plugin_id = ?",
                (plugin_id,),
            )
        return cursor.rowcount

    # ------------------------------------------------------------------
    # State and audit
    # ------------------------------------------------------------------

    def set_quota(self, plugin_id: str, quota_bytes: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_quotas(plugin_id, quota_bytes, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET
                    quota_bytes = excluded.quota_bytes,
                    updated_at = excluded.updated_at
                """,
                (plugin_id, int(quota_bytes), _utcnow_iso()),
            )

    def get_quota(self, plugin_id: str, default: int) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT quota_bytes FROM plugin_quotas WHERE plugin_id = ?",
                (plugin_id,),
            ).fetchone()
        return int(row["quota_bytes"]) if row is not None else default

    def get_state_payload(self, plugin_id: str, key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json FROM plugin_state
                WHERE plugin_id = ? AND state_key = ?
                """,
                (plugin_id, key),
            ).fetchone()
        return self._decode(str(row["payload_json"])) if row is not None else None

    def list_state_payloads(self, plugin_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json FROM plugin_state
                WHERE plugin_id = ? ORDER BY state_key
                """,
                (plugin_id,),
            ).fetchall()
        return [self._decode(str(row["payload_json"])) for row in rows]

    def set_state_payload(
        self,
        plugin_id: str,
        key: str,
        payload: dict[str, Any],
        *,
        size_bytes: int,
        quota_bytes: int,
        audit_factory: Callable[
            [dict[str, Any] | None],
            dict[str, Any],
        ],
    ) -> None:
        now = _utcnow_iso()

        def _set(connection: sqlite3.Connection) -> None:
            quota_row = connection.execute(
                "SELECT quota_bytes FROM plugin_quotas WHERE plugin_id = ?",
                (plugin_id,),
            ).fetchone()
            effective_quota = (
                int(quota_row["quota_bytes"])
                if quota_row is not None
                else quota_bytes
            )
            current = connection.execute(
                """
                SELECT COALESCE(SUM(size_bytes), 0) AS total
                FROM plugin_state WHERE plugin_id = ?
                """,
                (plugin_id,),
            ).fetchone()
            existing = connection.execute(
                """
                SELECT size_bytes, payload_json FROM plugin_state
                WHERE plugin_id = ? AND state_key = ?
                """,
                (plugin_id, key),
            ).fetchone()
            total = int(current["total"]) if current is not None else 0
            if existing is not None:
                total -= int(existing["size_bytes"])
            if total + size_bytes > effective_quota:
                raise ValueError(
                    f"State quota exceeded for plugin {plugin_id!r} "
                    f"({total + size_bytes} > {effective_quota})"
                )
            connection.execute(
                """
                INSERT INTO plugin_state(
                    plugin_id, state_key, payload_json, size_bytes, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(plugin_id, state_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    size_bytes = excluded.size_bytes,
                    updated_at = excluded.updated_at
                """,
                (plugin_id, key, self._encode(payload), size_bytes, now),
            )
            connection.execute(
                """
                INSERT INTO plugin_state_audit(
                    plugin_id, payload_json, created_at
                ) VALUES (?, ?, ?)
                """,
                (
                    plugin_id,
                    self._encode(
                        audit_factory(
                            self._decode(str(existing["payload_json"]))
                            if existing is not None
                            else None
                        )
                    ),
                    now,
                ),
            )

        self._transaction(_set)

    def delete_state_payload(
        self,
        plugin_id: str,
        key: str,
        *,
        audit_factory: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> bool:
        now = _utcnow_iso()

        def _delete(connection: sqlite3.Connection) -> bool:
            row = connection.execute(
                """
                SELECT payload_json FROM plugin_state
                WHERE plugin_id = ? AND state_key = ?
                """,
                (plugin_id, key),
            ).fetchone()
            if row is None:
                return False
            old_payload = self._decode(str(row["payload_json"]))
            connection.execute(
                "DELETE FROM plugin_state WHERE plugin_id = ? AND state_key = ?",
                (plugin_id, key),
            )
            connection.execute(
                """
                INSERT INTO plugin_state_audit(plugin_id, payload_json, created_at)
                VALUES (?, ?, ?)
                """,
                (plugin_id, self._encode(audit_factory(old_payload)), now),
            )
            return True

        return self._transaction(_delete)

    def list_state_audit(self, plugin_id: str, limit: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json FROM plugin_state_audit
                WHERE plugin_id = ? ORDER BY id DESC LIMIT ?
                """,
                (plugin_id, int(limit)),
            ).fetchall()
        return [self._decode(str(row["payload_json"])) for row in reversed(rows)]

    def total_state_size(self, plugin_id: str) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(size_bytes), 0) AS total
                FROM plugin_state WHERE plugin_id = ?
                """,
                (plugin_id,),
            ).fetchone()
        return int(row["total"]) if row is not None else 0

    def clear_plugin_state(self, plugin_id: str) -> None:
        def _clear(connection: sqlite3.Connection) -> None:
            connection.execute("DELETE FROM plugin_state WHERE plugin_id = ?", (plugin_id,))
            connection.execute(
                "DELETE FROM plugin_state_audit WHERE plugin_id = ?", (plugin_id,)
            )
            connection.execute("DELETE FROM plugin_quotas WHERE plugin_id = ?", (plugin_id,))

        self._transaction(_clear)

    # ------------------------------------------------------------------
    # Runtime records
    # ------------------------------------------------------------------

    def upsert_correlation_payload(self, payload: dict[str, Any]) -> None:
        now = _utcnow_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_runtime_correlations(
                    correlation_id, plugin_id, payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(correlation_id) DO UPDATE SET
                    plugin_id = excluded.plugin_id,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    str(payload["correlation_id"]),
                    str(payload["plugin_id"]),
                    self._encode(payload),
                    str(payload.get("created_at") or now),
                    str(payload.get("updated_at") or now),
                ),
            )

    def get_correlation_payload(self, correlation_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json FROM plugin_runtime_correlations
                WHERE correlation_id = ?
                """,
                (correlation_id,),
            ).fetchone()
        return self._decode(str(row["payload_json"])) if row is not None else None

    def list_correlation_payloads(self, plugin_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT payload_json FROM plugin_runtime_correlations"
        params: tuple[Any, ...] = ()
        if plugin_id is not None:
            query += " WHERE plugin_id = ?"
            params = (plugin_id,)
        query += " ORDER BY created_at DESC"
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._decode(str(row["payload_json"])) for row in rows]

    def delete_correlation(self, correlation_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM plugin_runtime_correlations WHERE correlation_id = ?",
                (correlation_id,),
            )
        return cursor.rowcount > 0

    def upsert_session_payload(self, payload: dict[str, Any]) -> None:
        self._upsert_plugin_payload("plugin_websocket_sessions", payload)

    def get_session_payload(self, plugin_id: str) -> dict[str, Any] | None:
        return self._get_plugin_payload("plugin_websocket_sessions", plugin_id)

    def upsert_registration_payload(self, payload: dict[str, Any]) -> None:
        self._upsert_plugin_payload("plugin_callback_registrations", payload)

    def get_registration_payload(self, plugin_id: str) -> dict[str, Any] | None:
        return self._get_plugin_payload("plugin_callback_registrations", plugin_id)

    def _upsert_plugin_payload(self, table: str, payload: dict[str, Any]) -> None:
        if table not in {"plugin_websocket_sessions", "plugin_callback_registrations"}:
            raise ValueError("Unsupported plugin payload table")
        now = _utcnow_iso()
        with self._connect() as connection:
            connection.execute(
                f"""
                INSERT INTO {table}(plugin_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (str(payload["plugin_id"]), self._encode(payload), now),
            )

    def _get_plugin_payload(self, table: str, plugin_id: str) -> dict[str, Any] | None:
        if table not in {"plugin_websocket_sessions", "plugin_callback_registrations"}:
            raise ValueError("Unsupported plugin payload table")
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT payload_json FROM {table} WHERE plugin_id = ?",
                (plugin_id,),
            ).fetchone()
        return self._decode(str(row["payload_json"])) if row is not None else None

    def clear_plugin_runtime(self, plugin_id: str) -> None:
        def _clear(connection: sqlite3.Connection) -> None:
            connection.execute(
                "DELETE FROM plugin_runtime_correlations WHERE plugin_id = ?",
                (plugin_id,),
            )
            connection.execute(
                "DELETE FROM plugin_websocket_sessions WHERE plugin_id = ?",
                (plugin_id,),
            )
            connection.execute(
                "DELETE FROM plugin_callback_registrations WHERE plugin_id = ?",
                (plugin_id,),
            )

        self._transaction(_clear)

    def purge_plugin_data(self, plugin_id: str) -> dict[str, int]:
        """Atomically remove operator-approved data for a disabled plugin.

        The installation row is retained as a disabled tombstone so an
        accidental manifest reappearance cannot silently re-enable the plugin.
        """

        tables = (
            "plugin_delivery_queue",
            "plugin_event_cursors",
            "plugin_scope_grants",
            "plugin_packages",
            "plugin_package_activations",
            "plugin_configuration",
            "plugin_api_tokens",
            "plugin_capability_grants",
            "plugin_capability_grant_sets",
            "plugin_secrets",
            "plugin_quotas",
            "plugin_state",
            "plugin_state_audit",
            "plugin_runtime_correlations",
            "plugin_websocket_sessions",
            "plugin_callback_registrations",
            "plugin_delivery_attempts",
        )

        def _purge(connection: sqlite3.Connection) -> dict[str, int]:
            counts: dict[str, int] = {}
            connection.execute("DELETE FROM plugin_repository_meta WHERE key LIKE ?", (f"plugin:{plugin_id}:host:%",))
            for table in tables:
                cursor = connection.execute(
                    f"DELETE FROM {table} WHERE plugin_id = ?",  # noqa: S608
                    (plugin_id,),
                )
                counts[table] = cursor.rowcount
            connection.execute(
                """
                INSERT INTO plugin_installations(
                    plugin_id, enabled, missing_since, updated_at
                ) VALUES (?, 0, NULL, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET
                    enabled = 0,
                    updated_at = excluded.updated_at
                """,
                (plugin_id, _utcnow_iso()),
            )
            return counts

        return self._transaction(_purge)

    # ------------------------------------------------------------------
    # Delivery observability
    # ------------------------------------------------------------------

    def upsert_delivery_attempt(self, payload: dict[str, Any]) -> None:
        now = _utcnow_iso()
        attempted_at = str(payload.get("attempted_at") or now)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_delivery_attempts(
                    delivery_id, plugin_id, target_name, payload_json, status,
                    next_retry_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(delivery_id) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    status = excluded.status,
                    next_retry_at = excluded.next_retry_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(payload["delivery_id"]),
                    str(payload["plugin_id"]),
                    str(payload["target_name"]),
                    self._encode(payload),
                    str(payload.get("status", "pending")),
                    payload.get("next_retry_at"),
                    attempted_at,
                    now,
                ),
            )
            connection.execute(
                """
                DELETE FROM plugin_delivery_attempts
                WHERE delivery_id IN (
                    SELECT delivery_id FROM plugin_delivery_attempts
                    ORDER BY updated_at DESC LIMIT -1 OFFSET 5000
                )
                """
            )

    def list_delivery_attempts(
        self,
        plugin_id: str | None = None,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        query = "SELECT payload_json FROM plugin_delivery_attempts"
        params: list[Any] = []
        if plugin_id is not None:
            query += " WHERE plugin_id = ?"
            params.append(plugin_id)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._decode(str(row["payload_json"])) for row in rows]

    # ------------------------------------------------------------------
    # Cross-worker runtime ownership observability
    # ------------------------------------------------------------------

    def claim_runtime_owner(self, owner_id: str, pid: int) -> None:
        now = _utcnow_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_runtime_owner(
                    singleton, owner_id, pid, started_at, heartbeat_at
                ) VALUES (1, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    pid = excluded.pid,
                    started_at = excluded.started_at,
                    heartbeat_at = excluded.heartbeat_at
                """,
                (owner_id, int(pid), now, now),
            )

    def heartbeat_runtime_owner(self, owner_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE plugin_runtime_owner
                SET heartbeat_at = ?
                WHERE singleton = 1 AND owner_id = ?
                """,
                (_utcnow_iso(), owner_id),
            )
        return cursor.rowcount == 1

    def release_runtime_owner(self, owner_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                DELETE FROM plugin_runtime_owner
                WHERE singleton = 1 AND owner_id = ?
                """,
                (owner_id,),
            )

    def get_runtime_owner(self, *, stale_after_seconds: float = 5.0) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT owner_id, pid, started_at, heartbeat_at
                FROM plugin_runtime_owner
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        heartbeat_at = datetime.fromisoformat(str(row["heartbeat_at"]))
        age_seconds = max(
            0.0,
            (datetime.now(UTC) - heartbeat_at).total_seconds(),
        )
        return {
            "owner_id": str(row["owner_id"]),
            "pid": int(row["pid"]),
            "started_at": str(row["started_at"]),
            "heartbeat_at": str(row["heartbeat_at"]),
            "heartbeat_age_seconds": age_seconds,
            "active": age_seconds <= stale_after_seconds,
        }

    # ------------------------------------------------------------------
    # Legacy JSON import
    # ------------------------------------------------------------------

    def _import_legacy_json_once(self, runtime_dir: Path) -> None:
        with self._connect() as connection:
            already_imported = connection.execute(
                "SELECT value FROM plugin_repository_meta WHERE key = 'legacy_json_imported'"
            ).fetchone()
        if already_imported is not None:
            return

        def _read(name: str) -> Any:
            path = runtime_dir / name
            if not path.is_file():
                return None
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"Could not import legacy plugin runtime file {path}; "
                    "the source was retained and must be repaired before retrying"
                ) from exc

        enabled = _read("enabled_plugins.json")
        grants = _read("capability_grants.json")
        secrets = _read("secrets.json")
        state = _read("state.json")
        runtime = _read("runtime.json")
        now = _utcnow_iso()

        if enabled is not None and not isinstance(enabled, list):
            raise ValueError("Legacy enabled_plugins.json must contain a JSON list")
        for name, payload in (
            ("capability_grants.json", grants),
            ("secrets.json", secrets),
            ("state.json", state),
            ("runtime.json", runtime),
        ):
            if payload is not None and not isinstance(payload, dict):
                raise ValueError(f"Legacy {name} must contain a JSON object")

        def _import(connection: sqlite3.Connection) -> bool:
            # Multiple Uvicorn workers may initialize concurrently. Re-check
            # under the write transaction so the one-time import is truly
            # process-safe rather than only instance-safe.
            imported = connection.execute(
                """
                SELECT value FROM plugin_repository_meta
                WHERE key = 'legacy_json_imported'
                """
            ).fetchone()
            if imported is not None:
                return False

            if isinstance(enabled, list):
                connection.executemany(
                    """
                    INSERT INTO plugin_installations(plugin_id, enabled, missing_since, updated_at)
                    VALUES (?, 1, NULL, ?)
                    ON CONFLICT(plugin_id) DO UPDATE SET
                        enabled = 1,
                        updated_at = excluded.updated_at
                    """,
                    [(str(plugin_id), now) for plugin_id in enabled],
                )

            raw_grants = grants.get("grants", {}) if isinstance(grants, dict) else {}
            if not isinstance(raw_grants, dict):
                raise ValueError("Legacy capability grants must be an object")
            for plugin_id, capabilities in raw_grants.items():
                if not isinstance(capabilities, list) or not all(
                    isinstance(capability, str) for capability in capabilities
                ):
                    raise ValueError(
                        f"Legacy capability grants for {plugin_id!r} "
                        "must be a list of strings"
                    )
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO plugin_capability_grants(
                        plugin_id, capability, granted_at
                    ) VALUES (?, ?, ?)
                    """,
                    [
                        (str(plugin_id), str(capability), now)
                        for capability in capabilities
                    ],
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO plugin_capability_grant_sets(
                        plugin_id, approved_at
                    ) VALUES (?, ?)
                    """,
                    (str(plugin_id), now),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO plugin_installations(
                        plugin_id, enabled, missing_since, updated_at
                    ) VALUES (?, 0, NULL, ?)
                    """,
                    (str(plugin_id), now),
                )

            raw_secrets = secrets.get("secrets", []) if isinstance(secrets, dict) else []
            if not isinstance(raw_secrets, list):
                raise ValueError("Legacy secrets must be a list")
            for payload in raw_secrets:
                if not isinstance(payload, dict):
                    raise ValueError("Legacy secret entries must be objects")
                try:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO plugin_secrets(
                            plugin_id, scope_kind, scope_name, payload_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            str(payload["plugin_id"]),
                            str(payload["scope_kind"]),
                            str(payload["scope_name"]),
                            self._encode(payload),
                            now,
                        ),
                    )
                except KeyError as exc:
                    raise ValueError(
                        "Legacy secret entry is missing an identity field"
                    ) from exc

            if isinstance(state, dict):
                quotas = state.get("quotas", {})
                if not isinstance(quotas, dict):
                    raise ValueError("Legacy plugin quotas must be an object")
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO plugin_quotas(plugin_id, quota_bytes, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    [(str(pid), int(quota), now) for pid, quota in quotas.items()],
                )
                stores = state.get("stores", {})
                if not isinstance(stores, dict):
                    raise ValueError("Legacy plugin state stores must be an object")
                for plugin_id, entries in stores.items():
                    if not isinstance(entries, dict):
                        raise ValueError(
                            f"Legacy state store for {plugin_id!r} must be an object"
                        )
                    for key, payload in entries.items():
                        if not isinstance(payload, dict):
                            raise ValueError(
                                f"Legacy state entry {plugin_id!r}/{key!r} "
                                "must be an object"
                            )
                        value_size = len(
                            json.dumps(
                                payload.get("value"),
                                separators=(",", ":"),
                            ).encode()
                        )
                        connection.execute(
                            """
                            INSERT OR IGNORE INTO plugin_state(
                                plugin_id, state_key, payload_json, size_bytes, updated_at
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                str(plugin_id),
                                str(key),
                                self._encode(payload),
                                value_size,
                                now,
                            ),
                        )
                audits = state.get("audit_log", {})
                if not isinstance(audits, dict):
                    raise ValueError("Legacy plugin audit log must be an object")
                for plugin_id, entries in audits.items():
                    if not isinstance(entries, list) or not all(
                        isinstance(payload, dict) for payload in entries
                    ):
                        raise ValueError(
                            f"Legacy audit log for {plugin_id!r} "
                            "must be a list of objects"
                        )
                    connection.executemany(
                        """
                        INSERT INTO plugin_state_audit(plugin_id, payload_json, created_at)
                        VALUES (?, ?, ?)
                        """,
                        [
                            (str(plugin_id), self._encode(payload), now)
                            for payload in entries
                        ],
                    )

            if isinstance(runtime, dict):
                correlations = runtime.get("correlations", [])
                if not isinstance(correlations, list):
                    raise ValueError("Legacy runtime correlations must be a list")
                for payload in correlations:
                    if not isinstance(payload, dict) or "correlation_id" not in payload:
                        raise ValueError(
                            "Legacy runtime correlation entries require correlation_id"
                        )
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO plugin_runtime_correlations(
                            correlation_id, plugin_id, payload_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            str(payload["correlation_id"]),
                            str(payload.get("plugin_id", "")),
                            self._encode(payload),
                            str(payload.get("created_at") or now),
                            str(payload.get("updated_at") or now),
                        ),
                    )
                for table, key in (
                    ("plugin_websocket_sessions", "sessions"),
                    ("plugin_callback_registrations", "registrations"),
                ):
                    entries = runtime.get(key, [])
                    if not isinstance(entries, list):
                        raise ValueError(f"Legacy runtime {key} must be a list")
                    for payload in entries:
                        if not isinstance(payload, dict) or "plugin_id" not in payload:
                            raise ValueError(
                                f"Legacy runtime {key} entries require plugin_id"
                            )
                        connection.execute(
                            f"""
                            INSERT OR IGNORE INTO {table}(plugin_id, payload_json, updated_at)
                            VALUES (?, ?, ?)
                            """,
                            (str(payload["plugin_id"]), self._encode(payload), now),
                        )

            connection.execute(
                """
                INSERT INTO plugin_repository_meta(key, value)
                VALUES ('legacy_json_imported', ?)
                """,
                (now,),
            )
            return True

        if self._transaction(_import):
            logger.info("Imported legacy plugin runtime JSON into %s", self.database_path)
