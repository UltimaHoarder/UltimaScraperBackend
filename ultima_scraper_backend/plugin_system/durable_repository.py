"""Durable plugin delivery, permissions, packages, and fenced leases."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

DURABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS plugin_delivery_queue (
    delivery_id TEXT PRIMARY KEY, plugin_id TEXT NOT NULL,
    target_name TEXT NOT NULL, event_id TEXT NOT NULL,
    payload_json TEXT NOT NULL, manifest_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL, lease_token TEXT, lease_until TEXT,
    last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(plugin_id, target_name, event_id)
);
CREATE INDEX IF NOT EXISTS ix_plugin_delivery_queue_due
    ON plugin_delivery_queue(plugin_id, status, available_at);
CREATE TABLE IF NOT EXISTS plugin_event_cursors (
    plugin_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_scope_grants (
    plugin_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_packages (
    plugin_id TEXT NOT NULL, digest TEXT NOT NULL, plugin_version TEXT NOT NULL,
    payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
    PRIMARY KEY(plugin_id, digest)
);
CREATE TABLE IF NOT EXISTS plugin_configuration (
    plugin_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_api_tokens (
    token_hash TEXT PRIMARY KEY, plugin_id TEXT NOT NULL, expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_runtime_leases (
    resource_key TEXT PRIMARY KEY, owner_id TEXT NOT NULL,
    lease_token TEXT NOT NULL, lease_until TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_package_activations (
    plugin_id TEXT PRIMARY KEY, digest TEXT NOT NULL,
    previous_digest TEXT, updated_at TEXT NOT NULL
);
"""


class PluginBacklogFull(RuntimeError):
    pass


class DurablePluginRepository:
    storage_kind = "sqlite"

    def now(self):
        return datetime.now(UTC)

    def cursor(self, plugin_id, *, initial=0):
        def get(connection):
            connection.execute(
                "INSERT INTO plugin_event_cursors(plugin_id,sequence,updated_at) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                (plugin_id, initial, self.now().isoformat()),
            )
            return int(
                connection.execute(
                    "SELECT sequence FROM plugin_event_cursors WHERE plugin_id=?",
                    (plugin_id,),
                ).fetchone()["sequence"]
            )

        return self._transaction(get)

    def enqueue_deliveries(
        self, plugin_id, deliveries, manifest, *, sequence=None, expected_cursor=None
    ):
        """Commit subscription progress and its fanout in one transaction."""
        now = self.now().isoformat()
        max_pending = manifest["operational_policy"].get("max_pending_deliveries", 1000)

        def enqueue(connection):
            enabled = connection.execute(
                "SELECT enabled FROM plugin_installations WHERE plugin_id=?",
                (plugin_id,),
            ).fetchone()
            if not enabled or not enabled["enabled"]:
                return False
            if expected_cursor is not None:
                current = connection.execute(
                    "SELECT sequence FROM plugin_event_cursors WHERE plugin_id=?",
                    (plugin_id,),
                ).fetchone()
                if current is None or int(current["sequence"]) != expected_cursor:
                    return False
            pending = int(
                connection.execute(
                    "SELECT COUNT(*) AS total FROM plugin_delivery_queue WHERE plugin_id=? AND status IN ('pending','retry','delivering')",
                    (plugin_id,),
                ).fetchone()["total"]
            )
            for target, envelope in deliveries:
                identity = str(envelope["event_id"])
                delivery_id = hashlib.sha256(
                    f"{plugin_id}\0{target}\0{identity}".encode()
                ).hexdigest()
                if connection.execute(
                    "SELECT 1 FROM plugin_delivery_queue WHERE delivery_id=?",
                    (delivery_id,),
                ).fetchone():
                    continue
                if pending >= max_pending:
                    raise PluginBacklogFull(
                        f"Plugin {plugin_id!r} delivery backlog is full"
                    )
                envelope = {**envelope, "delivery_id": delivery_id}
                connection.execute(
                    "INSERT INTO plugin_delivery_queue(delivery_id,plugin_id,target_name,event_id,payload_json,manifest_json,available_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    (
                        delivery_id,
                        plugin_id,
                        target,
                        identity,
                        self._encode(envelope),
                        self._encode(manifest),
                        now,
                        now,
                        now,
                    ),
                )
                pending += 1
            if sequence is not None:
                connection.execute(
                    "INSERT INTO plugin_event_cursors(plugin_id,sequence,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET sequence=excluded.sequence,updated_at=excluded.updated_at",
                    (plugin_id, sequence, now),
                )
            return True

        return self._transaction(enqueue)

    def claim_delivery(self, plugin_id, *, max_concurrency=4, lease_seconds=60):
        now = self.now()
        token = uuid4().hex

        def claim(connection):
            enabled = connection.execute(
                "SELECT enabled FROM plugin_installations WHERE plugin_id=?",
                (plugin_id,),
            ).fetchone()
            if not enabled or not enabled["enabled"]:
                return None
            running = connection.execute(
                "SELECT COUNT(*) AS total FROM plugin_delivery_queue WHERE plugin_id=? AND status='delivering' AND lease_until>?",
                (plugin_id, now.isoformat()),
            ).fetchone()["total"]
            if running >= max_concurrency:
                return None
            row = connection.execute(
                "SELECT * FROM plugin_delivery_queue WHERE plugin_id=? AND ((status IN ('pending','retry') AND available_at<=?) OR (status='delivering' AND lease_until<=?)) ORDER BY available_at,delivery_id LIMIT 1",
                (plugin_id, now.isoformat(), now.isoformat()),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                "UPDATE plugin_delivery_queue SET status='delivering',lease_token=?,lease_until=?,attempts=attempts+1,updated_at=? WHERE delivery_id=?",
                (
                    token,
                    (now + timedelta(seconds=lease_seconds)).isoformat(),
                    now.isoformat(),
                    row["delivery_id"],
                ),
            )
            return {
                **dict(row),
                "lease_token": token,
                "attempts": int(row["attempts"]) + 1,
            }

        return self._transaction(claim)

    def finish_delivery(self, identity, token, *, error=None, retry_at=None):
        now = self.now().isoformat()
        with self._connect() as connection:
            result = connection.execute(
                "UPDATE plugin_delivery_queue SET status=?,last_error=?,available_at=COALESCE(?,available_at),lease_token=NULL,lease_until=NULL,updated_at=? WHERE delivery_id=? AND lease_token=? AND status='delivering' AND lease_until>?",
                (
                    "delivered"
                    if error is None
                    else "retry"
                    if retry_at
                    else "exhausted",
                    error,
                    retry_at,
                    now,
                    identity,
                    token,
                    now,
                ),
            )
            return result.rowcount == 1

    def queue_health(self, plugin_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT status,COUNT(*) AS total,MIN(created_at) AS oldest FROM plugin_delivery_queue WHERE plugin_id=? GROUP BY status",
                (plugin_id,),
            ).fetchall()
        counts = {str(row["status"]): int(row["total"]) for row in rows}
        oldest = min(
            (
                str(row["oldest"])
                for row in rows
                if row["status"] in {"pending", "retry", "delivering"}
            ),
            default=None,
        )
        return {
            "counts": counts,
            "pending": sum(
                counts.get(s, 0) for s in ("pending", "retry", "delivering")
            ),
            "oldest_pending_at": oldest,
        }

    def get_document(self, table, plugin_id):
        if table not in {"plugin_scope_grants", "plugin_configuration"}:
            raise ValueError("Unsupported plugin document")
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT payload_json FROM {table} WHERE plugin_id=?", (plugin_id,)
            ).fetchone()
        return self._decode(row["payload_json"]) if row else None

    def set_document(self, table, plugin_id, payload):
        if table not in {"plugin_scope_grants", "plugin_configuration"}:
            raise ValueError("Unsupported plugin document")
        with self._connect() as connection:
            connection.execute(
                f"INSERT INTO {table}(plugin_id,payload_json,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (plugin_id, self._encode(payload), self.now().isoformat()),
            )

    def save_package(self, plugin_id, manifest, *, digest=None):
        raw = self._encode(manifest)
        digest = digest or hashlib.sha256(raw.encode()).hexdigest()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO plugin_packages(plugin_id,digest,plugin_version,payload_json,created_at) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                (
                    plugin_id,
                    digest,
                    manifest["plugin_version"],
                    raw,
                    self.now().isoformat(),
                ),
            )
        return digest

    def get_package(self, plugin_id, digest):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM plugin_packages WHERE plugin_id=? AND digest=?",
                (plugin_id, digest),
            ).fetchone()
        return self._decode(row["payload_json"]) if row else None

    def active_package(self, plugin_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT digest,previous_digest FROM plugin_package_activations WHERE plugin_id=?",
                (plugin_id,),
            ).fetchone()
        return dict(row) if row else None

    def activate_package(self, plugin_id, digest):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO plugin_package_activations(plugin_id,digest,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET previous_digest=plugin_package_activations.digest,digest=excluded.digest,updated_at=excluded.updated_at WHERE plugin_package_activations.digest!=excluded.digest",
                (plugin_id, digest, self.now().isoformat()),
            )

    def activate_reviewed_package(
        self,
        plugin_id,
        digest,
        capabilities,
        scopes,
        *,
        approve_permissions,
        expected_digest,
        state_quota_bytes,
    ):
        """Fence the reviewed version and atomically replace grants and tokens."""
        from .services import pending_scopes

        now = self.now().isoformat()

        def activate(connection):
            active = connection.execute(
                "SELECT digest FROM plugin_package_activations WHERE plugin_id=?",
                (plugin_id,),
            ).fetchone()
            if (active["digest"] if active else None) != expected_digest:
                raise ValueError(
                    "Active package changed during review; review the current version"
                )
            granted = {
                row["capability"]
                for row in connection.execute(
                    "SELECT capability FROM plugin_capability_grants WHERE plugin_id=?",
                    (plugin_id,),
                ).fetchall()
            }
            approved = connection.execute(
                "SELECT payload_json FROM plugin_scope_grants WHERE plugin_id=?",
                (plugin_id,),
            ).fetchone()
            if not approve_permissions and (
                set(capabilities) - granted
                or pending_scopes(
                    scopes, self._decode(approved["payload_json"]) if approved else {}
                )
            ):
                raise PermissionError("The staged package requires permission approval")
            connection.execute(
                "DELETE FROM plugin_capability_grants WHERE plugin_id=?", (plugin_id,)
            )
            connection.executemany(
                "INSERT INTO plugin_capability_grants(plugin_id,capability,granted_at) VALUES (?,?,?)",
                [(plugin_id, item, now) for item in sorted(capabilities)],
            )
            connection.execute(
                "INSERT INTO plugin_capability_grant_sets(plugin_id,approved_at) VALUES (?,?) ON CONFLICT(plugin_id) DO UPDATE SET approved_at=excluded.approved_at",
                (plugin_id, now),
            )
            connection.execute(
                "INSERT INTO plugin_scope_grants(plugin_id,payload_json,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (plugin_id, self._encode(scopes), now),
            )
            connection.execute(
                "DELETE FROM plugin_api_tokens WHERE plugin_id=?", (plugin_id,)
            )
            connection.execute(
                "INSERT INTO plugin_quotas(plugin_id,quota_bytes,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET quota_bytes=excluded.quota_bytes,updated_at=excluded.updated_at",
                (plugin_id, state_quota_bytes, now),
            )
            connection.execute(
                "INSERT INTO plugin_package_activations(plugin_id,digest,updated_at) VALUES (?,?,?) ON CONFLICT(plugin_id) DO UPDATE SET previous_digest=plugin_package_activations.digest,digest=excluded.digest,updated_at=excluded.updated_at WHERE plugin_package_activations.digest!=excluded.digest",
                (plugin_id, digest, now),
            )

        self._transaction(activate)

    def acquire_lease(self, resource, owner, *, seconds=15, token=None):
        now = self.now()
        new_token = token or uuid4().hex
        with self._connect() as connection:
            result = connection.execute(
                "INSERT INTO plugin_runtime_leases(resource_key,owner_id,lease_token,lease_until) VALUES (?,?,?,?) ON CONFLICT(resource_key) DO UPDATE SET owner_id=excluded.owner_id,lease_token=excluded.lease_token,lease_until=excluded.lease_until WHERE plugin_runtime_leases.lease_until<=? OR (plugin_runtime_leases.owner_id=? AND plugin_runtime_leases.lease_token=? AND plugin_runtime_leases.lease_until>?)",
                (
                    resource,
                    owner,
                    new_token,
                    (now + timedelta(seconds=seconds)).isoformat(),
                    now.isoformat(),
                    owner,
                    new_token,
                    now.isoformat(),
                ),
            )
        return new_token if result.rowcount == 1 else None

    def release_lease(self, resource, token):
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM plugin_runtime_leases WHERE resource_key=? AND lease_token=?",
                (resource, token),
            )

    def issue_api_token(self, plugin_id, *, seconds=300):
        token = uuid4().hex + uuid4().hex
        now = self.now()
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM plugin_api_tokens WHERE expires_at<=?", (now.isoformat(),)
            )
            connection.execute(
                "INSERT INTO plugin_api_tokens(token_hash,plugin_id,expires_at,created_at) VALUES (?,?,?,?)",
                (
                    hashlib.sha256(token.encode()).hexdigest(),
                    plugin_id,
                    (now + timedelta(seconds=seconds)).isoformat(),
                    now.isoformat(),
                ),
            )
        return token

    def authenticate_api_token(self, token):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT t.plugin_id FROM plugin_api_tokens t JOIN plugin_installations i ON i.plugin_id=t.plugin_id WHERE t.token_hash=? AND t.expires_at>? AND i.enabled=1",
                (hashlib.sha256(token.encode()).hexdigest(), self.now().isoformat()),
            ).fetchone()
        return str(row["plugin_id"]) if row else None

    def revoke_api_tokens(self, plugin_id):
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM plugin_api_tokens WHERE plugin_id=?", (plugin_id,)
            )

    def revoke_api_token(self, token):
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM plugin_api_tokens WHERE token_hash=?",
                (hashlib.sha256(token.encode()).hexdigest(),),
            )

    def prune_deliveries(self, *, days=30):
        """Keep pending/retry work; expire only terminal delivery history."""
        cutoff = (self.now() - timedelta(days=days)).isoformat()
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM plugin_delivery_queue WHERE status IN ('delivered','exhausted') AND updated_at<?",
                (cutoff,),
            )
