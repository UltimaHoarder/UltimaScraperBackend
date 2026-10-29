"""Scoped auxiliary settings in the Archive PostgreSQL database.

Canonical application config uses config_settings_sync for value/revision reads
and CAS writes. This service retains cached CRUD for independent settings and
setup status; it is not a runtime config-file loader.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import overload

from sqlmodel import select
from ultima_scraper_db_archive.crypto import (
    decrypt_secret_value,
    encrypt_secret_value,
)
from ultima_scraper_db_archive.global_db import (
    SettingsModel,
    global_session,
)

_logger = logging.getLogger(__name__)

SECRET_MASK = "********"


class SettingsService:
    """Thread-safe, cached CRUD layer over the ``settings`` table.

    Usage::

        svc = SettingsService()

        # write
        svc.set("backend-api", "custom.output_format", "json")
        svc.set("backend-api", "custom.credential", "example-secret", is_secret=True)

        # read
        db_type = svc.get("backend-api", "custom.output_format")            # "json"
        db_pw   = svc.get("backend-api", "custom.credential")        # "********"
        db_pw   = svc.get("backend-api", "custom.credential",
                          unmask=True)                          # "example-secret"

        # bulk
        all_settings = svc.get_all("backend-api")               # dict[str, Any]

        # invalidate
        svc.invalidate()
    """

    _instance: SettingsService | None = None
    _lock = threading.Lock()

    def __new__(cls) -> SettingsService:
        """Singleton so every caller shares the same cache."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._cache = {}
                cls._instance._cache_loaded = False
            return cls._instance

    # ------------------------------------------------------------------ #
    # Cache helpers
    # ------------------------------------------------------------------ #

    def _ensure_cache(self) -> None:
        if self._cache_loaded:
            return
        with global_session() as session:
            rows = session.exec(select(SettingsModel)).all()
            self._cache = {(r.backend, r.key): r for r in rows}
            # Expunge so they don't expire when session closes
            for r in rows:
                session.expunge(r)
        self._cache_loaded = True

    def invalidate(self) -> None:
        """Clear the in-memory cache so the next read hits the DB."""
        self._cache.clear()
        self._cache_loaded = False

    # ------------------------------------------------------------------ #
    # Read
    # ------------------------------------------------------------------ #

    @overload
    def get(
        self,
        backend: str,
        key: str,
        *,
        default: str = ...,
        unmask: bool = ...,
        as_json: bool = ...,
    ) -> object: ...

    @overload
    def get(
        self,
        backend: str,
        key: str,
    ) -> str | None: ...

    def get(
        self,
        backend: str,
        key: str,
        *,
        default: object = None,
        unmask: bool = False,
        as_json: bool = False,
    ) -> object:
        """Return a single setting value.

        Args:
            backend: Logical backend name (e.g. ``"global"``, ``"backend-api"``).
            key: Setting key.
            default: Fallback when the key doesn't exist.
            unmask: If ``True``, return the real value even for secrets.
            as_json: If ``True``, ``json.loads`` the stored string.
        """
        self._ensure_cache()
        row = self._cache.get((backend, key))
        if row is None:
            return default
        if row.is_secret and not unmask:
            return SECRET_MASK
        value = row.value
        if row.is_secret and unmask:
            value = decrypt_secret_value(value) or ""
        if as_json:
            return json.loads(value)
        return value

    def get_all(
        self,
        backend: str,
        *,
        unmask: bool = False,
    ) -> dict[str, object]:
        """Return all settings for a backend as ``{key: value}``."""
        self._ensure_cache()
        result: dict[str, object] = {}
        for (b, k), row in self._cache.items():
            if b != backend:
                continue
            if row.is_secret and not unmask:
                result[k] = SECRET_MASK
            else:
                result[k] = (
                    decrypt_secret_value(row.value) if row.is_secret else row.value
                )
        return result

    def get_model(self, backend: str, key: str) -> SettingsModel | None:
        """Return the raw ``SettingsModel`` row (never masked)."""
        self._ensure_cache()
        return self._cache.get((backend, key))

    # ------------------------------------------------------------------ #
    # Write
    # ------------------------------------------------------------------ #

    def set(
        self,
        backend: str,
        key: str,
        value: object,
        *,
        is_secret: bool = False,
        description: str | None = None,
    ) -> SettingsModel:
        """Create or update a setting.

        ``value`` is stored as a JSON string.  If you pass a plain ``str``
        it will be JSON-quoted (``'"hello"'``).
        """
        value_str = value if isinstance(value, str) else json.dumps(value)
        if is_secret:
            value_str = encrypt_secret_value(value_str) or ""

        with global_session() as session:
            stmt = select(SettingsModel).where(
                SettingsModel.backend == backend,
                SettingsModel.key == key,
            )
            existing = session.exec(stmt).first()

            if existing is not None:
                existing.value = value_str
                existing.is_secret = is_secret
                if description is not None:
                    existing.description = description
                session.add(existing)
                session.commit()
                session.refresh(existing)
                row = existing
            else:
                row = SettingsModel(
                    backend=backend,
                    key=key,
                    value=value_str,
                    is_secret=is_secret,
                    description=description,
                )
                session.add(row)
                session.commit()
                session.refresh(row)

            # Update cache
            session.expunge(row)
            self._cache[(backend, key)] = row
            return row

    def delete(self, backend: str, key: str) -> bool:
        """Delete a setting. Returns ``True`` if it existed."""
        with global_session() as session:
            stmt = select(SettingsModel).where(
                SettingsModel.backend == backend,
                SettingsModel.key == key,
            )
            existing = session.exec(stmt).first()
            if existing is None:
                return False
            session.delete(existing)
            session.commit()
            self._cache.pop((backend, key), None)
            return True

    # ------------------------------------------------------------------ #
    # Setup helpers
    # ------------------------------------------------------------------ #

    def is_configured(self) -> bool:
        """Check whether first-time setup has been completed.

        The ground truth is this backend's canonical config row. Without a
        reachable database (first run) setup is reported as incomplete.
        """
        from ultima_scraper_db_archive.global_db import is_setup_complete

        return is_setup_complete()

    def get_setup_status(self) -> dict[str, object]:
        """Return a summary of what has been configured so far."""
        from ultima_scraper_core import archive_database_url

        configured = self.is_configured()
        # A connection supplied by the deployment lets the wizard skip
        # re-entering it; this reads configuration only, not the database.
        has_db = bool(archive_database_url())

        if configured:
            self._ensure_cache()
            has_config = any(key == "config" for _, key in self._cache)
        else:
            has_config = False

        return {
            "is_configured": configured,
            "has_config": has_config,
            "has_database": has_db,
            "data_directory": str(self._get_data_directory()),
        }

    @staticmethod
    def _get_data_directory() -> str:
        from ultima_scraper_backend.database.usr_factory import (
            get_default_data_directory,
        )

        return str(get_default_data_directory())
