"""Canonical, versioned application settings persistence and snapshot loading."""

from __future__ import annotations

import json
import socket
from dataclasses import dataclass, field
from typing import Any, cast

from sqlalchemy import and_, func, or_
from sqlalchemy import cast as sa_cast
from sqlalchemy import delete as sa_delete
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import ProgrammingError
from sqlmodel import select
from ultima_scraper_db_archive.global_db import (
    BackendInventoryModel,
    SettingsModel,
    get_global_session,
)

from ultima_scraper_backend.config import (
    JobExecutionSettings,
    UltimaScraperCollectionConfig,
    decrypt_config_secrets,
    encrypt_config_secrets,
)
from ultima_scraper_backend.services.settings_service import SettingsService
from ultima_scraper_core import archive_database_url, runtime_env_value


def load_job_execution_settings(session: Any) -> JobExecutionSettings:
    """Read non-secret live window controls on the caller's SQL connection."""
    backend_id = runtime_env_value("BACKEND_ID") or socket.gethostname()
    inventory = select(BackendInventoryModel.settings_id).where(
        BackendInventoryModel.hostname == backend_id
    )
    payload = session.scalar(
        select(sa_cast(SettingsModel.value, JSONB)["settings"]["job_execution"])
        .where(
            SettingsModel.key == "config",
            or_(
                SettingsModel.id == inventory.scalar_subquery(),
                and_(~inventory.exists(), SettingsModel.backend == backend_id),
            ),
        )
    )
    return JobExecutionSettings.model_validate(payload or {})


def load_config_snapshot[C: UltimaScraperCollectionConfig](config_type: type[C]) -> C:
    """Read values and revision from the same row, without filesystem recovery."""
    backend_id = runtime_env_value("BACKEND_ID") or socket.gethostname()
    if not archive_database_url():
        return config_type()
    with get_global_session() as session:
        try:
            inventory = session.exec(
                select(BackendInventoryModel).where(
                    BackendInventoryModel.hostname == backend_id
                )
            ).one_or_none()
        except ProgrammingError as exc:
            # A new database gets its schema from the setup wizard's database
            # step, so until then there is no configuration to read.
            if getattr(exc.orig, "sqlstate", None) != "42P01":  # undefined_table
                raise
            return config_type()
        query = select(SettingsModel).where(SettingsModel.key == "config")
        query = (
            query.where(SettingsModel.id == inventory.settings_id)
            if inventory
            else query.where(SettingsModel.backend == backend_id)
        )
        row = session.exec(query).one_or_none()
        scope = row.backend if row else backend_id
        if row is None:
            # No row means setup has not completed for this backend.
            return config_type()
        payload = json.loads(row.value)
        if (
            not isinstance(payload, dict)
            or payload.get("configuration_schema_version") != 2
        ):
            raise RuntimeError(
                "Configuration row requires configuration_schema_version == 2"
            )
        decrypt_config_secrets(payload)
        config = config_type(**payload)
        config._settings_backend = scope
        config._settings_id = row.id
        config._settings_version = row.version
        config._settings_updated_at = (
            row.updated_at.isoformat() if row.updated_at else None
        )
        config.is_configured = True
        return config


class ConfigVersionConflict(RuntimeError):
    """Raised when a config snapshot no longer matches the edited revision."""

    def __init__(self, current_version: int) -> None:
        super().__init__(
            f"Config changed since it was loaded (version {current_version})"
        )
        self.current_version = current_version


class ConfigVersionRequired(RuntimeError):
    """Raised when an existing config row is edited without its revision."""

    def __init__(self, current_version: int) -> None:
        super().__init__(
            "expected_version is required for existing configuration updates "
            f"(current version {current_version})"
        )
        self.current_version = current_version


@dataclass(frozen=True)
class PersistedConfigSnapshot:
    """Identity of a successfully persisted canonical config row."""

    settings_id: int
    settings_scope: str
    version: int
    updated_at: str | None
    previous_value: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class CanonicalStorageConfigSnapshot:
    """Canonical version and non-secret storage payload for selected sections."""

    version: int
    payload: dict[str, object]


def get_canonical_storage_config_snapshot(
    settings_scope: str,
    changed_sections: set[str],
) -> CanonicalStorageConfigSnapshot | None:
    """Load the selected storage sections from the canonical config row."""

    supported_sections = changed_sections & {"download_setup", "metadata_setup"}
    with get_global_session() as session:
        row = session.exec(
            select(SettingsModel).where(
                SettingsModel.backend == settings_scope,
                SettingsModel.key == "config",
            )
        ).one_or_none()
        if row is None:
            return None
        version = row.version
        raw_config = json.loads(row.value)

    raw_settings = raw_config.get("settings")
    if not isinstance(raw_settings, dict):
        raise ValueError("canonical config row is missing its settings payload")
    settings_payload = cast(dict[str, object], raw_settings)
    payload: dict[str, object] = {}
    for section in supported_sections:
        if section not in settings_payload:
            raise ValueError(f"canonical config row is missing {section}")
        payload[section] = settings_payload[section]
    return CanonicalStorageConfigSnapshot(version=version, payload=payload)


def persist_config_snapshot(
    config: UltimaScraperCollectionConfig,
    *,
    expected_version: int | None = None,
) -> PersistedConfigSnapshot:
    """Persist the canonical scoped config with optimistic concurrency."""

    target_scope = config.config_write_backend()
    payload = config.persistence_payload()
    encrypt_config_secrets(payload)
    config_json = json.dumps(payload)

    with get_global_session() as session:
        current = session.exec(
            select(SettingsModel).where(
                SettingsModel.backend == target_scope,
                SettingsModel.key == "config",
            )
        ).one_or_none()

        previous_value = current.value if current is not None else None
        if current is None:
            row = SettingsModel(
                backend=target_scope,
                key="config",
                value=config_json,
                description=f"Main application configuration (scope={target_scope})",
            )
            session.add(row)
            session.commit()
            session.refresh(row)
        else:
            if expected_version is None:
                raise ConfigVersionRequired(current.version)
            compare_version = expected_version
            updated_id = cast(
                int | None,
                cast(Any, session)
                .exec(
                    sa_update(SettingsModel)
                    .where(
                        SettingsModel.id == current.id,
                        SettingsModel.version == compare_version,
                    )
                    .values(
                        value=config_json,
                        version=compare_version + 1,
                        updated_at=func.now(),
                    )
                    .returning(SettingsModel.id)
                )
                .scalar_one_or_none(),
            )
            if updated_id is None:
                latest = session.exec(
                    select(SettingsModel).where(SettingsModel.id == current.id)
                ).one_or_none()
                raise ConfigVersionConflict(
                    latest.version if latest is not None else current.version
                )
            session.commit()
            row = session.exec(
                select(SettingsModel).where(SettingsModel.id == current.id)
            ).one_or_none()
            if row is None:
                raise RuntimeError("Persisted config row could not be reloaded")

        session.expunge(row)

    config._settings_backend = row.backend
    config._settings_id = row.id
    config._settings_version = row.version
    config._settings_updated_at = row.updated_at.isoformat() if row.updated_at else None
    config.is_configured = True
    SettingsService().invalidate()
    return PersistedConfigSnapshot(
        settings_id=row.id,
        settings_scope=row.backend,
        version=row.version,
        updated_at=row.updated_at.isoformat() if row.updated_at is not None else None,
        previous_value=previous_value,
    )


def rollback_config_snapshot(snapshot: PersistedConfigSnapshot) -> None:
    """Compensate a canonical write when its matching disk write fails."""

    with get_global_session() as session:
        if snapshot.previous_value is None:
            rolled_back_id = cast(
                int | None,
                cast(Any, session)
                .exec(
                    sa_delete(SettingsModel)
                    .where(
                        SettingsModel.id == snapshot.settings_id,
                        SettingsModel.version == snapshot.version,
                    )
                    .returning(SettingsModel.id)
                )
                .scalar_one_or_none(),
            )
        else:
            rolled_back_id = cast(
                int | None,
                cast(Any, session)
                .exec(
                    sa_update(SettingsModel)
                    .where(
                        SettingsModel.id == snapshot.settings_id,
                        SettingsModel.version == snapshot.version,
                    )
                    .values(
                        value=snapshot.previous_value,
                        version=snapshot.version + 1,
                        updated_at=func.now(),
                    )
                    .returning(SettingsModel.id)
                )
                .scalar_one_or_none(),
            )

        if rolled_back_id is None:
            latest = session.exec(
                select(SettingsModel).where(SettingsModel.id == snapshot.settings_id)
            ).one_or_none()
            raise ConfigVersionConflict(
                latest.version if latest is not None else snapshot.version
            )
        session.commit()

    SettingsService().invalidate()
