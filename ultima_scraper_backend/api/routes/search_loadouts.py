"""Search loadout persistence endpoints.

Loadouts are stored in their own table: ``management.search_loadouts``.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import update as sa_update
from sqlmodel import select

from ultima_scraper_db_archive.global_db import (
    SearchLoadoutModel,
    SettingsModel,
    get_global_session,
)

router = APIRouter(prefix="/search-loadouts", tags=["search-loadouts"])

_LEGACY_SETTINGS_BACKEND = "search_loadouts"
_LEGACY_WEBUI_BACKEND = "webui"
_LEGACY_WEBUI_KEY = "search_loadouts_v1"


class LoadoutType(str, Enum):
    users = "users"
    subscriptions = "subscriptions"
    content = "content"


class SearchLoadoutItem(BaseModel):
    name: str
    type: LoadoutType
    identifier: str = ""
    description: str = ""
    site: str = ""
    filters: dict[str, Any] = Field(default_factory=dict)
    is_default: bool = False
    updated_at: str | None = None


class SearchLoadoutsResponse(BaseModel):
    success: bool = True
    type: LoadoutType
    default_name: str | None = None
    loadouts: list[SearchLoadoutItem] = Field(default_factory=lambda: [])
    error: str | None = None


class SearchLoadoutMutationResponse(BaseModel):
    success: bool = True
    type: LoadoutType
    name: str
    default_name: str | None = None
    error: str | None = None


class SearchLoadoutUpsertRequest(BaseModel):
    identifier: str = ""
    description: str = ""
    site: str = ""
    filters: dict[str, Any] = Field(default_factory=dict)
    set_as_default: bool = False


def _validate_name(name: str) -> str:
    clean = name.strip()
    if not clean:
        raise HTTPException(status_code=400, detail="Loadout name must not be empty")
    if len(clean) > 90:
        raise HTTPException(
            status_code=400, detail="Loadout name too long (max 90 chars)"
        )
    return clean


def _parse_filters(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw or "{}")
    except json.JSONDecodeError:
        parsed = {}
    if isinstance(parsed, dict):
        return cast(dict[str, Any], parsed)
    return {}


def _default_name(session: Any, loadout_type: LoadoutType) -> str | None:
    row = session.exec(
        select(SearchLoadoutModel)
        .where(
            SearchLoadoutModel.loadout_type == loadout_type.value,
            SearchLoadoutModel.is_default.is_(True),
        )
        .order_by(SearchLoadoutModel.updated_at.desc())
    ).first()
    return row.name if row else None


def _set_default(session: Any, loadout_type: LoadoutType, name: str) -> None:
    session.execute(
        sa_update(SearchLoadoutModel)
        .where(SearchLoadoutModel.loadout_type == loadout_type.value)
        .values(is_default=(SearchLoadoutModel.name == name))
    )


def _clear_default(session: Any, loadout_type: LoadoutType) -> None:
    session.execute(
        sa_update(SearchLoadoutModel)
        .where(
            SearchLoadoutModel.loadout_type == loadout_type.value,
            SearchLoadoutModel.is_default.is_(True),
        )
        .values(is_default=False)
    )


def _upsert_row(
    session: Any,
    loadout_type: LoadoutType,
    name: str,
    payload: SearchLoadoutUpsertRequest,
) -> SearchLoadoutModel:
    row = session.exec(
        select(SearchLoadoutModel).where(
            SearchLoadoutModel.loadout_type == loadout_type.value,
            SearchLoadoutModel.name == name,
        )
    ).first()
    if row is None:
        row = SearchLoadoutModel(
            loadout_type=loadout_type.value,
            name=name,
            identifier=payload.identifier,
            description=payload.description,
            site=payload.site,
            filters_json=json.dumps(
                payload.filters,
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            is_default=False,
        )
        session.add(row)
    else:
        row.identifier = payload.identifier
        row.description = payload.description
        row.site = payload.site
        row.filters_json = json.dumps(
            payload.filters,
            ensure_ascii=True,
            separators=(",", ":"),
        )
        session.add(row)
    session.flush()
    return row


def _migrate_legacy_if_needed(session: Any) -> None:
    """One-time migration from older settings-backed storage."""
    existing = session.exec(select(SearchLoadoutModel.id).limit(1)).first()
    if existing is not None:
        return

    # Migrate row-per-loadout in settings backend=search_loadouts
    legacy_rows = session.exec(
        select(SettingsModel).where(SettingsModel.backend == _LEGACY_SETTINGS_BACKEND)
    ).all()
    for legacy in legacy_rows:
        key = legacy.key or ""
        if key.startswith("loadout:"):
            parts = key.split(":", 2)
            if len(parts) != 3:
                continue
            type_part, name = parts[1], parts[2]
            if type_part not in {t.value for t in LoadoutType}:
                continue
            payload = _parse_filters(legacy.value or "{}")
            row = SearchLoadoutModel(
                loadout_type=type_part,
                name=name,
                identifier=str(payload.get("identifier") or ""),
                description=str(payload.get("description") or ""),
                site=str(payload.get("site") or ""),
                filters_json=json.dumps(
                    (
                        payload.get("filters")
                        if isinstance(payload.get("filters"), dict)
                        else {}
                    ),
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                is_default=False,
            )
            session.add(row)

    session.flush()

    # Apply legacy default:* markers
    for legacy in legacy_rows:
        key = legacy.key or ""
        if not key.startswith("default:"):
            continue
        parts = key.split(":", 1)
        if len(parts) != 2:
            continue
        type_part = parts[1]
        default_name = (legacy.value or "").strip()
        if type_part in {t.value for t in LoadoutType} and default_name:
            session.execute(
                sa_update(SearchLoadoutModel)
                .where(SearchLoadoutModel.loadout_type == type_part)
                .values(is_default=(SearchLoadoutModel.name == default_name))
            )

    # Migrate very old webui blob format (if still present)
    blob_row = session.exec(
        select(SettingsModel).where(
            SettingsModel.backend == _LEGACY_WEBUI_BACKEND,
            SettingsModel.key == _LEGACY_WEBUI_KEY,
        )
    ).first()
    if blob_row and blob_row.value:
        try:
            doc = json.loads(blob_row.value)
        except json.JSONDecodeError:
            doc = {}
        if isinstance(doc, dict):
            doc_data = cast(dict[str, Any], doc)
            types_blob = doc_data.get("types")
            if isinstance(types_blob, dict):
                types_blob_data = cast(dict[str, Any], types_blob)
                for loadout_type in LoadoutType:
                    bucket = types_blob_data.get(loadout_type.value)
                    if not isinstance(bucket, dict):
                        continue
                    bucket_data = cast(dict[str, Any], bucket)
                    items = bucket_data.get("items")
                    if isinstance(items, dict):
                        items_data = cast(dict[str, Any], items)
                        for name, payload in items_data.items():
                            if not isinstance(payload, dict):
                                continue
                            payload_data = cast(dict[str, Any], payload)
                            exists_row = session.exec(
                                select(SearchLoadoutModel).where(
                                    SearchLoadoutModel.loadout_type
                                    == loadout_type.value,
                                    SearchLoadoutModel.name == name.strip(),
                                )
                            ).first()
                            if exists_row is not None:
                                continue
                            session.add(
                                SearchLoadoutModel(
                                    loadout_type=loadout_type.value,
                                    name=name.strip(),
                                    identifier=str(
                                        payload_data.get("identifier") or ""
                                    ),
                                    description=str(
                                        payload_data.get("description") or ""
                                    ),
                                    site=str(payload_data.get("site") or ""),
                                    filters_json=json.dumps(
                                        (
                                            payload_data.get("filters")
                                            if isinstance(
                                                payload_data.get("filters"), dict
                                            )
                                            else {}
                                        ),
                                        ensure_ascii=True,
                                        separators=(",", ":"),
                                    ),
                                    is_default=False,
                                )
                            )
                    default_name = bucket_data.get("default_name")
                    if isinstance(default_name, str) and default_name.strip():
                        _set_default(
                            session,
                            loadout_type,
                            default_name.strip(),
                        )

    session.commit()


@router.get("/", response_model=SearchLoadoutsResponse)
async def list_search_loadouts(
    loadout_type: LoadoutType = Query(LoadoutType.users, alias="type"),
) -> SearchLoadoutsResponse:
    with get_global_session() as session:
        _migrate_legacy_if_needed(session)

        rows = session.exec(
            select(SearchLoadoutModel)
            .where(SearchLoadoutModel.loadout_type == loadout_type.value)
            .order_by(SearchLoadoutModel.name.asc())
        ).all()
        default_name = _default_name(session, loadout_type)

        loadouts = [
            SearchLoadoutItem(
                name=row.name,
                type=loadout_type,
                identifier=row.identifier or "",
                description=row.description or "",
                site=row.site or "",
                filters=_parse_filters(row.filters_json or "{}"),
                is_default=bool(row.is_default),
                updated_at=row.updated_at.isoformat() if row.updated_at else None,
            )
            for row in rows
        ]

    return SearchLoadoutsResponse(
        success=True,
        type=loadout_type,
        default_name=default_name,
        loadouts=loadouts,
    )


@router.put("/{name}", response_model=SearchLoadoutMutationResponse)
async def upsert_search_loadout(
    name: str,
    payload: SearchLoadoutUpsertRequest,
    loadout_type: LoadoutType = Query(LoadoutType.users, alias="type"),
) -> SearchLoadoutMutationResponse:
    clean_name = _validate_name(name)

    with get_global_session() as session:
        _migrate_legacy_if_needed(session)
        _upsert_row(session, loadout_type, clean_name, payload)
        if payload.set_as_default:
            _set_default(session, loadout_type, clean_name)
        session.commit()
        default_name = _default_name(session, loadout_type)

    return SearchLoadoutMutationResponse(
        success=True,
        type=loadout_type,
        name=clean_name,
        default_name=default_name,
    )


@router.put("/{name}/default", response_model=SearchLoadoutMutationResponse)
async def set_default_search_loadout(
    name: str,
    loadout_type: LoadoutType = Query(LoadoutType.users, alias="type"),
) -> SearchLoadoutMutationResponse:
    clean_name = _validate_name(name)
    with get_global_session() as session:
        _migrate_legacy_if_needed(session)
        row = session.exec(
            select(SearchLoadoutModel).where(
                SearchLoadoutModel.loadout_type == loadout_type.value,
                SearchLoadoutModel.name == clean_name,
            )
        ).first()
        if row is None:
            raise HTTPException(status_code=404, detail="Loadout not found")
        _set_default(session, loadout_type, clean_name)
        session.commit()

    return SearchLoadoutMutationResponse(
        success=True,
        type=loadout_type,
        name=clean_name,
        default_name=clean_name,
    )


@router.delete("/default", response_model=SearchLoadoutMutationResponse)
async def clear_default_search_loadout(
    loadout_type: LoadoutType = Query(LoadoutType.users, alias="type"),
) -> SearchLoadoutMutationResponse:
    with get_global_session() as session:
        _migrate_legacy_if_needed(session)
        _clear_default(session, loadout_type)
        session.commit()

    return SearchLoadoutMutationResponse(
        success=True,
        type=loadout_type,
        name="",
        default_name=None,
    )


@router.delete("/{name}", response_model=SearchLoadoutMutationResponse)
async def delete_search_loadout(
    name: str,
    loadout_type: LoadoutType = Query(LoadoutType.users, alias="type"),
) -> SearchLoadoutMutationResponse:
    clean_name = _validate_name(name)
    with get_global_session() as session:
        _migrate_legacy_if_needed(session)
        row = session.exec(
            select(SearchLoadoutModel).where(
                SearchLoadoutModel.loadout_type == loadout_type.value,
                SearchLoadoutModel.name == clean_name,
            )
        ).first()
        if row is None:
            raise HTTPException(status_code=404, detail="Loadout not found")
        was_default = bool(row.is_default)
        session.delete(row)
        session.commit()

        default_name = _default_name(session, loadout_type)
        if was_default:
            default_name = None

    return SearchLoadoutMutationResponse(
        success=True,
        type=loadout_type,
        name=clean_name,
        default_name=default_name,
    )
