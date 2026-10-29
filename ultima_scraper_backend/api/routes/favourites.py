"""Local archive favourites; no upstream platform like/favourite actions."""
from typing import Literal

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from ultima_scraper_db_archive.global_db import global_session

from ...services import favourites_service as service
from ...services.profile_refresh import queue_cached_profile_refresh
from ...redis_events import publish_ws_event
from ..dependencies import get_backend_instance
from .favourite_filters import favourite_filters

router = APIRouter(prefix="/favourites", tags=["favourites"])
EntityType = Literal["user", "post", "story", "message"]


class EntityKey(BaseModel):
    site: str
    entity_type: EntityType
    entity_id: int = Field(gt=0, lt=2**63)


class BatchRequest(BaseModel):
    keys: list[EntityKey] = Field(max_length=200)


class ToggleRequest(BaseModel):
    expected: bool


@router.get("/status")
def status():
    # The WebUI hides favourites until every site has completed its cutover.
    try:
        with global_session() as session:
            service.ensure_active(session)
    except HTTPException as error:
        if error.status_code != 503:
            raise
        return {"enabled": False}
    return {"enabled": True}


@router.get("")
def list_favourites(background_tasks: BackgroundTasks = None, site: str | None = None, entity_type: EntityType | None = None,
                    search: str = Query("", max_length=512), order: Literal["asc","desc"] = "desc",
                    limit: int = Query(50,ge=1,le=100), cursor: str | None = Query(None,max_length=4096), advanced: dict = Depends(favourite_filters)):
    with global_session() as session:
        service.ensure_active(session, site)
        result = service.list_favourites(session,site,entity_type,search,order,limit,cursor,
                                         advanced if isinstance(advanced, dict) else None)
    queue_cached_profile_refresh(background_tasks,
        [item["entity"] for item in result["items"] if item["entity_type"] == "user" and item["entity"]])
    return result


@router.get("/counts")
def counts(site: str | None = None, entity_type: EntityType | None = None, search: str = Query("",max_length=512), advanced: dict = Depends(favourite_filters)):
    with global_session() as session:
        service.ensure_active(session,site)
        return service.counts(session,site,entity_type,search,advanced if isinstance(advanced, dict) else None)


@router.post("/states")
def batch_states(payload: BatchRequest):
    with global_session() as session:
        service.ensure_active(session)
        return {"states": service.batch_states(session,[key.model_dump() for key in payload.keys])}


@router.get("/{site}/{entity_type}/{entity_id}")
def state(site: str, entity_type: EntityType, entity_id: int):
    with global_session() as session:
        service.ensure_active(session,site)
        return service.state(session,site,entity_type,entity_id)


async def mutate(site,kind,entity_id,desired,backend,expected=None):
    with global_session() as session:
        service.ensure_active(session,site)
        result = service.set_state(session,site,kind,entity_id,desired,expected)
        session.commit()
    if kind == 'user':
        from ...services.user_stats_cache import user_stats_cache
        user_stats_cache.invalidate_site(site)
    await publish_ws_event(getattr(backend,"redis",None), {"type":"favourite_changed","site":site,"entity_type":kind,"entity_id":entity_id})
    return result


@router.put("/{site}/{entity_type}/{entity_id}")
async def add(site: str, entity_type: EntityType, entity_id: int, backend=Depends(get_backend_instance)):
    return await mutate(site,entity_type,entity_id,True,backend)


@router.delete("/{site}/{entity_type}/{entity_id}")
async def remove(site: str, entity_type: EntityType, entity_id: int, backend=Depends(get_backend_instance)):
    return await mutate(site,entity_type,entity_id,False,backend)


@router.post("/{site}/{entity_type}/{entity_id}/toggle")
async def toggle(site: str, entity_type: EntityType, entity_id: int, payload: ToggleRequest, backend=Depends(get_backend_instance)):
    return await mutate(site,entity_type,entity_id,not payload.expected,backend,payload.expected)
