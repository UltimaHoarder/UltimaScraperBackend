"""Authentication credentials API routes.

Provides endpoints for managing stored authentication credentials.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, List, cast

import aiohttp
from aiohttp_socks import ProxyConnector
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlmodel import select
from ultima_scraper_db_archive import UserAuthModel, UserModel

from ...database.factory import MultiSiteDatabaseManager
from ...database.helpers.user_sync import schedule_user_sync
from ...helpers.age_verification import normalize_age_verification_state
from ...helpers.proxy_url import resolve_proxy_target
from ...manager import BackendManager
from ...services.plugin_proxies import guard_proxy_assignment
from ...usapi_initializer import (
    AssignedProxyResolutionError,
    resolve_assigned_proxy_url,
)
from ..dependencies import get_backend_manager, get_config
from ._auth_event_utils import publish_auth_updated

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auths", tags=["auths"])


class AuthCredential(BaseModel):
    """Auth credential display model (sensitive data masked)."""

    id: int
    site: str
    username: str | None = None
    user_id: int
    status: str
    active: bool
    last_tested_at: str | None = None
    created_at: str | None = None
    # Masked versions of sensitive data
    cookie_preview: str | None = None
    has_authorization: bool = False
    user_agent_preview: str | None = None
    proxy_id: str | None = None
    # Worker shard assignment (which worker owns this auth)
    worker_shard: int | None = None
    # Age verification fields (OnlyFans-specific)
    is_age_verified: bool | None = None
    age_verification_required: bool | None = None
    # Spender configuration
    spend: bool = False
    spender_type: str | None = None


class AuthListResponse(BaseModel):
    """Response for listing auth credentials."""

    success: bool
    auths: List[AuthCredential] = []
    total: int = 0
    error: str | None = None


class AuthStatusUpdate(BaseModel):
    """Request to update auth status."""

    active: bool


class AuthEditRequest(BaseModel):
    """Request to edit auth credential fields."""

    username: str | None = None
    cookie: str | None = None
    x_bc: str | None = None
    authorization: str | None = None
    user_agent: str | None = None
    active: bool | None = None
    proxy_id: str | None = None
    spend: bool | None = None
    spender_type: str | None = None
    email_notifications_enabled: bool | None = None


class AuthTestResult(BaseModel):
    """Result of testing an auth credential."""

    success: bool
    status: str
    message: str


class AuthProxyTestResult(BaseModel):
    """Result of testing an auth proxy."""

    success: bool
    message: str
    latency_ms: int | None = None
    skipped: bool = False
    return_ip: str | None = None


class AgeVerificationResponse(BaseModel):
    """Response for age verification URL request."""

    success: bool
    redirect_url: str | None = None
    api_flow: str | None = None
    error: str | None = None


class AuthBalanceResponse(BaseModel):
    """Response for retrieving account balance."""

    success: bool
    balance: float | None = None
    error: str | None = None


def _mask_string(value: str | None, visible_chars: int = 8) -> str | None:
    """Mask a sensitive string, showing only first few characters."""
    if not value:
        return None
    if len(value) <= visible_chars:
        return "*" * len(value)
    return value[:visible_chars] + "..." + "*" * 8


def _format_datetime(dt: datetime | None) -> str | None:
    """Format datetime to ISO string."""
    if dt is None:
        return None
    return dt.isoformat()


async def _apply_onlyfans_email_notifications(
    *,
    backend_manager: BackendManager,
    site: str,
    user_id: int,
    username: str | None,
    cookie: str | None,
    x_bc: str | None,
    user_agent: str,
    proxy_id: str | None,
    email_notifications_enabled: bool | None,
) -> None:
    if site.lower() != "onlyfans" or email_notifications_enabled is None:
        return

    try:
        proxy_url = resolve_assigned_proxy_url(proxy_id)
    except AssignedProxyResolutionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    auth_json = {
        "id": user_id,
        "username": username or "",
        "cookie": cookie or "",
        "x_bc": x_bc or "",
        "user_agent": user_agent,
    }
    if proxy_url:
        auth_json["proxy_url"] = proxy_url

    try:
        async with asyncio.timeout(30):
            result = await backend_manager.execute_worker_api_method(
                site="onlyfans",
                account_id=user_id,
                method="notification_settings",
                kwargs={
                    "email_notifications_enabled": email_notifications_enabled
                },
                auth_override=auth_json,
            )
            if isinstance(result, dict):
                result_data = cast(dict[str, Any], result)
                error = result_data.get("error")
            else:
                error = None
            if error:
                detail = (
                    cast(dict[str, Any], error).get("message")
                    if isinstance(error, dict)
                    else str(error)
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to update email notifications: {detail}",
                )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to update email notifications: {exc}",
        ) from exc
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Updating email notifications timed out",
        ) from exc


def _apply_auth_filters(
    query: Any,
    *,
    site_name: str,
    active_only: bool,
    status_filter: str | None,
    username: str | None,
    user_id: int | None,
) -> Any:
    """Apply common auth filters to a SQLModel query."""
    query = query.where(UserAuthModel.site == site_name)

    if active_only:
        query = query.where(UserAuthModel.active.is_(True))

    normalized_status = (status_filter or "").strip().lower()
    if normalized_status:
        valid_statuses = ("valid", "ok")
        status_value = func.lower(func.coalesce(UserAuthModel.status, ""))
        if normalized_status == "valid":
            query = query.where(status_value.in_(valid_statuses))
        elif normalized_status == "invalid":
            query = query.where(~status_value.in_(valid_statuses))

    if username:
        query = query.where(UserAuthModel.username.ilike(f"%{username}%"))

    if user_id is not None:
        query = query.where(UserAuthModel.user_id == user_id)

    return query


def _get_worker_shard(auth_id: int) -> int:
    """Calculate which worker shard owns this auth.

    Uses modulo-based distribution: auth_id % worker_count.
    """
    try:
        config = get_config()
        worker_count = config.settings.worker_pool.size
        if worker_count <= 0:
            worker_count = 1
        return auth_id % worker_count
    except Exception:
        return 0


def _test_proxy_connectivity(
    proxy_url: str, timeout_seconds: float = 5.0
) -> tuple[bool, str, int | None]:
    try:
        host, port = resolve_proxy_target(proxy_url)
    except ValueError as exc:
        return False, str(exc), None

    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            latency_ms = int((time.perf_counter() - start) * 1000)
            return True, "TCP connection succeeded", latency_ms
    except Exception as exc:
        return False, str(exc), None


async def _fetch_return_ip(proxy_url: str | None) -> str | None:
    timeout = aiohttp.ClientTimeout(total=5)
    connector = ProxyConnector.from_url(proxy_url) if proxy_url else None
    try:
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            async with session.get("https://api.ipify.org?format=json") as response:
                if response.status != 200:
                    return None
                payload = await response.json()
                ip_value = payload.get("ip")
                return str(ip_value) if ip_value else None
    except Exception:
        return None


@router.get("/", response_model=AuthListResponse)
def list_auths(
    site: str | None = Query(None, description="Filter by site (onlyfans, fansly)"),
    # default to true so UI and other clients see active credentials by default
    active_only: bool = Query(True, description="Only show active credentials"),
    status_filter: str | None = Query(
        None, description="Filter by auth validity (valid or invalid)"
    ),
    username: str | None = Query(
        None, description="Search by username (partial match)"
    ),
    user_id: int | None = Query(None, description="Search by user ID (exact match)"),
    limit: int | None = Query(None, ge=1, le=500, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> AuthListResponse:
    """List all stored authentication credentials.

    By default this endpoint only returns *active* credentials unless the
    `active_only` query parameter is explicitly set to false. Sensitive data
    (cookies, tokens) are masked for security. Supports filtering by site,
    username, and user_id. Results are paginated server-side to avoid
    materializing large auth tables.
    """
    try:
        db_manager = MultiSiteDatabaseManager()

        all_auths: List[AuthCredential] = []

        # Get sites to query
        sites_to_query = [site.lower()] if site else ["onlyfans", "fansly"]

        site_totals: dict[str, int] = {}

        for site_name in sites_to_query:
            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                site_totals[site_name] = 0
                continue

            with site_db.get_session() as session:
                count_query = _apply_auth_filters(
                    select(func.count()).select_from(UserAuthModel),
                    site_name=site_name,
                    active_only=active_only,
                    status_filter=status_filter,
                    username=username,
                    user_id=user_id,
                )
                site_totals[site_name] = int(
                    cast(Any, session.exec(count_query)).one() or 0
                )

        total_auths = sum(site_totals.values())
        remaining_offset = offset
        remaining_limit = limit

        for site_name in sites_to_query:
            if remaining_limit is not None and remaining_limit <= 0:
                break

            site_total = site_totals.get(site_name, 0)
            if site_total <= 0:
                continue

            if remaining_offset >= site_total:
                remaining_offset -= site_total
                continue

            site_db = db_manager.get_or_create_manager(site_name)
            if not site_db:
                continue

            site_offset = remaining_offset
            remaining_offset = 0

            with site_db.get_session() as session:
                query = _apply_auth_filters(
                    select(UserAuthModel, UserModel).outerjoin(
                        UserModel, UserAuthModel.user_id == UserModel.id
                    ),
                    site_name=site_name,
                    active_only=active_only,
                    status_filter=status_filter,
                    username=username,
                    user_id=user_id,
                ).order_by(UserAuthModel.active.desc(), UserAuthModel.id.desc())

                query = query.offset(site_offset)
                if remaining_limit is not None:
                    query = query.limit(remaining_limit)

                results = session.exec(query).all()

                for auth, user in results:
                    is_age_verified = auth.is_age_verified
                    age_verification_required = auth.age_verification_required
                    if auth.site.lower() == "onlyfans":
                        is_age_verified, age_verification_required = (
                            normalize_age_verification_state(
                                is_age_verified,
                                age_verification_required,
                            )
                        )
                    all_auths.append(
                        AuthCredential(
                            id=auth.id,
                            site=auth.site,
                            username=auth.username or (user.username if user else None),
                            user_id=auth.user_id,
                            status=auth.status,
                            active=auth.active,
                            last_tested_at=_format_datetime(auth.last_tested_at),
                            created_at=_format_datetime(auth.created_at),
                            cookie_preview=_mask_string(auth.cookie),
                            has_authorization=bool(auth.authorization),
                            user_agent_preview=_mask_string(auth.user_agent, 20),
                            proxy_id=auth.proxy_id,
                            worker_shard=_get_worker_shard(auth.id),
                            is_age_verified=is_age_verified,
                            age_verification_required=age_verification_required,
                            spend=user.spend if user else False,
                            spender_type=user.spender_type if user else None,
                        )
                    )
                if remaining_limit is not None:
                    remaining_limit -= len(results)

        return AuthListResponse(
            success=True,
            auths=all_auths,
            total=total_auths,
        )

    except Exception as e:
        logger.exception("Failed to list auths")
        return AuthListResponse(
            success=False,
            error=str(e),
        )


@router.get("/{auth_id}", response_model=AuthCredential)
def get_auth(
    auth_id: int, site: str = Query(..., description="Site name")
) -> AuthCredential:
    """Get a specific auth credential by ID."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            result = session.exec(
                select(UserAuthModel, UserModel)
                .outerjoin(UserModel, UserAuthModel.user_id == UserModel.id)
                .where(UserAuthModel.id == auth_id)
            ).first()

            if not result:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            auth, user = result
            is_age_verified = auth.is_age_verified
            age_verification_required = auth.age_verification_required
            if auth.site.lower() == "onlyfans":
                is_age_verified, age_verification_required = (
                    normalize_age_verification_state(
                        is_age_verified,
                        age_verification_required,
                    )
                )

            return AuthCredential(
                id=auth.id,
                site=auth.site,
                username=auth.username or (user.username if user else None),
                user_id=auth.user_id,
                status=auth.status,
                active=auth.active,
                last_tested_at=_format_datetime(auth.last_tested_at),
                created_at=_format_datetime(auth.created_at),
                cookie_preview=_mask_string(auth.cookie),
                has_authorization=bool(auth.authorization),
                user_agent_preview=_mask_string(auth.user_agent, 20),
                proxy_id=auth.proxy_id,
                worker_shard=_get_worker_shard(auth.id),
                is_age_verified=is_age_verified,
                age_verification_required=age_verification_required,
                spend=user.spend if user else False,
                spender_type=user.spender_type if user else None,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get auth")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{auth_id}/balance", response_model=AuthBalanceResponse)
async def get_auth_balance(
    auth_id: int,
    site: str = Query(..., description="Site name"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AuthBalanceResponse:
    """Get account balance for a specific auth credential."""
    try:
        auth = await backend_manager.get_authed_session(site, auth_id)
        if not auth:
            return AuthBalanceResponse(
                success=False, error=f"Auth {auth_id} not found for site {site}"
            )

        def _extract_balance(user_obj: object | None) -> float | None:
            if user_obj is None:
                return None
            raw_balance = None
            if hasattr(user_obj, "credit_balance"):
                raw_balance = getattr(user_obj, "credit_balance")
            if raw_balance is None and hasattr(user_obj, "balance"):
                raw_balance = getattr(user_obj, "balance")
            if raw_balance is None:
                return None
            try:
                return float(raw_balance)
            except (TypeError, ValueError):
                return None

        balance = None
        site_lower = site.lower()

        if site_lower == "onlyfans" and hasattr(auth, "get_user"):
            get_user = getattr(auth, "get_user", None)
            try:
                user_from_api = (
                    await cast(Any, get_user)(str(auth.id), refresh=True)
                    if callable(get_user)
                    else None
                )
            except Exception as exc:
                logger.debug(f"Balance fetch via get_user failed: {exc}")
                user_from_api = None

            if user_from_api is not None:
                schedule_user_sync(user_from_api, site_lower)
                raw_balance = None
                if hasattr(user_from_api, "credit_balance"):
                    raw_balance = getattr(user_from_api, "credit_balance")
                if raw_balance is None and hasattr(user_from_api, "creditBalance"):
                    raw_balance = getattr(user_from_api, "creditBalance")
                if raw_balance is None and hasattr(user_from_api, "balance"):
                    raw_balance = getattr(user_from_api, "balance")
                if raw_balance is None and hasattr(user_from_api, "__raw__"):
                    raw_balance = getattr(user_from_api, "__raw__", {}).get(
                        "creditBalance"
                    )
                if raw_balance is not None:
                    try:
                        balance = float(raw_balance)
                    except (TypeError, ValueError):
                        balance = None

        if balance is None:
            get_authed_user = getattr(auth, "get_authed_user", None)
            user = (
                await cast(Any, get_authed_user)()
                if callable(get_authed_user)
                else None
            )
            balance = _extract_balance(user)

        if balance is None and hasattr(auth, "get_me"):
            try:
                get_me = getattr(auth, "get_me", None)
                me = await cast(Any, get_me)() if callable(get_me) else None
            except Exception:
                me = None
            balance = _extract_balance(me)

        if balance is None and hasattr(auth, "user"):
            balance = _extract_balance(getattr(auth, "user"))

        return AuthBalanceResponse(success=True, balance=balance)

    except Exception as exc:
        logger.exception("Failed to get auth balance")
        return AuthBalanceResponse(success=False, error=str(exc))


@router.patch("/{auth_id}/status")
async def update_auth_status(
    auth_id: int,
    update: AuthStatusUpdate,
    site: str = Query(..., description="Site name"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> dict[str, object]:
    """Enable or disable an auth credential."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.id == auth_id)
            ).first()

            if not auth:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            auth.active = update.active
            auth.touch()
            session.add(auth)
            session.commit()
            await publish_auth_updated(
                backend_manager,
                auth.user_id,
                username=auth.username,
                site=auth.site,
            )

            return {
                "success": True,
                "message": f"Auth {'enabled' if update.active else 'disabled'} successfully",
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to update auth status")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/{auth_id}", dependencies=[Depends(guard_proxy_assignment)])
async def update_auth(
    auth_id: int,
    update: AuthEditRequest,
    site: str = Query(..., description="Site name"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> dict[str, object]:
    """Update an auth credential's fields.

    Only provided fields will be updated. Sensitive fields (cookie, authorization, etc.)
    can be updated with new values.
    """
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            result = session.exec(
                select(UserAuthModel, UserModel)
                .outerjoin(UserModel, UserAuthModel.user_id == UserModel.id)
                .where(UserAuthModel.id == auth_id)
            ).first()

            if not result:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            auth, user = result

            effective_username = (
                update.username
                if "username" in update.model_fields_set
                else auth.username
            )
            effective_cookie = (
                update.cookie if "cookie" in update.model_fields_set else auth.cookie
            )
            effective_x_bc = (
                update.x_bc if "x_bc" in update.model_fields_set else auth.x_bc
            )
            effective_user_agent = (
                update.user_agent
                if "user_agent" in update.model_fields_set
                and update.user_agent is not None
                else auth.user_agent
            )
            effective_proxy_id = (
                update.proxy_id
                if "proxy_id" in update.model_fields_set
                else auth.proxy_id
            )
            user_id = auth.user_id

        await _apply_onlyfans_email_notifications(
            backend_manager=backend_manager,
            site=site,
            user_id=user_id,
            username=effective_username,
            cookie=effective_cookie,
            x_bc=effective_x_bc,
            user_agent=effective_user_agent,
            proxy_id=effective_proxy_id,
            email_notifications_enabled=update.email_notifications_enabled,
        )

        with site_db.get_session() as session:
            result = session.exec(
                select(UserAuthModel, UserModel)
                .outerjoin(UserModel, UserAuthModel.user_id == UserModel.id)
                .where(UserAuthModel.id == auth_id)
            ).first()

            if not result:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            auth, user = result

            # Update only provided fields (honor explicit nulls)
            if "username" in update.model_fields_set:
                auth.username = update.username
            if "cookie" in update.model_fields_set:
                auth.cookie = update.cookie
            if "x_bc" in update.model_fields_set:
                auth.x_bc = update.x_bc
            if "authorization" in update.model_fields_set:
                auth.authorization = update.authorization
            if (
                "user_agent" in update.model_fields_set
                and update.user_agent is not None
            ):
                auth.user_agent = update.user_agent
            if "active" in update.model_fields_set and update.active is not None:
                auth.active = update.active
            if "proxy_id" in update.model_fields_set:
                auth.proxy_id = update.proxy_id

            # Update UserModel fields if user exists
            if user:
                if "spend" in update.model_fields_set and update.spend is not None:
                    user.spend = update.spend
                if "spender_type" in update.model_fields_set:
                    user.spender_type = update.spender_type
                user.touch()
                session.add(user)

            auth.touch()
            session.add(auth)
            session.commit()
            session.refresh(auth)
            if user:
                session.refresh(user)
            await publish_auth_updated(
                backend_manager,
                auth.user_id,
                username=auth.username,
                site=auth.site,
            )

            return {
                "success": True,
                "message": "Auth credential updated successfully",
                "auth": AuthCredential(
                    id=auth.id,
                    site=auth.site,
                    username=auth.username,
                    user_id=auth.user_id,
                    status=auth.status,
                    active=auth.active,
                    last_tested_at=_format_datetime(auth.last_tested_at),
                    created_at=_format_datetime(auth.created_at),
                    cookie_preview=_mask_string(auth.cookie),
                    has_authorization=bool(auth.authorization),
                    user_agent_preview=_mask_string(auth.user_agent, 20),
                    proxy_id=auth.proxy_id,
                    spend=user.spend if user else False,
                    spender_type=user.spender_type if user else None,
                ).model_dump(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to update auth")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/{auth_id}/test", response_model=AuthTestResult)
async def test_auth(
    auth_id: int,
    site: str = Query(..., description="Site name"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AuthTestResult:
    """Test if an auth credential is still valid."""
    try:
        from ultima_scraper_api.apis.fansly.fansly import FanslyAPI

        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        # 1. Fetch auth data
        with site_db.get_session() as session:
            auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.id == auth_id)
            ).first()

            if not auth:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            proxy_id = auth.proxy_id
            user_id = auth.user_id
            username = auth.username
            user_agent = auth.user_agent
            authorization = auth.authorization

        # 2. Network Testing (outside of DB lock)
        try:
            proxy_url = resolve_assigned_proxy_url(proxy_id)
        except AssignedProxyResolutionError as exc:
            with site_db.get_session() as session:
                auth = session.exec(
                    select(UserAuthModel).where(UserAuthModel.id == auth_id)
                ).first()
                if auth:
                    auth.status = "error"
                    auth.last_tested_at = datetime.now(timezone.utc)
                    auth.touch()
                    session.add(auth)
                    session.commit()
            return AuthTestResult(
                success=False,
                status="proxy_error",
                message=str(exc),
            )

        if proxy_url:
            proxy_ok, proxy_message, _latency = _test_proxy_connectivity(proxy_url)
            if not proxy_ok:
                with site_db.get_session() as session:
                    auth = session.exec(
                        select(UserAuthModel).where(UserAuthModel.id == auth_id)
                    ).first()
                    if auth:
                        auth.status = "error"
                        auth.last_tested_at = datetime.now(timezone.utc)
                        auth.touch()
                        session.add(auth)
                        session.commit()
                return AuthTestResult(
                    success=False,
                    status="proxy_error",
                    message=f"Proxy test failed: {proxy_message}",
                )

        site_lower = site.lower()

        if site_lower == "onlyfans":
            try:
                async with asyncio.timeout(30):
                    authed = await backend_manager.get_authed_session(site, auth_id)
                    if not authed:
                        raise RuntimeError("Authentication is unavailable")
                    user = await authed.get_authed_user()
                    if user is None:
                        raise RuntimeError("No authenticated profile was returned")
                    needs_age_verification = getattr(
                        authed, "needs_age_verification", None
                    )
                    age_verification_required = (
                        await cast(Any, needs_age_verification)()
                        if callable(needs_age_verification)
                        else None
                    )
                    is_age_verified, age_verification_required = (
                        normalize_age_verification_state(
                            getattr(user, "is_age_verified", None),
                            age_verification_required,
                        )
                    )

                with site_db.get_session() as session:
                    auth = session.exec(
                        select(UserAuthModel).where(UserAuthModel.id == auth_id)
                    ).first()
                    if auth:
                        auth.status = "valid"
                        auth.last_tested_at = datetime.now(timezone.utc)
                        auth.is_age_verified = is_age_verified
                        auth.age_verification_required = age_verification_required
                        auth.touch()
                        session.add(auth)
                        session.commit()
                return AuthTestResult(
                    success=True,
                    status="valid",
                    message="Authentication is valid",
                )
            except asyncio.TimeoutError:
                status = "timeout"
                message = "Auth test timed out"
            except Exception as auth_error:
                status = "invalid"
                message = f"Authentication failed: {auth_error}"

            with site_db.get_session() as session:
                auth = session.exec(
                    select(UserAuthModel).where(UserAuthModel.id == auth_id)
                ).first()
                if auth:
                    auth.status = status
                    auth.last_tested_at = datetime.now(timezone.utc)
                    auth.touch()
                    session.add(auth)
                    session.commit()
            return AuthTestResult(
                success=False,
                status=status,
                message=message,
            )

        # Fansly still uses its existing site-specific login flow.

        if site_lower == "fansly":
            auth_json = {
                "id": user_id,
                "username": username or "",
                "authorization": authorization or "",
                "user_agent": user_agent,
            }
            if proxy_url:
                auth_json["proxy_url"] = proxy_url
            api = FanslyAPI()
        else:
            return AuthTestResult(
                success=False,
                status="error",
                message=f"Unsupported site: {site}",
            )

        # Try to authenticate using login_context for proper cleanup
        try:
            async with asyncio.timeout(30):
                async with api.login_context(auth_json) as authed:
                    if authed and authed.is_authed():
                        is_age_verified = None
                        age_verification_required = None
                        if site_lower == "onlyfans":
                            needs_age_verification = getattr(
                                authed,
                                "needs_age_verification",
                                None,
                            )
                            age_verification_required = (
                                await cast(Any, needs_age_verification)()
                                if callable(needs_age_verification)
                                else None
                            )
                            is_age_verified, age_verification_required = (
                                normalize_age_verification_state(
                                    is_age_verified,
                                    age_verification_required,
                                )
                            )

                        with site_db.get_session() as session:
                            auth = session.exec(
                                select(UserAuthModel).where(UserAuthModel.id == auth_id)
                            ).first()
                            if auth:
                                auth.status = "valid"
                                auth.last_tested_at = datetime.now(timezone.utc)
                                if site_lower == "onlyfans":
                                    auth.is_age_verified = is_age_verified
                                    auth.age_verification_required = (
                                        age_verification_required
                                    )
                                auth.touch()
                                session.add(auth)
                                session.commit()

                        return AuthTestResult(
                            success=True,
                            status="valid",
                            message="Authentication is valid",
                        )

                    with site_db.get_session() as session:
                        auth = session.exec(
                            select(UserAuthModel).where(UserAuthModel.id == auth_id)
                        ).first()
                        if auth:
                            auth.status = "invalid"
                            auth.last_tested_at = datetime.now(timezone.utc)
                            auth.touch()
                            session.add(auth)
                            session.commit()

                    return AuthTestResult(
                        success=False,
                        status="invalid",
                        message="Authentication failed - credentials may be expired",
                    )

        except asyncio.TimeoutError:
            with site_db.get_session() as session:
                auth = session.exec(
                    select(UserAuthModel).where(UserAuthModel.id == auth_id)
                ).first()
                if auth:
                    auth.status = "error"
                    auth.last_tested_at = datetime.now(timezone.utc)
                    auth.touch()
                    session.add(auth)
                    session.commit()

            return AuthTestResult(
                success=False,
                status="timeout",
                message="Auth test timed out",
            )

        except Exception as auth_error:
            with site_db.get_session() as session:
                auth = session.exec(
                    select(UserAuthModel).where(UserAuthModel.id == auth_id)
                ).first()
                if auth:
                    auth.status = "error"
                    auth.last_tested_at = datetime.now(timezone.utc)
                    auth.touch()
                    session.add(auth)
                    session.commit()

            return AuthTestResult(
                success=False,
                status="error",
                message=f"Test failed: {str(auth_error)}",
            )
        finally:
            # Ensure all websocket connections are closed
            with suppress(Exception):
                await api.websocket_manager.close_all_connections()

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to test auth")
        return AuthTestResult(
            success=False,
            status="error",
            message=str(e),
        )


@router.post("/{auth_id}/verify-age", response_model=AgeVerificationResponse)
async def get_age_verification_url(
    auth_id: int,
    site: str = Query(..., description="Site name"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AgeVerificationResponse:
    """Get age verification URL for an auth credential.

    Only works for OnlyFans accounts that require age verification.
    Returns a redirect URL that the user can visit to complete verification.
    """
    try:
        # Only OnlyFans supports age verification
        if site.lower() != "onlyfans":
            return AgeVerificationResponse(
                success=False,
                error="Age verification is only supported for OnlyFans",
            )

        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        # 1. Fetch auth data
        with site_db.get_session() as session:
            auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.id == auth_id)
            ).first()

            if not auth:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            # Check if verification is needed
            if auth.is_age_verified:
                return AgeVerificationResponse(
                    success=False,
                    error="Account is already age verified",
                )

            if not auth.age_verification_required:
                return AgeVerificationResponse(
                    success=False,
                    error="Age verification is not required for this account",
                )

            proxy_id = auth.proxy_id

        # 2. Network/Proxy operations outside DB lock. The authed session
        # applies the assigned proxy; refuse early if it cannot be resolved.
        try:
            resolve_assigned_proxy_url(proxy_id)
        except AssignedProxyResolutionError as exc:
            return AgeVerificationResponse(
                success=False,
                error=str(exc),
            )

        try:
            async with asyncio.timeout(30):
                authed = await backend_manager.get_authed_session(site, auth_id)
                if not authed:
                    return AgeVerificationResponse(
                        success=False,
                        error="Failed to authenticate - credentials may be expired",
                    )

                user = await authed.get_authed_user()
                age_verification_required = await authed.needs_age_verification()
                is_age_verified, age_verification_required = (
                    normalize_age_verification_state(
                        getattr(user, "is_age_verified", None),
                        age_verification_required,
                    )
                )

                # Persist the fresh verification state before opening the flow.
                with site_db.get_session() as session:
                    auth = session.exec(
                        select(UserAuthModel).where(UserAuthModel.id == auth_id)
                    ).first()
                    if auth:
                        auth.is_age_verified = is_age_verified
                        auth.age_verification_required = age_verification_required
                        auth.touch()
                        session.add(auth)
                        session.commit()

                result = await authed.get_age_verification()
                if result:
                    return AgeVerificationResponse(
                        success=True,
                        redirect_url=result.get("redirectUrl"),
                        api_flow=result.get("apiFlow"),
                    )
                return AgeVerificationResponse(
                    success=False,
                    error="No verification URL returned - verification may not be required",
                )

        except asyncio.TimeoutError:
            return AgeVerificationResponse(
                success=False,
                error="Request timed out",
            )

        except Exception as auth_error:
            return AgeVerificationResponse(
                success=False,
                error=f"Failed to get verification URL: {str(auth_error)}",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get age verification URL")
        return AgeVerificationResponse(
            success=False,
            error=str(e),
        )


@router.post("/{auth_id}/verify-identity", response_model=AgeVerificationResponse)
async def get_identity_verification_url(
    auth_id: int,
    site: str = Query(..., description="Site name"),
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AgeVerificationResponse:
    """Get identity verification URL for an auth credential.

    Exposed primarily for testing the OnlyFans identity verification flow.
    """
    try:
        if site.lower() != "onlyfans":
            return AgeVerificationResponse(
                success=False,
                error="Identity verification is only supported for OnlyFans",
            )

        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.id == auth_id)
            ).first()

            if not auth:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            proxy_id = auth.proxy_id

        try:
            resolve_assigned_proxy_url(proxy_id)
        except AssignedProxyResolutionError as exc:
            return AgeVerificationResponse(
                success=False,
                error=str(exc),
            )

        try:
            async with asyncio.timeout(30):
                authed = await backend_manager.get_authed_session(site, auth_id)
                if not authed:
                    return AgeVerificationResponse(
                        success=False,
                        error="Failed to authenticate - credentials may be expired",
                    )
                redirect_url = await authed.get_identity_verification()
                if redirect_url:
                    return AgeVerificationResponse(
                        success=True,
                        redirect_url=redirect_url,
                        api_flow=None,
                    )
                return AgeVerificationResponse(
                    success=False,
                    error="No verification URL returned",
                )

        except asyncio.TimeoutError:
            return AgeVerificationResponse(
                success=False,
                error="Request timed out",
            )

        except Exception as auth_error:
            return AgeVerificationResponse(
                success=False,
                error=f"Failed to get identity verification URL: {str(auth_error)}",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get identity verification URL")
        return AgeVerificationResponse(
            success=False,
            error=str(e),
        )


@router.post("/{auth_id}/proxy-test", response_model=AuthProxyTestResult)
async def test_auth_proxy(
    auth_id: int,
    site: str = Query(..., description="Site name"),
) -> AuthProxyTestResult:
    """Test the assigned proxy for an auth credential."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.id == auth_id)
            ).first()

            if not auth:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            proxy_id = auth.proxy_id

        if not proxy_id:
            return AuthProxyTestResult(
                success=True,
                message="No proxy assigned",
                skipped=True,
                return_ip=await _fetch_return_ip(None),
            )

        try:
            proxy_url = resolve_assigned_proxy_url(proxy_id)
        except AssignedProxyResolutionError as exc:
            return AuthProxyTestResult(
                success=False,
                message=str(exc),
            )

        if proxy_url is None:
            return AuthProxyTestResult(
                success=False,
                message="No proxy assigned to this auth credential",
            )

        proxy_ok, proxy_message, latency_ms = _test_proxy_connectivity(proxy_url)
        return AuthProxyTestResult(
            success=proxy_ok,
            message=proxy_message,
            latency_ms=latency_ms,
            return_ip=await _fetch_return_ip(proxy_url) if proxy_ok else None,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to test auth proxy")
        return AuthProxyTestResult(
            success=False,
            message=str(exc),
        )


@router.delete("/{auth_id}")
def delete_auth(
    auth_id: int,
    site: str = Query(..., description="Site name"),
) -> dict[str, object]:
    """Delete an auth credential."""
    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(site.lower())

        if not site_db:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")

        with site_db.get_session() as session:
            auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.id == auth_id)
            ).first()

            if not auth:
                raise HTTPException(status_code=404, detail="Auth credential not found")

            session.delete(auth)
            session.commit()

            return {
                "success": True,
                "message": "Auth credential deleted successfully",
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to delete auth")
        raise HTTPException(status_code=500, detail=str(e)) from e
