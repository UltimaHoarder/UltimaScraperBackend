"""Selected-account external purchase execution.

The caller selects an authenticated account, supplies a maximum permitted
charge, and provides an idempotency key. Business ownership remains outside
this generic USR execution boundary.
"""

from __future__ import annotations

import asyncio
import hashlib
from decimal import Decimal
from typing import Awaitable, Callable, Literal, cast

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, model_validator
from sqlmodel import select
from ultima_scraper_api import auth_types

from ultima_scraper_db_archive import UserAuthModel

from ...database.factory import MultiSiteDatabaseManager
from ...manager import BackendManager
from ...usapi_initializer import (
    AssignedProxyResolutionError,
    resolve_assigned_proxy_url,
)
from ..dependencies import get_backend_manager
from .orders import (
    _execute_ppv,
    _execute_subscription,
    _extract_balance,
    _refresh_spender_balance_after_purchase,
)

router = APIRouter(prefix="/account-purchases", tags=["account-purchases"])


class AccountPurchaseRequest(BaseModel):
    site: Literal["onlyfans", "fansly"]
    auth_record_id: int = Field(ge=1)
    site_account_user_id: int = Field(ge=1)
    required_proxy_id: str = Field(min_length=1, max_length=256)
    purchase_type: Literal["subscription", "ppv"]
    creator_id: str = Field(min_length=1, max_length=128)
    creator_username: str | None = Field(default=None, max_length=256)
    post_id: int | None = Field(default=None, ge=1)
    content_type: Literal["post", "message", "mass_message"] | None = None
    maximum_charge_usd: Decimal = Field(ge=0)
    require_proxy: bool = True

    @model_validator(mode="after")
    def validate_ppv(self) -> AccountPurchaseRequest:
        if self.purchase_type == "ppv" and self.post_id is None:
            raise ValueError("post_id is required for PPV purchases")
        return self


class AccountPurchaseResponse(BaseModel):
    success: bool
    status: Literal["confirmed", "failed", "reconciling"]
    actual_usd: Decimal | None = None
    external_reference_id: str | None = None
    already_owned: bool = False
    error_code: str | None = None


class AccountPurchaseQuoteResponse(BaseModel):
    success: bool
    quoted_usd: Decimal | None = None
    already_owned: bool = False
    error_code: str | None = None


def _price(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal, str)):
        try:
            price = Decimal(str(value)).quantize(Decimal("0.01"))
            return price if price.is_finite() and price >= 0 else None
        except Exception:
            return None
    return None


def _confirmed_balance_charge(
    balance_before: Decimal,
    refreshed_balance: object,
    site_reported_charge: Decimal,
    maximum_charge: Decimal,
) -> tuple[Decimal | None, bool]:
    balance_after = _price(refreshed_balance)
    charge = (
        (balance_before - balance_after).quantize(Decimal("0.01"))
        if balance_after is not None and balance_after <= balance_before
        else None
    )
    confirmed = (
        charge is not None
        and charge == site_reported_charge
        and charge <= maximum_charge
    )
    return charge, confirmed


async def _subscription_quote(
    authed: auth_types, creator_id: str, username: str | None
) -> tuple[Decimal | None, bool]:
    user = await authed.get_user(username or creator_id)
    if not user:
        return None, False
    price_attr = getattr(user, "subscription_price", None)
    value = (
        await cast(Callable[[], Awaitable[object]], price_attr)()
        if callable(price_attr)
        else price_attr
    )
    existing = False
    get_subscription = getattr(authed, "get_subscription", None)
    if callable(get_subscription):
        try:
            existing = (
                await cast(Callable[[object], Awaitable[object]], get_subscription)(
                    username or creator_id
                )
                is not None
            )
        except Exception:
            existing = False
    return _price(value), existing


async def _ppv_quote(
    authed: auth_types,
    creator_id: int,
    post_id: int,
    content_type: str | None,
) -> tuple[Decimal | None, bool]:
    user = None
    find_user = getattr(authed, "find_user", None)
    if callable(find_user):
        user = find_user(creator_id)
    if not user:
        get_user = getattr(authed, "get_user", None)
        if callable(get_user):
            user = await cast(Callable[[object], Awaitable[object]], get_user)(creator_id)
    if not user:
        return None, False
    normalized = (content_type or "post").strip().lower()
    if normalized in {"message", "mass_message"}:
        message = None
        get_message = getattr(user, "get_message_by_id", None)
        if callable(get_message):
            try:
                message = await cast(Callable[..., Awaitable[object]], get_message)(
                    user_id=creator_id, message_id=post_id, limit=10, offset=0
                )
            except Exception:
                message = None
        if not message:
            return None, False
        return _price(getattr(message, "price", None)), bool(
            getattr(message, "canViewMedia", False)
        )
    get_post = getattr(user, "get_post", None)
    post = None
    if callable(get_post):
        try:
            post = await cast(Callable[..., Awaitable[object]], get_post)(
                identifier=post_id
            )
        except Exception:
            post = None
    if not post:
        return None, False
    return _price(getattr(post, "price", None)), bool(
        getattr(post, "canViewMedia", False)
    )


async def _live_balance(authed: auth_types) -> Decimal | None:
    getter = getattr(authed, "get_authed_user", None)
    if not callable(getter):
        return None
    user = await cast(Callable[[], Awaitable[object]], getter)()
    value = _extract_balance(user)
    return _price(value)


async def _resolve_authed_account(
    payload: AccountPurchaseRequest,
    backend_manager: BackendManager,
) -> tuple[auth_types | None, str | None]:
    db_manager = MultiSiteDatabaseManager()
    site_db = db_manager.get_or_create_manager(payload.site)
    if not site_db:
        return None, "site_unavailable"
    with site_db.get_session() as session:
        auth_record = session.exec(
            select(UserAuthModel).where(
                UserAuthModel.id == payload.auth_record_id,
                UserAuthModel.site == payload.site,
                UserAuthModel.active.is_(True),
            )
        ).first()
        if not auth_record:
            return None, "auth_not_found"
        if auth_record.user_id != payload.site_account_user_id:
            return None, "auth_identity_mismatch"
        if (auth_record.proxy_id or "").strip() != payload.required_proxy_id.strip():
            return None, "proxy_assignment_mismatch"
        try:
            proxy_url = resolve_assigned_proxy_url(auth_record.proxy_id)
        except AssignedProxyResolutionError:
            return None, "proxy_unavailable"
        if payload.require_proxy and not proxy_url:
            return None, "proxy_required"
    authed = await backend_manager.get_authed_session(
        payload.site, payload.site_account_user_id
    )
    if not authed or not authed.is_authed():
        return None, "auth_expired"
    return authed, None


async def _quote_purchase(
    payload: AccountPurchaseRequest,
    authed: auth_types,
) -> tuple[Decimal | None, bool, str | None]:
    try:
        if payload.purchase_type == "subscription":
            quote, already_owned = await _subscription_quote(
                authed, payload.creator_id, payload.creator_username
            )
        else:
            quote, already_owned = await _ppv_quote(
                authed,
                int(payload.creator_id),
                int(payload.post_id or 0),
                payload.content_type,
            )
    except (TypeError, ValueError):
        return None, False, "invalid_creator_id"
    if quote is None:
        return None, already_owned, "price_unavailable"
    return (Decimal("0.00") if already_owned else quote), already_owned, None


@router.post("/quote", response_model=AccountPurchaseQuoteResponse)
async def quote_account_purchase(
    payload: AccountPurchaseRequest,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AccountPurchaseQuoteResponse:
    authed, error = await _resolve_authed_account(payload, backend_manager)
    if not authed:
        return AccountPurchaseQuoteResponse(success=False, error_code=error)
    try:
        async with asyncio.timeout(20):
            quote, already_owned, error = await _quote_purchase(payload, authed)
    except asyncio.TimeoutError:
        return AccountPurchaseQuoteResponse(
            success=False, error_code="site_timeout"
        )
    except Exception:
        return AccountPurchaseQuoteResponse(success=False, error_code="site_error")
    if error:
        return AccountPurchaseQuoteResponse(
            success=False, already_owned=already_owned, error_code=error
        )
    return AccountPurchaseQuoteResponse(
        success=True,
        quoted_usd=quote,
        already_owned=already_owned,
    )


@router.post("/execute", response_model=AccountPurchaseResponse)
async def execute_account_purchase(
    payload: AccountPurchaseRequest,
    request: Request,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AccountPurchaseResponse:
    idempotency_key = request.headers.get("idempotency-key", "").strip()
    if not 8 <= len(idempotency_key) <= 200:
        return AccountPurchaseResponse(
            success=False,
            status="failed",
            error_code="idempotency_key_required",
        )
    authed, account_error = await _resolve_authed_account(payload, backend_manager)
    if not authed:
        return AccountPurchaseResponse(
            success=False, status="failed", error_code=account_error
        )
    execution_started = False
    try:
        async with asyncio.timeout(45):
            quote, already_owned, quote_error = await _quote_purchase(payload, authed)
            if quote_error or quote is None:
                return AccountPurchaseResponse(
                    success=False,
                    status="failed",
                    error_code=quote_error or "price_unavailable",
                )
            if quote > payload.maximum_charge_usd:
                return AccountPurchaseResponse(
                    success=False, status="failed", error_code="price_exceeds_reservation"
                )
            balance = await _live_balance(authed)
            if balance is None or balance < quote:
                return AccountPurchaseResponse(
                    success=False, status="failed", error_code="insufficient_balance"
                )
            suffix = payload.post_id or "subscription"
            external_reference = (
                f"{payload.site}:{payload.site_account_user_id}:"
                f"{payload.creator_id}:{suffix}:"
                f"{hashlib.sha256(idempotency_key.encode()).hexdigest()[:16]}"
            )
            if already_owned:
                return AccountPurchaseResponse(
                    success=True,
                    status="confirmed",
                    actual_usd=Decimal("0.00"),
                    external_reference_id=external_reference,
                    already_owned=True,
                )
            execution_started = True
            if payload.purchase_type == "subscription":
                ok, error, actual = await _execute_subscription(
                    authed,
                    int(payload.creator_id),
                    payload.creator_username,
                    payload.site,
                )
            else:
                ok, error, actual, _username = await _execute_ppv(
                    authed,
                    int(payload.creator_id),
                    int(payload.post_id or 0),
                    payload.content_type,
                    payload.site,
                )
            actual_price = _price(actual)
            if not ok:
                return AccountPurchaseResponse(
                    success=False,
                    status="failed",
                    actual_usd=actual_price,
                    error_code="site_purchase_rejected",
                )
            if actual_price is None or actual_price > payload.maximum_charge_usd:
                return AccountPurchaseResponse(
                    success=False,
                    status="reconciling",
                    actual_usd=actual_price,
                    error_code="actual_charge_unconfirmed",
                )
            refreshed_balance = await _refresh_spender_balance_after_purchase(
                site=payload.site,
                authed=authed,
                auth_id=payload.site_account_user_id,
            )
            confirmed_charge, charge_confirmed = _confirmed_balance_charge(
                balance,
                refreshed_balance,
                actual_price,
                payload.maximum_charge_usd,
            )
            if not charge_confirmed:
                return AccountPurchaseResponse(
                    success=False,
                    status="reconciling",
                    actual_usd=confirmed_charge,
                    external_reference_id=external_reference,
                    error_code="actual_charge_unconfirmed",
                )
            return AccountPurchaseResponse(
                success=True,
                status="confirmed",
                actual_usd=confirmed_charge,
                external_reference_id=external_reference,
                already_owned=already_owned,
            )
    except asyncio.TimeoutError:
        return AccountPurchaseResponse(
            success=False,
            status="reconciling" if execution_started else "failed",
            error_code=("site_result_ambiguous" if execution_started else "site_timeout"),
        )
    except Exception:
        return AccountPurchaseResponse(
            success=False,
            status="reconciling" if execution_started else "failed",
            error_code=("site_result_ambiguous" if execution_started else "site_error"),
        )
