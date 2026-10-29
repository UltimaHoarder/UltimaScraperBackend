"""cURL parsing API routes.

Provides endpoints for parsing cURL commands or raw auth payloads to extract
authentication data.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, cast

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from ...manager import BackendManager
from ...services.plugin_proxies import guard_proxy_assignment
from ...usapi_initializer import resolve_proxy_url
from ..dependencies import get_backend_manager
from ._auth_event_utils import publish_auth_added, publish_auth_updated

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/curl", tags=["curl"])

RAW_AUTH_INPUT_ERROR = "Invalid auth JSON payload."
CURL_INPUT_ERROR = "Invalid cURL command - must start with 'curl'"


class CurlParseRequest(BaseModel):
    """Request to parse a cURL command."""

    curl_command: str
    site: str = "onlyfans"


class AuthJsonParseRequest(BaseModel):
    """Request to parse a raw auth JSON payload."""

    auth_json: str
    site: str = "onlyfans"


class ParsedCurlData(BaseModel):
    """Extracted data from a cURL command."""

    success: bool
    site: str
    cookie: str | None = None
    x_bc: str | None = None
    authorization: str | None = None
    user_agent: str | None = None
    user_id: int | None = None
    app_token: str | None = None
    sess: str | None = None
    auth_id: str | None = None
    error: str | None = None
    raw_headers: Dict[str, str] = {}


def _normalize_import_key(key: str) -> str:
    key = re.sub(r"(?<!^)(?=[A-Z])", "_", key.strip())
    return key.lower().replace("-", "_")


def _coerce_import_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _parse_json_auth_payload(raw_input: str) -> dict[str, str] | None:
    try:
        payload = json.loads(raw_input)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None

    normalized: dict[str, str] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            continue
        coerced = _coerce_import_value(value)
        if coerced is None:
            continue
        normalized[_normalize_import_key(key)] = coerced
    return normalized


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_onlyfans_cookie_values(
    cookie: str | None,
) -> tuple[str | None, str | None]:
    auth_id = None
    sess = None

    if cookie:
        auth_id_match = re.search(r"(?:^|;\s*)auth_id=([^;]+)", cookie)
        if auth_id_match:
            auth_id = auth_id_match.group(1)
        sess_match = re.search(r"(?:^|;\s*)sess=([^;]+)", cookie)
        if sess_match:
            sess = sess_match.group(1)

    return auth_id, sess


def _build_onlyfans_cookie(payload: dict[str, str]) -> str | None:
    existing_cookie = payload.get("cookie")
    if existing_cookie:
        return existing_cookie

    cookie_parts: list[str] = []
    sess = payload.get("sess")
    auth_id = payload.get("auth_id")
    auth_uid = payload.get("auth_uid_") or payload.get("auth_uid")

    if sess:
        cookie_parts.append(f"sess={sess}")
    if auth_id:
        cookie_parts.append(f"auth_id={auth_id}")
    if auth_uid:
        cookie_parts.append(f"auth_uid_={auth_uid}")

    return "; ".join(cookie_parts) or None


def parse_onlyfans_auth_payload(raw_input: str) -> ParsedCurlData:
    """Parse a raw OnlyFans auth JSON payload."""
    payload = _parse_json_auth_payload(raw_input)
    if payload is None:
        return ParsedCurlData(
            success=False,
            site="onlyfans",
            error=RAW_AUTH_INPUT_ERROR,
        )

    cookie = _build_onlyfans_cookie(payload)
    x_bc = payload.get("x_bc")
    authorization = payload.get("authorization")
    user_agent = payload.get("user_agent")
    app_token = payload.get("app_token")
    auth_uid = payload.get("auth_uid_") or payload.get("auth_uid")

    cookie_auth_id, cookie_sess = _extract_onlyfans_cookie_values(cookie)
    auth_id = payload.get("auth_id") or cookie_auth_id or auth_uid
    sess = payload.get("sess") or cookie_sess
    user_id_value = payload.get("user_id") or auth_id or auth_uid
    user_id_int = _parse_int(user_id_value)

    raw_headers: dict[str, str] = {}
    if cookie:
        raw_headers["cookie"] = cookie
    if x_bc:
        raw_headers["x-bc"] = x_bc
    if authorization:
        raw_headers["authorization"] = authorization
    if user_agent:
        raw_headers["user-agent"] = user_agent
    if app_token:
        raw_headers["app-token"] = app_token
    if sess:
        raw_headers["sess"] = sess
    if auth_id:
        raw_headers["auth_id"] = auth_id
    if auth_uid:
        raw_headers["auth_uid_"] = auth_uid

    if not cookie and not authorization:
        return ParsedCurlData(
            success=False,
            site="onlyfans",
            error="No cookie data or authorization header found in auth payload",
            raw_headers=raw_headers,
        )

    return ParsedCurlData(
        success=True,
        site="onlyfans",
        cookie=cookie,
        x_bc=x_bc,
        authorization=authorization,
        user_agent=user_agent,
        user_id=user_id_int,
        app_token=app_token,
        sess=sess,
        auth_id=auth_id,
        raw_headers=raw_headers,
    )


def _build_auth_login_request(
    site: str,
    parsed: ParsedCurlData,
    proxy_url: str | None = None,
) -> tuple[dict[str, Any], Any | None] | tuple[None, None]:
    from ultima_scraper_api.apis.fansly.fansly import FanslyAPI

    site_lower = site.lower()
    if site_lower == "onlyfans":
        auth_json: dict[str, Any] = {
            "id": parsed.user_id or _parse_int(parsed.auth_id) or 0,
            "username": "",
            "cookie": parsed.cookie or "",
            "x_bc": parsed.x_bc or "",
            "user_agent": parsed.user_agent or "",
        }
        if proxy_url:
            auth_json["proxy_url"] = proxy_url
        return auth_json, None

    if site_lower == "fansly":
        auth_json = {
            "id": parsed.user_id or 0,
            "username": "",
            "authorization": parsed.authorization or "",
            "user_agent": parsed.user_agent or "",
        }
        if proxy_url:
            auth_json["proxy_url"] = proxy_url
        return auth_json, FanslyAPI()

    return None, None


async def _test_parsed_auth(
    site: str,
    parsed: ParsedCurlData,
    backend_manager: BackendManager,
    proxy_url: str | None = None,
    email_notifications_enabled: bool | None = None,
) -> tuple[bool, str]:
    """Test parsed authentication data without saving it."""
    from contextlib import suppress

    auth_json, api = _build_auth_login_request(site, parsed, proxy_url)
    site_lower = site.lower()
    if auth_json is None or api is None:
        if site_lower != "onlyfans" or auth_json is None:
            return False, f"Unsupported site: {site}"

        try:
            async with asyncio.timeout(30):
                user = await backend_manager.execute_worker_api_method(
                    site=site_lower,
                    account_id=None,
                    method="get_authed_user",
                    auth_override=auth_json,
                )
                if user is None:
                    return False, "Authentication failed - credentials may be expired"

                if email_notifications_enabled is not None:
                    result = await backend_manager.execute_worker_api_method(
                        site=site_lower,
                        account_id=None,
                        method="notification_settings",
                        kwargs={
                            "email_notifications_enabled": email_notifications_enabled
                        },
                        auth_override=auth_json,
                    )
                    if isinstance(result, dict):
                        error = result.get("error")
                        if error is not None:
                            if isinstance(error, dict):
                                raw_message = error.get("message")
                                message = (
                                    raw_message
                                    if isinstance(raw_message, str)
                                    else str(raw_message)
                                )
                            else:
                                message = str(error)
                            return (
                                False,
                                f"Failed to update email notifications: {message}",
                            )
                return True, "Authentication is valid"
        except asyncio.TimeoutError:
            return False, "Authentication test timed out"
        except Exception as auth_error:  # pragma: no cover - defensive
            return False, f"Test failed: {auth_error}"

    try:
        async with api.login_context(auth_json) as authed:
            if authed and authed.is_authed():
                if site_lower == "onlyfans" and email_notifications_enabled is not None:
                    notification_settings = getattr(
                        authed, "notification_settings", None
                    )
                    if not callable(notification_settings):
                        return (
                            False,
                            "Failed to update email notifications: unsupported auth model",
                        )

                    result = await cast(Any, notification_settings)(
                        email_notifications_enabled=email_notifications_enabled
                    )
                    if isinstance(result, dict):
                        result_data = cast(dict[str, Any], result)
                        error = result_data.get("error")
                    else:
                        error = None
                    if error is not None:
                        if isinstance(error, dict):
                            error_data = cast(dict[str, Any], error)
                            raw_message = error_data.get("message")
                            message = (
                                raw_message
                                if isinstance(raw_message, str)
                                else str(raw_message)
                            )
                        else:
                            message = str(error)
                        return False, f"Failed to update email notifications: {message}"
                return True, "Authentication is valid"
            return False, "Authentication failed - credentials may be expired"
    except Exception as auth_error:  # pragma: no cover - defensive
        return False, f"Test failed: {auth_error}"
    finally:
        # Ensure all websocket connections are closed
        with suppress(Exception):
            if api is not None:
                await api.websocket_manager.close_all_connections()


async def _sync_imported_auth_user(
    *,
    site: str,
    parsed: ParsedCurlData,
    site_db: Any,
    backend_manager: BackendManager,
    account_id: int,
    proxy_url: str | None = None,
) -> bool:
    from contextlib import suppress

    from ...database.helpers.user_sync import sync_authed_user_from_api

    if site.strip().lower() == "onlyfans":
        try:
            authed = await backend_manager.get_authed_session(site, account_id)
            if authed is None:
                return False
            return await sync_authed_user_from_api(
                site_db,
                authed,
                site=site.lower(),
            )
        except Exception as exc:
            logger.debug("Failed to sync imported auth user through worker: %s", exc)
            return False

    auth_json, api = _build_auth_login_request(site, parsed, proxy_url)
    if auth_json is None or api is None:
        return False

    try:
        async with api.login_context(auth_json) as authed:
            if not authed or not authed.is_authed():
                return False
            return await sync_authed_user_from_api(
                site_db,
                authed,
                site=site.lower(),
            )
    except Exception as exc:
        logger.debug("Failed to sync imported auth user for %s: %s", site, exc)
        return False
    finally:
        with suppress(Exception):
            await api.websocket_manager.close_all_connections()


def parse_onlyfans_curl(curl_command: str) -> ParsedCurlData:
    """Parse an OnlyFans cURL command."""
    headers: Dict[str, str] = {}

    cookie_match = re.search(
        r"-b\s+[\"']([^\"']+)[\"']|--cookie\s+[\"']([^\"']+)[\"']",
        curl_command,
        re.IGNORECASE,
    )
    if cookie_match:
        cookie_value = cookie_match.group(1) or cookie_match.group(2) or ""
        if cookie_value:
            headers["cookie"] = cookie_value.strip()

    # Extract headers from -H or --header flags
    header_pattern = (
        r'-H\s+[\'"]([^:]+):\s*([^\'"]+)[\'"]|--header\s+[\'"]([^:]+):\s*([^\'"]+)[\'"]'
    )
    for match in re.finditer(header_pattern, curl_command, re.IGNORECASE):
        if match.group(1):
            key, value = match.group(1).strip(), match.group(2).strip()
        else:
            key, value = match.group(3).strip(), match.group(4).strip()
        headers[key.lower()] = value

    # Extract specific values
    cookie = headers.get("cookie")
    x_bc = headers.get("x-bc")
    authorization = headers.get("authorization")
    user_agent = headers.get("user-agent")
    user_id = headers.get("user-id")
    app_token = headers.get("app-token")

    # Parse user_id to int if present
    user_id_int = None
    if user_id:
        try:
            user_id_int = int(user_id)
        except ValueError:
            pass

    auth_id, sess = _extract_onlyfans_cookie_values(cookie)

    if not cookie and not authorization:
        return ParsedCurlData(
            success=False,
            site="onlyfans",
            error="No cookie or authorization header found in cURL command",
            raw_headers=headers,
        )

    return ParsedCurlData(
        success=True,
        site="onlyfans",
        cookie=cookie,
        x_bc=x_bc,
        authorization=authorization,
        user_agent=user_agent,
        user_id=user_id_int,
        app_token=app_token,
        sess=sess,
        auth_id=auth_id,
        raw_headers=headers,
    )


def parse_fansly_curl(curl_command: str) -> ParsedCurlData:
    """Parse a Fansly cURL command."""
    headers: Dict[str, str] = {}

    cookie_match = re.search(
        r"-b\s+[\"']([^\"']+)[\"']|--cookie\s+[\"']([^\"']+)[\"']",
        curl_command,
        re.IGNORECASE,
    )
    if cookie_match:
        cookie_value = cookie_match.group(1) or cookie_match.group(2) or ""
        if cookie_value:
            headers["cookie"] = cookie_value.strip()

    # Extract headers from -H or --header flags
    header_pattern = (
        r'-H\s+[\'"]([^:]+):\s*([^\'"]+)[\'"]|--header\s+[\'"]([^:]+):\s*([^\'"]+)[\'"]'
    )
    for match in re.finditer(header_pattern, curl_command, re.IGNORECASE):
        if match.group(1):
            key, value = match.group(1).strip(), match.group(2).strip()
        else:
            key, value = match.group(3).strip(), match.group(4).strip()
        headers[key.lower()] = value

    # Extract specific values for Fansly
    authorization = headers.get("authorization")
    user_agent = headers.get("user-agent")

    # Parse Fansly's session_active_session cookie
    cookie = headers.get("cookie", "")
    session_token = None
    if cookie:
        session_match = re.search(r"session_active_session=([^;]+)", cookie)
        if session_match:
            session_token = session_match.group(1)

    if not authorization:
        return ParsedCurlData(
            success=False,
            site="fansly",
            error="No authorization header found in cURL command",
            raw_headers=headers,
        )

    return ParsedCurlData(
        success=True,
        site="fansly",
        cookie=cookie if cookie else None,
        authorization=authorization,
        user_agent=user_agent,
        sess=session_token,
        raw_headers=headers,
    )


@router.post("/parse", response_model=ParsedCurlData)
async def parse_curl(request: CurlParseRequest) -> ParsedCurlData:
    """Parse a cURL command to extract authentication data.

    This endpoint accepts a cURL command (typically copied from browser
    DevTools) and extracts the relevant authentication headers and cookies.

    Supported sites:
    - onlyfans: Extracts cookie, x-bc, authorization, user-agent, user-id
    - fansly: Extracts authorization, user-agent, session token
    """
    curl_command = request.curl_command.strip()
    site = request.site.lower()

    if not curl_command:
        return ParsedCurlData(
            success=False,
            site=site,
            error="Empty cURL command",
        )

    if not curl_command.lower().startswith("curl"):
        return ParsedCurlData(
            success=False,
            site=site,
            error=CURL_INPUT_ERROR,
        )

    if site == "onlyfans":
        return parse_onlyfans_curl(curl_command)
    if site == "fansly":
        return parse_fansly_curl(curl_command)

    return ParsedCurlData(
        success=False,
        site=site,
        error=f"Unsupported site: {site}. Supported: onlyfans, fansly",
    )


@router.post("/parse-json", response_model=ParsedCurlData)
async def parse_auth_json(request: AuthJsonParseRequest) -> ParsedCurlData:
    """Parse a raw auth JSON payload to extract authentication data."""
    auth_json = request.auth_json.strip()
    site = request.site.lower()

    if not auth_json:
        return ParsedCurlData(
            success=False,
            site=site,
            error="Empty auth JSON payload",
        )

    if site == "onlyfans":
        return parse_onlyfans_auth_payload(auth_json)

    return ParsedCurlData(
        success=False,
        site=site,
        error=f"Unsupported site: {site}. Supported: onlyfans",
    )


class AddAuthFromCurlRequest(BaseModel):
    """Request to add auth from parsed cURL data."""

    curl_command: str
    site: str = "onlyfans"
    input_type: str = "curl"
    proxy_id: str | None = None
    email_notifications_enabled: bool | None = None


class AddAuthFromCurlResponse(BaseModel):
    """Response after adding auth from cURL."""

    success: bool
    auth_id: int | None = None
    username: str | None = None
    user_id: int | None = None
    error: str | None = None


@router.post("/add-auth", response_model=AddAuthFromCurlResponse, dependencies=[Depends(guard_proxy_assignment)])
async def add_auth_from_curl(
    request: AddAuthFromCurlRequest,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> AddAuthFromCurlResponse:
    """Parse cURL and add authentication to the database.

    This is a convenience endpoint that combines cURL parsing with
    adding the auth to the database.
    """
    from sqlmodel import select
    from ultima_scraper_db_archive import UserAuthModel, UserModel

    from ...database.factory import MultiSiteDatabaseManager

    input_type = request.input_type.strip().lower()
    if input_type == "json":
        parse_request = AuthJsonParseRequest(
            auth_json=request.curl_command,
            site=request.site,
        )
        parsed = await parse_auth_json(parse_request)
    else:
        parse_request = CurlParseRequest(
            curl_command=request.curl_command,
            site=request.site,
        )
        parsed = await parse_curl(parse_request)

    if not parsed.success:
        return AddAuthFromCurlResponse(
            success=False,
            error=parsed.error,
        )

    proxy_url = resolve_proxy_url(request.proxy_id)
    if request.proxy_id and not proxy_url:
        return AddAuthFromCurlResponse(
            success=False,
            error="Assigned proxy not found in config",
        )

    test_success, test_message = await _test_parsed_auth(
        request.site,
        parsed,
        backend_manager,
        proxy_url=proxy_url,
        email_notifications_enabled=request.email_notifications_enabled,
    )
    if not test_success:
        return AddAuthFromCurlResponse(
            success=False,
            error=test_message,
        )

    try:
        db_manager = MultiSiteDatabaseManager()
        site_db = db_manager.get_or_create_manager(request.site.lower())

        session = site_db.get_session()
        try:
            # Get or create user
            user_id = parsed.user_id
            if parsed.auth_id:
                try:
                    user_id = int(parsed.auth_id)
                except ValueError:
                    pass

            if not user_id:
                return AddAuthFromCurlResponse(
                    success=False,
                    error=(
                        "Could not determine user_id from imported auth data. "
                        "Please include auth_id, auth_uid_, or user_id."
                    ),
                )

            # Check if user exists
            user = session.exec(
                select(UserModel).where(UserModel.id == user_id)
            ).first()

            if not user:
                # Create new user
                user = UserModel(
                    id=user_id,
                    username=None,
                    active=True,
                )
                session.add(user)
                session.flush()

            # Check if auth already exists
            existing_auth = session.exec(
                select(UserAuthModel).where(UserAuthModel.user_id == user_id)
            ).first()

            if existing_auth:
                # Update existing auth
                existing_auth.cookie = parsed.cookie
                existing_auth.x_bc = parsed.x_bc
                existing_auth.authorization = parsed.authorization
                existing_auth.user_agent = parsed.user_agent or ""
                existing_auth.proxy_id = request.proxy_id
                existing_auth.status = "valid"
                existing_auth.active = True
                existing_auth.touch()
                session.add(existing_auth)
                session.commit()
                await _sync_imported_auth_user(
                    site=request.site,
                    parsed=parsed,
                    site_db=site_db,
                    backend_manager=backend_manager,
                    account_id=user_id,
                    proxy_url=proxy_url,
                )
                await publish_auth_updated(
                    backend_manager,
                    existing_auth.user_id,
                    username=existing_auth.username,
                    site=request.site.lower(),
                )

                return AddAuthFromCurlResponse(
                    success=True,
                    auth_id=existing_auth.id,
                    user_id=user_id,
                    username=existing_auth.username,
                )
            else:
                # Create new auth
                new_auth = UserAuthModel(
                    site=request.site.lower(),
                    user_id=user_id,
                    cookie=parsed.cookie,
                    x_bc=parsed.x_bc,
                    authorization=parsed.authorization,
                    user_agent=parsed.user_agent or "",
                    proxy_id=request.proxy_id,
                    status="valid",
                    active=True,
                )
                session.add(new_auth)
                session.commit()
                session.refresh(new_auth)
                await _sync_imported_auth_user(
                    site=request.site,
                    parsed=parsed,
                    site_db=site_db,
                    backend_manager=backend_manager,
                    account_id=user_id,
                    proxy_url=proxy_url,
                )
                await publish_auth_added(
                    backend_manager,
                    new_auth.user_id,
                    username=new_auth.username,
                    site=request.site.lower(),
                )

                return AddAuthFromCurlResponse(
                    success=True,
                    auth_id=new_auth.id,
                    user_id=user_id,
                )
        finally:
            session.close()

    except Exception as e:
        logger.exception("Failed to add auth from cURL")
        return AddAuthFromCurlResponse(
            success=False,
            error=str(e),
        )
