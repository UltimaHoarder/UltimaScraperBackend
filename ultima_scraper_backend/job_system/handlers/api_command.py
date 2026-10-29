"""
API Command task handler for the job system.

This handler processes API command tasks by utilizing the worker's USAPI instance
to execute read-only API operations. It supports all API Explorer commands:
- get_authed_user: Get authenticated account profile
- get_user: Lookup user by username/ID
- get_lists: Fetch account lists
- get_subscriptions: Get subscription list
- get_subscription_count: Get subscription and subscriber counts
- get_transactions: Fetch transaction history
- get_chats: List all chats/conversations
- search_chats: Search chats by username or text
- get_messages: Get messages from specific user
- get_mass_message_stats: Retrieve mass-message statistics
- get_paid_content: Retrieve paid posts and messages
- blocked_users: List blocked users
- restricted_users: List restricted users
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import base64
import dataclasses
import ipaddress
import os
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from ultima_scraper_api import auth_types

import json

from ultima_scraper_backend.ultima_control.redis_service import publish_api_command_result

from ...helpers.user_summary import summarize_user
from ...redis_channels import HOOKS_CHANNEL
from ..events import APICommandEvent
from ..models import (
    TaskResult,
    TaskRunContext,
    TaskType,
    register_task_handler,
)

logger = logging.getLogger(__name__)

type APICommandHandler = Callable[
    ["auth_types", TaskRunContext, dict[str, Any]], Awaitable[dict[str, Any]]
]


def _stringify_mapping(value: Mapping[object, object] | None) -> dict[str, Any]:
    if value is None:
        return {}
    return {str(key): item for key, item in value.items()}


def _serialize_api_value(value: Any) -> Any:
    return _serialize_api_value_inner(value, set())


def _serialize_api_value_inner(value: Any, seen: set[int]) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return _serialize_api_value_inner(value.value, seen)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return {"__worker_bytes__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_serialize_api_value_inner(item, seen) for item in value]
    if isinstance(value, Mapping):
        value_id = id(value)
        if value_id in seen:
            return None
        seen.add(value_id)
        return {
            str(key): _serialize_api_value_inner(item, seen)
            for key, item in value.items()
        }

    value_id = id(value)
    if value_id in seen:
        return None
    seen.add(value_id)

    raw_value = getattr(value, "__raw__", None)
    if isinstance(raw_value, Mapping):
        return _serialize_api_value_inner(raw_value, seen)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _serialize_api_value_inner(model_dump(), seen)
        except Exception:
            pass

    as_dict = getattr(value, "dict", None)
    if callable(as_dict):
        try:
            return _serialize_api_value_inner(as_dict(), seen)
        except Exception:
            pass

    if dataclasses.is_dataclass(value):
        try:
            return _serialize_api_value_inner(dataclasses.asdict(value), seen)
        except Exception:
            pass

    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, dict):
        sensitive_keys = {
            "api", "api_instance", "auth", "authed", "auth_session",
            "authorization", "cookie", "credentials", "email", "password",
            "requester", "session", "websocket_manager", "x_bc",
        }
        return {
            str(key): _serialize_api_value_inner(item, seen)
            for key, item in attributes.items()
            if not str(key).startswith("_")
            and str(key).lower() not in sensitive_keys
            and not callable(item)
        }

    return str(value)


_REMOTE_METHODS: dict[str, frozenset[str]] = {
    "auth": frozenset(
        {
            "get_user", "get_authed_user", "get_me", "get_subscription",
            "get_subscriptions", "get_chats", "send_message",
            "notification_settings", "needs_age_verification",
            "get_age_verification", "get_identity_verification",
            "get_transactions", "get_lists", "get_paid_content",
            "get_mass_message_stats", "get_subscription_count", "blocked_users",
            "restricted_users",
        }
    ),
    "user": frozenset(
        {
            "get_messages", "get_message_by_id", "get_post",
            "subscription_price", "buy_subscription", "get_requester",
            "get_socials",
        }
    ),
    "message": frozenset({"buy_message", "buy_ppv", "like_message"}),
    "post": frozenset({"buy_ppv"}),
    "requester": frozenset({"json_request"}),
    "drm": frozenset({"resolve_drm"}),
    "auth_session": frozenset({"request", "json_request"}),
}


def _decode_worker_api_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_worker_api_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "__worker_bytes__" in value:
        return base64.b64decode(str(value["__worker_bytes__"]))
    if value.get("__worker_type__") == "DRMMedia":
        from ultima_scraper_api.apis.onlyfans.classes.only_drm import DRMMedia

        data = value.get("value")
        if not isinstance(data, dict):
            raise ValueError("Malformed DRM media argument")
        return DRMMedia(**data)
    return {str(key): _decode_worker_api_value(item) for key, item in value.items()}


async def _resolve_remote_target(
    authed: auth_types,
    target: Mapping[str, Any],
) -> Any:
    kind = str(target.get("kind") or "")
    if kind == "auth":
        return authed
    if kind == "drm":
        drm = getattr(authed, "drm", None)
        if drm is None:
            raise RuntimeError("Authenticated session does not expose DRM operations")
        return drm
    if kind == "auth_session":
        session = getattr(authed, "auth_session", None)
        if session is None:
            raise RuntimeError("Authenticated session has no HTTP session")
        return session

    user_id = target.get("user_id")
    user = None
    if user_id is not None:
        find_user = getattr(authed, "find_user", None)
        if callable(find_user):
            user = find_user(user_id)
        if user is None:
            get_user = getattr(authed, "get_user", None)
            if callable(get_user):
                user = get_user(user_id)
                if inspect.isawaitable(user):
                    user = await user
    if kind == "user":
        if user is None:
            raise RuntimeError(f"User '{user_id}' is not available for this account")
        return user
    if kind == "requester":
        owner = user if user is not None else authed
        get_requester = getattr(owner, "get_requester", None)
        if not callable(get_requester):
            raise RuntimeError("Authenticated API object has no requester")
        requester = get_requester()
        if inspect.isawaitable(requester):
            requester = await requester
        return requester
    if kind in {"message", "post"}:
        if user is None:
            raise RuntimeError(f"User '{user_id}' is not available for this account")
        if kind == "post":
            get_post = getattr(user, "get_post", None)
            if not callable(get_post):
                raise RuntimeError("User object does not support post lookup")
            post = get_post(identifier=target.get("post_id"))
            if inspect.isawaitable(post):
                post = await post
            return post

        message_id = target.get("message_id")
        get_message = getattr(user, "get_message_by_id", None)
        if not callable(get_message):
            raise RuntimeError("User object does not support message lookup")
        message = get_message(
            user_id=user_id,
            message_id=message_id,
            limit=20,
            offset=0,
        )
        if inspect.isawaitable(message):
            message = await message
        if getattr(message, "id", None) is None:
            return None
        return message

    raise ValueError(f"Unsupported API target kind '{kind}'")


def _remote_result_kind(target_kind: str, method: str) -> str | None:
    if method in {"get_user", "get_authed_user", "get_me"}:
        return "user"
    if method == "get_messages":
        return "message"
    if method == "get_message_by_id":
        return "message"
    if method == "get_post":
        return "post"
    if method == "get_chats":
        return "chat"
    if method == "get_authed":
        return "auth"
    return None


def _validate_worker_request_url(url: Any) -> str:
    parsed = urlsplit(str(url or ""))
    hostname = (parsed.hostname or "").lower().rstrip(".")
    if parsed.scheme != "https" or not hostname or parsed.username or parsed.password:
        raise ValueError("Worker API request requires an HTTPS URL")
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        raise ValueError("Worker API request cannot target an IP address")
    if not (
        hostname == "onlyfans.com"
        or hostname.endswith(".onlyfans.com")
        or hostname.endswith(".cloudfront.net")
        or hostname.endswith(".amazonaws.com")
    ):
        raise ValueError("Worker API request host is not an approved OnlyFans/CDN host")
    return parsed.geturl()


async def _refresh_profile(
    authed: auth_types,
    context: TaskRunContext,
    args: dict[str, Any],
) -> dict[str, Any]:
    """Fetch profile data in one job; the API process owns persistence."""
    from ...api.routes.users import (
        PROFILE_REFRESH_WORKER_TIMEOUT_SECONDS,
        PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS,
        _lookup_site_user,
    )
    from ...database.helpers.user_sync import _fetch_socials, _get_value

    async def fetch() -> dict[str, Any]:
        user_id = args.get("identifier")
        if user_id is None:
            user_id = int(args["user_id"])
        username = str(args.get("username") or "").strip().lstrip("@")
        lookups: dict[str, Any] = {}
        identifiers: list[int | str] = [user_id]
        if username and username.casefold() != f"u{user_id}":
            identifiers.append(username)
        for identifier in identifiers:
            result = await _lookup_site_user(
                str(context.site), identifier, use_worker_pool=False
            )
            data = None
            socials = []
            if result.user is not None:
                data = _serialize_api_value(result.user)
                # Retain computed fields used by profile persistence, without
                # sending a remote facade that dispatches additional jobs.
                for field in ("is_active", "is_performer", "join_date", "credit_balance"):
                    value = await _get_value(result.user, field)
                    if value is not None:
                        data[field] = _serialize_api_value(value)
                try:
                    socials = await asyncio.wait_for(
                        _fetch_socials(result.user),
                        timeout=PROFILE_LOOKUP_ROUTE_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    logger.warning("Profile socials timed out for %s/%s", context.site, identifier)
            lookups[str(identifier)] = {
                "user": data,
                "socials": socials,
                "conclusive_missing": result.conclusive_missing,
                "attempted_routes": result.attempted_routes,
                "completed_routes": result.completed_routes,
            }
            if result.user is not None:
                break
        return {"lookups": lookups}

    return await asyncio.wait_for(fetch(), timeout=PROFILE_REFRESH_WORKER_TIMEOUT_SECONDS)


async def _invoke_api_method(
    authed: auth_types,
    context: TaskRunContext,
    args: dict[str, Any],
) -> dict[str, Any]:
    target_ref = args.get("target")
    if not isinstance(target_ref, Mapping):
        raise ValueError("API method invocation requires a target")
    target_kind = str(target_ref.get("kind") or "")
    method_name = str(args.get("method") or "")
    if method_name not in _REMOTE_METHODS.get(target_kind, frozenset()):
        raise ValueError(
            f"API method '{method_name}' is not allowed for target '{target_kind}'"
        )

    target = await _resolve_remote_target(authed, target_ref)
    method = getattr(target, method_name, None)
    if not callable(method):
        raise RuntimeError(
            f"API target '{target_kind}' does not support '{method_name}'"
        )

    call_args = _decode_worker_api_value(args.get("args") or [])
    call_kwargs = _decode_worker_api_value(args.get("kwargs") or {})
    if not isinstance(call_args, list) or not isinstance(call_kwargs, dict):
        raise ValueError("API method arguments must be JSON arrays and objects")

    if target_kind in {"auth_session", "requester"} and method_name in {
        "request",
        "json_request",
    }:
        if call_args:
            call_args[0] = _validate_worker_request_url(call_args[0])
        elif "url" in call_kwargs:
            call_kwargs["url"] = _validate_worker_request_url(call_kwargs["url"])

    result = method(*call_args, **call_kwargs)
    if inspect.isawaitable(result):
        result = await result

    if (
        target_kind == "auth_session"
        and method_name == "request"
    ):
        if result is None:
            raise RuntimeError("Upstream returned no response")
        reader = getattr(result, "read", None)
        body = reader() if callable(reader) else getattr(result, "body", None)
        if inspect.isawaitable(body):
            body = await body
        if not isinstance(body, bytes):
            content = getattr(result, "content", None)
            body = getattr(content, "_body", b"")
        max_bytes = max(
            1024,
            int(os.getenv("USR_WORKER_API_RESPONSE_MAX_BYTES", str(32 * 1024 * 1024))),
        )
        if len(body) > max_bytes:
            raise RuntimeError(
                f"Worker API response exceeded the {max_bytes}-byte response limit"
            )
        headers = getattr(result, "headers", None) or {}
        return {
            "response": True,
            "value": {
                "status": int(getattr(result, "status", 0) or 0),
                "headers": {str(key): str(value) for key, value in headers.items()},
                "content_type": str(getattr(result, "content_type", "") or ""),
                "body_base64": base64.b64encode(body).decode("ascii"),
            },
        }

    return {
        "kind": _remote_result_kind(target_kind, method_name),
        "value": _serialize_api_value(result),
    }


def _parse_identifier(value: Any, *, field_name: str) -> str:
    identifier = str(value).strip()
    if not identifier:
        raise ValueError(f"{field_name} cannot be empty.")
    return identifier


async def _invoke_auth_method(
    authed: auth_types,
    method_name: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    method = getattr(cast(Any, authed), method_name, None)
    if not callable(method):
        raise RuntimeError(
            f"Authenticated session does not support '{method_name}' for this site."
        )
    result = method(*args, **kwargs)
    if not inspect.isawaitable(result):
        raise RuntimeError(
            f"Authenticated session returned a non-awaitable result for '{method_name}'."
        )
    return await result


def _iter_auths(api: Any) -> list[auth_types]:
    auths = getattr(api, "auths", None)
    if isinstance(auths, dict):
        return cast(list["auth_types"], list(cast(Any, auths).values()))
    if isinstance(auths, list):
        return cast(list["auth_types"], auths)
    return []


def _format_content_item(item: Any, content_type: str) -> dict[str, Any]:
    """Format content item (post/message) for JSON response."""
    result: dict[str, Any] = {
        "id": getattr(item, "id", None),
        "created_at": getattr(item, "createdAt", None)
        or getattr(item, "postedAt", None),
        "text": getattr(item, "text", None),
        "price": getattr(item, "price", None),
        "is_archived": getattr(item, "isArchived", False),
        "can_purchase": getattr(item, "canPurchase", None),
    }

    # Add media info
    if hasattr(item, "medias") and item.medias:
        result["media_count"] = len(item.medias)
        result["media_types"] = list(
            {getattr(m, "type", "unknown") for m in item.medias}
        )

    # Add author info for messages
    if hasattr(item, "fromUser"):
        from_user = getattr(item, "fromUser", None)
        result["author"] = {
            "id": getattr(from_user, "id", None),
            "username": getattr(from_user, "username", None),
            "name": getattr(from_user, "name", None),
        }

    # Add response type for paid content
    if content_type == "paid":
        response_type = getattr(item, "responseType", None)
        result["type"] = (
            response_type
            if response_type
            else "message" if hasattr(item, "fromUser") else "post"
        )

    return result


async def _get_authed_user(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Get authenticated account profile."""
    try:
        user = await _invoke_auth_method(authed, "get_authed_user")
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_authed_user failed for site={context.site}"
        )
        raise RuntimeError(f"get_authed_user failed: {exc}") from exc

    if user is None:
        raise RuntimeError("No authenticated profile available.")

    summary = summarize_user(user)
    summary["site"] = context.site
    return {"user": summary}


async def _get_user(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Lookup user by username or ID."""
    identifier = args.get("identifier")
    if not identifier:
        raise ValueError("Identifier is required for get_user.")

    # Try to convert to int, otherwise use as string
    raw_str = str(identifier).strip()
    lookup: int | str = int(raw_str) if raw_str.isdigit() else _parse_identifier(identifier, field_name="Identifier")

    # Fetch user
    try:
        user = await _invoke_auth_method(authed, "get_user", lookup)
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] Failed to fetch user {identifier} on {context.site}"
        )
        raise RuntimeError(f"Failed to get user: {exc}") from exc

    if user is None:
        raise RuntimeError(f"User not found: {identifier}")

    return {"user": summarize_user(user)}


async def _get_lists(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Retrieve account lists."""
    limit = args.get("limit", 20)
    offset = args.get("offset", 0)

    try:
        limit_int = max(1, min(int(limit), 100))
        offset_int = max(0, int(offset))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit/offset: {exc}") from exc

    try:
        records = await _invoke_auth_method(
            authed, "get_lists", refresh=True, limit=limit_int, offset=offset_int
        )
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_lists failed for site={context.site}"
        )
        raise RuntimeError(f"get_lists failed: {exc}") from exc

    payload: Any
    if isinstance(records, list):
        payload = cast(list[Any], records)[:limit_int]
        message = f"Fetched {len(payload)} lists."
    else:
        payload = records
        message = "Lists fetched."

    return {"lists": payload, "message": message}


async def _get_subscriptions(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """List subscriptions."""
    limit = args.get("limit", 25)
    sub_type = str(args.get("sub_type", "all")).lower()
    filter_by = args.get("filter_by", "")

    try:
        limit_int = max(1, min(int(limit), 100))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit: {exc}") from exc

    # Validate sub_type
    if sub_type not in ("all", "active", "expired"):
        sub_type = "all"

    try:
        subs = await _invoke_auth_method(authed, "get_subscriptions")
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_subscriptions failed for site={context.site}"
        )
        raise RuntimeError(f"get_subscriptions failed: {exc}") from exc

    if not isinstance(subs, list):
        return {"subscriptions": subs}
    subscriptions = cast(list[Any], subs)

    # Filter by type
    if sub_type == "active":
        subscriptions = [s for s in subscriptions if getattr(s, "is_active", True)]
    elif sub_type == "expired":
        subscriptions = [s for s in subscriptions if not getattr(s, "is_active", True)]

    # Filter by username/id
    if filter_by:
        filter_str = str(filter_by).lower()
        subscriptions = [
            s
            for s in subscriptions
            if (
                filter_str in str(getattr(s, "username", "")).lower()
                or filter_str in str(getattr(s, "id", ""))
            )
        ]

    # Limit results
    subscriptions = subscriptions[:limit_int]

    return {"subscriptions": subscriptions, "count": len(subscriptions)}


async def _get_transactions(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Fetch transaction history."""
    limit = args.get("limit", 20)

    try:
        limit_int = max(1, min(int(limit), 100))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit: {exc}") from exc

    try:
        transactions = await _invoke_auth_method(
            authed, "get_transactions", limit=limit_int
        )
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_transactions failed for site={context.site}"
        )
        raise RuntimeError(f"get_transactions failed: {exc}") from exc

    if not isinstance(transactions, list):
        return {"transactions": transactions}
    transactions_list = cast(list[Any], transactions)

    return {
        "transactions": transactions_list[:limit_int],
        "count": len(transactions_list),
    }


async def _get_chats(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """List all chats/conversations."""
    limit = args.get("limit", 100)
    offset = args.get("offset", 0)

    try:
        limit_int = max(1, min(int(limit), 1000))
        offset_int = max(0, int(offset))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit/offset: {exc}") from exc

    try:
        chats = await _invoke_auth_method(
            authed, "get_chats", limit=limit_int, offset=offset_int
        )
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_chats failed for site={context.site}"
        )
        raise RuntimeError(f"get_chats failed: {exc}") from exc

    if not isinstance(chats, list):
        return {"chats": chats}
    chat_list = cast(list[Any], chats)

    return {"chats": chat_list[:limit_int], "count": len(chat_list)}


async def _get_messages(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Get messages from specific user."""
    user_id_arg = args.get("user_id")
    if not user_id_arg:
        raise ValueError("user_id is required for get_messages.")

    limit = args.get("limit", 20)

    try:
        limit_int = max(1, min(int(limit), 100))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit: {exc}") from exc

    # Parse user_id
    lookup = _parse_identifier(user_id_arg, field_name="user_id")

    # Get user first
    try:
        user = await _invoke_auth_method(authed, "get_user", lookup)
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] Failed to fetch user {lookup} for messages"
        )
        raise RuntimeError(f"Failed to get user: {exc}") from exc

    if user is None:
        raise RuntimeError(f"User not found: {lookup}")

    # Get messages
    try:
        messages = await user.get_messages(limit=limit_int)
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] Failed to fetch messages from user {lookup}"
        )
        raise RuntimeError(f"Failed to get messages: {exc}") from exc

    if not isinstance(messages, list):
        return {"messages": messages, "user": summarize_user(user)}
    message_list = cast(list[Any], messages)

    # Format messages
    formatted = [
        _format_content_item(msg, "message") for msg in message_list[:limit_int]
    ]

    return {
        "messages": formatted,
        "user": summarize_user(user),
        "count": len(formatted),
    }


async def _get_paid_content(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Retrieve paid posts and messages."""
    performer_id_arg = args.get("performer_id")
    limit = args.get("limit", 10)

    try:
        limit_int = max(1, min(int(limit), 100))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit: {exc}") from exc

    # Parse optional performer_id filter
    performer_lookup: int | str | None = None
    if performer_id_arg:
        try:
            performer_lookup = int(performer_id_arg)
        except (ValueError, TypeError):
            performer_lookup = str(performer_id_arg).strip() or None

    # Get paid content
    try:
        paid_content = await _invoke_auth_method(
            authed, "get_paid_contents", limit=limit_int
        )
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_paid_content failed for site={context.site}"
        )
        raise RuntimeError(f"get_paid_content failed: {exc}") from exc

    if not isinstance(paid_content, list):
        return {"paid_content": paid_content}
    paid_content_list = cast(list[Any], paid_content)

    # Filter by performer if specified
    if performer_lookup is not None:
        paid_content_list = [
            item
            for item in paid_content_list
            if (
                getattr(item, "user_id", None) == performer_lookup
                or getattr(item, "username", None) == performer_lookup
            )
        ]

    # Format content
    formatted = [
        _format_content_item(item, "paid") for item in paid_content_list[:limit_int]
    ]

    return {"paid_content": formatted, "count": len(formatted)}


async def _get_mass_message_stats(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Retrieve mass-message statistics."""
    limit = args.get("limit", 100)
    offset = args.get("offset", 0)

    try:
        limit_int = max(1, min(int(limit), 100))
        offset_int = max(0, int(offset))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit/offset: {exc}") from exc

    try:
        stats = await _invoke_auth_method(
            authed,
            "get_mass_message_stats",
            limit=limit_int,
            offset=offset_int,
        )
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_mass_message_stats failed for site={context.site}"
        )
        raise RuntimeError(f"get_mass_message_stats failed: {exc}") from exc

    payload = _serialize_api_value(stats)
    if isinstance(payload, list):
        return {
            "mass_message_stats": cast(list[Any], payload)[:limit_int],
            "count": len(cast(list[Any], payload)[:limit_int]),
        }
    return {"mass_message_stats": payload}


async def _get_subscription_count(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Retrieve subscription and subscriber counts."""
    del args

    try:
        counts = await _invoke_auth_method(authed, "get_subscription_count")
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] get_subscription_count failed for site={context.site}"
        )
        raise RuntimeError(f"get_subscription_count failed: {exc}") from exc

    return {"subscription_count": _serialize_api_value(counts)}


async def _get_user_collection(
    authed: auth_types,
    context: TaskRunContext,
    args: dict[str, Any],
    *,
    method_name: str,
    result_key: str,
) -> dict[str, Any]:
    limit = args.get("limit", 100)
    offset = args.get("offset", 0)

    try:
        limit_int = max(1, min(int(limit), 100))
        offset_int = max(0, int(offset))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid limit/offset: {exc}") from exc

    try:
        results = await _invoke_auth_method(
            authed,
            method_name,
            limit=limit_int,
            offset=offset_int,
        )
    except Exception as exc:
        logger.exception(
            f"[{context.worker_id}] {method_name} failed for site={context.site}"
        )
        raise RuntimeError(f"{method_name} failed: {exc}") from exc

    if not isinstance(results, list):
        return {result_key: _serialize_api_value(results)}

    items = cast(list[Any], results)[:limit_int]
    payload = [
        (
            summarize_user(user)
            if hasattr(user, "id") and hasattr(user, "username")
            else _serialize_api_value(user)
        )
        for user in items
    ]
    return {result_key: payload, "count": len(payload)}


async def _get_blocked_users(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Retrieve blocked users list."""
    return await _get_user_collection(
        authed,
        context,
        args,
        method_name="blocked_users",
        result_key="blocked_users",
    )


async def _get_restricted_users(
    authed: auth_types, context: TaskRunContext, args: dict[str, Any]
) -> dict[str, Any]:
    """Retrieve restricted users list."""
    return await _get_user_collection(
        authed,
        context,
        args,
        method_name="restricted_users",
        result_key="restricted_users",
    )


# Command routing table
_API_COMMAND_HANDLERS: dict[str, APICommandHandler] = {
    "refresh_profile": _refresh_profile,
    "invoke_method": _invoke_api_method,
    "get_authed_user": _get_authed_user,
    "get_user": _get_user,
    "get_lists": _get_lists,
    "get_subscriptions": _get_subscriptions,
    "refresh_subscriptions": _get_subscriptions,
    "get_subscription_count": _get_subscription_count,
    "get_transactions": _get_transactions,
    "get_chats": _get_chats,
    "get_messages": _get_messages,
    "get_mass_message_stats": _get_mass_message_stats,
    "get_paid_content": _get_paid_content,
    "blocked_users": _get_blocked_users,
    "restricted_users": _get_restricted_users,
}


async def handle_api_command_task(
    task: Any,  # Task object
    context: TaskRunContext,  # Worker execution context
) -> TaskResult:
    """
    Execute an API command task using the worker's USAPI instance.

    This handler retrieves and deserializes the API command specification from the task data,
    gets a logged-in USAPI instance from the worker's auth cache, executes the requested
    API operation, and publishes the result to Redis pub/sub for clients.

    Task data structure (in task.payload):
        {
            "command": str,        # API command name (e.g., "get_user", "get_lists")
            "site": str,           # Site name (e.g., "onlyfans")
            "account_id": int,     # Account ID for authentication
            "args": dict           # Command-specific arguments
        }

    Args:
        task: Task object containing command specification
        context: Worker runtime context with API and Redis access

    Returns:
        TaskResult with API command response or error details.
    """
    worker_id = context.worker_id
    site = context.site
    api = context.site_api  # OnlyFansAPI, FanslyAPI, etc.
    job = context.job
    if api is None:
        raise RuntimeError(
            "API command task requires site_api in the execution context."
        )

    # Initialize result variables
    success = False
    message = ""
    result_payload: dict[str, Any] = {}
    command = "unknown"
    temporary_api: Any = None

    try:
        # Extract task payload (APICommandTaskPayload)
        payload = task.payload
        if not payload:
            raise ValueError("API command task requires payload")

        # Access Pydantic model attributes directly (not dict.get())
        command = payload.command
        if not command:
            raise ValueError("API command task requires 'command' in payload")

        args = _stringify_mapping(payload.args)
        account_id = task.auth_id
        auth_override = args.pop("_auth_override", None)
        guest = bool(args.pop("_guest", False))

        if command == "invoke_method":
            operation = args.get("method", "unknown")
            target = args.get("target")
            target_kind = (
                target.get("kind", "unknown") if isinstance(target, Mapping) else "unknown"
            )
            logger.info(
                "[%s] Executing worker API method %s.%s on %s (account_id=%s)",
                worker_id,
                target_kind,
                operation,
                site,
                account_id,
            )
        else:
            logger.info(
                f"[{worker_id}] Executing API command '{command}' on {site} "
                f"(account_id={account_id}, args={args})"
            )

        # Get command handler
        handler = _API_COMMAND_HANDLERS.get(command)
        if not handler:
            raise ValueError(f"Unknown API command: {command}")

        # Credential edits can validate and use the submitted auth data without
        # logging in from the API process or mutating the worker's shared cache.
        if command == "refresh_profile":
            # This command owns its anonymous sessions and route attempts.
            authed = None
        elif auth_override is not None:
            if not isinstance(auth_override, Mapping):
                raise ValueError("Worker auth override must be a JSON object")
            from ultima_scraper_api import select_api

            temporary_api = select_api(
                str(site or ""), getattr(api, "config", None)
            )
            authed = await temporary_api.login(
                auth_json=dict(auth_override),
                guest=guest,
            )
            if not authed:
                raise RuntimeError("Worker could not authenticate the supplied credentials")
            api = temporary_api
        elif account_id:
            # Get authenticated session from the worker's shared auth cache.
            authed = cast("auth_types | None", api.find_auth(account_id))
            if not authed:
                raise RuntimeError(
                    f"Account {account_id} not found in worker's auth cache. "
                    f"This usually means:\n"
                    f"  1. Account credentials are invalid/expired (check logs for 'Failed to login account {account_id}')\n"
                    f"  2. Account cookies need to be refreshed\n"
                    f"  3. Account is not properly configured in the database\n"
                    f"Tip: Check the Settings -> Accounts tab to verify account status and re-authenticate if needed."
                )
        else:
            # Use first available auth
            auths = _iter_auths(api)
            if guest:
                auths = [auth for auth in auths if bool(getattr(auth, "guest", False))]
            if not auths:
                raise RuntimeError(
                    "API instance has no matching guest/auth session for this command"
                )
            authed = auths[0]

        # Execute command
        result_payload = await handler(authed, context, args)
        success = True
        message = f"Command '{command}' completed successfully"

    except Exception as exc:
        logger.exception(f"[{worker_id}] API command '{command}' failed: {exc}")
        result_payload = {"error": str(exc)}
        success = False
        message = f"Command '{command}' failed: {exc}"
    finally:
        if temporary_api is not None:
            close_pools = getattr(temporary_api, "close_pools", None)
            if callable(close_pools):
                try:
                    closed = close_pools()
                    if inspect.isawaitable(closed):
                        await closed
                except Exception:
                    logger.debug(
                        "[%s] Failed to close temporary API pools after %s",
                        worker_id,
                        command,
                        exc_info=True,
                    )

    # Publish results to the hooks channel even on error.
    try:
        # Create typed event with validation
        api_event = APICommandEvent(
            job_id=job.job_id,
            task_id=task.task_id,
            command=command,
            success=success,
            message=message,
            output=result_payload,
        )

        event_dict = api_event.model_dump(mode="json")

        # Prefer the worker's JobStorage Redis client (guaranteed connected)
        redis_manager = getattr(context.storage, "redis", None)
        redis_client = getattr(redis_manager, "client", None)
        published = False

        if redis_client is not None:
            try:
                payload = json.dumps(event_dict)
                await redis_client.publish(HOOKS_CHANNEL, payload)
                published = True
            except Exception as client_exc:
                logger.debug(
                    f"[{worker_id}] Direct Redis publish failed, falling back: {client_exc}"
                )

        if not published:
            await publish_api_command_result(event_dict)

        logger.debug(f"[{worker_id}] Published API command result for {command}")
    except Exception as pub_exc:
        logger.error(f"[{worker_id}] Failed to publish API command result: {pub_exc}")

    return TaskResult(
        success=success,
        message=message,
        output=result_payload,
    )


# Register the API command handler
register_task_handler(TaskType.API_COMMAND, handle_api_command_task)
