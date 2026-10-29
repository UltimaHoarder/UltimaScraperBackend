"""API Requests tool routes.

Execute API methods on authenticated sessions (e.g. get_paid_content,
get_subscriptions) and return the raw JSON results.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, cast

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ...manager import API_COMMANDS, APICommandArg, BackendManager
from ..dependencies import get_backend_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api-requests", tags=["api-requests"])


def _arg_schema(arg: APICommandArg) -> dict[str, Any]:
    type_map = {
        "string": "string",
        "int": "integer",
        "float": "number",
        "bool": "boolean",
        "enum": "string",
    }

    payload: dict[str, Any] = {
        "type": type_map.get(arg.kind, "string"),
        "description": arg.description,
        "required": arg.required,
    }
    if arg.default is not None:
        payload["default"] = arg.default
    if arg.choices:
        payload["options"] = list(arg.choices)
    if arg.placeholder:
        payload["placeholder"] = arg.placeholder
    return payload


def _build_allowed_methods() -> dict[str, dict[str, Any]]:
    methods: dict[str, dict[str, Any]] = {}
    for descriptor in API_COMMANDS:
        method_payload: dict[str, Any] = {
            "method": descriptor.name,
            "description": descriptor.description,
            "args": {arg.name: _arg_schema(arg) for arg in descriptor.args},
        }
        if descriptor.sites:
            method_payload["sites"] = list(descriptor.sites)
        methods[descriptor.name] = method_payload
    return methods


ALLOWED_METHODS: dict[str, dict[str, Any]] = _build_allowed_methods()


class APIRequestExecute(BaseModel):
    """Request body for executing an API method."""

    site: str
    auth_id: int
    method: str
    args: dict[str, Any] = {}


class APIRequestResponse(BaseModel):
    """Response from an API method execution."""

    success: bool
    method: str
    data: Any = None
    error: str | None = None


@router.get("/methods")
async def list_methods() -> dict[str, Any]:
    """Return the catalogue of available API methods and their arguments."""
    return {"methods": ALLOWED_METHODS}


@router.post("/execute", response_model=APIRequestResponse)
async def execute_api_method(
    request: APIRequestExecute,
    backend_manager: BackendManager = Depends(get_backend_manager),
) -> APIRequestResponse:
    """Execute an API method on an authenticated session."""

    if request.method not in ALLOWED_METHODS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown method '{request.method}'. "
            f"Allowed: {list(ALLOWED_METHODS.keys())}",
        )

    result = await backend_manager.run_api_command(
        request.site,
        request.method,
        request.args,
        request.auth_id,
    )
    if not result.success:
        return APIRequestResponse(
            success=False,
            method=request.method,
            error=result.message,
        )

    data = _serialise(result.payload)

    return APIRequestResponse(success=True, method=request.method, data=data)


def _serialise(obj: Any, _seen: set[int] | None = None) -> Any:
    """Best-effort JSON-safe serialisation of API model results."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (int, float)):
        return obj

    # Cycle detection for complex objects
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return None
    _seen.add(obj_id)

    if isinstance(obj, dict):
        obj_dict = cast(dict[object, Any], obj)
        return {str(key): _serialise(value, _seen) for key, value in obj_dict.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_serialise(item, _seen) for item in obj]

    # SiteContent models (PostModel, MessageModel, etc.) store the original
    # API JSON in __raw__ — use it directly to avoid circular references.
    raw_value = cast(Any, getattr(obj, "__raw__", None))
    if isinstance(raw_value, dict):
        raw_dict = cast(dict[object, Any], raw_value)
        return {str(key): _serialise(value, _seen) for key, value in raw_dict.items()}

    # Pydantic v2
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:
            pass

    # Pydantic v1
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass

    # Dataclass
    if hasattr(obj, "__dataclass_fields__"):
        import dataclasses

        try:
            data = cast(dict[object, Any], dataclasses.asdict(obj))
            return {str(key): _serialise(value, _seen) for key, value in data.items()}
        except Exception:
            pass

    # datetime
    if isinstance(obj, datetime):
        return obj.isoformat()

    # Generic __dict__ fallback
    if hasattr(obj, "__dict__"):
        attrs = cast(dict[str, Any], obj.__dict__)
        return {
            key: _serialise(value, _seen)
            for key, value in attrs.items()
            if not key.startswith("_")
        }

    return str(obj)
