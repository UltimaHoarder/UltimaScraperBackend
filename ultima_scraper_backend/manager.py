from __future__ import annotations

import asyncio
import base64
import contextlib
import dataclasses
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal, Optional, Sequence, cast
from uuid import uuid4

import ujson
import ultima_scraper_api
from sqlalchemy import event as sa_event
from sqlalchemy.orm import load_only
from sqlmodel import Session, select
from ultima_scraper_api import (
    api_types,
    auth_types,
)
from ultima_scraper_api.apis.onlyfans.authenticator import (
    extract_auth_details_from_curl as extract_onlyfans_auth,
)
from ultima_scraper_api.config import UltimaScraperAPIConfig as USAPIConfig
from ultima_scraper_db_archive import (
    DEFAULT_DB_PATH,
    UserAuthModel,
    UserModel,
    utcnow,
)
from ultima_scraper_db_archive.global_db import get_global_engine

from ultima_scraper_backend.config import UltimaScraperCollectionConfig
from ultima_scraper_backend.database import MultiSiteDatabaseManager
from ultima_scraper_backend.database.base_manager import BaseDatabaseManager
from ultima_scraper_backend.helpers.user_summary import summarize_user
from ultima_scraper_backend.redis.client import RedisManager
from ultima_scraper_backend.redis.worker_registry import WorkerRegistry
from ultima_scraper_backend.type_protocols import (
    ChatLike,
    SubscriptionLike,
    UserLike,
)
from ultima_scraper_backend.ultima_control.backend_registry import BackendRegistry
from ultima_scraper_backend.websocket_actions import (
    WebSocketActionHandler,
    create_websocket_action_handler,
)

from .backend_result import BackendResult
from .content_persistence import ContentPersistenceMixin

logger = logging.getLogger(__name__)

SUPPORTED_SITES = tuple(site.lower() for site in ultima_scraper_api.SUPPORTED_SITES)
_AUTH_EXTRACTORS = {
    "onlyfans": extract_onlyfans_auth,
    # Additional extractors can be registered here as upstream support expands.
}


@dataclass(frozen=True)
class APICommandArg:
    name: str
    label: str
    kind: Literal["string", "int", "float", "bool", "enum"] = "string"
    required: bool = False
    default: Any = None
    is_primary: bool = False
    description: str = ""
    placeholder: Optional[str] = None
    choices: Optional[Sequence[str]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "type": self.kind,
            "required": self.required,
            "default": self.default,
            "is_primary": self.is_primary,
            "description": self.description,
            "placeholder": self.placeholder,
            "choices": list(self.choices) if self.choices else None,
        }

    def parse(self, value: Any) -> Any:
        if value is None:
            if self.required and self.default is None:
                raise ValueError(f"{self.label} is required.")
            return self.default

        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                if self.required and self.default is None:
                    raise ValueError(f"{self.label} is required.")
                return self.default
        else:
            raw = value

        if self.kind == "string":
            return str(raw)
        if self.kind == "int":
            try:
                return int(raw)
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                raise ValueError(f"{self.label} must be an integer.") from exc
        if self.kind == "float":
            try:
                return float(raw)
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                raise ValueError(f"{self.label} must be a number.") from exc
        if self.kind == "bool":
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, (int, float)):
                return bool(raw)
            if isinstance(raw, str):
                lowered = raw.lower()
                if lowered in {"true", "1", "yes", "y"}:
                    return True
                if lowered in {"false", "0", "no", "n"}:
                    return False
            raise ValueError(f"{self.label} must be true or false.")
        if self.kind == "enum":
            if not self.choices:
                raise ValueError(f"{self.label} has no available options.")
            if isinstance(raw, str):
                candidate = raw.strip().lower()
            else:
                candidate = str(raw).strip().lower()
            for choice in self.choices:
                if candidate == choice.lower():
                    return choice
            allowed = ", ".join(self.choices)
            raise ValueError(f"{self.label} must be one of: {allowed}.")
        raise ValueError(f"Unsupported argument type: {self.kind}")


@dataclass(frozen=True)
class APICommandDescriptor:
    name: str
    label: str
    description: str
    handler: str
    args: Sequence[APICommandArg] = ()
    sites: Optional[Sequence[str]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "description": self.description,
            "args": [arg.to_dict() for arg in self.args],
            "sites": list(self.sites) if self.sites else None,
        }

    def allows_site(self, site: str) -> bool:
        if not self.sites:
            return True
        site_key = site.lower()
        return any(site_key == allowed.lower() for allowed in self.sites)


API_COMMANDS: tuple[APICommandDescriptor, ...] = (
    APICommandDescriptor(
        name="get_authed_user",
        label="Account Profile",
        description="Fetch the authenticated account profile for the stored credentials.",
        handler="_cmd_get_authed_user",
        sites=("onlyfans",),
    ),
    APICommandDescriptor(
        name="get_user",
        label="Lookup User",
        description="Fetch a user profile by username or numeric identifier.",
        handler="_cmd_get_user",
        args=(
            APICommandArg(
                name="identifier",
                label="Username or ID",
                required=True,
                is_primary=True,
                placeholder="username or numeric id",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_lists",
        label="Lists",
        description="Retrieve account lists (blacklists, favorites, etc.).",
        handler="_cmd_get_lists",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=20,
                description="Number of list records to fetch (max 100).",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset into the lists collection.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_subscriptions",
        label="Subscriptions",
        description="List subscriptions followed by the authenticated account.",
        handler="_cmd_get_subscriptions",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=25,
                description="Maximum subscriptions to retrieve (max 100).",
            ),
            APICommandArg(
                name="sub_type",
                label="Type",
                kind="enum",
                default="all",
                choices=("all", "active", "expired"),
                description="Subscription category to filter by.",
            ),
            APICommandArg(
                name="filter_by",
                label="Filter",
                kind="string",
                default="",
                description="Optional username or id filter.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_transactions",
        label="Transactions",
        description="Fetch recent transaction activity for the account.",
        handler="_cmd_get_transactions",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=20,
                description="Maximum number of transaction rows to return.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_chats",
        label="Chats",
        description="List all chats/conversations for the authenticated account.",
        handler="_cmd_get_chats",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=100,
                description="Maximum number of chats to retrieve.",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset for pagination.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="search_chats",
        label="Search Chats",
        description="Search chats by username or message text.",
        handler="_cmd_search_chats",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="query",
                label="Query",
                required=True,
                is_primary=True,
                placeholder="username or text fragment",
                description="Search term used to filter chats.",
            ),
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=100,
                description="Maximum number of chats to retrieve.",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset for pagination.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_messages",
        label="Messages",
        description="Retrieve messages from a specific user/chat.",
        handler="_cmd_get_messages",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="user_id",
                label="User ID",
                required=True,
                is_primary=True,
                placeholder="numeric user id or username",
                description="User ID or username to fetch messages from.",
            ),
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=20,
                description="Maximum number of messages to retrieve.",
            ),
            APICommandArg(
                name="offset_id",
                label="Offset Message ID",
                kind="int",
                default=None,
                description="Start pagination after this message id.",
            ),
            APICommandArg(
                name="cutoff_id",
                label="Cutoff Message ID",
                kind="int",
                default=None,
                description="Stop when this message id is reached.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_paid_content",
        label="Paid Content",
        description="Retrieve paid posts and messages (mass messages, PPV, etc).",
        handler="_cmd_get_paid_content",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="performer_id",
                label="Performer ID",
                required=False,
                placeholder="optional performer id or username",
                description="Filter by specific performer (leave empty for all).",
            ),
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=10,
                description="Maximum number of paid content items to fetch.",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset for pagination.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_mass_message_stats",
        label="Mass Message Stats",
        description="Retrieve aggregate mass-message statistics for the authenticated account.",
        handler="_cmd_get_mass_message_stats",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=100,
                description="Maximum number of mass-message stat rows to fetch.",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset for pagination.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="get_subscription_count",
        label="Subscription Count",
        description="Retrieve subscription and subscriber counts broken down by category.",
        handler="_cmd_get_subscription_count",
        sites=("onlyfans",),
    ),
    APICommandDescriptor(
        name="blocked_users",
        label="Blocked Users",
        description="Retrieve blocked users for the authenticated account.",
        handler="_cmd_blocked_users",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=100,
                description="Maximum number of blocked users to retrieve.",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset for pagination.",
            ),
        ),
    ),
    APICommandDescriptor(
        name="restricted_users",
        label="Restricted Users",
        description="Retrieve restricted users for the authenticated account.",
        handler="_cmd_restricted_users",
        sites=("onlyfans",),
        args=(
            APICommandArg(
                name="limit",
                label="Limit",
                kind="int",
                default=100,
                description="Maximum number of restricted users to retrieve.",
            ),
            APICommandArg(
                name="offset",
                label="Offset",
                kind="int",
                default=0,
                description="Offset for pagination.",
            ),
        ),
    ),
)

API_COMMAND_MAP: dict[str, APICommandDescriptor] = {
    descriptor.name: descriptor for descriptor in API_COMMANDS
}


@dataclass(slots=True)
class AccountRecord:
    """Lightweight representation of an account row."""

    id: int
    site: str
    username: Optional[str]
    user_id: Optional[int]
    status: str
    last_tested_at: Optional[Any]
    created_at: Any
    updated_at: Any

    @classmethod
    def from_model(cls, model: UserAuthModel) -> "AccountRecord":
        canonical_id = model.id or model.user_id
        return cls(
            id=canonical_id or 0,
            site=model.site,
            username=model.username,
            user_id=model.user_id,
            status=model.status,
            last_tested_at=model.last_tested_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@dataclass(frozen=True, slots=True)
class AccountRequestContext:
    """Detached credentials for CDN traffic that does not require an API login."""

    cookie: str = field(default="", repr=False)
    authorization: str = field(default="", repr=False)
    user_agent: str = ""
    proxy_url: str | None = field(default=None, repr=False)


_WORKER_API_METHODS: dict[str, frozenset[str]] = {
    "auth": frozenset(
        {
            "get_user",
            "get_authed_user",
            "get_me",
            "get_subscription",
            "get_subscriptions",
            "get_chats",
            "send_message",
            "notification_settings",
            "needs_age_verification",
            "get_age_verification",
            "get_identity_verification",
            "get_transactions",
            "get_lists",
            "get_paid_content",
            "get_mass_message_stats",
            "get_subscription_count",
            "blocked_users",
            "restricted_users",
        }
    ),
    "user": frozenset(
        {
            "get_messages",
            "get_message_by_id",
            "get_post",
            "subscription_price",
            "buy_subscription",
            "get_requester",
            "get_socials",
        }
    ),
    "message": frozenset(
        {"buy_message", "buy_ppv", "like_message"}
    ),
    "post": frozenset({"buy_ppv"}),
    "requester": frozenset({"json_request"}),
    "drm": frozenset({"resolve_drm"}),
    "auth_session": frozenset({"request", "json_request"}),
}


class _WorkerPoolAPIData(dict[str, Any]):
    """Dictionary snapshot that also supports the API models' attr access."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        aliases = {name, _camelize_worker_field(name), _snake_case_worker_field(name)}
        for candidate, value in self.items():
            if candidate in aliases or _snake_case_worker_field(candidate) in aliases:
                return value
        raise AttributeError(name)


def _wrap_worker_api_data(value: Any) -> Any:
    if isinstance(value, list):
        return [_wrap_worker_api_data(item) for item in value]
    if isinstance(value, dict):
        return _WorkerPoolAPIData(
            {key: _wrap_worker_api_data(item) for key, item in value.items()}
        )
    return value


class _WorkerPoolAPIObject:
    """Small API-model facade whose method calls execute as worker jobs.

    Auth and model objects cannot cross the worker process boundary. This facade
    carries only stable identities and serialized fields; each callable method
    becomes an API_COMMAND job against the worker's authenticated-session cache.
    """

    def __init__(
        self,
        manager: "BackendManager",
        site: str,
        auth_id: int | None,
        kind: str,
        *,
        identifier: int | str | None = None,
        user_id: int | str | None = None,
        snapshot: dict[str, Any] | None = None,
        auth_override: Mapping[str, Any] | None = None,
        guest: bool = False,
    ) -> None:
        self._manager = manager
        self._site = site
        self._auth_id = auth_id
        self._kind = kind
        self._identifier = identifier
        self._user_id = user_id
        self._snapshot = snapshot or {}
        self._auth_override = dict(auth_override) if auth_override is not None else None
        self._guest = guest

    def __bool__(self) -> bool:
        if self._kind == "message":
            return self._value("id") not in (None, 0, "")
        return self._identifier not in (None, "") or bool(self._snapshot)

    def __repr__(self) -> str:
        return (
            f"WorkerPoolAPIObject(kind={self._kind!r}, "
            f"identifier={self._identifier!r}, site={self._site!r})"
        )

    def __getitem__(self, key: str) -> Any:
        value = self._value(key)
        if value is None and not self._has_snapshot_value(key):
            raise KeyError(key)
        return value

    def get(self, key: str, default: Any = None) -> Any:
        value = self._value(key)
        return default if value is None and not self._has_snapshot_value(key) else value

    @property
    def __raw__(self) -> dict[str, Any]:
        raw = self._snapshot.get("__raw__")
        return raw if isinstance(raw, dict) else self._snapshot

    def model_dump(self) -> dict[str, Any]:
        return dict(self.__raw__)

    def dict(self) -> dict[str, Any]:
        return self.model_dump()

    def _value(self, name: str) -> Any:
        if name == "id" and self._identifier is not None:
            return self._identifier
        if name == "site" and self._kind == "auth":
            return self._site
        if name == "username" and self._kind == "auth":
            return self._snapshot.get("username")

        aliases = {name, _camelize_worker_field(name), _snake_case_worker_field(name)}
        for candidate, value in self._snapshot.items():
            if candidate in aliases:
                return self._wrap_attribute(candidate, value)
            if _snake_case_worker_field(candidate) in aliases:
                return self._wrap_attribute(candidate, value)
        return None

    def _has_snapshot_value(self, name: str) -> bool:
        aliases = {name, _camelize_worker_field(name), _snake_case_worker_field(name)}
        return any(
            candidate in aliases or _snake_case_worker_field(candidate) in aliases
            for candidate in self._snapshot
        )

    def _wrap_attribute(self, name: str, value: Any) -> Any:
        if not isinstance(value, dict):
            if isinstance(value, list):
                return [self._wrap_attribute(name, item) for item in value]
            return value

        normalized_name = _snake_case_worker_field(name)
        if normalized_name in {"user", "author", "from_user", "creator"}:
            return self._manager._worker_api_object(
                self._site,
                self._auth_id,
                "user",
                identifier=value.get("id") or value.get("user_id"),
                snapshot=value,
                auth_override=self._auth_override,
                guest=self._guest,
            )
        if normalized_name in {"last_message", "message"}:
            owner_id = self._user_id
            if owner_id is None:
                user = self._snapshot.get("user") or self._snapshot.get("withUser")
                if isinstance(user, dict):
                    owner_id = user.get("id") or user.get("user_id")
            return self._manager._worker_api_object(
                self._site,
                self._auth_id,
                "message",
                identifier=value.get("id"),
                user_id=owner_id,
                snapshot=value,
                auth_override=self._auth_override,
                guest=self._guest,
            )
        if normalized_name in {
            "medias",
            "media",
            "files",
            "drm",
            "full",
            "thumb",
            "preview",
            "square_preview",
            "dash",
            "hls",
        }:
            return _wrap_worker_api_data(value)
        return value

    def _target(self) -> dict[str, Any]:
        target: dict[str, Any] = {"kind": self._kind}
        if self._user_id is not None:
            target["user_id"] = self._user_id
        if self._kind == "user" and self._identifier is not None:
            target["user_id"] = self._identifier
        if self._kind in {"message", "post"} and self._identifier is not None:
            target[f"{self._kind}_id"] = self._identifier
        return target

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        value = self._value(name)
        if value is not None or self._has_snapshot_value(name):
            return value

        if self._kind == "auth":
            if name == "is_authed":
                return lambda: True
            if name == "find_user":
                return lambda identifier: self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "user",
                    identifier=identifier,
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
            if name == "resolve_user":
                return lambda data: self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "user",
                    identifier=(data.get("id") if isinstance(data, dict) else None),
                    snapshot=(data if isinstance(data, dict) else None),
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
            if name == "get_requester":
                return lambda: self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "requester",
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
            if name == "drm":
                return self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "drm",
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
            if name == "auth_session":
                return self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "auth_session",
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
        elif self._kind == "user":
            if name == "get_authed":
                return lambda: self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "auth",
                    identifier=self._auth_id,
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
            if name == "get_requester":
                return lambda: self._manager._worker_api_object(
                    self._site,
                    self._auth_id,
                    "requester",
                    user_id=self._identifier,
                    auth_override=self._auth_override,
                    guest=self._guest,
                )
        elif self._kind == "message" and name == "get_scrape_job_params":
            author = self._snapshot.get("fromUser") or self._snapshot.get("author")
            author_username = (
                author.get("username")
                if isinstance(author, dict)
                else None
            )
            return lambda: {
                "site": self._site,
                "auth_id": self._auth_id,
                "user_id": self._user_id,
                "username": author_username
                or self._value("username")
                or self._value("name"),
            }

        if name not in _WORKER_API_METHODS.get(self._kind, frozenset()):
            raise AttributeError(name)

        async def invoke(*args: Any, **kwargs: Any) -> Any:
            return await self._manager._invoke_worker_api_method(
                site=self._site,
                auth_id=self._auth_id,
                target=self._target(),
                method=name,
                args=args,
                kwargs=kwargs,
                auth_override=self._auth_override,
                guest=self._guest,
            )

        return invoke


class _WorkerPoolAPIResponseContent:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def iter_chunked(self, size: int):
        chunk_size = max(1, int(size))
        for offset in range(0, len(self._body), chunk_size):
            yield self._body[offset : offset + chunk_size]

    async def read(self) -> bytes:
        return self._body


class _WorkerPoolAPIResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        encoded_body = payload.get("body_base64") or ""
        self.status = int(payload.get("status") or 0)
        self.headers = dict(payload.get("headers") or {})
        self.content_type = str(payload.get("content_type") or "")
        self.content = _WorkerPoolAPIResponseContent(
            base64.b64decode(encoded_body) if encoded_body else b""
        )

    async def read(self) -> bytes:
        return self.content._body

    async def text(
        self,
        encoding: str | None = None,
        errors: str = "replace",
    ) -> str:
        return self.content._body.decode(encoding or "utf-8", errors=errors)


def _camelize_worker_field(value: str) -> str:
    parts = value.split("_")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


def _snake_case_worker_field(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


class BackendManager(ContentPersistenceMixin):
    """Coordinates lightweight auth storage and upstream API interactions."""

    def __init__(
        self,
        db_path: Optional[Path | str] = None,
        *,
        auto_create: bool = False,
        enable_redis: bool = True,
        config_path: Any = None,
    ) -> None:
        self.config = UltimaScraperCollectionConfig.load_or_create_default_config()

        # Legacy sqlite path retained only for compatibility with older worker
        # interfaces that still pass a path around. Global auth/account storage is
        # PostgreSQL-backed and does not use this file.
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        self.db_path = Path(db_path)

        self.engine = get_global_engine()
        self.multi_site_db_manager = MultiSiteDatabaseManager()
        self.db_managers: dict[str, BaseDatabaseManager] = {}
        for site in SUPPORTED_SITES:
            site_manager = self.multi_site_db_manager.get_or_create_manager(
                site,
                auto_connect=False,
                auto_create=False,
            )
            try:
                site_manager.connect()
            except Exception as exc:
                logger.warning(
                    "Database manager for %s is unavailable during startup compatibility "
                    "initialization; continuing with lazy engine access: %s",
                    site,
                    exc,
                )
            self.db_managers[site] = site_manager

        if auto_create:
            logger.warning(
                "BackendManager auto_create is ignored; PostgreSQL/Alembic is authoritative"
            )

        self._lock = asyncio.Lock()
        self._api_command_map = API_COMMAND_MAP

        # Redis distributed system components
        self._enable_redis = enable_redis
        self._redis_manager: Optional[RedisManager] = None
        self._backend_registry: Optional[BackendRegistry] = None
        self._redis_initialized = False
        self._owns_redis_manager = False
        self._redis_init_lock: Optional[asyncio.Lock] = None
        self._live_apis: list[api_types] = []

        # Auth event publisher for worker synchronization
        self._auth_event_publisher: Optional[Any] = None  # AuthEventPublisher

        # WebSocket action handler
        self._ws_action_handler: Optional[WebSocketActionHandler] = None
        self.ultima_scraper_api: Optional[ultima_scraper_api.UltimaScraperAPI] = None

        # Process-based scraping components
        self._process_manager: Optional[Any] = (
            None  # ProcessManager from worker.scraper_process
        )

        # Task-based job system (unified)
        self._job_storage: Optional[Any] = None  # JobStorage from job_system
        self._job_manager: Optional[Any] = None  # JobManager from job_system
        self._backend_router: Optional[Any] = None  # BackendRouter from job_system
        self._use_worker_pool: bool = True  # Prefer worker pool over process spawning

    async def initialize_redis(self) -> None:
        """Initialize Redis connection and register backend with the distributed system."""
        if not self._enable_redis or self._redis_initialized:
            return

        if self._redis_init_lock is None:
            self._redis_init_lock = asyncio.Lock()

        async with self._redis_init_lock:
            if not self._enable_redis or self._redis_initialized:
                return

            try:
                from ultima_scraper_api.managers.redis import attach_redis, get_redis

                attached = get_redis()
                if attached is not None and getattr(attached, "is_connected", False):
                    self._redis_manager = cast(Any, attached)
                    self._owns_redis_manager = False
                    logger.info("Reusing existing connected Redis manager for distributed system")
                else:
                    # Connect to Redis using config
                    self._redis_manager = RedisManager(config=self.config.settings.redis)
                    await self._redis_manager.connect()
                    attach_redis(self._redis_manager)
                    self._owns_redis_manager = True
                    logger.info("Connected to Redis for distributed system")

                # Register this backend
                from .cluster import runtime_version

                self._backend_registry = BackendRegistry(
                    redis_manager=self._redis_manager,
                    max_workers=getattr(self.config.settings.worker_pool, "size", 10),
                    version=runtime_version(),
                    capabilities=["scraping", "downloading", "api_commands"],
                )
                await self._backend_registry.register()
                logger.info("Backend registered: %s", self._backend_registry.backend_id)

                # Start heartbeat to keep backend alive
                await self._backend_registry.start_heartbeat()
                logger.info("Backend heartbeat started")

                # Initialize WebSocket action handler if API instances are available
                if self.ultima_scraper_api is not None:
                    try:
                        self._ws_action_handler = await create_websocket_action_handler(self)
                        logger.info("WebSocket action handler initialized")
                    except Exception as exc:
                        logger.warning("Could not initialize WebSocket action handler: %s", exc)

                # Initialize auth event publisher for worker synchronization
                try:
                    from ultima_scraper_backend.auth_events import AuthEventPublisher

                    self._auth_event_publisher = AuthEventPublisher(
                        cast(Any, self._redis_manager)
                    )
                    logger.info("Auth event publisher initialized")
                except Exception as exc:
                    logger.warning("Could not initialize auth event publisher: %s", exc)

                # Initialize task-based job system
                try:
                    from ultima_scraper_backend.job_system.manager import JobManager
                    from ultima_scraper_backend.job_system.routing import (
                        BackendRouter,
                    )
                    from ultima_scraper_backend.job_system.storage import JobStorage

                    self._job_storage = JobStorage(cast(Any, self._redis_manager))
                    self._job_manager = JobManager(cast(Any, self._redis_manager))
                    self._backend_router = BackendRouter(cast(Any, self._redis_manager))
                    logger.info("Task-based job system initialized")
                except Exception as exc:
                    logger.warning("Could not initialize job system: %s", exc)
                    self._use_worker_pool = False

                self._redis_initialized = True

            except Exception as exc:
                logger.warning("Failed to initialize Redis: %s", exc, exc_info=True)
                if self._redis_manager is not None and self._owns_redis_manager:
                    from ultima_scraper_api.managers.redis import detach_redis

                    detach_redis(self._redis_manager)
                    with contextlib.suppress(Exception):
                        await self._redis_manager.disconnect()
                self._redis_initialized = False
                self._redis_manager = None
                self._backend_registry = None

    async def shutdown_redis(self) -> None:
        """Shutdown Redis connection and cleanup."""
        if not self._redis_initialized:
            return

        try:
            if self._backend_registry:
                await self._backend_registry.stop_heartbeat()
                await self._backend_registry.unregister()
                logger.info("Backend unregistered from distributed system")

            if self._redis_manager:
                if self._owns_redis_manager:
                    from ultima_scraper_api.managers.redis import detach_redis

                    detach_redis(self._redis_manager)
                    await self._redis_manager.disconnect()
                    logger.info("Disconnected from Redis")
                else:
                    logger.info("Preserving shared Redis connection during manager shutdown")

        except Exception as exc:
            logger.warning("Error during Redis shutdown: %s", exc, exc_info=True)
        finally:
            self._redis_initialized = False
            self._redis_manager = None
            self._backend_registry = None
            self._owns_redis_manager = False

    @property
    def supported_sites(self) -> tuple[str, ...]:
        """Sites supported by the embedded ultima_scraper_api version."""

        return SUPPORTED_SITES

    def save_config(self) -> None:
        """Persist the loaded revision to PostgreSQL."""
        from .config_settings_sync import persist_config_snapshot

        persist_config_snapshot(self.config, expected_version=self.config._settings_version)

    # ------------------------------------------------------------------
    # Account management helpers
    # ------------------------------------------------------------------
    @contextlib.contextmanager
    def _site_session(self, site: str):
        """Create a session with ``search_path`` scoped to *site*.

        All site-specific tables (``users``, ``user_auths``, …) live in a
        PostgreSQL schema named after the site (e.g. ``onlyfans``).
        Setting ``search_path`` ensures unqualified table names resolve
        into the correct schema.
        """
        schema = site.lower()
        if not schema.isidentifier():
            raise ValueError(f"Invalid site schema: {site!r}")

        # Transaction-pooled PgBouncer can hand each transaction a different
        # server connection, and a session-level SET would leak to other
        # clients; set search_path for every transaction this session begins.
        def scope(_session, _transaction, connection):
            connection.exec_driver_sql(f"SET LOCAL search_path TO {schema}, public")

        with Session(self.engine) as session:
            sa_event.listen(session, "after_begin", scope)
            yield session

    @property
    def redis(self) -> RedisManager | None:
        return self._redis_manager

    def get_redis(self) -> RedisManager | None:
        """Return the active Redis manager."""
        return self._redis_manager

    def get_site_db_manager(
        self,
        site: str,
        *,
        auto_connect: bool = True,
    ) -> BaseDatabaseManager:
        site_key = site.strip().lower()
        if site_key in self.db_managers:
            return self.db_managers[site_key]

        manager = self.multi_site_db_manager.get_or_create_manager(
            site_key,
            auto_connect=auto_connect,
        )
        self.db_managers[site_key] = manager
        return manager

    async def get_authed_session(
        self,
        site: str,
        account_id: int | None = None,
        *,
        use_db_init: bool = True,
    ) -> Any | None:
        site_key = site.strip().lower()
        if site_key == "onlyfans":
            account = self._find_account(site_key, account_id)
            if account is None:
                return None
            canonical_auth_id = account.user_id or account.id
            if not canonical_auth_id:
                return None
            return self._worker_api_object(
                site_key,
                int(canonical_auth_id),
                "auth",
                identifier=int(canonical_auth_id),
                snapshot={"id": int(canonical_auth_id), "username": account.username},
            )

        login_result = await self._login_with_account(
            site_key,
            account_id=account_id,
            use_db_init=use_db_init,
        )
        if isinstance(login_result, BackendResult):
            return None

        api, authed = login_result
        self._live_apis.append(api)
        return authed

    async def get_all_authed_sessions(self, site: str) -> list[Any]:
        sessions: list[Any] = []
        for account in self.list_accounts(site):
            auth = await self.get_authed_session(site, account.id)
            if auth is not None:
                sessions.append(auth)
        return sessions

    def _worker_api_object(
        self,
        site: str,
        auth_id: int | None,
        kind: str,
        *,
        identifier: int | str | None = None,
        user_id: int | str | None = None,
        snapshot: dict[str, Any] | None = None,
        auth_override: Mapping[str, Any] | None = None,
        guest: bool = False,
    ) -> _WorkerPoolAPIObject:
        return _WorkerPoolAPIObject(
            self,
            site,
            auth_id,
            kind,
            identifier=identifier,
            user_id=user_id,
            snapshot=snapshot,
            auth_override=auth_override,
            guest=guest,
        )

    @staticmethod
    def _serialize_worker_api_argument(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bytes):
            return {
                "__worker_bytes__": base64.b64encode(value).decode("ascii")
            }
        if isinstance(value, Mapping):
            return {
                str(key): BackendManager._serialize_worker_api_argument(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set, frozenset)):
            return [BackendManager._serialize_worker_api_argument(item) for item in value]
        if type(value).__name__ == "DRMMedia":
            payload = {
                field_name: getattr(value, field_name, None)
                for field_name in (
                    "media_id",
                    "manifest_url",
                    "key_pair_id",
                    "policy",
                    "signature",
                    "content_id",
                    "video_url",
                    "audio_url",
                    "video_size",
                    "audio_size",
                    "content_type",
                )
            }
            return {
                "__worker_type__": "DRMMedia",
                "value": BackendManager._serialize_worker_api_argument(payload),
            }
        if dataclasses.is_dataclass(value):
            payload = dataclasses.asdict(value)
            return BackendManager._serialize_worker_api_argument(payload)
        if isinstance(value, datetime):
            return value.isoformat()
        raw = getattr(value, "__raw__", None)
        if isinstance(raw, dict):
            return BackendManager._serialize_worker_api_argument(raw)
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            return BackendManager._serialize_worker_api_argument(model_dump())
        raise TypeError(
            f"Cannot pass {type(value).__name__} to a worker API operation."
        )

    async def _invoke_worker_api_method(
        self,
        *,
        site: str,
        auth_id: int | None,
        target: dict[str, Any],
        method: str,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
        auth_override: Mapping[str, Any] | None = None,
        guest: bool = False,
        timeout_seconds: float = 120.0,
    ) -> Any:
        operation = {
            "target": target,
            "method": method,
            "args": self._serialize_worker_api_argument(list(args)),
            "kwargs": self._serialize_worker_api_argument(dict(kwargs)),
        }
        result = await self._submit_worker_api_command(
            site,
            "invoke_method",
            operation,
            auth_id,
            timeout_seconds=timeout_seconds,
            auth_override=auth_override,
            guest=guest,
        )
        if not result.success:
            raise RuntimeError(result.message or "Worker API operation failed")
        payload = result.payload or {}
        value = payload.get("value")
        if payload.get("response") is True and isinstance(value, dict):
            return _WorkerPoolAPIResponse(value)
        kind = payload.get("kind")
        return self._wrap_worker_api_result(
            site,
            auth_id,
            str(kind) if kind else None,
            value,
            target=target,
            auth_override=auth_override,
            guest=guest,
        )

    async def execute_worker_api_method(
        self,
        *,
        site: str,
        account_id: int | None,
        method: str,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        target: Mapping[str, Any] | None = None,
        auth_override: Mapping[str, Any] | None = None,
        guest: bool = False,
        timeout_seconds: float = 120.0,
    ) -> Any:
        """Execute one explicit API-model method inside a worker-pool job.

        ``auth_override`` supports credential edits that must be validated before
        they are persisted. It is sent only in this task payload and used by the
        worker to create a short-lived authenticated session for the operation.
        """

        resolved_target = dict(target or {"kind": "auth"})
        return await self._invoke_worker_api_method(
            site=site,
            auth_id=account_id,
            target=resolved_target,
            method=method,
            args=args,
            kwargs=dict(kwargs or {}),
            auth_override=auth_override,
            guest=guest,
            timeout_seconds=timeout_seconds,
        )

    def _wrap_worker_api_result(
        self,
        site: str,
        auth_id: int | None,
        kind: str | None,
        value: Any,
        *,
        target: Mapping[str, Any],
        auth_override: Mapping[str, Any] | None = None,
        guest: bool = False,
    ) -> Any:
        if value is None or kind is None:
            return value
        if isinstance(value, list):
            return [
                self._wrap_worker_api_result(
                    site,
                    auth_id,
                    kind,
                    item,
                    target=target,
                    auth_override=auth_override,
                    guest=guest,
                )
                for item in value
            ]
        if not isinstance(value, dict):
            return value

        identifier = value.get("id") or value.get("user_id")
        user_id = target.get("user_id")
        if kind == "user" and identifier is None:
            identifier = value.get("username")
        if kind in {"message", "post"} and user_id is None:
            user_id = value.get("user_id")
        return self._worker_api_object(
            site,
            auth_id,
            kind,
            identifier=identifier,
            user_id=user_id,
            snapshot=value,
            auth_override=auth_override,
            guest=guest,
        )

    async def close_live_sessions(self) -> None:
        while self._live_apis:
            api = self._live_apis.pop()
            with contextlib.suppress(Exception):
                await api.close_pools()

    def list_accounts(self, site: Optional[str] = None) -> list[AccountRecord]:
        sites = [site.lower()] if site else list(SUPPORTED_SITES)
        all_accounts: list[UserAuthModel] = []
        for s in sites:
            try:
                with self._site_session(s) as session:
                    statement = (
                        select(UserAuthModel)
                        .options(
                            load_only(
                                UserAuthModel.id,
                                UserAuthModel.site,
                                UserAuthModel.username,
                                UserAuthModel.user_id,
                                UserAuthModel.status,
                                UserAuthModel.last_tested_at,
                                UserAuthModel.created_at,
                                UserAuthModel.updated_at,
                            )
                        )
                        .where(UserAuthModel.site == s)
                    )
                    all_accounts.extend(session.exec(statement))
            except Exception:
                logger.debug("No user_auths table in schema %r – skipping", s)
        all_accounts.sort(
            key=lambda item: item.updated_at or item.created_at, reverse=True
        )
        return [AccountRecord.from_model(account) for account in all_accounts]

    def _find_account(
        self, site: str, account_id: Optional[int] = None
    ) -> Optional[UserAuthModel]:
        with self._site_session(site) as session:
            statement = select(UserAuthModel).where(UserAuthModel.site == site.lower())
            if account_id is not None:
                # Most user-facing auth selectors use the auth account's user_id,
                # while some legacy flows still pass the internal auth row id.
                statement = statement.where(
                    (UserAuthModel.user_id == account_id)
                    | (UserAuthModel.id == account_id)
                )
            accounts = list(session.exec(statement))
            accounts.sort(
                key=lambda item: item.updated_at or item.created_at, reverse=True
            )
            account = accounts[0] if accounts else None
            if account:
                session.expunge(account)
            return account

    def get_account_request_context(
        self,
        site: str,
        account_id: int | None = None,
    ) -> AccountRequestContext | None:
        """Return stored CDN credentials without opening a site API session.

        Media URLs and CloudFront thumbnails only need the persisted headers
        and assigned proxy. They must not call ``_login_with_account`` because
        API authentication belongs to worker-pool jobs.
        """

        site_key = site.strip().lower()
        account = self._find_account(site_key, account_id)
        if account is None:
            return None

        payload = self._build_auth_payload(account, site_key)
        return AccountRequestContext(
            cookie=str(payload.get("cookie") or ""),
            authorization=str(payload.get("authorization") or ""),
            user_agent=str(payload.get("user_agent") or ""),
            proxy_url=(str(payload["proxy_url"]) if payload.get("proxy_url") else None),
        )

    @staticmethod
    def _build_auth_payload(account: UserAuthModel, site: str) -> dict[str, Any]:
        """
        Construct a site-specific auth_json payload expected by ultima_scraper_api.

        Different sites require different authentication fields:
        - OnlyFans/LoyalFans: cookie, x_bc
        - Fansly: authorization (NOT cookie/x_bc)
        """

        account_identifier = account.user_id or account.id
        if not account_identifier:
            raise ValueError("Account is missing a valid identifier for login")

        site_key = (site or account.site or "").lower()

        # Base fields common to all sites
        payload: dict[str, Any] = {
            "id": account_identifier,
            "username": account.username or "",
            "user_agent": account.user_agent or "",
            "email": account.email or "",
            "password": account.password or "",
        }

        # Add site-specific authentication fields
        if site_key == "onlyfans" or site_key == "loyalfans":
            # OnlyFans and LoyalFans use cookie + x_bc
            payload["cookie"] = account.cookie or ""
            payload["x_bc"] = account.x_bc or ""
        elif site_key == "fansly":
            # Fansly uses authorization token
            payload["authorization"] = account.authorization or ""
        else:
            # Default to OnlyFans-style for unknown sites
            payload["cookie"] = account.cookie or ""
            payload["x_bc"] = account.x_bc or ""

        from ultima_scraper_backend.usapi_initializer import (
            apply_assigned_proxy_to_auth_json,
        )

        return apply_assigned_proxy_to_auth_json(payload, account.proxy_id)

    @staticmethod
    def _normalise_identifier(value: Optional[Any]) -> Optional[int]:
        """Normalize user identifier to integer.

        Args:
            value: User ID as int, numeric string, or None

        Returns:
            Integer ID if valid, None otherwise

        Example:
            >>> BackendManager._normalise_identifier("12345")
            12345
            >>> BackendManager._normalise_identifier("username")
            None
        """
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def add_account_from_curl(self, site: str, curl: str) -> BackendResult:
        site_key = site.lower()
        if site_key not in SUPPORTED_SITES:
            return BackendResult(
                False, f"Site '{site}' is not supported by ultima_scraper_api."
            )

        extractor = _AUTH_EXTRACTORS.get(site_key)
        if extractor is None:
            return BackendResult(
                False,
                f"Curl import has not been implemented for '{site_key}'.",
            )

        try:
            auth_details = extractor(curl)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to parse curl for site=%s", site_key)
            return BackendResult(False, f"Failed to parse curl string: {exc}")

        auth_export = auth_details.export()
        username = auth_export.get("username") or None
        user_id = self._normalise_identifier(auth_export.get("id"))
        auth_override = dict(auth_export)
        auth_override.setdefault("id", user_id or 0)

        try:
            authed_user = await self.execute_worker_api_method(
                site=site_key,
                account_id=None,
                method="get_authed_user",
                auth_override=auth_override,
            )
        except Exception as exc:  # pragma: no cover - network errors
            logger.exception("Worker login attempt threw for site=%s", site_key)
            return BackendResult(False, f"Login attempt failed: {exc}")

        if authed_user is None:
            return BackendResult(
                False, "Upstream API did not return an authenticated session."
            )

        username_from_auth = getattr(authed_user, "username", None)
        if isinstance(username_from_auth, str) and username_from_auth:
            username = username_from_auth

        if user_id is None:
            user_id = self._normalise_identifier(getattr(authed_user, "id", None))

        if user_id is None:
            return BackendResult(
                False,
                "Authenticated session did not expose an account id; cannot persist credentials.",
            )

        record = await self._store_account(site_key, username, user_id, auth_export)

        from ultima_scraper_backend.database.helpers.user_sync import (
            sync_authed_user_from_api,
        )

        with contextlib.suppress(Exception):
            site_db = self.get_site_db_manager(site_key)
            stored_auth = await self.get_authed_session(site_key, user_id)
            if stored_auth is not None:
                await sync_authed_user_from_api(site_db, stored_auth, site=site_key)

        # Publish auth_added event to notify workers
        if self._auth_event_publisher:
            try:
                await self._auth_event_publisher.publish_auth_added(
                    auth_id=user_id,
                    username=record.username or username,
                    site=site_key,
                )
            except Exception as exc:
                logger.warning(f"Failed to publish auth_added event: {exc}")

        # Prefer the stored record username if available; fall back to extracted username,
        # then user_id, and finally a readable 'unknown' placeholder to avoid 'None'.
        display_name = record.username or username or str(user_id) or "unknown"
        message = f"{site_key.title()} account '{display_name}' authenticated successfully."
        payload: dict[str, Any] = {"account": record, "auth": auth_export}
        return BackendResult(True, message, payload)

    async def get_user(self, site: str, identifier: int | str) -> BackendResult:
        site_key = site.strip().lower()
        if not site_key:
            return BackendResult(False, "Choose a site before requesting a user.")
        identifier_value = (
            identifier.strip() if isinstance(identifier, str) else str(identifier)
        )
        if not identifier_value:
            return BackendResult(False, "Identifier is required for get_user.")
        return await self.run_api_command(
            site_key,
            "get_user",
            {"identifier": identifier_value},
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _store_account(
        self,
        site: str,
        username: Optional[str],
        user_id: Optional[int],
        auth_export: dict[str, Any],
    ) -> AccountRecord:
        async with self._lock:
            with self._site_session(site) as session:
                # Ensure user_id is not None for new accounts
                if user_id is None:
                    raise ValueError("user_id is required for storing accounts")

                # First, ensure the user exists in the users table
                user = session.exec(
                    select(UserModel).where(UserModel.id == user_id)
                ).first()

                if user is None:
                    # Create new user record
                    user = UserModel(
                        id=user_id,
                        username=username,
                        active=True,
                    )
                    session.add(user)
                else:
                    # Update username if provided and different
                    if username and user.username != username:
                        user.username = username
                        user.touch()
                        session.add(user)

                # Now handle the auth record
                account: Optional[UserAuthModel] = None
                account = session.exec(
                    select(UserAuthModel)
                    .where(UserAuthModel.site == site)
                    .where(UserAuthModel.user_id == user_id)
                ).first()

                if account is None and username:
                    # Also try to find by username
                    account = session.exec(
                        select(UserAuthModel)
                        .where(UserAuthModel.site == site)
                        .where(UserAuthModel.username == username)
                    ).first()

                if account:
                    # Update existing account with new auth details
                    account.cookie = auth_export.get("cookie")
                    account.x_bc = auth_export.get("x_bc")
                    account.authorization = auth_export.get("authorization")
                    account.user_agent = auth_export.get("user_agent", "")
                    account.email = auth_export.get("email")
                    account.password = auth_export.get("password")
                    account.username = username or account.username
                    account.user_id = user_id
                    account.id = user_id
                    account.status = "ok"
                    account.last_tested_at = utcnow()
                    account.touch()
                    session.add(account)
                else:
                    # Create new auth record
                    account = UserAuthModel(
                        id=user_id,
                        site=site,
                        username=username,
                        user_id=user_id,
                        cookie=auth_export.get("cookie"),
                        x_bc=auth_export.get("x_bc"),
                        authorization=auth_export.get("authorization"),
                        user_agent=auth_export.get("user_agent", ""),
                        email=auth_export.get("email"),
                        password=auth_export.get("password"),
                        status="ok",
                        last_tested_at=utcnow(),
                    )
                    session.add(account)

                session.commit()
                session.refresh(account)
                return AccountRecord.from_model(account)

    async def migrate_account_ids(self) -> BackendResult:
        """Align stored account primary keys with their upstream user identifiers."""

        updated = 0
        skipped = 0
        missing = 0

        async with self._lock:
            for _site in SUPPORTED_SITES:
                try:
                    with self._site_session(_site) as session:
                        accounts = list(session.exec(select(UserAuthModel)))
                        for account in accounts:
                            target = self._normalise_identifier(account.user_id)
                            if target is None:
                                target = self._normalise_identifier(account.id)
                            if target is None:
                                missing += 1
                                continue

                            if account.id == target:
                                if account.user_id != target:
                                    account.user_id = target
                                    account.touch()
                                    session.add(account)
                                    updated += 1
                                continue

                            conflict = session.exec(
                                select(UserAuthModel)
                                .where(UserAuthModel.site == account.site)
                                .where(UserAuthModel.id == target)
                            ).first()
                            if conflict is not None and conflict is not account:
                                skipped += 1
                                continue

                            account.id = target
                            account.user_id = target
                            account.touch()
                            session.add(account)
                            updated += 1

                        session.commit()
                except Exception:
                    logger.debug("migrate_account_ids: skipping schema %r", _site)

        message = (
            "Account ID migration complete: "
            f"{updated} updated, {skipped} skipped, {missing} missing identifiers."
        )
        payload: dict[str, Any] = {
            "updated": updated,
            "skipped": skipped,
            "missing": missing,
            "total": updated + skipped + missing,
        }
        return BackendResult(True, message, payload)

    async def _fetch_user_payload(
        self, authed: auth_types, identifier: str
    ) -> tuple[bool, str, Optional[dict[str, Any]]]:
        try:
            lookup = int(identifier)
        except ValueError:
            lookup = identifier

        get_user = getattr(authed, "get_user", None)
        if not callable(get_user):
            return False, "Authenticated session does not support get_user.", None
        try:
            user = await cast(
                Callable[[int | str], Awaitable[UserLike | None]], get_user
            )(lookup)
        except Exception as exc:  # pragma: no cover
            logger.exception("get_user failed for identifier=%s", identifier)
            return False, f"get_user failed: {exc}", None

        if user is None:
            return False, "User not found.", None

        summary = summarize_user(user)
        return True, "User fetched successfully.", summary

    @staticmethod
    def _serialize_datetime(value: datetime | str | None) -> Optional[str]:
        """Convert datetime to ISO format string."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat()
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _summarise_subscription(subscription: SubscriptionLike) -> dict[str, Any]:
        """
        Extract subscription summary information.

        Uses SubscriptionLike protocol for better type safety across different APIs.
        """
        user = subscription.user
        expires = subscription.subscribed_by_expire_date
        is_active = False
        try:
            is_active = bool(subscription.is_active())
        except Exception:  # pragma: no cover - defensive fallback
            # If is_active() fails, try subscribed_is_expired_now attribute
            is_active = not getattr(subscription, "subscribed_is_expired_now", False)

        return {
            "user_id": user.id,
            "username": user.username,
            "name": getattr(user, "name", None),  # name not in UserLike protocol
            "price": getattr(
                subscription, "subscribe_price", None
            ),  # Optional attribute
            "current_price": getattr(
                subscription, "current_subscribe_price", None
            ),  # Optional
            "renewal": getattr(
                subscription, "subscribed_by_autoprolong", None
            ),  # Optional
            "expires_at": BackendManager._serialize_datetime(expires),
            "is_active": is_active,
        }

    @staticmethod
    def _extract_content_dict(
        item: Any,
        content_type: Literal["post", "message", "paid"] = "post",
    ) -> dict[str, Any]:
        """
        Extract common fields from a content item (post, message, paid content).

        Reduces code duplication across different content type handlers.
        Handles both dict objects and API model objects.

        Args:
            item: Content item (post, message, or paid content object)
            content_type: Type of content for specialized field handling

        Returns:
            Dictionary with standardized content fields
        """
        # If already a dict, return as-is
        if isinstance(item, dict):
            item_dict = cast(dict[object, Any], item)
            return {str(key): value for key, value in item_dict.items()}

        # Extract common fields
        result: dict[str, Any] = {
            "id": getattr(item, "id", None),
            "text": getattr(item, "text", ""),
            "price": getattr(item, "price", None),
            "created_at": BackendManager._serialize_datetime(
                getattr(item, "created_at", None)
            ),
        }

        # Extract author/sender based on content type
        if hasattr(item, "author"):
            author = item.author
            result["author"] = {
                "id": getattr(author, "id", None),
                "username": getattr(author, "username", None),
                "name": getattr(author, "name", None),
            }
        elif hasattr(item, "fromUser"):  # Fansly messages
            from_user = item.fromUser
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
                else "message"
                if hasattr(item, "fromUser")
                else "post"
            )

        return result

    @staticmethod
    def _serialize_api_value(value: Any) -> Any:
        """Best-effort conversion for API model objects used in backend responses."""

        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [BackendManager._serialize_api_value(item) for item in value]
        if isinstance(value, dict):
            value_dict = cast(dict[object, Any], value)
            return {
                str(key): BackendManager._serialize_api_value(item)
                for key, item in value_dict.items()
            }

        raw_value = cast(Any, getattr(value, "__raw__", None))
        if isinstance(raw_value, dict):
            return BackendManager._serialize_api_value(raw_value)

        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            with contextlib.suppress(Exception):
                return BackendManager._serialize_api_value(model_dump())

        as_dict = getattr(value, "dict", None)
        if callable(as_dict):
            with contextlib.suppress(Exception):
                return BackendManager._serialize_api_value(as_dict())

        return value

    async def _login_with_account(
        self, site_key: str, account_id: Optional[int] = None, use_db_init: bool = True
    ) -> tuple[api_types, auth_types] | BackendResult:
        """
        Authenticate legacy non-OnlyFans callers with stored account credentials.

        OnlyFans authentication is handled exclusively by worker-pool jobs.
        """
        if site_key.strip().lower() == "onlyfans":
            logger.warning(
                "Manager._login_with_account is deprecated for onlyfans. "
                "Use worker pool jobs instead for all API operations."
            )
            return BackendResult(
                False,
                "OnlyFans API authentication must run through a worker-pool job.",
            )

        account = self._find_account(site_key, account_id)
        if account is None:
            return BackendResult(False, f"No stored credentials for '{site_key}'.")

        # Build auth_json matching AuthDetails structure
        auth_json = self._build_auth_payload(account, site_key)

        api = ultima_scraper_api.select_api(site_key, USAPIConfig())

        try:
            # Simple login without WebSocket listeners (temporary connection only)
            authed = await api.login(auth_json=auth_json)

        except Exception as exc:  # pragma: no cover - network/auth errors
            logger.exception(
                "Login with stored credentials failed for site=%s", site_key
            )
            with contextlib.suppress(Exception):
                await api.close_pools()
            return BackendResult(False, f"Login with stored credentials failed: {exc}")

        if authed is None:
            with contextlib.suppress(Exception):
                await api.close_pools()
            return BackendResult(False, "Authentication layer returned no session.")

        return api, authed

    async def _execute_with_authed(
        self,
        site_key: str,
        runner: Callable[[auth_types], Awaitable[BackendResult]],
        account_id: Optional[int] = None,
        use_db_init: bool = True,
    ) -> BackendResult:
        login_result = await self._login_with_account(site_key, account_id, use_db_init)
        if isinstance(login_result, BackendResult):
            return login_result

        api, authed = login_result
        try:
            return await runner(authed)
        finally:
            with contextlib.suppress(Exception):
                await api.close_pools()

    async def _api_get_user(self, site_key: str, identifier: str) -> BackendResult:
        async def runner(authed: auth_types) -> BackendResult:
            success, message, payload = await self._fetch_user_payload(
                authed, identifier
            )
            if not success:
                return BackendResult(False, message)
            return BackendResult(True, message, {"user": payload or {}})

        return await self._execute_with_authed(site_key, runner)

    async def _cmd_get_user(self, site_key: str, args: dict[str, Any]) -> BackendResult:
        identifier = args.get("identifier")
        if identifier is None:
            return BackendResult(False, "Identifier is required for get_user.")
        value = identifier.strip() if isinstance(identifier, str) else str(identifier)
        if not value:
            return BackendResult(False, "Identifier is required for get_user.")
        return await self._api_get_user(site_key, value)

    async def _cmd_get_authed_user(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")

        async def runner(authed: auth_types) -> BackendResult:
            get_authed_user = getattr(authed, "get_authed_user", None)
            if not callable(get_authed_user):
                return BackendResult(
                    False,
                    f"Authenticated profile not supported for {type(authed).__name__}.",
                )
            try:
                user = await cast(
                    Callable[[], Awaitable[UserLike | None]], get_authed_user
                )()
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_authed_user failed for site=%s", site_key)
                return BackendResult(False, f"get_authed_user failed: {exc}")
            if user is None:
                return BackendResult(False, "No authenticated profile available.")
            summary = summarize_user(user)
            summary["site"] = site_key
            return BackendResult(
                True, "Authenticated profile fetched.", {"user": summary}
            )

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_lists(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        limit = args.get("limit", 20)
        offset = args.get("offset", 0)
        try:
            limit_int = max(1, min(int(limit), 100))
            offset_int = max(0, int(offset))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit/offset: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            get_lists = getattr(authed, "get_lists", None)
            if not callable(get_lists):
                return BackendResult(
                    False, f"Lists not supported for {type(authed).__name__}"
                )
            try:
                records = await cast(Callable[..., Awaitable[Any]], get_lists)(
                    refresh=True, limit=limit_int, offset=offset_int
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_lists failed for site=%s", site_key)
                return BackendResult(False, f"get_lists failed: {exc}")
            payload: Any
            if isinstance(records, list):
                payload = cast(list[Any], records)[:limit_int]
                message = f"Fetched {len(payload)} lists."
            else:
                payload = records
                message = "Lists fetched."
            return BackendResult(True, message, {"lists": payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_subscriptions(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        limit = args.get("limit", 25)
        sub_type = str(args.get("sub_type", "all")).lower()
        filter_by = args.get("filter_by", "")
        try:
            limit_int = max(1, min(int(limit), 100))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            get_subscriptions = getattr(authed, "get_subscriptions", None)
            if not callable(get_subscriptions):
                return BackendResult(
                    False,
                    f"Subscriptions not supported for {type(authed).__name__}",
                )
            try:
                call_subscriptions = cast(
                    Callable[..., Awaitable[Any]], get_subscriptions
                )
                try:
                    subs = await call_subscriptions(
                        identifiers=[],
                        limit=limit_int,
                        sub_type=sub_type,
                        filter_by=filter_by,
                    )
                except TypeError:
                    subs = await call_subscriptions(
                        identifiers=[],
                        sub_type=sub_type,
                    )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_subscriptions failed for site=%s", site_key)
                return BackendResult(False, f"get_subscriptions failed: {exc}")

            payload: Any
            if isinstance(subs, list):
                limited = cast(list[Any], subs)[:limit_int]
                payload = [self._summarise_subscription(item) for item in limited]
                message = f"Fetched {len(payload)} subscriptions."
            else:
                payload = subs
                message = "Subscriptions fetched."
            return BackendResult(True, message, {"subscriptions": payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_transactions(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        limit = args.get("limit", 20)
        try:
            limit_int = max(1, min(int(limit), 100))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            try:
                # get_transactions is only available on OnlyFansAuthModel
                if not hasattr(authed, "get_transactions"):
                    return BackendResult(
                        False,
                        f"Transactions not supported for {type(authed).__name__}",
                    )

                results = await authed.get_transactions()  # type: ignore[attr-defined]
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_transactions failed for site=%s", site_key)
                return BackendResult(False, f"get_transactions failed: {exc}")

            payload: Any
            if isinstance(results, list):
                payload = cast(list[Any], results)[:limit_int]
            elif isinstance(results, dict):
                result_dict = cast(dict[str, Any], results)
                list_candidates: tuple[Any, ...] = (
                    result_dict.get("list"),
                    result_dict.get("transactions"),
                    result_dict.get("data"),
                )
                for candidate in list_candidates:
                    if isinstance(candidate, list):
                        payload = cast(list[Any], candidate)[:limit_int]
                        break
                else:
                    payload = result_dict
            else:
                payload = cast(Any, results)

            if isinstance(payload, list):
                payload_list = cast(list[Any], payload)
                message = f"Fetched {len(payload_list)} transactions."
            else:
                message = "Transactions fetched."
            return BackendResult(True, message, {"transactions": payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_chats(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        limit = args.get("limit", 100)
        offset = args.get("offset", 0)
        try:
            limit_int = max(1, min(int(limit), 200))
            offset_int = max(0, int(offset))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit/offset: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            get_chats = getattr(authed, "get_chats", None)
            if not callable(get_chats):
                return BackendResult(
                    False, f"Chats not supported for {type(authed).__name__}"
                )
            try:
                chats = await cast(Callable[..., Awaitable[Any]], get_chats)(
                    limit=limit_int, offset=offset_int
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_chats failed for site=%s", site_key)
                return BackendResult(False, f"get_chats failed: {exc}")

            payload: Any
            if isinstance(chats, list):
                chat_items = cast(list[Any], chats)
                chat_list: list[dict[str, Any]] = []
                for chat in chat_items:
                    chat_obj = cast(ChatLike, chat)
                    chat_dict: dict[str, Any] = {}
                    user = getattr(chat_obj, "user", None)
                    if user is not None:
                        chat_dict["user"] = summarize_user(user)
                    msg = getattr(chat_obj, "last_message", None)
                    if msg is not None:
                        chat_dict["last_message"] = {
                            "id": getattr(msg, "id", None),
                            "text": getattr(msg, "text", ""),
                            "created_at": self._serialize_datetime(
                                getattr(msg, "created_at", None)
                            ),
                        }
                    chat_dict["last_read_message_id"] = getattr(
                        chat_obj, "last_read_message_id", None
                    )
                    chat_list.append(chat_dict)
                payload = chat_list[:limit_int]
                message = f"Fetched {len(payload)} chats."
            else:
                payload = chats
                message = "Chats fetched."
            return BackendResult(True, message, {"chats": payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_search_chats(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        query = args.get("query")
        if query is None or not str(query).strip():
            return BackendResult(False, "Query is required for search_chats.")
        limit = args.get("limit", 100)
        offset = args.get("offset", 0)
        try:
            limit_int = max(1, min(int(limit), 200))
            offset_int = max(0, int(offset))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit/offset: {exc}")

        query_text = str(query).strip()

        async def runner(authed: auth_types) -> BackendResult:
            search_chats = getattr(authed, "search_chats", None)
            if not callable(search_chats):
                return BackendResult(
                    False, f"search_chats not supported for {type(authed).__name__}"
                )
            try:
                chats = await cast(Callable[..., Awaitable[Any]], search_chats)(
                    query=query_text, limit=limit_int, offset=offset_int
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("search_chats failed for site=%s", site_key)
                return BackendResult(False, f"search_chats failed: {exc}")

            payload: Any
            if isinstance(chats, list):
                chat_items = cast(list[Any], chats)
                chat_list: list[dict[str, Any]] = []
                for chat in chat_items:
                    chat_obj = cast(ChatLike, chat)
                    chat_dict: dict[str, Any] = {}
                    user = getattr(chat_obj, "user", None)
                    if user is not None:
                        chat_dict["user"] = summarize_user(user)
                    msg = getattr(chat_obj, "last_message", None)
                    if msg is not None:
                        chat_dict["last_message"] = {
                            "id": getattr(msg, "id", None),
                            "text": getattr(msg, "text", ""),
                            "created_at": self._serialize_datetime(
                                getattr(msg, "created_at", None)
                            ),
                        }
                    chat_dict["last_read_message_id"] = getattr(
                        chat_obj, "last_read_message_id", None
                    )
                    chat_list.append(chat_dict)
                payload = chat_list[:limit_int]
                message = f"Fetched {len(payload)} chat search results."
            else:
                payload = chats
                message = "Chat search completed."
            return BackendResult(True, message, {"chats": payload, "query": query_text})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_messages(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        user_id = args.get("user_id")
        if user_id is None:
            return BackendResult(False, "User ID is required for get_messages.")
        limit = args.get("limit", 20)
        offset_id = args.get("offset_id")
        cutoff_id = args.get("cutoff_id")
        try:
            limit_int = max(1, min(int(limit), 100))
            offset_id_int = int(offset_id) if offset_id is not None else None
            cutoff_id_int = int(cutoff_id) if cutoff_id is not None else None
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid get_messages arguments: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            try:
                # First resolve the user
                get_user = getattr(authed, "get_user", None)
                if not callable(get_user):
                    return BackendResult(
                        False,
                        f"Messages not supported for {type(authed).__name__}",
                    )
                user = await cast(
                    Callable[[int | str], Awaitable[UserLike | None]], get_user
                )(user_id)
                if user is None:
                    return BackendResult(False, f"User '{user_id}' not found.")
                # Now get messages
                get_messages = getattr(user, "get_messages", None)
                if not callable(get_messages):
                    return BackendResult(
                        False, f"User '{user_id}' does not support get_messages."
                    )
                messages = await cast(
                    Callable[..., Awaitable[list[Any] | dict[str, Any]]], get_messages
                )(
                    limit=limit_int,
                    offset_id=offset_id_int,
                    cutoff_id=cutoff_id_int,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_messages failed for site=%s", site_key)
                return BackendResult(False, f"get_messages failed: {exc}")

            payload: Any
            if isinstance(messages, list):
                message_list: list[dict[str, Any]] = []
                for msg in messages[:limit_int]:
                    # Use helper to extract content fields (reduces duplication)
                    msg_dict = self._extract_content_dict(msg, content_type="message")
                    message_list.append(msg_dict)
                payload = message_list
                message_str = f"Fetched {len(payload)} messages from user '{user_id}'."
            else:
                payload = messages
                message_str = f"Messages fetched from user '{user_id}'."
            return BackendResult(True, message_str, {"messages": payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_paid_content(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        performer_id = args.get("performer_id")
        limit = args.get("limit", 10)
        offset = args.get("offset", 0)
        try:
            limit_int = max(1, min(int(limit), 100))
            offset_int = max(0, int(offset))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit/offset: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            try:
                # performer_id can be None (fetch all)
                resolved_performer_id = None
                if performer_id:
                    # Try to resolve as user
                    try:
                        user = await authed.get_user(performer_id)
                        if user:
                            resolved_performer_id = getattr(user, "id", None)
                    except Exception as user_err:  # pragma: no cover - defensive
                        logger.debug(
                            "Failed to resolve performer_id=%s to user: %s",
                            performer_id,
                            user_err,
                        )
                get_paid_content = getattr(authed, "get_paid_content", None)
                if not callable(get_paid_content):
                    return BackendResult(
                        False,
                        f"Paid content not supported for {type(authed).__name__}",
                    )
                paid = await cast(Callable[..., Awaitable[Any]], get_paid_content)(
                    performer_id=resolved_performer_id,
                    limit=limit_int,
                    offset=offset_int,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_paid_content failed for site=%s", site_key)
                return BackendResult(False, f"get_paid_content failed: {exc}")

            payload: Any
            if isinstance(paid, list):
                paid_items = cast(list[Any], paid)
                content_list: list[dict[str, Any]] = []
                for item in paid_items[:limit_int]:
                    # Use helper to extract content fields (reduces duplication and # type: ignore)
                    item_dict = self._extract_content_dict(item, content_type="paid")
                    content_list.append(item_dict)
                payload = content_list
                message_str = f"Fetched {len(payload)} paid content items."
            else:
                payload = paid
                message_str = "Paid content fetched."
            return BackendResult(True, message_str, {"paid_content": payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_mass_message_stats(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")
        limit = args.get("limit", 100)
        offset = args.get("offset", 0)
        try:
            limit_int = max(1, min(int(limit), 100))
            offset_int = max(0, int(offset))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit/offset: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            get_mass_message_stats = getattr(authed, "get_mass_message_stats", None)
            if not callable(get_mass_message_stats):
                return BackendResult(
                    False,
                    f"Mass message stats not supported for {type(authed).__name__}",
                )
            try:
                stats = await cast(
                    Callable[..., Awaitable[Any]], get_mass_message_stats
                )(limit=limit_int, offset=offset_int)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_mass_message_stats failed for site=%s", site_key)
                return BackendResult(False, f"get_mass_message_stats failed: {exc}")

            payload = self._serialize_api_value(stats)
            if isinstance(payload, list):
                limited = cast(list[Any], payload)[:limit_int]
                return BackendResult(
                    True,
                    f"Fetched {len(limited)} mass message stats.",
                    {"mass_message_stats": limited},
                )
            return BackendResult(
                True,
                "Mass message stats fetched.",
                {"mass_message_stats": payload},
            )

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_get_subscription_count(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        account_id = args.get("_account_id")

        async def runner(authed: auth_types) -> BackendResult:
            get_subscription_count = getattr(authed, "get_subscription_count", None)
            if not callable(get_subscription_count):
                return BackendResult(
                    False,
                    f"Subscription counts not supported for {type(authed).__name__}",
                )
            try:
                counts = await cast(
                    Callable[[], Awaitable[Any]], get_subscription_count
                )()
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("get_subscription_count failed for site=%s", site_key)
                return BackendResult(False, f"get_subscription_count failed: {exc}")

            return BackendResult(
                True,
                "Subscription counts fetched.",
                {"subscription_count": self._serialize_api_value(counts)},
            )

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_list_user_collection(
        self,
        site_key: str,
        args: dict[str, Any],
        *,
        method_name: str,
        result_key: str,
        label: str,
    ) -> BackendResult:
        account_id = args.get("_account_id")
        limit = args.get("limit", 100)
        offset = args.get("offset", 0)
        try:
            limit_int = max(1, min(int(limit), 100))
            offset_int = max(0, int(offset))
        except (TypeError, ValueError) as exc:
            return BackendResult(False, f"Invalid limit/offset: {exc}")

        async def runner(authed: auth_types) -> BackendResult:
            get_users = getattr(authed, method_name, None)
            if not callable(get_users):
                return BackendResult(
                    False,
                    f"{label} not supported for {type(authed).__name__}",
                )
            try:
                users = await cast(Callable[..., Awaitable[Any]], get_users)(
                    limit=limit_int,
                    offset=offset_int,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("%s failed for site=%s", method_name, site_key)
                return BackendResult(False, f"{method_name} failed: {exc}")

            payload: Any
            if isinstance(users, list):
                user_rows = cast(list[Any], users)[:limit_int]
                payload = [
                    (
                        summarize_user(user)
                        if hasattr(user, "id") and hasattr(user, "username")
                        else self._serialize_api_value(user)
                    )
                    for user in user_rows
                ]
                message = f"Fetched {len(payload)} {result_key.replace('_', ' ')}."
            else:
                payload = self._serialize_api_value(users)
                message = f"{label} fetched."

            return BackendResult(True, message, {result_key: payload})

        return await self._execute_with_authed(site_key, runner, account_id)

    async def _cmd_blocked_users(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        return await self._cmd_list_user_collection(
            site_key,
            args,
            method_name="blocked_users",
            result_key="blocked_users",
            label="Blocked users",
        )

    async def _cmd_restricted_users(
        self, site_key: str, args: dict[str, Any]
    ) -> BackendResult:
        return await self._cmd_list_user_collection(
            site_key,
            args,
            method_name="restricted_users",
            result_key="restricted_users",
            label="Restricted users",
        )

    async def run_api_command(
        self,
        site: str,
        command: str,
        args: Optional[dict[str, Any]] = None,
        account_id: Optional[int] = None,
        priority: int = -10,
    ) -> BackendResult:
        """Execute an API Explorer command as a worker-pool job."""

        command_key = command.strip().lower()
        descriptor = self._api_command_map.get(command_key)
        if descriptor is None:
            return BackendResult(False, f"Unknown API command '{command}'.")

        site_key = site.strip().lower()
        if not site_key:
            return BackendResult(False, "Choose a site before running a command.")
        if not descriptor.allows_site(site_key):
            allowed = ", ".join(descriptor.sites or [])
            return BackendResult(
                False,
                f"Command '{command_key}' is not available for site '{site_key}'."
                + (f" Supported sites: {allowed}." if allowed else ""),
            )

        provided = args or {}
        resolved: dict[str, Any] = {}
        for arg in descriptor.args:
            value = provided.get(arg.name)
            if value is None and arg.is_primary and "identifier" in provided:
                value = provided.get("identifier")
            try:
                resolved[arg.name] = arg.parse(value)
            except ValueError as exc:
                return BackendResult(False, str(exc))

        for key, value in provided.items():
            if key not in resolved:
                resolved[key] = value

        return await self._submit_worker_api_command(
            site_key,
            command_key,
            resolved,
            account_id,
            priority=priority,
        )

    async def _submit_worker_api_command(
        self,
        site: str,
        command: str,
        args: Mapping[str, Any] | None,
        account_id: int | None,
        *,
        timeout_seconds: float = 120.0,
        priority: int = -10,
        auth_override: Mapping[str, Any] | None = None,
        guest: bool = False,
        idempotency_key: str | None = None,
    ) -> BackendResult:
        """Run one API operation as an API_COMMAND worker job."""

        site_key = site.strip().lower()
        if not site_key:
            return BackendResult(False, "Choose a site before running a command.")

        worker_auth_id: int | None = None
        if auth_override is None and not guest:
            account = self._find_account(site_key, account_id)
            if account is None:
                return BackendResult(False, f"No stored credentials for '{site_key}'.")
            worker_auth_id = account.user_id or account.id
            if not worker_auth_id:
                return BackendResult(False, f"Account for '{site_key}' has no user ID.")

        if not self._job_manager or not self._job_storage:
            await self.initialize_redis()
        if not self._job_manager or not self._job_storage:
            return BackendResult(
                False,
                "Worker-pool job system is unavailable; API operations require Redis and workers.",
            )

        from hashlib import sha256

        from ultima_scraper_backend.job_system.api_command_policy import (
            PROFILE_REFRESH_MAX_ATTEMPTS,
        )
        from ultima_scraper_backend.job_system.models import (
            Job,
            Task,
            TaskState,
            TaskType,
        )
        from ultima_scraper_backend.job_system.payloads import APICommandTaskPayload
        identity = sha256(idempotency_key.encode()).hexdigest()[:32] if idempotency_key else uuid4().hex
        job_id = f"api-{identity}"
        task_id = f"task-{identity if idempotency_key else uuid4().hex}"
        task_args = dict(args or {})
        if auth_override is not None:
            task_args["_auth_override"] = self._serialize_worker_api_argument(
                dict(auth_override)
            )
        if guest:
            task_args["_guest"] = True
        task = Task(
            user_id=f"api-{job_id}",
            task_type=TaskType.API_COMMAND,
            site_name=site_key,
            payload=APICommandTaskPayload(
                command=command,
                args=task_args,
            ),
            auth_id=int(worker_auth_id) if worker_auth_id is not None else None,
            job_id=job_id,
            task_id=task_id,
            capabilities=[site_key, "api_commands"],
            # Profile lookup is a read and can safely survive a lost worker.
            # Other commands may mutate upstream state and retain one attempt.
            max_attempts=PROFILE_REFRESH_MAX_ATTEMPTS if command == "refresh_profile" else 1,
        )
        job = Job(
            job_id=job_id,
            name=f"API command: {command}",
            created_by="api",
            tasks=[task],
            metadata={
                "source": "api",
                "admission_idempotent": idempotency_key is not None,
                "priority": priority,
                "api_command": command,
                "api_site": site_key,
                "required_capabilities": [site_key, "api_commands"],
            },
        )

        try:
            prepared_job = await self._job_manager.prepare_job(
                job,
                priority=priority,
            )
            await self._job_manager.activate_job(
                prepared_job,
                distributed_mode=True,
                priority=priority,
            )
        except Exception as exc:
            logger.exception(
                "Failed to dispatch worker API command %s for %s", command, site_key
            )
            return BackendResult(
                False,
                f"Failed to dispatch API command '{command}' to the worker pool: {exc}",
                {"job_id": job_id},
            )

        poll_count = 0
        deadline = asyncio.get_running_loop().time() + max(1.0, timeout_seconds)
        while asyncio.get_running_loop().time() < deadline:
            stored_task = await self._job_storage.fetch_task(job_id, task_id)
            if stored_task is not None and stored_task.state in {
                TaskState.COMPLETED,
                TaskState.FAILED,
                TaskState.CANCELLED,
            }:
                task_result = stored_task.result
                if task_result is None:
                    return BackendResult(
                        False,
                        f"Worker API command '{command}' ended without a result.",
                        {"job_id": job_id},
                    )
                output = task_result.output
                payload = output if isinstance(output, dict) else {"result": output}
                return BackendResult(
                    bool(task_result.success),
                    task_result.message or f"API command '{command}' completed.",
                    payload,
                )
            poll_count += 1
            await asyncio.sleep(0.02 if poll_count < 10 else (0.05 if poll_count < 30 else 0.1))

        logger.warning(
            "Timed out waiting for worker API command %s (job_id=%s)",
            command,
            job_id,
        )
        return BackendResult(
            False,
            f"Timed out waiting for worker API command '{command}'.",
            {"job_id": job_id, "pending": True},
        )

    async def dispatch_api_command(
        self,
        site: str,
        command: str,
        args: Optional[dict[str, Any]] = None,
        account_id: Optional[int] = None,
        priority: int = 5,
    ) -> BackendResult:
        """
        Compatibility wrapper that forwards an API command to a worker-pool job.

        Args:
            site: Site name (e.g., "onlyfans")
            command: API command name (e.g., "get_authed_user", "get_subscriptions")
            args: Command arguments dictionary
            account_id: Optional account ID to use for authentication
            priority: Job priority (higher = processed first)

        Returns:
            BackendResult with the worker-pool command result
        """
        logger.warning(
            "dispatch_api_command is deprecated; forwarding to an API_COMMAND worker job."
        )
        return await self.run_api_command(
            site,
            command,
            args,
            account_id,
            priority=priority,
        )

    async def get_job_status(self, job_id: str) -> BackendResult:
        """
        Get the status of a job from the task-based job system.

        Args:
            job_id: Job ID to query

        Returns:
            BackendResult with job status information
        """
        if not self._job_storage:
            return BackendResult(False, "Job system not available")
        if not self._redis_manager:
            return BackendResult(False, "Redis not available")

        try:
            job = await self._job_storage.fetch_job(job_id)
            if not job:
                return BackendResult(False, f"Job {job_id} not found")

            # Extract backend_id from metadata
            backend_id = job.metadata.get("backend_id")

            return BackendResult(
                True,
                f"Job {job_id} status: {job.state}",
                {
                    "job_id": job.job_id,
                    "name": job.name,
                    "state": job.state,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "created_by": job.created_by,
                    "backend_id": backend_id,  # Include backend_id in response
                    "metadata": job.metadata,
                    "tasks": [
                        {
                            "task_id": task.task_id,
                            "task_type": task.task_type.value,
                            "state": task.state,
                            "user_id": task.user_id,
                            "content_type": task.content_type,
                            "worker_id": task.worker_id,  # Include worker_id
                        }
                        for task in job.tasks
                    ],
                },
            )

        except Exception as exc:
            logger.exception("Failed to get job status: %s", exc)
            return BackendResult(False, f"get_job_status error: {exc}")

    async def get_queue_stats(self) -> BackendResult:
        """
        Get statistics about the task-based job system.

        Returns:
            BackendResult with queue statistics
        """
        if not self._job_storage or not self._redis_manager:
            return BackendResult(False, "Job system not available")

        try:
            from ultima_scraper_backend.job_system.models import Job

            job_ids = await self._job_storage.list_job_ids()

            # Categorize jobs by state
            pending_jobs: list[Job] = []
            running_jobs: list[Job] = []
            completed_jobs: list[Job] = []
            failed_jobs: list[Job] = []
            paused_jobs: list[Job] = []

            for job_id in job_ids[:100]:  # Limit to 100 most recent
                job = await self._job_storage.fetch_job(job_id)
                if not job:
                    continue

                if job.state == "pending":
                    pending_jobs.append(job)
                elif job.state == "running":
                    running_jobs.append(job)
                elif job.state == "completed":
                    completed_jobs.append(job)
                elif job.state == "failed":
                    failed_jobs.append(job)
                elif job.state == "paused":
                    paused_jobs.append(job)

            return BackendResult(
                True,
                f"Queue stats: {len(pending_jobs)} pending, {len(paused_jobs)} paused, {len(running_jobs)} running, {len(completed_jobs)} completed, {len(failed_jobs)} failed",
                {
                    "pending_count": len(pending_jobs),
                    "paused_count": len(paused_jobs),
                    "running_count": len(running_jobs),
                    "completed_count": len(completed_jobs),
                    "failed_count": len(failed_jobs),
                    "active_jobs": [
                        {
                            "job_id": job.job_id,
                            "name": job.name,
                            "state": job.state,
                            "created_at": job.created_at,
                            "started_at": job.started_at,
                            "created_by": job.created_by,
                            "task_count": len(job.tasks),
                            "system": "task_based",
                        }
                        for job in running_jobs
                    ],
                    "completed_jobs": [
                        {
                            "job_id": job.job_id,
                            "name": job.name,
                            "state": job.state,
                            "created_at": job.created_at,
                            "completed_at": job.completed_at,
                            "task_count": len(job.tasks),
                            "system": "task_based",
                        }
                        for job in completed_jobs[:20]  # Limit display
                    ],
                    "pending_jobs": [
                        {
                            "job_id": job.job_id,
                            "name": job.name,
                            "state": job.state,
                            "created_at": job.created_at,
                            "task_count": len(job.tasks),
                            "system": "task_based",
                        }
                        for job in pending_jobs[:50]
                    ],
                    "paused_jobs": [
                        {
                            "job_id": job.job_id,
                            "name": job.name,
                            "state": job.state,
                            "created_at": job.created_at,
                            "task_count": len(job.tasks),
                            "system": "task_based",
                        }
                        for job in paused_jobs[:50]
                    ],
                },
            )

        except Exception as exc:
            logger.exception("Failed to get queue stats: %s", exc)
            return BackendResult(False, f"get_queue_stats error: {exc}")

    async def pause_job(self, job_id: str) -> BackendResult:
        """Pause a pending job in the task-based system."""
        if not job_id:
            return BackendResult(False, "Job ID is required")
        if not self._job_storage:
            return BackendResult(False, "Job system not available")

        try:
            job = await self._job_storage.fetch_job(job_id)
            if not job:
                return BackendResult(False, f"Job {job_id} not found")

            if job.state != "pending":
                return BackendResult(
                    False, f"Job {job_id} is not pending (current state: {job.state})"
                )

            job.state = "paused"
            await self._job_storage.persist_job(job)
            return BackendResult(True, f"Job {job_id} paused")
        except Exception as exc:
            logger.exception("Failed to pause job %s: %s", job_id, exc)
            return BackendResult(False, f"pause_job error: {exc}")

    async def resume_job(self, job_id: str) -> BackendResult:
        """Resume a paused job back into the pending state.

        This method:
        1. Changes job state from "paused" to "pending"
        2. Restores task state sets (ready, blocked, completed) in Redis
        3. Re-enqueues ready tasks to the backend's priority queue
        """
        if not job_id:
            return BackendResult(False, "Job ID is required")
        if not self._job_storage or not self._backend_router:
            return BackendResult(False, "Job system not available")
        if not self._redis_manager:
            return BackendResult(False, "Redis not available")

        try:
            job = await self._job_storage.fetch_job(job_id)
            if not job:
                return BackendResult(False, f"Job {job_id} not found")

            if job.state != "paused":
                return BackendResult(
                    False, f"Job {job_id} is not paused (current state: {job.state})"
                )

            # Get the backend_id for this job from the envelope
            envelope_key = f"{self._backend_router.JOB_ENVELOPE_PREFIX}{job_id}"
            envelope_data = await self._redis_manager.get(envelope_key)
            if not envelope_data:
                return BackendResult(
                    False, f"Job {job_id} envelope not found - cannot determine backend"
                )

            try:
                envelope_dict = cast(dict[str, Any], ujson.loads(envelope_data))
                backend_id = envelope_dict.get("backend_id")
                if not backend_id:
                    return BackendResult(
                        False, f"Job {job_id} has no backend assignment"
                    )
            except (ujson.JSONDecodeError, ValueError) as e:
                return BackendResult(False, f"Failed to parse job envelope: {e}")

            # Restore task state sets from Redis (detects completed tasks)
            ready_task_ids = await self._job_storage.restore_task_state(job)

            # Update job state to pending
            job.state = "pending"
            await self._job_storage.persist_job(job)

            # Re-enqueue ready tasks to the backend's priority queue
            if ready_task_ids:
                lookup = {task.task_id: task for task in job.tasks}
                for task_id in ready_task_ids:
                    task = lookup.get(task_id)
                    priority = 0
                    if task and task.payload:
                        raw_priority = task.payload.get("priority")
                        if isinstance(raw_priority, int):
                            priority = raw_priority

                    await self._backend_router.enqueue_ready_task(
                        backend_id=backend_id,
                        job_id=job_id,
                        task_id=task_id,
                        priority=priority,
                    )

                logger.info(
                    f"Resumed job {job_id} with {len(ready_task_ids)} ready tasks"
                )
                return BackendResult(
                    True,
                    f"Job {job_id} resumed with {len(ready_task_ids)} ready task(s)",
                )
            else:
                logger.warning(
                    f"Resumed job {job_id} but no ready tasks found (possibly all completed)"
                )
                return BackendResult(
                    True, f"Job {job_id} resumed (no ready tasks - job may be complete)"
                )

        except Exception as exc:
            logger.exception("Failed to resume job %s: %s", job_id, exc)
            return BackendResult(False, f"resume_job error: {exc}")

    async def cancel_job(self, job_id: str) -> BackendResult:
        """Cancel a job from the task-based system."""
        if not job_id:
            return BackendResult(False, "Job ID is required")
        if not self._job_storage:
            return BackendResult(False, "Job system not available")

        try:
            job = await self._job_storage.fetch_job(job_id)
            if not job:
                return BackendResult(False, f"Job {job_id} not found")

            job.state = "cancelled"
            await self._job_storage.persist_job(job)
            return BackendResult(True, f"Job {job_id} cancelled")
        except Exception as exc:
            logger.exception("Failed to cancel job %s: %s", job_id, exc)
            return BackendResult(False, f"cancel_job error: {exc}")

    async def get_worker_pool_status(self) -> BackendResult:
        """
        Get status of all workers in the worker pool.

        Returns:
            BackendResult with worker pool status including count and details
        """
        if not self._redis_manager:
            return BackendResult(False, "Redis not available")

        try:
            workers = await WorkerRegistry.get_all_workers_info(
                cast(Any, self._redis_manager)
            )
            worker_count = len(workers)

            by_backend: dict[str, list[dict[str, Any]]] = {}
            worker_payload: list[dict[str, Any]] = []
            for info in workers.values():
                data = info.to_dict()
                worker_payload.append(data)
                by_backend.setdefault(info.backend_id, []).append(data)

            return BackendResult(
                True,
                f"Found {worker_count} active workers",
                {
                    "count": worker_count,
                    "workers": worker_payload,
                    "by_backend": by_backend,
                },
            )

        except Exception as exc:
            logger.exception("Failed to get worker pool status: %s", exc)
            return BackendResult(False, f"get_worker_pool_status error: {exc}")

    def stop_scrape_process(self, process_id: str) -> BackendResult:
        """
        Stop a specific scraping process.

        Args:
            process_id: Process ID returned by start_scrape

        Returns:
            BackendResult indicating success or failure
        """
        try:
            if self._process_manager is None:
                return BackendResult(False, "Process manager not initialized")

            success = self._process_manager.stop_process(process_id)
            if success:
                return BackendResult(True, f"Stopped process {process_id}")
            else:
                return BackendResult(False, f"Process {process_id} not found")
        except Exception as exc:
            logger.exception("Failed to stop process: %s", exc)
            return BackendResult(False, f"stop_scrape_process error: {exc}")

    def stop_all_scrape_processes(self) -> BackendResult:
        """
        Stop all active scraping processes.

        Returns:
            BackendResult indicating success
        """
        try:
            if self._process_manager is None:
                return BackendResult(True, "No processes to stop")

            self._process_manager.stop_all_processes()
            return BackendResult(True, "Stopped all scraping processes")
        except Exception as exc:
            logger.exception("Failed to stop all processes: %s", exc)
            return BackendResult(False, f"stop_all_scrape_processes error: {exc}")


__all__ = ["BackendManager", "BackendResult", "AccountRecord"]
