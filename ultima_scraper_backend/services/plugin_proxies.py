"""Reconcile plugin-owned proxies without touching another provider's entries."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import contextmanager
from urllib.parse import urlsplit

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlmodel import select
from ultima_scraper_core.processes import finish_owned
from ultima_scraper_db_archive import UserAuthModel
from ultima_scraper_db_archive.global_db import get_global_session

from ..config import PluginProxyOwner, Proxy, UltimaScraperCollectionConfig
from ..database.factory import MultiSiteDatabaseManager
from ..database.threads import run_database_call
from ..plugin_system.permissions import PluginCapability


class ProviderProxy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider_id: str = Field(min_length=1, max_length=128)
    url: str = Field(min_length=1, max_length=2048)
    username: str = Field(min_length=1, max_length=512)
    password: str = Field(min_length=1, max_length=4096, repr=False)

    @field_validator("url")
    @classmethod
    def endpoint(cls, value):
        parts = urlsplit(value)
        if (
            parts.scheme not in {"http", "https", "socks5"}
            or not parts.hostname
            or not parts.port
            or parts.username
            or parts.password
            or parts.path not in {"", "/"}
            or parts.query
            or parts.fragment
        ):
            raise ValueError(
                "Proxy requires an endpoint without credentials, path or query"
            )
        host = (
            f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname.lower()
        )
        return f"{parts.scheme}://{host}:{parts.port}"


class ProxySyncRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")
    proxies: list[ProviderProxy] = Field(min_length=1, max_length=20000)
    idempotency_key: str = Field(min_length=1, max_length=128)
    default_group: str = Field(default="Webshare", max_length=128)


@contextmanager
def proxy_assignment_lock():
    """All control-plane assignment writers share this management DB lock.

    It is a transaction lock, so it also works behind transaction PgBouncer.
    Site commits finish before the lock's management transaction is released.
    """
    with get_global_session() as session:
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext('usr:proxy-assignments'))")
        )
        yield


async def guard_proxy_assignment(request: Request):
    """FastAPI dependency held until an assignment-writing request completes."""
    body = await request.json()

    def requested_proxies(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"proxy_id", "proxy_url"} and isinstance(item, str) and item:
                    yield item
                elif isinstance(item, (dict, list)):
                    yield from requested_proxies(item)
        elif isinstance(value, list):
            for item in value:
                yield from requested_proxies(item)

    targets = set(requested_proxies(body))
    from ultima_scraper_core import archive_database_url

    if not targets or not archive_database_url():
        yield
        return
    context = proxy_assignment_lock()
    acquire = asyncio.create_task(run_database_call(context.__enter__))
    try:
        await asyncio.shield(acquire)
    except asyncio.CancelledError:
        await finish_owned(acquire)
        await finish_owned(
            asyncio.create_task(run_database_call(context.__exit__, None, None, None))
        )
        raise
    try:
        config = await run_database_call(
            UltimaScraperCollectionConfig.load_or_create_default_config
        )
        available = {proxy.url for proxy in config.settings.network.proxies}
        if not targets <= available:
            raise HTTPException(
                status_code=409,
                detail="Assigned proxy is no longer configured; reload and retry",
            )
        yield
    finally:
        await finish_owned(
            asyncio.create_task(run_database_call(context.__exit__, None, None, None))
        )


def assigned_accounts(urls):
    """Fail closed: an unavailable site cannot prove that a proxy is unused."""
    if not urls:
        return []
    manager = MultiSiteDatabaseManager()
    from ultima_scraper_api import get_enabled_sites

    sites = set(manager.get_all_sites()) | {
        site.lower() for site in get_enabled_sites()
    }
    records = []
    for site in sites:
        try:
            database = manager.get_or_create_manager(site)
        except Exception as error:
            raise ValueError("Cannot verify proxy account assignments") from error
        if database is None:
            raise ValueError("Cannot verify proxy account assignments")
        try:
            with database.get_session() as session:
                for auth in session.exec(
                    select(UserAuthModel).where(UserAuthModel.proxy_id.in_(list(urls)))
                ).all():
                    records.append(
                        {
                            "proxy_id": auth.proxy_id,
                            "user_id": auth.user_id,
                            "username": auth.username,
                            "site": site,
                        }
                    )
        except Exception as error:
            raise ValueError("Cannot verify proxy account assignments") from error
    return records


def reconcile_proxies(
    existing, incoming, *, plugin_id, source, default_group, accounts
):
    """Pure reconciliation used by the broker and regression tests."""
    targets = {item.url: item for item in incoming}
    if len(targets) != len(incoming) or len(
        {item.provider_id for item in incoming}
    ) != len(incoming):
        raise ValueError("Provider snapshot contains duplicate proxies")
    assigned = {item["proxy_id"] for item in accounts}
    counts = dict.fromkeys(
        [
            "added",
            "adopted",
            "updated",
            "unchanged",
            "deleted",
            "disabled",
            "retained_assigned",
            "conflicts",
        ],
        0,
    )
    result = []
    changed = set()
    for original in existing:
        proxy = original.model_copy(deep=True)
        owner = proxy.managed_by
        incoming_proxy = targets.pop(proxy.url, None)
        own = owner and owner.plugin_id == plugin_id and owner.source == source
        if incoming_proxy:
            if owner and not own:
                counts["conflicts"] += 1
            else:
                credentials_changed = (
                    proxy.username != incoming_proxy.username
                    or proxy.password != incoming_proxy.password
                )
                proxy.username, proxy.password = (
                    incoming_proxy.username,
                    incoming_proxy.password,
                )
                proxy.managed_by = PluginProxyOwner(
                    plugin_id=plugin_id,
                    source=source,
                    provider_id=incoming_proxy.provider_id,
                )
                counts[
                    "adopted"
                    if owner is None
                    else "updated"
                    if credentials_changed
                    or owner.provider_id != incoming_proxy.provider_id
                    else "unchanged"
                ] += 1
                if credentials_changed:
                    changed.add(proxy.url)
        elif own:
            if proxy.url not in assigned:
                counts["deleted"] += 1
                continue
            counts["retained_assigned"] += 1
            if proxy.enabled:
                counts["disabled"] += 1
                changed.add(proxy.url)
            proxy.enabled = False
        result.append(proxy)
    for item in targets.values():
        result.append(
            Proxy(
                url=item.url,
                username=item.username,
                password=item.password,
                group=default_group,
                managed_by=PluginProxyOwner(
                    plugin_id=plugin_id, source=source, provider_id=item.provider_id
                ),
            )
        )
        counts["added"] += 1
    return result, counts, changed


def sync_plugin_proxies(platform, plugin_id, request):
    from ..api.routes.proxies import _persist_proxy_config

    def require_permission():
        record = platform.require_scope(plugin_id, "proxy_sources", request.source)
        if not platform._has_capability(record, PluginCapability.PROXIES_SYNC):
            raise PermissionError("Proxy sync permission is required")

    fingerprint = hashlib.sha256(
        json.dumps(request.model_dump(), sort_keys=True).encode()
    ).hexdigest()
    receipt_key = plugin_id + ":" + request.source
    with proxy_assignment_lock():
        require_permission()
        # Always load after acquiring the assignment lock, never apply to the
        # request dependency's potentially stale settings object.
        config = UltimaScraperCollectionConfig.load_or_create_default_config()
        network = config.settings.network
        previous = network.plugin_sync_receipts.get(receipt_key)
        if previous and previous["idempotency_key"] == request.idempotency_key:
            if previous["fingerprint"] != fingerprint:
                raise ValueError("Proxy sync key was already used for another snapshot")
            accounts = assigned_accounts(
                {
                    proxy.url
                    for proxy in network.proxies
                    if proxy.managed_by
                    and proxy.managed_by.plugin_id == plugin_id
                    and proxy.managed_by.source == request.source
                }
            )
            return config, previous["result"], accounts, True
        accounts = assigned_accounts({proxy.url for proxy in network.proxies})
        proxies, counts, changed = reconcile_proxies(
            network.proxies,
            request.proxies,
            plugin_id=plugin_id,
            source=request.source,
            default_group=request.default_group,
            accounts=accounts,
        )
        require_permission()
        network.proxies = proxies
        result = {"status": "success", **counts}
        network.plugin_sync_receipts[receipt_key] = {
            "idempotency_key": request.idempotency_key,
            "fingerprint": fingerprint,
            "result": result,
        }
        _persist_proxy_config(config)
        return (
            config,
            result,
            [item for item in accounts if item["proxy_id"] in changed],
            False,
        )
