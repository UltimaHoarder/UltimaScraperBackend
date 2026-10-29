"""Webhook configuration API routes."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ...config_settings_sync import ConfigVersionConflict, ConfigVersionRequired, persist_config_snapshot
from ...security import validate_outbound_url
from ...webhook_config import HOOK_CATALOG, HookDefinition, SiteName, SiteWebhookRouting, effective_site_routing
from ..dependencies import get_config

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
logger = logging.getLogger(__name__)


class WebhookConfigurationResponse(BaseModel):
    version: int | None
    catalog: list[HookDefinition]
    sites: dict[SiteName, SiteWebhookRouting]


class WebhookConfigurationUpdate(BaseModel):
    expected_version: int = Field(ge=1)
    routing: SiteWebhookRouting


class WebhookInfo(BaseModel):
    """Webhook configuration details."""

    active: bool
    url: str | None = None
    hide_info: list[str] = Field(default_factory=list)


class WebhooksInfo(BaseModel):
    """Per-site webhook configuration."""

    auth: WebhookInfo
    download: WebhookInfo


class WebhookSitesResponse(BaseModel):
    """Webhook settings for all sites."""

    onlyfans: WebhooksInfo
    fansly: WebhooksInfo


class WebhooksResponse(BaseModel):
    """Webhook response wrapper."""

    sites: WebhookSitesResponse
    configuration: WebhookConfigurationResponse


def _get_site_webhooks(site_name: str, config=None) -> WebhooksInfo:
    config = config or get_config()
    if getattr(config.settings.webhook_routing, site_name) is not None:
        routing = effective_site_routing(config, site_name)
        hooks = {}
        for name in ("auth", "download"):
            rules = [rule for rule in routing.rules if name in rule.hooks]
            destinations = [destination for destination in routing.destinations if any(destination.id in rule.destination_ids for rule in rules)]
            hooks[name] = WebhookInfo(active=any(rule.active for rule in rules), url=destinations[0].url if destinations else None, hide_info=sorted({field for rule in rules for field in rule.hide_info}))
        return WebhooksInfo(**hooks)
    site_config = config.get_site_config(site_name)
    webhooks = site_config.webhooks
    return WebhooksInfo(
        auth=WebhookInfo(
            active=webhooks.auth.active,
            url=webhooks.auth.url,
            hide_info=list(webhooks.auth.hide_info),
        ),
        download=WebhookInfo(
            active=webhooks.download.active,
            url=webhooks.download.url,
            hide_info=list(webhooks.download.hide_info),
        ),
    )


def _configuration_response(config) -> WebhookConfigurationResponse:
    return WebhookConfigurationResponse(
        version=config._settings_version,
        catalog=HOOK_CATALOG,
        sites={site: effective_site_routing(config, site) for site in ("onlyfans", "fansly")},
    )


@router.get("/", response_model=WebhooksResponse)
async def get_webhooks() -> WebhooksResponse:
    """Get webhook settings for all sites."""
    config = get_config()
    return WebhooksResponse(
        sites=WebhookSitesResponse(
            onlyfans=_get_site_webhooks("onlyfans", config),
            fansly=_get_site_webhooks("fansly", config),
        ),
        configuration=_configuration_response(config),
    )


@router.put("/configuration/{site}", response_model=WebhookConfigurationResponse)
async def update_webhook_configuration(site: SiteName, update: WebhookConfigurationUpdate) -> WebhookConfigurationResponse:
    """Save all hooks and destinations for one site with revision protection."""
    try:
        update.routing.validate_site(site)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    for destination in update.routing.destinations:
        await asyncio.to_thread(validate_outbound_url, destination.url)

    def persist():
        config = get_config()
        if not config.is_configured or config._settings_version is None:
            raise HTTPException(status_code=409, detail="Complete backend setup before saving webhook configuration.")
        if config._settings_version != update.expected_version:
            raise HTTPException(status_code=409, detail="Configuration changed. Reload the configuration and apply your changes again.")
        setattr(config.settings.webhook_routing, site, update.routing.model_copy(deep=True))
        try:
            persist_config_snapshot(config, expected_version=update.expected_version)
        except (ConfigVersionConflict, ConfigVersionRequired) as exc:
            raise HTTPException(status_code=409, detail="Configuration changed. Reload the configuration and apply your changes again.") from exc
        return _configuration_response(config)

    return await asyncio.to_thread(persist)
