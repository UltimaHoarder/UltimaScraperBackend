"""Site webhook subscriptions and their reusable destinations."""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SiteName = Literal["onlyfans", "fansly"]
HookName = Literal[
    "auth", "download", "chat_message", "subscriber", "post_published",
    "post_updated", "stream_started", "comment", "price_changed", "online",
    "site_message",
]


class HookDefinition(BaseModel):
    id: HookName
    label: str
    description: str
    sites: list[SiteName]
    group: Literal["Account", "Downloads", "Site messages"]


HOOK_CATALOG = [
    HookDefinition(id="auth", label="Auth", description="Accounts added, updated, or removed.", sites=["onlyfans", "fansly"], group="Account"),
    HookDefinition(id="download", label="Download", description="Completed or failed download batches.", sites=["onlyfans", "fansly"], group="Downloads"),
    HookDefinition(id="chat_message", label="Chat messages", description="Incoming API chat messages.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="subscriber", label="New subscribers", description="New subscriptions, including trial subscribers.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="post_published", label="Published posts", description="Notifications when a post is published.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="post_updated", label="Updated posts", description="Notifications when a post changes.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="stream_started", label="Live streams", description="New stream notifications.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="comment", label="Comments", description="New comment notifications.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="price_changed", label="Price changes", description="Subscription price changes.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="online", label="Online activity", description="User online notifications.", sites=["onlyfans"], group="Site messages"),
    HookDefinition(id="site_message", label="All site messages", description="All incoming site notifications except connection keepalives.", sites=["onlyfans"], group="Site messages"),
]


class WebhookDestination(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(min_length=1, max_length=100)
    url: str = Field(min_length=1, max_length=2048)
    format: Literal["json", "discord"] = "json"

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        try:
            parsed = urlsplit(value)
            parsed.port
        except ValueError as exc:
            raise ValueError("Enter a valid HTTP or HTTPS destination URL.") from exc
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            raise ValueError("Use an HTTP or HTTPS URL without credentials or a fragment.")
        return value


class WebhookRule(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(min_length=1, max_length=100)
    hooks: list[HookName] = Field(min_length=1, max_length=11)
    destination_ids: list[str] = Field(min_length=1, max_length=50)
    active: bool = True
    hide_info: list[str] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def unique_selections(self) -> WebhookRule:
        if len(set(self.hooks)) != len(self.hooks) or len(set(self.destination_ids)) != len(self.destination_ids):
            raise ValueError("Select each hook and destination once.")
        return self


class SiteWebhookRouting(BaseModel):
    model_config = ConfigDict(extra="forbid")
    destinations: list[WebhookDestination] = Field(default_factory=list, max_length=50)
    rules: list[WebhookRule] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def validate_references(self) -> SiteWebhookRouting:
        destination_ids = {destination.id for destination in self.destinations}
        if len(destination_ids) != len(self.destinations) or len({rule.id for rule in self.rules}) != len(self.rules):
            raise ValueError("Destination and rule IDs must be unique.")
        if len({destination.url for destination in self.destinations}) != len(self.destinations):
            raise ValueError("This destination URL is already configured.")
        for rule in self.rules:
            if not set(rule.destination_ids) <= destination_ids:
                raise ValueError("A rule refers to a destination that does not exist.")
        return self

    def validate_site(self, site: SiteName) -> None:
        available = {hook.id for hook in HOOK_CATALOG if site in hook.sites}
        if any(not set(rule.hooks) <= available for rule in self.rules):
            raise ValueError("One or more hooks are unavailable for this site.")


class WebhookRouting(BaseModel):
    onlyfans: SiteWebhookRouting | None = None
    fansly: SiteWebhookRouting | None = None

    @model_validator(mode="after")
    def validate_sites(self) -> WebhookRouting:
        for site in ("onlyfans", "fansly"):
            routing = getattr(self, site)
            if routing is not None:
                routing.validate_site(site)
        return self


def effective_site_routing(config: Any, site: SiteName) -> SiteWebhookRouting:
    """Keep existing URL settings until that site's routing is explicitly saved."""
    routing = getattr(config.settings.webhook_routing, site)
    if routing is not None:
        return routing.model_copy(deep=True)
    legacy = config.get_site_config(site).webhooks
    destinations: list[WebhookDestination] = []
    rules: list[WebhookRule] = []
    for name in ("auth", "download"):
        hook = getattr(legacy, name)
        if not hook.url:
            continue
        destination = next((item for item in destinations if item.url == hook.url), None)
        if destination is None:
            destination = WebhookDestination(id=f"legacy-{name}", name=f"{name.capitalize()} destination", url=hook.url, format="discord")
            destinations.append(destination)
        rules.append(WebhookRule(id=f"legacy-{name}", name=name.capitalize(), hooks=[name], destination_ids=[destination.id], active=hook.active, hide_info=list(hook.hide_info)))
    return SiteWebhookRouting(destinations=destinations, rules=rules)
