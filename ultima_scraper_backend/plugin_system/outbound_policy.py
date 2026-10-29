"""Manifest-scoped outbound network policy enforcement."""

from __future__ import annotations

from urllib.parse import urlsplit

from fastapi import HTTPException

from ultima_scraper_backend.security import validate_outbound_url

from .manifest import PluginManifest


def validate_plugin_http_url(manifest: PluginManifest, url: str) -> None:
    """Validate an HTTP target against the plugin's operational policy."""
    parsed = urlsplit(url)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Plugin outbound URLs may not contain userinfo credentials")
    policy = manifest.operational_policy
    if parsed.scheme == "http" and not policy.allow_insecure_http:
        raise ValueError(
            f"Cleartext outbound URL blocked for plugin {manifest.plugin_id!r}; "
            "set operational_policy.allow_insecure_http only for a trusted target"
        )
    try:
        validate_outbound_url(
            url,
            allowed_domains=policy.allowed_outbound_domains,
            allow_private=policy.allow_private_networks,
        )
    except HTTPException as exc:
        raise ValueError(
            f"Outbound URL blocked for plugin {manifest.plugin_id!r}: {exc.detail}"
        ) from exc


def validate_plugin_websocket_url(manifest: PluginManifest, url: str) -> None:
    """Apply the HTTP policy to a websocket destination."""
    if url.startswith("wss://"):
        policy_url = "https://" + url.removeprefix("wss://")
    elif url.startswith("ws://"):
        policy_url = "http://" + url.removeprefix("ws://")
    else:
        raise ValueError("Plugin websocket URL must use ws:// or wss://")
    validate_plugin_http_url(manifest, policy_url)
