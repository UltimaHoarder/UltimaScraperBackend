"""Which sites can run each API Requests command.

Each command names the authenticated-session methods it needs. An enabled site
can run it when the API library's auth model for that site has one of them, so
a site gains commands as soon as the library implements them.
"""

from __future__ import annotations

import inspect
from functools import cache
from typing import Any, get_args

import ultima_scraper_api

from ultima_scraper_backend.sites import ENABLED_SITE_KEYS, site_title


@cache
def auth_model_classes() -> dict[str, type]:
    """Each enabled site's auth model class, keyed by site key."""
    by_name = {cls.__name__.lower(): cls for cls in get_args(ultima_scraper_api.auth_types)}
    classes: dict[str, type] = {}
    for site in ENABLED_SITE_KEYS:
        cls = by_name.get(f"{site}authmodel")
        if cls is not None:
            classes[site] = cls
    return classes


def accepts(func: Any, param: str) -> bool:
    """Whether ``func`` takes a keyword argument called ``param``."""
    try:
        parameters = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return True
    if param in parameters:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())


def has_method(cls: type, requirement: str) -> bool:
    """Whether ``cls`` has the method ``requirement`` names.

    ``"get_chats(search)"`` also requires the method to take ``search``.
    """
    name, _, param = requirement.partition("(")
    method = getattr(cls, name, None)
    if not callable(method):
        return False
    param = param.rstrip(")")
    return not param or accepts(method, param)


@cache
def sites_supporting(requirements: tuple[str, ...]) -> tuple[str, ...]:
    """Enabled sites whose auth model meets any one of ``requirements``."""
    return tuple(
        site
        for site, cls in auth_model_classes().items()
        if any(has_method(cls, requirement) for requirement in requirements)
    )


def enabled_sites() -> list[dict[str, str]]:
    """Every enabled site as ``{"key": "onlyfans", "label": "OnlyFans"}``."""
    return [{"key": site, "label": site_title(site)} for site in ENABLED_SITE_KEYS]
