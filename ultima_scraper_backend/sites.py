"""The sites the backend offers, from the site API's own lists.

``ENABLED_SITE_KEYS`` are the sites listings, filters and sign-in imports offer
(``ultima_scraper_api.ENABLED_SITES_LITERALS``); ``SUPPORTED_SITE_KEYS`` also
counts sites that have an archive schema but are switched off. Use these
instead of spelling site names out, so a new site appears everywhere at once.
"""

from __future__ import annotations

from ultima_scraper_api import SUPPORTED_SITES, get_enabled_sites, get_site_title

ENABLED_SITE_KEYS: tuple[str, ...] = tuple(site.lower() for site in get_enabled_sites())
SUPPORTED_SITE_KEYS: tuple[str, ...] = tuple(site.lower() for site in SUPPORTED_SITES)


def site_title(site: str) -> str:
    """The site's display name ("OnlyFans") for any spelling of its key;
    an unknown site comes back as given."""
    try:
        return get_site_title(site)
    except ValueError:
        return site
