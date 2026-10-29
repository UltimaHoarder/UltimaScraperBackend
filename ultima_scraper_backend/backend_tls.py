"""HTTPS for other machines: a second listener next to the plain one.

The API keeps serving plain HTTP where it always has (loopback, or the
deployment's own network for its WebUI and healthcheck). When the backend's
settings say to share it (``backend.share_https``), it also serves HTTPS on
``backend.https_port`` (8443) for other machines. A self-signed certificate is
kept next to the backend's .env and is trusted by its SHA-256 fingerprint,
which the backend reports in its identity.

The environment overrides the settings, for deployments that fix it:
USR_BACKEND_TLS (self-signed or off), USR_BACKEND_TLS_CERT_FILE and
USR_BACKEND_TLS_KEY_FILE (a certificate of your own), CONTROL_TLS_PORT and
USR_ADVERTISE_HOST. Both are read once, at startup.
"""

from __future__ import annotations

import functools
import logging
from pathlib import Path

from ultima_scraper_core import runtime_env_value
from ultima_scraper_core.environment import get_env_path
from ultima_scraper_core.tls import TLSFiles, ensure_self_signed, fingerprint

logger = logging.getLogger(__name__)

DEFAULT_TLS_PORT = 8443
_SELF_SIGNED = "self-signed"


_OFF = {"off", "0", "false", "no"}


class TLSConfigurationError(ValueError):
    pass


@functools.cache
def _settings():
    """This backend's own settings (the backend section), as they were at startup."""
    from .config import BackendSettings, UltimaScraperCollectionConfig

    try:
        return UltimaScraperCollectionConfig.load_or_create_default_config().settings.backend
    except Exception:
        # No database yet: nothing is shared until it is set up.
        logger.debug("Could not read the backend settings for HTTPS", exc_info=True)
        return BackendSettings()


def _mode() -> str:
    return (runtime_env_value("USR_BACKEND_TLS") or "").strip().lower()


def tls_enabled() -> bool:
    """Whether HTTPS is configured, without creating anything."""
    if (runtime_env_value("USR_BACKEND_TLS_CERT_FILE") or "").strip():
        return True
    mode = _mode()
    if mode:
        return mode == _SELF_SIGNED
    return _settings().share_https


def tls_port() -> int:
    raw = (runtime_env_value("CONTROL_TLS_PORT") or "").strip()
    if not raw:
        return _settings().https_port
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid CONTROL_TLS_PORT %r; using %s", raw, DEFAULT_TLS_PORT)
        return DEFAULT_TLS_PORT


def advertise_host() -> str | None:
    """The address other machines reach this host at, when one is set."""
    return (runtime_env_value("USR_ADVERTISE_HOST") or "").strip() or (_settings().advertise_host or "").strip() or None


def _extra_names() -> list[str]:
    names = (runtime_env_value("USR_BACKEND_TLS_HOSTNAMES") or "").split(",")
    return [name.strip() for name in [*names, advertise_host() or ""] if name.strip()]


@functools.cache
def tls_files() -> TLSFiles | None:
    """The certificate to serve; created once, then reused. None without TLS."""
    cert = (runtime_env_value("USR_BACKEND_TLS_CERT_FILE") or "").strip()
    key = (runtime_env_value("USR_BACKEND_TLS_KEY_FILE") or "").strip()
    if cert or key:
        if not (cert and key):
            raise TLSConfigurationError("Set both USR_BACKEND_TLS_CERT_FILE and USR_BACKEND_TLS_KEY_FILE.")
        for path in (cert, key):
            if not Path(path).is_file():
                raise TLSConfigurationError(f"{path} does not exist.")
        return TLSFiles(Path(cert), Path(key))
    mode = _mode()
    if mode in _OFF or (not mode and not _settings().share_https):
        return None
    if mode and mode != _SELF_SIGNED:
        raise TLSConfigurationError("USR_BACKEND_TLS must be self-signed or off, or set the certificate files.")
    return ensure_self_signed(get_env_path().parent / "tls", _extra_names(), common_name="UltimaScraper backend")


@functools.cache
def tls_fingerprint() -> str | None:
    """SHA-256 of the certificate the HTTPS listener serves, for pinning."""
    try:
        files = tls_files()
    except TLSConfigurationError:
        logger.exception("HTTPS is misconfigured")
        return None
    return fingerprint(files.cert_file) if files else None
