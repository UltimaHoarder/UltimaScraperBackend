"""Proxy URL parsing helpers."""

from urllib.parse import urlparse

_DEFAULT_PORTS = {
    "http": 80,
    "https": 443,
    "socks4": 1080,
    "socks4a": 1080,
    "socks5": 1080,
    "socks5h": 1080,
}


def resolve_proxy_target(proxy_url: str) -> tuple[str, int]:
    """Return the host and effective port for a supported proxy URL."""

    parsed = urlparse(proxy_url)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError("Proxy URL must include scheme and host")

    if parsed.scheme not in _DEFAULT_PORTS and parsed.port is None:
        raise ValueError("Proxy URL scheme is not supported")

    port = parsed.port or _DEFAULT_PORTS.get(parsed.scheme)
    if port is None:
        raise ValueError("Proxy URL must include a port")

    return parsed.hostname, port
