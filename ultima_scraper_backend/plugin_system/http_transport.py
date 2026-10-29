"""Bounded outbound transports used by declarative plugins.

Redirects are deliberately not followed. A redirect target is a new network
destination and must not bypass the manifest's outbound policy after the
original URL has been validated.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

MAX_PLUGIN_HTTP_REQUEST_BYTES = 2 * 1024 * 1024
MAX_PLUGIN_HTTP_RESPONSE_BYTES = 2 * 1024 * 1024


@dataclass(slots=True)
class HTTPResponse:
    status_code: int
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)


async def default_http_request(
    method: str,
    url: str,
    payload: bytes | None,
    headers: dict[str, str],
    *,
    timeout: float = 30.0,
) -> HTTPResponse:
    """Perform one HTTP request without redirects and with a bounded body."""
    if payload is not None and len(payload) > MAX_PLUGIN_HTTP_REQUEST_BYTES:
        raise ValueError(
            f"Plugin HTTP request exceeds {MAX_PLUGIN_HTTP_REQUEST_BYTES} bytes"
        )

    import httpx

    async with (
        asyncio.timeout(timeout),
        httpx.AsyncClient(
            follow_redirects=False, trust_env=False, timeout=timeout
        ) as client,
        client.stream(method.upper(), url, content=payload, headers=headers) as response,
    ):
        chunks = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > MAX_PLUGIN_HTTP_RESPONSE_BYTES:
                raise ValueError(
                    f"Plugin HTTP response exceeds {MAX_PLUGIN_HTTP_RESPONSE_BYTES} bytes"
                )
            chunks.append(chunk)
        return HTTPResponse(
            response.status_code, b"".join(chunks), dict(response.headers)
        )


async def default_http_post(
    url: str,
    payload: bytes,
    headers: dict[str, str],
    *,
    timeout: float = 30.0,
) -> int:
    response = await default_http_request(
        "POST",
        url,
        payload,
        headers,
        timeout=timeout,
    )
    return response.status_code
