"""Plugin SDK client using the scoped broker identity, never an admin token."""

from __future__ import annotations

import json
from urllib.parse import quote

import httpx


class PluginClient:
    def __init__(self, base_url: str, token: str, *, timeout: float = 30):
        self._client = httpx.Client(
            base_url=base_url.rstrip("/") + "/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
        )

    @classmethod
    def from_task(cls, task: dict):
        return cls(**task["broker"])

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _request(self, method, path, **kwargs):
        with self._client.stream(
            method, "plugins/runtime/" + path, **kwargs
        ) as response:
            response.raise_for_status()
            chunks = []
            size = 0
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > 2 * 1024 * 1024:
                    raise ValueError("Plugin broker response exceeds 2 MiB")
                chunks.append(chunk)
            return json.loads(b"".join(chunks))

    def profile(self, site: str, user_id: int):
        return self._request("GET", f"profiles/{quote(site, safe='')}/{int(user_id)}")

    def http_get(self, service_id: str, query: dict):
        return self._request("POST", "http/" + quote(service_id, safe=""), json={"query": query})

    def sync_proxies(self, source: str, proxies: list[dict], *, idempotency_key: str, default_group: str = "Webshare"):
        return self._request("POST", "proxies/sync", json={"source": source, "proxies": proxies, "idempotency_key": idempotency_key, "default_group": default_group})

    def get_state(self, key: str):
        return self._request("GET", "state/" + quote(key, safe=""))["value"]

    def set_state(self, key: str, value):
        return self._request("PUT", "state", json={"key": key, "value": value})

    def invoke(self, task_type: str, payload: dict):
        return self._request(
            "POST", "invoke", json={"task_type": task_type, "payload": payload}
        )

    def submit(self, task_type: str, payload: dict, *, idempotency_key: str):
        return self._request(
            "POST",
            "jobs",
            json={
                "task_type": task_type,
                "payload": payload,
                "idempotency_key": idempotency_key,
            },
        )
