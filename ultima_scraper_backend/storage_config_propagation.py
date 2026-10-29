"""Backend-scoped storage configuration propagation.

Drive mutations are persisted by the API before this module is called.  The
helpers below publish the exact non-secret storage payload to workers owned by
the same backend and wait briefly for revision-matched acknowledgements.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from typing import Any, Literal, cast
from uuid import uuid4

from pydantic import BaseModel, Field

from .config import UltimaScraperCollectionConfig
from .models.backend import BackendInstance
from .redis.worker_registry import WorkerRegistry
from .redis_channels import CONFIG_EVENTS_CHANNEL

logger = logging.getLogger(__name__)

StoragePropagationStatus = Literal[
    "acknowledged",
    "partial",
    "pending",
    "no_workers",
    "local_only",
]


class StorageConfigPropagation(BaseModel):
    """Result of delivering one persisted storage configuration revision."""

    status: StoragePropagationStatus
    target_backend_id: str
    settings_scope: str
    config_version: int
    revision: str
    request_id: str
    expected_workers: list[str] = Field(default_factory=list)
    acknowledged_workers: list[str] = Field(default_factory=list)
    failed_workers: list[str] = Field(default_factory=list)
    message: str


def _storage_payload(
    config: UltimaScraperCollectionConfig,
    changed_sections: set[str],
) -> dict[str, object]:
    payload: dict[str, object] = {}
    if "download_setup" in changed_sections:
        payload["download_setup"] = config.settings.download_setup.model_dump(
            mode="json"
        )
    if "metadata_setup" in changed_sections:
        payload["metadata_setup"] = config.settings.metadata_setup.model_dump(
            mode="json"
        )
    return payload


def _payload_revision(payload: dict[str, object]) -> str:
    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _decode_message(raw_message: object) -> dict[str, Any] | None:
    if not isinstance(raw_message, dict):
        return None
    message = cast(dict[str, Any], raw_message)
    if message.get("type") != "message":
        return None
    data = message.get("data")
    if isinstance(data, bytes):
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(data, str):
        return None
    try:
        decoded = json.loads(data)
    except json.JSONDecodeError:
        return None
    return cast(dict[str, Any], decoded) if isinstance(decoded, dict) else None


async def _close_pubsub(pubsub: object) -> None:
    unsubscribe = getattr(pubsub, "unsubscribe", None)
    if callable(unsubscribe):
        result = unsubscribe(CONFIG_EVENTS_CHANNEL)
        if inspect.isawaitable(result):
            await result

    close = getattr(pubsub, "aclose", None) or getattr(pubsub, "close", None)
    if callable(close):
        result = close()
        if inspect.isawaitable(result):
            await result


async def _publish_storage_event(
    backend: BackendInstance,
    event: dict[str, object],
) -> bool:
    try:
        if not backend.redis:
            return False
        await backend.redis.publish(CONFIG_EVENTS_CHANNEL, json.dumps(event))
        return True
    except Exception:
        logger.warning("Could not publish storage config update", exc_info=True)
        return False


async def propagate_storage_config(
    backend: BackendInstance,
    config: UltimaScraperCollectionConfig,
    *,
    changed_sections: set[str],
    config_version: int,
    timeout_seconds: float = 5.0,
) -> StorageConfigPropagation:
    """Publish storage settings and report which current workers applied them."""

    supported_sections = changed_sections & {"download_setup", "metadata_setup"}
    payload = _storage_payload(config, supported_sections)
    revision = _payload_revision(payload)
    request_id = str(uuid4())
    target_backend_id = backend.instance_id
    settings_scope = config.settings.get_backend_id()

    event = {
        "type": "storage_config_updated",
        "target_backend_id": target_backend_id,
        "settings_scope": settings_scope,
        "config_version": config_version,
        "request_id": request_id,
        "revision": revision,
        "changed_fields": sorted(supported_sections),
        **payload,
    }

    if not backend.redis or not backend.redis.is_connected:
        return StorageConfigPropagation(
            status="local_only",
            target_backend_id=target_backend_id,
            settings_scope=settings_scope,
            config_version=config_version,
            revision=revision,
            request_id=request_id,
            message=(
                "Saved locally; workers will verify the persisted config before "
                "their next task."
            ),
        )

    expected_workers: set[str] = set()
    registry_lookup_failed = False
    try:
        worker_info = await WorkerRegistry.get_backend_workers_info(
            cast(Any, backend.redis),
            target_backend_id,
        )
        expected_workers = set(worker_info)
    except Exception:
        registry_lookup_failed = True
        logger.warning(
            "Could not enumerate workers for storage config propagation",
            exc_info=True,
        )

    if registry_lookup_failed:
        published = await _publish_storage_event(backend, event)
        return StorageConfigPropagation(
            status="pending",
            target_backend_id=target_backend_id,
            settings_scope=settings_scope,
            config_version=config_version,
            revision=revision,
            request_id=request_id,
            message=(
                "Saved, but the worker registry could not be read"
                + ("; the update was published" if published else " or published")
                + "; "
                "workers will verify config before their next task."
            ),
        )

    if not expected_workers:
        published = await _publish_storage_event(backend, event)
        return StorageConfigPropagation(
            status="no_workers" if published else "pending",
            target_backend_id=target_backend_id,
            settings_scope=settings_scope,
            config_version=config_version,
            revision=revision,
            request_id=request_id,
            message=(
                "Saved; no workers are currently registered on this backend."
                if published
                else (
                    "Saved, but the update could not be published. Workers will "
                    "verify config before their next task."
                )
            ),
        )

    pubsub: object | None = None
    acknowledged: set[str] = set()
    failed: set[str] = set()
    try:
        redis_client = cast(Any, backend.redis.client)
        pubsub = redis_client.pubsub()
        await cast(Any, pubsub).subscribe(CONFIG_EVENTS_CHANNEL)
        if not await _publish_storage_event(backend, event):
            raise RuntimeError("storage config update could not be published")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.1, timeout_seconds)
        while (acknowledged | failed) != expected_workers:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            raw_message = await cast(Any, pubsub).get_message(
                ignore_subscribe_messages=True,
                timeout=remaining,
            )
            if raw_message is None:
                break
            acknowledgement = _decode_message(raw_message)
            if not acknowledgement:
                continue
            if acknowledgement.get("type") != "storage_config_reload_ack":
                continue
            if acknowledgement.get("request_id") != request_id:
                continue
            if acknowledgement.get("revision") != revision:
                continue
            if acknowledgement.get("config_version") != config_version:
                continue
            if acknowledgement.get("target_backend_id") != target_backend_id:
                continue

            worker_id = str(acknowledgement.get("worker_id") or "").strip()
            if not worker_id or worker_id not in expected_workers:
                continue
            if acknowledgement.get("success") is True:
                acknowledged.add(worker_id)
                failed.discard(worker_id)
            else:
                failed.add(worker_id)
                acknowledged.discard(worker_id)
    except Exception:
        logger.warning(
            "Failed while waiting for storage config acknowledgements",
            exc_info=True,
        )
    finally:
        if pubsub is not None:
            try:
                await _close_pubsub(pubsub)
            except Exception:
                logger.warning("Could not close storage config pubsub", exc_info=True)

    pending = expected_workers - acknowledged - failed
    if acknowledged == expected_workers:
        status: StoragePropagationStatus = "acknowledged"
        message = f"Applied by all {len(acknowledged)} registered worker(s)."
    elif acknowledged or failed:
        status = "partial"
        message = (
            f"Applied by {len(acknowledged)}/{len(expected_workers)} worker(s); "
            f"{len(failed)} failed and {len(pending)} did not acknowledge in time."
        )
    else:
        status = "pending"
        message = (
            "Saved, but no registered worker acknowledged this revision before "
            "the timeout. Workers will verify config again before their next task."
        )

    return StorageConfigPropagation(
        status=status,
        target_backend_id=target_backend_id,
        settings_scope=settings_scope,
        config_version=config_version,
        revision=revision,
        request_id=request_id,
        expected_workers=sorted(expected_workers),
        acknowledged_workers=sorted(acknowledged),
        failed_workers=sorted(failed),
        message=message,
    )
