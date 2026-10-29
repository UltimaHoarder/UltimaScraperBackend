"""Plugin tasks admitted to the existing SQL execution ledger."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from uuid import uuid4

from sqlalchemy import text
from ultima_scraper_db_archive.global_db import CentralJobModel

from ..database.task_admission import create_task_plan
from ..database.threads import run_database_call
from ..job_system.ledger import ExecutionLedger, StaleAttempt
from ..job_system.models import Job, Task, TaskResult, TaskState, TaskType
from .manifest import load_manifest_from_dict
from .permissions import PluginCapability
from .sdk import SDK_VERSION, validate_schema


def validate_task(platform, plugin_id, task_type, payload):
    if len(json.dumps(payload).encode()) > 1024 * 1024:
        raise ValueError("Plugin task input exceeds 1 MiB")
    platform.submit_job(plugin_id, task_type, payload)
    record = platform._require_enabled(plugin_id)
    definition = next(
        item for item in record.manifest.task_types if item.task_type == task_type
    )
    if definition.handler is None:
        raise ValueError(f"Task {task_type!r} has no execution adapter")
    if record.manifest.sdk:
        platform.require_scope(plugin_id, "task_types", task_type)
    if definition.input_schema:
        validate_schema(definition.input_schema, payload)
    return record, definition


def submit_execution(platform, plugin_id, task_type, payload, *, idempotency_key=None):
    record, _definition = validate_task(platform, plugin_id, task_type, payload)
    if platform.repository.storage_kind != "postgresql":
        raise RuntimeError("Plugin task execution requires PostgreSQL")
    request_hash = hashlib.sha256(
        json.dumps(
            {"task_type": task_type, "payload": payload}, sort_keys=True
        ).encode()
    ).hexdigest()
    identity = hashlib.sha256(
        f"{plugin_id}\0{idempotency_key or uuid4().hex}".encode()
    ).hexdigest()[:32]
    job_id = f"plugin-{identity}"
    digest = platform.package_digest(record)
    task = Task(
        user_id=payload.get("user_id") or 0,
        site_name=str(payload.get("site", "plugin")),
        task_type=TaskType.PLUGIN,
        content_type=task_type,
        capabilities=[f"plugin:{plugin_id}"],
        max_attempts=3,
        payload={
            "plugin_id": plugin_id,
            "task_type": task_type,
            "package_digest": digest,
            "input": payload,
        },
    )
    job = Job.create(
        name=f"{record.manifest.display_name}: {task_type}",
        created_by=f"plugin:{plugin_id}",
        tasks=[task],
    )
    job.job_id = job_id
    job.metadata.update(
        plugin_id=plugin_id,
        plugin_version=record.manifest.plugin_version,
        plugin_package_digest=digest,
        plugin_request_hash=request_hash,
    )
    with platform.repository.session_factory() as session:
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
            {"identity": job_id},
        )
        existing = session.get(CentralJobModel, job_id)
        if existing:
            config = json.loads(existing.job_config_json)
            if config.get("metadata", {}).get("plugin_request_hash") != request_hash:
                raise ValueError(
                    "Idempotency key was already used for another plugin request"
                )
            return {
                "job_id": job_id,
                "plugin_id": plugin_id,
                "task_type": task_type,
                "reused": True,
            }
        create_task_plan(
            session,
            job_id=job_id,
            job_type="plugin",
            site=task.site_name,
            job_config=job.to_config(include_task_ids=False).to_dict(),
            args_payload={},
            tasks=[task],
            batch_size=1,
            max_bytes=2 * 1024 * 1024,
        )
        session.commit()
    return {
        "job_id": job_id,
        "plugin_id": plugin_id,
        "task_type": task_type,
        "plugin_version": record.manifest.plugin_version,
        "reused": False,
    }


async def _bounded_read(stream, limit):
    chunks = []
    size = 0
    while chunk := await stream.read(min(65536, limit + 1 - size)):
        chunks.append(chunk)
        size += len(chunk)
        if size > limit:
            raise ValueError("Plugin process output exceeds its response limit")
    return b"".join(chunks)


async def invoke_task(
    platform, plugin_id, task_type, payload, *, digest=None, idempotency_key=None
):
    current = platform._require_enabled(plugin_id)
    if not platform._has_capability(current, PluginCapability.JOBS_SUBMIT):
        raise PermissionError("Plugin task permission was revoked")
    if digest:
        snapshot = platform.repository.get_package(plugin_id, digest)
        if snapshot is None:
            raise ValueError("Pinned plugin package is missing")
        manifest = load_manifest_from_dict(snapshot)
        definition = next(
            (item for item in manifest.task_types if item.task_type == task_type), None
        )
        if definition is None or definition.handler is None:
            raise ValueError("Pinned plugin task adapter is missing")
    else:
        current, definition = validate_task(platform, plugin_id, task_type, payload)
        manifest = current.manifest
    if current.manifest.sdk:
        platform.require_scope(plugin_id, "task_types", task_type)
    if definition.input_schema:
        validate_schema(definition.input_schema, payload)
    handler = definition.handler
    configuration = platform.configuration(manifest)
    if manifest.sdk:
        for name, field in manifest.sdk.configuration_schema.get(
            "properties", {}
        ).items():
            if isinstance(field, dict) and "default" in field:
                configuration.setdefault(name, field["default"])
        validate_schema(
            manifest.sdk.configuration_schema, configuration, name="configuration"
        )
    if handler.kind == "http":
        url = handler.url
        # Configuration placeholders are confined to explicit, validated
        # string values. The resolved URL still passes both network policies.
        for name, value in configuration.items():
            if isinstance(value, str):
                url = url.replace("${config." + name + "}", value)
        await platform._validate_http_target(manifest, url)
        await platform._validate_http_target(current.manifest, url)
        platform.require_outbound_scope(plugin_id, url)
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key or uuid4().hex,
        }
        if handler.auth_scope:
            secret = platform.secrets.get_raw_secret(
                plugin_id, "task", handler.auth_scope
            )
            if not secret:
                raise ValueError(f"Missing task credential: {handler.auth_scope}")
            headers["Authorization"] = f"Bearer {secret}"
        response = await platform._http_request(
            "POST",
            url,
            json.dumps(payload).encode(),
            headers,
            timeout=handler.timeout_seconds,
        )
        if not 200 <= response.status_code < 300:
            raise ValueError(
                f"Plugin task endpoint returned HTTP {response.status_code}"
            )
        if len(response.body) > handler.response_limit_bytes:
            raise ValueError("Plugin task response exceeds its response limit")
        output = json.loads(response.body)
    else:
        if not platform._has_capability(current, PluginCapability.RUNTIME_PROCESS):
            raise PermissionError("Trusted process execution permission is required")
        directory = platform.package_directory(
            plugin_id, digest or platform.package_digest(current)
        )
        # Process adapters are explicitly trusted code. They receive no host
        # environment credentials and communicate through bounded JSON stdio.
        env = {
            key: os.environ[key]
            for key in ("PATH", "SYSTEMROOT", "LANG")
            if key in os.environ
        }
        broker_token = platform.repository.issue_api_token(
            plugin_id, seconds=handler.timeout_seconds + 10
        )
        host = os.environ.get("CONTROL_HOST", "127.0.0.1")
        host = "127.0.0.1" if host == "0.0.0.0" else "[::1]" if host == "::" else host
        broker_url = os.environ.get(
            "USR_PLUGIN_API_BASE_URL",
            f"http://{host}:{os.environ.get('CONTROL_PORT', '21121')}/api/v1",
        )
        try:
            command = list(handler.command)
            if command[0] in {"python", "python3"}:
                command[0] = sys.executable
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=directory,
                env=env,
                start_new_session=os.name == "posix",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except BaseException:
            platform.repository.revoke_api_token(broker_token)
            raise
        try:
            async with asyncio.timeout(handler.timeout_seconds):
                process.stdin.write(
                    json.dumps(
                        {
                            "sdk_version": SDK_VERSION,
                            "task_type": task_type,
                            "input": payload,
                            "configuration": configuration,
                            "idempotency_key": idempotency_key,
                            "broker": {"base_url": broker_url, "token": broker_token},
                        }
                    ).encode()
                    + b"\n"
                )
                await process.stdin.drain()
                process.stdin.close()
                stdout, _ = await asyncio.gather(
                    _bounded_read(process.stdout, handler.response_limit_bytes),
                    _bounded_read(process.stderr, 65536),
                )
                await process.wait()
                if process.returncode:
                    raise ValueError(
                        f"Plugin process exited with code {process.returncode}"
                    )
                output = json.loads(stdout)
        finally:
            platform.repository.revoke_api_token(broker_token)
            if os.name == "posix":
                import signal

                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.returncode is None:
                process.kill()
            if process.returncode is None:
                await process.wait()
    if definition.output_schema:
        validate_schema(definition.output_schema, output, name="result")
    return output


class PluginTaskRunner:
    def __init__(self, platform, *, ledger=None):
        self.platform = platform
        self.ledger = ledger or ExecutionLedger(platform.repository.session_factory)
        self.owner = f"plugin-supervisor-{uuid4().hex}"

    async def execute(self, claim):
        task = Task.from_dict(claim.task)
        data = task.payload
        operation = asyncio.create_task(
            invoke_task(
                self.platform,
                data["plugin_id"],
                data["task_type"],
                data["input"],
                digest=data["package_digest"],
                idempotency_key=f"{claim.run_id}:{claim.task_id}",
            )
        )
        try:
            while not operation.done():
                await asyncio.wait({operation}, timeout=1)
                self.platform._require_enabled(data["plugin_id"])
                await run_database_call(
                    self.ledger.progress, claim, task.to_dict(), renew=True
                )
            output = await operation
            task.state = TaskState.COMPLETED
            task.result = TaskResult(success=True, output=output)
        except asyncio.CancelledError, StaleAttempt:
            raise
        except Exception as exc:  # noqa: BLE001 - contain failures at the plugin execution boundary
            task.state = TaskState.FAILED
            task.result = TaskResult(success=False, message=str(exc), errors=[str(exc)])
        finally:
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
        task.completed_at = task.result.completed_at
        await run_database_call(self.ledger.finish, claim, task)

    async def loop(self):
        from ultima_scraper_api import get_enabled_sites

        active = {}
        try:
            while self.platform._runtime_started:
                active = {
                    item: plugin_id
                    for item, plugin_id in active.items()
                    if not item.done()
                }
                for record in self.platform.registry.list_enabled():
                    if not self.platform._has_capability(
                        record, PluginCapability.JOBS_SUBMIT
                    ):
                        continue
                    capacity = min(
                        8 - len(active),
                        record.manifest.operational_policy.task_concurrency
                        - sum(value == record.plugin_id for value in active.values()),
                    )
                    if capacity <= 0:
                        continue
                    claims = await run_database_call(
                        self.ledger.claim_many,
                        owner=self.owner,
                        backend_id=getattr(
                            self.platform._backend, "instance_id", "default"
                        ),
                        capabilities=[
                            "plugin",
                            *(site.lower() for site in get_enabled_sites()),
                            f"plugin:{record.plugin_id}",
                        ],
                        lane="plugin",
                        count=capacity,
                    )
                    for claim in claims:
                        task = asyncio.create_task(self.execute(claim))
                        task.add_done_callback(_observe_execution)
                        active[task] = record.plugin_id
                await asyncio.sleep(0.25)
        finally:
            for task in active:
                task.cancel()
            await asyncio.gather(*active, return_exceptions=True)


def _observe_execution(task):
    import logging

    if (
        not task.cancelled()
        and (error := task.exception()) is not None
        and not isinstance(error, StaleAttempt)
    ):
        logging.getLogger(__name__).error("Plugin execution failed", exc_info=error)
