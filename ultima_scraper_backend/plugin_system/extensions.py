"""Host-owned setup, scoped GET services and durable recurring plugin tasks."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode
from uuid import uuid4

from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from ultima_scraper_db_archive.global_db import CentralJobModel

from ..database.threads import run_database_call
from .permissions import PluginCapability
from .sdk import validate_schema
from .secrets import PluginSecret

logger = logging.getLogger(__name__)


class PluginExtensions:
    def setup_description(self, plugin_id):
        manifest = self._require_known(plugin_id).manifest
        setup = manifest.sdk.setup if manifest and manifest.sdk else None
        credentials = []
        if setup:
            credentials = [
                {
                    **field.model_dump(),
                    "configured": bool(
                        self.secrets.get_raw_secret(plugin_id, "setup", field.name)
                    ),
                }
                for field in setup.credentials
            ]
        if manifest and manifest.remote_delegation:
            for scope in manifest.remote_delegation.auth_scopes:
                credentials.append({
                    'name': 'remote_auth:' + scope, 'scope_kind': 'remote_auth', 'scope_name': scope,
                    'title': scope.replace('_', ' ').title() + ' credential',
                    'description': 'Authentication for the remote service, stored as a managed secret.',
                    'required': True, 'configured': bool(self.secrets.get_raw_secret(plugin_id, 'remote_auth', scope)),
                })
        schema = self.configuration_schema(manifest) if manifest else None
        values = self.configuration(manifest) if manifest else {}
        ready = all(
            not field["required"] or field["configured"] for field in credentials
        )
        if schema:
            try:
                validate_schema(schema, values, name="configuration")
            except ValueError:
                ready = False
        installation = self.repository.get_host_document(plugin_id, 'installation')
        if installation.get('source') and not installation.get('setup_completed'):
            ready = False
        return {
            "required": setup is not None or schema is not None or bool(credentials) or bool(installation.get('source')),
            "ready": ready,
            "schema": schema,
            "values": values,
            "credentials": credentials,
        }

    def _require_http_service(self, plugin_id, service_id, *, setup=False):
        record = (
            self._require_known(plugin_id)
            if setup
            else self._require_enabled(plugin_id)
        )
        if not record.manifest or not record.manifest.sdk:
            raise PermissionError("HTTP services require an SDK installation")
        if not self._has_capability(record, PluginCapability.HTTP_REQUEST):
            raise PermissionError("HTTP request permission is required")
        requested = record.manifest.sdk.access.http_services
        granted = self.repository.get_document("plugin_scope_grants", plugin_id) or {}
        if service_id not in requested or service_id not in granted.get(
            "http_services", []
        ):
            raise PermissionError("HTTP service is outside the approved scope")
        service = next(
            (
                item
                for item in record.manifest.sdk.http_services
                if item.service_id == service_id
            ),
            None,
        )
        if service is None:
            raise PermissionError("HTTP service is unavailable")
        return record, service

    async def scoped_http_get(
        self, plugin_id, service_id, query, *, setup=False, credentials=None
    ):
        record, service = self._require_http_service(plugin_id, service_id, setup=setup)
        validate_schema(service.query_schema, query, name="HTTP query")
        url = service.url + ("?" + urlencode(query) if query else "")
        await self._validate_http_target(
            record.manifest, url, require_enabled=not setup
        )
        # require_outbound_scope also works for disabled installations.
        self.require_outbound_scope(plugin_id, url, require_enabled=not setup)
        headers = {"Accept": "application/json"}
        if service.credential:
            secret = (credentials or {}).get(
                service.credential
            ) or self.secrets.get_raw_secret(plugin_id, "setup", service.credential)
            if not secret:
                raise ValueError("Required service credential is missing")
            if "\n" in secret or "\r" in secret:
                raise ValueError("Invalid service credential")
            headers["Authorization"] = f"{service.auth_scheme} {secret}"
        try:
            response = await self._http_request(
                "GET", url, None, headers, timeout=service.timeout_seconds
            )
        except Exception:  # noqa: BLE001 - sanitize credential-bearing transport errors
            # Transport exceptions may include the request or response. Never
            # expose these through an operator message or a ledger task result.
            raise ValueError("Plugin service request failed") from None
        if not 200 <= response.status_code < 300:
            raise ValueError(f"Plugin service returned HTTP {response.status_code}")
        if len(response.body) > service.response_limit_bytes:
            raise ValueError("Plugin service response exceeds its limit")
        try:
            result = json.loads(response.body)
        except ValueError, UnicodeError:
            raise ValueError("Plugin service returned invalid JSON") from None
        return result

    async def save_plugin_setup(
        self, plugin_id, configuration, credentials, *, enable=False
    ):
        record = self._require_known(plugin_id)
        setup = (
            record.manifest.sdk.setup
            if record.manifest and record.manifest.sdk
            else None
        )
        if record.manifest is None or self.is_uninstalled(plugin_id):
            raise ValueError('Plugin has no installed manifest')
        values = self.configuration(record.manifest)
        values.update(configuration)
        schema = self.configuration_schema(record.manifest)
        if schema is not None:
            validate_schema(schema, values, name="configuration")
        elif configuration:
            raise ValueError('Plugin does not declare configuration')
        description = self.setup_description(plugin_id)
        declared = {field['name']: field for field in description['credentials']}
        if not set(credentials) <= set(declared):
            raise ValueError("Unknown setup credential")
        candidates = {
            name: value.strip() for name, value in credentials.items() if value.strip()
        }
        if any(
            len(value) > 8192 or "\r" in value or "\n" in value
            for value in candidates.values()
        ):
            raise ValueError("Invalid setup credential")
        for field in declared.values():
            if field['required'] and not (candidates.get(field['name']) or field['configured']):
                raise ValueError(f"Required credential missing: {field['title']}")
        preflight = self.preflight_plugin(plugin_id)
        if preflight.get('pending_capabilities') or preflight.get('pending_scopes'):
            raise PermissionError('Approve requested permissions during setup first')
        if setup and setup.validation_service:
            await self.scoped_http_get(
                plugin_id,
                setup.validation_service,
                setup.validation_query,
                setup=True,
                credentials=candidates,
            )
        payloads = [
            PluginSecret(plugin_id, declared[name].get('scope_kind', 'setup'), declared[name].get('scope_name', name), value).to_storage_dict()
            for name, value in candidates.items()
        ]
        await run_database_call(self.repository.save_setup, plugin_id, values, payloads)
        await run_database_call(self.repository.update_host_document, plugin_id, 'installation', lambda old: {**old, 'setup_completed': True})
        if enable:
            return await run_database_call(self.enable_plugin, plugin_id)
        return self._plugin_view(record)

    def schedule_status(self, plugin_id):
        manifest = self._require_known(plugin_id).manifest
        if not manifest or not manifest.sdk:
            return []
        result = []
        for schedule in manifest.sdk.schedules:
            state = self.repository.get_host_document(
                plugin_id, "schedule:" + schedule.schedule_id
            )
            result.append(
                {
                    "schedule_id": schedule.schedule_id,
                    "task_type": schedule.task_type,
                    **state,
                }
            )
        return result

    def _scheduled_job(self, plugin_id, job_id=None):
        """A manual or recurring task already queued for this installation."""
        with self.repository.session_factory() as session:
            if job_id:
                job = session.get(CentralJobModel, job_id)
            else:
                job = session.scalar(
                    select(CentralJobModel)
                    .where(
                        CentralJobModel.job_type == "plugin",
                        CentralJobModel.status.not_in(
                            ["completed", "failed", "cancelled"]
                        ),
                        cast(CentralJobModel.job_config_json, JSONB)["metadata"][
                            "plugin_id"
                        ].astext
                        == plugin_id,
                    )
                    .order_by(CentralJobModel.created_at)
                    .limit(1)
                )
            if not job:
                return None
            return {"job_id": job.job_id, "status": job.status}

    def submit_schedule(self, plugin_id, schedule_id, *, force=False):
        record = self._require_enabled(plugin_id)
        if not self._has_capability(record, PluginCapability.JOBS_SCHEDULE):
            raise PermissionError("Scheduling permission is required")
        schedule = next(
            (
                item
                for item in record.manifest.sdk.schedules
                if item.schedule_id == schedule_id
            ),
            None,
        )
        if schedule is None:
            raise ValueError("Unknown plugin schedule")
        values = self.configuration(record.manifest)
        validate_schema(
            record.manifest.sdk.configuration_schema, values, name="configuration"
        )
        interval = values[schedule.interval_setting]
        now = self.repository.now()

        def reserve(state):
            # The pending key is saved before job admission. A crash between
            # admission and saving job_id reuses the same deterministic job.
            if state.get("pending_key"):
                job = (
                    self._scheduled_job(plugin_id, state.get("job_id"))
                    if state.get("job_id")
                    else None
                )
                if job is None or job["status"] not in {
                    "completed",
                    "failed",
                    "cancelled",
                }:
                    return state
                state["last_result"] = {
                    **(
                        state.get("last_result", {})
                        if job["status"] == "completed"
                        else {}
                    ),
                    **job,
                }
                state.pop("pending_key", None)
                state.pop("job_id", None)
            active = self._scheduled_job(plugin_id)
            if active:
                return {**state, "blocked_by_job": active["job_id"]}
            state.pop("blocked_by_job", None)
            if state.get("last_submitted_at"):
                state["next_at"] = (
                    datetime.fromisoformat(state["last_submitted_at"])
                    + timedelta(seconds=interval)
                ).isoformat()
            if (
                not force
                and state.get("next_at")
                and datetime.fromisoformat(state["next_at"]) > now
            ):
                return state
            return {
                **state,
                "pending_key": "schedule:" + uuid4().hex,
                "last_submitted_at": now.isoformat(),
                "next_at": (now + timedelta(seconds=interval)).isoformat(),
            }

        state = self.repository.update_host_document(
            plugin_id, "schedule:" + schedule_id, reserve
        )
        if state.get("blocked_by_job"):
            return {"job_id": state["blocked_by_job"], "reused": True}
        if not state.get("pending_key"):
            return None
        result = self.submit_execution(
            plugin_id, schedule.task_type, {}, idempotency_key=state["pending_key"]
        )

        def admitted(current):
            if current.get("pending_key") == state["pending_key"]:
                current["job_id"] = result["job_id"]
            return current

        self.repository.update_host_document(
            plugin_id, "schedule:" + schedule_id, admitted
        )
        return result

    async def schedule_loop(self):
        while self._runtime_started:
            if self.repository.storage_kind == "postgresql":
                for record in self.registry.list_enabled():
                    if not record.manifest.sdk or not self._has_capability(
                        record, PluginCapability.JOBS_SCHEDULE
                    ):
                        continue
                    for schedule in record.manifest.sdk.schedules:
                        try:
                            await run_database_call(
                                self.submit_schedule,
                                record.plugin_id,
                                schedule.schedule_id,
                            )
                        except PermissionError, ValueError:
                            logger.warning(
                                "Plugin schedule %s/%s is unavailable",
                                record.plugin_id,
                                schedule.schedule_id,
                            )
            await asyncio.sleep(5)
