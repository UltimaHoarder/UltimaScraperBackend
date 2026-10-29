"""SDK administration and scoped operations behind the platform facade."""

from __future__ import annotations

import asyncio
import json
import logging
from urllib.parse import urlsplit

from packaging.specifiers import SpecifierSet
from packaging.version import Version

from .extensions import PluginExtensions
from .installation import PluginInstallations
from .manifest import load_manifest_from_dict
from .permissions import PluginCapability
from .sdk import SDK_VERSION, validate_schema

logger = logging.getLogger(__name__)


def pending_scopes(requested, granted):
    pending = []
    for key, value in requested.items():
        if isinstance(value, list):
            pending.extend(
                f"{key}:{item}" for item in value if item not in granted.get(key, [])
            )
        elif value and not granted.get(key, False):
            pending.append(key)
    return sorted(pending)


class PluginServices(PluginExtensions, PluginInstallations):
    @staticmethod
    def configuration_schema(manifest):
        return (
            manifest.sdk.configuration_schema
            if manifest.sdk
            else manifest.configuration_schema
        )

    def configuration(self, manifest):
        values = (
            self.repository.get_document("plugin_configuration", manifest.plugin_id)
            or {}
        )
        schema = self.configuration_schema(manifest)
        if schema is not None:
            for key, field in schema.get("properties", {}).items():
                if isinstance(field, dict) and "default" in field:
                    values.setdefault(key, field["default"])
        return values

    def configured_url(self, manifest, url, setting=None, *, configuration=None):
        values = (
            self.configuration(manifest) if configuration is None else configuration
        )
        if setting:
            url = values.get(setting)
            if not isinstance(url, str) or not url:
                raise ValueError(f"Setting {setting!r} must contain a URL")
        for key, value in values.items():
            if isinstance(value, str):
                url = url.replace("${config." + key + "}", value)
        if "${config." in url:
            raise ValueError("Plugin URL references an unset setting")
        return url

    def connection_targets(self, manifest, configuration=None, *, resolve=True):
        targets = []
        remote = manifest.remote_delegation
        if remote and remote.external_system_url:
            targets.append(
                {
                    "name": "remote-service",
                    "label": "Remote service",
                    "url": self.configured_url(
                        manifest,
                        remote.external_system_url,
                        remote.external_system_url_setting,
                        configuration=configuration,
                    )
                    if resolve
                    else "",
                }
            )
        for target in manifest.outbound_deliveries:
            targets.append(
                {
                    "name": target.target_name,
                    "label": target.description or target.target_name,
                    "url": self.configured_url(
                        manifest,
                        target.url,
                        target.url_setting,
                        configuration=configuration,
                    )
                    if resolve
                    else "",
                }
            )
        for task in manifest.task_types:
            if task.handler and task.handler.kind == "http":
                targets.append(
                    {
                        "name": task.task_type,
                        "label": task.description or task.task_type,
                        "url": self.configured_url(
                            manifest, task.handler.url, configuration=configuration
                        )
                        if resolve
                        else "",
                    }
                )
        return targets

    def plugin_settings(self, plugin_id):
        record = self._require_known(plugin_id)
        if record.manifest is None:
            raise ValueError("Plugin manifest is unavailable")
        manifest = record.manifest
        return {
            "schema": self.configuration_schema(manifest),
            "values": self.configuration(manifest),
            "connections": [
                {"name": item["name"], "label": item["label"]}
                for item in self.connection_targets(manifest, resolve=False)
            ],
        }

    async def test_plugin_connection(self, plugin_id, target_name, configuration):
        manifest = self._require_known(plugin_id).manifest
        if manifest is None:
            raise ValueError("Plugin manifest is unavailable")
        values = self.configuration(manifest)
        values.update(configuration)
        schema = self.configuration_schema(manifest)
        if schema is not None:
            validate_schema(schema, values, name="configuration")
        target = next(
            (
                item
                for item in self.connection_targets(manifest, values)
                if item["name"] == target_name
            ),
            None,
        )
        if target is None:
            raise ValueError("Unknown plugin connection")
        # A read-only probe uses the same network policy as execution. No
        # credentials, callbacks or job submissions are sent by this action.
        from .outbound_policy import validate_plugin_http_url

        await asyncio.to_thread(validate_plugin_http_url, manifest, target["url"])
        if manifest.sdk:
            self.require_outbound_scope(plugin_id, target["url"], require_enabled=False)
        response = await self._http_request("HEAD", target["url"], None, {}, timeout=5)
        return {
            "reachable": True,
            "status_code": response.status_code,
            "message": f"Reached service (HTTP {response.status_code}); authentication and task execution were not tested.",
        }

    def _load_active_manifest(self, plugin_id, source):
        active = self.repository.active_package(plugin_id)
        if active:
            payload = self.repository.get_package(plugin_id, active["digest"])
            if payload is None:
                raise ValueError("Active plugin package is missing")
            manifest = load_manifest_from_dict(payload)
            if manifest.manifest_schema_version == 2:
                return manifest
        return source

    def package_directory(self, plugin_id, digest):
        from .packages import package_directory

        return package_directory(self, plugin_id, digest)

    def package_digest(self, record):
        from .packages import snapshot_package

        if record.manifest.manifest_schema_version == 2:
            active = self.repository.active_package(record.plugin_id)
            if active:
                return active["digest"]
        return snapshot_package(self, record)

    def sdk_checks(self, manifest):
        if manifest.sdk is None:
            schema = self.configuration_schema(manifest)
            if schema is not None:
                try:
                    validate_schema(
                        schema, self.configuration(manifest), name="configuration"
                    )
                except ValueError as exc:
                    return [
                        {
                            "name": "configuration",
                            "status": "failed",
                            "message": str(exc),
                        }
                    ], []
            return [], []
        access = manifest.sdk.access.model_dump(mode="json")
        granted = (
            self.repository.get_document("plugin_scope_grants", manifest.plugin_id)
            or {}
        )
        pending = pending_scopes(access, granted)
        compatible = Version(SDK_VERSION) in SpecifierSet(manifest.sdk.compatibility)
        checks = [
            {
                "name": "sdk",
                "status": "passed" if compatible else "failed",
                "message": f"Plugin SDK {SDK_VERSION}; requested {manifest.sdk.compatibility}",
            },
            {
                "name": "scopes",
                "status": "approval_required" if pending else "passed",
                "message": "Scope approval required: " + ", ".join(pending)
                if pending
                else "Requested access scopes are approved",
            },
        ]
        if manifest.sdk.setup:
            setup = self.setup_description(manifest.plugin_id)
            checks.append({"name": "setup", "status": "passed" if setup["ready"] else "failed", "message": "Setup complete" if setup["ready"] else "Plugin setup is required"})
        try:
            validate_schema(
                manifest.sdk.configuration_schema,
                self.configuration(manifest),
                name="configuration",
            )
        except ValueError as exc:
            checks.append(
                {"name": "configuration", "status": "failed", "message": str(exc)}
            )
        return checks, pending

    def approve_scopes(self, record):
        if record.manifest.sdk:
            self.repository.set_document(
                "plugin_scope_grants",
                record.plugin_id,
                record.manifest.sdk.access.model_dump(mode="json"),
            )

    def require_scope(self, plugin_id, name, value):
        record = self._require_enabled(plugin_id)
        if record.manifest.sdk is None:
            raise PermissionError("Scoped operations require an SDK manifest")
        requested = getattr(record.manifest.sdk.access, name)
        granted = self.repository.get_document("plugin_scope_grants", plugin_id) or {}
        if value not in requested or value not in granted.get(name, []):
            raise PermissionError(
                f"{name}:{value} is outside the plugin's approved scope"
            )
        return record

    def approved_access(self, record):
        requested = record.manifest.sdk.access.model_dump(mode="json")
        grants = (
            self.repository.get_document("plugin_scope_grants", record.plugin_id) or {}
        )
        result = {
            key: [item for item in value if item in grants.get(key, [])]
            if isinstance(value, list)
            else bool(value and grants.get(key))
            for key, value in requested.items()
        }
        if not self._has_capability(record, PluginCapability.PROFILES_READ):
            result["profile_fields"] = []
        if not self._has_capability(record, PluginCapability.PROFILES_PROPOSE):
            result["propose_profile_changes"] = False
        return result

    def require_outbound_scope(self, plugin_id, url, *, require_enabled=True):
        record = self._require_enabled(plugin_id) if require_enabled else self._require_known(plugin_id)
        if record.manifest.sdk is None:
            return
        granted = self.repository.get_document("plugin_scope_grants", plugin_id) or {}
        requested = record.manifest.sdk.access.outbound_domains
        host = (urlsplit(url).hostname or "").lower()
        if not any(
            host == domain or host.endswith("." + domain)
            for domain in requested
            if domain in granted.get("outbound_domains", [])
        ):
            raise PermissionError(
                f"Outbound host {host!r} is outside the plugin's approved scope"
            )

    def set_configuration(self, plugin_id, configuration):
        record = self._require_known(plugin_id)
        if not record.manifest or self.configuration_schema(record.manifest) is None:
            raise ValueError("Plugin has no configuration contract")
        validate_schema(
            self.configuration_schema(record.manifest),
            configuration,
            name="configuration",
        )
        raw = json.dumps(configuration)
        if len(raw.encode()) > 65536:
            raise ValueError("Plugin configuration exceeds 64 KiB")
        self.repository.set_document("plugin_configuration", plugin_id, configuration)
        self._publish_catalog_change()
        return self._plugin_view(record)

    def stage_upgrade(self, plugin_id, manifest_data):
        from .packages import stage_upgrade

        return stage_upgrade(self, plugin_id, manifest_data)

    def activate_upgrade(self, plugin_id, digest, *, approve_permissions=False):
        record = self._require_known(plugin_id)
        payload = self.repository.get_package(plugin_id, digest)
        if payload is None or not self.package_directory(plugin_id, digest).is_dir():
            raise ValueError("Staged plugin package is missing")
        candidate = load_manifest_from_dict(payload)
        if candidate.plugin_id != plugin_id:
            raise ValueError("Plugin package identity does not match")
        from .sdk import SDK_VERSION

        if Version(self._usr_version) not in SpecifierSet(candidate.usr_compatibility):
            raise ValueError("Staged plugin version is incompatible with USR")
        if candidate.sdk:
            if Version(SDK_VERSION) not in SpecifierSet(candidate.sdk.compatibility):
                raise ValueError("Staged plugin version is incompatible with the SDK")
            validate_schema(
                candidate.sdk.configuration_schema,
                self.configuration(candidate),
                name="configuration",
            )
        requested = {cap.value for cap in candidate.capabilities}
        scopes = candidate.sdk.access.model_dump(mode="json") if candidate.sdk else {}
        if not approve_permissions and (
            requested - self.capability_grants.get(plugin_id)
            or pending_scopes(
                scopes,
                self.repository.get_document("plugin_scope_grants", plugin_id) or {},
            )
        ):
            raise PermissionError("The staged package requires permission approval")
        active = self.repository.active_package(plugin_id)
        self.repository.activate_reviewed_package(
            plugin_id,
            digest,
            requested,
            scopes,
            approve_permissions=approve_permissions,
            expected_digest=active["digest"] if active else None,
            state_quota_bytes=candidate.operational_policy.state_quota_bytes,
        )
        # The manifest catalog retains the operator-visible active declaration;
        # executable assets are always served from the immutable package.
        try:
            self._write_manifest_atomically(record.manifest_path, payload)
        except OSError:
            logger.warning(
                "Activated package %s for %s; source manifest could not be updated",
                digest,
                plugin_id,
                exc_info=True,
            )
        self.registry.reload_plugin(plugin_id)
        self._init_secrets_for_plugin(self.registry.get(plugin_id))
        self._publish_catalog_change()
        self._schedule_runtime_reconcile()
        return self._plugin_view(self.registry.get(plugin_id))

    def rollback_package(self, plugin_id):
        active = self.repository.active_package(plugin_id)
        if not active or not active["previous_digest"]:
            raise ValueError("Plugin has no previous package to restore")
        return self.activate_upgrade(plugin_id, active["previous_digest"])

    async def invoke_operation(self, plugin_id, task_type, payload):
        from .execution import invoke_task, validate_task

        _, definition = validate_task(self, plugin_id, task_type, payload)
        if definition.handler.kind != "http":
            raise ValueError("Process operations must be submitted as ledger jobs")
        # The operator API can proxy short previews; background work belongs
        # to the ledger and supervisor.
        if definition.handler.timeout_seconds > 60:
            raise ValueError("Long operations must be submitted as ledger jobs")
        return await invoke_task(self, plugin_id, task_type, payload)

    def scoped_state(self, plugin_id, key, *, value=None, write=False):
        record = self.require_scope(plugin_id, "state_keys", key)
        capability = (
            PluginCapability.STATE_WRITE if write else PluginCapability.STATE_READ
        )
        if not self._has_capability(record, capability):
            raise PermissionError(f"Plugin requires {capability.value}")
        if write:
            self.set_state(
                plugin_id,
                key,
                value,
                actor=f"plugin:{plugin_id}",
                reason="SDK state update",
            )
        return {
            "key": key,
            "value": self.get_state(plugin_id, key) if not write else value,
        }

    def scoped_profile(self, plugin_id, site, user_id):
        from sqlalchemy import select, text
        from ultima_scraper_api import get_enabled_sites
        from ultima_scraper_db_archive import UserInfoModel, UserModel

        record = self._require_enabled(plugin_id)
        if not record.manifest.sdk or not self._has_capability(
            record, PluginCapability.PROFILES_READ
        ):
            raise PermissionError("Plugin requires approved profile access")
        if site not in {name.lower() for name in get_enabled_sites()} or user_id < 1:
            raise ValueError("Unknown profile site or identity")
        granted = self.repository.get_document("plugin_scope_grants", plugin_id) or {}
        fields = set(record.manifest.sdk.access.profile_fields) & set(
            granted.get("profile_fields", [])
        )
        if not fields:
            raise PermissionError("Plugin has no approved profile fields")
        columns = {
            "user_id": UserModel.id,
            "username": UserModel.username,
            "name": UserInfoModel.name,
            "description": UserInfoModel.description,
        }
        selected = [columns[field].label(field) for field in sorted(fields - {"site"})]
        with self.repository.session_factory() as session:
            # The site is selected from the application's finite site registry.
            session.execute(text(f'SET LOCAL search_path="{site}",management,public'))
            row = (
                session.execute(
                    select(UserModel.id.label("identity"), *selected)
                    .outerjoin(UserInfoModel, UserInfoModel.user_id == UserModel.id)
                    .where(UserModel.id == user_id)
                )
                .mappings()
                .first()
            )
        if row is None:
            raise KeyError("Saved profile not found")
        result = {key: value for key, value in row.items() if key != "identity"}
        if "site" in fields:
            result["site"] = site
        return result

    def submit_execution(self, plugin_id, task_type, payload, *, idempotency_key=None):
        from .execution import submit_execution

        return submit_execution(
            self, plugin_id, task_type, payload, idempotency_key=idempotency_key
        )
