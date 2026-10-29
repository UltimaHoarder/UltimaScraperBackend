"""Source acquisition and explicit, data-retaining installation lifecycle."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import yaml
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from .manifest import load_manifest_from_dict
from .packages import package_content, package_directory, snapshot_package
from .runtime_owner import PluginRuntimeOwnership
from .sdk import SDK_VERSION


class InstallationConflict(ValueError):
    pass


def _git(directory, *arguments, binary=False):
    environment = os.environ.copy()
    # Do not inherit arbitrary command/config overrides or run repository hooks.
    for key in tuple(environment):
        if key.startswith("GIT_"):
            environment.pop(key)
    environment.update(
        GIT_TERMINAL_PROMPT="0",
        GIT_LFS_SKIP_SMUDGE="1",
        GIT_SSH_COMMAND="ssh -oBatchMode=yes -oStrictHostKeyChecking=yes",
    )
    try:
        config = subprocess.run(
            [
                "git",
                "-C",
                str(directory),
                "config",
                "--name-only",
                "--get-regexp",
                r"^filter\.",
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        filters = {
            key.split(".")[1]
            for key in config.stdout.splitlines()
            if len(key.split(".")) >= 3
        }
        overrides = [
            argument
            for name in filters
            for field, value in [
                ("clean", ""),
                ("smudge", ""),
                ("process", ""),
                ("required", "false"),
            ]
            for argument in ["-c", f"filter.{name}.{field}={value}"]
        ]
        result = subprocess.run(
            [
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "protocol.allow=never",
                "-c",
                "protocol.https.allow=always",
                "-c",
                "protocol.ssh.allow=always",
                "-c",
                "core.fsmonitor=false",
                *overrides,
                "-C",
                str(directory),
                *arguments,
            ],
            env=environment,
            capture_output=True,
            text=not binary,
            timeout=120,
            check=True,
        )
        return result.stdout if binary else result.stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        # Git diagnostics may contain credentials from server credential helpers.
        raise ValueError(
            "Git operation failed or timed out. Check the repository, revision, and server authentication."
        ) from error


def _validate_git_url(url):
    if any(ord(character) <= 32 for character in url):
        raise ValueError("Git URL cannot contain whitespace or control characters")
    parsed = urlsplit(url)
    if (
        parsed.scheme == "https"
        and parsed.hostname
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    ):
        return
    if (
        parsed.scheme == "ssh"
        and parsed.hostname
        and not parsed.hostname.startswith("-")
        and (
            parsed.username is None
            or re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", parsed.username)
        )
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    ):
        return
    if re.fullmatch(
        r"[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9.-]*:[A-Za-z0-9_./-]+", url
    ):
        return
    raise ValueError("Use an HTTPS or SSH Git URL without embedded credentials.")


class PluginInstallations:
    def sources_directory(self):
        configured = os.environ.get("USR_PLUGIN_SOURCES_DIR")
        return (
            Path(configured).expanduser().resolve()
            if configured
            else self._plugins_dir.resolve().parent.parent / "USRPlugins"
        )

    def is_uninstalled(self, plugin_id):
        return bool(
            self.repository.get_host_document(plugin_id, "installation").get(
                "uninstalled"
            )
        )

    @contextmanager
    def installation_lock(self, plugin_id):
        if not re.fullmatch(r"[a-z][a-z0-9_-]{1,62}[a-z0-9]", plugin_id):
            raise ValueError("Invalid plugin identity")
        resource = "plugin-install:" + plugin_id
        owner = uuid4().hex
        directory = self._plugins_dir / ".runtime" / "installation-locks"
        directory.mkdir(parents=True, exist_ok=True)
        ownership = PluginRuntimeOwnership(directory / plugin_id)
        if not ownership.try_acquire():
            raise InstallationConflict("Another installation operation is in progress.")
        token = None
        try:
            token = self.repository.acquire_lease(resource, owner, seconds=300)
            if not token:
                raise InstallationConflict(
                    "Another installation operation is in progress."
                )

            def fence():
                if not self.repository.acquire_lease(
                    resource, owner, seconds=300, token=token
                ):
                    raise InstallationConflict(
                        "Installation ownership changed. Prepare the package again."
                    )

            yield fence
        finally:
            if token:
                self.repository.release_lease(resource, token)
            ownership.release()

    def _assert_installable(self, plugin_id):
        self._sync_catalog_from_repository()
        if not self.is_uninstalled(plugin_id) and (
            self.registry.get(plugin_id) is not None
            or plugin_id in self.repository.list_installed_plugins()
            or (self._plugins_dir / plugin_id).exists()
        ):
            raise InstallationConflict(
                f"Plugin {plugin_id!r} is already installed. Select its existing installation."
            )

    def _source_manifest(self, directory):
        if directory.is_symlink():
            raise ValueError("Plugin sources cannot be symbolic links")
        declaration = directory / "manifest.yaml"
        if not declaration.exists():
            declaration = directory / "manifest.yml"
        if declaration.is_symlink():
            raise ValueError("Plugin manifests cannot be symbolic links")
        with declaration.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("Plugin manifest exceeds 64 KiB")
        try:
            data = yaml.safe_load(raw)
        except yaml.YAMLError as error:
            raise ValueError("Plugin manifest contains invalid YAML") from error
        if not isinstance(data, dict):
            raise ValueError("Plugin manifest must be a mapping")  # noqa: TRY004 - operator validation error
        return load_manifest_from_dict(data), declaration, raw.decode("utf-8")

    def _validate_install_package(
        self, root, manifest, *, immutable=False, include_paths=None
    ):
        if Version(self._usr_version) not in SpecifierSet(manifest.usr_compatibility):
            raise ValueError("Plugin is incompatible with this USR version")
        if manifest.sdk and Version(SDK_VERSION) not in SpecifierSet(
            manifest.sdk.compatibility
        ):
            raise ValueError("Plugin is incompatible with this SDK version")
        files, _ = package_content(
            root,
            manifest.model_dump(mode="json"),
            immutable=immutable,
            include_paths=include_paths,
        )
        available = {str(path) for path, _, _ in files}
        references = []
        if manifest.webui:
            references.extend([manifest.webui.entry, *manifest.webui.styles])
        for task in manifest.task_types:
            if task.handler and task.handler.kind == "process":
                # Package-relative script arguments must exist; the interpreter is supplied by the host.
                references.extend(
                    item
                    for item in task.handler.command[1:]
                    if item.startswith("./") or item.endswith((".py", ".js", ".sh"))
                )
        for reference in references:
            path = Path(reference)
            if path.is_absolute() or ".." in path.parts or str(path) not in available:
                raise ValueError(
                    f"Referenced plugin asset is missing or unsafe: {reference}"
                )

    def list_plugin_sources(self):
        root = self.sources_directory()
        if not root.is_dir():
            return []
        result = []
        installed = {item["plugin_id"] for item in self.list_plugins()}
        for directory in sorted(root.iterdir()):
            if directory.name.startswith(".") or not directory.is_dir():
                continue
            try:
                manifest, _, _ = self._source_manifest(directory)
                if (
                    Path(_git(directory, "rev-parse", "--show-toplevel")).resolve()
                    != directory.resolve()
                ):
                    raise ValueError("Each plugin needs its own Git repository")
                revision = _git(directory, "rev-parse", "--verify", "HEAD^{commit}")
                result.append(
                    {
                        "source_id": directory.name,
                        "plugin_id": manifest.plugin_id,
                        "display_name": manifest.display_name,
                        "plugin_version": manifest.plugin_version,
                        "revision": revision,
                        "installed": manifest.plugin_id in installed,
                        "data_retained": bool(
                            self.repository.get_host_document(
                                manifest.plugin_id, "installation"
                            ).get("data_retained")
                        ),
                    }
                )
            except OSError, ValueError, yaml.YAMLError:
                continue
        return result

    def prepare_plugin_install(self, source):
        root = self.sources_directory()
        root.mkdir(parents=True, exist_ok=True)
        checkout = None
        try:
            if source["kind"] == "local":
                name = source.get("source_id", "")
                if not name or Path(name).name != name or name.startswith("."):
                    raise ValueError("Choose a source folder from the plugin catalog")
                directory = root / name
                if directory.is_symlink() or not directory.resolve().is_relative_to(
                    root
                ):
                    raise ValueError("Plugin source escapes the source directory")
            elif source["kind"] == "git":
                url = source.get("url", "")
                _validate_git_url(url)
                checkout = Path(tempfile.mkdtemp(prefix=".install-", dir=root))
                directory = checkout
                _git(root, "clone", "--no-checkout", "--", url, str(checkout))
                ref = source.get("revision") or "HEAD"
                if ref.startswith("-") or len(ref) > 200:
                    raise ValueError("Invalid Git revision")
                revision = _git(
                    directory,
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    ref + "^{commit}",
                )
                # Read blobs directly: checkout must not invoke smudge filters supplied by Git config.
                entries = _git(directory, "ls-tree", "-r", revision).splitlines()
                if len(entries) > 1100:
                    raise ValueError("Plugin repository exceeds its file limit")
                total = 0
                for entry in entries:
                    descriptor, name = entry.split("\t", 1)
                    mode, kind, blob = descriptor.split()
                    relative = Path(name)
                    if (
                        mode == "120000"
                        or kind != "blob"
                        or relative.is_absolute()
                        or ".." in relative.parts
                        or name.startswith('"')
                    ):
                        raise ValueError(
                            "Plugin repositories cannot contain symlinks, submodules, or unsafe paths"
                        )
                    size = int(_git(directory, "cat-file", "-s", blob))
                    total += size
                    if total > 32 * 1024 * 1024:
                        raise ValueError("Plugin repository exceeds 32 MiB")
                # The checked-out tree is bounded. Disable every configured filter during checkout.
                # Extract validated blobs without executing clean/smudge filters.
                for entry in entries:
                    descriptor, name = entry.split("\t", 1)
                    mode, _, blob = descriptor.split()
                    target = directory / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(
                        _git(directory, "cat-file", "blob", blob, binary=True)
                    )
                    target.chmod(0o755 if mode == "100755" else 0o644)
                _git(directory, "update-ref", "HEAD", revision)
                _git(directory, "read-tree", revision)
            else:
                raise ValueError("Unknown plugin source")
            if (
                Path(_git(directory, "rev-parse", "--show-toplevel")).resolve()
                != directory.resolve()
            ):
                raise ValueError("Each plugin needs its own Git repository")
            revision = _git(directory, "rev-parse", "--verify", "HEAD^{commit}")
            manifest, declaration, manifest_yaml = self._source_manifest(directory)
            include_paths = {
                name.decode("utf-8")
                for name in _git(
                    directory,
                    "ls-files",
                    "--cached",
                    "--others",
                    "--exclude-standard",
                    "-z",
                    binary=True,
                ).split(b"\0")
                if name
            }
            self._validate_install_package(
                directory, manifest, include_paths=include_paths
            )
            with self.installation_lock(manifest.plugin_id) as fence:
                self._assert_installable(manifest.plugin_id)
                fence()
                digest = snapshot_package(
                    self,
                    SimpleNamespace(
                        plugin_id=manifest.plugin_id,
                        manifest=manifest,
                        manifest_path=declaration,
                    ),
                    include_paths=include_paths,
                )
                self._validate_install_package(
                    package_directory(self, manifest.plugin_id, digest),
                    manifest,
                    immutable=True,
                )
                token = uuid4().hex
                receipt = {
                    "manifest_yaml": manifest_yaml,
                    "manifest_name": declaration.name,
                    "preview_token": token,
                    "plugin_id": manifest.plugin_id,
                    "display_name": manifest.display_name,
                    "plugin_version": manifest.plugin_version,
                    "package_digest": digest,
                    "requested_capabilities": [
                        cap.value for cap in manifest.capabilities
                    ],
                    "requested_scopes": manifest.sdk.access.model_dump(mode="json")
                    if manifest.sdk
                    else {},
                    "source": {
                        "kind": source["kind"],
                        "source_id": directory.name
                        if not checkout
                        else manifest.plugin_id,
                        "url": source.get("url"),
                        "revision": revision,
                        "dirty": bool(_git(directory, "status", "--porcelain")),
                    },
                    "checkout": str(checkout) if checkout else None,
                    "expires_at": (
                        self.repository.now() + timedelta(minutes=30)
                    ).isoformat(),
                }
                self.repository.update_host_document(
                    manifest.plugin_id, "install-preview:" + token, lambda _: receipt
                )
                checkout = None  # Retained for confirmation or explicit cancellation.
                return {
                    key: value
                    for key, value in receipt.items()
                    if key not in {"checkout", "manifest_yaml", "manifest_name"}
                }
        finally:
            if checkout is not None:
                shutil.rmtree(checkout)

    def cancel_plugin_install(self, plugin_id, token):
        with self.installation_lock(plugin_id):
            receipt = self.repository.get_host_document(
                plugin_id, "install-preview:" + token
            )
            if receipt.get("checkout"):
                path = Path(receipt["checkout"])
                if path.parent == self.sources_directory() and path.name.startswith(
                    ".install-"
                ):
                    shutil.rmtree(path, ignore_errors=True)
            self.repository.update_host_document(
                plugin_id, "install-preview:" + token, lambda _: {}
            )

    def confirm_plugin_install(self, plugin_id, token):
        with self.installation_lock(plugin_id) as fence:
            receipt = self.repository.get_host_document(
                plugin_id, "install-preview:" + token
            )
            if (
                not receipt
                or receipt.get("consumed")
                or receipt["expires_at"] <= self.repository.now().isoformat()
            ):
                raise InstallationConflict(
                    "Installation preview expired or was used. Prepare it again."
                )
            self._assert_installable(plugin_id)
            root = package_directory(self, plugin_id, receipt["package_digest"])
            manifest = self.repository.get_package(plugin_id, receipt["package_digest"])
            files, actual = package_content(root, manifest, immutable=True)
            if actual != receipt["package_digest"]:
                raise ValueError("Prepared package changed. Prepare it again.")
            candidate = load_manifest_from_dict(manifest)
            self._validate_install_package(root, candidate, immutable=True)
            destination = self._plugins_dir / plugin_id
            if destination.exists() or destination.is_symlink():
                raise InstallationConflict(
                    "The runtime installation directory already exists"
                )
            temporary = Path(
                tempfile.mkdtemp(prefix=".install-", dir=self._plugins_dir)
            )
            source_destination = None
            source_published = False
            published = False
            try:
                for relative, content, executable in files:
                    target = temporary / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(content)
                    target.chmod(0o755 if executable else 0o644)
                if (
                    load_manifest_from_dict(
                        yaml.safe_load(receipt["manifest_yaml"])
                    ).model_dump(mode="json")
                    != manifest
                ):
                    raise ValueError("Prepared manifest changed. Prepare it again.")
                (temporary / receipt["manifest_name"]).write_text(
                    receipt["manifest_yaml"], encoding="utf-8"
                )
                fence()
                if receipt.get("checkout"):
                    checkout = Path(receipt["checkout"])
                    source_destination = self.sources_directory() / plugin_id
                    if source_destination.exists():
                        raise InstallationConflict(
                            "A source repository already exists. Install from that local folder instead."
                        )
                    checkout.rename(source_destination)
                    source_published = True
                # Keep permission approval explicit, including legacy first enablement.
                self.repository.set_plugin_enabled(plugin_id, False)
                if not self.capability_grants.has_record(plugin_id):
                    self.capability_grants.grant(plugin_id, set())
                temporary.rename(destination)
                published = True
                self.repository.set_plugin_missing(plugin_id, False)
                self.repository.activate_package(plugin_id, receipt["package_digest"])
                self.repository.update_host_document(
                    plugin_id,
                    "installation",
                    lambda old: {
                        **old,
                        "uninstalled": False,
                        "data_retained": False,
                        "source": receipt["source"],
                        "setup_completed": False,
                    },
                )
                self.registry.reload_plugin(plugin_id)
                record = self.registry.get(plugin_id)
                self._init_secrets_for_plugin(record)
                self.state.set_quota(
                    plugin_id, candidate.operational_policy.state_quota_bytes
                )
                # Reinstallation must not keep an earlier active declaration.
                self.repository.update_host_document(
                    plugin_id,
                    "install-preview:" + token,
                    lambda old: {**old, "consumed": True, "checkout": None},
                )
                self._publish_catalog_change()
                return self._plugin_view(record)
            except BaseException:
                if published:
                    shutil.rmtree(destination)
                    self.repository.set_plugin_missing(plugin_id, True)
                    self.repository.update_host_document(
                        plugin_id,
                        "installation",
                        lambda old: {**old, "uninstalled": True, "data_retained": True},
                    )
                    self.registry.remove_plugin(plugin_id)
                if (
                    source_published
                    and source_destination is not None
                    and source_destination.exists()
                ):
                    source_destination.rename(Path(receipt["checkout"]))
                raise
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)

    def uninstall_plugin(self, plugin_id, *, delete_data=False, confirmation=""):
        with self.installation_lock(plugin_id) as fence:
            if delete_data and confirmation != plugin_id:
                raise ValueError("Type the exact plugin ID to delete retained data")
            self._require_known(plugin_id)
            directory = self._plugins_dir / plugin_id
            if directory.is_symlink() or not directory.resolve().is_relative_to(
                self._plugins_dir.resolve()
            ):
                raise ValueError("Unsafe plugin installation directory")
            fence()
            self.disable_plugin(plugin_id)
            self.repository.update_host_document(
                plugin_id,
                "installation",
                lambda old: {**old, "uninstalled": True, "data_retained": True},
            )
            retired = self._plugins_dir / (".uninstall-" + uuid4().hex)
            if directory.exists():
                directory.rename(retired)
            self.repository.set_plugin_missing(plugin_id, True)
            deleted = (
                self.purge_plugin_data(plugin_id, "PURGE " + plugin_id)
                if delete_data
                else None
            )
            if delete_data:
                self.repository.update_host_document(
                    plugin_id,
                    "installation",
                    lambda old: {**old, "uninstalled": True, "data_retained": False},
                )
            self.registry.remove_plugin(plugin_id)
            self._publish_catalog_change()
            self._schedule_runtime_reconcile()
            if retired.exists():
                shutil.rmtree(retired)
            return {
                "plugin_id": plugin_id,
                "status": "uninstalled",
                "data_retained": not delete_data,
                "deleted_records": deleted["deleted_records"] if deleted else 0,
            }
