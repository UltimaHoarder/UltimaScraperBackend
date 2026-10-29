"""Immutable, content-addressed plugin packages, retained for running jobs."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path


def package_content(root, manifest, *, immutable=False, include_paths=None):
    """Read a bounded package and fingerprint exactly the bytes being published."""
    if root.is_symlink():
        raise ValueError("Plugin packages cannot contain symbolic links")
    files = []
    total = 0
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(
            part.startswith(".") or part == "__pycache__" for part in relative.parts
        ):
            continue
        if include_paths is not None and str(relative) not in include_paths:
            continue
        if path.is_symlink():
            raise ValueError("Plugin packages cannot contain symbolic links")
        if not path.is_file() or path.name in {"manifest.yaml", "manifest.yml"}:
            continue
        if relative == Path("manifest.json"):
            if immutable:
                continue
            raise ValueError("manifest.json is reserved for the package declaration")
        if len(files) >= 1024:
            raise ValueError("Plugin package exceeds its file/byte limit")
        with path.open("rb") as source:
            content = source.read(32 * 1024 * 1024 - total + 1)
        total += len(content)
        if total > 32 * 1024 * 1024:
            raise ValueError("Plugin package exceeds its file/byte limit")
        files.append((relative, content, bool(path.stat().st_mode & 0o111)))
    fingerprint = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    )
    for path, content, executable in files:
        fingerprint.update(
            str(path).encode()
            + b"\0"
            + bytes([executable])
            + hashlib.sha256(content).digest()
        )
    digest = fingerprint.hexdigest()
    return files, digest


def snapshot_package(platform, record, *, manifest=None, include_paths=None):
    manifest = manifest or record.manifest.model_dump(mode="json")
    root = record.manifest_path.parent
    files, digest = package_content(root, manifest, include_paths=include_paths)
    destination = package_directory(platform, record.plugin_id, digest)
    if not destination.is_dir():
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=".stage-", dir=destination.parent))
        try:
            for path, content, executable in files:
                target = temporary / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
                target.chmod(0o555 if executable else 0o444)
            (temporary / "manifest.json").write_text(
                json.dumps(manifest, sort_keys=True)
            )
            (temporary / "manifest.json").chmod(0o444)
            try:
                temporary.rename(destination)
            except OSError:
                if not destination.is_dir():
                    raise
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    platform.repository.save_package(record.plugin_id, manifest, digest=digest)
    return digest


def package_directory(platform, plugin_id, digest):
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError("Invalid plugin package digest")
    if not plugin_id or Path(plugin_id).name != plugin_id:
        raise ValueError("Invalid plugin identity")
    return platform._plugins_dir / ".runtime" / "packages" / plugin_id / digest


def stage_upgrade(platform, plugin_id, manifest_data):
    from .manifest import load_manifest_from_dict

    candidate = load_manifest_from_dict(manifest_data)
    if candidate.plugin_id != plugin_id:
        raise ValueError("Staged package identity does not match the installation")
    record = platform._require_known(plugin_id)
    manifest = candidate.model_dump(mode="json")
    digest = snapshot_package(platform, record, manifest=manifest)
    current = record.manifest
    granted = platform.repository.get_document("plugin_scope_grants", plugin_id) or {}
    requested = candidate.sdk.access.model_dump(mode="json") if candidate.sdk else {}
    return {
        "plugin_id": plugin_id,
        "package_digest": digest,
        "plugin_version": candidate.plugin_version,
        "previous_version": current.plugin_version if current else None,
        "requested_capabilities": [cap.value for cap in candidate.capabilities],
        "added_capabilities": sorted(
            {cap.value for cap in candidate.capabilities}
            - platform.capability_grants.get(plugin_id)
        ),
        "requested_scopes": requested,
        "scope_changes": requested != granted,
        "configuration_schema": candidate.sdk.configuration_schema
        if candidate.sdk
        else None,
        "status": "staged",
    }
