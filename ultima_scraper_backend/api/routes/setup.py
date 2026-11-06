"""Setup API routes for first-time configuration.

Provides endpoints for the WebUI's First-Time Setup wizard:
- Check setup status
- Test database connections
- Save initial configuration
- Complete setup
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast

from fastapi import APIRouter, Depends, HTTPException
from platformdirs import user_state_dir
from pydantic import BaseModel, Field
from sqlalchemy.engine import make_url
from ultima_scraper_core.processes import finish_owned
from ultima_scraper_db_archive import MIGRATION_BUNDLE

from ultima_scraper_backend.config import StagingDirectory
from ultima_scraper_backend.services.plugin_proxies import guard_proxy_assignment
from ultima_scraper_backend.services.settings_service import SettingsService
from ultima_scraper_core import archive_database_url, runtime_env_value

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/setup", tags=["setup"])

_migration_session_lock = asyncio.Lock()
_pending_startup_migration: dict[str, Any] | None = None
_MIGRATION_DIFF_MAX_CHARS = 30_000
_BACKEND_STARTUP_ID_ENV = "USR_BACKEND_STARTUP_ID"
_BACKEND_STARTUP_ID = uuid.uuid4().hex
_SCHEMA_MANAGEMENT_ENV = "USR_SCHEMA_MANAGEMENT_ENABLED"


def _schema_management_enabled() -> bool:
    """Return whether this process may create or migrate PostgreSQL schemas."""
    raw = runtime_env_value(_SCHEMA_MANAGEMENT_ENV)
    if raw is None:
        return True
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _require_schema_management_enabled() -> None:
    """Reject owner-only setup operations in restricted runtime deployments."""
    if _schema_management_enabled():
        return
    raise HTTPException(
        status_code=403,
        detail=(
            "Schema management is disabled for this runtime. Run migrations "
            "with the dedicated owner-role migration service."
        ),
    )


def get_backend_startup_id() -> str:
    """Return the control-runtime token shared by every API worker."""

    return (
        os.environ.get(_BACKEND_STARTUP_ID_ENV, "").strip()
        or os.environ.get("USR_BACKEND_STARTUP_ID", "").strip()
        or _BACKEND_STARTUP_ID
    )


_DB_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_ENCRYPTEDTEXT_FQN_RE = re.compile(
    r"(?:(?:src\.)?UltimaScraperDBMini|ultima_scraper_db_archive)"
    r"\.crypto\.EncryptedText\(\)"
)


def _validate_database_identifier(value: str, field_name: str = "database name") -> str:
    candidate = value.strip()
    if not _DB_IDENTIFIER_RE.fullmatch(candidate):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name}. Use 1-63 characters: letters, digits, underscores; "
                "must start with a letter or underscore."
            ),
        )
    return candidate


# ------------------------------------------------------------------ #
# Request / Response models
# ------------------------------------------------------------------ #


class SetupStatusResponse(BaseModel):
    """Current setup state returned by GET /setup/status."""

    is_configured: bool = False
    has_config: bool = False
    has_database: bool = False
    data_directory: str = ""
    startup_id: str = Field(default_factory=get_backend_startup_id)
    # Set when the deployment pins BACKEND_ID; setup cannot choose another.
    backend_id: str | None = None
    # host:port of the Redis the deployment configures, if any.
    redis_target: str | None = None


class SSHConnection(BaseModel):
    username: str | None = None
    private_key_filepath: str | None = None
    private_key_password: str | None = None
    host: str | None = None
    port: int = 22
    active: bool = False


class DatabaseConnectionRequest(BaseModel):
    """Postgres connection details submitted during setup."""

    host: str = "localhost"
    port: int = 5432
    name: str = "ultima_archive"
    username: str = "ultimascraper"
    password: str = ""
    ssh: SSHConnection = Field(default_factory=SSHConnection)


def _build_database_url(
    request: DatabaseConnectionRequest,
    *,
    drivername: str,
) -> str:
    """Build a database URL without misparsing reserved credential characters."""
    from sqlalchemy.engine import URL

    return URL.create(
        drivername=drivername,
        username=request.username,
        password=request.password or None,
        host=request.host,
        port=request.port,
        database=_validate_database_identifier(request.name),
    ).render_as_string(hide_password=False)


class DatabaseConnectionResponse(BaseModel):
    """Result of a database connection test."""

    success: bool
    message: str
    latency_ms: float | None = None


class StartupMigrationPreviewResponse(BaseModel):
    """Autogenerated migration preview for startup review."""

    status: str
    message: str
    preview_id: str | None = None
    revision: str | None = None
    filename: str | None = None
    diff: str | None = None
    has_changes: bool = False
    can_apply: bool = False
    output: list[str] = Field(default_factory=list)
    outcome: Literal["checked", "applied", "skipped"] | None = None


class StartupMigrationApplyRequest(BaseModel):
    """Request payload for applying a reviewed startup migration."""

    preview_id: str | None = None


class StartupMigrationActionResponse(BaseModel):
    """Generic mutation result for migration actions."""

    success: bool
    message: str
    output: list[str] = Field(default_factory=list)


def _resolve_async_database_url() -> str | None:
    """Resolve the archive database URL from runtime configuration and normalize it."""
    db_url = archive_database_url()
    if not db_url:
        return None
    if db_url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + db_url[len("postgresql://") :]
    if db_url.startswith("postgresql+psycopg://"):
        return "postgresql+asyncpg://" + db_url[len("postgresql+psycopg://") :]
    if db_url.startswith("postgresql+psycopg2://"):
        return "postgresql+asyncpg://" + db_url[len("postgresql+psycopg2://") :]
    return db_url


def _build_alembic_config(db_url: str):
    """The bundle's config, also reading revisions generated for review."""
    config = MIGRATION_BUNDLE.create_config(db_url)
    config.set_main_option(
        "version_locations",
        os.pathsep.join(str(path) for path in (_get_versions_dir(), _pending_revisions_dir())),
    )
    return config


_REQUIRED_POSTGRES_EXTENSIONS = ("uuid-ossp", "pg_trgm")
_OPTIONAL_POSTGRES_EXTENSIONS = ("vector",)


async def _verify_cluster_secret_key(engine: Any) -> str | None:
    """None when this backend's USR_SECRET_KEY is the cluster's, else why not.

    Every backend sharing a database needs the same key; the first one to check
    records it. See ultima_scraper_db_archive.secret_key_check.
    """
    from ultima_scraper_db_archive.secret_key_check import (
        SecretKeyMismatch,
        verify_secret_key,
    )

    try:
        async with engine.connect() as connection:
            await connection.run_sync(verify_secret_key)
    except SecretKeyMismatch as exc:
        return str(exc)
    except Exception:
        # Startup checks again; an unreachable database is reported elsewhere.
        logger.warning("Could not check USR_SECRET_KEY against the cluster", exc_info=True)
    return None


async def _ensure_postgres_extensions(engine: Any) -> None:
    """Install shared extensions in ``public`` and repair older placements."""
    from sqlalchemy import text

    async with engine.connect() as connection:
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        for extension_name in (
            *_REQUIRED_POSTGRES_EXTENSIONS,
            *_OPTIONAL_POSTGRES_EXTENSIONS,
        ):
            try:
                await connection.execute(
                    text(
                        f'CREATE EXTENSION IF NOT EXISTS "{extension_name}" '
                        "WITH SCHEMA public"
                    )
                )
                result = await connection.execute(
                    text(
                        "SELECT namespace.nspname "
                        "FROM pg_extension AS extension "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = extension.extnamespace "
                        "WHERE extension.extname = :extension_name"
                    ),
                    {"extension_name": extension_name},
                )
                extension_schema = result.scalar_one_or_none()
                if extension_schema is None:
                    raise RuntimeError(
                        f"PostgreSQL extension {extension_name!r} was not installed"
                    )
                if extension_schema != "public":
                    await connection.execute(
                        text(
                            f'ALTER EXTENSION "{extension_name}" SET SCHEMA public'
                        )
                    )
            except Exception:
                if extension_name in _REQUIRED_POSTGRES_EXTENSIONS:
                    raise
                logger.info(
                    "Optional PostgreSQL extension %s is unavailable",
                    extension_name,
                )


def _get_versions_dir() -> Path:
    versions_dir = MIGRATION_BUNDLE.script_path / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)
    return versions_dir


def _migration_runtime_dir() -> Path:
    """Return a writable, checkout-specific state directory."""
    bundle_key = hashlib.sha256(
        str(MIGRATION_BUNDLE.script_path.resolve()).encode("utf-8")
    ).hexdigest()[:12]
    runtime_dir = (
        Path(user_state_dir("UltimaScraper", "UltimaScraper"))
        / "startup_migrations"
        / bundle_key
    )
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir


def _pending_revisions_dir() -> Path:
    """Revisions generated at startup wait here, outside the source tree, until
    applied; only then do they join the migrations in the package."""
    path = _migration_runtime_dir() / "pending-revisions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _migration_preview_state_path() -> Path:
    """Return the shared pending-preview state file used by every API worker."""
    return _migration_runtime_dir() / "pending_preview.json"


def _migration_preview_lock_path() -> Path:
    return _migration_runtime_dir() / "pending_preview.lock"


def _startup_migration_result_path() -> Path:
    return _migration_runtime_dir() / "startup_result.json"


def _startup_database_fingerprint() -> str:
    # Bind the result to the configured target without writing credentials.
    return hashlib.sha256((_resolve_async_database_url() or "").encode()).hexdigest()


def _load_startup_migration_result() -> StartupMigrationPreviewResponse | None:
    """Reuse one result across tabs and API workers for this backend startup."""
    try:
        cached = json.loads(_startup_migration_result_path().read_text(encoding="utf-8"))
        if (
            cached["startup_id"] != get_backend_startup_id()
            or cached["database_fingerprint"] != _startup_database_fingerprint()
        ):
            return None
        result = StartupMigrationPreviewResponse.model_validate(cached["response"])
        if result.status not in {"ready", "no_changes", "requires_database", "completed"}:
            return None
        if result.status == "ready":
            pending = _load_pending_startup_migration()
            if (
                not pending
                or pending["preview_id"] != result.preview_id
                or not Path(str(pending["file_path"])).exists()
            ):
                return None
        return result
    except FileNotFoundError:
        return None
    except (OSError, ValueError, KeyError, TypeError) as exc:
        logger.warning("Could not load shared startup migration result: %s", exc)
        return None


def _clear_startup_migration_result() -> None:
    with suppress(FileNotFoundError):
        _startup_migration_result_path().unlink()


def _save_startup_migration_result(result: StartupMigrationPreviewResponse) -> None:
    if result.status == "error":
        # An explicit retry must be able to recover from a transient failure.
        _clear_startup_migration_result()
        return
    _write_migration_state(_startup_migration_result_path(), {
        "startup_id": get_backend_startup_id(),
        "database_fingerprint": _startup_database_fingerprint(),
        "response": result.model_dump(mode="json"),
    })


def _load_pending_startup_migration() -> dict[str, Any] | None:
    """Load pending preview state from the shared filesystem."""
    global _pending_startup_migration

    state_path = _migration_preview_state_path()
    try:
        pending = json.loads(state_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        _pending_startup_migration = None
        return None
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        logger.warning("Could not load startup migration preview state: %s", exc)
        _pending_startup_migration = None
        return None

    if not isinstance(pending, dict) or not all(
        pending.get(key) for key in ("preview_id", "file_path")
    ):
        logger.warning("Ignoring invalid startup migration preview state")
        _pending_startup_migration = None
        return None

    _pending_startup_migration = pending
    return pending


def _save_pending_startup_migration(pending: dict[str, Any]) -> None:
    """Atomically publish pending preview state to every API worker."""
    global _pending_startup_migration

    _write_migration_state(_migration_preview_state_path(), pending)
    _pending_startup_migration = dict(pending)


def _write_migration_state(state_path: Path, payload: dict[str, Any]) -> None:
    """Publish shared state atomically while the migration guard is held."""
    temp_path = state_path.with_name(
        f".{state_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        temp_path.write_text(
            json.dumps(payload, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temp_path, state_path)
    finally:
        with suppress(FileNotFoundError):
            temp_path.unlink()


def _clear_pending_startup_migration() -> None:
    """Remove shared pending preview state after apply, discard, or no-op."""
    global _pending_startup_migration

    with suppress(FileNotFoundError):
        _migration_preview_state_path().unlink()
    _pending_startup_migration = None


@asynccontextmanager
async def _migration_state_guard() -> AsyncIterator[None]:
    """Serialize migration preview mutations across coroutines and API workers."""
    async with _migration_session_lock:
        lock_path = _migration_preview_lock_path()
        lock_handle = lock_path.open("a+b")
        try:
            while True:
                try:
                    fcntl.flock(
                        lock_handle.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.05)
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_handle.close()


def _version_files() -> set[Path]:
    return {
        path.resolve()
        for directory in (_get_versions_dir(), _pending_revisions_dir())
        for path in directory.glob("*.py")
    }


def _extract_revision(script_text: str, fallback: str | None = None) -> str | None:
    match = re.search(r'revision\s*:\s*str\s*=\s*["\']([^"\']+)["\']', script_text)
    if match:
        return match.group(1)
    match = re.search(r'revision\s*=\s*["\']([^"\']+)["\']', script_text)
    if match:
        return match.group(1)
    return fallback


def _migration_has_changes(script_text: str) -> bool:
    import ast

    try:
        tree = ast.parse(script_text)
    except SyntaxError:
        return True
    upgrades = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "upgrade"]
    if len(upgrades) != 1:
        return True
    # Only literal empty upgrade bodies are bookkeeping. Unknown code needs review.
    return any(
        not isinstance(node, ast.Pass)
        and not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str))
        for node in upgrades[0].body
    )


def _truncate_diff(script_text: str) -> str:
    if len(script_text) <= _MIGRATION_DIFF_MAX_CHARS:
        return script_text
    head = script_text[:_MIGRATION_DIFF_MAX_CHARS]
    return f"{head}\n\n# ... truncated for preview ..."


def _sanitize_migration_script_text(script_text: str) -> tuple[str, bool]:
    """Normalize app-level EncryptedText refs to physical DB TEXT for Alembic."""
    updated = script_text

    # Autogen may emit fully qualified project paths for custom types.
    updated, replaced_fqn = _ENCRYPTEDTEXT_FQN_RE.subn("sa.TEXT()", updated)

    # Handle files that imported EncryptedText and emitted bare constructor calls.
    updated, removed_src_import = re.subn(
        r"^\s*from\s+src\.UltimaScraperDBMini\.crypto\s+import\s+EncryptedText\s*$\n?",
        "",
        updated,
        flags=re.MULTILINE,
    )
    updated, removed_pkg_import = re.subn(
        r"^\s*from\s+UltimaScraperDBMini\.crypto\s+import\s+EncryptedText\s*$\n?",
        "",
        updated,
        flags=re.MULTILINE,
    )
    updated, removed_archive_import = re.subn(
        r"^\s*from\s+ultima_scraper_db_archive\.crypto\s+import\s+EncryptedText\s*$\n?",
        "",
        updated,
        flags=re.MULTILINE,
    )
    updated, replaced_bare = re.subn(r"\bEncryptedText\(\)", "sa.TEXT()", updated)

    changed = bool(
        replaced_fqn
        or removed_src_import
        or removed_pkg_import
        or removed_archive_import
        or replaced_bare
    )
    if changed and "import sqlalchemy as sa" not in updated:
        if "from alembic import op" in updated:
            updated = updated.replace(
                "from alembic import op",
                "from alembic import op\nimport sqlalchemy as sa",
            )
        else:
            updated = f"import sqlalchemy as sa\n{updated}"
    return updated, changed


def _normalize_migration_file(path: Path, *, output: list[str] | None = None) -> str:
    script_text = path.read_text(encoding="utf-8")
    normalized_text, changed = _sanitize_migration_script_text(script_text)
    if changed:
        path.write_text(normalized_text, encoding="utf-8")
        if output is not None:
            output.append(
                f"Normalized custom type refs in {path.name} (EncryptedText -> sa.TEXT)."
            )
    return normalized_text


def _build_preview_from_pending(
    pending: dict[str, Any],
    *,
    message: str,
    output: list[str] | None = None,
) -> StartupMigrationPreviewResponse:
    path = Path(str(pending["file_path"]))
    output_lines = list(output or [])
    script_text = _normalize_migration_file(path, output=output_lines)
    return StartupMigrationPreviewResponse(
        status="ready",
        message=message,
        preview_id=str(pending["preview_id"]),
        revision=str(pending.get("revision") or ""),
        filename=path.name,
        diff=_truncate_diff(script_text),
        has_changes=True,
        can_apply=True,
        output=output_lines,
    )


def _is_target_db_not_up_to_date_error(exc: Exception) -> bool:
    return "target database is not up to date" in str(exc).lower()


async def _get_applied_revisions(db_url: str) -> set[str]:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text("SELECT version_num FROM public.alembic_version")
            )
            return {str(row[0]) for row in rows if row and row[0]}
    except Exception as exc:
        # If the version table does not exist yet, treat all local revisions as pending.
        # Any other DB error should surface so we don't incorrectly report fake "pending" revisions.
        exc_text = str(exc).lower()
        if "alembic_version" in exc_text and (
            "does not exist" in exc_text or "undefinedtable" in exc_text
        ):
            return set()
        raise
    finally:
        await engine.dispose()


def _latest_unapplied_revision_file(
    alembic_cfg: Any,
    applied_revisions: set[str],
) -> tuple[Path, str | None] | None:
    known_applied_revisions: set[str] = set(applied_revisions)
    try:
        from alembic.script import ScriptDirectory

        script_dir = ScriptDirectory.from_config(alembic_cfg)
        # alembic_version stores only current head revision(s), not full history.
        # Expand each current revision to all reachable ancestors so historical
        # revisions are not misclassified as pending.
        known_applied_revisions = set()
        stack = [rev for rev in applied_revisions if rev]
        while stack:
            revision_id = stack.pop()
            if revision_id in known_applied_revisions:
                continue
            known_applied_revisions.add(revision_id)
            revision_obj = script_dir.get_revision(revision_id)
            down_revision = cast(
                tuple[object, ...] | list[object] | str | None,
                getattr(revision_obj, "down_revision", None),
            )
            if isinstance(down_revision, (list, tuple)):
                stack.extend(str(item) for item in down_revision if item)
            elif down_revision:
                stack.append(str(down_revision))

        for revision in script_dir.walk_revisions():
            revision_id = getattr(revision, "revision", None)
            path_raw = getattr(revision, "path", None)
            if (
                not revision_id
                or revision_id in known_applied_revisions
                or not path_raw
            ):
                continue
            path = Path(str(path_raw)).resolve()
            if path.exists():
                return path, str(revision_id)
    except Exception:
        pass

    # Fallback to raw file timestamps if script graph introspection fails.
    for path in sorted(_version_files(), key=lambda p: p.stat().st_mtime, reverse=True):
        script_text = path.read_text(encoding="utf-8")
        revision = _extract_revision(script_text)
        if revision and revision not in known_applied_revisions:
            return path, revision
    return None


async def _preview_latest_unapplied_migration(
    db_url: str,
    *,
    output: list[str],
) -> StartupMigrationPreviewResponse | None:
    alembic_cfg = _build_alembic_config(db_url)
    applied_revisions = await _get_applied_revisions(db_url)
    import hashlib

    from alembic.script import ScriptDirectory

    graph = ScriptDirectory.from_config(alembic_cfg)
    target = graph.get_current_head()
    if target is None:
        return None
    revisions = list(reversed(list(graph.iterate_revisions(target, tuple(sorted(applied_revisions)), implicit_base=True))))
    if not revisions:
        _clear_pending_startup_migration()
        return None

    scripts = []
    has_changes = False
    for revision_obj in revisions:
        path = Path(revision_obj.path).resolve()
        source = _normalize_migration_file(path, output=output)
        changes = _migration_has_changes(source)
        has_changes = has_changes or changes
        kind = "migration operations" if changes else "revision bookkeeping only"
        scripts.append(f"# Revision {revision_obj.revision}: {path.name} ({kind})\n{source}")
        output.append(f"Pending {revision_obj.revision}: {kind}")
    full_preview = "\n\n".join(scripts)
    preview_id = uuid.uuid4().hex
    revision_path = Path(graph.get_revision(target).path).resolve()
    _save_pending_startup_migration(
        {
            "preview_id": preview_id,
            "revision": target,
            "file_path": str(revision_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "delete_on_discard": False,
            "review_hash": hashlib.sha256(full_preview.encode()).hexdigest(),
            "review_base": sorted(applied_revisions),
            "review_files": [str(Path(rev.path).resolve()) for rev in revisions],
        }
    )
    return StartupMigrationPreviewResponse(
        status="ready",
        message=(
            f"Review all {len(revisions)} pending revisions before applying."
            if has_changes else
            "Only revision bookkeeping is pending; no migration operations will run. Approve to record these revisions."
        ),
        preview_id=preview_id,
        revision=target,
        filename=revision_path.name,
        diff=full_preview,
        has_changes=has_changes,
        can_apply=True,
        output=output,
    )


async def _generate_startup_preview_locked() -> StartupMigrationPreviewResponse:
    """Create an autogen migration and return a preview payload."""
    output: list[str] = ["Generating Alembic migration preview..."]
    db_url = _resolve_async_database_url()
    if not db_url:
        return StartupMigrationPreviewResponse(
            status="requires_database",
            message="No ULTIMA_ARCHIVE_DATABASE_URL found. Configure the database first.",
            output=output,
        )

    try:
        from alembic import command
        from alembic.util.exc import AutogenerateDiffsDetected, CommandError

        alembic_cfg = _build_alembic_config(db_url)
        pending_preview = await _preview_latest_unapplied_migration(
            db_url, output=output
        )
        if pending_preview is not None:
            return pending_preview

        output.append("Checking for schema changes before generating revision...")
        try:
            await asyncio.to_thread(command.check, alembic_cfg)
        except AutogenerateDiffsDetected:
            output.append("Schema changes detected. Generating revision preview...")
        except CommandError as exc:
            if _is_target_db_not_up_to_date_error(exc):
                output.append(f"Autogenerate check blocked: {exc}")
                pending_preview = await _preview_latest_unapplied_migration(
                    db_url, output=output
                )
                if pending_preview is not None:
                    return pending_preview
            raise
        else:
            output.append("No schema changes detected by Alembic check.")
            _clear_pending_startup_migration()
            return StartupMigrationPreviewResponse(
                status="no_changes",
                message="No DB schema changes detected. Migration is not required.",
                has_changes=False,
                can_apply=False,
                output=output,
            )

        before_files = _version_files()
        message = (
            f"startup_autogen_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        )
        output.append(f"Autogenerating revision ({message})")
        revision_obj = await asyncio.to_thread(
            command.revision,
            alembic_cfg,
            message=message,
            autogenerate=True,
            version_path=str(_pending_revisions_dir()),
        )
    except Exception as exc:
        if _is_target_db_not_up_to_date_error(exc):
            output.append(f"Autogenerate blocked: {exc}")
            pending_preview = await _preview_latest_unapplied_migration(
                db_url, output=output
            )
            if pending_preview is not None:
                return pending_preview
        logger.exception("Startup migration preview failed")
        return StartupMigrationPreviewResponse(
            status="error",
            message=f"Failed to generate migration preview: {exc}",
            output=output,
        )

    created_files = sorted(
        _version_files() - before_files,
        key=lambda p: p.stat().st_mtime,
    )
    revision_path: Path | None = None

    revision_path_raw = getattr(revision_obj, "path", None)
    if revision_path_raw:
        candidate = Path(str(revision_path_raw)).resolve()
        if candidate.exists():
            revision_path = candidate
    if revision_path is None and created_files:
        revision_path = created_files[-1]

    if revision_path is None:
        return StartupMigrationPreviewResponse(
            status="error",
            message="Alembic did not produce a migration file.",
            output=output,
        )

    script_text = _normalize_migration_file(revision_path, output=output)
    revision = _extract_revision(
        script_text,
        fallback=getattr(revision_obj, "revision", None),
    )

    if not _migration_has_changes(script_text):
        output.append("No schema changes detected in autogenerated migration.")
        with suppress(OSError):
            revision_path.unlink()
        _clear_pending_startup_migration()
        return StartupMigrationPreviewResponse(
            status="no_changes",
            message="No DB schema changes detected. Migration is not required.",
            revision=revision,
            filename=revision_path.name,
            diff=_truncate_diff(script_text),
            has_changes=False,
            can_apply=False,
            output=output,
        )

    preview_id = uuid.uuid4().hex
    _save_pending_startup_migration(
        {
            "preview_id": preview_id,
            "revision": revision,
            "file_path": str(revision_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "delete_on_discard": True,
        }
    )
    output.append(f"Generated {revision_path.name}")
    return StartupMigrationPreviewResponse(
        status="ready",
        message="Review the migration diff before applying.",
        preview_id=preview_id,
        revision=revision,
        filename=revision_path.name,
        diff=_truncate_diff(script_text),
        has_changes=True,
        can_apply=True,
        output=output,
    )


def _adopt_pending_revision(path: Path, output: list[str]) -> Path | None:
    """Move a generated revision into the package's versions; returns where it was.

    Revisions already in the package (a reviewed chain) stay where they are.
    """
    pending_dir = _pending_revisions_dir().resolve()
    if not path.exists() or path.resolve().parent != pending_dir:
        return None
    versions_dir = _get_versions_dir()
    if not os.access(versions_dir, os.W_OK):
        raise RuntimeError(
            "This installation's migrations are read-only, so a generated revision cannot "
            "become part of them. Generate and apply schema changes from a source checkout."
        )
    destination = versions_dir / path.name
    os.replace(path, destination)
    output.append(f"Added {path.name} to the migrations")
    return destination


def _return_pending_revision(adopted: Path, pending_path: Path) -> None:
    """Undo _adopt_pending_revision when the upgrade failed."""
    with suppress(OSError):
        os.replace(adopted, pending_path)


def _discard_pending_preview_locked() -> tuple[bool, list[str]]:
    """Delete pending startup preview file if it exists."""
    output: list[str] = []
    pending = _load_pending_startup_migration()
    if not pending:
        output.append("No pending startup migration preview to discard.")
        return False, output

    path = Path(str(pending["file_path"]))
    delete_on_discard = bool(pending.get("delete_on_discard", True))
    if delete_on_discard and path.exists():
        path.unlink()
        output.append(f"Deleted {path.name}")
    elif delete_on_discard:
        output.append("Pending migration file was already missing.")
    else:
        output.append(
            f"Kept existing migration file {path.name}; discarded preview only."
        )
    _clear_pending_startup_migration()
    return True, output


@router.post("/migrations/preview", response_model=StartupMigrationPreviewResponse)
async def preview_startup_migration() -> StartupMigrationPreviewResponse:
    """Autogenerate a revision and return diff preview for user approval."""
    _require_schema_management_enabled()
    async with _migration_state_guard():
        cached = _load_startup_migration_result()
        if cached is not None:
            return cached
        pending = _load_pending_startup_migration()
        if pending is not None:
            pending_path = Path(str(pending["file_path"]))
            if pending_path.exists() and not pending.get("delete_on_discard", True):
                db_url = _resolve_async_database_url()
                if db_url:
                    return await _generate_shared_startup_preview_locked()
            if pending_path.exists():
                result = _build_preview_from_pending(
                    pending,
                    message="Pending migration preview already exists.",
                    output=["Reusing existing pending migration preview."],
                )
                _save_startup_migration_result(result)
                return result
            _clear_pending_startup_migration()
        return await _generate_shared_startup_preview_locked()


async def _generate_shared_startup_preview_locked() -> StartupMigrationPreviewResponse:
    """Keep generation and its result owned until publication, even on disconnect."""
    async def generate() -> StartupMigrationPreviewResponse:
        result = await _generate_startup_preview_locked()
        _save_startup_migration_result(result)
        return result

    task = asyncio.create_task(generate())
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Alembic's thread can outlive a cancelled HTTP waiter. Retain the file
        # fence until it finishes and publish the result for the next tab.
        await finish_owned(task)
        raise


@router.post("/migrations/discard", response_model=StartupMigrationActionResponse)
async def discard_startup_migration() -> StartupMigrationActionResponse:
    """Discard pending startup migration preview and remove generated file."""
    _require_schema_management_enabled()
    async with _migration_state_guard():
        discarded, output = _discard_pending_preview_locked()
        _save_startup_migration_result(StartupMigrationPreviewResponse(
            status="completed", outcome="skipped",
            message="Migration skipped for this backend startup.", output=output,
        ))
        if discarded:
            return StartupMigrationActionResponse(
                success=True,
                message="Pending migration preview discarded.",
                output=output,
            )
        return StartupMigrationActionResponse(
            success=True,
            message="No pending migration preview.",
            output=output,
        )


@router.post("/migrations/regenerate", response_model=StartupMigrationPreviewResponse)
async def regenerate_startup_migration() -> StartupMigrationPreviewResponse:
    """Discard pending preview and generate a new migration diff."""
    _require_schema_management_enabled()
    async with _migration_state_guard():
        _clear_startup_migration_result()
        _discard_pending_preview_locked()
        return await _generate_shared_startup_preview_locked()


@router.post("/migrations/apply", response_model=StartupMigrationActionResponse)
async def apply_startup_migration(
    request: StartupMigrationApplyRequest,
) -> StartupMigrationActionResponse:
    """Apply reviewed startup migration via Alembic upgrade."""
    _require_schema_management_enabled()
    async with _migration_state_guard():
        output: list[str] = []
        pending = _load_pending_startup_migration()
        if pending is None:
            return StartupMigrationActionResponse(
                success=False,
                message="No pending migration preview to apply.",
                output=output,
            )

        pending_id = str(pending["preview_id"])
        if request.preview_id and request.preview_id != pending_id:
            return StartupMigrationActionResponse(
                success=False,
                message="Preview ID mismatch. Refresh preview before applying.",
                output=output,
            )

        db_url = _resolve_async_database_url()
        if not db_url:
            return StartupMigrationActionResponse(
                success=False,
                message="No ULTIMA_ARCHIVE_DATABASE_URL found. Configure the database first.",
                output=output,
            )

        output.append("Validating reviewed migration target...")
        try:
            from alembic import command

            alembic_cfg = _build_alembic_config(db_url)
            pending_path = Path(str(pending["file_path"]))
            if pending_path.exists():
                _normalize_migration_file(pending_path, output=output)
            if not pending.get("delete_on_discard", True) and not pending.get("review_hash"):
                raise RuntimeError("This preview does not include the pending migration chain. Refresh preview before applying.")
            if pending.get("review_hash"):
                import hashlib

                base = await _get_applied_revisions(db_url)
                scripts = []
                for raw_path in pending["review_files"]:
                    path = Path(raw_path)
                    source = path.read_text(encoding="utf-8")
                    kind = "migration operations" if _migration_has_changes(source) else "revision bookkeeping only"
                    scripts.append(f"# Revision {_extract_revision(source)}: {path.name} ({kind})\n{source}")
                digest = hashlib.sha256("\n\n".join(scripts).encode()).hexdigest()
                if sorted(base) != pending["review_base"] or digest != pending["review_hash"]:
                    raise RuntimeError("Migration history or files changed. Refresh preview before applying.")
                from alembic.script import ScriptDirectory

                graph = ScriptDirectory.from_config(alembic_cfg)
                actual_files = [
                    str(Path(rev.path).resolve())
                    for rev in reversed(list(graph.iterate_revisions(
                        str(pending["revision"]), tuple(sorted(base)), implicit_base=True
                    )))
                ]
                if actual_files != pending["review_files"]:
                    raise RuntimeError("Preview omitted pending migration revisions. Refresh preview before applying.")
            target = str(pending.get("revision") or "head")
            # A generated revision joins the package's migrations only now, so
            # the source holds just what was reviewed and applied.
            moved_from = _adopt_pending_revision(pending_path, output)
            output.append(f"Applying reviewed target {target}...")
            try:
                await asyncio.to_thread(command.upgrade, alembic_cfg, target)
            except Exception:
                if moved_from is not None:
                    _return_pending_revision(moved_from, pending_path)
                raise
        except Exception as exc:
            logger.exception("Startup migration apply failed")
            output.append(str(exc))
            return StartupMigrationActionResponse(
                success=False,
                message=f"Migration apply failed: {exc}",
                output=output,
            )

        revision = pending.get("revision")
        _clear_pending_startup_migration()
        output.append(f"Applied revision: {revision or 'head'}")
        _save_startup_migration_result(StartupMigrationPreviewResponse(
            status="completed", outcome="applied", revision=revision,
            message="Migration applied successfully.", output=output,
        ))
        from sqlalchemy.ext.asyncio import create_async_engine

        check_engine = create_async_engine(db_url)
        try:
            key_problem = await _verify_cluster_secret_key(check_engine)
        finally:
            await check_engine.dispose()
        if key_problem:
            output.append(key_problem)
            return StartupMigrationActionResponse(
                success=False,
                message=f"Migration applied, but {key_problem}",
                output=output,
            )
        return StartupMigrationActionResponse(
            success=True,
            message="Migration applied successfully.",
            output=output,
        )


@router.get("/migrate")
async def run_migrations():
    """Run database migrations and stream the output as Server-Sent Events."""
    _require_schema_management_enabled()
    import asyncio

    from fastapi.responses import StreamingResponse

    async def event_stream():
        # Pre-emit start
        yield f"data: {json.dumps({'status': 'Initializing Database Migrations...'})}\n\n"

        process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "alembic",
            "upgrade",
            "head",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        if process.stdout is None:
            yield f"data: {json.dumps({'status': 'ERROR', 'code': -1})}\n\n"
            return

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line_str = line.decode("utf-8").strip()
            if line_str:
                yield f"data: {json.dumps({'status': line_str})}\n\n"

        await process.wait()

        if process.returncode == 0:
            yield f"data: {json.dumps({'status': 'DONE', 'code': process.returncode})}\n\n"
        else:
            yield f"data: {json.dumps({'status': 'ERROR', 'code': process.returncode})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


class RedisConnectionRequest(BaseModel):
    """Redis connection details submitted during setup."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str = ""
    enabled: bool = True


class RedisConnectionResponse(BaseModel):
    """Result of a Redis connection test."""

    success: bool
    message: str
    latency_ms: float | None = None


class DRMConfig(BaseModel):
    """DRM configuration details."""

    model_config = {"extra": "forbid"}

    device_client_blob_filepath: str | None = None
    device_private_key_filepath: str | None = None


class NetworkDownloadsConfig(BaseModel):
    """Network downloads configuration details."""

    head_concurrency: int = 64
    buffer_size_bytes: int = 10485760


class SetupProxy(BaseModel):
    """A proxy added in the setup wizard, as the network page's form describes it."""

    url: str
    username: str | None = None
    password: str | None = None
    max_connections: int = -1
    group: str | None = None


class NetworkConfig(BaseModel):
    """Network configuration details."""

    max_connections: int = -1
    # Plain URLs are still accepted from older clients.
    proxies: list[SetupProxy | str] = Field(default_factory=list)
    proxy_fallback: bool = False
    guest_lookup_timeout: float = 3.5
    guest_route_timeout: float = 3.0
    downloads: NetworkDownloadsConfig = Field(default_factory=NetworkDownloadsConfig)


class SetupAuthRequest(BaseModel):
    """Auth credential submitted during setup."""

    site: str
    curl_command: str
    proxy_id: str | None = None


class InitialSetupRequest(BaseModel):
    """Payload for completing first-time setup.

    The WebUI wizard collects these values and POSTs them here.
    """

    database: DatabaseConnectionRequest | None = None
    redis: RedisConnectionRequest | None = None
    drm: DRMConfig | None = None
    network: NetworkConfig | None = None
    auths: list[SetupAuthRequest] = cast(
        list[SetupAuthRequest],
        Field(default_factory=list),
    )
    download_directories: list[str] = Field(default_factory=list)
    staging_directories: list[StagingDirectory] = Field(default_factory=list)
    hostname: str | None = None


class InitialSetupResponse(BaseModel):
    """Result of completing first-time setup."""

    success: bool
    message: str
    is_configured: bool = False
    # Accounts that could not be added; setup itself still completed.
    auth_errors: list[str] = Field(default_factory=list)


def _deployment_backend_id() -> str | None:
    """BACKEND_ID keys the stored configuration, so setup must use it."""
    return (runtime_env_value("BACKEND_ID") or "").strip() or None


# ------------------------------------------------------------------ #
# Endpoints
# ------------------------------------------------------------------ #


@router.get("/status", response_model=SetupStatusResponse)
async def get_setup_status() -> SetupStatusResponse:
    """Check whether first-time setup has been completed.

    The WebUI calls this on load to decide whether to show the setup
    wizard or the main dashboard.
    """
    svc = SettingsService()
    status = svc.get_setup_status()
    return SetupStatusResponse(
        **status,  # type: ignore[arg-type]
        backend_id=_deployment_backend_id(),
        redis_target=_configured_redis_target(),
    )


@router.post("/test-database", response_model=DatabaseConnectionResponse)
async def test_database_connection(
    request: DatabaseConnectionRequest,
) -> DatabaseConnectionResponse:
    """Test a Postgres connection without persisting anything.

    The setup wizard calls this so the user can verify credentials
    before committing.
    """
    _require_schema_management_enabled()
    import asyncio
    import time

    # Timeouts (seconds) used for testing database connections and running
    # potentially-long migrations. Increase if your environment is slow.
    DB_CONNECT_TIMEOUT = 30.0
    DB_MIGRATION_TIMEOUT = 300.0

    try:
        import asyncpg
    except ImportError:
        return DatabaseConnectionResponse(
            success=False,
            message="asyncpg not installed — run: uv add asyncpg",
        )

    db_name = _validate_database_identifier(request.name)

    dsn = _build_database_url(request, drivername="postgresql")

    start = time.perf_counter()
    try:
        conn = await asyncio.wait_for(
            cast(Any, asyncpg).connect(dsn=dsn),
            timeout=DB_CONNECT_TIMEOUT,
        )
        try:
            # Check if we have permissions to create tables (i.e., migrate)
            has_schema_privilege = cast(
                bool,
                await conn.fetchval(
                    "SELECT has_schema_privilege(current_user, 'public', 'CREATE')"
                ),
            )
            if not has_schema_privilege:
                return DatabaseConnectionResponse(
                    success=False,
                    message=f"Connected to {db_name}, but user '{request.username}' lacks CREATE privilege on the 'public' schema to migrate it.",
                )

            version = cast(str | None, await conn.fetchval("SELECT version()"))
            latency_ms = (time.perf_counter() - start) * 1000
            short_version = version.split(",")[0] if version else "unknown"

            # Create/migrate tables
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import create_async_engine
            from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates.postgres_site import (
                SUPPORTED_SITES,
            )

            async_dsn = _build_database_url(
                request,
                drivername="postgresql+asyncpg",
            )
            engine = create_async_engine(async_dsn)
            try:
                await _ensure_postgres_extensions(engine)

                # Create schemas
                async with engine.begin() as sa_conn:
                    await sa_conn.execute(
                        text("CREATE SCHEMA IF NOT EXISTS management")
                    )
                    for site in SUPPORTED_SITES:
                        await sa_conn.execute(
                            text(f"CREATE SCHEMA IF NOT EXISTS {site.lower()}")
                        )

                # Run Alembic migrations
                from alembic import command

                alembic_cfg = _build_alembic_config(async_dsn)

                # Check if versions folder has no migration scripts
                versions_dir = _get_versions_dir()
                has_migrations = any(versions_dir.glob("*.py"))
                if not has_migrations:
                    await asyncio.to_thread(
                        command.revision,
                        alembic_cfg,
                        autogenerate=True,
                        message="Initial migration",
                    )

                # Run migrations in a thread to avoid blocking the event loop
                # Wrap the migration in a wait_for to avoid indefinite hangs.
                await asyncio.wait_for(
                    asyncio.to_thread(command.upgrade, alembic_cfg, "head"),
                    timeout=DB_MIGRATION_TIMEOUT,
                )
                key_problem = await _verify_cluster_secret_key(engine)
            finally:
                await engine.dispose()

            if key_problem:
                return DatabaseConnectionResponse(success=False, message=key_problem)
            return DatabaseConnectionResponse(
                success=True,
                message=f"Connected to {short_version} and verified schema",
                latency_ms=round(latency_ms, 2),
            )
        finally:
            await conn.close()
    except asyncpg.InvalidCatalogNameError:
        # Database does not exist, try to create it
        try:
            postgres_dsn = _build_database_url(
                request.model_copy(update={"name": "postgres"}),
                drivername="postgresql",
            )
            sys_conn = await asyncio.wait_for(
                cast(Any, asyncpg).connect(dsn=postgres_dsn),
                timeout=DB_CONNECT_TIMEOUT,
            )
            try:
                await sys_conn.execute(f'CREATE DATABASE "{db_name}"')
            finally:
                await sys_conn.close()

            # Now try connecting to the newly created database
            conn = await asyncio.wait_for(
                cast(Any, asyncpg).connect(dsn=dsn),
                timeout=DB_CONNECT_TIMEOUT,
            )
            try:
                version = cast(
                    str | None,
                    await conn.fetchval("SELECT version()"),
                )
                latency_ms = (time.perf_counter() - start) * 1000
                short_version = version.split(",")[0] if version else "unknown"

                # Create/migrate tables
                from sqlalchemy import text
                from sqlalchemy.ext.asyncio import create_async_engine
                from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates.postgres_site import (
                    SUPPORTED_SITES,
                )

                async_dsn = _build_database_url(
                    request,
                    drivername="postgresql+asyncpg",
                )
                engine = create_async_engine(async_dsn)
                try:
                    await _ensure_postgres_extensions(engine)

                    # Create schemas
                    async with engine.begin() as sa_conn:
                        await sa_conn.execute(
                            text("CREATE SCHEMA IF NOT EXISTS management")
                        )
                        for site in SUPPORTED_SITES:
                            await sa_conn.execute(
                                text(f"CREATE SCHEMA IF NOT EXISTS {site.lower()}")
                            )

                    # Run Alembic migrations
                    from alembic import command

                    alembic_cfg = _build_alembic_config(async_dsn)

                    # Check if versions folder has no migration scripts
                    versions_dir = _get_versions_dir()
                    has_migrations = any(versions_dir.glob("*.py"))
                    if not has_migrations:
                        await asyncio.to_thread(
                            command.revision,
                            alembic_cfg,
                            autogenerate=True,
                            message="Initial migration",
                        )

                    # Run migrations in a thread to avoid blocking the event loop
                    # Wrap the migration in a wait_for to avoid indefinite hangs.
                    await asyncio.wait_for(
                        asyncio.to_thread(command.upgrade, alembic_cfg, "head"),
                        timeout=DB_MIGRATION_TIMEOUT,
                    )
                    key_problem = await _verify_cluster_secret_key(engine)
                finally:
                    await engine.dispose()

                if key_problem:
                    return DatabaseConnectionResponse(success=False, message=key_problem)
                return DatabaseConnectionResponse(
                    success=True,
                    message=f"Created database '{db_name}' and initialized schema on {short_version}",
                    latency_ms=round(latency_ms, 2),
                )
            finally:
                await conn.close()
        except Exception as create_exc:
            return DatabaseConnectionResponse(
                success=False,
                message=f"Database '{db_name}' does not exist and could not be created: {create_exc}",
            )
    except asyncio.TimeoutError:
        return DatabaseConnectionResponse(
            success=False,
            message="Connection timed out (10 s).",
        )
    except OSError as exc:
        msg = str(exc)
        if "Connection refused" in msg:
            msg = f"Connection refused — is Postgres running on {request.host}:{request.port}?"
        return DatabaseConnectionResponse(success=False, message=msg)
    except Exception as exc:
        return DatabaseConnectionResponse(success=False, message=str(exc))


def _configured_database_request() -> DatabaseConnectionRequest | None:
    configured = archive_database_url()
    if not configured:
        return None
    url = make_url(configured)
    return DatabaseConnectionRequest(
        host=url.host or "localhost",
        port=url.port or 5432,
        name=url.database or "ultima_archive",
        username=url.username or "",
        password=url.password or "",
    )


@router.post("/test-configured-database", response_model=DatabaseConnectionResponse)
async def test_configured_database() -> DatabaseConnectionResponse:
    """Test the deployment's own database connection and prepare its schema.

    Choosing the configured connection skips the form, but the schema must
    still exist before setup can store the configuration.
    """
    request = _configured_database_request()
    if request is None:
        raise HTTPException(
            status_code=400,
            detail="This deployment does not configure a database connection.",
        )
    return await test_database_connection(request)


def _configured_redis_request() -> RedisConnectionRequest | None:
    host = (runtime_env_value("REDIS_HOST") or "").strip()
    if not host:
        return None
    return RedisConnectionRequest(
        host=host,
        port=int(runtime_env_value("REDIS_PORT") or 6379),
        db=int(runtime_env_value("REDIS_DB") or 0),
        password=runtime_env_value("REDIS_PASSWORD") or "",
    )


def _configured_redis_target() -> str | None:
    request = _configured_redis_request()
    return f"{request.host}:{request.port}" if request else None


@router.post("/test-configured-redis", response_model=RedisConnectionResponse)
async def test_configured_redis() -> RedisConnectionResponse:
    """Test the Redis connection the deployment already configures."""
    request = _configured_redis_request()
    if request is None:
        raise HTTPException(
            status_code=400,
            detail="This deployment does not configure a Redis connection.",
        )
    return await test_redis_connection(request)


@router.post("/test-redis", response_model=RedisConnectionResponse)
async def test_redis_connection(
    request: RedisConnectionRequest,
) -> RedisConnectionResponse:
    """Test a Redis connection without persisting anything."""
    import time

    try:
        import redis.asyncio as redis
    except ImportError:
        return RedisConnectionResponse(
            success=False,
            message="redis not installed — run: uv add redis",
        )

    if not request.enabled:
        return RedisConnectionResponse(
            success=True,
            message="Redis is disabled.",
        )

    start = time.perf_counter()
    client: Any | None = None
    try:
        client = cast(Any, redis).Redis(
            host=request.host,
            port=request.port,
            db=request.db,
            password=request.password or None,
            socket_timeout=5.0,
        )
        if client is None:
            raise RuntimeError("Redis client initialization failed")
        await client.ping()
        latency_ms = (time.perf_counter() - start) * 1000
        return RedisConnectionResponse(
            success=True,
            message="Connected to Redis successfully.",
            latency_ms=round(latency_ms, 2),
        )
    except Exception as exc:
        return RedisConnectionResponse(
            success=False,
            message=f"Failed to connect to Redis: {exc}",
        )
    finally:
        if client is not None:
            await client.close()


@router.post("/complete", response_model=InitialSetupResponse, dependencies=[Depends(guard_proxy_assignment)])
async def complete_setup(
    request: InitialSetupRequest,
) -> InitialSetupResponse:
    """Finalise first-time setup.

    Flow:
    1. Bootstrap postgres engine with the user-supplied credentials.
    2. Persist the canonical application snapshot.
    3. Merge owned bootstrap keys into the shared environment file.
    """
    from ultima_scraper_db_archive.global_db import (
        get_postgres_engine,
        set_postgres_url,
    )

    from ultima_scraper_backend.config_settings_sync import (
        persist_config_snapshot,
        rollback_config_snapshot,
    )
    from ultima_scraper_core import update_env

    try:
        deployment_backend_id = _deployment_backend_id()
        if (
            deployment_backend_id
            and request.hostname
            and request.hostname.strip() != deployment_backend_id
        ):
            # The stored configuration is looked up by BACKEND_ID, so a
            # different name here would leave this backend unconfigured.
            raise HTTPException(
                status_code=409,
                detail=(
                    f"This deployment sets BACKEND_ID={deployment_backend_id}. "
                    "Leave the instance hostname empty, or change BACKEND_ID and restart."
                ),
            )

        # ---------------------------------------------------------- #
        # 0. Database is mandatory for first-time setup. A connection
        #    supplied by the deployment environment is used as-is.
        # ---------------------------------------------------------- #
        configured_database_url = archive_database_url()
        if request.database is not None:
            db_port = request.database.port or 5432
            database_url = _build_database_url(request.database, drivername="postgresql")
        elif configured_database_url:
            db_port = make_url(configured_database_url).port or 5432
            database_url = configured_database_url
            # The wizard prepares the schema in its database step; an API
            # client that skipped it gets the same preparation here. Runtimes
            # without schema management are migrated by their own service.
            if _schema_management_enabled():
                prepared = await test_configured_database()
                if not prepared.success:
                    raise HTTPException(status_code=400, detail=prepared.message)
        else:
            raise HTTPException(
                status_code=400,
                detail="Database configuration is required for first-time setup.",
            )

        # Bootstrap the sync postgres engine so every global_session()
        # call from here on writes to the user's chosen postgres.
        engine = get_postgres_engine()
        if engine is None:
            set_postgres_url(database_url)
        elif engine.url.set(
            drivername="postgresql", port=engine.url.port or 5432
        ) != make_url(database_url).set(drivername="postgresql", port=db_port):
            raise HTTPException(
                status_code=409,
                detail=(
                    "The running service already uses a different database connection. "
                    "Update its bootstrap environment and restart before completing setup."
                ),
            )

        svc = SettingsService()

        # ---------------------------------------------------------- #
        # 3. Process Auths
        # ---------------------------------------------------------- #
        auth_errors: list[str] = []
        if request.auths:
            from .curl_parser import AddAuthFromCurlRequest, add_auth_from_curl

            for auth_req in request.auths:
                try:
                    await add_auth_from_curl(
                        AddAuthFromCurlRequest(
                            curl_command=auth_req.curl_command,
                            site=auth_req.site,
                            proxy_id=auth_req.proxy_id,
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to process auth for {auth_req.site}: {e}")
                    detail = e.detail if isinstance(e, HTTPException) else str(e)
                    auth_errors.append(f"{auth_req.site}: {detail}")

        # ---------------------------------------------------------- #
        # 4. Build full config and save to postgres
        # ---------------------------------------------------------- #
        from ultima_scraper_backend.config import (
            UltimaScraperCollectionConfig,
        )

        config = UltimaScraperCollectionConfig()

        if request.drm is not None:
            from pathlib import Path as _Path

            config.settings.drm.device_client_blob_filepath = (
                _Path(request.drm.device_client_blob_filepath)
                if request.drm.device_client_blob_filepath
                else None
            )
            config.settings.drm.device_private_key_filepath = (
                _Path(request.drm.device_private_key_filepath)
                if request.drm.device_private_key_filepath
                else None
            )

        if request.network is not None:
            config.settings.network.max_connections = request.network.max_connections
            config.settings.network.proxy_fallback = request.network.proxy_fallback
            config.settings.network.downloads.head_concurrency = (
                request.network.downloads.head_concurrency
            )
            config.settings.network.downloads.buffer_size_bytes = (
                request.network.downloads.buffer_size_bytes
            )

            from ultima_scraper_backend.config import Proxy

            config.settings.network.proxies = [
                Proxy(url=p) if isinstance(p, str) else Proxy(**p.model_dump())
                for p in request.network.proxies
            ]

        config.settings.hostname = request.hostname or runtime_env_value("BACKEND_ID") or config.settings.hostname
        config.settings.backend.backend_id = config.settings.hostname

        if request.download_directories:
            from pathlib import Path as _Path

            from ultima_scraper_backend.config import (
                Drive,
                split_drive_configs_by_mount,
            )

            config.settings.download_setup.drives = split_drive_configs_by_mount(
                [Drive(directories=[_Path(d) for d in request.download_directories])]
            )
        config.settings.download_setup.staging_directories = [
            directory.model_copy(deep=True) for directory in request.staging_directories
        ]

        config.is_configured = True
        scope = config.config_write_backend()
        svc.invalidate()
        existing = svc.get_model(scope, "config")
        if existing is not None:
            raise HTTPException(status_code=409, detail="Configuration already exists; use application settings")
        snapshot = persist_config_snapshot(config)

        # Setup owns only these bootstrap keys; other projects retain theirs.
        # The config row above is the setup state, so the legacy IS_SETUP
        # flag is removed. DATABASE_URL is an accepted alias that sibling
        # services may still read, so an existing value is left untouched.
        env_data: dict[str, object | None] = {"IS_SETUP": None}
        if database_url != configured_database_url:
            env_data["ULTIMA_ARCHIVE_DATABASE_URL"] = database_url
        if request.hostname:
            env_data["BACKEND_ID"] = request.hostname
        if request.redis is not None:
            for field, key in {
                "host": "REDIS_HOST", "port": "REDIS_PORT", "db": "REDIS_DB",
                "password": "REDIS_PASSWORD", "enabled": "REDIS_ENABLED",
            }.items():
                value = getattr(request.redis, field)
                env_data[key] = str(value).lower() if isinstance(value, bool) else value
        try:
            update_env(env_data)
        except Exception:
            rollback_config_snapshot(snapshot)
            raise

        # A new archive has no legacy favourites to copy, so it starts on the
        # canonical store. Sites with legacy data still need the explicit
        # favourites cutover; failure here does not undo setup.
        try:
            from ultima_scraper_db_archive.favourites_migration import (
                activate_empty_sites,
            )

            with get_postgres_engine().connect() as connection:
                activated = activate_empty_sites(connection)
            if activated:
                logger.info("Activated canonical favourites for %s", ", ".join(activated))
        except Exception:
            logger.warning(
                "Could not activate canonical favourites; run "
                "`python -m ultima_scraper_db_archive.favourites_migration activate-empty`",
                exc_info=True,
            )

        logger.info("First-time setup completed successfully")
        return InitialSetupResponse(
            success=True,
            message="Setup completed successfully",
            is_configured=True,
            auth_errors=auth_errors,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("First-time setup failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reset", response_model=InitialSetupResponse)
async def reset_setup() -> InitialSetupResponse:
    """Explain that setup cannot be re-run over an existing configuration.

    Setup state is the canonical config row, so the wizard cannot be
    restarted without discarding it. Edit an existing installation through
    application settings instead.
    """
    if SettingsService().is_configured():
        raise HTTPException(
            status_code=409,
            detail=(
                "Setup is complete because this backend has a stored configuration. "
                "Change it in Settings; the wizard cannot run over it."
            ),
        )
    return InitialSetupResponse(
        success=True,
        message="Setup has not been completed; the wizard is already available.",
        is_configured=False,
    )
