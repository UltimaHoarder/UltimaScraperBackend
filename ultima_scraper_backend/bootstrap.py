"""Create the deployment bootstrap environment before the first start.

The WebUI setup wizard only edits application settings. Root secrets and
connections must exist before the API starts, so this command writes them:

    python -m ultima_scraper_backend.bootstrap
    python -m ultima_scraper_backend.bootstrap --compose --env-file .env
    python -m ultima_scraper_backend.bootstrap --database-url postgresql+psycopg://... --check

Existing values are never replaced (only empty values and ``replace-with-...``
placeholders from .env.example are), so re-running it is safe. Generated values
are written only to the file (mode 0600); the output names keys, not values.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from pathlib import Path

from ultima_scraper_core import get_env_path, read_env, update_env


def generated_secrets(*, compose: bool) -> dict[str, str]:
    """Random root secrets. Compose also needs the bundled service passwords."""
    values = {
        "USR_ADMIN_TOKEN": secrets.token_urlsafe(32),
        "USR_SECRET_KEY": secrets.token_urlsafe(32),
        "USR_ENCRYPT_SECRETS": "true",
    }
    if compose:
        values["POSTGRES_PASSWORD"] = secrets.token_urlsafe(24)
        values["REDIS_PASSWORD"] = secrets.token_urlsafe(24)
    return values


def bootstrap_values(args: argparse.Namespace) -> dict[str, str]:
    values = generated_secrets(compose=args.compose)
    if args.database_url:
        values["ULTIMA_ARCHIVE_DATABASE_URL"] = args.database_url
    if args.redis_host:
        values["REDIS_HOST"] = args.redis_host
    if args.redis_port is not None:
        values["REDIS_PORT"] = str(args.redis_port)
    if args.backend_id:
        values["BACKEND_ID"] = args.backend_id
    return values


# .env.example placeholders, which are replaced like missing keys.
_PLACEHOLDER_PREFIX = "replace-with-"


def write_bootstrap(path: Path, values: dict[str, str]) -> list[str]:
    """Add missing, empty or placeholder keys only; return the names that were written."""
    existing = read_env(path)
    added = [
        key
        for key in values
        if not existing.get(key) or existing[key].startswith(_PLACEHOLDER_PREFIX)
    ]
    if added:
        update_env({key: values[key] for key in added}, path)
    if os.name != "nt" and path.exists():
        path.chmod(0o600)
    return added


def check_connections(path: Path) -> list[str]:
    """Connect to the configured PostgreSQL and Redis; return failures."""
    values = {**read_env(path), **os.environ}
    failures: list[str] = []
    database_url = (
        values.get("ULTIMA_ARCHIVE_DATABASE_URL") or values.get("DATABASE_URL")
    )
    if not database_url:
        failures.append("PostgreSQL: no ULTIMA_ARCHIVE_DATABASE_URL configured")
    else:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url, pool_pre_ping=True)
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception as exc:
            failures.append(f"PostgreSQL: {type(exc).__name__}: {exc}")
        finally:
            engine.dispose()

    import redis

    client = redis.Redis(
        host=values.get("REDIS_HOST", "127.0.0.1"),
        port=int(values.get("REDIS_PORT", "6379")),
        db=int(values.get("REDIS_DB", "0")),
        password=values.get("REDIS_PASSWORD") or None,
        socket_connect_timeout=5,
    )
    try:
        client.ping()
    except Exception as exc:
        failures.append(f"Redis: {type(exc).__name__}: {exc}")
    finally:
        client.close()
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Environment file to write (default: the shared UltimaScraper .env)",
    )
    parser.add_argument(
        "--compose",
        action="store_true",
        help="Also generate POSTGRES_PASSWORD and REDIS_PASSWORD for the bundled Compose stack",
    )
    parser.add_argument("--database-url")
    parser.add_argument("--redis-host")
    parser.add_argument("--redis-port", type=int)
    parser.add_argument("--backend-id")
    parser.add_argument(
        "--check", action="store_true", help="Test PostgreSQL and Redis afterwards"
    )
    args = parser.parse_args(argv)

    path = args.env_file or get_env_path()
    added = write_bootstrap(path, bootstrap_values(args))
    if added:
        print(f"Added to {path}: {', '.join(added)}")
    else:
        print(f"{path} already has every bootstrap key; nothing changed.")
    if "USR_ADMIN_TOKEN" in added:
        print("Sign in to the WebUI with the USR_ADMIN_TOKEN value from that file.")

    if args.check:
        failures = check_connections(path)
        for failure in failures:
            print(failure, file=sys.stderr)
        if failures:
            return 1
        print("PostgreSQL and Redis are reachable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
