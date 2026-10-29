from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from datetime import date, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

DEFAULT_LOG_ROOT = "logs"
DEFAULT_LOG_RETENTION_DAYS = 7


class RuntimeLogFormatter(logging.Formatter):
    """Keep diagnostic stacks while excluding credentials and SQL bind data."""

    def format(self, record: logging.LogRecord) -> str:
        from .security import redact_sensitive_text

        rendered = super().format(record)
        rendered = re.sub(
            r"(?m)^([ \t|+]*)\[parameters:.*$", r"\1[parameters: REDACTED]", rendered,
        )
        rendered = re.sub(
            r"((?:postgresql(?:\+\w+)?|postgres|redis|rediss|https?)://[^\s:/@]+:)[^@\s]+@",
            r"\1[REDACTED]@", rendered, flags=re.IGNORECASE,
        )
        rendered = re.sub(
            r'''(["']?\b(?:authorization|cookie|x-bc|password|passwd|secret|client_secret|api_secret|(?:access_|refresh_|admin_|auth_)?token|api[_-]?key)["']?\s*[:=]\s*)'''
            r'''(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|(?:Basic|Bearer)\s+[^\s,;]+|[^\s,;]+)''',
            r"\1[REDACTED]", rendered, flags=re.IGNORECASE,
        )
        rendered = re.sub(
            r"(\b(?:cookie|set-cookie)\s*:\s*)[^\r\n]*", r"\1[REDACTED]",
            rendered, flags=re.IGNORECASE,
        )
        return redact_sensitive_text(rendered)


class DailyErrorFileHandler(logging.Handler):
    """Append complete error records from every process to one daily file."""

    def __init__(self, directory: Path):
        super().__init__(logging.ERROR)
        self.directory = directory

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record) + "\n"
            day = datetime.now().astimezone().date().isoformat()
            path = self.directory / f"errors-{day}.log"
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
                if os.name == "posix":
                    import fcntl

                    # Error stacks can exceed one OS write. Serialize the whole
                    # record across processes so simultaneous traces stay intact.
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
                stream.write(message)
                stream.flush()
        except Exception:  # noqa: BLE001 - logging must not reject application work
            self.handleError(record)


def get_runtime_log_directory() -> Path:
    explicit = os.environ.get("USR_LOG_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    root = os.environ.get("LOG_ROOT")
    if root:
        return (Path(root).expanduser() / "runtime").resolve()
    project = Path(os.environ.get("USR_REPOSITORY_ROOT") or Path.cwd())
    return (project / "logs" / "runtime").resolve()


def configure_runtime_logging(
    service: str, *, console: bool = True, file_level: int = logging.INFO,
) -> Path:
    """Configure each spawned process independently; share only the error file."""
    service = re.sub(r"[^A-Za-z0-9_.-]", "-", service)
    directory = get_runtime_log_directory()
    directory.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    identity = f"usr-runtime:{(os.getpid(), service, str(directory), console, file_level)!r}:"
    if any((handler.name or "").startswith(identity) for handler in root.handlers):
        return directory
    for handler in root.handlers[:]:
        if (handler.name or "").startswith("usr-runtime:"):
            root.removeHandler(handler)
            handler.close()
    root.setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").propagate = True
    logging.getLogger("uvicorn.error").propagate = True
    formatter = RuntimeLogFormatter(
        f"%(asctime)s [pid=%(process)d service={service}] %(levelname)s %(name)s: %(message)s"
    )
    file_handler = TimedRotatingFileHandler(
        directory / f"{service}-{os.getpid()}.log", when="midnight",
        backupCount=get_log_retention_days(), encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    handlers: list[logging.Handler] = [file_handler, DailyErrorFileHandler(directory)]
    if console:
        handlers.append(logging.StreamHandler())
    for number, handler in enumerate(handlers):
        handler.name = identity + str(number)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    _capture_unhandled_exceptions()
    logging.getLogger(__name__).info("Persistent runtime logs: %s", directory)
    return directory


def _capture_unhandled_exceptions() -> None:
    if getattr(sys.excepthook, "_usr_runtime_hook", False):
        return
    previous = sys.excepthook
    previous_thread = threading.excepthook

    def exception_hook(kind, value, traceback):
        if not issubclass(kind, KeyboardInterrupt):
            logging.getLogger(__name__).critical("Unhandled exception", exc_info=(kind, value, traceback))
        previous(kind, value, traceback)

    def thread_hook(args):
        if not issubclass(args.exc_type, (KeyboardInterrupt, SystemExit)):
            logging.getLogger(__name__).critical(
                "Unhandled exception in thread %s", args.thread.name if args.thread else "unknown",
                exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
            )
        previous_thread(args)

    setattr(exception_hook, "_usr_runtime_hook", True)  # noqa: B010 - annotate a callable hook
    sys.excepthook = exception_hook
    threading.excepthook = thread_hook


def get_log_root() -> Path:
    configured_root = os.environ.get("LOG_ROOT", DEFAULT_LOG_ROOT).strip()
    return Path(configured_root or DEFAULT_LOG_ROOT)


def get_log_retention_days(default: int = DEFAULT_LOG_RETENTION_DAYS) -> int:
    configured_days = os.environ.get("LOG_RETENTION_DAYS")
    if configured_days is None:
        return default

    try:
        parsed_days = int(configured_days)
    except ValueError:
        return default

    return parsed_days if parsed_days > 0 else default


def ensure_log_directory(log_type: str) -> Path:
    log_dir = get_log_root() / log_type
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def get_daily_log_path(log_type: str, base_name: str, day: date | None = None) -> Path:
    target_day = day or datetime.now().astimezone().date()
    return ensure_log_directory(log_type) / f"{base_name}-{target_day.isoformat()}.log"


def prune_old_logs(log_type: str, retention_days: int | None = None) -> None:
    days_to_keep = retention_days or get_log_retention_days()
    cutoff_timestamp = time.time() - (days_to_keep * 24 * 60 * 60)

    for log_path in ensure_log_directory(log_type).glob("*.log*"):
        try:
            if log_path.stat().st_mtime < cutoff_timestamp:
                log_path.unlink()
        except FileNotFoundError:
            continue


def create_daily_rotating_file_handler(
    log_type: str,
    filename: str,
    level: int,
    retention_days: int | None = None,
) -> TimedRotatingFileHandler:
    handler = TimedRotatingFileHandler(
        filename=ensure_log_directory(log_type) / filename,
        when="midnight",
        interval=1,
        backupCount=retention_days or get_log_retention_days(),
        encoding="utf-8",
        utc=False,
    )
    handler.setLevel(level)
    return handler
