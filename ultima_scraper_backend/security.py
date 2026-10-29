from __future__ import annotations

import hmac
import ipaddress
import logging
import re
import socket
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlparse

from starlette.requests import Request
from ultima_scraper_core import runtime_env_value
from ultima_scraper_db_archive.crypto import set_secret_decrypt_failure_observer

logger = logging.getLogger(__name__)


DEFAULT_LOCAL_ORIGINS = [
    "http://127.0.0.1:21120",
    "http://localhost:21120",
    "http://127.0.0.1:2112",
    "http://localhost:2112",
    "http://127.0.0.1:21121",
    "http://localhost:21121",
]

DEFAULT_LOCAL_HOSTS = [
    "127.0.0.1",
    "localhost",
]

PRIVATE_NETWORK_ORIGIN_REGEX = (
    r"^https?://("
    r"localhost"
    r"|127(?:\.\d{1,3}){3}"
    r"|10(?:\.\d{1,3}){3}"
    r"|192\.168(?:\.\d{1,3}){2}"
    r"|172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2}"
    r"|[A-Za-z0-9-]+\.local"
    r")(?::\d{1,5})?$"
)

SECURITY_METRICS: Counter[str] = Counter()


def _record_secret_decrypt_failure() -> None:
    SECURITY_METRICS["secret_decrypt_fail_total"] += 1


set_secret_decrypt_failure_observer(_record_secret_decrypt_failure)

_TOKEN_RE = re.compile(r"(bearer\s+)([A-Za-z0-9._~+\-/=]+)", re.IGNORECASE)
_SECRET_KV_RE = re.compile(
    r"(?i)\b(cookie|authorization|x-bc|token|password|secret)\b\s*[:=]\s*([^\s,;]+)"
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = runtime_env_value(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _split_csv_env(name: str, fallback: Sequence[str]) -> list[str]:
    raw = runtime_env_value(name)
    if not raw:
        return [x.strip() for x in fallback if x.strip()]
    values = [v.strip() for v in raw.split(",") if v.strip()]
    return values or [x.strip() for x in fallback if x.strip()]


def get_security_enforcement_mode() -> str:
    mode = (runtime_env_value("SECURITY_ENFORCEMENT") or "strict").strip().lower()
    if mode not in {"strict", "report_only", "off"}:
        return "strict"
    return mode


def admin_token_required(default: bool = True) -> bool:
    return _env_bool("USR_ADMIN_TOKEN_REQUIRED", default)


def get_admin_token() -> str | None:
    token = (runtime_env_value("USR_ADMIN_TOKEN") or "").strip()
    return token or None


def get_allowed_origins(default: Sequence[str] | None = None) -> list[str]:
    return _split_csv_env("USR_ALLOWED_ORIGINS", default or DEFAULT_LOCAL_ORIGINS)


def get_allowed_hosts(default: Sequence[str] | None = None) -> list[str]:
    raw_hosts = runtime_env_value("USR_ALLOWED_HOSTS")
    if raw_hosts:
        hosts = [h.strip() for h in raw_hosts.split(",") if h.strip()]
    elif allow_remote_bind(False):
        # Remote bind UX default: allow host headers unless explicitly restricted.
        hosts = ["*"]
    else:
        hosts = [x.strip() for x in (default or DEFAULT_LOCAL_HOSTS) if x.strip()]
    normalized = [h.split(":")[0].strip().lower() for h in hosts if h.strip()]
    return normalized or [h.lower() for h in (default or DEFAULT_LOCAL_HOSTS)]


def get_ws_allowed_origins(default: Sequence[str] | None = None) -> list[str]:
    return _split_csv_env("USR_WS_ALLOWED_ORIGINS", default or DEFAULT_LOCAL_ORIGINS)


def get_allowed_origin_regex(default: str | None = None) -> str | None:
    raw = (runtime_env_value("USR_ALLOWED_ORIGIN_REGEX") or "").strip()
    if raw:
        return raw
    if allow_remote_bind(False):
        return PRIVATE_NETWORK_ORIGIN_REGEX
    return default


def allow_docs(default: bool = False) -> bool:
    return _env_bool("USR_ALLOW_DOCS", default)


def allow_remote_bind(default: bool = False) -> bool:
    return _env_bool("USR_ALLOW_REMOTE_BIND", default)


def get_default_bind_host(default: str = "127.0.0.1") -> str:
    if allow_remote_bind(False):
        return "0.0.0.0"
    return default


def extract_admin_token(headers: Mapping[str, str]) -> str | None:
    direct = headers.get("x-usr-admin-token")
    if direct:
        return direct.strip()
    auth_header = headers.get("authorization", "")
    if not auth_header:
        return None
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip() or None
    return None


def extract_admin_token_from_request(request: Request) -> str | None:
    header_token = extract_admin_token(request.headers)
    if header_token:
        return header_token

    for key in ("token", "admin_token", "x_usr_admin_token"):
        candidate = request.query_params.get(key)
        if candidate and candidate.strip():
            return candidate.strip()

    return None


def is_admin_token_valid(token: str | None) -> bool:
    """The backend's own admin token, or the token of a client in its cluster."""
    if not token:
        return False
    expected = get_admin_token()
    if expected and hmac.compare_digest(expected, token):
        return True
    from .cluster_clients import is_client_token

    return is_client_token(token)


def _request_source(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "").strip()
    if xff:
        return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _audit_denied(request: Request, reason: str, request_id: str) -> None:
    SECURITY_METRICS["auth_denied_total"] += 1
    logger.warning(
        "[SECURITY] Request denied: reason=%s method=%s path=%s source=%s request_id=%s",
        reason,
        request.method,
        request.url.path,
        _request_source(request),
        request_id,
    )


def ensure_http_admin_auth(
    request: Request,
    *,
    request_id: str,
    bypass_paths: set[str] | None = None,
    token_required: bool = True,
) -> None:
    from fastapi import HTTPException

    mode = get_security_enforcement_mode()
    if mode == "off":
        return
    if not admin_token_required(token_required):
        return

    bypass = bypass_paths or set()

    if request.url.path in bypass:
        return

    token = extract_admin_token_from_request(request)
    if is_admin_token_valid(token):
        return

    _audit_denied(request, "invalid_or_missing_admin_token", request_id)
    if mode == "report_only":
        return

    raise HTTPException(status_code=401, detail="Unauthorized")


def is_origin_allowed(
    origin: str | None,
    allowed_origins: Sequence[str],
    *,
    allowed_origin_regex: str | None = None,
) -> bool:
    if not origin:
        # Non-browser clients usually omit Origin.
        return True
    normalized = origin.strip().lower()
    allow = {x.strip().lower() for x in allowed_origins if x.strip()}
    if normalized in allow:
        return True
    if allowed_origin_regex:
        try:
            return re.match(allowed_origin_regex, normalized) is not None
        except re.error:
            logger.warning("Invalid USR_ALLOWED_ORIGIN_REGEX; ignoring regex check")
    return False


def check_ws_origin(
    origin: str | None,
    allowed_origins: Sequence[str],
    *,
    allowed_origin_regex: str | None = None,
) -> bool:
    return is_origin_allowed(
        origin,
        allowed_origins,
        allowed_origin_regex=allowed_origin_regex,
    )


def ensure_ws_admin_auth(
    *,
    origin: str | None,
    token: str | None,
    allowed_origins: Sequence[str],
    allowed_origin_regex: str | None = None,
    token_required: bool = True,
) -> None:
    from fastapi import HTTPException

    mode = get_security_enforcement_mode()
    if mode == "off":
        return

    if not check_ws_origin(
        origin,
        allowed_origins,
        allowed_origin_regex=allowed_origin_regex,
    ):
        SECURITY_METRICS["ws_origin_denied_total"] += 1
        if mode != "report_only":
            raise HTTPException(status_code=403, detail="Forbidden origin")

    if not admin_token_required(token_required):
        return

    if is_admin_token_valid(token):

        return

    SECURITY_METRICS["ws_auth_denied_total"] += 1
    if mode != "report_only":
        raise HTTPException(status_code=401, detail="Unauthorized")


def build_request_id(existing: str | None = None) -> str:
    if existing:
        return existing
    return uuid.uuid4().hex


def redact_sensitive_text(text: str) -> str:
    redacted = _TOKEN_RE.sub(r"\1[REDACTED]", text)
    redacted = _SECRET_KV_RE.sub(r"\1=[REDACTED]", redacted)
    return redacted


class RedactingLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            record.msg = redact_sensitive_text(str(msg))
            record.args = ()
        except Exception:
            return True
        return True


def configure_log_redaction() -> None:
    root = logging.getLogger()
    already = any(isinstance(f, RedactingLogFilter) for f in root.filters)
    if already:
        return
    root.addFilter(RedactingLogFilter())


def _domain_allowed(host: str, allowed_domains: Sequence[str] | None) -> bool:
    if not allowed_domains:
        return True
    host_l = host.lower()
    for domain in allowed_domains:
        d = domain.strip().lower()
        if not d:
            continue
        if host_l == d or host_l.endswith("." + d):
            return True
    return False


def _is_unsafe_ip(ip_text: str) -> bool:
    ip = ipaddress.ip_address(ip_text)
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def validate_outbound_url(
    url: str,
    *,
    allowed_domains: Sequence[str] | None = None,
    allow_private: bool = False,
) -> None:
    from fastapi import HTTPException

    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"}:
        SECURITY_METRICS["ssrf_blocked_total"] += 1
        raise HTTPException(status_code=400, detail="Only HTTP/HTTPS URLs are allowed")

    host = parsed.hostname
    if not host:
        SECURITY_METRICS["ssrf_blocked_total"] += 1
        raise HTTPException(status_code=400, detail="URL host is required")

    if not _domain_allowed(host, allowed_domains):
        SECURITY_METRICS["ssrf_blocked_total"] += 1
        raise HTTPException(status_code=403, detail="Domain is not allowed")

    default_port = 443 if parsed.scheme == "https" else 80
    try:
        infos = socket.getaddrinfo(
            host,
            parsed.port or default_port,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror:
        SECURITY_METRICS["ssrf_blocked_total"] += 1
        raise HTTPException(status_code=400, detail="Host could not be resolved") from None

    if allow_private:
        return

    for info in infos:
        sockaddr = info[4]
        ip_text = str(sockaddr[0])
        if _is_unsafe_ip(ip_text):
            SECURITY_METRICS["ssrf_blocked_total"] += 1
            raise HTTPException(status_code=403, detail="Target IP is not allowed")


@dataclass(slots=True)
class RateLimitResult:
    allowed: bool
    retry_after_seconds: int


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def hit(self, key: str, *, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            q = self._events[key]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= limit:
                retry_after = int(max(1, window_seconds - (now - q[0])))
                return RateLimitResult(False, retry_after)
            q.append(now)
            return RateLimitResult(True, 0)


_rate_limiter = InMemoryRateLimiter()


def _rate_key(bucket: str, request: Request) -> str:
    token = extract_admin_token(request.headers) or "-"
    source = _request_source(request)
    return f"{bucket}:{source}:{token}"


def enforce_rate_limit(
    request: Request,
    *,
    bucket: str,
    limit: int,
    window_seconds: int,
) -> None:
    from fastapi import HTTPException

    key = _rate_key(bucket, request)
    result = _rate_limiter.hit(key, limit=limit, window_seconds=window_seconds)
    if result.allowed:
        return
    SECURITY_METRICS["rate_limit_block_total"] += 1
    raise HTTPException(
        status_code=429,
        detail="Rate limit exceeded",
        headers={"Retry-After": str(result.retry_after_seconds)},
    )


def log_security_startup_report(
    *,
    bind_host: str,
    cors_origins: Sequence[str],
    trusted_hosts: Sequence[str],
    docs_enabled: bool,
) -> None:
    logger.info(
        "[SECURITY] Startup report: bind_host=%s cors_origins=%s trusted_hosts=%s docs_enabled=%s admin_token_required=%s enforcement=%s",
        bind_host,
        ",".join(cors_origins),
        ",".join(trusted_hosts),
        docs_enabled,
        admin_token_required(True),
        get_security_enforcement_mode(),
    )


# .env.example ships replace-with-... values; running with one would publish a
# well-known credential.
_PLACEHOLDER_MARKER = "replace-with-"
_PLACEHOLDER_CHECKED_VARIABLES = (
    "USR_ADMIN_TOKEN",
    "USR_SECRET_KEY",
    "USR_SECRET_KEY_PREVIOUS",
    "ULTIMA_ARCHIVE_DATABASE_URL",
    "DATABASE_URL",
    "REDIS_PASSWORD",
)


def reject_placeholder_secrets() -> None:
    for name in _PLACEHOLDER_CHECKED_VARIABLES:
        if _PLACEHOLDER_MARKER in (runtime_env_value(name) or ""):
            raise RuntimeError(
                f"{name} still has the .env.example placeholder; run "
                "`python -m ultima_scraper_backend.bootstrap` or set a real value"
            )


def validate_security_configuration() -> None:
    reject_placeholder_secrets()
    mode = get_security_enforcement_mode()
    if mode == "off":
        return
    if admin_token_required(True) and not get_admin_token():
        if mode == "report_only":
            logger.warning(
                "[SECURITY] USR_ADMIN_TOKEN is missing while report-only mode is enabled"
            )
            return
        raise RuntimeError(
            "USR_ADMIN_TOKEN is required when auth enforcement is enabled"
        )
