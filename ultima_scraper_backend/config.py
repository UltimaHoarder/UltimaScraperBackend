import os
import socket
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Self, cast
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, Field, PrivateAttr, field_validator
from ultima_scraper_api.config import DRM as USAPIDRM
from ultima_scraper_api.config import FanslyAPIConfig as USAFanslyAPIConfig
from ultima_scraper_api.config import LoyalFansAPIConfig as USALoyalFansAPIConfig
from ultima_scraper_api.config import MediaQuality as USAPIMediaQuality
from ultima_scraper_api.config import Network as USAPINetwork
from ultima_scraper_api.config import OnlyFansAPIConfig as USAOnlyFansAPIConfig
from ultima_scraper_api.config import Proxy as USAPIProxy
from ultima_scraper_api.config import ProxyGroupSettings
from ultima_scraper_api.config import Redis as USAPIRedisConfig
from ultima_scraper_api.config import Settings as USAPISettings
from ultima_scraper_api.config import Sites as USASites
from ultima_scraper_api.config import UltimaScraperAPIConfig
from ultima_scraper_api.config import Webhook as USAPIWebhook
from ultima_scraper_api.config import Webhooks as USAPIWebhooks
from ultima_scraper_core import runtime_env_value
from ultima_scraper_db_archive.crypto import (
    decrypt_secret_value,
    encrypt_secret_value,
)

from .security import (
    DEFAULT_LOCAL_HOSTS,
    DEFAULT_LOCAL_ORIGINS,
    PRIVATE_NETWORK_ORIGIN_REGEX,
)
from .webhook_config import WebhookRouting


class ExecutionMode(str, Enum):
    LOCAL = "local"
    DISTRIBUTED = "distributed"
    REDIS_METRICS = "redis_metrics"


class ProcessRole(str, Enum):
    CONTROL = "control"
    WORKER = "worker"


class Webhook(BaseModel):
    url: str | None = None
    hide_info: list[str] = []
    active: bool


class Webhooks(BaseModel):
    auth: Webhook = Webhook(active=False)
    download: Webhook = Webhook(active=False)


class MediaQuality(BaseModel):
    image: str = "source"
    video: str = "source"
    audio: str = "source"


class DownloadSettings(BaseModel):
    max_concurrency: int = Field(default=64, gt=0, strict=True)
    # When omitted, file admission follows the configured part-request budget.
    max_file_concurrency: int | None = Field(default=None, gt=0, strict=True)
    per_domain_concurrency: int = Field(default=8, gt=0, strict=True)
    head_concurrency: int = Field(default=64, gt=0, strict=True)
    buffer_size_bytes: int = Field(default=10 * 1024 * 1024, gt=0, strict=True)


class PluginProxyOwner(BaseModel):
    plugin_id: str
    source: str
    provider_id: str


class Proxy(BaseModel):
    url: str
    username: str | None = None
    password: str | None = None
    enabled: bool = True
    max_connections: int = -1
    downloads: DownloadSettings = DownloadSettings()
    group: str | None = None
    managed_by: PluginProxyOwner | None = None
    guest_lookup_timeout: float | None = None

    def to_usapi_proxy(self) -> USAPIProxy:
        return USAPIProxy(
            url=self.url,
            username=self.username,
            password=self.password,
            enabled=self.enabled,
            max_connections=self.max_connections,
            group=self.group,
            guest_lookup_timeout=self.guest_lookup_timeout,
        )


class Network(BaseModel):
    max_connections: int = -1
    proxies: list[Proxy] = []
    proxy_fallback: bool = False
    # Host-owned receipts commit with the proxy snapshot; never contain credentials.
    plugin_sync_receipts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    # Guest requests per second per route (proxy IP) across all workers.
    # OnlyFans answers more than ~38/s from one IP with HTTP 429. 0 disables.
    guest_route_rate_limit: float = Field(default=35.0, ge=0)
    # Give each proxy a fixed set of guest sessions (visitors) shared across
    # workers. OnlyFans also limits each guest session to about 3 requests/s,
    # so spreading a proxy's traffic over many sessions avoids HTTP 429.
    guest_session_pool: bool = True
    # Maximum duration in seconds to wait for guest user lookup across proxy routes before timing out.
    guest_lookup_timeout: float = Field(default=3.5, ge=0.5)
    # Per-group overrides, keyed by Proxy.group; a proxy's own value wins.
    proxy_groups: dict[str, ProxyGroupSettings] = Field(default_factory=dict)
    # Maximum duration in seconds for guest route HTTP requests before timing out.
    guest_route_timeout: float = Field(default=3.0, ge=0.5)
    downloads: DownloadSettings = DownloadSettings()

    def to_usapi_network(self) -> USAPINetwork:
        return USAPINetwork(
            max_connections=self.max_connections,
            proxies=[proxy.to_usapi_proxy() for proxy in self.proxies],
            proxy_fallback=self.proxy_fallback,
            guest_route_rate_limit=self.guest_route_rate_limit,
            guest_session_pool=self.guest_session_pool,
            guest_lookup_timeout=self.guest_lookup_timeout,
            proxy_groups={
                name: group.model_copy() for name, group in self.proxy_groups.items()
            },
            guest_route_timeout=self.guest_route_timeout,
        )


class Redis(BaseModel):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None
    enabled: bool = True

    def to_usapi_redis(self) -> USAPIRedisConfig:
        return USAPIRedisConfig(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            enabled=self.enabled,
        )


class Jobs(BaseModel):
    class Scrape(BaseModel):
        subscriptions: bool = True
        messages: bool = True
        paid_contents: bool = True

    class Metadata:
        content: bool = True
        comments: bool = True

    scrape: Scrape = Scrape()
    metadata: Scrape = Scrape()


class Drive(BaseModel):
    """Drive configuration with directories and settings."""

    minimum_space: int = 0  # Minimum free space in bytes (-1 = no limit)
    store: bool = True  # Whether this drive can receive a performer that does not already exist elsewhere
    scan_existing: bool = (
        True  # Whether this drive is scanned for existing performer folders
    )
    overflow: bool = True  # Whether this drive may be used even when free space is below minimum_space
    directories: list[Path] = []  # List of directories on this drive


PERFORMER_UNIQUE_DIRECTORY_TOKENS = frozenset(
    {"{user_username}", "{model_username}", "{user_id}"}
)


class StagingUsage(str, Enum):
    DOWNLOADS = "downloads"
    MERGE = "merge"
    DRM = "drm"


class StagingPolicy(str, Enum):
    DRM_ONLY = "drm_only"


class StagingDirectory(BaseModel):
    path: Path
    usage: list[StagingUsage] = Field(default_factory=list)
    policy: list[StagingPolicy] = Field(default_factory=list)

    @field_validator("path")
    @classmethod
    def ensure_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("staging directory path must be absolute")
        return value

    @field_validator("usage")
    @classmethod
    def normalize_usage(cls, value: list[StagingUsage]) -> list[StagingUsage]:
        normalized = list(dict.fromkeys(value))
        if not normalized:
            raise ValueError("staging directory usage must not be empty")
        return normalized

    @field_validator("policy")
    @classmethod
    def normalize_policy(cls, value: list[StagingPolicy]) -> list[StagingPolicy]:
        return list(dict.fromkeys(value))


class DRM(BaseModel):
    model_config = {"extra": "ignore"}

    device_client_blob_filepath: Path | None = None
    device_private_key_filepath: Path | None = None

    def to_usapi_drm(self) -> USAPIDRM:
        return USAPIDRM(
            device_client_blob_filepath=self.device_client_blob_filepath,
            device_private_key_filepath=self.device_private_key_filepath,
        )


def _resolve_mount_point(path: Path) -> str:
    resolved = os.path.realpath(str(path))
    while not os.path.ismount(resolved):
        parent = os.path.dirname(resolved)
        if parent == resolved:
            break
        resolved = parent
    return resolved


def split_drive_configs_by_mount(drives: list[Drive]) -> list[Drive]:
    """Normalize explicit storage edits/exports, never configuration reads."""
    normalized: list[Drive] = []

    for drive in drives:
        if len(drive.directories) <= 1:
            normalized.append(drive)
            continue

        grouped_directories: dict[str, list[Path]] = {}
        ordered_mounts: list[str] = []
        for directory in drive.directories:
            mount_point = _resolve_mount_point(directory)
            if mount_point not in grouped_directories:
                grouped_directories[mount_point] = []
                ordered_mounts.append(mount_point)
            grouped_directories[mount_point].append(directory)

        if len(ordered_mounts) <= 1:
            normalized.append(drive)
            continue

        for mount_point in ordered_mounts:
            normalized.append(
                Drive(
                    minimum_space=drive.minimum_space,
                    store=drive.store,
                    scan_existing=drive.scan_existing,
                    overflow=drive.overflow,
                    directories=list(grouped_directories[mount_point]),
                )
            )

    return normalized


class GlobalXPathSetup(BaseModel):
    """Stored paths; loading settings must not probe their filesystems."""

    drives: list[Drive] = Field(
        default_factory=lambda: [Drive(directories=[Path("__user_data__").absolute()])]
    )
    directory_format: Path = Path()


class DownloadPathSetup(GlobalXPathSetup):
    alt_directory_formats: list[Path] = Field(default_factory=list)
    site_directory_aliases: dict[str, list[str]] = Field(default_factory=dict)
    staging_directories: list[StagingDirectory] = Field(default_factory=list)
    filename_format: Path = Path()
    text_length: int = 255
    date_format: str = "%Y-%m-%d"
    overwrite_files: bool = True

    @field_validator("site_directory_aliases", mode="before")
    @classmethod
    def normalize_site_directory_aliases(cls, value: Any) -> dict[str, list[str]]:
        if not value:
            return {}
        if not isinstance(value, dict):
            raise ValueError("site directory aliases must be an object")

        normalized: dict[str, list[str]] = {}
        for site_key, raw_aliases in value.items():
            cleaned_key = str(site_key).strip().lower()
            if not cleaned_key:
                raise ValueError("site directory alias site keys must not be empty")
            if "/" in cleaned_key or "\\" in cleaned_key:
                raise ValueError("site directory alias site keys must be plain names")

            if isinstance(raw_aliases, str):
                raw_aliases = [raw_aliases]
            if not isinstance(raw_aliases, (list, tuple)):
                raise ValueError(
                    f"site directory aliases for {cleaned_key} must be a list"
                )

            seen: set[str] = set()
            cleaned_aliases: list[str] = []
            for alias in raw_aliases or []:
                cleaned_alias = str(alias).strip()
                if not cleaned_alias:
                    continue
                if (
                    cleaned_alias in {".", ".."}
                    or "/" in cleaned_alias
                    or "\\" in cleaned_alias
                ):
                    raise ValueError(
                        "site directory aliases must be single directory names"
                    )
                alias_key = cleaned_alias.casefold()
                if alias_key not in seen:
                    seen.add(alias_key)
                    cleaned_aliases.append(cleaned_alias)
            normalized[cleaned_key] = cleaned_aliases
        return normalized

    def get_site_directory_aliases(self, site_name: str) -> list[str]:
        cleaned_key = str(site_name).strip().lower()
        return list(self.site_directory_aliases.get(cleaned_key, []))

    @field_validator("alt_directory_formats", mode="before")
    @classmethod
    def normalize_alt_directory_formats(
        cls, value: list[str | Path] | None
    ) -> list[str | Path]:
        if not value:
            return []

        normalized: list[str] = []
        seen: set[str] = set()
        for raw_format in value:
            candidate = str(raw_format).strip()
            if not candidate:
                raise ValueError("alt directory format must not be empty")
            if not any(
                token in candidate for token in PERFORMER_UNIQUE_DIRECTORY_TOKENS
            ):
                raise ValueError(
                    "alt directory format must include one of "
                    "{user_username}, {model_username}, or {user_id}"
                )
            if candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
        return normalized


class Trash(BaseModel):
    cleanup: bool = True


class SecuritySettings(BaseModel):
    allowed_origins: list[str] = Field(
        default_factory=lambda: list(DEFAULT_LOCAL_ORIGINS)
    )
    allowed_origin_regex: str | None = PRIVATE_NETWORK_ORIGIN_REGEX
    allowed_hosts: list[str] = Field(default_factory=lambda: list(DEFAULT_LOCAL_HOSTS))
    ws_allowed_origins: list[str] = Field(
        default_factory=lambda: list(DEFAULT_LOCAL_ORIGINS)
    )
    allow_docs: bool = False
    admin_token_required: bool = True


class BackendSettings(BaseModel):
    backend_id: str | None = None
    execution_mode: ExecutionMode = ExecutionMode.DISTRIBUTED
    use_redis_metrics: bool = True
    # Let other machines (WebUIs joining the cluster) use this backend: it also
    # serves HTTPS, with a self-signed certificate, on https_port. Read at
    # startup; USR_BACKEND_TLS, CONTROL_TLS_PORT and USR_ADVERTISE_HOST override.
    share_https: bool = False
    https_port: int = Field(default=8443, ge=1, le=65535)
    # The address other machines reach this host at; empty detects its LAN address.
    advertise_host: str | None = Field(default=None, max_length=255)


class JobExecutionSettings(BaseModel):
    """Live controls for bounded durable-job execution windows."""

    window_max_tasks: int | None = Field(default=None, ge=1)
    # Zero/null keeps the adaptive ready<=inflight trigger.
    refill_ready_threshold: int | None = Field(default=None, ge=0)
    # Delivered job events are kept this long, and always until every enabled
    # event-reading plugin has consumed them.
    event_retention_hours: int = Field(default=168, ge=1)
    # Finished jobs keep only failed tasks, tasks that spawned children and
    # their attempts after this many days. Zero keeps full task history.
    task_history_retention_days: int = Field(default=3, ge=0)
    # Committed write receipts are kept this long (at least the 15-minute
    # result wait); live runs keep theirs until they finish.
    receipt_retention_minutes: int = Field(default=60, ge=15)
    # Terminal-job Redis cleanup: cycle interval, minimum job age and jobs
    # per cycle. Read again before every cycle.
    terminal_reconcile_interval_seconds: int = Field(default=300, ge=30)
    terminal_reconcile_min_age_seconds: int = Field(default=300, ge=0)
    terminal_reconcile_batch_size: int = Field(default=2, ge=1)
    # Cached headers of completed jobs are retired after this age, once their
    # durable history is verified. Zero retires them immediately.
    completed_job_retention_seconds: int = Field(default=24 * 60 * 60, ge=0)
    completed_job_retention_batch_size: int = Field(default=1000, ge=1)
    # Batch size when archiving completed tasks from Redis into persistent storage.
    task_archive_batch_size: int = Field(default=2000, ge=1)
    # Target batch size for MPS replacement tasks.
    mps_replacement_batch_size: int = Field(default=1000, ge=1)
    # Planner batch size for scheduling ledger tasks.
    ledger_planner_batch: int = Field(default=512, ge=1)


class ToolSettings(BaseModel):
    active: bool = True


class Renamer(ToolSettings):
    pass


class Reformatter(ToolSettings):
    pass


class Downloader(ToolSettings):
    pass


class Tools(BaseModel):
    renamer: Renamer = Renamer()
    reformatter: Reformatter = Reformatter()
    downloader: Downloader = Downloader()


class WorkerPool(BaseModel):
    """Worker pool configuration with multi-site support.

    Workers can handle jobs from multiple sites simultaneously by maintaining
    separate API instances for each configured site.
    """

    enabled: bool = True
    size: int = 5
    max_claimed_jobs: int = 50
    # Async task slots per worker; null derives them from proxy capacity.
    task_concurrency: int | None = Field(default=None, ge=1)
    # Ledger tasks reserved per claim; null uses the ledger batch size.
    claim_batch_size: int | None = Field(default=None, ge=1)
    sites: list[str] = Field(
        default_factory=lambda: ["onlyfans", "fansly"]
    )  # Sites this worker pool can handle


class GlobalCache(BaseModel):
    pass


class GlobalAPI(BaseModel):
    media_quality: MediaQuality = MediaQuality()
    webhooks: Webhooks = Webhooks()
    jobs: Jobs = Jobs()
    blacklists: list[str] = []


class OnlyFansAPIConfig(GlobalAPI):
    class OnlyFansCache(GlobalCache):
        paid_content: int = 3600 * 1

    dynamic_rules_url: str = (
        "https://raw.githubusercontent.com/DATAHOARDERS/dynamic-rules/main/onlyfans.json"
    )
    cache: OnlyFansCache = OnlyFansCache()


class FanslyAPIConfig(GlobalAPI):
    class FanslyCache(GlobalCache):
        pass

    cache: FanslyCache = FanslyCache()


class LoyalFansAPIConfig( BaseModel):
    class LoyalFansCache(GlobalCache):
        pass

    media_quality: MediaQuality = MediaQuality()
    webhooks: Webhooks = Webhooks()
    cache: LoyalFansCache = LoyalFansCache()


class Sites(BaseModel):
    OnlyFansAPIConfig: ClassVar[type[OnlyFansAPIConfig]] = OnlyFansAPIConfig
    FanslyAPIConfig: ClassVar[type[FanslyAPIConfig]] = FanslyAPIConfig
    LoyalFansAPIConfig: ClassVar[type[LoyalFansAPIConfig]] = LoyalFansAPIConfig

    onlyfans: OnlyFansAPIConfig = OnlyFansAPIConfig()
    fansly: FanslyAPIConfig = FanslyAPIConfig()
    loyalfans: LoyalFansAPIConfig = LoyalFansAPIConfig()

    def get_settings(self, site_name: str):
        if site_name == "OnlyFans":
            return self.onlyfans
        elif site_name == "Fansly":
            return self.fansly
        else:
            return self.loyalfans

    def to_usapi_sites(self) -> USASites:
        return USASites(
            onlyfans=USAOnlyFansAPIConfig(
                media_quality=USAPIMediaQuality(
                    **self.onlyfans.media_quality.model_dump()
                ),
                webhooks=USAPIWebhooks(
                    auth=USAPIWebhook(**self.onlyfans.webhooks.auth.model_dump()),
                    download=USAPIWebhook(
                        **self.onlyfans.webhooks.download.model_dump()
                    ),
                ),
                dynamic_rules_url=self.onlyfans.dynamic_rules_url,
                cache=USAOnlyFansAPIConfig.OnlyFansCache(
                    **self.onlyfans.cache.model_dump()
                ),
            ),
            fansly=USAFanslyAPIConfig(
                media_quality=USAPIMediaQuality(
                    **self.fansly.media_quality.model_dump()
                ),
                webhooks=USAPIWebhooks(
                    auth=USAPIWebhook(**self.fansly.webhooks.auth.model_dump()),
                    download=USAPIWebhook(
                        **self.fansly.webhooks.download.model_dump()
                    ),
                ),
                cache=USAFanslyAPIConfig.FanslyCache(**self.fansly.cache.model_dump()),
            ),
            loyalfans=USALoyalFansAPIConfig(
                media_quality=USAPIMediaQuality(
                    **self.loyalfans.media_quality.model_dump()
                ),
                webhooks=USAPIWebhooks(
                    auth=USAPIWebhook(**self.loyalfans.webhooks.auth.model_dump()),
                    download=USAPIWebhook(
                        **self.loyalfans.webhooks.download.model_dump()
                    ),
                ),
                cache=USALoyalFansAPIConfig.LoyalFansCache(
                    **self.loyalfans.cache.model_dump()
                ),
            ),
        )


site_config_types = OnlyFansAPIConfig | FanslyAPIConfig


def _environment_int(name: str, current: int) -> int:
    raw = runtime_env_value(name)
    if raw is None:
        return current
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _environment_bool(name: str, current: bool) -> bool:
    raw = runtime_env_value(name)
    if raw is None:
        return current
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _redis_environment_config(current: Redis) -> Redis:
    """Apply process-level Redis overrides without persisting container secrets."""
    values = current.model_dump()
    redis_url = (runtime_env_value("REDIS_URL") or "").strip()
    if redis_url:
        parsed = urlparse(redis_url)
        if parsed.scheme != "redis" or not parsed.hostname:
            raise ValueError("REDIS_URL must be a redis:// URL with a hostname")
        if parsed.username not in {None, "", "default"}:
            raise ValueError("REDIS_URL ACL usernames are not supported")
        values["host"] = parsed.hostname
        values["port"] = parsed.port or 6379
        values["password"] = unquote(parsed.password) if parsed.password else None
        if parsed.path and parsed.path != "/":
            try:
                values["db"] = int(parsed.path.lstrip("/"))
            except ValueError as exc:
                raise ValueError("REDIS_URL database must be an integer") from exc

    host_value = runtime_env_value("REDIS_HOST")
    if host_value is not None:
        host = host_value.strip()
        if not host:
            raise ValueError("REDIS_HOST must not be empty")
        values["host"] = host
    values["port"] = _environment_int("REDIS_PORT", int(values["port"]))
    values["db"] = _environment_int("REDIS_DB", int(values["db"]))
    values["enabled"] = _environment_bool(
        "REDIS_ENABLED", bool(values.get("enabled", True))
    )
    password = runtime_env_value("REDIS_PASSWORD")
    if password is not None:
        values["password"] = password or None
    return Redis(**values)


class UltimaScraperCollectionConfig(BaseModel):
    class Settings(BaseModel):
        hostname: str = socket.gethostname()
        network: Network = Field(default_factory=Network)
        tools: Tools = Field(default_factory=Tools)
        trash: Trash = Field(default_factory=Trash)
        worker_pool: WorkerPool = Field(default_factory=WorkerPool)
        webhook_routing: WebhookRouting = Field(default_factory=WebhookRouting)
        backend: BackendSettings = Field(default_factory=BackendSettings)
        job_execution: JobExecutionSettings = Field(default_factory=JobExecutionSettings)
        redis: Redis = Field(default_factory=Redis)
        security: SecuritySettings = Field(default_factory=SecuritySettings)
        drm: DRM = Field(default_factory=DRM)
        infinite_loop: bool = False
        exit_on_completion: bool = True
        previews_directory: Path | None = None
        subtitles_directory: Path | None = None
        metadata_setup: GlobalXPathSetup = Field(
            default_factory=lambda: GlobalXPathSetup(
                directory_format=Path(
                    "{site_name}/{first_letter}/{model_username}/Metadata"
                )
            )
        )
        download_setup: DownloadPathSetup = Field(
            default_factory=lambda: DownloadPathSetup(
                directory_format=Path(
                    "{site_name}/{first_letter}/{model_username}/{api_type}/{value}/{media_type}"
                ),
                filename_format=Path("{filename}.{ext}"),
            )
        )

        def get_backend_id(self) -> str:
            configured = (self.backend.backend_id or "").strip()
            return configured or self.hostname

    settings: Settings = Field(default_factory=Settings)
    site_apis: Sites = Field(default_factory=Sites)
    configuration_schema_version: Literal[2] = 2
    is_configured: bool = False
    _settings_backend: str | None = PrivateAttr(default=None)
    _settings_id: int | None = PrivateAttr(default=None)
    _settings_version: int | None = PrivateAttr(default=None)
    _settings_updated_at: str | None = PrivateAttr(default=None)

    def to_usapi_config(self) -> UltimaScraperAPIConfig:
        return UltimaScraperAPIConfig(
            settings=USAPISettings(
                network=self.settings.network.to_usapi_network(),
                drm=self.settings.drm.to_usapi_drm(),
                redis=self.settings.redis.to_usapi_redis(),
            ),
            site_apis=self.site_apis.to_usapi_sites(),
        )

    @classmethod
    def load_or_create_default_config(cls) -> Self:
        """Load one canonical DB snapshot; unconfigured startup is read-only."""
        from .config_settings_sync import load_config_snapshot

        config = load_config_snapshot(cls)
        config.settings.redis = _redis_environment_config(Redis())
        config.settings.hostname = (
            runtime_env_value("BACKEND_ID") or socket.gethostname()
        )
        config.settings.backend.backend_id = config.settings.hostname
        from . import security

        config.settings.security = SecuritySettings(
            allowed_origins=security.get_allowed_origins(),
            allowed_origin_regex=security.get_allowed_origin_regex(),
            allowed_hosts=security.get_allowed_hosts(),
            ws_allowed_origins=security.get_ws_allowed_origins(),
            allow_docs=security.allow_docs(),
            admin_token_required=security.admin_token_required(),
        )
        return config

    def get_site_config(self, site_name: str) -> site_config_types:
        return getattr(self.site_apis, site_name.lower())

    @property
    def backend_settings(self) -> Settings:
        return self.settings

    def _config_write_backend(self) -> str:
        return self._settings_backend or self.settings.get_backend_id()

    def config_write_backend(self) -> str:
        return self._config_write_backend()

    def persistence_payload(self) -> dict[str, Any]:
        """Application-owned values only; deployment overlays never round-trip."""
        self.settings.download_setup.drives = split_drive_configs_by_mount(
            self.settings.download_setup.drives
        )
        self.settings.metadata_setup.drives = split_drive_configs_by_mount(
            self.settings.metadata_setup.drives
        )
        payload = self.model_dump(mode="json", exclude={"is_configured"})
        settings = payload["settings"]
        for field in ("redis", "security", "hostname"):
            settings.pop(field, None)
        settings["backend"].pop("backend_id", None)
        return payload


def _walk_secret_fields(data: Any, mutator: Any) -> None:
    if not isinstance(data, dict):
        return

    data_dict = cast(dict[str, Any], data)
    settings_obj = data_dict.get("settings")
    if not isinstance(settings_obj, dict):
        return
    settings = cast(dict[str, Any], settings_obj)

    network_obj = settings.get("network")
    if isinstance(network_obj, dict):
        network = cast(dict[str, Any], network_obj)
        proxies = cast(list[Any] | None, network.get("proxies"))
        if isinstance(proxies, list):
            for proxy_obj in proxies:
                if isinstance(proxy_obj, dict) and "password" in proxy_obj:
                    proxy = cast(dict[str, Any], proxy_obj)
                    proxy["password"] = mutator(proxy.get("password"))


def _encrypt_config_secrets(data: dict[str, Any]) -> None:
    _walk_secret_fields(data, encrypt_secret_value)


def _decrypt_config_secrets(data: dict[str, Any]) -> None:
    _walk_secret_fields(data, decrypt_secret_value)


def encrypt_config_secrets(data: dict[str, Any]) -> None:
    _encrypt_config_secrets(data)


def decrypt_config_secrets(data: dict[str, Any]) -> None:
    _decrypt_config_secrets(data)

