"""User and storage management for ultima_scraper_backend."""

from pathlib import Path

from ultima_scraper_api.helpers.identifier_helper import resolve_site_name

from ultima_scraper_backend.job_system.path_formatter import (
    FormatAttributes,
    PathFormatter,
)

from ultima_scraper_backend.config import UltimaScraperCollectionConfig
from ultima_scraper_backend.storage_manager import (
    StorageManager,
)

__all__ = ["UserManager", "Sites", "UserItem", "UserStorage"]


class UserManager:
    def __init__(self, config: UltimaScraperCollectionConfig):
        self.sites = Sites()
        self.storage_manager = StorageManager(config)
        self.config = config

    def add_user(
        self, user_id: int, usernames: list[str], site_name: str
    ) -> "UserItem":
        site_dict = getattr(self.sites, site_name.lower(), None)
        if site_dict is None:
            raise ValueError(f"Unsupported site name: {site_name}")
        if user_id in site_dict:
            return site_dict[user_id]
        user_item = UserItem(user_id, usernames, site_name)
        user_item.storage = UserStorage(user_item, self)
        site_dict[user_id] = user_item
        return user_item

    def find_user(self, user_id: int, site_name: str) -> "UserItem | None":
        site_dict = getattr(self.sites, site_name.lower(), None)
        if site_dict is None:
            return None
        return site_dict.get(user_id, None)


class Sites:
    def __init__(self):
        self.onlyfans: dict[int, UserItem] = {}
        self.fansly: dict[int, UserItem] = {}
        self.loyalfans: dict[int, UserItem] = {}


class UserItem:
    """Represents a user across a specific site."""

    def __init__(self, user_id: int, usernames: list[str], site_name: str) -> None:
        self.user_id = user_id
        self.usernames = usernames
        self.site_name = site_name
        self.storage: UserStorage | None = None  # Set by UserManager.add_user()


class UserStorage:
    """Manages file storage and deduplication for a user."""

    def __init__(self, user: UserItem, user_manager: UserManager) -> None:
        self.user = user
        self.user_manager = user_manager
        self.directory_paths = self._resolve_directories()
        self.directory_path = self.directory_paths[0] if self.directory_paths else None
        self.files: list[Path] | None = None

        self.duplicate_files: list[list[Path]] | None = None

    def build_directory_paths(self) -> list[Path]:

        directories = self.user_manager.storage_manager.download_directories
        user_directories: list[Path] = []

        path_formatter = PathFormatter(
            directory_format=self.user_manager.storage_manager.directory_format,
            filename_format=self.user_manager.storage_manager.filename_format,
        )

        user_directory_format = path_formatter.get_user_directory_format()
        if not user_directory_format:
            return user_directories

        format_attributes = FormatAttributes()
        format_attributes.site_name = resolve_site_name(self.user.site_name)
        format_attributes.user_id = str(self.user.user_id)

        for directory in directories:
            for username in self.user.usernames:
                format_attributes.user_username = username

                formatted_path = path_formatter.format_path(
                    format_attributes,
                    user_directory_format,
                    self.user_manager.config.settings.download_setup.date_format,
                    self.user_manager.config.settings.download_setup.text_length,
                )

                user_directory = directory / formatted_path
                user_directories.append(user_directory)
        return user_directories

    def _resolve_directories(self) -> list[Path]:
        """Resolve user-specific directories from configured download paths."""
        user_directories: list[Path] = []
        directory_paths = self.build_directory_paths()
        for user_directory in directory_paths:
            if user_directory.exists():
                user_directories.append(user_directory)

        return user_directories
