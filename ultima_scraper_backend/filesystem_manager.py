from __future__ import annotations

import os
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import ultima_scraper_api

from ultima_scraper_backend import main_helper as usc_helper
from ultima_scraper_backend.config import (
    DownloadPathSetup,
    GlobalXPathSetup,
)
from ultima_scraper_backend.ultima_scraper_renamer.reformat import (
    FormatAttributes,
)

if TYPE_CHECKING:
    api_types = ultima_scraper_api.api_types
    user_types = ultima_scraper_api.user_types


class FilesystemConfig(Protocol):
    metadata_setup: GlobalXPathSetup
    download_setup: DownloadPathSetup


class FilesystemManager:
    def __init__(self) -> None:
        self.user_data_directory = Path("__user_data__")
        self.trash_directory = self.user_data_directory.joinpath("trash")
        self.profiles_directory = self.user_data_directory.joinpath("profiles")
        self.devices_directory = self.user_data_directory.joinpath("drm_device")
        self.settings_directory = Path("__user_data__")
        self.ignore_files = ["desktop.ini", ".DS_Store", ".DS_store", "@eaDir"]
        self.directory_manager: DirectoryManager | None = None

    def __iter__(self):
        for each in self.__dict__.values():
            yield each

    def check(self):
        for directory in self:
            if isinstance(directory, Path):
                directory.mkdir(exist_ok=True)

    def move(self, src: Path, trg: Path):
        shutil.move(src, trg)

    def remove_mandatory_files(self, files: Iterable[Path]) -> list[Path]:
        return [x for x in files if x.name not in self.ignore_files]

    async def performer_directory(self, site_name: str, usernames: list[str]):
        found_performer_directory = None
        for username in usernames:
            assert self.directory_manager
            # Flatten drives and directories into a list of paths
            download_paths = [
                directory_path
                for drive in self.directory_manager.site_config.download_setup.drives
                for directory_path in drive.directories
            ]
            for download_path in download_paths:
                site_directory = download_path.joinpath(site_name)
                performer_directory = site_directory.joinpath(
                    username[0].capitalize()
                ).joinpath(username)
                if performer_directory.exists():
                    found_performer_directory = performer_directory
                    break
        return found_performer_directory


class DirectoryManager:
    def __init__(
        self,
        site_config: FilesystemConfig,
        root_metadata_directory: Path,
        root_download_directory: Path,
    ) -> None:
        self.root_directory = Path()
        self.root_metadata_directory = Path(root_metadata_directory)
        self.root_download_directory = Path(root_download_directory)
        self.user = self.UserDirectories()
        self.site_config = site_config
        formats = FormatTypes(site_config)
        string, status = formats.check_rules()
        if not status:
            print(string)
            exit(0)
        self.formats = formats

    def delete_empty_directories(
        self, directory: Path, filesystem_manager: FilesystemManager
    ):
        for root, dirnames, _files in os.walk(directory, topdown=False):
            for dirname in dirnames:
                full_path = os.path.realpath(os.path.join(root, dirname))
                contents = os.listdir(full_path)
                if not contents:
                    shutil.rmtree(full_path, ignore_errors=True)
                else:
                    content_paths = [Path(full_path, content) for content in contents]
                    contents = filesystem_manager.remove_mandatory_files(content_paths)
                    if not contents:
                        shutil.rmtree(full_path, ignore_errors=True)

        if os.path.exists(directory) and not os.listdir(directory):
            os.rmdir(directory)

    class UserDirectories:
        def __init__(self) -> None:
            self.metadata_directory: Path | None = None
            self.download_directory: Path | None = None

    async def walk(self, directory: Path):
        return await usc_helper.walk(directory)


class FormatTypes:
    def __init__(self, site_settings: FilesystemConfig) -> None:
        self.metadata_directory_format = site_settings.metadata_setup.directory_format
        self.file_directory_format = site_settings.download_setup.directory_format
        self.filename_format = site_settings.download_setup.filename_format

    def check_rules(self):
        """Checks for invalid filepath

        Returns:
            tuple(str,bool): Returns a string which explains invalid filepath format
        """
        bool_status = True
        wl = []
        invalid_list = []
        string = ""
        for key, _value in self:
            if key == "file_directory_format":
                bl = FormatAttributes()
                wl = [v for _k, v in bl.__dict__.items()]
                bl = bl.whitelist(wl)
                invalid_list = []
                for b in bl:
                    if b in self.file_directory_format.as_posix():
                        invalid_list.append(b)
            if key == "filename_format":
                bl = FormatAttributes()
                wl = [v for _k, v in bl.__dict__.items()]
                bl = bl.whitelist(wl)
                invalid_list = []
                for b in bl:
                    if b in self.filename_format.as_posix():
                        invalid_list.append(b)
            if key == "metadata_directory_format":
                wl = [
                    "{site_name}",
                    "{first_letter}",
                    "{model_id}",
                    "{profile_username}",
                    "{model_username}",
                ]
                bl = FormatAttributes().whitelist(wl)
                invalid_list: list[str] = []
                for b in bl:
                    if b in self.metadata_directory_format.as_posix():
                        invalid_list.append(b)
            if invalid_list:
                string += f"You cannot use {','.join(invalid_list)} in {key}. Use any from this list {','.join(wl)}"
                bool_status = False

        return string, bool_status

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
