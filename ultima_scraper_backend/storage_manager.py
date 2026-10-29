from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Protocol

from .config import UltimaScraperCollectionConfig


class _Digest(Protocol):
    def update(self, data: bytes, /) -> object: ...
    def hexdigest(self) -> str: ...


class StorageManager:
    def __init__(self, config: UltimaScraperCollectionConfig) -> None:
        self.config = config

    @property
    def download_directories(self) -> list[Path]:
        """Resolve download directories from the current config on every access."""
        directories: list[Path] = []
        for drive in self.config.settings.download_setup.drives:
            if not drive.store:
                continue
            directories.extend(drive.directories)
        return directories

    @property
    def directory_format(self) -> Path:
        return self.config.settings.download_setup.directory_format

    @property
    def filename_format(self) -> Path:
        return self.config.settings.download_setup.filename_format


def _hash_stream(path: Path, *, chunk_size: int = 1024 * 1024) -> _Digest:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest


def full_hash(path: Path) -> str:
    return _hash_stream(path).hexdigest()


def partial_hash(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read(chunk_size))
        try:
            handle.seek(-chunk_size, 2)
        except OSError:
            handle.seek(0)
        digest.update(handle.read(chunk_size))
    return digest.hexdigest()


def get_file_age_key(path: Path) -> tuple[float, float]:
    stat_result = path.stat()
    return (stat_result.st_mtime, stat_result.st_ctime)


__all__ = [
    "StorageManager",
    "full_hash",
    "partial_hash",
    "get_file_age_key",
]
