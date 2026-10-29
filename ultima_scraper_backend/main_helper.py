import os
from pathlib import Path




async def walk(directory: Path) -> list[Path]:
    all_files: list[Path] = []
    for root, _subdirs, files in os.walk(directory):
        x = [Path(root, x) for x in files]
        all_files.extend(x)
    return all_files
