"""Detached restoration worker. Requests and results remain on disk."""

import os
import sys
from pathlib import Path

from .server_restore_workspace import Store, run


def main():
    root, identifier, global_fd, review_fd = sys.argv[1:]
    # Inherited open-file descriptions retain the parent's flock continuously.
    with os.fdopen(int(global_fd), "a"), os.fdopen(int(review_fd), "a"):
        run(Store(Path(root)), identifier)


if __name__ == "__main__":
    main()
