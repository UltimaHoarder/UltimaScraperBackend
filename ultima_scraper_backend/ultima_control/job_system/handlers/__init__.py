"""Runtime task handlers for the job system."""

# Import handler modules to ensure registration side-effects occur.
from . import download  # noqa: F401
from . import scrape  # noqa: F401

__all__ = ["download", "scrape"]
