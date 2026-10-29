"""Global orders database models and helpers."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

from sqlalchemy.orm import Session

from ultima_scraper_db_archive.global_db import get_global_session
from ultima_scraper_db_archive.databases.ultima_archive.schemas.templates.postgres_site import (
    OrderedPerformerModel,
    OrderedPpvModel,
    OrderModel,
    SiteModel,
)

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def ensure_orders_schema() -> None:
    """Ensure that the orders tables exist. Now managed globally via Alembic."""
    pass


@contextmanager
def create_orders_session() -> Generator[Session, None, None]:
    """Context manager for an orders database session."""
    with get_global_session() as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise


def get_or_create_site(session: Session, site_name: str, url: str | None) -> SiteModel:
    """Fetch or create a site record."""
    existing = session.query(SiteModel).filter(SiteModel.name == site_name).first()
    if existing:
        return existing
    site = SiteModel(name=site_name, url=url)
    session.add(site)
    session.flush()
    return site


__all__ = [
    "OrderedPerformerModel",
    "OrderedPpvModel",
    "OrderModel",
    "SiteModel",
    "create_orders_session",
    "ensure_orders_schema",
    "get_or_create_site",
]
