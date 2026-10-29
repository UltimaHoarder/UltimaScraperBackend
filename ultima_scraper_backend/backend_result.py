"""The canonical result type for backend actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(slots=True)
class BackendResult:
    """Canonical response for backend actions."""

    success: bool
    message: str
    payload: Optional[dict[str, Any]] = None
