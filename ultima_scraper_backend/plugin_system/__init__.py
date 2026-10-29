"""USR Plugin System.

A manifest-driven plugin platform for integrating external systems with USR.
Versioned manifests declare scoped integrations, ledger task handlers and UI.

Key concepts:
- Plugins live in a ``plugins/`` directory, one subdirectory per plugin.
- Each plugin declares capabilities, job types, event subscriptions, callbacks,
  and outbound delivery targets in a YAML manifest.
- The platform enforces permissions, validates schemas, manages secrets,
  delivers events, and provides a state store for each plugin.
"""

from .manifest import PluginManifest
from .permissions import PluginCapability
from .registry import PluginRegistry

__all__ = [
    "PluginCapability",
    "PluginManifest",
    "PluginRegistry",
]
