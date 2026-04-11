"""Dynamically load a TransportPlugin from config."""

from __future__ import annotations

import importlib
import logging

from .base import TransportPlugin

logger = logging.getLogger(__name__)


def load_plugin(config: dict) -> TransportPlugin | None:
    """
    Return an instantiated plugin, or None when no plugin is configured.

    config.yaml layout::

        plugin:
          name: aes_ctr   # module name inside framework/plugins/ (no .py)
          # plugin-specific keys can go here too

    The loader finds the first class in that module that subclasses
    TransportPlugin and instantiates it with the ``plugin`` sub-dict.
    """
    plugin_cfg = config.get("plugin") or {}
    name = plugin_cfg.get("name")
    if not name:
        return None

    module_path = f"framework.plugins.{name}"
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ImportError(
            f"Plugin '{name}' not found — tried '{module_path}'. "
            "Make sure the module exists in framework/plugins/."
        ) from exc

    for attr_name in dir(module):
        cls = getattr(module, attr_name)
        if (
            isinstance(cls, type)
            and issubclass(cls, TransportPlugin)
            and cls is not TransportPlugin
        ):
            logger.info("Loaded plugin: %s (%s)", cls.__name__, module_path)
            return cls(plugin_cfg)

    raise RuntimeError(
        f"No TransportPlugin subclass found in '{module_path}'. "
        "Define a class that inherits from TransportPlugin."
    )
