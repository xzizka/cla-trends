"""Source-plugin registry.

`REGISTRY` maps the `type:` string used in sources.yml to the plugin class.
A new technology becomes available purely by adding a module here — see
`base.py` for the contract.
"""
from __future__ import annotations

from typing import Dict, Type

from .base import Row, Source, SourceError, Stream  # noqa: F401  (re-exported)

REGISTRY = {}  # type: Dict[str, Type[Source]]


def register(type_name):
    """Class decorator: make a Source subclass selectable as `type: <name>`."""

    def deco(cls):
        if type_name in REGISTRY:
            raise RuntimeError("duplicate source type %r" % type_name)
        cls.type_name = type_name
        REGISTRY[type_name] = cls
        return cls

    return deco


def build(cfg: dict, http) -> Source:
    """Instantiate the plugin for one source config block."""
    type_name = cfg.get("type")
    if type_name not in REGISTRY:
        raise ValueError(
            "unknown source type %r — known types: %s"
            % (type_name, ", ".join(sorted(REGISTRY)))
        )
    cls = REGISTRY[type_name]
    cls.validate(cfg)
    return cls(cfg, http)


# Importing these modules is what runs their @register decorators. Add new
# technologies to this list.
from . import influxdb1, influxdb2, prometheus  # noqa: E402,F401
