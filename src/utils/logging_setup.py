from __future__ import annotations

import logging
import os
from typing import Optional


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def configure_logging(
    *,
    default_level: str = "INFO",
    fmt: str = "%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
    force: bool = True,
) -> None:
    """Configure stdlib logging in a consistent, idempotent way.

    Environment variables:
    - LOG_LEVEL: overrides default_level (e.g. DEBUG, INFO)
    - LOG_FORCE: when set to 0/false, disables reconfiguration
    """

    level_name = _env("LOG_LEVEL", default_level).upper()
    level = getattr(logging, level_name, logging.INFO)

    env_force = _env("LOG_FORCE", "1").lower() not in {"0", "false", "no"}

    # basicConfig(force=...) is available on Python 3.8+
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, force=(force and env_force))

    # Make Werkzeug logs follow the same level (helpful when running Flask directly).
    logging.getLogger("werkzeug").setLevel(level)
