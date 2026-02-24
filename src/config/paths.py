from __future__ import annotations

import os
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve_repo_relative(path_str: str) -> str:
    """Resolve a path string relative to the repo root.

    Keeps env var overrides intact:
    - absolute paths are returned as-is
    - relative paths are interpreted as repo-root relative
    """

    p = Path(path_str)
    if p.is_absolute():
        return str(p)
    return str((_REPO_ROOT / p).resolve())


def app_config_path() -> str:
    """Path to the main app config JSON.

    Controlled by APP_CONFIG_PATH.
    """

    return _resolve_repo_relative(_env("APP_CONFIG_PATH", "config/config.json"))


def app_secret_path() -> str:
    """Path to the app secret JSON.

    Controlled by APP_SECRET_PATH.
    """

    return _resolve_repo_relative(_env("APP_SECRET_PATH", "config/secret.json"))
