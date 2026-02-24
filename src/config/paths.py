from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def app_config_path() -> str:
    """Path to the main app config JSON.

    Controlled by APP_CONFIG_PATH.
    """

    return _env("APP_CONFIG_PATH", "config/config.json")


def app_secret_path() -> str:
    """Path to the app secret JSON.

    Controlled by APP_SECRET_PATH.
    """

    return _env("APP_SECRET_PATH", "config/secret.json")
