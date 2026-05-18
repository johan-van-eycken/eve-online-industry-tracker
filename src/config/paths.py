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


def streamlit_preferences_path() -> str:
    """Path to the local Streamlit UI preferences JSON.

    Controlled by STREAMLIT_PREFERENCES_PATH.
    """

    return _env("STREAMLIT_PREFERENCES_PATH", "database/streamlit_preferences.json")


def admin_settings_path() -> str:
    """Path to the admin settings JSON.

    Controlled by ADMIN_SETTINGS_PATH.
    """

    return _env("ADMIN_SETTINGS_PATH", "database/admin_settings.json")
