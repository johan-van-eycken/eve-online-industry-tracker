from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _bool(name: str, default: bool) -> bool:
    raw = _env(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _int(name: str, default: int) -> int:
    raw = _env(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def flask_host() -> str:
    return os.getenv("FLASK_HOST", "localhost")


def flask_port() -> int:
    return int(os.getenv("FLASK_PORT", "5000"))


def flask_debug() -> bool:
    return _bool("FLASK_DEBUG", default=False)


def refresh_metadata_on_startup() -> bool:
    # Keeping existing behavior (True) unless explicitly disabled.
    return _bool("FLASK_REFRESH_METADATA", default=True)


def health_poll_timeout_seconds() -> int:
    return _int("FLASK_HEALTH_POLL_TIMEOUT", default=120)


def health_request_timeout_seconds() -> int:
    return _int("FLASK_HEALTH_REQUEST_TIMEOUT", default=2)


def api_request_timeout_seconds() -> int:
    return _int("FLASK_API_REQUEST_TIMEOUT", default=10)


def api_base() -> str:
    return f"http://{flask_host()}:{flask_port()}"
