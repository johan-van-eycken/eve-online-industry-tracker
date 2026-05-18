from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any, Mapping

from config.paths import streamlit_preferences_path


_PREFERENCES_LOCK = Lock()


def _preferences_file_path() -> Path:
    return Path(streamlit_preferences_path())


def _read_all_preferences() -> dict[str, Any]:
    path = _preferences_file_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_page_preferences(namespace: str) -> dict[str, Any]:
    with _PREFERENCES_LOCK:
        payload = _read_all_preferences()
    page_payload = payload.get(str(namespace)) or {}
    return page_payload if isinstance(page_payload, dict) else {}


def save_page_preferences(namespace: str, preferences: Mapping[str, Any]) -> None:
    path = _preferences_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    with _PREFERENCES_LOCK:
        payload = _read_all_preferences()
        payload[str(namespace)] = dict(preferences)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(path)