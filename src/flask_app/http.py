from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from flask import jsonify, request

from eve_online_industry_tracker.application.errors import ServiceError


def ok(*, data: Any = None, message: Optional[str] = None, status_code: int = 200, **extra: Any):
    payload: Dict[str, Any] = {"status": "success"}
    if message is not None:
        payload["message"] = message
    if data is not None:
        payload["data"] = data
    payload.update(extra)
    return jsonify(payload), status_code


def optional_id_param(key: str) -> Optional[int]:
    """An optional id query value (character_id, corporation_id): None when
    absent or blank ("all"). Anything but a whole number is a 400 naming the
    field and the value, never a silent "all"."""
    raw = (request.args.get(key) or "").strip()
    if not raw:
        return None
    if not (raw.isascii() and raw.isdigit()):
        raise ServiceError(f"Invalid {key}: must be a whole number, got {raw!r}.", status_code=400)
    return int(raw)


def error(*, message: str, status_code: int = 500, code: Optional[str] = None, **extra: Any):
    payload: Dict[str, Any] = {
        "status": "error",
        "message": message,
        "error": {"message": message},
    }
    if code is not None:
        payload["error"]["code"] = code
    payload.update(extra)
    return jsonify(payload), status_code
