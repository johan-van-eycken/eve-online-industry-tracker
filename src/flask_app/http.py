from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from flask import jsonify


def ok(*, data: Any = None, message: Optional[str] = None, status_code: int = 200, **extra: Any):
    payload: Dict[str, Any] = {"status": "success"}
    if message is not None:
        payload["message"] = message
    if data is not None:
        payload["data"] = data
    payload.update(extra)
    return jsonify(payload), status_code


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
