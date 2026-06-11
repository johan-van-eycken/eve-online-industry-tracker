from __future__ import annotations

from flask import Blueprint, request

from flask_app.deps import get_state
from flask_app.http import ok, error
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

settings_bp = Blueprint("settings", __name__)


@settings_bp.get("/admin_settings")
def get_admin_settings():
    """Return all current admin settings and the schema for the UI."""
    state = get_state()
    mgr = getattr(state, "admin_settings", None)
    if mgr is None:
        return error(message="Admin settings not initialized", status_code=503)
    return ok(data={
        "schema": ADMIN_SETTINGS_SCHEMA,
        "values": mgr.get_all(),
    })


@settings_bp.put("/admin_settings")
def update_admin_settings():
    """Bulk-update admin settings.  Expects JSON: { category: { key: value } }."""
    state = get_state()
    mgr = getattr(state, "admin_settings", None)
    if mgr is None:
        return error(message="Admin settings not initialized", status_code=503)

    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return error(message="Request body must be a JSON object", status_code=400)

    try:
        mgr.set_bulk(body)
    except (KeyError, ValueError, TypeError) as exc:
        return error(message=str(exc), status_code=400)

    return ok(data=mgr.get_all(), message="Settings updated")


@settings_bp.post("/admin_settings/reset")
def reset_admin_settings():
    """Reset all admin settings to schema defaults."""
    state = get_state()
    mgr = getattr(state, "admin_settings", None)
    if mgr is None:
        return error(message="Admin settings not initialized", status_code=503)
    mgr.reset_to_defaults()
    return ok(data=mgr.get_all(), message="Settings reset to defaults")
