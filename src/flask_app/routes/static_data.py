from __future__ import annotations

from flask import Blueprint, request, current_app

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.static_data.service import StaticDataService
from flask_app.bootstrap import require_ready, require_sde_ready
from flask_app.deps import get_state
from flask_app.http import ok
from flask_app.session_provider import FlaskSessionProvider


static_data_bp = Blueprint("static_data", __name__)


@static_data_bp.get("/static/<path:filename>")
def get_static_file(filename: str):
    return current_app.send_static_file(filename)


@static_data_bp.get("/facilities")
def facilities():
    require_ready(get_state())
    svc = StaticDataService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.list_facilities())


@static_data_bp.post("/optimize")
def optimize():
    """Ore optimization endpoint (ported from original flask_app.py)."""
    require_ready(get_state())
    payload = request.get_json(silent=True) or {}
    required = ("demands", "character_id", "facility_id")
    missing = [k for k in required if k not in payload]
    if missing:
        raise ServiceError(f"Missing field(s): {', '.join(missing)}", status_code=400)

    svc = StaticDataService(state=get_state(), sessions=FlaskSessionProvider())
    result = svc.optimize_ore_plan(payload)
    return ok(data=result)


@static_data_bp.get("/materials")
def materials():
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = StaticDataService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.list_materials_cached())


@static_data_bp.get("/ores")
def ores():
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = StaticDataService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.list_ores())
