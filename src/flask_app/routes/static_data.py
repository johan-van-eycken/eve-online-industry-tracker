from __future__ import annotations

from flask import Blueprint, request, current_app

from flask_app.bootstrap import require_ready
from flask_app.state import state

from flask_app.http import ok, error

from flask_app.data.facility_repo import get_all_facilities
from flask_app.db import get_db_sde_session
from flask_app.services.sde_context import ensure_sde_ready, get_language
from flask_app.services.sde_static_service import build_all_materials, build_all_ores
from flask_app.services.optimizer_service import run_optimize


static_data_bp = Blueprint("static_data", __name__)


@static_data_bp.get("/static/<path:filename>")
def get_static_file(filename: str):
    return current_app.send_static_file(filename)


@static_data_bp.get("/facilities")
def facilities():
    try:
        require_ready()
        facilities_data = get_all_facilities()
        return ok(data=facilities_data)
    except Exception as e:
        return error(message="Error in GET Method `/facilities`: " + str(e))


@static_data_bp.post("/optimize")
def optimize():
    """Ore optimization endpoint (ported from original flask_app.py)."""
    try:
        require_ready()
        payload = request.get_json(force=True) or {}
        demands = payload["demands"]
        character_id = payload["character_id"]
        implant_pct = payload.get("implant_pct", 0)
        facility_id = payload["facility_id"]
        opt_only_compressed = payload.get("only_compressed", False)

        character = state.char_manager.get_character_by_id(character_id)
        if not character:
            return error(message=f"Error in POST Method `/optimize`: Character ID {character_id} not found", status_code=400)

        result = run_optimize(payload, character=character, esi_service=state.esi_service)
        return ok(data=result)
    except KeyError as ke:
        return error(message=f"Error in POST Method `/optimize`: Missing field {ke}", status_code=400)
    except Exception as e:
        return error(message="Error in POST Method `/optimize`: " + str(e))


@static_data_bp.get("/materials")
def materials():
    try:
        require_ready()
        if state.materials_cache is None:
            ensure_sde_ready()
            session = get_db_sde_session()
            language = get_language()
            state.materials_cache = build_all_materials(session, language)
        return ok(data=state.materials_cache)
    except Exception as e:
        return error(message="Error in GET Method `/materials`: " + str(e), status_code=500, data=[])


@static_data_bp.get("/ores")
def ores():
    try:
        require_ready()
        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()
        ores_data = build_all_ores(session, language)
        return ok(data=ores_data)
    except Exception as e:
        return error(message="Error in GET Method `/ores`: " + str(e), status_code=500, data=[])
