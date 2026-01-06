from __future__ import annotations

from flask import Blueprint, request

from flask_app.bootstrap import require_ready
from flask_app.state import state
from flask_app.http import ok, error


locations_bp = Blueprint("locations", __name__)


@locations_bp.post("/locations")
def locations():
    try:
        require_ready()
        data = request.get_json()
        location_ids = data.get("location_ids", [])
        result = {}
        for location_id in location_ids:
            info = state.esi_service.get_location_info(location_id)
            result[str(location_id)] = info
        return ok(data=result)
    except Exception as e:
        return error(message="Error in POST Method `/locations`: " + str(e))


@locations_bp.get("/location/<int:location_id>")
def location(location_id: int):
    try:
        require_ready()
        location_info = state.esi_service.get_location_info(location_id)
        return ok(data=location_info)
    except Exception as e:
        return error(message=f"Error in GET Method `/location/{location_id}`: " + str(e))
