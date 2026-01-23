from __future__ import annotations

from flask import Blueprint, request

from flask_app.bootstrap import require_ready
from flask_app.deps import get_state
from flask_app.http import ok

from eve_online_industry_tracker.application.locations.service import LocationsService


locations_bp = Blueprint("locations", __name__)


@locations_bp.post("/locations")
def locations():
    require_ready(get_state())
    data = request.get_json(silent=True) or {}
    location_ids = data.get("location_ids", [])
    svc = LocationsService(state=get_state())
    return ok(data=svc.get_locations(location_ids))


@locations_bp.get("/location/<int:location_id>")
def location(location_id: int):
    require_ready(get_state())
    svc = LocationsService(state=get_state())
    return ok(data=svc.get_location(location_id))
