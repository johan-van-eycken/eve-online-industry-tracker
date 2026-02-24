from __future__ import annotations

from flask import Blueprint, request

from eve_online_industry_tracker.application.industry.service import IndustryService
from flask_app.bootstrap import require_ready, require_sde_ready
from flask_app.deps import get_state
from flask_app.http import ok
from flask_app.session_provider import FlaskSessionProvider


industry_bp = Blueprint("industry", __name__)


@industry_bp.post("/industry_builder_update/<int:character_id>")
def industry_builder_update(character_id: int):
    """Kick off a background computation of Industry Builder data (incl. submanufacturing)."""
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.start_industry_builder_update(character_id=character_id, payload=payload))


@industry_bp.get("/industry_builder_update_status/<job_id>")
def industry_builder_update_status(job_id: str):
    require_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.industry_builder_update_status(job_id=job_id))


@industry_bp.get("/industry_builder_update_result/<job_id>")
def industry_builder_update_result(job_id: str):
    require_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    data, meta = svc.industry_builder_update_result(job_id=job_id)
    return ok(data=data, meta=meta)


@industry_bp.get("/structure_type_bonuses/<int:type_id>")
def structure_type_bonuses(type_id: int):
    """Return base industry bonuses for a given structure type."""
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.structure_type_bonuses(type_id=type_id))


@industry_bp.get("/industry_builder_data/<int:character_id>")
def industry_builder(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    profile_id = request.args.get("profile_id", default=None, type=int)
    maximize_runs = bool(request.args.get("maximize_runs", default=0, type=int))
    include_submanufacturing = bool(request.args.get("include_submanufacturing", default=0, type=int))
    submanufacturing_blueprint_type_id = request.args.get("blueprint_type_id", default=None, type=int)

    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    data, meta = svc.industry_builder_data(
        character_id=character_id,
        profile_id=profile_id,
        maximize_runs=maximize_runs,
        include_submanufacturing=include_submanufacturing,
        submanufacturing_blueprint_type_id=submanufacturing_blueprint_type_id,
    )
    return ok(data=data, meta=meta)


@industry_bp.post("/industry_submanufacturing_plan/<int:character_id>")
def industry_submanufacturing_plan(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    data, meta = svc.industry_submanufacturing_plan(character_id=character_id, payload=payload)
    return ok(data=data, meta=meta)


@industry_bp.post("/industry_invention_options/<int:character_id>/<int:blueprint_type_id>")
def industry_invention_options(character_id: int, blueprint_type_id: int):
    """Compute T2 invention options (decryptor ROI) for a single blueprint."""
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    data, meta = svc.industry_invention_options(
        character_id=int(character_id),
        blueprint_type_id=int(blueprint_type_id),
        payload=payload,
    )
    return ok(data=data, meta=meta)


@industry_bp.get("/solar_systems")
def solar_systems():
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.solar_systems())


@industry_bp.get("/npc_stations/<int:system_id>")
def npc_stations(system_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.npc_stations(system_id=system_id))


@industry_bp.get("/public_structures/<int:system_id>")
def structures(system_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    data, meta = svc.public_structures(system_id=system_id)
    return ok(data=data, meta=meta)


@industry_bp.get("/corporation_structures/<int:character_id>")
def corporation_structures(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.corporation_structures(character_id=character_id))


@industry_bp.get("/industry_profiles/<int:character_id>")
def industry_profiles(character_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.industry_profiles(character_id=character_id))


@industry_bp.get("/industry_system_cost_index/<int:system_id>")
def industry_system_cost_index(system_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.industry_system_cost_index(system_id=system_id))


@industry_bp.get("/industry_facility/<int:facility_id>")
def industry_facility(facility_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.industry_facility(facility_id=facility_id))


@industry_bp.get("/structure_rigs")
def structure_rigs():
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    return ok(data=svc.structure_rigs())


@industry_bp.post("/industry_profiles")
def create_industry_profile():
    require_ready(get_state())
    data = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    profile_id = svc.create_industry_profile(data=data)
    return ok(data={"id": profile_id}, status_code=201)


@industry_bp.put("/industry_profiles/<int:profile_id>")
def update_industry_profile(profile_id: int):
    require_ready(get_state())
    data = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    svc.update_industry_profile(profile_id=profile_id, data=data)
    return ok(message="Industry profile updated successfully.")


@industry_bp.delete("/industry_profiles/<int:profile_id>")
def delete_industry_profile(profile_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state(), sessions=FlaskSessionProvider())
    svc.delete_industry_profile(profile_id=profile_id)
    return ok(message="Industry profile deleted successfully.")
