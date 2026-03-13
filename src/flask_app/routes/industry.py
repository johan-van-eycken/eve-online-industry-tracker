from __future__ import annotations

from flask import Blueprint, request

from eve_online_industry_tracker.application.industry.service import IndustryService
from flask_app.bootstrap import require_ready, require_sde_ready
from flask_app.deps import get_state
from flask_app.http import ok


industry_bp = Blueprint("industry", __name__)


@industry_bp.get("/structure_type_bonuses/<int:type_id>")
def structure_type_bonuses(type_id: int):
    """Return base industry bonuses for a given structure type."""
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.structure_type_bonuses(type_id=type_id))


@industry_bp.get("/solar_systems")
def solar_systems():
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.solar_systems())


@industry_bp.get("/npc_stations/<int:system_id>")
def npc_stations(system_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.npc_stations(system_id=system_id))


@industry_bp.get("/public_structures/<int:system_id>")
def structures(system_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state())
    data, meta = svc.public_structures(system_id=system_id)
    return ok(data=data, meta=meta)


@industry_bp.get("/corporation_structures/<int:character_id>")
def corporation_structures(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.corporation_structures(character_id=character_id))


@industry_bp.get("/industry_profiles/<int:character_id>")
def industry_profiles(character_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_profiles(character_id=character_id))


@industry_bp.get("/industry_products/<int:character_id>")
def industry_products(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    refresh_raw = (request.args.get("refresh") or "0").strip().lower()
    refresh = refresh_raw in {"1", "true", "yes", "y", "on"}
    maximize_bp_runs_raw = (request.args.get("maximize_bp_runs") or "0").strip().lower()
    maximize_bp_runs = maximize_bp_runs_raw in {"1", "true", "yes", "y", "on"}
    build_from_bpc_raw = (request.args.get("build_from_bpc") or "1").strip().lower()
    build_from_bpc = build_from_bpc_raw in {"1", "true", "yes", "y", "on"}
    have_blueprint_source_only_raw = (request.args.get("have_blueprint_source_only") or "1").strip().lower()
    have_blueprint_source_only = have_blueprint_source_only_raw in {"1", "true", "yes", "y", "on"}
    include_reactions_raw = (request.args.get("include_reactions") or "0").strip().lower()
    include_reactions = include_reactions_raw in {"1", "true", "yes", "y", "on"}
    industry_profile_id_raw = (request.args.get("industry_profile_id") or "").strip()
    try:
        industry_profile_id = int(industry_profile_id_raw) if industry_profile_id_raw else None
    except Exception:
        industry_profile_id = None
    if industry_profile_id is not None and industry_profile_id <= 0:
        industry_profile_id = None
    owned_blueprints_scope = (request.args.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
    svc = IndustryService(state=get_state())
    return ok(
        data=svc.industry_manufacturing_product_overview(
            force_refresh=refresh,
            maximize_bp_runs=maximize_bp_runs,
            build_from_bpc=build_from_bpc,
            have_blueprint_source_only=have_blueprint_source_only,
            include_reactions=include_reactions,
            industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope,
            character_id=int(character_id),
        )
    )


@industry_bp.post("/industry_products/<int:character_id>/refresh")
def industry_products_refresh(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}
    maximize_bp_runs = bool(payload.get("maximize_bp_runs", False))
    build_from_bpc = bool(payload.get("build_from_bpc", True))
    have_blueprint_source_only = bool(payload.get("have_blueprint_source_only", True))
    include_reactions = bool(payload.get("include_reactions", False))
    force_refresh = bool(payload.get("force_refresh", True))
    raw_industry_profile_id = payload.get("industry_profile_id")
    try:
        industry_profile_id = int(raw_industry_profile_id) if raw_industry_profile_id is not None else None
    except Exception:
        industry_profile_id = None
    if industry_profile_id is not None and industry_profile_id <= 0:
        industry_profile_id = None
    owned_blueprints_scope = str(payload.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
    svc = IndustryService(state=get_state())
    return ok(
        data=svc.start_industry_manufacturing_product_overview_refresh(
            force_refresh=force_refresh,
            maximize_bp_runs=maximize_bp_runs,
            build_from_bpc=build_from_bpc,
            have_blueprint_source_only=have_blueprint_source_only,
            include_reactions=include_reactions,
            industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope,
            character_id=int(character_id),
        ),
        status_code=202,
    )


@industry_bp.get("/industry_products/refresh/<job_id>")
def industry_products_refresh_status(job_id: str):
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_manufacturing_product_overview_refresh_status(job_id=job_id))


@industry_bp.get("/industry_job_manager/status")
def industry_job_manager_status():
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_job_manager_status())


@industry_bp.get("/industry_system_cost_index/<int:system_id>")
def industry_system_cost_index(system_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_system_cost_index(system_id=system_id))


@industry_bp.get("/industry_facility/<int:facility_id>")
def industry_facility(facility_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_facility(facility_id=facility_id))


@industry_bp.get("/industry_structure_rigs")
def industry_structure_rigs():
    require_ready(get_state())
    require_sde_ready(get_state())
    svc = IndustryService(state=get_state())
    return ok(data=svc.structure_rigs())


@industry_bp.post("/industry_profiles")
def create_industry_profile():
    require_ready(get_state())
    data = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state())
    profile_id = svc.create_industry_profile(data=data)
    return ok(data={"id": profile_id}, status_code=201)


@industry_bp.put("/industry_profiles/<int:profile_id>")
def update_industry_profile(profile_id: int):
    require_ready(get_state())
    data = request.get_json(silent=True) or {}
    svc = IndustryService(state=get_state())
    svc.update_industry_profile(profile_id=profile_id, data=data)
    return ok(message="Industry profile updated successfully.")


@industry_bp.delete("/industry_profiles/<int:profile_id>")
def delete_industry_profile(profile_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    svc.delete_industry_profile(profile_id=profile_id)
    return ok(message="Industry profile deleted successfully.")
