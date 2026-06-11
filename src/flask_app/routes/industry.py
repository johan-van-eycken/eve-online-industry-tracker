from __future__ import annotations

from flask import Blueprint, request

from eve_online_industry_tracker.application.industry.portfolio_service import (
    IndustryPortfolioService,
    PortfolioCandidateDirective,
    PortfolioCandidateScope,
    PortfolioPlanRequest,
)
from eve_online_industry_tracker.application.industry.service import IndustryService
from flask_app.bootstrap import require_ready, require_sde_ready
from flask_app.deps import get_state
from flask_app.http import error, ok


industry_bp = Blueprint("industry", __name__)


def _string_list_payload(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        item_value = str(item or "").strip()
        if item_value:
            normalized.append(item_value)
    return normalized


def _candidate_directives_payload(value: object) -> list[PortfolioCandidateDirective]:
    if not isinstance(value, list):
        return []
    directives: list[PortfolioCandidateDirective] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        overview_row_id = str(entry.get("overview_row_id") or "").strip()
        if not overview_row_id:
            continue

        def _optional_int(raw_value: object) -> int | None:
            try:
                parsed = int(raw_value) if raw_value is not None else None
            except Exception:
                return None
            return parsed if parsed is not None and parsed > 0 else None

        directives.append(
            PortfolioCandidateDirective(
                overview_row_id=overview_row_id,
                force_include=bool(entry.get("force_include", False)),
                exclude=bool(entry.get("exclude", False)),
                lock_required=bool(entry.get("lock_required", False)),
                max_batches_override=_optional_int(entry.get("max_batches_override")),
                target_batches_override=_optional_int(entry.get("target_batches_override")),
                target_units_override=_optional_int(entry.get("target_units_override")),
            )
        )
    return directives


def _portfolio_plan_request_from_payload(payload: dict[str, object]) -> PortfolioPlanRequest:
    try:
        planning_horizon_hours = float(payload.get("planning_horizon_hours") or 24.0)
    except Exception:
        planning_horizon_hours = 24.0
    try:
        capital_limit_isk = float(payload.get("capital_limit_isk") or 0.0)
    except Exception:
        capital_limit_isk = 0.0
    try:
        manufacturing_slots_available = int(payload.get("manufacturing_slots_available") or 0)
    except Exception:
        manufacturing_slots_available = 0
    try:
        research_slots_available = int(payload.get("research_slots_available") or 0)
    except Exception:
        research_slots_available = 0
    try:
        reaction_slots_available = int(payload.get("reaction_slots_available") or 0)
    except Exception:
        reaction_slots_available = 0
    try:
        min_margin_pct = float(payload.get("min_margin_pct") or 0.0)
    except Exception:
        min_margin_pct = 0.0
    try:
        min_isk_per_hour = float(payload.get("min_isk_per_hour") or 0.0)
    except Exception:
        min_isk_per_hour = 0.0
    try:
        min_region_daily_volume = int(payload.get("min_region_daily_volume") or 0)
    except Exception:
        min_region_daily_volume = 0
    try:
        min_owned_input_coverage_pct = float(payload.get("min_owned_input_coverage_pct") or 0.0)
    except Exception:
        min_owned_input_coverage_pct = 0.0

    return PortfolioPlanRequest(
        candidate_snapshot_id=str(payload.get("candidate_snapshot_id") or "").strip(),
        capital_limit_isk=capital_limit_isk,
        manufacturing_slots_available=manufacturing_slots_available,
        research_slots_available=research_slots_available,
        reaction_slots_available=reaction_slots_available,
        planning_horizon_hours=planning_horizon_hours,
        objective=str(payload.get("objective") or "balanced"),
        minimum_pricing_confidence=str(payload.get("minimum_pricing_confidence") or "low"),
        candidate_directives=_candidate_directives_payload(payload.get("candidate_directives")),
        candidate_scope=PortfolioCandidateScope(
            categories=_string_list_payload(payload.get("candidate_categories")),
            meta_groups=_string_list_payload(payload.get("candidate_meta_groups")),
            pricing_confidences=_string_list_payload(payload.get("candidate_pricing_confidences")),
            blueprint_sources=_string_list_payload(payload.get("candidate_blueprint_sources")),
            positive_profit_only=bool(payload.get("positive_profit_only", False)),
            min_margin_pct=min_margin_pct,
            min_isk_per_hour=min_isk_per_hour,
            min_region_daily_volume=min_region_daily_volume,
            min_owned_input_coverage_pct=min_owned_input_coverage_pct,
        ),
    )


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
    group_identical_bpcs_raw = (request.args.get("group_identical_bpcs") or "1").strip().lower()
    group_identical_bpcs = group_identical_bpcs_raw in {"1", "true", "yes", "y", "on"}
    build_from_bpc_raw = (request.args.get("build_from_bpc") or "1").strip().lower()
    build_from_bpc = build_from_bpc_raw in {"1", "true", "yes", "y", "on"}
    have_blueprint_source_only_raw = (request.args.get("have_blueprint_source_only") or "1").strip().lower()
    have_blueprint_source_only = have_blueprint_source_only_raw in {"1", "true", "yes", "y", "on"}
    include_reactions_raw = (request.args.get("include_reactions") or "0").strip().lower()
    include_reactions = include_reactions_raw in {"1", "true", "yes", "y", "on"}
    market_hub = str(request.args.get("market_hub") or "jita").strip().lower() or "jita"
    material_price_side = str(request.args.get("material_price_side") or "sell").strip().lower() or "sell"
    product_price_side = str(request.args.get("product_price_side") or "sell").strip().lower() or "sell"
    industry_profile_id_raw = (request.args.get("industry_profile_id") or "").strip()
    if industry_profile_id_raw:
        try:
            industry_profile_id = int(industry_profile_id_raw)
        except (ValueError, TypeError):
            return error(message="Invalid industry_profile_id: must be an integer.", status_code=400)
        if industry_profile_id <= 0:
            industry_profile_id = None
    else:
        industry_profile_id = None
    owned_blueprints_scope = (request.args.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
    svc = IndustryService(state=get_state())
    payload = svc.industry_manufacturing_product_overview_payload(
        force_refresh=refresh,
        maximize_bp_runs=maximize_bp_runs,
        group_identical_bpcs=group_identical_bpcs,
        build_from_bpc=build_from_bpc,
        have_blueprint_source_only=have_blueprint_source_only,
        include_reactions=include_reactions,
        market_hub=market_hub,
        material_price_side=material_price_side,
        product_price_side=product_price_side,
        industry_profile_id=industry_profile_id,
        owned_blueprints_scope=owned_blueprints_scope,
        character_id=int(character_id),
    )
    return ok(
        data=payload.get("rows") or [],
        meta={"pricing_batch": payload.get("pricing_batch") or {}},
    )


@industry_bp.post("/industry_products/<int:character_id>/refresh")
def industry_products_refresh(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}
    maximize_bp_runs = bool(payload.get("maximize_bp_runs", False))
    group_identical_bpcs = bool(payload.get("group_identical_bpcs", True))
    build_from_bpc = bool(payload.get("build_from_bpc", True))
    have_blueprint_source_only = bool(payload.get("have_blueprint_source_only", True))
    include_reactions = bool(payload.get("include_reactions", False))
    market_hub = str(payload.get("market_hub") or "jita").strip().lower() or "jita"
    material_price_side = str(payload.get("material_price_side") or "sell").strip().lower() or "sell"
    product_price_side = str(payload.get("product_price_side") or "sell").strip().lower() or "sell"
    force_refresh = bool(payload.get("force_refresh", True))
    raw_industry_profile_id = payload.get("industry_profile_id")
    if raw_industry_profile_id is not None:
        try:
            industry_profile_id = int(raw_industry_profile_id)
        except (ValueError, TypeError):
            return error(message="Invalid industry_profile_id: must be an integer.", status_code=400)
        if industry_profile_id <= 0:
            industry_profile_id = None
    else:
        industry_profile_id = None
    owned_blueprints_scope = str(payload.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
    svc = IndustryService(state=get_state())
    return ok(
        data=svc.start_industry_manufacturing_product_overview_refresh(
            force_refresh=force_refresh,
            maximize_bp_runs=maximize_bp_runs,
            group_identical_bpcs=group_identical_bpcs,
            build_from_bpc=build_from_bpc,
            have_blueprint_source_only=have_blueprint_source_only,
            include_reactions=include_reactions,
            market_hub=market_hub,
            material_price_side=material_price_side,
            product_price_side=product_price_side,
            industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope,
            character_id=int(character_id),
        ),
        status_code=202,
    )


@industry_bp.get("/industry_products/refresh/<job_id>")
def industry_products_refresh_status(job_id: str):
    require_ready(get_state())
    if not job_id or not job_id.strip():
        return error(message="job_id is required.", status_code=400)
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_manufacturing_product_overview_refresh_status(job_id=job_id))


@industry_bp.post("/industry_products/<int:character_id>/portfolio_candidates/start")
def industry_portfolio_candidates_start(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}
    maximize_bp_runs = bool(payload.get("maximize_bp_runs", False))
    group_identical_bpcs = bool(payload.get("group_identical_bpcs", True))
    build_from_bpc = bool(payload.get("build_from_bpc", True))
    have_blueprint_source_only = bool(payload.get("have_blueprint_source_only", True))
    include_reactions = bool(payload.get("include_reactions", False))
    market_hub = str(payload.get("market_hub") or "jita").strip().lower() or "jita"
    material_price_side = str(payload.get("material_price_side") or "sell").strip().lower() or "sell"
    product_price_side = str(payload.get("product_price_side") or "sell").strip().lower() or "sell"
    owned_blueprints_scope = str(payload.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
    raw_industry_profile_id = payload.get("industry_profile_id")
    if raw_industry_profile_id is not None:
        try:
            industry_profile_id = int(raw_industry_profile_id)
        except (ValueError, TypeError):
            return error(message="Invalid industry_profile_id: must be an integer.", status_code=400)
        if industry_profile_id <= 0:
            industry_profile_id = None
    else:
        industry_profile_id = None
    try:
        planning_horizon_hours = float(payload.get("planning_horizon_hours") or 24.0)
    except Exception:
        planning_horizon_hours = 24.0
    have_skills_only = bool(payload.get("have_skills_only", True))

    svc = IndustryService(state=get_state())
    refresh_job = svc.start_industry_manufacturing_portfolio_candidates_refresh(
        maximize_bp_runs=maximize_bp_runs,
        group_identical_bpcs=group_identical_bpcs,
        build_from_bpc=build_from_bpc,
        have_blueprint_source_only=have_blueprint_source_only,
        include_reactions=include_reactions,
        market_hub=market_hub,
        material_price_side=material_price_side,
        product_price_side=product_price_side,
        industry_profile_id=industry_profile_id,
        owned_blueprints_scope=owned_blueprints_scope,
        character_id=int(character_id),
        planning_horizon_hours=planning_horizon_hours,
        have_skills_only=have_skills_only,
    )
    return ok(data=refresh_job, status_code=202)


@industry_bp.get("/industry_products/portfolio_candidates/<job_id>")
def industry_portfolio_candidates_status(job_id: str):
    require_ready(get_state())
    if not job_id or not job_id.strip():
        return error(message="job_id is required.", status_code=400)
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_manufacturing_portfolio_candidates_refresh_status(job_id=job_id))


@industry_bp.get("/industry_products/<int:character_id>/portfolio_candidates")
def industry_portfolio_candidates(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    maximize_bp_runs_raw = (request.args.get("maximize_bp_runs") or "0").strip().lower()
    maximize_bp_runs = maximize_bp_runs_raw in {"1", "true", "yes", "y", "on"}
    group_identical_bpcs_raw = (request.args.get("group_identical_bpcs") or "1").strip().lower()
    group_identical_bpcs = group_identical_bpcs_raw in {"1", "true", "yes", "y", "on"}
    build_from_bpc_raw = (request.args.get("build_from_bpc") or "1").strip().lower()
    build_from_bpc = build_from_bpc_raw in {"1", "true", "yes", "y", "on"}
    have_blueprint_source_only_raw = (request.args.get("have_blueprint_source_only") or "1").strip().lower()
    have_blueprint_source_only = have_blueprint_source_only_raw in {"1", "true", "yes", "y", "on"}
    include_reactions_raw = (request.args.get("include_reactions") or "0").strip().lower()
    include_reactions = include_reactions_raw in {"1", "true", "yes", "y", "on"}
    market_hub = str(request.args.get("market_hub") or "jita").strip().lower() or "jita"
    material_price_side = str(request.args.get("material_price_side") or "sell").strip().lower() or "sell"
    product_price_side = str(request.args.get("product_price_side") or "sell").strip().lower() or "sell"
    planning_horizon_hours_raw = (request.args.get("planning_horizon_hours") or "24").strip()
    try:
        planning_horizon_hours = float(planning_horizon_hours_raw or 24.0)
    except Exception:
        planning_horizon_hours = 24.0
    industry_profile_id_raw = (request.args.get("industry_profile_id") or "").strip()
    if industry_profile_id_raw:
        try:
            industry_profile_id = int(industry_profile_id_raw)
        except (ValueError, TypeError):
            return error(message="Invalid industry_profile_id: must be an integer.", status_code=400)
        if industry_profile_id <= 0:
            industry_profile_id = None
    else:
        industry_profile_id = None
    owned_blueprints_scope = (request.args.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
    svc = IndustryService(state=get_state())
    payload = svc.industry_manufacturing_portfolio_candidates_payload(
        maximize_bp_runs=maximize_bp_runs,
        group_identical_bpcs=group_identical_bpcs,
        build_from_bpc=build_from_bpc,
        have_blueprint_source_only=have_blueprint_source_only,
        include_reactions=include_reactions,
        market_hub=market_hub,
        material_price_side=material_price_side,
        product_price_side=product_price_side,
        industry_profile_id=industry_profile_id,
        owned_blueprints_scope=owned_blueprints_scope,
        character_id=int(character_id),
        planning_horizon_hours=planning_horizon_hours,
    )
    return ok(
        data=payload.get("candidates") or [],
        meta={
            "summary": payload.get("summary") or {},
            "pricing_batch": payload.get("pricing_batch") or {},
        },
    )


_VALID_OBJECTIVES = {"balanced", "max_isk_per_hour"}
_VALID_PRICING_CONFIDENCES = {"low", "medium", "high"}


@industry_bp.post("/industry_products/<int:character_id>/portfolio_plan")
def industry_portfolio_plan(character_id: int):
    require_ready(get_state())
    require_sde_ready(get_state())
    payload = request.get_json(silent=True) or {}

    raw_objective = str(payload.get("objective") or "").strip().lower()
    if raw_objective and raw_objective not in _VALID_OBJECTIVES:
        return error(
            message=f"Invalid objective '{raw_objective}'. Must be one of: {sorted(_VALID_OBJECTIVES)}.",
            status_code=400,
        )
    raw_confidence = str(payload.get("minimum_pricing_confidence") or "").strip().lower()
    if raw_confidence and raw_confidence not in _VALID_PRICING_CONFIDENCES:
        return error(
            message=f"Invalid minimum_pricing_confidence '{raw_confidence}'. Must be one of: {sorted(_VALID_PRICING_CONFIDENCES)}.",
            status_code=400,
        )

    plan_request = _portfolio_plan_request_from_payload(payload)

    svc = IndustryService(state=get_state())
    if plan_request.candidate_snapshot_id:
        candidate_snapshot = svc.industry_manufacturing_portfolio_candidate_snapshot(
            snapshot_id=plan_request.candidate_snapshot_id,
            character_id=int(character_id),
        )
    else:
        maximize_bp_runs = bool(payload.get("maximize_bp_runs", False))
        group_identical_bpcs = bool(payload.get("group_identical_bpcs", True))
        build_from_bpc = bool(payload.get("build_from_bpc", True))
        have_blueprint_source_only = bool(payload.get("have_blueprint_source_only", True))
        include_reactions = bool(payload.get("include_reactions", False))
        market_hub = str(payload.get("market_hub") or "jita").strip().lower() or "jita"
        material_price_side = str(payload.get("material_price_side") or "sell").strip().lower() or "sell"
        product_price_side = str(payload.get("product_price_side") or "sell").strip().lower() or "sell"
        owned_blueprints_scope = str(payload.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters"
        raw_industry_profile_id = payload.get("industry_profile_id")
        if raw_industry_profile_id is not None:
            try:
                industry_profile_id = int(raw_industry_profile_id)
            except (ValueError, TypeError):
                return error(message="Invalid industry_profile_id: must be an integer.", status_code=400)
            if industry_profile_id <= 0:
                industry_profile_id = None
        else:
            industry_profile_id = None

        candidate_payload = svc.industry_manufacturing_portfolio_candidates_payload(
            maximize_bp_runs=maximize_bp_runs,
            group_identical_bpcs=group_identical_bpcs,
            build_from_bpc=build_from_bpc,
            have_blueprint_source_only=have_blueprint_source_only,
            include_reactions=include_reactions,
            market_hub=market_hub,
            material_price_side=material_price_side,
            product_price_side=product_price_side,
            industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope,
            character_id=int(character_id),
            planning_horizon_hours=float(plan_request.planning_horizon_hours or 24.0),
        )
        candidate_snapshot = {
            "snapshot_id": "",
            "created_at": None,
            "updated_at": None,
            "request_params": {
                "maximize_bp_runs": maximize_bp_runs,
                "group_identical_bpcs": group_identical_bpcs,
                "build_from_bpc": build_from_bpc,
                "have_blueprint_source_only": have_blueprint_source_only,
                "include_reactions": include_reactions,
                "market_hub": market_hub,
                "material_price_side": material_price_side,
                "product_price_side": product_price_side,
                "industry_profile_id": industry_profile_id,
                "owned_blueprints_scope": owned_blueprints_scope,
                "character_id": int(character_id),
            },
            "candidates": list(candidate_payload.get("candidates") or []),
            "summary": candidate_payload.get("summary") or {},
            "pricing_batch": candidate_payload.get("pricing_batch") or {},
        }

    planner = IndustryPortfolioService()
    plan = planner.optimize_manufacturing_portfolio(
        candidates=list(candidate_snapshot.get("candidates") or []),
        plan_request=plan_request,
    )
    return ok(
        data={
            **plan,
            "summary": candidate_snapshot.get("summary") or {},
            "pricing_batch": candidate_snapshot.get("pricing_batch") or {},
            "candidate_snapshot_id": candidate_snapshot.get("snapshot_id") or plan_request.candidate_snapshot_id,
            "candidate_snapshot_created_at": candidate_snapshot.get("created_at"),
            "candidate_snapshot_updated_at": candidate_snapshot.get("updated_at"),
            "candidate_snapshot_request_params": candidate_snapshot.get("request_params") or {},
        }
    )


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
    profile_name = str(data.get("profile_name") or "").strip()
    if not profile_name:
        return error(message="profile_name is required and must be non-empty.", status_code=400)
    svc = IndustryService(state=get_state())
    profile_id = svc.create_industry_profile(data=data)
    return ok(data={"id": profile_id}, status_code=201)


@industry_bp.put("/industry_profiles/<int:profile_id>")
def update_industry_profile(profile_id: int):
    require_ready(get_state())
    data = request.get_json(silent=True) or {}
    if "profile_name" in data and not str(data.get("profile_name") or "").strip():
        return error(message="profile_name must be non-empty when provided.", status_code=400)
    svc = IndustryService(state=get_state())
    svc.update_industry_profile(profile_id=profile_id, data=data)
    return ok(message="Industry profile updated successfully.")


@industry_bp.delete("/industry_profiles/<int:profile_id>")
def delete_industry_profile(profile_id: int):
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    svc.delete_industry_profile(profile_id=profile_id)
    return ok(message="Industry profile deleted successfully.")


@industry_bp.get("/industry_active_jobs")
def industry_active_jobs():
    require_ready(get_state())
    character_id_raw = (request.args.get("character_id") or "").strip()
    character_id = int(character_id_raw) if character_id_raw.isdigit() else None
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_active_jobs(character_id=character_id))
