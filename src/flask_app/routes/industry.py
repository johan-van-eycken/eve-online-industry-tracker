from __future__ import annotations

import math

from flask import Blueprint, request

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.industry.service import IndustryService
from flask_app.bootstrap import require_ready, require_sde_ready
from flask_app.deps import get_state
from flask_app.http import error, ok, optional_id_param


industry_bp = Blueprint("industry", __name__)


def _strict_number(raw: object, cast):
    """`raw` as a finite float (cast float) or a whole int (cast int).

    Raises ValueError for a non-number, a non-finite value (inf, nan, or an
    int too large for a float: OverflowError) and a fractional value given
    for an int, so none of them is silently truncated or kept as inf."""
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise ValueError(f"not a number: {raw!r}")
    try:
        value = float(raw)
    except OverflowError:
        raise ValueError(f"out of range: {raw!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"not finite: {raw!r}")
    if cast is int:
        if isinstance(raw, int):
            return int(raw)
        if not value.is_integer():
            raise ValueError(f"not a whole number: {raw!r}")
        return int(value)
    return cast(value)


def _number_param(params, key: str, default, cast):
    """A numeric request value, or `default` when it is absent or blank.

    A value that is present but not a (finite, for an int whole) number is a
    400 naming the field, not a silent default (a bad capital limit must not
    plan with 0). An explicit 0 is kept: it is not "absent"."""
    raw = params.get(key)
    if isinstance(raw, str):
        raw = raw.strip()
    if raw is None or raw == "":
        return default
    try:
        return _strict_number(raw, cast)
    except ValueError:
        kind = "an integer" if cast is int else "a finite number"
        raise ServiceError(f"Invalid {key}: must be {kind}, got {raw!r}.", status_code=400) from None


def _industry_profile_id_param(params) -> int | None:
    """industry_profile_id: None when absent or blank; otherwise a positive
    integer. A malformed, fractional, boolean, 0 or negative id is a 400
    naming the field (a 0 used to mean "no profile" silently)."""
    profile_id = _number_param(params, "industry_profile_id", None, int)
    if profile_id is not None and profile_id <= 0:
        raise ServiceError(
            f"Invalid industry_profile_id: must be a positive integer, got {params.get('industry_profile_id')!r}.",
            status_code=400,
        )
    return profile_id


def _flag(params, key: str, default: bool) -> bool:
    """A boolean from a query string or a JSON body: a real bool as is, a
    string by its meaning ("false" is False, not bool("false")), absent or
    blank the default."""
    raw = params.get(key)
    if isinstance(raw, str):
        raw = raw.strip().lower()
        return default if raw == "" else raw in {"1", "true", "yes", "y", "on"}
    return default if raw is None else bool(raw)


def _overview_params(params, character_id: int) -> dict[str, object]:
    """The request values of the overview routes, GET or POST."""
    return {
        "maximize_bp_runs": _flag(params, "maximize_bp_runs", False),
        "group_identical_bpcs": _flag(params, "group_identical_bpcs", True),
        "build_from_bpc": _flag(params, "build_from_bpc", True),
        "have_blueprint_source_only": _flag(params, "have_blueprint_source_only", True),
        "include_reactions": _flag(params, "include_reactions", False),
        "market_hub": str(params.get("market_hub") or "jita").strip().lower() or "jita",
        "material_price_side": str(params.get("material_price_side") or "sell").strip().lower() or "sell",
        "product_price_side": str(params.get("product_price_side") or "sell").strip().lower() or "sell",
        "industry_profile_id": _industry_profile_id_param(params),
        "owned_blueprints_scope": str(params.get("owned_blueprints_scope") or "all_characters").strip() or "all_characters",
        "character_id": int(character_id),
    }


def _type_ids_param(key: str) -> list[int]:
    """A comma-separated id list query value: [] when absent or blank. A
    malformed entry (not a whole number, or an empty entry between commas)
    is a 400 naming the field, not an empty result."""
    raw = (request.args.get(key) or "").strip()
    if not raw:
        return []
    ids: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part.isdigit():
            raise ServiceError(f"Invalid {key}: must be comma-separated type ids, got {raw!r}.", status_code=400)
        ids.append(int(part))
    return ids


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
    svc = IndustryService(state=get_state())
    payload = svc.industry_manufacturing_product_overview_payload(
        force_refresh=_flag(request.args, "refresh", False),
        **_overview_params(request.args, character_id),
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
    svc = IndustryService(state=get_state())
    return ok(
        data=svc.start_industry_manufacturing_product_overview_refresh(
            force_refresh=_flag(payload, "force_refresh", True),
            **_overview_params(payload, character_id),
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


_VALID_OBJECTIVES = {"balanced", "max_isk_per_hour"}
_VALID_PRICING_CONFIDENCES = {"low", "medium", "high"}


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


@industry_bp.get("/industry_profile_context/<int:profile_id>")
def industry_profile_context(profile_id: int):
    """The profile as the producer resolves it: the profile plus its system
    cost indices, facility and structure bonuses, with the reasons a part
    could not be read (lookup_error, cost_index_unknown_reason), so the
    profile page can show them."""
    require_ready(get_state())
    svc = IndustryService(state=get_state())
    context = svc.industry_profile_context(int(profile_id))
    if context is None:
        return error(message=f"Industry profile ID {profile_id} not found", status_code=404)
    return ok(data=context)


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
    character_id = optional_id_param("character_id")
    svc = IndustryService(state=get_state())
    return ok(data=svc.industry_active_jobs(character_id=character_id))


@industry_bp.get("/industry/reorder_alerts")
def industry_reorder_alerts():
    require_ready(get_state())
    type_ids = _type_ids_param("type_ids")
    if not type_ids:
        return ok(data={})
    from datetime import date
    today_iso = date.today().isoformat()
    svc = IndustryService(state=get_state())
    result = svc.get_industry_reorder_alerts(type_ids=type_ids, today_iso=today_iso)
    return ok(data={str(k): v for k, v in result.items()})


@industry_bp.get("/industry/track_record")
def industry_track_record():
    require_ready(get_state())
    type_ids = _type_ids_param("type_ids")
    if not type_ids:
        return ok(data={})
    from datetime import date
    today_iso = date.today().isoformat()
    svc = IndustryService(state=get_state())
    result = svc.get_type_track_record(type_ids=type_ids, today_iso=today_iso)
    return ok(data={str(k): v for k, v in result.items()})
