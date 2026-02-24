from __future__ import annotations

import math
from typing import Any, cast

from classes.asset_provenance import fifo_allocate_cost_breakdown
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors
from eve_online_industry_tracker.infrastructure.sde.rig_effects import compute_rig_reduction_for


def _as_fraction(v: Any) -> float:
    try:
        f = float(v or 0.0)
    except Exception:
        return 0.0
    if f >= 1.0:
        f = f / 100.0
    return max(0.0, min(f, 1.0))


def _infer_rig_group_label_from_products(products: list[dict]) -> str:
    if not products:
        return "All"

    p0 = next((p for p in products if isinstance(p, dict)), None) or {}
    cat = str(p0.get("category_name") or "").strip().lower()
    grp = str(p0.get("group_name") or "").strip().lower()

    # Reactions
    if "reaction" in grp or "reaction" in cat:
        if "biochemical" in grp or "biochemical" in cat:
            return "Biochemical Reactions"
        if "composite" in grp or "composite" in cat:
            return "Composite Reactions"
        if "hybrid" in grp or "hybrid" in cat:
            return "Hybrid Reactions"
        return "Biochemical Reactions"

    # Structures
    if cat == "structure" or "structure" in grp:
        return "Structures"

    # Ammo & charges
    if cat in {"charge", "charges"} or "ammo" in grp or "charge" in grp:
        return "Ammo & Charges"

    # Drones
    if cat == "drone" or "drone" in grp:
        return "Drones"

    # Modules
    if cat == "module" or "module" in grp:
        return "Modules"

    # Components
    if "component" in grp:
        if "capital" in grp:
            return "Capital Components"
        if "advanced" in grp or "adv" in grp:
            return "Advanced Components"
        return "Advanced Components"

    # Ships
    if cat == "ship" or "ship" in grp:
        advanced_tokens = [
            "assault",
            "interceptor",
            "interdictor",
            "covert",
            "strategic",
            "command",
            "marauder",
            "black ops",
            "logistics",
            "heavy assault",
            "recon",
            "electronic attack",
            "stealth bomber",
            "tactical destroyer",
        ]
        is_advanced = any(t in grp for t in advanced_tokens) or "t2" in grp or "t3" in grp

        if "capital" in grp or "supercarrier" in grp or "titan" in grp or "dread" in grp or "carrier" in grp:
            return "Capital Ships"

        small = ["frigate", "destroyer"]
        medium = ["cruiser", "battlecruiser"]
        large = ["battleship"]

        if any(x in grp for x in small):
            return "Advanced Small Ships" if is_advanced else "Basic Small Ships"
        if any(x in grp for x in medium):
            return "Advanced Medium Ships" if is_advanced else "Basic Medium Ships"
        if any(x in grp for x in large):
            return "Advanced Large Ships" if is_advanced else "Basic Large Ships"

        return "All Ships"

    return "All"


def _market_price_map_from_esi_prices(market_prices: list[dict] | None) -> dict[int, dict[str, float | None]]:
    out: dict[int, dict[str, float | None]] = {}
    for r in market_prices or []:
        if not isinstance(r, dict):
            continue
        tid = r.get("type_id")
        if tid is None:
            continue
        try:
            tid_i = int(tid)
        except Exception:
            continue
        if tid_i <= 0:
            continue
        avg = r.get("average_price")
        adj = r.get("adjusted_price")
        rec: dict[str, float | None] = {}
        try:
            if avg is not None:
                rec["average_price"] = float(avg)
        except Exception:
            pass
        try:
            if adj is not None:
                rec["adjusted_price"] = float(adj)
        except Exception:
            pass
        if rec:
            out[tid_i] = rec
    return out


def market_price_map_from_esi_prices(market_prices: list[dict] | None) -> dict[int, dict[str, float | None]]:
    """Public wrapper for building the market price map once.

    Useful for callers that want to fetch ESI market prices once and reuse the
    resulting map across many calculations.
    """

    return _market_price_map_from_esi_prices(market_prices)


def _avg_price(price_map: dict[int, dict[str, float | None]], *, type_id: int) -> float | None:
    rec = price_map.get(int(type_id))
    if not isinstance(rec, dict):
        return None
    p = rec.get("average_price")
    if isinstance(p, (int, float)) and float(p) > 0:
        return float(p)
    p2 = rec.get("adjusted_price")
    if isinstance(p2, (int, float)) and float(p2) > 0:
        return float(p2)
    return None


def _adjusted_or_avg_price(price_map: dict[int, dict[str, float | None]], *, type_id: int) -> float | None:
    rec = price_map.get(int(type_id))
    if not isinstance(rec, dict):
        return None
    p = rec.get("adjusted_price")
    if isinstance(p, (int, float)) and float(p) > 0:
        return float(p)
    return _avg_price(price_map, type_id=int(type_id))


def _job_fee(
    *,
    estimated_item_value_isk: float,
    system_cost_index: float,
    effective_cost_reduction_fraction: float,
    surcharge_rate_total_fraction: float,
) -> dict[str, Any]:
    job_value = max(0.0, float(estimated_item_value_isk or 0.0))
    ci = max(0.0, float(system_cost_index or 0.0))
    cost_red = max(0.0, min(float(effective_cost_reduction_fraction or 0.0), 1.0))
    surcharge = max(0.0, float(surcharge_rate_total_fraction or 0.0))

    gross_cost = job_value * ci
    gross_cost_after_bonuses = gross_cost * (1.0 - cost_red)
    taxes = job_value * surcharge
    total_job_cost = max(0.0, gross_cost_after_bonuses + taxes)

    return {
        "estimated_item_value_total_isk": float(job_value),
        "system_cost_index": float(ci),
        "structure_cost_reduction_fraction": float(cost_red),
        "surcharge_rate_total_fraction": float(surcharge),
        "gross_cost_isk": float(gross_cost),
        "gross_cost_after_bonuses_isk": float(gross_cost_after_bonuses),
        "taxes_isk": float(taxes),
        "total_job_cost_isk": float(total_job_cost),
    }


def compute_invention_options_for_blueprint(
    *,
    sde_session: Any,
    esi_service: Any | None,
    language: str,
    blueprint_type_id: int,
    character_skills: list[dict] | None = None,
    industry_profile: Any | None = None,
    rig_payload: list[dict] | None = None,
    manufacturing_system_cost_index: float | None = None,
    invention_system_cost_index: float | None = None,
    copying_system_cost_index: float | None = None,
    blueprint_data_map: dict[int, dict] | None = None,
    market_price_map: dict[int, dict[str, float | None]] | None = None,
    inventory_on_hand_by_type: dict[int, int] | None = None,
    inventory_fifo_lots_by_type: dict[int, list] | None = None,
    use_fifo_inventory_costing: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compute decryptor options and a simple ROI signal for T2 invention.

    This focuses on what you asked for first: decryptor choice that maximizes ROI.

        Current model supports optional facility rigs/taxes and job fees when an
        `industry_profile` is provided.
        - invention material cost uses FIFO inventory valuation when inventory maps are provided
            (take on-hand first; price FIFO lots; remainder at market average). Otherwise it falls
            back to market average (fallback adjusted).
    - manufacturing cost/revenue uses ESI market average (fallback adjusted)
    - ROI is computed per manufactured item after amortizing expected invention costs

    Returns (data, meta).
    """

    if not blueprint_type_id:
        raise ServiceError("Blueprint type ID is required.", status_code=400)

    bp: dict | None = None
    if isinstance(blueprint_data_map, dict):
        bp = blueprint_data_map.get(int(blueprint_type_id))
    if not isinstance(bp, dict):
        bp_data_map = get_blueprint_manufacturing_data(sde_session, language, [int(blueprint_type_id)])
        bp = bp_data_map.get(int(blueprint_type_id)) if isinstance(bp_data_map, dict) else None
    if not isinstance(bp, dict):
        raise ServiceError("Blueprint not found in SDE.", status_code=404)

    invention = bp.get("invention") if isinstance(bp.get("invention"), dict) else {}
    if not invention:
        raise ServiceError("This blueprint has no invention activity.", status_code=400)

    products = invention.get("products") or []
    if not isinstance(products, list) or not products:
        raise ServiceError("Invention products are missing in SDE for this blueprint.", status_code=400)

    base_prob = invention.get("probability")
    if base_prob is None:
        # Some SDE exports store invention probability on the product entry.
        for p in products:
            if not isinstance(p, dict):
                continue
            if p.get("probability") is None:
                continue
            base_prob = p.get("probability")
            break
    try:
        base_prob_f = float(base_prob) if base_prob is not None else None
    except Exception:
        base_prob_f = None

    if base_prob_f is None or base_prob_f <= 0:
        raise ServiceError("Invention probability is missing in SDE for this blueprint.", status_code=400)

    # Character skill effects on invention success chance
    # Formula (EVE University / common calculators):
    # success = base * (1 + (science1 + science2)/30 + encryption/40) * decryptor_multiplier
    trained_level_by_skill_id: dict[int, int] = {}
    for s in character_skills or []:
        if not isinstance(s, dict):
            continue
        sid = s.get("skill_id")
        lvl = s.get("trained_skill_level")
        if sid is None:
            continue
        try:
            sid_i = int(sid)
        except Exception:
            continue
        if sid_i <= 0:
            continue
        try:
            lvl_i = int(lvl or 0)
        except Exception:
            lvl_i = 0
        trained_level_by_skill_id[sid_i] = max(0, min(lvl_i, 5))

    required_skills = invention.get("skills") if isinstance(invention.get("skills"), list) else []
    encryption_skill_type_id: int | None = None
    science_skill_type_ids: list[int] = []
    for r in required_skills or []:
        if not isinstance(r, dict):
            continue
        tid = r.get("type_id")
        if tid is None:
            continue
        try:
            tid_i = int(tid)
        except Exception:
            continue
        if tid_i <= 0:
            continue

        name_l = str(r.get("type_name") or "").strip().lower()
        group_l = str(r.get("group_name") or "").strip().lower()
        if encryption_skill_type_id is None and ("encryption" in name_l or "encryption" in group_l):
            encryption_skill_type_id = int(tid_i)
        else:
            science_skill_type_ids.append(int(tid_i))

    # In current SDE, invention.skills is the 3 required skills: 1 encryption + 2 science.
    if encryption_skill_type_id is None and len(science_skill_type_ids) == 3:
        # Fallback: if we couldn't infer it by name, keep last as encryption.
        encryption_skill_type_id = int(science_skill_type_ids[-1])
        science_skill_type_ids = [int(x) for x in science_skill_type_ids[:-1]]

    encryption_level = trained_level_by_skill_id.get(int(encryption_skill_type_id), 0) if encryption_skill_type_id else 0
    science_levels = [trained_level_by_skill_id.get(int(tid), 0) for tid in science_skill_type_ids[:2]]
    science_sum = int(sum(science_levels))
    skill_success_multiplier = 1.0 + (float(science_sum) / 30.0) + (float(encryption_level) / 40.0)
    skill_success_multiplier = max(0.0, float(skill_success_multiplier))

    out_bp = next((p for p in products if isinstance(p, dict)), None) or {}
    out_blueprint_type_id = out_bp.get("type_id")
    if out_blueprint_type_id is None:
        raise ServiceError("Invention output blueprint type_id missing.", status_code=400)

    try:
        out_blueprint_type_id_i = int(out_blueprint_type_id)
    except Exception:
        raise ServiceError("Invention output blueprint type_id is invalid.", status_code=400)

    base_out_runs_raw = out_bp.get("quantity")
    try:
        base_out_runs = int(base_out_runs_raw or 0)
    except Exception:
        base_out_runs = 0
    base_out_runs = max(1, base_out_runs)

    # Default invented BPC stats in EVE (common for T2 invention)
    base_out_me = -4
    base_out_te = -4

    # Load T2 blueprint manufacturing data for ROI estimate
    out_bp_data: dict | None = None
    if isinstance(blueprint_data_map, dict):
        out_bp_data = blueprint_data_map.get(int(out_blueprint_type_id_i))
    if not isinstance(out_bp_data, dict):
        out_bp_data_map = get_blueprint_manufacturing_data(sde_session, language, [int(out_blueprint_type_id_i)])
        out_bp_data = out_bp_data_map.get(int(out_blueprint_type_id_i)) if isinstance(out_bp_data_map, dict) else None
    if not isinstance(out_bp_data, dict) or not isinstance(out_bp_data.get("manufacturing"), dict):
        raise ServiceError("Invention output blueprint has no manufacturing activity in SDE.", status_code=400)

    mfg_raw = out_bp_data.get("manufacturing")
    manufacturing: dict[str, Any] = cast(dict[str, Any], mfg_raw) if isinstance(mfg_raw, dict) else {}
    mfg_materials = manufacturing.get("materials") if isinstance(manufacturing.get("materials"), list) else []
    mfg_products = manufacturing.get("products") if isinstance(manufacturing.get("products"), list) else []

    if not isinstance(mfg_products, list) or not mfg_products:
        raise ServiceError("Output blueprint manufacturing products are missing.", status_code=400)

    mfg_prod0 = next((p for p in mfg_products if isinstance(p, dict)), None) or {}
    prod_type_id = mfg_prod0.get("type_id")
    if prod_type_id is None:
        raise ServiceError("Output blueprint manufacturing product type_id missing.", status_code=400)

    try:
        prod_type_id_i = int(prod_type_id)
    except Exception:
        raise ServiceError("Output blueprint manufacturing product type_id invalid.", status_code=400)

    prod_qty_per_run_raw = mfg_prod0.get("quantity")
    try:
        prod_qty_per_run = int(prod_qty_per_run_raw or 0)
    except Exception:
        prod_qty_per_run = 0
    prod_qty_per_run = max(1, prod_qty_per_run)

    # Price map
    if market_price_map is not None and isinstance(market_price_map, dict):
        price_map = market_price_map
    else:
        market_prices = None
        if esi_service is not None:
            try:
                market_prices = esi_service.get_market_prices() or []
            except Exception:
                market_prices = None

        price_map = _market_price_map_from_esi_prices(market_prices if isinstance(market_prices, list) else None)

    # Optional facility + rigs context
    profile_material_reduction = _as_fraction(getattr(industry_profile, "material_efficiency_bonus", None)) if industry_profile is not None else 0.0
    profile_time_reduction = _as_fraction(getattr(industry_profile, "time_efficiency_bonus", None)) if industry_profile is not None else 0.0
    profile_cost_reduction = _as_fraction(getattr(industry_profile, "facility_cost_bonus", None)) if industry_profile is not None else 0.0

    facility_tax = _as_fraction(getattr(industry_profile, "facility_tax", None)) if industry_profile is not None else 0.0
    scc_surcharge = _as_fraction(getattr(industry_profile, "scc_surcharge", None)) if industry_profile is not None else 0.0
    surcharge_rate = max(0.0, float(facility_tax) + float(scc_surcharge))

    rig_group = _infer_rig_group_label_from_products(mfg_products if isinstance(mfg_products, list) else [])

    inv_rig_time_reduction = 0.0
    inv_rig_cost_reduction = 0.0
    mfg_rig_material_reduction = 0.0
    mfg_rig_time_reduction = 0.0
    mfg_rig_cost_reduction = 0.0
    copy_rig_time_reduction = 0.0
    copy_rig_cost_reduction = 0.0
    if rig_payload:
        inv_rig_time_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="invention", group=rig_group, metric="time"
        )
        inv_rig_cost_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="invention", group=rig_group, metric="cost"
        )
        mfg_rig_material_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="manufacturing", group=rig_group, metric="material"
        )
        mfg_rig_time_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="manufacturing", group=rig_group, metric="time"
        )
        mfg_rig_cost_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="manufacturing", group=rig_group, metric="cost"
        )
        copy_rig_time_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="copying", group=rig_group, metric="time"
        )
        copy_rig_cost_reduction = compute_rig_reduction_for(
            rigs_payload=rig_payload, activity="copying", group=rig_group, metric="cost"
        )

    inv_effective_time_reduction = 1.0 - ((1.0 - profile_time_reduction) * (1.0 - inv_rig_time_reduction))
    inv_effective_cost_reduction = 1.0 - ((1.0 - profile_cost_reduction) * (1.0 - inv_rig_cost_reduction))

    mfg_effective_material_reduction = 1.0 - ((1.0 - profile_material_reduction) * (1.0 - mfg_rig_material_reduction))
    mfg_effective_time_reduction = 1.0 - ((1.0 - profile_time_reduction) * (1.0 - mfg_rig_time_reduction))
    mfg_effective_cost_reduction = 1.0 - ((1.0 - profile_cost_reduction) * (1.0 - mfg_rig_cost_reduction))

    copy_effective_time_reduction = 1.0 - ((1.0 - profile_time_reduction) * (1.0 - copy_rig_time_reduction))
    copy_effective_cost_reduction = 1.0 - ((1.0 - profile_cost_reduction) * (1.0 - copy_rig_cost_reduction))

    # Copying skill effect (best-effort):
    # - Science reduces copying time by 5% per level
    # - Advanced Industry reduces all industry job durations by 3% per level
    SCIENCE_SKILL_TYPE_ID = 3402
    science_level = 0
    try:
        science_level = int(trained_level_by_skill_id.get(int(SCIENCE_SKILL_TYPE_ID), 0) or 0)
    except Exception:
        science_level = 0
    science_level = max(0, min(science_level, 5))
    science_mult = max(0.0, min(1.0, 1.0 - (0.05 * float(science_level))))

    # Invention job time reduction: Advanced Industry reduces *all* industry job durations by 3% per level.
    # The in-game client applies this to invention; include it so UI durations match.
    ADVANCED_INDUSTRY_SKILL_TYPE_ID = 3388
    adv_industry_level = 0
    try:
        adv_industry_level = int(trained_level_by_skill_id.get(int(ADVANCED_INDUSTRY_SKILL_TYPE_ID), 0) or 0)
    except Exception:
        adv_industry_level = 0
    adv_industry_level = max(0, min(adv_industry_level, 5))
    inv_skill_time_multiplier = max(0.0, min(1.0, 1.0 - (0.03 * float(adv_industry_level))))

    copy_skill_time_multiplier = max(0.0, min(1.0, float(science_mult) * float(inv_skill_time_multiplier)))

    # Input blueprint copying parameters (used to estimate copy overhead per invention option).
    # `copying_time_seconds` is per-run (seconds/run). Convert to max-runs time here.
    base_copy_time_seconds_per_run = 0.0
    try:
        base_copy_time_seconds_per_run = float(bp.get("copying_time_seconds", 0) or 0.0)
    except Exception:
        base_copy_time_seconds_per_run = 0.0

    max_production_limit = 0
    try:
        max_production_limit = int(bp.get("max_production_limit") or 0)
    except Exception:
        max_production_limit = 0

    base_copy_time_seconds_max_runs = float(base_copy_time_seconds_per_run) * float(max_production_limit)

    input_bp_type_id_for_price = 0
    try:
        input_bp_type_id_for_price = int(bp.get("type_id") or blueprint_type_id)
    except Exception:
        input_bp_type_id_for_price = int(blueprint_type_id)

    input_bp_price = _adjusted_or_avg_price(price_map, type_id=int(input_bp_type_id_for_price))

    # EIV per-run for the blueprint's manufacturing inputs (ME0 quantities, adjusted prices).
    # This is the in-game basis for copying job cost.
    input_bp_eiv_per_run = 0.0
    for m in mfg_materials or []:
        if not isinstance(m, dict):
            continue
        tid = m.get("type_id")
        qty = m.get("quantity")
        if tid is None or qty is None:
            continue
        try:
            tid_i = int(tid)
            qty_i = int(qty)
        except Exception:
            continue
        if tid_i <= 0 or qty_i <= 0:
            continue
        adj = _adjusted_or_avg_price(price_map, type_id=int(tid_i))
        if adj is not None and float(adj) > 0:
            input_bp_eiv_per_run += float(qty_i) * float(adj)

    def _mfg_unit_profit_per_run(
        *,
        me_percent: int,
    ) -> tuple[
        float | None,
        float | None,
        float | None,
        float | None,
        dict[str, Any] | None,
        list[dict[str, Any]],
    ]:
        # Materials
        try:
            me_multiplier = 1.0 - (float(me_percent) * 0.01)
        except Exception:
            me_multiplier = 1.0

        total_mat_cost_per_run = 0.0
        eiv_per_run = 0.0
        any_price = False

        materials_per_run: list[dict[str, Any]] = []

        for m in mfg_materials or []:
            if not isinstance(m, dict):
                continue
            tid = m.get("type_id")
            qty = m.get("quantity")
            if tid is None or qty is None:
                continue
            try:
                tid_i = int(tid)
                qty_i = int(qty)
            except Exception:
                continue
            if tid_i <= 0 or qty_i <= 0:
                continue

            # Keep same rounding behavior as manufacturing calc:
            # - facility+rig material reductions apply multiplicatively
            # - ceil per-run, min 1
            raw = float(qty_i) * float(me_multiplier) * (1.0 - float(mfg_effective_material_reduction))
            adj_qty = max(1, int(math.ceil(max(0.0, raw))))

            materials_per_run.append(
                {
                    "type_id": int(tid_i),
                    "type_name": m.get("type_name"),
                    "group_name": m.get("group_name"),
                    "category_name": m.get("category_name"),
                    "quantity_me0": int(qty_i),
                    "quantity_after_efficiency": int(adj_qty),
                }
            )

            unit_price = _avg_price(price_map, type_id=tid_i)
            if unit_price is not None and unit_price > 0:
                any_price = True
                total_mat_cost_per_run += float(adj_qty) * float(unit_price)

            # EIV uses adjusted price and ME0 quantities
            adj = _adjusted_or_avg_price(price_map, type_id=tid_i)
            if adj is not None and adj > 0:
                eiv_per_run += float(qty_i) * float(adj)

        # Revenue
        prod_unit_price = _avg_price(price_map, type_id=prod_type_id_i)
        if prod_unit_price is None or prod_unit_price <= 0:
            return (None, None, None, None, None, materials_per_run)

        revenue_per_run = float(prod_qty_per_run) * float(prod_unit_price)
        mat_cost_per_run = float(total_mat_cost_per_run) if any_price else None
        if mat_cost_per_run is None:
            return (None, float(revenue_per_run), None, None, None, materials_per_run)

        mfg_fee_per_run = 0.0
        mfg_fee_breakdown: dict[str, Any] | None = None
        if industry_profile is not None:
            ci = float(manufacturing_system_cost_index or 0.0)
            # Manufacturing job fee: client uses JCB = 1% of EIV.
            job_cost_base_fraction = 0.01
            jcb = float(eiv_per_run) * float(job_cost_base_fraction)
            mfg_fee_breakdown = _job_fee(
                estimated_item_value_isk=float(jcb),
                system_cost_index=float(ci),
                effective_cost_reduction_fraction=float(mfg_effective_cost_reduction),
                surcharge_rate_total_fraction=float(surcharge_rate),
            )
            # Preserve EIV context for debugging/UX; `_job_fee` treats `estimated_item_value_isk`
            # as the job-cost basis (JCB) that SCI and surcharges apply to.
            mfg_fee_breakdown["estimated_item_value_total_isk"] = float(eiv_per_run)
            mfg_fee_breakdown["job_cost_base_fraction"] = float(job_cost_base_fraction)
            mfg_fee_breakdown["job_cost_base_total_isk"] = float(jcb)
            mfg_fee_breakdown["estimated_item_value_basis"] = "manufacturing_eiv_per_run (ME0 adjusted prices)"
            mfg_fee_per_run = float(mfg_fee_breakdown.get("total_job_cost_isk") or 0.0)

        profit_per_run = float(revenue_per_run) - float(mat_cost_per_run) - float(mfg_fee_per_run)
        return (
            float(mat_cost_per_run),
            float(revenue_per_run),
            float(profit_per_run),
            float(mfg_fee_per_run),
            mfg_fee_breakdown,
            materials_per_run,
        )

    # Invention attempt cost (materials)
    inv_materials = invention.get("materials") or []

    inv_inventory_on_hand_by_type = inventory_on_hand_by_type if isinstance(inventory_on_hand_by_type, dict) else {}
    inv_inventory_fifo_lots_by_type = inventory_fifo_lots_by_type if isinstance(inventory_fifo_lots_by_type, dict) else {}
    use_fifo = bool(use_fifo_inventory_costing)

    def _effective_cost_breakdown_for_type_qty(*, type_id: int, qty: int) -> dict[str, Any]:
        """Return a breakdown for inventory-aware effective cost of a required input."""

        tid_i = int(type_id)
        qty_i = max(0, int(qty or 0))
        if tid_i <= 0 or qty_i <= 0:
            return {
                "type_id": int(tid_i),
                "required_quantity": int(qty_i),
                "buy_unit_price_isk": None,
                "buy_cost_isk": 0.0,
                "inventory_on_hand_qty": int(inv_inventory_on_hand_by_type.get(int(tid_i), 0) or 0),
                "inventory_used_qty": 0,
                "inventory_fifo_priced_qty": 0,
                "inventory_fifo_cost_isk": 0.0,
                "inventory_cost_isk": None,
                "effective_cost_isk": 0.0,
                "buy_now_qty": 0,
            }

        # Use adjusted price as fallback when average is missing. This matters for some
        # low-volume items where ESI may return average_price = null.
        unit_market = _adjusted_or_avg_price(price_map, type_id=tid_i)
        buy_cost = None
        try:
            if unit_market is not None and float(unit_market) > 0:
                buy_cost = float(unit_market) * float(qty_i)
        except Exception:
            buy_cost = None

        if not use_fifo:
            # Even when FIFO costing is disabled, still reflect *availability* from on-hand inventory
            # so the UI can show take/buy split. Without FIFO lots we value any taken inventory at
            # market average (best-effort).
            on_hand = int(inv_inventory_on_hand_by_type.get(int(tid_i), 0) or 0)
            inv_used = min(int(qty_i), max(0, int(on_hand)))
            buy_now_qty = max(0, int(qty_i) - int(inv_used))

            inv_cost = None
            try:
                if unit_market is not None and float(unit_market) > 0 and int(inv_used) > 0:
                    inv_cost = float(unit_market) * float(inv_used)
            except Exception:
                inv_cost = None

            # Keep overall valuation at market average when FIFO is disabled.
            return {
                "type_id": int(tid_i),
                "required_quantity": int(qty_i),
                "buy_unit_price_isk": float(unit_market) if unit_market is not None else None,
                "buy_cost_isk": float(buy_cost) if buy_cost is not None else None,
                "inventory_on_hand_qty": int(on_hand),
                "inventory_used_qty": int(inv_used),
                "inventory_fifo_priced_qty": 0,
                "inventory_fifo_cost_isk": 0.0,
                "inventory_cost_isk": float(inv_cost) if inv_cost is not None else None,
                "effective_cost_isk": float(buy_cost) if buy_cost is not None else None,
                "buy_now_qty": int(buy_now_qty),
            }

        on_hand = int(inv_inventory_on_hand_by_type.get(int(tid_i), 0) or 0)
        inv_used = min(int(qty_i), max(0, int(on_hand)))
        buy_now_qty = max(0, int(qty_i) - int(inv_used))

        lots = inv_inventory_fifo_lots_by_type.get(int(tid_i)) or []
        bd = fifo_allocate_cost_breakdown(lots=lots, quantity=int(inv_used))
        fifo_cost = float(bd.get("total_cost") or 0.0)
        fifo_priced_qty = int(bd.get("priced_quantity") or 0)
        unknown_inv_qty = max(0, int(inv_used) - int(fifo_priced_qty))

        needs_market = (unknown_inv_qty + buy_now_qty) > 0
        if needs_market and (unit_market is None or float(unit_market) <= 0):
            # If FIFO priced everything, we can still return that value.
            if fifo_priced_qty >= qty_i:
                return {
                    "type_id": int(tid_i),
                    "required_quantity": int(qty_i),
                    "buy_unit_price_isk": None,
                    "buy_cost_isk": None,
                    "inventory_on_hand_qty": int(on_hand),
                    "inventory_used_qty": int(inv_used),
                    "inventory_fifo_priced_qty": int(fifo_priced_qty),
                    "inventory_fifo_cost_isk": float(fifo_cost),
                    "inventory_cost_isk": float(fifo_cost) if int(inv_used) > 0 else None,
                    "effective_cost_isk": float(fifo_cost),
                    "buy_now_qty": int(buy_now_qty),
                }
            return {
                "type_id": int(tid_i),
                "required_quantity": int(qty_i),
                "buy_unit_price_isk": None,
                "buy_cost_isk": None,
                "inventory_on_hand_qty": int(on_hand),
                "inventory_used_qty": int(inv_used),
                "inventory_fifo_priced_qty": int(fifo_priced_qty),
                "inventory_fifo_cost_isk": float(fifo_cost),
                "inventory_cost_isk": float(fifo_cost) if int(inv_used) > 0 else None,
                "effective_cost_isk": None,
                "buy_now_qty": int(buy_now_qty),
            }

        market_cost = float(unknown_inv_qty + buy_now_qty) * float(unit_market or 0.0)
        effective_cost = float(fifo_cost) + float(market_cost)

        inventory_cost_display = None
        if int(inv_used) > 0:
            if int(fifo_priced_qty) > 0:
                inventory_cost_display = float(fifo_cost)
            else:
                inventory_cost_display = float(unit_market or 0.0) * float(inv_used)

        return {
            "type_id": int(tid_i),
            "required_quantity": int(qty_i),
            "buy_unit_price_isk": float(unit_market) if unit_market is not None else None,
            "buy_cost_isk": float(buy_cost) if buy_cost is not None else None,
            "inventory_on_hand_qty": int(on_hand),
            "inventory_used_qty": int(inv_used),
            "inventory_fifo_priced_qty": int(fifo_priced_qty),
            "inventory_fifo_cost_isk": float(fifo_cost),
            "inventory_cost_isk": float(inventory_cost_display) if inventory_cost_display is not None else None,
            "effective_cost_isk": float(effective_cost),
            "buy_now_qty": int(buy_now_qty),
        }

    def _effective_cost_for_type_qty(*, type_id: int, qty: int) -> float | None:
        """Inventory-aware valuation for a required input.

        Matches the manufacturing materials logic:
        - use on-hand inventory first
        - FIFO-price what we can via reconstructed lots
        - price any unknown-basis inventory + remaining shortfall at market average

        This is valuation only; it does not reserve inventory.
        """

        bd = _effective_cost_breakdown_for_type_qty(type_id=int(type_id), qty=int(qty))
        eff = bd.get("effective_cost_isk")
        try:
            return float(eff) if eff is not None else None
        except Exception:
            return None

    # Precompute an inventory-aware breakdown for the required invention materials (datacores, etc.).
    # This is decryptor-independent, so we compute it once for UI display.
    invention_materials_breakdown: list[dict[str, Any]] = []
    for m in inv_materials or []:
        if not isinstance(m, dict):
            continue
        tid = m.get("type_id")
        qty = m.get("quantity")
        if tid is None or qty is None:
            continue
        try:
            tid_i = int(tid)
            qty_i = int(qty)
        except Exception:
            continue
        if tid_i <= 0 or qty_i <= 0:
            continue

        bd = _effective_cost_breakdown_for_type_qty(type_id=int(tid_i), qty=int(qty_i))

        eff = bd.get("effective_cost_isk")
        eff_unit = None
        try:
            if eff is not None and qty_i > 0:
                eff_unit = float(eff) / float(qty_i)
        except Exception:
            eff_unit = None

        invention_materials_breakdown.append(
            {
                "type_id": int(tid_i),
                "type_name": m.get("type_name"),
                "group_name": m.get("group_name"),
                "category_name": m.get("category_name"),
                "required_quantity": int(qty_i),
                "effective_unit_price_isk": float(eff_unit) if eff_unit is not None else None,
                **bd,
            }
        )

    def _invention_attempt_cost(
        *,
        decryptor_type_id: int | None,
    ) -> tuple[float | None, float | None, float | None, dict[str, Any] | None]:
        material_cost = 0.0
        eiv = 0.0
        any_cost = False

        for m in inv_materials or []:
            if not isinstance(m, dict):
                continue
            tid = m.get("type_id")
            qty = m.get("quantity")
            if tid is None or qty is None:
                continue
            try:
                tid_i = int(tid)
                qty_i = int(qty)
            except Exception:
                continue
            if tid_i <= 0 or qty_i <= 0:
                continue

            eff = _effective_cost_for_type_qty(type_id=int(tid_i), qty=int(qty_i))
            if eff is not None:
                any_cost = True
                material_cost += float(eff)

            adj = _adjusted_or_avg_price(price_map, type_id=tid_i)
            if adj is not None and adj > 0:
                eiv += float(qty_i) * float(adj)

        if decryptor_type_id is not None and int(decryptor_type_id) > 0:
            eff = _effective_cost_for_type_qty(type_id=int(decryptor_type_id), qty=1)
            if eff is not None:
                any_cost = True
                material_cost += float(eff)

            adj = _adjusted_or_avg_price(price_map, type_id=int(decryptor_type_id))
            if adj is not None and adj > 0:
                eiv += float(adj) * 1.0

        material_cost_out = float(material_cost) if any_cost else None

        inv_fee = 0.0
        inv_fee_breakdown: dict[str, Any] | None = None
        if industry_profile is not None:
            ci = float(invention_system_cost_index or 0.0)
            inv_fee_breakdown = _job_fee(
                estimated_item_value_isk=float(eiv),
                system_cost_index=float(ci),
                effective_cost_reduction_fraction=float(inv_effective_cost_reduction),
                surcharge_rate_total_fraction=float(surcharge_rate),
            )
            inv_fee = float(inv_fee_breakdown.get("total_job_cost_isk") or 0.0)

        total = None
        if material_cost_out is not None:
            total = float(material_cost_out) + float(inv_fee)

        return material_cost_out, float(inv_fee), total, inv_fee_breakdown

    decryptors = get_t2_invention_decryptors(sde_session, language=language)

    options: list[dict[str, Any]] = []

    # Include explicit no-decryptor option
    options.append(
        {
            "decryptor_type_id": None,
            "decryptor_type_name": "No Decryptor",
            "invention_probability_multiplier": 1.0,
            "invention_me_modifier": 0,
            "invention_te_modifier": 0,
            "invention_max_run_modifier": 0,
        }
    )

    for d in decryptors or []:
        if not isinstance(d, dict):
            continue
        d_tid = d.get("type_id")
        if d_tid is None:
            continue
        try:
            d_tid_i = int(d_tid)
        except Exception:
            continue
        options.append(
            {
                "decryptor_type_id": int(d_tid_i),
                "decryptor_type_name": d.get("type_name"),
                "invention_probability_multiplier": float(d.get("invention_probability_multiplier") or 1.0),
                "invention_me_modifier": int(d.get("invention_me_modifier") or 0),
                "invention_te_modifier": int(d.get("invention_te_modifier") or 0),
                "invention_max_run_modifier": int(d.get("invention_max_run_modifier") or 0),
            }
        )

    for opt in options:
        dec_tid = opt.get("decryptor_type_id")
        dec_tid_i = int(dec_tid) if isinstance(dec_tid, int) else None

        # Decryptor cost breakdown for UI (Build Tree columns).
        if dec_tid_i is not None and int(dec_tid_i) > 0:
            dec_bd = _effective_cost_breakdown_for_type_qty(type_id=int(dec_tid_i), qty=1)
            opt.update(
                {
                    "decryptor_buy_unit_price_isk": dec_bd.get("buy_unit_price_isk"),
                    "decryptor_buy_cost_isk": dec_bd.get("buy_cost_isk"),
                    "decryptor_effective_cost_isk": dec_bd.get("effective_cost_isk"),
                    "decryptor_inventory_on_hand_qty": dec_bd.get("inventory_on_hand_qty"),
                    "decryptor_inventory_used_qty": dec_bd.get("inventory_used_qty"),
                    "decryptor_inventory_fifo_priced_qty": dec_bd.get("inventory_fifo_priced_qty"),
                    "decryptor_inventory_fifo_cost_isk": dec_bd.get("inventory_fifo_cost_isk"),
                    "decryptor_inventory_cost_isk": dec_bd.get("inventory_cost_isk"),
                }
            )
        else:
            opt.update(
                {
                    "decryptor_buy_unit_price_isk": None,
                    "decryptor_buy_cost_isk": None,
                    "decryptor_effective_cost_isk": 0.0,
                    "decryptor_inventory_used_qty": 0,
                    "decryptor_inventory_fifo_priced_qty": 0,
                    "decryptor_inventory_fifo_cost_isk": 0.0,
                    "decryptor_inventory_cost_isk": None,
                }
            )

        prob_mult = float(opt.get("invention_probability_multiplier") or 1.0)
        me_mod = int(opt.get("invention_me_modifier") or 0)
        te_mod = int(opt.get("invention_te_modifier") or 0)
        run_mod = int(opt.get("invention_max_run_modifier") or 0)

        success_prob_before_decryptor = float(base_prob_f) * float(skill_success_multiplier)
        success_prob = float(success_prob_before_decryptor) * float(prob_mult)
        success_prob = max(0.0, min(success_prob, 1.0))

        attempts_per_success = (1.0 / float(success_prob)) if success_prob > 0 else None

        invented_runs = max(1, int(base_out_runs) + int(run_mod))
        invented_me = int(base_out_me) + int(me_mod)
        invented_te = int(base_out_te) + int(te_mod)

        attempt_material_cost, inv_job_fee, attempt_total_cost, inv_fee_breakdown = _invention_attempt_cost(
            decryptor_type_id=dec_tid_i
        )

        (
            mat_cost_per_run,
            revenue_per_run,
            profit_per_run,
            mfg_job_fee,
            mfg_fee_breakdown,
            mfg_materials_per_run,
        ) = _mfg_unit_profit_per_run(
            me_percent=invented_me
        )

        items_per_attempt = float(invented_runs) * float(prod_qty_per_run)
        expected_successful_items = float(success_prob) * float(items_per_attempt)

        invention_cost_per_item = None
        if attempt_total_cost is not None and expected_successful_items > 0:
            invention_cost_per_item = float(attempt_total_cost) / float(expected_successful_items)

        net_profit_per_run = None
        if profit_per_run is not None and invention_cost_per_item is not None:
            # per-run produces prod_qty_per_run items
            inv_cost_per_run = float(invention_cost_per_item) * float(prod_qty_per_run)
            net_profit_per_run = float(profit_per_run) - float(inv_cost_per_run)

        roi_pct = None
        if net_profit_per_run is not None and mat_cost_per_run is not None and invention_cost_per_item is not None:
            inv_cost_per_run = float(invention_cost_per_item) * float(prod_qty_per_run)
            denom = float(mat_cost_per_run) + float(inv_cost_per_run)
            if denom > 0:
                roi_pct = (float(net_profit_per_run) / float(denom)) * 100.0

        # Copying overhead for invention (best-effort): estimate the blueprint copy job
        # required to produce enough input-BPC runs for the expected invention attempts.
        copying_expected_runs = float(attempts_per_success) if attempts_per_success is not None else None
        copying_run_ratio = None
        copying_expected_time_seconds = None
        copying_job_fee = None
        copying_fee_breakdown = None

        if (
            copying_expected_runs is not None
            and copying_expected_runs > 0
            and base_copy_time_seconds_max_runs > 0
            and max_production_limit > 0
        ):
            copying_run_ratio = max(0.0, min(1.0, float(copying_expected_runs) / float(max_production_limit)))
            base_copy_time_for_runs = float(base_copy_time_seconds_max_runs) * float(copying_run_ratio)
            # Apply profile/rig time reduction only when a profile is provided.
            time_red = float(copy_effective_time_reduction) if industry_profile is not None else 0.0
            copying_expected_time_seconds = max(
                0.0,
                float(base_copy_time_for_runs)
                * (1.0 - float(time_red))
                * float(copy_skill_time_multiplier),
            )

            if industry_profile is not None and copying_system_cost_index is not None:
                # Client-like copying job fee: JCB = 2% of EIV (ME0 adjusted materials), scaled by runs.
                copy_eiv_total = float(input_bp_eiv_per_run) * float(copying_expected_runs)
                copy_job_cost_base_fraction = 0.02
                copy_jcb = float(copy_eiv_total) * float(copy_job_cost_base_fraction)

                if copy_jcb > 0:
                    bd = _job_fee(
                        estimated_item_value_isk=float(copy_jcb),
                        system_cost_index=float(copying_system_cost_index or 0.0),
                        effective_cost_reduction_fraction=float(copy_effective_cost_reduction),
                        surcharge_rate_total_fraction=float(surcharge_rate),
                    )
                    bd["job_cost_base_fraction"] = float(copy_job_cost_base_fraction)
                    bd["estimated_item_value_total_isk"] = float(copy_eiv_total)
                    bd["job_cost_base_total_isk"] = float(copy_jcb)
                    bd["estimated_item_value_basis"] = "manufacturing_eiv_per_run * runs (ME0 adjusted prices)"
                    copying_fee_breakdown = bd
                    try:
                        copying_job_fee = float(bd.get("total_job_cost_isk") or 0.0)
                    except Exception:
                        copying_job_fee = None

        opt.update(
            {
                "base_invention_probability": float(base_prob_f),
                "skill_success_multiplier": float(skill_success_multiplier),
                "success_probability_before_decryptor": float(success_prob_before_decryptor),
                "success_probability": float(success_prob),
                "invented_blueprint_type_id": int(out_blueprint_type_id_i),
                "invented_runs": int(invented_runs),
                "invented_me": int(invented_me),
                "invented_te": int(invented_te),
                # Backwards-compatible field name: now includes job fee if profile context is provided.
                "invention_attempt_cost_isk": float(attempt_total_cost) if attempt_total_cost is not None else None,
                "invention_attempt_material_cost_isk": (
                    float(attempt_material_cost) if attempt_material_cost is not None else None
                ),
                "invention_job_fee_isk": float(inv_job_fee) if inv_job_fee is not None else None,
                "invention_job_fee_breakdown": inv_fee_breakdown,
                "expected_invention_cost_per_item_isk": float(invention_cost_per_item)
                if invention_cost_per_item is not None
                else None,
                "manufacturing_material_cost_per_run_isk": float(mat_cost_per_run) if mat_cost_per_run is not None else None,
                "manufacturing_materials_per_run": mfg_materials_per_run,
                "manufacturing_revenue_per_run_isk": float(revenue_per_run) if revenue_per_run is not None else None,
                "manufacturing_profit_per_run_isk": float(profit_per_run) if profit_per_run is not None else None,
                "manufacturing_job_fee_per_run_isk": float(mfg_job_fee) if mfg_job_fee is not None else None,
                "manufacturing_job_fee_breakdown": mfg_fee_breakdown,
                "net_profit_per_run_after_invention_isk": float(net_profit_per_run) if net_profit_per_run is not None else None,
                "roi_percent": float(roi_pct) if roi_pct is not None else None,

                # Copying overhead estimate for this option (based on expected attempts).
                "copying_expected_runs": float(copying_expected_runs) if copying_expected_runs is not None else None,
                "copying_run_ratio": float(copying_run_ratio) if copying_run_ratio is not None else None,
                "copying_expected_time_seconds": float(copying_expected_time_seconds)
                if copying_expected_time_seconds is not None
                else None,
                "copying_job_fee_isk": float(copying_job_fee) if copying_job_fee is not None else None,
                "copying_job_fee_breakdown": copying_fee_breakdown,
            }
        )

    # Rank best ROI first (fallback to net profit)
    def _sort_key(r: dict[str, Any]) -> tuple[float, float]:
        roi = r.get("roi_percent")
        net = r.get("net_profit_per_run_after_invention_isk")
        try:
            roi_f = float(roi) if roi is not None else float("-inf")
        except Exception:
            roi_f = float("-inf")
        try:
            net_f = float(net) if net is not None else float("-inf")
        except Exception:
            net_f = float("-inf")
        return (roi_f, net_f)

    options_sorted = sorted(options, key=_sort_key, reverse=True)

    data: dict[str, Any] = {
        "input_blueprint": {
            "type_id": int(bp.get("type_id") or blueprint_type_id),
            "type_name": bp.get("type_name"),
        },
        "copying": {
            "time_seconds_max_runs": float(base_copy_time_seconds_max_runs) if base_copy_time_seconds_max_runs > 0 else None,
            "max_production_limit": int(max_production_limit) if max_production_limit > 0 else None,
            "facility_context": {
                "system_cost_index": float(copying_system_cost_index or 0.0) if copying_system_cost_index is not None else None,
                "surcharge_rate_total_fraction": float(surcharge_rate),
                "facility_tax_fraction": float(facility_tax),
                "scc_surcharge_fraction": float(scc_surcharge),
                "profile_time_reduction_fraction": float(profile_time_reduction),
                "profile_cost_reduction_fraction": float(profile_cost_reduction),
                "rig_group_label": str(rig_group),
                "rig_time_reduction_fraction": float(copy_rig_time_reduction),
                "rig_cost_reduction_fraction": float(copy_rig_cost_reduction),
                "effective_time_reduction_fraction": float(copy_effective_time_reduction),
                "effective_cost_reduction_fraction": float(copy_effective_cost_reduction),
                "skill_time_multiplier": float(copy_skill_time_multiplier),
                "skill_time_reduction_fraction": float(1.0 - copy_skill_time_multiplier),
                "blueprint_price_isk": float(input_bp_price) if input_bp_price is not None else None,
            },
        },
        "invention": {
            "time_seconds": invention.get("time"),
            "probability": float(base_prob_f),
            "materials": inv_materials,
            "materials_breakdown": invention_materials_breakdown,
            "products": products,
            "required_skills": invention.get("skills") or [],
            "character_skill_context": {
                "skill_success_multiplier": float(skill_success_multiplier),
                "encryption_skill_type_id": int(encryption_skill_type_id) if encryption_skill_type_id else None,
                "encryption_skill_level": int(encryption_level),
                "science_skill_type_ids": [int(x) for x in science_skill_type_ids[:2]],
                "science_skill_levels": [int(x) for x in science_levels[:2]],
            },
            "facility_context": {
                "system_cost_index": float(invention_system_cost_index or 0.0),
                "surcharge_rate_total_fraction": float(surcharge_rate),
                "facility_tax_fraction": float(facility_tax),
                "scc_surcharge_fraction": float(scc_surcharge),
                "profile_time_reduction_fraction": float(profile_time_reduction),
                "profile_cost_reduction_fraction": float(profile_cost_reduction),
                "rig_group_label": str(rig_group),
                "rig_time_reduction_fraction": float(inv_rig_time_reduction),
                "rig_cost_reduction_fraction": float(inv_rig_cost_reduction),
                "effective_time_reduction_fraction": float(inv_effective_time_reduction),
                "effective_cost_reduction_fraction": float(inv_effective_cost_reduction),
                "skill_time_multiplier": float(inv_skill_time_multiplier),
                "skill_time_reduction_fraction": float(1.0 - inv_skill_time_multiplier),
                "estimated_time_seconds": (
                    float(invention.get("time") or 0.0)
                    * (1.0 - float(inv_effective_time_reduction))
                    * float(inv_skill_time_multiplier)
                    if (invention.get("time") is not None)
                    else None
                ),
            },
            "base_output": {
                "blueprint_type_id": int(out_blueprint_type_id_i),
                "blueprint_type_name": str(out_bp_data.get("type_name") or "") or None,
                "runs": int(base_out_runs),
                "me": int(base_out_me),
                "te": int(base_out_te),
            },
        },
        "manufacturing": {
            "product_type_id": int(prod_type_id_i),
            "product_type_name": mfg_prod0.get("type_name"),
            "product_group_name": mfg_prod0.get("group_name"),
            "product_category_name": mfg_prod0.get("category_name"),
            "product_quantity_per_run": int(prod_qty_per_run),
            "time_seconds": manufacturing.get("time"),
            "facility_context": {
                "system_cost_index": float(manufacturing_system_cost_index or 0.0),
                "surcharge_rate_total_fraction": float(surcharge_rate),
                "facility_tax_fraction": float(facility_tax),
                "scc_surcharge_fraction": float(scc_surcharge),
                "profile_material_reduction_fraction": float(profile_material_reduction),
                "profile_time_reduction_fraction": float(profile_time_reduction),
                "profile_cost_reduction_fraction": float(profile_cost_reduction),
                "rig_group_label": str(rig_group),
                "rig_material_reduction_fraction": float(mfg_rig_material_reduction),
                "rig_time_reduction_fraction": float(mfg_rig_time_reduction),
                "rig_cost_reduction_fraction": float(mfg_rig_cost_reduction),
                "effective_material_reduction_fraction": float(mfg_effective_material_reduction),
                "effective_time_reduction_fraction": float(mfg_effective_time_reduction),
                "effective_cost_reduction_fraction": float(mfg_effective_cost_reduction),
                "estimated_time_seconds_per_run": (
                    float(manufacturing.get("time") or 0.0) * (1.0 - float(mfg_effective_time_reduction))
                    if (industry_profile is not None and manufacturing.get("time") is not None)
                    else None
                ),
            },
        },
        "options": options_sorted,
    }

    meta = {
        "pricing": "esi_average_or_adjusted",
        "notes": "ROI model includes facility/rig/job-fee effects when a profile is provided. Copying overhead is estimated per option based on expected attempts.",
    }

    return data, meta
