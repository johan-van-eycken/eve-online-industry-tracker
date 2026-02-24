from __future__ import annotations

import math
from typing import Any


_PATH_SEP = "|||"


def format_duration(seconds: float | int | None) -> str:
    try:
        s = int(round(float(seconds or 0.0)))
    except Exception:
        s = 0
    if s < 0:
        s = 0

    day_s = 24 * 3600

    days = s // day_s
    s = s % day_s
    hours = s // 3600
    s = s % 3600
    minutes = s // 60
    secs = s % 60

    parts: list[str] = []
    if days:
        parts.append(f"{days}D")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def type_icon_url(type_id: Any, *, size: int = 32) -> str | None:
    try:
        tid = int(type_id)
    except Exception:
        return None
    if tid <= 0:
        return None
    return f"https://images.evetech.net/types/{tid}/icon?size={int(size)}"


def blueprint_image_url(blueprint_type_id: Any, *, is_bpc: bool, size: int = 32) -> str | None:
    try:
        tid = int(blueprint_type_id)
    except Exception:
        return None
    if tid <= 0:
        return None
    variation = "bpc" if bool(is_bpc) else "bp"
    return f"https://images.evetech.net/types/{tid}/{variation}?size={int(size)}"


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _node_name(n: dict) -> str:
    return str(n.get("type_name") or n.get("type_id") or "")


def _node_key(n: dict) -> str:
    tid = _safe_int(n.get("type_id"), default=0)
    nm = _node_name(n)
    return f"{nm}#{tid}" if tid else nm


def _choose_buy_vs_build(*, buy_cost_total: Any, build_cost_total: Any) -> str | None:
    buy_n = _safe_float(buy_cost_total)
    build_n = _safe_float(build_cost_total)

    if buy_n is None and build_n is None:
        return None
    if build_n is None:
        return "buy"
    if buy_n is None:
        return "build"
    return "buy" if float(buy_n) <= float(build_n) else "build"


def _inventory_cost_display(*, n: dict, market_unit: Any) -> float | None:
    inv_used_qty_i = _safe_int(n.get("inventory_used_qty"), default=0)
    if inv_used_qty_i <= 0:
        return None

    inv_fifo_cost = _safe_float(n.get("inventory_fifo_cost_isk"))
    inv_fifo_priced_qty_i = _safe_int(n.get("inventory_fifo_priced_qty"), default=0)

    try:
        mu = float(market_unit) if market_unit is not None else None
    except Exception:
        mu = None

    if inv_fifo_cost is not None and inv_fifo_priced_qty_i > 0:
        total = float(inv_fifo_cost)
        unknown_qty = max(0, int(inv_used_qty_i) - int(inv_fifo_priced_qty_i))
        if unknown_qty > 0 and mu is not None:
            total += float(mu) * float(unknown_qty)
        return float(total)

    if mu is not None:
        return float(mu) * float(inv_used_qty_i)

    return None


def _synth_plan_rows_from_required_materials(required_materials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rm in required_materials or []:
        if not isinstance(rm, dict):
            continue
        tid = _safe_int(rm.get("type_id"), default=0)
        if tid <= 0:
            continue
        qty = rm.get("quantity_after_efficiency")
        if qty is None:
            qty = rm.get("quantity_me0")
        qty_i = _safe_int(qty, default=0)
        if qty_i <= 0:
            continue

        unit = rm.get("effective_unit_cost_isk")
        if unit is None:
            unit = rm.get("unit_price_isk")
        total = rm.get("effective_total_cost_isk")
        if total is None:
            total = rm.get("inventory_effective_total_cost_isk")
        if total is None:
            total = rm.get("total_cost_isk")

        rows.append(
            {
                "type_id": int(tid),
                "type_name": rm.get("type_name") or str(tid),
                "recommendation": "buy",
                "required_quantity": int(qty_i),
                "children": [],
                "effective_cost_isk": _safe_float(total),
                "buy_unit_price_isk": _safe_float(unit),
                "inventory_used_qty": rm.get("inventory_used_qty"),
                "inventory_fifo_priced_qty": rm.get("inventory_fifo_priced_qty"),
                "inventory_fifo_cost_isk": rm.get("inventory_fifo_total_cost_isk"),
                "shortfall_quantity": rm.get("inventory_buy_now_qty"),
            }
        )

    return rows


def map_invention_materials_breakdown_to_rows(inv_mats_bd: Any, *, attempts_scale: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        attempts_scale_f = float(attempts_scale)
    except Exception:
        attempts_scale_f = 1.0
    if attempts_scale_f <= 0:
        attempts_scale_f = 1.0

    job_runs = max(1, int(math.ceil(float(attempts_scale_f))))

    if not isinstance(inv_mats_bd, list):
        return rows

    for r in inv_mats_bd:
        if not isinstance(r, dict):
            continue

        type_id = _safe_int(r.get("type_id"), default=0)
        req = _safe_int(r.get("required_quantity"), default=0)
        on_hand = _safe_int(r.get("inventory_on_hand_qty"), default=0)

        total_required_qty = int(req) * int(job_runs)
        inv_used_total = min(int(total_required_qty), int(on_hand))
        buy_now_total = max(0, int(total_required_qty) - int(inv_used_total))

        if inv_used_total > 0 and buy_now_total == 0:
            action = "take"
        elif inv_used_total > 0 and buy_now_total > 0:
            action = "take+buy"
        else:
            action = "buy"

        eff_cost = _safe_float(r.get("effective_cost_isk"))
        inv_cost = _safe_float(r.get("inventory_cost_isk"))
        buy_cost = _safe_float(r.get("buy_cost_isk"))
        eff_unit = _safe_float(r.get("effective_unit_price_isk"))

        rows.append(
            {
                "type_id": int(type_id),
                "Material": str(r.get("type_name") or r.get("type_id") or ""),
                "Job Runs": None,
                "Qty": float(int(req) * int(job_runs)),
                "Action": str(action),
                "Effective Cost": (float(eff_cost) * float(job_runs)) if eff_cost is not None else None,
                "Effective / unit": float(eff_unit) if eff_unit is not None else None,
                "Inventory Cost": (float(inv_cost) * float(job_runs)) if inv_cost is not None else None,
                "Buy Cost": (float(buy_cost) * float(job_runs)) if buy_cost is not None else None,
            }
        )

    return rows


def compute_ui_copy_invention_jobs_rows_for_best_option(
    *,
    inv_data: dict[str, Any] | None,
    best_option: dict[str, Any] | None,
    output_blueprint_type_id: int,
    output_blueprint_type_name: str,
) -> list[dict[str, Any]]:
    """Build the Streamlit TreeData rows for the 'Copy & Invention Jobs' table.

    Returns a list of dicts with: path, Icon, Action, Job Runs, Job Fee, Qty, Effective Cost,
    Duration, Effective / unit, Inventory Cost, Buy Cost.
    """

    if not isinstance(inv_data, dict) or not isinstance(best_option, dict):
        return []

    best = best_option

    inv_sec = inv_data.get("invention") if isinstance(inv_data.get("invention"), dict) else {}
    inv_fac = inv_sec.get("facility_context") if isinstance(inv_sec.get("facility_context"), dict) else {}

    p = _safe_float(best.get("success_probability"))
    attempts_per_success = (1.0 / float(p)) if (p is not None and p > 0) else None

    attempts_jobs: int | None = None
    try:
        if attempts_per_success is not None and float(attempts_per_success) > 0:
            attempts_jobs = int(math.ceil(float(attempts_per_success)))
    except Exception:
        attempts_jobs = None

    attempts_scale = float(attempts_jobs) if attempts_jobs is not None else 1.0

    inv_time_attempt = _safe_float(inv_fac.get("estimated_time_seconds"))
    if inv_time_attempt is None:
        inv_time_attempt = _safe_float(inv_sec.get("time_seconds"))

    inv_attempt_fee = _safe_float(best.get("invention_job_fee_isk"))
    inv_fee_total = (float(inv_attempt_fee) * float(attempts_scale)) if inv_attempt_fee is not None else None
    inv_time_total = (float(inv_time_attempt) * float(attempts_scale)) if inv_time_attempt is not None else None

    copy_runs_exp = _safe_float(best.get("copying_expected_runs"))
    if copy_runs_exp is None and attempts_jobs is not None:
        copy_runs_exp = float(attempts_jobs)
    try:
        if copy_runs_exp is not None and float(copy_runs_exp) > 0:
            copy_runs_exp = float(int(math.ceil(float(copy_runs_exp))))
    except Exception:
        pass

    copy_fee_total = _safe_float(best.get("copying_job_fee_isk"))
    copy_time_total = _safe_float(best.get("copying_expected_time_seconds"))
    if copy_time_total is None:
        copying_ctx = inv_data.get("copying") if isinstance(inv_data.get("copying"), dict) else None
        if isinstance(copying_ctx, dict):
            copy_time_max = _safe_float(copying_ctx.get("time_seconds_max_runs"))
            copy_max_runs = _safe_int(copying_ctx.get("max_production_limit"), default=0)
            fc = copying_ctx.get("facility_context") if isinstance(copying_ctx.get("facility_context"), dict) else {}
            time_red = _safe_float(fc.get("effective_time_reduction_fraction"))
            skill_mult = _safe_float(fc.get("skill_time_multiplier"))
            try:
                if attempts_scale is not None and copy_time_max is not None and copy_max_runs > 0:
                    run_ratio = max(0.0, min(1.0, float(attempts_scale) / float(copy_max_runs)))
                    copy_time_total = float(copy_time_max) * float(run_ratio)
                    if time_red is not None:
                        copy_time_total *= (1.0 - float(time_red))
                    if skill_mult is not None:
                        copy_time_total *= float(skill_mult)
            except Exception:
                pass

    inv_mats_bd = inv_sec.get("materials_breakdown") if isinstance(inv_sec, dict) else None
    invention_material_rows = map_invention_materials_breakdown_to_rows(inv_mats_bd, attempts_scale=float(attempts_scale))

    decryptor = str(best.get("decryptor_type_name") or "(none)")
    decryptor_type_id = _safe_int(best.get("decryptor_type_id"), default=0)
    decryptor_unit_cost = _safe_float(best.get("decryptor_effective_cost_isk"))

    decryptor_row: dict[str, Any] | None = None
    dec_name_norm = decryptor.strip().lower()
    is_no_decryptor = (decryptor_type_id <= 0) or (dec_name_norm in {"(none)", "none", "no decryptor", "(no decryptor)"})

    if is_no_decryptor:
        try:
            p_txt = f"{float(p) * 100.0:.2f}%" if p is not None else "-%"
        except Exception:
            p_txt = "-%"
        decryptor_row = {
            "Icon": None,
            "Material": f"No Decryptor - {p_txt}",
            "Job Runs": None,
            "Qty": None,
            "Action": "--",
            "Effective Cost": None,
            "Effective / unit": None,
            "Inventory Cost": None,
            "Buy Cost": None,
        }
    elif decryptor and decryptor != "(none)":
        dec_inv_cost_unit = _safe_float(best.get("decryptor_inventory_cost_isk"))
        dec_buy_cost_unit = _safe_float(best.get("decryptor_buy_cost_isk"))
        dec_inv_on_hand = _safe_int(best.get("decryptor_inventory_on_hand_qty"), default=0)
        dec_inv_used_qty = _safe_int(best.get("decryptor_inventory_used_qty"), default=0)

        try:
            p_txt = f"{float(p) * 100.0:.2f}%" if p is not None else "-%"
        except Exception:
            p_txt = "-%"

        try:
            target_jobs = int(math.ceil(float(attempts_scale or 0.0)))
        except Exception:
            target_jobs = 0

        if dec_inv_on_hand >= target_jobs and target_jobs > 0:
            dec_action = "take"
        elif dec_inv_on_hand > 0:
            dec_action = "take+buy"
        elif dec_inv_used_qty > 0:
            dec_action = "take"
        else:
            dec_action = "buy"

        decryptor_row = {
            "Icon": type_icon_url(int(decryptor_type_id), size=32) if int(decryptor_type_id) > 0 else None,
            "Material": f"{decryptor} - {p_txt}",
            "Job Runs": None,
            "Qty": float(attempts_jobs) if attempts_jobs is not None else float(target_jobs),
            "Action": str(dec_action),
            "Effective Cost": (float(decryptor_unit_cost) * float(attempts_scale)) if decryptor_unit_cost is not None else None,
            "Effective / unit": float(decryptor_unit_cost) if decryptor_unit_cost is not None else None,
            "Inventory Cost": (float(dec_inv_cost_unit) * float(attempts_scale)) if dec_inv_cost_unit is not None else None,
            "Buy Cost": (
                (float(dec_buy_cost_unit) * float(attempts_scale))
                if dec_buy_cost_unit is not None
                else ((float(decryptor_unit_cost) * float(attempts_scale)) if decryptor_unit_cost is not None else None)
            ),
        }

    inv_materials_total_effective_cost = 0.0
    inv_materials_total_has_any = False
    for r in invention_material_rows:
        v = r.get("Effective Cost")
        try:
            if v is not None:
                inv_materials_total_effective_cost += float(v)
                inv_materials_total_has_any = True
        except Exception:
            pass
    if isinstance(decryptor_row, dict):
        try:
            v = decryptor_row.get("Effective Cost")
            if v is not None:
                inv_materials_total_effective_cost += float(v)
                inv_materials_total_has_any = True
        except Exception:
            pass
    inv_mat_total = float(inv_materials_total_effective_cost) if inv_materials_total_has_any else None

    total_time = None
    try:
        total_time = float(inv_time_total or 0.0) + float(copy_time_total or 0.0)
    except Exception:
        total_time = None

    inv_input = inv_data.get("input_blueprint") if isinstance(inv_data.get("input_blueprint"), dict) else None
    inv_input_bp_name = str((inv_input or {}).get("type_name") or "") or None
    produced_bpc_name = f"{inv_input_bp_name} (BPC)" if inv_input_bp_name else "(BPC)"
    outcome_bpc_name = f"{output_blueprint_type_name} (BPC)" if output_blueprint_type_name else "(BPC)"

    inv_input_bp_type_id = _safe_int((inv_input or {}).get("type_id"), default=0)
    icon_copy = blueprint_image_url(int(inv_input_bp_type_id), is_bpc=True, size=32) if int(inv_input_bp_type_id) > 0 else None
    icon_invention = blueprint_image_url(int(output_blueprint_type_id), is_bpc=True, size=32) if int(output_blueprint_type_id) > 0 else None

    copy_label = f"{produced_bpc_name} (Copying)"
    invention_label = f"{outcome_bpc_name} (Invention)"

    rows: list[dict[str, Any]] = []

    def _add(path_parts: list[str], row: dict[str, Any]) -> None:
        rows.append({"path": _PATH_SEP.join([p for p in path_parts if p]), **row})

    _add(
        [copy_label],
        {
            "Icon": icon_copy,
            "Action": None,
            "Job Runs": float(copy_runs_exp) if copy_runs_exp is not None else None,
            "Job Fee": float(copy_fee_total) if copy_fee_total is not None else None,
            "Qty": None,
            "Effective Cost": None,
            "Duration": float(copy_time_total) if copy_time_total is not None else None,
            "Effective / unit": None,
            "Inventory Cost": None,
            "Buy Cost": None,
        },
    )
    _add(
        [invention_label],
        {
            "Icon": icon_invention,
            "Action": "Make",
            "Job Runs": float(attempts_jobs) if attempts_jobs is not None else None,
            "Job Fee": float(inv_fee_total) if inv_fee_total is not None else None,
            "Qty": None,
            "Effective Cost": float(inv_mat_total) if inv_mat_total is not None else None,
            "Duration": float(inv_time_total) if inv_time_total is not None else None,
            "Effective / unit": None,
            "Inventory Cost": None,
            "Buy Cost": None,
        },
    )

    if isinstance(decryptor_row, dict):
        _add(
            [invention_label, str(decryptor_row.get("Material") or "Decryptor")],
            {
                "Job Fee": None,
                "Duration": None,
                **decryptor_row,
            },
        )

    for r in invention_material_rows:
        _add(
            [invention_label, str(r.get("Material") or "")],
            {
                "Icon": type_icon_url(int(r.get("type_id") or 0), size=32),
                "Job Fee": None,
                "Duration": None,
                **{k: v for k, v in r.items() if k != "type_id"},
            },
        )

    _add(
        ["Total"],
        {
            "Icon": None,
            "Action": None,
            "Job Runs": None,
            "Job Fee": (float(inv_fee_total or 0.0) + float(copy_fee_total or 0.0)) if (inv_fee_total is not None or copy_fee_total is not None) else None,
            "Qty": None,
            "Effective Cost": float(inv_mat_total) if inv_mat_total is not None else None,
            "Duration": float(total_time) if total_time is not None else None,
            "Effective / unit": None,
            "Inventory Cost": None,
            "Buy Cost": None,
        },
    )

    return rows


def compute_ui_invention_overview_row_from_summary(
    *,
    bp: dict[str, Any],
    invention_best_summary: dict[str, Any],
    pricing_preferences: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Compute the Streamlit 'virtual' invention overview row for a blueprint.

    Mirrors the Streamlit computations for Profit/ROI/Duration, including sales tax and broker fees.
    Returns None when required inputs are missing.
    """

    if not isinstance(bp, dict) or not isinstance(invention_best_summary, dict):
        return None

    best = invention_best_summary.get("best_option")
    mfg = invention_best_summary.get("manufacturing")
    inv = invention_best_summary.get("invention")
    if not isinstance(best, dict) or not isinstance(mfg, dict) or not isinstance(inv, dict):
        return None

    prod_type_id = _safe_int(mfg.get("product_type_id"), default=0)
    if prod_type_id <= 0:
        return None
    prod_name = str(mfg.get("product_type_name") or "")
    prod_cat = mfg.get("product_category_name")

    prod_qty_per_run = _safe_int(mfg.get("product_quantity_per_run"), default=0)
    if prod_qty_per_run <= 0:
        return None

    invented_runs = _safe_int(best.get("invented_runs"), default=0)
    if invented_runs <= 0:
        return None

    invented_me = _safe_int(best.get("invented_me"), default=0)
    invented_te = _safe_int(best.get("invented_te"), default=0)

    p = _safe_float(best.get("success_probability"))
    if p is None or p <= 0:
        return None
    attempts_per_success = 1.0 / float(p)

    inv_attempt_mat = _safe_float(best.get("invention_attempt_material_cost_isk"))
    inv_attempt_fee = _safe_float(best.get("invention_job_fee_isk"))

    inv_fc = inv.get("facility_context") if isinstance(inv.get("facility_context"), dict) else {}
    inv_time_per_attempt = _safe_float(inv_fc.get("estimated_time_seconds"))
    if inv_time_per_attempt is None:
        inv_time_per_attempt = _safe_float(inv.get("time_seconds"))

    expected_inv_mat_total = (float(inv_attempt_mat) * float(attempts_per_success)) if inv_attempt_mat is not None else None
    expected_inv_fee_total = (float(inv_attempt_fee) * float(attempts_per_success)) if inv_attempt_fee is not None else None
    expected_inv_time_total = (float(inv_time_per_attempt) * float(attempts_per_success)) if inv_time_per_attempt is not None else None

    mfg_mat_per_run = _safe_float(best.get("manufacturing_material_cost_per_run_isk"))
    mfg_fee_per_run = _safe_float(best.get("manufacturing_job_fee_per_run_isk"))
    mfg_rev_per_run = _safe_float(best.get("manufacturing_revenue_per_run_isk"))
    if mfg_mat_per_run is None or mfg_fee_per_run is None or mfg_rev_per_run is None:
        return None

    mfg_fc = mfg.get("facility_context") if isinstance(mfg.get("facility_context"), dict) else {}
    mfg_time_per_run = _safe_float(mfg_fc.get("estimated_time_seconds_per_run"))
    if mfg_time_per_run is None:
        mfg_time_per_run = _safe_float(mfg.get("time_seconds"))

    runs = int(invented_runs)
    units_total = int(runs) * int(prod_qty_per_run)
    if units_total <= 0:
        return None

    mfg_mat_total = float(mfg_mat_per_run) * float(runs)
    mfg_fee_total = float(mfg_fee_per_run) * float(runs)
    revenue_total = float(mfg_rev_per_run) * float(runs)

    inv_mat_total = float(expected_inv_mat_total or 0.0)
    inv_fee_total = float(expected_inv_fee_total or 0.0)

    copy_cost_total = _safe_float(best.get("copying_job_fee_isk"))
    copy_cost_total_f = float(copy_cost_total or 0.0)

    mat_cost_total = float(mfg_mat_total) + float(inv_mat_total)
    job_fee_total = float(mfg_fee_total) + float(inv_fee_total)

    # Apply market fees (same policy as Streamlit).
    pp = pricing_preferences if isinstance(pricing_preferences, dict) else {}
    try:
        sales_tax_fraction = float(pp.get("sales_tax_fraction"))
    except Exception:
        sales_tax_fraction = 0.0
    try:
        broker_fee_fraction = float(pp.get("broker_fee_fraction"))
    except Exception:
        broker_fee_fraction = 0.0

    mat_src = str(pp.get("material_price_source") or "")
    prod_src = str(pp.get("product_price_source") or "")
    apply_buy_broker_fee = (mat_src in {"jita_buy", "Jita Buy"}) or ("buy" in mat_src.lower() and "jita" in mat_src.lower())
    apply_sell_broker_fee = (prod_src in {"jita_sell", "Jita Sell"}) or ("sell" in prod_src.lower() and "jita" in prod_src.lower())

    broker_fee_buy_total = float(mat_cost_total) * float(broker_fee_fraction) if apply_buy_broker_fee else 0.0
    broker_fee_sell_total = float(revenue_total) * float(broker_fee_fraction) if apply_sell_broker_fee else 0.0
    broker_fee_total = float(broker_fee_buy_total) + float(broker_fee_sell_total)
    sales_tax_total = float(revenue_total) * float(sales_tax_fraction)

    profit_total_net = (
        float(revenue_total)
        - float(mat_cost_total)
        - float(copy_cost_total_f)
        - float(job_fee_total)
        - float(sales_tax_total)
        - float(broker_fee_total)
    )

    copy_time_total = _safe_float(best.get("copying_expected_time_seconds"))

    time_total_seconds = None
    if mfg_time_per_run is not None or expected_inv_time_total is not None or copy_time_total is not None:
        try:
            time_total_seconds = (
                float(expected_inv_time_total or 0.0)
                + (float(mfg_time_per_run or 0.0) * float(runs))
                + float(copy_time_total or 0.0)
            )
        except Exception:
            time_total_seconds = None

    job_duration_display = format_duration(time_total_seconds) if time_total_seconds is not None else "-"

    profit_per_hour = None
    if time_total_seconds is not None:
        try:
            hours = float(time_total_seconds) / 3600.0
        except Exception:
            hours = 0.0
        if hours > 0:
            profit_per_hour = float(profit_total_net) / float(hours)

    denom_total = float(mat_cost_total) + float(copy_cost_total_f) + float(job_fee_total) + float(broker_fee_total)
    roi_total = (float(profit_total_net) / float(denom_total)) if denom_total > 0 else None
    roi_total_percent = (float(roi_total) * 100.0) if roi_total is not None else None

    mat_cost_per_item = float(mat_cost_total) / float(units_total)
    job_fee_per_item = float(job_fee_total) / float(units_total)
    copy_cost_per_item = float(copy_cost_total_f) / float(units_total)
    revenue_per_item = float(revenue_total) / float(units_total)
    sales_tax_per_item = float(sales_tax_total) / float(units_total)
    broker_fee_per_item = float(broker_fee_total) / float(units_total)
    profit_per_item = float(profit_total_net) / float(units_total)

    total_cost_total = float(mat_cost_total) + float(copy_cost_total_f) + float(job_fee_total) + float(sales_tax_total)
    total_cost_per_item = float(total_cost_total) / float(units_total)

    loc = bp.get("location") if isinstance(bp.get("location"), dict) else {}
    solar = (loc.get("solar_system") or {}) if isinstance(loc, dict) else {}
    source_bp_type_id = _safe_int(bp.get("type_id"), default=0)

    return {
        "type_id": int(prod_type_id),
        "Name": f"{prod_name} (invention)" if prod_name else f"typeID {int(prod_type_id)} (invention)",
        "Category": prod_cat,
        "_product_row_key": f"invention:{int(prod_type_id)}",
        "_row_kind": "invention",
        "_invention_source_blueprint_type_id": int(source_bp_type_id),
        "_invention_decryptor": best.get("decryptor_type_name"),
        "Runs": int(runs),
        "Units": int(units_total),
        "ME": int(invented_me),
        "TE": int(invented_te),
        "Job Duration": str(job_duration_display),
        "Mat. Cost / item": float(mat_cost_per_item),
        "Copy Cost / item": float(copy_cost_per_item),
        "Total Cost / item": float(total_cost_per_item),
        "Revenue / item": float(revenue_per_item),
        "Sales Tax / item": float(sales_tax_per_item),
        "Broker Fee / item": float(broker_fee_per_item),
        "Profit / item": float(profit_per_item),
        "Job Fee / item": float(job_fee_per_item),
        "Mat. Cost": float(mat_cost_total),
        "Copy Cost": float(copy_cost_total_f),
        "Total Cost": float(total_cost_total),
        "Revenue": float(revenue_total),
        "Sales Tax": float(sales_tax_total),
        "Broker Fee": float(broker_fee_total),
        "Profit": float(profit_total_net),
        "Profit / hour": float(profit_per_hour) if profit_per_hour is not None else None,
        "Job Fee": float(job_fee_total),
        "ROI": float(roi_total_percent) if roi_total_percent is not None else None,
        "Location": (loc.get("display_name") if isinstance(loc, dict) else None),
        "Solar System": (solar.get("name") if isinstance(solar, dict) else None),
        "Solar System Security": (solar.get("security_status") if isinstance(solar, dict) else None),
    }


def compute_ui_build_tree_rows_by_product(
    *,
    plan_rows: list[dict[str, Any]] | None,
    required_materials: list[dict[str, Any]] | None,
    root_required_quantity: int,
    allocation_share: float | None = None,
) -> list[dict[str, Any]]:
    """Build a flattened TreeData payload for Streamlit-AgGrid.

    Returns rows with keys matching the UI expectation:
    path,type_id,action,qty,qty_required,unit,effective_cost,inventory_cost,buy_cost,build_cost,roi,...
    """

    required_materials = required_materials or []
    if not plan_rows:
        plan_rows = _synth_plan_rows_from_required_materials(required_materials)

    # Always wrap under a single root row.
    root_node = {
        "type_id": 0,
        "type_name": "Manufacturing Job",
        "recommendation": "build",
        "required_quantity": int(max(0, root_required_quantity)),
        "children": list(plan_rows or []),
    }

    tree_rows: list[dict[str, Any]] = []

    def _walk(n: dict, *, parent_path: list[str]) -> None:
        rec = str(n.get("recommendation") or "-").lower()
        key = _node_key(n)
        path = [*parent_path, key]
        path_str = _PATH_SEP.join([p for p in path if p is not None])

        qty_required = n.get("required_quantity")
        qty_required_i = _safe_int(qty_required, default=0)

        effective_cost = n.get("effective_cost_isk")
        market_unit = n.get("buy_unit_price_isk")

        market_buy_cost = None
        try:
            mu = float(market_unit) if market_unit is not None else None
            if mu is not None and qty_required_i > 0:
                market_buy_cost = float(mu) * float(qty_required_i)
        except Exception:
            market_buy_cost = None

        build = n.get("build") if isinstance(n.get("build"), dict) else None
        build_full = n.get("build_full") if isinstance(n.get("build_full"), dict) else None
        build_for_cost = build if (rec == "build" and isinstance(build, dict)) else build_full
        build_cost = build_for_cost.get("total_build_cost_isk") if isinstance(build_for_cost, dict) else None

        shortfall_qty = n.get("shortfall_quantity")
        shortfall_action = n.get("shortfall_recommendation")
        shortfall_buy_cost = n.get("shortfall_buy_cost_isk")
        shortfall_build_cost = n.get("shortfall_build_cost_isk")
        savings_isk = n.get("savings_isk")

        inv_used_qty_i = _safe_int(n.get("inventory_used_qty"), default=0)
        shortfall_qty_i = _safe_int(shortfall_qty, default=max(0, qty_required_i - inv_used_qty_i))

        inv_cost = _inventory_cost_display(n=n, market_unit=market_unit)

        # Action display policy: mirror the Streamlit logic.
        action_display = rec
        try:
            if inv_used_qty_i > 0:
                if shortfall_qty_i > 0:
                    rem_buy = shortfall_buy_cost
                    if rem_buy is None and market_unit is not None:
                        rem_buy = float(market_unit) * float(shortfall_qty_i)
                    rem_build = shortfall_build_cost if shortfall_build_cost is not None else build_cost
                    rem_choice = _choose_buy_vs_build(buy_cost_total=rem_buy, build_cost_total=rem_build)
                    action_display = f"take/{rem_choice}" if rem_choice else "take"
                else:
                    action_display = "take"
            else:
                choice = _choose_buy_vs_build(buy_cost_total=market_buy_cost, build_cost_total=build_cost)
                if choice is not None:
                    action_display = choice
        except Exception:
            action_display = rec

        # Effective cost: prefer backend planner field; fallback only when missing.
        effective_cost_display = _safe_float(effective_cost)
        if effective_cost_display is None:
            try:
                if inv_cost is not None:
                    if shortfall_qty_i > 0:
                        rem = None
                        if shortfall_buy_cost is not None or shortfall_build_cost is not None:
                            rem_choice_val = []
                            if shortfall_buy_cost is not None:
                                rem_choice_val.append(float(shortfall_buy_cost))
                            if shortfall_build_cost is not None:
                                rem_choice_val.append(float(shortfall_build_cost))
                            rem = min(rem_choice_val) if rem_choice_val else None
                        if rem is None:
                            candidates: list[float] = []
                            if market_buy_cost is not None:
                                candidates.append(float(market_buy_cost))
                            if build_cost is not None:
                                candidates.append(float(build_cost))
                            rem = min(candidates) if candidates else None
                        effective_cost_display = float(inv_cost) + float(rem or 0.0)
                    else:
                        effective_cost_display = float(inv_cost)
                else:
                    candidates: list[float] = []
                    if market_buy_cost is not None:
                        candidates.append(float(market_buy_cost))
                    if build_cost is not None:
                        candidates.append(float(build_cost))
                    if candidates:
                        effective_cost_display = float(min(candidates))
            except Exception:
                effective_cost_display = None

        effective_unit = None
        try:
            if effective_cost_display is not None and qty_required_i > 0:
                effective_unit = float(effective_cost_display) / float(qty_required_i)
        except Exception:
            effective_unit = None

        roi_display = None
        if market_buy_cost is not None and build_cost is not None:
            try:
                bc = float(build_cost)
                roi_display = ((float(market_buy_cost) - float(build_cost)) / bc) * 100.0 if bc > 0 else None
            except Exception:
                roi_display = None

        buy_cost_display = market_buy_cost
        try:
            if str(action_display) == "take":
                buy_cost_display = None
            elif str(action_display) == "take/buy":
                rem_buy = shortfall_buy_cost
                if rem_buy is None and market_unit is not None and shortfall_qty_i > 0:
                    rem_buy = float(market_unit) * float(shortfall_qty_i)
                buy_cost_display = rem_buy
        except Exception:
            buy_cost_display = market_buy_cost

        build_cost_display = build_cost
        try:
            if inv_used_qty_i > 0 and shortfall_qty_i > 0 and shortfall_build_cost is not None:
                build_cost_display = float(shortfall_build_cost)
        except Exception:
            build_cost_display = build_cost

        tree_rows.append(
            {
                "path": path_str,
                "type_id": _safe_int(n.get("type_id"), default=0),
                "Icon": n.get("icon_url"),
                "action": str(action_display) if action_display is not None else rec,
                "reason": (str(n.get("reason")) if n.get("reason") is not None else None),
                "qty": qty_required_i if qty_required_i > 0 else None,
                "qty_required": qty_required_i if qty_required_i > 0 else None,
                "unit": effective_unit,
                "market_unit": _safe_float(market_unit),
                "shortfall_qty": shortfall_qty_i if shortfall_qty_i > 0 else None,
                "shortfall_action": (str(shortfall_action) if shortfall_action is not None else None),
                "shortfall_buy_cost": _safe_float(shortfall_buy_cost),
                "shortfall_build_cost": _safe_float(shortfall_build_cost),
                "savings_isk": _safe_float(savings_isk),
                "inventory_used_qty": inv_used_qty_i if inv_used_qty_i > 0 else None,
                "inventory_fifo_priced_qty": _safe_int(n.get("inventory_fifo_priced_qty"), default=0) or None,
                "inventory_cost": float(inv_cost) if inv_cost is not None else None,
                "effective_cost": float(effective_cost_display) if effective_cost_display is not None else None,
                "buy_cost": _safe_float(buy_cost_display),
                "build_cost": _safe_float(build_cost_display),
                "roi": float(roi_display) if roi_display is not None else None,
            }
        )

        children = n.get("children") or []
        if isinstance(children, list) and children:
            for ch in children:
                if isinstance(ch, dict):
                    _walk(ch, parent_path=path)

    _walk(root_node, parent_path=[])

    # Optional: scale money columns for multi-output blueprints.
    # Quantities remain the full job requirements; only ISK totals are allocated.
    if allocation_share is not None:
        try:
            share = float(allocation_share)
        except Exception:
            share = None
        if share is not None and share != 1.0:
            money_cols = {
                "inventory_cost",
                "effective_cost",
                "buy_cost",
                "build_cost",
                "shortfall_buy_cost",
                "shortfall_build_cost",
                "savings_isk",
            }
            for r in tree_rows:
                for col in money_cols:
                    v = r.get(col)
                    if isinstance(v, (int, float)):
                        r[col] = float(v) * float(share)

    # Root rollups: sum immediate children to avoid double counting.
    def _depth(path_str: str) -> int:
        return len(str(path_str).split(_PATH_SEP))

    root_path = tree_rows[0].get("path") if tree_rows else None
    if root_path:
        child_rows = [r for r in tree_rows if str(r.get("path") or "").startswith(str(root_path) + _PATH_SEP) and _depth(str(r.get("path") or "")) == 2]
        for col in ["effective_cost", "inventory_cost", "buy_cost", "build_cost"]:
            vals = [r.get(col) for r in child_rows]
            nums = [float(v) for v in vals if isinstance(v, (int, float))]
            if nums:
                tree_rows[0][col] = float(sum(nums))

        # Root unit uses root required quantity.
        root_qty = _safe_int(root_node.get("required_quantity"), default=0)
        try:
            if root_qty > 0 and isinstance(tree_rows[0].get("effective_cost"), (int, float)):
                tree_rows[0]["unit"] = float(tree_rows[0]["effective_cost"]) / float(root_qty)
        except Exception:
            pass

    return tree_rows


def compute_ui_copy_jobs(*, blueprint_name: str, manufacture_job: dict | None, plan_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    # Root manufacturing blueprint copy job.
    try:
        props = (manufacture_job or {}).get("properties") if isinstance(manufacture_job, dict) else None
        root_copy = (props or {}).get("copy_job") if isinstance(props, dict) else None
        if isinstance(root_copy, dict):
            time_d = root_copy.get("time") if isinstance(root_copy.get("time"), dict) else {}
            cost_d = root_copy.get("job_cost") if isinstance(root_copy.get("job_cost"), dict) else {}
            out.append(
                {
                    "Blueprint": str(blueprint_name),
                    "Runs": _safe_int(root_copy.get("runs"), default=0),
                    "Max Runs": _safe_int(root_copy.get("max_runs"), default=0) or None,
                    "Duration": _safe_float(time_d.get("estimated_copy_time_seconds")),
                    "Job Fee": _safe_float(cost_d.get("total_job_cost_isk")),
                }
            )
    except Exception:
        pass

    def _walk(nodes: list[dict[str, Any]]) -> None:
        for n in nodes or []:
            if not isinstance(n, dict):
                continue
            rec = str(n.get("recommendation") or "").lower()
            if rec == "build":
                build = n.get("build") if isinstance(n.get("build"), dict) else None
                if isinstance(build, dict):
                    co = build.get("copy_overhead") if isinstance(build.get("copy_overhead"), dict) else None
                    if isinstance(co, dict):
                        out.append(
                            {
                                "Blueprint": str(build.get("blueprint_type_name") or build.get("blueprint_type_id") or ""),
                                "Runs": _safe_int(build.get("runs_needed"), default=0),
                                "Max Runs": _safe_int(co.get("max_production_limit"), default=0) or None,
                                "Duration": _safe_float(co.get("estimated_copy_time_seconds")),
                                "Job Fee": _safe_float(co.get("estimated_copy_fee_isk")),
                            }
                        )

            children = n.get("children")
            if isinstance(children, list) and children:
                _walk([c for c in children if isinstance(c, dict)])

    if isinstance(plan_rows, list) and plan_rows:
        _walk(plan_rows)

    return out


def compute_ui_missing_blueprints(plan_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    by_bp: dict[int, dict[str, Any]] = {}

    def _walk(n: dict[str, Any]) -> None:
        rec = str(n.get("recommendation") or "").lower()
        build = n.get("build") if isinstance(n.get("build"), dict) else None
        if rec == "build" and isinstance(build, dict):
            owned = build.get("blueprint_owned")
            if owned is False:
                bp_type_id = _safe_int(build.get("blueprint_type_id"), default=0)
                if bp_type_id > 0:
                    slot = by_bp.get(bp_type_id)
                    if slot is None:
                        eff = build.get("blueprint_efficiency") if isinstance(build.get("blueprint_efficiency"), dict) else {}
                        slot = {
                            "blueprint_type_id": int(bp_type_id),
                            "Blueprint": str(build.get("blueprint_type_name") or bp_type_id),
                            "Assumed ME": _safe_float(eff.get("me_percent")),
                            "Assumed TE": _safe_float(eff.get("te_percent")),
                            "Assumption Source": str(eff.get("source") or ""),
                            "Est. BPO Buy Cost": _safe_float(build.get("blueprint_bpo_buy_cost_isk")),
                            "Used For": set(),
                        }
                        by_bp[int(bp_type_id)] = slot

                    prod_name = str(n.get("type_name") or n.get("type_id") or "")
                    req_qty = _safe_int(n.get("required_quantity"), default=0)
                    if prod_name:
                        slot["Used For"].add(f"{prod_name} ({req_qty})")

        children = n.get("children")
        if isinstance(children, list):
            for ch in children:
                if isinstance(ch, dict):
                    _walk(ch)

    for root in plan_rows or []:
        if isinstance(root, dict):
            _walk(root)

    rows: list[dict[str, Any]] = []
    for _bp_type_id, slot in by_bp.items():
        used_for = slot.get("Used For")
        used_list = sorted([str(x) for x in (used_for or []) if x])
        used_txt = ", ".join(used_list[:4])
        if len(used_list) > 4:
            used_txt += " …"
        rows.append(
            {
                "blueprint_type_id": slot.get("blueprint_type_id"),
                "Blueprint": slot.get("Blueprint"),
                "Assumed ME": slot.get("Assumed ME"),
                "Assumed TE": slot.get("Assumed TE"),
                "Assumption Source": slot.get("Assumption Source"),
                "Est. BPO Buy Cost": slot.get("Est. BPO Buy Cost"),
                "Used For": used_txt,
            }
        )

    rows.sort(key=lambda r: str(r.get("Blueprint") or ""))
    return rows


def apply_multi_output_cost_allocations(
    *,
    bp: dict[str, Any],
    total_material_cost: float | None,
    total_product_value: float | None,
    total_job_fee: float | None,
    total_copy_cost: float | None,
) -> None:
    """Attach allocation shares + allocated totals to each product row.

    Keeps allocation logic consistent between overview and details.
    """

    products_list = [p for p in (bp.get("products") or []) if isinstance(p, dict)]
    if not products_list:
        return

    product_value_totals: list[float] = []
    product_qty_totals: list[int] = []
    for prod in products_list:
        q = _safe_int(prod.get("quantity_total") or prod.get("quantity") or 0, default=0)
        unit_price = _safe_float(prod.get("market_unit_price_isk"))
        if unit_price is None:
            unit_price = _safe_float(prod.get("average_price"))
        product_qty_totals.append(max(0, int(q)))
        product_value_totals.append(max(0.0, float(q) * float(unit_price or 0.0)))

    value_total_sum = float(sum(product_value_totals))
    qty_total_sum = int(sum(product_qty_totals))

    tm = float(total_material_cost or 0.0)
    tv = float(total_product_value or 0.0)
    tj = float(total_job_fee or 0.0)
    tc = float(total_copy_cost or 0.0)

    for idx, prod in enumerate(products_list):
        if value_total_sum > 0:
            share = float(product_value_totals[idx]) / float(value_total_sum)
            share_basis = "value"
        elif qty_total_sum > 0:
            share = float(product_qty_totals[idx]) / float(qty_total_sum)
            share_basis = "qty"
        else:
            share = 1.0
            share_basis = "fallback"

        prod["allocation_share"] = float(share)
        prod["allocation_share_basis"] = str(share_basis)
        prod["allocated_material_cost_isk"] = float(tm) * float(share)
        prod["allocated_product_value_isk"] = float(tv) * float(share)
        prod["allocated_job_fee_isk"] = float(tj) * float(share)
        prod["allocated_copy_cost_isk"] = float(tc) * float(share)
