from __future__ import annotations

import math
from typing import Any, Iterable

from classes.asset_provenance import FifoLot, fifo_allocate_cost_breakdown
from eve_online_industry_tracker.db_models import Blueprints

from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data


_ALL_BLUEPRINT_MFG_CACHE_BY_LANG: dict[str, dict[int, dict]] = {}


def _get_all_blueprint_manufacturing_data_cached(sde_session, language: str) -> dict[int, dict]:
    lang = str(language or "en")
    cached = _ALL_BLUEPRINT_MFG_CACHE_BY_LANG.get(lang)
    if isinstance(cached, dict) and cached:
        return cached
    data = get_blueprint_manufacturing_data(sde_session, lang)
    if isinstance(data, dict) and data:
        _ALL_BLUEPRINT_MFG_CACHE_BY_LANG[lang] = data
    return data


def _build_price_map(esi_service) -> dict[int, dict[str, float | None]]:
    """Return type_id -> {average_price, adjusted_price} map (best-effort)."""

    if esi_service is None:
        return {}

    try:
        market_prices = esi_service.get_market_prices() or []
    except Exception:
        return {}

    out: dict[int, dict[str, float | None]] = {}
    for row in market_prices:
        if not isinstance(row, dict):
            continue
        tid = row.get("type_id")
        if tid is None:
            continue
        try:
            type_id = int(tid)
        except Exception:
            continue
        avg = row.get("average_price")
        adj = row.get("adjusted_price")
        try:
            avg_f = float(avg) if avg is not None else None
        except Exception:
            avg_f = None
        try:
            adj_f = float(adj) if adj is not None else None
        except Exception:
            adj_f = None
        out[type_id] = {"average_price": avg_f, "adjusted_price": adj_f}

    return out


def _best_unit_price(prices: dict[str, float | None]) -> float | None:
    """Pick the most useful unit price.

    For cost comparisons, prefer average price; fall back to adjusted price.
    """

    if not isinstance(prices, dict):
        return None

    avg = prices.get("average_price")
    if isinstance(avg, (int, float)) and float(avg) > 0:
        return float(avg)

    adj = prices.get("adjusted_price")
    if isinstance(adj, (int, float)) and float(adj) > 0:
        return float(adj)

    return None


def _avg_unit_price(prices: dict[str, float | None]) -> float | None:
    """Buy decisions use average price (fallback to None).

    Adjusted price is reserved for EIV/job fee estimation.
    """

    if not isinstance(prices, dict):
        return None

    avg = prices.get("average_price")
    if isinstance(avg, (int, float)) and float(avg) > 0:
        return float(avg)
    return None


def _adjusted_unit_price(prices: dict[str, float | None]) -> float | None:
    if not isinstance(prices, dict):
        return None
    adj = prices.get("adjusted_price")
    if isinstance(adj, (int, float)) and float(adj) > 0:
        return float(adj)
    return None


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _ceil_div(a: int, b: int) -> int:
    if b <= 0:
        return 0
    return int(math.ceil(float(a) / float(b)))


def _index_blueprints_by_product(all_bp_data: dict[int, dict]) -> dict[int, list[dict]]:
    """Return product_type_id -> list of blueprint descriptors.

    Each descriptor includes blueprint_type_id, blueprint_type_name, and product_quantity_per_run.
    """

    out: dict[int, list[dict]] = {}
    for bp_type_id, bp in (all_bp_data or {}).items():
        if not isinstance(bp, dict):
            continue

        bp_name = str(bp.get("type_name") or "")

        mfg = bp.get("manufacturing")
        if not isinstance(mfg, dict):
            continue

        products = mfg.get("products") or []
        if not isinstance(products, list):
            continue

        for prod in products:
            if not isinstance(prod, dict):
                continue
            pid = prod.get("type_id")
            if pid is None:
                continue
            try:
                product_type_id = int(pid)
            except Exception:
                continue

            qty = _safe_int(prod.get("quantity"), default=0)
            if qty <= 0:
                continue

            out.setdefault(product_type_id, []).append(
                {
                    "blueprint_type_id": int(bp_type_id),
                    "blueprint_type_name": (bp_name or str(bp_type_id)),
                    "product_quantity_per_run": int(qty),
                    "product_type_name": str(prod.get("type_name") or "") or None,
                }
            )

    # Prefer the largest output per run first.
    for k, v in out.items():
        v.sort(key=lambda d: int(d.get("product_quantity_per_run") or 0), reverse=True)
        out[k] = v

    return out


def _index_activity_product_type_ids(sde_session, *, activity: str) -> set[int]:
    """Return set of product type_ids produced by a given blueprint activity.

    This is used to classify reaction-only materials robustly via SDE blueprint activity data,
    rather than via name heuristics.
    """

    if sde_session is None:
        return set()

    out: set[int] = set()
    try:
        blueprints = sde_session.query(Blueprints).all()
    except Exception:
        return set()

    for bp in blueprints or []:
        activities = bp.activities if isinstance(getattr(bp, "activities", None), dict) else {}
        if not isinstance(activities, dict):
            continue

        act = activities.get(str(activity))
        if not isinstance(act, dict):
            continue

        products = act.get("products") or []
        if not isinstance(products, list):
            continue

        for prod in products:
            if not isinstance(prod, dict):
                continue
            tid = prod.get("typeID")
            if tid is None:
                continue
            try:
                out.add(int(tid))
            except Exception:
                continue

    return out


def plan_submanufacturing_tree(
    *,
    sde_session,
    language: str,
    esi_service,
    materials: Iterable[dict],
    owned_blueprint_type_ids: set[int] | None = None,
    owned_blueprint_best_by_type_id: dict[int, dict] | None = None,
    manufacturing_system_cost_index: float = 0.0,
    copying_system_cost_index: float = 0.0,
    research_me_system_cost_index: float = 0.0,
    research_te_system_cost_index: float = 0.0,
    material_reduction_total_fraction: float = 0.0,
    time_reduction_total_fraction: float = 0.0,
    job_cost_reduction_total_fraction: float = 0.0,
    surcharge_rate_total_fraction: float = 0.0,
    inventory_on_hand_by_type: dict[int, int] | None = None,
    inventory_fifo_lots_by_type: dict[int, list[FifoLot]] | None = None,
    use_fifo_inventory_costing: bool = True,
    max_depth: int = 3,
) -> list[dict]:
    """Recursive build-vs-buy planner.

    Rules from your requirements:
    - Reactions are not implemented; if a material is only producible by a Reaction Formula, we buy it.
    - Buy decisions use average price (ESI market average).
    - Job fees use adjusted price (EIV basis), like the in-game UI.
    - If you don't own an intermediate blueprint, we still suggest it, and include the BPO market cost
      plus optional research/copy overhead estimates.
    """

    lang = str(language or "en")
    owned_bps = {int(x) for x in (owned_blueprint_type_ids or set()) if x is not None}
    owned_bp_best = owned_blueprint_best_by_type_id or {}
    if owned_bp_best:
        owned_bps |= {int(k) for k in owned_bp_best.keys() if k is not None}

    all_bp_data = _get_all_blueprint_manufacturing_data_cached(sde_session, lang)
    mfg_product_to_bps = _index_blueprints_by_product(all_bp_data)
    reaction_product_type_ids = _index_activity_product_type_ids(sde_session, activity="reaction")

    price_map = _build_price_map(esi_service)

    use_fifo_inventory_costing = bool(use_fifo_inventory_costing)
    inventory_on_hand_by_type = inventory_on_hand_by_type or {}
    inventory_fifo_lots_by_type = inventory_fifo_lots_by_type or {}

    mat_red = max(0.0, min(1.0, float(material_reduction_total_fraction or 0.0)))
    time_red = max(0.0, min(1.0, float(time_reduction_total_fraction or 0.0)))
    cost_red = max(0.0, min(1.0, float(job_cost_reduction_total_fraction or 0.0)))

    def _estimate_job_fee_from_eiv(eiv_total_isk: float | None, *, ci: float) -> float | None:
        if eiv_total_isk is None:
            return None
        try:
            eiv = float(eiv_total_isk)
        except Exception:
            return None
        if eiv <= 0:
            return 0.0
        ci_f = max(0.0, float(ci or 0.0))
        surcharge = max(0.0, float(surcharge_rate_total_fraction or 0.0))
        gross = eiv * ci_f
        gross_after_bonuses = gross * (1.0 - cost_red)
        taxes = eiv * surcharge
        return max(0.0, float(gross_after_bonuses) + float(taxes))

    def _buy_cost_for_type(
        type_id: int,
        qty: int,
        *,
        use_inventory: bool = True,
    ) -> tuple[float | None, float | None, dict[str, Any]]:
        """Return (market_unit_price, total_buy_cost, details).

        When FIFO inventory costing is enabled, the buy cost is priced as:
        - consume up to on-hand qty at FIFO historical cost (best-effort)
        - any unknown-basis inventory + any remaining shortfall at market average

        Notes:
        - This does not "reserve" inventory across nodes; it is a snapshot valuation.
        """

        type_id_i = int(type_id)
        qty_i = max(0, int(qty or 0))

        prices = price_map.get(type_id_i, {})
        market_unit = _avg_unit_price(prices)

        details: dict[str, Any] = {
            "inventory_on_hand_qty": None,
            "inventory_used_qty": None,
            "inventory_fifo_priced_qty": None,
            "inventory_unknown_cost_qty": None,
            "inventory_fifo_cost_isk": None,
            "inventory_fifo_breakdown_by_source": None,
            "inventory_fifo_market_buy_qty": None,
            "inventory_fifo_market_buy_cost_isk": None,
            "inventory_fifo_industry_build_qty": None,
            "inventory_fifo_industry_build_cost_isk": None,
            "buy_now_qty": None,
            "buy_effective_unit_price_isk": None,
        }

        if qty_i <= 0:
            return market_unit, 0.0, details

        if not (use_fifo_inventory_costing and use_inventory):
            if market_unit is None:
                return None, None, details
            total = float(qty_i) * float(market_unit)
            details["buy_effective_unit_price_isk"] = float(total) / float(qty_i) if qty_i > 0 else None
            details["buy_now_qty"] = int(qty_i)
            return float(market_unit), float(total), details

        on_hand = int(inventory_on_hand_by_type.get(type_id_i, 0) or 0)
        inv_used = min(int(qty_i), max(0, on_hand))

        lots = inventory_fifo_lots_by_type.get(type_id_i) or []
        bd = fifo_allocate_cost_breakdown(lots=lots, quantity=int(inv_used))
        fifo_cost = float(bd.get("total_cost") or 0.0)
        fifo_priced_qty = int(bd.get("priced_quantity") or 0)
        by_source = bd.get("by_source") if isinstance(bd.get("by_source"), dict) else None
        unknown_inv_qty = max(0, int(inv_used) - int(fifo_priced_qty))
        buy_now_qty = max(0, int(qty_i) - int(inv_used))

        details["inventory_on_hand_qty"] = int(on_hand)
        details["inventory_used_qty"] = int(inv_used)
        details["inventory_fifo_priced_qty"] = int(fifo_priced_qty)
        details["inventory_unknown_cost_qty"] = int(unknown_inv_qty)
        details["inventory_fifo_cost_isk"] = float(fifo_cost)
        details["inventory_fifo_breakdown_by_source"] = by_source
        if isinstance(by_source, dict):
            mb = by_source.get("market_buy") if isinstance(by_source.get("market_buy"), dict) else None
            ib = by_source.get("industry_build") if isinstance(by_source.get("industry_build"), dict) else None
            if mb:
                details["inventory_fifo_market_buy_qty"] = int(mb.get("quantity") or 0)
                details["inventory_fifo_market_buy_cost_isk"] = float(mb.get("cost") or 0.0)
            if ib:
                details["inventory_fifo_industry_build_qty"] = int(ib.get("quantity") or 0)
                details["inventory_fifo_industry_build_cost_isk"] = float(ib.get("cost") or 0.0)
        details["buy_now_qty"] = int(buy_now_qty)

        needs_market = (unknown_inv_qty + buy_now_qty) > 0
        if needs_market and (market_unit is None):
            if fifo_priced_qty >= qty_i:
                details["buy_effective_unit_price_isk"] = float(fifo_cost) / float(qty_i)
                return None, float(fifo_cost), details
            return None, None, details

        market_cost = float(unknown_inv_qty + buy_now_qty) * float(market_unit or 0.0)
        total_cost = float(fifo_cost) + float(market_cost)
        details["buy_effective_unit_price_isk"] = float(total_cost) / float(qty_i) if qty_i > 0 else None

        return float(market_unit) if market_unit is not None else None, float(total_cost), details

    def _blueprint_bpo_buy_cost(bp_type_id: int) -> float | None:
        unit, _, _ = _buy_cost_for_type(int(bp_type_id), 1, use_inventory=False)
        return float(unit) if unit is not None else None

    def _pick_best_mfg_blueprint_for_product(product_type_id: int) -> dict | None:
        cands = mfg_product_to_bps.get(int(product_type_id), [])
        return cands[0] if cands else None

    def _is_reaction_only(product_type_id: int) -> bool:
        if int(product_type_id) in mfg_product_to_bps:
            return False
        return int(product_type_id) in reaction_product_type_ids

    def _compute_eiv_total_for_inputs(inputs_me0: list[dict]) -> float | None:
        total = 0.0
        for inp in inputs_me0:
            if not isinstance(inp, dict):
                continue
            tid = inp.get("type_id")
            qty = inp.get("quantity")
            if tid is None or qty is None:
                return None
            try:
                t = int(tid)
                q = int(qty)
            except Exception:
                return None
            adj = _adjusted_unit_price(price_map.get(t, {}))
            if adj is None:
                return None
            total += float(q) * float(adj)
        return float(total)

    def _plan_one(type_id: int, qty: int, *, depth: int, path: set[int]) -> dict:
        type_id_i = int(type_id)
        qty_i = max(0, int(qty))

        buy_unit, buy_cost, buy_details = _buy_cost_for_type(type_id_i, qty_i)

        take_possible = False
        try:
            inv_used = int(buy_details.get("inventory_used_qty") or 0)
            buy_now = int(buy_details.get("buy_now_qty") or 0)
            fifo_priced = int(buy_details.get("inventory_fifo_priced_qty") or 0)
            eff_unit = buy_details.get("buy_effective_unit_price_isk")
            if (
                qty_i > 0
                and inv_used == qty_i
                and buy_now == 0
                and fifo_priced == qty_i
                and (buy_unit is not None)
                and (eff_unit is not None)
                and float(eff_unit) < float(buy_unit)
            ):
                take_possible = True
        except Exception:
            take_possible = False

        node: dict[str, Any] = {
            "type_id": type_id_i,
            "type_name": None,
            "required_quantity": qty_i,
            "buy_unit_price_isk": buy_unit,
            "buy_cost_isk": buy_cost,
            "buy_effective_unit_price_isk": buy_details.get("buy_effective_unit_price_isk"),
            "inventory_on_hand_qty": buy_details.get("inventory_on_hand_qty"),
            "inventory_used_qty": buy_details.get("inventory_used_qty"),
            "inventory_fifo_priced_qty": buy_details.get("inventory_fifo_priced_qty"),
            "inventory_unknown_cost_qty": buy_details.get("inventory_unknown_cost_qty"),
            "inventory_fifo_cost_isk": buy_details.get("inventory_fifo_cost_isk"),
            "inventory_fifo_breakdown_by_source": buy_details.get("inventory_fifo_breakdown_by_source"),
            "inventory_fifo_market_buy_qty": buy_details.get("inventory_fifo_market_buy_qty"),
            "inventory_fifo_market_buy_cost_isk": buy_details.get("inventory_fifo_market_buy_cost_isk"),
            "inventory_fifo_industry_build_qty": buy_details.get("inventory_fifo_industry_build_qty"),
            "inventory_fifo_industry_build_cost_isk": buy_details.get("inventory_fifo_industry_build_cost_isk"),
            "buy_now_qty": buy_details.get("buy_now_qty"),
            "reason": None,
            "recommendation": None,
            "savings_isk": None,
            "effective_cost_isk": None,
            "effective_time_seconds": None,
            "build": None,
            "children": [],
        }

        if depth >= max_depth:
            node["reason"] = "max_depth_reached"
            node["recommendation"] = ("take" if take_possible else "buy") if buy_cost is not None else None
            node["effective_cost_isk"] = buy_cost
            node["effective_time_seconds"] = 0.0 if buy_cost is not None else None
            return node

        if type_id_i in path:
            node["reason"] = "cycle_detected"
            node["recommendation"] = ("take" if take_possible else "buy") if buy_cost is not None else None
            node["effective_cost_isk"] = buy_cost
            node["effective_time_seconds"] = 0.0 if buy_cost is not None else None
            return node

        if _is_reaction_only(type_id_i):
            node["reason"] = "reaction_formula_not_supported"
            node["recommendation"] = ("take" if take_possible else "buy") if buy_cost is not None else None
            node["effective_cost_isk"] = buy_cost
            node["effective_time_seconds"] = 0.0 if buy_cost is not None else None
            return node

        best = _pick_best_mfg_blueprint_for_product(type_id_i)
        if best is None:
            node["reason"] = "no_blueprint_found"
            node["recommendation"] = ("take" if take_possible else "buy") if buy_cost is not None else None
            node["effective_cost_isk"] = buy_cost
            node["effective_time_seconds"] = 0.0 if buy_cost is not None else None
            return node

        blueprint_type_id = int(best.get("blueprint_type_id") or 0)
        bp_info = all_bp_data.get(blueprint_type_id, {}) if isinstance(all_bp_data, dict) else {}

        output_per_run = int(best.get("product_quantity_per_run") or 0)
        if output_per_run <= 0:
            node["reason"] = "invalid_blueprint_output"
            node["recommendation"] = ("take" if take_possible else "buy") if buy_cost is not None else None
            node["effective_cost_isk"] = buy_cost
            node["effective_time_seconds"] = 0.0 if buy_cost is not None else None
            return node

        runs_needed = _ceil_div(qty_i, output_per_run)
        output_total = runs_needed * output_per_run

        mfg = bp_info.get("manufacturing") if isinstance(bp_info, dict) else None
        mfg_materials = (mfg.get("materials") if isinstance(mfg, dict) else None) or []

        owned_eff = owned_bp_best.get(int(blueprint_type_id)) if isinstance(owned_bp_best, dict) else None
        eff_source = "assumed_unowned"
        me_percent_used = 10.0
        te_percent_used = 20.0
        owned_is_bpc: bool | None = None
        owned_runs: int | None = None

        if isinstance(owned_eff, dict):
            try:
                me_percent_used = float(owned_eff.get("me_percent") or 0.0)
            except Exception:
                me_percent_used = 0.0
            try:
                te_percent_used = float(owned_eff.get("te_percent") or 0.0)
            except Exception:
                te_percent_used = 0.0
            owned_is_bpc = bool(owned_eff.get("is_blueprint_copy"))
            try:
                rr = owned_eff.get("runs")
                owned_runs = int(rr) if rr is not None else None
            except Exception:
                owned_runs = None
            eff_source = "owned_blueprint"

        me_percent_used = max(0.0, min(float(me_percent_used), 100.0))
        te_percent_used = max(0.0, min(float(te_percent_used), 100.0))
        me_multiplier_used = max(0.0, min(1.0, 1.0 - (me_percent_used / 100.0)))
        te_multiplier_used = max(0.0, min(1.0, 1.0 - (te_percent_used / 100.0)))

        children: list[dict] = []
        child_inputs_me0_for_eiv: list[dict] = []
        for inp in mfg_materials:
            if not isinstance(inp, dict):
                continue
            inp_tid = inp.get("type_id")
            if inp_tid is None:
                continue
            per_run_qty = _safe_int(inp.get("quantity"), default=0)
            total_qty_me0 = int(per_run_qty) * int(runs_needed)
            if total_qty_me0 <= 0:
                continue

            raw_after_me = float(total_qty_me0) * float(me_multiplier_used) * (1.0 - mat_red)
            if total_qty_me0 > 0:
                total_qty_after_me = max(1, int(math.ceil(max(0.0, raw_after_me))))
            else:
                total_qty_after_me = 0
            if total_qty_after_me <= 0:
                continue

            child_inputs_me0_for_eiv.append({"type_id": int(inp_tid), "quantity": int(total_qty_me0)})

            child = _plan_one(
                int(inp_tid),
                int(total_qty_after_me),
                depth=depth + 1,
                path={*path, type_id_i},
            )
            child["type_name"] = str(inp.get("type_name") or "") or child.get("type_name")
            children.append(child)

        children_cost_known = all((c.get("effective_cost_isk") is not None) for c in children)
        children_cost = sum(float(c.get("effective_cost_isk") or 0.0) for c in children) if children_cost_known else None

        children_time_known = all((c.get("effective_time_seconds") is not None) for c in children)
        children_time_seconds = (
            sum(float(c.get("effective_time_seconds") or 0.0) for c in children) if children_time_known else None
        )

        mfg_time_per_run = 0.0
        try:
            mfg_time_per_run = float(((bp_info.get("manufacturing") or {}).get("time")) or 0.0)  # type: ignore[union-attr]
        except Exception:
            mfg_time_per_run = 0.0
        manufacturing_time_seconds = max(0.0, float(mfg_time_per_run) * float(runs_needed) * float(te_multiplier_used))
        manufacturing_time_seconds *= (1.0 - time_red)

        eiv_total = _compute_eiv_total_for_inputs(child_inputs_me0_for_eiv)
        mfg_job_fee = _estimate_job_fee_from_eiv(eiv_total, ci=float(manufacturing_system_cost_index or 0.0))

        total_build_cost = None
        if children_cost is not None:
            total_build_cost = float(children_cost) + (float(mfg_job_fee) if mfg_job_fee is not None else 0.0)

        owns_bp = blueprint_type_id in owned_bps
        bpo_buy_cost = _blueprint_bpo_buy_cost(blueprint_type_id) if not owns_bp else None

        max_runs = _safe_int(bp_info.get("max_production_limit"), default=0) if isinstance(bp_info, dict) else 0
        copying_time_s = float(bp_info.get("copying", 0) or 0.0) if isinstance(bp_info, dict) else 0.0
        research_me_time_s = float(bp_info.get("research_material", 0) or 0.0) if isinstance(bp_info, dict) else 0.0
        research_te_time_s = float(bp_info.get("research_time", 0) or 0.0) if isinstance(bp_info, dict) else 0.0

        copy_overhead: dict[str, Any] | None = None
        copy_fee_included = False
        copy_time_included = False
        copy_time_seconds_included = 0.0
        if copying_time_s > 0 and runs_needed > 0:
            run_ratio = 1.0
            if max_runs > 0:
                run_ratio = max(0.0, min(1.0, float(runs_needed) / float(max_runs)))
            est_copy_time = float(copying_time_s) * float(run_ratio)
            est_copy_time *= (1.0 - time_red)
            copy_fee = _estimate_job_fee_from_eiv(eiv_total, ci=float(copying_system_cost_index or 0.0))
            copy_overhead = {
                "assumption": "Estimated copy overhead for creating a BPC with enough runs.",
                "max_production_limit": int(max_runs),
                "estimated_copy_time_seconds": float(est_copy_time),
                "estimated_copy_fee_isk": float(copy_fee) if copy_fee is not None else None,
            }

            if not owns_bp:
                if total_build_cost is not None and copy_fee is not None:
                    total_build_cost = float(total_build_cost) + float(copy_fee)
                    copy_fee_included = True
                if copy_overhead.get("estimated_copy_time_seconds") is not None:
                    copy_time_included = True
                    try:
                        copy_time_seconds_included = float(copy_overhead.get("estimated_copy_time_seconds") or 0.0)
                    except Exception:
                        copy_time_seconds_included = 0.0

        research_overhead: dict[str, Any] | None = None
        if (research_me_time_s > 0 or research_te_time_s > 0) and runs_needed > 0:
            me_fee = _estimate_job_fee_from_eiv(eiv_total, ci=float(research_me_system_cost_index or 0.0))
            te_fee = _estimate_job_fee_from_eiv(eiv_total, ci=float(research_te_system_cost_index or 0.0))
            research_overhead = {
                "assumption": "Estimated ME/TE research overhead for the BPO; informational only.",
                "estimated_research_me_time_seconds": float(research_me_time_s) * (1.0 - time_red),
                "estimated_research_te_time_seconds": float(research_te_time_s) * (1.0 - time_red),
                "estimated_research_me_fee_isk": float(me_fee) if me_fee is not None else None,
                "estimated_research_te_fee_isk": float(te_fee) if te_fee is not None else None,
            }

        total_build_time_seconds = None
        if children_time_seconds is not None:
            total_build_time_seconds = float(children_time_seconds) + float(manufacturing_time_seconds)
            if copy_time_included:
                total_build_time_seconds += float(copy_time_seconds_included)

        node["children"] = children
        node["build"] = {
            "blueprint_type_id": int(blueprint_type_id),
            "blueprint_type_name": str(best.get("blueprint_type_name") or blueprint_type_id),
            "runs_needed": int(runs_needed),
            "product_quantity_per_run": int(output_per_run),
            "output_total": int(output_total),
            "blueprint_efficiency": {
                "me_percent": float(me_percent_used),
                "te_percent": float(te_percent_used),
                "source": str(eff_source),
                "owned_is_blueprint_copy": owned_is_bpc,
                "owned_runs": owned_runs,
                "note": "ME/TE affects materials/time estimates. Job fee estimates remain ME0-based (EIV/client behavior).",
            },
            "children_cost_isk": float(children_cost) if children_cost is not None else None,
            "estimated_manufacturing_job_fee_isk": float(mfg_job_fee) if mfg_job_fee is not None else None,
            "total_build_cost_isk": float(total_build_cost) if total_build_cost is not None else None,
            "children_time_seconds": float(children_time_seconds) if children_time_seconds is not None else None,
            "manufacturing_time_seconds": float(manufacturing_time_seconds),
            "total_build_time_seconds": float(total_build_time_seconds) if total_build_time_seconds is not None else None,
            "blueprint_owned": bool(owns_bp),
            "blueprint_acquisition_included_in_total_build_cost": False,
            "copy_overhead_included_in_total_build_cost": bool(copy_fee_included),
            "copy_overhead_included_in_total_time": bool(copy_time_included),
            "blueprint_bpo_buy_cost_isk": float(bpo_buy_cost) if bpo_buy_cost is not None else None,
            "research_overhead": research_overhead,
            "copy_overhead": copy_overhead,
        }

        if buy_cost is not None and total_build_cost is not None:
            savings = float(buy_cost) - float(total_build_cost)
            rec = "build" if savings > 0 else "buy"
            node["recommendation"] = ("take" if take_possible else "buy") if rec == "buy" else rec
            node["savings_isk"] = float(savings)
            node["effective_cost_isk"] = float(total_build_cost) if rec == "build" else float(buy_cost)
            node["effective_time_seconds"] = (
                float(total_build_time_seconds)
                if (rec == "build" and total_build_time_seconds is not None)
                else 0.0
            )
        else:
            if total_build_cost is not None and buy_cost is None:
                node["recommendation"] = "build"
                node["effective_cost_isk"] = float(total_build_cost)
                node["effective_time_seconds"] = (
                    float(total_build_time_seconds) if total_build_time_seconds is not None else None
                )
                node["reason"] = "buy_price_missing"
            else:
                node["recommendation"] = ("take" if take_possible else "buy") if buy_cost is not None else None
                node["effective_cost_isk"] = buy_cost
                node["effective_time_seconds"] = 0.0 if buy_cost is not None else None
                if node.get("reason") is None:
                    node["reason"] = "insufficient_price_data"

        return node

    roots: list[dict] = []
    for mat in list(materials or []):
        if not isinstance(mat, dict):
            continue
        tid = mat.get("type_id")
        if tid is None:
            continue
        qty = mat.get("quantity")
        if qty is None:
            qty = mat.get("quantity_after_efficiency")
        if qty is None:
            qty = mat.get("quantity_me0")
        try:
            type_id_i = int(tid)
            qty_i = int(qty or 0)
        except Exception:
            continue
        if qty_i <= 0:
            continue
        root = _plan_one(type_id_i, qty_i, depth=0, path=set())
        root["type_name"] = str(mat.get("type_name") or "") or root.get("type_name")
        roots.append(root)

    return roots
