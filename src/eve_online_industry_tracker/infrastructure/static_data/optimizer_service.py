from __future__ import annotations

import logging
import time
from typing import Any

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.esi_service import ESIService

from eve_online_industry_tracker.infrastructure.static_data.facility_repo import (
    CONFIGURED_FACILITY_ID,
    configured_facility,
)
from eve_online_industry_tracker.infrastructure.sde.static_data import build_all_materials, build_all_ores
from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields
from eve_online_industry_tracker.infrastructure.static_data.optimizer import optimize_ore_tiered

logger = logging.getLogger(__name__)


def _build_raw_comparator_rows(
    *,
    demands: dict[str, Any],
    material_rows: list[dict[str, Any]],
    material_prices: dict[str, float | None],
) -> list[dict[str, Any]]:
    material_map = {str(row.get("name") or ""): row for row in material_rows if isinstance(row, dict)}
    rows: list[dict[str, Any]] = []
    for material_name, quantity in demands.items():
        unit_price = material_prices.get(material_name)
        material_row = material_map.get(str(material_name), {})
        material_volume = float(material_row.get("volume") or 0.0)
        rows.append(
            {
                "Material": str(material_name),
                "quantity": float(quantity or 0),
                "Unit Price (ISK)": unit_price,
                "Total Cost (ISK)": (float(quantity or 0) * float(unit_price or 0.0)) if unit_price is not None else None,
                "Volume (m3)": float(quantity or 0) * material_volume,
            }
        )
    return rows


def _build_demand_coverage(
    *,
    demands: dict[str, Any],
    ore_yields_map: dict[int, dict[str, Any]],
    solution: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    yielded_by_material: dict[str, float] = {str(material): 0.0 for material in demands.keys()}
    for row in solution:
        ore_info = ore_yields_map.get(int(row.get("ore_id") or 0), {})
        batches = float(row.get("batches") or 0.0)
        for material_name, per_batch in (ore_info.get("batch_yields") or {}).items():
            yielded_by_material[str(material_name)] = yielded_by_material.get(str(material_name), 0.0) + (float(per_batch or 0.0) * batches)

    coverage: dict[str, dict[str, float]] = {}
    for material_name, demand in demands.items():
        demand_value = float(demand or 0.0)
        yielded = float(yielded_by_material.get(str(material_name), 0.0))
        surplus = max(yielded - demand_value, 0.0)
        shortfall = max(demand_value - yielded, 0.0)
        coverage[str(material_name)] = {
            "demand": demand_value,
            "yielded": yielded,
            "surplus": surplus,
            "shortfall": shortfall,
        }
    return coverage


def _build_effective_contribution_rows(
    *,
    demands: dict[str, Any],
    demand_coverage: dict[str, dict[str, float]],
    ore_yields_map: dict[int, dict[str, Any]],
    solution: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in solution:
        ore_id = int(row.get("ore_id") or 0)
        ore_info = ore_yields_map.get(ore_id, {})
        batches = float(row.get("batches") or 0.0)
        for material_name, per_batch in (ore_info.get("batch_yields") or {}).items():
            total_yield = float(per_batch or 0.0) * batches
            demand = float(demands.get(str(material_name), 0.0) or 0.0)
            total_yielded = float((demand_coverage.get(str(material_name), {}) or {}).get("yielded", 0.0) or 0.0)
            rows.append(
                {
                    "Ore": str(row.get("ore_name") or ore_id),
                    "Material": str(material_name),
                    "Batches": batches,
                    "Yield per Batch": float(per_batch or 0.0),
                    "Total Yield": total_yield,
                    "Demand": demand,
                    "Coverage %": ((total_yield / demand) * 100.0) if demand > 0 else 0.0,
                    "Share of Yielded %": ((total_yield / total_yielded) * 100.0) if total_yielded > 0 else 0.0,
                }
            )
    return rows


def _build_price_provenance(
    *,
    esi_service: ESIService,
    ore_ids: list[int],
    material_ids: list[int],
    material_rows: list[dict[str, Any]],
    material_prices_by_name: dict[str, float | None],
    solution: list[dict[str, Any]],
) -> dict[str, Any]:
    ore_metadata = esi_service.get_sell_order_book_metadata(ore_ids)
    material_metadata = esi_service.get_sell_order_book_metadata(material_ids)
    material_rows_by_name = {str(row.get("name") or ""): row for row in material_rows if isinstance(row, dict)}

    material_price_rows = []
    for material_name, unit_price in material_prices_by_name.items():
        material_row = material_rows_by_name.get(str(material_name), {})
        material_price_rows.append(
            {
                "Type": "Material",
                "Name": str(material_name),
                "Type ID": int(material_row.get("id") or 0),
                "Unit Price (ISK)": unit_price,
            }
        )

    ore_price_rows = []
    for row in solution:
        tiers = row.get("tiers") or []
        best_price = None
        if tiers:
            first_tier = tiers[0] if isinstance(tiers[0], dict) else {}
            best_price = first_tier.get("unit_price")
        ore_price_rows.append(
            {
                "Type": "Ore",
                "Name": str(row.get("ore_name") or row.get("ore_id") or ""),
                "Type ID": int(row.get("ore_id") or 0),
                "Unit Price (ISK)": best_price,
            }
        )

    return {
        "source": "ESI sell orders",
        "region_id": int(ore_metadata.get("region_id") or material_metadata.get("region_id") or 0),
        "generated_at": time.time(),
        "ore_orders": ore_metadata,
        "material_orders": material_metadata,
        "price_rows": [*material_price_rows, *ore_price_rows],
    }


def _adjusted_prices(esi_service: ESIService, type_ids: list[int]) -> dict[int, float]:
    """{type id: adjusted_price} from ESI /markets/prices, the source the
    industry job cost uses (ESIService.get_market_prices, read by
    IndustryService._get_adjusted_market_price_map). A type without an
    adjusted price is left out (no average or base price stands in)."""
    wanted = {int(t) for t in type_ids}
    out: dict[int, float] = {}
    for row in esi_service.get_market_prices() or []:
        if not isinstance(row, dict) or row.get("adjusted_price") is None:
            continue
        try:
            type_id = int(row.get("type_id"))
        except (TypeError, ValueError):
            continue
        if type_id in wanted:
            out[type_id] = float(row["adjusted_price"])
    return out


def _reprocessing_fee(
    *,
    tax: float,
    demand_coverage: dict[str, dict[str, float]],
    material_ids: dict[str, int],
    adjusted_prices: dict[int, float],
) -> tuple[float | None, str | None, dict[str, dict[str, float]]]:
    """(fee, reason, basis): the facility's tax on every mineral produced
    (surplus included) at its adjusted price (ESI /markets/prices
    adjusted_price), x the refined quantity x the tax.

    That EVE charges the reprocessing tax on the adjusted price, the price
    the industry job cost uses, rests on community testing, not on an
    official CCP statement. (None, reason, {}) when a produced mineral has
    no adjusted price. basis is {mineral: {"adjusted_price", "quantity"}}."""
    produced = {m: float(c.get("yielded") or 0.0) for m, c in demand_coverage.items()
                if float(c.get("yielded") or 0.0) > 0}
    unpriced = sorted(m for m in produced
                      if material_ids.get(m) is None or adjusted_prices.get(material_ids[m]) is None)
    if unpriced:
        return None, (f"no ESI adjusted price for {', '.join(unpriced)}: the reprocessing fee is unknown"), {}
    basis = {m: {"adjusted_price": adjusted_prices[material_ids[m]], "quantity": quantity}
             for m, quantity in produced.items()}
    value = sum(b["adjusted_price"] * b["quantity"] for b in basis.values())
    return value * tax, None, basis


def run_optimize(
    payload: dict,
    *,
    character: Any,
    esi_service: ESIService,
    sde_session: Any,
    language: str,
    admin_settings: Any,
) -> dict:
    demands = payload["demands"]
    implant_pct = payload.get("implant_pct", 0)
    facility_id = payload["facility_id"]
    opt_only_compressed = payload.get("only_compressed", False)
    optimization_mode = str(payload.get("mode") or "min_cost")

    if facility_id != CONFIGURED_FACILITY_ID:
        raise ServiceError(f"Unknown facility_id {facility_id!r}: the ore calculator has one facility "
                           f"({CONFIGURED_FACILITY_ID}), configured in the admin settings", status_code=400)
    facility = configured_facility(admin_settings, sde_session, language=language)

    # {skill type id: level} (ore.refining_skill_levels).
    skills = getattr(character, "reprocessing_skills", None) or character.extract_reprocessing_skills()

    ores = build_all_ores(sde_session, language)
    ore_yields = compute_yields(ores, skills, facility, float(implant_pct) / 100.0)

    req_mats = set(demands.keys())
    ore_yields = [
        o
        for o in ore_yields
        if set(o["batch_yields"].keys()).issubset(req_mats) and len(o["batch_yields"].keys()) > 0
    ]

    materials = build_all_materials(sde_session, language)
    req_mat_ids = [m["id"] for m in materials if m["name"] in req_mats]
    raw_req_mat_prices = esi_service.get_material_prices(req_mat_ids)
    req_mat_prices = {
        m: raw_req_mat_prices.get(m, [{}])[0].get("price", None)
        for m in req_mat_ids
        if m in raw_req_mat_prices and raw_req_mat_prices[m]
    }

    mat_name_to_price: dict[str, float | None] = {}
    for m in materials:
        if m["name"] in req_mats:
            mat_name_to_price[m["name"]] = req_mat_prices.get(m["id"], None)

    tiered_total_cost = 0.0
    for mat, qty in demands.items():
        price = mat_name_to_price.get(mat)
        if price is None:
            price = req_mat_prices.get(mat, None)
        if price is not None:
            tiered_total_cost += qty * price

    viable_ores = [
        o
        for o in ore_yields
        if any(m in req_mats for m in o["batch_yields"].keys()) and len(o["batch_yields"].keys()) > 0
    ]
    if opt_only_compressed:
        # The SDE's compressibleTypes decides (build_all_ores "compressed"), not the name.
        viable_ores = [o for o in viable_ores if o.get("compressed")]

    ore_ids = [o["id"] for o in viable_ores]
    processed_ore_prices = esi_service.get_ore_prices(ore_ids)
    order_book = {oid: processed_ore_prices.get(oid, []) for oid in ore_ids}

    result = optimize_ore_tiered(
        demands=demands,
        ores=viable_ores,
        materials=req_mats,
        order_book=order_book,
        max_ore_types=len(req_mats),
        mode=optimization_mode,
    )

    if result.get("status") != "ok":
        return result

    ore_yields_map = {int(ore.get("id") or 0): ore for ore in viable_ores}
    demand_coverage = _build_demand_coverage(
        demands=demands,
        ore_yields_map=ore_yields_map,
        solution=result.get("solution", []),
    )
    effective_contributions = _build_effective_contribution_rows(
        demands=demands,
        demand_coverage=demand_coverage,
        ore_yields_map=ore_yields_map,
        solution=result.get("solution", []),
    )
    raw_comparator = _build_raw_comparator_rows(
        demands=demands,
        material_rows=materials,
        material_prices=mat_name_to_price,
    )

    raw_total_volume = sum(float(row.get("Volume (m3)") or 0.0) for row in raw_comparator)
    surplus = result.get("surplus") or {}
    resale = {material_name: (float(mat_name_to_price.get(material_name) or 0.0) * 0.8) for material_name in surplus.keys()}
    result["raw_comparator"] = raw_comparator
    result["effective_contributions"] = effective_contributions
    result["demand_coverage"] = demand_coverage
    result["raw_total_volume"] = raw_total_volume
    result["total_raw_volume"] = raw_total_volume
    material_ids = {str(m["name"]): int(m["id"]) for m in materials if m["name"] in req_mats}
    try:
        adjusted_prices = _adjusted_prices(esi_service, list(material_ids.values()))
    except RuntimeError as exc:   # ESIService.get_market_prices: the request failed
        logger.warning("Ore calculator: ESI adjusted prices unavailable, the reprocessing fee is unknown: %s", exc)
        fee, fee_unknown_reason, fee_basis = None, f"ESI adjusted prices unavailable ({exc}): the reprocessing fee is unknown", {}
    else:
        fee, fee_unknown_reason, fee_basis = _reprocessing_fee(
            tax=facility.tax, demand_coverage=demand_coverage, material_ids=material_ids,
            adjusted_prices=adjusted_prices)
    result["reprocessing_fee"] = fee
    result["reprocessing_fee_unknown_reason"] = fee_unknown_reason
    result["reprocessing_fee_basis"] = fee_basis
    result["total_cost_with_reprocessing"] = (
        float(result.get("total_cost") or 0.0) + fee if fee is not None else None)
    result["facility"] = {"name": facility.name, "tax": facility.tax, "tax_source": facility.tax_source,
                          "multipliers": {cls: facility.multiplier_for(cls) for cls in facility.yields}}
    result["resale"] = resale
    result["resale_toggle"] = True
    result["price_provenance"] = _build_price_provenance(
        esi_service=esi_service,
        ore_ids=ore_ids,
        material_ids=req_mat_ids,
        material_rows=materials,
        material_prices_by_name=mat_name_to_price,
        solution=result.get("solution", []),
    )

    result["ore_yields"] = ore_yields
    result["tiered_total_cost"] = tiered_total_cost

    return result
