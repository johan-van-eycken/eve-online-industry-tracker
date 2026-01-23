from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.esi_service import ESIService

from eve_online_industry_tracker.infrastructure.static_data.facility_repo import get_facility
from eve_online_industry_tracker.infrastructure.sde.static_data import build_all_materials, build_all_ores
from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields
from eve_online_industry_tracker.infrastructure.static_data.optimizer import optimize_ore_tiered


def run_optimize(
    payload: dict,
    *,
    character: Any,
    esi_service: ESIService,
    sde_session: Any,
    language: str,
) -> dict:
    demands = payload["demands"]
    implant_pct = payload.get("implant_pct", 0)
    facility_id = payload["facility_id"]
    opt_only_compressed = payload.get("only_compressed", False)

    skills = getattr(character, "reprocessing_skills", None) or character.extract_reprocessing_skills()
    implants = [{"slot": 7, "group": "reprocessing", "bonus": (implant_pct / 100)}]

    facility = get_facility(facility_id)

    ores = build_all_ores(sde_session, language)
    ore_yields = compute_yields(ores, skills, facility, implants)

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
        viable_ores = [o for o in viable_ores if "Compressed" in o["name"]]

    ore_ids = [o["id"] for o in viable_ores]
    processed_ore_prices = esi_service.get_ore_prices(ore_ids)
    order_book = {oid: processed_ore_prices.get(oid, []) for oid in ore_ids}

    result = optimize_ore_tiered(
        demands=demands,
        ores=viable_ores,
        materials=req_mats,
        order_book=order_book,
        max_ore_types=len(req_mats),
    )

    result["ore_yields"] = ore_yields
    result["tiered_total_cost"] = tiered_total_cost

    return result
