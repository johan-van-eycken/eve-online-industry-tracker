from __future__ import annotations

from pulp import (  # pyright: ignore[reportMissingImports]
    LpBinary,
    LpInteger,
    LpMinimize,
    LpProblem,
    LpStatus,
    LpVariable,
    PULP_CBC_CMD,
    lpSum,
)


def optimize_ore_tiered(
    demands,
    ores,
    materials,
    order_book,
    max_ore_types=None,
    mode="min_cost",
):
    prob = LpProblem("TieredOreOptimization", LpMinimize)

    order_book = _prune_and_merge_order_book(order_book, ores, demands)

    y = {o["id"]: LpVariable(f"y_{o['id']}", 0, 1, LpBinary) for o in ores}
    ore_batch_volume = {o["id"]: float(o.get("batch_volume", 0.0) or 0.0) for o in ores}

    tier_vars = {}
    for o in ores:
        oid = o["id"]
        batch_size = o["batch_size"]
        tiers = order_book.get(oid, [])
        tier_vars[oid] = []
        if all(o["batch_yields"].get(m, 0) == 0 for m in demands.keys()):
            continue
        for idx, tier in enumerate(tiers):
            max_batches = int(tier["volume_remain"] // batch_size) if batch_size > 0 else 0
            if max_batches <= 0:
                continue
            var = LpVariable(f"z_{oid}_{idx}", lowBound=0, upBound=max_batches, cat=LpInteger)
            tier_vars[oid].append(
                {
                    "var": var,
                    "price": tier["price"],
                    "batch_size": batch_size,
                    "order_id": tier["order_id"],
                    "max_batches": max_batches,
                }
            )
            prob += var <= max_batches * y[oid]

    s = {m: LpVariable(f"s_{m}", lowBound=0) for m in materials}

    yield_map = {o["id"]: o["batch_yields"] for o in ores}

    for m in materials:
        prob += (
            lpSum(tv["var"] * yield_map[oid].get(m, 0) for oid, tvs in tier_vars.items() for tv in tvs)
            - s[m]
            >= demands.get(m, 0)
        )

    cost_terms = []
    for oid, tvs in tier_vars.items():
        for tv in tvs:
            cost_terms.append(tv["var"] * tv["batch_size"] * tv["price"])

    base_cost = lpSum(cost_terms)
    total_ore_volume = lpSum(tv["var"] * ore_batch_volume.get(oid, 0.0) for oid, tvs in tier_vars.items() for tv in tvs)
    ore_type_count = lpSum(y[oid] for oid in y)

    penalty = 1e-6
    overflow_penalty = penalty * lpSum(s[m] for m in materials)

    if mode == "min_volume":
        objective = total_ore_volume + (1e-9 * base_cost) + overflow_penalty
    elif mode == "min_ore_types":
        objective = ore_type_count + (1e-9 * base_cost) + overflow_penalty
    elif mode == "balanced":
        cost_upper_bound = sum(tv["max_batches"] * tv["batch_size"] * tv["price"] for tvs in tier_vars.values() for tv in tvs)
        volume_upper_bound = sum(tv["max_batches"] * ore_batch_volume.get(oid, 0.0) for oid, tvs in tier_vars.items() for tv in tvs)
        normalized_cost_weight = 1.0 / max(float(cost_upper_bound or 0.0), 1.0)
        normalized_volume_weight = 1.0 / max(float(volume_upper_bound or 0.0), 1.0)
        objective = (normalized_cost_weight * base_cost) + (normalized_volume_weight * total_ore_volume) + overflow_penalty
    else:
        objective = base_cost + overflow_penalty
    prob += objective

    if max_ore_types is not None:
        prob += lpSum(y[oid] for oid in y) <= max_ore_types

    try:
        solver = PULP_CBC_CMD(msg=True, gapRel=0.001, timeLimit=30)
    except TypeError:
        solver = PULP_CBC_CMD(msg=True, timeLimit=30)
    prob.solve(solver)

    status = LpStatus[prob.status]
    if status != "Optimal" and status != "Integer Feasible":
        return {"status": "failed", "reason": status}

    per_ore = {}
    for o in ores:
        oid = o["id"]
        if oid not in tier_vars:
            continue
        batch_size = o["batch_size"]
        batches_total = 0
        ore_units_total = 0
        cost_total = 0.0
        tiers_used = []
        for tv in tier_vars[oid]:
            val = tv["var"].value()
            if val and val > 0:
                val_int = int(val)
                ore_units = val_int * batch_size
                tier_cost = ore_units * tv["price"]
                batches_total += val_int
                ore_units_total += ore_units
                cost_total += tier_cost
                tiers_used.append(
                    {
                        "order_id": tv["order_id"],
                        "location_id": tv.get("location_id"),
                        "batches": val_int,
                        "ore_units": ore_units,
                        "unit_price": tv["price"],
                        "cost": tier_cost,
                    }
                )
        if batches_total > 0:
            per_ore[oid] = {
                "ore_id": oid,
                "ore_name": o["name"],
                "batch_size": batch_size,
                "batches": batches_total,
                "ore_units": ore_units_total,
                "cost": cost_total,
                "avg_unit_price": cost_total / ore_units_total if ore_units_total else None,
                "tiers": tiers_used,
                "selected": int(y[oid].value() or 0),
            }

    total_cost = sum(o["cost"] for o in per_ore.values())
    surplus_out = {m: s[m].value() for m in materials}
    total_volume_m3 = sum(ore_batch_volume.get(oid, 0.0) * data["batches"] for oid, data in per_ore.items())

    return {
        "status": "ok",
        "total_cost": total_cost,
        "total_ore_volume_m3": total_volume_m3,
        "selected_ore_type_count": sum(1 for data in per_ore.values() if data.get("selected")),
        "solution": list(per_ore.values()),
        "surplus": surplus_out,
        "pricing_mode": "tiered_orders",
        "optimization_mode": str(mode),
    }


def _prune_and_merge_order_book(order_book, ores, demands, safety_factor=1.05):
    total_demand_units = sum(demands.values())
    reduced = {}
    for ore_id, tiers in order_book.items():
        if not tiers:
            continue
        merged = []
        for t in tiers:
            if merged and merged[-1]["price"] == t["price"]:
                merged[-1]["volume_remain"] += t.get("volume_remain", 0)
            else:
                merged.append(dict(t))
        cumulative = 0
        kept = []
        for t in merged:
            kept.append(t)
            cumulative += t.get("volume_remain", 0)
            if cumulative >= total_demand_units * safety_factor:
                break
        reduced[ore_id] = kept
    return reduced
