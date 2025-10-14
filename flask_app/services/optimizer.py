from math import floor
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, LpInteger, PULP_CBC_CMD, LpBinary

def optimize_ore_tiered(
    demands,
    ores,
    materials,
    order_book,
    max_ore_types=None  # <-- add this parameter
):
    """
    demands: { material_name: qty }
    ores: list[{ id, name, batch_size, batch_yields }]
    order_book: { ore_id: [ {price, volume_remain, ...}, ... ] } (sell orders asc price)
    Decision vars:
       z_{o,k} = integer number of batches bought from order k of ore o
    Constraints:
       For each order tier: 0 <= z_{o,k} <= max_batches_{o,k}
       Mineral coverage: sum_o,k z_{o,k} * batch_yields_o[m] - s_m >= demand_m
    Cost:
       sum_o,k z_{o,k} * batch_size_o * price_{o,k}
    """
    prob = LpProblem("TieredOreOptimization", LpMinimize)

    # Preprocess order book
    order_book = _prune_and_merge_order_book(order_book, ores, demands)

    # Binary ore selection variables
    y = {o["id"]: LpVariable(f"y_{o['id']}", 0, 1, LpBinary) for o in ores}

    # Build tier variables and caps
    tier_vars = {}
    for o in ores:
        oid = o["id"]
        batch_size = o["batch_size"]
        tiers = order_book.get(oid, [])
        tier_vars[oid] = []
        # skip ores with zero yields across all demanded materials
        if all(o["batch_yields"].get(m, 0) == 0 for m in demands.keys()):
            continue
        for idx, tier in enumerate(tiers):
            max_batches = int(tier["volume_remain"] // batch_size) if batch_size > 0 else 0
            if max_batches <= 0:
                continue
            var = LpVariable(f"z_{oid}_{idx}", lowBound=0, upBound=max_batches, cat=LpInteger)
            tier_vars[oid].append({
                "var": var,
                "price": tier["price"],
                "batch_size": batch_size,
                "order_id": tier["order_id"],
                "max_batches": max_batches
            })
            # Linking constraint: z <= M * y
            prob += var <= max_batches * y[oid]

    # Surplus variables
    s = {m: LpVariable(f"s_{m}", lowBound=0) for m in materials}

    # Coverage constraints
    # Build quick lookup: yields[ore_id][material]
    yield_map = {o["id"]: o["batch_yields"] for o in ores}

    for m in materials:
        prob += (
            lpSum(tv["var"] * yield_map[oid].get(m, 0)
                  for oid, tvs in tier_vars.items()
                  for tv in tvs)
            - s[m] >= demands.get(m, 0)
        )

   # Objective
    cost_terms = []
    for oid, tvs in tier_vars.items():
        for tv in tvs:
            cost_terms.append(tv["var"] * tv["batch_size"] * tv["price"])

    base_cost = lpSum(cost_terms)

    # Surplus handling
    penalty = 1e-6
    objective = base_cost + penalty * lpSum(s[m] for m in materials)
    prob += objective

    # Add max ore types constraint if requested
    if max_ore_types is not None:
        prob += lpSum(y[oid] for oid in y) <= max_ore_types

    # Choose CBC solver with relative gap + time limit (fallback if older PuLP)
    try:
        solver = PULP_CBC_CMD(msg=True, gapRel=0.001, timeLimit=30)  # 0.1% relative gap
    except TypeError:
        # Older PuLP versions: gapRel not supported
        solver = PULP_CBC_CMD(msg=True, timeLimit=30)
    prob.solve(solver)

    status = LpStatus[prob.status]
    if status != "Optimal" and status != "Integer Feasible":
        return {"status": "failed", "reason": status}

    # Build solution aggregation
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
                tiers_used.append({
                    "order_id": tv["order_id"],
                    "batches": val_int,
                    "ore_units": ore_units,
                    "unit_price": tv["price"],
                    "cost": tier_cost
                })
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
                "selected": int(y[oid].value() or 0)
            }

    total_cost = sum(o["cost"] for o in per_ore.values())
    surplus_out = {m: s[m].value() for m in materials}

    return {
        "status": "ok",
        "total_cost": total_cost,
        "solution": list(per_ore.values()),
        "surplus": surplus_out,
        "pricing_mode": "tiered_orders",
    }

def _prune_and_merge_order_book(order_book, ores, demands, safety_factor=1.05):
    """
    order_book: { ore_id: [ {price, volume_remain, ...}, ... ] } (sorted asc price)
    Returns reduced order_book.
    - Keeps only enough cumulative volume (in ore units) to cover material demands * safety_factor
      using a heuristic upper bound (ignores yields, just ore volume proxy).
    - Merges consecutive tiers with identical price for same ore.
    """
    # Rough required ore units upper bound:
    total_demand_units = sum(demands.values())
    # Find average Tritanium equivalent yield just as proxy if available
    reduced = {}
    for ore_id, tiers in order_book.items():
        if not tiers:
            continue
        # Merge identical price tiers
        merged = []
        for t in tiers:
            if merged and merged[-1]["price"] == t["price"]:
                merged[-1]["volume_remain"] += t.get("volume_remain", 0)
            else:
                merged.append(dict(t))  # copy
        # Optional pruning: keep cumulative volume up to some cap
        cumulative = 0
        kept = []
        for t in merged:
            kept.append(t)
            cumulative += t.get("volume_remain", 0)
            if cumulative >= total_demand_units * safety_factor:
                break
        reduced[ore_id] = kept
    return reduced