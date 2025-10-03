from math import floor
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, LpInteger, PULP_CBC_CMD, LpBinary

def optimize_ore_tiered(
    demands,
    ores,
    minerals,
    order_book,
    resale=None,
    surplus_penalty=0.0,
    max_ores=None,              # NEW: hard cap on distinct ores
    sparsity_penalty=0.0        # NEW: cost added per ore used
):
    """
    demands: { mineral_name: qty }
    ores: list[{ id, name, portionSize, batch_yields }]
    order_book: { ore_id: [ {price, volume_remain, ...}, ... ] } (sell orders asc price)
    Decision vars:
       z_{o,k} = integer number of batches bought from order k of ore o
    Constraints:
       For each order tier: 0 <= z_{o,k} <= max_batches_{o,k}
       Mineral coverage: sum_o,k z_{o,k} * batch_yields_o[m] - s_m >= demand_m
    Cost:
       sum_o,k z_{o,k} * portionSize_o * price_{o,k}  (+ surplus penalty or - resale)
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
        portion = o["portionSize"]
        tiers = order_book.get(oid, [])
        tier_vars[oid] = []
        # skip ores with zero yields across all demanded minerals
        if all(o["batch_yields"].get(m, 0) == 0 for m in demands.keys()):
            continue
        for idx, tier in enumerate(tiers):
            max_batches = int(tier["volume_remain"] // portion) if portion > 0 else 0
            if max_batches <= 0:
                continue
            var = LpVariable(f"z_{oid}_{idx}", lowBound=0, upBound=max_batches, cat=LpInteger)
            tier_vars[oid].append({
                "var": var,
                "price": tier["price"],
                "portionSize": portion,
                "order_id": tier["order_id"],
                "max_batches": max_batches
            })
            # Linking constraint: z <= M * y
            prob += var <= max_batches * y[oid]

    # Surplus variables
    s = {m: LpVariable(f"s_{m}", lowBound=0) for m in minerals}

    # Coverage constraints
    # Build quick lookup: yields[ore_id][mineral]
    yield_map = {o["id"]: o["batch_yields"] for o in ores}

    for m in minerals:
        prob += (
            lpSum(tv["var"] * yield_map[oid].get(m, 0)
                  for oid, tvs in tier_vars.items()
                  for tv in tvs)
            - s[m] >= demands.get(m, 0)
        )

    # Max distinct ores (hard cap)
    if max_ores is not None and max_ores > 0:
        prob += lpSum(y.values()) <= max_ores

    # Objective
    cost_terms = []
    for oid, tvs in tier_vars.items():
        for tv in tvs:
            cost_terms.append(tv["var"] * tv["portionSize"] * tv["price"])

    base_cost = lpSum(cost_terms)

    # Surplus handling
    if resale:
        resale_value = lpSum(s[m] * resale.get(m, 0) for m in minerals)
        objective = base_cost - resale_value
    else:
        objective = base_cost + surplus_penalty * lpSum(s[m] for m in minerals)

    # Add sparsity penalty (soft encouragement to use fewer ores)
    if sparsity_penalty > 0:
        objective += sparsity_penalty * lpSum(y.values())

    prob += objective
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
        portion = o["portionSize"]
        batches_total = 0
        ore_units_total = 0
        cost_total = 0.0
        tiers_used = []
        for tv in tier_vars[oid]:
            val = tv["var"].value()
            if val and val > 0:
                val_int = int(val)
                ore_units = val_int * portion
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
                "portionSize": portion,
                "batches": batches_total,
                "ore_units": ore_units_total,
                "cost": cost_total,
                "avg_unit_price": cost_total / ore_units_total if ore_units_total else None,
                "tiers": tiers_used,
                "selected": int(y[oid].value() or 0)
            }

    total_cost = sum(o["cost"] for o in per_ore.values())
    surplus_out = {m: s[m].value() for m in minerals}
    distinct_used = sum(int(yv.value() or 0) for yv in y.values())

    return {
        "status": "ok",
        "total_cost": total_cost,
        "solution": list(per_ore.values()),
        "surplus": surplus_out,
        "pricing_mode": "tiered_orders",
        "distinct_ores": distinct_used
    }

def _prune_and_merge_order_book(order_book, ores, demands, safety_factor=1.05):
    """
    order_book: { ore_id: [ {price, volume_remain, ...}, ... ] } (sorted asc price)
    Returns reduced order_book.
    - Keeps only enough cumulative volume (in ore units) to cover mineral demands * safety_factor
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