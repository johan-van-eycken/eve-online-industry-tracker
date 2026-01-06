import math
from utils.ore_skill_map import get_processing_skill_for_ore

def compute_actual_yields(ore, skills, facility, batch_size):
    """
    Returns dict of {material: yield_per_batch} after applying skills/facility/waste.
    Rounds down to nearest integer (EVE rules).
    """
    yields = {}
    for material, base_qty in ore['batch_yields'].items():
        # Use specific ore processing skill if available
        ore_skill_name = get_processing_skill_for_ore(ore.get('type_name', ''))
        specific_skill = skills.get(ore_skill_name, 0) if ore_skill_name else 0

        multiplier = facility * (1 + 0.02 * skills.get('Reprocessing', 0)) * (1 + 0.02 * skills.get('Reprocessing Efficiency', 0))
        multiplier *= (1 + 0.02 * specific_skill)
        waste_factor = ore.get('waste_factor', 0.1)
        yield_qty = math.floor(base_qty * multiplier * batch_size * (1 - waste_factor))
        if yield_qty > 0:
            yields[material] = yield_qty
    return yields

def weighted_cost_allocation(ore_price, yields, market_prices):
    """
    Allocates ore cost to materials based on market value weighting.
    Returns dict {material: allocated_cost_per_unit}
    """
    total_value = sum(market_prices.get(m, 0) * qty for m, qty in yields.items())
    if total_value == 0:
        return {m: float('inf') for m in yields}
    allocation = {}
    for m, qty in yields.items():
        value = market_prices.get(m, 0) * qty
        allocation[m] = (value / total_value) * ore_price / qty if qty > 0 else float('inf')
    return allocation

def filter_viable_ores(ores, market_prices, skills, facility, batch_size, strict=True, slack=0.0):
    """
    Returns list of viable ores, each with actual yields and allocated costs per material.
    """
    viable = []
    for ore in ores:
        yields = compute_actual_yields(ore, skills, facility, batch_size)
        ore_price = ore.get('market_price', None)
        if ore_price is None or not yields:
            continue
        for mineral, qty in yields.items():
            if qty > 0 and mineral in market_prices:
                cost_per_unit = ore_price / qty
                if not strict or cost_per_unit < market_prices[mineral] + slack:
                    ore_out = ore.copy()
                    ore_out['actual_yields'] = yields
                    ore_out['cost_per_unit'] = cost_per_unit
                    viable.append(ore_out)
                    break  # Only need one viable mineral per ore
    return viable

def prune_order_ladder(order_ladder, demand, buffer=1.05):
    """
    Prune market order ladder to only enough depth to cover demand * buffer.
    """
    pruned = []
    cumulative = 0
    for order in sorted(order_ladder, key=lambda o: o['price']):
        if cumulative >= demand * buffer:
            break
        take = min(order['volume_remain'], demand * buffer - cumulative)
        pruned.append({**order, 'volume_remain': take})
        cumulative += take
    return pruned