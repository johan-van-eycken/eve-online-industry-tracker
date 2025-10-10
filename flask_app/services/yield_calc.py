from utils.ore_skill_map import get_processing_skill_for_ore

def compute_yields(ores, char_skills, facility, implants):
    refining = char_skills.get("Refining", 0)
    re_eff = char_skills.get("Reprocessing Efficiency", 0)
    facility_base = facility.get("base_yield", 0.5)
    rig_bonus = facility.get("rig_bonus", 0.0)
    structure_bonus = facility.get("structure_bonus", 0.0)
    implant_bonus = sum(i.get("bonus", 0.0) for i in implants if i.get("group") == "reprocessing")
    
    results = []
    for o in ores:
        portion = o.get("portionSize", 100) or 100
        skill = get_processing_skill_for_ore(o["name"])
        if not skill:
            print(f"  WARNING: No skill mapping found for ore '{o['name']}'")
            print(f"  Required skill: {skill}")
            continue
        
        ore_skill = char_skills.get(skill, 0)

        mult = facility_base
        mult *= (1 + 0.02 * refining)
        mult *= (1 + 0.02 * re_eff)
        mult *= (1 + 0.02 * ore_skill)
        mult *= (1 + rig_bonus + structure_bonus + implant_bonus)

        # Per BATCH (portion) yield: CCP quantities are per portion already
        batch_yields = {}
        for m in o["materials"]:
            qty_per_portion = m["quantity"]
            per_batch = qty_per_portion * mult      # DO NOT divide by portionSize
            batch_yields[m["name"]] = per_batch

        results.append({
            "id": o["id"],
            "name": o["name"],
            "batch_yields": batch_yields,                       # materials produced per batch (portionSize units)
            "batch_size": portion,                              # batch size in ore units
            "batch_volume": o.get("volume", 0.0) * portion,     # volume of one batch
            "batch_yield_percent": mult * 100                   # total reprocessing yield percent
        })
    
    return results