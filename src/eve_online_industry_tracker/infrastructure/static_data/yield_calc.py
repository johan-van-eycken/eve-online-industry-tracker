from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def compute_yields(ores, char_skills, facility, implants):
    """The reprocessing yield per batch of each ore (build_all_ores rows).

    The ore skill is the SDE's (the row's "reprocessingSkill", typeDogma
    attribute 790), looked up by name in char_skills. An ore with
    reprocessing materials but no skill in the SDE is left out with a
    WARNING naming it; an ore without materials yields nothing and is left
    out quietly."""
    refining = char_skills.get("Refining", 0)
    re_eff = char_skills.get("Reprocessing Efficiency", 0)
    facility_base = facility.get("base_yield", 0.5)
    rig_bonus = facility.get("rig_bonus", 0.0)
    structure_bonus = facility.get("structure_bonus", 0.0)
    implant_bonus = sum(i.get("bonus", 0.0) for i in implants if i.get("group") == "reprocessing")

    results = []
    no_skill: list[str] = []
    for o in ores:
        portion = o.get("portionSize", 100) or 100
        if not o.get("materials"):
            continue
        skill = o.get("reprocessingSkill")
        if not skill:
            skill_type_id = o.get("reprocessingSkillTypeID")
            no_skill.append(f"{o['name']} ({o['id']}" + (f", skill type {skill_type_id} not in the SDE types)"
                                                         if skill_type_id is not None else ")"))
            continue

        ore_skill = char_skills.get(skill, 0)

        mult = facility_base
        mult *= (1 + 0.02 * refining)
        mult *= (1 + 0.02 * re_eff)
        mult *= (1 + 0.02 * ore_skill)
        mult *= (1 + rig_bonus + structure_bonus + implant_bonus)

        batch_yields = {}
        for m in o["materials"]:
            qty_per_portion = m["quantity"]
            per_batch = qty_per_portion * mult
            batch_yields[m["name"]] = per_batch

        results.append(
            {
                "id": o["id"],
                "name": o["name"],
                "batch_yields": batch_yields,
                "batch_size": portion,
                "batch_volume": o.get("volume", 0.0) * portion,
                "batch_yield_percent": mult * 100,
                "compressed": bool(o.get("compressed")),
            }
        )

    if no_skill:
        logger.warning("Ore calculator: %d ore(s) left out, the SDE names no reprocessing skill for them "
                       "(typeDogma attribute 790): %s", len(no_skill), ", ".join(no_skill))
    return results
