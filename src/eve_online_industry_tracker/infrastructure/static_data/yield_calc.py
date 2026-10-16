from __future__ import annotations

import logging
from typing import Any, Mapping

from eve_online_industry_tracker.domain import reprocessing as _r
from eve_online_industry_tracker.infrastructure.sde import ore as _ore
from eve_online_industry_tracker.infrastructure.sde.industry import REIMPORT_HINT, SdeIndustryDataMissing

logger = logging.getLogger(__name__)


def _bonus_per_level(data: _ore.OreData, skill_type_id: int) -> float | None:
    percent = data.refining_yield_bonus.get(int(skill_type_id))
    return None if percent is None else float(percent) / 100.0


def compute_yields(ores: list[dict], skill_levels: Mapping[int, int], facility: _r.ReprocessingFacility,
                   implant_bonus: float, data: _ore.OreData | None = None) -> list[dict[str, Any]]:
    """The reprocessing yield per batch of each ore (build_all_ores rows), by
    EVE's formula (domain.reprocessing).

    skill_levels is the character's {skill type id: trained level}
    (ore.refining_skill_levels); an untrained skill counts 0. The skills are
    Reprocessing (3385), Reprocessing Efficiency (3389) and the ore's own
    (typeDogma attribute 790), each with its SDE per-level bonus (attribute
    379). Each material is rounded down per batch.

    An ore with materials but no ore skill, no bonus for its skill, or no
    portionSize or volume is left out with a WARNING naming it; an ore
    without materials yields nothing and is left out quietly. A missing
    Reprocessing or Reprocessing Efficiency bonus raises
    SdeIndustryDataMissing."""
    data = data if data is not None else _ore.ore_data()
    base_skills: dict[int, float] = {}
    for skill_id in (_ore.REPROCESSING_SKILL_TYPE_ID, _ore.REPROCESSING_EFFICIENCY_SKILL_TYPE_ID):
        bonus = _bonus_per_level(data, skill_id)
        if bonus is None:
            raise SdeIndustryDataMissing(f"SDE typeDogma lacks attribute 379 (refiningYieldMutator) of skill "
                                         f"{skill_id}; ore yields cannot be computed: {REIMPORT_HINT}")
        base_skills[skill_id] = bonus

    results = []
    left_out: list[str] = []
    for o in ores:
        if not o.get("materials"):
            continue
        label = f"{o['name']} ({o['id']})"
        portion, volume = o.get("portionSize"), o.get("volume")
        if not portion or volume is None:
            left_out.append(f"{label}: no portionSize or volume in the SDE")
            continue
        ore_skill = o.get("reprocessingSkillTypeID")
        if ore_skill is None:
            left_out.append(f"{label}: no reprocessing skill (typeDogma attribute 790)")
            continue
        ore_bonus = _bonus_per_level(data, ore_skill)
        if ore_bonus is None:
            left_out.append(f"{label}: its skill {ore_skill} has no bonus (typeDogma attribute 379)")
            continue

        fraction = _r.reprocessing_yield(facility, skill_bonuses={**base_skills, int(ore_skill): ore_bonus},
                                         skill_levels=skill_levels, implant_bonus=implant_bonus)
        results.append(
            {
                "id": o["id"],
                "name": o["name"],
                "batch_yields": {m["name"]: _r.batch_output(int(m["quantity"]), fraction) for m in o["materials"]},
                "batch_size": portion,
                "batch_volume": float(volume) * portion,
                "batch_yield_percent": fraction * 100,
                "compressed": bool(o.get("compressed")),
            }
        )

    if left_out:
        logger.warning("Ore calculator: %d ore(s) left out: %s", len(left_out), "; ".join(left_out))
    return results
