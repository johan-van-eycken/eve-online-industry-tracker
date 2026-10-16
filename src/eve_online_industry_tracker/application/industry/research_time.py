"""ME/TE research time skills: the one table the producer's research
durations and the daily planner's per-pilot research durations share.

EVE: Metallurgy cuts ME research time 5% per level, Research cuts TE
research time 5% per level, Advanced Industry cuts both 3% per level. The
reductions multiply (IndustryService._combine_reductions).
"""
from __future__ import annotations

from typing import Mapping

RESEARCH_TIME_SKILL_RATES: dict[str, tuple[tuple[str, float], ...]] = {
    "research_material": (("Metallurgy", 0.05), ("Advanced Industry", 0.03)),
    "research_time": (("Research", 0.05), ("Advanced Industry", 0.03)),
}


def research_skill_reductions(activity: str, skill_levels_by_name: Mapping[str, int]) -> list[float]:
    """The per-skill time reductions of a research activity (fractions)."""
    return [
        rate * int(skill_levels_by_name.get(name, 0) or 0)
        for name, rate in RESEARCH_TIME_SKILL_RATES.get(activity, ())
    ]


def research_skill_time_multiplier(activity: str, skill_levels_by_name: Mapping[str, int]) -> float:
    """The factor a pilot's skills leave of a research job's unskilled time:
    the product of (1 - reduction) over the activity's skills."""
    multiplier = 1.0
    for reduction in research_skill_reductions(activity, skill_levels_by_name):
        multiplier *= max(0.0, 1.0 - reduction)
    return multiplier
