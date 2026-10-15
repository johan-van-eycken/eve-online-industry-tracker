"""Industry slot capacity per character: the one slot model the daily
planner's assignment (CharacterAssigner._compute_available_slots) and the
producer's parallel invention lanes (invention_model.invention_lanes) share.

EVE: one base slot, plus one per trained level of Mass Production and
Advanced Mass Production (manufacturing), or of Laboratory Operation and
Advanced Laboratory Operation (research: copying, ME/TE research and
invention share these slots).
"""
from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

# Slot capacity skills. Base one slot, plus one per level of each.
MFG_SLOT_SKILLS = ("Mass Production", "Advanced Mass Production")
RESEARCH_SLOT_SKILLS = ("Laboratory Operation", "Advanced Laboratory Operation")


def skill_levels_by_name(char: dict[str, Any]) -> dict[str, int]:
    """{skill_name: trained_skill_level} for one character.

    Reads the enriched skill list built in characters/character.py, which joins
    ESI's skill list against every published SDE skill and carries skill_name
    alongside trained_skill_level. Note the stored payload has no
    `active_skill_level` key at all — reading one yields 0 for every skill.

    A character with no skills data at all (missing/`None`/empty) is a
    legitimate, quiet `{}` — nothing trained yet, or not fetched. A `skills`
    value that *is* present but not the expected `{"skills": [...]}` wrapper
    (e.g. a bare list) is a malformed payload, not an absence: it is logged
    at warning and still returns `{}` rather than raising, since one pilot's
    bad payload must not abort the whole plan -- it just falls back to base
    slot capacity for that pilot.
    """
    raw = char.get("skills")
    if not raw:
        return {}

    entries = raw.get("skills") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        logger.warning(
            "CharacterAssigner: character %s has a 'skills' payload that is not the "
            "expected {'skills': [...]} wrapper (got %s) -- treating as no trained "
            "skills for this pilot rather than aborting the plan",
            char.get("character_id") or char.get("id") or char.get("character_name") or char.get("name") or "?",
            type(raw).__name__,
        )
        return {}

    levels: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("skill_name") or "").strip()
        if not name:
            continue
        try:
            level = int(entry.get("trained_skill_level") or 0)
        except (TypeError, ValueError):
            continue
        levels[name] = max(level, levels.get(name, 0))
    return levels


def skill_levels_by_id(char: dict[str, Any]) -> dict[int, int]:
    """{skill type_id: trained_skill_level} for one character, from the same
    enriched list as skill_levels_by_name. Ids, not names: the producer's
    invention chance names its skills by SDE type id."""
    raw = char.get("skills")
    entries = raw.get("skills") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return {}
    levels: dict[int, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            skill_id = int(entry.get("skill_id") or 0)
            level = int(entry.get("trained_skill_level") or 0)
        except (TypeError, ValueError):
            continue
        if skill_id > 0:
            levels[skill_id] = max(level, levels.get(skill_id, 0))
    return levels


def skills_loaded(char: dict[str, Any]) -> bool:
    """Whether the character carries a loaded skill list ({"skills": [...]},
    possibly empty). Without one its levels are unknown, not 0."""
    raw = char.get("skills")
    return isinstance(raw, dict) and isinstance(raw.get("skills"), list)


def slot_capacity(levels: Mapping[str, int], skill_names: tuple[str, ...]) -> int:
    """One base slot plus one per trained level of each contributing skill."""
    return 1 + sum(int(levels.get(name, 0)) for name in skill_names)


def research_slot_capacity(levels_by_name: Mapping[str, int]) -> int:
    """A character's total research slots (copying, ME/TE research and
    invention) from skill_levels_by_name: the count the daily planner starts
    from before it deducts active jobs. No skill list: the base slot."""
    return slot_capacity(levels_by_name, RESEARCH_SLOT_SKILLS)
