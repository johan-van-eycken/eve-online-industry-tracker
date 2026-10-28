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
from typing import Any, Iterable, Mapping

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
            "Character slots: character %s has a 'skills' payload that is not the "
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


# ── Job skill requirements ───────────────────────────────────────────────────
# A job runs only for a pilot whose trained skills reach every level the SDE
# blueprint names under activities.<activity>.skills. The daily planner's
# skill gate and assignment (CharacterAssigner), the invention lanes
# (invention_model.can_run_invention) and the producer's
# get_blueprint_skill_qualification all compare through meets_skill_levels.

_ACTIVITY_LABELS = {
    "manufacturing": "manufacturing", "reaction": "reaction", "invention": "invention",
    "copying": "copying", "research_material": "ME research", "research_time": "TE research",
}


def meets_skill_levels(required_levels: Mapping[int, int], levels_by_skill_type_id: Mapping[int, int]) -> bool:
    """Whether one pilot has every required skill (type_id -> level) trained."""
    return all(
        int(levels_by_skill_type_id.get(int(type_id), 0) or 0) >= int(level)
        for type_id, level in required_levels.items()
    )


def required_skill_levels(entries: Iterable[Mapping[str, Any]]) -> dict[int, int]:
    """{skill type_id: required level} of SDE skill entries ({type_id, level})."""
    levels: dict[int, int] = {}
    for entry in entries:
        type_id, level = _as_int(entry.get("type_id")), _as_int(entry.get("level"))
        if type_id > 0 and level > 0:
            levels[type_id] = max(level, levels.get(type_id, 0))
    return levels


def activity_skill_requirements(
    blueprint_data: Any, blueprint_type_id: int | None, activity: str,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """(the SDE skills one activity of a blueprint requires, None), or (None,
    why they are unknown): no blueprint, the blueprint or its skills not in
    the SDE data, or no such activity on it. `blueprint_data` is
    get_blueprint_manufacturing_data's map. A reaction formula has no
    manufacturing activity: "manufacturing" then reads its reaction."""
    label = _ACTIVITY_LABELS.get(activity, activity)
    entry = blueprint_data.get(int(blueprint_type_id or 0))
    skills_by_activity = entry.get("activity_skills") if isinstance(entry, Mapping) else None
    if not isinstance(skills_by_activity, Mapping):
        if not blueprint_type_id:
            return None, f"the {label} skills are unknown: the row names no blueprint"
        return None, f"the {label} skills are unknown: blueprint {int(blueprint_type_id)} is not in the SDE data"
    if activity == "manufacturing" and activity not in skills_by_activity and "reaction" in skills_by_activity:
        activity = "reaction"
    entries = skills_by_activity.get(activity)
    if not isinstance(entries, list):
        return None, f"the SDE has no {label} activity for blueprint {int(blueprint_type_id)}"
    return [dict(e) for e in entries if isinstance(e, Mapping)], None


def skill_shortfall(
    entries: Iterable[Mapping[str, Any]], pilots: Iterable[tuple[str, Mapping[int, int] | None]],
) -> str:
    """Why no pilot qualifies, e.g. "no pilot has the skills: Energy Weapon
    Rigging 4 (best: Pilot Alpha at 2)". `pilots`: (name, skill levels by type_id,
    None when not loaded). When every skill is trained by someone but no one
    pilot has them all: what each pilot lacks. Pilots whose skills are not
    loaded are named: they never qualify, their levels being unknown."""
    entries, pilots = list(entries), list(pilots)
    if not pilots:
        return "no industry pilot"
    required = [
        (type_id, _entry_name(entries, type_id), level)
        for type_id, level in required_skill_levels(entries).items()
    ]
    loaded = [(name, levels) for name, levels in pilots if levels is not None]
    unknown = [name for name, levels in pilots if levels is None]
    parts: list[str] = []
    for type_id, name, level in required:
        if not loaded:
            parts.append(f"{name} {level}")
            continue
        # Ties: the first pilot listed.
        best_name, best_levels = max(loaded, key=lambda p: int(p[1].get(type_id, 0) or 0))
        best = int(best_levels.get(type_id, 0) or 0)
        if best < level:
            parts.append(f"{name} {level} (best: {best_name} at {best})")
    if parts:
        text = f"no pilot has the skills: {', '.join(parts)}"
    elif loaded:
        lacks = [
            f"{pilot} lacks " + ", ".join(
                f"{name} {level} (at {int(levels.get(type_id, 0) or 0)})"
                for type_id, name, level in required if int(levels.get(type_id, 0) or 0) < level
            )
            for pilot, levels in loaded
        ]
        text = f"no one pilot has all the skills: {'; '.join(lacks)}"
    else:
        return f"no pilot qualifies: skills not loaded for {', '.join(unknown)}"
    if unknown:
        text += f"; skills not loaded for {', '.join(unknown)}"
    return text


def _entry_name(entries: list[Mapping[str, Any]], type_id: int) -> str:
    for entry in entries:
        if _as_int(entry.get("type_id")) == type_id and entry.get("type_name"):
            return str(entry["type_name"])
    return f"skill {type_id}"


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
