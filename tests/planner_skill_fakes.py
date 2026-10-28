"""SDE skill requirements for the daily planner's assignment tests.

CharacterAssigner.assign and DailyPlannerService phase 4 only plan a job for a
pilot whose loaded skills meet the SDE skills of that job's activity
(get_blueprint_manufacturing_data's activity_skills). Tests that are not
about skills pass NO_SKILL_REQUIREMENTS: every blueprint has every activity,
none of which requires a skill. Fake data.
"""
from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.application.industry.character_slots import SDE_ACTIVITIES


class _NoSkillRequirements:
    """blueprint_data where any blueprint id has every activity, without skills."""

    def get(self, blueprint_type_id: Any, default: Any = None) -> dict[str, Any]:
        return {"activity_skills": {activity: [] for activity in SDE_ACTIVITIES}}


NO_SKILL_REQUIREMENTS = _NoSkillRequirements()


def skill(type_id: int, name: str, level: int) -> dict[str, Any]:
    """One required skill, in get_blueprint_manufacturing_data's enriched shape."""
    return {"type_id": type_id, "type_name": name, "level": level}


def blueprint(**activity_skills: list[dict[str, Any]]) -> dict[str, Any]:
    """One blueprint_data entry with these activities' required skills."""
    return {"activity_skills": dict(activity_skills)}
