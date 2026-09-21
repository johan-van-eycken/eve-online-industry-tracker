# tests/test_daily_planner_assignment.py
from __future__ import annotations

from datetime import datetime, timezone

from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    CharacterAssigner,
)
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision


def _now():
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


class _AdminStub:
    def get(self, section, key, default=None):
        return default


def _char(char_id, name, skills):
    return {"character_id": char_id, "character_name": name,
            "skills": {"skills": skills, "total_sp": 0}}


def _skill(skill_name, level, skill_id=0):
    """One entry in the enriched skill list produced by characters/character.py.

    Note there is deliberately no active_skill_level key — the real stored
    payload does not have one.
    """
    return {"skill_id": skill_id, "skill_name": skill_name,
            "group_name": "Production", "trained_skill_level": level}


class _Chars:
    def __init__(self, chars):
        self._chars = chars

    def list_characters(self):
        return self._chars


def _decision(**overrides):
    base = dict(
        type_id=12345, type_name="Hobgoblin II", decision="build",
        decision_reason="test", adjusted_score=1.0, absolute_profit_per_batch=1.0,
        isk_per_hour=1.0, margin_pct=1.0, days_of_supply_current=1.0,
        effective_velocity=1.0, meta_group_id=2, pipeline_stage="manufacturing",
        overview_row={"type_id": 12345},
    )
    base.update(overrides)
    return ItemDecision(**base)


def test_skill_levels_are_read_by_name_from_the_enriched_list():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        skill_levels_by_name,
    )

    char = _char(1, "Pilot", [_skill("Mass Production", 5), _skill("Science", 4)])
    assert skill_levels_by_name(char) == {"Mass Production": 5, "Science": 4}


def test_skill_levels_of_a_character_without_skills_is_empty():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        skill_levels_by_name,
    )

    assert skill_levels_by_name({"character_id": 1}) == {}
    assert skill_levels_by_name({"character_id": 1, "skills": {}}) == {}


def test_slot_capacity_uses_trained_skill_levels():
    # Mass Production 5 + Advanced Mass Production 4 => 1 + 5 + 4 = 10 mfg slots
    chars = [_char(1, "Pilot", [_skill("Mass Production", 5),
                                _skill("Advanced Mass Production", 4)])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_mfg"] == 10


def test_a_fully_skilled_pilot_has_eleven_slots_of_each_kind():
    # The real measured character: every industry skill at 5.
    chars = [_char(1, "Pilot", [
        _skill("Mass Production", 5), _skill("Advanced Mass Production", 5),
        _skill("Laboratory Operation", 5), _skill("Advanced Laboratory Operation", 5),
    ])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_mfg"] == 11
    assert slots[1]["free_research"] == 11


def test_research_slot_capacity_uses_trained_skill_levels():
    chars = [_char(1, "Pilot", [_skill("Laboratory Operation", 5),
                                _skill("Advanced Laboratory Operation", 3)])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_research"] == 9


def test_an_unskilled_pilot_still_has_one_slot_of_each_kind():
    chars = [_char(1, "Pilot", [])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_mfg"] == 1
    assert slots[1]["free_research"] == 1


def test_me_and_te_research_take_two_separate_slots():
    chars = [_char(1, "Pilot", [_skill("Laboratory Operation", 1)])]  # 1 + 1 = 2 slots
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_me_research": True,
                      "needs_te_research": True, "blueprint_type_id": 999},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub())
    kinds = sorted(a.action_type for a in actions if a.action_type.endswith("_research"))
    assert kinds == ["me_research", "te_research"]


def test_a_single_research_slot_yields_only_one_research_action():
    chars = [_char(1, "Pilot", [])]  # 1 + 0 = 1 research slot only
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_me_research": True,
                      "needs_te_research": True, "blueprint_type_id": 999},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub())
    research = [a for a in actions if a.action_type.endswith("_research")]
    assert len(research) == 1


def test_invention_prefers_the_higher_science_skilled_pilot():
    low = _char(1, "Low", [_skill("Laboratory Operation", 5), _skill("Science", 1)])
    high = _char(2, "High", [_skill("Laboratory Operation", 5), _skill("Science", 5)])
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars([low, high]), _AdminStub())
    invent = [a for a in actions if a.action_type == "invent"]
    assert len(invent) == 1
    assert invent[0].character_id == 2
