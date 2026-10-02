# tests/test_daily_planner_assignment.py
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    CharacterAssigner,
)
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
from eve_online_industry_tracker.infrastructure.models import CharacterIndustryJobsModel


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
    # Every top-level row has passed PlannerInputRow, which requires a positive
    # manufacturing_job.runs; the manufacture action now reads it (and raises
    # PlannerInputError without it), so fixture rows carry one too.
    base["overview_row"].setdefault("manufacturing_job", {}).setdefault("runs", 1)
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


def test_skill_levels_of_a_malformed_bare_list_payload_warns_but_does_not_raise(caplog):
    """Real payloads are always wrapped as {"skills": [...]}. A bare list is a
    malformed producer bug, not a legitimate absence -- it must be logged at
    warning (review finding 1's symptom: every pilot silently collapsing to 1
    slot) but must never raise, since one pilot's bad payload must not abort
    the whole plan."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        skill_levels_by_name,
    )

    bare_list_char = {"character_id": 1, "skills": [_skill("Mass Production", 5)]}
    with caplog.at_level("WARNING"):
        assert skill_levels_by_name(bare_list_char) == {}
    assert any("skills" in r.message.lower() for r in caplog.records)


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


def test_a_character_installed_job_deducts_a_manufacturing_slot(session_provider):
    """Defect 2 (task-18a remediation): the planner could not see
    character-installed jobs at all (DailyPlannerService._get_industry_jobs
    queried only CorporationIndustryJobsModel), so a pilot's own job never
    counted against their slots and the planner over-assigned work EVE would
    then refuse to start. This goes through the real _get_industry_jobs (not
    a hand-built job list) so a regression in either the query or in
    _compute_available_slots's character_id/installer_id keying would be
    caught. A wrong implementation (corp-only query) would leave free_mfg at
    1 -- the character job would simply not exist as far as the planner is
    concerned."""
    session = session_provider.app_session()
    try:
        session.add(CharacterIndustryJobsModel(
            character_id=7, job_id=301, status="active", activity_id=1,  # manufacturing
        ))
        session.commit()
    finally:
        session.close()

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: []),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=SimpleNamespace(),
        admin_settings=SimpleNamespace(get=lambda *a, **k: None),
        session_provider=session_provider,
    )
    jobs = svc._get_industry_jobs()
    assert {j.job_id for j in jobs} == {301}, "the character job must come through the real query"

    chars = [_char(7, "Pilot", [])]  # unskilled: 1 base mfg slot
    slots = CharacterAssigner()._compute_available_slots(chars, jobs, _now())
    assert slots[7]["free_mfg"] == 0


# --- copy action (T1 BPO copy feeding invention) --------------------------------
# This branch never executed before: nothing wrote has_t1_bpo. ChainPlanner now
# writes has_t1_bpo + t1_blueprint_type_id when the corp owns the T1 BPO an
# invention target is invented from.

def _research_pilot():
    return [_char(1, "Pilot", [_skill("Laboratory Operation", 5)])]


def test_a_copy_action_is_created_when_an_owned_t1_bpo_feeds_invention():
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True, "has_t1_bpo": True,
                      "t1_blueprint_type_id": 999,
                      # The T2 blueprint being invented: NOT what gets copied.
                      "manufacturing_job": {"blueprint_sde": {"blueprint_type_id": 1999}}},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(_research_pilot()), _AdminStub())
    copies = [a for a in actions if a.action_type == "copy"]
    assert len(copies) == 1
    assert copies[0].type_id == 999


def test_no_copy_action_without_an_owned_t1_bpo():
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True, "needs_t1_bpo": True},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(_research_pilot()), _AdminStub())
    assert [a for a in actions if a.action_type == "copy"] == []


def test_no_copy_action_when_the_t1_blueprint_is_unknown(caplog):
    """Copying the T2 product's type_id would be meaningless; skip loudly."""
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True, "has_t1_bpo": True},
    )])
    with caplog.at_level("WARNING"):
        actions = CharacterAssigner().assign(plan, [], _Chars(_research_pilot()), _AdminStub())
    assert [a for a in actions if a.action_type == "copy"] == []
    assert any("t1_blueprint_type_id" in r.message for r in caplog.records)


def test_research_actions_carry_the_nested_blueprint_type_id():
    """ME/TE research targets the blueprint, read from manufacturing_job.blueprint_sde
    (a real row has no top-level blueprint_type_id)."""
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_me_research": True, "needs_te_research": True,
                      "manufacturing_job": {"blueprint_sde": {"blueprint_type_id": 999}}},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(_research_pilot()), _AdminStub())
    research = [a for a in actions if a.action_type.endswith("_research")]
    assert sorted(a.type_id for a in research) == [999, 999]
