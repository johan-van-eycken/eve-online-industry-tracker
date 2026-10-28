"""Pilot skills in the producer: the best inventor's chance and parallel inventors."""

from __future__ import annotations

from types import MethodType, SimpleNamespace
import pytest
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_QUALIFYING_INVENTOR_REASON,
    ONE_SOURCE_BPO_REASON,
)
from eve_online_industry_tracker.application.industry.service import IndustryService
from planner_skill_fakes import NO_SKILL_REQUIREMENTS
from industry_service_fakes import (
    _NESTED_FAKE_PRICES,
    _NESTED_FEES_PROFILE,
    _invented_t2_blueprint_rows,
    _invention_service,
    _nested_invention_blueprint_rows,
)


# --- round 5 Task 2: invention chance from the corp's best inventor -----------
# Fake skill levels only.

_INVENTION_SKILLS = [
    {"type_id": 11433, "type_name": "High Energy Physics", "group_id": 270},
    {"type_id": 11454, "type_name": "Caldari Starship Engineering", "group_id": 270},
    {"type_id": 21790, "type_name": "Caldari Encryption Methods", "group_id": 270},
]


def _skilled_overview(monkeypatch, *, levels, encryption=frozenset({21790}), real_confidence=False,
                      skill_entries=None, **kwargs):
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in (skill_entries or _INVENTION_SKILLS)]
    service = _invention_service(monkeypatch, blueprint_rows=rows, real_confidence=real_confidence, **kwargs)
    service._get_inventor_skill_levels = MethodType(lambda self: levels, service)  # type: ignore[attr-defined]
    service._load_encryption_skill_type_ids = MethodType(lambda self: encryption, service)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}


def test_the_best_inventors_skills_raise_the_chance(monkeypatch) -> None:
    levels = {1: {11433: 4, 11454: 4, 21790: 4}, 2: {11433: 5, 11454: 5, 21790: 3}}
    mj = _skilled_overview(monkeypatch, levels=levels)[5002]["manufacturing_job"]

    factor = 1 + 10 / 30 + 3 / 40  # pilot 2 beats pilot 1 (1 + 8/30 + 4/40)
    p = 0.5 * factor
    chance = mj["invention_chance"]
    assert chance["inventor_character_id"] == 2
    assert chance["skill_factor"] == pytest.approx(factor)
    assert chance["effective_probability"] == pytest.approx(p)
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(p)
    # 1 run / 10 per BPC / p attempts, 2 datacores x 100 each.
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * (1 / 10) / p)
    assert chance["skills_unknown_reason"] is None
    assert chance["note"] is None


def test_an_untrained_corp_cannot_run_the_invention_so_the_base_chance_is_used(monkeypatch) -> None:
    """Task 15 I3: a pilot with none of the invention skills cannot run it, so
    it is never the inventor: the SDE base chance, with the reason."""
    mj = _skilled_overview(monkeypatch, levels={1: {}})[5002]["manufacturing_job"]

    assert mj["invention_chance"]["skill_factor"] is None
    assert mj["invention_chance"]["inventor_character_id"] is None
    assert mj["invention_chance"]["skills_unknown_reason"] == NO_QUALIFYING_INVENTOR_REASON
    assert mj["invention_material_cost"] == pytest.approx(40.0)  # base 0.5


def test_unloaded_skills_fall_back_to_the_base_chance_with_a_reason(monkeypatch, caplog) -> None:
    with caplog.at_level("WARNING"):
        mj = _skilled_overview(monkeypatch, levels={}, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["invention_chance"]["skill_factor"] is None
    assert mj["invention_chance"]["skills_unknown_reason"] == "no corp character has its skills loaded"
    assert mj["invention_material_cost"] == pytest.approx(40.0)  # base 0.5
    assert any("SDE base chance" in r for r in mj["pricing_confidence_reasons"])
    assert sum("no corp character has its skills loaded" in r.getMessage()
               for r in caplog.records if r.levelname == "WARNING") == 1


def test_a_blueprint_without_an_encryption_skill_uses_the_science_term_with_a_row_note(monkeypatch) -> None:
    """A3: the formula with the science term only, not the base chance."""
    levels = {1: {11433: 4, 11454: 4, 21790: 5}}
    mj = _skilled_overview(
        monkeypatch, levels=levels, skill_entries=_INVENTION_SKILLS[:2], real_confidence=True,
    )[5002]["manufacturing_job"]

    p = 0.5 * (1 + 8 / 30)
    chance = mj["invention_chance"]
    assert chance["skill_factor"] == pytest.approx(1 + 8 / 30)
    assert chance["encryption_skill_type_id"] is None
    assert chance["skills_unknown_reason"] is None
    assert "no encryption skill" in chance["note"]
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * (1 / 10) / p)
    # The note reaches the UI as an informational reason line ...
    assert f"Invention chance: {chance['note']}." in mj["pricing_confidence_reasons"]
    assert not any("SDE base chance" in r for r in mj["pricing_confidence_reasons"])
    # ... and does not change the confidence level.
    with_encryption = _skilled_overview(monkeypatch, levels=levels, real_confidence=True)[5002]["manufacturing_job"]
    assert mj["overall_pricing_confidence"] == with_encryption["overall_pricing_confidence"]
    assert mj["material_pricing_confidence"] == with_encryption["material_pricing_confidence"]


def test_the_admin_floor_still_applies_to_the_skilled_chance(monkeypatch) -> None:
    """A non-zero base whose skilled chance stays under the floor:
    0.02 x (1 + 10/30 + 5/40) = 0.0292 < 0.05."""
    rows_levels = {1: {11433: 5, 11454: 5, 21790: 5}}
    service_rows = _invented_t2_blueprint_rows()
    service_rows[0]["invention_job"]["products"][0]["probability_pct"] = 2.0
    service_rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    service = _invention_service(monkeypatch, blueprint_rows=service_rows, adm_overrides={"invention_probability_floor": 0.05})
    service._get_inventor_skill_levels = MethodType(lambda self: rows_levels, service)  # type: ignore[attr-defined]
    service._load_encryption_skill_type_ids = MethodType(lambda self: frozenset({21790}), service)  # type: ignore[attr-defined]
    rows = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )}
    mj = rows[5002]["manufacturing_job"]
    assert mj["invention_chance"]["probability"] == pytest.approx(0.02 * (1 + 10 / 30 + 5 / 40))
    assert mj["invention_chance"]["effective_probability"] == pytest.approx(0.05)
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(0.05)


def test_inventor_skills_come_from_every_loaded_corp_character() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=lambda: [  # type: ignore[attr-defined]
        {"character_id": 1, "skills": {"skills": [
            {"skill_id": 11433, "trained_skill_level": 4}, {"skill_id": 21790, "trained_skill_level": 2},
        ]}},
        {"character_id": 2, "skills": {}},          # not fetched: unknown, left out
        {"character_id": 3},                        # not fetched either
        {"character_id": 4, "industry_enabled": False, "skills": {"skills": [  # excluded from industry
            {"skill_id": 11433, "trained_skill_level": 5},
        ]}},
        {"character_id": 5, "industry_enabled": True, "skills": {"skills": [
            {"skill_id": 11433, "trained_skill_level": 1},
        ]}},
    ]))
    assert service._get_inventor_skill_levels() == {1: {11433: 4, 21790: 2}, 5: {11433: 1}}


def test_no_character_manager_means_no_inventor_skills() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    assert service._get_inventor_skill_levels() == {}


def _nested_skilled_overview(monkeypatch, *, levels, real_confidence=False):
    service = _invention_service(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, profile=_NESTED_FEES_PROFILE, real_confidence=real_confidence,
    )
    service._get_inventor_skill_levels = MethodType(lambda self: levels, service)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}


def test_a_nested_invention_uses_the_best_inventors_chance(monkeypatch) -> None:
    mj = _nested_skilled_overview(monkeypatch, levels={1: {11433: 4, 11454: 4, 21790: 4}})[7100]["manufacturing_job"]

    factor = 1 + 8 / 30 + 4 / 40
    invention = mj["recursive_activity_breakdown"]["manufacturing:5002"]["nested"]["invention:9002"]
    assert invention["success_probability"] == pytest.approx(0.5 * factor)
    assert invention["invention_chance"]["skill_factor"] == pytest.approx(factor)
    assert invention["invention_chance"]["inventor_character_id"] == 1
    # 1 component run / 10 per BPC / p.
    assert invention["attempts"] == pytest.approx(0.1 / (0.5 * factor))
    assert "invention_chance_notes" not in mj


def test_a_nested_skills_fallback_is_a_row_reason(monkeypatch) -> None:
    mj = _nested_skilled_overview(monkeypatch, levels={}, real_confidence=True)[7100]["manufacturing_job"]

    invention = mj["recursive_activity_breakdown"]["manufacturing:5002"]["nested"]["invention:9002"]
    assert invention["success_probability"] == pytest.approx(0.5)
    assert invention["invention_chance"]["skills_unknown_reason"] == "no corp character has its skills loaded"
    assert len(mj["invention_chance_notes"]) == 1
    assert "sub-build: uses the SDE base chance (no corp character has its skills loaded)" in mj["invention_chance_notes"][0]
    assert f"Invention chance: {mj['invention_chance_notes'][0]}." in mj["pricing_confidence_reasons"]


def _overview_with_real_skill_loader(monkeypatch, characters):
    """The real _get_inventor_skill_levels over a fake character manager."""
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    service = _invention_service(monkeypatch, blueprint_rows=rows, real_confidence=True)
    del service._get_inventor_skill_levels  # back to the class method
    service._state.char_manager = SimpleNamespace(get_characters=lambda: characters)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}[5002]["manufacturing_job"]


_SKILLED = {"skills": [{"skill_id": 11433, "trained_skill_level": 5}]}


@pytest.mark.parametrize(("characters", "reason"), [
    # Every character with loaded skills is excluded from industry.
    ([{"character_id": 1, "industry_enabled": False, "skills": _SKILLED},
      {"character_id": 2, "industry_enabled": True, "skills": {}}],
     "every corp character with loaded skills is excluded from industry"),
    # Nobody has loaded skills (an excluded pilot without skills changes nothing).
    ([{"character_id": 1, "industry_enabled": True, "skills": {}},
      {"character_id": 2, "industry_enabled": False}],
     "no corp character has its skills loaded"),
])
def test_an_empty_inventor_list_names_why(monkeypatch, caplog, characters, reason) -> None:
    with caplog.at_level("WARNING"):
        mj = _overview_with_real_skill_loader(monkeypatch, characters)

    assert mj["invention_chance"]["skills_unknown_reason"] == reason
    assert mj["invention_chance"]["skill_factor"] is None
    assert mj["invention_material_cost"] == pytest.approx(40.0)  # base 0.5
    assert f"Invention chance uses the SDE base chance ({reason})" in " ".join(mj["pricing_confidence_reasons"])
    assert sum(reason in r.getMessage() for r in caplog.records if r.levelname == "WARNING") == 1


# --- round 5 Task 12: invention elapsed time over parallel inventors ----------
# Every pilot at 1/1/1: p = 0.5 x (1 + 2/30 + 1/40) = 0.546, so 1 BPC needs
# ceil(1 / 0.546) = 2 whole attempts of 40 s (no profile, no time skills).

_LEVEL_ONE_INVENTOR = {"skills": [
    {"skill_id": 11433, "skill_name": "High Energy Physics", "trained_skill_level": 1},
    {"skill_id": 11454, "skill_name": "Caldari Starship Engineering", "trained_skill_level": 1},
    {"skill_id": 21790, "skill_name": "Caldari Encryption Methods", "trained_skill_level": 1},
]}


def test_two_parallel_inventors_halve_the_invention_elapsed_time(monkeypatch) -> None:
    mj = _overview_with_real_skill_loader(monkeypatch, [
        {"character_id": 1, "skills": _LEVEL_ONE_INVENTOR},
        {"character_id": 2, "skills": _LEVEL_ONE_INVENTOR},
        {"character_id": 3, "industry_enabled": False, "skills": _LEVEL_ONE_INVENTOR},  # excluded
    ])
    invention = mj["activity_breakdown"]["invention"]
    assert invention["whole_attempts"] == 2
    assert invention["elapsed_parallel"]["lanes"] == 2  # one base slot each, pilot 3 excluded
    assert invention["elapsed_duration_seconds"] == 40  # was 2 x 40 in sequence
    # No owned source BPO counted: the 2 copy runs stay in sequence, with the reason.
    copying = mj["activity_breakdown"]["invention_source_copying"]
    assert copying["elapsed_duration_seconds"] == 40
    assert copying["elapsed_parallel"]["sequential_reason"] == ONE_SOURCE_BPO_REASON
    assert mj["elapsed_time_seconds"] == 96 + 40 + 40
    # Fix round 1: a row note, shown with the confidence reasons and the Build time.
    note = "invention assumes 2 research slots over 2 inventors (capacity, not today's free slots)"
    assert mj["elapsed_lane_notes"] == [note]
    assert f"Build time: {note}." in mj["pricing_confidence_reasons"]


def test_the_daily_plan_schedules_the_lanes_the_producers_elapsed_assumes(monkeypatch) -> None:
    """Task 12 fix round 1: with every qualifying slot free, the planner's
    invent actions are the producer's lanes: same pilots, same attempts per
    lane, so the longest lane is the producer's rounds."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
    from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision

    characters = [
        {"character_id": 1, "character_name": "Pilot A", "skills": _LEVEL_ONE_INVENTOR},
        {"character_id": 2, "character_name": "Pilot B", "skills": _LEVEL_ONE_INVENTOR},
    ]
    service = _invention_service(monkeypatch, blueprint_rows=_invented_t2_blueprint_rows())
    del service._get_inventor_skill_levels  # back to the class method
    service._state.char_manager = SimpleNamespace(get_characters=lambda: characters)  # type: ignore[attr-defined]
    rows = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=True, character_id=1,
    )}
    row = {**rows[5002], "needs_invention": True}
    parallel = row["manufacturing_job"]["activity_breakdown"]["invention"]["elapsed_parallel"]
    plan = ChainPlan(decisions=[ItemDecision(
        type_id=5002, type_name="T2 Module", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2,
        pipeline_stage="invention", overview_row=row,
    )])
    chars = SimpleNamespace(list_characters=lambda: characters)
    admin = SimpleNamespace(get=lambda section, key, default=None: default)
    invents = [a for a in CharacterAssigner().assign(plan, [], chars, admin, price_map={}, blueprint_data=NO_SKILL_REQUIREMENTS) if a.action_type == "invent"]
    assert [a.character_id for a in invents] == [1, 2]
    assert len(invents) == parallel["lanes_used"]
    assert max(a.runs for a in invents) == parallel["rounds"]
    assert sum(a.runs for a in invents) == parallel["lane_whole_jobs"]


def _lane_skills(level: int, lab_operation: int = 0) -> dict:
    return {"skills": [
        {"skill_id": 11433, "skill_name": "High Energy Physics", "trained_skill_level": level},
        {"skill_id": 11454, "skill_name": "Caldari Starship Engineering", "trained_skill_level": level},
        {"skill_id": 21790, "skill_name": "Caldari Encryption Methods", "trained_skill_level": level},
        {"skill_id": 990001, "skill_name": "Laboratory Operation", "trained_skill_level": lab_operation},
    ]}


def test_the_portfolio_list_buys_the_lanes_whole_attempts(monkeypatch) -> None:
    """Fix round 2: base 10 %, 1 BPC. The 5/5/5 pilot alone (p 0.1458)
    needs ceil(1 / 0.1458) = 7 attempts; its 1 slot plus a 1/1/1 alt's 5
    (p 0.1092) run ceil(1 / 0.6917) = 2 rounds: 2 + 2 + 2 + 2 + 1 = 9
    attempts. The Portfolio buys the 9, and the cost prices the lanes'
    expected attempts, 1 x 9 / (2 x 0.1458 + 7 x 0.1092)."""
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 10.0
    service = _invention_service(monkeypatch, blueprint_rows=rows)
    del service._get_inventor_skill_levels  # back to the class method
    service._state.char_manager = SimpleNamespace(get_characters=lambda: [  # type: ignore[attr-defined]
        {"character_id": 1, "skills": _lane_skills(5)},
        {"character_id": 2, "skills": _lane_skills(1, lab_operation=4)},
    ])
    out = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=True, character_id=1,
    )}
    mj = out[5002]["manufacturing_job"]
    invention = mj["activity_breakdown"]["invention"]
    p_best, p_alt = 0.1 * (1 + 10 / 30 + 5 / 40), 0.1 * (1 + 2 / 30 + 1 / 40)
    assert invention["best_inventor_whole_attempts"] == 7
    assert invention["whole_attempts"] == 9
    assert invention["runs"] == pytest.approx(9 / (2 * p_best + 7 * p_alt))
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 2 * 9  # was 2 x 7
    # The copies cover every lane attempt too (no owned source runs).
    assert mj["activity_breakdown"]["invention_source_copying"]["elapsed_parallel"]["whole_jobs"] == 9


def test_no_qualifying_inventor_is_a_build_time_note(monkeypatch) -> None:
    untrained = {"skills": [{"skill_id": 11433, "skill_name": "High Energy Physics", "trained_skill_level": 5}]}
    mj = _overview_with_real_skill_loader(monkeypatch, [{"character_id": 1, "skills": untrained}])
    note = f"invention timed in one slot: {NO_QUALIFYING_INVENTOR_REASON}"
    assert mj["elapsed_lane_notes"] == [note]
    assert f"Build time: {note}." in mj["pricing_confidence_reasons"]
    assert mj["activity_breakdown"]["invention"]["elapsed_duration_seconds"] == 2 * 40


def test_a_nested_invention_runs_over_the_parallel_inventors_too(monkeypatch) -> None:
    service = _invention_service(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, profile=_NESTED_FEES_PROFILE,
    )
    del service._get_inventor_skill_levels  # back to the class method
    service._state.char_manager = SimpleNamespace(get_characters=lambda: [  # type: ignore[attr-defined]
        {"character_id": 1, "skills": _LEVEL_ONE_INVENTOR},
        {"character_id": 2, "skills": _LEVEL_ONE_INVENTOR},
    ])
    rows = {row["type_id"]: row for row in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )}
    mj = rows[7100]["manufacturing_job"]
    invention = mj["recursive_activity_breakdown"]["manufacturing:5002"]["nested"]["invention:9002"]
    assert invention["whole_attempts"] == 2
    assert invention["elapsed_parallel"]["lanes"] == 2
    assert invention["elapsed_time_seconds"] == 40
    # Parent 200 + component 96 + invention 40 (1 round) + copy 2 x 20.
    assert mj["elapsed_time_seconds"] == 200 + 96 + 40 + 40
    assert mj["elapsed_lane_notes"] == [
        "sub-build invention assumes 2 research slots over 2 inventors (capacity, not today's free slots)",
    ]
