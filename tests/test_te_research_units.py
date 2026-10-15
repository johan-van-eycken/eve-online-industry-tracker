# tests/test_te_research_units.py
"""Task 8c: TE research in EVE's units, end to end.

EVE's TE is a percentage, 2% per research level: level L gives TE 2L%, so
TE runs 0..20 and research levels 0..10. The planner stores and compares TE
as the percentage (the asset's blueprint_time_efficiency) and converts to a
level only where a level is needed: the research fee table. Before the fix
compute_optimal_te stepped 1% per level and returned 10, read as TE 10% =
level 5: a TE 0 BPO was researched only halfway and TE 10..18 BPOs were
never scheduled. Fake numbers throughout.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision

BLUEPRINT_DATA = {
    999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                            "materials": [{"type_id": 34, "quantity": 100}]}},
}

# Cumulative fee from research level 0 to level L (index L), as the producer
# publishes it: fake, distinct per level so a wrong span shows.
FEE_TO_LEVEL = [0.0, 1.0, 3.0, 7.0, 15.0, 31.0, 63.0, 127.0, 255.0, 511.0, 1023.0]


class _AdminStub:
    def get(self, section, key, default=None):
        raise KeyError(key)  # ChainPlanner's own fallbacks: TE threshold 1.0%


class _Admin:
    def get(self, section, key, default=None):
        return default


class _Chars:
    def list_characters(self):
        return [{"character_id": 1, "character_name": "Pilot", "skills": {"skills": [
            {"skill_id": 3406, "skill_name": "Laboratory Operation", "trained_skill_level": 5},
            {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 5},
        ]}}]


def _sde_session(manufacturing_time: int) -> MagicMock:
    blueprint = MagicMock()
    blueprint.activities = {"manufacturing": {"time": manufacturing_time, "materials": []}}
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = blueprint
    return session


def _planner(manufacturing_time: int = 6000) -> ChainPlanner:
    """A ChainPlanner whose optimal TE comes from the real compute_optimal_te
    on a fake SDE blueprint; ME is held at its optimum so only TE varies."""
    provider = SimpleNamespace(sde_session=lambda: _sde_session(manufacturing_time))
    planner = ChainPlanner(industry_service=None, session_provider=provider, admin_settings=_AdminStub())
    planner._compute_optimal_me = lambda bp_type_id, runs=1: 10
    return planner


def _row() -> dict:
    return {"type_id": 12345, "type_name": "Thing", "quantity": 1, "manufacturing_job": {
        "runs": 1, "manufacturing_job_cost": 10.0,
        "blueprint_sde": {"blueprint_type_id": 999},
        "research_job_cost_by_level": {
            "research_material": FEE_TO_LEVEL,
            "research_time": [2 * v for v in FEE_TO_LEVEL],
        },
    }}


def _decision(row) -> ItemDecision:
    return ItemDecision(
        type_id=12345, type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )


def _plan(te: int, manufacturing_time: int = 6000) -> dict:
    row = _row()
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False,
                          blueprint_material_efficiency=10, blueprint_time_efficiency=te)
    _planner(manufacturing_time).plan_chain(
        [_decision(row)], {"bpo_assets_by_type_id": {999: [bpo]}, "blueprint_data": BLUEPRINT_DATA},
    )
    return row


def _te_action(row):
    plan = ChainPlan(decisions=[_decision(row)])
    actions = {a.action_type: a for a in CharacterAssigner().assign(plan, [], _Chars(), _Admin())}
    return actions.get("te_research")


def test_a_te_0_bpo_gets_a_te_20_target():
    row = _plan(te=0)
    assert row["needs_te_research"] is True
    assert row["te_current"] == 0
    assert row["te_research_target"] == 20  # level 10, not TE 10% (level 5)


@pytest.mark.parametrize("te", [10, 12, 18])
def test_a_bpo_between_te_10_and_18_is_still_scheduled_up_to_20(te):
    row = _plan(te=te)
    assert row["needs_te_research"] is True
    assert row["te_current"] == te
    assert row["te_research_target"] == 20


def test_a_te_20_bpo_is_not_scheduled():
    row = _plan(te=20)
    assert "needs_te_research" not in row


def test_the_ceil_noise_case_from_the_sde_simulation_is_scheduled_to_20():
    # A 10 s blueprint: level 1 rounds back up to 10 s, which made the old
    # loop return 0 (never researched).
    row = _plan(te=0, manufacturing_time=10)
    assert row["te_research_target"] == 20


@pytest.mark.parametrize("te, from_level", [(0, 0), (10, 5), (14, 7), (18, 9)])
def test_the_fee_covers_exactly_levels_current_to_target(te, from_level):
    action = _te_action(_plan(te=te))
    assert action is not None
    assert action.estimated_job_fee_isk == pytest.approx(2 * (FEE_TO_LEVEL[10] - FEE_TO_LEVEL[from_level]))


def test_the_te_note_names_percentages_and_levels():
    action = _te_action(_plan(te=10))
    assert action.notes == "TE research: 10% → 20% (level 5 → 10)"
