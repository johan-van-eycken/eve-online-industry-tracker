# tests/test_te_research_units.py
"""Task 8c: TE research in EVE's units, end to end.

EVE's TE is a percentage, 2% per research level: level L gives TE 2L%, so
TE runs 0..20 and research levels 0..10. The planner stores and compares TE
as the percentage (the asset's blueprint_time_efficiency) and converts to a
level only where a level is needed: the research fee and duration tables.

Fix round 1 (the user's decisions):
- TE is always researched to the max (industry.max_blueprint_time_efficiency,
  default 20); the TE threshold setting is gone.
- Research first: a BPO with a research action in the plan is not used for
  manufacturing in that plan; the item falls back to its owned BPCs, or none.
- Research actions carry their duration (the producer's research-time model
  for the exact level span) and an estimated completion.
Fake numbers throughout.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision

BLUEPRINT_DATA = {
    999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                            "materials": [{"type_id": 34, "quantity": 100}]}},
}

# Cumulative fee / duration from research level 0 to level L (index L), as
# the producer publishes them: fake, distinct per level so a wrong span shows.
FEE_TO_LEVEL = [0.0, 1.0, 3.0, 7.0, 15.0, 31.0, 63.0, 127.0, 255.0, 511.0, 1023.0]
SECONDS_TO_LEVEL = [0, 60, 180, 420, 900, 1860, 3780, 7620, 15300, 30660, 61380]


class _AdminStub:
    """No settings store: ChainPlanner's own fallbacks (TE max 20)."""

    def get(self, section, key, default=None):
        raise KeyError(key)


class _AdminMaxTe:
    def __init__(self, max_te):
        self.max_te = max_te

    def get(self, section, key, default=None):
        if (section, key) == ("industry", "max_blueprint_time_efficiency"):
            return self.max_te
        raise KeyError(key)


class _Admin:
    def get(self, section, key, default=None):
        return default


class _Chars:
    def list_characters(self):
        return [{"character_id": 1, "character_name": "Pilot", "skills": {"skills": [
            {"skill_id": 3406, "skill_name": "Laboratory Operation", "trained_skill_level": 5},
            {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 5},
        ]}}]


def _planner(admin=None) -> ChainPlanner:
    """ME is held at its optimum so only TE varies; TE needs no SDE lookup."""
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=admin or _AdminStub())
    planner._compute_optimal_me = lambda bp_type_id, runs=1: 10
    return planner


def _row(**job) -> dict:
    manufacturing_job = {
        "runs": 10, "manufacturing_job_cost": 50.0, "material_cost": 1000.0, "total_cost": 1050.0,
        "materials": {"34": {"type_id": 34, "quantity": 1000}},
        "blueprint_sde": {"blueprint_type_id": 999},
        "blueprint_source_kind": "owned_blueprint_original",
        "job_segments": [{"runs": 10, "source": "owned_blueprint_original", "materials": {"34": 1000}}],
        "research_job_cost_by_level": {
            "research_material": FEE_TO_LEVEL,
            "research_time": [2 * v for v in FEE_TO_LEVEL],
        },
        "research_duration_seconds_by_level": {
            "research_material": SECONDS_TO_LEVEL,
            "research_time": [3 * s for s in SECONDS_TO_LEVEL],
        },
    }
    manufacturing_job.update(job)
    return {"type_id": 12345, "type_name": "Thing", "quantity": 10, "manufacturing_job": manufacturing_job}


def _decision(row) -> ItemDecision:
    return ItemDecision(
        type_id=12345, type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )


def _plan(te: int, *, me: int = 10, admin=None, row=None) -> dict:
    row = row if row is not None else _row()
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False,
                          blueprint_material_efficiency=me, blueprint_time_efficiency=te)
    _planner(admin).plan_chain(
        [_decision(row)], {"bpo_assets_by_type_id": {999: [bpo]}, "blueprint_data": BLUEPRINT_DATA},
    )
    return row


def _actions(row) -> dict:
    plan = ChainPlan(decisions=[_decision(row)])
    return {a.action_type: a for a in CharacterAssigner().assign(plan, [], _Chars(), _Admin())}


# --- the TE target: always the max ---------------------------------------------

def test_a_te_0_bpo_gets_a_te_20_target():
    row = _plan(te=0)
    assert row["needs_te_research"] is True
    assert row["te_current"] == 0
    assert row["te_research_target"] == 20  # level 10, not TE 10% (level 5)


@pytest.mark.parametrize("te", [2, 10, 12, 18])
def test_a_bpo_below_the_max_is_scheduled_up_to_20(te):
    row = _plan(te=te)
    assert row["needs_te_research"] is True
    assert row["te_current"] == te
    assert row["te_research_target"] == 20


def test_a_te_20_bpo_is_not_scheduled():
    row = _plan(te=20)
    assert "needs_te_research" not in row


def test_the_target_is_the_max_blueprint_te_setting():
    row = _plan(te=10, admin=_AdminMaxTe(16))
    assert row["te_research_target"] == 16
    assert "needs_te_research" not in _plan(te=16, admin=_AdminMaxTe(16))


def test_the_te_target_needs_no_sde_lookup():
    """The target is the setting, so an SDE failure cannot block TE research."""
    planner = _planner()
    assert not hasattr(planner, "_compute_optimal_te")


# --- fee, note and duration over the exact level span -----------------------------

@pytest.mark.parametrize("te, from_level", [(0, 0), (10, 5), (14, 7), (18, 9)])
def test_the_fee_covers_exactly_levels_current_to_target(te, from_level):
    action = _actions(_plan(te=te))["te_research"]
    assert action.estimated_job_fee_isk == pytest.approx(2 * (FEE_TO_LEVEL[10] - FEE_TO_LEVEL[from_level]))


def test_the_te_note_names_percentages_levels_and_the_duration():
    action = _actions(_plan(te=10))["te_research"]
    # Levels 5 -> 10: 3 x (61380 - 1860) s = 178560 s = 2d 1h 36m.
    assert action.notes.startswith("TE research: 10% → 20% (level 5 → 10); duration 2d 1h 36m")


def test_the_te_0_to_20_duration_sets_the_estimated_completion():
    before = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    action = _actions(_plan(te=0))["te_research"]
    after = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    span = timedelta(seconds=3 * SECONDS_TO_LEVEL[10])  # levels 0 -> 10
    assert before + span <= action.estimated_completion <= after + span
    assert "duration 2d 3h 9m" in action.notes


def test_the_me_research_carries_its_span_duration_too():
    action = _actions(_plan(te=20, me=4))["me_research"]
    assert "duration 16h 48m" in action.notes  # levels 4 -> 10: 61380 - 900 s


def test_an_unknown_research_duration_is_said_and_has_no_completion():
    row = _row()
    del row["manufacturing_job"]["research_duration_seconds_by_level"]
    action = _actions(_plan(te=0, row=row))["te_research"]
    assert action.estimated_completion is None
    assert "duration unknown" in action.notes


# --- research first ----------------------------------------------------------------

def test_a_bpo_in_research_gets_no_manufacture_action(caplog):
    caplog.set_level(logging.INFO)
    actions = _actions(_plan(te=0))
    assert "te_research" in actions
    assert "manufacture" not in actions
    assert "BPO in research" in actions["te_research"].notes
    assert any("BPO in research" in r.getMessage() for r in caplog.records)


def test_a_copy_of_the_bpo_conflicts_with_research_too():
    row = _row(blueprint_source_kind="copied_from_owned_blueprint_original", job_segments=[
        {"runs": 10, "source": "copied_from_owned_blueprint_original", "materials": {"34": 1000}},
    ])
    assert "manufacture" not in _actions(_plan(te=0, row=row))


def test_a_bpo_in_research_falls_back_to_the_owned_bpc_runs():
    row = _row(blueprint_source_kind="owned_blueprint_copy", job_segments=[
        {"runs": 4, "source": "owned_blueprint_copy", "materials": {"34": 400}},
        {"runs": 6, "source": "copied_from_owned_blueprint_original", "materials": {"34": 594}},
    ])
    actions = _actions(_plan(te=0, row=row))
    build = actions["manufacture"]
    assert build.runs == 4
    assert build.quantity == 4
    assert build.materials == {34: 400}
    assert build.estimated_job_fee_isk == pytest.approx(50.0 / 10 * 4)
    assert "BPO in research" in build.notes
    assert "te_research" in actions


def test_a_batch_on_owned_bpcs_only_is_built_in_full_while_the_bpo_researches():
    row = _row(blueprint_source_kind="owned_blueprint_copy", job_segments=[
        {"runs": 10, "source": "owned_blueprint_copy", "materials": {"34": 1000}},
    ])
    actions = _actions(_plan(te=0, row=row))
    assert actions["manufacture"].runs == 10
    assert actions["manufacture"].materials == {34: 1000}
    assert "te_research" in actions


def test_a_fully_researched_bpo_is_built_from():
    actions = _actions(_plan(te=20))
    assert "te_research" not in actions and "me_research" not in actions
    assert actions["manufacture"].runs == 10
