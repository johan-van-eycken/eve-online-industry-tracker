# tests/test_daily_planner_job_fees.py
"""D4 + A2: every planner job action carries its estimated install fee.

The fee is the producer's own estimate (EIV x cost index x structure/rig
modifiers, plus the surcharge), scaled to the job the action launches:
- manufacture: the batch's manufacturing_job_cost;
- sub_manufacture: the component's fee per run x the job's runs;
- invent: the fee of one attempt x the whole attempts the action launches;
- copy: the fee of one source-copy run x the copy runs (one per attempt);
- ME/TE research: the producer's cumulative research fee to the target level
  minus the fee to the current level.
Unknown is None, never 0. Fake numbers throughout.
"""
from __future__ import annotations

import logging

import pytest

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.industry import overview_row as orow

PILOT = 1


class _Admin:
    def get(self, section, key, default=None):
        return default


class _Chars:
    def list_characters(self):
        # Laboratory Operation 5 / Mass Production 5: enough slots for every action.
        return [{"character_id": PILOT, "character_name": "Pilot", "skills": {"skills": [
            {"skill_id": 3406, "skill_name": "Laboratory Operation", "trained_skill_level": 5},
            {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 5},
        ]}}]


def _decision(row, **overrides):
    base = dict(
        type_id=int(row["type_id"]), type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2,
        pipeline_stage="manufacturing", overview_row=row,
    )
    base.update(overrides)
    return ItemDecision(**base)


def _actions(row, **overrides):
    plan = ChainPlan(decisions=[_decision(row, **overrides)])
    return {a.action_type: a for a in CharacterAssigner().assign(plan, [], _Chars(), _Admin())}


def _invent_row(*, per_attempt=4.4, per_copy_run=0.0099, whole_attempts=4):
    """A T2 row whose batch 1 invents: 20 runs, 10 runs per BPC, chance 0.5
    for the best inventor (the only pilot), so 4 whole attempts."""
    return {
        "type_id": 12345, "quantity": 20, "needs_invention": True, "has_t1_bpo": True,
        "t1_blueprint_type_id": 999,
        "manufacturing_job": {
            "runs": 20, "manufacturing_job_cost": 2_500.0,
            "invention_chance": {
                "base_probability": 0.5, "skill_factor": 1.0, "decryptor_multiplier": 1.0,
                "probability": 0.5, "effective_probability": 0.5, "inventor_character_id": PILOT,
                "science_skill_type_ids": [11433, 11454], "encryption_skill_type_id": 21790,
            },
            "activity_breakdown": {"invention": {
                "runs": 4.0, "whole_attempts": whole_attempts, "invented_runs": 20,
                "runs_per_invented_blueprint": 10, "success_probability": 0.5,
                "total_job_cost": 17.6,
                "job_cost_per_attempt": per_attempt,
                "source_copy_job_cost_per_run": per_copy_run,
            }},
        },
    }


def test_manufacture_invent_and_copy_actions_carry_their_fees():
    actions = _actions(_invent_row())
    assert actions["manufacture"].estimated_job_fee_isk == 2_500.0
    # 4 whole attempts x 4.4 per attempt: the cash for the jobs really launched.
    assert actions["invent"].runs == 4
    assert actions["invent"].estimated_job_fee_isk == pytest.approx(17.6)
    # One source-copy run per attempt: 4 x 0.0099.
    assert actions["copy"].runs == 4
    assert actions["copy"].estimated_job_fee_isk == pytest.approx(0.0396)


def test_an_unpriced_fee_stays_unknown_and_warns(caplog):
    caplog.set_level(logging.WARNING)
    row = _invent_row(per_attempt=None, per_copy_run=None)
    row["manufacturing_job"]["manufacturing_job_cost"] = None
    actions = _actions(row)
    assert actions["manufacture"].estimated_job_fee_isk is None
    assert actions["invent"].estimated_job_fee_isk is None
    assert actions["copy"].estimated_job_fee_isk is None
    assert "install fee unknown" in caplog.text


def _research_row(**flags):
    # Cumulative fee from level 0 to level L, as the producer publishes it.
    to_level = [0.0, 1.0, 3.0, 7.0, 15.0, 31.0, 63.0, 127.0, 255.0, 511.0, 1023.0]
    return {
        "type_id": 12345, "quantity": 1, **flags,
        "manufacturing_job": {
            "runs": 1, "manufacturing_job_cost": 10.0,
            "blueprint_sde": {"blueprint_type_id": 999},
            "research_job_cost_by_level": {
                "research_material": to_level,
                "research_time": [2 * v for v in to_level],
            },
        },
    }


def test_me_research_fee_is_the_fee_to_the_target_minus_the_fee_to_the_current_level():
    row = _research_row(needs_me_research=True, me_current=2, me_research_target=5)
    assert _actions(row)["me_research"].estimated_job_fee_isk == pytest.approx(31.0 - 3.0)


def test_te_research_fee_reads_te_as_a_percentage_two_per_level():
    # TE 4% = level 2, TE 10% = level 5 (EVE: 2% per TE level).
    row = _research_row(needs_te_research=True, te_current=4, te_research_target=10)
    assert _actions(row)["te_research"].estimated_job_fee_isk == pytest.approx(2 * (31.0 - 3.0))


def test_an_odd_te_target_rounds_up_to_the_level_that_reaches_it():
    # TE 5% needs level 3 (6%).
    row = _research_row(needs_te_research=True, te_current=0, te_research_target=5)
    assert _actions(row)["te_research"].estimated_job_fee_isk == pytest.approx(2 * 7.0)


def test_research_without_a_published_fee_table_is_unknown(caplog):
    caplog.set_level(logging.WARNING)
    row = _research_row(needs_me_research=True, me_current=0, me_research_target=4)
    del row["manufacturing_job"]["research_job_cost_by_level"]
    assert _actions(row)["me_research"].estimated_job_fee_isk is None
    assert "install fee unknown" in caplog.text


def test_research_fee_accessor_rejects_out_of_range_levels():
    row = _research_row()
    assert orow.get_research_fee(row, "research_material", 0, 10) == pytest.approx(1023.0)
    assert orow.get_research_fee(row, "research_material", 3, 3) == 0.0
    assert orow.get_research_fee(row, "research_material", 0, 11) is None
    assert orow.get_research_fee(row, "research_material", 5, 2) is None
    assert orow.get_research_fee(row, "research_time", -1, 2) is None


def test_a_sub_job_fee_is_the_components_fee_per_run_times_its_runs():
    phase1 = {"overview_rows": [{"type_id": 54321, "manufacturing_job": {"runs": 4, "manufacturing_job_cost": 40.0}}]}
    assert ChainPlanner._component_fee_per_run(54321, phase1) == 10.0
    assert ChainPlanner._component_fee_per_run(11111, phase1) is None


def test_a_sub_manufacture_action_carries_its_jobs_fee():
    row = {
        "type_id": 54321, "type_name": "Part", "market_buy_cost": 1.0, "sub_max_runs_per_job": 6,
        "sub_job_batches": [
            {"runs": 6, "quantity": 6, "output": 6, "materials": {34: 60}, "cost": 300.0, "fee": 60.0},
            {"runs": 2, "quantity": 2, "output": 2, "materials": {34: 20}, "cost": 100.0, "fee": None},
        ],
    }
    plan = ChainPlan(decisions=[_decision(row, is_sub_component=True, meta_group_id=1)])
    subs = [a for a in CharacterAssigner().assign(plan, [], _Chars(), _Admin()) if a.action_type == "sub_manufacture"]
    assert [a.estimated_job_fee_isk for a in subs] == [60.0, None]


def test_manufacturing_fee_per_run_accessor():
    assert orow.get_manufacturing_fee_per_run({"manufacturing_job": {"runs": 4, "manufacturing_job_cost": 40.0}}) == 10.0
    assert orow.get_manufacturing_fee_per_run({"manufacturing_job": {"runs": 0, "manufacturing_job_cost": 40.0}}) is None
    assert orow.get_manufacturing_fee_per_run({"manufacturing_job": {"runs": 4}}) is None


def test_invention_fee_accessors_fall_back_to_the_later_batch_plan():
    # An owned T2 BPC covers batch 1: only invention_bpc_plan carries the rates.
    row = {"manufacturing_job": {"invention_bpc_plan": {
        "batch_runs": 20, "runs_per_invented_blueprint": 10,
        "job_cost_per_attempt": 4.4, "source_copy_job_cost_per_run": 0.0099,
    }}}
    assert orow.get_invention_fee_per_attempt(row) == 4.4
    assert orow.get_source_copy_fee_per_run(row) == 0.0099
    assert orow.get_invention_fee_per_attempt({"manufacturing_job": {}}) is None


# --- Task 8b fix round 1: an unknown fee carries its cause to Capital Reserved ---


def test_a_priced_fee_records_no_unknown_reason():
    actions = _actions(_invent_row())
    assert all(a.estimated_job_fee_unknown_reason is None for a in actions.values())


def test_each_unknown_fee_carries_the_producers_reason():
    row = _invent_row(per_attempt=None, per_copy_run=None)
    job = row["manufacturing_job"]
    job["manufacturing_job_cost"] = None
    job["manufacturing_job_cost_unknown_reason"] = "no manufacturing cost index"
    job["activity_breakdown"]["invention"]["job_cost_per_attempt_unknown_reason"] = "no invention cost index"
    job["activity_breakdown"]["invention"]["source_copy_job_cost_per_run_unknown_reason"] = "no copying cost index"
    actions = _actions(row)
    assert actions["manufacture"].estimated_job_fee_unknown_reason == "no manufacturing cost index"
    assert actions["invent"].estimated_job_fee_unknown_reason == "no invention cost index"
    assert actions["copy"].estimated_job_fee_unknown_reason == "no copying cost index"


def test_an_unknown_fee_without_a_published_reason_has_none_so_the_refresh_hint_shows():
    row = _invent_row()
    row["manufacturing_job"]["manufacturing_job_cost"] = None  # an old cached overview
    assert _actions(row)["manufacture"].estimated_job_fee_unknown_reason is None


def test_research_fee_reasons():
    row = _research_row(needs_me_research=True, me_current=0, me_research_target=4)
    row["manufacturing_job"]["research_job_cost_by_level"]["research_material"] = None
    row["manufacturing_job"]["research_job_cost_by_level_unknown_reason"] = {"research_material": "no research index"}
    assert _actions(row)["me_research"].estimated_job_fee_unknown_reason == "no research index"
    unknown_levels = _research_row(needs_me_research=True, me_current=None, me_research_target=4)
    assert "level" in _actions(unknown_levels)["me_research"].estimated_job_fee_unknown_reason


def test_a_sub_manufacture_action_carries_its_jobs_fee_reason():
    row = {
        "type_id": 54321, "type_name": "Part", "market_buy_cost": 1.0, "sub_max_runs_per_job": 6,
        "sub_job_batches": [
            {"runs": 2, "quantity": 2, "output": 2, "materials": {34: 20}, "cost": 100.0, "fee": None,
             "fee_unknown_reason": "no index"},
        ],
    }
    plan = ChainPlan(decisions=[_decision(row, is_sub_component=True, meta_group_id=1)])
    (sub,) = [a for a in CharacterAssigner().assign(plan, [], _Chars(), _Admin()) if a.action_type == "sub_manufacture"]
    assert sub.estimated_job_fee_unknown_reason == "no index"
