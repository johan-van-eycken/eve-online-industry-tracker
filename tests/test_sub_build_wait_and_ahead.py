"""A sub-build makes its parent wait for the next daily check, so it is built
only when it saves more than that wait costs; free slots build the components
of the next batch ahead (2026-11-24: Knight X1 sub-built 6 Mobile Large Warp
Disruptor I that Duke X1's MLWD II job needed at 11:00)."""
from __future__ import annotations

from types import SimpleNamespace

from test_chain_planner_keying import (
    _SUB_PHASE1, _batch_materials, _decision, _planner, _row, _shopping,
)
from eve_online_industry_tracker.application.daily_planner import character_assigner as ca
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction


def _plan(profit, *, stock=None, component_row=None):
    """A 1-run parent of 1 h needing 100 of component 54321: built for 18 ISK
    of Tritanium versus 100 at the market, so building saves 82."""
    row = _row(materials=_batch_materials(**{"54321": 100}), time_seconds=3600)
    phase1 = {**_SUB_PHASE1, "corp_material_stock": stock or {},
              "overview_rows": [component_row] if component_row else []}
    parent = _decision(row, absolute_profit_per_batch=profit)
    return _planner().plan_chain([parent], phase1), parent


def _subs(plan):
    return [d for d in plan.decisions if d.is_sub_component]


def test_a_wait_costing_more_than_the_saving_buys_the_component():
    plan, parent = _plan(profit=1000.0)
    assert _subs(plan) == []
    assert "less than the 0.0M a day the job would wait" in parent.overview_row["component_buy_reasons"][54321]


def test_a_saving_larger_than_the_wait_still_sub_builds():
    plan, _parent = _plan(profit=10.0)
    (sub,) = _subs(plan)
    assert sub.overview_row["quantity_needed"] == 100


def test_ahead_builds_one_batch_net_of_the_stock_today_leaves():
    component = {"type_id": 54321, "manufacturing_job": {"runs": 10, "time_seconds": 600}}
    plan, _parent = _plan(profit=1000.0, stock={54321: 150}, component_row=component)
    # Today the parent takes 100 of the 150; the next batch needs 100, 50 are left.
    (ahead,) = plan.plan_ahead_sub_builds({12345}, set())
    assert ahead.overview_row["quantity_needed"] == 50
    assert ahead.overview_row["sub_seconds_per_run"] == 60
    assert ahead.decision_reason.startswith("Sub-manufacture ahead: components for the next batch of Thing")
    # A component sub-built today already uses its BPO; a parent not built today needs nothing.
    assert plan.plan_ahead_sub_builds({12345}, {54321}) == []
    assert plan.plan_ahead_sub_builds(set(), set()) == []


def _ahead_sub(runs=10, seconds=3600.0):
    return SimpleNamespace(
        type_id=54321, type_name="Widget", decision_reason="Sub-manufacture ahead: components for the next batch of Thing",
        overview_row={
            "blueprint_type_id": 888, "sub_seconds_per_run": seconds, "market_buy_cost": 100.0,
            "sub_job_batches": [{"runs": runs, "output": runs * 10, "quantity": runs * 10,
                                 "materials": {34: 2 * runs}, "cost": 2.0 * runs, "fee": 1.0 * runs,
                                 "after_job": None}],
        },
    )


class _Gate:
    def check(self, _bp, _activity):
        return frozenset({1}), None


def _assigner(deadline_seconds):
    assigner = ca.CharacterAssigner()
    assigner._deadlines = [deadline_seconds]
    return assigner


def test_an_ahead_job_is_cut_to_end_before_the_next_check_and_charged():
    slots = {1: {"character_id": 1, "character_name": "Knight", "free_mfg": 1, "free_research": 0, "total_active_jobs": 0}}
    budget = SimpleNamespace(charged=[], charge=lambda amount: budget.charged.append(amount) or True)
    (action,) = _assigner(4 * 3600 + 5)._ahead_sub_build(_ahead_sub(), slots, [], _Gate(), budget)
    assert (action.runs, action.quantity, action.for_next_batch) == (4, 40, True)
    assert action.materials == {34: 8}
    assert budget.charged == [8.0 + 4.0]
    assert "cut from 10 to 4 runs" in action.notes
    assert slots[1]["free_mfg"] == 0


def test_no_ahead_job_without_a_build_time():
    slots = {1: {"character_id": 1, "character_name": "Knight", "free_mfg": 1, "free_research": 0, "total_active_jobs": 0}}
    assert _assigner(86_400)._ahead_sub_build(_ahead_sub(seconds=None), slots, [], _Gate(), None) == []


def _action(action_type, type_id, qty, *, ahead=False, materials=None):
    return AssignedAction(
        type_id=type_id, type_name=f"T{type_id}", action_type=action_type, character_id=1,
        character_name="Knight", quantity=qty, runs=1, estimated_cost_isk=0.0, estimated_profit_isk=None,
        estimated_completion=None, notes="Build", materials=materials, for_next_batch=ahead,
    )


def test_the_parent_says_it_waits_for_todays_sub_build_but_not_for_an_ahead_one():
    sub = SimpleNamespace(is_sub_component=True, type_id=54321, overview_row={"requested_by_type_ids": [12345]})
    parent = _action("manufacture", 12345, 1)
    ca._note_waiting_parents([sub], [parent, _action("sub_manufacture", 54321, 100, ahead=True)])
    assert parent.notes == "Build"
    ca._note_waiting_parents([sub], [parent, _action("sub_manufacture", 54321, 100)])
    assert parent.notes == "Build; starts at the next daily check, after the sub-build of T54321 (Knight)"


def test_an_ahead_build_does_not_cover_todays_parent_purchase():
    parent = _action("manufacture", 12345, 1, materials={54321: 100})
    ahead = _action("sub_manufacture", 54321, 100, ahead=True, materials={34: 20})
    bought = _shopping([parent, ahead], stock={}, phase1=_SUB_PHASE1)
    assert bought[54321] == 100
