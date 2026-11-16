# tests/test_daily_planner_budget_assignment.py
"""Phase 6 charges every manufacture to the corp wallet (WalletBudget). Fake data."""
from __future__ import annotations

import math

import pytest

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner, cut_batch
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars
from planner_skill_fakes import NO_SKILL_REQUIREMENTS

MAT, COMP, SUB_INPUT = 34, 777, 35
M = 1_000_000


class _AdminStub:
    def get(self, section, key, default=None):
        return default


def _char(char_id, mfg_slots=1):
    # Mass Production N gives 1 + N manufacturing slots.
    skills = [{"skill_id": 0, "skill_name": "Mass Production", "group_name": "Production",
               "trained_skill_level": mfg_slots - 1}] if mfg_slots > 1 else []
    return {"character_id": char_id, "character_name": f"Pilot {char_id}", "skills": {"skills": skills, "total_sp": 0}}


def _parent(type_id=100, runs=30, materials=None, fee=0.0, crpd=None, score=1.0, cap=None):
    materials = {MAT: runs} if materials is None else materials
    return ItemDecision(
        type_id=type_id, type_name=f"Product {type_id}", decision="build", decision_reason="test",
        adjusted_score=score, absolute_profit_per_batch=30 * M, isk_per_hour=1.0, margin_pct=20.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1, pipeline_stage="manufacturing",
        capital_return_per_day=crpd, market_cap_runs=cap,
        overview_row={
            "type_id": type_id, "quantity": runs, "blueprint_type_id": type_id + 1,
            "manufacturing_job": {
                "runs": runs, "material_cost": 10.0 * M * runs, "total_cost": 10.0 * M * runs + fee,
                "manufacturing_job_cost": fee,
                "materials": {str(t): {"type_id": t, "quantity": q} for t, q in materials.items()},
            },
        },
    )


def _sub(need=10, parents=(100,), per_run=1, run_cost=1 * M):
    runs = math.ceil(need / per_run)
    return ItemDecision(
        type_id=COMP, type_name="Component", decision="build", decision_reason="Sub-manufacture",
        adjusted_score=0.0, absolute_profit_per_batch=0.0, isk_per_hour=0.0, margin_pct=0.0,
        days_of_supply_current=0.0, effective_velocity=1.0, meta_group_id=1, pipeline_stage="manufacturing",
        is_sub_component=True,
        overview_row={
            "type_id": COMP, "blueprint_type_id": 778, "quantity_needed": need,
            "requested_by_type_ids": list(parents),
            "sub_runs": runs, "sub_batch_materials": {SUB_INPUT: runs},
            "sub_manufacture_cost": 0.8 * run_cost * runs, "sub_install_fee": 0.2 * run_cost * runs,
            "sub_max_runs_per_job": runs, "market_buy_cost": 2.0 * M * need,
            "sub_job_batches": [{"quantity": need, "output": runs * per_run, "runs": runs,
                                 "cost": 0.8 * run_cost * runs, "materials": {SUB_INPUT: runs},
                                 "fee": 0.2 * run_cost * runs, "after_job": None}],
        },
    )


#: The sub's input costs 1 ISK: one unit per reserved sub job is charged for ME rounding.
PRICES = {MAT: {"vwap_5d": 10.0 * M}, COMP: {"vwap_5d": 2.0 * M}, SUB_INPUT: {"vwap_5d": 1.0}}


def _assign(decisions, chars, budget, stock=None, prices=PRICES, plan=None):
    plan = plan or ChainPlan(decisions=decisions)
    return CharacterAssigner().assign(
        plan, [], _Chars(chars), _AdminStub(), dict(prices),
        blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget, corp_material_stock=stock,
    )


def _of(actions, kind):
    return [a for a in actions if a.action_type == kind]


def test_runs_cut_to_budget():
    budget = WalletBudget(balance=95 * M, reserve_pct=0)
    actions = _assign([_parent()], [_char(1)], budget)
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs == 9 and mfg.materials == {MAT: 9} and mfg.quantity == 9
    assert budget.available < 10 * M and budget.spent == 90 * M


def test_one_run_unaffordable_gives_no_manufacture():
    decision = _parent(runs=1, materials={MAT: 20})  # 200M for one run
    budget = WalletBudget(balance=100 * M, reserve_pct=0)
    actions = _assign([decision], [_char(1)], budget)
    assert not _of(actions, "manufacture")
    assert "not built today: wallet (100,000,000 ISK left, 1 run needs 200,000,000)" in decision.decision_reason
    assert budget.spent == 0


def test_order_is_capital_return():
    low_score = _parent(type_id=100, runs=1, crpd=0.5, score=1.0)
    high_score = _parent(type_id=200, runs=1, crpd=0.1, score=9.0)
    actions = _assign([high_score, low_score], [_char(1)], WalletBudget(balance=1e12, reserve_pct=0))
    assert [a.type_id for a in _of(actions, "manufacture")] == [100]


def test_stock_lowers_the_charge():
    budget = WalletBudget(balance=1e12, reserve_pct=0)
    _assign([_parent(fee=5 * M)], [_char(1)], budget, stock={MAT: 30})
    assert budget.spent == 5 * M


def test_sub_component_priced_at_build_cost():
    budget = WalletBudget(balance=1e12, reserve_pct=0)
    actions = _assign([_parent(runs=1, materials={COMP: 10}), _sub()], [_char(1, mfg_slots=2)], budget)
    assert budget.spent == 10 * M + 1  # 10 x (0.8M + 0.2M), not 10 x 2M; + 1 ISK rounding unit
    (sub,) = _of(actions, "sub_manufacture")
    assert sub.character_id == 1


def test_sub_job_without_slot_priced_at_market():
    budget = WalletBudget(balance=1e12, reserve_pct=0)
    sub = _sub()
    actions = _assign([_parent(runs=1, materials={COMP: 10}), sub], [_char(1)], budget)
    assert budget.spent == 20 * M
    assert not _of(actions, "sub_manufacture")
    assert sub.overview_row["reserved_sub_pilots"] == []


def test_cut_parent_resizes_subs():
    need = 30
    parent = _parent(runs=30, materials={MAT: 30, COMP: need})  # 10M + 1M per run
    seen = {}

    def resize(fallen, dropped=frozenset(), scale=None):
        seen["scale"] = scale
        return [_sub(need=math.ceil(need * scale[100]))]

    plan = ChainPlan(decisions=[parent, _sub(need=need)], resize_sub_components=resize)
    budget = WalletBudget(balance=100 * M, reserve_pct=0)
    actions = _assign(None, [_char(1, mfg_slots=3)], budget, plan=plan)
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs == 9 and seen["scale"] == {100: 9 / 30}
    assert [a.quantity for a in _of(actions, "sub_manufacture")] == [math.ceil(9 / 30 * need)]
    assert budget.spent == 9 * 11 * M + 1


def test_sub_charged_in_whole_runs():
    """30 units per sub run at 30M; each parent run needs 1 component + 10M."""
    parent = _parent(runs=30, materials={MAT: 30, COMP: 30})
    resized = {}

    def resize(fallen, dropped=frozenset(), scale=None):
        resized["sub"] = _sub(need=math.ceil(30 * scale[100]), per_run=30, run_cost=30 * M)
        return [resized["sub"]]

    plan = ChainPlan(decisions=[parent, _sub(need=30, per_run=30, run_cost=30 * M)], resize_sub_components=resize)
    budget = WalletBudget(balance=100 * M, reserve_pct=0)
    actions = _assign(None, [_char(1, mfg_slots=3)], budget, plan=plan)
    (mfg,) = _of(actions, "manufacture")
    sub_row = resized["sub"].overview_row
    assert mfg.runs == 6  # 7 runs: 70M + 30M + 1 ISK rounding > 100M
    assert budget.spent >= sub_row["sub_manufacture_cost"] + sub_row["sub_install_fee"] + mfg.runs * 10 * M
    assert budget.spent <= 100 * M


@pytest.mark.parametrize("balance, runs, sub_units", [(1000.0, 10, None), (2403.0, 24, 4)])
def test_charge_mirrors_the_real_sub_re_size(balance, runs, sub_units):
    """ChainPlanner's real re-size: request 30 (1 a run), corp stock 20. Cut to
    10 runs the 10 come from stock (no sub); cut to 24 the sub builds 4, in one
    whole run. Tritanium (34) 100 a run, 1 ISK each."""
    from test_chain_planner_keying import _SUB_PHASE1, _batch_materials, _decision, _planner, _row

    row = _row(runs=30, materials=_batch_materials(**{"54321": 30, "34": 3000}), manufacturing_job_cost=0.0)
    row["quantity"] = 30
    phase1 = {**_SUB_PHASE1, "corp_material_stock": {54321: 20}}
    plan = _planner().plan_chain([_decision(row)], phase1)
    (sub,) = [d for d in plan.decisions if d.is_sub_component]
    assert sub.overview_row["quantity_needed"] == 10

    budget = WalletBudget(balance=balance, reserve_pct=0)
    actions = _assign(None, [_char(1, mfg_slots=3)], budget, stock={54321: 20},
                      prices=_SUB_PHASE1["market_depth_cache"], plan=plan)
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs == runs
    resized = [d for d in plan.decisions if d.is_sub_component]
    sub_cost = sum(d.overview_row["sub_manufacture_cost"] + d.overview_row["sub_install_fee"] for d in resized)
    assert [d.overview_row["quantity_needed"] for d in resized] == ([sub_units] if sub_units else [])
    assert sub_cost + runs * 100 <= budget.spent <= balance


def test_sub_units_use_the_re_sizes_float_ceil():
    """204 x 7/12 is 119, but _sub_decisions' ceil(204 * (7 / 12)) builds 120: charge 120."""
    parent = _parent(runs=12, materials={MAT: 12, COMP: 204})
    resized = {}

    def resize(fallen, dropped=frozenset(), scale=None):
        resized["sub"] = _sub(need=math.ceil(204 * scale[100]))
        return [resized["sub"]]

    plan = ChainPlan(decisions=[parent, _sub(need=204)], resize_sub_components=resize)
    budget = WalletBudget(balance=191 * M, reserve_pct=0)
    actions = _assign(None, [_char(1, mfg_slots=3)], budget, plan=plan)
    (mfg,) = _of(actions, "manufacture")
    row = resized["sub"].overview_row
    assert mfg.runs == 7 and row["quantity_needed"] == 120
    assert budget.spent == 70 * M + 120 * M + 1
    assert budget.spent >= row["sub_manufacture_cost"] + row["sub_install_fee"] + 70 * M


def test_no_budget_means_old_behaviour():
    decisions = [_parent(type_id=100, score=2.0), _parent(type_id=200, score=1.0)]
    actions = _assign(decisions, [_char(1)], None)
    (mfg,) = _of(actions, "manufacture")
    assert mfg.type_id == 100 and mfg.runs == 30 and mfg.materials == {MAT: 30}


def test_cut_batch_scales_the_figures():
    action = AssignedAction(
        type_id=1, type_name="x", action_type="manufacture", character_id=1, character_name="p",
        quantity=30, runs=30, estimated_cost_isk=300.0, estimated_profit_isk=None, estimated_completion=None,
        notes="", materials={MAT: 31}, estimated_build_cost_isk=330.0, estimated_job_fee_isk=30.0,
    )
    cut_batch(action, 10, 30)
    assert (action.runs, action.quantity, action.materials) == (10, 10, {MAT: 11})
    assert (action.estimated_cost_isk, action.estimated_build_cost_isk, action.estimated_job_fee_isk) == (100.0, 110.0, 10.0)
    assert action.estimated_profit_isk is None
