"""ME/TE research on a product's only BPO stops its production while it is in
the lab: it is planned only when the saving pays that back within
planner_research_payback_days. Fake data."""
from __future__ import annotations

import math
from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner, _research_payback
from eve_online_industry_tracker.application.daily_planner.models import ItemDecision

DAY = 86_400
# Level 0 -> 10 of ME takes 10 days; TE 0 -> 20 (level 10) as well. No fees.
TEN_DAYS = [0, *[DAY * (i + 1) for i in range(10)]]


def _row(*, material_cost):
    return {"type_id": 12345, "type_name": "Thing", "quantity": 10, "manufacturing_job": {
        "runs": 10, "material_cost": material_cost, "total_cost": material_cost, "manufacturing_job_cost": 0.0,
        "materials": {"34": {"type_id": 34, "quantity": 1000}},
        "blueprint_sde": {"blueprint_type_id": 999}, "blueprint_source_kind": "owned_blueprint_original",
        "job_segments": [{"runs": 10, "source": "owned_blueprint_original"}],
        "research_job_cost_by_level": {"research_material": [0.0] * 11, "research_time": [0.0] * 11},
        "research_duration_seconds_by_level": {"research_material": TEN_DAYS, "research_time": TEN_DAYS},
    }}


def _decision(row, *, isk_per_hour=1e6 / 24, velocity=100.0, profit_per_batch=100.0):
    """Market-bound: one slot makes 1M a day, the market buys 100 units a day at 10 profit each (1,000/day)."""
    return ItemDecision(
        type_id=12345, type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=profit_per_batch, isk_per_hour=isk_per_hour, margin_pct=20.0,
        days_of_supply_current=1.0, effective_velocity=velocity, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )


def test_me_research_that_saves_a_lot_pays_back_fast():
    # 1,000 material a unit x 10% x 100 units a day = 10,000/day saved; 10 days x 1,000/day lost.
    payback, days = _research_payback(_decision(_row(material_cost=10_000)), "research_material", 0, 10)
    assert days == 10
    assert payback == 1.0


def test_me_research_on_cheap_materials_pays_back_slowly():
    # 10 material a unit: 100/day saved for 10,000 lost.
    payback, _ = _research_payback(_decision(_row(material_cost=100)), "research_material", 0, 10)
    assert payback == 100.0


def test_te_research_adds_nothing_while_the_market_is_the_limit():
    payback, _ = _research_payback(_decision(_row(material_cost=100)), "research_time", 0, 10)
    assert math.isinf(payback)


def test_te_research_queued_behind_me_counts_both_lab_stays():
    # Slot-bound: the slot makes 240/day, the market would take 1,000/day; TE +20% = 48/day.
    decision = _decision(_row(material_cost=100), isk_per_hour=10.0)
    payback, _ = _research_payback(decision, "research_time", 0, 10, queued_days=10)
    assert payback == 240 * 20 / 48


def test_an_unknown_input_returns_none():
    row = _row(material_cost=100)
    del row["manufacturing_job"]["research_duration_seconds_by_level"]
    assert _research_payback(_decision(row), "research_material", 0, 10) is None


# --- the gate in ChainPlanner ------------------------------------------------

class _Admin:
    def get(self, section, key, default=None):
        if key == "planner_research_payback_days":
            return 30
        if (section, key) == ("industry", "max_blueprint_time_efficiency"):
            return 20
        raise KeyError(key)


def _plan(decision, *, bpos=1, bpcs=()):
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=_Admin())
    planner._compute_optimal_me = lambda bp_data, runs=1: 10
    assets = [SimpleNamespace(type_id=999, item_id=900 + i, is_blueprint_copy=False,
                              blueprint_material_efficiency=0, blueprint_time_efficiency=20) for i in range(bpos)]
    planner.plan_chain([decision], {
        "bpo_assets_by_type_id": {999: assets},
        "bpc_assets_by_type_id": {999: list(bpcs)} if bpcs else {},
        "blueprint_data": {999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                                                   "materials": [{"type_id": 34, "quantity": 100}]}}},
    })
    return decision


def test_research_that_pays_back_within_the_setting_is_planned():
    decision = _plan(_decision(_row(material_cost=10_000)))
    assert decision.overview_row.get("needs_me_research") is True


def test_research_that_does_not_pay_back_is_skipped_and_the_bpo_builds():
    decision = _plan(_decision(_row(material_cost=100)))
    assert "needs_me_research" not in decision.overview_row
    assert "no ME research: pays back in 100 days (max 30)" in decision.decision_reason


def test_with_a_second_bpo_research_costs_no_production_and_goes_ahead():
    decision = _plan(_decision(_row(material_cost=100)), bpos=2)
    assert decision.overview_row.get("needs_me_research") is True
