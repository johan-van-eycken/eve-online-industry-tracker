"""Planner review 2026-11-24: A (the slot order reads the manufacturing slots),
B (blueprints are booked in the slot order), C (learned accuracy weighs the
profit) and the per-compute reset of the plan summary diagnostics."""
from __future__ import annotations

import dataclasses

from test_blueprint_ledger import A, NO_SKILL_REQUIREMENTS, _bpo, _dec, _planner, _t1_row, _t2_row, BP_B, _chars
from test_daily_planner_parallel_jobs import _assign, _Blueprints, _char, _missiles

from eve_online_industry_tracker.application.daily_planner import character_assigner as ca
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars


def _timed(product, seconds=3 * 86_400):
    product.overview_row["manufacturing_job"]["time_seconds"] = seconds
    return product


def test_with_a_budget_the_slot_order_uses_the_manufacturing_slots_not_the_labs():
    """A: the science reservation reused `slots` for the research count, so the
    value and the extra-job cap read the labs (1) instead of 5 factory slots."""
    product = _timed(_missiles(room_runs=100))
    budget = WalletBudget(balance=1e12, reserve_pct=0.0)
    money = budget.available
    assigner = CharacterAssigner()
    plan = ChainPlan(decisions=[product], claim_parallel_blueprint=_Blueprints())
    assigner.assign(plan, [], _Chars([_char(1, 5)]), _AdminStub(), {},
                    blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget)
    assert assigner._job_value(product) > 0
    assert assigner._job_value(product) == ca._resource_value(
        product, assigner._job_runs(product), 5, money, 45.0)


class _AdminStub:
    def get(self, section, key, default=None):
        return default


def _ledger_run(decisions, order_key):
    plan = _planner(0).plan_chain(decisions, {
        "bpo_assets_by_type_id": {998: [_bpo(77, 998)]}, "bpc_assets_by_type_id": {},
        "blueprint_data": BP_B, "market_depth_cache": A.PRICES, "variant_rows_by_type_id": {},
    }, order_key=order_key)
    return CharacterAssigner().assign(plan, [], A._Chars(_chars()), A._Admin(), price_map=A.PRICES,
                                      blueprint_data=NO_SKILL_REQUIREMENTS)


def test_the_shared_bpo_goes_to_the_product_the_slot_order_ranks_first():
    """B: by score the T2 copy takes the BPO; the slot order puts the T1 build first."""
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = _dec(_t1_row(20), 12344, "Widget I", 1.0, 1)
    actions = _ledger_run([t2, t1], order_key=lambda d: 5.0 if d.type_id == 12344 else 1.0)
    assert [a.action_type for a in actions if a.type_id == 12344] == ["manufacture"]
    assert "copy" not in [a.action_type for a in actions]


def test_learned_accuracy_weighs_the_profit_in_the_slot_order():
    """C: a product that realized half its predicted ISK/h ranks at half its value."""
    product = _timed(_missiles(room_runs=100))
    learned = dataclasses.replace(product, accuracy_ema=0.5)
    full = ca._resource_value(product, 10, 5, None, 45.0)
    assert full > 0
    assert ca._resource_value(learned, 10, 5, None, 45.0) == full / 2


def test_no_characters_clears_the_previous_computes_diagnostics():
    assigner = CharacterAssigner()
    assigner.slot_order, assigner.science_drops = {"order": "stale"}, [{"stale": True}]

    class _None:
        def list_characters(self):
            raise ValueError("no characters cached")

    assert assigner.assign(ChainPlan(decisions=[]), [], _None(), _AdminStub(), {}, blueprint_data={}) == []
    assert (assigner.slot_order, assigner.science_drops) == (None, [])
