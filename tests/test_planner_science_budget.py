"""Science gets its share of the wallet (plan 41: 2.69B of 2.73B built, 8 science
slots idle on "wallet"), every invention input gets a hub price, and an
unknown invent cost names its cause. Fake data."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner, _BuildCharger
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars, _bare_service
from planner_skill_fakes import NO_SKILL_REQUIREMENTS
from test_daily_planner_budget_assignment import PRICES, _char, _of, _parent

M = 1_000_000


class _Admin:
    def get(self, section, key, default=None):
        return 5.0 if key == "planner_science_budget_pct" else default


def test_builds_leave_the_science_budget_untouched():
    budget = WalletBudget(balance=100 * M, reserve_pct=0)
    actions = CharacterAssigner().assign(
        ChainPlan(decisions=[_parent(runs=30)]), [], _Chars([_char(1)]), _Admin(), dict(PRICES),
        blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget,
    )
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs == 9                     # 95M for builds at 10M a run; 5M kept for science
    assert budget.spent == 90 * M


def test_every_invention_input_of_a_row_gets_a_hub_price():
    asked = []
    svc = _bare_service()
    svc._market_pricing = SimpleNamespace(get_type_price_map=lambda type_ids, hub, side: (
        asked.extend(type_ids) or {t: {"unit_price": 100.0} for t in type_ids}))
    row = {"type_id": 1, "manufacturing_job": {"expected_invention_materials": {
        "20410": {"type_id": 20410, "quantity": 2.4}, "20171": {"type_id": 20171, "quantity": 2.4}}}}
    prices = svc._get_material_prices([row], {}, {}, "jita")
    assert {20410, 20171} <= set(asked) and prices[20410] == {"spot_sell_price": 100.0}


def test_an_unknown_invent_cost_names_the_input_without_a_price():
    charger = object.__new__(_BuildCharger)
    charger._budget = WalletBudget(balance=100 * M, reserve_pct=0)
    charger.science_dropped, charger.science_drops = {}, []
    decision = ItemDecision(type_id=5, type_name="Thing II", decision="build", decision_reason="r",
                            adjusted_score=1, absolute_profit_per_batch=1, isk_per_hour=1, margin_pct=1,
                            days_of_supply_current=1, effective_velocity=1, meta_group_id=2,
                            pipeline_stage="invention")
    action = AssignedAction(type_id=5, type_name="Thing II", action_type="invent", character_id=1,
                            character_name="P", quantity=None, runs=3, estimated_cost_isk=None,
                            estimated_profit_isk=None, estimated_completion=None, notes="",
                            estimated_job_fee_isk=1000.0,
                            invention_chance_reason="invention cost unknown: invention input 20410 has no price")
    assert charger.charge_science(decision, action) is False
    assert "invention input 20410 has no price" in charger.science_drops[0]["why"]
