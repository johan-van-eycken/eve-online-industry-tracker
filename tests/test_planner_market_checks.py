"""The planner's market checks of 2026-11-23: a batch sells at the lower of
the recent trades and the cheapest order; a product without a trade in 30
days is not built; one product takes at most its share of the budget. Fake data."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.profitability_scorer import _sell_price


def test_a_batch_sells_at_the_lower_of_recent_trades_and_the_cheapest_order():
    """Mobile Large Warp Disruptor I: trades at 16.8M, undercut to 13.9M, cost 14.0M."""
    assert _sell_price(SimpleNamespace(vwap_5d=16.8, spot_sell_price=13.9)) == 13.9
    assert _sell_price(SimpleNamespace(vwap_5d=10.0, spot_sell_price=12.0)) == 10.0
    assert _sell_price(SimpleNamespace(vwap_5d=None, spot_sell_price=12.0)) == 12.0
    assert _sell_price(None) is None


def test_a_product_without_a_recent_trade_is_unscoreable():
    from eve_online_industry_tracker.application.daily_planner.profitability_scorer import ProfitabilityScorer
    from test_daily_planner_scoring import _input_row, _pipeline_state
    row = _input_row()
    scored = ProfitabilityScorer().score(
        pipeline=_pipeline_state(row.type_id), row=row, weights=None,
        market_depth={"vwap_5d": None, "spot_sell_price": 350.0}, margin_correlation=None,
    )
    assert "no trades at the hub in the last 30 days" in (scored.unscoreable_reason or "")


def test_one_product_takes_at_most_its_capital_share():
    """Large Trimark Armor Pump I, 2026-11-23: 8 jobs took 2.18B of a 2.73B budget."""
    from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
    from test_daily_planner_parallel_jobs import _Blueprints, _char, _jobs, _missiles, M
    from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
    from eve_online_industry_tracker.application.daily_planner.models import ChainPlan
    from planner_skill_fakes import NO_SKILL_REQUIREMENTS
    from fakes import _Chars

    class _Admin:
        def get(self, section, key, default=None):
            return 25.0 if key == "planner_max_product_capital_pct" else default

    blueprints = _Blueprints(*({"item_id": i, "owner_character_id": None} for i in range(2, 9)))
    budget = WalletBudget(balance=100 * M, reserve_pct=0)          # 10M a batch; cap 25M a product
    plan = ChainPlan(decisions=[_missiles(room_runs=100)], claim_parallel_blueprint=blueprints)
    actions = CharacterAssigner().assign(plan, [], _Chars([_char(1, 9)]), _Admin(), {34: {"vwap_5d": 1.0 * M}},
                                         blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget)
    jobs = _jobs(actions)
    assert [j.runs for j in jobs] == [10, 10, 5]
    assert budget.spent == 25 * M
