"""The planner's market checks of 2026-11-23: a batch sells at the lower of
the recent trades and the cheapest order; a product without a trade in 30
days is not built; one product takes at most its share of the budget. Fake data."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.profitability_scorer import _sell_price

M = 1_000_000


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


def test_the_order_weighs_capital_tied_up_until_sold_when_money_is_short():
    """Plan 40: R-O Trigger (10%, 42 days to sell, 661M) ranked above a
    Prototype Cloaking Device I that sells in a day; with the capital share
    the fast seller comes first, and with plenty of money the slots decide."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import _resource_value
    from test_daily_planner_day_sized_jobs import _row_decision
    slow = _row_decision(type_id=1, runs=10, hours_per_run=1.0, profit=66 * M, room_runs=10,
                         source="owned_blueprint_copy")
    slow.overview_row["manufacturing_job"]["total_cost"] = 661 * M
    slow.expected_days_to_sell = 42.0
    fast = _row_decision(type_id=2, runs=10, hours_per_run=1.0, profit=40 * M, room_runs=10,
                         source="owned_blueprint_copy")
    fast.overview_row["manufacturing_job"]["total_cost"] = 200 * M
    fast.expected_days_to_sell = 1.0
    short = 2.7e9
    assert _resource_value(fast, 10, 22, short, 45) > _resource_value(slow, 10, 22, short, 45)
    assert _resource_value(slow, 10, 22, None, 45) > _resource_value(fast, 10, 22, None, 45)


def test_the_capital_cap_counts_the_whole_batch_not_only_what_is_bought():
    """Squall, plan 40: 1.08B of a 683M cap; its stock materials were not charged."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
    from eve_online_industry_tracker.application.daily_planner.models import ChainPlan
    from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
    from fakes import _Chars
    from planner_skill_fakes import NO_SKILL_REQUIREMENTS
    from test_daily_planner_budget_assignment import MAT, PRICES, _char, _of, _parent

    class _Admin:
        def get(self, section, key, default=None):
            return 25.0 if key == "planner_max_product_capital_pct" else default

    budget = WalletBudget(balance=400 * M, reserve_pct=0)          # cap 100M a product
    decision = _parent(runs=30)                                      # 10M a run of build cost
    actions = CharacterAssigner().assign(
        ChainPlan(decisions=[decision]), [], _Chars([_char(1)]), _Admin(), dict(PRICES),
        blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget, corp_material_stock={MAT: 30},
    )
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs == 10          # all materials from stock (nothing charged), the cap still holds


def test_the_batch_profit_is_net_of_fees_and_the_whole_cost():
    """R-O Trigger Neurolink Conduit, plan 40: 3.5M a unit as price minus
    materials, ~2.2M net of sales tax, broker fee and job fees."""
    from eve_online_industry_tracker.application.daily_planner.profitability_scorer import _compute_absolute_profit
    from test_daily_planner_scoring import _input_row
    row = _input_row()
    row.raw.update({"gross_sale_value": 100.0 * row.quantity, "net_proceeds": 95.0 * row.quantity})
    row.raw.setdefault("manufacturing_job", {})["total_cost"] = 80.0 * row.quantity
    profit, reason = _compute_absolute_profit(row, {"vwap_5d": 100.0, "spot_sell_price": 100.0})
    assert reason is None and profit == (95.0 - 80.0) * row.quantity
