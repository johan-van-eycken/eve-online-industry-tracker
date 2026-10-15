"""Task 14 minor 1: the Shopping tab's budget agrees with Capital Reserved.

It counts the PENDING purchase lines only (a done or skipped line is no
longer to be paid) and adds the pending jobs' install fees as their own
line, so the wallet is compared with the same total Capital Reserved
shows. Fake numbers.
"""
from __future__ import annotations

from streamlit_ui.components.daily_planner.status_bar import compute_capital_reserved
from streamlit_ui.components.daily_planner.tab_shopping import shopping_budget


def _buy(cost, status="pending"):
    return {"action_type": "buy_materials", "shopping_category": "current_job", "quantity": 10,
            "estimated_cost_isk": cost, "status": status}


def _job(fee, status="pending", action_type="manufacture"):
    return {"action_type": action_type, "estimated_job_fee_isk": fee, "status": status}


def test_only_pending_purchases_count_and_the_install_fees_are_added():
    actions = [_buy(100.0), _buy(40.0, "done"), _buy(30.0, "skipped"), _job(5.0), _job(7.0, "done")]
    budget = shopping_budget(actions)
    assert (budget.purchases, budget.install_fees, budget.total) == (100.0, 5.0, 105.0)
    assert budget.unknown_lines == 0
    assert budget.total == compute_capital_reserved(actions).total


def test_an_unknown_fee_or_price_makes_the_total_a_lower_bound():
    budget = shopping_budget([_buy(None), _buy(10.0), _job(None, action_type="invent")])
    assert (budget.purchases, budget.install_fees, budget.total) == (10.0, 0.0, 10.0)
    assert (budget.unpriced_lines, budget.unknown_fee_jobs, budget.unknown_lines) == (1, 1, 2)
