"""Task 14 minor 1: the Shopping tab's budget agrees with Capital Reserved.

It counts the PENDING purchase lines only (a done or skipped line is no
longer to be paid) and adds the pending jobs' install fees as their own
line, so the wallet is compared with the same total Capital Reserved
shows. Fake numbers.
"""
from __future__ import annotations

from streamlit_ui.components.daily_planner.status_bar import _fmt_isk, compute_capital_reserved
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


# --- fix round 1, M5 ---------------------------------------------------------------

def _bpo_buy(cost, status="pending"):
    return {"action_type": "buy_bpo", "estimated_cost_isk": cost, "status": status}


def test_a_pending_bpo_purchase_is_in_the_budget_like_capital_reserved():
    actions = [_buy(100.0), _bpo_buy(1000.0), _bpo_buy(50.0, "done"), _job(5.0)]
    budget = shopping_budget(actions)
    assert (budget.purchases, budget.bpo_purchases, budget.install_fees) == (100.0, 1000.0, 5.0)
    assert budget.total == 1105.0 == compute_capital_reserved(actions).total
    # A BPO's "Fits Budget?" is checked against the materials and fees only:
    # the BPO rows themselves are what is being fitted.
    assert budget.materials_and_fees == 105.0


def test_the_section_subtotal_and_the_clipboard_count_pending_lines_only():
    from streamlit_ui.components.daily_planner.tab_shopping import (
        _build_clipboard_text,
        _build_shopping_rows,
        section_subtotal,
    )

    actions = [{**_buy(100.0), "type_name": "Tritanium"}, {**_buy(40.0, "done"), "type_name": "Pyerite"}]
    rows = _build_shopping_rows(actions, "current_job")
    assert section_subtotal(rows) == 100.0
    text = _build_clipboard_text(actions, shopping_budget(actions).purchases)
    assert "Tritanium" in text and "Pyerite" not in text
    assert text.splitlines()[-1] == f"Total: {_fmt_isk(100.0)}"


def test_unknown_fees_are_worded_apart_from_unpriced_lines():
    from streamlit_ui.components.daily_planner.tab_shopping import budget_metric

    _label, value, caption = budget_metric(1000.0, 100.0, 1, unknown_fee_jobs=2)
    assert value == "unknown"
    assert "1 unpriced line(s)" in caption and "2 job(s) with an unknown install fee" in caption
    _label, _value, fees_only = budget_metric(1000.0, 100.0, 0, unknown_fee_jobs=2)
    assert "unpriced" not in fees_only and "2 job(s) with an unknown install fee" in fees_only
