"""An unknown corp wallet renders as unknown, never as a 0 ISK balance."""
from __future__ import annotations

from streamlit_ui.components.daily_planner.status_bar import _fmt_isk, wallet_snapshot
from streamlit_ui.components.daily_planner.tab_shopping import (
    budget_fit,
    remaining_after_shopping,
    wallet_is_short,
)


def test_an_unknown_wallet_snapshot_stays_unknown():
    assert wallet_snapshot({"corp_wallet_snapshot": None}) is None
    assert wallet_snapshot({}) is None
    assert _fmt_isk(wallet_snapshot({})) == "—"


def test_a_real_zero_wallet_is_still_zero():
    assert wallet_snapshot({"corp_wallet_snapshot": 0.0}) == 0.0


def test_budget_fit_is_unknown_without_a_wallet():
    assert budget_fit(None, 100.0, 50.0) == "Unknown"


def test_budget_fit_compares_the_remaining_wallet_to_the_cumulative_cost():
    assert budget_fit(1000.0, 400.0, 600.0) == "Yes"
    assert budget_fit(1000.0, 400.0, 601.0) == "No"


def test_a_real_zero_wallet_with_a_shopping_list_is_short_by_the_full_total():
    assert wallet_is_short(0.0, 500.0) is True
    assert remaining_after_shopping(0.0, 500.0) == -500.0
    assert _fmt_isk(abs(remaining_after_shopping(0.0, 500.0))) == "500 ISK"


def test_a_zero_wallet_with_nothing_to_buy_is_not_short():
    assert wallet_is_short(0.0, 0.0) is False
    assert remaining_after_shopping(0.0, 0.0) == 0.0


def test_an_unknown_wallet_is_neither_short_nor_has_a_remainder():
    assert wallet_is_short(None, 500.0) is False
    assert remaining_after_shopping(None, 500.0) is None
    assert _fmt_isk(remaining_after_shopping(None, 500.0)) == "—"


def test_a_wallet_covering_the_list_is_not_short():
    assert wallet_is_short(1000.0, 400.0) is False
    assert remaining_after_shopping(1000.0, 400.0) == 600.0
    assert wallet_is_short(1000.0, 1000.01) is True


def test_a_non_finite_wallet_snapshot_is_unknown_not_a_number():
    for raw in (float("nan"), float("inf"), float("-inf"), "nan", "inf"):
        assert wallet_snapshot({"corp_wallet_snapshot": raw}) is None, raw
    assert _fmt_isk(wallet_snapshot({"corp_wallet_snapshot": float("nan")})) == "—"


def test_the_shopping_total_names_its_unpriced_lines():
    """Unpriced lines are bought; the total says it
    excludes them instead of passing as exact."""
    from streamlit_ui.components.daily_planner import tab_shopping

    actions = [
        {"action_type": "buy_materials", "shopping_category": "current_job", "type_name": "A",
         "quantity": 10, "estimated_cost_isk": 50.0},
        {"action_type": "buy_materials", "shopping_category": "current_job", "type_name": "B",
         "quantity": 3, "estimated_cost_isk": None},
    ]
    rows = tab_shopping._build_shopping_rows(actions, "current_job")
    assert tab_shopping.unpriced_total_caption(rows) == (
        "1 line(s) have no price: the total excludes them, so the real cost is higher."
    )
    assert tab_shopping.unpriced_total_caption(rows[:1]) is None


# --- An unpriced line makes the remainder unknown ------------------------

from streamlit_ui.components.daily_planner.tab_shopping import budget_metric  # noqa: E402


def test_with_every_line_priced_the_budget_is_exact():
    assert budget_metric(1000.0, 400.0, 0) == ("Remaining", "600 ISK", None)
    assert budget_metric(100.0, 400.0, 0) == ("Deficit", "300 ISK", None)
    assert budget_metric(None, 400.0, 0) == ("Remaining", "—", None)


def test_an_unpriced_line_makes_a_positive_remainder_unknown():
    label, value, caption = budget_metric(1000.0, 400.0, 2)
    assert (label, value) == ("Remaining", "unknown")
    assert "2 unpriced line(s)" in caption and "600 ISK" in caption
    assert wallet_is_short(1000.0, 400.0, unpriced_lines=2) is None


def test_an_unpriced_line_makes_a_deficit_a_lower_bound():
    label, value, caption = budget_metric(100.0, 400.0, 1)
    assert (label, value) == ("Deficit", "at least 300 ISK")
    assert "1 unpriced line(s)" in caption
    # Already short on the priced lines alone: short for certain.
    assert wallet_is_short(100.0, 400.0, unpriced_lines=1) is True
    assert remaining_after_shopping(100.0, 400.0, unpriced_lines=1) is None
