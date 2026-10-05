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
