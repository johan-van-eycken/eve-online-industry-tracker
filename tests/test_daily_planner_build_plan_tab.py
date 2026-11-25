"""The build-plan tab says why a BPO analysis is blank."""
from __future__ import annotations

from streamlit_ui.components.daily_planner.tab_build_plan import bpo_analysis_caption


def test_a_skipped_analysis_shows_its_reason():
    item = {"bpo_analysis_skip_reason": "sell velocity unknown (no corp sales in 30 days)",
            "bpo_market_price": None}
    assert bpo_analysis_caption(item) == (
        "BPO analysis skipped: sell velocity unknown (no corp sales in 30 days)"
    )


def test_a_completed_analysis_shows_its_result():
    item = {"bpo_analysis_skip_reason": None, "bpo_market_price": 20_000_000.0,
            "break_even_days": 12.4, "projected_annual_savings": 600_000_000.0}
    assert bpo_analysis_caption(item) == "BPO 20.00M ISK: 12d break-even, saves 600.00M ISK/yr"


def test_no_analysis_and_no_reason_shows_nothing():
    assert bpo_analysis_caption({"bpo_market_price": None}) is None


def test_decision_counts_put_build_first():
    from streamlit_ui.components.daily_planner.tab_build_plan import decision_counts
    items = [{"decision": "skip"}, {"decision": "build"}, {"decision": "skip"}, {"decision": "odd"}]
    assert list(decision_counts(items).items()) == [("build", 1), ("skip", 2), ("odd", 1)]


def test_search_matches_name_or_reason():
    from streamlit_ui.components.daily_planner.tab_build_plan import matches_search
    item = {"type_name": "Large Armor Pump II", "decision_reason": "Watch: declining market"}
    assert matches_search(item, "armor") and matches_search(item, "DECLINING") and matches_search(item, " ")
    assert not matches_search(item, "shield")
