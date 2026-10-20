"""Task A item 4: the Streamlit Portfolio page shows the "N rows capped: job
fees unknown" hint the API result carries (portfolio_service), where its
result is empty or filtered. Fake rows only."""
from __future__ import annotations

from eve_online_industry_tracker.application.industry.portfolio_service import IndustryPortfolioService


def _row(type_id, confidence, capped):
    return {"type_id": type_id, "type_name": f"Fake {type_id}", "pricing_confidence": confidence,
            "profit_is_upper_bound": capped,
            "manufacturing_job": {"profit_is_upper_bound": capped,
                                  "job_fee_unknown_reasons": ["copy (no cost index)"] if capped else []}}


def test_the_hint_wording_is_shared_with_the_api():
    assert IndustryPortfolioService.confidence_capped_hint(2) == "2 rows capped: job fees unknown"
    assert IndustryPortfolioService.confidence_capped_hint(1) == "1 row capped: job fees unknown"
    assert IndustryPortfolioService.confidence_capped_hint(0) is None


def test_the_page_counts_only_capped_rows_it_does_not_show():
    """Fix round 1 (M2): a capped row still in the result needs no hint."""
    from streamlit_ui.pages import portfolio_planner as page

    rows = [_row(1, "Medium", True), _row(2, "Medium", True), _row(3, "Medium", False), _row(4, "High", False)]
    assert page.confidence_capped_hint(rows, shown_rows=rows) is None  # nothing filtered
    assert page.confidence_capped_hint(rows, shown_rows=[rows[0], rows[3]]) == "1 row capped: job fees unknown"
    assert page.confidence_capped_hint(rows, shown_rows=[rows[2], rows[3]]) == "2 rows capped: job fees unknown"
    assert page.confidence_capped_hint(rows, shown_rows=[]) == "2 rows capped: job fees unknown"
    assert page.confidence_capped_hint(rows, shown_rows=rows[:2]) is None  # only uncapped rows filtered


def test_shown_rows_are_matched_by_key_not_identity():
    """filter_overview_rows is st.cache_data: it returns copies of the rows."""
    from streamlit_ui.pages import portfolio_planner as page

    rows = [_row(1, "Medium", True), _row(2, "Medium", True)]
    assert page.confidence_capped_hint(rows, shown_rows=[dict(rows[0])]) == "1 row capped: job fees unknown"


def test_no_page_hint_without_capped_rows():
    from streamlit_ui.pages import portfolio_planner as page

    assert page.confidence_capped_hint([_row(3, "Low", False)], shown_rows=[]) is None


def test_the_page_renders_the_hint_on_an_empty_result(monkeypatch):
    from streamlit_ui.pages import portfolio_planner as page

    shown: list[str] = []
    monkeypatch.setattr(page.st, "info", lambda text, **_k: shown.append(text))
    page._render_confidence_capped_hint([_row(1, "Medium", True)], shown_rows=[])
    assert shown == ["1 row capped: job fees unknown (their pricing confidence is held at Medium)"]


def test_the_page_renders_nothing_without_a_hint(monkeypatch):
    from streamlit_ui.pages import portfolio_planner as page

    shown: list[str] = []
    monkeypatch.setattr(page.st, "info", lambda text, **_k: shown.append(text))
    rows = [_row(1, "Medium", True)]
    page._render_confidence_capped_hint(rows, shown_rows=rows)
    assert shown == []
