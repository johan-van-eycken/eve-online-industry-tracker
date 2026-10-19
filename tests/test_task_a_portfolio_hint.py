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


def test_the_page_counts_rows_capped_at_medium_for_unknown_job_fees():
    from streamlit_ui.pages import portfolio_planner as page

    rows = [_row(1, "Medium", True), _row(2, "Medium", True), _row(3, "Medium", False), _row(4, "High", False)]
    assert page.confidence_capped_hint(rows, shown=4) is None  # nothing filtered, not empty
    assert page.confidence_capped_hint(rows, shown=2) == "2 rows capped: job fees unknown"
    assert page.confidence_capped_hint(rows, shown=0) == "2 rows capped: job fees unknown"


def test_no_page_hint_without_capped_rows():
    from streamlit_ui.pages import portfolio_planner as page

    assert page.confidence_capped_hint([_row(3, "Low", False)], shown=0) is None


def test_the_page_renders_the_hint_on_an_empty_result(monkeypatch):
    from streamlit_ui.pages import portfolio_planner as page

    shown: list[str] = []
    monkeypatch.setattr(page.st, "info", lambda text, **_k: shown.append(text))
    page._render_confidence_capped_hint([_row(1, "Medium", True)], shown=0)
    assert shown == ["1 row capped: job fees unknown (their pricing confidence is held at Medium)"]


def test_the_page_renders_nothing_without_a_hint(monkeypatch):
    from streamlit_ui.pages import portfolio_planner as page

    shown: list[str] = []
    monkeypatch.setattr(page.st, "info", lambda text, **_k: shown.append(text))
    page._render_confidence_capped_hint([_row(1, "Medium", True)], shown=1)
    assert shown == []
