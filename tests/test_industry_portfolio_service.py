from __future__ import annotations

from types import MethodType

from eve_online_industry_tracker.application.industry.portfolio_service import (  # noqa: E402
    IndustryPortfolioService,
    PortfolioCandidateDirective,
    PortfolioPlanRequest,
)
from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402


def test_portfolio_candidate_payload_derives_candidate_metrics() -> None:
    service = object.__new__(IndustryService)
    service._resolve_industry_profile_context = MethodType(  # type: ignore[attr-defined]
        lambda self, *, character_id, industry_profile_id: None,
        service,
    )
    service.industry_manufacturing_product_overview_payload = lambda **kwargs: {  # type: ignore[attr-defined]
        "rows": [
            {
                "overview_row_id": "product:1",
                "type_id": 5002,
                "type_name": "T2 Module",
                "quantity": 2,
                "profit_amount": 400.0,
                "profit_margin_fraction": 0.2,
                "isk_per_hour": 100.0,
                "pricing_confidence": "Medium",
                "region_daily_volume": 24,
                "region_daily_volume_7d_avg": 30.0,
                "manufacturing_job": {
                    "time_seconds": 7200,
                    "manufacturing_time_seconds": 3600,
                    "preparation_time_seconds": 3600,
                    "total_cost": 1200.0,
                    "material_cost": 1000.0,
                    "total_job_cost": 200.0,
                    "procurement_materials": {
                        "34": {"type_id": 34, "quantity": 5, "price_source": "owned_asset_item_value"},
                        "35": {"type_id": 35, "quantity": 5, "price_source": "market_sell"},
                    },
                    "blueprint_source_kind": "owned_blueprint_original",
                },
            }
        ],
        "pricing_batch": {"market_hub": "jita"},
    }

    payload = service.industry_manufacturing_portfolio_candidates_payload(  # type: ignore[attr-defined]
        character_id=1,
        planning_horizon_hours=24.0,
    )

    assert payload["summary"]["candidate_count"] == 1
    candidate = payload["candidates"][0]
    assert candidate["type_id"] == 5002
    assert candidate["cash_outlay_per_batch"] == 1200.0
    assert candidate["slot_hours_per_batch"] == 2.0
    assert candidate["max_batches_total"] == 15
    assert candidate["owned_input_coverage_fraction"] == 0.5
    assert candidate["confidence_penalty_factor"] == 0.85
    assert candidate["is_portfolio_candidate"] is True


def test_optimize_manufacturing_portfolio_allocates_within_constraints() -> None:
    planner = IndustryPortfolioService()
    plan = planner.optimize_manufacturing_portfolio(
        candidates=[
            {
                "type_id": 1,
                "type_name": "High Profit",
                "is_portfolio_candidate": True,
                "profit_amount": 500.0,
                "effective_profit_per_batch": 500.0,
                "effective_isk_per_hour": 250.0,
                "profit_margin_fraction": 0.25,
                "isk_per_hour": 250.0,
                "pricing_confidence": "High",
                "cash_outlay_per_batch": 1000.0,
                "slot_hours_per_batch": 2.0,
                "max_batches_total": 3,
                "quantity_per_batch": 1,
                "region_daily_volume": 10,
            },
            {
                "type_id": 2,
                "type_name": "Lower Profit",
                "is_portfolio_candidate": True,
                "profit_amount": 200.0,
                "effective_profit_per_batch": 200.0,
                "effective_isk_per_hour": 200.0,
                "profit_margin_fraction": 0.2,
                "isk_per_hour": 200.0,
                "pricing_confidence": "High",
                "cash_outlay_per_batch": 500.0,
                "slot_hours_per_batch": 1.0,
                "max_batches_total": 5,
                "quantity_per_batch": 1,
                "region_daily_volume": 10,
            },
        ],
        plan_request=PortfolioPlanRequest(
            capital_limit_isk=2500.0,
            manufacturing_slots_available=1,
            planning_horizon_hours=4.0,
            objective="max_profit",
            minimum_pricing_confidence="low",
        ),
    )

    assert plan["selected_count"] == 1
    assert plan["total_allocated_batches"] == 2
    assert plan["capital_committed"] == 2000.0
    assert plan["slot_hours_committed"] == 4.0
    assert plan["selected_items"][0]["type_id"] == 1
    assert plan["selected_items"][0]["batches"] == 2
    assert plan["candidate_scope_count"] == 2


def test_optimize_manufacturing_portfolio_honors_operator_directives() -> None:
    planner = IndustryPortfolioService()
    plan = planner.optimize_manufacturing_portfolio(
        candidates=[
            {
                "overview_row_id": "candidate:forced",
                "type_id": 1,
                "type_name": "Forced Item",
                "is_portfolio_candidate": True,
                "profit_amount": 100.0,
                "effective_profit_per_batch": 100.0,
                "effective_isk_per_hour": 50.0,
                "profit_margin_fraction": 0.1,
                "isk_per_hour": 50.0,
                "pricing_confidence": "Low",
                "cash_outlay_per_batch": 100.0,
                "slot_hours_per_batch": 1.0,
                "max_batches_total": 5,
                "quantity_per_batch": 1,
                "region_daily_volume": 10,
            },
            {
                "overview_row_id": "candidate:locked",
                "type_id": 2,
                "type_name": "Locked Item",
                "is_portfolio_candidate": True,
                "profit_amount": 500.0,
                "effective_profit_per_batch": 500.0,
                "effective_isk_per_hour": 250.0,
                "profit_margin_fraction": 0.25,
                "isk_per_hour": 250.0,
                "pricing_confidence": "High",
                "cash_outlay_per_batch": 1000.0,
                "slot_hours_per_batch": 2.0,
                "max_batches_total": 5,
                "quantity_per_batch": 1,
                "region_daily_volume": 10,
            },
            {
                "overview_row_id": "candidate:excluded",
                "type_id": 3,
                "type_name": "Excluded Item",
                "is_portfolio_candidate": True,
                "profit_amount": 900.0,
                "effective_profit_per_batch": 900.0,
                "effective_isk_per_hour": 300.0,
                "profit_margin_fraction": 0.3,
                "isk_per_hour": 300.0,
                "pricing_confidence": "High",
                "cash_outlay_per_batch": 100.0,
                "slot_hours_per_batch": 1.0,
                "max_batches_total": 5,
                "quantity_per_batch": 1,
                "region_daily_volume": 10,
            },
        ],
        plan_request=PortfolioPlanRequest(
            capital_limit_isk=300.0,
            manufacturing_slots_available=2,
            planning_horizon_hours=2.0,
            objective="max_profit",
            minimum_pricing_confidence="high",
            candidate_directives=[
                PortfolioCandidateDirective(
                    overview_row_id="candidate:forced",
                    force_include=True,
                    max_batches_override=2,
                    target_batches_override=1,
                ),
                PortfolioCandidateDirective(
                    overview_row_id="candidate:locked",
                    lock_required=True,
                    target_batches_override=1,
                ),
                PortfolioCandidateDirective(
                    overview_row_id="candidate:excluded",
                    exclude=True,
                ),
            ],
        ),
    )

    assert plan["selected_count"] == 1
    assert plan["selected_items"][0]["overview_row_id"] == "candidate:forced"
    assert plan["selected_items"][0]["batches"] == 2
    assert plan["selected_items"][0]["force_include"] is True
    assert plan["operator_decisions"]["forced_includes"][0]["overview_row_id"] == "candidate:forced"
    assert plan["operator_decisions"]["exclusions"][0]["overview_row_id"] == "candidate:excluded"
    assert plan["operator_decisions"]["unfulfilled_locked_items"][0]["overview_row_id"] == "candidate:locked"
    assert "Locked item could not fit" in str(plan["operator_decisions"]["unfulfilled_locked_items"][0]["reason"])


# --- the Portfolio page's capped-rows hint -----------------------------------------------------
#
# Task A item 4: the Streamlit Portfolio page shows the "N rows capped: job
# fees unknown" hint the API result carries (portfolio_service), where its
# result is empty or filtered. Fake rows only.

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
