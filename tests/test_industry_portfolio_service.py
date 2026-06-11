from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.portfolio_service import (  # noqa: E402
    IndustryPortfolioService,
    PortfolioCandidateDirective,
    PortfolioPlanRequest,
)
from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402


def test_portfolio_candidate_payload_derives_candidate_metrics() -> None:
    service = object.__new__(IndustryService)
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
