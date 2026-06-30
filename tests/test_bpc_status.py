from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402


def _make_row(**kwargs) -> dict:
    """Build a minimal row dict for _build_portfolio_candidate."""
    defaults = {
        "overview_row_id": "product:0:0:bpc:none:bpo:none",
        "type_id": 1234,
        "type_name": "Test Item",
        "category_name": "Module",
        "meta_group_name": "Tech I",
        "quantity": 1,
        "profit_amount": 100_000.0,
        "profit_margin_fraction": 0.10,
        "isk_per_hour": 50_000.0,
        "net_proceeds": 1_100_000.0,
        "region_daily_volume": 100,
        "region_daily_volume_7d_avg": 100.0,
        "hub_buy_liquidity": 50,
        "hub_sell_liquidity": 50,
        "hub_sell_order_count": 5,
        "pricing_confidence": "High",
        "pricing_confidence_reasons": [],
        "market_price_age_minutes": 10.0,
        "manufacturing_group": "T1",
        "manufacturing_job": {
            "time_seconds": 3600,
            "manufacturing_time_seconds": 3600,
            "preparation_time_seconds": 0,
            "total_cost": 1_000_000.0,
            "material_cost": 900_000.0,
            "total_job_cost": 100_000.0,
            "procurement_materials": {},
            "blueprint_source_kind": "owned_blueprint_copy",
        },
        "bpc_count": 0,
        "has_bpo": False,
    }
    defaults.update(kwargs)
    return defaults


def test_bpc_status_bpo() -> None:
    """has_bpo=True, bpc_count=0 → bpc_status == 'needed'"""
    row = _make_row(has_bpo=True, bpc_count=0)
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["bpc_status"] == "needed"
    assert candidate["has_bpo"] is True
    assert candidate["bpc_count"] == 0


def test_bpc_status_stocked_t1() -> None:
    """T1 item, has_bpo=False, bpc_count=4 → bpc_status == 'stocked', bpc_threshold == 3"""
    row = _make_row(has_bpo=False, bpc_count=4, meta_group_name="Tech I")
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["bpc_status"] == "stocked"
    assert candidate["bpc_threshold"] == 3
    assert candidate["bpc_count"] == 4


def test_bpc_status_stocked_t2() -> None:
    """T2 item, bpc_count=6 → bpc_status == 'stocked', bpc_threshold == 5"""
    row = _make_row(has_bpo=False, bpc_count=6, meta_group_name="Tech II")
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["bpc_status"] == "stocked"
    assert candidate["bpc_threshold"] == 5
    assert candidate["bpc_count"] == 6


def test_bpc_status_low_t2() -> None:
    """T2 item, bpc_count=3 → bpc_status == 'low' (below threshold of 5)"""
    row = _make_row(has_bpo=False, bpc_count=3, meta_group_name="Tech II")
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["bpc_status"] == "low"
    assert candidate["bpc_threshold"] == 5
    assert candidate["bpc_count"] == 3


def test_bpc_status_invent() -> None:
    """has_bpo=False, bpc_count=0 → bpc_status == 'invent'"""
    row = _make_row(has_bpo=False, bpc_count=0)
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["bpc_status"] == "invent"
    assert candidate["bpc_count"] == 0
    assert candidate["has_bpo"] is False
