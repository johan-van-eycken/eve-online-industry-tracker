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
        "quantity": 10,
        "profit_amount": 10_000_000.0,
        "profit_margin_fraction": 0.10,
        "isk_per_hour": 5_000_000.0,
        "net_proceeds": 110_000_000.0,
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
            "time_seconds": 86400,
            "manufacturing_time_seconds": 86400,
            "preparation_time_seconds": 0,
            "total_cost": 90_000_000.0,
            "material_cost": 80_000_000.0,
            "total_job_cost": 10_000_000.0,
            "procurement_materials": {},
            "blueprint_source_kind": "owned_blueprint_copy",
        },
        "bpc_count": 5,
        "has_bpo": False,
    }
    defaults.update(kwargs)
    return defaults


def test_isk_per_cycle_day_normal() -> None:
    """
    Verify the basic isk_per_cycle_day calculation:
    - time_seconds = 86400 → build_days = 1.0
    - quantity = 1, region_daily_volume_7d_avg = 10, horizon = 72h (3 days)
    - absorption = floor(10 * 3) = 30, max_batches = 30
    - sell_days = (30 * 1) / 10 = 3.0
    - cycle_days = 1 + 3 = 4.0
    - isk_per_cycle_day = (10_000_000 * 30) / 4 = 75_000_000
    """
    row = _make_row(
        quantity=1,
        region_daily_volume=10,
        region_daily_volume_7d_avg=10.0,
        manufacturing_job={
            "time_seconds": 86400,
            "manufacturing_time_seconds": 86400,
            "preparation_time_seconds": 0,
            "total_cost": 90_000_000.0,
            "material_cost": 80_000_000.0,
            "total_job_cost": 10_000_000.0,
            "procurement_materials": {},
            "blueprint_source_kind": "owned_blueprint_copy",
        },
    )
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=72.0)
    assert candidate["max_batches_total"] == 30
    assert candidate["isk_per_cycle_day"] is not None
    assert abs(candidate["isk_per_cycle_day"] - 75_000_000.0) < 1.0


def test_isk_per_cycle_day_zero_volume() -> None:
    """region_daily_volume_7d_avg=0 and region_daily_volume=0 → isk_per_cycle_day = None"""
    row = _make_row(
        region_daily_volume=0,
        region_daily_volume_7d_avg=0.0,
    )
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["isk_per_cycle_day"] is None


def test_isk_per_cycle_day_zero_batches() -> None:
    """max_batches_total=0 (zero volume) → isk_per_cycle_day = None"""
    row = _make_row(
        region_daily_volume=0,
        region_daily_volume_7d_avg=0.0,
    )
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert candidate["max_batches_total"] == 0
    assert candidate["isk_per_cycle_day"] is None


def test_isk_per_cycle_day_in_candidate() -> None:
    """Call _build_portfolio_candidate with a suitable row; verify 'isk_per_cycle_day' key is present."""
    row = _make_row(
        quantity=5,
        region_daily_volume=50,
        region_daily_volume_7d_avg=50.0,
        manufacturing_job={
            "time_seconds": 3600,
            "manufacturing_time_seconds": 3600,
            "preparation_time_seconds": 0,
            "total_cost": 1_000_000.0,
            "material_cost": 900_000.0,
            "total_job_cost": 100_000.0,
            "procurement_materials": {},
            "blueprint_source_kind": "owned_blueprint_copy",
        },
    )
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=24.0)
    assert "isk_per_cycle_day" in candidate


def test_capital_cycle_build_days_use_elapsed_time_for_invented_t2() -> None:
    """An invented T2 row's time_seconds is amortized slot time (1 day here);
    the capital cycle must use the whole-job elapsed time (3 days) instead.
    sell_days = 3.0 as in the test above, so cycle_days = 3 + 3 = 6.0 and
    isk_per_cycle_day = (10_000_000 * 30) / 6 = 50_000_000."""
    row = _make_row(
        quantity=1,
        region_daily_volume=10,
        region_daily_volume_7d_avg=10.0,
        manufacturing_job={
            "time_seconds": 86400,
            "elapsed_time_seconds": 3 * 86400,
            "manufacturing_time_seconds": 43200,
            "preparation_time_seconds": 43200,
            "total_cost": 90_000_000.0,
            "material_cost": 80_000_000.0,
            "total_job_cost": 10_000_000.0,
            "procurement_materials": {},
            "blueprint_source_kind": "unowned_blueprint_copy",
        },
    )
    candidate = IndustryService._build_portfolio_candidate(row, planning_horizon_hours=72.0)
    assert candidate["elapsed_time_seconds"] == 3 * 86400
    assert abs(candidate["isk_per_cycle_day"] - 50_000_000.0) < 1.0
