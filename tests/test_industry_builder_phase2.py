from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.infrastructure.persistence import market_orderbook_view_cache_repo as repo


def test_legacy_liquidity_rows_with_levels_are_treated_as_missing() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE market_orderbook_view_cache ("
                "hub TEXT, region_id INTEGER, station_id INTEGER, side TEXT, type_id INTEGER, "
                "at_hub INTEGER, depth INTEGER, levels TEXT, total_volume INTEGER, order_count INTEGER, "
                "fetched_at REAL, version INTEGER)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO market_orderbook_view_cache "
                "(hub, region_id, station_id, side, type_id, at_hub, depth, levels, total_volume, order_count, fetched_at, version) "
                "VALUES (:hub, :region_id, :station_id, :side, :type_id, :at_hub, :depth, :levels, :total_volume, :order_count, :fetched_at, :version)"
            ),
            [
                {
                    "hub": "jita",
                    "region_id": 10000002,
                    "station_id": 60003760,
                    "side": "sell",
                    "type_id": 34,
                    "at_hub": 1,
                    "depth": 5,
                    "levels": json.dumps([[10.0, 50], [10.5, 25]]),
                    "total_volume": 0,
                    "order_count": 0,
                    "fetched_at": 9999999999.0,
                    "version": 1,
                },
                {
                    "hub": "jita",
                    "region_id": 10000002,
                    "station_id": 60003760,
                    "side": "sell",
                    "type_id": 35,
                    "at_hub": 1,
                    "depth": 5,
                    "levels": json.dumps([]),
                    "total_volume": 0,
                    "order_count": 0,
                    "fetched_at": 9999999999.0,
                    "version": 1,
                },
                {
                    "hub": "jita",
                    "region_id": 10000002,
                    "station_id": 60003760,
                    "side": "sell",
                    "type_id": 36,
                    "at_hub": 1,
                    "depth": 5,
                    "levels": json.dumps([[11.0, 12]]),
                    "total_volume": 12,
                    "order_count": 1,
                    "fetched_at": 9999999999.0,
                    "version": 1,
                },
            ],
        )

    with Session(engine) as session:
        result = repo.get_liquidity_summaries(
            session,
            hub="jita",
            region_id=10000002,
            station_id=60003760,
            side="sell",
            at_hub=True,
            type_ids=[34, 35, 36],
            ttl_seconds=3600,
        )

    assert 34 not in result
    assert result[35] == {"total_volume": 0, "order_count": 0}
    assert result[36] == {"total_volume": 12, "order_count": 1}


def test_pricing_confidence_scores_high_for_fresh_liquid_active_product() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(cfg_manager=None, esi_service=None)  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]

    rows = [
        {
            "type_id": 100,
            "type_name": "Example Product",
            "market_unit_price": 1500000.0,
            "market_price_sample_size": 5,
            "market_price_fetched_at": 4102444800.0,
            "hub_sell_liquidity": 1000,
            "hub_sell_order_count": 8,
            "region_daily_volume": 120,
            "region_daily_volume_7d_avg": 95.0,
            "manufacturing_job": {},
        }
    ]

    result = service._enrich_product_rows_with_pricing_confidence(rows, product_price_side="sell")

    assert result[0]["pricing_confidence"] == "High"
    assert result[0]["market_price_age_minutes"] is not None
    assert any("fresh" in reason.lower() for reason in result[0]["pricing_confidence_reasons"])


def test_pricing_confidence_scores_low_without_price() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(cfg_manager=None, esi_service=None)  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]

    rows = [
        {
            "type_id": 101,
            "type_name": "Unpriced Product",
            "market_unit_price": None,
            "market_price_sample_size": 0,
            "market_price_fetched_at": None,
            "hub_sell_liquidity": 0,
            "hub_sell_order_count": 0,
            "region_daily_volume": 0,
            "region_daily_volume_7d_avg": 0.0,
            "manufacturing_job": {},
        }
    ]

    result = service._enrich_product_rows_with_pricing_confidence(rows, product_price_side="sell")

    assert result[0]["pricing_confidence"] == "Low"
    assert any("no usable market price" in reason.lower() for reason in result[0]["pricing_confidence_reasons"])