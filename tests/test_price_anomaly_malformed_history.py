"""F1 item 9: a malformed market history row (None, or a None/non-numeric
price field) on the price-anomaly fetch_and_store_history path is skipped
explicitly, with one WARNING counting the skipped rows; the good rows are
stored and the enrichment does not crash. Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel

TYPE, REGION = 34, 10000002
GOOD = {"date": "2026-10-01", "average": 5.0, "highest": 6.0, "lowest": 4.0, "volume": 100, "order_count": 9}


def _history(app_engine, rows):
    esi = SimpleNamespace(get_market_history=lambda type_ids, region_id=None: {TYPE: rows})
    factory = sessionmaker(bind=app_engine, future=True)
    return MarketHistoryService(state=SimpleNamespace(esi_service=esi),
                                sessions=SimpleNamespace(app_session=factory)), factory


def _stored(factory):
    s = factory()
    try:
        return [(r.date, r.close, r.volume) for r in s.query(MarketHistoryModel).all()]
    finally:
        s.close()


def test_malformed_rows_are_skipped_with_one_counted_warning(app_engine, caplog):
    rows = [None, {**GOOD, "date": "2026-10-02", "average": None}, {**GOOD, "date": "2026-10-03", "volume": "lots"},
            "junk", GOOD]
    svc, factory = _history(app_engine, rows)
    with caplog.at_level(logging.WARNING):
        svc.fetch_and_store_history(type_id=TYPE, region_id=REGION)
    assert _stored(factory) == [("2026-10-01", 5.0, 100)]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "4 of 5" in warnings[0].getMessage() and "type_id=34" in warnings[0].getMessage()


def test_a_none_row_list_stores_nothing_with_a_warning(app_engine, caplog):
    svc, factory = _history(app_engine, None)
    with caplog.at_level(logging.WARNING):
        svc.fetch_and_store_history(type_id=TYPE, region_id=REGION)
    assert _stored(factory) == []
    assert "type_id=34" in caplog.text


def test_clean_rows_log_nothing(app_engine, caplog):
    svc, factory = _history(app_engine, [GOOD])
    with caplog.at_level(logging.WARNING):
        svc.fetch_and_store_history(type_id=TYPE, region_id=REGION)
    assert _stored(factory) == [("2026-10-01", 5.0, 100)]
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_the_price_anomaly_enrichment_survives_a_malformed_history(tmp_path, monkeypatch):
    from sqlalchemy import create_engine

    from eve_online_industry_tracker.application.industry import market_enrichment
    from eve_online_industry_tracker.application.industry.service import IndustryService
    from eve_online_industry_tracker.infrastructure.models import BaseApp

    # a file database: the history fetch runs on a worker thread
    app_engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}", future=True)
    BaseApp.metadata.create_all(app_engine)

    class _Pricing:
        def __init__(self, **kw):
            pass

        def _market_hub_context(self, hub):
            return {"region_id": REGION}

    history, _factory = _history(app_engine, [None, {**GOOD, "highest": None}])
    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    monkeypatch.setattr(market_enrichment, "MarketHistoryService", lambda **kw: history)
    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(esi_service=None, admin_settings=None)
    svc._sessions = None
    rows = svc._enrich_product_rows_with_price_anomaly([{"type_id": TYPE}], market_hub="jita")
    assert rows[0]["type_id"] == TYPE
    assert _stored(_factory) == []          # both rows were malformed
    app_engine.dispose()
