"""Round H2.9: the character and corporation market-order enrichment share
one helper (application/market_analysis/market_order_enrichment), behaviour
unchanged (checked against a before/after snapshot of both services on the
same fake orders). Plus the ESIService retry cleanup. Fake values only.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import requests

from eve_online_industry_tracker.application.characters import service as characters_service
from eve_online_industry_tracker.application.corporations import service as corporations_service
from eve_online_industry_tracker.application.market_analysis import market_order_enrichment as moe
from eve_online_industry_tracker.infrastructure import esi_service as esi_service_module

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)


def _enrich(order, **overrides):
    kwargs = dict(now=NOW, buy_order_book={}, sell_order_book={34: [{"price": 8.0, "volume_remain": 1}]},
                  compare_with_books=True, history_unavailable={34: "stale"},
                  pricing_svc=SimpleNamespace(suggest_price=lambda **k: {"advised_price": 7.5, "confidence": "high"}),
                  pricing_character_id=90000001, corporation_id=None, owner="Pilot", station="Jita",
                  region_name=None, is_corporation=False, price_label="Market order", pricing_failure_label="order")
    kwargs.update(overrides)
    return moe.enrich_market_order(order, **kwargs)


def test_a_sell_order_is_compared_priced_and_scored():
    row = _enrich({"order_id": 1, "type_id": 34, "is_buy_order": False, "price": 9.0,
                   "issued": "2026-10-18T12:00:00Z", "duration": 2, "volume_remain": 3, "volume_total": 3,
                   "region_id": 10000002})
    assert row["expires_in"] == "1d 0h 0m"
    assert row["advised_price"] == 7.5 and row["advised_price_confidence"] == "high"
    assert row["history_unavailable_reason"] == "stale"
    assert row["region"] == "Region 10000002"
    assert row["price_difference"] is not None
    moe.add_reprice_priority_scores([row])
    assert row["reprice_priority_score"] > 0


def test_without_book_comparison_the_status_is_na_and_no_pricing_without_a_character():
    row = _enrich({"order_id": 2, "type_id": 34, "is_buy_order": False, "price": 9.0, "issued": "bad"},
                  compare_with_books=False, pricing_character_id=None)
    assert row["price_status"] == moe.NA_STATUS and row["price_difference"] == 0.0
    assert "advised_price" not in row and row["expires_in"] == "Expired"


def test_both_services_use_the_shared_helper():
    for module in (characters_service, corporations_service):
        source = inspect.getsource(module)
        assert "enrich_market_order(" in source and "add_reprice_priority_scores(" in source
        assert '["reprice_priority_score"]' not in source   # the score is the helper's


def test_the_public_get_retry_has_no_dead_reraise_construct():
    source = inspect.getsource(esi_service_module.ESIService._public_esi_get)
    assert "exc = None" not in source and "if exc is None" not in source


def test_a_public_get_transport_error_is_retried_then_raised_as_itself(monkeypatch):
    monkeypatch.setattr(esi_service_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(esi_service_module, "get_esi_monitor", None)
    error = requests.ConnectionError("reset")
    calls = []

    def get(*a, **k):
        calls.append(1)
        raise error

    svc = esi_service_module.ESIService.__new__(esi_service_module.ESIService)
    svc._esi_client = SimpleNamespace(esi_base_uri="https://esi.example")
    svc._admin_settings = None
    svc._http_session = SimpleNamespace(get=get)
    with pytest.raises(requests.ConnectionError) as info:
        svc._public_esi_get("/x/", max_retries=2, timeout_seconds=1.0)
    assert info.value is error
    assert len(calls) == 3
