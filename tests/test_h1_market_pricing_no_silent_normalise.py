"""H1.5: an unknown market hub or order side is refused (a 400 ServiceError
naming it), not silently read as Jita / sell; an invalid orderbook_smoothing
config value is refused, a missing one is the config schema's default; an
out-of-range min_target_margin_pct is clamped with a WARNING naming it.
None or an empty value still means the default hub / side."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService


@pytest.mark.parametrize("hub, expected", [(None, "jita"), ("", "jita"), (" Amarr ", "amarr"), ("jita", "jita")])
def test_a_known_or_omitted_hub(hub, expected):
    assert MarketPricingService.normalize_market_hub(hub) == expected


def test_an_unknown_hub_is_refused():
    with pytest.raises(ServiceError) as exc:
        MarketPricingService.normalize_market_hub("Perimeter")
    assert exc.value.status_code == 400
    assert "'perimeter'" in exc.value.message and "jita" in exc.value.message


@pytest.mark.parametrize("side, expected", [(None, "sell"), ("", "sell"), ("BUY", "buy"), ("sell", "sell")])
def test_a_known_or_omitted_side(side, expected):
    assert MarketPricingService.normalize_order_side(side) == expected


def test_an_unknown_side_is_refused():
    with pytest.raises(ServiceError) as exc:
        MarketPricingService.normalize_order_side("bid")
    assert exc.value.status_code == 400
    assert "'bid'" in exc.value.message


def _pricing(market_pricing_cfg):
    cfg = {"defaults": {"market_pricing": market_pricing_cfg}}
    return MarketPricingService(state=SimpleNamespace(admin_settings=None, esi_service=None,
                                                      cfg_manager=SimpleNamespace(all=lambda: cfg)))


def test_an_invalid_orderbook_smoothing_is_refused():
    with pytest.raises(ValueError) as exc:
        _pricing({"orderbook_smoothing": "mode_best_n"}).orderbook_smoothing()
    assert "orderbook_smoothing" in str(exc.value) and "mode_best_n" in str(exc.value)


def test_a_missing_orderbook_smoothing_is_the_schema_default():
    expected = CONFIG_SCHEMA["defaults"]["market_pricing"]["orderbook_smoothing"]
    assert _pricing({}).orderbook_smoothing() == expected


def test_a_valid_orderbook_smoothing_is_used():
    assert _pricing({"orderbook_smoothing": "Mean_Best_N"}).orderbook_smoothing() == "mean_best_n"


def _suggestion(pct):
    from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
        PricingSuggestionService,
    )

    svc = PricingSuggestionService.__new__(PricingSuggestionService)
    cfg = {"defaults": {"market_advisory": {"min_target_margin_pct": pct}}}
    svc._state = SimpleNamespace(cfg_manager=SimpleNamespace(all=lambda: cfg))
    return svc


@pytest.mark.parametrize("pct, expected", [(80, 0.5), (-5, 0.0)])
def test_an_out_of_range_min_target_margin_is_clamped_with_a_warning(caplog, pct, expected):
    with caplog.at_level(logging.WARNING):
        assert _suggestion(pct)._get_min_target_margin() == expected
    assert any("min_target_margin_pct" in r.getMessage() and str(pct) in r.getMessage() for r in caplog.records)


def test_an_in_range_min_target_margin_logs_nothing(caplog):
    with caplog.at_level(logging.WARNING):
        assert _suggestion(12)._get_min_target_margin() == pytest.approx(0.12)
    assert caplog.records == []
