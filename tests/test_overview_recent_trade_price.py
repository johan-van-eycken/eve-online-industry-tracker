"""The product overview prices a product like the Daily Planner: the lower of
the market depth cache's recent trade price and its cheapest sell order without
our own; a type without a cache row at the lower of its order book and its last
trading days (market history); the order book only without either. Fake data."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.infrastructure.models import MarketDepthCacheModel, MarketHistoryModel


class _Query:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args):
        return self

    def order_by(self, *args):
        return self

    def all(self):
        return self._rows


class _Session:
    def __init__(self, depth, history):
        self._rows = {MarketDepthCacheModel: depth, MarketHistoryModel: history}

    def query(self, model):
        return _Query(self._rows[model])

    def close(self):
        pass


def _service(depth, history=()):
    service = object.__new__(IndustryService)
    service._sessions = SimpleNamespace(app_session=lambda: _Session(list(depth), list(history)))
    return service


def test_the_lower_of_recent_trades_and_the_cheapest_order_prices_a_cached_type():
    depth = [SimpleNamespace(type_id=1, vwap_5d=100.0, spot_sell_price=150.0),
             SimpleNamespace(type_id=2, vwap_5d=120.0, spot_sell_price=80.0),     # undercut below the trades
             SimpleNamespace(type_id=3, vwap_5d=None, spot_sell_price=90.0)]
    book = {1: {"unit_price": 190.0}, 2: {"unit_price": 200.0}, 3: {"unit_price": 95.0}}
    out = _service(depth)._priced_at_recent_trades(book, "jita")
    assert (out[1]["unit_price"], out[1]["price_source"]) == (100.0, "market_depth:vwap_5d")
    assert (out[2]["unit_price"], out[2]["price_source"]) == (80.0, "market_depth:spot_sell_price")
    assert out[3]["unit_price"] == 90.0
    assert out[1]["order_book_unit_price"] == 190.0


def test_a_type_without_a_cache_row_is_priced_at_its_recent_trades_not_a_troll_order():
    """Target Painter I, 2026-11-23: one order at ~1M, trading at 61-98K."""
    history = [SimpleNamespace(type_id=4, close=60_000.0, volume=200),
               SimpleNamespace(type_id=4, close=90_000.0, volume=100)]
    out = _service([], history)._priced_at_recent_trades({4: {"unit_price": 1_000_000.0}}, "jita")
    assert out[4]["unit_price"] == 70_000.0
    assert out[4]["price_source"] == "market_history:vwap_5d"


def test_without_a_cache_row_or_recent_trades_the_order_book_stands():
    book = {5: {"unit_price": 50.0, "price_source": "book"}}
    assert _service([])._priced_at_recent_trades(book, "jita") == book
