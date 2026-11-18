"""The product overview prices a product like the Daily Planner: the market
depth cache's 5-day average trade price, else its cheapest sell order without
our own; the order book only without a cache row. Fake data."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.industry.service import IndustryService


class _Query:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args):
        return self

    def all(self):
        return self._rows


class _Session:
    def __init__(self, rows):
        self._rows = rows

    def query(self, model):
        return _Query(self._rows)

    def close(self):
        pass


def _service(rows):
    service = object.__new__(IndustryService)
    service._sessions = SimpleNamespace(app_session=lambda: _Session(rows))
    return service


def test_the_recent_trade_price_replaces_the_order_book_price():
    rows = [SimpleNamespace(type_id=1, vwap_5d=100.0, spot_sell_price=150.0),
            SimpleNamespace(type_id=2, vwap_5d=None, spot_sell_price=80.0)]
    book = {1: {"unit_price": 190.0, "price_source": "book"}, 2: {"unit_price": 90.0},
            3: {"unit_price": 50.0, "price_source": "book"}}
    out = _service(rows)._priced_at_recent_trades(book, "jita")
    assert out[1]["unit_price"] == 100.0 and out[1]["price_source"] == "market_depth:vwap_5d"
    assert out[1]["order_book_unit_price"] == 190.0
    assert out[2]["unit_price"] == 80.0 and out[2]["price_source"] == "market_depth:spot_sell_price"
    assert out[3] == book[3]   # no cache row: the order book stands
