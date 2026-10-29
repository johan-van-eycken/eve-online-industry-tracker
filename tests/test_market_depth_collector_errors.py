"""The market depth collector catches only data, ESI and DB
errors per type (a WARNING naming the type id, the previous row kept and
marked stale) and lets a programming error propagate; and a type whose order
book fetch failed is not an empty book: nothing is written, the previous row
is marked stale with the ESI reason and a WARNING is logged. Fake values."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
import requests
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
from eve_online_industry_tracker.application.market_pricing.order_values import refresh_market_history

TYPE_A, TYPE_B = 34, 35
REASON = "ESI sell order book unavailable: fake 503"


class _Esi:
    def __init__(self, book=None, failed=None, raise_exc=None):
        self.book = {TYPE_A: [{"price": 5.0, "volume_remain": 100}], TYPE_B: [{"price": 7.0, "volume_remain": 50}]} \
            if book is None else book
        self.failed = failed or {}
        self.raise_exc = raise_exc

    def get_sell_order_book(self, type_ids, region_id=None):
        if self.raise_exc is not None:
            raise self.raise_exc
        return {t: v for t, v in self.book.items() if t not in self.failed}

    def order_book_unavailable_reason(self, type_id, *, side="sell", region_id=None):
        assert side == "sell" and region_id == 10000002
        return self.failed.get(int(type_id))


def _state(app_engine, esi):
    return SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        corp_manager=SimpleNamespace(_corporation_ids=set()),
        char_manager=SimpleNamespace(_character_list=[]),
        esi_service=esi,
    )


class _History:
    calls: list = []
    result = None
    raise_exc = None

    def __init__(self, **k):
        pass

    def fetch_and_store_history(self, **k):
        if _History.raise_exc is not None:
            raise _History.raise_exc
        return _History.result


@pytest.fixture(autouse=True)
def _fake_history(monkeypatch):
    from eve_online_industry_tracker.application.market_analysis import market_history_service

    _History.result = None
    _History.raise_exc = None
    monkeypatch.setattr(market_history_service, "MarketHistoryService", _History)


def _collector(monkeypatch, state):
    c = MarketDepthCollector(state=state)
    monkeypatch.setattr(c, "_get_type_ids_from_active_plan", lambda: [TYPE_A, TYPE_B])
    monkeypatch.setattr(c, "_get_effective_velocity", lambda type_id, *owners: None)
    return c


def _rows(app_engine):
    with app_engine.connect() as conn:
        return {r.type_id: dict(r._mapping) for r in conn.execute(text("SELECT * FROM market_depth_cache"))}


def _db_error(*a, **k):
    raise OperationalError("SELECT fake", {}, Exception("fake db down"))


# --- The per-type except ------------------------------------------------------------------------

def test_a_programming_error_in_one_type_propagates(app_engine, monkeypatch):
    c = _collector(monkeypatch, _state(app_engine, _Esi()))

    def _bug(type_id, region_id):
        raise TypeError("a bug")
    monkeypatch.setattr(c, "_get_avg_30d_stats", _bug)
    with pytest.raises(TypeError, match="a bug"):
        c.run()


def test_a_db_error_in_one_type_warns_with_its_id_keeps_its_row_stale_and_goes_on(app_engine, monkeypatch, caplog):
    state = _state(app_engine, _Esi())
    c = _collector(monkeypatch, state)
    c.run()   # a first, good snapshot of both types
    first = _rows(app_engine)

    real_vwap = c._compute_vwap_5d

    def _vwap(type_id, region_id, days=None):
        if type_id == TYPE_A:
            _db_error()
        return real_vwap(type_id, region_id, days=days)
    monkeypatch.setattr(c, "_compute_vwap_5d", _vwap)
    state.esi_service = _Esi(book={TYPE_A: [{"price": 9.0, "volume_remain": 1}],
                                   TYPE_B: [{"price": 8.0, "volume_remain": 2}]})
    with caplog.at_level(logging.WARNING):
        c.run()
    rows = _rows(app_engine)
    assert rows[TYPE_A]["competitor_units"] == first[TYPE_A]["competitor_units"]   # kept
    assert "fake db down" in rows[TYPE_A]["stale_reason"]
    assert rows[TYPE_B]["competitor_units"] == 2 and rows[TYPE_B]["stale_reason"] is None   # the next type went on
    assert f"type_id={TYPE_A}" in caplog.text


def test_an_order_with_an_unreadable_price_is_skipped_not_a_type_error(app_engine, monkeypatch, caplog):
    esi = _Esi(book={TYPE_A: [{"price": None, "volume_remain": 100}, {"price": 6.0, "volume_remain": 10}],
                     TYPE_B: [{"price": 7.0, "volume_remain": "lots"}]})
    c = _collector(monkeypatch, _state(app_engine, esi))
    with caplog.at_level(logging.WARNING):
        c.run()
    rows = _rows(app_engine)
    assert rows[TYPE_A]["spot_sell_price"] == 6.0
    assert rows[TYPE_A]["competitor_units"] == 10
    assert rows[TYPE_B]["competitor_units"] == 0 and rows[TYPE_B]["spot_sell_price"] is None
    assert f"type_id={TYPE_A}" in caplog.text and "unreadable" in caplog.text


# --- item 3c: a failed order book is not an empty book ------------------------------------------

def test_a_failed_order_book_keeps_the_previous_row_marked_stale(app_engine, monkeypatch, caplog):
    state = _state(app_engine, _Esi())
    c = _collector(monkeypatch, state)
    c.run()
    first = _rows(app_engine)

    state.esi_service = _Esi(failed={TYPE_A: REASON})
    with caplog.at_level(logging.WARNING):
        c.run()
    rows = _rows(app_engine)
    assert rows[TYPE_A]["competitor_units"] == first[TYPE_A]["competitor_units"] == 100   # not 0
    assert rows[TYPE_A]["spot_sell_price"] == 5.0
    assert rows[TYPE_A]["snapshot_at"] == first[TYPE_A]["snapshot_at"]
    assert rows[TYPE_A]["stale_reason"] == REASON
    assert rows[TYPE_B]["stale_reason"] is None
    assert f"type_id={TYPE_A}" in caplog.text and REASON in caplog.text


def test_a_failed_order_book_without_a_previous_row_writes_nothing(app_engine, monkeypatch, caplog):
    c = _collector(monkeypatch, _state(app_engine, _Esi(failed={TYPE_A: REASON})))
    with caplog.at_level(logging.WARNING):
        c.run()
    assert set(_rows(app_engine)) == {TYPE_B}
    assert REASON in caplog.text


def test_a_type_with_a_real_empty_book_is_still_written(app_engine, monkeypatch):
    c = _collector(monkeypatch, _state(app_engine, _Esi(book={TYPE_B: [{"price": 7.0, "volume_remain": 50}]})))
    c.run()
    rows = _rows(app_engine)
    assert rows[TYPE_A]["competitor_units"] == 0 and rows[TYPE_A]["stale_reason"] is None


def test_a_failed_order_book_request_marks_every_type_stale(app_engine, monkeypatch, caplog):
    state = _state(app_engine, _Esi())
    c = _collector(monkeypatch, state)
    c.run()
    state.esi_service = _Esi(raise_exc=requests.ConnectionError("fake ESI down"))
    with caplog.at_level(logging.WARNING):
        c.run()
    rows = _rows(app_engine)
    assert rows[TYPE_A]["competitor_units"] == 100 and rows[TYPE_B]["competitor_units"] == 50
    assert all("fake ESI down" in r["stale_reason"] for r in rows.values())


def test_a_programming_error_in_the_order_book_request_propagates(app_engine, monkeypatch):
    c = _collector(monkeypatch, _state(app_engine, _Esi(raise_exc=TypeError("a bug"))))
    with pytest.raises(TypeError, match="a bug"):
        c.run()


# --- the history step ---------------------------------------------------------------------------

def test_a_failed_history_refresh_is_a_warning_and_the_cycle_goes_on(app_engine, monkeypatch, caplog):
    _History.result = "ESI market history unavailable: fake 502"
    c = _collector(monkeypatch, _state(app_engine, _Esi()))
    with caplog.at_level(logging.WARNING):
        c.run()
    assert set(_rows(app_engine)) == {TYPE_A, TYPE_B}
    assert "fake 502" in caplog.text and f"type_id={TYPE_A}" in caplog.text


def test_an_esi_error_in_the_history_refresh_is_a_warning(app_engine, monkeypatch, caplog):
    _History.raise_exc = requests.ConnectionError("fake ESI down")
    c = _collector(monkeypatch, _state(app_engine, _Esi()))
    with caplog.at_level(logging.WARNING):
        c.run()
    assert set(_rows(app_engine)) == {TYPE_A, TYPE_B}
    assert "fake ESI down" in caplog.text


def test_a_programming_error_in_the_history_refresh_propagates(app_engine, monkeypatch):
    _History.raise_exc = TypeError("a bug")
    c = _collector(monkeypatch, _state(app_engine, _Esi()))
    with pytest.raises(TypeError, match="a bug"):
        c.run()


# --- a malformed ESI value skips one item ------------------------------------------------------
#
# ValueError is not an ESI error, so
# the per-item loops name it explicitly (a TypeError stays a bug and
# propagates): a malformed value skips that one item, with its reason, and the
# others are still processed.

def test_a_malformed_value_in_one_types_depth_skips_that_type_only(app_engine, monkeypatch, caplog):
    from eve_online_industry_tracker.application.market_analysis import market_history_service

    class _History:
        def __init__(self, **k):
            pass

        def fetch_and_store_history(self, *, type_id, region_id):
            if type_id == 34:
                raise ValueError("fake malformed history")
            return None

    monkeypatch.setattr(market_history_service, "MarketHistoryService", _History)
    state = SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        corp_manager=None, char_manager=None,
        esi_service=SimpleNamespace(get_sell_order_book=lambda ids, region_id=None: {
            t: [{"price": 5.0, "volume_remain": 10}] for t in ids}),
    )
    c = MarketDepthCollector(state=state)
    monkeypatch.setattr(c, "_get_type_ids_from_active_plan", lambda: [34, 35])
    monkeypatch.setattr(c, "_get_effective_velocity", lambda *a: None)
    real = c._process_type

    def _process(**kw):
        if kw["type_id"] == 34:
            raise ValueError("fake malformed order")
        return real(**kw)

    monkeypatch.setattr(c, "_process_type", _process)
    with caplog.at_level(logging.WARNING):
        c.run()
    with app_engine.connect() as conn:
        rows = {r[0] for r in conn.execute(text("SELECT type_id FROM market_depth_cache"))}
    assert rows == {35}
    assert "fake malformed history" in caplog.text and "fake malformed order" in caplog.text


def test_a_malformed_history_skips_that_type_only(caplog):
    class _Svc:
        def fetch_and_store_history(self, *, type_id, region_id):
            if type_id == 34:
                raise ValueError("fake malformed payload")
            return None

    with caplog.at_level(logging.WARNING):
        assert set(refresh_market_history(_Svc(), {34, 35}, label="Test")) == {34}
