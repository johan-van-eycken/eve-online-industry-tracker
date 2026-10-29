"""F1 items 1-2: the market depth collector counts as own volume every sell
order of every tracked corporation AND character (the "primary corp" lookup
is gone), and when the own orders cannot be read it keeps the previous
market_depth_cache row, marks it stale (stale_reason) and logs a WARNING
instead of writing competitor units that include our own orders. Fake
values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
from eve_online_industry_tracker.infrastructure.models import CharacterMarketOrdersModel, CorporationMarketOrdersModel
from eve_online_industry_tracker.infrastructure.models import (
    CharacterWalletTransactionsModel,
    CorporationWalletTransactionsModel,
)

CORP_A, CORP_B = 98000001, 98000002
CHAR_A, CHAR_B = 90000001, 90000002
TYPE = 34


def _book(*units, price=5.0):
    return SimpleNamespace(get_sell_order_book=lambda type_ids, region_id=None: {
        TYPE: [{"price": price, "volume_remain": u} for u in units]})


def _state(app_engine, *, book_units=(100,), corps=(CORP_A, CORP_B), chars=(CHAR_A, CHAR_B)):
    return SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        corp_manager=SimpleNamespace(_corporation_ids=set(corps)),
        char_manager=SimpleNamespace(_character_list=[SimpleNamespace(character_id=c) for c in chars]),
        esi_service=_book(*book_units),
    )


def _collector(monkeypatch, state):
    from eve_online_industry_tracker.application.market_analysis import market_history_service

    class _History:
        def __init__(self, **k):
            pass

        def fetch_and_store_history(self, **k):
            return None

    monkeypatch.setattr(market_history_service, "MarketHistoryService", _History)
    c = MarketDepthCollector(state=state)
    monkeypatch.setattr(c, "_get_type_ids_from_active_plan", lambda: [TYPE])
    monkeypatch.setattr(c, "_get_effective_velocity", lambda type_id, *owners: None)
    return c


def _seed_orders(app_engine):
    s = sessionmaker(bind=app_engine, future=True)()
    oid = 1
    for corp, units in ((CORP_A, 10), (CORP_B, 20)):
        s.add(CorporationMarketOrdersModel(corporation_id=corp, order_id=oid, type_id=TYPE, is_buy_order=False,
                                           volume_remain=units, region_id=10000002))
        oid += 1
    s.add(CorporationMarketOrdersModel(corporation_id=CORP_A, order_id=oid, type_id=TYPE, is_buy_order=True,
                                       volume_remain=1000))   # a buy order is not own sell volume
    oid += 1
    for char, units in ((CHAR_A, 5), (CHAR_B, 7)):
        s.add(CharacterMarketOrdersModel(character_id=char, order_id=oid, type_id=TYPE, is_buy_order=False,
                                         volume_remain=units, region_id=10000002))
        oid += 1
    s.add(CharacterMarketOrdersModel(character_id=90000099, order_id=oid, type_id=TYPE, is_buy_order=False,
                                     volume_remain=500))    # an untracked character
    s.commit()
    s.close()


def _cache_rows(app_engine):
    with app_engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text("SELECT * FROM market_depth_cache"))]


def _boom(*a, **k):
    raise OperationalError("SELECT fake", {}, Exception("fake db down"))


def test_own_volume_sums_every_tracked_corporation_and_character(app_engine, monkeypatch):
    _seed_orders(app_engine)
    c = _collector(monkeypatch, _state(app_engine, book_units=(100, 50)))
    c.run()
    rows = _cache_rows(app_engine)
    assert len(rows) == 1
    # 150 on the book minus 10 + 20 (corps) + 5 + 7 (characters) of our own
    assert rows[0]["competitor_units"] == 150 - 42
    assert rows[0]["stale_reason"] is None


def test_the_tracked_owner_ids_are_every_corporation_and_character(app_engine):
    c = MarketDepthCollector(state=_state(app_engine))
    assert c._tracked_owner_ids() == ({CORP_A, CORP_B}, {CHAR_A, CHAR_B})


def test_unreadable_own_orders_keep_the_previous_row_and_mark_it_stale(app_engine, monkeypatch, caplog):
    state = _state(app_engine, book_units=(100,))
    c = _collector(monkeypatch, state)
    c.run()   # a first, good snapshot
    before = _cache_rows(app_engine)[0]
    assert before["competitor_units"] == 100

    monkeypatch.setattr(c, "_query_own_sell_volume", _boom)
    state.esi_service = _book(999, price=6.0)
    with caplog.at_level(logging.WARNING):
        c.run()
    after = _cache_rows(app_engine)
    assert len(after) == 1
    assert after[0]["competitor_units"] == 100           # the previous row is kept
    assert after[0]["spot_sell_price"] == before["spot_sell_price"]
    assert after[0]["snapshot_at"] == before["snapshot_at"]
    assert "fake db down" in after[0]["stale_reason"]
    assert "type_id=34" in caplog.text and "stale" in caplog.text


def test_a_fresh_write_clears_the_stale_reason(app_engine, monkeypatch):
    c = _collector(monkeypatch, _state(app_engine))
    c.run()
    with app_engine.begin() as conn:
        conn.execute(text("UPDATE market_depth_cache SET stale_reason = 'old'"))
    c.run()
    assert _cache_rows(app_engine)[0]["stale_reason"] is None


def test_unreadable_own_orders_without_a_previous_row_write_nothing(app_engine, monkeypatch, caplog):
    c = _collector(monkeypatch, _state(app_engine))
    monkeypatch.setattr(c, "_query_own_sell_volume", _boom)
    with caplog.at_level(logging.WARNING):
        c.run()
    assert _cache_rows(app_engine) == []
    assert "fake db down" in caplog.text


def test_no_tracked_owners_means_no_own_volume(app_engine, monkeypatch):
    _seed_orders(app_engine)
    c = _collector(monkeypatch, _state(app_engine, corps=(), chars=()))
    c.run()
    assert _cache_rows(app_engine)[0]["competitor_units"] == 100


def test_the_velocity_fallback_reads_the_sales_history_of_every_tracked_corporation(app_engine, monkeypatch):
    from eve_online_industry_tracker.application.industry import sales_history_service
    seen = []

    class _Svc:
        def __init__(self, **k):
            pass

        def get_sold_history(self, *, character_id, corporation_id, type_id, days):
            seen.append(corporation_id)
            return [{"quantity": 30}]

    monkeypatch.setattr(sales_history_service, "SalesHistoryService", _Svc)
    c = MarketDepthCollector(state=_state(app_engine))
    # no active plan row (the build_plan tables are empty): 60 units over 30 days from both corporations
    assert c._get_effective_velocity(TYPE, {CORP_A, CORP_B}) == pytest.approx(2.0)
    assert sorted(seen) == [CORP_A, CORP_B]


# --- own sell volume: hub region only, a pilot's corp order counted once -----------------------
#
# H1.2: own sell volume counts only the hub region's orders, and a pilot's
# order for a tracked corporation once (it is in both the character and the
# corporation order endpoints); the velocity fallback leaves out a pilot's
# non-personal (corporation) transactions. Fake ids.

CORP, OTHER_CORP, CHAR, TYPE = 98000001, 98000077, 90000001, 34
JITA_REGION, AMARR_REGION = 10000002, 10000043


def _own_sell_state(app_engine, *, char_corp=CORP):
    return SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        corp_manager=SimpleNamespace(_corporation_ids={CORP}),
        char_manager=SimpleNamespace(_character_list=[SimpleNamespace(character_id=CHAR, corporation_id=char_corp)]),
        esi_service=SimpleNamespace(get_sell_order_book=lambda type_ids, region_id=None: {
            TYPE: [{"price": 5.0, "volume_remain": 100}, {"price": 5.0, "volume_remain": 50}]}),
    )


def _seed(app_engine, *, char_region=JITA_REGION):
    s = sessionmaker(bind=app_engine, future=True)()
    s.add(CorporationMarketOrdersModel(corporation_id=CORP, order_id=1, type_id=TYPE, is_buy_order=False,
                                       volume_remain=50, region_id=JITA_REGION, location_id=60003760))
    s.add(CorporationMarketOrdersModel(corporation_id=CORP, order_id=2, type_id=TYPE, is_buy_order=False,
                                       volume_remain=40, region_id=AMARR_REGION, location_id=60008494))
    # The pilot's view of corp order 1: the same order, from the character endpoint
    s.add(CharacterMarketOrdersModel(character_id=CHAR, order_id=1, type_id=TYPE, is_buy_order=False,
                                     volume_remain=50, region_id=char_region, location_id=60003760,
                                     is_corporation=True))
    s.commit()
    s.close()


def _rows(app_engine):
    with app_engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text("SELECT * FROM market_depth_cache"))]


def test_off_region_and_duplicate_corporation_orders_are_not_own_volume(app_engine, monkeypatch):
    _seed(app_engine)
    _collector(monkeypatch, _own_sell_state(app_engine)).run()
    # 150 on the book minus the corp's one Jita order (50); not minus Amarr (40) or the pilot's copy (50)
    assert _rows(app_engine)[0]["competitor_units"] == 100


def test_a_pilots_corporation_order_counts_when_its_corporation_is_not_tracked(app_engine, monkeypatch):
    s = sessionmaker(bind=app_engine, future=True)()
    s.add(CharacterMarketOrdersModel(character_id=CHAR, order_id=9, type_id=TYPE, is_buy_order=False,
                                     volume_remain=30, region_id=JITA_REGION, is_corporation=True))
    s.commit()
    s.close()
    _collector(monkeypatch, _own_sell_state(app_engine, char_corp=OTHER_CORP)).run()
    assert _rows(app_engine)[0]["competitor_units"] == 120


def test_an_order_without_a_region_is_left_out_with_a_warning(app_engine, monkeypatch, caplog):
    s = sessionmaker(bind=app_engine, future=True)()
    s.add(CorporationMarketOrdersModel(corporation_id=CORP, order_id=1, type_id=TYPE, is_buy_order=False,
                                       volume_remain=50, region_id=None))
    s.commit()
    s.close()
    with caplog.at_level(logging.WARNING):
        _collector(monkeypatch, _own_sell_state(app_engine)).run()
    assert _rows(app_engine)[0]["competitor_units"] == 150
    assert "no region" in caplog.text


def test_the_velocity_fallback_leaves_out_non_personal_character_transactions(app_engine):
    recent = (datetime.utcnow() - timedelta(days=1)).isoformat()
    s = sessionmaker(bind=app_engine, future=True)()
    s.add(CorporationWalletTransactionsModel(corporation_id=CORP, transaction_id=1, date=recent, is_buy=False,
                                             quantity=30, type_id=TYPE, unit_price=5.0, total_price=150.0))
    # The pilot's copy of the corp sale (is_personal False) and a personal sale
    s.add(CharacterWalletTransactionsModel(character_id=CHAR, transaction_id=1, date=recent, is_buy=False,
                                           quantity=30, type_id=TYPE, unit_price=5.0, is_personal=False))
    s.add(CharacterWalletTransactionsModel(character_id=CHAR, transaction_id=2, date=recent, is_buy=False,
                                           quantity=15, type_id=TYPE, unit_price=5.0, is_personal=True))
    s.commit()
    s.close()
    c = MarketDepthCollector(state=_own_sell_state(app_engine))
    assert c._get_effective_velocity(TYPE, {CORP}, {CHAR}) == pytest.approx(45 / 30)
