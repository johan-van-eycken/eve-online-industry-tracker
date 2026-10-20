"""H1.2: own sell volume counts only the hub region's orders, and a pilot's
order for a tracked corporation once (it is in both the character and the
corporation order endpoints); the velocity fallback leaves out a pilot's
non-personal (corporation) transactions. Fake ids."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
from eve_online_industry_tracker.infrastructure.models import (
    CharacterMarketOrdersModel,
    CharacterWalletTransactionsModel,
    CorporationMarketOrdersModel,
    CorporationWalletTransactionsModel,
)

CORP, OTHER_CORP, CHAR, TYPE = 98000001, 98000077, 90000001, 34
JITA_REGION, AMARR_REGION = 10000002, 10000043


def _state(app_engine, *, char_corp=CORP):
    return SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        corp_manager=SimpleNamespace(_corporation_ids={CORP}),
        char_manager=SimpleNamespace(_character_list=[SimpleNamespace(character_id=CHAR, corporation_id=char_corp)]),
        esi_service=SimpleNamespace(get_sell_order_book=lambda type_ids, region_id=None: {
            TYPE: [{"price": 5.0, "volume_remain": 100}, {"price": 5.0, "volume_remain": 50}]}),
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
    _collector(monkeypatch, _state(app_engine)).run()
    # 150 on the book minus the corp's one Jita order (50); not minus Amarr (40) or the pilot's copy (50)
    assert _rows(app_engine)[0]["competitor_units"] == 100


def test_a_pilots_corporation_order_counts_when_its_corporation_is_not_tracked(app_engine, monkeypatch):
    s = sessionmaker(bind=app_engine, future=True)()
    s.add(CharacterMarketOrdersModel(character_id=CHAR, order_id=9, type_id=TYPE, is_buy_order=False,
                                     volume_remain=30, region_id=JITA_REGION, is_corporation=True))
    s.commit()
    s.close()
    _collector(monkeypatch, _state(app_engine, char_corp=OTHER_CORP)).run()
    assert _rows(app_engine)[0]["competitor_units"] == 120


def test_an_order_without_a_region_is_left_out_with_a_warning(app_engine, monkeypatch, caplog):
    s = sessionmaker(bind=app_engine, future=True)()
    s.add(CorporationMarketOrdersModel(corporation_id=CORP, order_id=1, type_id=TYPE, is_buy_order=False,
                                       volume_remain=50, region_id=None))
    s.commit()
    s.close()
    with caplog.at_level(logging.WARNING):
        _collector(monkeypatch, _state(app_engine)).run()
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
    c = MarketDepthCollector(state=_state(app_engine))
    assert c._get_effective_velocity(TYPE, {CORP}, {CHAR}) == pytest.approx(45 / 30)
