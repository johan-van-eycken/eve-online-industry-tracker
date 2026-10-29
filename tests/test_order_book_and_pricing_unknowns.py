"""F2b: unreadable order-book rows, order prices and history rows. A row that
cannot be read is skipped with one counted WARNING per call; an order price
that cannot be read is unknown (None) with `price_unknown_reason`, never 0
ISK; one failed history fetch does not stop the others. None never reaches
float(): it is checked explicitly. Fake values only."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_pricing import MarketPricingService
from eve_online_industry_tracker.application.market_pricing.order_values import (
    book_comparison,
    readable_number,
)
from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.application.errors import ServiceError

JITA = 60003760


def _warnings(caplog, text):
    return [r for r in caplog.records if r.levelno == logging.WARNING and text in r.getMessage()]


# --- readable_number ---------------------------------------------------------------------------

@pytest.mark.parametrize("value, expected", [
    (4.5, 4.5), (3, 3.0), ("12.5", 12.5), (" 7 ", 7.0),
    (None, None), ("", None), ("abc", None), (True, None), (float("nan"), None), (float("inf"), None),
    ([1], None), ({"price": 1}, None),
])
def test_readable_number(value, expected):
    assert readable_number(value) == expected


# --- order book rows (MarketPricingService) -----------------------------------------------------

def _pricing():
    return MarketPricingService(state=SimpleNamespace(admin_settings=None, cfg_manager=None, esi_service=None))


_BOOK = {34: [
    {"location_id": JITA, "price": 5.0, "volume_remain": 100},
    {"location_id": JITA, "price": None, "volume_remain": 100},         # unreadable price
    {"location_id": JITA, "price": "abc", "volume_remain": 100},        # unreadable price
    {"location_id": JITA, "price": 4.0, "volume_remain": None},         # unreadable volume
    {"location_id": None, "price": 3.0, "volume_remain": 10},           # unreadable location
    {"location_id": 60008494, "price": 1.0, "volume_remain": 10},       # another station: not ours
    "not a row",
]}


def test_unreadable_order_book_rows_are_skipped_with_one_counted_warning(caplog):
    with caplog.at_level(logging.WARNING):
        views = _pricing()._build_cached_views(order_books=_BOOK, station_id=JITA, side="sell")
    assert views == {34: [(5.0, 100)]}
    warnings = _warnings(caplog, "order book")
    assert len(warnings) == 1
    assert "5 unreadable row(s)" in warnings[0].getMessage()


def test_unreadable_rows_do_not_crash_the_sort_or_take_a_depth_slot(caplog):
    rows = [{"location_id": JITA, "price": None, "volume_remain": 1}] * 10 + [
        {"location_id": JITA, "price": 6.0, "volume_remain": 1},
        {"location_id": JITA, "price": 5.5, "volume_remain": 2},
    ]
    views = _pricing()._build_cached_views(order_books={34: rows}, station_id=JITA, side="sell")
    assert views == {34: [(5.5, 2), (6.0, 1)]}


def test_unreadable_liquidity_rows_are_skipped_with_one_counted_warning(caplog):
    with caplog.at_level(logging.WARNING):
        out = _pricing()._build_liquidity_summaries(order_books=_BOOK, station_id=JITA)
    # the volume of the rows at Jita with a readable volume, whatever their price
    assert out == {34: {"total_volume": 300, "order_count": 3}}
    warnings = _warnings(caplog, "liquidity")
    assert len(warnings) == 1
    assert "3 unreadable order book row(s)" in warnings[0].getMessage()


def test_unreadable_levels_are_skipped_with_one_counted_warning(caplog):
    with caplog.at_level(logging.WARNING):
        summary = _pricing()._summarize_levels([[5.0, 10], [None, 5], ["x", 1], [4.0, None], "bad"])
    assert summary["sample_size"] == 1 and summary["unit_price"] == 5.0
    warnings = _warnings(caplog, "price level")
    assert len(warnings) == 1
    assert "4 unreadable" in warnings[0].getMessage()


def test_a_clean_book_logs_nothing(caplog):
    with caplog.at_level(logging.WARNING):
        pricing = _pricing()
        pricing._build_cached_views(order_books={34: _BOOK[34][:1]}, station_id=JITA, side="sell")
        pricing._build_liquidity_summaries(order_books={34: _BOOK[34][:1]}, station_id=JITA)
        pricing._summarize_levels([[5.0, 10]])
    assert caplog.records == []


def test_unreadable_history_volumes_are_skipped_in_the_daily_volume(caplog):
    history = {34: [
        {"date": "2026-10-16", "volume": 100, "order_count": 10},
        {"date": "2026-10-17", "volume": None, "order_count": 10},
        {"date": "2026-10-18", "volume": 300, "order_count": 30},
        {"date": "2026-10-19", "volume": "n/a", "order_count": None},
    ]}
    MarketPricingService._region_volume_cache.clear()
    state = SimpleNamespace(admin_settings=None, cfg_manager=None,
                            esi_service=SimpleNamespace(get_market_history=lambda ids, region_id: history))
    with caplog.at_level(logging.WARNING):
        out = MarketPricingService(state=state).get_region_daily_volume_map(type_ids=[34])
    MarketPricingService._region_volume_cache.clear()
    assert out[34]["daily_volume"] == 300                     # the latest readable day, not 0
    assert out[34]["daily_volume_date"] == "2026-10-18"
    assert out[34]["daily_volume_7d_avg"] == 200.0
    assert out[34]["daily_volume_7d_sample_size"] == 2
    assert len(_warnings(caplog, "2 unreadable history row(s)")) == 1


# --- the book comparison of an order ----------------------------------------------------------------

def test_the_book_comparison_skips_unreadable_book_prices():
    book = [{"price": None}, {"price": 5.0}, {"price": "abc"}, {"price": 6.0}]
    assert book_comparison(order_price=5.5, book=book, is_buy_order=False) == ("🔴Undercut", 0.5)
    assert book_comparison(order_price=6.5, book=book, is_buy_order=True) == ("🟢Best price", 0.5)


def test_a_book_without_readable_prices_or_an_unknown_order_price_is_na():
    assert book_comparison(order_price=5.0, book=[{"price": None}], is_buy_order=False) == ("⚪N/A", 0.0)
    assert book_comparison(order_price=None, book=[{"price": 5.0}], is_buy_order=False) == ("⚪N/A", None)
    assert book_comparison(order_price=5.0, book=[], is_buy_order=True) == ("⚪N/A", 0.0)


# --- enriched market orders (characters and corporations) ------------------------------------------

def _orders(price):
    return [{"character_id": 1, "corporation_id": 2, "market_orders": [
        {"order_id": 7, "type_id": 35, "is_buy_order": False, "price": price, "volume_remain": 10,
         "volume_total": 10, "issued": "2026-10-01T00:00:00Z", "duration": 90},
        {"order_id": 8, "type_id": 36, "is_buy_order": False, "price": 9.0, "volume_remain": 10,
         "volume_total": 10, "issued": "2026-10-01T00:00:00Z", "duration": 90},
    ]}]


class _Esi:
    def __init__(self, sell_book=None):
        self._sell_book = sell_book or {}

    def get_type_buyprices(self, type_ids):
        return {}

    def get_type_sellprices(self, type_ids):
        return self._sell_book

    def get_location_info(self, location_id):
        return {}


def _enrich(monkeypatch, kind, orders, *, esi=None, history=None, suggest=None):
    from eve_online_industry_tracker.application.market_analysis import market_history_service as mh
    from eve_online_industry_tracker.application.market_analysis import pricing_suggestion_service as ps

    suggested = []

    def _suggest(self, **k):
        suggested.append(k)
        return {"advised_price": 8.5}

    monkeypatch.setattr(mh.MarketHistoryService, "fetch_and_store_history", history or (lambda self, **k: None))
    monkeypatch.setattr(ps.PricingSuggestionService, "suggest_price", suggest or _suggest)
    esi = esi or _Esi()
    if kind == "character":
        from eve_online_industry_tracker.application.characters import service as module

        monkeypatch.setattr(module.MarketHistoryService, "fetch_and_store_history",
                            history or (lambda self, **k: None))
        monkeypatch.setattr(module.PricingSuggestionService, "suggest_price", suggest or _suggest)
        state = SimpleNamespace(esi_service=esi, char_manager=SimpleNamespace(get_market_orders=lambda refresh: orders),
                                admin_settings=None, db_app=None, db_sde=None, db_oauth=None)
        return module.CharactersService(state=state).get_market_orders_enriched(), suggested
    from eve_online_industry_tracker.application.corporations import service as module

    state = SimpleNamespace(
        esi_service=esi, corp_manager=SimpleNamespace(get_market_orders=lambda corporation_id, refresh: orders),
        char_manager=SimpleNamespace(get_corp_director=lambda: SimpleNamespace(character_id=1),
                                     get_main_character=lambda: None),
        admin_settings=None, db_app=None, db_sde=None, db_oauth=None)
    return module.CorporationsService(state=state).get_market_orders_enriched(corporation_id=2), suggested


KINDS = ["character", "corporation"]


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("price", [None, "abc", float("nan")])
def test_an_unreadable_order_price_is_unknown_with_a_reason_not_zero(monkeypatch, caplog, kind, price):
    with caplog.at_level(logging.WARNING):
        rows, suggested = _enrich(monkeypatch, kind, _orders(price), esi=_Esi({35: [{"price": 5.0}]}))
    row = next(r for r in rows if r["order_id"] == 7)
    assert row["price"] is None
    assert row["total_price"] is None
    assert row["price_difference"] is None
    assert row["price_status"] == "⚪N/A"
    assert "order price unreadable" in row["price_unknown_reason"]
    assert "advised_price" not in row                       # no suggestion from a made-up 0 ISK
    assert row["reprice_priority_score"] == 0.0
    assert [k["type_id"] for k in suggested] == [36]
    assert _warnings(caplog, "no readable price")
    other = next(r for r in rows if r["order_id"] == 8)
    assert other["price_unknown_reason"] is None and other["advised_price"] == 8.5


@pytest.mark.parametrize("kind", KINDS)
def test_unreadable_book_prices_are_skipped_in_the_comparison(monkeypatch, kind):
    rows, _ = _enrich(monkeypatch, kind, _orders(5.5),
                      esi=_Esi({35: [{"price": None}, {"price": 5.0}], 36: [{"price": None}]}))
    row = next(r for r in rows if r["order_id"] == 7)
    assert (row["price_status"], row["price_difference"]) == ("🔴Undercut", 0.5)
    assert next(r for r in rows if r["order_id"] == 8)["price_status"] == "⚪N/A"


@pytest.mark.parametrize("kind", KINDS)
def test_one_failed_history_fetch_does_not_stop_the_others(monkeypatch, caplog, kind):
    fetched = []

    def history(self, *, type_id, region_id):
        fetched.append(type_id)
        if type_id == 35:
            raise OperationalError("INSERT", {}, Exception("database is locked"))

    with caplog.at_level(logging.WARNING):
        _enrich(monkeypatch, kind, _orders(5.5), history=history)
    assert sorted(fetched) == [35, 36]
    warnings = _warnings(caplog, "market history")
    assert len(warnings) == 1 and "35" in warnings[0].getMessage()


@pytest.mark.parametrize("kind", KINDS)
def test_a_bug_in_the_history_fetch_propagates(monkeypatch, kind):
    def history(self, *, type_id, region_id):
        raise NameError("name 'oops' is not defined")

    with pytest.raises(NameError):
        _enrich(monkeypatch, kind, _orders(5.5), history=history)


@pytest.mark.parametrize("kind", KINDS)
def test_a_failed_history_refresh_is_named_on_the_sell_orders_of_that_type(monkeypatch, kind):
    reason = "ESI market history unavailable: fake 502"

    def history(self, *, type_id, region_id):
        return reason if type_id == 35 else None

    orders = _orders(5.5)
    orders[0]["market_orders"].append(
        {"order_id": 9, "type_id": 35, "is_buy_order": True, "price": 4.0, "volume_remain": 10,
         "volume_total": 10, "issued": "2026-10-01T00:00:00Z", "duration": 90})
    rows, _ = _enrich(monkeypatch, kind, orders, history=history)
    by_id = {r["order_id"]: r for r in rows}
    assert by_id[7]["history_unavailable_reason"] == reason
    assert by_id[7]["advised_price"] == 8.5          # still advised, from the stored history
    assert by_id[8]["history_unavailable_reason"] is None
    assert by_id[9]["history_unavailable_reason"] is None   # a buy order uses no history


@pytest.mark.parametrize("kind", KINDS)
def test_a_history_refresh_that_raised_is_named_on_the_order(monkeypatch, kind):
    def history(self, *, type_id, region_id):
        if type_id == 35:
            raise OperationalError("INSERT", {}, Exception("database is locked"))

    rows, _ = _enrich(monkeypatch, kind, _orders(5.5), history=history)
    row = next(r for r in rows if r["order_id"] == 7)
    assert "database is locked" in row["history_unavailable_reason"]


# --- market history rows ---------------------------------------------------------------------------

def test_a_history_row_with_a_none_field_is_skipped_with_a_counted_warning(app_session, caplog):
    from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
    from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel

    rows = [
        {"date": "2026-10-17", "average": 5.0, "highest": 6.0, "lowest": 4.0, "volume": 100, "order_count": 10},
        {"date": "2026-10-18", "average": 5.0, "highest": None, "lowest": 4.0, "volume": 100, "order_count": 10},
        {"date": "2026-10-19", "average": "abc", "highest": 6.0, "lowest": 4.0, "volume": 100, "order_count": 10},
    ]
    state = SimpleNamespace(esi_service=SimpleNamespace(get_market_history=lambda ids, region_id: {34: rows}))
    svc = MarketHistoryService(state=state, sessions=SimpleNamespace(app_session=lambda: app_session))
    with caplog.at_level(logging.WARNING):
        svc.fetch_and_store_history(type_id=34)
    stored = app_session.query(MarketHistoryModel).all()
    assert [r.date for r in stored] == ["2026-10-17"]
    warnings = _warnings(caplog, "2 unreadable history row(s)")
    assert len(warnings) == 1


# --- pricing suggestion: the data failures are WARNINGs ------------------------------------------------

def test_pricing_suggestion_data_failures_are_warnings(monkeypatch, caplog):
    from eve_online_industry_tracker.application.market_analysis import pricing_suggestion_service as ps

    svc = ps.PricingSuggestionService.__new__(ps.PricingSuggestionService)
    svc._market_pricing = SimpleNamespace(get_type_price_map=lambda **k: (_ for _ in ()).throw(
        OperationalError("SELECT", {}, Exception("database is locked"))))
    svc._sessions = SimpleNamespace(app_session=lambda: (_ for _ in ()).throw(
        OperationalError("SELECT", {}, Exception("database is locked"))))
    with caplog.at_level(logging.WARNING):
        assert svc._get_hub_price(type_id=34) is None
        assert svc._get_hub_buy_price(type_id=34) is None
        assert svc._get_orderbook_levels(type_id=34) == []
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("hub sell price" in m and "34" in m for m in messages)
    assert any("hub buy price" in m and "34" in m for m in messages)
    assert any("order book levels" in m and "34" in m for m in messages)


# --- an unknown hub, order side or smoothing value is refused ----------------------------------
#
# An unknown market hub or order side is refused (a 400 ServiceError
# naming it), not silently read as Jita / sell; an invalid orderbook_smoothing
# config value is refused, a missing one is the config schema's default; an
# out-of-range min_target_margin_pct is clamped with a WARNING naming it.
# None or an empty value still means the default hub / side.

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


def _strict_pricing(market_pricing_cfg):
    cfg = {"defaults": {"market_pricing": market_pricing_cfg}}
    return MarketPricingService(state=SimpleNamespace(admin_settings=None, esi_service=None,
                                                      cfg_manager=SimpleNamespace(all=lambda: cfg)))


def test_an_invalid_orderbook_smoothing_is_refused():
    with pytest.raises(ValueError) as exc:
        _strict_pricing({"orderbook_smoothing": "mode_best_n"}).orderbook_smoothing()
    assert "orderbook_smoothing" in str(exc.value) and "mode_best_n" in str(exc.value)


def test_a_missing_orderbook_smoothing_is_the_schema_default():
    expected = CONFIG_SCHEMA["defaults"]["market_pricing"]["orderbook_smoothing"]
    assert _strict_pricing({}).orderbook_smoothing() == expected


def test_a_valid_orderbook_smoothing_is_used():
    assert _strict_pricing({"orderbook_smoothing": "Mean_Best_N"}).orderbook_smoothing() == "mean_best_n"


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


# --- a failed order-book fetch: unknown price and hub liquidity --------------------------------
#
# A type whose order-book fetch failed (ESIService leaves it out of the
# book and keeps the reason) has an unknown price and hub liquidity, None with
# `price_unavailable_reason` / `liquidity_unavailable_reason`, never an empty
# book read as 0 units; it is not cached. The industry service carries the
# None through: the reason replaces the "thin market" note and the scoring
# treats it as unknown. Fake values only.

JITA_STATION, JITA_REGION = 60003760, 10000002
REASON = "ESI order book unavailable: fake 502"


class _FailingBookEsi:
    def __init__(self, failed=None):
        self.failed = dict(failed or {})   # {(side, type_id): reason}

    def _book(self, side, type_ids):
        return {
            int(t): [{"location_id": JITA_STATION, "price": 5.0, "volume_remain": 100}]
            for t in type_ids if (side, int(t)) not in self.failed
        }

    def get_sell_order_book(self, type_ids, region_id=None):
        return self._book("sell", type_ids)

    def get_buy_order_book(self, type_ids, region_id=None):
        return self._book("buy", type_ids)

    def order_book_unavailable_reason(self, type_id, *, side="sell", region_id=None):
        assert region_id == JITA_REGION
        return self.failed.get((side, int(type_id)))


def _book_pricing(app_engine, esi):
    factory = sessionmaker(bind=app_engine, future=True)
    return MarketPricingService(
        state=SimpleNamespace(admin_settings=None, cfg_manager=None, esi_service=esi),
        sessions=SimpleNamespace(app_session=factory),
    )


def test_a_failed_fetch_gives_an_unknown_price_with_the_reason(app_engine):
    esi = _FailingBookEsi({("sell", 35): REASON})
    out = _book_pricing(app_engine, esi).get_type_price_map(type_ids=[34, 35])
    assert out[34]["unit_price"] == 5.0 and out[34].get("price_unavailable_reason") is None
    assert out[35]["unit_price"] is None
    assert out[35]["price_unavailable_reason"] == REASON


def test_a_failed_price_fetch_is_not_cached(app_engine):
    esi = _FailingBookEsi({("sell", 35): REASON})
    pricing = _book_pricing(app_engine, esi)
    pricing.get_type_price_map(type_ids=[35])
    esi.failed.clear()
    out = pricing.get_type_price_map(type_ids=[35])
    assert out[35]["unit_price"] == 5.0 and out[35]["cached"] is False


def test_a_type_with_no_orders_is_a_known_empty_book(app_engine):
    class _Empty(_FailingBookEsi):
        def _book(self, side, type_ids):
            return {}
    out = _book_pricing(app_engine, _Empty()).get_hub_liquidity_map(type_ids=[36])
    assert out[36]["sell_volume_total"] == 0 and out[36]["liquidity_unavailable_reason"] is None


def test_a_failed_fetch_gives_unknown_liquidity_with_the_reason(app_engine):
    esi = _FailingBookEsi({("sell", 35): REASON})
    out = _book_pricing(app_engine, esi).get_hub_liquidity_map(type_ids=[34, 35])
    assert out[34]["sell_volume_total"] == 100 and out[34]["liquidity_unavailable_reason"] is None
    assert out[35]["sell_volume_total"] is None and out[35]["sell_order_count"] is None
    assert out[35]["buy_volume_total"] == 100          # the buy side was fetched
    assert REASON in out[35]["liquidity_unavailable_reason"]
    assert "sell" in out[35]["liquidity_unavailable_reason"]


def test_a_failed_liquidity_fetch_is_not_cached(app_engine):
    esi = _FailingBookEsi({("sell", 35): REASON})
    pricing = _book_pricing(app_engine, esi)
    pricing.get_hub_liquidity_map(type_ids=[35])
    esi.failed.clear()
    assert pricing.get_hub_liquidity_map(type_ids=[35])[35]["sell_volume_total"] == 100


# --- the industry service ---------------------------------------------------------------------

def _industry():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(cfg_manager=None, esi_service=None, admin_settings=None)
    svc._sessions = None
    return svc


def test_the_market_activity_carries_unknown_liquidity_through(monkeypatch):
    from eve_online_industry_tracker.application.industry import market_enrichment

    class _Pricing:
        def __init__(self, **kw):
            pass

        def get_region_daily_volume_map(self, *, type_ids, hub, unavailable=None):
            return {}

        def get_hub_liquidity_map(self, *, type_ids, hub):
            return {35: {"sell_volume_total": None, "sell_order_count": None, "buy_volume_total": 7,
                         "buy_order_count": 1, "liquidity_unavailable_reason": f"sell: {REASON}"}}

    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    (row,) = _industry()._enrich_product_rows_with_market_activity(
        [{"type_id": 35, "manufacturing_job": {"runs": 1}}], market_hub="jita")
    assert row["hub_sell_liquidity"] is None and row["hub_sell_order_count"] is None
    assert row["hub_buy_liquidity"] == 7
    assert row["liquidity_unavailable_reason"] == f"sell: {REASON}"
    assert row["manufacturing_job"]["hub_sell_liquidity"] is None
    assert row["manufacturing_job"]["liquidity_unavailable_reason"] == f"sell: {REASON}"


def test_unknown_liquidity_has_unknown_metrics():
    (row,) = _industry()._enrich_product_rows_with_liquidity_metrics([{
        "type_id": 35, "region_daily_volume_7d_avg": 50.0, "liquidity_unavailable_reason": REASON,
        "manufacturing_job": {"hub_sell_liquidity": None, "hub_sell_order_count": None,
                              "region_daily_volume_7d_avg": 50.0, "runs": 1},
    }])
    assert row["days_of_supply"] is None
    assert row["sell_through_rate"] is None
    assert row["liquidity_indicator"] == "Unknown"
    assert row["liquidity_score"] is None


def test_the_reason_replaces_the_thin_market_note(monkeypatch, tmp_path):
    from eve_online_industry_tracker.application.industry import market_enrichment

    class _Pricing:
        def __init__(self, **kw):
            pass

        def _market_hub_context(self, hub):
            return {"region_id": JITA_REGION}

    class _History:
        def get_price_stats(self, **kw):
            return {"has_data": True, "avg_7d": 10.0, "avg_42w": 10.0}

        def fetch_and_store_history(self, **kw):
            return None

    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    monkeypatch.setattr(market_enrichment, "MarketHistoryService", lambda **kw: _History())
    (row,) = _industry()._enrich_product_rows_with_price_anomaly(
        [{"type_id": 35, "market_unit_price": 10.0, "hub_sell_order_count": None,
          "liquidity_unavailable_reason": REASON}], market_hub="jita")
    reasons = row["price_anomaly_reasons"]
    assert not any("thin market" in r for r in reasons)
    assert any(REASON in r for r in reasons)


def test_the_pricing_confidence_names_unknown_liquidity():
    (row,) = _industry()._enrich_product_rows_with_pricing_confidence([{
        "type_id": 35, "market_unit_price": 10.0, "market_price_sample_size": 5,
        "market_price_fetched_at": 4102444800.0, "hub_sell_liquidity": None, "hub_sell_order_count": None,
        "liquidity_unavailable_reason": REASON, "region_daily_volume": 30, "region_daily_volume_7d_avg": 30.0,
        "manufacturing_job": {"runs": 1},
    }], product_price_side="sell")
    reasons = row["pricing_confidence_reasons"]
    assert any("unknown" in r and REASON in r for r in reasons)
    assert not any("No sell-side hub liquidity" in r for r in reasons)


