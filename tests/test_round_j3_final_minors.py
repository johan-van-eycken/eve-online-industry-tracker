"""J3: the final review's minors. Missing data gives a reason, a WARNING or a
visible error: an unknown region volume is an unknown liquidity score (None),
a non-list order-book reply is an unavailable book with a reason, the unknown
corporation check names its caller, /industry_active_jobs of an unknown
character is a 404, a partial market-orders reply is cached for a minute
only, and the one-time blended-lot re-stamp runs at startup (a failure a
WARNING). Fake ids and values only."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

# --- J3.3: an unknown region volume is an unknown liquidity score ------------------------------


def _industry_cls():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    return IndustryService


@pytest.mark.parametrize("volume", [None, "n/a"])
def test_an_unknown_region_volume_is_an_unknown_liquidity_score(volume):
    score = _industry_cls()._calculate_liquidity_score(
        hub_sell_liquidity=100, region_daily_volume_7d_avg=volume, hub_sell_orders=10)
    assert score is None


def test_a_known_region_volume_still_scores():
    score = _industry_cls()._calculate_liquidity_score(
        hub_sell_liquidity=100, region_daily_volume_7d_avg=100.0, hub_sell_orders=100)
    assert score == pytest.approx((100.0 - 1 / 30 * 100.0) * 0.7 + 30.0, abs=0.01)


def test_the_liquidity_metrics_of_an_unknown_region_volume_are_unknown():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    svc = IndustryService.__new__(IndustryService)
    (row,) = svc._enrich_product_rows_with_liquidity_metrics([{
        "type_id": 35, "region_daily_volume_7d_avg": None,
        "manufacturing_job": {"hub_sell_liquidity": 100, "hub_sell_order_count": 10,
                              "region_daily_volume_7d_avg": None, "runs": 1},
    }])
    assert row["liquidity_score"] is None
    assert row["days_of_supply"] is None
    assert row["liquidity_indicator"] == "Unknown"


# --- J3.4: a non-list order-book reply is an unavailable book with a reason --------------------


def _esi_svc(reply):
    from eve_online_industry_tracker.infrastructure.esi_service import ESIService

    svc = ESIService.__new__(ESIService)
    svc._esi_client = SimpleNamespace()
    svc._admin_settings = None
    svc._type_orders_cache, svc._market_history_cache = {}, {}
    svc._type_orders_cache_ttl_seconds, svc._market_history_cache_ttl_seconds = 300, 21600
    svc._region_id, svc._station_id = 10000002, 60003760
    svc._public_esi_get = lambda endpoint, *, params=None, **kw: reply
    return svc


@pytest.mark.parametrize("side", ["sell", "buy"])
@pytest.mark.parametrize("reply", [{"error": "fake"}, None, "oops"])
def test_a_non_list_order_book_reply_is_unavailable_with_a_reason(monkeypatch, caplog, side, reply):
    monkeypatch.setattr("eve_online_industry_tracker.infrastructure.esi_service.time.sleep", lambda s: None)
    svc = _esi_svc(reply)
    with caplog.at_level(logging.WARNING):
        book = getattr(svc, f"get_{side}_order_book")([35])
    assert book == {}
    reason = svc.order_book_unavailable_reason(35, side=side)
    assert reason and "not a list" in reason and type(reply).__name__ in reason
    assert (side, 10000002, 35) not in svc._type_orders_cache          # not cached as an empty book
    assert any("35" in r.getMessage() and "not a list" in r.getMessage() for r in caplog.records)


# --- J3.5: the unknown corporation check names its caller --------------------------------------


def test_the_unknown_corporation_error_names_the_calling_method():
    from eve_online_industry_tracker.application.corporations.corporation_manager import UnknownCorporationError
    from eve_online_industry_tracker.application.unknown_owner import require_known_corporation

    def get_corporation(corporation_id=None, corporation_name=None):
        raise UnknownCorporationError("get_corporation", f"corporation_id {corporation_id}")

    state = SimpleNamespace(corp_manager=SimpleNamespace(get_corporation=get_corporation))
    with pytest.raises(UnknownCorporationError) as info:
        require_known_corporation(state, 98000002, "get_realized_profit_ledger")
    assert info.value.method_name == "get_realized_profit_ledger"
    assert str(info.value).startswith("get_realized_profit_ledger failed")
    assert "get_corporation" not in str(info.value)
    assert "corporation_id 98000002" in str(info.value)
    assert isinstance(info.value.__cause__, UnknownCorporationError)


def test_a_known_corporation_and_no_id_pass():
    from eve_online_industry_tracker.application.unknown_owner import require_known_corporation

    state = SimpleNamespace(corp_manager=SimpleNamespace(get_corporation=lambda corporation_id=None: object()))
    require_known_corporation(state, 98000001, "get_realized_profit_ledger")
    require_known_corporation(SimpleNamespace(), None, "get_realized_profit_ledger")


# --- J3.6: /industry_active_jobs of an unknown character is a 404 ------------------------------


def _jobs_svc(session_provider, known=(7001,)):
    from eve_online_industry_tracker.application.industry.service import IndustryService

    state = SimpleNamespace(
        char_manager=SimpleNamespace(get_character_by_id=lambda cid: object() if cid in known else None),
        db_sde=SimpleNamespace(language="en"),
    )
    svc = IndustryService(state=state, sessions=session_provider)
    svc._character_slot_capacities = lambda: {}
    return svc


def test_active_jobs_of_an_unknown_character_raise(session_provider):
    from eve_online_industry_tracker.application.unknown_owner import UnknownCharacterError

    with pytest.raises(UnknownCharacterError, match="character_id 7002") as info:
        _jobs_svc(session_provider).industry_active_jobs(character_id=7002)
    assert info.value.method_name == "industry_active_jobs"


def test_active_jobs_of_a_known_character_and_of_all_read_as_before(session_provider):
    svc = _jobs_svc(session_provider)
    assert svc.industry_active_jobs(character_id=7001) == {"jobs": [], "slot_capacities": {"7001": {}}}
    assert svc.industry_active_jobs() == {"jobs": [], "slot_capacities": {}}


def test_the_active_jobs_route_answers_404_unknown_character(monkeypatch, session_provider):
    from flask_app.app import create_app
    from flask_app.routes import industry as industry_routes

    svc = _jobs_svc(session_provider)
    monkeypatch.setattr(industry_routes, "require_ready", lambda state: None)
    monkeypatch.setattr(industry_routes, "get_state", lambda: SimpleNamespace())
    monkeypatch.setattr(industry_routes, "IndustryService", lambda state=None: svc)
    response = create_app().test_client().get("/industry_active_jobs?character_id=7002")
    assert response.status_code == 404
    assert response.get_json()["error"]["code"] == "unknown_character"


# --- J3.7: a partial market-orders reply is cached for a minute only ---------------------------


@pytest.fixture()
def orders_api(monkeypatch):
    from streamlit_ui.api import market_orders as api

    calls: list[str] = []
    reply = {"value": {"status": "success", "data": [{"order_id": 1}]}}

    def api_get(path, timeout_seconds=None):
        calls.append(path)
        return dict(reply["value"]), None

    now = {"t": 1_000_000.0}
    monkeypatch.setattr(api, "api_get_or_error", api_get)
    monkeypatch.setattr(api, "time", SimpleNamespace(time=lambda: now["t"]))
    api.clear_market_orders_cache()
    yield SimpleNamespace(api=api, calls=calls, reply=reply, now=now)
    api.clear_market_orders_cache()


def test_a_partial_reply_is_fetched_again_after_a_minute(orders_api):
    orders_api.reply["value"] = {"status": "success", "data": [{"order_id": 1}], "errors": {"Pilot B": "502"}}
    api = orders_api.api
    assert api.fetch_market_orders()["errors"] == {"Pilot B": "502"}
    orders_api.now["t"] += 30
    api.fetch_market_orders()
    assert len(orders_api.calls) == 1                     # within the minute: the cached reply
    orders_api.reply["value"] = {"status": "success", "data": [{"order_id": 1}, {"order_id": 2}]}
    orders_api.now["t"] += 31
    response = api.fetch_market_orders()
    assert len(orders_api.calls) == 2                     # older than a minute: fetched again
    assert "errors" not in response and len(response["data"]) == 2


def test_a_complete_reply_stays_cached(orders_api):
    api = orders_api.api
    api.fetch_market_orders()
    orders_api.now["t"] += 600
    assert api.fetch_market_orders()["data"] == [{"order_id": 1}]
    assert len(orders_api.calls) == 1


def test_a_failed_fetch_raises_and_is_not_cached(orders_api, monkeypatch):
    api = orders_api.api
    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: (None, "down"))
    with pytest.raises(RuntimeError, match="down"):
        api.fetch_market_orders()
    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: (
        {"status": "success", "data": []}, None))
    assert api.fetch_market_orders()["data"] == []


# --- J3.1: the one-time blended-lot re-stamp at startup ----------------------------------------


def _init(monkeypatch, restamp):
    from eve_online_industry_tracker.application.characters import corp_transfer_costs
    from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
    from eve_online_industry_tracker.infrastructure import schema_migrations
    from utils import app_init

    session = SimpleNamespace(rollbacks=0)
    session.rollback = lambda: setattr(session, "rollbacks", session.rollbacks + 1)
    db_app = SimpleNamespace(session=session)
    cfg = SimpleNamespace(all=lambda: {"app": {"database_oauth_uri": "x", "database_app_uri": "y",
                                                "database_sde_uri": "z"}})
    monkeypatch.setattr(app_init, "init_db_oauth", lambda cfg: SimpleNamespace(session=None))
    monkeypatch.setattr(app_init, "init_db_app", lambda cfg: db_app)
    monkeypatch.setattr(app_init, "init_db_sde", lambda cfg: SimpleNamespace())
    monkeypatch.setattr(schema_migrations, "ensure_app_schema", lambda db: None)
    monkeypatch.setattr(esi_client_module, "prune_stale_esi_cache", lambda session: None)
    monkeypatch.setattr(corp_transfer_costs, "restamp_all_blended_corp_transfer_lots_once", restamp)
    return app_init, cfg, session


def test_startup_runs_the_one_time_restamp_on_the_app_session(monkeypatch):
    seen = []
    app_init, cfg, session = _init(monkeypatch, lambda s: seen.append(s))
    app_init.init_db_managers(cfg)
    assert seen == [session]


def test_a_failed_one_time_restamp_is_a_warning_and_a_bug_propagates(monkeypatch, caplog):
    def fail(session):
        raise OperationalError("UPDATE", {}, Exception("database is locked"))

    app_init, cfg, session = _init(monkeypatch, fail)
    with caplog.at_level(logging.WARNING):
        app_init.init_db_managers(cfg)
    assert "re-stamp" in caplog.text and "database is locked" in caplog.text
    assert session.rollbacks == 1

    def bug(session):
        raise AttributeError("bug")

    app_init, cfg, _ = _init(monkeypatch, bug)
    with pytest.raises(AttributeError):
        app_init.init_db_managers(cfg)
