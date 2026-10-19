"""Task C1: the market intelligence collector and correlator no longer
swallow every exception. A failed read logs a WARNING with the reason (and
keeps its fallback, or fails the cycle where nothing can be collected); a
programming error (KeyError for an unknown setting, a TypeError) propagates.
Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.market_intelligence import market_depth_collector as mdc
from eve_online_industry_tracker.application.market_intelligence.margin_correlator import MarginCorrelator
from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector


def _db_error():
    return OperationalError("SELECT fake", {}, Exception("fake db down"))


class _Admin:
    def __init__(self, exc):
        self._exc = exc

    def get(self, category, key):
        raise self._exc


class _FailingSession:
    def __init__(self, exc):
        self._exc = exc

    def execute(self, *a, **k):
        raise self._exc

    def query(self, *a, **k):
        raise self._exc

    def close(self):
        pass


class _Sessions:
    def __init__(self, session):
        self._session = session

    def app_session(self):
        return self._session


def _collector(session=None, admin=None, state=None):
    c = MarketDepthCollector.__new__(MarketDepthCollector)
    c._state = state or SimpleNamespace()
    c._admin = admin
    c._sessions = _Sessions(session or _FailingSession(_db_error()))
    return c


# --- settings readers: a KeyError (unknown setting) is a programming error ---------------

@pytest.mark.parametrize("reader", ["_get_min_days", "_get_threshold"])
def test_correlator_setting_reader_propagates_an_unknown_setting(reader):
    corr = MarginCorrelator(state=SimpleNamespace(), admin_settings=_Admin(KeyError("Unknown setting")))
    with pytest.raises(KeyError):
        getattr(corr, reader)()


@pytest.mark.parametrize("reader,default", [("_get_min_days", 30), ("_get_threshold", -0.7)])
def test_correlator_setting_reader_warns_on_an_unparsable_value(reader, default, caplog):
    corr = MarginCorrelator(state=SimpleNamespace(), admin_settings=_Admin(ValueError("not a number")))
    with caplog.at_level(logging.WARNING):
        assert getattr(corr, reader)() == default
    assert "not a number" in caplog.text


def test_collector_outlier_sigma_propagates_an_unknown_setting():
    with pytest.raises(KeyError):
        _collector(admin=_Admin(KeyError("Unknown setting")))._get_outlier_sigma()


def test_collector_outlier_sigma_warns_on_an_unparsable_value(caplog):
    with caplog.at_level(logging.WARNING):
        assert _collector(admin=_Admin(TypeError("fake")))._get_outlier_sigma() == 3.0
    assert "planner_price_outlier_sigma" in caplog.text


# --- correlator reads -------------------------------------------------------------------------

def test_correlator_is_due_warns_when_the_cache_cannot_be_read(monkeypatch, caplog):
    from eve_online_industry_tracker.infrastructure import session_provider
    monkeypatch.setattr(session_provider, "StateSessionProvider",
                        lambda state: _Sessions(_FailingSession(_db_error())))
    corr = MarginCorrelator(state=SimpleNamespace())
    with caplog.at_level(logging.WARNING):
        assert corr._is_due() is True
    assert "margin_correlation_cache" in caplog.text and "fake db down" in caplog.text


def test_correlator_is_due_propagates_a_programming_error(monkeypatch):
    from eve_online_industry_tracker.infrastructure import session_provider
    monkeypatch.setattr(session_provider, "StateSessionProvider",
                        lambda state: _Sessions(_FailingSession(AttributeError("fake bug"))))
    with pytest.raises(AttributeError):
        MarginCorrelator(state=SimpleNamespace())._is_due()


# --- collector reads --------------------------------------------------------------------------

def test_active_plan_type_ids_failure_fails_the_cycle_loudly():
    with pytest.raises(OperationalError):
        _collector()._get_type_ids_from_active_plan()


def test_effective_velocity_warns_when_the_plan_and_sales_history_cannot_be_read(monkeypatch, caplog):
    from eve_online_industry_tracker.application.industry import sales_history_service

    class _Svc:
        def __init__(self, **k):
            pass

        def get_sold_history(self, **k):
            raise _db_error()

    monkeypatch.setattr(sales_history_service, "SalesHistoryService", _Svc)
    with caplog.at_level(logging.WARNING):
        assert _collector()._get_effective_velocity(34, {98000001}) is None
    assert caplog.text.count("fake db down") == 2
    assert "type_id=34" in caplog.text


def test_effective_velocity_propagates_a_programming_error_in_the_sales_history(monkeypatch):
    from eve_online_industry_tracker.application.industry import sales_history_service

    class _Session(_FailingSession):
        def execute(self, *a, **k):
            return SimpleNamespace(fetchone=lambda: None)

    class _Svc:
        def __init__(self, **k):
            pass

        def get_sold_history(self, **k):
            raise TypeError("fake signature bug")

    monkeypatch.setattr(sales_history_service, "SalesHistoryService", _Svc)
    with pytest.raises(TypeError):
        _collector(session=_Session(None))._get_effective_velocity(34, {98000001})


def test_own_volume_read_failure_raises_for_the_caller_to_mark_the_row_stale():
    # F1 item 2: no silent 0 (competitor_units would include our own orders); run() keeps the old row
    with pytest.raises(OperationalError):
        _collector()._query_own_sell_volume(34, {98000001}, set())


def test_the_corporation_set_is_read_without_indexing(monkeypatch):
    # _corporation_ids is a set: every corporation counts, there is no "primary" one to index
    state = SimpleNamespace(corp_manager=SimpleNamespace(_corporation_ids={98000001, 98000002}))
    assert _collector(state=state)._tracked_owner_ids() == ({98000001, 98000002}, set())
