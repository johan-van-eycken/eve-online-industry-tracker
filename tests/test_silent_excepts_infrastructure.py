"""Task C2: the formerly silent `except Exception` handlers in infrastructure and
utils. Expected (remote/data) failures now log a WARNING and keep the per-item
skip; programming errors propagate. Fake values only."""
from __future__ import annotations

import logging
import threading
from types import SimpleNamespace

import pytest
import requests
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.esi_service import ESIService
from eve_online_industry_tracker.infrastructure.esi_client import EsiRequestError
from fakes import _raiser


def _svc(esi_client) -> ESIService:
    svc = ESIService.__new__(ESIService)
    svc._esi_client = esi_client
    svc._public_structures_cache, svc._public_structures_cache_ttl_seconds = {}, 600
    return svc


# --- /universe/names/ -----------------------------------------------------------------------

@pytest.mark.parametrize("method", ["resolve_universe_names", "get_universe_names"])
def test_a_failed_universe_names_chunk_is_a_warning_and_the_ids_stay_unresolved(method, caplog):
    svc = _svc(SimpleNamespace(esi_post=_raiser(EsiRequestError("ESI POST failed after retries"))))
    with caplog.at_level(logging.WARNING):
        out = getattr(svc, method)([90000001, 90000002])
    assert out == {}
    assert any("/universe/names/" in r.getMessage() and "2 id(s)" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("method", ["resolve_universe_names", "get_universe_names"])
def test_a_transport_error_on_universe_names_is_skipped_too(method):
    svc = _svc(SimpleNamespace(esi_post=_raiser(requests.ConnectionError("reset"))))
    assert getattr(svc, method)([90000001]) == {}


@pytest.mark.parametrize("method", ["resolve_universe_names", "get_universe_names"])
def test_a_bug_behind_universe_names_propagates(method):
    svc = _svc(SimpleNamespace(esi_post=_raiser(AttributeError("no attribute 'x'"))))
    with pytest.raises(AttributeError):
        getattr(svc, method)([90000001])


# --- public structure scan (ESIService.get_public_structures) -------------------------------

def test_an_unreadable_public_structure_is_skipped_with_a_summary_warning(caplog):
    svc = _svc(SimpleNamespace(esi_get=lambda *a, **k: [1021, 1022]))

    def get_structure(sid, **k):
        if sid == 1022:
            raise EsiRequestError("ESI request failed for universe structure 1022: timeout")
        return {"name": "S", "solar_system_id": 30000142, "owner_id": 1, "type_id": 35825, "services": []}

    svc.get_universe_structure = get_structure
    with caplog.at_level(logging.WARNING):
        rows = svc.get_public_structures(30000142, max_workers=2)
    assert [r["station_id"] for r in rows] == [1021]
    assert any("1 of 2 structure(s) could not be read" in r.getMessage() for r in caplog.records)


def test_a_bug_in_the_public_structure_scan_surfaces():
    svc = _svc(SimpleNamespace(esi_get=lambda *a, **k: [1021]))
    svc.get_universe_structure = _raiser(NameError("name 'oops' is not defined"))
    # get_public_structures wraps only ESI failures in RuntimeError; a bug propagates as itself.
    with pytest.raises(NameError, match="oops"):
        svc.get_public_structures(30000142, max_workers=1)


# --- ESI monitor: telemetry, a failure is a WARNING (not swallowed silently, not fatal) --------

def test_a_bug_in_the_esi_monitor_is_a_warning_not_a_failed_request(monkeypatch, caplog):
    class _BrokenMonitor:
        def record_sleep(self, **kwargs):
            raise TypeError("record_sleep() got an unexpected keyword argument")

    slept = []
    monkeypatch.setattr(esi_client_module, "_ESI_MONITOR_WARNING_LIMITER",
                        esi_client_module._EsiRequestErrorLogLimiter(window_seconds=300.0, max_logs_per_key=1))
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", lambda: _BrokenMonitor())
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "suggested_sleep_seconds", lambda: 0.5)
    monkeypatch.setattr(esi_client_module, "_sleep_seconds", lambda s: slept.append(s))
    with caplog.at_level(logging.WARNING):
        esi_client_module._esi_gate("test")
    assert slept == [0.5]
    assert any(r.levelno == logging.WARNING and "ESI monitor record_sleep failed" in r.getMessage()
               and "unexpected keyword argument" in r.getMessage() for r in caplog.records)


def test_esi_request_errors_do_not_include_programming_errors():
    for exc in (NameError, AttributeError, TypeError, KeyError):
        assert not issubclass(exc, esi_client_module.ESI_REQUEST_ERRORS)
    assert issubclass(requests.HTTPError, esi_client_module.ESI_REQUEST_ERRORS)


# --- background public structures refresh ---------------------------------------------------

def _refresh_state(get_public_structures):
    return SimpleNamespace(
        shutdown_event=None,
        db_app=SimpleNamespace(),
        esi_service=SimpleNamespace(get_public_structures=get_public_structures),
        public_structures_last_refresh_by_system={},
        public_structures_refresh_lock=threading.Lock(),
        public_structures_refreshing_system_ids=set(),
    )


def _join_refresh_thread(system_id):
    for t in threading.enumerate():
        if t.name == f"public-structures-refresh-{system_id}":
            t.join(timeout=5)


def test_a_failed_background_structure_refresh_is_logged_and_kept_in_state(caplog):
    from eve_online_industry_tracker.infrastructure.public_structures_cache_service import (
        trigger_refresh_public_structures_for_system,
    )

    state = _refresh_state(_raiser(RuntimeError("ESI request failed for system 30000142: 503")))
    with caplog.at_level(logging.WARNING):
        assert trigger_refresh_public_structures_for_system(state=state, system_id=30000142) is True
        _join_refresh_thread(30000142)
    assert "503" in state.public_structures_last_refresh_error
    assert 30000142 not in state.public_structures_refreshing_system_ids
    assert any("Background public structures refresh" in r.getMessage() for r in caplog.records)


# --- schema migrations -----------------------------------------------------------------------

def test_unreadable_table_columns_are_an_error_that_fails_the_migration(caplog):
    # Round H2.6: no longer a WARNING and an empty set (then a failing ADD COLUMN).
    from eve_online_industry_tracker.infrastructure.schema_migrations import SchemaMigrationError, _table_columns

    db = SimpleNamespace(query=_raiser(OperationalError("PRAGMA", {}, Exception("database is locked"))))
    with caplog.at_level(logging.ERROR), pytest.raises(SchemaMigrationError, match="some_table"):
        _table_columns(db, "some_table")
    assert any("Failed reading columns of some_table" in r.getMessage() for r in caplog.records)


def test_a_bug_reading_table_columns_propagates():
    from eve_online_industry_tracker.infrastructure.schema_migrations import _table_columns

    db = SimpleNamespace(query=_raiser(NameError("x")))
    with pytest.raises(NameError):
        _table_columns(db, "some_table")
