"""F2b: the ESIService RuntimeError wrappers wrap only ESI failures
(ESI_REQUEST_ERRORS); a programming error propagates as itself. Plus the
background public structures error.
Fake values only."""
from __future__ import annotations

import logging
import threading
import time
from types import SimpleNamespace

import pytest
import requests

from eve_online_industry_tracker.infrastructure.esi_service import ESIService


def _raiser(exc):
    def _f(*a, **k):
        raise exc
    return _f


def _svc(esi_client=None) -> ESIService:
    svc = ESIService.__new__(ESIService)
    svc._esi_client = esi_client if esi_client is not None else SimpleNamespace()
    svc._admin_settings = None
    svc._public_structures_cache, svc._public_structures_cache_ttl_seconds = {}, 600
    svc._market_prices_cache, svc._market_prices_cache_ttl_seconds = None, 3600
    svc._station_name_cache, svc._station_name_failures = {}, {}
    return svc


ESI_FAILURES = [requests.ConnectionError("reset"), requests.HTTPError("502"),
                RuntimeError("ESI GET failed after retries"), ValueError("No refresh token")]
BUGS = [AttributeError("'NoneType' object has no attribute 'get'"), NameError("name 'oops' is not defined"),
        TypeError("unsupported operand"), KeyError("name")]


# --- get_universe_structure ------------------------------------------------------------------

@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_an_esi_failure_of_a_universe_structure_is_a_runtime_error(exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(RuntimeError, match="universe structure 1021"):
        svc.get_universe_structure(1021)


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_behind_a_universe_structure_propagates(exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(type(exc)):
        svc.get_universe_structure(1021)


# --- get_location_info -------------------------------------------------------------------------

def _location_svc(public_get, esi_get=None):
    svc = _svc(SimpleNamespace(
        get_id_type=lambda i: "structure" if i >= 1020000000000 else "station",
        esi_get=esi_get or _raiser(AssertionError("not called")),
    ))
    svc._public_esi_get = public_get
    return svc


@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_an_esi_failure_of_a_location_is_a_runtime_error(exc):
    with pytest.raises(RuntimeError, match="location 60003760"):
        _location_svc(_raiser(exc)).get_location_info(60003760)
    with pytest.raises(RuntimeError, match="location 1021000000000"):
        _location_svc(None, esi_get=_raiser(exc)).get_location_info(1021000000000)


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_behind_a_location_propagates(exc):
    with pytest.raises(type(exc)):
        _location_svc(_raiser(exc)).get_location_info(60003760)


# --- _fetch_station_name -----------------------------------------------------------------------

@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_an_esi_failure_of_a_station_name_is_a_runtime_error_and_cached(exc):
    svc = _svc()
    svc._public_esi_get = _raiser(exc)
    with pytest.raises(RuntimeError, match="station 60003760"):
        svc._fetch_station_name(60003760, "en")
    assert (60003760, "en") in svc._station_name_failures


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_behind_a_station_name_propagates_and_is_not_cached(exc):
    svc = _svc()
    svc._public_esi_get = _raiser(exc)
    with pytest.raises(type(exc)):
        svc._fetch_station_name(60003760, "en")
    assert svc._station_name_failures == {}


# --- get_market_prices -------------------------------------------------------------------------

@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_an_esi_failure_of_the_market_prices_is_a_runtime_error(exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(RuntimeError, match="ESI request failed"):
        svc.get_market_prices()


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_behind_the_market_prices_propagates(exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(type(exc)):
        svc.get_market_prices()


# --- get_public_structures (the outer wrapper and the per-structure skip) ---------------------

@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_an_esi_failure_of_the_structure_list_is_a_runtime_error(exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(RuntimeError, match="system 30000142"):
        svc.get_public_structures(30000142)


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_in_the_public_structure_scan_propagates_as_itself(exc):
    svc = _svc(SimpleNamespace(esi_get=lambda *a, **k: [1021]))
    svc.get_universe_structure = _raiser(exc)
    with pytest.raises(type(exc)):
        svc.get_public_structures(30000142, max_workers=1)


# --- background public structures refresh: the error is always the latest -------------------------

def test_a_failed_background_refresh_replaces_an_older_error(caplog):
    from eve_online_industry_tracker.infrastructure.public_structures_cache_service import (
        trigger_refresh_public_structures_for_system,
    )

    state = SimpleNamespace(
        shutdown_event=None,
        db_app=None,               # refresh fails before it resets the state
        esi_service=None,
        public_structures_last_refresh_error="an older failure",
        public_structures_last_refresh_by_system={},
        public_structures_refresh_lock=threading.Lock(),
        public_structures_refreshing_system_ids=set(),
    )
    with caplog.at_level(logging.WARNING):
        assert trigger_refresh_public_structures_for_system(state=state, system_id=30000144) is True
        deadline = time.time() + 5
        while 30000144 in state.public_structures_refreshing_system_ids and time.time() < deadline:
            time.sleep(0.01)
    assert state.public_structures_last_refresh_error == "db_app or esi_service not initialized"
