"""Round H2.3: a dedicated EsiRequestError(RuntimeError).

ESI_REQUEST_ERRORS is (EsiRequestError, requests.RequestException): a plain
ValueError or RuntimeError of our own code is no longer read as an ESI
failure by the catch sites, and propagates as itself. Fake values only.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS, EsiRequestError
from eve_online_industry_tracker.infrastructure.esi_service import ESIService


def _raiser(exc):
    def _f(*a, **k):
        raise exc
    return _f


def _svc(esi_client) -> ESIService:
    svc = ESIService.__new__(ESIService)
    svc._esi_client = esi_client
    svc._admin_settings = None
    svc._market_prices_cache, svc._market_prices_cache_ttl_seconds = None, 3600
    svc._industry_facilities_cache, svc._industry_facilities_cache_ttl_seconds = None, 3600
    svc._public_structures_cache, svc._public_structures_cache_ttl_seconds = {}, 600
    return svc


def test_the_tuple_is_the_esi_error_and_requests_only():
    assert ESI_REQUEST_ERRORS == (EsiRequestError, requests.RequestException)
    assert issubclass(EsiRequestError, RuntimeError)
    assert not issubclass(RuntimeError, ESI_REQUEST_ERRORS)
    assert not issubclass(ValueError, ESI_REQUEST_ERRORS)


@pytest.mark.parametrize("bug", [RuntimeError("our own bug"), ValueError("bad literal")])
@pytest.mark.parametrize("method", ["get_market_prices", "get_industry_facilities", "get_industry_systems"])
def test_a_plain_runtime_or_value_error_propagates_unwrapped(method, bug):
    svc = _svc(SimpleNamespace(esi_get=_raiser(bug)))
    with pytest.raises(type(bug)) as info:
        getattr(svc, method)()
    assert info.value is bug


@pytest.mark.parametrize("method", ["get_market_prices", "get_industry_facilities", "get_industry_systems"])
def test_an_esi_failure_is_an_esi_request_error(method):
    svc = _svc(SimpleNamespace(esi_get=_raiser(EsiRequestError("ESI GET failed after retries"))))
    with pytest.raises(EsiRequestError, match="ESI request failed"):
        getattr(svc, method)()


def test_a_missing_refresh_token_is_an_esi_request_error():
    client = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    client.refresh_token = None
    with pytest.raises(EsiRequestError, match="No refresh token"):
        client.refresh_access_token()


def test_a_bad_token_payload_is_an_esi_request_error():
    with pytest.raises(EsiRequestError, match="unexpected SSO token response"):
        esi_client_module.ESIClient._parse_token_payload({"access_token": "a"})


@pytest.mark.parametrize("raw", ["abc", "0", None])
def test_an_unreadable_x_pages_header_is_an_esi_request_error(raw):
    with pytest.raises(EsiRequestError, match="X-Pages"):
        esi_client_module.esi_total_pages({"X-Pages": raw}, "https://esi.example/x/")


def test_no_x_pages_header_is_one_page():
    assert esi_client_module.esi_total_pages({}, "https://esi.example/x/") == 1
