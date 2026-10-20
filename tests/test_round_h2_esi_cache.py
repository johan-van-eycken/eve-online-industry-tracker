"""Round H2.5: the ESI cache.

- No If-None-Match when the cached data cannot be decoded (a 304 would then
  give None as the data), nor for a paginated request (its pages are never
  cached as a whole).
- save_to_cache respects use_cache=False.
- get_market_prices does not cache None (a 403/404) as a success: it raises
  EsiRequestError with the reason; get_industry_facilities/systems raise on a
  non-list instead of returning [].

Fake values only.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.esi_client import EsiRequestError
from eve_online_industry_tracker.infrastructure.esi_service import ESIService


class _Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.request = SimpleNamespace(headers={}, body=None)
        self.text = json.dumps(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class _Session:
    def __init__(self, response):
        self.response = response
        self.headers_seen = []

    def get(self, url, headers=None, **kwargs):
        self.headers_seen.append(dict(headers or {}))
        return self.response

    def post(self, url, headers=None, **kwargs):
        self.headers_seen.append(dict(headers or {}))
        return self.response


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(esi_client_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", None)
    monkeypatch.setattr(esi_client_module, "esi_max_retries", lambda: 0)
    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.access_token, c.token_expiry, c.refresh_token = "token", None, "refresh"
    c.character_name = "Test Pilot"
    c.esi_base_uri, c.user_agent = "https://esi.example", "test"
    c.esi_header_accept = c.esi_header_acceptlanguage = c.esi_header_xcompatibilitydate = c.esi_header_xtenant = "x"
    c.db_oauth = MagicMock()
    c._http_session = _Session(_Response(200, {"ok": 1}))
    monkeypatch.setattr(esi_client_module.requests, "post",
                        lambda url, headers=None, **kwargs: c._http_session.post(url, headers=headers))
    return c


def _cache_row(client, data, etag="W/\"abc\""):
    row = SimpleNamespace(endpoint="k", etag=etag, data=data)
    client.db_oauth.session.query.return_value.filter.return_value.first.return_value = row
    return row


@pytest.mark.parametrize("method", ["esi_get", "esi_post"])
def test_an_undecodable_cache_entry_sends_no_etag(client, method, caplog):
    _cache_row(client, "{not json")
    with caplog.at_level(logging.WARNING):
        assert getattr(client, method)("/x/", use_cache=True) == {"ok": 1}
    assert "If-None-Match" not in client._http_session.headers_seen[0]


@pytest.mark.parametrize("method", ["esi_get", "esi_post"])
def test_a_decodable_cache_entry_sends_its_etag_and_a_304_gives_the_cached_data(client, method):
    _cache_row(client, json.dumps({"cached": True}))
    client._http_session.response = _Response(304)
    assert getattr(client, method)("/x/", use_cache=True) == {"cached": True}
    assert client._http_session.headers_seen[0]["If-None-Match"] == "W/\"abc\""


def test_a_304_of_a_cached_json_string_is_the_string(client):
    _cache_row(client, json.dumps("a name"))
    client._http_session.response = _Response(304)
    assert client.esi_get("/x/", use_cache=True) == "a name"


@pytest.mark.parametrize("method", ["esi_get", "esi_post"])
def test_a_paginated_request_sends_no_etag(client, method):
    _cache_row(client, json.dumps([{"page": "one only"}]))
    client._http_session.response = _Response(200, [{"a": 1}], headers={"X-Pages": "1"})
    assert getattr(client, method)("/x/", use_cache=True, paginate=True) == [{"a": 1}]
    assert "If-None-Match" not in client._http_session.headers_seen[0]


@pytest.mark.parametrize("method", ["esi_get", "esi_post"])
def test_use_cache_false_does_not_save_to_cache(client, method, monkeypatch):
    saved = []
    monkeypatch.setattr(client, "save_to_cache", lambda *a: saved.append(a))
    getattr(client, method)("/x/", use_cache=False)
    assert saved == []
    getattr(client, method)("/x/", use_cache=True)
    assert len(saved) == 1


# --- ESIService ------------------------------------------------------------------------------------

def _svc(esi_get):
    svc = ESIService.__new__(ESIService)
    svc._esi_client = SimpleNamespace(esi_get=esi_get)
    svc._admin_settings = None
    svc._market_prices_cache, svc._market_prices_cache_ttl_seconds = None, 3600
    svc._industry_facilities_cache, svc._industry_facilities_cache_ttl_seconds = None, 3600
    return svc


@pytest.mark.parametrize("payload", [None, {"not": "a list"}])
def test_market_prices_that_are_not_a_list_raise_and_are_not_cached(payload):
    calls = []

    def esi_get(*a, **k):
        calls.append(1)
        return payload

    svc = _svc(esi_get)
    with pytest.raises(EsiRequestError, match="markets/prices"):
        svc.get_market_prices()
    assert svc._market_prices_cache is None
    with pytest.raises(EsiRequestError):
        svc.get_market_prices()
    assert len(calls) == 2


def test_market_prices_are_cached():
    svc = _svc(lambda *a, **k: [{"type_id": 34, "adjusted_price": 5.0}])
    assert svc.get_market_prices() == [{"type_id": 34, "adjusted_price": 5.0}]
    svc._esi_client = SimpleNamespace(esi_get=lambda *a, **k: pytest.fail("not cached"))
    assert svc.get_market_prices() == [{"type_id": 34, "adjusted_price": 5.0}]


@pytest.mark.parametrize("method", ["get_industry_facilities", "get_industry_systems"])
@pytest.mark.parametrize("payload", [None, {"not": "a list"}])
def test_industry_lists_that_are_not_a_list_raise(method, payload):
    svc = _svc(lambda *a, **k: payload)
    with pytest.raises(EsiRequestError, match="/industry/"):
        getattr(svc, method)()
    assert getattr(svc, "_industry_facilities_cache") is None
    assert getattr(svc, "_industry_systems_cache", None) is None
