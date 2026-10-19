"""ESIService.get_station_name asks ESI for the station's name in the SDE
language the app uses (the Accept-Language header of
/universe/stations/{station_id}), cached per (station, language). When ESI
has no name in that language (its Content-Language is another one, or it
does not say) or does not support the language at all, the name ESI gives
(English) is used with a note saying so. Fake values throughout.
"""
from __future__ import annotations

import pytest

from eve_online_industry_tracker.infrastructure.esi_service import ESIService
from eve_online_industry_tracker.infrastructure.static_data import facility_repo


class _Response:
    def __init__(self, name, content_language):
        self._name = name
        self.headers = {} if content_language is None else {"Content-Language": content_language}

    def json(self):
        return {"station_id": 60008494, "name": self._name}


def _service(names_by_language, content_language=lambda requested: requested):
    """An ESIService whose public GET answers with the name for the requested
    Accept-Language (English when it has none), and the calls it got."""
    svc = ESIService.__new__(ESIService)
    svc._station_name_cache = {}
    svc._station_name_failures = {}
    calls = []

    def fake_get(endpoint, **kwargs):
        calls.append((endpoint, kwargs))
        requested = kwargs["headers"]["Accept-Language"]
        if requested in names_by_language:
            return _Response(names_by_language[requested], content_language(requested))
        return _Response(names_by_language["en"], "en")

    svc._public_esi_get = fake_get
    return svc, calls


EN = "Amarr VIII (Oris) - Emperor Family Academy"
DE = "Amarr VIII (Oris) - Akademie der kaiserlichen Familie"


def test_the_name_is_asked_in_the_sde_language():
    svc, calls = _service({"en": EN, "de": DE})
    assert svc.get_station_name(60008494, "de") == (DE, None)
    (endpoint, kwargs), = calls
    assert endpoint == "/universe/stations/60008494"
    assert kwargs == {"headers": {"Accept-Language": "de"}, "timeout_seconds": 3.0, "max_retries": 0,
                      "return_response": True}


def test_english_is_the_default_and_has_no_note():
    svc, calls = _service({"en": EN})
    assert svc.get_station_name(60008494) == (EN, None)
    assert calls[0][1]["headers"] == {"Accept-Language": "en"}


def test_without_a_translation_the_english_name_comes_with_a_note():
    svc, _ = _service({"en": EN})
    name, note = svc.get_station_name(60008494, "fr")
    assert name == EN
    assert "fr" in note and "English" in note


def test_an_unconfirmed_language_comes_with_a_note():
    svc, _ = _service({"en": EN, "ja": "fake ja name"}, content_language=lambda requested: None)
    name, note = svc.get_station_name(60008494, "ja")
    assert name == "fake ja name" and "ja" in note and "Content-Language" in note


def test_a_language_esi_does_not_have_asks_english_with_a_note():
    svc, calls = _service({"en": EN})
    name, note = svc.get_station_name(60008494, "it")
    assert name == EN and "it" in note and "English" in note
    assert calls[0][1]["headers"] == {"Accept-Language": "en"}


def test_the_cache_is_keyed_by_language():
    svc, calls = _service({"en": EN, "de": DE})
    assert svc.get_station_name(60008494, "de")[0] == DE
    assert svc.get_station_name(60008494, "en")[0] == EN
    assert svc.get_station_name(60008494, "de")[0] == DE
    assert svc.get_station_name(60008494, "DE")[0] == DE          # the same language
    assert len(calls) == 2


def test_a_failure_is_cached_per_language():
    svc = ESIService.__new__(ESIService)
    svc._station_name_cache = {}
    svc._station_name_failures = {}
    calls = []

    def down_for_german(endpoint, **kwargs):
        calls.append(kwargs["headers"]["Accept-Language"])
        if kwargs["headers"]["Accept-Language"] == "de":
            raise ConnectionError("ESI unreachable")
        return _Response(EN, "en")

    svc._public_esi_get = down_for_german
    with pytest.raises(RuntimeError, match="unreachable"):
        svc.get_station_name(60008494, "de")
    with pytest.raises(RuntimeError, match="unreachable"):
        svc.get_station_name(60008494, "de")
    assert svc.get_station_name(60008494, "en") == (EN, None)
    assert calls == ["de", "en"]


def test_the_facility_name_passes_the_language_and_shows_the_note(monkeypatch):
    svc, calls = _service({"en": EN})
    monkeypatch.setattr(facility_repo._locations, "npc_station_name",
                        lambda session, station_id, language: pytest.fail("the ESI name is used"))
    name, problem, note = facility_repo.npc_station_name(object(), 60008494, "fr", esi_service=svc)
    assert name == EN and problem is None and "fr" in note
    assert calls[0][1]["headers"] == {"Accept-Language": "fr"}
    svc, _ = _service({"en": EN, "de": DE})
    assert facility_repo.npc_station_name(object(), 60008494, "de", esi_service=svc) == (DE, None, None)


def test_the_public_get_sends_the_headers_and_can_return_the_response():
    class _Http:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, timeout=None, headers=None):
            self.calls.append((url, headers))
            response = _Response(EN, "de")
            response.status_code = 200
            response.raise_for_status = lambda: None
            return response

    svc = ESIService.__new__(ESIService)
    svc._esi_client = None
    svc._http_session = _Http()
    response = svc._public_esi_get("/universe/stations/60008494", headers={"Accept-Language": "de"},
                                   timeout_seconds=3.0, max_retries=0, return_response=True)
    assert response.headers["Content-Language"] == "de"
    assert svc._http_session.calls == [("https://esi.evetech.net/universe/stations/60008494",
                                        {"Accept-Language": "de"})]
    assert svc._public_esi_get("/universe/stations/60008494", timeout_seconds=3.0, max_retries=0)["name"] == EN
