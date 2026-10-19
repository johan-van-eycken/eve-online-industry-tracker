"""ESIService.get_station_name asks ESI for the station's name in the SDE
language the app uses (the Accept-Language header of
/universe/stations/{station_id}, its primary subtag: en-us is en).

ESI echoes the requested Content-Language but does not translate station
names (a live GET of /universe/stations/60003760 with de and ja gave the
English name with Content-Language de / ja), so the reply's
Content-Language is not trusted: the English name is fetched too (cached)
and a name equal to it, or one that cannot be compared with it, comes with
the note "ESI gives this station name in English only". A language ESI does
not have at all is asked in English, with a note. Names and failures are
cached per (station, language). Fake values throughout.
"""
from __future__ import annotations

import pytest
import requests

from eve_online_industry_tracker.infrastructure.esi_service import ESIService
from eve_online_industry_tracker.infrastructure.static_data import facility_repo

ENGLISH_ONLY = "ESI gives this station name in English only"


class _Response:
    def __init__(self, name, content_language):
        self._name = name
        self.status_code = 200
        self.headers = {"Content-Language": content_language}

    def raise_for_status(self):
        return None

    def json(self):
        return {"station_id": 60000001, "name": self._name}


class _EchoingEsi:
    """The HTTP session of a fake ESI that, like ESI, echoes the requested
    Accept-Language as the Content-Language, with the name it has for that
    language (the English one when it has none)."""

    def __init__(self, names_by_language, fail=()):
        self._names, self._fail, self.calls = names_by_language, fail, []

    def get(self, url, params=None, timeout=None, headers=None):
        requested = (headers or {})["Accept-Language"]
        self.calls.append((url, dict(headers or {}), timeout))
        if requested in self._fail:
            raise requests.ConnectionError(f"ESI unreachable for {requested}")
        return _Response(self._names.get(requested, self._names["en"]), requested)


def _service(names_by_language, fail=()):
    svc = ESIService.__new__(ESIService)
    svc._esi_client = None
    svc._station_name_cache = {}
    svc._station_name_failures = {}
    svc._http_session = _EchoingEsi(names_by_language, fail)
    return svc, svc._http_session.calls


def _asked(calls):
    return [headers["Accept-Language"] for _, headers, _ in calls]


EN = "Fakeburg VIII (Fakeplanet) - Fake Academy"
DE = "Fakeburg VIII (Fakeplanet) - Falsche Akademie"


def test_an_echoed_content_language_with_the_english_name_is_noted_as_english_only():
    svc, calls = _service({"en": EN})                       # ESI as it is: no translation, the language echoed
    name, note = svc.get_station_name(60000001, "de")
    assert name == EN and note == ENGLISH_ONLY
    assert _asked(calls) == ["de", "en"]


def test_a_real_translation_has_no_note():
    svc, calls = _service({"en": EN, "de": DE})
    assert svc.get_station_name(60000001, "de") == (DE, None)
    assert calls[0] == ("https://esi.evetech.net/universe/stations/60000001", {"Accept-Language": "de"}, 3.0)


def test_english_is_the_default_one_call_and_no_note():
    svc, calls = _service({"en": EN})
    assert svc.get_station_name(60000001) == (EN, None)
    assert _asked(calls) == ["en"]


def test_the_english_name_is_fetched_once_for_every_language():
    svc, calls = _service({"en": EN, "de": DE})
    svc.get_station_name(60000001, "de")
    svc.get_station_name(60000001, "fr")
    svc.get_station_name(60000001, "en")
    svc.get_station_name(60000001, "de")
    assert _asked(calls) == ["de", "en", "fr"]


def test_a_name_that_cannot_be_compared_is_noted_as_english_only():
    svc, _ = _service({"en": EN, "de": DE}, fail=("en",))
    name, note = svc.get_station_name(60000001, "de")
    assert name == DE
    assert note.startswith(ENGLISH_ONLY) and "could not" in note and "unreachable" in note


@pytest.mark.parametrize("language, asked", [("en-US", "en"), ("de-DE", "de"), ("DE", "de"), ("zh_CN", "zh")])
def test_the_primary_language_subtag_is_asked(language, asked):
    svc, calls = _service({"en": EN, "de": DE, "zh": "fake zh name"})
    svc.get_station_name(60000001, language)
    assert _asked(calls)[0] == asked


def test_a_language_esi_does_not_have_asks_english_with_a_note():
    svc, calls = _service({"en": EN})
    name, note = svc.get_station_name(60000001, "it")
    assert name == EN and "'it'" in note and "English" in note
    assert _asked(calls) == ["en"]


def test_a_failure_is_cached_per_language():
    svc, calls = _service({"en": EN}, fail=("de",))
    with pytest.raises(RuntimeError, match="unreachable"):
        svc.get_station_name(60000001, "de")
    with pytest.raises(RuntimeError, match="unreachable"):
        svc.get_station_name(60000001, "de")
    assert svc.get_station_name(60000001, "en") == (EN, None)
    assert _asked(calls) == ["de", "en"]


def test_the_facility_name_passes_the_language_and_shows_the_note(monkeypatch):
    svc, calls = _service({"en": EN})
    monkeypatch.setattr(facility_repo._locations, "npc_station_name",
                        lambda session, station_id, language: pytest.fail("the ESI name is used"))
    assert facility_repo.npc_station_name(object(), 60000001, "fr", esi_service=svc) == (EN, None, ENGLISH_ONLY)
    assert _asked(calls)[0] == "fr"
    svc, _ = _service({"en": EN, "de": DE})
    assert facility_repo.npc_station_name(object(), 60000001, "de", esi_service=svc) == (DE, None, None)
