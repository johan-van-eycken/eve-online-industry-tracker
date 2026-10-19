"""ESI pins its response shapes with the X-Compatibility-Date header, not with
a /latest/ or /vN/ route prefix (the prefix is a legacy alias the header
overrides). These tests hold the endpoints unversioned, the date in one place,
and the parsing of the fields that differ between the routes and dates."""
import json
import re
from unittest.mock import MagicMock

import pytest

from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure import esi_service as esi_service_module
from eve_online_industry_tracker.infrastructure import esi_versions

# Every route the app calls, as the ESI OpenAPI spec names it
# (https://esi.evetech.net/meta/openapi.json?compatibility_date=2025-08-26).
SPEC_ROUTES = {
    "ESI_MARKETS_ORDERS": "/markets/{region_id}/orders",
    "ESI_MARKETS_HISTORY": "/markets/{region_id}/history",
    "ESI_MARKETS_PRICES": "/markets/prices",
    "ESI_CHAR_PUBLIC": "/characters/{character_id}",
    "ESI_CHAR_WALLET": "/characters/{character_id}/wallet",
    "ESI_CHAR_WALLET_JOURNAL": "/characters/{character_id}/wallet/journal",
    "ESI_CHAR_WALLET_TRANSACTIONS": "/characters/{character_id}/wallet/transactions",
    "ESI_CHAR_STANDINGS": "/characters/{character_id}/standings",
    "ESI_CHAR_SKILLS": "/characters/{character_id}/skills",
    "ESI_CHAR_SKILLQUEUE": "/characters/{character_id}/skillqueue",
    "ESI_CHAR_IMPLANTS": "/characters/{character_id}/implants",
    "ESI_CHAR_ORDERS": "/characters/{character_id}/orders",
    "ESI_CHAR_ASSETS": "/characters/{character_id}/assets",
    "ESI_CHAR_ASSETS_NAMES": "/characters/{character_id}/assets/names",
    "ESI_CHAR_BLUEPRINTS": "/characters/{character_id}/blueprints",
    "ESI_CHAR_INDUSTRY_JOBS": "/characters/{character_id}/industry/jobs",
    "ESI_CORP_PUBLIC": "/corporations/{corporation_id}",
    "ESI_CORP_DIVISIONS": "/corporations/{corporation_id}/divisions",
    "ESI_CORP_WALLETS": "/corporations/{corporation_id}/wallets",
    "ESI_CORP_WALLETS_JOURNAL": "/corporations/{corporation_id}/wallets/{division}/journal",
    "ESI_CORP_WALLETS_TRANSACTIONS": "/corporations/{corporation_id}/wallets/{division}/transactions",
    "ESI_CORP_STANDINGS": "/corporations/{corporation_id}/standings",
    "ESI_CORP_STRUCTURES": "/corporations/{corporation_id}/structures",
    "ESI_CORP_MEMBERS": "/corporations/{corporation_id}/members",
    "ESI_CORP_MEMBERS_TITLES": "/corporations/{corporation_id}/members/titles",
    "ESI_CORP_TITLES": "/corporations/{corporation_id}/titles",
    "ESI_CORP_ORDERS": "/corporations/{corporation_id}/orders",
    "ESI_CORP_ASSETS": "/corporations/{corporation_id}/assets",
    "ESI_CORP_ASSETS_NAMES": "/corporations/{corporation_id}/assets/names",
    "ESI_CORP_BLUEPRINTS": "/corporations/{corporation_id}/blueprints",
    "ESI_CORP_INDUSTRY_JOBS": "/corporations/{corporation_id}/industry/jobs",
    "ESI_UNIVERSE_NAMES": "/universe/names",
    "ESI_UNIVERSE_TYPE": "/universe/types/{type_id}",
    "ESI_UNIVERSE_STATIONS": "/universe/stations/{station_id}",
    "ESI_UNIVERSE_STRUCTURES_LIST": "/universe/structures",
    "ESI_UNIVERSE_STRUCTURE": "/universe/structures/{structure_id}",
    "ESI_UNIVERSE_REGIONS": "/universe/regions/{region_id}",
    "ESI_UNIVERSE_CONSTELLATIONS": "/universe/constellations/{constellation_id}",
    "ESI_UNIVERSE_SYSTEMS": "/universe/systems/{system_id}",
    "ESI_INDUSTRY_FACILITIES": "/industry/facilities",
    "ESI_INDUSTRY_SYSTEMS": "/industry/systems",
}


def _endpoint_constants() -> dict[str, str]:
    return {
        name: value
        for name, value in vars(esi_versions).items()
        if name.startswith("ESI_") and name != "ESI_COMPATIBILITY_DATE" and isinstance(value, str)
    }


def test_no_endpoint_has_a_version_prefix():
    legacy = re.compile(r"^/(latest|dev|legacy|v\d+)/")
    offenders = {name: path for name, path in _endpoint_constants().items() if legacy.match(path)}
    assert offenders == {}


def test_every_endpoint_is_the_spec_route():
    assert _endpoint_constants() == SPEC_ROUTES


def test_the_compatibility_date_is_set_once_and_the_config_matches_it():
    from config.schemas import CONFIG_SCHEMA

    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", esi_versions.ESI_COMPATIBILITY_DATE)
    assert CONFIG_SCHEMA["esi"]["headers"]["X-Compatibility-Date"] == esi_versions.ESI_COMPATIBILITY_DATE
    with open("config/config.json", encoding="utf-8") as fh:
        configured = json.load(fh)["esi"]["headers"]["X-Compatibility-Date"]
    assert configured == esi_versions.ESI_COMPATIBILITY_DATE


def test_the_pinned_date_predates_the_breaking_character_and_corporation_changes():
    # 2026-06-09: /characters/{id} renamed 'title' to 'corporation_title'.
    # 2026-07-21: /corporations/{id} moved 'tax_rate' to 'tax_rates.isk', renamed
    # 'faction_id' to 'enlisted_faction_id' and made 'ceo_id'/'creator_id' optional.
    # The code reads 'tax_rate' and 'ceo_id'; move the date past these only
    # together with that parsing.
    assert esi_versions.ESI_COMPATIBILITY_DATE < "2026-06-09"


class _Cfg:
    def __init__(self, compatibility_date: str):
        self._data = {
            "esi": {
                "base": "https://esi.evetech.net/latest",
                "token_url": "t", "verify_url": "v", "auth_url": "a",
                "headers": {
                    "Accept": "application/json", "Accept-Language": "en",
                    "X-Compatibility-Date": compatibility_date, "X-Tenant": "tranquility",
                },
            },
            "app": {"redirect_uri": "r", "user_agent": "test"},
            "oauth": {"client_id": "c"},
            "client_secret": "s",
            "defaults": {"scopes": [], "scopes_corp_director": []},
        }

    def get(self, key):
        return self._data[key]


@pytest.mark.parametrize("configured", [esi_versions.ESI_COMPATIBILITY_DATE, "2099-01-01"])
def test_the_client_sends_the_pinned_date_whatever_the_config_says(configured):
    client = esi_client_module.ESIClient(_Cfg(configured), MagicMock(), "Fake Pilot", True, False)
    assert client.esi_header_xcompatibilitydate == esi_versions.ESI_COMPATIBILITY_DATE
    assert client._http_session.headers["X-Compatibility-Date"] == esi_versions.ESI_COMPATIBILITY_DATE
    assert client.esi_base_uri == "https://esi.evetech.net"


def test_the_public_service_sends_the_pinned_date():
    svc = esi_service_module.ESIService.__new__(esi_service_module.ESIService)
    svc._esi_client = object()   # no client attributes: the fallbacks
    assert svc._public_headers()["X-Compatibility-Date"] == esi_versions.ESI_COMPATIBILITY_DATE


# --- the fields that differ between routes -------------------------------------------------------

def test_a_character_job_gives_its_facility_location_as_station_id():
    from eve_online_industry_tracker.application.characters.character import industry_job_location_id

    # /characters/{id}/industry/jobs: 'station_id'; /corporations/{id}/industry/jobs: 'location_id'.
    assert industry_job_location_id({"job_id": 1, "station_id": 60003760}) == 60003760
    assert industry_job_location_id({"job_id": 1, "location_id": 1022734985679}) == 1022734985679
    assert industry_job_location_id({"job_id": 1}) is None
