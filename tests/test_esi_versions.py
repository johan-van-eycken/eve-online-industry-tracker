"""ESI pins its response shapes with the X-Compatibility-Date header, not with
a /latest/ or /vN/ route prefix (the prefix is a legacy alias the header
overrides). These tests hold the endpoints unversioned, the date in one place,
and the parsing of the fields that differ between the routes and dates."""
import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace
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
    with open(Path(__file__).resolve().parents[1] / "config" / "config.json", encoding="utf-8") as fh:
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
    fake_structure_id = 1000000000001
    assert industry_job_location_id({"job_id": 1, "location_id": fake_structure_id}) == fake_structure_id
    assert industry_job_location_id({"job_id": 1}) is None


# --- Task D review minors -------------------------------------------------------------------------

@pytest.fixture()
def fresh_compat_warning(monkeypatch):
    monkeypatch.setattr(esi_client_module, "_COMPATIBILITY_DATE_WARNED", set())


def test_the_ignored_config_date_is_warned_once_per_process(caplog, fresh_compat_warning):
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            esi_client_module.ESIClient(_Cfg("2099-01-01"), MagicMock(), "Fake Pilot", True, False)
    warnings = [r.getMessage() for r in caplog.records if "X-Compatibility-Date" in r.getMessage()]
    assert len(warnings) == 1 and "2099-01-01" in warnings[0]


def test_the_active_jobs_rows_name_the_facility_location_location_id():
    import inspect

    from eve_online_industry_tracker.application.industry.service import IndustryService

    source = inspect.getsource(IndustryService.industry_active_jobs)
    assert "blueprint_location_id" not in source
    assert source.count('"location_id": int(getattr(job, "location_id", 0) or 0)') == 2


def _oauth_db():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from eve_online_industry_tracker.infrastructure.models import BaseOauth

    engine = create_engine("sqlite:///:memory:")
    BaseOauth.metadata.create_all(bind=engine)
    return SimpleNamespace(session=sessionmaker(bind=engine)())


def test_the_cache_key_carries_the_compatibility_date(monkeypatch):
    key = esi_client_module.esi_cache_key("/characters/1/skills")
    assert key.startswith(f"[{esi_versions.ESI_COMPATIBILITY_DATE}]") and key.endswith("/characters/1/skills")
    monkeypatch.setattr(esi_client_module, "ESI_COMPATIBILITY_DATE", "2099-01-01")
    assert esi_client_module.esi_cache_key("/characters/1/skills") != key


class _CacheResponse:
    def __init__(self, status, body=None, etag=None):
        self.status_code, self._body = status, body
        self.headers = {"ETag": etag} if etag else {}
        self.request = SimpleNamespace(headers={}, body=None)
        self.text = json.dumps(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        return None


class _CacheSession:
    def __init__(self, responses):
        self.responses, self.headers_sent = list(responses), []

    def get(self, url, headers=None, timeout=None, **kwargs):
        self.headers_sent.append(dict(headers or {}))
        return self.responses.pop(0)


def _cache_client(monkeypatch, db, session):
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", None)
    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.access_token, c.token_expiry = "token", None
    c.esi_base_uri, c.user_agent = "https://esi.example", "test"
    c.esi_header_accept = c.esi_header_acceptlanguage = c.esi_header_xtenant = "x"
    c.esi_header_xcompatibilitydate = esi_versions.ESI_COMPATIBILITY_DATE
    c.db_oauth = db
    c._http_session = session
    return c


def test_a_date_bump_never_serves_an_old_shape_304(monkeypatch):
    db = _oauth_db()
    first = _CacheSession([_CacheResponse(200, {"title": "old shape"}, etag="etag-1")])
    assert _cache_client(monkeypatch, db, first).esi_get("/characters/1") == {"title": "old shape"}
    # Same date: the ETag is sent, and a 304 serves the cached body.
    again = _CacheSession([_CacheResponse(304)])
    assert _cache_client(monkeypatch, db, again).esi_get("/characters/1") == {"title": "old shape"}
    assert again.headers_sent[0].get("If-None-Match") == "etag-1"
    # A later date: the old entry is not this date's, so no ETag is sent (no 304 of the old shape).
    monkeypatch.setattr(esi_client_module, "ESI_COMPATIBILITY_DATE", "2099-01-01")
    bumped = _CacheSession([_CacheResponse(200, {"corporation_title": "new shape"}, etag="etag-2")])
    assert _cache_client(monkeypatch, db, bumped).esi_get("/characters/1") == {"corporation_title": "new shape"}
    assert "If-None-Match" not in bumped.headers_sent[0]


def test_the_startup_cleanup_prunes_the_orphaned_cache_rows_once(caplog):
    from eve_online_industry_tracker.infrastructure.models import EsiCache

    db = _oauth_db()
    current = esi_client_module.esi_cache_key("/characters/1/skills")
    for endpoint in ("/latest/characters/1/skills", "/v4/characters/1/skills", "/characters/1/skills",
                     "[2000-01-01]/characters/1/skills", current):
        db.session.add(EsiCache(endpoint=endpoint, etag=None, data="{}", last_updated=0.0))
    db.session.commit()
    with caplog.at_level(logging.INFO):
        assert esi_client_module.prune_stale_esi_cache(db.session) == 4
    assert [row.endpoint for row in db.session.query(EsiCache).all()] == [current]
    assert any("4" in r.getMessage() and "esi_cache" in r.getMessage() for r in caplog.records)
    assert esi_client_module.prune_stale_esi_cache(db.session) == 0          # idempotent


def test_the_startup_cleanup_runs_at_db_init():
    import inspect

    from utils import app_init

    assert "prune_stale_esi_cache" in inspect.getsource(app_init.init_db_managers)


def test_the_repo_root_config_schemas_copy_is_gone():
    root = Path(__file__).resolve().parents[1]
    assert not (root / "config" / "schemas.py").exists()
    assert "Keep this in sync" not in (root / "src" / "config" / "schemas.py").read_text(encoding="utf-8")


def test_the_startup_cleanup_without_a_cache_table_is_a_no_op():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    session = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    assert esi_client_module.prune_stale_esi_cache(session) == 0
