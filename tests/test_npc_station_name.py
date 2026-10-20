"""The configured NPC station by name, e.g. "Jita IV - Moon 4 - Caldari Navy
Assembly Plant", composed from the SDE like EVE names a station: the
system, the planet (celestialIndex, in roman numerals), the moon
(orbitIndex, when the station orbits one), the owner corporation and, when
useOperationName, the operation (infrastructure/sde/locations.get_npc_stations
composes the owner and operation part the same way). A name that cannot be
resolved gives the id and a reason; it never fails the facility.

Made-up ids and names in the fake tables; the local-SDE test reads Jita 4-4.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import sde_industry_fakes as fakes
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager
from eve_online_industry_tracker.infrastructure.sde import locations
from eve_online_industry_tracker.infrastructure.static_data import facility_repo

MOON_STATION, PLANET_STATION, NO_OPERATION_NAME, NO_SYSTEM = 60999901, 60999902, 60999903, 60999904


def _loc(en):
    return json.dumps({"en": en, "de": f"{en} (de)"})


@pytest.fixture()
def station_sde():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE "npcStations" (id BIGINT, "celestialIndex" FLOAT, "operationID" BIGINT, '
                          '"orbitID" BIGINT, "orbitIndex" FLOAT, "ownerID" BIGINT, "reprocessingEfficiency" FLOAT, '
                          '"reprocessingStationsTake" FLOAT, "solarSystemID" BIGINT, "useOperationName" BOOLEAN)'))
        conn.execute(text('CREATE TABLE "mapSolarSystems" (id BIGINT, name TEXT)'))
        conn.execute(text('CREATE TABLE "npcCorporations" (id BIGINT, name TEXT)'))
        conn.execute(text('CREATE TABLE "stationOperations" (id BIGINT, "operationName" TEXT)'))
        for row in [(MOON_STATION, 4, 14, 1, 4, 1000001, 0.5, 0.05, 30000001, True),
                    (PLANET_STATION, 8, 32, 2, None, 1000002, 0.3, 0.05, 30000001, True),
                    (NO_OPERATION_NAME, 2, 14, 3, 1, 1000002, 0.5, 0.05, 30000001, False),
                    (NO_SYSTEM, 1, 14, 4, 1, 1000001, 0.5, 0.05, 30009999, True)]:
            conn.execute(text('INSERT INTO "npcStations" VALUES (:a, :b, :c, :d, :e, :f, :g, :h, :i, :j)'),
                         dict(zip("abcdefghij", row)))
        conn.execute(text('INSERT INTO "mapSolarSystems" VALUES (30000001, :n)'), {"n": _loc("Fakeburg")})
        conn.execute(text('INSERT INTO "npcCorporations" VALUES (1000001, :n), (1000002, :m)'),
                     {"n": _loc("Fake Navy"), "m": _loc("Fake Family")})
        conn.execute(text('INSERT INTO "stationOperations" VALUES (14, :n), (32, :m)'),
                     {"n": _loc("Assembly Plant"), "m": _loc("Academy")})
    try:
        yield engine
    finally:
        engine.dispose()


def test_a_moon_station_is_system_planet_moon_owner_operation(station_sde):
    with Session(station_sde) as session:
        assert locations.npc_station_name(session, MOON_STATION, "en") == (
            "Fakeburg IV - Moon 4 - Fake Navy Assembly Plant", None)
        assert locations.npc_station_name(session, MOON_STATION, "de")[0] == (
            "Fakeburg (de) IV - Moon 4 - Fake Navy (de) Assembly Plant (de)")


def test_a_planet_station_has_no_moon_and_an_owner_only_name_has_no_operation(station_sde):
    with Session(station_sde) as session:
        assert locations.npc_station_name(session, PLANET_STATION, "en")[0] == "Fakeburg VIII - Fake Family Academy"
        assert locations.npc_station_name(session, NO_OPERATION_NAME, "en")[0] == "Fakeburg II - Moon 1 - Fake Family"


def test_an_unresolvable_name_gives_a_reason_not_an_error(station_sde):
    with Session(station_sde) as session:
        name, reason = locations.npc_station_name(session, 60123456, "en")
        assert name is None and "60123456" in reason and "npcStations" in reason
        name, reason = locations.npc_station_name(session, NO_SYSTEM, "en")
        assert name is None and "30009999" in reason
    empty = create_engine("sqlite://")
    with Session(empty) as session:
        name, reason = locations.npc_station_name(session, MOON_STATION, "en")
    assert name is None and reason
    empty.dispose()


def test_the_facility_name_is_the_station_name(station_sde, tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set("reprocessing", "npc_station_id", MOON_STATION)
    with Session(station_sde) as session:
        facility = facility_repo.configured_facility(mgr, session, fakes.fake_ore_data())
        (payload,) = facility_repo.get_all_facilities(mgr, session, data=fakes.fake_ore_data())
    assert facility.name.startswith(f"Fakeburg IV - Moon 4 - Fake Navy Assembly Plant ({MOON_STATION})")
    assert payload["name"] == facility.name
    assert payload["npc_station_name"] == "Fakeburg IV - Moon 4 - Fake Navy Assembly Plant"
    assert payload["npc_station_name_problem"] is None


def test_an_unresolved_station_name_shows_the_id_and_the_reason():
    facility = facility_repo.facility_from_choice(
        facility_type="npc_station", rig="none", security_band="high", tax=None,
        npc_station_id=fakes.FAKE_NPC_STATION_ID, npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data(),
        station_name=lambda station_id: (None, "no system 30009999 in mapSolarSystems", None))
    assert f"NPC station {fakes.FAKE_NPC_STATION_ID}" in facility.name
    assert "name unknown: no system 30009999 in mapSolarSystems" in facility.name
    assert facility.tax == 0.05   # the facility itself is not affected


def test_the_station_name_route_answers_with_the_name_or_the_reason(station_sde):
    from sqlalchemy.orm import sessionmaker

    from flask_app.app import create_app

    app = create_app()
    app.extensions["app_state"] = SimpleNamespace(
        init_state="Ready", db_app=None, db_oauth=None,
        db_sde=SimpleNamespace(Session=sessionmaker(bind=station_sde), language="en"))
    client = app.test_client()
    body = client.get(f"/npc_stations/{MOON_STATION}/name").get_json()
    data = body["data"]
    assert (data["station_id"], data["name"], data["problem"]) == (
        MOON_STATION, "Fakeburg IV - Moon 4 - Fake Navy Assembly Plant", None)
    assert "unique name" in data["note"]   # no ESI here: the SDE name, with the note
    body = client.get("/npc_stations/60123456/name").get_json()
    assert body["data"]["name"] is None and "60123456" in body["data"]["problem"]


def test_the_settings_page_shows_the_resolved_station_name(monkeypatch):
    from streamlit_ui.pages import admin_settings as page

    # Round H3: the name is cached per station; each response here is a fresh read.
    page._station_name_data.clear()
    monkeypatch.setattr(page, "api_get_or_error", lambda path: (
        {"data": {"station_id": 60003760, "name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                  "problem": None}}, None))
    assert page._station_caption(60003760) == ("NPC station 60003760: Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                                               None)
    monkeypatch.setattr(page, "api_get_or_error", lambda path: (
        {"data": {"station_id": 1, "name": None, "problem": "no NPC station 1 in the SDE npcStations"}}, None))
    caption, problem = page._station_caption(1)
    assert caption is None and "no NPC station 1" in problem
    page._station_name_data.clear()
    monkeypatch.setattr(page, "api_get_or_error", lambda path: (None, "HTTP 503"))
    caption, problem = page._station_caption(1)
    assert caption is None and "HTTP 503" in problem
    assert page._station_caption(None) == (None, None)
    page._station_name_data.clear()


def test_the_local_sde_names_jita_4_4():
    if not os.path.exists(fakes.LOCAL_SDE):
        pytest.skip("local SDE database/eve_sde.db not present")
    engine = create_engine(f"sqlite:///file:{fakes.LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            assert locations.npc_station_name(session, 60003760, "en") == (
                "Jita IV - Moon 4 - Caldari Navy Assembly Plant", None)
    finally:
        engine.dispose()
