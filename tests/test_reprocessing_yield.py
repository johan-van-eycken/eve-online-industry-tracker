"""EVE's reprocessing yield, the configured facility and the reprocessing fee.

    yield = (50 + Rm)/100 x (1 + Sec) x (1 + Sm)
            x (1 + 0.03 R) x (1 + 0.02 Re) x (1 + 0.02 Op) x (1 + Im)

The skills are keyed by type id (Reprocessing 3385, Reprocessing Efficiency
3389, the ore's typeDogma attribute 790 skill), each with its SDE per-level
bonus (attribute 379). The facility comes from the admin settings
(reprocessing/facility_type, rig, security_band, tax) and the SDE dogma of the
structure (2722), the rig (717, 2355-2357) and the reprocessing service (717).
Each material is rounded down per batch.

The reference: every skill at 5 and an RX-804 (4%) on Veldspar (400 Tritanium
per batch of 100).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import sde_industry_fakes as fakes
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingsManager
from eve_online_industry_tracker.domain import reprocessing
from eve_online_industry_tracker.infrastructure.sde import industry, ore
from eve_online_industry_tracker.infrastructure.static_data import facility_repo
from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields

ALL_FIVE = {3385: 5, 3389: 5, fakes.SKILL_SIMPLE_ORE_PROCESSING: 5}
RX_804 = 0.04
VELDSPAR = {"id": 1230, "name": "Veldspar", "portionSize": 100, "volume": 0.1, "compressed": False,
            "reprocessingSkillTypeID": fakes.SKILL_SIMPLE_ORE_PROCESSING, "reprocessingSkill": "Simple Ore Processing",
            "materials": [{"name": "Tritanium", "quantity": 400}]}


def _facility(facility_type, rig="none", band="high", tax=0.0, station=fakes.FAKE_NPC_STATION_ID):
    return facility_repo.facility_from_choice(
        facility_type=facility_type, rig=rig, security_band=band, tax=tax,
        npc_station_id=station if facility_type == "npc_station" else None,
        npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data())


def _percent(facility):
    (row,) = compute_yields([VELDSPAR], ALL_FIVE, facility, RX_804)
    return row


# --- the formula ---------------------------------------------------------------------

@pytest.mark.parametrize("base, sec, sm, expected", [
    (0.50, 1.00, 0.0, 0.72358),        # NPC station / a structure without a reprocessing bonus, no rig
    (0.51, 1.00, 0.0, 0.7380516),      # the same with a T1 rig, highsec
    (0.53, 1.00, 0.0, 0.7669948),      # the same with a T2 rig, highsec
])
def test_the_reference_numbers_without_a_structure_bonus(base, sec, sm, expected):
    facility = reprocessing.ReprocessingFacility(name="x", base_yield=base, security_multiplier=sec,
                                                 structure_bonus=sm, tax=0.0, tax_source="setting")
    bonuses = {3385: 0.03, 3389: 0.02, 60377: 0.02}
    assert reprocessing.reprocessing_yield(facility, skill_bonuses=bonuses, skill_levels=ALL_FIVE | {60377: 5},
                                           implant_bonus=RX_804) == pytest.approx(expected, abs=1e-9)


@pytest.mark.parametrize("facility_type, rig, band, percent, tritanium", [
    ("npc_station", "none", "high", 72.36, 289),
    ("npc_station", "none", "null", 72.36, 289),     # no rig: security plays no part
    ("athanor", "none", "high", 73.81, 295),
    ("athanor", "none", "null", 73.81, 295),
    ("athanor", "t1", "high", 75.28, 301),
    ("athanor", "t2", "high", 78.23, 312),
    ("athanor", "t2", "null", 87.62, 350),
    ("tatara", "none", "high", 76.34, 305),
    ("tatara", "t1", "high", 77.86, 311),
    ("tatara", "t2", "high", 80.92, 323),
    ("tatara", "t2", "low", 85.77, 343),
    ("tatara", "t2", "null", 90.63, 362),
])
def test_all_skills_at_5_and_an_rx_804_on_veldspar(facility_type, rig, band, percent, tritanium):
    row = _percent(_facility(facility_type, rig, band))
    assert round(row["batch_yield_percent"], 2) == percent
    assert row["batch_yields"] == {"Tritanium": tritanium}


def test_each_material_is_rounded_down_per_batch():
    assert reprocessing.batch_output(400, 0.72358) == 289
    assert reprocessing.batch_output(100, 0.5) == 50
    assert reprocessing.batch_output(3, 0.3) == 0


def test_the_skills_count_by_type_id_with_their_sde_bonus():
    npc = _facility("npc_station")
    base = _percent(npc)
    no_skills = compute_yields([VELDSPAR], {}, npc, 0.0)[0]
    assert no_skills["batch_yield_percent"] == pytest.approx(50.0)   # an untrained skill counts 0
    only_reprocessing = compute_yields([VELDSPAR], {3385: 5}, npc, 0.0)[0]
    assert only_reprocessing["batch_yield_percent"] == pytest.approx(50.0 * 1.15)
    only_ore_skill = compute_yields([VELDSPAR], {fakes.SKILL_SIMPLE_ORE_PROCESSING: 5}, npc, 0.0)[0]
    assert only_ore_skill["batch_yield_percent"] == pytest.approx(50.0 * 1.10)
    by_name = compute_yields([VELDSPAR], {"Reprocessing": 5, "Simple Ore Processing": 5}, npc, 0.0)[0]
    assert by_name["batch_yield_percent"] == pytest.approx(50.0)     # names are not skill keys
    assert base["batch_yield_percent"] > only_reprocessing["batch_yield_percent"]


def test_another_ore_skill_does_not_count():
    row = compute_yields([VELDSPAR], {fakes.SKILL_COMPLEX_ORE_PROCESSING: 5}, _facility("npc_station"), 0.0)[0]
    assert row["batch_yield_percent"] == pytest.approx(50.0)


def test_a_missing_skill_bonus_in_the_sde_raises_with_the_hint():
    data = fakes.fake_ore_data()
    broken = ore.OreData(compressed_by_base=data.compressed_by_base, base_by_compressed=data.base_by_compressed,
                         skill_by_type=data.skill_by_type,
                         refining_yield_bonus={k: v for k, v in data.refining_yield_bonus.items() if k != 3385},
                         facility_attributes=data.facility_attributes)
    with pytest.raises(industry.SdeIndustryDataMissing, match="3385.*re-run scripts/import_sde.py"):
        compute_yields([VELDSPAR], ALL_FIVE, _facility("npc_station"), 0.0, broken)


def test_an_ore_skill_without_a_bonus_leaves_the_ore_out_with_a_warning(caplog):
    unknown = {**VELDSPAR, "id": 9, "name": "Oddite", "reprocessingSkillTypeID": 4242, "reprocessingSkill": "Odd"}
    with caplog.at_level(logging.WARNING):
        rows = compute_yields([unknown, VELDSPAR], ALL_FIVE, _facility("npc_station"), 0.0)
    assert [r["id"] for r in rows] == [1230]
    assert any("Oddite" in r.getMessage() and "379" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("missing", ["portionSize", "volume"])
def test_an_ore_without_portion_size_or_volume_is_left_out_with_a_warning(caplog, missing):
    broken = {k: v for k, v in VELDSPAR.items() if k != missing}
    with caplog.at_level(logging.WARNING):
        assert compute_yields([broken], ALL_FIVE, _facility("npc_station"), 0.0) == []
    assert any(missing in r.getMessage() and "Veldspar" in r.getMessage() for r in caplog.records)


# --- the facility from the SDE ------------------------------------------------------------

def test_the_facility_values_come_from_the_sde_attributes():
    athanor_t2_null = _facility("athanor", "t2", "null", tax=0.02)
    assert (athanor_t2_null.base_yield, athanor_t2_null.security_multiplier, athanor_t2_null.structure_bonus,
            athanor_t2_null.tax) == (0.53, 1.12, pytest.approx(0.02), 0.02)
    tatara = _facility("tatara")
    assert (tatara.base_yield, tatara.security_multiplier, tatara.structure_bonus) == (0.5, 1.0, pytest.approx(0.055))
    npc = _facility("npc_station", band="null")
    assert (npc.base_yield, npc.security_multiplier, npc.structure_bonus) == (0.5, 1.0, 0.0)
    assert not hasattr(reprocessing, "NPC_STATION_BASE_YIELD")


def test_an_npc_stations_base_yield_is_its_sde_reprocessing_efficiency():
    poor = _facility("npc_station", station=fakes.FAKE_POOR_NPC_STATION_ID)
    assert poor.base_yield == 0.3
    assert round(_percent(poor)["batch_yield_percent"], 2) == 43.41
    assert str(fakes.FAKE_POOR_NPC_STATION_ID) in poor.name


@pytest.mark.parametrize("station", [None, 12345])
def test_an_npc_station_without_a_known_station_id_fails_loudly(station):
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="npc_station_id"):
        _facility("npc_station", station=station)


def test_an_npc_stations_tax_is_its_sde_base_unless_the_setting_gives_the_rate_after_standings():
    base = _facility("npc_station", tax=None)
    assert (base.tax, base.tax_source) == (0.05, "sde_station_base")
    after_standings = _facility("npc_station", tax=0.0213)
    assert (after_standings.tax, after_standings.tax_source) == (0.0213, "setting_after_standings")
    assert "after standings" in after_standings.name and "SDE base" in base.name


def test_a_structures_tax_is_required():
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="reprocessing/tax"):
        _facility("tatara", tax=None)
    tatara = _facility("tatara", tax=0.03)
    assert (tatara.tax, tatara.tax_source) == (0.03, "setting")


def test_a_missing_facility_attribute_raises_with_the_hint():
    data = fakes.fake_ore_data()
    attributes = {k: v for k, v in data.facility_attributes.items() if k != ore.TATARA_TYPE_ID}
    broken = ore.OreData(compressed_by_base={}, base_by_compressed={}, skill_by_type={},
                         refining_yield_bonus=data.refining_yield_bonus, facility_attributes=attributes)
    with pytest.raises(industry.SdeIndustryDataMissing, match="attribute 2722 of type 35836.*re-run scripts/import_sde.py"):
        facility_repo.facility_from_choice(facility_type="tatara", rig="none", security_band="high", tax=0.0,
                                           npc_station_id=None, npc_station=fakes.fake_npc_station, data=broken)


@pytest.mark.parametrize("facility_type, rig, band, match", [
    ("citadel", "none", "high", "facility_type"),
    ("athanor", "t3", "high", "rig"),
    ("athanor", "t1", "wormhole", "security_band"),
    ("npc_station", "t1", "high", "NPC station"),
    (None, "none", "high", "facility_type"),
])
def test_an_invalid_setting_fails_loudly(facility_type, rig, band, match):
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match=match):
        facility_repo.facility_from_choice(facility_type=facility_type, rig=rig, security_band=band, tax=0.0,
                                           npc_station_id=fakes.FAKE_NPC_STATION_ID,
                                           npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data())


def test_a_negative_tax_fails_loudly():
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="tax"):
        _facility("npc_station", tax=-0.01)


# --- the admin settings ------------------------------------------------------------------

def test_the_settings_schema_has_the_reprocessing_facility():
    settings = ADMIN_SETTINGS_SCHEMA["reprocessing"]["settings"]
    assert [o["value"] for o in settings["facility_type"]["options"]] == list(reprocessing.FACILITY_TYPES)
    assert [o["value"] for o in settings["rig"]["options"]] == list(reprocessing.RIGS)
    assert [o["value"] for o in settings["security_band"]["options"]] == list(reprocessing.SECURITY_BANDS)
    assert (settings["facility_type"]["default"], settings["rig"]["default"],
            settings["security_band"]["default"]) == ("npc_station", "none", "high")
    # No hidden tax or station: both start unset.
    assert settings["tax"]["default"] is None and settings["tax"]["optional"] is True
    assert settings["npc_station_id"]["default"] is None and settings["npc_station_id"]["optional"] is True
    assert "standings" in settings["tax"]["help"] and "effective" in settings["tax"]["help"]


def _station_sde(sde_engine):
    from eve_online_industry_tracker.infrastructure.models import NpcStations

    with Session(sde_engine) as session:
        for sid, (efficiency, take) in fakes.NPC_STATIONS.items():
            session.add(NpcStations(id=sid, celestialIndex=1, operationID=1, orbitID=1, orbitIndex=1, ownerID=1,
                                    position={"x": 0.0}, reprocessingEfficiency=efficiency,
                                    reprocessingHangarFlag="4", reprocessingStationsTake=take,
                                    solarSystemID=30000001, typeID=1, useOperationName=False))
        session.commit()
    return Session(sde_engine)


def test_the_station_lookup_reads_npc_stations(sde_engine):
    with _station_sde(sde_engine) as session:
        assert facility_repo.npc_station_reprocessing(session, fakes.FAKE_POOR_NPC_STATION_ID) == (0.3, 0.05)
        assert facility_repo.npc_station_reprocessing(session, 1) is None


def test_the_station_lookup_without_the_columns_raises_with_the_hint():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE "npcStations" (id BIGINT, "solarSystemID" BIGINT)'))
    with Session(engine) as session, pytest.raises(industry.SdeIndustryDataMissing, match="reprocessingEfficiency"):
        facility_repo.npc_station_reprocessing(session, 1)
    engine.dispose()


def test_the_configured_facility_reads_the_settings(tmp_path, sde_engine):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    with _station_sde(sde_engine) as session:
        with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="npc_station_id"):
            facility_repo.configured_facility(mgr, session, fakes.fake_ore_data())   # the default: no station yet
        mgr.set("reprocessing", "npc_station_id", str(fakes.FAKE_NPC_STATION_ID))
        npc = facility_repo.configured_facility(mgr, session, fakes.fake_ore_data())
        assert (npc.base_yield, npc.tax, npc.tax_source) == (0.5, 0.05, "sde_station_base")
        mgr.set_bulk({"reprocessing": {"facility_type": "tatara", "rig": "t2", "security_band": "null", "tax": 0.04}})
        tatara = facility_repo.configured_facility(mgr, session, fakes.fake_ore_data())
    assert round(_percent(tatara)["batch_yield_percent"], 2) == 90.63 and tatara.tax == 0.04
    assert "Tatara" in tatara.name and "T2" in tatara.name and "nullsec" in tatara.name


def test_the_optional_settings_can_be_cleared(tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set("reprocessing", "tax", 0.02)
    mgr.set("reprocessing", "tax", None)
    assert mgr.get("reprocessing", "tax") is None
    mgr.set("reprocessing", "npc_station_id", "")
    assert mgr.get("reprocessing", "npc_station_id") is None
    with pytest.raises(ValueError):
        mgr.set("reprocessing", "npc_station_id", "Jita")


def test_setting_an_invalid_value_is_refused(tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    with pytest.raises(ValueError, match="facility_type|not one of"):
        mgr.set("reprocessing", "facility_type", "citadel")
    with pytest.raises(ValueError):
        mgr.set("reprocessing", "rig", None)
    assert mgr.get("reprocessing", "facility_type") == "npc_station"


def test_a_stored_invalid_value_is_kept_and_fails_loudly_at_use(tmp_path, caplog):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"reprocessing": {"facility_type": "citadel"}}), encoding="utf-8")
    with caplog.at_level(logging.ERROR):
        mgr = AdminSettingsManager(path)
    assert any("reprocessing/facility_type" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="citadel"):
        facility_repo.configured_facility(mgr, None, fakes.fake_ore_data())


def test_the_invalid_setting_is_a_service_error_naming_the_admin_page():
    with pytest.raises(ServiceError) as err:
        _facility("citadel")
    assert err.value.status_code == 500 and "admin" in err.value.message.lower()


def test_the_facility_list_is_the_configured_facility(tmp_path, sde_engine):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set("reprocessing", "npc_station_id", fakes.FAKE_NPC_STATION_ID)
    with _station_sde(sde_engine) as session:
        (only,) = facility_repo.get_all_facilities(mgr, session)
    assert only["id"] == facility_repo.CONFIGURED_FACILITY_ID
    assert (only["facility_type"], only["npc_station_id"], only["tax"], only["tax_source"]) == (
        "npc_station", fakes.FAKE_NPC_STATION_ID, 0.05, "sde_station_base")
    assert "Waira" not in only["name"]


# --- the admin page ----------------------------------------------------------------------

def test_the_admin_page_shows_a_stored_invalid_select_value_instead_of_preselecting_another():
    from streamlit_ui.pages.admin_settings import _select_choices

    spec = ADMIN_SETTINGS_SCHEMA["reprocessing"]["settings"]["facility_type"]
    values, labels, index, problem = _select_choices(spec, "citadel")
    assert values[index] == "citadel" and "citadel" in labels[index]
    assert problem and "citadel" in problem
    values, labels, index, problem = _select_choices(spec, "tatara")
    assert values[index] == "tatara" and problem is None and "citadel" not in " ".join(labels)


def test_the_admin_page_shows_an_unset_optional_number_as_empty():
    from streamlit_ui.pages.admin_settings import _number_input_args

    spec = ADMIN_SETTINGS_SCHEMA["reprocessing"]["settings"]["tax"]
    assert _number_input_args(spec, None)["value"] is None
    assert _number_input_args(spec, 0.02)["value"] == 0.02


# --- the character's skills ----------------------------------------------------------------

def test_the_characters_reprocessing_skills_are_keyed_by_type_id():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE "typeDogma" (id BIGINT, "dogmaAttributes" TEXT, "dogmaEffects" TEXT)'))
        for type_id, row in fakes.TYPE_DOGMA.items():
            conn.execute(text('INSERT INTO "typeDogma" VALUES (:i, :a, NULL)'),
                         {"i": type_id, "a": json.dumps(row["dogmaAttributes"])})
        conn.execute(text('INSERT INTO "typeDogma" VALUES (3300, :a, NULL)'),
                     {"a": json.dumps([{"attributeID": 181, "value": 165.0}])})  # a skill without a reprocessing bonus
    trained = [{"skill_id": 3385, "trained_skill_level": 5}, {"skill_id": 3389, "trained_skill_level": 4},
               {"skill_id": fakes.SKILL_SIMPLE_ORE_PROCESSING, "trained_skill_level": 3},
               {"skill_id": 3300, "trained_skill_level": 5}]
    with Session(engine) as session:
        assert ore.refining_skill_levels(session, trained) == {
            3385: 5, 3389: 4, fakes.SKILL_SIMPLE_ORE_PROCESSING: 3}
        assert ore.refining_skill_levels(session, []) == {}
    engine.dispose()


def test_the_character_method_uses_the_id_keyed_levels(monkeypatch):
    from eve_online_industry_tracker.application.characters.character import Character

    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE "typeDogma" (id BIGINT, "dogmaAttributes" TEXT, "dogmaEffects" TEXT)'))
        conn.execute(text('INSERT INTO "typeDogma" VALUES (3385, :a, NULL)'),
                     {"a": json.dumps([{"attributeID": 379, "value": 3.0}])})
    fake = SimpleNamespace(skills={"skills": [{"skill_id": 3385, "trained_skill_level": 5}]},
                           _db_sde=SimpleNamespace(session=Session(engine), language="de"),
                           character_name="Pilot", reprocessing_skills=None)
    assert Character.extract_reprocessing_skills(fake) == {3385: 5}
    assert fake.reprocessing_skills == {3385: 5}
    fake._db_sde.session.close()
    engine.dispose()


# --- the optimizer -------------------------------------------------------------------------

class _FakeEsi:
    def __init__(self, prices):
        self._prices = prices

    def get_material_prices(self, ids):
        return {i: [{"price": p}] for i, p in self._prices.items() if i in ids}

    def get_ore_prices(self, ids):
        return {i: [{"price": 10.0, "volume_remain": 10_000_000}] for i in ids}

    def get_sell_order_book_metadata(self, ids):
        return {"region_id": 10000002}


def _run(monkeypatch, tmp_path, prices, *, facility_id=None, solution_batches=10.0):
    from eve_online_industry_tracker.infrastructure.static_data import optimizer_service

    monkeypatch.setattr(optimizer_service, "build_all_ores", lambda session, language: [VELDSPAR])
    monkeypatch.setattr(optimizer_service, "build_all_materials",
                        lambda session, language: [{"id": 34, "name": "Tritanium", "volume": 0.01}])

    def fake_optimize(**kwargs):
        return {"status": "ok", "total_cost": 1000.0, "surplus": {},
                "solution": [{"ore_id": 1230, "ore_name": "Veldspar", "batches": solution_batches, "tiers": []}]}

    monkeypatch.setattr(optimizer_service, "optimize_ore_tiered", fake_optimize)
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set_bulk({"reprocessing": {"facility_type": "athanor", "tax": 0.05}})
    character = SimpleNamespace(reprocessing_skills=ALL_FIVE)
    payload = {"demands": {"Tritanium": 2000}, "facility_id": facility_id or facility_repo.CONFIGURED_FACILITY_ID,
               "implant_pct": 4}
    return optimizer_service.run_optimize(payload, character=character, esi_service=_FakeEsi(prices),
                                          sde_session=None, language="en", admin_settings=mgr)


def test_the_fee_taxes_the_value_of_the_minerals_produced(monkeypatch, tmp_path):
    result = _run(monkeypatch, tmp_path, {34: 4.0})
    # An unrigged Athanor (73.81%): 10 batches x 295 Tritanium x 4 ISK x 5% tax, not 5% of the 1000 ISK
    # paid for the ore.
    assert result["demand_coverage"]["Tritanium"]["yielded"] == 2950
    assert result["reprocessing_fee"] == pytest.approx(2950 * 4.0 * 0.05)
    assert result["total_cost_with_reprocessing"] == pytest.approx(1000.0 + 2950 * 4.0 * 0.05)
    assert result["ore_yields"][0]["batch_yields"] == {"Tritanium": 295}


def test_the_fee_without_a_mineral_price_is_unknown_with_a_reason(monkeypatch, tmp_path):
    result = _run(monkeypatch, tmp_path, {})
    assert result["reprocessing_fee"] is None and result["total_cost_with_reprocessing"] is None
    assert "Tritanium" in result["reprocessing_fee_unknown_reason"]


def test_an_unknown_facility_id_is_refused(monkeypatch, tmp_path):
    with pytest.raises(ServiceError, match="facility") as err:
        _run(monkeypatch, tmp_path, {34: 4.0}, facility_id=999)
    assert err.value.status_code == 400


# --- the /ores route without the ore data ----------------------------------------------

def test_the_ores_route_without_the_ore_data_is_a_503(sde_engine):
    from flask_app.app import create_app

    app = create_app()
    app.extensions["app_state"] = SimpleNamespace(
        init_state="Ready", db_app=None, db_oauth=None,
        db_sde=SimpleNamespace(Session=sessionmaker(bind=sde_engine), language="en"))
    ore.reset_ore_data()
    response = app.test_client().get("/ores")
    assert response.status_code == 503
    body = response.get_json()
    assert body["error"]["code"] == "sde_reimport_required" and "re-run scripts/import_sde.py" in body["message"]


# --- the local SDE ---------------------------------------------------------------------------

_LOCAL_SDE = os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db")


def test_the_local_sde_has_the_reprocessing_attributes_and_the_reference_numbers():
    data = fakes.local_ore_data_or_skip()
    assert {t: data.refining_yield_bonus.get(t) for t in (3385, 3389, 60377, 27174)} == {
        3385: 3.0, 3389: 2.0, 60377: 2.0, 27174: 4.0}
    assert data.facility_attributes[ore.ATHANOR_TYPE_ID][ore.STR_REFINING_YIELD_BONUS_ATTRIBUTE_ID] == 2.0
    assert data.facility_attributes[ore.TATARA_TYPE_ID][ore.STR_REFINING_YIELD_BONUS_ATTRIBUTE_ID] == 5.5
    service = data.facility_attributes[ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID]
    assert service[ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID] == 0.5
    for (_, tier), rig in ore.RIG_TYPE_IDS.items():
        attrs = data.facility_attributes[rig]
        assert attrs[ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID] == {"t1": 0.51, "t2": 0.53}[tier]
        assert [attrs[a] for a in (ore.HISEC_MODIFIER_ATTRIBUTE_ID, ore.LOWSEC_MODIFIER_ATTRIBUTE_ID,
                                   ore.NULLSEC_MODIFIER_ATTRIBUTE_ID)] == [1.0, 1.06, 1.12]
    every_ore_skill = set(data.skill_by_type.values())
    assert every_ore_skill and {data.refining_yield_bonus.get(s) for s in every_ore_skill} == {2.0}
    conn = sqlite3.connect(f"file:{_LOCAL_SDE}?mode=ro", uri=True)
    try:
        jita_44 = conn.execute('SELECT "reprocessingEfficiency", "reprocessingStationsTake" FROM "npcStations" '
                               'WHERE id = 60003760').fetchone()
    finally:
        conn.close()
    assert jita_44 == (0.5, 0.05)   # Jita IV - Moon 4 - Caldari Navy Assembly Plant

    def station(station_id):
        return tuple(jita_44) if station_id == 60003760 else None

    for facility_type, rig, band, percent in [("npc_station", "none", "high", 72.36), ("athanor", "t1", "high", 75.28),
                                              ("tatara", "t2", "null", 90.63)]:
        facility = facility_repo.facility_from_choice(facility_type=facility_type, rig=rig, security_band=band,
                                                      tax=0.0, npc_station_id=60003760, npc_station=station,
                                                      data=data)
        (row,) = compute_yields([VELDSPAR], {3385: 5, 3389: 5, 60377: 5}, facility, RX_804, data)
        assert round(row["batch_yield_percent"], 2) == percent


def test_the_local_station_lookup_reads_the_sde():
    if not os.path.exists(_LOCAL_SDE):
        pytest.skip("local SDE database/eve_sde.db not present")
    engine = create_engine(f"sqlite:///file:{os.path.abspath(_LOCAL_SDE)}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            assert facility_repo.npc_station_reprocessing(session, 60003760) == (0.5, 0.05)
            low = session.execute(text('SELECT id, "reprocessingEfficiency", "reprocessingStationsTake" FROM '
                                       '"npcStations" WHERE "reprocessingEfficiency" < 0.5 LIMIT 1')).first()
            assert low is not None
            assert facility_repo.npc_station_reprocessing(session, low[0]) == (low[1], low[2])
    finally:
        engine.dispose()


def test_the_ore_page_shows_why_there_is_no_facility(monkeypatch):
    """The facility fetch raises with the API's reason (shown by the page)
    instead of an empty list, and is not cached across a settings change."""
    from streamlit_ui.api import ore_calculator as api

    monkeypatch.setattr(api, "api_get_or_error", lambda path: (None, "admin setting reprocessing/npc_station_id "
                                                                      "is not set"))
    with pytest.raises(RuntimeError, match="npc_station_id"):
        api.fetch_all_facilities()
    monkeypatch.setattr(api, "api_get_or_error", lambda path: ({"data": [{"id": 1, "name": "Tatara"}]}, None))
    assert api.fetch_all_facilities() == [{"id": 1, "name": "Tatara"}]


# --- the tax bounds: refused, not clamped ------------------------------------------------

@pytest.mark.parametrize("value", [0.6, -0.01, "0.51"])
def test_a_tax_outside_its_bounds_is_refused_naming_the_setting(tmp_path, value):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    with pytest.raises(ValueError, match="reprocessing/tax"):
        mgr.set("reprocessing", "tax", value)
    with pytest.raises(ValueError, match="reprocessing/tax"):
        mgr.set_bulk({"reprocessing": {"tax": value}})
    assert mgr.get("reprocessing", "tax") is None
    mgr.set("reprocessing", "tax", 0.5)
    assert mgr.get("reprocessing", "tax") == 0.5


def test_a_stored_tax_outside_its_bounds_is_kept_and_fails_loudly_at_use(tmp_path, caplog):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"reprocessing": {"facility_type": "athanor", "tax": 0.7}}), encoding="utf-8")
    with caplog.at_level(logging.ERROR):
        mgr = AdminSettingsManager(path)
    assert mgr.get("reprocessing", "tax") == 0.7
    assert any("reprocessing/tax" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="reprocessing/tax=0.7"):
        facility_repo.configured_facility(mgr, None, fakes.fake_ore_data())


def test_the_facility_tax_bound_is_the_settings_bound():
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="reprocessing/tax"):
        _facility("athanor", tax=0.51)
    assert _facility("athanor", tax=0.5).tax == 0.5


def test_other_float_settings_still_clamp(tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set("market_defaults", "default_sales_tax", 0.9)
    assert mgr.get("market_defaults", "default_sales_tax") == 0.20
