"""A reprocessing rig per ore class.

On an Athanor asteroid ore, moon ore and ice each need their own M-Set rig
(Asteroid Ore Grading Processor 46633/46634, Moon Ore Grading Processor
46637/46638, Ice Grading Processor 46635/46636); on a Tatara the L-Set
Reprocessing Monitor 46639/46640 covers all three. The SDE says which class a
rig affects: its typeDogma effects (structureRigAsteroidOreYield 6674,
structureRigMoonOreYield 6675, structureRigIceYield 6678) pre-assign the
rig's refiningYieldMultiplier (717) to the reprocessing service's
refiningYieldNormalOres 2444 / refiningYieldMoonOres 2445 / refiningYieldIce
2448. An ore gets the rig of its class; without one it gets the unrigged
base (the service's attribute for the class) and no security modifier.

Fake tables (tests/sde_industry_fakes.py, CCP's public dogma ids and values)
except the local-SDE tests at the end.
"""
from __future__ import annotations

import json
import logging

import pytest

import sde_industry_fakes as fakes
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingsManager
from eve_online_industry_tracker.domain import reprocessing
from eve_online_industry_tracker.infrastructure.sde import industry, ore
from eve_online_industry_tracker.infrastructure.static_data import facility_repo
from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields

ALL_FIVE = {3385: 5, 3389: 5, fakes.SKILL_SIMPLE_ORE_PROCESSING: 5, fakes.SKILL_UBIQUITOUS_MOON_ORE_PROCESSING: 5,
            fakes.SKILL_ICE_PROCESSING: 5}
RX_804 = 0.04
VELDSPAR = {"id": 1230, "name": "Veldspar", "portionSize": 100, "volume": 0.1, "compressed": False,
            "groupID": 462, "categoryID": 25,
            "reprocessingSkillTypeID": fakes.SKILL_SIMPLE_ORE_PROCESSING, "materials": [{"name": "Tritanium", "quantity": 400}]}
BITUMENS = {"id": 45492, "name": "Bitumens", "portionSize": 100, "volume": 10.0, "compressed": False,
            "groupID": 1884, "categoryID": 25,
            "reprocessingSkillTypeID": fakes.SKILL_UBIQUITOUS_MOON_ORE_PROCESSING,
            "materials": [{"name": "Pyerite", "quantity": 6000}]}
CLEAR_ICICLE = {"id": 16262, "name": "Clear Icicle", "portionSize": 1, "volume": 1000.0, "compressed": False,
                "groupID": 465, "categoryID": 25, "reprocessingSkillTypeID": fakes.SKILL_ICE_PROCESSING,
                "materials": [{"name": "Heavy Water", "quantity": 69}]}


def _athanor(*, asteroid="none", moon="none", ice="none", band="null"):
    return facility_repo.facility_from_choice(
        facility_type="athanor", rig="none", athanor_rigs={"asteroid": asteroid, "moon": moon, "ice": ice},
        security_band=band, tax=0.0, npc_station_id=None, npc_station=fakes.fake_npc_station,
        data=fakes.fake_ore_data())


def _tatara(rig, band="null"):
    return facility_repo.facility_from_choice(
        facility_type="tatara", rig=rig, security_band=band, tax=0.0, npc_station_id=None,
        npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data())


def _percents(facility):
    rows = compute_yields([VELDSPAR, BITUMENS, CLEAR_ICICLE], ALL_FIVE, facility, RX_804)
    return {r["name"]: round(r["batch_yield_percent"], 2) for r in rows}


# --- the SDE says which class a rig affects -------------------------------------------------

def test_the_rig_classes_come_from_the_rigs_dogma_effects():
    data = fakes.fake_ore_data()
    assert data.rig_ore_classes[46633] == data.rig_ore_classes[46634] == frozenset({"asteroid"})
    assert data.rig_ore_classes[46637] == data.rig_ore_classes[46638] == frozenset({"moon"})
    assert data.rig_ore_classes[46635] == data.rig_ore_classes[46636] == frozenset({"ice"})
    assert data.rig_ore_classes[46639] == data.rig_ore_classes[46640] == frozenset({"asteroid", "moon", "ice"})


def test_an_ores_class_is_its_sde_group():
    assert ore.ore_class(462, 25) == "asteroid"          # Veldspar
    assert ore.ore_class(468, 25) == "asteroid"          # Mercoxit
    assert ore.ore_class(1884, 25) == "moon"             # Ubiquitous Moon Asteroids
    assert ore.ore_class(1923, 25) == "moon"             # Exceptional Moon Asteroids
    assert ore.ore_class(465, 25) == "ice"
    assert ore.ore_class(711, 2) is None                 # a gas cloud: not reprocessed as ore
    assert ore.ore_class(None, None) is None


# --- the pinned numbers (every skill at 5, an RX-804) ---------------------------------------

def test_an_athanor_with_only_an_asteroid_t2_rig_in_nullsec_gives_moon_ore_and_ice_the_unrigged_yield():
    percents = _percents(_athanor(asteroid="t2", band="null"))
    assert percents == {"Veldspar": 87.62, "Bitumens": 73.81, "Clear Icicle": 73.81}
    unrigged = _percents(_athanor(band="null"))
    assert unrigged == {"Veldspar": 73.81, "Bitumens": 73.81, "Clear Icicle": 73.81}


def test_each_athanor_rig_lifts_only_its_own_class():
    assert _percents(_athanor(moon="t1", band="high")) == {"Veldspar": 73.81, "Bitumens": 75.28, "Clear Icicle": 73.81}
    assert _percents(_athanor(ice="t2", band="low")) == {"Veldspar": 73.81, "Bitumens": 73.81, "Clear Icicle": 82.93}
    assert _percents(_athanor(asteroid="t2", moon="t2", ice="t2", band="null")) == {
        "Veldspar": 87.62, "Bitumens": 87.62, "Clear Icicle": 87.62}


def test_the_tatara_monitor_covers_every_class():
    assert _percents(_tatara("t2", "null")) == {"Veldspar": 90.63, "Bitumens": 90.63, "Clear Icicle": 90.63}
    assert _percents(_tatara("none", "null")) == {"Veldspar": 76.34, "Bitumens": 76.34, "Clear Icicle": 76.34}


def test_the_facility_carries_the_yield_of_each_class():
    athanor = _athanor(asteroid="t2", band="null")
    assert athanor.yields["asteroid"] == reprocessing.ClassYield(base_yield=0.53, security_multiplier=1.12, rig="t2")
    assert athanor.yields["moon"] == reprocessing.ClassYield(base_yield=0.5, security_multiplier=1.0, rig="none")
    assert athanor.multiplier_for("moon") == pytest.approx(0.5 * 1.02)
    assert "asteroid T2" in athanor.name and "moon none" in athanor.name and "ice none" in athanor.name


def test_an_npc_station_gives_every_class_its_base():
    npc = facility_repo.facility_from_choice(
        facility_type="npc_station", rig="none", security_band="null", tax=None,
        npc_station_id=fakes.FAKE_POOR_NPC_STATION_ID, npc_station=fakes.fake_npc_station,
        data=fakes.fake_ore_data())
    assert {c: y.base_yield for c, y in npc.yields.items()} == {"asteroid": 0.3, "moon": 0.3, "ice": 0.3}


def test_an_ore_outside_the_asteroid_category_is_left_out_with_a_warning(caplog):
    gas = {**VELDSPAR, "id": 9, "name": "Fakeite Gas", "groupID": 711, "categoryID": 2}
    with caplog.at_level(logging.WARNING):
        rows = compute_yields([gas, VELDSPAR], ALL_FIVE, _athanor(), RX_804)
    assert [r["name"] for r in rows] == ["Veldspar"] and rows[0]["ore_class"] == "asteroid"
    assert any("Fakeite Gas" in r.getMessage() and "class" in r.getMessage() for r in caplog.records)


# --- invalid settings and SDE data -----------------------------------------------------------

@pytest.mark.parametrize("rigs, match", [
    ({"asteroid": "t3", "moon": "none", "ice": "none"}, "athanor_rig_asteroid"),
    ({"asteroid": "none", "moon": None, "ice": "none"}, "athanor_rig_moon"),
    ({"asteroid": "none", "moon": "none"}, "athanor_rig_ice"),
])
def test_an_invalid_athanor_rig_fails_loudly(rigs, match):
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match=match):
        facility_repo.facility_from_choice(facility_type="athanor", rig="none", athanor_rigs=rigs,
                                           security_band="high", tax=0.0, npc_station_id=None,
                                           npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data())


def test_a_rig_the_sde_does_not_give_its_class_raises_with_the_hint():
    data = fakes.fake_ore_data()
    broken = ore.OreData(compressed_by_base={}, base_by_compressed={}, skill_by_type={},
                         refining_yield_bonus=data.refining_yield_bonus, facility_attributes=data.facility_attributes,
                         rig_ore_classes={**data.rig_ore_classes, 46637: frozenset({"ice"})})
    with pytest.raises(industry.SdeIndustryDataMissing, match="46637.*moon.*re-run scripts/import_sde.py"):
        facility_repo.facility_from_choice(facility_type="athanor", rig="none",
                                           athanor_rigs={"asteroid": "none", "moon": "t1", "ice": "none"},
                                           security_band="high", tax=0.0, npc_station_id=None,
                                           npc_station=fakes.fake_npc_station, data=broken)


@pytest.mark.parametrize("covered", [frozenset(), frozenset({"asteroid", "ice"})])
def test_a_tatara_monitor_the_sde_gives_not_every_class_raises_with_the_hint(covered):
    """H1: the L-Set monitor covers all three classes; dogma that gives it
    none, or not all, is missing SDE data, not an unrigged moon ore."""
    data = fakes.fake_ore_data()
    broken = ore.OreData(compressed_by_base={}, base_by_compressed={}, skill_by_type={},
                         refining_yield_bonus=data.refining_yield_bonus, facility_attributes=data.facility_attributes,
                         rig_ore_classes={**data.rig_ore_classes, 46639: covered})
    match = "46639.*" + ("asteroid" if not covered else "moon") + ".*re-run scripts/import_sde.py"
    with pytest.raises(industry.SdeIndustryDataMissing, match=match):
        facility_repo.facility_from_choice(facility_type="tatara", rig="t1", security_band="high", tax=0.0,
                                           npc_station_id=None, npc_station=fakes.fake_npc_station, data=broken)


def test_the_unrigged_base_of_each_class_is_the_reprocessing_services_attribute():
    data = fakes.fake_ore_data()
    service = dict(data.facility_attributes[ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID])
    service.pop(2448)
    broken = ore.OreData(compressed_by_base={}, base_by_compressed={}, skill_by_type={},
                         refining_yield_bonus=data.refining_yield_bonus,
                         facility_attributes={**data.facility_attributes, ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID: service},
                         rig_ore_classes=data.rig_ore_classes)
    with pytest.raises(industry.SdeIndustryDataMissing, match="attribute 2448 of type 35899"):
        facility_repo.facility_from_choice(facility_type="athanor", rig="none",
                                           athanor_rigs={"asteroid": "none", "moon": "none", "ice": "none"},
                                           security_band="high", tax=0.0, npc_station_id=None,
                                           npc_station=fakes.fake_npc_station, data=broken)


# --- the settings ------------------------------------------------------------------------------

def test_the_schema_has_an_athanor_rig_per_class_and_the_tatara_monitor():
    settings = ADMIN_SETTINGS_SCHEMA["reprocessing"]["settings"]
    for key in ("athanor_rig_asteroid", "athanor_rig_moon", "athanor_rig_ice", "rig"):
        assert settings[key]["strict"] and settings[key]["default"] == "none"
        assert [o["value"] for o in settings[key]["options"]] == list(reprocessing.RIGS)
    assert "Tatara" in settings["rig"]["label"]
    assert "Moon" in settings["athanor_rig_moon"]["help"] and "Ice" in settings["athanor_rig_ice"]["help"]


def test_the_configured_athanor_reads_its_rig_per_class(tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set_bulk({"reprocessing": {"facility_type": "athanor", "athanor_rig_asteroid": "t2", "security_band": "null",
                                   "tax": 0.02}})
    facility = facility_repo.configured_facility(mgr, None, fakes.fake_ore_data())
    assert _percents(facility) == {"Veldspar": 87.62, "Bitumens": 73.81, "Clear Icicle": 73.81}


def test_a_stored_single_rig_on_an_athanor_migrates_to_the_asteroid_rig(tmp_path, caplog):
    """PR #14's one `rig` on an Athanor priced the M-Set Asteroid Ore Grading
    Processor for every ore. With no athanor_rig_* stored it is read as the
    asteroid rig (moon ore and ice unrigged until set), with a WARNING."""
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"reprocessing": {"facility_type": "athanor", "rig": "t2", "security_band": "null",
                                                 "tax": 0.02}}), encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        mgr = AdminSettingsManager(path)
    assert mgr.get("reprocessing", "athanor_rig_asteroid") == "t2"
    assert mgr.get("reprocessing", "athanor_rig_moon") == "none"
    warning = next(r.getMessage() for r in caplog.records if "athanor_rig_asteroid" in r.getMessage())
    assert "reprocessing/rig" in warning and "moon" in warning
    facility = facility_repo.configured_facility(mgr, None, fakes.fake_ore_data())
    assert _percents(facility) == {"Veldspar": 87.62, "Bitumens": 73.81, "Clear Icicle": 73.81}
    assert json.loads(path.read_text(encoding="utf-8"))["reprocessing"] == {
        "facility_type": "athanor", "rig": "t2", "security_band": "null", "tax": 0.02}   # the file is not rewritten


@pytest.mark.parametrize("stored", [
    {"facility_type": "athanor", "rig": "t2", "athanor_rig_moon": "t1"},   # a per-class rig is set: no migration
    {"facility_type": "tatara", "rig": "t2"},                               # the Tatara's own monitor
    {"facility_type": "athanor", "rig": "none"},
])
def test_no_migration_otherwise(tmp_path, stored):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"reprocessing": stored}), encoding="utf-8")
    mgr = AdminSettingsManager(path)
    assert mgr.get("reprocessing", "athanor_rig_asteroid") == "none"


def test_the_facility_payload_lists_each_class(tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set_bulk({"reprocessing": {"facility_type": "athanor", "athanor_rig_ice": "t1", "security_band": "low",
                                   "tax": 0.02}})
    (payload,) = facility_repo.get_all_facilities(mgr, None, data=fakes.fake_ore_data())
    assert payload["athanor_rigs"] == {"asteroid": "none", "moon": "none", "ice": "t1"}
    assert payload["by_ore_class"]["ice"] == {"rig": "t1", "base_yield": 0.51, "security_multiplier": 1.06,
                                              "multiplier": pytest.approx(0.51 * 1.06 * 1.02)}
    assert payload["by_ore_class"]["asteroid"]["multiplier"] == pytest.approx(0.5 * 1.02)


# --- the local SDE --------------------------------------------------------------------------

def test_the_local_sde_rigs_affect_the_classes_their_names_say():
    data = fakes.local_ore_data_or_skip()
    for rig, classes in {46633: {"asteroid"}, 46634: {"asteroid"}, 46635: {"ice"}, 46636: {"ice"},
                         46637: {"moon"}, 46638: {"moon"}, 46639: {"asteroid", "moon", "ice"},
                         46640: {"asteroid", "moon", "ice"}}.items():
        assert data.rig_ore_classes[rig] == frozenset(classes), rig
        attrs = data.facility_attributes[rig]
        assert attrs[ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID] in (0.51, 0.53)
    service = data.facility_attributes[ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID]
    assert [service[a] for a in (2444, 2445, 2448)] == [0.5, 0.5, 0.5]


def test_the_local_sde_ore_groups_match_the_classes():
    """The class constants are SDE group ids: the moon groups are the ones
    whose ores use a moon ore processing skill, ice the Ice Processing
    group, and every other reprocessable asteroid-category group an
    asteroid ore."""
    import sqlite3

    import os
    fakes.skip_without_local_sde()
    conn = sqlite3.connect(f"file:{fakes.LOCAL_SDE}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            'SELECT g.id, json_extract(g.name, \'$.en\'), json_extract(s.name, \'$.en\') '
            'FROM types t JOIN groups g ON g.id = t."groupID" JOIN "typeDogma" d ON d.id = t.id '
            'JOIN json_each(d."dogmaAttributes") a ON json_extract(a.value, \'$.attributeID\') = 790 '
            'JOIN types s ON s.id = CAST(json_extract(a.value, \'$.value\') AS INTEGER) '
            'WHERE g."categoryID" = 25 AND t.id IN (SELECT id FROM "typeMaterials") GROUP BY g.id, s.id').fetchall()
    finally:
        conn.close()
    assert rows
    for group_id, group_name, skill_name in rows:
        cls = ore.ore_class(group_id, 25)
        expected = "moon" if "Moon Ore" in skill_name else "ice" if skill_name == "Ice Processing" else "asteroid"
        assert cls == expected, (group_id, group_name, skill_name)
