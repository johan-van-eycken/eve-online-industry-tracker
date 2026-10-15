"""Engineering-rig coverage by SDE category/group (Task 11, FU-14).

EVE applies a manufacturing rig by the product's category and group (the
industry target filters, domain/rig_groups.py), never by its name. The
producer (IndustryService) and the daily planner (facility_bonus) use the
one classification. Public SDE ids only; the SDE tests read the local
database/eve_sde.db read-only and skip when it is absent.
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest

from eve_online_industry_tracker.domain import rig_groups as rg

_LOCAL_SDE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db"))
_needs_sde = pytest.mark.skipif(not os.path.exists(_LOCAL_SDE), reason="local SDE database/eve_sde.db not present")


def _entry(group_id, group_name, category_id, category_name, **extra):
    return {"type_id": 1, "group_id": group_id, "group_name": group_name,
            "category_id": category_id, "category_name": category_name, **extra}


def _producer():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    return IndustryService


def _profile(group, value=0.02, structure=0.01):
    return {"material_efficiency_bonus": structure, "structure_rigs": [{"type_id": 1, "effects": [
        {"activity": "manufacturing", "metric": "material", "group": group, "value": value}]}]}


# --- classification -----------------------------------------------------------

def test_a_fighter_is_covered_by_the_drone_and_fighter_rig():
    """Group 1652 "Light Fighter", category 87 "Fighter" (e.g. Templar I)."""
    fighter = _entry(1652, "Light Fighter", 87, "Fighter", meta_group_name="Tech I")
    assert rg.rig_groups_for_entry(fighter) == {"Drones"}
    assert rg.primary_group(fighter) == "Drones"
    assert _producer()._profile_rig_reduction(
        profile_payload=_profile("Drones"), activity="manufacturing", metric="material",
        manufacturing_group=_producer()._manufacturing_rig_groups(fighter)) == pytest.approx(0.02)


@pytest.mark.parametrize("entry", [
    _entry(763, "Nanofiber Internal Structure", 7, "Module"),  # old token: "structure"
    _entry(72, "Smart Bomb", 7, "Module"),                      # old token: "bomb"
    _entry(645, "Drone Damage Modules", 7, "Module"),           # old token: "drone"
    _entry(773, "Rig Armor", 7, "Module"),
    _entry(300, "Cyberimplant", 20, "Implant"),
    _entry(1246, "Mobile Depot", 22, "Deployable"),
    _entry(340, "Secure Cargo Container", 2, "Celestial"),
])
def test_equipment_the_name_tokens_missed_gets_the_equipment_rig(entry):
    assert rg.rig_groups_for_entry(entry) == {"Modules"}
    assert _producer()._infer_manufacturing_group(entry) == "Modules"


def test_a_missile_launcher_module_gets_the_equipment_rig_never_the_ammo_rig():
    """Group 508 "Missile Launcher Heavy" is a Module (category 7)."""
    S = _producer()
    launcher = _entry(508, "Missile Launcher Heavy", 7, "Module")
    groups = S._manufacturing_rig_groups(launcher)
    assert groups == {"Modules"}
    ammo = S._profile_rig_reduction(profile_payload=_profile("Ammo & Charges"), activity="manufacturing",
                                    metric="material", manufacturing_group=groups)
    equipment = S._profile_rig_reduction(profile_payload=_profile("Modules"), activity="manufacturing",
                                         metric="material", manufacturing_group=groups)
    assert (ammo, equipment) == (0.0, pytest.approx(0.02))
    # A real missile is a Charge (category 8).
    assert rg.rig_groups_for_entry(_entry(385, "Heavy Missile", 8, "Charge")) == {"Ammo & Charges"}


@pytest.mark.parametrize("group_id, group_name, category_id, expected", [
    (25, "Frigate", 6, {"Basic Small Ships", "All Ships"}),
    (31, "Shuttle", 6, {"Basic Small Ships", "All Ships"}),
    (420, "Destroyer", 6, {"Basic Small Ships", "All Ships"}),
    (324, "Assault Frigate", 6, {"Advanced Small Ships", "All Ships"}),
    (1305, "Tactical Destroyer", 6, {"Advanced Small Ships", "All Ships"}),
    (26, "Cruiser", 6, {"Basic Medium Ships", "All Ships"}),
    (463, "Mining Barge", 6, {"Basic Medium Ships", "All Ships"}),
    (28, "Hauler", 6, {"Basic Medium Ships", "All Ships"}),
    (358, "Heavy Assault Cruiser", 6, {"Advanced Medium Ships", "All Ships"}),
    (963, "Strategic Cruiser", 6, {"Advanced Medium Ships", "All Ships"}),
    (543, "Exhumer", 6, {"Advanced Medium Ships", "All Ships"}),
    (4902, "Expedition Command Ship", 6, {"Basic Medium Ships", "Advanced Medium Ships", "All Ships"}),
    (27, "Battleship", 6, {"Basic Large Ships", "All Ships"}),
    (513, "Freighter", 6, {"Basic Large Ships", "All Ships"}),
    (941, "Industrial Command Ship", 6, {"Basic Large Ships", "All Ships"}),
    (900, "Marauder", 6, {"Advanced Large Ships", "All Ships"}),
    (902, "Jump Freighter", 6, {"Advanced Large Ships", "All Ships"}),
    (485, "Dreadnought", 6, {"Capital Ships", "All Ships"}),
    (883, "Capital Industrial Ship", 6, {"Capital Ships", "All Ships"}),
    (30, "Titan", 6, {"All Ships"}),
    (659, "Supercarrier", 6, {"All Ships"}),
    (956, "Offensive Subsystem", 32, {"Advanced Medium Ships", "All Ships"}),
])
def test_ship_classes_map_to_the_right_ship_rig(group_id, group_name, category_id, expected):
    category_name = {6: "Ship", 32: "Subsystem"}[category_id]
    assert rg.rig_groups_for_entry(_entry(group_id, group_name, category_id, category_name)) == expected


def test_faction_ships_follow_their_group_not_their_meta_group():
    """A faction frigate (group 25, meta Faction) is a Basic Small Ship."""
    entry = _entry(25, "Frigate", 6, "Ship", meta_group_name="Faction")
    assert rg.primary_group(entry) == "Basic Small Ships"


def test_a_titan_gets_only_the_xl_ship_rig():
    """The Capital Ships filter does not name Titans (group 30): only the XL
    "All Ships" rig covers them."""
    S = _producer()
    titan = _entry(30, "Titan", 6, "Ship")
    assert S._infer_manufacturing_group(titan) == "All Ships"
    assert S._profile_rig_reduction(profile_payload=_profile("Capital Ships"), activity="manufacturing",
                                    metric="material", manufacturing_group=S._manufacturing_rig_groups(titan)) == 0.0
    assert S._profile_rig_reduction(profile_payload=_profile("All Ships"), activity="manufacturing",
                                    metric="material", manufacturing_group=S._manufacturing_rig_groups(titan)) == (
        pytest.approx(0.02))


@pytest.mark.parametrize("group_id, group_name", [(30, "Titan"), (659, "Supercarrier"), (513, "Freighter"),
                                                  (902, "Jump Freighter"), (485, "Dreadnought")])
def test_capital_hulls_stay_out_of_a_raitaru(group_id, group_name):
    """The facility gate is not a rig group: a Freighter's rig group is Basic
    Large Ships, but it is still built in an Azbel or Sotiyo only."""
    S = _producer()
    entry = _entry(group_id, group_name, 6, "Ship")
    label = S._infer_manufacturing_group(entry)
    raitaru, azbel = {"structure_type_id": 35825}, {"structure_type_id": 35826}
    assert not S._facility_can_manufacture_group(manufacturing_group=label, profile_payload=raitaru, group_id=group_id)
    assert S._facility_can_manufacture_group(manufacturing_group=label, profile_payload=azbel, group_id=group_id)
    # A Battleship is not capital.
    assert S._facility_can_manufacture_group(manufacturing_group="Basic Large Ships", profile_payload=raitaru, group_id=27)


@pytest.mark.parametrize("entry, expected", [
    (_entry(334, "Construction Components", 17, "Commodity"), {"Advanced Components"}),
    (_entry(964, "Hybrid Tech Components", 17, "Commodity"), {"Advanced Components"}),
    (_entry(332, "Tool", 17, "Commodity"), {"Advanced Components"}),
    (_entry(873, "Capital Construction Components", 17, "Commodity"), {"Capital Components"}),
    (_entry(913, "Advanced Capital Construction Components", 17, "Commodity"), {"Advanced Capital Components"}),
    (_entry(536, "Structure Components", 17, "Commodity"), {"Structures"}),
    (_entry(1136, "Fuel Block", 4, "Material"), {"Structures"}),
    (_entry(1657, "Citadel", 65, "Structure"), {"Structures"}),
    (_entry(1415, "Structure Engineering Service Module", 66, "Structure Module"), {"Structures"}),
    (_entry(365, "Control Tower", 23, "Starbase"), {"Structures"}),
    (_entry(4768, "Sovereignty Hub Site Detection Upgrades", 39, "Infrastructure Upgrades"), {"Structures"}),
    (_entry(1964, "Mutaplasmids", 17, "Commodity"), frozenset()),
    (_entry(1676, "Named Components", 4, "Material"), frozenset()),
])
def test_components_and_structures(entry, expected):
    assert rg.rig_groups_for_entry(entry) == expected


def test_names_alone_classify_when_they_say_enough():
    group = rg.rig_groups_for_entry
    assert group({"group_name": "Smart Bomb", "category_name": "Module"}) == {"Modules"}
    assert group({"group_name": "Frigate"}) == {"Basic Small Ships", "All Ships"}
    assert group({"group_name": "Mutaplasmids", "category_name": "Commodity"}) == frozenset()
    # Not enough: no group or category at all, a group name alone outside the
    # filters, or a Commodity whose group is unknown.
    assert group({"type_id": 1}) is None
    assert group({"group_name": "Smart Bomb"}) is None
    assert group({"category_name": "Commodity"}) is None
    assert group({"category_name": "Module"}) == {"Modules"}


def test_an_unclassifiable_product_gets_all_rigs_only_and_is_logged(caplog):
    S = _producer()
    with caplog.at_level("WARNING"):
        groups = S._manufacturing_rig_groups({"type_id": 4242, "type_name": "Mystery"})
    assert groups == frozenset()
    assert any("rig group" in r.getMessage() and "4242" in r.getMessage() for r in caplog.records)
    profile = _profile("All", value=0.01)
    profile["structure_rigs"].append(_profile("Modules")["structure_rigs"][0])
    assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                    manufacturing_group=groups) == pytest.approx(0.01)


# --- producer and planner agree ------------------------------------------------

_AGREEMENT_ENTRIES = [
    _entry(1652, "Light Fighter", 87, "Fighter"),
    _entry(508, "Missile Launcher Heavy", 7, "Module"),
    _entry(763, "Nanofiber Internal Structure", 7, "Module"),
    _entry(385, "Heavy Missile", 8, "Charge"),
    _entry(25, "Frigate", 6, "Ship"),
    _entry(4902, "Expedition Command Ship", 6, "Ship"),
    _entry(30, "Titan", 6, "Ship"),
    _entry(956, "Offensive Subsystem", 32, "Subsystem"),
    _entry(334, "Construction Components", 17, "Commodity"),
    _entry(1136, "Fuel Block", 4, "Material"),
    _entry(1964, "Mutaplasmids", 17, "Commodity"),
    {"type_id": 4242},
]


@pytest.mark.parametrize("entry", _AGREEMENT_ENTRIES)
@pytest.mark.parametrize("rig_group", sorted(rg.ALL_LABELS | {"All"}))
def test_the_producer_and_the_planner_give_the_same_rig_reduction(entry, rig_group):
    from eve_online_industry_tracker.application.daily_planner import facility_bonus

    S = _producer()
    profile = _profile(rig_group)
    producer = S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                        manufacturing_group=S._manufacturing_rig_groups(entry))
    planner = facility_bonus.sub_job_facility_reductions(profile, entry)
    assert planner["rig_material_reduction"] == producer
    assert planner["structure_material_reduction"] == S._profile_base_reduction(
        profile_payload=profile, activity="manufacturing", metric="material")


# --- the table against the SDE ---------------------------------------------------

def _sde():
    return sqlite3.connect(f"file:{_LOCAL_SDE}?mode=ro", uri=True)


def _en(raw):
    try:
        return json.loads(raw).get("en")
    except (TypeError, ValueError, AttributeError):
        return raw


@_needs_sde
def test_every_filter_id_and_name_is_the_sdes():
    with _sde() as conn:
        categories = {r[0]: _en(r[1]) for r in conn.execute("SELECT id, name FROM categories")}
        groups = {r[0]: (r[1], _en(r[2])) for r in conn.execute("SELECT id, categoryID, name FROM groups")}
    for category_id, name in rg.CATEGORY_NAMES.items():
        assert categories[category_id] == name
    for group_id, (category_id, name) in rg.GROUPS.items():
        assert groups[group_id] == (category_id, name)
    for f in rg.RIG_TARGET_FILTERS:
        assert f.category_ids <= set(rg.CATEGORY_NAMES)
        assert f.group_ids <= set(rg.GROUPS)


@_needs_sde
def test_each_rig_effects_label_names_the_filter_of_the_attribute_it_modifies():
    """The producer labels a rig effect from its name (rig_effects); EVE binds
    the structure attribute the effect modifies to a target filter. They agree
    for every manufacturing rig effect in the SDE."""
    from eve_online_industry_tracker.infrastructure.sde.rig_effects import (
        _RIG_GROUP_TOKEN_LABELS, _parse_rig_effect)

    by_attribute = {}
    for f in rg.RIG_TARGET_FILTERS:
        by_attribute[f.material_attribute_id] = f.label
        by_attribute[f.time_attribute_id] = f.label
    checked = 0
    with _sde() as conn:
        rows = conn.execute("SELECT name, modifierInfo FROM dogmaEffects WHERE name LIKE 'rig%Manufacture%'").fetchall()
        descriptions = {r[0]: r[1] or "" for r in conn.execute("SELECT id, description FROM dogmaAttributes")}
    for name, modifier_info in rows:
        activity, token, metric = _parse_rig_effect(name)
        if activity != "manufacturing" or metric not in ("material", "time"):
            continue
        label = _RIG_GROUP_TOKEN_LABELS.get(token)
        attribute_ids = {m["modifiedAttributeID"] for m in json.loads(modifier_info)}
        assert {by_attribute.get(a) for a in attribute_ids} == {label}, name
        assert all("manufacturing the following" in descriptions[a] or "all ships" in descriptions[a].lower()
                   for a in attribute_ids), name
        checked += 1
    assert checked >= 30


@_needs_sde
def test_the_drone_rig_attribute_names_fighters():
    """Rig 37156 "Standup M-Set Drone and Fighter Manufacturing Material
    Efficiency I" modifies attribute 2542, whose SDE description lists
    "Drones, Fighters"; the Drones filter holds categories 18 and 87."""
    with _sde() as conn:
        (description,) = conn.execute("SELECT description FROM dogmaAttributes WHERE id = 2542").fetchone()
        (effects,) = conn.execute("SELECT dogmaEffects FROM typeDogma WHERE id = 37156").fetchone()
        (modifier_info,) = conn.execute("SELECT modifierInfo FROM dogmaEffects WHERE id = 6810").fetchone()
    assert "Drones, Fighters" in description
    assert 6810 in {e["effectID"] for e in json.loads(effects)}
    assert json.loads(modifier_info)[0]["modifiedAttributeID"] == 2542
    drones = next(f for f in rg.RIG_TARGET_FILTERS if f.label == "Drones")
    assert drones.material_attribute_id == 2542 and drones.category_ids == {18, 87}


# Product groups no engineering rig covers in EVE (empty rig groups, "All"
# rigs only). Every other manufacturable product has at least one.
_KNOWN_UNCOVERED_GROUPS = {
    314,   # Miscellaneous (Commodity)
    526,   # Commodities
    1314,  # Unknown Components
    1964,  # Mutaplasmids
    4041, 4072, 4145, 4776,  # Jump / Warp Matrix / Abyssal filaments
    1106,  # Orbital Construction Platform (Orbitals)
    1194,  # Special Edition Commodities
}


@_needs_sde
def test_every_manufacturable_product_in_the_sde_classifies():
    with _sde() as conn:
        groups = {r[0]: (r[1], _en(r[2])) for r in conn.execute("SELECT id, categoryID, name FROM groups")}
        categories = {r[0]: _en(r[1]) for r in conn.execute("SELECT id, name FROM categories")}
        types = {r[0]: r[1] for r in conn.execute("SELECT id, groupID FROM types")}
        blueprints = conn.execute(
            "SELECT b.activities FROM blueprints b JOIN types t ON t.id = b.id WHERE t.published = 1").fetchall()
    unclassified, uncovered, total = [], set(), 0
    for (activities,) in blueprints:
        for product in (json.loads(activities or "{}").get("manufacturing") or {}).get("products") or []:
            group_id = types.get(product["typeID"])
            if group_id is None:
                continue
            category_id, group_name = groups[group_id]
            entry = _entry(group_id, group_name, category_id, categories[category_id])
            total += 1
            covered = rg.rig_groups_for_entry(entry)
            # Names alone give the same answer as ids.
            assert rg.rig_groups_for_entry(
                {"group_name": group_name, "category_name": categories[category_id]}) == covered
            if covered is None:
                unclassified.append(product["typeID"])
            elif not covered:
                uncovered.add(group_id)
    assert total > 4000
    assert unclassified == []
    assert uncovered == _KNOWN_UNCOVERED_GROUPS
