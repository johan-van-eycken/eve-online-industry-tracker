"""Engineering-rig coverage by SDE category/group (Task 11, FU-14; SDE update).

EVE applies a manufacturing rig by the product's category and group (CCP's
industry target filters, read from the SDE's industryTargetFilters and
industryModifierSources into a domain.rig_groups.RigCatalog), never by its
name. The producer (IndustryService) and the daily planner (facility_bonus)
use the one classification. The classification tests run twice: on the
small fake SDE tables (tests/sde_industry_fakes.py) and on the local SDE
(database/eve_sde.db, read-only; skipped when it or its industry tables are
absent). Public SDE ids only.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

import sde_industry_fakes as fakes
from eve_online_industry_tracker.domain import rig_groups as rg
from eve_online_industry_tracker.infrastructure.sde import industry

from sde_industry_fakes import LOCAL_SDE as _LOCAL_SDE
_needs_sde = pytest.mark.needs_local_sde


@pytest.fixture(params=["fake_sde", "local_sde"])
def catalog(request):
    """The rig catalog the producer and planner read: the fake tables, then the
    local SDE's (installed for the test, so the producer reads it too)."""
    if request.param == "local_sde":
        industry.install_industry_data(fakes.local_industry_data_or_skip())
    return industry.rig_catalog()


def _groups(entry, activity="manufacturing"):
    return rg.rig_groups_for_entry(entry, activity, catalog=industry.rig_catalog())


def _primary(entry):
    return rg.primary_group(entry, catalog=industry.rig_catalog())


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

def test_a_fighter_is_covered_by_the_drone_and_fighter_rig(catalog):
    """Group 1652 "Light Fighter", category 87 "Fighter" (e.g. Templar I)."""
    fighter = _entry(1652, "Light Fighter", 87, "Fighter", meta_group_name="Tech I")
    assert _groups(fighter) == {"Drones"}
    assert _primary(fighter) == "Drones"
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
def test_equipment_the_name_tokens_missed_gets_the_equipment_rig(entry, catalog):
    assert _groups(entry) == {"Modules"}
    assert _producer()._infer_manufacturing_group(entry) == "Modules"


def test_a_missile_launcher_module_gets_the_equipment_rig_never_the_ammo_rig(catalog):
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
    assert _groups(_entry(385, "Heavy Missile", 8, "Charge")) == {"Ammo & Charges"}


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
def test_ship_classes_map_to_the_right_ship_rig(group_id, group_name, category_id, expected, catalog):
    category_name = {6: "Ship", 32: "Subsystem"}[category_id]
    assert _groups(_entry(group_id, group_name, category_id, category_name)) == expected


def test_faction_ships_follow_their_group_not_their_meta_group(catalog):
    """A faction frigate (group 25, meta Faction) is a Basic Small Ship."""
    entry = _entry(25, "Frigate", 6, "Ship", meta_group_name="Faction")
    assert _primary(entry) == "Basic Small Ships"


def test_a_titan_gets_only_the_xl_ship_rig(catalog):
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


# The facility gate (capital and supercapital hulls by the SDE's service-module
# fitting data) is tested in tests/test_rig_values_and_security.py.


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
    (_entry(1194, "Special Edition Commodities", 63, "Special Edition Assets"), frozenset()),
])
def test_components_and_structures(entry, expected, catalog):
    assert _groups(entry) == expected


def test_names_alone_classify_when_they_say_enough(catalog):
    group = _groups
    assert group({"group_name": "Smart Bomb", "category_name": "Module"}) == {"Modules"}
    assert group({"group_name": "Frigate"}) == {"Basic Small Ships", "All Ships"}
    assert group({"category_name": "Module"}) == {"Modules"}
    # Not enough: no group or category at all, a group name alone outside the
    # filters, or a Commodity whose group is unknown.
    assert group({"type_id": 1}) is None
    assert group({"group_name": "Smart Bomb"}) is None
    assert group({"category_name": "Commodity"}) is None


@pytest.mark.parametrize("entry", [
    {"group_name": "Fregatte", "category_name": "Schiff"},           # localized (de)
    {"group_name": "Smart Bomb", "category_name": "Modul"},
    {"group_name": "Mutaplasmids", "category_name": "Commodity"},    # a group the table does not list
    {"group_name": "Spezialausgabe", "category_name": "Special Edition Assets"},
])
def test_a_name_the_table_cannot_place_is_unclassified_with_a_warning(entry, caplog, catalog):
    """The SDE is localized: an English-only lookup can miss. A miss is never
    read as "outside every filter"; the entry is unclassified (All rigs only)."""
    rg._warned_names.clear()
    with caplog.at_level("WARNING"):
        assert _groups(entry) is None
    assert any("name" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records)


def test_a_missed_name_is_warned_once(caplog, catalog):
    rg._warned_names.clear()
    with caplog.at_level("WARNING"):
        for _ in range(3):
            _groups({"group_name": "Fregatte", "category_name": "Schiff"})
    assert len([r for r in caplog.records if "Schiff" in r.getMessage()]) == 1


def test_an_unclassifiable_product_gets_all_rigs_only_and_is_logged(caplog, catalog):
    S = _producer()
    S._unclassified_rig_group_warned.clear()
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


def _multi_rig_profile(*groups, value=0.02):
    """Rigs as the UI saves them: per-effect entries plus the aggregate
    structure_rig_*_bonus keys (industry_profiles.py)."""
    profile = {"material_efficiency_bonus": 0.01, "structure_rig_material_bonus": 0.05,
               "structure_rig_time_bonus": 0.2, "structure_rig_cost_bonus": 0.0, "structure_rigs": [
                   {"type_id": i + 1, "effects": [
                       {"activity": "manufacturing", "metric": "material", "group": g, "value": value}]}
                   for i, g in enumerate(groups)]}
    return profile


_PARITY_PROFILES = (
    [(label,) for label in sorted(fakes.fake_industry_data().rig_catalog.labels | {"All"})]
    + [("All", "Modules"), ("All", "Ammo & Charges"), ("Ammo & Charges", "Drones"),
       ("Modules", "Basic Small Ships"), ("Basic Medium Ships", "Advanced Medium Ships"),
       ("Capital Ships", "All Ships")]
)


@pytest.mark.parametrize("entry", _AGREEMENT_ENTRIES)
@pytest.mark.parametrize("rig_groups", _PARITY_PROFILES, ids=" + ".join)
def test_the_producer_and_the_planner_give_the_same_rig_reduction(entry, rig_groups, catalog):
    from eve_online_industry_tracker.application.daily_planner import facility_bonus

    S = _producer()
    profile = _multi_rig_profile(*rig_groups)
    producer = S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                        manufacturing_group=S._manufacturing_rig_groups(entry))
    planner = facility_bonus.sub_job_facility_reductions(profile, entry)
    assert planner["rig_material_reduction"] == producer
    assert planner["structure_material_reduction"] == S._profile_base_reduction(
        profile_payload=profile, activity="manufacturing", metric="material")


def test_a_launcher_with_only_an_ammo_rig_gets_no_rig_bonus_despite_the_aggregate(catalog):
    """The UI saves the combined rig bonus as structure_rig_material_bonus; it
    is not a fallback for a product no per-effect rig covers (an under-buy)."""
    from eve_online_industry_tracker.application.daily_planner import facility_bonus

    S = _producer()
    launcher = _entry(508, "Missile Launcher Heavy", 7, "Module")
    profile = _multi_rig_profile("Ammo & Charges")
    for metric in ("material", "time", "cost"):
        assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric=metric,
                                        manufacturing_group=S._manufacturing_rig_groups(launcher)) == 0.0
    assert facility_bonus.sub_job_facility_reductions(profile, launcher)["rig_material_reduction"] == 0.0


def test_two_rigs_covering_the_product_both_apply(catalog):
    S = _producer()
    frigate = S._manufacturing_rig_groups(_entry(25, "Frigate", 6, "Ship"))
    profile = _multi_rig_profile("Basic Small Ships", "All Ships", "Modules")
    assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                    manufacturing_group=frigate) == pytest.approx(1 - 0.98 * 0.98)


def test_the_aggregate_is_never_applied(catalog):
    """Task 15 M1 (supersedes Task 11 I1 / 11b M2): the aggregate names no
    rig, so a profile with no per-effect rigs gets no rig bonus, in any band."""
    S = _producer()
    groups = S._manufacturing_rig_groups(_entry(508, "Missile Launcher Heavy", 7, "Module"))
    aggregate_only = {"structure_rig_material_bonus": 0.05, "system_security_band": "low"}
    assert S._profile_rig_reduction(profile_payload=aggregate_only, activity="manufacturing", metric="material",
                                    manufacturing_group=groups) == 0.0
    no_effects = {"structure_rig_material_bonus": 0.05, "structure_rigs": [{"type_id": 1, "effects": []}]}
    assert S._profile_rig_reduction(profile_payload=no_effects, activity="manufacturing", metric="material",
                                    manufacturing_group=groups) == 0.0


# --- the catalog against CCP's filters --------------------------------------------

# label: (CCP filter id, categories, groups, material attribute, time attribute),
# in order of specificity ("All Ships" last). The catalog is read from the
# SDE; this pins what it reads (the table domain/rig_groups.py used to hold).
_EXPECTED_FILTERS = {
    "Modules": (2, {7, 20, 22}, {12, 340, 448, 649}, 2538, 2539),
    "Ammo & Charges": (4, {8}, set(), 2540, 2541),
    "Drones": (1, {18, 87}, set(), 2542, 2543),
    "Basic Small Ships": (5, set(), {25, 31, 420}, 2544, 2545),
    "Advanced Small Ships": (6, set(), {324, 541, 830, 831, 834, 893, 1283, 1305, 1527, 1534}, 2550, 2551),
    "Basic Medium Ships": (7, set(), {26, 28, 419, 463, 1201, 4902, 5087}, 2546, 2547),
    "Advanced Medium Ships": (8, {32}, {358, 380, 540, 543, 832, 833, 894, 906, 963, 1202, 1972, 4902}, 2552, 2553),
    "Basic Large Ships": (9, set(), {27, 513, 941}, 2548, 2549),
    "Advanced Large Ships": (10, set(), {898, 900, 902}, 2555, 2556),
    "Capital Ships": (11, set(), {485, 547, 883, 1538, 4594, 5120}, 2575, 2576),
    "Structures": (12, {23, 39, 40, 65, 66}, {536, 1136, 4736}, 2561, 2562),
    "Capital Components": (13, set(), {873}, 2559, 2560),
    "Advanced Components": (14, set(), {332, 334, 716, 964}, 2557, 2558),
    "Advanced Capital Components": (15, set(), {913}, 2658, 2659),
    "All Ships": (3, {6, 32}, set(), 2592, 2591),
}
_EXPECTED_REACTION_FILTERS = {
    "Hybrid Reactions": (16, set(), {974}, 2716, 2715),
    "Biochemical Reactions": (17, set(), {712, 4096}, 2720, 2719),
    "Composite Reactions": (18, set(), {428, 429, 4932}, 2718, 2717),
}


def _table(filters):
    return {f.label: (f.filter_id, set(f.category_ids), set(f.group_ids), f.material_attribute_id,
                      f.time_attribute_id) for f in filters}


def test_the_catalog_is_ccps_filters_in_order_of_specificity(catalog):
    assert _table(catalog.manufacturing_filters) == _EXPECTED_FILTERS
    assert [f.label for f in catalog.manufacturing_filters] == list(_EXPECTED_FILTERS)
    assert _table(catalog.reaction_filters) == _EXPECTED_REACTION_FILTERS
    assert catalog.labels == frozenset(_EXPECTED_FILTERS)
    assert catalog.reaction_labels == frozenset(_EXPECTED_REACTION_FILTERS)


def test_every_filtered_attribute_is_bound_to_its_filter_and_the_science_ones_to_all(catalog):
    bindings = catalog.attribute_bindings
    for activity, expected in (("manufacturing", _EXPECTED_FILTERS), ("reaction", _EXPECTED_REACTION_FILTERS)):
        for label, (filter_id, _, _, material, time) in expected.items():
            assert bindings[material] == rg.AttributeBinding(activity, "material", label, filter_id)
            assert bindings[time] == rg.AttributeBinding(activity, "time", label, filter_id)
    for attribute_id, (activity, metric) in rg.UNFILTERED_ATTRIBUTE_BINDINGS.items():
        assert bindings[attribute_id] == rg.AttributeBinding(activity, metric, "All", None)
    # A structure's own role bonuses (2600-2602, 2721) are no rig binding.
    assert not {2600, 2601, 2602, 2721} & set(bindings)


def test_the_catalog_names_come_from_the_sde_groups_and_categories(catalog):
    for f in (*catalog.manufacturing_filters, *catalog.reaction_filters):
        assert f.category_ids <= set(catalog.category_names)
        assert f.group_ids <= set(catalog.groups)
    for group_id, (category_id, name) in catalog.groups.items():
        assert catalog.category_names[category_id] and name
    assert catalog.groups[4902] == (6, "Expedition Command Ship")
    assert catalog.category_names[87] == "Fighter"


def _catalog_from(target_filters, modifier_sources):
    return rg.build_rig_catalog(target_filters, modifier_sources, groups=fakes.GROUPS,
                                categories=fakes.CATEGORIES)


def test_a_filter_the_sde_adds_gets_its_sde_name_and_a_warning(caplog):
    target_filters = {**fakes.TARGET_FILTERS, 19: {"groupIDs": [4999], "name": "New Ship Class"}}
    sources = {**fakes.MODIFIER_SOURCES, 990019: {"manufacturing": {
        "material": [{"dogmaAttributeID": 2900, "filterID": 19}], "time": [{"dogmaAttributeID": 2901, "filterID": 19}]}}}
    with caplog.at_level("WARNING"):
        catalog = _catalog_from(target_filters, sources)
    labels = [f.label for f in catalog.manufacturing_filters]
    assert "New Ship Class" in labels and labels[-1] == "All Ships"
    assert catalog.attribute_bindings[2900] == rg.AttributeBinding("manufacturing", "material", "New Ship Class", 19)
    assert any("19" in r.getMessage() and "New Ship Class" in r.getMessage() for r in caplog.records
               if r.levelname == "WARNING")


def test_a_label_whose_filter_the_sde_dropped_is_warned(caplog):
    sources = {k: v for k, v in fakes.MODIFIER_SOURCES.items() if k != fakes.FAKE_RIG_TYPE_BASE + 13}
    with caplog.at_level("WARNING"):
        catalog = _catalog_from(fakes.TARGET_FILTERS, sources)
    assert "Capital Components" not in catalog.labels
    assert any("Capital Components" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")


def test_a_binding_to_a_filter_the_sde_lacks_is_an_error():
    sources = {990777: {"manufacturing": {"material": [{"dogmaAttributeID": 2538, "filterID": 77}],
                                          "time": [{"dogmaAttributeID": 2539, "filterID": 77}]}}}
    with pytest.raises(ValueError, match="77"):
        _catalog_from(fakes.TARGET_FILTERS, sources)


def test_a_filter_with_two_material_attributes_is_an_error():
    sources = {**fakes.MODIFIER_SOURCES, 990777: {"manufacturing": {
        "material": [{"dogmaAttributeID": 2999, "filterID": 2}]}}}
    with pytest.raises(ValueError, match="filter 2"):
        _catalog_from(fakes.TARGET_FILTERS, sources)


# --- the catalog against the local SDE -------------------------------------------

def _sde():
    return sqlite3.connect(f"file:{_LOCAL_SDE}?mode=ro", uri=True)


def _en(raw):
    try:
        return json.loads(raw).get("en")
    except (TypeError, ValueError, AttributeError):
        return raw


@_needs_sde
def test_each_rig_effects_label_names_the_filter_of_the_attribute_it_modifies():
    """The producer labels a rig effect by the structure attribute it
    modifies (the catalog's attribute_bindings, read in rig_effects from the
    effect's modifierInfo). Every manufacturing and reaction rig effect in
    the SDE modifies a bound attribute whose SDE description names its
    targets."""
    bindings_by_attribute = fakes.local_industry_data_or_skip().rig_catalog.attribute_bindings
    checked = 0
    with _sde() as conn:
        rows = conn.execute(
            "SELECT name, modifierInfo FROM dogmaEffects WHERE name LIKE 'rig%Manufacture%' "
            "OR name LIKE 'rigReaction%'").fetchall()
        descriptions = {r[0]: r[1] or "" for r in conn.execute("SELECT id, description FROM dogmaAttributes")}
    for name, modifier_info in rows:
        attribute_ids = {m["modifiedAttributeID"] for m in json.loads(modifier_info)}
        bindings = {bindings_by_attribute[a] for a in attribute_ids}
        assert len(bindings) == 1, name
        (binding,) = bindings
        assert binding.activity == ("reaction" if name.startswith("rigReaction") else "manufacturing"), name
        assert binding.metric == ("material" if ("Material" in name or "Mat" in name) else "time"), name
        for a in attribute_ids:
            text = descriptions[a].lower()
            assert ("manufacturing the following" in text or "all ships" in text or "reaction" in text), name
        checked += 1
    assert checked >= 36


@_needs_sde
def test_the_drone_rig_attribute_names_fighters():
    """Rig 37156 "Standup M-Set Drone and Fighter Manufacturing Material
    Efficiency I" modifies attribute 2542, whose SDE description lists
    "Drones, Fighters"; the Drones filter holds categories 18 and 87."""
    catalog = fakes.local_industry_data_or_skip().rig_catalog
    with _sde() as conn:
        (description,) = conn.execute("SELECT description FROM dogmaAttributes WHERE id = 2542").fetchone()
        (effects,) = conn.execute("SELECT dogmaEffects FROM typeDogma WHERE id = 37156").fetchone()
        (modifier_info,) = conn.execute("SELECT modifierInfo FROM dogmaEffects WHERE id = 6810").fetchone()
    assert "Drones, Fighters" in description
    assert 6810 in {e["effectID"] for e in json.loads(effects)}
    assert json.loads(modifier_info)[0]["modifiedAttributeID"] == 2542
    drones = next(f for f in catalog.manufacturing_filters if f.label == "Drones")
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
    catalog = fakes.local_industry_data_or_skip().rig_catalog
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
            covered = rg.rig_groups_for_entry(entry, catalog=catalog)
            # Names alone give the same answer as ids, or none at all.
            assert rg.rig_groups_for_entry(
                {"group_name": group_name, "category_name": categories[category_id]}, catalog=catalog) in (
                covered, None)
            if covered is None:
                unclassified.append(product["typeID"])
            elif not covered:
                uncovered.add(group_id)
    assert total > 4000
    assert unclassified == []
    assert uncovered == _KNOWN_UNCOVERED_GROUPS
