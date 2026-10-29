"""Small SDE industry tables for the unit tests, in the SDE's YAML shape.

The four tables are what CCP's SDE ships as industryTargetFilters,
industryModifierSources, industryAssemblyLines and
industryInstallationTypes (build 3607705 on), keyed by id the way
yaml.safe_load reads them. The target filters are CCP's 18 (public game
data: category and group ids); the modifier sources are made-up rig and
structure types (99xxxx) that bind each filter to its structure attributes;
the assembly lines are cut down to the hull groups the facility tests use.
The groups and categories are the SDE's English names for the ids the
filters name. tests/test_rig_groups.py and
tests/test_facility_gate_and_reactions.py pin the same answers against the
local SDE (database/eve_sde.db) when it is present.
"""
from __future__ import annotations

from functools import cache
from types import SimpleNamespace
from typing import Any, Callable

from sqlalchemy.orm import sessionmaker

TARGET_FILTERS: dict[int, dict[str, Any]] = {
    1: {"categoryIDs": [18, 87], "name": "Drones/Fighters"},
    2: {"categoryIDs": [7, 20, 22], "groupIDs": [12, 340, 448, 649], "name": "Equipment"},
    3: {"categoryIDs": [6, 32], "name": "Ships"},
    4: {"categoryIDs": [8], "name": "Charges"},
    5: {"groupIDs": [25, 31, 420], "name": "Small T1 Ships"},
    6: {"groupIDs": [324, 541, 830, 831, 834, 893, 1283, 1305, 1527, 1534], "name": "Small T2 Ships"},
    7: {"groupIDs": [26, 28, 419, 463, 1201, 4902, 5087], "name": "Medium T1 Ships"},
    8: {"categoryIDs": [32], "groupIDs": [358, 380, 540, 543, 832, 833, 894, 906, 963, 1202, 1972, 4902],
        "name": "Medium T2 Ships"},
    9: {"groupIDs": [27, 513, 941], "name": "Large T1 Ships"},
    10: {"groupIDs": [898, 900, 902], "name": "Large T2 Ships"},
    11: {"groupIDs": [485, 547, 883, 1538, 4594, 5120], "name": "Capital Ships"},
    12: {"categoryIDs": [23, 39, 40, 65, 66], "groupIDs": [536, 1136, 4736], "name": "Structures"},
    13: {"groupIDs": [873], "name": "Capital Components"},
    14: {"groupIDs": [332, 334, 716, 964], "name": "Components"},
    15: {"groupIDs": [913], "name": "Advanced Capital Components"},
    16: {"groupIDs": [974], "name": "Hybrid Reactions"},
    17: {"groupIDs": [712, 4096], "name": "Biochemical Reactions"},
    18: {"groupIDs": [428, 429, 4932], "name": "Composite Reactions"},
}

# filter id -> (material attribute, time attribute) of the structure.
FILTER_ATTRIBUTES: dict[int, tuple[int, int]] = {
    1: (2542, 2543), 2: (2538, 2539), 3: (2592, 2591), 4: (2540, 2541), 5: (2544, 2545),
    6: (2550, 2551), 7: (2546, 2547), 8: (2552, 2553), 9: (2548, 2549), 10: (2555, 2556),
    11: (2575, 2576), 12: (2561, 2562), 13: (2559, 2560), 14: (2557, 2558), 15: (2658, 2659),
    16: (2716, 2715), 17: (2720, 2719), 18: (2718, 2717),
}

FAKE_RIG_TYPE_BASE = 990000  # fake rig type 990000 + filter id binds that filter
FAKE_STRUCTURE_TYPE_ID = 990100
FAKE_LAB_RIG_TYPE_ID = 990200


def _rig_source(filter_id: int) -> dict[str, Any]:
    material, time = FILTER_ATTRIBUTES[filter_id]
    activity = "reaction" if filter_id >= 16 else "manufacturing"
    return {activity: {"material": [{"dogmaAttributeID": material, "filterID": filter_id}],
                       "time": [{"dogmaAttributeID": time, "filterID": filter_id}]}}


MODIFIER_SOURCES: dict[int, dict[str, Any]] = {
    **{FAKE_RIG_TYPE_BASE + fid: _rig_source(fid) for fid in TARGET_FILTERS},
    # A structure's own role bonuses: no filter, not a rig binding.
    FAKE_STRUCTURE_TYPE_ID: {
        "manufacturing": {"cost": [{"dogmaAttributeID": 2601}], "material": [{"dogmaAttributeID": 2600}],
                          "time": [{"dogmaAttributeID": 2602}]},
        "reaction": {"time": [{"dogmaAttributeID": 2721}]},
    },
    # A lab rig: the science activities, unfiltered.
    FAKE_LAB_RIG_TYPE_ID: {
        "invention": {"cost": [{"dogmaAttributeID": 2563}], "time": [{"dogmaAttributeID": 2564}]},
        "researchMaterial": {"cost": [{"dogmaAttributeID": 2565}], "time": [{"dogmaAttributeID": 2566}]},
    },
}

CAPITAL_HULL_GROUPS = [485, 547, 883, 1538, 4594, 5120]


def _line(name: str, activity_id: int, groups: list[int], categories: list[int] = ()) -> dict[str, Any]:
    line: dict[str, Any] = {"activityID": activity_id, "baseMaterialMultiplier": 1.0, "baseTimeMultiplier": 1.0,
                            "name": name}
    if groups:
        line["detailsPerGroup"] = [{"costMultiplier": 1.0, "groupID": g, "materialMultiplier": 1.0,
                                    "timeMultiplier": 1.0} for g in groups]
    if categories:
        line["detailsPerCategory"] = [{"categoryID": c, "materialMultiplier": 1.0, "timeMultiplier": 1.0}
                                      for c in categories]
    return line


ASSEMBLY_LINES: dict[int, dict[str, Any]] = {
    35: _line("STATION 0.5+ Manufacturing", 1, [25, 27, 513, 902], [7, 8]),
    175: _line("Structure Basic Manufacturing", 1, [25, 27, 513, 902, 963], [7, 8, 32]),
    176: _line("Structure Capital Shipyard", 1, CAPITAL_HULL_GROUPS),
    177: _line("Structure Supercapital Shipyard", 1, [30, *CAPITAL_HULL_GROUPS, 659]),
    182: _line("Structure Hybrid Reactions", 11, [974]),
}

INSTALLATION_TYPES: dict[int, dict[str, Any]] = {
    35878: {"assemblyLines": [{"assemblyLineID": 175}]},
    35881: {"assemblyLines": [{"assemblyLineID": 176}]},
    35877: {"assemblyLines": [{"assemblyLineID": 176}, {"assemblyLineID": 177}]},
    45537: {"assemblyLines": [{"assemblyLineID": 182}]},
    45538: {"assemblyLines": [{"assemblyLineID": 182}]},
    45539: {"assemblyLines": [{"assemblyLineID": 182}]},
}

# SDE English names: categories, and groups with their category.
CATEGORIES: dict[int, str] = {
    2: "Celestial", 4: "Material", 17: "Commodity", 46: "Orbitals", 6: "Ship", 7: "Module", 8: "Charge",
    18: "Drone", 20: "Implant", 22: "Deployable", 23: "Starbase", 32: "Subsystem", 39: "Infrastructure Upgrades",
    40: "Sovereignty Structures", 65: "Structure", 66: "Structure Module", 87: "Fighter",
}
GROUPS: dict[int, tuple[int, str]] = {
    12: (2, "Cargo Container"), 340: (2, "Secure Cargo Container"),
    448: (2, "Audit Log Secure Container"), 649: (2, "Freight Container"),
    25: (6, "Frigate"), 31: (6, "Shuttle"), 420: (6, "Destroyer"),
    324: (6, "Assault Frigate"), 541: (6, "Interdictor"), 830: (6, "Covert Ops"),
    831: (6, "Interceptor"), 834: (6, "Stealth Bomber"), 893: (6, "Electronic Attack Ship"),
    1283: (6, "Expedition Frigate"), 1305: (6, "Tactical Destroyer"),
    1527: (6, "Logistics Frigate"), 1534: (6, "Command Destroyer"),
    26: (6, "Cruiser"), 28: (6, "Hauler"), 419: (6, "Combat Battlecruiser"),
    463: (6, "Mining Barge"), 1201: (6, "Attack Battlecruiser"),
    4902: (6, "Expedition Command Ship"), 5087: (6, "Special Edition Yachts"),
    358: (6, "Heavy Assault Cruiser"), 380: (6, "Deep Space Transport"), 540: (6, "Command Ship"),
    543: (6, "Exhumer"), 832: (6, "Logistics"), 833: (6, "Force Recon Ship"),
    894: (6, "Heavy Interdiction Cruiser"), 906: (6, "Combat Recon Ship"),
    963: (6, "Strategic Cruiser"), 1202: (6, "Blockade Runner"), 1972: (6, "Flag Cruiser"),
    27: (6, "Battleship"), 513: (6, "Freighter"), 941: (6, "Industrial Command Ship"),
    898: (6, "Black Ops"), 900: (6, "Marauder"), 902: (6, "Jump Freighter"),
    485: (6, "Dreadnought"), 547: (6, "Carrier"), 883: (6, "Capital Industrial Ship"),
    1538: (6, "Force Auxiliary"), 4594: (6, "Lancer Dreadnought"), 5120: (6, "Command Carrier"),
    536: (17, "Structure Components"), 1136: (4, "Fuel Block"), 4736: (46, "Skyhook"),
    873: (17, "Capital Construction Components"), 332: (17, "Tool"),
    334: (17, "Construction Components"), 716: (17, "Data Interfaces"),
    964: (17, "Hybrid Tech Components"), 913: (17, "Advanced Capital Construction Components"),
    974: (4, "Hybrid Polymers"), 712: (4, "Biochemical Material"), 4096: (4, "Molecular-Forged Materials"),
    428: (4, "Intermediate Materials"), 429: (4, "Composite"), 4932: (4, "Unrefined Mineral"),
}


@cache
def fake_industry_data():
    """IndustryData built from the tables above, through the production builder
    (immutable, so built once)."""
    from eve_online_industry_tracker.infrastructure.sde.industry import industry_data_from_tables

    return industry_data_from_tables(
        target_filters=TARGET_FILTERS, modifier_sources=MODIFIER_SOURCES, assembly_lines=ASSEMBLY_LINES,
        installation_types=INSTALLATION_TYPES, groups=GROUPS, categories=CATEGORIES,
    )


# --- compressibleTypes, the ore reprocessing skill, industryActivities ---------------
#
# compressibleTypes maps a base ore to its compressed form; typeDogma attribute
# 790 (reprocessingSkillType) names the skill that reprocesses an ore. Made-up
# type ids (98xxxx) except the skills, whose ids and names are CCP's (public
# game data). industryActivities is CCP's table as the SDE ships it.

FAKE_ORE_BASE = 980001          # an ore with a compressed form, both with attribute 790
FAKE_ORE_COMPRESSED = 980002
FAKE_ORE_BASE_ONLY_SKILL = 980003       # its compressed form has no attribute 790 of its own
FAKE_ORE_COMPRESSED_NO_SKILL = 980004
FAKE_ORE_NO_SKILL = 980005      # an ore with neither attribute 790 nor a compressed form
FAKE_ORE_LEGACY_COMPRESSED = 980006     # named "Batch Compressed ...": not in compressibleTypes
FAKE_GAS = 980007               # compressible, but no reprocessing skill at all
FAKE_GAS_COMPRESSED = 980008

SKILL_SIMPLE_ORE_PROCESSING = 60377
SKILL_COMPLEX_ORE_PROCESSING = 60380
SKILL_UBIQUITOUS_MOON_ORE_PROCESSING = 46152
SKILL_ICE_PROCESSING = 18025
SKILL_NAMES: dict[int, str] = {SKILL_SIMPLE_ORE_PROCESSING: "Simple Ore Processing",
                               SKILL_COMPLEX_ORE_PROCESSING: "Complex Ore Processing",
                               SKILL_UBIQUITOUS_MOON_ORE_PROCESSING: "Ubiquitous Moon Ore Processing",
                               SKILL_ICE_PROCESSING: "Ice Processing"}

COMPRESSIBLE_TYPES: dict[int, dict[str, Any]] = {
    FAKE_ORE_BASE: {"compressedTypeID": FAKE_ORE_COMPRESSED},
    FAKE_ORE_BASE_ONLY_SKILL: {"compressedTypeID": FAKE_ORE_COMPRESSED_NO_SKILL},
    FAKE_GAS: {"compressedTypeID": FAKE_GAS_COMPRESSED},
}


def _dogma(*attributes: tuple[int, float], effects: tuple[int, ...] = ()) -> dict[str, Any]:
    row: dict[str, Any] = {"dogmaAttributes": [{"attributeID": a, "value": v} for a, v in attributes]}
    if effects:
        row["dogmaEffects"] = [{"effectID": e, "isDefault": e == 2663} for e in effects]
    return row


# The reprocessing rigs' effects (CCP's dogmaEffects, build 3607705): 6673 applies the
# security modifier to the rig's own 717; 6674/6675/6678 pre-assign the rig's 717 to the
# reprocessing service's (group 1322) refiningYieldNormalOres 2444 / MoonOres 2445 / Ice 2448.
DOGMA_EFFECTS: dict[int, dict[str, Any]] = {
    2663: {"name": "rigSlot", "effectCategoryID": 0},
    6673: {"name": "structureReprocessingRigSecurityModification", "effectCategoryID": 0,
           "modifierInfo": [{"domain": "itemID", "func": "ItemModifier", "modifiedAttributeID": 717,
                             "modifyingAttributeID": 2358, "operation": 4}]},
    **{effect: {"name": name, "effectCategoryID": 0,
                "modifierInfo": [{"domain": "structureID", "func": "LocationGroupModifier", "groupID": 1322,
                                  "modifiedAttributeID": attribute, "modifyingAttributeID": 717,
                                  "operation": -1}]}
       for effect, name, attribute in [(6674, "structureRigAsteroidOreYield", 2444),
                                       (6675, "structureRigMoonOreYield", 2445),
                                       (6678, "structureRigIceYield", 2448)]},
    16: {"name": "online", "effectCategoryID": 4},   # an effect without modifiers
}
_RIG_EFFECTS = {46633: (6674,), 46634: (6674,), 46635: (6678,), 46636: (6678,), 46637: (6675,), 46638: (6675,),
                46639: (6674, 6675, 6678), 46640: (6674, 6675, 6678)}


TYPE_DOGMA: dict[int, dict[str, Any]] = {
    FAKE_ORE_BASE: _dogma((182, 3386.0), (790, float(SKILL_SIMPLE_ORE_PROCESSING)), (2711, float(FAKE_ORE_BASE))),
    FAKE_ORE_COMPRESSED: _dogma((790, float(SKILL_SIMPLE_ORE_PROCESSING))),
    FAKE_ORE_BASE_ONLY_SKILL: _dogma((790, float(SKILL_COMPLEX_ORE_PROCESSING))),
    FAKE_ORE_COMPRESSED_NO_SKILL: _dogma((182, 3386.0)),
    FAKE_ORE_NO_SKILL: _dogma((1980, 0.5)),
    FAKE_ORE_LEGACY_COMPRESSED: _dogma((790, float(SKILL_COMPLEX_ORE_PROCESSING)),
                                       (2711, float(FAKE_ORE_BASE_ONLY_SKILL))),
    FAKE_GAS: _dogma((1980, 0.5)),
    # The reprocessing formula's attributes, with CCP's ids and values (build 3607705):
    # 379 refiningYieldMutator on the skills, 2722 strRefiningYieldBonus on the
    # refineries, 717 refiningYieldMultiplier and 2355-2357 the security modifiers on
    # the rigs, 717 on the Standup Reprocessing Facility service.
    3385: _dogma((181, 165.0), (379, 3.0)),                 # Reprocessing
    3389: _dogma((181, 165.0), (379, 2.0)),                 # Reprocessing Efficiency
    SKILL_SIMPLE_ORE_PROCESSING: _dogma((181, 165.0), (379, 2.0)),
    SKILL_COMPLEX_ORE_PROCESSING: _dogma((181, 165.0), (379, 2.0)),
    SKILL_UBIQUITOUS_MOON_ORE_PROCESSING: _dogma((181, 165.0), (379, 2.0)),
    SKILL_ICE_PROCESSING: _dogma((181, 165.0), (379, 2.0)),
    27174: _dogma((331, 8.0), (379, 4.0)),                  # Zainou 'Beancounter' Reprocessing RX-804
    35835: _dogma((2339, -20.0), (2722, 2.0)),              # Athanor
    35836: _dogma((2339, -25.0), (2722, 5.5)),              # Tatara
    35899: _dogma((717, 0.5), (2444, 0.5), (2445, 0.5), (2448, 0.5), effects=(16,)),  # Standup Reprocessing Facility I
    **{rig: _dogma((717, base), (2355, 1.0), (2356, 1.06), (2357, 1.12), (2358, 1.0),
                   effects=(2663, 6673, *_RIG_EFFECTS[rig]))
       for rig, base in [(46633, 0.51), (46634, 0.53), (46635, 0.51), (46636, 0.53), (46637, 0.51),
                         (46638, 0.53), (46639, 0.51), (46640, 0.53)]},
}

# npcStations: made-up station ids with the SDE's reprocessing columns
# (reprocessingEfficiency, reprocessingStationsTake): a standard 50% station and
# a poor 30% one.
FAKE_NPC_STATION_ID = 60999901
FAKE_POOR_NPC_STATION_ID = 60999902
NPC_STATIONS: dict[int, tuple[float, float]] = {FAKE_NPC_STATION_ID: (0.5, 0.05),
                                                FAKE_POOR_NPC_STATION_ID: (0.3, 0.05)}


def fake_npc_station(station_id: int) -> tuple[float, float] | None:
    """The (base yield, base tax) of a fake station, the shape
    facility_repo.npc_station_reprocessing returns; None when unknown."""
    return NPC_STATIONS.get(int(station_id))


INDUSTRY_ACTIVITIES: dict[int, dict[str, Any]] = {
    1: {"description": "Manufacturing of things", "name": "Manufacturing"},
    3: {"description": "Researching time efficiency", "name": "Time Efficiency Research"},
    4: {"description": "Researching material efficiency", "name": "Material Efficiency Research"},
    5: {"description": "Making blueprint copies", "name": "Copying"},
    8: {"description": "The process of creating a more advanced item based on an existing item",
        "name": "Invention"},
    9: {"description": "The process of combining raw and intermediate materials to create advanced components",
        "name": "Reactions"},
}


@cache
def fake_ore_data():
    """OreData built from the tables above, through the production builder."""
    from eve_online_industry_tracker.infrastructure.sde.ore import ore_data_from_tables

    return ore_data_from_tables(compressible_types=COMPRESSIBLE_TYPES, type_dogma=TYPE_DOGMA,
                                dogma_effects=DOGMA_EFFECTS)


@cache
def fake_industry_activities():
    """IndustryActivities built from INDUSTRY_ACTIVITIES, through the production builder."""
    from eve_online_industry_tracker.infrastructure.sde.activities import industry_activities_from_table

    return industry_activities_from_table(INDUSTRY_ACTIVITIES)


# --- the local SDE (database/eve_sde.db), for the integration tests ------------------

import os as _os

LOCAL_SDE = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "database", "eve_sde.db"))
LOCAL_SDE_MISSING = "local SDE database/eve_sde.db not present"
OVERVIEW_FIXTURE = _os.path.join(_os.path.dirname(__file__), "fixtures", "overview_rows_real.json")
OVERVIEW_FIXTURE_MISSING = ("tests/fixtures/overview_rows_real.json not captured yet: run the app with "
                            "EVE_ENABLE_DEBUG_FIXTURE=1 and GET /planner/debug/overview-fixture")


def skip_without_local_sde() -> None:
    """For a fixture or helper that opens the local SDE; a test uses @pytest.mark.needs_local_sde."""
    import pytest

    if not _os.path.exists(LOCAL_SDE):
        pytest.skip(LOCAL_SDE_MISSING)


@cache
def _local_industry_data() -> Any:
    """IndustryData from the local SDE, or the reason it cannot be read."""
    if not _os.path.exists(LOCAL_SDE):
        return LOCAL_SDE_MISSING
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from eve_online_industry_tracker.infrastructure.sde import industry

    engine = create_engine(f"sqlite:///file:{LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            missing = industry.missing_industry_tables(session)
            if missing:
                return (f"local SDE lacks {', '.join(missing)}: re-run scripts/import_sde.py --download --import --force "
                        "to import CCP's industry tables")
            return industry.load_industry_data(session)
    finally:
        engine.dispose()


def local_industry_data_or_skip():
    import pytest

    data = _local_industry_data()
    if isinstance(data, str):
        pytest.skip(data)
    return data


def _local_sde_read(load: Callable[[Any], Any], tables: tuple[str, ...]) -> Any:
    """load(session) on the local SDE, or the reason it cannot be read."""
    if not _os.path.exists(LOCAL_SDE):
        return LOCAL_SDE_MISSING
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from eve_online_industry_tracker.infrastructure.sde import industry

    engine = create_engine(f"sqlite:///file:{LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            missing = industry.missing_tables(session, tables)
            if missing:
                return (f"local SDE lacks {', '.join(missing)}: re-run scripts/import_sde.py --download --import "
                        "--force to import them")
            return load(session)
    finally:
        engine.dispose()


@cache
def _local_ore_data() -> Any:
    from eve_online_industry_tracker.infrastructure.sde import ore

    return _local_sde_read(ore.load_ore_data, ore.ORE_TABLES)


@cache
def _local_industry_activities() -> Any:
    from eve_online_industry_tracker.infrastructure.sde import activities

    return _local_sde_read(activities.load_industry_activities, activities.ACTIVITY_TABLES)


def local_ore_data_or_skip():
    import pytest

    data = _local_ore_data()
    if isinstance(data, str):
        pytest.skip(data)
    return data


def local_industry_activities_or_skip():
    import pytest

    data = _local_industry_activities()
    if isinstance(data, str):
        pytest.skip(data)
    return data


def _state(engine):
    return SimpleNamespace(db_sde=SimpleNamespace(Session=sessionmaker(bind=engine)), init_warnings=[],
                           sde_warnings=[], init_state="Ready", init_error=None)


def _producer():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    return IndustryService
