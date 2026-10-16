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
from typing import Any

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


# --- the local SDE (database/eve_sde.db), for the integration tests ------------------

import os as _os

LOCAL_SDE = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "database", "eve_sde.db"))


@cache
def _local_industry_data() -> Any:
    """IndustryData from the local SDE, or the reason it cannot be read."""
    if not _os.path.exists(LOCAL_SDE):
        return "local SDE database/eve_sde.db not present"
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from eve_online_industry_tracker.infrastructure.sde import industry

    engine = create_engine(f"sqlite:///file:{LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            missing = industry.missing_industry_tables(session)
            if missing:
                return (f"local SDE lacks {', '.join(missing)}: re-run scripts/import_sde.py --import --force "
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
