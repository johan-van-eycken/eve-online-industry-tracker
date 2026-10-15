"""Which engineering-rig groups cover a manufactured product: EVE's rule.

EVE applies an engineering rig to a manufacturing job by the product's SDE
category and group, never by its name. Each rig's dogma effect modifies a
structure attribute (e.g. 2538 attributeEquipmentManufactureMaterialMultiplier),
and the client's industry modifier sources bind that attribute to an
industry target filter: a set of categoryIDs and groupIDs. A product is
covered when its category or its group is in the filter.

The local SDE (database/eve_sde.db) has the attributes, whose descriptions
list the targets in words, but not the filters themselves. The ids below are
CCP's filters as published from the TQ client
(sde.hoboleaks.space/tq/industrytargetfilters.json and
industrymodifiersources.json, revision 3619879, fetched 2026-10-15; CCP
game data). tests/test_rig_groups.py diffs this table against the snapshot
in tests/fixtures/hoboleaks/ and, with RIG_FILTERS_ONLINE=1, against the
live files (the staleness check), and checks every id against the local SDE.

Keys are the group labels the producer's rig effects carry: a rig effect
gets the label of the structure attribute it modifies (ATTRIBUTE_BINDINGS,
read by infrastructure/sde/rig_effects from each effect's SDE modifierInfo).
The reactor rigs bind to the reaction filters 16-18 the same way. A product can be
in more than one: every ship is also in "All Ships" (the XL ship rig), a
T3 subsystem is in "Advanced Medium Ships" and "All Ships", and group 4902
(Expedition Command Ship) is in both medium ship filters.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RigTargetFilter:
    label: str
    filter_id: int  # CCP industry target filter id
    category_ids: frozenset[int]
    group_ids: frozenset[int]
    # The structure attributes the rig effects for this filter modify.
    material_attribute_id: int
    time_attribute_id: int


def _f(label: str, filter_id: int, categories: tuple[int, ...], groups: tuple[int, ...],
       material_attribute_id: int, time_attribute_id: int) -> RigTargetFilter:
    return RigTargetFilter(label, filter_id, frozenset(categories), frozenset(groups),
                           material_attribute_id, time_attribute_id)


# In order of specificity: the first matching label is a product's primary
# manufacturing group; "All Ships" is last.
RIG_TARGET_FILTERS: tuple[RigTargetFilter, ...] = (
    _f("Modules", 2, (7, 20, 22), (12, 340, 448, 649), 2538, 2539),
    _f("Ammo & Charges", 4, (8,), (), 2540, 2541),
    _f("Drones", 1, (18, 87), (), 2542, 2543),
    _f("Basic Small Ships", 5, (), (25, 31, 420), 2544, 2545),
    _f("Advanced Small Ships", 6, (), (324, 541, 830, 831, 834, 893, 1283, 1305, 1527, 1534), 2550, 2551),
    _f("Basic Medium Ships", 7, (), (26, 28, 419, 463, 1201, 4902, 5087), 2546, 2547),
    _f("Advanced Medium Ships", 8, (32,),
       (358, 380, 540, 543, 832, 833, 894, 906, 963, 1202, 1972, 4902), 2552, 2553),
    _f("Basic Large Ships", 9, (), (27, 513, 941), 2548, 2549),
    _f("Advanced Large Ships", 10, (), (898, 900, 902), 2555, 2556),
    _f("Capital Ships", 11, (), (485, 547, 883, 1538, 4594, 5120), 2575, 2576),
    _f("Structures", 12, (23, 39, 40, 65, 66), (536, 1136, 4736), 2561, 2562),
    _f("Capital Components", 13, (), (873,), 2559, 2560),
    _f("Advanced Components", 14, (), (332, 334, 716, 964), 2557, 2558),
    _f("Advanced Capital Components", 15, (), (913,), 2658, 2659),
    _f("All Ships", 3, (6, 32), (), 2592, 2591),
)

# The reaction filters: a reactor rig applies to a reaction job by the
# reaction product's group, the same way (filters 16-18).
REACTION_TARGET_FILTERS: tuple[RigTargetFilter, ...] = (
    _f("Hybrid Reactions", 16, (), (974,), 2716, 2715),
    _f("Biochemical Reactions", 17, (), (712, 4096), 2720, 2719),
    _f("Composite Reactions", 18, (), (428, 429, 4932), 2718, 2717),
)

# Structure attributes a rig effect can modify that no target filter
# restricts: the science activities (SDE descriptions "... Invention",
# "Material Efficiency Blueprint Research", ...). Activity names are the
# rig effects' (research_me / research_te), metric as above.
UNFILTERED_ATTRIBUTE_BINDINGS: dict[int, tuple[str, str]] = {
    2563: ("invention", "cost"), 2564: ("invention", "time"),
    2565: ("research_me", "cost"), 2566: ("research_me", "time"),
    2567: ("research_te", "cost"), 2568: ("research_te", "time"),
    2569: ("copying", "cost"), 2570: ("copying", "time"),
}


@dataclass(frozen=True)
class AttributeBinding:
    activity: str  # manufacturing / reaction / invention / copying / research_me / research_te
    metric: str  # material / time / cost
    label: str  # the rig group ("All" when no filter restricts it)
    filter_id: int | None


def _attribute_bindings() -> dict[int, AttributeBinding]:
    out: dict[int, AttributeBinding] = {}
    for activity, filters in (("manufacturing", RIG_TARGET_FILTERS), ("reaction", REACTION_TARGET_FILTERS)):
        for f in filters:
            out[f.material_attribute_id] = AttributeBinding(activity, "material", f.label, f.filter_id)
            out[f.time_attribute_id] = AttributeBinding(activity, "time", f.label, f.filter_id)
    for attribute_id, (activity, metric) in UNFILTERED_ATTRIBUTE_BINDINGS.items():
        out[attribute_id] = AttributeBinding(activity, metric, "All", None)
    return out


# Structure attribute id -> what a rig effect that modifies it reduces.
ATTRIBUTE_BINDINGS: dict[int, AttributeBinding] = _attribute_bindings()

# SDE (English) names of every category the filters name or that holds a
# group they name, and of every group they name with its category, for
# entries that carry names but no ids.
CATEGORY_NAMES: dict[int, str] = {
    2: "Celestial", 4: "Material", 17: "Commodity", 46: "Orbitals", 6: "Ship", 7: "Module", 8: "Charge", 18: "Drone", 20: "Implant", 22: "Deployable",
    23: "Starbase", 32: "Subsystem", 39: "Infrastructure Upgrades",
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

_CATEGORY_ID_BY_NAME = {name.casefold(): cid for cid, name in CATEGORY_NAMES.items()}
_GROUP_ID_BY_NAME = {name.casefold(): gid for gid, (_, name) in GROUPS.items()}

_CATEGORIES_WITH_GROUP_FILTERS = frozenset(category for category, _ in GROUPS.values())

ALL_LABELS: frozenset[str] = frozenset(f.label for f in RIG_TARGET_FILTERS)
REACTION_LABELS: frozenset[str] = frozenset(f.label for f in REACTION_TARGET_FILTERS)

# Hulls built only in a capital-capable facility (Azbel / Sotiyo) that the
# Capital Ships rig filter does not name: Titan 30 and Supercarrier 659 (only
# the XL "All Ships" rig covers them), Freighter 513 and Jump Freighter 902
# (Large Ship rigs). The producer's facility gate reads these with the
# Capital Ships / capital component groups.
CAPITAL_FACILITY_GROUP_IDS: frozenset[int] = frozenset({30, 659, 513, 902})
# Titan and Supercarrier: Standup Supercapital Shipyard I (35877) fits only
# the Sotiyo, so these are Sotiyo-only.
SUPERCAPITAL_GROUP_IDS: frozenset[int] = frozenset({30, 659})


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _name(value: Any) -> str:
    return str(value or "").strip().casefold()


# (kind, name) pairs already warned about by _name_miss.
_warned_names: set[tuple[str, str]] = set()


def _name_miss(kind: str, name: str) -> None:
    key = (kind, name)
    if key not in _warned_names:
        _warned_names.add(key)
        logger.warning(
            "Rig group: SDE %s name %r is not in the rig target filter table (a localized or unlisted "
            "name); without its id the item is unclassified and only \"All\" rigs apply", kind, name,
        )


def resolve_ids(entry: dict[str, Any]) -> tuple[int, int] | None:
    """(group_id, category_id) of a type entry, from its ids or SDE names.

    Ids are used when present. Names are looked up in the English table
    above; the SDE is localized, so a name the table lacks is never read as
    "outside every filter": the entry is unclassified (None) and a WARNING
    is logged once per name. None too when the entry says too little: no
    category, or no group in a category where some groups have a filter of
    their own (Ship, Commodity, ...). A group the filters do not name reads
    0, which only matters in a category with no group filters."""
    group_id = _int(entry.get("group_id"))
    group_name = str(entry.get("group_name") or "").strip()
    if group_id <= 0 and group_name:
        group_id = _GROUP_ID_BY_NAME.get(group_name.casefold(), 0)
    category_id = _int(entry.get("category_id"))
    category_name = str(entry.get("category_name") or "").strip()
    if category_id <= 0 and category_name:
        category_id = _CATEGORY_ID_BY_NAME.get(category_name.casefold(), 0)
        if category_id <= 0:
            _name_miss("category", category_name)
            return None
    if category_id <= 0 and group_id in GROUPS:
        category_id = GROUPS[group_id][0]
    if category_id <= 0:
        return None  # e.g. a group name alone: its category is unknown
    if group_id <= 0 and category_id in _CATEGORIES_WITH_GROUP_FILTERS:
        if group_name:
            _name_miss("group", group_name)
        return None
    return group_id, category_id


def rig_groups(group_id: int, category_id: int, activity: str = "manufacturing") -> frozenset[str]:
    """The labels of every rig target filter that covers this group/category:
    the engineering-rig filters for manufacturing, the reactor-rig filters
    for a reaction, none for any other activity."""
    filters = (RIG_TARGET_FILTERS if activity == "manufacturing"
               else REACTION_TARGET_FILTERS if activity == "reaction" else ())
    return frozenset(
        f.label for f in filters
        if group_id in f.group_ids or category_id in f.category_ids
    )


def rig_groups_for_entry(entry: Any, activity: str = "manufacturing") -> frozenset[str] | None:
    """The rig groups covering a type entry for a manufacturing (default) or
    reaction job; None when it cannot be classified.

    An empty set is an exact answer: no group-specific rig covers the item
    (e.g. a Mutaplasmid). None means the entry lacks the group/category ids
    and names needed to tell."""
    if not isinstance(entry, dict):
        return None
    ids = resolve_ids(entry)
    if ids is None:
        return None
    return rig_groups(*ids, activity=activity)


def primary_group(entry: Any) -> str | None:
    """The product's manufacturing group label: its most specific rig group
    ("All Ships" only for a ship no class filter names, e.g. a Titan). None
    when the entry cannot be classified or no rig covers it."""
    groups = rig_groups_for_entry(entry)
    if not groups:
        return None
    return next(f.label for f in RIG_TARGET_FILTERS if f.label in groups)
