"""Which engineering-rig groups cover a manufactured product: EVE's rule.

EVE applies an engineering rig to a manufacturing job by the product's SDE
category and group, never by its name. Each rig's dogma effect modifies a
structure attribute (e.g. 2538 attributeEquipmentManufactureMaterialMultiplier),
and CCP's industry modifier sources bind that attribute to an industry
target filter: a set of categoryIDs and groupIDs. A product is covered when
its category or its group is in the filter.

The filters and the bindings are CCP's: the SDE's industryTargetFilters and
industryModifierSources (build 3607705 on). infrastructure/sde/industry
reads them, with the SDE's English group and category names, and
build_rig_catalog turns them into a RigCatalog; the functions below take
that catalog. Nothing here holds CCP's ids: only our label for each filter
(FILTER_LABELS, the names the UI and the saved rig effects carry) and the
order of specificity.

A rig effect gets the label of the structure attribute it modifies
(RigCatalog.attribute_bindings, read by infrastructure/sde/rig_effects from
each effect's SDE modifierInfo). The reactor rigs bind to the reaction
filters the same way. A product can be in more than one: every ship is
also in "All Ships" (the XL ship rig), a T3 subsystem is in "Advanced Medium
Ships" and "All Ships", and group 4902 (Expedition Command Ship) is in both
medium ship filters.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping

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


# Our label for each of CCP's manufacturing target filters, in order of
# specificity: the first matching label is a product's primary
# manufacturing group; "All Ships" (filter 3) is last. CCP's own names
# ("Equipment", "Small T1 Ships", ...) differ; these are the ones the UI
# shows and the rig effects carry.
FILTER_LABELS: dict[int, str] = {
    2: "Modules",
    4: "Ammo & Charges",
    1: "Drones",
    5: "Basic Small Ships",
    6: "Advanced Small Ships",
    7: "Basic Medium Ships",
    8: "Advanced Medium Ships",
    9: "Basic Large Ships",
    10: "Advanced Large Ships",
    11: "Capital Ships",
    12: "Structures",
    13: "Capital Components",
    14: "Advanced Components",
    15: "Advanced Capital Components",
    3: "All Ships",
}
# The reaction filters: a reactor rig applies to a reaction job by the
# reaction product's group, the same way.
REACTION_FILTER_LABELS: dict[int, str] = {
    16: "Hybrid Reactions",
    17: "Biochemical Reactions",
    18: "Composite Reactions",
}
_LAST_FILTER_ID = 3  # "All Ships": the least specific, always last

# Structure attributes a rig effect can modify that no target filter
# restricts: the science activities (SDE descriptions "... Invention",
# "Material Efficiency Blueprint Research", ...). Activity names are the
# rig effects' (research_me / research_te), metric as above. Not read from
# the SDE's modifier sources: CCP's lab rigs list 2565-2568 under both
# research activities (37182/37183 swap ME and TE), so the ME/TE split
# rests on the attribute descriptions (tests/test_rig_values_and_security.py).
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


@dataclass(frozen=True)
class RigCatalog:
    """CCP's rig target filters and attribute bindings, from the SDE
    (build_rig_catalog), with the SDE's English names of every category and
    group the filters name (for entries that carry names but no ids)."""
    manufacturing_filters: tuple[RigTargetFilter, ...]
    reaction_filters: tuple[RigTargetFilter, ...]
    attribute_bindings: Mapping[int, AttributeBinding]
    category_names: Mapping[int, str]
    groups: Mapping[int, tuple[int, str]]  # group id -> (category id, name)
    labels: frozenset[str] = field(init=False)
    reaction_labels: frozenset[str] = field(init=False)
    _category_id_by_name: Mapping[str, int] = field(init=False, repr=False, compare=False)
    _group_id_by_name: Mapping[str, int] = field(init=False, repr=False, compare=False)
    _categories_with_group_filters: frozenset[int] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        put = object.__setattr__
        put(self, "labels", frozenset(f.label for f in self.manufacturing_filters))
        put(self, "reaction_labels", frozenset(f.label for f in self.reaction_filters))
        put(self, "_category_id_by_name", {name.casefold(): cid for cid, name in self.category_names.items()})
        put(self, "_group_id_by_name", {name.casefold(): gid for gid, (_, name) in self.groups.items()})
        put(self, "_categories_with_group_filters", frozenset(category for category, _ in self.groups.values()))


def _ids(values: Any) -> frozenset[int]:
    return frozenset(int(v) for v in values or ())


def build_rig_catalog(
    target_filters: Mapping[int, Mapping[str, Any]],
    modifier_sources: Mapping[int, Mapping[str, Any]],
    *,
    groups: Mapping[int, tuple[int, str]],
    categories: Mapping[int, str],
) -> RigCatalog:
    """The RigCatalog from the SDE's industryTargetFilters and
    industryModifierSources ({id: row} as the SDE YAML has them) and the
    SDE's English group ({id: (category id, name)}) and category names.

    Every (activity, filter) a modifier source binds must have one material
    and one time attribute, and every bound filter must exist: anything else
    is a ValueError (the SDE tables are not what this code reads). A filter
    with no label of ours gets its SDE name and a WARNING; a label whose
    filter no source binds any more is left out with a WARNING."""
    bound: dict[tuple[str, int], dict[str, set[int]]] = {}
    for source in modifier_sources.values():
        for activity in ("manufacturing", "reaction"):
            for metric, entries in (source.get(activity) or {}).items():
                for entry in entries or ():
                    if entry.get("filterID") is None:
                        continue  # a structure's own bonus, no rig filter
                    if metric not in ("material", "time"):
                        logger.warning("Rig catalog: SDE binds %s %s to filter %s; only material and time are read",
                                       activity, metric, entry["filterID"])
                        continue
                    key = (activity, int(entry["filterID"]))
                    bound.setdefault(key, {"material": set(), "time": set()})[metric].add(
                        int(entry["dogmaAttributeID"]))

    labels_by_activity = {"manufacturing": FILTER_LABELS, "reaction": REACTION_FILTER_LABELS}
    filters: dict[str, list[RigTargetFilter]] = {"manufacturing": [], "reaction": []}
    for (activity, filter_id), attributes in sorted(bound.items()):
        if filter_id not in target_filters:
            raise ValueError(f"SDE industryModifierSources binds filter {filter_id}, which industryTargetFilters lacks")
        if len(attributes["material"]) != 1 or len(attributes["time"]) != 1:
            raise ValueError(f"SDE industryModifierSources binds {activity} filter {filter_id} to material "
                             f"{sorted(attributes['material'])} and time {sorted(attributes['time'])}; one each expected")
        row = target_filters[filter_id]
        label = labels_by_activity[activity].get(filter_id)
        if label is None:
            label = str(row.get("name") or f"Filter {filter_id}")
            logger.warning("Rig catalog: SDE %s filter %s (%r) has no label here; its rigs are shown under the SDE "
                           "name", activity, filter_id, label)
        (material,), (time,) = attributes["material"], attributes["time"]
        filters[activity].append(RigTargetFilter(label, filter_id, _ids(row.get("categoryIDs")),
                                                 _ids(row.get("groupIDs")), material, time))

    for activity, labels in labels_by_activity.items():
        present = {f.filter_id for f in filters[activity]}
        for filter_id, label in labels.items():
            if filter_id not in present:
                logger.warning("Rig catalog: the SDE binds no %s rig to filter %s (%s); no product is in that rig "
                               "group", activity, filter_id, label)

    position = {fid: i for i, fid in enumerate([*FILTER_LABELS, *REACTION_FILTER_LABELS])}

    def specificity(f: RigTargetFilter) -> tuple[int, int]:
        # Ours in FILTER_LABELS order, then any unlabelled filter, "All Ships" last.
        if f.filter_id == _LAST_FILTER_ID:
            return 2, 0
        if f.filter_id in position:
            return 0, position[f.filter_id]
        return 1, f.filter_id

    def ordered(fs: list[RigTargetFilter]) -> tuple[RigTargetFilter, ...]:
        return tuple(sorted(fs, key=specificity))

    manufacturing, reaction = ordered(filters["manufacturing"]), ordered(filters["reaction"])
    bindings: dict[int, AttributeBinding] = {}
    for activity, fs in (("manufacturing", manufacturing), ("reaction", reaction)):
        for f in fs:
            bindings[f.material_attribute_id] = AttributeBinding(activity, "material", f.label, f.filter_id)
            bindings[f.time_attribute_id] = AttributeBinding(activity, "time", f.label, f.filter_id)
    for attribute_id, (activity, metric) in UNFILTERED_ATTRIBUTE_BINDINGS.items():
        bindings[attribute_id] = AttributeBinding(activity, metric, "All", None)

    named_groups = {g for f in (*manufacturing, *reaction) for g in f.group_ids}
    named_categories = {c for f in (*manufacturing, *reaction) for c in f.category_ids}
    group_names = {gid: (int(groups[gid][0]), str(groups[gid][1])) for gid in sorted(named_groups) if gid in groups}
    missing = sorted(named_groups - set(group_names))
    if missing:
        logger.warning("Rig catalog: the SDE groups table lacks groups %s that a rig filter names; they classify "
                       "by id only", missing)
    category_ids = named_categories | {cat for cat, _ in group_names.values()}
    category_names = {cid: str(categories[cid]) for cid in sorted(category_ids) if cid in categories}
    return RigCatalog(manufacturing, reaction, bindings, category_names, group_names)


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
            "Rig group: SDE %s name %r is not one the rig target filters name (a localized or unlisted "
            "name); without its id the item is unclassified and only \"All\" rigs apply", kind, name,
        )


def resolve_ids(entry: dict[str, Any], *, catalog: RigCatalog) -> tuple[int, int] | None:
    """(group_id, category_id) of a type entry, from its ids or SDE names.

    Ids are used when present. Names are looked up in the catalog's English
    SDE names; the SDE is localized, so a name the catalog lacks is never
    read as "outside every filter": the entry is unclassified (None) and a
    WARNING is logged once per name. None too when the entry says too
    little: no category, or no group in a category where some groups have a
    filter of their own (Ship, Commodity, ...). A group the filters do not
    name reads 0, which only matters in a category with no group filters."""
    group_id = _int(entry.get("group_id"))
    group_name = str(entry.get("group_name") or "").strip()
    if group_id <= 0 and group_name:
        group_id = catalog._group_id_by_name.get(group_name.casefold(), 0)
    category_id = _int(entry.get("category_id"))
    category_name = str(entry.get("category_name") or "").strip()
    if category_id <= 0 and category_name:
        category_id = catalog._category_id_by_name.get(category_name.casefold(), 0)
        if category_id <= 0:
            _name_miss("category", category_name)
            return None
    if category_id <= 0 and group_id in catalog.groups:
        category_id = catalog.groups[group_id][0]
    if category_id <= 0:
        return None  # e.g. a group name alone: its category is unknown
    if group_id <= 0 and category_id in catalog._categories_with_group_filters:
        if group_name:
            _name_miss("group", group_name)
        return None
    return group_id, category_id


def rig_groups(group_id: int, category_id: int, activity: str = "manufacturing", *,
               catalog: RigCatalog) -> frozenset[str]:
    """The labels of every rig target filter that covers this group/category:
    the engineering-rig filters for manufacturing, the reactor-rig filters
    for a reaction, none for any other activity."""
    filters = (catalog.manufacturing_filters if activity == "manufacturing"
               else catalog.reaction_filters if activity == "reaction" else ())
    return frozenset(
        f.label for f in filters
        if group_id in f.group_ids or category_id in f.category_ids
    )


def rig_groups_for_entry(entry: Any, activity: str = "manufacturing", *,
                         catalog: RigCatalog) -> frozenset[str] | None:
    """The rig groups covering a type entry for a manufacturing (default) or
    reaction job; None when it cannot be classified.

    An empty set is an exact answer: no group-specific rig covers the item
    (e.g. a Mutaplasmid). None means the entry lacks the group/category ids
    and names needed to tell."""
    if not isinstance(entry, dict):
        return None
    ids = resolve_ids(entry, catalog=catalog)
    if ids is None:
        return None
    return rig_groups(*ids, activity=activity, catalog=catalog)


def primary_group(entry: Any, *, catalog: RigCatalog) -> str | None:
    """The product's manufacturing group label: its most specific rig group
    ("All Ships" only for a ship no class filter names, e.g. a Titan). None
    when the entry cannot be classified or no rig covers it."""
    groups = rig_groups_for_entry(entry, catalog=catalog)
    if not groups:
        return None
    return next(f.label for f in catalog.manufacturing_filters if f.label in groups)
