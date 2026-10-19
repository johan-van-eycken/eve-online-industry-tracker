"""Ore compression and the ore reprocessing skill, from the SDE.

compressibleTypes maps a base ore, ice, gas or mineral to its compressed form
(one row per base type id, with compressedTypeID). typeDogma attribute 790
(reprocessingSkillType) is the skill that reprocesses a type. They replace
the name tests ("Compressed" in the name) and the hand-made ore -> skill map
the ore calculator used.

A compressed type uses its own attribute 790 and, only when it has none, its
base type's (in build 3607705 every compressed ore and ice has its own,
equal to its base's; compressed gas has none and neither has its base).
The "Batch Compressed" ores (the pre-2021 compression, made by blueprints
that are no longer published) are not in compressibleTypes: they are not a
compressed form here, but they carry their own attribute 790.

Loaded once per process and cached like infrastructure.sde.industry: an SDE
without the tables raises SdeIndustryDataMissing naming the re-import.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from sqlalchemy import text

from eve_online_industry_tracker.infrastructure.sde.data_cache import SdeDataCache
from eve_online_industry_tracker.infrastructure.sde.industry import (
    REIMPORT_HINT,
    SdeIndustryDataMissing,
    missing_tables,
    read_table,
    tables_missing_message,
)

logger = logging.getLogger(__name__)

ORE_TABLES: tuple[str, ...] = ("compressibleTypes", "typeDogma")
PURPOSE = "ore compression and the ore reprocessing skills (the ore calculator) cannot be computed"
REPROCESSING_SKILL_ATTRIBUTE_ID = 790  # reprocessingSkillType

# The reprocessing formula's SDE attributes (domain.reprocessing).
REFINING_YIELD_MUTATOR_ATTRIBUTE_ID = 379      # a skill's or implant's bonus, percent per level
REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID = 717   # a rig's or the reprocessing service's base yield
STR_REFINING_YIELD_BONUS_ATTRIBUTE_ID = 2722   # a structure's own bonus, percent
HISEC_MODIFIER_ATTRIBUTE_ID = 2355             # a rig's security modifiers
LOWSEC_MODIFIER_ATTRIBUTE_ID = 2356
NULLSEC_MODIFIER_ATTRIBUTE_ID = 2357

REPROCESSING_SKILL_TYPE_ID = 3385              # Reprocessing
REPROCESSING_EFFICIENCY_SKILL_TYPE_ID = 3389   # Reprocessing Efficiency

ATHANOR_TYPE_ID = 35835
TATARA_TYPE_ID = 35836
STANDUP_REPROCESSING_FACILITY_TYPE_ID = 35899  # the service module: an unrigged structure's base yield
# The rigs of each refinery's size; their values equal the moon-ore and ice
# variants of the same size (M-Set rigs on an Athanor, L-Set on a Tatara).
RIG_TYPE_IDS: Mapping[tuple[int, str], int] = {
    (ATHANOR_TYPE_ID, "t1"): 46633,   # Standup M-Set Asteroid Ore Grading Processor I
    (ATHANOR_TYPE_ID, "t2"): 46634,   # Standup M-Set Asteroid Ore Grading Processor II
    (TATARA_TYPE_ID, "t1"): 46639,    # Standup L-Set Reprocessing Monitor I
    (TATARA_TYPE_ID, "t2"): 46640,    # Standup L-Set Reprocessing Monitor II
}
FACILITY_TYPE_IDS: frozenset[int] = frozenset(
    {ATHANOR_TYPE_ID, TATARA_TYPE_ID, STANDUP_REPROCESSING_FACILITY_TYPE_ID, *RIG_TYPE_IDS.values()})


@dataclass(frozen=True)
class OreData:
    compressed_by_base: Mapping[int, int]   # base type id -> its compressed form
    base_by_compressed: Mapping[int, int]   # compressed type id -> its base type
    skill_by_type: Mapping[int, int]        # type id -> attribute 790 (a skill type id)
    # type id -> attribute 379 refiningYieldMutator (percent per level), for skills and implants
    refining_yield_bonus: Mapping[int, float] = dataclasses.field(default_factory=dict)
    # FACILITY_TYPE_IDS -> {attribute id: value}: the structures, rigs and service module
    facility_attributes: Mapping[int, Mapping[int, float]] = dataclasses.field(default_factory=dict)

    def is_compressed(self, type_id: int) -> bool:
        return int(type_id) in self.base_by_compressed

    def base_of(self, type_id: int) -> int | None:
        """The base type of a compressed form; None for any other type."""
        return self.base_by_compressed.get(int(type_id))

    def compressed_form_of(self, type_id: int) -> int | None:
        """The compressed form of a base type; None for any other type."""
        return self.compressed_by_base.get(int(type_id))

    def reprocessing_skill(self, type_id: int) -> int | None:
        """The skill type id that reprocesses this type: its own attribute
        790, else (a compressed form) its base type's; None when neither has
        one."""
        type_id = int(type_id)
        own = self.skill_by_type.get(type_id)
        if own is not None:
            return own
        base = self.base_by_compressed.get(type_id)
        return self.skill_by_type.get(base) if base is not None else None


def ore_data_from_tables(*, compressible_types: Mapping[int, Mapping[str, Any]],
                         type_dogma: Mapping[int, Mapping[str, Any]]) -> OreData:
    """OreData from compressibleTypes and typeDogma ({id: row}, the SDE YAML
    shape). ValueError when a row is not what this code reads."""
    compressed_by_base: dict[int, int] = {}
    base_by_compressed: dict[int, int] = {}
    for base, row in compressible_types.items():
        compressed = row.get("compressedTypeID")
        if compressed is None:
            raise ValueError(f"compressibleTypes row {base} has no compressedTypeID")
        base, compressed = int(base), int(compressed)
        if compressed in base_by_compressed:
            raise ValueError(f"type {compressed} is the compressed form of more than one type "
                             f"({base_by_compressed[compressed]}, {base})")
        compressed_by_base[base] = compressed
        base_by_compressed[compressed] = base

    skill_by_type: dict[int, int] = {}
    refining_yield_bonus: dict[int, float] = {}
    facility_attributes: dict[int, dict[int, float]] = {}
    for type_id, row in type_dogma.items():
        type_id = int(type_id)
        for attribute in row.get("dogmaAttributes") or []:
            attribute_id = attribute.get("attributeID")
            if attribute_id == REPROCESSING_SKILL_ATTRIBUTE_ID:
                skill_by_type[type_id] = int(attribute["value"])
            elif attribute_id == REFINING_YIELD_MUTATOR_ATTRIBUTE_ID:
                refining_yield_bonus[type_id] = float(attribute["value"])
            if type_id in FACILITY_TYPE_IDS:
                facility_attributes.setdefault(type_id, {})[int(attribute_id)] = float(attribute["value"])
    return OreData(compressed_by_base=compressed_by_base, base_by_compressed=base_by_compressed,
                   skill_by_type=skill_by_type, refining_yield_bonus=refining_yield_bonus,
                   facility_attributes=facility_attributes)


def refining_skill_levels(session: Any, trained_skills: list[Mapping[str, Any]]) -> dict[int, int]:
    """{skill type id: trained level} for the trained skills (ESI's
    skill_id/trained_skill_level rows) that carry a reprocessing bonus in the
    SDE (typeDogma attribute 379): Reprocessing, Reprocessing Efficiency and
    the ore, ice and moon processing skills. Keyed by id, so the language of
    the SDE names plays no part. Reads typeDogma directly (not the cached
    OreData), so a character refresh works on an SDE without the ore tables."""
    levels = {int(s["skill_id"]): int(s.get("trained_skill_level") or 0)
              for s in trained_skills if s.get("skill_id") is not None}
    if not levels:
        return {}
    ids = ", ".join(str(i) for i in sorted(levels))
    rows = session.execute(text(f'SELECT id, "dogmaAttributes" FROM "typeDogma" WHERE id IN ({ids})')).fetchall()
    with_bonus: set[int] = set()
    for type_id, raw in rows:
        attributes = json.loads(raw) if isinstance(raw, str) else (raw or [])
        if any(a.get("attributeID") == REFINING_YIELD_MUTATOR_ATTRIBUTE_ID for a in attributes):
            with_bonus.add(int(type_id))
    return {skill_id: level for skill_id, level in levels.items() if skill_id in with_bonus}


def load_ore_data(session: Any) -> OreData:
    """OreData from this SDE. SdeIndustryDataMissing when a table is missing
    or its content is not what this code reads."""
    missing = missing_tables(session, ORE_TABLES)
    if missing:
        raise SdeIndustryDataMissing(tables_missing_message(missing, PURPOSE))
    try:
        return ore_data_from_tables(compressible_types=read_table(session, "compressibleTypes"),
                                    type_dogma=read_table(session, "typeDogma"))
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise SdeIndustryDataMissing(f"SDE ore tables unreadable ({exc}); {REIMPORT_HINT}") from exc


# --- the process-wide data ---------------------------------------------------------

_cache: SdeDataCache[OreData] = SdeDataCache(
    missing=lambda: SdeIndustryDataMissing(f"SDE ore data not loaded (no SDE configured); {REIMPORT_HINT}"),
    logger=logger,
)


def install_ore_data(data: OreData) -> None:
    _cache.install(data)


def configure_ore_data_loader(loader: Callable[[], OreData] | None) -> None:
    _cache.configure_loader(loader)


def reset_ore_data() -> None:
    _cache.reset()


def ore_data() -> OreData:
    """The cached OreData, loading it on first use. Raises
    SdeIndustryDataMissing (logged once as an ERROR) when there is none."""
    return _cache.get(SdeIndustryDataMissing)
