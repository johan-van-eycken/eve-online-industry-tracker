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

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping

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


@dataclass(frozen=True)
class OreData:
    compressed_by_base: Mapping[int, int]   # base type id -> its compressed form
    base_by_compressed: Mapping[int, int]   # compressed type id -> its base type
    skill_by_type: Mapping[int, int]        # type id -> attribute 790 (a skill type id)

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
    for type_id, row in type_dogma.items():
        for attribute in row.get("dogmaAttributes") or []:
            if attribute.get("attributeID") == REPROCESSING_SKILL_ATTRIBUTE_ID:
                skill_by_type[int(type_id)] = int(attribute["value"])
    return OreData(compressed_by_base=compressed_by_base, base_by_compressed=base_by_compressed,
                   skill_by_type=skill_by_type)


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
