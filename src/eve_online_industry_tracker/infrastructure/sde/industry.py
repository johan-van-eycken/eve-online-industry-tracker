"""CCP's industry tables from the SDE: the rig catalog and the shipyard lines.

The SDE (build 3607705 on) ships industryTargetFilters,
industryModifierSources, industryAssemblyLines and
industryInstallationTypes; scripts/import_sde.py stores each as one row per
id with its nested lists as JSON text (flatten_row). This module reads them
back in the SDE's YAML shape and builds the domain's RigCatalog
(domain.rig_groups) and ShipyardLines (domain.industry_facility).

The data is loaded once per process and cached: the producer and planner
classify products from static methods with no session, so the app
configures a loader at startup (flask_app.bootstrap.check_sde_industry_data)
and the first use loads it. An SDE imported before these tables existed has
none of them: every use raises SdeIndustryDataMissing, whose message says to
re-run the importer, and the first failure is logged as an ERROR. A failed
load is retried on the next use, so a re-import is picked up without a
restart. There is no constant fallback.

The same mechanism serves the ore compression and reprocessing skills
(infrastructure.sde.ore) and the industry activities
(infrastructure.sde.activities): sde_schema_problems and sde_health_problems
name every table the app reads, each dataset failing on its own, and every
failure is an SdeIndustryDataMissing (a 503 sde_reimport_required).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from sqlalchemy import inspect, text

from eve_online_industry_tracker.domain import industry_facility as _industry_facility
from eve_online_industry_tracker.domain import rig_groups as _rig_groups
from eve_online_industry_tracker.infrastructure.sde.data_cache import SdeDataCache
from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized

logger = logging.getLogger(__name__)

INDUSTRY_TABLES: tuple[str, ...] = (
    "industryTargetFilters",
    "industryModifierSources",
    "industryAssemblyLines",
    "industryInstallationTypes",
)
REIMPORT_HINT = ("re-run scripts/import_sde.py --download --import --force (from the repository root) to re-import "
                 "the SDE, then restart the app")


class SdeIndustryDataMissing(RuntimeError):
    """The SDE lacks (or has unreadable) industry tables the app needs."""


@dataclass(frozen=True)
class IndustryData:
    rig_catalog: _rig_groups.RigCatalog
    shipyard_lines: _industry_facility.ShipyardLines


def industry_data_from_tables(
    *,
    target_filters: Mapping[int, Mapping[str, Any]],
    modifier_sources: Mapping[int, Mapping[str, Any]],
    assembly_lines: Mapping[int, Mapping[str, Any]],
    installation_types: Mapping[int, Mapping[str, Any]],
    groups: Mapping[int, tuple[int, str]],
    categories: Mapping[int, str],
) -> IndustryData:
    """IndustryData from the four tables ({id: row}, the SDE YAML shape) and
    the SDE's English group ({id: (category id, name)}) and category names."""
    return IndustryData(
        rig_catalog=_rig_groups.build_rig_catalog(target_filters, modifier_sources, groups=groups,
                                                  categories=categories),
        shipyard_lines=_industry_facility.build_shipyard_lines(
            assembly_lines, installation_types, group_categories={g: cat for g, (cat, _) in groups.items()}),
    )


# --- reading the SDE ---------------------------------------------------------------

def _table_names(session: Any) -> set[str]:
    return set(inspect(session.get_bind()).get_table_names())


def missing_tables(session: Any, tables: Iterable[str]) -> list[str]:
    """The tables (of `tables`) this SDE lacks or has empty (an import in
    progress, or one that failed), in the given order."""
    present = _table_names(session)
    missing: list[str] = []
    for table in tables:
        if table not in present or session.execute(text(f'SELECT 1 FROM "{table}" LIMIT 1')).first() is None:
            missing.append(table)
    return missing


def missing_industry_tables(session: Any) -> list[str]:
    """The industry tables this SDE lacks or has empty, in INDUSTRY_TABLES
    order. The load is tried only when this is empty."""
    return missing_tables(session, INDUSTRY_TABLES)


def tables_missing_message(missing: list[str], purpose: str) -> str:
    """The problem for tables a dataset other than the industry tables needs."""
    return (f"SDE tables missing: {', '.join(missing)} (missing or empty: an SDE imported with an older importer "
            f"config, or an import still running); {purpose}: {REIMPORT_HINT}")


def _missing_message(missing: list[str]) -> str:
    return (f"SDE industry tables missing: {', '.join(missing)} (missing or empty: an SDE imported before build "
            f"3607705, with an older importer, or an import still running); rig groups and the shipyard gate cannot be computed: {REIMPORT_HINT}")


def _decode(value: Any) -> Any:
    if isinstance(value, str) and value[:1] in ("[", "{"):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def read_table(session: Any, table: str) -> dict[int, dict[str, Any]]:
    """{id: row} for an SDE table as the importer stores it (flatten_row):
    nested lists and dicts decoded from JSON, NULL columns left out."""
    rows = session.execute(text(f'SELECT * FROM "{table}"')).mappings().all()
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        out[int(row["id"])] = {k: _decode(v) for k, v in row.items() if k != "id" and v is not None}
    return out


def read_industry_tables(session: Any) -> dict[str, dict[int, dict[str, Any]]]:
    """{table: {id: row}} for the four industry tables, in the SDE YAML shape
    (nested lists decoded, absent keys left out). Raises
    SdeIndustryDataMissing when any table is missing."""
    missing = missing_industry_tables(session)
    if missing:
        raise SdeIndustryDataMissing(_missing_message(missing))
    return {table: read_table(session, table) for table in INDUSTRY_TABLES}


def _english_names(session: Any) -> tuple[dict[int, tuple[int, str]], dict[int, str]]:
    groups = {
        int(gid): (int(category_id), parse_localized(name, "en"))
        for gid, category_id, name in session.execute(text("SELECT id, categoryID, name FROM groups")).fetchall()
        if category_id is not None
    }
    categories = {
        int(cid): parse_localized(name, "en")
        for cid, name in session.execute(text("SELECT id, name FROM categories")).fetchall()
    }
    return groups, categories


def load_industry_data(session: Any) -> IndustryData:
    """IndustryData from this SDE. SdeIndustryDataMissing when a table is
    missing or its content is not what this code reads."""
    tables = read_industry_tables(session)
    groups, categories = _english_names(session)
    try:
        return industry_data_from_tables(
            target_filters=tables["industryTargetFilters"],
            modifier_sources=tables["industryModifierSources"],
            assembly_lines=tables["industryAssemblyLines"],
            installation_types=tables["industryInstallationTypes"],
            groups=groups,
            categories=categories,
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise SdeIndustryDataMissing(f"SDE industry tables unreadable ({exc}); {REIMPORT_HINT}") from exc


def _datasets() -> list[tuple[tuple[str, ...], Callable[[list[str]], str], Callable[[], Any]]]:
    """(tables, problem for the missing ones, the cached data) per dataset the
    app reads from the SDE through a process-wide cache."""
    from eve_online_industry_tracker.infrastructure.sde import activities, ore

    return [
        (INDUSTRY_TABLES, _missing_message, industry_data),
        (ore.ORE_TABLES, lambda missing: tables_missing_message(missing, ore.PURPOSE), ore.ore_data),
        (activities.ACTIVITY_TABLES, lambda missing: tables_missing_message(missing, activities.PURPOSE),
         activities.industry_activities),
    ]


def _missing_table_problems(session: Any) -> list[tuple[str | None, Callable[[], Any]]]:
    """(problem or None, the cached data) per dataset."""
    out: list[tuple[str | None, Callable[[], Any]]] = []
    for tables, message, data in _datasets():
        missing = missing_tables(session, tables)
        out.append((message(missing) if missing else None, data))
    return out


def _column_problems(session: Any) -> list[str]:
    if "types" not in _table_names(session):
        return []
    columns = {c["name"] for c in inspect(session.get_bind()).get_columns("types")}
    if "packagedVolume" in columns:
        return []
    return ["SDE column types.packagedVolume missing (an SDE before build 3607705); every type "
            f"lookup fails: {REIMPORT_HINT}"]


def sde_schema_problems(session: Any) -> list[str]:
    """What this SDE lacks that the app reads, each with the re-import hint:
    types.packagedVolume (build 3607705 on), the industry tables, the ore
    tables (compressibleTypes, typeDogma) and industryActivities."""
    return _column_problems(session) + [problem for problem, _ in _missing_table_problems(session) if problem]


def sde_health_problems(session: Any) -> list[str]:
    """sde_schema_problems now, and, for each dataset whose tables are there,
    whether it loads (the /health re-check after a live re-import). Each
    dataset fails on its own: a missing ore table leaves the rig data alone."""
    problems = _column_problems(session)
    for problem, data in _missing_table_problems(session):
        if problem is not None:
            problems.append(problem)
            continue
        try:
            data()
        except SdeIndustryDataMissing as exc:
            problems.append(str(exc))
    return problems


# --- the process-wide data ---------------------------------------------------------

_cache: SdeDataCache[IndustryData] = SdeDataCache(
    missing=lambda: SdeIndustryDataMissing(f"SDE industry data not loaded (no SDE configured); {REIMPORT_HINT}"),
    logger=logger,
)


def install_industry_data(data: IndustryData) -> None:
    """Use `data` from now on (tests; or a loader's result)."""
    _cache.install(data)


def configure_industry_data_loader(loader: Callable[[], IndustryData] | None) -> None:
    """The loader the first use calls (and a later use, while it fails)."""
    _cache.configure_loader(loader)


def reset_industry_data() -> None:
    """Forget the data, the loader and the logged failures."""
    _cache.reset()


def industry_data() -> IndustryData:
    """The cached IndustryData, loading it on first use. Raises
    SdeIndustryDataMissing (logged once as an ERROR) when there is none."""
    return _cache.get(SdeIndustryDataMissing)


def rig_catalog() -> _rig_groups.RigCatalog:
    return industry_data().rig_catalog


def shipyard_lines() -> _industry_facility.ShipyardLines:
    return industry_data().shipyard_lines
