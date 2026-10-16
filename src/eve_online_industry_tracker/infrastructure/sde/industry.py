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
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from sqlalchemy import inspect, text

from eve_online_industry_tracker.domain import industry_facility as _industry_facility
from eve_online_industry_tracker.domain import rig_groups as _rig_groups
from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized

logger = logging.getLogger(__name__)

INDUSTRY_TABLES: tuple[str, ...] = (
    "industryTargetFilters",
    "industryModifierSources",
    "industryAssemblyLines",
    "industryInstallationTypes",
)
REIMPORT_HINT = "re-run scripts/import_sde.py --import --force (from the repository root) to re-import the SDE"


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
        shipyard_lines=_industry_facility.build_shipyard_lines(assembly_lines, installation_types),
    )


# --- reading the SDE ---------------------------------------------------------------

def _table_names(session: Any) -> set[str]:
    return set(inspect(session.get_bind()).get_table_names())


def missing_industry_tables(session: Any) -> list[str]:
    """The industry tables this SDE lacks, in INDUSTRY_TABLES order."""
    present = _table_names(session)
    return [t for t in INDUSTRY_TABLES if t not in present]


def _missing_message(missing: list[str]) -> str:
    return (f"SDE industry tables missing: {', '.join(missing)} (an SDE imported before build 3607705 or with an "
            f"older importer); rig groups and the shipyard gate cannot be computed: {REIMPORT_HINT}")


def _decode(value: Any) -> Any:
    if isinstance(value, str) and value[:1] in ("[", "{"):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _read_table(session: Any, table: str) -> dict[int, dict[str, Any]]:
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
    return {table: _read_table(session, table) for table in INDUSTRY_TABLES}


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


def sde_schema_problems(session: Any) -> list[str]:
    """What this SDE lacks that the app reads, each with the re-import hint:
    the industry tables and types.packagedVolume (build 3607705 on)."""
    problems: list[str] = []
    present = _table_names(session)
    if "types" in present:
        columns = {c["name"] for c in inspect(session.get_bind()).get_columns("types")}
        if "packagedVolume" not in columns:
            problems.append("SDE column types.packagedVolume missing (an SDE before build 3607705); every type "
                            f"lookup fails: {REIMPORT_HINT}")
    missing = [t for t in INDUSTRY_TABLES if t not in present]
    if missing:
        problems.append(_missing_message(missing))
    return problems


# --- the process-wide data ---------------------------------------------------------

_lock = threading.Lock()
_data: IndustryData | None = None
_loader: Callable[[], IndustryData] | None = None
_logged_failures: set[str] = set()


def install_industry_data(data: IndustryData) -> None:
    """Use `data` from now on (tests; or a loader's result)."""
    global _data
    with _lock:
        _data = data


def configure_industry_data_loader(loader: Callable[[], IndustryData] | None) -> None:
    """The loader the first use calls (and a later use, while it fails)."""
    global _loader
    with _lock:
        _loader = loader


def reset_industry_data() -> None:
    """Forget the data, the loader and the logged failures."""
    global _data, _loader
    with _lock:
        _data, _loader = None, None
        _logged_failures.clear()


def industry_data() -> IndustryData:
    """The cached IndustryData, loading it on first use. Raises
    SdeIndustryDataMissing (logged once as an ERROR) when there is none."""
    global _data
    with _lock:
        if _data is not None:
            return _data
        loader = _loader
        try:
            if loader is None:
                raise SdeIndustryDataMissing(f"SDE industry data not loaded (no SDE configured); {REIMPORT_HINT}")
            _data = loader()
            return _data
        except SdeIndustryDataMissing as exc:
            message = str(exc)
            if message not in _logged_failures:
                _logged_failures.add(message)
                logger.error("%s", message)
            raise


def rig_catalog() -> _rig_groups.RigCatalog:
    return industry_data().rig_catalog


def shipyard_lines() -> _industry_facility.ShipyardLines:
    return industry_data().shipyard_lines
