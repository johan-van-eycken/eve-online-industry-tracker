"""The industry activities from the SDE (industryActivities).

The SDE lists each activity id with its English name and description. The
app shows the label from domain.industry_activities.activity_labels (its own
label where it has one, else the SDE's name) and "Unknown (<id>)" for an id
the SDE does not have. Streamlit gets the labels through the API payloads'
activity_name.

Loaded once per process and cached like infrastructure.sde.industry: an SDE
without the table raises SdeIndustryDataMissing naming the re-import.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from eve_online_industry_tracker.domain import industry_activities as _domain
from eve_online_industry_tracker.infrastructure.sde.data_cache import SdeDataCache
from eve_online_industry_tracker.infrastructure.sde.industry import (
    REIMPORT_HINT,
    SdeIndustryDataMissing,
    missing_tables,
    read_table,
    tables_missing_message,
)
from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized

logger = logging.getLogger(__name__)

ACTIVITY_TABLES: tuple[str, ...] = ("industryActivities",)
PURPOSE = "the industry activity names (the industry jobs) cannot be shown"


@dataclass(frozen=True)
class IndustryActivities:
    names: Mapping[int, str]    # the SDE's English name per activity id
    labels: Mapping[int, str]   # what the app shows

    def label(self, activity_id: Any) -> str:
        try:
            key = int(activity_id)
        except (TypeError, ValueError):
            return _domain.unknown_activity_label(activity_id)
        label = self.labels.get(key)
        return label if label is not None else _domain.unknown_activity_label(activity_id)


def industry_activities_from_table(rows: Mapping[int, Mapping[str, Any]]) -> IndustryActivities:
    """IndustryActivities from industryActivities ({id: row}, the SDE YAML
    shape). ValueError for an activity without a name."""
    names: dict[int, str] = {}
    for activity_id, row in rows.items():
        name = parse_localized(row.get("name"), "en")
        if not name:
            raise ValueError(f"industryActivities row {activity_id} has no name")
        names[int(activity_id)] = name
    return IndustryActivities(names=names, labels=_domain.activity_labels(names))


def load_industry_activities(session: Any) -> IndustryActivities:
    """IndustryActivities from this SDE. SdeIndustryDataMissing when the
    table is missing or its content is not what this code reads."""
    missing = missing_tables(session, ACTIVITY_TABLES)
    if missing:
        raise SdeIndustryDataMissing(tables_missing_message(missing, PURPOSE))
    try:
        return industry_activities_from_table(read_table(session, "industryActivities"))
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise SdeIndustryDataMissing(f"SDE table industryActivities unreadable ({exc}); {REIMPORT_HINT}") from exc


# --- the process-wide data ---------------------------------------------------------

_cache: SdeDataCache[IndustryActivities] = SdeDataCache(
    missing=lambda: SdeIndustryDataMissing(
        f"SDE industry activities not loaded (no SDE configured); {REIMPORT_HINT}"),
    logger=logger,
)


def install_industry_activities(data: IndustryActivities) -> None:
    _cache.install(data)


def configure_industry_activities_loader(loader: Callable[[], IndustryActivities] | None) -> None:
    _cache.configure_loader(loader)


def reset_industry_activities() -> None:
    _cache.reset()


def industry_activities() -> IndustryActivities:
    """The cached IndustryActivities, loading them on first use. Raises
    SdeIndustryDataMissing (logged once as an ERROR) when there are none."""
    return _cache.get(SdeIndustryDataMissing)


def activity_label(activity_id: Any) -> str:
    """The label the app shows for an activity id; "Unknown (<id>)" for an
    id the SDE does not have."""
    return industry_activities().label(activity_id)
