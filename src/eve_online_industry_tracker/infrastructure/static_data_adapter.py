from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.infrastructure.sde.static_data import build_all_materials, build_all_ores
from eve_online_industry_tracker.infrastructure.static_data.facility_repo import get_all_facilities
from eve_online_industry_tracker.infrastructure.static_data.optimizer_service import run_optimize


__all__ = [
    "get_all_facilities",
    "run_optimize",
    "build_all_materials",
    "build_all_ores",
]
