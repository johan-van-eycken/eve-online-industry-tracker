from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.config.settings import public_structures_cache_ttl_seconds as _public_structures_cache_ttl_seconds
from eve_online_industry_tracker.infrastructure import public_structures_cache_service
from eve_online_industry_tracker.infrastructure.persistence import corporation_structures_repo
from eve_online_industry_tracker.infrastructure.persistence import industry_profiles_repo
from eve_online_industry_tracker.infrastructure.sde.locations import get_npc_stations, get_solar_systems
from eve_online_industry_tracker.infrastructure.sde.rig_effects import get_rig_effects_for_type_ids
from eve_online_industry_tracker.infrastructure.sde.types import get_type_data
from eve_online_industry_tracker.infrastructure.submanufacturing_planner_service import plan_submanufacturing_tree


def get_cached_public_structures(*, state: Any, system_id: int, ttl_seconds: int) -> tuple[list[dict], bool]:
    return public_structures_cache_service.get_cached_public_structures(
        state=state,
        system_id=system_id,
        ttl_seconds=int(ttl_seconds),
    )


def trigger_refresh_public_structures_for_system(*, state: Any, system_id: int) -> bool:
    return public_structures_cache_service.trigger_refresh_public_structures_for_system(
        state=state,
        system_id=int(system_id),
    )


def public_structures_cache_ttl_seconds() -> int:
    return int(_public_structures_cache_ttl_seconds())


def corporation_structures_list_by_corporation_id(db_app_session: Any, corporation_id: int) -> list[Any]:
    return corporation_structures_repo.list_by_corporation_id(db_app_session, corporation_id)


def industry_profile_get_by_id(db_app_session: Any, profile_id: int) -> Any:
    return industry_profiles_repo.get_by_id(db_app_session, profile_id)


def industry_profile_get_default_for_character_id(db_app_session: Any, character_id: int) -> Any:
    return industry_profiles_repo.get_default_for_character_id(db_app_session, character_id)


def industry_profile_list_by_character_id(db_app_session: Any, character_id: int) -> list[Any]:
    return industry_profiles_repo.list_by_character_id(db_app_session, character_id)


def industry_profile_create(db_app_session: Any, data: dict) -> int:
    return industry_profiles_repo.create(db_app_session, data)


def industry_profile_update(db_app_session: Any, profile_id: int, data: dict) -> None:
    industry_profiles_repo.update(db_app_session, profile_id, data)


def industry_profile_delete(db_app_session: Any, profile_id: int) -> None:
    industry_profiles_repo.delete(db_app_session, profile_id)
