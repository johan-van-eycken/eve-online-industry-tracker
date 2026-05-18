from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.infrastructure.models import StructureNameCacheModel


def _is_structure_id(location_id: int) -> bool:
    """Upwell structures have IDs >= 1020000000000."""
    return isinstance(location_id, int) and location_id >= 1020000000000


class LocationsService:
    def __init__(self, *, state: Any):
        self._state = state

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_location(self, location_id: int) -> dict:
        info = self._resolve_location(location_id)
        return info if info else {}

    def get_locations(self, location_ids: list[int]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for location_id in location_ids:
            info = self._resolve_location(location_id)
            result[str(location_id)] = info if info else {}
        return result

    # ------------------------------------------------------------------
    # Resolution pipeline
    # ------------------------------------------------------------------

    def _resolve_location(self, location_id: int) -> dict | None:
        """Try every resolution strategy in order, cache on success."""

        # 1. Main ESI service (stations, systems, etc. + structures if accessible)
        info = self._state.esi_service.get_location_info(
            location_id,
            suppress_forbidden_log=True,
            suppress_not_found_log=True,
        )
        if info:
            if _is_structure_id(location_id):
                self._cache_structure(location_id, info)
            return info

        if not _is_structure_id(location_id):
            return None

        # 2. Multi-character fallback for structures
        info = self._resolve_structure_via_characters(location_id)
        if info:
            self._cache_structure(location_id, info)
            return info

        # 3. Persistent DB cache (structure was resolved in a previous session)
        info = self._lookup_structure_cache(location_id)
        if info:
            return info

        # 4. Public structures table
        info = self._lookup_public_structures(location_id)
        if info:
            return info

        logging.debug("Structure %s could not be resolved from any source", location_id)
        return None

    # ------------------------------------------------------------------
    # Multi-character ESI fallback
    # ------------------------------------------------------------------

    def _resolve_structure_via_characters(self, structure_id: int) -> dict | None:
        """Try each character's authenticated ESI client to resolve a structure name."""
        char_manager = getattr(self._state, "char_manager", None)
        if char_manager is None:
            return None

        main_esi_client = getattr(self._state.esi_service, "_esi_client", None)
        characters = char_manager.get_characters()

        for char in characters:
            try:
                char.ensure_esi()
                if char.esi_client is main_esi_client:
                    continue
                data = char.esi_client.esi_get(
                    f"/universe/structures/{structure_id}/",
                    suppress_forbidden_log=True,
                    suppress_not_found_log=True,
                )
                if data and isinstance(data, dict) and data.get("name"):
                    logging.info(
                        "Resolved structure %s (%s) via character %s",
                        structure_id,
                        data.get("name"),
                        char.character_name,
                    )
                    return data
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    # Persistent structure name cache
    # ------------------------------------------------------------------

    def _cache_structure(self, structure_id: int, info: dict) -> None:
        """Upsert a structure name into the persistent cache."""
        name = info.get("name")
        if not name:
            return
        try:
            session = self._state.db_app.session
            record = session.query(StructureNameCacheModel).filter_by(
                structure_id=structure_id,
            ).first()
            if record:
                record.name = name
                record.solar_system_id = info.get("solar_system_id")
                record.owner_id = info.get("owner_id")
                record.type_id = info.get("type_id")
                record.updated_at = datetime.now(timezone.utc)
            else:
                record = StructureNameCacheModel(
                    structure_id=structure_id,
                    name=name,
                    solar_system_id=info.get("solar_system_id"),
                    owner_id=info.get("owner_id"),
                    type_id=info.get("type_id"),
                )
                session.add(record)
            session.commit()
        except Exception as e:
            logging.debug("Failed to cache structure %s: %s", structure_id, e)
            try:
                self._state.db_app.session.rollback()
            except Exception:
                pass

    def _lookup_structure_cache(self, structure_id: int) -> dict | None:
        """Look up a structure name from the persistent cache."""
        try:
            record = self._state.db_app.session.query(StructureNameCacheModel).filter_by(
                structure_id=structure_id,
            ).first()
            if record and record.name:
                return {
                    "name": record.name,
                    "solar_system_id": record.solar_system_id,
                    "owner_id": record.owner_id,
                    "type_id": record.type_id,
                }
        except Exception:
            pass
        return None

    def _lookup_public_structures(self, structure_id: int) -> dict | None:
        """Look up a structure name from the public_structures table."""
        try:
            from eve_online_industry_tracker.infrastructure.models import PublicStructuresModel
            record = self._state.db_app.session.query(PublicStructuresModel).filter_by(
                structure_id=structure_id,
            ).first()
            if record and record.structure_name:
                return {
                    "name": record.structure_name,
                    "solar_system_id": record.system_id,
                    "owner_id": record.owner_id,
                    "type_id": record.type_id,
                }
        except Exception:
            pass
        return None
