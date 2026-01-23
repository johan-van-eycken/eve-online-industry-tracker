from __future__ import annotations

from typing import Any


class LocationsService:
    def __init__(self, *, state: Any):
        self._state = state

    def get_location(self, location_id: int) -> dict:
        return self._state.esi_service.get_location_info(location_id)

    def get_locations(self, location_ids: list[int]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for location_id in location_ids:
            info = self._state.esi_service.get_location_info(location_id)
            result[str(location_id)] = info
        return result
