from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.session_provider import (
    SessionProvider,
    StateSessionProvider,
)

from eve_online_industry_tracker.infrastructure.static_data_adapter import (
    build_all_materials,
    build_all_ores,
    get_all_facilities,
    run_optimize,
)


class StaticDataService:
    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

    def list_facilities(self) -> Any:
        return get_all_facilities()

    def optimize_ore_plan(self, payload: dict) -> Any:
        character_id = payload["character_id"]
        character = self._state.char_manager.get_character_by_id(character_id)
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)

        sde_session = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        return run_optimize(
            payload,
            character=character,
            esi_service=self._state.esi_service,
            sde_session=sde_session,
            language=language,
        )

    def list_materials_cached(self) -> Any:
        if self._state.materials_cache is None:
            session = self._sessions.sde_session()
            language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
            self._state.materials_cache = build_all_materials(session, language)
        return self._state.materials_cache

    def list_ores(self) -> Any:
        session = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        return build_all_ores(session, language)
