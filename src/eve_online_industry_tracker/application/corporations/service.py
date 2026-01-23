from __future__ import annotations

from typing import Any


class CorporationsService:
    def __init__(self, *, state: Any):
        self._state = state

    def list_corporations(self) -> Any:
        return self._state.corp_manager.get_corporations()

    def list_assets(self) -> Any:
        return self._state.corp_manager.get_assets()
