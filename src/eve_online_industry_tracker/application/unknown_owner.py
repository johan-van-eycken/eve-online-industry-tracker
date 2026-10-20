"""Checks that a requested character_id or corporation_id is one the app
has, raising the managers' own UnknownCharacterError / UnknownCorporationError
(the flask app answers them 404: unknown_character / unknown_corporation)."""
from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.application.characters.character_manager import UnknownCharacterError
from eve_online_industry_tracker.application.corporations.corporation_manager import UnknownCorporationError

__all__ = ["UnknownCharacterError", "UnknownCorporationError", "require_known_character", "require_known_corporation"]


def require_known_character(state: Any, character_id: int | None, method_name: str) -> None:
    """UnknownCharacterError when `character_id` is given and the character
    manager does not have it."""
    if character_id is None:
        return
    if state.char_manager.get_character_by_id(int(character_id)) is None:
        raise UnknownCharacterError(method_name, f"character_id {int(character_id)}")


def require_known_corporation(state: Any, corporation_id: int | None, method_name: str) -> None:
    """UnknownCorporationError when `corporation_id` is given and the
    corporation manager does not have it."""
    if corporation_id is None:
        return
    if state.corp_manager.get_corporation(corporation_id=int(corporation_id)) is None:
        raise UnknownCorporationError(method_name, f"corporation_id {int(corporation_id)}")
