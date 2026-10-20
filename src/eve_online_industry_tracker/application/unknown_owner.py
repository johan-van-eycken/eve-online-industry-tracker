"""A character_id or corporation_id the app does not have.

Stand-ins for the errors the round-j1 branch adds to
characters/character_manager.py (UnknownCharacterError) and
corporations/corporation_manager.py (UnknownCorporationError): same names,
constructor and message, and the flask app answers them 404 with the codes
unknown_character / unknown_corporation. On merge, the managers' classes
replace these (import them from there and drop this module).
"""
from __future__ import annotations

from typing import Any


class _UnknownOwnerError(Exception):
    kind = "owner"

    def __init__(self, method_name: str, selector: str):
        self.method_name = method_name
        self.selector = selector
        self.errors = {selector: f"no such {self.kind} in this manager"}
        details = "; ".join(f"{name}: {message}" for name, message in self.errors.items())
        super().__init__(f"{method_name} failed for 1 {self.kind}(s): {details}")


class UnknownCharacterError(_UnknownOwnerError):
    """A character_id or name the manager does not have: nothing was read."""

    kind = "character"


class UnknownCorporationError(_UnknownOwnerError):
    """A corporation_id or name the manager does not have: nothing was read."""

    kind = "corporation"


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
