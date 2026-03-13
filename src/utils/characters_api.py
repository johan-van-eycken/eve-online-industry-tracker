from __future__ import annotations

from typing import Any

from utils.flask_api import cached_api_get


def build_character_options(characters: list[dict]) -> dict[int, str]:
    """Build a mapping usable in Streamlit selectboxes.

    Keeps insertion order from the provided list.
    """

    out: dict[int, str] = {}
    for c in characters:
        if not isinstance(c, dict):
            continue

        raw_id = c.get("character_id")
        if raw_id is None:
            continue
        try:
            character_id = int(raw_id)
        except Exception:
            continue

        label = c.get("character_name") or c.get("name") or raw_id
        out[character_id] = str(label)

    return out


def build_owned_blueprint_character_scope_options(
    characters: list[dict[str, Any]],
) -> tuple[list[str], dict[str, str], str | None]:
    option_values: list[str] = []
    label_by_value: dict[str, str] = {}
    default_value: str | None = None

    for character in characters:
        if not isinstance(character, dict):
            continue

        raw_id = character.get("character_id")
        if raw_id is None:
            continue
        try:
            character_id = int(raw_id)
        except Exception:
            continue
        if character_id <= 0:
            continue

        character_name = str(character.get("character_name") or character.get("name") or character_id)
        option_value = f"character:{character_id}"
        option_values.append(option_value)
        label_by_value[option_value] = character_name
        if default_value is None and bool(character.get("is_main")):
            default_value = option_value

    return option_values, label_by_value, default_value


def build_owned_blueprint_character_corporation_scope_options(
    characters: list[dict[str, Any]],
) -> tuple[list[str], dict[str, str], str | None]:
    option_values: list[str] = []
    label_by_value: dict[str, str] = {}
    default_value: str | None = None

    for character in characters:
        if not isinstance(character, dict):
            continue

        try:
            character_id = int(character.get("character_id") or 0)
        except Exception:
            character_id = 0
        try:
            corporation_id = int(character.get("corporation_id") or 0)
        except Exception:
            corporation_id = 0
        if character_id <= 0 or corporation_id <= 0:
            continue

        character_name = str(character.get("character_name") or character.get("name") or character_id)
        corporation_name = str(character.get("corporation_name") or corporation_id)
        option_value = f"character_and_corporation:{character_id}:{corporation_id}"
        option_values.append(option_value)
        label_by_value[option_value] = f"{character_name} + {corporation_name}"
        if default_value is None and bool(character.get("is_main")):
            default_value = option_value

    return option_values, label_by_value, default_value


def fetch_characters() -> list[dict]:
    """Fetch character list from the Flask API.

    Returns the raw list of dicts from `GET /characters`.

    Raises:
        RuntimeError: when the API returns a non-success response.
    """

    resp = cached_api_get("/characters") or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load characters")

    data = resp.get("data") or []
    return data if isinstance(data, list) else []
