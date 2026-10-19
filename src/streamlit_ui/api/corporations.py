from __future__ import annotations

from typing import Any


def fetch_director_corporations() -> list[dict[str, Any]]:
    """Return all corporations for which the app has director-level ESI access.

    Raises:
        RuntimeError: when the API call fails, with the API's reason.
    """
    from streamlit_ui.api import client

    response, reason = client.api_get_or_error("/corporations")
    if reason is not None or not isinstance(response, dict):
        raise RuntimeError(f"Failed to load corporations: {reason or 'unexpected response'}")
    corps = response.get("data") or []
    return [
        c for c in corps
        if isinstance(c, dict) and c.get("has_director_access") and c.get("corporation_id")
    ]


def build_owned_blueprint_corporation_scope_options(
    characters: list[dict[str, Any]],
) -> tuple[list[str], dict[str, str]]:
    option_values: list[str] = []
    label_by_value: dict[str, str] = {}
    seen_corporations: set[int] = set()

    for character in characters:
        if not isinstance(character, dict):
            continue

        try:
            corporation_id = int(character.get("corporation_id") or 0)
        except (ValueError, TypeError):
            corporation_id = 0
        if corporation_id <= 0 or corporation_id in seen_corporations:
            continue

        seen_corporations.add(corporation_id)
        corporation_name = str(character.get("corporation_name") or f"Corporation {corporation_id}")
        option_value = f"corporation:{corporation_id}"
        option_values.append(option_value)
        label_by_value[option_value] = corporation_name

    return option_values, label_by_value