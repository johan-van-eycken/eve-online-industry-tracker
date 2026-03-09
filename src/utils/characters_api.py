from __future__ import annotations

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
