from __future__ import annotations

import streamlit as st  # pyright: ignore[reportMissingImports]

from utils.characters_api import build_character_options, fetch_characters


def select_character_id(
    *,
    label: str = "Select Character",
    key: str = "character_id",
    default_to_main: bool = True,
) -> int | None:
    """Render a character selectbox and return the selected character_id.

    Uses `GET /characters` as the single source of truth.

    Returns:
        Selected character_id, or None when no characters are available.
    """

    try:
        characters = fetch_characters()
    except Exception as e:
        st.error(f"Failed to get characters: {e}")
        return None

    if not characters:
        st.warning("No character data found. Run main.py first.")
        return None

    char_options = build_character_options(characters)
    if not char_options:
        st.warning("No character data found. Run main.py first.")
        return None

    character_ids = list(char_options.keys())

    default_index = 0
    if default_to_main:
        main_id: int | None = None
        for c in characters:
            if not isinstance(c, dict):
                continue
            if not bool(c.get("is_main")):
                continue
            raw_id = c.get("character_id")
            if raw_id is None:
                continue
            try:
                main_id = int(raw_id)
            except Exception:
                main_id = None
            break

        if main_id in char_options:
            default_index = character_ids.index(int(main_id))

    selected = st.selectbox(
        label,
        options=character_ids,
        index=int(default_index),
        format_func=lambda x: char_options.get(int(x), str(x)),
        key=key,
    )

    if selected is None:
        return None
    try:
        return int(selected)
    except Exception:
        return None
