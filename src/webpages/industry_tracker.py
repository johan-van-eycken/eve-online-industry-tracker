import streamlit as st

from utils.characters_api import build_character_options, fetch_characters
from utils.flask_api import api_get


def _fetch_profiles(character_id: int) -> list[dict]:
    resp = api_get(f"/industry_profiles/{int(character_id)}") or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load industry profiles")
    data = resp.get("data") or []
    return data if isinstance(data, list) else []


def render() -> None:
    st.subheader("Industry Tracker")

    try:
        characters = fetch_characters()
    except Exception as e:
        st.error(f"Failed to get characters: {e}")
        return

    if not characters:
        st.info("No characters found.")
        return

    char_options = build_character_options(characters)

    if not char_options:
        st.info("No characters found.")
        return

    col_char, col_profile = st.columns(2)

    with col_char:
        selected_character_id = st.selectbox(
            "Character",
            options=list(char_options.keys()),
            format_func=lambda x: char_options.get(int(x), str(x)),
            key="industry_tracker_character",
        )

    profiles: list[dict] = []
    try:
        profiles = _fetch_profiles(int(selected_character_id))
    except Exception as e:
        with col_profile:
            st.error(f"Failed to get industry profiles: {e}")
        return

    profile_options: dict[int, str] = {}
    for p in profiles:
        if not isinstance(p, dict):
            continue
        raw_id = p.get("id")
        if raw_id is None:
            continue
        try:
            profile_id = int(raw_id)
        except Exception:
            continue
        profile_options[profile_id] = str(p.get("profile_name") or raw_id)

    default_profile_id: int | None = None
    for p in profiles:
        if not isinstance(p, dict):
            continue
        if not bool(p.get("is_default")):
            continue
        raw_id = p.get("id")
        if raw_id is None:
            continue
        try:
            default_profile_id = int(raw_id)
        except Exception:
            default_profile_id = None
        break

    if default_profile_id is None and profile_options:
        default_profile_id = next(iter(profile_options.keys()))

    with col_profile:
        if not profile_options:
            st.selectbox(
                "Industry Profile",
                options=[],
                key=f"industry_tracker_profile_{int(selected_character_id)}",
            )
            return

        profile_ids = list(profile_options.keys())
        default_index = 0
        if default_profile_id in profile_ids:
            default_index = profile_ids.index(int(default_profile_id))

        st.selectbox(
            "Industry Profile",
            options=profile_ids,
            index=int(default_index),
            format_func=lambda x: profile_options.get(int(x), str(x)),
            key=f"industry_tracker_profile_{int(selected_character_id)}",
        )
