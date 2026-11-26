"""The character roster: add, activate (EVE login), renew scopes, edit flags and delete."""
from __future__ import annotations

from urllib.parse import quote

import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import streamlit as st  # pyright: ignore[reportMissingImports]

from streamlit_ui.api.client import api_delete_or_error, api_get, api_get_or_error, api_patch_or_error, api_post_or_error

# The message of the 409 of POST /characters/login (flask_app/routes/characters.py).
_LOGIN_BUSY_MESSAGE = "Another character login is in progress."
_FLAG_LABELS = {"is_main": "Main", "is_corp_director": "Director", "industry_enabled": "Industry"}


def _health() -> dict | None:
    """The /health body when it is 200; None while Flask restarts or initialises (503)."""
    return api_get("/health", timeout_seconds=3)


def active_character_count() -> int | None:
    """The number of active characters per /health; None when Flask is not ready."""
    health = _health()
    return (health.get("characters") or {}).get("active") if health else None


def _status(row: dict) -> str:
    if not row.get("has_refresh_token"):
        return "Not activated"
    return "Scopes missing" if row.get("missing_scopes") else "Active"


def _notify(kind: str, text: str) -> None:
    """Show text after the rerun that follows (st.error / st.info / ...)."""
    st.session_state["character_roster_notice"] = (kind, text)


def _start_login(payload: dict) -> None:
    body, err = api_post_or_error("/characters/login", payload)
    if err:
        st.error("Another login is still running." if err == _LOGIN_BUSY_MESSAGE else f"Login failed to start: {err}")
        return
    st.session_state["login_for"] = payload["character_name"]
    st.session_state["login_url"] = ((body or {}).get("data") or {}).get("login_url") or ""
    st.rerun()


def _after_change(body: dict | None, err: str | None, done: str) -> None:
    if err or body is None:
        st.error(f"Failed: {err or 'no response'}")
        return
    if ((body.get("data") or {}).get("restarting")) is False:
        _notify("info", "Restart the app to load the change.")
    else:
        st.cache_data.clear()  # stale after the reload; failed responses during it aren't cached
        _notify("success", f"{done} The app reloads.")
    st.rerun()


def _end_login(kind: str, text: str) -> None:
    st.session_state.pop("login_for", None)
    st.session_state.pop("login_url", None)
    _notify(kind, text)
    st.rerun()


@st.fragment(run_every=2)
def _login_progress() -> None:
    name = st.session_state.get("login_for")
    if not name:
        return
    body, _ = api_get_or_error("/characters/login/status", timeout_seconds=3)
    status = (body or {}).get("data") or {}
    state = status.get("state")
    if state in ("wrong_character", "timeout", "failed"):
        _end_login("error", status.get("message") or f"Login of {name}: {state}")
        return
    if state == "waiting":
        st.markdown(f"Log in as **{name}** in the browser window that opened (5 min).")
        st.link_button("Open the EVE login", st.session_state.get("login_url") or "")
        return
    if state == "done" and status.get("restarting") is False:
        _end_login("info", "Restart the app to load the change.")
        return
    # Only the restarted Flask reports idle; before that /health still shows the old startup's characters.
    health = _health() if state == "idle" else None
    if health is not None:
        not_activated = {n.casefold() for n in (health.get("characters") or {}).get("not_activated") or []}
        if name.casefold() not in not_activated:
            # The reloaded app has other characters, so every cached page response is stale.
            st.cache_data.clear()
            _end_login("success", f"{name} is active.")
            return
    st.info(f"Loading {name}…")


def _render_row(row: dict) -> None:
    name = row["character_name"]
    status = _status(row)
    key = f"character_roster_{name}"
    cols = st.columns([2, 1, 3, 2])
    cols[0].markdown(f"**{name}**")
    if status != "Active":
        label = "Activate" if status == "Not activated" else "Renew scopes"
        if cols[1].button(label, key=f"{key}_login", disabled=bool(st.session_state.get("login_for"))):
            _start_login({"character_name": name})
            return
    with cols[2].expander("Edit"):
        flags = {f: st.checkbox(label, value=bool(row.get(f)), key=f"{key}_{f}") for f, label in _FLAG_LABELS.items()}
        confirm = st.checkbox("This reloads the app.", key=f"{key}_edit_confirm")
        if st.button("Save", key=f"{key}_save", disabled=not confirm) and confirm:
            _after_change(*api_patch_or_error(f"/characters/{quote(name, safe='')}", flags), f"{name} saved.")
    with cols[3]:
        confirm = st.checkbox("This reloads the app.", key=f"{key}_delete_confirm")
        if st.button("Delete", key=f"{key}_delete", disabled=not confirm) and confirm:
            _after_change(*api_delete_or_error(f"/characters/{quote(name, safe='')}"), f"{name} deleted.")


def render_character_roster() -> None:
    st.subheader("Character roster")
    notice = st.session_state.pop("character_roster_notice", None)
    if notice:
        getattr(st, notice[0])(notice[1])
    _login_progress()

    body, err = api_get_or_error("/characters/roster")
    if err:
        st.warning(f"The roster is unavailable: {err}")
        return
    rows = [r for r in (body or {}).get("data") or [] if isinstance(r, dict)]
    if rows:
        st.dataframe(pd.DataFrame([
            {"Name": r["character_name"], "Status": _status(r),
             **{label: bool(r.get(f)) for f, label in _FLAG_LABELS.items()}}
            for r in rows
        ]), hide_index=True)
        for row in rows:
            _render_row(row)
    else:
        st.info("No characters yet: add one below.")

    with st.form("character_roster_add", clear_on_submit=True):
        st.markdown("**Add character**")
        name = st.text_input("Character name")
        flags = {f: st.checkbox(label, value=(f == "industry_enabled")) for f, label in _FLAG_LABELS.items()}
        if st.form_submit_button("Add and log in", disabled=bool(st.session_state.get("login_for"))):
            if name.strip():
                _start_login({"character_name": name.strip(), **flags})
            else:
                st.error("Enter the character name.")
