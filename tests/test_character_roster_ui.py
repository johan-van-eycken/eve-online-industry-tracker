"""The Characters page roster and the zero-character gate, with the API patched."""
from __future__ import annotations

from unittest.mock import patch

from streamlit.testing.v1 import AppTest


def _roster_script():
    from streamlit_ui.components.character_roster import render_character_roster
    render_character_roster()


def _shell_script():
    from streamlit_ui.shell import render_application
    render_application()


ROWS = [
    {"character_name": "Alpha", "has_refresh_token": True, "missing_scopes": [], "is_main": True,
     "is_corp_director": False, "industry_enabled": True},
    {"character_name": "Bravo", "has_refresh_token": False, "missing_scopes": ["a"], "is_main": False,
     "is_corp_director": False, "industry_enabled": True},
    {"character_name": "Charlie", "has_refresh_token": True, "missing_scopes": ["esi-x.v1"], "is_main": False,
     "is_corp_director": True, "industry_enabled": False},
]


def _fake_api(status=None, health=None, login=(None, "unused"), sent=None):
    def get_or_error(path, timeout_seconds=None):
        if path == "/characters/roster":
            return {"status": "success", "data": ROWS}, None
        if path == "/characters/login/status":
            return (None, "request failed") if status is None else ({"status": "success", "data": status}, None)
        raise AssertionError(path)

    def get(path, timeout_seconds=None):
        assert path == "/health"
        return health

    return [
        patch("streamlit_ui.components.character_roster.api_get_or_error", get_or_error),
        patch("streamlit_ui.components.character_roster.api_get", get),
        patch("streamlit_ui.components.character_roster.api_post_or_error", lambda path, payload: login),
        patch("streamlit_ui.components.character_roster.api_patch_or_error",
              lambda path, payload: (sent.append(("PATCH", path)), (None, "unused"))[1]),
        patch("streamlit_ui.components.character_roster.api_delete_or_error",
              lambda path: (sent.append(("DELETE", path)), (None, "unused"))[1]),
    ]


def _run(script, patches, session=None):
    at = AppTest.from_function(script)
    for key, value in (session or {}).items():
        at.session_state[key] = value
    return _rerun(at, patches)


def _rerun(at, patches):
    for p in patches:
        p.start()
    try:
        at.run(timeout=10)
    finally:
        for p in patches:
            p.stop()
    return at


def test_roster_shows_status_per_row():
    at = _run(_roster_script, _fake_api())
    table = at.dataframe[0].value
    assert list(table["Name"]) == ["Alpha", "Bravo", "Charlie"]
    assert list(table["Status"]) == ["Active", "Not activated", "Scopes missing"]
    assert list(table["Main"]) == [True, False, False]
    assert list(table["Director"]) == [False, False, True]
    assert list(table["Industry"]) == [True, True, False]
    labels = [b.label for b in at.button]
    assert "Activate" in labels and "Renew scopes" in labels


def test_add_character_shows_named_instruction():
    patches = _fake_api(status={"state": "waiting", "character_name": "Delta", "message": ""},
                        login=({"status": "success", "data": {"login_url": "https://login.example/x"}}, None))
    at = _run(_roster_script, patches, {"login_for": "Delta", "login_url": "https://login.example/x"})
    assert any("Log in as **Delta** in the browser window that opened (5 min)." in m.value for m in at.markdown)
    link = [e for e in at.get("link_button")]
    assert link and link[0].proto.url == "https://login.example/x"


def test_wrong_character_status_shows_error():
    status = {"state": "wrong_character", "character_name": "Delta", "message": "You logged in as Echo. Log in as Delta."}
    at = _run(_roster_script, _fake_api(status=status), {"login_for": "Delta", "login_url": "u"})
    assert [e.value for e in at.error] == ["You logged in as Echo. Log in as Delta."]
    assert "login_for" not in at.session_state


def test_unreachable_status_after_start_shows_loading():
    at = _run(_roster_script, _fake_api(status=None, health=None), {"login_for": "Delta", "login_url": "u"})
    assert any(i.value == "Loading Delta…" for i in at.info)
    assert at.session_state["login_for"] == "Delta"


def test_loading_ends_when_health_shows_the_character_active():
    health = {"status": "OK", "characters": {"active": 1, "not_activated": []}}
    at = _run(_roster_script, _fake_api(status={"state": "idle"}, health=health), {"login_for": "Delta", "login_url": "u"})
    assert "login_for" not in at.session_state
    assert not any("Loading" in i.value for i in at.info)


def test_done_while_restarting_keeps_waiting_even_with_health_200():
    # Flask exits ~0.5 s after done; its /health still lists the startup snapshot, without the new character.
    health = {"status": "OK", "characters": {"active": 1, "not_activated": []}}
    at = _run(_roster_script, _fake_api(status={"state": "done", "restarting": True}, health=health),
              {"login_for": "Delta", "login_url": "u"})
    assert at.session_state["login_for"] == "Delta"
    assert any(i.value == "Loading Delta…" for i in at.info)


def test_edit_and_delete_need_the_confirm_checkbox():
    sent = []
    at = _run(_roster_script, _fake_api(sent=sent))
    for kind in ("save", "delete"):
        button = at.button(key=f"character_roster_Alpha_{kind}")
        assert button.disabled
        button.click()
    _rerun(at, _fake_api(sent=sent))
    assert sent == []

    at.checkbox(key="character_roster_Alpha_delete_confirm").check()
    at.button(key="character_roster_Alpha_delete").click()
    _rerun(at, _fake_api(sent=sent))
    assert sent == [("DELETE", "/characters/Alpha")]


def test_gate_blocks_other_pages_with_zero_active():
    health = {"status": "OK", "characters": {"active": 0, "not_activated": ["Bravo"]}}
    with patch("streamlit_ui.shell._render_module_page") as render_page:
        at = _run(_shell_script, [patch("streamlit_ui.components.character_roster.api_get", lambda p, timeout_seconds=None: health)],
                  {"app_shell_navigation": "Daily Planner"})
    assert [i.value for i in at.info] == ["Activate a character first: open **Characters** in the navigation."]
    render_page.assert_not_called()
