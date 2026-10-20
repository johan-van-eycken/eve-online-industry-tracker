"""Round J1.4: an admin settings notice that starts with "ERROR:" (an
unreadable admin_settings.json) renders as st.error, the others as
st.warning. Fake values only.
"""
from __future__ import annotations

from types import SimpleNamespace


def test_an_error_notice_renders_as_an_error_and_others_as_warnings(monkeypatch):
    from streamlit_ui.pages import admin_settings as page

    shown = []
    fake = SimpleNamespace(
        error=lambda body, *a, **k: shown.append(("error", body)),
        warning=lambda body, *a, **k: shown.append(("warning", body)),
    )
    monkeypatch.setattr(page, "st", fake)
    monkeypatch.setattr(page, "api_get", lambda path: {"status": "success", "data": {
        "notices": ["ERROR: admin settings file is unreadable; ...", "industry/x was out of range"],
        "schema": {}, "values": {}, "invalid": {},
    }})
    page._load_settings()
    assert shown == [
        ("error", "ERROR: admin settings file is unreadable; ..."),
        ("warning", "industry/x was out of range"),
    ]
