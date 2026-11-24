"""Audit 2026-11-24, Flask group: POST /planner/action/done answers like the
PATCH route (9), admin settings change only once saved (17), and foreign Host
headers are refused (19)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager


class _Planner:
    def __init__(self, actions, processed=()):
        self.actions, self.processed, self.done = actions, set(processed), []

    def get_action(self, action_id):
        return self.actions.get(action_id)

    def mark_action_done(self, action_id):
        if action_id in self.processed:
            raise ValueError("feedback already processed")
        self.done.append(action_id)


@pytest.fixture
def planner_client(monkeypatch):
    from flask_app.app import create_app
    import flask_app.routes.daily_planner as routes

    planner = _Planner({5: {"status": "pending"}, 6: {"status": "done"}}, processed={6})
    monkeypatch.setattr(routes, "require_ready", lambda s: None)
    app = create_app()
    app.extensions["app_state"] = SimpleNamespace(daily_planner_service=planner, admin_settings=None, db_app=None,
                                                  db_sde=None, db_oauth=None)
    return app.test_client(), planner


@pytest.mark.parametrize("body, status", [
    ({"action_id": 5}, 200), ({"action_id": "5"}, 200), ({"action_id": 99999}, 404),
    ({"action_id": 6}, 409), ({"action_id": "abc"}, 400), ({"action_id": 0}, 400), ({}, 400),
    ({"action_id": True}, 400),
])
def test_mark_done_answers_each_case(planner_client, body, status):
    client, planner = planner_client
    assert client.post("/planner/action/done", json=body).status_code == status
    assert planner.done == ([5] if status == 200 else [])


@pytest.mark.parametrize("host, status", [
    ("127.0.0.1:5000", 200), ("localhost:5000", 200), ("[::1]:5000", 200), ("evil.example:5000", 403),
    ("evil.example", 403),
])
def test_only_local_host_names_are_served(planner_client, host, status):
    client, _planner = planner_client
    assert client.post("/planner/action/done", json={"action_id": 5}, headers={"Host": host}).status_code == status


def test_a_failed_settings_save_keeps_the_running_values(tmp_path, monkeypatch):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    before = mgr.get("daily_planner", "planner_min_margin_pct")

    def disk_full(self, *a, **kw):
        raise OSError("No space left on device")

    monkeypatch.setattr(type(tmp_path), "write_text", disk_full)
    with pytest.raises(OSError):
        mgr.set("daily_planner", "planner_min_margin_pct", 25.0)
    with pytest.raises(OSError):
        mgr.set_bulk({"daily_planner": {"planner_min_margin_pct": 30.0}})
    assert mgr.get("daily_planner", "planner_min_margin_pct") == before
