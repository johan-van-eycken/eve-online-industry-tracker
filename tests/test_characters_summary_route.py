"""GET /characters?summary=1 leaves out each character's full history, which
the selectors never read."""
from __future__ import annotations


def test_the_summary_drops_the_history_fields(monkeypatch):
    import flask_app.routes.characters as route
    from flask_app.app import create_app

    full = {"character_id": 1, "character_name": "Pilot", "is_main": True, "corporation_id": 9,
            "wallet_journal": [{"id": 1}], "wallet_transactions": [{"id": 2}],
            "market_orders": [{"id": 3}], "assets": [{"id": 4}]}
    monkeypatch.setattr(route, "require_ready", lambda state: None)
    monkeypatch.setattr(route, "get_state", lambda: None)
    monkeypatch.setattr(route.CharactersService, "list_characters", lambda self: [dict(full)])
    client = create_app().test_client()

    (summary,) = client.get("/characters?summary=1").get_json()["data"]
    assert summary == {"character_id": 1, "character_name": "Pilot", "is_main": True, "corporation_id": 9}
    (complete,) = client.get("/characters").get_json()["data"]
    assert complete == full
