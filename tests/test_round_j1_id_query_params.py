"""Round J1.5: a character_id or corporation_id query value that is not a
whole number is a 400 naming the field and the value, not a silent "all
characters" / "all corporations". Absent or blank still means all. Fake
values only.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import flask_app.routes.characters as character_routes
import flask_app.routes.corporations as corporation_routes
import flask_app.routes.industry as industry_routes
from flask_app.app import create_app

_ROUTES = [
    ("/characters/assets", "character_id"),
    ("/characters/realized_profit", "character_id"),
    ("/corporations/assets", "corporation_id"),
    ("/corporations/realized_profit", "corporation_id"),
    ("/industry_active_jobs", "character_id"),
]


@pytest.fixture
def client(monkeypatch):
    class _no_service:
        def __init__(self, *a, **k):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"{name}: the service must not be reached with an invalid id")

    for module in (character_routes, corporation_routes, industry_routes):
        monkeypatch.setattr(module, "require_ready", lambda state: None)
        monkeypatch.setattr(module, "get_state", lambda: SimpleNamespace())
    monkeypatch.setattr(character_routes, "CharactersService", _no_service)
    monkeypatch.setattr(corporation_routes, "CorporationsService", _no_service)
    monkeypatch.setattr(industry_routes, "IndustryService", _no_service)
    return create_app().test_client()


@pytest.mark.parametrize("path,field", _ROUTES)
@pytest.mark.parametrize("value", ["abc", "12x", "-5", "1.5"])
def test_an_invalid_id_is_a_400_naming_it(client, path, field, value):
    response = client.get(path, query_string={field: value})
    assert response.status_code == 400
    message = response.get_json()["message"]
    assert field in message and repr(value) in message


@pytest.mark.parametrize("raw,expected", [(None, None), ("", None), ("  ", None), (" 90000001 ", 90000001)])
def test_the_optional_id_param(raw, expected):
    from flask_app.http import optional_id_param

    app = create_app()
    query = {} if raw is None else {"character_id": raw}
    with app.test_request_context("/", query_string=query):
        assert optional_id_param("character_id") == expected
