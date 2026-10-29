"""The overview routes read their request values one way:
a POST body with "false" strings means the same as a GET query with them
(bool("false") used to be True), and absent values keep their defaults."""
from __future__ import annotations

import pytest

import flask_app.routes.industry as routes

FLAGS_OFF = {"maximize_bp_runs": "false", "group_identical_bpcs": "false", "build_from_bpc": "false",
             "have_blueprint_source_only": "false", "include_reactions": "false"}


@pytest.fixture
def calls(monkeypatch):
    from flask_app.app import create_app

    seen: list[dict] = []

    class _Svc:
        def __init__(self, state):
            pass

        def industry_manufacturing_product_overview_payload(self, **kw):
            seen.append(kw)
            return {}

        def start_industry_manufacturing_product_overview_refresh(self, **kw):
            seen.append(kw)
            return {}

    monkeypatch.setattr(routes, "require_ready", lambda s: None)
    monkeypatch.setattr(routes, "require_sde_ready", lambda s: None)
    monkeypatch.setattr(routes, "IndustryService", _Svc)
    client = create_app().test_client()
    return client, seen


@pytest.mark.parametrize("method, path", [
    ("get", "/industry_products/1"),
    ("post", "/industry_products/1/refresh"),
])
def test_false_strings_are_false_on_every_route(calls, method, path):
    client, seen = calls
    if method == "get":
        resp = client.get(path, query_string=FLAGS_OFF)
    else:
        resp = client.post(path, json=FLAGS_OFF)
    assert resp.status_code in (200, 202), resp.get_json()
    assert {k: seen[-1][k] for k in FLAGS_OFF} == {k: False for k in FLAGS_OFF}


@pytest.mark.parametrize("method, path", [
    ("get", "/industry_products/1"),
    ("post", "/industry_products/1/refresh"),
])
def test_absent_values_keep_the_defaults(calls, method, path):
    client, seen = calls
    resp = client.get(path) if method == "get" else client.post(path, json={})
    assert resp.status_code in (200, 202)
    kw = seen[-1]
    assert (kw["maximize_bp_runs"], kw["group_identical_bpcs"], kw["build_from_bpc"],
            kw["have_blueprint_source_only"], kw["include_reactions"]) == (False, True, True, True, False)
    assert (kw["market_hub"], kw["material_price_side"], kw["product_price_side"]) == ("jita", "sell", "sell")
    assert kw["owned_blueprints_scope"] == "all_characters" and kw["character_id"] == 1


def test_real_json_booleans_still_work(calls):
    client, seen = calls
    client.post("/industry_products/1/refresh", json={"include_reactions": True, "build_from_bpc": False})
    assert seen[-1]["include_reactions"] is True and seen[-1]["build_from_bpc"] is False
