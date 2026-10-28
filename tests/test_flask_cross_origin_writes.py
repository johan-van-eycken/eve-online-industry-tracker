"""Any web page the user opens runs on the same machine as the local API, so a
cross-site write (one that carries an Origin header) is refused, and
/shutdown needs a POST."""
from __future__ import annotations

import pytest


@pytest.fixture()
def client():
    from flask_app.app import create_app
    return create_app().test_client()


def test_a_cross_origin_post_is_refused(client):
    # The guard runs before routing, so an unknown path shows it without resetting real settings.
    response = client.post("/no-such-route", headers={"Origin": "https://evil.example"})
    assert response.status_code == 403
    assert client.post("/no-such-route").status_code == 404


def test_shutdown_is_not_reachable_by_get(client):
    assert client.get("/shutdown").status_code == 405


def test_a_get_with_an_origin_header_still_works(client):
    # Only writes are refused; a GET must not change anything.
    assert client.get("/no-such-route", headers={"Origin": "https://evil.example"}).status_code == 404
