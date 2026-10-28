"""api_get_or_error and api_put_or_error share one request path: the body on
success, the API's own message (or the HTTP status) on failure."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from requests import exceptions as requests_exceptions

from streamlit_ui.api import client


def _response(status, body):
    def _json():
        if body is None:
            raise ValueError("no JSON")
        return body
    return SimpleNamespace(status_code=status, json=_json, text=str(body))


@pytest.fixture()
def sent(monkeypatch):
    calls = []
    replies = []
    monkeypatch.setattr(client, "api_base", lambda: "http://api")

    def fake_request(method, url, json=None, timeout=None):
        calls.append((method, url, json))
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(client.requests, "request", fake_request)
    return SimpleNamespace(calls=calls, replies=replies)


def test_get_and_put_return_the_body_on_success(sent):
    sent.replies += [_response(200, {"status": "success"}), _response(200, {"status": "success", "data": 1})]
    assert client.api_get_or_error("/admin_settings", timeout_seconds=3) == ({"status": "success"}, None)
    assert client.api_put_or_error("/admin_settings", {"a": 1}) == ({"status": "success", "data": 1}, None)
    assert sent.calls == [("GET", "http://api/admin_settings", None), ("PUT", "http://api/admin_settings", {"a": 1})]


def test_a_failure_gives_the_api_message_or_the_status(sent):
    sent.replies += [_response(400, {"status": "error", "message": "x is out of range"}),
                     _response(503, None), _response(200, None),
                     requests_exceptions.ConnectionError("refused")]
    assert client.api_put_or_error("/admin_settings", {}) == (None, "x is out of range")
    assert client.api_get_or_error("/x") == (None, "HTTP 503")
    assert client.api_get_or_error("/x") == (None, "invalid JSON")
    body, reason = client.api_get_or_error("/x")
    assert body is None and reason.startswith("request failed: ")
