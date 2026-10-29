import logging

import requests  # pyright: ignore[reportMissingModuleSource]
from requests import Response
from requests import exceptions as requests_exceptions

from flask_app.settings import api_base, api_request_timeout_seconds


def _handle_failure(path: str, response: Response) -> None:
    logging.error("%s failed with %s: %s", path, response.status_code, response.text)


def _request(method, path, *, payload=None, timeout_seconds=None):
    timeout = api_request_timeout_seconds() if timeout_seconds is None else timeout_seconds
    url = f"{api_base()}{path}"

    try:
        if method == "GET":
            response = requests.get(url, timeout=timeout)
        elif method == "POST":
            response = requests.post(url, json=payload, timeout=timeout)
        elif method == "PUT":
            response = requests.put(url, json=payload, timeout=timeout)
        elif method == "DELETE":
            response = requests.delete(url, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
    except requests_exceptions.RequestException as exc:
        logging.error("%s request failed: %s", path, exc)
        return None

    if not (200 <= response.status_code < 300):
        _handle_failure(path, response)
        return None

    try:
        return response.json()
    except ValueError:
        logging.error("%s returned invalid JSON", path)
        return None


def _request_or_error(method, path, *, payload=None, timeout_seconds=None):
    """(body, None) on success; (None, the API's error message) on failure."""
    timeout = api_request_timeout_seconds() if timeout_seconds is None else timeout_seconds
    try:
        response = requests.request(method, f"{api_base()}{path}", json=payload, timeout=timeout)
    except requests_exceptions.RequestException as exc:
        logging.error("%s request failed: %s", path, exc)
        return None, f"request failed: {exc}"
    try:
        body = response.json()
    except ValueError:
        body = None
    if not (200 <= response.status_code < 300):
        _handle_failure(path, response)
        message = (body or {}).get("message") if isinstance(body, dict) else None
        return None, message or f"HTTP {response.status_code}"
    if body is None:
        return None, "invalid JSON"
    return body, None


def api_get_or_error(path, timeout_seconds=None):
    """(body, None) on success; (None, the API's error message) on failure,
    so a page can show why instead of an empty result."""
    return _request_or_error("GET", path, timeout_seconds=timeout_seconds)


def api_post(path, payload):
    return _request("POST", path, payload=payload)


def api_get(path, timeout_seconds=None):
    return _request("GET", path, timeout_seconds=timeout_seconds)


def api_put(path, payload):
    return _request("PUT", path, payload=payload)


def api_put_or_error(path, payload, timeout_seconds=None):
    """(body, None) on success; (None, the API's error message) on failure,
    e.g. the settings route's 400 naming a refused setting and its range."""
    return _request_or_error("PUT", path, payload=payload, timeout_seconds=timeout_seconds)


def api_delete(path):
    return _request("DELETE", path)
