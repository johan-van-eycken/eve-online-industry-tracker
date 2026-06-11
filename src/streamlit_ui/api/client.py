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


def api_post(path, payload):
    return _request("POST", path, payload=payload)


def cached_api_get(path, timeout_seconds=None):
    return api_get(path, timeout_seconds=timeout_seconds)


def api_get(path, timeout_seconds=None):
    return _request("GET", path, timeout_seconds=timeout_seconds)


def api_put(path, payload):
    return _request("PUT", path, payload=payload)


def api_delete(path):
    return _request("DELETE", path)
