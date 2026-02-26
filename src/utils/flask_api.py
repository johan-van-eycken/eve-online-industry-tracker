import logging

try:
    import streamlit as st  # pyright: ignore[reportMissingImports]
except Exception:  # pragma: no cover
    st = None
import requests  # pyright: ignore[reportMissingModuleSource]

from flask_app.settings import api_base, api_request_timeout_seconds

#-- API Helpers --
def _handle_failure(path, r):
    if st is not None:
        st.error(f"{path} failed: {r.text}")
    else:
        logging.error("%s failed: %s", path, r.text)


def _request(method, path, *, payload=None, timeout_seconds=None):
    timeout = api_request_timeout_seconds() if timeout_seconds is None else timeout_seconds
    url = f"{api_base()}{path}"

    if method == "GET":
        r = requests.get(url, timeout=timeout)
    elif method == "POST":
        r = requests.post(url, json=payload, timeout=timeout)
    elif method == "PUT":
        r = requests.put(url, json=payload, timeout=timeout)
    elif method == "DELETE":
        r = requests.delete(url, timeout=timeout)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")

    if not (200 <= r.status_code < 300):
        _handle_failure(path, r)
        return None

    return r.json()


def api_post(path, payload):
    return _request("POST", path, payload=payload)


def cached_api_get(path, timeout_seconds=None):
    return _request("GET", path, timeout_seconds=timeout_seconds)


def api_get(path, timeout_seconds=None):
    return _request("GET", path, timeout_seconds=timeout_seconds)


def api_put(path, payload):
    return _request("PUT", path, payload=payload)


def api_delete(path):
    return _request("DELETE", path)


if st is not None:
    cached_api_get = st.cache_data(ttl=300)(cached_api_get)