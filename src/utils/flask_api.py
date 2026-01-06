import logging

try:
    import streamlit as st  # pyright: ignore[reportMissingImports]
except Exception:  # pragma: no cover
    st = None
import requests  # pyright: ignore[reportMissingModuleSource]

from flask_app.settings import api_base, api_request_timeout_seconds

#-- API Helpers --
def api_post(path, payload):
    r = requests.post(
        f"{api_base()}{path}",
        json=payload,
        timeout=api_request_timeout_seconds(),
    )
    if not (200 <= r.status_code < 300):
        if st is not None:
            st.error(f"{path} failed: {r.text}")
        else:
            logging.error("%s failed: %s", path, r.text)
        return None
    return r.json()

def cached_api_get(path):
    r = requests.get(f"{api_base()}{path}", timeout=api_request_timeout_seconds())
    if not (200 <= r.status_code < 300):
        if st is not None:
            st.error(f"{path} failed: {r.text}")
        else:
            logging.error("%s failed: %s", path, r.text)
        return None
    return r.json()

def api_get(path):
    r = requests.get(f"{api_base()}{path}", timeout=api_request_timeout_seconds())
    if not (200 <= r.status_code < 300):
        if st is not None:
            st.error(f"{path} failed: {r.text}")
        else:
            logging.error("%s failed: %s", path, r.text)
        return None
    return r.json()

def api_put(path, payload):
    r = requests.put(
        f"{api_base()}{path}",
        json=payload,
        timeout=api_request_timeout_seconds(),
    )
    if not (200 <= r.status_code < 300):
        if st is not None:
            st.error(f"{path} failed: {r.text}")
        else:
            logging.error("%s failed: %s", path, r.text)
        return None
    return r.json()

def api_delete(path):
    r = requests.delete(f"{api_base()}{path}", timeout=api_request_timeout_seconds())
    if not (200 <= r.status_code < 300):
        if st is not None:
            st.error(f"{path} failed: {r.text}")
        else:
            logging.error("%s failed: %s", path, r.text)
        return None
    return r.json()


if st is not None:
    cached_api_get = st.cache_data(ttl=300)(cached_api_get)