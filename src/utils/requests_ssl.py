from __future__ import annotations

import logging
import os
from typing import Any

import requests

try:
    import truststore
except ImportError:
    truststore = None


_TRUSTSTORE_ACTIVE = False

if truststore is not None:
    try:
        truststore.inject_into_ssl()
        _TRUSTSTORE_ACTIVE = True
    except Exception as exc:  # third-party, runs at import: any failure falls back to the certifi bundle
        logging.warning("truststore.inject_into_ssl() failed, falling back to the certifi CA bundle: %s", exc)
        _TRUSTSTORE_ACTIVE = False


def get_requests_ssl_verify() -> bool | str:
    override = str(os.getenv("EIT_SSL_VERIFY") or "").strip().lower()
    if override in {"0", "false", "no", "off"}:
        return False

    bundle_path = str(os.getenv("EIT_SSL_CA_BUNDLE") or "").strip()
    if bundle_path:
        return bundle_path

    if _TRUSTSTORE_ACTIVE:
        return True

    try:
        return requests.certs.where()
    except (AttributeError, OSError):
        return True


def get_requests_ssl_kwargs() -> dict[str, Any]:
    return {"verify": get_requests_ssl_verify()}