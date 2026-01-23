from __future__ import annotations

from typing import cast

from flask import current_app

from flask_app.state import AppState, state as default_state


def get_state() -> AppState:
    """Return the AppState for the current Flask app.

    This avoids importing the module-level `state` in every route and makes it
    possible to swap the state instance in tests or alternative entrypoints.
    """

    try:
        app = current_app._get_current_object()
    except RuntimeError:
        # No Flask app context (e.g., startup code or background threads).
        return default_state

    return cast(AppState, app.extensions.get("app_state", default_state))
