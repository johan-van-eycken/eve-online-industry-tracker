from __future__ import annotations

from flask_app.state import state


def ensure_sde_ready() -> None:
    if state.db_sde is None:
        raise RuntimeError("SDE DB not initialized")


def get_language() -> str:
    if state.db_sde is None:
        return "en"
    return state.db_sde.language or "en"
