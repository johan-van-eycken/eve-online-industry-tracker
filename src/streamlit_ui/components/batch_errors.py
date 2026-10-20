"""One warning per character or corporation a batch read could not serve.

The Flask routes over several characters or corporations reply with the
data of each that worked plus `errors`: {owner name: error message} for each
that failed (flask_app.http.ok_batch); when every one fails the reply is an
error instead.
"""
from __future__ import annotations

from typing import Any

import streamlit as st


def batch_errors(response: Any) -> dict[str, str]:
    """{owner name: error message} of a batch reply; {} when none failed."""
    errors = response.get("errors") if isinstance(response, dict) else None
    return {str(name): str(message) for name, message in errors.items()} if isinstance(errors, dict) else {}


def show_batch_errors(response: Any, owner_kind: str) -> None:
    """st.warning naming each failed owner ("Character", "Corporation")."""
    show_errors(batch_errors(response), owner_kind)


def show_errors(errors: dict[str, str], owner_kind: str) -> None:
    for name, message in sorted(errors.items()):
        st.warning(f"{owner_kind} {name} failed: {message}. Its data is not shown.")
