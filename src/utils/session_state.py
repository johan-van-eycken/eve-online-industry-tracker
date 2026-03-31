from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any, TypeVar, cast

import streamlit as st


T = TypeVar("T")


def ensure_state_defaults(defaults: Mapping[str, Any]) -> None:
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def ensure_valid_state_value(
    key: str,
    default_value: T,
    *,
    valid_values: Iterable[Any] | None = None,
    coerce: Callable[[Any], T] | None = None,
) -> T:
    current_value: Any = st.session_state.get(key, default_value)

    if coerce is not None:
        try:
            current_value = coerce(current_value)
        except Exception:
            current_value = default_value

    if valid_values is not None:
        allowed_values = list(valid_values)
        if current_value not in allowed_values:
            current_value = default_value

    st.session_state[key] = current_value
    return cast(T, current_value)