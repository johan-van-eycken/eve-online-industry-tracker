"""Streamlit admin settings page.

Auto-generates form controls from the admin settings schema served by Flask.
"""
from __future__ import annotations

import streamlit as st

from streamlit_ui.api.client import api_get, api_put, api_post


def _load_settings() -> tuple[dict, dict] | None:
    response = api_get("/admin_settings")
    if not response or response.get("status") != "success":
        return None
    data = response.get("data") or {}
    return data.get("schema") or {}, data.get("values") or {}


def render() -> None:
    st.title("Application Settings")
    st.caption("Runtime-configurable settings. Changes are saved immediately and take effect on the next operation that reads them.")

    result = _load_settings()
    if result is None:
        st.error("Failed to load settings from backend. Is Flask running?")
        return

    schema, current_values = result

    # Track pending changes across all categories
    pending: dict[str, dict[str, object]] = {}

    for cat_key, cat_schema in schema.items():
        cat_label = cat_schema.get("label", cat_key)
        cat_values = current_values.get(cat_key, {})
        cat_settings = cat_schema.get("settings", {})

        st.subheader(cat_label)

        cols = st.columns(2)
        for i, (key, spec) in enumerate(cat_settings.items()):
            col = cols[i % 2]
            with col:
                current = cat_values.get(key, spec.get("default"))
                label = spec.get("label", key)
                help_text = spec.get("help")
                typ = spec.get("type", "str")
                widget_key = f"admin_{cat_key}_{key}"

                if typ == "int":
                    new_val = st.number_input(
                        label,
                        value=int(current),
                        min_value=spec.get("min"),
                        max_value=spec.get("max"),
                        step=spec.get("step", 1),
                        help=help_text,
                        key=widget_key,
                    )
                elif typ == "float":
                    new_val = st.number_input(
                        label,
                        value=float(current),
                        min_value=float(spec["min"]) if "min" in spec else None,
                        max_value=float(spec["max"]) if "max" in spec else None,
                        step=float(spec.get("step", 0.01)),
                        format=spec.get("format", "%.4f"),
                        help=help_text,
                        key=widget_key,
                    )
                elif typ == "bool":
                    new_val = st.checkbox(
                        label,
                        value=bool(current),
                        help=help_text,
                        key=widget_key,
                    )
                elif typ == "select":
                    options = spec.get("options", [])
                    option_values = [opt["value"] for opt in options]
                    option_labels = [opt["label"] for opt in options]
                    try:
                        current_index = option_values.index(current)
                    except ValueError:
                        current_index = 0
                    selected_label = st.selectbox(
                        label,
                        options=option_labels,
                        index=current_index,
                        help=help_text,
                        key=widget_key,
                    )
                    # Map selected label back to its value
                    new_val = option_values[option_labels.index(selected_label)]
                else:
                    new_val = st.text_input(
                        label,
                        value=str(current),
                        help=help_text,
                        key=widget_key,
                    )

                # Detect changes
                if new_val != current:
                    pending.setdefault(cat_key, {})[key] = new_val

    st.divider()

    col_save, col_reset = st.columns([1, 1])
    with col_save:
        if st.button("Save Settings", type="primary", disabled=not pending):
            resp = api_put("/admin_settings", pending)
            if resp and resp.get("status") == "success":
                st.success("Settings saved.")
                st.rerun()
            else:
                st.error("Failed to save settings.")

    with col_reset:
        if st.button("Reset to Defaults"):
            resp = api_post("/admin_settings/reset", {})
            if resp and resp.get("status") == "success":
                st.success("Settings reset to defaults.")
                st.rerun()
            else:
                st.error("Failed to reset settings.")

    if pending:
        st.info(f"{sum(len(v) for v in pending.values())} unsaved change(s)")
