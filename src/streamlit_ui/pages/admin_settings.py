"""Streamlit admin settings page.

Auto-generates form controls from the admin settings schema served by Flask.
"""
from __future__ import annotations

import streamlit as st

from streamlit_ui.api.client import api_get, api_get_or_error, api_post, api_put_or_error


def _load_settings() -> tuple[dict, dict, dict] | None:
    response = api_get("/admin_settings")
    if not response or response.get("status") != "success":
        return None
    data = response.get("data") or {}
    for notice in data.get("notices") or []:
        # "ERROR:": admin_settings.json is unreadable, so its settings read as their defaults.
        if str(notice).startswith("ERROR:"):
            st.error(notice)
        else:
            st.warning(notice)
    return data.get("schema") or {}, data.get("values") or {}, data.get("invalid") or {}


#: _pending_value's "leave this setting alone" (None is a value: an optional setting cleared).
NO_CHANGE = object()


def _consequence(spec: dict, invalid_entry: dict | None) -> str:
    if (invalid_entry or {}).get("startup_critical") or spec.get("startup_critical"):
        return f"until then the app uses the default {spec.get('default')!r}."
    return "until then the features that read it fail with an error."


def _local_number_problem(spec: dict, current: object) -> str | None:
    """Why `current` is not a valid value of this int/float setting, checked
    here too so the page never shows a value the backend would refuse."""
    if spec.get("optional") and current is None:
        return None
    is_int = spec.get("type") == "int"
    if isinstance(current, bool) or not isinstance(current, (int, float)) or (is_int and not float(current).is_integer()):
        return f"{current!r} is not {'an integer' if is_int else 'a number'}"
    if ("min" in spec and current < spec["min"]) or ("max" in spec and current > spec["max"]):
        return f"{current!r} is outside {spec.get('min')!r} to {spec.get('max')!r}"
    return None


def _number_choice(spec: dict, current: object, invalid_entry: dict | None) -> tuple[dict, str | None]:
    """(st.number_input's value/min/max/step(/format), problem) for an int or
    float setting. A stored invalid value (the backend's `invalid` entry, or
    one this page finds outside the bounds) is never clamped into range or
    replaced by the default: the input is left empty and the problem is shown,
    so a save does not write a value the user did not choose over it (and
    st.number_input, which raises on a value outside its bounds, does not
    crash the page)."""
    is_int = spec.get("type") == "int"
    cast = int if is_int else float
    min_value = cast(spec["min"]) if "min" in spec else None
    max_value = cast(spec["max"]) if "max" in spec else None
    reason = (invalid_entry or {}).get("problem") or _local_number_problem(spec, current)
    problem = None
    if reason:
        problem = (f"The stored value {current!r} is invalid ({reason}). Enter a valid value and save; "
                   + _consequence(spec, invalid_entry))
        value = None
    elif spec.get("optional") and current is None:
        value = None  # not set: an empty input, not a number the user did not choose
    else:
        value = cast(current)
    args: dict = {"value": value, "min_value": min_value, "max_value": max_value}
    if is_int:
        args["step"] = int(spec.get("step", 1))
    else:
        args["step"] = float(spec.get("step", 0.01))
        args["format"] = spec.get("format", "%.4f")
    return args, problem


def _pending_value(*, new: object, current: object, problem: str | None) -> object:
    """The value to save for a setting, or NO_CHANGE. A setting with a stored
    invalid value is only saved when the user enters one (its input starts
    empty)."""
    if problem and new is None:
        return NO_CHANGE
    return NO_CHANGE if new == current and not problem else new


class _StationNameFailed(RuntimeError):
    pass


@st.cache_data(ttl=600, show_spinner=False)
def _station_name_data(station_id: int) -> dict:
    """/npc_stations/<id>/name's data, cached per station (every rerun of the
    settings page used to ask again). A failed request raises, and
    st.cache_data keeps no result of a call that raised."""
    body, failure = api_get_or_error(f"/npc_stations/{station_id}/name")
    if failure:
        raise _StationNameFailed(failure)
    data = (body or {}).get("data") if isinstance(body, dict) else None
    return data if isinstance(data, dict) else {}


def _station_caption(station_id: object) -> tuple[str | None, str | None]:
    """(caption, problem) for the reprocessing/npc_station_id input: the
    station's full name from the SDE, or why it has none (never an error
    for the page)."""
    if station_id is None or isinstance(station_id, bool) or not isinstance(station_id, int):
        return None, None
    try:
        data = _station_name_data(station_id)
    except _StationNameFailed as exc:
        return None, f"NPC station {station_id}: the name could not be read ({exc})"
    if data.get("name"):
        note = f" ({data['note']})" if data.get("note") else ""
        return f"NPC station {station_id}: {data['name']}{note}", None
    return None, f"NPC station {station_id}: {data.get('problem') or 'no name in the SDE'}"


def _select_choices(spec: dict, current: object,
                    invalid_entry: dict | None = None) -> tuple[list, list[str], int, str | None]:
    """(values, labels, index of current, problem) for a select setting. A
    stored value that is not an option (an invalid strict value) is shown as
    its own entry and selected, with a problem to show, so the page does not
    pre-select another option that a save would then write over it."""
    options = spec.get("options", [])
    values = [opt["value"] for opt in options]
    labels = [opt["label"] for opt in options]
    if current in values:
        return values, labels, values.index(current), None
    problem = (f"The stored value {current!r} is not a valid option. Pick a valid one and save; "
               + _consequence(spec, invalid_entry))
    return [current, *values], [f"Invalid stored value: {current!r}", *labels], 0, problem


def _choice_to_save(new: object, current: object, *, problem: str | None) -> object:
    """The value to save for a select or bool setting, or NO_CHANGE. Type-aware
    while a problem is shown: picking On over a stored invalid 1.0 saves True
    (True == 1.0 would read as no change, so it could never be fixed); only
    keeping the stored value's own entry is no change."""
    if problem:
        return NO_CHANGE if (new == current and type(new) is type(current)) else new
    return NO_CHANGE if new == current else new


def _bool_choices(spec: dict, current: object,
                  invalid_entry: dict | None = None) -> tuple[list, list[str], int, str | None] | None:
    """None for a valid bool setting (its checkbox). For a stored invalid
    value (the backend's `invalid` entry, or not a bool): (values, labels,
    index of current, problem) for a selectbox that shows the stored value as
    its own selected entry, so the page does not show a checkbox state that
    a save would then write over it."""
    if invalid_entry is None and isinstance(current, bool):
        return None
    reason = (invalid_entry or {}).get("problem") or f"{current!r} is not a boolean"
    problem = (f"The stored value {current!r} is invalid ({reason}). Pick On or Off and save; "
               + _consequence(spec, invalid_entry))
    return [current, True, False], [f"Invalid stored value: {current!r}", "On", "Off"], 0, problem


def render() -> None:
    st.title("Application Settings")
    st.caption("Runtime-configurable settings. Changes are saved immediately and take effect on the next operation that reads them.")

    result = _load_settings()
    if result is None:
        st.error("Failed to load settings from backend. Is Flask running?")
        return

    schema, current_values, invalid_values = result

    # Track pending changes across all categories
    pending: dict[str, dict[str, object]] = {}

    for cat_key, cat_schema in schema.items():
        cat_label = cat_schema.get("label", cat_key)
        cat_values = current_values.get(cat_key, {})
        cat_invalid = invalid_values.get(cat_key, {}) or {}
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
                invalid_entry = cat_invalid.get(key)
                problem = None

                if typ in ("int", "float"):
                    number_args, problem = _number_choice(spec, current, invalid_entry)
                    if problem:
                        st.error(f"{label}: {problem}")
                    new_val = st.number_input(
                        label,
                        **number_args,
                        help=help_text,
                        key=widget_key,
                    )
                    if (cat_key, key) == ("reprocessing", "npc_station_id") and not problem:
                        station_caption, station_problem = _station_caption(current)
                        if station_caption:
                            st.caption(station_caption)
                        elif station_problem:
                            st.warning(station_problem)
                elif typ == "bool" and _bool_choices(spec, current, invalid_entry) is None:
                    new_val = st.checkbox(
                        label,
                        value=bool(current),
                        help=help_text,
                        key=widget_key,
                    )
                elif typ in ("select", "bool"):
                    option_values, option_labels, current_index, problem = (
                        _select_choices(spec, current, invalid_entry) if typ == "select"
                        else _bool_choices(spec, current, invalid_entry))
                    if problem:
                        st.error(f"{label}: {problem}")
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
                if typ == "select" or (typ == "bool" and problem):
                    # A stored invalid select or bool stays selected (its own entry) until another option is picked.
                    to_save = _choice_to_save(new_val, current, problem=problem)
                else:
                    to_save = _pending_value(new=new_val, current=current, problem=problem)
                if to_save is not NO_CHANGE:
                    pending.setdefault(cat_key, {})[key] = to_save

    st.divider()

    col_save, col_reset = st.columns([1, 1])
    with col_save:
        if st.button("Save Settings", type="primary", disabled=not pending):
            resp, save_problem = api_put_or_error("/admin_settings", pending)
            if resp and resp.get("status") == "success":
                st.success("Settings saved.")
                st.rerun()
            else:
                # The backend names the refused setting and its range (nothing is saved then).
                st.error(f"Settings not saved: {save_problem or 'unknown error'}")

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
