from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
import streamlit as st

from streamlit_ui.api.client import api_post
from streamlit_ui.state.page_preferences import load_page_preferences, save_page_preferences
from streamlit_ui.state.session_state import ensure_state_defaults, ensure_valid_state_value


RANGE_OPTIONS = ["Past Week", "Past Month", "Past 3 Months", "Year to Date", "Past Year", "All Time"]


def _parse_iso_date(value: Any) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    if raw_value.endswith("Z"):
        raw_value = raw_value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw_value)
    except Exception:
        return None


def format_wallet_datetime(value: Any) -> str:
    parsed = _parse_iso_date(value)
    if parsed is None:
        return str(value or "")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def camel_case_header(column_name: str) -> str:
    raw = str(column_name or "").strip()
    if not raw:
        return ""
    if "_" not in raw:
        return raw[:1].upper() + raw[1:]
    return "".join(part[:1].upper() + part[1:] for part in raw.split("_") if part)


def fetch_location_name_map(location_ids: Iterable[Any]) -> dict[int, str]:
    normalized_ids: list[int] = []
    for value in location_ids:
        try:
            if value is None or str(value).strip() == "":
                continue
            normalized_ids.append(int(value))
        except Exception:
            continue
    if not normalized_ids:
        return {}

    try:
        response = api_post("/locations", payload={"location_ids": normalized_ids}) or {}
    except Exception:
        return {}

    data = response.get("data") or {}
    resolved: dict[int, str] = {}
    for location_id in normalized_ids:
        info = data.get(str(location_id)) or {}
        name = str(info.get("name") or location_id)
        resolved[int(location_id)] = name
    return resolved


def apply_wallet_location_names(
    df: pd.DataFrame,
    *,
    source_column: str,
    target_column: str = "location_name",
) -> pd.DataFrame:
    if df is None or df.empty or source_column not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    out = df.copy()
    location_map = fetch_location_name_map(out[source_column].dropna().unique().tolist())
    if not location_map:
        return out

    out[target_column] = out[source_column].apply(
        lambda value: location_map.get(int(value), str(value)) if pd.notna(value) else None
    )
    return out


def _resolve_range_preset(
    preset: str,
    *,
    min_date: date | None,
    max_date: date | None,
) -> tuple[date | None, date | None]:
    if max_date is None:
        return None, None
    if preset == "All Time":
        return min_date, max_date
    if preset == "Year to Date":
        resolved_start = date(max_date.year, 1, 1)
        if min_date is not None:
            resolved_start = max(min_date, resolved_start)
        return resolved_start, max_date

    days_by_preset = {
        "Past Week": 7,
        "Past Month": 30,
        "Past 3 Months": 90,
        "Past Year": 365,
    }
    day_span = days_by_preset.get(preset, 30)
    resolved_start = max_date - timedelta(days=day_span)
    if min_date is not None:
        resolved_start = max(min_date, resolved_start)
    return resolved_start, max_date


def load_range_preset(*, namespace: str, preference_key: str, state_key: str) -> str:
    persisted_preferences = load_page_preferences(namespace)
    filters = persisted_preferences.get("filters") or {}
    default_value = str(filters.get(preference_key) or "Past Month")
    ensure_state_defaults({state_key: default_value})
    return ensure_valid_state_value(
        state_key,
        default_value,
        valid_values=RANGE_OPTIONS,
        coerce=str,
    )


def save_range_preset(*, namespace: str, preference_key: str, preset: str) -> None:
    persisted_preferences = load_page_preferences(namespace)
    filters = persisted_preferences.get("filters") or {}
    if not isinstance(filters, dict):
        filters = {}
    save_page_preferences(
        namespace,
        {
            **persisted_preferences,
            "filters": {
                **filters,
                preference_key: str(preset),
            },
        },
    )


def filter_dataframe_by_range(df: pd.DataFrame, *, date_column: str, preset: str) -> pd.DataFrame:
    if df is None or df.empty or date_column not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    filtered = df.copy()
    parsed_dates = filtered[date_column].apply(_parse_iso_date)
    date_values = parsed_dates.apply(lambda value: value.date() if value is not None else None)
    available_dates = [value for value in date_values.tolist() if value is not None]
    if not available_dates:
        return filtered

    start_date, end_date = _resolve_range_preset(
        str(preset),
        min_date=min(available_dates),
        max_date=max(available_dates),
    )
    if start_date is None and end_date is None:
        return filtered

    mask = date_values.apply(
        lambda value: True
        if value is None
        else (start_date is None or value >= start_date) and (end_date is None or value <= end_date)
    )
    return filtered.loc[mask].copy()


def reorder_columns(df: pd.DataFrame, ordered_columns: Iterable[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    preferred = [column for column in ordered_columns if column in df.columns]
    remaining = [column for column in df.columns if column not in preferred]
    return df[preferred + remaining]


def render_income_expense_tile(
    *,
    income_label: str,
    income_value: str,
    income_tooltip: str,
    expense_label: str,
    expense_value: str,
    expense_tooltip: str,
) -> None:
    st.markdown(
        """
        <style>
        .wallet-summary-tile {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 12px;
            overflow: visible;
            background: rgba(255,255,255,0.02);
            margin-bottom: 1rem;
        }
        .wallet-summary-card {
            position: relative;
            padding: 12px 14px;
            min-height: 72px;
            overflow: visible;
        }
        .wallet-summary-card:not(:last-child)::after {
            content: "";
            position: absolute;
            top: 14px;
            right: 0;
            width: 1px;
            height: calc(100% - 28px);
            background: rgba(255,255,255,0.10);
        }
        .wallet-summary-card .tooltip {
            position: relative;
            display: block;
            margin-bottom: 0;
            cursor: pointer;
        }
        .wallet-summary-card .tooltip .tooltiptext {
            visibility: hidden;
            position: absolute;
            z-index: 10;
            bottom: 125%;
            left: 50%;
            transform: translateX(-50%);
            opacity: 0;
            transition: opacity 0.2s ease;
            background-color: #1e293b;
            color: #f0f0f0;
            text-align: left;
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.5);
            font-size: 13px;
            line-height: 1.35;
            white-space: nowrap;
            min-width: 280px;
        }
        .wallet-summary-card .tooltip .tooltiptext div {
            display: flex;
            justify-content: space-between;
        }
        .wallet-summary-card .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
        .wallet-summary-label {
            color: #d4d4d8;
            font-size: 0.78rem;
            font-weight: 600;
            line-height: 1.15;
            margin-bottom: 8px;
        }
        .wallet-summary-value {
            color: #f4f4f5;
            font-size: 1.35rem;
            font-weight: 700;
            line-height: 1.0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            "<div class='wallet-summary-tile'>"
            f"<div class='wallet-summary-card'><div class='tooltip'><div class='wallet-summary-label'>{income_label}</div><div class='wallet-summary-value'>{income_value}</div><span class='tooltiptext'>{income_tooltip}</span></div></div>"
            f"<div class='wallet-summary-card'><div class='tooltip'><div class='wallet-summary-label'>{expense_label}</div><div class='wallet-summary-value'>{expense_value}</div><span class='tooltiptext'>{expense_tooltip}</span></div></div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )