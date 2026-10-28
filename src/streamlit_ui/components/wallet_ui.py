from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
import streamlit as st

from streamlit_ui.api.client import api_post
from streamlit_ui.components.formatters import format_isk, type_icon_url
from streamlit_ui.state.page_preferences import load_page_preferences, save_page_preferences
from streamlit_ui.state.session_state import ensure_state_defaults, ensure_valid_state_value


RANGE_OPTIONS = ["Past Week", "Past Month", "Past 3 Months", "Year to Date", "Past Year", "All Time"]


def parse_iso_date(value: Any) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    if raw_value.endswith("Z"):
        raw_value = raw_value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw_value)
    except ValueError:
        return None


def format_wallet_datetime(value: Any) -> str:
    parsed = parse_iso_date(value)
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


#: How long a failed /locations lookup is remembered: the reruns in between
#: (each widget change) show the same warning without asking again.
LOCATION_NAMES_FAILURE_MEMO_SECONDS = 60.0

#: id tuple -> (monotonic time of the failure, reason), per process.
_location_names_failures: dict[tuple[int, ...], tuple[float, str]] = {}


class _LocationNamesFailed(RuntimeError):
    pass


def _monotonic() -> float:
    return time.monotonic()


@st.cache_data(ttl=3600, show_spinner=False)
def _location_names_cached(location_ids: tuple[int, ...]) -> dict[int, str]:
    """One /locations request for these ids; a failure raises, and
    st.cache_data keeps no result of a call that raised."""
    response = api_post("/locations", payload={"location_ids": list(location_ids)})
    if not isinstance(response, dict):
        # api_post logged the HTTP failure.
        raise _LocationNamesFailed("the /locations request failed")
    if response.get("status") not in (None, "success"):
        raise _LocationNamesFailed(str(response.get("message") or "the /locations request failed"))
    data = response.get("data") or {}
    if not isinstance(data, dict):
        raise _LocationNamesFailed("the /locations response is malformed")
    resolved: dict[int, str] = {}
    for location_id in location_ids:
        info = data.get(str(location_id)) or {}
        resolved[int(location_id)] = str((info.get("name") if isinstance(info, dict) else None) or location_id)
    return resolved


def reset_location_names_cache() -> None:
    _location_names_cached.clear()
    _location_names_failures.clear()


def fetch_location_name_map(location_ids: Iterable[Any]) -> dict[int, str]:
    """{location id: name} for the wallet tables, one /locations request per
    id set (cached). A failure warns with its reason and is remembered for
    LOCATION_NAMES_FAILURE_MEMO_SECONDS (not cached as a result), so the
    reruns in between do not ask again; {} then (the ids are shown)."""
    normalized_ids: list[int] = []
    for value in location_ids:
        try:
            if value is None or str(value).strip() == "":
                continue
            normalized_ids.append(int(value))
        except (ValueError, TypeError):
            continue
    key = tuple(sorted(set(normalized_ids)))
    if not key:
        return {}

    failure = _location_names_failures.get(key)
    if failure is not None and _monotonic() - failure[0] < LOCATION_NAMES_FAILURE_MEMO_SECONDS:
        reason = failure[1]
    else:
        _location_names_failures.pop(key, None)
        try:
            return _location_names_cached(key)
        except _LocationNamesFailed as exc:
            reason = str(exc)
            _location_names_failures[key] = (_monotonic(), reason)
            logging.warning("Wallet location names could not be loaded for %d location(s): %s", len(key), reason)
    st.warning(f"Location names could not be loaded ({reason}); showing location IDs instead.")
    return {}


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


def resolve_range_preset(
    preset: str,
    *,
    min_date: date | None,
    max_date: date | None,
    fallback_days: int = 30,
    today: date | None = None,
) -> tuple[date | None, date | None]:
    """(start, end) of a range preset over data from min_date to max_date,
    counted back from today ("Past Week" is the last 7 days, not the week
    before the newest row); an unknown preset spans fallback_days."""
    if max_date is None:
        return None, None
    anchor = today or date.today()
    # A row dated after the local today (ESI dates are UTC) stays in range.
    end_date = max(anchor, max_date)
    if preset == "All Time":
        return min_date, end_date
    if preset == "Year to Date":
        resolved_start = date(anchor.year, 1, 1)
        if min_date is not None:
            resolved_start = max(min_date, resolved_start)
        return resolved_start, end_date

    days_by_preset = {
        "Past Week": 7,
        "Past Month": 30,
        "Past 3 Months": 90,
        "Past Year": 365,
    }
    day_span = days_by_preset.get(preset, fallback_days)
    resolved_start = anchor - timedelta(days=day_span)
    if min_date is not None:
        resolved_start = max(min_date, resolved_start)
    return resolved_start, end_date


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
    parsed_dates = filtered[date_column].apply(parse_iso_date)
    date_values = parsed_dates.apply(lambda value: value.date() if value is not None else None)
    available_dates = [value for value in date_values.tolist() if value is not None]
    if not available_dates:
        return filtered

    start_date, end_date = resolve_range_preset(
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


def build_tooltip(breakdown, category, formatter=format_isk, join_labels=True):
    if category not in breakdown.index.get_level_values(0):
        return ""

    items = breakdown.loc[category].abs().sort_values(ascending=False)
    tooltip_lines = []
    if isinstance(items.index, pd.MultiIndex):
        for idx, val in items.items():
            label = " / ".join(str(x) for x in idx) if join_labels else str(idx[-1])
            tooltip_lines.append(f"<div><span>{label}</span><span>{formatter(val)}</span></div>")
    else:
        for label, val in items.items():
            tooltip_lines.append(f"<div><span>{label}</span><span>{formatter(val)}</span></div>")

    return "".join(tooltip_lines)


def range_preset_control(*, namespace: str, label: str, preference_key: str, state_key: str) -> str:
    persisted_range_preset = load_range_preset(
        namespace=namespace,
        preference_key=preference_key,
        state_key=state_key,
    )
    range_preset = st.segmented_control(
        label,
        options=RANGE_OPTIONS,
        key=state_key,
        label_visibility="collapsed",
    )
    range_preset = str(range_preset or persisted_range_preset or "Past Month")
    if range_preset != persisted_range_preset:
        save_range_preset(
            namespace=namespace,
            preference_key=preference_key,
            preset=range_preset,
        )
    return range_preset


def wallet_journal_view(journal_data: list[dict[str, Any]], *, range_preset: str) -> pd.DataFrame:
    journal_df = pd.DataFrame(journal_data)
    journal_df["category"] = journal_df["amount"].apply(lambda value: "Income" if float(value or 0.0) > 0 else "Expenses")
    journal_df = filter_dataframe_by_range(journal_df, date_column="date", preset=range_preset)
    source_location_column = "location_id" if "location_id" in journal_df.columns else "context_id"
    journal_df = apply_wallet_location_names(journal_df, source_column=source_location_column)
    if "date" in journal_df.columns:
        journal_df["date"] = journal_df["date"].apply(format_wallet_datetime)
    journal_df = reorder_columns(
        journal_df,
        [
            "date",
            "category",
            "location_name",
            "description",
            "amount",
            "balance",
            "first_party_name",
            "second_party_name",
            "reason",
            "tax_receiver_name",
            "tax",
        ],
    )
    if "date" in journal_df.columns:
        journal_df = journal_df.sort_values(by=["date"], ascending=False)
    return journal_df


def wallet_transactions_view(transactions_data: list[dict[str, Any]], *, range_preset: str) -> pd.DataFrame:
    transactions_df = pd.DataFrame(transactions_data)
    transactions_df["category"] = transactions_df.apply(
        lambda row: "Expenses" if bool(row.get("is_buy")) else "Income", axis=1
    )
    transactions_df["Icon"] = transactions_df["type_id"].apply(lambda value: type_icon_url(value, size=32))
    transactions_df = filter_dataframe_by_range(transactions_df, date_column="date", preset=range_preset)
    transactions_df = apply_wallet_location_names(transactions_df, source_column="location_id")
    if "date" in transactions_df.columns:
        transactions_df["date"] = transactions_df["date"].apply(format_wallet_datetime)
    transactions_df = reorder_columns(
        transactions_df,
        [
            "date",
            "category",
            "Icon",
            "type_name",
            "quantity",
            "total_price",
            "unit_price",
            "client_name",
            "location_name",
            "type_category_name",
            "location_id",
        ],
    )
    if "date" in transactions_df.columns:
        transactions_df = transactions_df.sort_values(by=["date"], ascending=False)
    return transactions_df
