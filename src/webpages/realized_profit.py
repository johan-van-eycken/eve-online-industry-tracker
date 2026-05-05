from __future__ import annotations

import altair as alt
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import streamlit as st

from utils.aggrid_formatters import js_flag_text_style, js_icon_cell_renderer
from utils.characters_api import build_character_options, fetch_characters
from utils.formatters import format_isk_eu, format_isk_short, format_pct_eu, type_icon_url
from utils.page_preferences import load_page_preferences, save_page_preferences
from utils.realized_profit_api import (
    clear_realized_profit_cache,
    fetch_realized_profit,
    refresh_realized_profit,
)
from utils.session_state import ensure_state_defaults, ensure_valid_state_value
from utils.webpage_ui import render_aggrid_table, require_aggrid


_PREFERENCES_NAMESPACE = "realized_profit"
_RANGE_OPTIONS = ["Past Week", "Past Month", "Past 3 Months", "Year to Date", "Past Year", "All Time"]
_RANGE_STATE_KEY = "realized_profit_range_preset"


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


def _format_table_datetime(value: Any) -> str:
    dt = _parse_iso_date(value)
    if dt is None:
        return str(value or "")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _character_name_map() -> dict[int, str]:
    try:
        characters = fetch_characters()
    except Exception:
        return {}
    return build_character_options(characters)


def _character_payloads() -> list[dict[str, Any]]:
    try:
        characters = fetch_characters()
    except Exception:
        return []
    return characters if isinstance(characters, list) else []


def _owner_selector_options() -> list[tuple[str, int, str]]:
    options: list[tuple[str, int, str]] = [
        ("character", 0, "All"),
    ]
    for owner_id, owner_name in sorted(_character_name_map().items(), key=lambda item: item[1].lower()):
        options.append(("character", int(owner_id), str(owner_name)))
    return options


def _source_mix_label(source_mix: Any) -> str:
    if not isinstance(source_mix, dict) or not source_mix:
        return "Untracked Inventory"

    source_labels = {
        "industry_build": "Industry Build",
        "market_buy": "Market Trade",
        "opening_inventory": "Opening Inventory",
        "untracked_inventory": "Untracked Inventory",
    }
    labels: list[str] = []
    for key in sorted(source_mix.keys()):
        payload = source_mix.get(key) or {}
        quantity = int(payload.get("quantity") or 0) if isinstance(payload, dict) else 0
        labels.append(f"{source_labels.get(str(key), str(key).replace('_', ' ').title())} ({quantity})")
    return ", ".join(labels)


def _profit_type_label(source_mix: Any) -> str:
    return _primary_profit_bucket(source_mix)


def _load_range_preset() -> str:
    persisted_preferences = load_page_preferences(_PREFERENCES_NAMESPACE)
    filters = persisted_preferences.get("filters") or {}
    default_value = str(filters.get("range_preset") or "Past Month")
    ensure_state_defaults({_RANGE_STATE_KEY: default_value})
    return ensure_valid_state_value(
        _RANGE_STATE_KEY,
        default_value,
        valid_values=_RANGE_OPTIONS,
        coerce=str,
    )


def _save_range_preset(preset: str) -> None:
    persisted_preferences = load_page_preferences(_PREFERENCES_NAMESPACE)
    filters = persisted_preferences.get("filters") or {}
    if not isinstance(filters, dict):
        filters = {}
    save_page_preferences(
        _PREFERENCES_NAMESPACE,
        {
            **persisted_preferences,
            "filters": {
                **filters,
                "range_preset": str(preset),
            },
        },
    )


def _default_date_range(rows: list[dict[str, Any]]) -> tuple[date | None, date | None]:
    dates = sorted(
        [dt.date() for dt in (_parse_iso_date(row.get("date")) for row in rows) if dt is not None]
    )
    if not dates:
        return None, None
    end_date = dates[-1]
    start_date = max(dates[0], end_date - timedelta(days=30))
    return start_date, end_date


def _available_date_span(rows: list[dict[str, Any]]) -> tuple[date | None, date | None]:
    dates = sorted(
        [dt.date() for dt in (_parse_iso_date(row.get("date")) for row in rows) if dt is not None]
    )
    if not dates:
        return None, None
    return dates[0], dates[-1]


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
    day_span = days_by_preset.get(preset, 31)
    resolved_start = max_date - timedelta(days=day_span)
    if min_date is not None:
        resolved_start = max(min_date, resolved_start)
    return resolved_start, max_date


def _filter_rows(
    rows: list[dict[str, Any]],
    *,
    selected_character_id: int | None,
    start_date: date | None,
    end_date: date | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if selected_character_id is not None and int(row.get("character_id") or 0) != int(selected_character_id):
            continue
        row_dt = _parse_iso_date(row.get("date"))
        row_date = row_dt.date() if row_dt is not None else None
        if start_date is not None and row_date is not None and row_date < start_date:
            continue
        if end_date is not None and row_date is not None and row_date > end_date:
            continue
        filtered.append(row)
    return filtered


def _summary(filtered_rows: list[dict[str, Any]]) -> dict[str, Any]:
    realized_rows = [row for row in filtered_rows if row.get("realized_profit") is not None]
    total_profit = sum(float(row.get("realized_profit") or 0.0) for row in realized_rows)
    total_net = sum(float(row.get("net_revenue") or 0.0) for row in filtered_rows)
    total_fees = sum(float(row.get("total_fees_amount") or 0.0) for row in filtered_rows)
    fully_priced_count = sum(1 for row in filtered_rows if int(row.get("unpriced_quantity") or 0) == 0)
    return {
        "total_profit": float(total_profit),
        "total_net": float(total_net),
        "total_fees": float(total_fees),
        "row_count": len(filtered_rows),
        "fully_priced_count": fully_priced_count,
        "coverage_fraction": (float(fully_priced_count) / float(len(filtered_rows))) if filtered_rows else 0.0,
    }


def _primary_profit_bucket(source_mix: Any) -> str:
    if not isinstance(source_mix, dict) or not source_mix:
        return "Trade"
    sources = {str(key) for key in source_mix.keys()}
    if "industry_build" in sources:
        return "Manufacturing"
    return "Trade"


def _period_label(preset: str) -> str:
    labels = {
        "Past Week": "7d",
        "Past Month": "30d",
        "Past 3 Months": "90d",
        "Year to Date": "YTD",
        "Past Year": "365d",
        "All Time": "All Time",
    }
    return labels.get(str(preset), str(preset))


def _period_days(start_date: date | None, end_date: date | None) -> int:
    if start_date is None or end_date is None:
        return 30
    return max(1, int((end_date - start_date).days) + 1)


def _filtered_character_wallet_transactions(
    character_payloads: list[dict[str, Any]],
    *,
    selected_owner_id: int,
    start_date: date | None,
    end_date: date | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for character in character_payloads:
        if not isinstance(character, dict):
            continue
        character_id = int(character.get("character_id") or 0)
        if selected_owner_id > 0 and character_id != int(selected_owner_id):
            continue
        for tx in character.get("wallet_transactions") or []:
            if not isinstance(tx, dict):
                continue
            tx_dt = _parse_iso_date(tx.get("date"))
            tx_date = tx_dt.date() if tx_dt is not None else None
            if start_date is not None and tx_date is not None and tx_date < start_date:
                continue
            if end_date is not None and tx_date is not None and tx_date > end_date:
                continue
            filtered.append(tx)
    return filtered


def _trade_square_metrics(
    filtered_rows: list[dict[str, Any]],
    *,
    selected_owner_id: int,
    start_date: date | None,
    end_date: date | None,
) -> dict[str, Any]:
    trade_like_rows = [row for row in filtered_rows if _primary_profit_bucket(row.get("source_mix")) == "Trade"]
    manufacturing_rows = [row for row in filtered_rows if _primary_profit_bucket(row.get("source_mix")) == "Manufacturing"]

    trade_profit = sum(float(row.get("realized_profit") or 0.0) for row in trade_like_rows if row.get("realized_profit") is not None)
    manufacturing_profit = sum(float(row.get("realized_profit") or 0.0) for row in manufacturing_rows if row.get("realized_profit") is not None)
    trade_income = sum(float(row.get("gross_revenue") or 0.0) for row in filtered_rows)
    sales_tax = sum(float(row.get("sales_tax_amount") or 0.0) for row in filtered_rows)
    broker_fees = sum(float(row.get("other_fees_amount") or 0.0) for row in filtered_rows)

    trade_purchases = 0.0
    buy_transactions = 0
    sell_transactions = 0
    wallet_transactions = _filtered_character_wallet_transactions(
        _character_payloads(),
        selected_owner_id=int(selected_owner_id),
        start_date=start_date,
        end_date=end_date,
    )
    buy_transactions = sum(1 for tx in wallet_transactions if bool(tx.get("is_buy")) is True)
    sell_transactions = sum(1 for tx in wallet_transactions if bool(tx.get("is_buy")) is False)
    trade_purchases = sum(float(tx.get("total_price") or 0.0) for tx in wallet_transactions if bool(tx.get("is_buy")) is True)

    rolling_trade_profit = float(trade_income) - float(trade_purchases) - float(sales_tax) - float(broker_fees)
    return {
        "trade_profit": float(trade_profit),
        "manufacturing_profit": float(manufacturing_profit),
        "trade_income": float(trade_income),
        "trade_purchases": float(trade_purchases),
        "sales_tax": float(sales_tax),
        "broker_fees": float(broker_fees),
        "buy_transactions": int(buy_transactions),
        "sell_transactions": int(sell_transactions),
        "rolling_trade_profit": float(rolling_trade_profit),
    }


def _render_stat_card_grid(cards: list[tuple[str, str]], *, key_prefix: str) -> None:
    st.markdown(
        """
        <style>
        .rp-stat-tile {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 0;
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 12px;
            overflow: hidden;
            background: rgba(255, 255, 255, 0.02);
        }
        .rp-stat-card {
            padding: 12px 14px;
            min-height: 70px;
            position: relative;
        }
        .rp-stat-card:not(:last-child)::after {
            content: "";
            position: absolute;
            top: 14px;
            right: 0;
            width: 1px;
            height: calc(100% - 28px);
            background: rgba(255,255,255,0.10);
        }
        .rp-stat-card-label {
            color: #d4d4d8;
            font-size: 0.78rem;
            font-weight: 600;
            line-height: 1.15;
            margin-bottom: 8px;
        }
        .rp-stat-card-value {
            color: #f4f4f5;
            font-size: 1.45rem;
            font-weight: 700;
            line-height: 1.0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    cards_html = "".join(
        f"<div class='rp-stat-card'><div class='rp-stat-card-label'>{label}</div><div class='rp-stat-card-value'>{value}</div></div>"
        for label, value in cards
    )
    st.markdown(f"<div class='rp-stat-tile'>{cards_html}</div>", unsafe_allow_html=True)


def _profit_bucket_color_scale() -> alt.Scale:
    return alt.Scale(
        domain=["Trade", "Manufacturing"],
        range=["#ec4899", "#8b9bff"],
    )


def _combined_daily_chart(overview: dict[str, Any]) -> Any:
    daily = overview.get("daily")
    running_total = overview.get("running_total")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        return None
    if not isinstance(running_total, pd.DataFrame) or running_total.empty:
        return None

    daily_reset = daily.reset_index().rename(columns={"date": "Date"})
    bucket_columns = [column for column in daily_reset.columns if column != "Date"]
    if not bucket_columns:
        return None
    daily_long = daily_reset.melt(id_vars=["Date"], value_vars=bucket_columns, var_name="Bucket", value_name="Profit")
    running_reset = running_total.reset_index().rename(columns={"date": "Date"})

    bars = (
        alt.Chart(daily_long)
        .mark_bar()
        .encode(
            x=alt.X("Date:T", title=None),
            y=alt.Y("Profit:Q", title="Daily Profit"),
            color=alt.Color("Bucket:N", scale=_profit_bucket_color_scale(), title=None),
            tooltip=[
                alt.Tooltip("yearmonthdate(Date):T", title="Date"),
                alt.Tooltip("Bucket:N", title="Type"),
                alt.Tooltip("Profit:Q", title="Profit", format=",.2f"),
            ],
        )
    )

    line = (
        alt.Chart(running_reset)
        .mark_line(color="#e5e7eb", strokeWidth=2.5)
        .encode(
            x=alt.X("Date:T", title=None),
            y=alt.Y("Running Total:Q", title="Running Total", axis=alt.Axis(orient="right")),
            tooltip=[
                alt.Tooltip("yearmonthdate(Date):T", title="Date"),
                alt.Tooltip("Running Total:Q", title="Running Total", format=",.2f"),
            ],
        )
    )

    return alt.layer(bars, line).resolve_scale(y="independent").properties(height=320)


def _hourly_profit_chart(overview: dict[str, Any]) -> Any:
    hourly = overview.get("hourly")
    if not isinstance(hourly, pd.DataFrame) or hourly.empty:
        return None

    hourly_reset = hourly.reset_index().rename(columns={"hour": "Hour"})
    bucket_columns = [column for column in hourly_reset.columns if column != "Hour"]
    if not bucket_columns:
        return None

    hourly_long = hourly_reset.melt(id_vars=["Hour"], value_vars=bucket_columns, var_name="Bucket", value_name="Profit")
    return (
        alt.Chart(hourly_long)
        .mark_bar()
        .encode(
            x=alt.X("Hour:T", title=None),
            y=alt.Y("Profit:Q", title="Hourly Profit"),
            color=alt.Color("Bucket:N", scale=_profit_bucket_color_scale(), title=None),
            tooltip=[
                alt.Tooltip("yearmonthdatehours(Hour):T", title="Hour"),
                alt.Tooltip("Bucket:N", title="Type"),
                alt.Tooltip("Profit:Q", title="Profit", format=",.2f"),
            ],
        )
        .properties(height=300)
    )


def _overview_payload(filtered_rows: list[dict[str, Any]]) -> dict[str, Any]:
    classified_rows: list[dict[str, Any]] = []
    for row in filtered_rows:
        realized_profit = _safe_float(row.get("realized_profit"))
        if realized_profit is None:
            continue
        dt = _parse_iso_date(row.get("date"))
        classified_rows.append(
            {
                "bucket": _primary_profit_bucket(row.get("source_mix")),
                "type_id": int(row.get("type_id") or 0),
                "item": str(row.get("type_name") or row.get("type_id") or "-"),
                "date": dt.date() if dt is not None else None,
                "hour": dt.replace(minute=0, second=0, microsecond=0) if dt is not None else None,
                "quantity": int(row.get("quantity") or 0),
                "net_revenue": float(row.get("net_revenue") or 0.0),
                "allocated_cost": float(row.get("allocated_cost") or 0.0),
                "realized_profit": float(realized_profit),
            }
        )

    if not classified_rows:
        empty = pd.DataFrame()
        return {
            "daily": empty,
            "hourly": empty,
            "running_total": empty,
            "manufacturing": empty,
            "trade": empty,
            "profit_by_bucket": {"Manufacturing": 0.0, "Trade": 0.0},
        }

    overview_df = pd.DataFrame(classified_rows)

    daily = (
        overview_df.dropna(subset=["date"])
        .groupby(["date", "bucket"], as_index=False)["realized_profit"]
        .sum()
        .pivot(index="date", columns="bucket", values="realized_profit")
        .fillna(0.0)
        .sort_index()
    )
    running_total = pd.DataFrame(index=daily.index)
    running_total["Running Total"] = daily.sum(axis=1).cumsum()

    hourly = (
        overview_df.dropna(subset=["hour"])
        .groupby(["hour", "bucket"], as_index=False)["realized_profit"]
        .sum()
        .pivot(index="hour", columns="bucket", values="realized_profit")
        .fillna(0.0)
        .sort_index()
    )
    if len(hourly.index) > 48:
        hourly = hourly.tail(48)

    manufacturing = (
        overview_df[overview_df["bucket"] == "Manufacturing"]
        .groupby(["type_id", "item"], as_index=False)
        .agg(
            {
                "quantity": "sum",
                "realized_profit": "sum",
            }
        )
        .assign(
            Icon=lambda df: df["type_id"].apply(lambda value: type_icon_url(value, size=32)),
            _negative_flag=lambda df: df["realized_profit"].astype(float) < 0.0,
        )
        .rename(columns={"item": "Item", "quantity": "Qty Sold", "realized_profit": "Realized Profit"})
        [["Icon", "Item", "Qty Sold", "Realized Profit", "_negative_flag"]]
        .sort_values(["Realized Profit", "Qty Sold"], ascending=[False, False])
    )

    trade = (
        overview_df[overview_df["bucket"] == "Trade"]
        .groupby(["type_id", "item"], as_index=False)
        .agg(
            {
                "quantity": "sum",
                "realized_profit": "sum",
            }
        )
        .assign(
            Icon=lambda df: df["type_id"].apply(lambda value: type_icon_url(value, size=32)),
            _negative_flag=lambda df: df["realized_profit"].astype(float) < 0.0,
        )
        .rename(columns={"item": "Item", "quantity": "Qty Sold", "realized_profit": "Realized Profit"})
        [["Icon", "Item", "Qty Sold", "Realized Profit", "_negative_flag"]]
        .sort_values(["Realized Profit", "Qty Sold"], ascending=[False, False])
    )

    profit_by_bucket = {
        bucket: float(
            overview_df.loc[overview_df["bucket"] == bucket, "realized_profit"].sum()
        )
        for bucket in ["Manufacturing", "Trade"]
    }

    return {
        "daily": daily,
        "hourly": hourly,
        "running_total": running_total,
        "manufacturing": manufacturing,
        "trade": trade,
        "profit_by_bucket": profit_by_bucket,
    }


def _table_frame(
    rows: list[dict[str, Any]],
    owner_name_by_id: dict[int, str],
    *,
    owner_label: str,
    owner_id_key: str,
) -> pd.DataFrame:
    table_rows = []
    for row in rows:
        realized_margin_fraction = row.get("realized_margin_fraction")
        realized_profit = row.get("realized_profit")
        margin_pct = (float(realized_margin_fraction) * 100.0) if realized_margin_fraction is not None else None
        negative_flag = (
            realized_profit is not None
            and margin_pct is not None
            and float(realized_profit) < 0.0
            and float(margin_pct) < 0.0
        )
        table_rows.append(
            {
                "Date": _format_table_datetime(row.get("date")),
                owner_label: owner_name_by_id.get(int(row.get(owner_id_key) or 0), str(row.get(owner_id_key) or "-")),
                "Icon": type_icon_url(row.get("type_id"), size=32),
                "Item": str(row.get("type_name") or row.get("type_id") or "-"),
                "Qty": int(row.get("quantity") or 0),
                "Net Revenue": row.get("net_revenue"),
                "Allocated Cost": row.get("allocated_cost"),
                "Realized Profit": realized_profit,
                "Margin %": margin_pct,
                "Confidence": row.get("confidence"),
                "Priced Qty": int(row.get("priced_quantity") or 0),
                "Unpriced Qty": int(row.get("unpriced_quantity") or 0),
                "Transaction ID": int(row.get("transaction_id") or 0),
                "Profit Type": _profit_type_label(row.get("source_mix")),
                "_negative_flag": bool(negative_flag),
            }
        )
    frame = pd.DataFrame(table_rows)
    if not frame.empty:
        desired_columns = [
            "Date",
            owner_label,
            "Icon",
            "Item",
            "Qty",
            "Net Revenue",
            "Allocated Cost",
            "Realized Profit",
            "Margin %",
            "Confidence",
            "Priced Qty",
            "Unpriced Qty",
            "Transaction ID",
            "Profit Type",
            "_negative_flag",
        ]
        frame = frame[[column for column in desired_columns if column in frame.columns]]
    return frame


def render() -> None:
    st.header("Realized Profit")
    runtime = require_aggrid()

    character_name_by_id = _character_name_map()
    owner_selector_options = _owner_selector_options()

    controls_owner, controls_range, controls_refresh, controls_about = st.columns([2.5, 4.5, 1.5, 1.0])
    with controls_owner:
        selected_owner_option = st.selectbox(
            "Owner",
            options=owner_selector_options,
            format_func=lambda option: str(option[2] if isinstance(option, tuple) and len(option) >= 3 else "-"),
        )

    selected_owner_id = int(selected_owner_option[1] if isinstance(selected_owner_option, tuple) and len(selected_owner_option) >= 2 else 0)

    owner_name_by_id = character_name_by_id
    owner_label = "Character"
    owner_id_key = "character_id"
    with controls_range:
        persisted_range_preset = _load_range_preset()
        range_preset = st.segmented_control(
            "Range",
            options=_RANGE_OPTIONS,
            key=_RANGE_STATE_KEY,
        )
        range_preset = str(range_preset or persisted_range_preset or "Past Month")
        if range_preset != persisted_range_preset:
            _save_range_preset(range_preset)
    with controls_refresh:
        st.write("")
        st.write("")
        if st.button("Refresh Ledger", type="primary", width="stretch"):
            with st.spinner("Rebuilding realized profit ledger..."):
                refresh_realized_profit(
                    owner_scope="character",
                    owner_id=(None if int(selected_owner_id) == 0 else int(selected_owner_id)),
                )
            clear_realized_profit_cache()
            st.rerun()
    with controls_about:
        st.write("")
        st.write("")
        if hasattr(st, "popover"):
            with st.popover("?", help="About this page"):
                st.caption(
                    "Realized sales are matched against FIFO cost lots from prior market buys and completed industry jobs. "
                    "Character scope supports wallet journal-backed fee capture when linked."
                )
                st.caption(
                    "Profit Type groups sales into Manufacturing or Trade for readability. "
                    "Trade includes untracked inventory, older stock outside tracked history, transfers, loot, donations, and similar cases where no historical source lot was matched."
                )
                st.caption(
                    "Confidence indicates how complete the pricing evidence is for a sale. "
                    "High means the sale is fully priced with strong source matching, Medium means the core pricing is present but some parts rely on inferred or estimated inputs, and Low means the row still has notable gaps or weaker historical support."
                )
                st.caption("Coverage excludes rows with missing historical cost basis.")
        else:
            with st.expander("?", expanded=False):
                st.caption(
                    "Realized sales are matched against FIFO cost lots from prior market buys and completed industry jobs. "
                    "Character scope supports wallet journal-backed fee capture when linked."
                )
                st.caption(
                    "Profit Type groups sales into Manufacturing or Trade for readability. "
                    "Trade includes untracked inventory, older stock outside tracked history, transfers, loot, donations, and similar cases where no historical source lot was matched."
                )
                st.caption(
                    "Confidence indicates how complete the pricing evidence is for a sale. "
                    "High means the sale is fully priced with strong source matching, Medium means the core pricing is present but some parts rely on inferred or estimated inputs, and Low means the row still has notable gaps or weaker historical support."
                )
                st.caption("Coverage excludes rows with missing historical cost basis.")

    response = fetch_realized_profit(
        owner_scope="character",
        owner_id=(None if int(selected_owner_id) == 0 else int(selected_owner_id)),
    )
    if response.get("status") not in {None, "success"}:
        st.error(response.get("message") or "Failed to load realized profit data")
        return

    payload = response.get("data") or {}
    rows = payload.get("rows") or []
    if not isinstance(rows, list) or not rows:
        st.info("No realized sales have been recorded yet for the selected scope.")
        return

    min_available_date, max_available_date = _available_date_span(rows)
    start_date, end_date = _resolve_range_preset(
        str(range_preset),
        min_date=min_available_date,
        max_date=max_available_date,
    )

    filtered_rows = _filter_rows(
        rows,
        selected_character_id=(None if int(selected_owner_id) == 0 else int(selected_owner_id)),
        start_date=start_date,
        end_date=end_date,
    )

    summary = _summary(filtered_rows)
    if not filtered_rows:
        st.warning("No sales match the current filters.")
        return

    overview = _overview_payload(filtered_rows)
    period_label = _period_label(str(range_preset))
    square_metrics = _trade_square_metrics(
        filtered_rows,
        selected_owner_id=int(selected_owner_id),
        start_date=start_date,
        end_date=end_date,
    )

    groups_left, groups_right = st.columns([4, 6])
    with groups_left:
        _render_stat_card_grid(
            [
                (f"{period_label} Net Profit", format_isk_short(summary["total_profit"])),
                ("Trade", format_isk_short(square_metrics["trade_profit"])),
                ("Manufacturing", format_isk_short(square_metrics["manufacturing_profit"])),
            ],
            key_prefix="realized_profit_group1",
        )
    with groups_right:
        _render_stat_card_grid(
            [
                (f"{period_label} Rolling Trade Profit", format_isk_short(square_metrics["rolling_trade_profit"])),
                ("Trade Income", format_isk_short(square_metrics["trade_income"])),
                ("Trade Purchases", format_isk_short(-square_metrics["trade_purchases"])),
                ("Sales Tax", format_isk_short(-square_metrics["sales_tax"])),
                ("Broker Fees", format_isk_short(-square_metrics["broker_fees"])),
                ("Buy Transactions", f"{int(square_metrics['buy_transactions'])}"),
                ("Sell Transactions", f"{int(square_metrics['sell_transactions'])}"),
            ],
            key_prefix="realized_profit_group2",
        )

    st.subheader("Profit Overview")
    chart_left, chart_right = st.columns(2)
    with chart_left:
        st.markdown("**Daily Profits**")
        combined_chart = _combined_daily_chart(overview)
        if combined_chart is not None:
            st.altair_chart(combined_chart, width="stretch")
        else:
            st.info("No daily realized-profit data for the selected filters.")

    with chart_right:
        st.markdown("**Hourly Profits**")
        hourly_chart = _hourly_profit_chart(overview)
        if hourly_chart is not None:
            st.altair_chart(hourly_chart, width="stretch")
        else:
            st.info("No hourly realized-profit data for the selected filters.")

    table_left, table_right = st.columns(2)
    with table_left:
        st.markdown("**Trade Profits**")
        trade_df = overview["trade"]
        if isinstance(trade_df, pd.DataFrame) and not trade_df.empty:
            render_aggrid_table(
                trade_df,
                runtime=runtime,
                key=f"realized_profit_trade_summary_character_{int(selected_owner_id)}",
                isk_cols=["Realized Profit"],
                number_cols_0=["Qty Sold"],
                image_cols=["Icon"],
                image_renderer=js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24),
                hidden_cols=["_negative_flag"],
                column_configs={
                    "Item": {
                        "minWidth": 220,
                        "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag"),
                    },
                    "Realized Profit": {
                        "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag", align="right"),
                    },
                },
                fit_columns_on_grid_load=False,
                height_max=340,
            )
        else:
            st.info("No trade-profit rows in the selected range.")

    with table_right:
        st.markdown("**Manufacturing Profits**")
        manufacturing_df = overview["manufacturing"]
        if isinstance(manufacturing_df, pd.DataFrame) and not manufacturing_df.empty:
            render_aggrid_table(
                manufacturing_df,
                runtime=runtime,
                key=f"realized_profit_manufacturing_summary_character_{int(selected_owner_id)}",
                isk_cols=["Realized Profit"],
                number_cols_0=["Qty Sold"],
                image_cols=["Icon"],
                image_renderer=js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24),
                hidden_cols=["_negative_flag"],
                column_configs={
                    "Item": {
                        "minWidth": 220,
                        "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag"),
                    },
                    "Realized Profit": {
                        "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag", align="right"),
                    },
                },
                fit_columns_on_grid_load=False,
                height_max=340,
            )
        else:
            st.info("No manufacturing-profit rows in the selected range.")

    table_df = _table_frame(
        filtered_rows,
        owner_name_by_id,
        owner_label=owner_label,
        owner_id_key=owner_id_key,
    )
    render_aggrid_table(
        table_df,
        runtime=runtime,
        key=f"realized_profit_character_{int(selected_owner_id)}",
        isk_cols=["Net Revenue", "Allocated Cost", "Realized Profit"],
        pct_cols=["Margin %"],
        number_cols_0=["Qty", "Priced Qty", "Unpriced Qty", "Transaction ID"],
        image_cols=["Icon"],
        image_renderer=js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24),
        image_pin_left=False,
        hidden_cols=["Priced Qty", "Unpriced Qty", "Transaction ID", "_negative_flag"],
        column_configs={
            "Item": {
                "minWidth": 220,
                "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag"),
            },
            "Realized Profit": {
                "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag", align="right"),
            },
            "Margin %": {
                "cellStyle": js_flag_text_style(JsCode=runtime.js_code, flag_field="_negative_flag", align="right"),
            },
            "Profit Type": {
                "minWidth": 130,
            },
        },
        fit_columns_on_grid_load=False,
        height_max=720,
    )