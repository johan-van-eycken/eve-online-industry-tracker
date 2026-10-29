"""Range presets ("Past Week", ...) on the wallet and realized profit pages
count back from today, not from the newest row."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from streamlit_ui.components.wallet_ui import filter_dataframe_by_range, resolve_range_preset

TODAY = date(2026, 10, 28)
OLD = date(2026, 8, 20)  # the last sale was two months ago


def test_past_week_is_the_last_7_days_even_without_recent_rows():
    assert resolve_range_preset("Past Week", min_date=date(2026, 1, 5), max_date=OLD, today=TODAY) == (
        date(2026, 10, 21), TODAY)


def test_year_to_date_and_all_time_end_today():
    assert resolve_range_preset("Year to Date", min_date=date(2025, 6, 1), max_date=OLD, today=TODAY) == (
        date(2026, 1, 1), TODAY)
    assert resolve_range_preset("All Time", min_date=date(2025, 6, 1), max_date=OLD, today=TODAY) == (
        date(2025, 6, 1), TODAY)


def test_an_unknown_preset_uses_the_fallback_days_and_no_rows_gives_no_range():
    assert resolve_range_preset("?", min_date=None, max_date=OLD, fallback_days=31, today=TODAY) == (
        date(2026, 9, 27), TODAY)
    assert resolve_range_preset("Past Week", min_date=None, max_date=None, today=TODAY) == (None, None)


def test_a_row_dated_after_the_local_today_stays_in_range():
    tomorrow = TODAY + timedelta(days=1)  # a UTC date ahead of the local one
    assert resolve_range_preset("Past Week", min_date=OLD, max_date=tomorrow, today=TODAY)[1] == tomorrow


def test_the_filter_drops_rows_older_than_the_window_from_today():
    today = date.today()
    df = pd.DataFrame({"date": [(today - timedelta(days=60)).isoformat() + "T12:00:00Z",
                                (today - timedelta(days=2)).isoformat() + "T12:00:00Z"]})
    assert len(filter_dataframe_by_range(df, date_column="date", preset="Past Week")) == 1
    assert filter_dataframe_by_range(df.iloc[:1], date_column="date", preset="Past Week").empty
