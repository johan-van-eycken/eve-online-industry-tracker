from __future__ import annotations

from dataclasses import dataclass
import sys
from typing import Any

import pandas as pd
import streamlit as st

from utils.aggrid_formatters import (
    js_eu_isk_formatter,
    js_eu_number_formatter,
    js_eu_pct_formatter,
)
from utils.aggrid_import import import_aggrid


@dataclass(frozen=True)
class AgGridRuntime:
    aggrid_fn: Any
    grid_options_builder: Any
    js_code: Any
    locale: str = "nl-NL"


def require_aggrid(*, locale: str = "nl-NL") -> AgGridRuntime:
    ag = import_aggrid()
    aggrid_fn = ag.AgGrid
    grid_options_builder = ag.GridOptionsBuilder
    js_code = ag.JsCode
    import_error = ag.import_error

    if aggrid_fn is None or grid_options_builder is None or js_code is None:
        st.error(
            "streamlit-aggrid is required but could not be imported in this Streamlit process. "
            "Install it in the same Python environment and restart Streamlit."
        )
        st.caption(f"Python: {sys.executable}")
        if import_error:
            with st.expander("Import error details", expanded=False):
                st.code(import_error)
        st.stop()

    return AgGridRuntime(
        aggrid_fn=aggrid_fn,
        grid_options_builder=grid_options_builder,
        js_code=js_code,
        locale=locale,
    )


def aggrid_height(*, row_count: int, height_max: int = 700, base_height: int = 40, row_height: int = 35) -> int:
    return min(height_max, base_height + (max(0, int(row_count)) * row_height))


def render_aggrid_table(
    df_in: pd.DataFrame,
    *,
    runtime: AgGridRuntime,
    key: str | None = None,
    isk_cols: list[str] | None = None,
    pct_cols: list[str] | None = None,
    number_cols_0: list[str] | None = None,
    number_cols_2: list[str] | None = None,
    image_cols: list[str] | None = None,
    image_renderer: Any = None,
    text_right_cols: list[str] | None = None,
    height: int | None = None,
    height_max: int = 700,
    fit_columns_on_grid_load: bool = True,
    empty_message: str = "No data.",
    default_wrap_text: bool = False,
) -> None:
    if df_in is None or df_in.empty:
        st.info(empty_message)
        return

    right = {"textAlign": "right"}
    gb = runtime.grid_options_builder.from_dataframe(df_in)
    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=True,
        wrapText=default_wrap_text,
        autoHeight=False,
    )

    for col in (image_cols or []):
        if col in df_in.columns and image_renderer is not None:
            gb.configure_column(
                col,
                headerName="",
                width=60,
                pinned="left",
                cellRenderer=image_renderer,
                suppressSizeToFit=True,
            )

    for col in (isk_cols or []):
        if col in df_in.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_isk_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                cellStyle=right,
                minWidth=120,
            )

    for col in (pct_cols or []):
        if col in df_in.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_pct_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                cellStyle=right,
                minWidth=110,
            )

    for col in (number_cols_0 or []):
        if col in df_in.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
                cellStyle=right,
                minWidth=110,
            )

    for col in (number_cols_2 or []):
        if col in df_in.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                cellStyle=right,
                minWidth=110,
            )

    for col in (text_right_cols or []):
        if col in df_in.columns:
            gb.configure_column(col, cellStyle=right, minWidth=110)

    grid_options = gb.build()
    runtime.aggrid_fn(
        df_in,
        gridOptions=grid_options,
        key=key,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=height if height is not None else aggrid_height(row_count=len(df_in), height_max=height_max),
        fit_columns_on_grid_load=fit_columns_on_grid_load,
    )


def render_job_status_panel(
    *,
    title: str | None,
    is_running: bool,
    progress_fraction: float | None = None,
    progress_text: str | None = None,
    metrics: dict[str, str] | None = None,
    error_message: str | None = None,
    success_message: str | None = None,
    details: Any | None = None,
) -> None:
    with st.container(border=True):
        if title:
            st.caption(title)

        if progress_fraction is not None:
            st.progress(int(max(0.0, min(1.0, float(progress_fraction))) * 100), text=progress_text or "Working...")
        elif progress_text:
            st.caption(progress_text)

        if metrics:
            columns = st.columns(len(metrics))
            for column, (label, value) in zip(columns, metrics.items()):
                column.metric(label, value)

        if error_message:
            st.error(error_message)
        elif success_message:
            st.success(success_message)

        if details is not None:
            with st.expander("Details", expanded=False):
                st.json(details)
