import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import sys
import traceback
from typing import Any, cast

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # type: ignore
except Exception:  # pragma: no cover
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    JsCode = None  # type: ignore
    _AGGRID_IMPORT_ERROR = traceback.format_exc()
else:
    _AGGRID_IMPORT_ERROR = None

from utils.flask_api import api_get
from utils.aggrid_formatters import js_eu_number_formatter


def render() -> None:
    st.subheader("ESI Monitoring")

    if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
        st.error(
            "streamlit-aggrid is required but could not be imported in this Streamlit process. "
            "Install it in the same Python environment and restart Streamlit."
        )
        st.caption(f"Python: {sys.executable}")
        if _AGGRID_IMPORT_ERROR:
            with st.expander("Import error details", expanded=False):
                st.code(_AGGRID_IMPORT_ERROR)
        st.code(f"{sys.executable} -m pip install streamlit-aggrid")
        st.stop()

    aggrid_fn = cast(Any, AgGrid)
    grid_options_builder = cast(Any, GridOptionsBuilder)
    js_code = cast(Any, JsCode)

    eu_locale = "nl-NL"

    refresh_cols = st.columns([1, 2, 2])
    with refresh_cols[0]:
        refresh_now = st.button("Refresh now")
    with refresh_cols[1]:
        auto_refresh = st.checkbox(
            "Auto-refresh",
            value=True,
            help="Uses Streamlit fragments when supported to avoid a full-page rerun.",
        )
    with refresh_cols[2]:
        refresh_interval_s = st.slider(
            "Refresh interval (s)",
            min_value=1,
            max_value=10,
            value=2,
            step=1,
        )

    if refresh_now:
        cache_data = getattr(st, "cache_data", None)
        if cache_data is not None and hasattr(cache_data, "clear"):
            cache_data.clear()  # best-effort; safe even if nothing is cached.

    def _fetch_snapshot() -> dict:
        snap_resp = api_get("/esi_metrics?window=900&bucket=5&top=20") or {}
        snap = (snap_resp.get("data") if isinstance(snap_resp, dict) else {}) or {}
        return snap if isinstance(snap, dict) else {}

    def _render_live(snap: dict) -> None:
        totals = snap.get("totals")
        if not isinstance(totals, dict):
            totals = {}
        success = snap.get("success")
        if not isinstance(success, dict):
            success = {}
        latency = snap.get("latency")
        if not isinstance(latency, dict):
            latency = {}
        sleep = snap.get("sleep")
        if not isinstance(sleep, dict):
            sleep = {}
        retries = snap.get("retries")
        if not isinstance(retries, dict):
            retries = {}
        pagination = snap.get("pagination")
        if not isinstance(pagination, dict):
            pagination = {}
        cache = snap.get("cache")
        if not isinstance(cache, dict):
            cache = {}
        exceptions = snap.get("exceptions")
        if not isinstance(exceptions, dict):
            exceptions = {}

        ts_rows = snap.get("timeseries") if isinstance(snap.get("timeseries"), list) else []
        top_routes = snap.get("top_routes") if isinstance(snap.get("top_routes"), list) else []
        issues = snap.get("issues") if isinstance(snap.get("issues"), list) else []
        _ = issues  # details rendered outside the live fragment

        sleep_by_kind_any = sleep.get("by_kind_seconds")
        sleep_by_kind = sleep_by_kind_any if isinstance(sleep_by_kind_any, dict) else {}
        gate_sleep_s = float(sleep_by_kind.get("gate", 0.0) or 0.0)
        retry_sleep_s = float(sleep.get("retry_sleep_seconds", 0.0) or 0.0)

        success_rate = success.get("success_rate")
        try:
            success_rate_pct = float(success_rate) * 100.0 if success_rate is not None else None
        except Exception:
            success_rate_pct = None

        p95_raw = latency.get("p95_ms")
        try:
            p95_ms = float(p95_raw) if p95_raw is not None else None
        except Exception:
            p95_ms = None

        st.markdown("#### Overview")
        kpi1 = st.columns(3)
        kpi1[0].metric("Total calls", str(totals.get("calls", "-")))
        kpi1[1].metric("Warnings", str(totals.get("warnings", "-")))
        kpi1[2].metric("Errors", str(totals.get("errors", "-")))

        kpi2 = st.columns(3)
        kpi2[0].metric(
            "Success rate",
            f"{success_rate_pct:.1f}%" if isinstance(success_rate_pct, (int, float)) else "-",
        )
        kpi2[1].metric(
            "Latency p95",
            f"{p95_ms:.0f} ms" if p95_ms is not None else "-",
        )
        kpi2[2].metric(
            "Retry events",
            str(retries.get("events", "-")),
        )

        kpi3 = st.columns(3)
        kpi3[0].metric("Retry sleep", f"{retry_sleep_s:.1f} s")
        kpi3[1].metric("Gate sleep", f"{gate_sleep_s:.1f} s")
        kpi3[2].metric(
            "Latency samples",
            str(latency.get("samples", "-")),
        )

        # 1) Real-time graph of ESI calls
        st.markdown("#### Real-time ESI calls")
        df_ts = None
        if ts_rows:
            df_ts = pd.DataFrame(ts_rows)
            if "ts" in df_ts.columns:
                df_ts["time"] = pd.to_datetime(df_ts["ts"], unit="s")
                df_ts = df_ts.drop(columns=[c for c in ["ts"] if c in df_ts.columns])
                df_ts = df_ts.set_index("time")
            if "calls" in df_ts.columns:
                st.line_chart(df_ts[["calls"]])
            else:
                st.info("No timeseries data available yet.")
        else:
            st.info("No ESI calls recorded yet.")

        st.markdown("#### Latency (avg per bucket)")
        if df_ts is not None:
            if "avg_latency_ms" in df_ts.columns:
                latency_series = df_ts[["avg_latency_ms"]].copy()
                latency_series = latency_series.dropna()
                if not latency_series.empty:
                    st.line_chart(latency_series)
                else:
                    st.info("No latency samples recorded yet.")
            else:
                st.info("No latency timeseries available yet.")

        # 2) Top 20 most triggered calls since startup
        st.markdown("#### Top 20 calls (since startup)")
        if top_routes:
            df_top = pd.DataFrame(top_routes)
            # Split route into method + endpoint for readability.
            if "route" in df_top.columns:
                parts = df_top["route"].astype(str).str.split(" ", n=1, expand=True)
                if parts.shape[1] == 2:
                    df_top["method"] = parts[0]
                    df_top["endpoint"] = parts[1]
                else:
                    df_top["method"] = "?"
                    df_top["endpoint"] = df_top["route"].astype(str)

            show_cols = [c for c in ["method", "endpoint", "count"] if c in df_top.columns]
            df_top = df_top[show_cols]

            gb = grid_options_builder.from_dataframe(df_top)
            gb.configure_default_column(resizable=True, sortable=True, filter=True)
            right = {"textAlign": "right"}
            if "count" in df_top.columns:
                gb.configure_column(
                    "count",
                    header_name="Count",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=js_code, locale=eu_locale, decimals=0),
                    cellStyle=right,
                    width=120,
                )
            if "method" in df_top.columns:
                gb.configure_column("method", header_name="Method", width=110)
            if "endpoint" in df_top.columns:
                gb.configure_column("endpoint", header_name="Endpoint", minWidth=420)

            grid_opts = gb.build()
            height = max(220, min(520, 60 + (len(df_top) * 32)))
            aggrid_fn(
                df_top,
                gridOptions=grid_opts,
                allow_unsafe_jscode=True,
                theme="streamlit",
                height=height,
                fit_columns_on_grid_load=True,
                key="esi_top_routes",
            )
        else:
            st.info("No calls recorded yet.")

        st.markdown("#### Caching")
        cache_counts = cache.get("counts") if isinstance(cache.get("counts"), dict) else {}
        cache_enabled_attempts = cache.get("enabled_attempts")
        cache_hit_ratio = cache.get("hit_ratio")
        cache_304 = cache.get("cache_304")
        cache_200 = cache.get("cache_200")
        ccols = st.columns(4)
        ccols[0].metric("Cache-enabled attempts", str(cache_enabled_attempts if cache_enabled_attempts is not None else "-"))
        ccols[1].metric("304 revalidated", str(cache_304 if cache_304 is not None else "-"))
        ccols[2].metric("200 with cache", str(cache_200 if cache_200 is not None else "-"))
        try:
            ccols[3].metric("Hit ratio (304)", f"{float(cache_hit_ratio) * 100.0:.1f}%" if cache_hit_ratio is not None else "-")
        except Exception:
            ccols[3].metric("Hit ratio (304)", "-")

    def _render_details(snap: dict) -> None:
        calls_by_status_any = snap.get("calls_by_status")
        calls_by_status = calls_by_status_any if isinstance(calls_by_status_any, dict) else {}

        slow_routes = snap.get("top_slowest_routes") if isinstance(snap.get("top_slowest_routes"), list) else []
        issues = snap.get("issues") if isinstance(snap.get("issues"), list) else []

        pagination_any = snap.get("pagination")
        pagination = pagination_any if isinstance(pagination_any, dict) else {}

        retries_any = snap.get("retries")
        retries = retries_any if isinstance(retries_any, dict) else {}

        exceptions_any = snap.get("exceptions")
        exceptions = exceptions_any if isinstance(exceptions_any, dict) else {}

        cache_any = snap.get("cache")
        cache = cache_any if isinstance(cache_any, dict) else {}

        cache_counts_any = cache.get("counts")
        cache_counts = cache_counts_any if isinstance(cache_counts_any, dict) else {}

        st.markdown("#### Details")
        st.caption("Details tables don't auto-refresh to keep expanders stable. Use 'Refresh now' to update.")

        with st.expander("Status distribution", expanded=False):
            if calls_by_status:
                df_status = pd.DataFrame(
                    [{"status": str(k), "count": int(v)} for k, v in calls_by_status.items()]
                ).sort_values("count", ascending=False)
                height_st = max(240, min(520, 40 + (len(df_status) * 28)))
                st.dataframe(df_status, width='stretch', height=height_st, hide_index=True)
            else:
                st.info("No status data yet.")

        with st.expander("Cache counters", expanded=False):
            if cache_counts:
                df_cache = pd.DataFrame(
                    [{"metric": str(k), "count": float(v)} for k, v in cache_counts.items()]
                ).sort_values("count", ascending=False)
                df_cache["count"] = pd.to_numeric(df_cache["count"], errors="coerce").fillna(0).astype(int)
                height_c = max(240, min(520, 40 + (len(df_cache) * 28)))
                st.dataframe(df_cache, width='stretch', height=height_c, hide_index=True)
            else:
                st.info("No cache counters recorded yet.")

        with st.expander("Pagination", expanded=False):
            pages_total = pagination.get("pages_total")
            st.metric("Total pages fetched", str(pages_total if pages_total is not None else "-"))
            pages_by_route_any = pagination.get("pages_by_route")
            pages_by_route = pages_by_route_any if isinstance(pages_by_route_any, dict) else {}
            if pages_by_route:
                df_pages = pd.DataFrame(
                    [{"route": str(k), "pages": int(v)} for k, v in pages_by_route.items()]
                ).sort_values("pages", ascending=False)
                if "route" in df_pages.columns:
                    parts = df_pages["route"].astype(str).str.split(" ", n=1, expand=True)
                    if parts.shape[1] == 2:
                        df_pages["method"] = parts[0]
                        df_pages["endpoint"] = parts[1]
                show_cols = [c for c in ["method", "endpoint", "pages"] if c in df_pages.columns]
                df_pages = df_pages[show_cols]
                height_p = max(240, min(520, 40 + (len(df_pages) * 28)))
                st.dataframe(df_pages, width='stretch', height=height_p, hide_index=True)
            else:
                st.info("No pagination pages recorded yet.")

        with st.expander("Retries", expanded=False):
            retry_by_reason_any = retries.get("by_reason")
            retry_by_reason = retry_by_reason_any if isinstance(retry_by_reason_any, dict) else {}
            if retry_by_reason:
                df_r = pd.DataFrame(
                    [{"reason": str(k), "count": int(v)} for k, v in retry_by_reason.items()]
                ).sort_values("count", ascending=False)
                height_r = max(240, min(520, 40 + (len(df_r) * 28)))
                st.dataframe(df_r, width='stretch', height=height_r, hide_index=True)
            else:
                st.info("No retries recorded yet.")

        with st.expander("Exceptions", expanded=False):
            ex_by_type_any = exceptions.get("by_type")
            ex_by_type = ex_by_type_any if isinstance(ex_by_type_any, dict) else {}
            if ex_by_type:
                df_ex = pd.DataFrame(
                    [{"exception": str(k), "count": int(v)} for k, v in ex_by_type.items()]
                ).sort_values("count", ascending=False)
                height_ex = max(240, min(520, 40 + (len(df_ex) * 28)))
                st.dataframe(df_ex, width='stretch', height=height_ex, hide_index=True)
            else:
                st.info("No exceptions recorded yet.")

        with st.expander("Slowest endpoints (p95)", expanded=False):
            if slow_routes:
                df_slow = pd.DataFrame(slow_routes)
                if "route" in df_slow.columns:
                    parts = df_slow["route"].astype(str).str.split(" ", n=1, expand=True)
                    if parts.shape[1] == 2:
                        df_slow["method"] = parts[0]
                        df_slow["endpoint"] = parts[1]

                show_cols = [
                    c
                    for c in ["method", "endpoint", "count", "p50_ms", "p95_ms", "avg_ms", "success", "error"]
                    if c in df_slow.columns
                ]
                df_slow = df_slow[show_cols]

                for col in ["p50_ms", "p95_ms", "avg_ms"]:
                    if col in df_slow.columns:
                        df_slow[col] = pd.to_numeric(df_slow[col], errors="coerce").round(1)
                for col in ["count", "success", "error"]:
                    if col in df_slow.columns:
                        df_slow[col] = pd.to_numeric(df_slow[col], errors="coerce").fillna(0).astype(int)

                height_s = max(260, min(520, 40 + (len(df_slow) * 28)))
                st.dataframe(df_slow, width='stretch', height=height_s, hide_index=True)
            else:
                st.info("No latency samples recorded yet.")

        st.markdown("#### ESI errors and warnings")
        if issues:
            with st.expander("Issues (warnings/errors/exceptions)", expanded=False):
                df_issues = pd.DataFrame(issues)
                if "ts" in df_issues.columns:
                    df_issues["time"] = pd.to_datetime(df_issues["ts"], unit="s")

                show_cols = [
                    c
                    for c in [
                        "time",
                        "kind",
                        "method",
                        "endpoint",
                        "status_code",
                        "message",
                        "retry_after_seconds",
                        "error_limit_remain",
                        "error_limit_reset_seconds",
                    ]
                    if c in df_issues.columns
                ]
                df_issues = df_issues[show_cols]
                height2 = max(280, min(520, 40 + (len(df_issues) * 28)))
                st.dataframe(df_issues, width='stretch', height=height2, hide_index=True)
        else:
            st.info("No ESI warnings/errors recorded yet.")

    # Prefer fragments for real-time updates (no full app rerun).
    fragment = getattr(st, "fragment", None)
    if auto_refresh and callable(fragment):
        @st.fragment(run_every=f"{int(refresh_interval_s)}s")
        def _live_fragment() -> None:
            snap = _fetch_snapshot()
            st.session_state["esi_monitor_snapshot"] = snap
            _render_live(snap)

        _live_fragment()
        st.caption("Live updates: fragment refresh (no full-page rerun).")
        _render_details(st.session_state.get("esi_monitor_snapshot") or {})
        return

    # Manual refresh (and fallback for older Streamlit versions)
    if auto_refresh and not callable(fragment):
        st.info("Auto-refresh without full reruns requires Streamlit fragments. Upgrade Streamlit or use 'Refresh now'.")

    snap = _fetch_snapshot()
    st.session_state["esi_monitor_snapshot"] = snap
    _render_live(snap)
    _render_details(snap)
