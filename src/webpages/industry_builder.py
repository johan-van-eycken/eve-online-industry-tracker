import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]

import html
import json
import math
import time
import sys
import traceback
from typing import Any

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # type: ignore
except Exception:  # pragma: no cover
    _AGGRID_IMPORT_ERROR = traceback.format_exc()
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    JsCode = None  # type: ignore
    
else:
    _AGGRID_IMPORT_ERROR = None

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get, api_post


@st.cache_data(ttl=3600)
def _get_industry_profiles(character_id: int) -> dict | None:
    return api_get(f"/industry_profiles/{int(character_id)}")


def render():
    st.subheader("Industry Builder")

    def _parse_json_cell(value: Any) -> Any:
        try:
            if value is None:
                return None
            if isinstance(value, float) and math.isnan(value):
                return None
            if isinstance(value, (dict, list)):
                return value
            if isinstance(value, str):
                s = value.strip()
                if not s:
                    return None
                return json.loads(s)
        except Exception:
            return None
        return None

    def _coerce_fraction(value: Any, *, default: float) -> float:
        try:
            v = float(value)
        except Exception:
            v = float(default)
        if v >= 1.0:
            v = v / 100.0
        return float(min(1.0, max(0.0, v)))

    def _type_icon_url(type_id: Any, *, size: int = 32) -> str | None:
        try:
            tid = int(type_id)
        except Exception:
            return None
        if tid <= 0:
            return None
        return f"https://images.evetech.net/types/{tid}/icon?size={int(size)}"

    def _blueprint_image_url(blueprint_type_id: Any, *, is_bpc: bool, size: int = 32) -> str | None:
        try:
            tid = int(blueprint_type_id)
        except Exception:
            return None
        if tid <= 0:
            return None
        variation = "bpc" if bool(is_bpc) else "bp"
        return f"https://images.evetech.net/types/{tid}/{variation}?size={int(size)}"

    db: Any = None

    try:
        cfgManager = load_config()
        cfg = cfgManager.all()
        db = init_db_app(cfgManager)
    except Exception as e:
        st.error(f"Failed to load database: {e}")
        st.stop()

    if db is None:
        st.stop()

    characters_df: pd.DataFrame | None = None
    try:
        characters_df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")

        st.stop()

    assert characters_df is not None

    character_map = dict(zip(characters_df["character_id"], characters_df["character_name"]))

    selected_character_id = st.selectbox(
        "Select a character",
        options=characters_df["character_id"].tolist(),
        format_func=lambda character_id: character_map.get(character_id, str(character_id)),
    )

    if not selected_character_id:
        return

    # Background-update state (set up early so we can avoid extra backend calls during polling reruns).
    if "industry_builder_job_id" not in st.session_state:
        st.session_state["industry_builder_job_id"] = None
    if "industry_builder_job_key" not in st.session_state:
        st.session_state["industry_builder_job_key"] = None
    if "industry_builder_selected_profile_id" not in st.session_state:
        st.session_state["industry_builder_selected_profile_id"] = None
    if "industry_profiles_cache" not in st.session_state:
        st.session_state["industry_profiles_cache"] = {}

    # UI default: maximize runs for BPCs unless the user changes it.
    if "maximize_blueprint_runs" not in st.session_state:
        st.session_state["maximize_blueprint_runs"] = True

    job_id = st.session_state.get("industry_builder_job_id")
    job_running = bool(job_id)

    # Industry profile selector (affects job cost/time estimates)
    profiles_resp = None
    profiles_cache = st.session_state.get("industry_profiles_cache")
    if job_running:
        # While a background job is running, never re-fetch profiles (it causes extra calls on reruns).
        if isinstance(profiles_cache, dict):
            profiles_resp = profiles_cache.get(int(selected_character_id))
    if profiles_resp is None:
        profiles_resp = _get_industry_profiles(int(selected_character_id))
        if isinstance(profiles_cache, dict) and profiles_resp is not None:
            profiles_cache[int(selected_character_id)] = profiles_resp
    profiles = (profiles_resp or {}).get("data") if (profiles_resp or {}).get("status") == "success" else []
    profile_options: list[int | None] = [None]
    profile_label_by_id: dict[int | None, str] = {None: "(No profile)"}
    default_profile_id = None
    for p in profiles or []:
        pid = p.get("id")
        if pid is None:
            continue
        profile_options.append(int(pid))
        label = str(p.get("profile_name") or pid)
        if p.get("is_default"):
            label = f"⭐ {label}"
            default_profile_id = int(pid)
        profile_label_by_id[int(pid)] = label

    # If an update is in progress, lock the profile selection to the one used to start the job.
    locked_profile_id = st.session_state.get("industry_builder_selected_profile_id") if job_running else None
    selected_profile_id = st.selectbox(
        "Industry Profile",
        options=profile_options,
        index=(
            profile_options.index(locked_profile_id)
            if job_running and locked_profile_id in profile_options
            else (profile_options.index(default_profile_id) if default_profile_id in profile_options else 0)
        ),
        format_func=lambda pid: profile_label_by_id.get(pid, str(pid)),
        help="Select an Industry Profile to estimate job fees and job time.",
        disabled=job_running,
    )

    maximize_runs = bool(st.session_state.get("maximize_blueprint_runs", True))

    # --- Market pricing preferences (EveGuru-like profit tool) ---
    # These settings affect material costs + product revenue used for Profit/ROI.
    # Hub pricing outlier protection defaults (config-driven; no UI control).
    mp_defaults = ((cfg or {}).get("defaults") or {}).get("market_pricing") if isinstance(cfg, dict) else {}
    if not isinstance(mp_defaults, dict):
        mp_defaults = {}

    # UI defaults (config-driven; stored in session state once).
    material_price_source_default_cfg = str(mp_defaults.get("material_price_source_default") or "Jita Sell")
    product_price_source_default_cfg = str(mp_defaults.get("product_price_source_default") or "Jita Sell")
    if material_price_source_default_cfg not in {"Jita Buy", "Jita Sell"}:
        material_price_source_default_cfg = "Jita Sell"
    if product_price_source_default_cfg not in {"Jita Buy", "Jita Sell"}:
        product_price_source_default_cfg = "Jita Sell"

    if "industry_builder_material_price_source" not in st.session_state:
        st.session_state["industry_builder_material_price_source"] = material_price_source_default_cfg
    if "industry_builder_product_price_source" not in st.session_state:
        st.session_state["industry_builder_product_price_source"] = product_price_source_default_cfg

    # Config defaults (with a final hard fallback for robustness)
    _cfg_default_sales_tax = mp_defaults.get("sales_tax_fraction")
    _cfg_default_broker_fee = mp_defaults.get("broker_fee_fraction")
    default_sales_tax_fraction_cfg = _coerce_fraction(_cfg_default_sales_tax, default=0.03375)
    default_broker_fee_fraction_cfg = _coerce_fraction(_cfg_default_broker_fee, default=0.03)
    if "industry_builder_orderbook_depth" not in st.session_state:
        try:
            st.session_state["industry_builder_orderbook_depth"] = int(mp_defaults.get("orderbook_depth") or 5)
        except Exception:
            st.session_state["industry_builder_orderbook_depth"] = 5
    if "industry_builder_orderbook_smoothing" not in st.session_state:
        st.session_state["industry_builder_orderbook_smoothing"] = str(mp_defaults.get("orderbook_smoothing") or "median_best_n")
    if "industry_builder_sales_tax_fraction" not in st.session_state:
        # Market sales tax (applies when selling items on the market).
        # Default 3.375% corresponds to base 7.5% with Accounting V.
        try:
            st.session_state["industry_builder_sales_tax_fraction"] = float(default_sales_tax_fraction_cfg)
        except Exception:
            st.session_state["industry_builder_sales_tax_fraction"] = float(default_sales_tax_fraction_cfg)

    # Per-character market fees (Jita 4-4): prefer computed values if present.
    default_sales_tax_fraction = _coerce_fraction(
        st.session_state.get("industry_builder_sales_tax_fraction"),
        default=float(default_sales_tax_fraction_cfg),
    )
    default_broker_fee_fraction = float(default_broker_fee_fraction_cfg)

    market_fees_obj: dict[str, Any] | None = None
    try:
        row = characters_df.loc[characters_df["character_id"] == selected_character_id].iloc[0]
        market_fees_obj = _parse_json_cell(row.get("market_fees")) if hasattr(row, "get") else None
        if market_fees_obj is not None and not isinstance(market_fees_obj, dict):
            market_fees_obj = None
    except Exception:
        market_fees_obj = None

    jita_rates = (((market_fees_obj or {}).get("jita_4_4") or {}).get("rates") or {}) if isinstance(market_fees_obj, dict) else {}
    effective_sales_tax_fraction = _coerce_fraction(jita_rates.get("sales_tax_fraction"), default=float(default_sales_tax_fraction))
    effective_broker_fee_fraction = _coerce_fraction(jita_rates.get("broker_fee_fraction"), default=float(default_broker_fee_fraction))

    with st.expander("Market Pricing (Profit/ROI)", expanded=False):
        st.caption(
            "Choose which Jita hub prices to use for profitability calculations. "
            "These do not affect in-game job fees; they affect Material Cost, Revenue, Profit and ROI. "
            "Outlier protection uses median of best 5 orders (configurable in config/config.json). "
            "Profit includes market sales tax and broker fees based on the selected character (Jita 4-4)."
        )
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.selectbox(
                "Materials (procurement)",
                options=["Jita Buy", "Jita Sell"],
                key="industry_builder_material_price_source",
                disabled=job_running,
                help="Jita Buy = highest buy orders (placing buy orders). Jita Sell = lowest sell orders (buying instantly).",
            )
        with col_p2:
            st.selectbox(
                "Products (sale)",
                options=["Jita Sell", "Jita Buy"],
                key="industry_builder_product_price_source",
                disabled=job_running,
                help="Jita Sell = lowest sell orders (listing items). Jita Buy = highest buy orders (selling instantly).",
            )

        st.caption(
            f"Character fees (Jita 4-4): Sales tax {effective_sales_tax_fraction*100.0:.2f}% · Broker fee {effective_broker_fee_fraction*100.0:.2f}%"
        )

    # --- Explicit update workflow (required because full submanufacturing is expensive) ---
    # No backend calls happen here unless the user clicks the button.
    pricing_key = (
        f"jita:{st.session_state.get('industry_builder_material_price_source')}:"
        f"{st.session_state.get('industry_builder_product_price_source')}:"
        f"{str(st.session_state.get('industry_builder_orderbook_smoothing') or 'median_best_n')}:"
        f"depth{int(st.session_state.get('industry_builder_orderbook_depth') or 5)}:"
        f"stax{float(effective_sales_tax_fraction or 0.0):.6f}:"
        f"bfee{float(effective_broker_fee_fraction or 0.0):.6f}"
    )
    key = f"{int(selected_character_id)}:{int(selected_profile_id or 0)}:{1 if maximize_runs else 0}:{pricing_key}"
    cache: dict[str, dict] = st.session_state.setdefault("industry_builder_cache", {})


    cached = cache.get(key) if isinstance(cache, dict) else None
    if isinstance(cached, dict) and isinstance(cached.get("data"), list):
        industry_data = cached.get("data") or []
        response_meta = cached.get("meta")
    else:
        industry_data = []
        response_meta = None

    st.markdown("#### Update")
    st.caption(
        "Compute full Industry Builder data (incl. submanufacturing) for all owned blueprints. "
        "This can take a while; results are cached for this session."
    )

    col_update, col_clear = st.columns([1, 1])
    with col_update:
        if st.button("Update Industry Jobs", type="primary"):
            try:
                payload = {
                    "profile_id": (int(selected_profile_id) if selected_profile_id is not None else None),
                    "maximize_runs": bool(maximize_runs),
                    "pricing_preferences": {
                        "hub": "jita",
                        "material_price_source": (
                            "jita_buy" if str(st.session_state.get("industry_builder_material_price_source")) == "Jita Buy" else "jita_sell"
                        ),
                        "product_price_source": (
                            "jita_sell" if str(st.session_state.get("industry_builder_product_price_source")) == "Jita Sell" else "jita_buy"
                        ),
                        "orderbook_depth": int(st.session_state.get("industry_builder_orderbook_depth") or 5),
                        "orderbook_smoothing": str(st.session_state.get("industry_builder_orderbook_smoothing") or "median_best_n"),
                        "enabled": True,
                    },
                }
                resp = api_post(f"/industry_builder_update/{int(selected_character_id)}", payload) or {}
                if resp.get("status") != "success":
                    st.error(f"API error: {resp.get('message', 'Unknown error')}")
                else:
                    job_id = (resp.get("data") or {}).get("job_id")
                    st.session_state["industry_builder_job_id"] = job_id
                    st.session_state["industry_builder_job_key"] = key
                    st.session_state["industry_builder_selected_profile_id"] = selected_profile_id
                    # Clear cached data for this key to avoid stale display.
                    if isinstance(cache, dict):
                        cache.pop(key, None)
                    st.rerun()
            except Exception as e:
                st.error(f"Error calling backend: {e}")

    with col_clear:
        if st.button("Clear cached data"):
            if isinstance(cache, dict):
                cache.pop(key, None)
            st.session_state["industry_builder_job_id"] = None
            st.session_state["industry_builder_job_key"] = None
            st.session_state["industry_builder_selected_profile_id"] = None
            st.session_state.pop("industry_builder_poll_started_at", None)
            st.rerun()

    job_id = st.session_state.get("industry_builder_job_id")
    job_key = st.session_state.get("industry_builder_job_key")
    # While an update job is running (and we don't have cached results yet), avoid rendering the rest
    # of the page. This prevents flicker during frequent polling reruns.
    if job_id and not industry_data:
        if job_key != key:
            st.info("An Industry Builder update is running for different settings. Please wait for it to finish.")
            st.stop()

        # Adaptive polling backoff to reduce backend load and log spam.
        poll_started_at = st.session_state.get("industry_builder_poll_started_at")
        if poll_started_at is None:
            poll_started_at = time.time()
            st.session_state["industry_builder_poll_started_at"] = poll_started_at

        try:
            status_resp = api_get(f"/industry_builder_update_status/{job_id}") or {}
            if status_resp.get("status") != "success":
                st.error(f"API error: {status_resp.get('message', 'Unknown error')}")
                st.stop()

            s = status_resp.get("data") or {}
            status = s.get("status")
            done = int(s.get("progress_done") or 0)
            total = s.get("progress_total")
            try:
                total_i = int(total) if total is not None else None
            except Exception:
                total_i = None

            if status == "error":
                st.error(f"Backend update failed: {s.get('error')}")
                st.session_state["industry_builder_job_id"] = None
                st.session_state["industry_builder_job_key"] = None
                st.session_state.pop("industry_builder_poll_started_at", None)
                st.stop()
            elif status == "done":
                result_resp = api_get(
                    f"/industry_builder_update_result/{job_id}",
                    timeout_seconds=300,
                ) or {}
                if result_resp.get("status") != "success":
                    st.error(f"API error: {result_resp.get('message', 'Unknown error')}")
                    st.stop()

                data = result_resp.get("data") or []
                meta = result_resp.get("meta")
                if isinstance(cache, dict):
                    cache[key] = {"data": data, "meta": meta}
                st.session_state["industry_builder_job_id"] = None
                st.session_state["industry_builder_job_key"] = None
                st.session_state.pop("industry_builder_poll_started_at", None)
                st.rerun()
            else:
                # Backoff schedule (seconds): 1s for first 15s, then 2s, 4s, 7s.
                now = time.time()
                elapsed_s = max(0.0, float(now - float(poll_started_at or now)))
                if elapsed_s < 15.0:
                    poll_s = 1.0
                elif elapsed_s < 60.0:
                    poll_s = 2.0
                elif elapsed_s < 180.0:
                    poll_s = 4.0
                else:
                    poll_s = 7.0

                frac = (float(done) / float(total_i)) if total_i and total_i > 0 else 0.0
                minutes = int(elapsed_s // 60.0)
                seconds = int(elapsed_s % 60.0)
                elapsed_txt = f"{minutes:02d}:{seconds:02d}"
                st.progress(
                    min(1.0, max(0.0, frac)),
                    text=f"Updating: {done} / {total_i or '?'} blueprints (elapsed {elapsed_txt}; next check ~{int(poll_s)}s)",
                )
                time.sleep(poll_s)
                st.rerun()
        except Exception as e:
            st.error(f"Error calling backend: {e}")
            st.stop()

    if not industry_data:
        st.info("Click **Update Industry Jobs** to load data.")
        return

    def _format_duration(seconds: float | int | None) -> str:
        try:
            s = int(round(float(seconds or 0.0)))
        except Exception:
            s = 0
        if s < 0:
            s = 0

        month_s = 30 * 24 * 3600
        day_s = 24 * 3600

        months = s // month_s
        s = s % month_s
        days = s // day_s
        s = s % day_s

        hours = s // 3600
        s = s % 3600
        minutes = s // 60
        secs = s % 60

        parts = []
        if months:
            parts.append(f"{months}M")
        if days:
            parts.append(f"{days}D")
        parts.append(f"{hours:02d}:{minutes:02d}:{secs:02d}")
        return " ".join(parts)

    def _blueprint_passes_filters(bp: dict, *, bp_type_filter: str, skill_req_filter: bool, reactions_filter: bool, location_filter: str) -> bool:
        if not isinstance(bp, dict):
            return False

        flags = bp.get("flags") or {}
        is_bpc = bool(flags.get("is_blueprint_copy")) if isinstance(flags, dict) else False
        if bool(st.session_state.get("maximize_blueprint_runs", False)):
            if not is_bpc:
                return False
        else:
            if bp_type_filter == "Originals (BPO)" and is_bpc:
                return False
            if bp_type_filter == "Copies (BPC)" and not is_bpc:
                return False

        if skill_req_filter:
            if not bool(bp.get("skill_requirements_met", False)):
                return False

        if reactions_filter is False:
            flags = bp.get("flags") or {}
            is_reaction_bp = bool(flags.get("is_reaction_blueprint")) if isinstance(flags, dict) else False
            if is_reaction_bp:
                return False

        if location_filter != "All":
            loc = bp.get("location") or {}
            disp = (loc.get("display_name") if isinstance(loc, dict) else None) or ""
            if str(disp) != str(location_filter):
                return False

        return True

    # Precompute filter option lists from blueprint-level and product-level data.
    location_options = ["All"]
    all_locations: set[str] = set()
    all_product_categories: set[str] = set()
    for bp in industry_data:
        if not isinstance(bp, dict):
            continue
        loc = bp.get("location") or {}
        disp = (loc.get("display_name") if isinstance(loc, dict) else None)
        if disp:
            all_locations.add(str(disp))
        for prod in (bp.get("products") or []):
            if not isinstance(prod, dict):
                continue
            cn = prod.get("category_name")
            if cn:
                all_product_categories.add(str(cn))
    if all_locations:
        location_options += sorted(all_locations)

    category_options = sorted(all_product_categories)

    with st.expander("Filters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            maximize_runs_enabled = bool(st.session_state.get("maximize_blueprint_runs", True))
            if maximize_runs_enabled:
                st.radio(
                    "Blueprint Type",
                    options=["Copies (BPC)"],
                    index=0,
                    key="bp_type_filter_locked",
                    help="Locked because 'Maximize Blueprint Runs' only applies to BPCs.",
                )
                bp_type_filter = "Copies (BPC)"
            else:
                bp_type_filter = st.radio(
                    "Blueprint Type",
                    options=["All", "Originals (BPO)", "Copies (BPC)"],
                    index=0,
                    key="bp_type_filter",
                )
        with col2:
            skill_req_filter = st.checkbox("I have the skills", value=True)
            reactions_filter = st.checkbox(
                "Include Reactions",
                value=False,
                help="Reactions can only be done in 0.4-secure space or lower.",
            )
            st.checkbox(
                "Maximize Blueprint Runs",
                value=bool(st.session_state.get("maximize_blueprint_runs", True)),
                key="maximize_blueprint_runs",
                help="If enabled, BPC calculations use all remaining runs (materials, time, fees, and copy overhead).",
            )

        with col3:
            location_filter = st.selectbox("Location", options=location_options, index=0)
            selected_categories = st.multiselect(
                "Categories",
                options=category_options,
                default=[],
                help="Filter table rows by the produced item's category.",
            )

        st.divider()

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Min. ROI**")
            roi_cb_col, roi_val_col = st.columns([0.18, 0.82])
            with roi_cb_col:
                apply_min_roi = st.checkbox(
                    "",
                    value=False,
                    key="industry_builder_apply_min_roi",
                    label_visibility="collapsed",
                )
            with roi_val_col:
                min_roi_pct = st.number_input(
                    "ROI (%)",
                    min_value=0.0,
                    max_value=10_000.0,
                    value=10.0,
                    step=1.0,
                    disabled=not bool(apply_min_roi),
                    key="industry_builder_min_roi_pct",
                )

        with c2:
            st.markdown("**Min. Profit**")
            profit_cb_col, profit_val_col = st.columns([0.18, 0.82])
            with profit_cb_col:
                apply_min_profit = st.checkbox(
                    "",
                    value=False,
                    key="industry_builder_apply_min_profit",
                    label_visibility="collapsed",
                )
            with profit_val_col:
                min_profit_isk = st.number_input(
                    "Profit (ISK)",
                    min_value=0,
                    max_value=10_000_000_000_000,
                    value=1_000_000,
                    step=100_000,
                    disabled=not bool(apply_min_profit),
                    key="industry_builder_min_profit_isk",
                )

        with c3:
            st.markdown("**Min. ISK/h**")
            iskh_cb_col, iskh_val_col = st.columns([0.18, 0.82])
            with iskh_cb_col:
                apply_min_iskh = st.checkbox(
                    "",
                    value=False,
                    key="industry_builder_apply_min_iskh",
                    label_visibility="collapsed",
                    help="Filters by the 'Profit / hour' column.",
                )
            with iskh_val_col:
                min_iskh_isk = st.number_input(
                    "ISK / hour",
                    min_value=0,
                    max_value=10_000_000_000_000,
                    value=1_000_000,
                    step=100_000,
                    disabled=not bool(apply_min_iskh),
                    key="industry_builder_min_iskh_isk",
                )

    filtered_blueprints = [
        bp
        for bp in industry_data
        if _blueprint_passes_filters(
            bp,
            bp_type_filter=bp_type_filter,
            skill_req_filter=skill_req_filter,
            reactions_filter=reactions_filter,
            location_filter=location_filter,
        )
    ]

    # Market fee behavior depends on which pricing mode is selected.
    material_price_source = str(st.session_state.get("industry_builder_material_price_source") or "")
    product_price_source = str(st.session_state.get("industry_builder_product_price_source") or "")
    apply_buy_broker_fee = material_price_source == "Jita Buy"  # placing buy orders
    apply_sell_broker_fee = product_price_source == "Jita Sell"  # listing sell orders
    sales_tax_fraction = float(effective_sales_tax_fraction)
    broker_fee_fraction = float(effective_broker_fee_fraction)

    # Explode to produced-item grain.
    table_rows: list[dict] = []
    for bp in filtered_blueprints:
        if not isinstance(bp, dict):
            continue

        loc = bp.get("location") or {}
        solar = (loc.get("solar_system") or {}) if isinstance(loc, dict) else {}

        mj = bp.get("manufacture_job") or {}
        props = (mj.get("properties") or {}) if isinstance(mj, dict) else {}
        cost = (props.get("job_cost") or {}) if isinstance(props, dict) else {}
        effective = (props.get("effective_totals") or {}) if isinstance(props, dict) else {}
        time_eff = (props.get("total_time_efficiency") or {}) if isinstance(props, dict) else {}

        job_runs = props.get("job_runs")
        try:
            job_runs_i = int(job_runs or 1)
        except Exception:
            job_runs_i = 1

        # Job duration (seconds): main manufacturing job only.
        # Intentionally excludes submanufacturing time and any copy/research overhead.
        job_time_seconds: float | None = None
        try:
            v = time_eff.get("estimated_job_time_seconds")
            job_time_seconds = float(v) if v is not None else None
        except Exception:
            job_time_seconds = None

        job_duration_display = _format_duration(job_time_seconds) if job_time_seconds is not None else "-"

        # Manufacturing job fee (aligns with in-game "Total job cost").
        # Note: we compute optional copy overhead in the backend, but we don't include it
        # in the main products table by default.
        est_fee_total = cost.get("total_job_cost_isk")
        try:
            est_fee_total_f = float(est_fee_total or 0.0)
        except Exception:
            est_fee_total_f = 0.0

        # Prefer server-side effective costs (submanufacturing-aware) when present.
        raw_total_material_cost = (
            bp.get("total_material_cost_effective")
            if bp.get("total_material_cost_effective") is not None
            else bp.get("total_material_cost")
        )
        try:
            total_material_cost = float(raw_total_material_cost or 0.0)
        except Exception:
            total_material_cost = 0.0
        total_product_value = float(bp.get("total_product_value") or 0.0)

        # Broker fees: apply only when placing market orders.
        broker_fee_buy_total = float(total_material_cost) * float(broker_fee_fraction) if apply_buy_broker_fee else 0.0
        broker_fee_sell_total = float(total_product_value) * float(broker_fee_fraction) if apply_sell_broker_fee else 0.0

        # Profit (incl. job fee) is the most actionable for ROI.
        sales_tax_total_bp = float(total_product_value) * float(sales_tax_fraction)
        profit_total = total_product_value - total_material_cost - est_fee_total_f
        profit_total_net = profit_total - sales_tax_total_bp - broker_fee_buy_total - broker_fee_sell_total

        # Allocate blueprint-level costs across products to support multi-output blueprints.
        products_list = [p for p in (bp.get("products") or []) if isinstance(p, dict)]
        product_value_totals: list[float] = []
        product_qty_totals: list[int] = []
        for prod in products_list:
            try:
                q = int(prod.get("quantity_total") or prod.get("quantity") or 0)
            except Exception:
                q = 0
            try:
                unit_price = float(
                    prod.get("market_unit_price_isk")
                    if prod.get("market_unit_price_isk") is not None
                    else (prod.get("average_price") or 0.0)
                )
            except Exception:
                unit_price = 0.0
            product_qty_totals.append(max(0, q))
            product_value_totals.append(max(0.0, float(q) * float(unit_price)))

        value_total_sum = float(sum(product_value_totals))
        qty_total_sum = int(sum(product_qty_totals))

        for idx, prod in enumerate(products_list):
            if not isinstance(prod, dict):
                continue

            prod_type_id = prod.get("type_id")
            prod_type_name = prod.get("type_name")
            prod_cat = prod.get("category_name")
            prod_grp = prod.get("group_name")
            try:
                prod_qty_total = int(prod.get("quantity_total") or prod.get("quantity") or 0)
            except Exception:
                prod_qty_total = 0

            if prod_qty_total <= 0:
                continue

            # Allocate costs/fees by product share.
            if value_total_sum > 0:
                share = float(product_value_totals[idx]) / float(value_total_sum)
            elif qty_total_sum > 0:
                share = float(product_qty_totals[idx]) / float(qty_total_sum)
            else:
                share = 1.0

            allocated_material_cost = float(total_material_cost) * float(share)
            allocated_job_fee = float(est_fee_total_f) * float(share)
            allocated_product_value = float(total_product_value) * float(share)
            allocated_profit = allocated_product_value - allocated_material_cost - allocated_job_fee

            allocated_broker_fee_buy = float(broker_fee_buy_total) * float(share)
            allocated_broker_fee_sell = float(broker_fee_sell_total) * float(share)
            broker_fee_total = float(allocated_broker_fee_buy) + float(allocated_broker_fee_sell)

            sales_tax_total = float(allocated_product_value) * float(sales_tax_fraction)
            allocated_profit_net = float(allocated_profit) - float(sales_tax_total) - float(broker_fee_total)

            profit_per_hour: float | None = None
            if job_time_seconds is not None:
                try:
                    hours = float(job_time_seconds) / 3600.0
                except Exception:
                    hours = 0.0
                if hours > 0:
                    profit_per_hour = float(allocated_profit_net) / float(hours)

            # Per-item metrics
            mat_cost_per_item = allocated_material_cost / float(prod_qty_total)
            prod_value_per_item = allocated_product_value / float(prod_qty_total)
            sales_tax_per_item = float(sales_tax_total) / float(prod_qty_total)
            broker_fee_per_item = float(broker_fee_total) / float(prod_qty_total)
            profit_per_item = allocated_profit_net / float(prod_qty_total)
            job_fee_per_item = allocated_job_fee / float(prod_qty_total)

            denom_total = allocated_material_cost + allocated_job_fee + broker_fee_total
            roi_total = (allocated_profit_net / float(denom_total)) if denom_total > 0 else None
            roi_total_percent = (float(roi_total) * 100.0) if roi_total is not None else None

            row = {
                # Produced item grain
                "type_id": prod_type_id,
                "Name": prod_type_name,
                "Category": prod_cat,

                # Job configuration
                "Runs": int(job_runs_i),
                "Units": int(prod_qty_total),
                "ME": bp.get("blueprint_material_efficiency_percent"),
                "TE": bp.get("blueprint_time_efficiency_percent"),

                "Job Duration": str(job_duration_display),
                "Profit / hour": float(profit_per_hour) if profit_per_hour is not None else None,

                # Per-item outputs
                "Mat. Cost / item": float(mat_cost_per_item),
                "Revenue / item": float(prod_value_per_item),
                "Sales Tax / item": float(sales_tax_per_item),
                "Broker Fee / item": float(broker_fee_per_item),
                "Profit / item": float(profit_per_item),
                "Job Fee / item": float(job_fee_per_item),

                # Totals
                "Mat. Cost": float(allocated_material_cost),
                "Revenue": float(allocated_product_value),
                "Sales Tax": float(sales_tax_total),
                "Broker Fee": float(broker_fee_total),
                "Profit": float(allocated_profit_net),
                "Job Fee": float(allocated_job_fee),
                "ROI": float(roi_total_percent) if roi_total_percent is not None else None,

                # Location
                "Location": (loc.get("display_name") if isinstance(loc, dict) else None),
                "Solar System": (solar.get("name") if isinstance(solar, dict) else None),
                "Solar System Security": (solar.get("security_status") if isinstance(solar, dict) else None),

                # Internal (for consistency checks)
                "_profit_total": float(profit_total_net),
                "_total_material_cost": float(total_material_cost),
                "_total_product_value": float(total_product_value),
                "_total_job_fee": float(est_fee_total_f),
                "_job_time_seconds": float(job_time_seconds) if job_time_seconds is not None else None,
            }

            table_rows.append(row)

    products_df = pd.DataFrame(table_rows)

    if selected_categories:
        products_df = products_df[products_df["Category"].isin(selected_categories)]

    # Profitability threshold filters (product-row grain)
    if bool(apply_min_roi) and "ROI" in products_df.columns:
        try:
            roi_s = pd.to_numeric(products_df["ROI"], errors="coerce")
            products_df = products_df[roi_s >= float(min_roi_pct)]
        except Exception:
            pass

    if bool(apply_min_profit) and "Profit" in products_df.columns:
        try:
            p_s = pd.to_numeric(products_df["Profit"], errors="coerce")
            products_df = products_df[p_s >= float(min_profit_isk)]
        except Exception:
            pass

    if bool(apply_min_iskh) and "Profit / hour" in products_df.columns:
        try:
            h_s = pd.to_numeric(products_df["Profit / hour"], errors="coerce")
            products_df = products_df[h_s >= float(min_iskh_isk)]
        except Exception:
            pass

    st.caption(f"{len(products_df)} product rows")
    st.caption(
        "Job Duration shows the main manufacturing job time only (no submanufacturing, no copy/research overhead). "
        "Profit / hour uses this duration."
    )

    # Keep the main table focused: hide debug/internal fields.
    hidden_cols = {
        "blueprint",
        "_profit_total",
        "_total_material_cost",
        "_total_product_value",
        "_total_job_fee",
        "_job_time_seconds",
    }
    display_df = products_df.drop(columns=[c for c in hidden_cols if c in products_df.columns], errors="ignore")

    # Add item icon column right after type_id (if available).
    if "type_id" in display_df.columns:
        try:
            icon = display_df["type_id"].apply(lambda tid: _type_icon_url(tid, size=32))
            if "Icon" not in display_df.columns:
                if "Name" in display_df.columns:
                    insert_at = max(0, int(list(display_df.columns).index("Name")))
                else:
                    insert_at = min(1, len(display_df.columns))
                display_df.insert(insert_at, "Icon", icon)
        except Exception:
            pass

    # pyarrow (used by Streamlit) requires integers to fit in int64.
    # If a previous run produced corrupted huge values (e.g. repeated run scaling),
    # convert those columns to strings so the UI doesn't crash.
    for _col in ["type_id", "Runs", "Units"]:
        if _col not in display_df.columns:
            continue
        try:
            s = pd.to_numeric(display_df[_col], errors="coerce")
            too_big = s.abs() > 9e18
            if bool(too_big.any()):
                display_df[_col] = display_df[_col].astype(str)
            else:
                if _col in ["Runs", "Units", "type_id"]:
                    display_df[_col] = s.fillna(0).astype("int64")
        except Exception:
            display_df[_col] = display_df[_col].astype(str)

    # Products table (AgGrid): EU formatting + icons + true right alignment.

    _preferred_cols = [
        "type_id",
        "Icon",
        "Name",
        "ME",
        "TE",
        "Runs",
        "Units",
        "Job Duration",
        "Profit / hour",
        "ROI",
        "Mat. Cost",
        "Job Fee",
        "Revenue",
        "Sales Tax",
        "Broker Fee",
        "Profit",
        "Mat. Cost / item",
        "Job Fee / item",
        "Revenue / item",
        "Sales Tax / item",
        "Broker Fee / item",
        "Profit / item",
        "Location",
        "Solar System",
        "Solar System Security",
        "Category",
    ]
    _cols = [c for c in _preferred_cols if c in display_df.columns]
    _cols += [c for c in display_df.columns if c not in _cols]
    display_df = display_df[_cols]

    if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
        st.error(
            "Failed to import streamlit-aggrid in the running Streamlit process. "
            "This usually means it isn't installed in the same Python interpreter, or Streamlit needs a restart after install."
        )
        st.caption(f"Python: {sys.executable}")
        if _AGGRID_IMPORT_ERROR:
            with st.expander("Import error details", expanded=False):
                st.code(_AGGRID_IMPORT_ERROR)
        st.code(f"{sys.executable} -m pip install streamlit-aggrid")
        st.info("Fallback: showing a basic table without AgGrid.")

        # Fallback renderer (no true right-align): EU formatting as strings + icon images.
        def _fmt_decimal_eu(value: Any, *, decimals: int = 2) -> str:
            try:
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return "-"
                v = float(value)
            except Exception:
                return "-"
            s = f"{v:,.{int(decimals)}f}"  # 1,234,567.89
            return s.replace(",", "X").replace(".", ",").replace("X", ".")

        def _fmt_isk_eu(value: Any, *, decimals: int = 2) -> str:
            s = _fmt_decimal_eu(value, decimals=decimals)
            return f"{s} ISK" if s != "-" else "-"

        def _fmt_pct_eu(value: Any, *, decimals: int = 2) -> str:
            s = _fmt_decimal_eu(value, decimals=decimals)
            return f"{s}%" if s != "-" else "-"

        view_df = display_df.copy()
        for c in [
            "Mat. Cost",
            "Job Fee",
            "Revenue",
            "Sales Tax",
            "Broker Fee",
            "Profit",
            "Mat. Cost / item",
            "Job Fee / item",
            "Revenue / item",
            "Sales Tax / item",
            "Broker Fee / item",
            "Profit / item",
        ]:
            if c in view_df.columns:
                view_df[c] = view_df[c].apply(lambda x: _fmt_isk_eu(x, decimals=2))
        if "ROI" in view_df.columns:
            view_df["ROI"] = view_df["ROI"].apply(lambda x: _fmt_pct_eu(x, decimals=2))
        if "Solar System Security" in view_df.columns:
            view_df["Solar System Security"] = view_df["Solar System Security"].apply(lambda x: _fmt_decimal_eu(x, decimals=2))

        fallback_config: dict[str, Any] = {}
        if "Icon" in view_df.columns:
            fallback_config["Icon"] = st.column_config.ImageColumn("Icon", width="small")
        st.data_editor(
            view_df,
            width="stretch",
            hide_index=True,
            disabled=True,
            num_rows="fixed",
            column_config=fallback_config or None,
        )
        st.divider()
        return

    eu_locale = "nl-NL"  # '.' thousands, ',' decimals

    img_renderer = JsCode(
        """
            (function() {
                function IconRenderer() {}

                IconRenderer.prototype.init = function(params) {
                    this.eGui = document.createElement('div');
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.justifyContent = 'center';
                    this.eGui.style.width = '100%';

                    this.eImg = document.createElement('img');
                    this.eImg.style.width = '32px';
                    this.eImg.style.height = '32px';
                    this.eImg.style.display = 'block';
                    this.eImg.src = params.value ? String(params.value) : '';

                    this.eGui.appendChild(this.eImg);
                };

                IconRenderer.prototype.getGui = function() {
                    return this.eGui;
                };

                IconRenderer.prototype.refresh = function(params) {
                    if (this.eImg) {
                        this.eImg.src = params.value ? String(params.value) : '';
                    }
                    return true;
                };

                return IconRenderer;
            })()
        """
    )

    def _js_eu_number(decimals: int) -> JsCode:
        return JsCode(
            f"""
                function(params) {{
                if (params.value === null || params.value === undefined || params.value === "") return "";
                const n = Number(params.value);
                if (isNaN(n)) return "";
                return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n);
                }}
            """
        )

    def _js_eu_isk(decimals: int) -> JsCode:
        return JsCode(
            f"""
                function(params) {{
                if (params.value === null || params.value === undefined || params.value === "") return "";
                const n = Number(params.value);
                if (isNaN(n)) return "";
                return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + ' ISK';
                }}
            """
        )

    def _js_eu_pct(decimals: int) -> JsCode:
        return JsCode(
            f"""
                function(params) {{
                    if (params.value === null || params.value === undefined || params.value === "") return "";
                    const n = Number(params.value);
                    if (isNaN(n)) return "";
                    return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + '%';
                }}
            """
        )

    gb = GridOptionsBuilder.from_dataframe(display_df)
    gb.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)

    if "Icon" in display_df.columns:
        gb.configure_column(
            "Icon",
            header_name="",
            width=62,
            pinned="left",
            sortable=False,
            filter=False,
            suppressAutoSize=True,
            cellRenderer=img_renderer,
        )

    right = {"textAlign": "right"}

    # ISK columns
    for c in [
        "Mat. Cost",
        "Job Fee",
        "Revenue",
        "Sales Tax",
        "Broker Fee",
        "Profit",
        "Profit / hour",
        "Mat. Cost / item",
        "Job Fee / item",
        "Revenue / item",
        "Sales Tax / item",
        "Broker Fee / item",
        "Profit / item",
    ]:
        if c in display_df.columns:
            gb.configure_column(
                c,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=_js_eu_isk(2),
                minWidth=150,
                cellStyle=right,
            )

    if "ROI" in display_df.columns:
        gb.configure_column(
            "ROI",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=_js_eu_pct(2),
            minWidth=110,
            cellStyle=right,
        )

    if "Solar System Security" in display_df.columns:
        gb.configure_column(
            "Solar System Security",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=_js_eu_number(2),
            minWidth=150,
            cellStyle=right,
        )

    for c in ["Runs", "Units"]:
        if c in display_df.columns:
            gb.configure_column(
                c,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=_js_eu_number(0),
                minWidth=110,
                cellStyle=right,
            )

    grid_options = gb.build()
    # Column auto-sizing
    # - Prefer AG Grid's autoSizeStrategy when supported (keeps widths in sync with rendered values).
    # - Also trigger autoSizeAllColumns on key events as a fallback.
    grid_options["autoSizeStrategy"] = {"type": "fitCellContents"}

    _js_autosize_all = JsCode(
        """
            function(params) {
                setTimeout(function() {
                    try {
                        // skipHeader=false so header text is included in sizing.
                        params.columnApi.autoSizeAllColumns(false);
                    } catch (e) {}
                }, 50);
            }
        """
    )

    grid_options["onFirstDataRendered"] = JsCode(
        """
            function(params) {
                // Delay helps when the grid is still laying out / fonts not ready.
                setTimeout(function() {
                    try {
                        params.columnApi.autoSizeAllColumns(false);
                    } catch (e) {}
                }, 50);
            }
        """
    )
    grid_options["onGridSizeChanged"] = _js_autosize_all
    grid_options["onSortChanged"] = _js_autosize_all
    grid_options["onFilterChanged"] = _js_autosize_all
    height = min(800, 40 + (len(display_df) * 35))
    AgGrid(
        display_df,
        gridOptions=grid_options,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=height,
    )

    st.divider()

    if not filtered_blueprints or products_df.empty:
        return

    st.subheader("Product Details")

    # Select a produced item (product-centric workflow)
    # products_df uses "Name" (not "type_name") for the produced item label.
    prod_pairs = (
        products_df[["type_id", "Name"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["Name", "type_id"], ascending=[True, True])
    )
    if prod_pairs.empty:
        st.info("No producible products available with the current filters.")
        return

    prod_options = [(int(r["type_id"]), str(r["Name"])) for _, r in prod_pairs.iterrows()]
    prod_label_by_id = {tid: name for tid, name in prod_options}

    selected_product_id = st.selectbox(
        "Select a product",
        options=[tid for tid, _ in prod_options],
        format_func=lambda tid: f"{prod_label_by_id.get(int(tid), str(tid))} (typeID {int(tid)})",
    )

    # Show blueprints that can produce that item
    candidates: list[dict] = []
    for bp in filtered_blueprints:
        if not isinstance(bp, dict):
            continue
        prod = next(
            (
                p
                for p in (bp.get("products") or [])
                if isinstance(p, dict) and int(p.get("type_id") or 0) == int(selected_product_id)
            ),
            None,
        )
        if not isinstance(prod, dict):
            continue

        flags = bp.get("flags", {}) or {}
        is_bpc = bool(flags.get("is_blueprint_copy")) if isinstance(flags, dict) else False
        loc = bp.get("location") or {}
        solar = (loc.get("solar_system") or {}) if isinstance(loc, dict) else {}

        mj = bp.get("manufacture_job") or {}
        props = (mj.get("properties") or {}) if isinstance(mj, dict) else {}
        job_runs = props.get("job_runs")
        try:
            job_runs_i = int(job_runs or 1)
        except Exception:
            job_runs_i = 1

        qty_total = prod.get("quantity_total")
        qty_per_run = prod.get("quantity_per_run")
        try:
            qty_total_i = int(qty_total or 0)
        except Exception:
            qty_total_i = 0
        try:
            qty_per_run_i = int(qty_per_run or prod.get("quantity") or 0)
        except Exception:
            qty_per_run_i = 0

        remaining_runs = None
        if is_bpc:
            try:
                remaining_runs = int(bp.get("blueprint_runs") or 0)
            except Exception:
                remaining_runs = None

        candidates.append(
            {
                "blueprint_type_id": bp.get("type_id"),
                "blueprint": bp.get("type_name"),
                "type": "BPC" if is_bpc else "BPO",
                "remaining_runs": remaining_runs,
                "job_runs": int(job_runs_i),
                "units_per_run": int(qty_per_run_i),
                "units_total": int(qty_total_i),
                "ME": bp.get("blueprint_material_efficiency_percent"),
                "TE": bp.get("blueprint_time_efficiency_percent"),
                "skill_requirements_met": bool(bp.get("skill_requirements_met", False)),
                "solar_system": (solar.get("name") if isinstance(solar, dict) else None),
                "sec": (solar.get("security_status") if isinstance(solar, dict) else None),
                "location": (loc.get("display_name") if isinstance(loc, dict) else None),
            }
        )

    if not candidates:
        st.info("No blueprints found that can produce this product (with the current filters).")
        return

    candidates_df = pd.DataFrame(candidates).reset_index(drop=True)
    # Blueprint overview: show blueprint icon, hide blueprint_type_id.
    candidates_display_df = candidates_df.copy()
    if "blueprint_type_id" in candidates_display_df.columns:
        try:
            candidates_display_df.insert(
                0,
                "Icon",
                candidates_display_df.apply(
                    lambda r: _blueprint_image_url(r.get("blueprint_type_id"), is_bpc=(str(r.get("type") or "").upper() == "BPC"), size=32),
                    axis=1,
                ),
            )
        except Exception:
            pass
        candidates_display_df = candidates_display_df.drop(columns=["blueprint_type_id"], errors="ignore")
    st.dataframe(
        candidates_display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Icon": st.column_config.ImageColumn("Icon", width="small"),
        }
        if "Icon" in candidates_display_df.columns
        else None,
    )

    # Pick a blueprint candidate
    label_by_idx: dict[int, str] = {}
    idxs: list[int] = list(range(len(candidates_df)))
    for idx in idxs:
        r = candidates_df.iloc[idx]
        bp_name = str(r.get("blueprint") or "")
        kind = str(r.get("type") or "")
        loc = str(r.get("location") or "")
        rr = r.get("remaining_runs")
        rr_txt = ""
        try:
            if rr is not None and not bool(pd.isna(rr)):
                rr_txt = f" runs:{int(rr)}"
        except Exception:
            rr_txt = ""
        label_by_idx[int(idx)] = f"{bp_name} [{kind}] @ {loc}{rr_txt}"

    selected_idx = st.selectbox(
        "Select a blueprint",
        options=idxs,
        format_func=lambda i: label_by_idx.get(int(i), str(i)),
    )

    def _safe_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return default

    def _safe_int_opt(v: Any) -> int | None:
        try:
            if v is None:
                return None
            return int(v)
        except Exception:
            return None

    def _safe_float_opt(v: Any) -> float | None:
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    selected_row = candidates_df.iloc[int(selected_idx)]
    selected_bp_type_id_i = _safe_int(selected_row.get("blueprint_type_id"), default=0)
    full_bp_data = next(
        (bp for bp in filtered_blueprints if _safe_int(bp.get("type_id"), default=0) == selected_bp_type_id_i),
        None,
    )
    if not isinstance(full_bp_data, dict):
        st.warning("Blueprint details not found.")
        return

    bp_id = full_bp_data.get("type_id")
    bp_name = full_bp_data.get("type_name", "Unknown")
    flags = full_bp_data.get("flags", {}) or {}
    is_bpc = bool(flags.get("is_blueprint_copy")) if isinstance(flags, dict) else False
    variation = "bpc" if is_bpc else "bp"
    skill_requirements_met = bool(full_bp_data.get("skill_requirements_met", False))

    manufacture_job = full_bp_data.get("manufacture_job", {}) or {}
    mj_props = manufacture_job.get("properties", {}) or {}
    job_runs = mj_props.get("job_runs")

    materials = manufacture_job.get("required_materials", []) or []
    required_skills = manufacture_job.get("required_skills") or []

    col_bp_icon, col_bp_title = st.columns([1, 11])
    with col_bp_icon:
        if bp_id:
            st.markdown(
                f"<img src='https://images.evetech.net/types/{bp_id}/{variation}?size=64' alt='Icon' />",
                unsafe_allow_html=True,
            )
    with col_bp_title:
        st.markdown(f"### {bp_name}")
        if bp_id:
            st.caption(f"Blueprint typeID: {bp_id}")

    with st.expander("Manufacturing Job", expanded=True):
        st.markdown("#### Required Skills")
        st.write(f"Met: **{'Yes' if skill_requirements_met else 'No'}**")
        with st.expander("Show skill details", expanded=False):
            if isinstance(required_skills, list) and required_skills:
                rows = []
                for s in required_skills:
                    if not isinstance(s, dict):
                        continue
                    rows.append(
                        {
                            "Skill": s.get("type_name"),
                            "Required": s.get("required_level"),
                            "Character": s.get("character_level"),
                            "Met": bool(s.get("met", False)),
                        }
                    )
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            else:
                st.info("No required skills data available.")

        st.markdown("#### Required Materials")

        # Planner is computed server-side as part of industry_builder_data.
        plan_rows = full_bp_data.get("submanufacturing_plan") or []
        plan_by_type_id: dict[int, dict] = {}
        for r in plan_rows:
            if not isinstance(r, dict):
                continue
            tid = r.get("type_id")
            tid_i = _safe_int(tid, default=0)
            if tid_i <= 0:
                continue
            plan_by_type_id[int(tid_i)] = r

        # --- Required blueprints (not owned) ---
        # Based on the submanufacturing tree: only include nodes where we recommend building
        # and the required blueprint is not owned.
        def _collect_unowned_blueprints(nodes: list[dict]) -> list[dict]:
            by_bp: dict[int, dict] = {}

            def _walk(n: dict) -> None:
                if not isinstance(n, dict):
                    return
                rec = str(n.get("recommendation") or "").lower()
                build = n.get("build") if isinstance(n.get("build"), dict) else None
                if rec == "build" and isinstance(build, dict):
                    owned = build.get("blueprint_owned")
                    if owned is False:
                        bp_type_id = _safe_int(build.get("blueprint_type_id"), default=0)
                        if bp_type_id > 0:
                            slot = by_bp.get(bp_type_id)
                            if slot is None:
                                eff = build.get("blueprint_efficiency")
                                if not isinstance(eff, dict):
                                    eff = {}
                                me_v = eff.get("me_percent")
                                te_v = eff.get("te_percent")
                                try:
                                    me_f = float(me_v) if me_v is not None else None
                                except Exception:
                                    me_f = None
                                try:
                                    te_f = float(te_v) if te_v is not None else None
                                except Exception:
                                    te_f = None
                                slot = {
                                    "blueprint_type_id": int(bp_type_id),
                                    "Icon": _blueprint_image_url(int(bp_type_id), is_bpc=False, size=32),
                                    "Blueprint": str(build.get("blueprint_type_name") or bp_type_id),
                                    "Assumed ME": me_f,
                                    "Assumed TE": te_f,
                                    "Assumption Source": str(eff.get("source") or ""),
                                    "Est. BPO Buy Cost": build.get("blueprint_bpo_buy_cost_isk"),
                                    "Used For": set(),
                                }
                                by_bp[int(bp_type_id)] = slot

                            prod_name = str(n.get("type_name") or n.get("type_id") or "")
                            try:
                                req_qty = int(n.get("required_quantity") or 0)
                            except Exception:
                                req_qty = 0
                            if prod_name:
                                slot["Used For"].add(f"{prod_name} ({req_qty})")

                children = n.get("children")
                if isinstance(children, list):
                    for ch in children:
                        if isinstance(ch, dict):
                            _walk(ch)

            if isinstance(nodes, list):
                for root in nodes:
                    if isinstance(root, dict):
                        _walk(root)

            rows: list[dict] = []
            for bp_type_id, slot in by_bp.items():
                used_for = slot.get("Used For")
                used_list = []
                if isinstance(used_for, set):
                    used_list = sorted([str(x) for x in used_for if x])
                used_txt = ", ".join(used_list[:4])
                if len(used_list) > 4:
                    used_txt += " …"
                rows.append(
                    {
                        "Icon": slot.get("Icon"),
                        "Blueprint": slot.get("Blueprint"),
                        "Assumed ME": slot.get("Assumed ME"),
                        "Assumed TE": slot.get("Assumed TE"),
                        "Assumption Source": slot.get("Assumption Source"),
                        "Est. BPO Buy Cost": slot.get("Est. BPO Buy Cost"),
                        "Used For": used_txt,
                    }
                )

            rows.sort(key=lambda r: str(r.get("Blueprint") or ""))
            return rows

        if plan_rows:
            missing_bp_rows = _collect_unowned_blueprints(plan_rows)
            if missing_bp_rows:
                st.markdown("#### Required Blueprints (Not Owned)")
                st.caption(
                    "These submanufacturing steps recommend **build**, but you don't own the blueprint. "
                    "ME/TE shown are the planner assumptions for unowned blueprints (best-effort)."
                )
                df_missing_bp = pd.DataFrame(missing_bp_rows)
                st.dataframe(
                    df_missing_bp,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "Icon": st.column_config.ImageColumn("Icon", width="small"),
                        "Assumed ME": st.column_config.NumberColumn("Assumed ME", format="%.0f%%"),
                        "Assumed TE": st.column_config.NumberColumn("Assumed TE", format="%.0f%%"),
                        "Est. BPO Buy Cost": st.column_config.NumberColumn("Est. BPO Buy Cost", format="%.0f ISK"),
                    },
                )

        mat_rows = []
        for m in materials:
            if not isinstance(m, dict):
                continue
            tid = m.get("type_id")
            if tid is None:
                continue
            try:
                tid_i = int(tid)
            except Exception:
                continue

            qty = m.get("quantity_after_efficiency")
            if qty is None:
                qty = m.get("quantity_me0")
            try:
                qty_i = int(qty or 0)
            except Exception:
                qty_i = 0

            # Prefer effective costs (buy/build) computed server-side.
            unit_price = m.get("effective_unit_cost_isk")
            total_cost = m.get("effective_total_cost_isk")
            if unit_price is None:
                unit_price = m.get("unit_price_isk")
            if total_cost is None:
                total_cost = m.get("total_cost_isk")

            plan = plan_by_type_id.get(tid_i)
            rec = plan.get("recommendation") if isinstance(plan, dict) else None
            buy_cost = plan.get("buy_cost_isk") if isinstance(plan, dict) else None
            build = plan.get("build") if isinstance(plan, dict) and isinstance(plan.get("build"), dict) else None
            build_cost = (build or {}).get("total_build_cost_isk") if isinstance(build, dict) else None
            effective_cost = plan.get("effective_cost_isk") if isinstance(plan, dict) else None

            roi_build = None
            if rec == "build" and buy_cost is not None and effective_cost is not None:
                try:
                    bc = float(effective_cost)
                    profit = float(buy_cost) - float(effective_cost)
                    roi_build = (profit / bc) * 100.0 if bc > 0 else None
                except Exception:
                    roi_build = None

            mat_rows.append(
                {
                    "type_id": int(tid_i),
                    "Icon": _type_icon_url(tid_i, size=32),
                    "Material": m.get("type_name"),
                    "Qty": int(qty_i),
                    "Unit Price": float(unit_price) if unit_price is not None else None,
                    "Buy Cost": float(total_cost) if total_cost is not None else None,
                    "FIFO (Market)": (
                        _safe_float_opt(m.get("inventory_fifo_market_buy_cost_isk"))
                    ),
                    "FIFO (Built)": (
                        _safe_float_opt(m.get("inventory_fifo_industry_build_cost_isk"))
                    ),
                    "Inv Used": (
                        _safe_int_opt(m.get("inventory_used_qty"))
                    ),
                    "Buy-Now Qty": (
                        _safe_int_opt(m.get("inventory_buy_now_qty"))
                    ),
                    "Action": rec,
                    "Build Cost": float(build_cost) if build_cost is not None else None,
                    "Build ROI": float(roi_build) if roi_build is not None else None,
                }
            )

        mat_df = pd.DataFrame(mat_rows)
        if mat_rows:

            st.markdown("#### Materials (Buy/Build tree)")

            st.markdown(
                """
                    <style>
                    .tree-name {
                        display: inline-flex;
                        align-items: center;
                        gap: 0.35rem;
                    }
                    .tree-caret {
                        display: inline-block;
                        width: 0.9rem;
                        opacity: 0.7;
                        font-weight: 600;
                    }
                    .tree-caret-open {
                        transform: rotate(90deg);
                    }

                    .qty-cell {
                        color: inherit !important;
                    }

                    .cost-good {
                        color: #2ecc71;
                        font-weight: 600;
                    }
                    .cost-bad {
                        color: #e74c3c;
                        font-weight: 600;
                    }

                    hr.tree-divider {
                        margin: 0.2rem 0 !important;
                        border: 0;
                        border-top: 1px solid rgba(128, 128, 128, 0.35);
                    }
                    </style>
                """,

                unsafe_allow_html=True,
            )

            def _fmt_isk(value: float | int | str | None) -> str:
                try:
                    if value is None:
                        return "-"
                    return f"{float(value):,.0f} ISK"
                except Exception:
                    return "-"

            def _fmt_pct(value: float | int | str | None) -> str:
                try:
                    if value is None:
                        return "-"
                    return f"{float(value):.2f}%"
                except Exception:
                    return "-"

            # If planner data is available, render everything as a single aligned tree table.
            if plan_rows:
                col_widths = [0.9, 3.4, 0.8, 1.2, 1.4, 1.4, 1.0]

                h = st.columns(col_widths)
                h[0].markdown("**Action**")
                h[1].markdown("**Name**")
                h[2].markdown("**Qty**")
                h[3].markdown("**Unit**")
                h[4].markdown("**Buy Cost**")
                h[5].markdown("**Build Cost**")
                h[6].markdown("**ROI**")

                def _render_tree_row(node: dict, *, level: int) -> None:
                    if not isinstance(node, dict):
                        return

                    rec = str(node.get("recommendation") or "-")
                    name = str(node.get("type_name") or node.get("type_id") or "")
                    qty = node.get("required_quantity")
                    unit = node.get("buy_unit_price_isk")
                    buy_cost = node.get("buy_cost_isk")

                    build = node.get("build") if isinstance(node.get("build"), dict) else None
                    build_cost = build.get("total_build_cost_isk") if isinstance(build, dict) else None

                    # ROI rule:
                    # - If rec == build: show ROI (buy vs build)
                    # - If rec == buy but build_cost exists: show ROI only when building would be worse (negative)
                    roi_display = None
                    if buy_cost is not None and build_cost is not None:
                        try:
                            bc = float(build_cost)
                            roi_if_build = ((float(buy_cost) - float(build_cost)) / bc) * 100.0 if bc > 0 else None
                        except Exception:
                            roi_if_build = None

                        if rec == "build":
                            roi_display = roi_if_build
                        elif rec == "buy" and roi_if_build is not None and roi_if_build < 0:
                            roi_display = roi_if_build

                    children = node.get("children") or []
                    has_children = isinstance(children, list) and len(children) > 0

                    # Only show an indented required-material list when we actually recommend building.
                    show_children = bool(has_children and rec == "build")

                    indent_px = int(level) * 18
                    safe_name = html.escape(name)
                    caret_class = "tree-caret tree-caret-open" if show_children else "tree-caret"
                    icon_url = _type_icon_url(node.get("type_id"), size=32)
                    icon_html = (
                        f"<img src='{html.escape(icon_url)}' style='width:32px;height:32px' /> "
                        if isinstance(icon_url, str) and icon_url
                        else ""
                    )
                    name_html = (
                        f"<span class='tree-name' style='padding-left:{indent_px}px'>"
                        f"<span class='{caret_class}'>&gt;</span>{icon_html}{safe_name}"
                        f"</span>"
                    )

                    # Cost coloring rules (when both values exist):
                    # - Buy cost red when > Build cost else green
                    # - Build cost red when > Buy cost else green
                    buy_class = None
                    build_class = None
                    if buy_cost is not None and build_cost is not None:
                        try:
                            buy_v = float(buy_cost)
                            build_v = float(build_cost)
                            buy_class = "cost-bad" if buy_v > build_v else "cost-good"
                            build_class = "cost-bad" if build_v > buy_v else "cost-good"
                        except Exception:
                            buy_class = None
                            build_class = None

                    buy_html = _fmt_isk(buy_cost) if buy_cost is not None else "-"
                    build_html = _fmt_isk(build_cost) if build_cost is not None else "-"
                    if buy_class is not None and buy_cost is not None:
                        buy_html = f"<span class='{buy_class}'>{html.escape(buy_html)}</span>"
                    if build_class is not None and build_cost is not None:
                        build_html = f"<span class='{build_class}'>{html.escape(build_html)}</span>"

                    # Inventory/FIFO attribution (optional): show a compact breakdown under Buy Cost.
                    inv_used_qty = node.get("inventory_used_qty")
                    fifo_mkt = node.get("inventory_fifo_market_buy_cost_isk")
                    fifo_built = node.get("inventory_fifo_industry_build_cost_isk")
                    unknown_qty = node.get("inventory_unknown_cost_qty")
                    buy_now_qty = node.get("buy_now_qty")
                    market_unit = node.get("buy_unit_price_isk")

                    try:
                        inv_used_i = int(inv_used_qty) if inv_used_qty is not None else 0
                    except Exception:
                        inv_used_i = 0

                    breakdown_parts: list[str] = []
                    fifo_mkt_qty = node.get("inventory_fifo_market_buy_qty")
                    fifo_built_qty = node.get("inventory_fifo_industry_build_qty")

                    if fifo_mkt is not None:
                        try:
                            q_m = int(fifo_mkt_qty) if fifo_mkt_qty is not None else None
                        except Exception:
                            q_m = None
                        qty_label = f"{q_m}u " if q_m is not None and q_m > 0 else ""
                        breakdown_parts.append(f"mkt {qty_label}{_fmt_isk(fifo_mkt)}")
                    if fifo_built is not None:
                        try:
                            q_b = int(fifo_built_qty) if fifo_built_qty is not None else None
                        except Exception:
                            q_b = None
                        qty_label = f"{q_b}u " if q_b is not None and q_b > 0 else ""
                        breakdown_parts.append(f"built {qty_label}{_fmt_isk(fifo_built)}")

                    try:
                        market_priced_qty = int(unknown_qty or 0) + int(buy_now_qty or 0)
                    except Exception:
                        market_priced_qty = 0

                    market_priced_cost = None
                    if market_priced_qty > 0 and market_unit is not None:
                        try:
                            market_priced_cost = float(market_priced_qty) * float(market_unit)
                        except Exception:
                            market_priced_cost = None
                    if market_priced_cost is not None and market_priced_cost > 0:
                        breakdown_parts.append(f"mkt-priced {market_priced_qty}u {_fmt_isk(market_priced_cost)}")
                    elif market_priced_qty > 0:
                        breakdown_parts.append(f"mkt-priced {market_priced_qty}u")

                    if inv_used_i > 0 and breakdown_parts:
                        breakdown_text = " | ".join([html.escape(str(x)) for x in breakdown_parts])
                        buy_html = (
                            f"{buy_html}<br/>"
                            f"<span style='opacity:0.75;font-size:0.80em'>inv-{breakdown_text}</span>"
                        )

                    roi_html = "-"
                    if roi_display is not None:
                        roi_text = _fmt_pct(roi_display)
                        roi_class = "cost-good" if float(roi_display) > 0 else "cost-bad"
                        roi_html = f"<span class='{roi_class}'>{html.escape(roi_text)}</span>"

                    row = st.columns(col_widths)
                    row[0].write(rec)
                    row[1].markdown(name_html, unsafe_allow_html=True)
                    qty_text = str(int(qty)) if qty is not None else "-"
                    row[2].markdown(f"<span class='qty-cell'>{html.escape(qty_text)}</span>", unsafe_allow_html=True)
                    row[3].write(_fmt_isk(unit) if unit is not None else "-")
                    row[4].markdown(buy_html, unsafe_allow_html=True)
                    row[5].markdown(build_html, unsafe_allow_html=True)
                    row[6].markdown(roi_html, unsafe_allow_html=True)

                    if show_children:
                        for ch in children:
                            _render_tree_row(ch, level=level + 1)

                for idx, root in enumerate(plan_rows):
                    _render_tree_row(root, level=0)
                    if idx < (len(plan_rows) - 1):
                        st.markdown("<hr class='tree-divider' />", unsafe_allow_html=True)
            else:
                # Fallback when planner isn't available: show the simple materials table.
                st.dataframe(
                    mat_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "Icon": st.column_config.ImageColumn("Icon", width="small"),
                        "Unit Price": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
                        "Buy Cost": st.column_config.NumberColumn("Buy Cost", format="%.0f ISK"),
                        "FIFO (Market)": st.column_config.NumberColumn("FIFO (Market)", format="%.0f ISK"),
                        "FIFO (Built)": st.column_config.NumberColumn("FIFO (Built)", format="%.0f ISK"),
                        "Build Cost": st.column_config.NumberColumn("Build Cost", format="%.0f ISK"),
                        "Build ROI": st.column_config.NumberColumn("Build ROI", format="%.2f%%"),
                    },
                )
        else:
            st.info("No materials required")

    with st.expander("View Raw Blueprint Data (Debug)"):
        st.json(full_bp_data)
