import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]

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
from utils.formatters import (
    blueprint_image_url,
    format_decimal_eu,
    format_duration,
    format_isk_eu,
    format_pct_eu,
    type_icon_url,
)
from utils.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_eu_pct_formatter, js_icon_cell_renderer

from utils.industry_builder_utils import (
    BUILD_TREE_CAPTION,
    MATERIALS_TABLE_COLUMN_CONFIG,
    attach_aggrid_autosize,
    blueprint_passes_filters,
    coerce_fraction,
    industry_invention_cache_key,
    min_known_positive,
    parse_json_cell,
    safe_float_opt,
    safe_int,
    safe_int_opt,
)

@st.cache_data(ttl=3600)
def _get_industry_profiles(character_id: int) -> dict | None:
    return api_get(f"/industry_profiles/{int(character_id)}")


def _industry_builder_setup_and_get_industry_data() -> dict[str, Any]:
    """Build page context and fetch Industry Builder data.

    This function intentionally owns the heavy setup + update/polling workflow so the
    public `render()` stays small enough for static analyzers.
    """

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

    assert AgGrid is not None
    assert GridOptionsBuilder is not None
    assert JsCode is not None

    db: Any = None
    cfg: Any = None

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
        return {
            "cfg": cfg,
            "characters_df": characters_df,
            "selected_character_id": None,
            "selected_profile_id": None,
            "pricing_key": "",
            "effective_sales_tax_fraction": 0.0,
            "effective_broker_fee_fraction": 0.0,
            "industry_data": [],
        }

    # Background-update state (set up early so we can avoid extra backend calls during polling reruns).
    if "industry_builder_job_id" not in st.session_state:
        st.session_state["industry_builder_job_id"] = None
    if "industry_builder_job_key" not in st.session_state:
        st.session_state["industry_builder_job_key"] = None
    if "industry_builder_job_inflight" not in st.session_state:
        # True only while the "start update job" API call is in progress.
        st.session_state["industry_builder_job_inflight"] = False
    if "industry_builder_selected_profile_id" not in st.session_state:
        st.session_state["industry_builder_selected_profile_id"] = None
    if "industry_profiles_cache" not in st.session_state:
        st.session_state["industry_profiles_cache"] = {}

    # UI default: maximize runs for BPCs unless the user changes it.
    if "maximize_blueprint_runs" not in st.session_state:
        st.session_state["maximize_blueprint_runs"] = True

    # Optional workflow toggle: when you primarily own BPOs (often via corporation) but
    # manufacture from BPCs (copy -> manufacture), include estimated copy overhead even
    # for BPO-based manufacturing.
    if "industry_builder_assume_bpo_copy_overhead" not in st.session_state:
        st.session_state["industry_builder_assume_bpo_copy_overhead"] = True

    # Optional planning toggle: prefer consuming inventory (FIFO-valued) even when
    # it would be cheaper to buy/build at current market prices.
    if "industry_builder_prefer_inventory_consumption" not in st.session_state:
        st.session_state["industry_builder_prefer_inventory_consumption"] = True

    # Cache invention options per (character, blueprint_type_id) to avoid repeated calls.
    if "industry_invention_options_cache" not in st.session_state:
        st.session_state["industry_invention_options_cache"] = {}

    job_id = st.session_state.get("industry_builder_job_id")
    job_running = bool(job_id)
    job_inflight = bool(st.session_state.get("industry_builder_job_inflight"))

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
    default_sales_tax_fraction_cfg = coerce_fraction(_cfg_default_sales_tax, default=0.03375)
    default_broker_fee_fraction_cfg = coerce_fraction(_cfg_default_broker_fee, default=0.03)
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
    default_sales_tax_fraction = coerce_fraction(
        st.session_state.get("industry_builder_sales_tax_fraction"),
        default=float(default_sales_tax_fraction_cfg),
    )
    default_broker_fee_fraction = float(default_broker_fee_fraction_cfg)

    market_fees_obj: dict[str, Any] | None = None
    try:
        row = characters_df.loc[characters_df["character_id"] == selected_character_id].iloc[0]
        market_fees_obj = parse_json_cell(row.get("market_fees")) if hasattr(row, "get") else None
        if market_fees_obj is not None and not isinstance(market_fees_obj, dict):
            market_fees_obj = None
    except Exception:
        market_fees_obj = None

    jita_rates = (((market_fees_obj or {}).get("jita_4_4") or {}).get("rates") or {}) if isinstance(market_fees_obj, dict) else {}
    effective_sales_tax_fraction = coerce_fraction(jita_rates.get("sales_tax_fraction"), default=float(default_sales_tax_fraction))
    effective_broker_fee_fraction = coerce_fraction(jita_rates.get("broker_fee_fraction"), default=float(default_broker_fee_fraction))

    # --- Explicit update workflow (required because full submanufacturing is expensive) ---
    # No backend calls happen here unless the user clicks the button.
    pricing_key = (
        f"jita:{st.session_state.get('industry_builder_material_price_source')}:"
        f"{st.session_state.get('industry_builder_product_price_source')}:"
        f"{str(st.session_state.get('industry_builder_orderbook_smoothing') or 'median_best_n')}:"
        f"depth{int(st.session_state.get('industry_builder_orderbook_depth') or 5)}:"
        f"stax{float(effective_sales_tax_fraction or 0.0):.6f}:"
        f"bfee{float(effective_broker_fee_fraction or 0.0):.6f}:"
        f"preferinv{1 if bool(st.session_state.get('industry_builder_prefer_inventory_consumption')) else 0}:"
        f"bpocopy{1 if bool(st.session_state.get('industry_builder_assume_bpo_copy_overhead')) else 0}"
    )
    key = f"{int(selected_character_id)}:{int(selected_profile_id or 0)}:{1 if maximize_runs else 0}:{pricing_key}"
    cache: dict[str, dict] = st.session_state.setdefault("industry_builder_cache", {})

    cached = cache.get(key) if isinstance(cache, dict) else None
    if isinstance(cached, dict) and isinstance(cached.get("data"), list):
        industry_data = cached.get("data") or []
    else:
        industry_data = []

    st.markdown("#### Update")
    st.caption(
        "Compute full Industry Builder data (incl. submanufacturing) for all owned blueprints. "
        "This can take a while; results are cached for this session."
    )

    with st.expander("Update Industry Job settings", expanded=False):
        st.markdown("**Market Pricing (Profit/ROI)**")
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

        st.markdown("**Submanufacturing planning**")
        st.checkbox(
            "Prefer consuming inventory (FIFO) even if suboptimal",
            key="industry_builder_prefer_inventory_consumption",
            disabled=job_running,
            help=(
                "When enabled, submanufacturing planning will prefer consuming on-hand inventory (valued at FIFO) "
                "instead of switching to build decisions just because building is cheaper than your FIFO book value."
            ),
        )

        st.markdown("**Blueprint workflow**")
        st.checkbox(
            "Assume BPOs are copied to BPCs before manufacturing (include copy overhead)",
            key="industry_builder_assume_bpo_copy_overhead",
            disabled=job_running,
            help=(
                "When enabled, manufacturing jobs based on a BPO will include an estimated blueprint copying job "
                "to produce enough BPC runs. BPC-based blueprints already include this overhead."
            ),
        )

    col_update, col_clear = st.columns([1, 1])
    with col_update:
        if st.button("Update Industry Jobs", type="primary", disabled=(job_running or job_inflight)):
            try:
                st.session_state["industry_builder_job_inflight"] = True
                payload = {
                    "profile_id": (int(selected_profile_id) if selected_profile_id is not None else None),
                    "maximize_runs": bool(maximize_runs),
                    "prefer_inventory_consumption": bool(st.session_state.get("industry_builder_prefer_inventory_consumption")),
                    "assume_bpo_copy_overhead": bool(st.session_state.get("industry_builder_assume_bpo_copy_overhead")),
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
                    st.session_state["industry_builder_job_inflight"] = False
                else:
                    job_id = (resp.get("data") or {}).get("job_id")
                    st.session_state["industry_builder_job_id"] = job_id
                    st.session_state["industry_builder_job_key"] = key
                    st.session_state["industry_builder_selected_profile_id"] = selected_profile_id
                    st.session_state["industry_builder_job_inflight"] = False
                    # Clear cached data for this key to avoid stale display.
                    if isinstance(cache, dict):
                        cache.pop(key, None)
                    st.rerun()
            except Exception as e:
                st.error(f"Error calling backend: {e}")
                st.session_state["industry_builder_job_inflight"] = False

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
                st.session_state["industry_builder_job_inflight"] = False
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
                st.session_state["industry_builder_job_inflight"] = False
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
        # Avoid flicker while an update is in progress.
        if not bool(st.session_state.get("industry_builder_job_id")) and not bool(st.session_state.get("industry_builder_job_inflight")):
            st.info("Click **Update Industry Jobs** to load data.")
        return {
            "cfg": cfg,
            "characters_df": characters_df,
            "selected_character_id": int(selected_character_id),
            "selected_profile_id": (int(selected_profile_id) if selected_profile_id is not None else None),
            "pricing_key": str(pricing_key),
            "effective_sales_tax_fraction": float(effective_sales_tax_fraction),
            "effective_broker_fee_fraction": float(effective_broker_fee_fraction),
            "industry_data": [],
        }

    return {
        "cfg": cfg,
        "characters_df": characters_df,
        "selected_character_id": int(selected_character_id),
        "selected_profile_id": (int(selected_profile_id) if selected_profile_id is not None else None),
        "pricing_key": str(pricing_key),
        "effective_sales_tax_fraction": float(effective_sales_tax_fraction),
        "effective_broker_fee_fraction": float(effective_broker_fee_fraction),
        "industry_data": industry_data,
    }


def render():
    st.subheader("Industry Builder")
    ctx = _industry_builder_setup_and_get_industry_data()

    # AgGrid availability is guaranteed by `_industry_builder_setup_and_get_industry_data()`.
    assert AgGrid is not None
    assert GridOptionsBuilder is not None
    assert JsCode is not None

    cfg = ctx.get("cfg")
    characters_df = ctx.get("characters_df")
    selected_character_id = ctx.get("selected_character_id")
    selected_profile_id = ctx.get("selected_profile_id")
    pricing_key = ctx.get("pricing_key")
    effective_sales_tax_fraction = float(ctx.get("effective_sales_tax_fraction") or 0.0)
    effective_broker_fee_fraction = float(ctx.get("effective_broker_fee_fraction") or 0.0)
    industry_data = ctx.get("industry_data") or []

    if not selected_character_id:
        return

    if not isinstance(industry_data, list) or not industry_data:
        return

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
        col1, col2, col3, col4 = st.columns(4)
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
            include_inventions = st.checkbox(
                "Include Inventions",
                value=True,
                key="industry_builder_include_inventions",
                help="When enabled, adds a best-ROI invention-derived manufacturing row for T2 items.",
            )
            st.checkbox(
                "Maximize Blueprint Runs",
                value=bool(st.session_state.get("maximize_blueprint_runs", True)),
                key="maximize_blueprint_runs",
                help="If enabled, BPC calculations use all remaining runs (materials, time, fees, and copy overhead).",
            )

        with col3:
            roi_lbl_col, roi_cb_col, roi_val_col = st.columns([0.38, 0.12, 0.50])
            with roi_lbl_col:
                st.markdown("**Min. ROI (%)**")
            with roi_cb_col:
                apply_min_roi = st.checkbox(
                    "Apply Min ROI",
                    value=False,
                    key="industry_builder_apply_min_roi",
                    label_visibility="collapsed",
                )
            with roi_val_col:
                min_roi_pct = st.number_input(
                    "Min ROI (%)",
                    min_value=0.0,
                    max_value=10_000.0,
                    value=10.0,
                    step=1.0,
                    disabled=not bool(apply_min_roi),
                    key="industry_builder_min_roi_pct",
                    label_visibility="collapsed",
                )

            profit_lbl_col, profit_cb_col, profit_val_col = st.columns([0.38, 0.12, 0.50])
            with profit_lbl_col:
                st.markdown("**Min. Profit (ISK)**")
            with profit_cb_col:
                apply_min_profit = st.checkbox(
                    "Apply Min Profit",
                    value=False,
                    key="industry_builder_apply_min_profit",
                    label_visibility="collapsed",
                )
            with profit_val_col:
                min_profit_isk = st.number_input(
                    "Min Profit (ISK)",
                    min_value=0,
                    max_value=10_000_000_000_000,
                    value=1_000_000,
                    step=100_000,
                    disabled=not bool(apply_min_profit),
                    key="industry_builder_min_profit_isk",
                    label_visibility="collapsed",
                )

            iskh_lbl_col, iskh_cb_col, iskh_val_col = st.columns([0.38, 0.12, 0.50])
            with iskh_lbl_col:
                st.markdown("**Min. ISK/h**")
            with iskh_cb_col:
                apply_min_iskh = st.checkbox(
                    "Apply Min ISK/h",
                    value=False,
                    key="industry_builder_apply_min_iskh",
                    label_visibility="collapsed",
                    help="Filters by the 'Profit / hour' column.",
                )
            with iskh_val_col:
                min_iskh_isk = st.number_input(
                    "Min ISK/h",
                    min_value=0,
                    max_value=10_000_000_000_000,
                    value=1_000_000,
                    step=100_000,
                    disabled=not bool(apply_min_iskh),
                    key="industry_builder_min_iskh_isk",
                    label_visibility="collapsed",
                )
            

        with col4:
            location_filter = st.selectbox("Location", options=location_options, index=0)
            selected_categories = st.multiselect(
                "Categories",
                options=category_options,
                default=[],
                help="Filter table rows by the produced item's category.",
            )

    filtered_blueprints = [
        bp
        for bp in industry_data
        if blueprint_passes_filters(
            bp,
            maximize_blueprint_runs=bool(st.session_state.get("maximize_blueprint_runs", False)),
            bp_type_filter=bp_type_filter,
            skill_req_filter=skill_req_filter,
            reactions_filter=reactions_filter,
            location_filter=location_filter,
        )
    ]

    # -----------------
    # Invention helpers
    # -----------------
    invention_cache: dict[str, Any] = st.session_state.get("industry_invention_options_cache") or {}
    if not isinstance(invention_cache, dict):
        invention_cache = {}
        st.session_state["industry_invention_options_cache"] = invention_cache

    def _get_invention_options(*, blueprint_type_id: int, force: bool = False) -> dict[str, Any] | None:
        k = industry_invention_cache_key(
            character_id=int(selected_character_id),
            blueprint_type_id=int(blueprint_type_id),
            profile_id=(int(selected_profile_id) if selected_profile_id is not None else None),
            pricing_key=str(pricing_key),
        )
        if not force:
            cached = invention_cache.get(k)
            if isinstance(cached, dict) and cached.get("status") == "success":
                return cached

        payload: dict[str, Any] = {
            "profile_id": (int(selected_profile_id) if selected_profile_id is not None else None),
            "pricing_preferences": {
                "hub": "jita",
                "material_price_source": str(st.session_state.get("industry_builder_material_price_source") or "Jita Sell"),
                "product_price_source": str(st.session_state.get("industry_builder_product_price_source") or "Jita Sell"),
                "sales_tax_fraction": float(effective_sales_tax_fraction),
                "broker_fee_fraction": float(effective_broker_fee_fraction),
                "orderbook_smoothing": str(st.session_state.get("industry_builder_orderbook_smoothing") or "median_best_n"),
                "orderbook_depth": int(st.session_state.get("industry_builder_orderbook_depth") or 5),
            },
        }

        try:
            resp = api_post(
                f"/industry_invention_options/{int(selected_character_id)}/{int(blueprint_type_id)}",
                payload,
            )
        except Exception:
            resp = None

        if isinstance(resp, dict) and resp.get("status") == "success":
            invention_cache[k] = resp
            return resp

        return None

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
        time_eff = (props.get("total_time_efficiency") or {}) if isinstance(props, dict) else {}

        copy_job = (props.get("copy_job") or {}) if isinstance(props, dict) else {}
        copy_job_cost = (copy_job.get("job_cost") or {}) if isinstance(copy_job, dict) else {}
        copy_job_time = (copy_job.get("time") or {}) if isinstance(copy_job, dict) else {}

        job_runs = props.get("job_runs")
        try:
            job_runs_i = int(job_runs or 1)
        except Exception:
            job_runs_i = 1

        # Job duration (seconds): manufacturing job.
        job_time_seconds: float | None = None
        try:
            v = time_eff.get("estimated_job_time_seconds")
            job_time_seconds = float(v) if v is not None else None
        except Exception:
            job_time_seconds = None

        # Optional blueprint copy overhead (job time). When the backend does not compute
        # copy overhead (or the user disables it), this stays None.
        copy_time_seconds: float | None = None
        try:
            v = copy_job_time.get("estimated_copy_time_seconds")
            copy_time_seconds = float(v) if v is not None else None
        except Exception:
            copy_time_seconds = None

        # Manufacturing job fee (aligns with in-game "Total job cost").
        est_fee_total = cost.get("total_job_cost_isk")
        try:
            est_fee_total_f = float(est_fee_total or 0.0)
        except Exception:
            est_fee_total_f = 0.0

        # Optional blueprint copy overhead (job cost). When the backend does not compute
        # copy overhead (or the user disables it), this stays at 0.
        raw_copy_cost_total = copy_job_cost.get("total_job_cost_isk")
        try:
            copy_cost_total_f = float(raw_copy_cost_total or 0.0)
        except Exception:
            copy_cost_total_f = 0.0

        # Safety override: ensure Copy Cost is zero when the blueprint has no copying
        # activity in the SDE (or older cached payloads still include a copy_job).
        try:
            copying_time_seconds = float(bp.get("copying_time_seconds", 0) or 0.0)
            max_runs = int(bp.get("max_production_limit") or 0)
        except Exception:
            copying_time_seconds = 0.0
            max_runs = 0

        can_copy_from_bpo = bool(copying_time_seconds > 0 and max_runs > 0)
        if not bool(can_copy_from_bpo):
            copy_cost_total_f = 0.0
            copy_time_seconds = None

        total_job_time_seconds: float | None = None
        if job_time_seconds is not None or copy_time_seconds is not None:
            total_job_time_seconds = float(job_time_seconds or 0.0) + float(copy_time_seconds or 0.0)

        job_duration_display = "-"
        if total_job_time_seconds is not None:
            job_duration_display = format_duration(total_job_time_seconds)

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
        profit_total = total_product_value - total_material_cost - copy_cost_total_f - est_fee_total_f
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
            try:
                prod_qty_total = int(prod.get("quantity_total") or prod.get("quantity") or 0)
            except Exception:
                prod_qty_total = 0

            if prod_qty_total <= 0:
                continue

            # Allocate costs/fees by product share.
            share = safe_float_opt(prod.get("allocation_share"))
            if share is None:
                if value_total_sum > 0:
                    share = float(product_value_totals[idx]) / float(value_total_sum)
                elif qty_total_sum > 0:
                    share = float(product_qty_totals[idx]) / float(qty_total_sum)
                else:
                    share = 1.0

            allocated_material_cost = safe_float_opt(prod.get("allocated_material_cost_isk"))
            allocated_copy_cost = safe_float_opt(prod.get("allocated_copy_cost_isk"))
            allocated_job_fee = safe_float_opt(prod.get("allocated_job_fee_isk"))
            allocated_product_value = safe_float_opt(prod.get("allocated_product_value_isk"))

            if allocated_material_cost is None:
                allocated_material_cost = float(total_material_cost) * float(share)
            if allocated_copy_cost is None:
                allocated_copy_cost = float(copy_cost_total_f) * float(share)
            if allocated_job_fee is None:
                allocated_job_fee = float(est_fee_total_f) * float(share)
            if allocated_product_value is None:
                allocated_product_value = float(total_product_value) * float(share)
            allocated_profit = allocated_product_value - allocated_material_cost - allocated_copy_cost - allocated_job_fee

            allocated_broker_fee_buy = float(broker_fee_buy_total) * float(share)
            allocated_broker_fee_sell = float(broker_fee_sell_total) * float(share)
            broker_fee_total = float(allocated_broker_fee_buy) + float(allocated_broker_fee_sell)

            sales_tax_total = float(allocated_product_value) * float(sales_tax_fraction)
            allocated_profit_net = float(allocated_profit) - float(sales_tax_total) - float(broker_fee_total)

            profit_per_hour: float | None = None
            if total_job_time_seconds is not None:
                try:
                    hours = float(total_job_time_seconds) / 3600.0
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
            copy_cost_per_item = allocated_copy_cost / float(prod_qty_total)

            total_cost_total = allocated_material_cost + allocated_copy_cost + allocated_job_fee + float(sales_tax_total)
            total_cost_per_item = float(total_cost_total) / float(prod_qty_total)

            denom_total = allocated_material_cost + allocated_copy_cost + allocated_job_fee + broker_fee_total
            roi_total = (allocated_profit_net / float(denom_total)) if denom_total > 0 else None
            roi_total_percent = (float(roi_total) * 100.0) if roi_total is not None else None

            row = {
                # Produced item grain
                "type_id": prod_type_id,
                "Name": prod_type_name,
                "Category": prod_cat,

                # Internal
                "_product_row_key": f"base:{int(prod_type_id or 0)}",
                "_row_kind": "base",
                "_source_blueprint_type_id": bp.get("type_id"),

                # Job configuration
                "Runs": int(job_runs_i),
                        "sales_tax_fraction": float(effective_sales_tax_fraction),
                        "broker_fee_fraction": float(effective_broker_fee_fraction),
                "Units": int(prod_qty_total),
                "ME": bp.get("blueprint_material_efficiency_percent"),
                "TE": bp.get("blueprint_time_efficiency_percent"),

                "Job Duration": str(job_duration_display),

                # Per-item outputs
                "Mat. Cost / item": float(mat_cost_per_item),
                "Copy Cost / item": float(copy_cost_per_item),
                "Total Cost / item": float(total_cost_per_item),
                "Revenue / item": float(prod_value_per_item),
                "Sales Tax / item": float(sales_tax_per_item),
                "Broker Fee / item": float(broker_fee_per_item),
                "Profit / item": float(profit_per_item),
                "Job Fee / item": float(job_fee_per_item),

                # Totals
                "Mat. Cost": float(allocated_material_cost),
                "Copy Cost": float(allocated_copy_cost),
                "Total Cost": float(total_cost_total),
                "Revenue": float(allocated_product_value),
                "Sales Tax": float(sales_tax_total),
                "Broker Fee": float(broker_fee_total),
                "Profit": float(allocated_profit_net),
                "Profit / hour": float(profit_per_hour) if profit_per_hour is not None else None,
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
                "_total_copy_cost": float(copy_cost_total_f),
                "_job_time_seconds": float(total_job_time_seconds) if total_job_time_seconds is not None else None,
            }

            table_rows.append(row)

    # Inject invention-derived T2 manufacturing rows (best decryptor by ROI) into the product overview.
    # These are "virtual jobs": invention (expected) + manufacturing from the invented T2 BPC.
    if bool(include_inventions):
        best_inv_row_by_product_id: dict[int, dict] = {}

        def _better_invention_row(a: dict | None, b: dict) -> bool:
            if a is None:
                return True
            try:
                a_roi = float(a.get("ROI") or float("-inf"))
            except Exception:
                a_roi = float("-inf")
            try:
                b_roi = float(b.get("ROI") or float("-inf"))
            except Exception:
                b_roi = float("-inf")
            if b_roi != a_roi:
                return b_roi > a_roi
            try:
                a_p = float(a.get("Profit") or float("-inf"))
            except Exception:
                a_p = float("-inf")
            try:
                b_p = float(b.get("Profit") or float("-inf"))
            except Exception:
                b_p = float("-inf")
            return b_p > a_p

        for bp in filtered_blueprints:
            if not isinstance(bp, dict):
                continue
            bp_type_id_i = safe_int(bp.get("type_id"), default=0)
            if bp_type_id_i <= 0:
                continue

            row = bp.get("ui_invention_overview_row")
            if not isinstance(row, dict):
                continue

            prod_type_id = safe_int(row.get("type_id"), default=0)
            if prod_type_id <= 0:
                continue

            cur = best_inv_row_by_product_id.get(int(prod_type_id))
            if _better_invention_row(cur, row):
                best_inv_row_by_product_id[int(prod_type_id)] = row

        for row in best_inv_row_by_product_id.values():
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

    # Keep the main table focused: hide debug/internal fields.
    hidden_cols = {
        "blueprint",
        "_product_row_key",
        "_row_kind",
        "_source_blueprint_type_id",
        "_invention_source_blueprint_type_id",
        "_invention_decryptor",
        "_profit_total",
        "_total_material_cost",
        "_total_product_value",
        "_total_job_fee",
        "_total_copy_cost",
        "_job_time_seconds",
    }
    display_df = products_df.drop(columns=[c for c in hidden_cols if c in products_df.columns], errors="ignore")

    # Add item icon column right after type_id (if available).
    if "type_id" in display_df.columns:
        try:
            icon = display_df["type_id"].apply(lambda tid: type_icon_url(tid, size=32))
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
        "ROI",
        "Profit",
        "Profit / hour",
        "Total Cost",
        "Revenue",
        "Mat. Cost",
        "Copy Cost",
        "Job Fee",
        "Sales Tax",
        "Broker Fee",
        "Total Cost / item",
        "Mat. Cost / item",
        "Copy Cost / item",
        "Job Fee / item",
        "Sales Tax / item",
        "Broker Fee / item",
        "Revenue / item",
        "Profit / item",
        "Location",
        "Solar System",
        "Solar System Security",
        "Category",
    ]
    _cols = [c for c in _preferred_cols if c in display_df.columns]
    _cols += [c for c in display_df.columns if c not in _cols]
    display_df = display_df[_cols]

    eu_locale = "nl-NL"  # '.' thousands, ',' decimals

    img_renderer = js_icon_cell_renderer(JsCode=JsCode, size_px=32)

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
        "Copy Cost",
        "Total Cost",
        "Job Fee",
        "Revenue",
        "Sales Tax",
        "Broker Fee",
        "Profit",
        "Profit / hour",
        "Mat. Cost / item",
        "Copy Cost / item",
        "Total Cost / item",
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
                valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                minWidth=150,
                cellStyle=right,
            )

    if "ROI" in display_df.columns:
        gb.configure_column(
            "ROI",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_pct_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
            minWidth=110,
            cellStyle=right,
        )

    if "Solar System Security" in display_df.columns:
        gb.configure_column(
            "Solar System Security",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
            minWidth=150,
            cellStyle=right,
        )

    for c in ["Runs", "Units"]:
        if c in display_df.columns:
            gb.configure_column(
                c,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                minWidth=110,
                cellStyle=right,
            )

    grid_options = gb.build()
    attach_aggrid_autosize(grid_options, JsCode=JsCode)
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
    if "_product_row_key" not in products_df.columns:
        products_df["_product_row_key"] = products_df["type_id"].apply(lambda tid: f"base:{int(tid or 0)}")
    prod_pairs = (
        products_df[["_product_row_key", "type_id", "Name"]]
        .dropna()
        .drop_duplicates(subset=["_product_row_key"])
        .sort_values(["Name", "type_id"], ascending=[True, True])
    )
    if prod_pairs.empty:
        st.info("No producible products available with the current filters.")
        return

    prod_options = [(str(r["_product_row_key"]), int(r["type_id"]), str(r["Name"])) for _, r in prod_pairs.iterrows()]
    prod_type_id_by_key = {k: int(tid) for k, tid, _ in prod_options}
    prod_label_by_key = {k: name for k, _, name in prod_options}

    selected_product_key = st.selectbox(
        "Select a product",
        options=[k for k, _, _ in prod_options],
        format_func=lambda k: f"{prod_label_by_key.get(str(k), str(k))} (typeID {prod_type_id_by_key.get(str(k), 0)})",
    )

    selected_product_id = int(prod_type_id_by_key.get(str(selected_product_key), 0) or 0)
    selected_product_is_invention = str(selected_product_key).startswith("invention:")

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
        # For invention-derived rows, we still want to show the invented T2 BPC.
        # Avoid showing a misleading warning in that case.
        if not selected_product_is_invention:
            st.info("No blueprints found that can produce this product (with the current filters).")
            return

    # If the user selected the invention-derived product row, inject the invented T2 BPC as a candidate and default-select it.
    invented_candidate = None
    inv_source_bp_type_id = None
    if bool(selected_product_is_invention):
        try:
            inv_row = products_df.loc[products_df["_product_row_key"] == str(selected_product_key)].iloc[0]
            inv_source_bp_type_id = safe_int(inv_row.get("_invention_source_blueprint_type_id"), default=0)
        except Exception:
            inv_source_bp_type_id = 0

        if inv_source_bp_type_id and inv_source_bp_type_id > 0:
            inv_resp = _get_invention_options(blueprint_type_id=int(inv_source_bp_type_id), force=False)
        else:
            inv_resp = None

        if isinstance(inv_resp, dict):
            data = inv_resp.get("data") if isinstance(inv_resp.get("data"), dict) else {}
            options = data.get("options") if isinstance(data.get("options"), list) else []
            best = next((o for o in options if isinstance(o, dict)), None) or {}
            inv = data.get("invention") if isinstance(data.get("invention"), dict) else {}
            base_out = inv.get("base_output") if isinstance(inv.get("base_output"), dict) else {}
            out_bp_type_id = safe_int(base_out.get("blueprint_type_id"), default=0)

            mfg = data.get("manufacturing") if isinstance(data.get("manufacturing"), dict) else {}
            prod_qty_per_run = safe_int(mfg.get("product_quantity_per_run"), default=0)
            invented_runs = safe_int(best.get("invented_runs"), default=0)
            invented_me = safe_int(best.get("invented_me"), default=0)
            invented_te = safe_int(best.get("invented_te"), default=0)

            if out_bp_type_id > 0 and prod_qty_per_run > 0 and invented_runs > 0:
                invented_candidate = {
                    "blueprint_type_id": int(out_bp_type_id),
                    "blueprint": str(base_out.get("blueprint_type_name") or f"typeID {out_bp_type_id}"),
                    "type": "BPC (Invention)",
                    "remaining_runs": int(invented_runs),
                    "job_runs": int(invented_runs),
                    "units_per_run": int(prod_qty_per_run),
                    "units_total": int(invented_runs) * int(prod_qty_per_run),
                    "ME": int(invented_me),
                    "TE": int(invented_te),
                    "skill_requirements_met": True,
                    "solar_system": None,
                    "sec": None,
                    "location": "(invention)",
                    "_candidate_kind": "invention",
                    "_invention_source_blueprint_type_id": int(inv_source_bp_type_id or 0),
                }

    if isinstance(invented_candidate, dict):
        candidates = [invented_candidate, *candidates]

    candidates_df = pd.DataFrame(candidates).reset_index(drop=True)
    # Blueprint overview: show blueprint icon, hide blueprint_type_id.
    candidates_display_df = candidates_df.copy()
    # Hide internal fields (used for selection logic)
    try:
        internal_cols = [c for c in candidates_display_df.columns if str(c).startswith("_")]
        if internal_cols:
            candidates_display_df = candidates_display_df.drop(columns=internal_cols, errors="ignore")
    except Exception:
        pass
    if "blueprint_type_id" in candidates_display_df.columns:
        try:
            candidates_display_df.insert(
                0,
                "Icon",
                candidates_display_df.apply(
                    lambda r: blueprint_image_url(
                        r.get("blueprint_type_id"),
                        is_bpc=str(r.get("type") or "").upper().startswith("BPC"),
                        size=32,
                    ),
                    axis=1,
                ),
            )
        except Exception:
            pass
        candidates_display_df = candidates_display_df.drop(columns=["blueprint_type_id"], errors="ignore")

    # Candidates overview table: use AgGrid for consistency with the main table.
    gb2 = GridOptionsBuilder.from_dataframe(candidates_display_df)
    gb2.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)

    if "Icon" in candidates_display_df.columns:
        gb2.configure_column(
            "Icon",
            header_name="",
            width=62,
            pinned="left",
            sortable=False,
            filter=False,
            suppressAutoSize=True,
            cellRenderer=img_renderer,
        )

    # Light formatting for numeric columns.
    for c in ["ME", "TE", "remaining_runs", "job_runs", "units_per_run", "units_total"]:
        if c in candidates_display_df.columns:
            gb2.configure_column(
                c,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                minWidth=110,
                cellStyle=right,
            )

    if "sec" in candidates_display_df.columns:
        gb2.configure_column(
            "sec",
            header_name="sec",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
            minWidth=110,
            cellStyle=right,
        )

    grid_options2 = gb2.build()
    attach_aggrid_autosize(grid_options2, JsCode=JsCode)

    height2 = min(420, 40 + (len(candidates_display_df) * 35))
    AgGrid(
        candidates_display_df,
        gridOptions=grid_options2,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=height2,
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

    default_idx = 0 if (selected_product_is_invention and isinstance(invented_candidate, dict)) else 0
    selected_idx = st.selectbox(
        "Select a blueprint",
        options=idxs,
        format_func=lambda i: label_by_idx.get(int(i), str(i)),
        index=int(default_idx) if int(default_idx) in idxs else 0,
    )

    selected_row = candidates_df.iloc[int(selected_idx)]
    selected_bp_type_id_i = safe_int(selected_row.get("blueprint_type_id"), default=0)
    candidate_kind = str(selected_row.get("_candidate_kind") or "")

    full_bp_data = None
    if candidate_kind != "invention":
        full_bp_data = next(
            (bp for bp in filtered_blueprints if safe_int(bp.get("type_id"), default=0) == selected_bp_type_id_i),
            None,
        )
        if not isinstance(full_bp_data, dict):
            st.warning("Blueprint details not found.")
            return

    if candidate_kind == "invention":
        # Use invention options to construct a virtual manufacturing job from the invented T2 BPC.
        inv_source = safe_int(selected_row.get("_invention_source_blueprint_type_id"), default=0)
        inv_resp = _get_invention_options(blueprint_type_id=int(inv_source), force=False) if inv_source > 0 else None
        if not isinstance(inv_resp, dict):
            st.warning("Invention details not available for this selection.")
            return
        inv_data = inv_resp.get("data") if isinstance(inv_resp.get("data"), dict) else {}
        inv_opts = inv_data.get("options") if isinstance(inv_data.get("options"), list) else []
        best = next((o for o in inv_opts if isinstance(o, dict)), None) or {}

        inv = inv_data.get("invention") if isinstance(inv_data.get("invention"), dict) else {}
        base_out = inv.get("base_output") if isinstance(inv.get("base_output"), dict) else {}
        mfg = inv_data.get("manufacturing") if isinstance(inv_data.get("manufacturing"), dict) else {}

        bp_id = safe_int(base_out.get("blueprint_type_id"), default=0)
        bp_name = str(base_out.get("blueprint_type_name") or "Invented T2 BPC")
        variation = "bpc"
        skill_requirements_met = True

        materials = (
            inv_data.get("best_manufacturing_required_materials")
            if isinstance(inv_data.get("best_manufacturing_required_materials"), list)
            else []
        )
        required_skills = []
        job_runs = safe_int(best.get("invented_runs"), default=0)
    else:
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

    st.markdown("### Manufacturing Job")
    with st.container():
        if candidate_kind == "invention":
            st.markdown("#### Copy & Invention Jobs")

            units_total = safe_int(selected_row.get("units_total"), default=0)
            total_cost = None
            copy_fee_total = None
            copy_time_total = None
            inv_fee_total = None
            inv_time_total = None
            decryptor = str(best.get("decryptor_type_name") or "(none)")
            p = safe_float_opt(best.get("success_probability"))
            attempts_per_success = (1.0 / float(p)) if (p is not None and float(p) > 0) else None

            _PATH_SEP = "|||"
            ci_rows = (
                inv_data.get("best_ui_copy_invention_jobs_rows")
                if isinstance(inv_data.get("best_ui_copy_invention_jobs_rows"), list)
                else []
            )

            # Derive summary values from the UI table rows (computed server-side).
            # This keeps the caption/expander consistent with what the user sees in the tree.
            for r in ci_rows:
                if not isinstance(r, dict):
                    continue
                path = str(r.get("path") or "")
                top = path.split(_PATH_SEP)[0] if _PATH_SEP in path else path
                if top.endswith("(Copying)"):
                    copy_fee_total = safe_float_opt(r.get("Job Fee"))
                    copy_time_total = safe_float_opt(r.get("Duration"))
                elif top.endswith("(Invention)"):
                    inv_fee_total = safe_float_opt(r.get("Job Fee"))
                    inv_time_total = safe_float_opt(r.get("Duration"))
                elif top.strip().lower() == "total":
                    fee = safe_float_opt(r.get("Job Fee"))
                    eff = safe_float_opt(r.get("Effective Cost"))
                    try:
                        if fee is not None or eff is not None:
                            total_cost = float(fee or 0.0) + float(eff or 0.0)
                    except Exception:
                        total_cost = None
            if not ci_rows:
                st.caption("No Copy & Invention job breakdown available.")

            if ci_rows:
                ci_df = pd.DataFrame(ci_rows)
            else:
                ci_df = pd.DataFrame(
                    columns=[
                        "Icon",
                        "Action",
                        "Job Runs",
                        "Job Fee",
                        "Qty",
                        "Effective Cost",
                        "Duration",
                        "Effective / unit",
                        "Inventory Cost",
                        "Buy Cost",
                        "path",
                    ]
                )
            # Material names are already encoded into the tree path labels; don't show a separate
            # (and redundant) Material column.
            ci_df = ci_df.drop(columns=["Material"], errors="ignore")
            preferred_cols = [
                "Icon",
                "Action",
                "Job Runs",
                "Job Fee",
                "Qty",
                "Effective Cost",
                "Duration",
                "Effective / unit",
                "Inventory Cost",
                "Buy Cost",
                "path",
            ]
            ci_df = ci_df[[c for c in preferred_cols if c in ci_df.columns] + [c for c in ci_df.columns if c not in preferred_cols]]

            gb_ci = GridOptionsBuilder.from_dataframe(ci_df)
            gb_ci.configure_default_column(editable=False, sortable=False, filter=False, resizable=True)

            # Hide internal tree path
            if "path" in ci_df.columns:
                gb_ci.configure_column("path", hide=True)

            # Icon column (pinned left, after the group column)
            if "Icon" in ci_df.columns:
                gb_ci.configure_column(
                    "Icon",
                    header_name="",
                    pinned="left",
                    width=54,
                    minWidth=54,
                    maxWidth=54,
                    cellRenderer=img_renderer,
                    suppressSizeToFit=True,
                )

            # Action column next to Icon
            if "Action" in ci_df.columns:
                gb_ci.configure_column("Action", minWidth=110)

            right = {"textAlign": "right"}
            if "Job Runs" in ci_df.columns:
                gb_ci.configure_column(
                    "Job Runs",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    minWidth=110,
                    cellStyle=right,
                )
            if "Qty" in ci_df.columns:
                gb_ci.configure_column(
                    "Qty",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    minWidth=110,
                    cellStyle=right,
                )

            for c in ["Job Fee", "Effective Cost", "Effective / unit", "Inventory Cost", "Buy Cost"]:
                if c in ci_df.columns:
                    gb_ci.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(
                            JsCode=JsCode,
                            locale=eu_locale,
                            decimals=(0 if c != "Effective / unit" else 2),
                        ),
                        minWidth=150,
                        cellStyle=right,
                    )

            grid_opts_ci = gb_ci.build()
            grid_opts_ci["autoSizeStrategy"] = {"type": "fitCellContents"}
            grid_opts_ci["treeData"] = True
            grid_opts_ci["getDataPath"] = JsCode(
                """
                function(data) {
                    try {
                        if (!data || data.path === null || data.path === undefined) return [];
                        var s = String(data.path);
                        if (!s) return [];
                        return s.split('|||').filter(function(x) { return x !== null && x !== undefined && String(x).length > 0; });
                    } catch (e) {
                        return [];
                    }
                }
                """
            )
            grid_opts_ci["groupDefaultExpanded"] = 1
            grid_opts_ci["autoGroupColumnDef"] = {
                "headerName": "Copy & Invention Jobs",
                "pinned": "left",
                "minWidth": 320,
                "cellRendererParams": {"suppressCount": True},
            }

            height_ci = min(520, 70 + (len(ci_df) * 32))
            AgGrid(
                ci_df,
                gridOptions=grid_opts_ci,
                allow_unsafe_jscode=True,
                enable_enterprise_modules=True,
                theme="streamlit",
                height=height_ci,
            )
            if units_total > 0 and total_cost is not None:
                st.caption(f"Output: {int(units_total)} units · Copy+Invention expected cost: {float(total_cost):,.0f} ISK")

            if copy_fee_total is None or copy_time_total is None:
                st.caption(
                    "Copying job fee/duration is best-effort and depends on SDE copy time + selected Industry Profile (system + taxes/rigs)."
                )

            # Decryptor details (kept in a separate expander; materials are inline in the table above).
            with st.expander("Decryptors", expanded=False):
                best_dec_unit_cost = safe_float_opt(best.get("decryptor_effective_cost_isk"))
                best_dec_cost = None
                try:
                    if best_dec_unit_cost is not None and attempts_per_success is not None:
                        best_dec_cost = float(best_dec_unit_cost) * float(attempts_per_success)
                except Exception:
                    best_dec_cost = None
                best_inv_fee_total = float(inv_fee_total) if inv_fee_total is not None else None
                best_inv_time_total = float(inv_time_total) if inv_time_total is not None else None

                st.write(
                    {
                        "Best decryptor": decryptor,
                        "Success %": (float(p) * 100.0) if p is not None else None,
                        "Expected Attempts": float(attempts_per_success) if attempts_per_success is not None else None,
                        "Decryptor Cost": float(best_dec_cost) if best_dec_cost is not None else None,
                        "Job Fee": float(best_inv_fee_total) if best_inv_fee_total is not None else None,
                        "Duration": format_duration(best_inv_time_total) if best_inv_time_total is not None else "-",
                    }
                )

                with st.expander("Other decryptor options", expanded=False):
                    shown = 0
                    for rank, opt in enumerate((inv_opts or [])[:25]):
                        if not isinstance(opt, dict):
                            continue
                        dec_name = str(opt.get("decryptor_type_name") or "(none)")
                        p_opt = safe_float_opt(opt.get("success_probability"))
                        attempts = (1.0 / float(p_opt)) if (p_opt is not None and p_opt > 0) else None

                        att_mat = safe_float_opt(opt.get("invention_attempt_material_cost_isk"))
                        att_fee = safe_float_opt(opt.get("invention_job_fee_isk"))
                        expected_inv_fee = None
                        expected_inv_time = None
                        expected_cost = None
                        try:
                            if attempts is not None:
                                expected_cost = float(att_mat or 0.0) * float(attempts) + float(att_fee or 0.0) * float(attempts)
                                expected_inv_fee = float(att_fee or 0.0) * float(attempts)
                        except Exception:
                            expected_cost = None
                            expected_inv_fee = None

                        try:
                            if attempts is not None and inv_time_attempt is not None:
                                expected_inv_time = float(inv_time_attempt) * float(attempts)
                        except Exception:
                            expected_inv_time = None

                        dec_cost = safe_float_opt(opt.get("decryptor_effective_cost_isk"))
                        dec_cost_total = None
                        try:
                            if dec_cost is not None and attempts is not None:
                                dec_cost_total = float(dec_cost) * float(attempts)
                        except Exception:
                            dec_cost_total = None
                        copy_fee_opt = safe_float_opt(opt.get("copying_job_fee_isk"))
                        copy_time_opt = safe_float_opt(opt.get("copying_expected_time_seconds"))

                        with st.expander(f"#{int(rank + 1)} {dec_name}", expanded=False):
                            st.write(
                                {
                                    "Success %": (float(p_opt) * 100.0) if p_opt is not None else None,
                                    "Expected Attempts": float(attempts) if attempts is not None else None,
                                    "Invented Runs": safe_int(opt.get("invented_runs"), default=0),
                                    "ME": safe_int(opt.get("invented_me"), default=0),
                                    "TE": safe_int(opt.get("invented_te"), default=0),
                                    "Decryptor Cost": float(dec_cost_total) if dec_cost_total is not None else None,
                                    "Expected Invention Cost": float(expected_cost) if expected_cost is not None else None,
                                    "Job Fee": float(expected_inv_fee) if expected_inv_fee is not None else None,
                                    "Duration": format_duration(expected_inv_time) if expected_inv_time is not None else "-",
                                    "Copying Job Fee": float(copy_fee_opt) if copy_fee_opt is not None else None,
                                    "Copying Duration": format_duration(copy_time_opt) if copy_time_opt is not None else "-",
                                }
                            )

                        shown += 1
                        if shown >= 10:
                            st.caption("Showing top 10 options.")
                            break

        # --- Build tree / submanufacturing plan ---

        backend_tree_rows: list[dict[str, Any]] = []
        backend_copy_jobs: list[dict[str, Any]] = []
        backend_missing_bps: list[dict[str, Any]] = []

        # Planner is computed server-side as part of industry_builder_data.
        plan_rows = full_bp_data.get("submanufacturing_plan") or [] if isinstance(full_bp_data, dict) else []

        # For invention-derived jobs, use the backend-provided best manufacturing subplan.
        if candidate_kind == "invention":
            plan_children = (
                inv_data.get("best_manufacturing_submanufacturing_plan")
                if isinstance(inv_data.get("best_manufacturing_submanufacturing_plan"), list)
                else []
            )
            plan_rows = (
                [
                    {
                        "type_id": 0,
                        "type_name": "Manufacturing Job",
                        "recommendation": "build",
                        "required_quantity": safe_int(selected_row.get("units_total"), default=0),
                        "children": plan_children,
                    }
                ]
                if plan_children
                else []
            )

            backend_tree_rows = (
                inv_data.get("best_ui_build_tree_rows")
                if isinstance(inv_data.get("best_ui_build_tree_rows"), list)
                else []
            )
            backend_copy_jobs = (
                inv_data.get("best_ui_blueprint_copy_jobs")
                if isinstance(inv_data.get("best_ui_blueprint_copy_jobs"), list)
                else []
            )
            backend_missing_bps = (
                inv_data.get("best_ui_missing_blueprints")
                if isinstance(inv_data.get("best_ui_missing_blueprints"), list)
                else []
            )
        elif isinstance(full_bp_data, dict):
            tree_map = full_bp_data.get("ui_build_tree_rows_by_product_type_id")
            if isinstance(tree_map, dict):
                rows = tree_map.get(str(int(selected_product_id)))
                if rows is None:
                    rows = tree_map.get(int(selected_product_id))
                backend_tree_rows = rows if isinstance(rows, list) else []

            backend_copy_jobs = full_bp_data.get("ui_blueprint_copy_jobs") if isinstance(full_bp_data.get("ui_blueprint_copy_jobs"), list) else []
            backend_missing_bps = full_bp_data.get("ui_missing_blueprints") if isinstance(full_bp_data.get("ui_missing_blueprints"), list) else []

        # Normalize Build Tree shape across T1/T2 selections:
        # always render a single top-level "Manufacturing Job" node.
        def _wrap_under_mfg_root(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            if (
                isinstance(rows, list)
                and len(rows) == 1
                and isinstance(rows[0], dict)
                and safe_int(rows[0].get("type_id"), default=0) == 0
                and str(rows[0].get("type_name") or "").lower().startswith("manufact")
            ):
                return rows
            return [
                {
                    "type_id": 0,
                    "type_name": "Manufacturing Job",
                    "recommendation": "build",
                    "required_quantity": safe_int(selected_row.get("units_total"), default=0),
                    "children": rows,
                }
            ]

        # If no planner rows are available (common for T1-only workflows),
        # synthesize a minimal tree from the required materials so the Build Tree
        # still uses the same UI (expand Manufacturing Job to see materials).
        if not plan_rows and isinstance(materials, list) and materials:
            material_nodes: list[dict[str, Any]] = []
            for m in materials:
                if not isinstance(m, dict):
                    continue
                tid_i = safe_int(m.get("type_id"), default=0)
                if tid_i <= 0:
                    continue
                qty = m.get("quantity_after_efficiency")
                if qty is None:
                    qty = m.get("quantity_me0")
                qty_i = safe_int(qty, default=0)
                if qty_i <= 0:
                    continue
                material_nodes.append(
                    {
                        "type_id": int(tid_i),
                        "type_name": (m.get("type_name") or str(tid_i)),
                        "recommendation": "buy",
                        "required_quantity": int(qty_i),
                        "children": [],
                    }
                )
            if material_nodes:
                plan_rows = _wrap_under_mfg_root(material_nodes)

        if plan_rows:
            plan_rows = _wrap_under_mfg_root([r for r in plan_rows if isinstance(r, dict)])

        # --- Blueprint Copy Jobs overview (main + submanufacturing build steps) ---
        copy_job_rows: list[dict[str, Any]] = []

        # Prefer backend-provided copy jobs extraction.
        if isinstance(backend_copy_jobs, list) and backend_copy_jobs:
            for r in backend_copy_jobs:
                if not isinstance(r, dict):
                    continue
                dur_s = safe_float_opt(r.get("Duration"))
                copy_job_rows.append(
                    {
                        "Blueprint": r.get("Blueprint"),
                        "Runs": safe_int(r.get("Runs"), default=0),
                        "Max Runs": safe_int_opt(r.get("Max Runs")),
                        "Duration": (format_duration(dur_s) if dur_s is not None else "-"),
                        "Job Fee": safe_float_opt(r.get("Job Fee")),
                    }
                )

        # Root manufacturing blueprint copy job (when manufacturing is based on a BPC)
        if not copy_job_rows and candidate_kind != "invention" and isinstance(full_bp_data, dict):
            mj = full_bp_data.get("manufacture_job") if isinstance(full_bp_data.get("manufacture_job"), dict) else None
            props = mj.get("properties") if isinstance(mj, dict) and isinstance(mj.get("properties"), dict) else None
            root_copy = props.get("copy_job") if isinstance(props, dict) and isinstance(props.get("copy_job"), dict) else None
            if isinstance(root_copy, dict):
                time_d = root_copy.get("time") if isinstance(root_copy.get("time"), dict) else {}
                cost_d = root_copy.get("job_cost") if isinstance(root_copy.get("job_cost"), dict) else {}
                copy_job_rows.append(
                    {
                        "Blueprint": str(bp_name),
                        "Runs": safe_int(root_copy.get("runs"), default=0),
                        "Max Runs": safe_int_opt(root_copy.get("max_runs")),
                        "Duration": (
                            format_duration(safe_float_opt(time_d.get("estimated_copy_time_seconds")))
                            if safe_float_opt(time_d.get("estimated_copy_time_seconds")) is not None
                            else "-"
                        ),
                        "Job Fee": safe_float_opt(cost_d.get("total_job_cost_isk")),
                    }
                )

        # Submanufacturing copy overhead (only for nodes where recommendation == 'build')
        def _walk_copy_overhead(nodes: list[dict[str, Any]]) -> None:
            for n in nodes or []:
                if not isinstance(n, dict):
                    continue
                rec = str(n.get("recommendation") or "").lower()
                if rec == "build":
                    build = n.get("build") if isinstance(n.get("build"), dict) else None
                    if isinstance(build, dict):
                        co = build.get("copy_overhead") if isinstance(build.get("copy_overhead"), dict) else None
                        if isinstance(co, dict):
                            copy_job_rows.append(
                                {
                                    "Blueprint": str(build.get("blueprint_type_name") or build.get("blueprint_type_id") or ""),
                                    "Runs": safe_int(build.get("runs_needed"), default=0),
                                    "Max Runs": safe_int_opt(co.get("max_production_limit")),
                                    "Duration": (
                                        format_duration(safe_float_opt(co.get("estimated_copy_time_seconds")))
                                        if safe_float_opt(co.get("estimated_copy_time_seconds")) is not None
                                        else "-"
                                    ),
                                    "Job Fee": safe_float_opt(co.get("estimated_copy_fee_isk")),
                                }
                            )

                children = n.get("children")
                if isinstance(children, list) and children:
                    _walk_copy_overhead([c for c in children if isinstance(c, dict)])

        if not copy_job_rows and isinstance(plan_rows, list) and plan_rows:
            _walk_copy_overhead([r for r in plan_rows if isinstance(r, dict)])

        if copy_job_rows:
            st.markdown("#### Blueprint Copy Jobs")
            df_copy = pd.DataFrame(copy_job_rows)
            if "Output BPC" not in df_copy.columns:
                try:
                    df_copy.insert(1, "Output BPC", df_copy["Blueprint"].apply(lambda s: f"{s} (BPC)" if s else "(BPC)"))
                except Exception:
                    pass
            gb_copy = GridOptionsBuilder.from_dataframe(df_copy)
            gb_copy.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)
            right_style = {"textAlign": "right"}

            for c in ["Runs", "Max Runs"]:
                if c in df_copy.columns:
                    gb_copy.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=110,
                        cellStyle=right_style,
                    )

            if "Job Fee" in df_copy.columns:
                gb_copy.configure_column(
                    "Job Fee",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    minWidth=160,
                    cellStyle=right_style,
                )

            grid_opts_copy = gb_copy.build()
            attach_aggrid_autosize(grid_opts_copy, JsCode=JsCode)
            grid_opts_copy["autoSizeStrategy"] = {"type": "fitCellContents"}

            height_copy = min(320, 60 + (len(df_copy) * 32))
            AgGrid(
                df_copy,
                gridOptions=grid_opts_copy,
                allow_unsafe_jscode=True,
                theme="streamlit",
                height=height_copy,
            )
        else:
            st.markdown("#### Blueprint Copy Jobs")
            st.info("No blueprint copy jobs found for this build.")

        st.markdown("#### Build Tree")
        plan_by_type_id: dict[int, dict] = {}
        def _walk_plan(nodes: list[dict[str, Any]] | None) -> None:
            for n in nodes or []:
                if not isinstance(n, dict):
                    continue
                tid_i = safe_int(n.get("type_id"), default=0)
                if tid_i > 0:
                    plan_by_type_id[int(tid_i)] = n
                children = n.get("children")
                if isinstance(children, list) and children:
                    _walk_plan([c for c in children if isinstance(c, dict)])

        if isinstance(plan_rows, list) and plan_rows:
            _walk_plan([r for r in plan_rows if isinstance(r, dict)])

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
                        bp_type_id = safe_int(build.get("blueprint_type_id"), default=0)
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
                                    "Icon": blueprint_image_url(int(bp_type_id), is_bpc=False, size=32),
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
            for _bp_type_id, slot in by_bp.items():
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

        missing_bp_rows: list[dict[str, Any]] = []
        if isinstance(backend_missing_bps, list) and backend_missing_bps:
            for r in backend_missing_bps:
                if not isinstance(r, dict):
                    continue
                bp_tid = safe_int(r.get("blueprint_type_id"), default=0)
                missing_bp_rows.append(
                    {
                        "Icon": blueprint_image_url(int(bp_tid), is_bpc=False, size=32) if bp_tid > 0 else None,
                        "Blueprint": r.get("Blueprint"),
                        "Assumed ME": safe_float_opt(r.get("Assumed ME")),
                        "Assumed TE": safe_float_opt(r.get("Assumed TE")),
                        "Assumption Source": r.get("Assumption Source"),
                        "Est. BPO Buy Cost": safe_float_opt(r.get("Est. BPO Buy Cost")),
                        "Used For": r.get("Used For"),
                    }
                )
        elif plan_rows:
            missing_bp_rows = _collect_unowned_blueprints(plan_rows)

        if missing_bp_rows:
            st.markdown("#### Required Blueprints (Not Owned)")
            st.caption(
                "These submanufacturing steps recommend **build**, but you don't own the blueprint. "
                "ME/TE shown are the planner assumptions for unowned blueprints (best-effort)."
            )
            df_missing_bp = pd.DataFrame(missing_bp_rows)
            gb_missing = GridOptionsBuilder.from_dataframe(df_missing_bp)
            gb_missing.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)
            right_style = {"textAlign": "right"}

            if "Icon" in df_missing_bp.columns:
                gb_missing.configure_column(
                    "Icon",
                    header_name="",
                    width=62,
                    pinned="left",
                    sortable=False,
                    filter=False,
                    suppressAutoSize=True,
                    cellRenderer=img_renderer,
                )

            for c in ["Assumed ME", "Assumed TE"]:
                if c in df_missing_bp.columns:
                    gb_missing.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_pct_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=110,
                        cellStyle=right_style,
                    )

            if "Est. BPO Buy Cost" in df_missing_bp.columns:
                gb_missing.configure_column(
                    "Est. BPO Buy Cost",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    minWidth=140,
                    cellStyle=right_style,
                )

            grid_opts_missing = gb_missing.build()
            height_missing = min(420, 40 + (len(df_missing_bp) * 35))
            AgGrid(
                df_missing_bp,
                gridOptions=grid_opts_missing,
                allow_unsafe_jscode=True,
                theme="streamlit",
                height=height_missing,
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
            build_full = plan.get("build_full") if isinstance(plan, dict) and isinstance(plan.get("build_full"), dict) else None
            build_cost = (build or {}).get("total_build_cost_isk") if isinstance(build, dict) else None
            build_cost_full = (build_full or {}).get("total_build_cost_isk") if isinstance(build_full, dict) else None
            build_cost_for_row = build_cost if build_cost is not None else build_cost_full
            effective_cost = plan.get("effective_cost_isk") if isinstance(plan, dict) else None

            market_unit = plan.get("buy_unit_price_isk") if isinstance(plan, dict) else None
            market_buy_cost = None
            try:
                if market_unit is not None and qty_i is not None:
                    market_buy_cost = float(market_unit) * float(qty_i)
            except Exception:
                market_buy_cost = None

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
                    "Icon": type_icon_url(tid_i, size=32),
                    "Material": m.get("type_name"),
                    "Qty": int(qty_i),
                    "Unit Price": float(unit_price) if unit_price is not None else None,
                    "Effective Cost": float(total_cost) if total_cost is not None else None,
                    "Market Buy Cost": float(market_buy_cost) if market_buy_cost is not None else None,
                    "FIFO (Market)": (
                        safe_float_opt(m.get("inventory_fifo_market_buy_cost_isk"))
                    ),
                    "FIFO (Built)": (
                        safe_float_opt(m.get("inventory_fifo_industry_build_cost_isk"))
                    ),
                    "Inv Used": (
                        safe_int_opt(m.get("inventory_used_qty"))
                    ),
                    "Shortfall Qty": (
                        safe_int_opt(m.get("inventory_buy_now_qty"))
                    ),
                    "Action": rec,
                    "Build Cost": float(build_cost_for_row) if build_cost_for_row is not None else None,
                    "Build ROI": float(roi_build) if roi_build is not None else None,
                }
            )

        mat_df = pd.DataFrame(mat_rows)
        if mat_rows:
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

            # Prefer backend-provided flattened Build Tree rows when available.
            use_backend_tree = isinstance(backend_tree_rows, list) and len(backend_tree_rows) > 0

            # If planner data (or backend viewmodel rows) are available, render everything as a single aligned tree table.
            if plan_rows or use_backend_tree:
                # AgGrid tree view (collapsible).
                # Only include children when the planner recommends building (rec == 'build'),
                # matching the current manual renderer behavior.
                def _node_name(n: dict) -> str:
                    return str(n.get("type_name") or n.get("type_id") or "")

                def _has_expandable_build_steps(nodes: list[dict]) -> bool:
                    # Only enable TreeData when there are *build* steps that actually
                    # have required materials to show (non-empty children).
                    def _walk(x: dict) -> bool:
                        if not isinstance(x, dict):
                            return False
                        rec = str(x.get("recommendation") or "").lower()
                        children = x.get("children")
                        if rec == "build" and isinstance(children, list) and len(children) > 0:
                            return True
                        if isinstance(children, list):
                            for ch in children:
                                if isinstance(ch, dict) and _walk(ch):
                                    return True
                        return False

                    if not isinstance(nodes, list):
                        return False
                    for root in nodes:
                        if isinstance(root, dict) and _walk(root):
                            return True
                    return False

                def _node_key(n: dict) -> str:
                    # Key used for the tree path; keep it stable and unique-ish.
                    try:
                        tid = int(n.get("type_id") or 0)
                    except Exception:
                        tid = 0
                    nm = _node_name(n)
                    return f"{nm}#{tid}" if tid else nm

                # When backend rows exist, they already include rollups and allocation scaling.
                tree_rows: list[dict] = [r for r in backend_tree_rows if isinstance(r, dict)] if use_backend_tree else []
                # Use a plain delimiter (not a control char) to avoid any serialization/
                # rendering oddities in Streamlit-AgGrid/React.
                _PATH_SEP = "|||"

                def _walk_tree(n: dict, *, parent_path: list[str]) -> None:
                    if not isinstance(n, dict):
                        return

                    rec = str(n.get("recommendation") or "-").lower()
                    key = _node_key(n)
                    path = [*parent_path, key]
                    path_str = _PATH_SEP.join([str(p) for p in path if p is not None])

                    # The tree (auto-group) column already shows the hierarchy (Invention/Manufacturing -> ...),
                    # so we don't need a separate "Material" column.

                    # Planner cost fields:
                    # - buy_unit_price_isk: market unit price (avg)
                    # - buy_cost_isk: effective buy cost (FIFO inventory valuation + market for any shortfall)
                    # - effective_cost_isk: planner-chosen total cost for this node.
                    #   When FIFO preference is enabled and inventory is partially available, this becomes:
                    #   inventory(FIFO) + min(buy shortfall, build shortfall).
                    effective_cost = n.get("effective_cost_isk")
                    market_unit = n.get("buy_unit_price_isk")

                    qty_required = n.get("required_quantity")
                    qty_shortfall = n.get("shortfall_quantity")
                    inv_used_qty = n.get("inventory_used_qty")

                    # Display qty as the planner-required quantity. Shortfall and inventory-used
                    # quantities are shown via dedicated (hidden-by-default) columns.
                    qty = qty_required

                    market_buy_cost = None
                    try:
                        if market_unit is not None and qty is not None:
                            market_buy_cost = float(market_unit) * float(qty)
                    except Exception:
                        market_buy_cost = None

                    inv_used_qty = n.get("inventory_used_qty")
                    inv_fifo_priced_qty = n.get("inventory_fifo_priced_qty")
                    inv_fifo_cost = n.get("inventory_fifo_cost_isk")
                    build = n.get("build") if isinstance(n.get("build"), dict) else None
                    build_full = n.get("build_full") if isinstance(n.get("build_full"), dict) else None
                    build_for_cost = build if (rec == "build" and isinstance(build, dict)) else build_full

                    build_cost = build_for_cost.get("total_build_cost_isk") if isinstance(build_for_cost, dict) else None

                    shortfall_qty = n.get("shortfall_quantity")
                    shortfall_action = n.get("shortfall_recommendation")
                    shortfall_buy_cost = n.get("shortfall_buy_cost_isk")
                    shortfall_build_cost = n.get("shortfall_build_cost_isk")
                    savings_isk = n.get("savings_isk")

                    inv_used_qty_i = safe_int_opt(inv_used_qty)
                    inv_fifo_priced_qty_i = safe_int_opt(inv_fifo_priced_qty)

                    qty_required_i = safe_int_opt(qty_required)
                    shortfall_qty_i = safe_int_opt(shortfall_qty)
                    try:
                        if shortfall_qty_i is None and qty_required_i is not None and inv_used_qty_i is not None:
                            shortfall_qty_i = max(0, int(qty_required_i) - int(inv_used_qty_i))
                    except Exception:
                        shortfall_qty_i = shortfall_qty_i

                    # Inventory cost display value:
                    # - Prefer FIFO valuation when available.
                    # - Fallback to market valuation for the inventory-used quantity when FIFO isn't available.
                    inventory_cost_display = None
                    try:
                        if inv_used_qty_i is not None and int(inv_used_qty_i) > 0:
                            if inv_fifo_cost is not None and inv_fifo_priced_qty_i is not None and int(inv_fifo_priced_qty_i) > 0:
                                # FIFO priced part + market fallback for any unknown-basis inventory.
                                inventory_cost_display = float(inv_fifo_cost)
                                try:
                                    unknown_qty_i = max(0, int(inv_used_qty_i) - int(inv_fifo_priced_qty_i))
                                except Exception:
                                    unknown_qty_i = 0
                                if unknown_qty_i > 0 and market_unit is not None:
                                    inventory_cost_display += float(market_unit) * float(unknown_qty_i)
                            elif market_unit is not None:
                                inventory_cost_display = float(market_unit) * float(inv_used_qty_i)
                    except Exception:
                        inventory_cost_display = None

                    # Keep Build Tree display aligned with the rest of the UI:
                    # - Action is the planner recommendation (no extra inference like take/buy).
                    # - Costs are shown as the straightforward totals (no shortfall substitution, no blanking).
                    action_display = rec
                    effective_cost_display = effective_cost
                    buy_cost_display = market_buy_cost
                    build_cost_display = build_cost

                    effective_unit = None
                    try:
                        if effective_cost_display is not None and qty_required is not None and float(qty_required) > 0:
                            effective_unit = float(effective_cost_display) / float(qty_required)
                    except Exception:
                        effective_unit = None

                    roi_display = None
                    if market_buy_cost is not None and build_cost is not None:
                        try:
                            bc = float(build_cost)
                            roi_if_build = ((float(market_buy_cost) - float(build_cost)) / bc) * 100.0 if bc > 0 else None
                        except Exception:
                            roi_if_build = None
                        roi_display = roi_if_build

                    try:
                        type_id_i = int(n.get("type_id") or 0)
                    except Exception:
                        type_id_i = 0

                    icon_override = n.get("icon_url") if isinstance(n, dict) else None
                    icon_url = str(icon_override) if icon_override else (type_icon_url(type_id_i, size=32) if type_id_i > 0 else None)

                    tree_rows.append(
                        {
                            "path": path_str,
                            "type_id": type_id_i,
                            "Icon": icon_url,
                            "action": (str(action_display) if action_display is not None else rec),
                            "reason": (str(n.get("reason")) if n.get("reason") is not None else None),
                            "qty": safe_int_opt(qty),
                            "qty_required": safe_int_opt(qty_required),
                            # Displayed unit cost: planner-chosen effective cost per required unit.
                            "unit": safe_float_opt(effective_unit),
                            # Keep the market unit price available for JS comparisons and column menu.
                            "market_unit": safe_float_opt(market_unit),
                            # Shortfall decision metadata (usually only populated when prefer-inventory is enabled).
                            "shortfall_qty": safe_int_opt(shortfall_qty),
                            "shortfall_action": (str(shortfall_action) if shortfall_action is not None else None),
                            "shortfall_buy_cost": safe_float_opt(shortfall_buy_cost),
                            "shortfall_build_cost": safe_float_opt(shortfall_build_cost),
                            "savings_isk": safe_float_opt(savings_isk),
                            "inventory_used_qty": safe_int_opt(inv_used_qty),
                            "inventory_fifo_priced_qty": safe_int_opt(inv_fifo_priced_qty),
                            # Inventory cost (best-effort): FIFO when possible, else fallback to market for the inventory-used qty.
                            "inventory_cost": safe_float_opt(inventory_cost_display) if (inv_used_qty_i is not None and int(inv_used_qty_i) > 0) else None,
                            # Effective cost as provided by the planner (or backend rows).
                            "effective_cost": safe_float_opt(effective_cost_display),
                            "buy_cost": safe_float_opt(buy_cost_display),
                            "build_cost": safe_float_opt(build_cost_display),
                            "roi": float(roi_display) if roi_display is not None else None,
                        }
                    )

                    children = n.get("children") or []
                    # Always include the submanufacturing tree, regardless of Action.
                    # (Tree starts collapsed; user can expand nodes as needed.)
                    if isinstance(children, list) and len(children) > 0:
                        for ch in children:
                            if isinstance(ch, dict):
                                _walk_tree(ch, parent_path=path)

                if not use_backend_tree:
                    for root in plan_rows:
                        if isinstance(root, dict):
                            _walk_tree(root, parent_path=[])

                tree_df = pd.DataFrame(tree_rows)

                # Summarize totals on the top-level root row (e.g. "Manufacturing" / "Manufacturing Job").
                # Important: sum only *immediate children* to avoid double-counting,
                # because parent nodes already represent the total for their subtree.
                try:
                    if not tree_df.empty and "path" in tree_df.columns:
                        path_s = tree_df["path"].astype(str)
                        # NOTE: pandas .str.count treats the pattern as a regex; our separator is "|||",
                        # so use a literal split to compute depth.
                        depth_s = path_s.str.split(_PATH_SEP, regex=False).str.len()

                        root_paths = sorted(set(path_s.loc[depth_s == 1].tolist()))
                        for root_path in root_paths:
                            root_mask = path_s == root_path
                            if not bool(root_mask.any()):
                                continue

                            child_mask = path_s.str.startswith(root_path + _PATH_SEP) & (depth_s == 2)

                            def _parse_num(v: Any) -> float | None:
                                try:
                                    if v is None:
                                        return None
                                    if isinstance(v, (int, float)):
                                        return float(v)
                                    s = str(v).strip()
                                    if not s or s == "-":
                                        return None
                                    s = s.replace("\u202f", "").replace(" ", "")
                                    s = s.replace("ISK", "").replace("isk", "")
                                    s = s.replace("%", "")
                                    # Handle EU formatting:
                                    # - "1.234,56" => 1234.56
                                    # - "40,80" => 40.80
                                    if "," in s and "." in s:
                                        s = s.replace(".", "")
                                        s = s.replace(",", ".")
                                    elif "," in s and "." not in s:
                                        s = s.replace(",", ".")
                                    else:
                                        # No comma present.
                                        # Many values come in as EU thousands formatting, e.g. "114.095.973" or "25.225.200".
                                        # If there are multiple dots, they are almost certainly thousands separators.
                                        if s.count(".") > 1:
                                            s = s.replace(".", "")
                                        elif s.count(".") == 1:
                                            # Ambiguous single dot: treat as thousands separator when it looks like grouping (###.###).
                                            left, right = s.split(".", 1)
                                            if left.isdigit() and right.isdigit() and len(right) == 3:
                                                s = left + right

                                    # Keep only digits, dot, minus.
                                    cleaned = "".join(ch for ch in s if (ch.isdigit() or ch in {".", "-"}))
                                    if cleaned in {"", "-", "."}:
                                        return None
                                    return float(cleaned)
                                except Exception:
                                    return None

                            def _sum_numeric(col: str) -> float | None:
                                if col not in tree_df.columns:
                                    return None
                                raw = tree_df.loc[child_mask, col]
                                vals = [x for x in (_parse_num(v) for v in raw.tolist()) if x is not None]
                                if not vals:
                                    return None
                                return float(sum(vals))

                            eff_total = _sum_numeric("effective_cost")
                            inv_total = _sum_numeric("inventory_cost")
                            buy_total = _sum_numeric("buy_cost")

                            if eff_total is not None and "effective_cost" in tree_df.columns:
                                tree_df.loc[root_mask, "effective_cost"] = float(eff_total)
                            if inv_total is not None and "inventory_cost" in tree_df.columns:
                                tree_df.loc[root_mask, "inventory_cost"] = float(inv_total)
                            if buy_total is not None and "buy_cost" in tree_df.columns:
                                tree_df.loc[root_mask, "buy_cost"] = float(buy_total)

                            # Effective / Unit for the root.
                            if eff_total is not None and "unit" in tree_df.columns:
                                qty_base_col = "qty_required" if "qty_required" in tree_df.columns else "qty"
                                try:
                                    qty_raw = tree_df.loc[root_mask, qty_base_col].iloc[0]
                                    qty_n = _parse_num(qty_raw)
                                except Exception:
                                    qty_n = None
                                if qty_n is not None and qty_n > 0:
                                    tree_df.loc[root_mask, "unit"] = float(eff_total) / float(qty_n)
                except Exception:
                    pass

                # Enable TreeData only when we actually emitted child rows.
                # (We consider it hierarchical when any path contains the separator.)
                use_tree = any((_PATH_SEP in str(r.get("path") or "")) for r in tree_rows)

                # Keep a predictable column order.
                preferred_cols = [
                    "Icon",
                    "action",
                    "qty",
                    "unit",
                    "effective_cost",
                    "inventory_cost",
                    "buy_cost",
                    "build_cost",
                    "roi",
                    "shortfall_qty",
                    "shortfall_action",
                    "shortfall_buy_cost",
                    "shortfall_build_cost",
                    "savings_isk",
                    "path",
                    "type_id",
                ]
                tree_df = tree_df[[c for c in preferred_cols if c in tree_df.columns] + [c for c in tree_df.columns if c not in preferred_cols]]

                # Hide columns that are never populated (common for T1 blueprint selections).
                # This keeps the Build Tree table compact and avoids empty "Build Cost" / "ROI" columns.
                def _drop_if_all_empty(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
                    drop_cols: list[str] = []
                    for col in cols:
                        if col not in df.columns:
                            continue
                        s = pd.to_numeric(df[col], errors="coerce")
                        if s.isna().all() or ((s.isna()) | (s == 0)).all():
                            drop_cols.append(col)
                    return df.drop(columns=drop_cols) if drop_cols else df

                tree_df = _drop_if_all_empty(tree_df, ["build_cost", "roi"])

                gb_tree = GridOptionsBuilder.from_dataframe(tree_df)
                gb_tree.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)

                # Hide internal columns
                for c in [
                    "path",
                    "type_id",
                    "reason",
                    "inventory_used_qty",
                    "inventory_fifo_priced_qty",
                    "market_unit",
                    "qty_required",
                    # Keep these available via column menu, but hidden by default.
                    "shortfall_qty",
                    "shortfall_action",
                    "shortfall_buy_cost",
                    "shortfall_build_cost",
                    "savings_isk",
                ]:
                    if c in tree_df.columns:
                        gb_tree.configure_column(c, hide=True)

                # Decryptor influence columns: keep visible (only populated for invention rows).

                if "Icon" in tree_df.columns:
                    gb_tree.configure_column(
                        "Icon",
                        header_name="",
                        width=56,
                        pinned="left",
                        sortable=False,
                        filter=False,
                        suppressAutoSize=True,
                        cellRenderer=img_renderer,
                    )

                right_style = {"textAlign": "right"}
                if "action" in tree_df.columns:
                    gb_tree.configure_column("action", header_name="Action", minWidth=110)
                if "qty" in tree_df.columns:
                    gb_tree.configure_column(
                        "qty",
                        header_name="Qty",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=100,
                        cellStyle=right_style,
                    )

                # Invention/decryptor columns intentionally removed from the Build Tree table.

                if "unit" in tree_df.columns:
                    gb_tree.configure_column(
                        "unit",
                        header_name="Effective / Unit",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                        minWidth=140,
                        cellStyle=right_style,
                    )

                if "inventory_cost" in tree_df.columns:
                    gb_tree.configure_column(
                        "inventory_cost",
                        header_name="Inventory Cost",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=180,
                        cellStyle=right_style,
                    )

                if "effective_cost" in tree_df.columns:
                    gb_tree.configure_column(
                        "effective_cost",
                        header_name="Effective Cost",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=200,
                        cellStyle=right_style,
                    )
                if "buy_cost" in tree_df.columns:
                    gb_tree.configure_column(
                        "buy_cost",
                        header_name="Buy Cost",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=160,
                        cellStyle=right_style,
                    )
                if "build_cost" in tree_df.columns:
                    gb_tree.configure_column(
                        "build_cost",
                        header_name="Build Cost",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=160,
                        cellStyle=right_style,
                    )
                if "roi" in tree_df.columns:
                    gb_tree.configure_column(
                        "roi",
                        header_name="ROI",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_pct_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                        minWidth=110,
                        cellStyle=right_style,
                    )

                if "shortfall_buy_cost" in tree_df.columns:
                    gb_tree.configure_column(
                        "shortfall_buy_cost",
                        header_name="Shortfall Buy Cost",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=190,
                        cellStyle=right_style,
                        hide=True,
                    )
                if "shortfall_build_cost" in tree_df.columns:
                    gb_tree.configure_column(
                        "shortfall_build_cost",
                        header_name="Shortfall Build Cost",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=200,
                        cellStyle=right_style,
                        hide=True,
                    )
                if "shortfall_qty" in tree_df.columns:
                    gb_tree.configure_column(
                        "shortfall_qty",
                        header_name="Shortfall Qty",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=150,
                        cellStyle=right_style,
                        hide=True,
                    )
                if "shortfall_action" in tree_df.columns:
                    gb_tree.configure_column(
                        "shortfall_action",
                        header_name="Shortfall Action",
                        minWidth=170,
                        hide=True,
                    )
                if "savings_isk" in tree_df.columns:
                    gb_tree.configure_column(
                        "savings_isk",
                        header_name="Savings vs Buy",
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                        minWidth=170,
                        cellStyle=right_style,
                        hide=True,
                    )

                grid_opts = gb_tree.build()
                grid_opts["autoSizeStrategy"] = {"type": "fitCellContents"}

                if use_tree:
                    grid_opts["treeData"] = True
                    grid_opts["getDataPath"] = JsCode(
                        """
                        function(data) {
                            try {
                                if (!data || data.path === null || data.path === undefined) return [];
                                var s = String(data.path);
                                if (!s) return [];
                                return s.split('|||').filter(function(x) { return x !== null && x !== undefined && String(x).length > 0; });
                            } catch (e) {
                                return [];
                            }
                        }
                        """
                    )
                    # Start collapsed so the user can expand nodes manually.
                    # Expand the top-level "Manufacturing Job" row by default,
                    # while keeping deeper levels collapsed.
                    grid_opts["groupDefaultExpanded"] = 1

                    # Configure the tree column so it shows the actual submanufacturing step/item
                    # instead of a generic "Group" column.
                    grid_opts["autoGroupColumnDef"] = {
                        "headerName": "Build Tree",
                        "pinned": "left",
                        "minWidth": 420,
                        "cellRendererParams": {
                            "suppressCount": True,
                            "innerRenderer": JsCode(
                                """
                                function(params) {
                                    try {
                                        var raw = (params && params.value !== null && params.value !== undefined) ? String(params.value) : '';
                                        var name = raw.split('#')[0];
                                        return name;
                                    } catch (e) {
                                        return (params && params.value !== null && params.value !== undefined) ? String(params.value) : '';
                                    }
                                }
                                """
                            ),
                        },
                    }

                    grid_opts["animateRows"] = True
                    grid_opts["suppressRowClickSelection"] = False
                    grid_opts["rowSelection"] = "single"
                    grid_opts["onRowClicked"] = JsCode(
                        """
                        function(event) {
                            try {
                                if (!event || !event.node) return;
                                if (event.node.childrenAfterGroup && event.node.childrenAfterGroup.length > 0) {
                                    event.node.setExpanded(!event.node.expanded);
                                }
                            } catch (e) {}
                        }
                        """
                    )

                    height = min(650, 60 + (len(tree_df) * 32))
                    AgGrid(
                        tree_df,
                        gridOptions=grid_opts,
                        allow_unsafe_jscode=True,
                        enable_enterprise_modules=True,
                        theme="streamlit",
                        height=height,
                    )

                    st.caption(
                        BUILD_TREE_CAPTION
                    )
                else:
                    # Flat table: hide tree-only internals and don't enable TreeData.
                    grid_opts.pop("treeData", None)
                    grid_opts.pop("getDataPath", None)
                    height = min(420, 60 + (len(tree_df) * 32))
                    AgGrid(
                        tree_df.drop(columns=[c for c in ["path", "type_id"] if c in tree_df.columns], errors="ignore"),
                        gridOptions=grid_opts,
                        allow_unsafe_jscode=True,
                        theme="streamlit",
                        height=height,
                    )

                    st.caption(
                        BUILD_TREE_CAPTION
                    )
            else:
                # Fallback when planner isn't available: show the simple materials table.
                gb_mat = GridOptionsBuilder.from_dataframe(mat_df)
                gb_mat.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)
                if "Icon" in mat_df.columns:
                    gb_mat.configure_column(
                        "Icon",
                        header_name="",
                        width=62,
                        pinned="left",
                        sortable=False,
                        filter=False,
                        suppressAutoSize=True,
                        cellRenderer=img_renderer,
                    )

                grid_opts_mat = gb_mat.build()
                attach_aggrid_autosize(grid_opts_mat, JsCode=JsCode)
                height_mat = min(420, 40 + (len(mat_df) * 35))
                AgGrid(
                    mat_df,
                    gridOptions=grid_opts_mat,
                    allow_unsafe_jscode=True,
                    theme="streamlit",
                    height=height_mat,
                )
        else:
            st.info("No materials required")

        if candidate_kind != "invention":
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
                    df_skills = pd.DataFrame(rows)
                    gb_skills = GridOptionsBuilder.from_dataframe(df_skills)
                    gb_skills.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)
                    right_style = {"textAlign": "right"}
                    for c in ["Required", "Character"]:
                        if c in df_skills.columns:
                            gb_skills.configure_column(
                                c,
                                type=["numericColumn", "numberColumnFilter"],
                                valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                                minWidth=110,
                                cellStyle=right_style,
                            )
                    grid_opts_skills = gb_skills.build()
                    height_skills = min(260, 40 + (len(df_skills) * 35))
                    AgGrid(
                        df_skills,
                        gridOptions=grid_opts_skills,
                        allow_unsafe_jscode=True,
                        theme="streamlit",
                        height=height_skills,
                    )
                else:
                    st.info("No required skills data available.")

    with st.expander("View Raw Blueprint Data (Debug)"):
        st.json(full_bp_data)
