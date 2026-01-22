import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]

import hashlib
import html
import json
import time
from typing import Any

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get, api_post


@st.cache_data(ttl=3600)
def _get_industry_profiles(character_id: int) -> dict | None:
    return api_get(f"/industry_profiles/{int(character_id)}")


def render():
    st.subheader("Industry Builder")

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

    # --- Explicit update workflow (required because full submanufacturing is expensive) ---
    # No backend calls happen here unless the user clicks the button.
    key = f"{int(selected_character_id)}:{int(selected_profile_id or 0)}:{1 if maximize_runs else 0}"
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
            st.rerun()

    job_id = st.session_state.get("industry_builder_job_id")
    job_key = st.session_state.get("industry_builder_job_key")
    if job_id and job_key == key and not industry_data:
        try:
            status_resp = api_get(f"/industry_builder_update_status/{job_id}") or {}
            if status_resp.get("status") != "success":
                st.error(f"API error: {status_resp.get('message', 'Unknown error')}")
            else:
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
                elif status == "done":
                    result_resp = api_get(
                        f"/industry_builder_update_result/{job_id}",
                        timeout_seconds=300,
                    ) or {}
                    if result_resp.get("status") != "success":
                        st.error(f"API error: {result_resp.get('message', 'Unknown error')}")
                    else:
                        data = result_resp.get("data") or []
                        meta = result_resp.get("meta")
                        if isinstance(cache, dict):
                            cache[key] = {"data": data, "meta": meta}
                        st.session_state["industry_builder_job_id"] = None
                        st.session_state["industry_builder_job_key"] = None
                        st.rerun()
                else:
                    frac = (float(done) / float(total_i)) if total_i and total_i > 0 else 0.0
                    st.progress(min(1.0, max(0.0, frac)), text=f"Updating: {done} / {total_i or '?'} blueprints")
                    time.sleep(1)
                    st.rerun()
        except Exception as e:
            st.error(f"Error calling backend: {e}")

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

        job_runs = props.get("job_runs")
        try:
            job_runs_i = int(job_runs or 1)
        except Exception:
            job_runs_i = 1

        # Effective totals (includes copy overhead for BPCs when present)
        est_fee_total = effective.get("estimated_total_job_cost_isk")
        if est_fee_total is None:
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

        # Profit (incl. job fee) is the most actionable for ROI.
        profit_total = total_product_value - total_material_cost - est_fee_total_f

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
                unit_price = float(prod.get("average_price") or 0.0)
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

            # Per-item metrics
            mat_cost_per_item = allocated_material_cost / float(prod_qty_total)
            prod_value_per_item = allocated_product_value / float(prod_qty_total)
            profit_per_item = allocated_profit / float(prod_qty_total)
            job_fee_per_item = allocated_job_fee / float(prod_qty_total)

            denom_total = allocated_material_cost + allocated_job_fee
            roi_total = (allocated_profit / float(denom_total)) if denom_total > 0 else None
            roi_total_percent = (float(roi_total) * 100.0) if roi_total is not None else None

            # Per-item ROI is identical to total ROI for linear costs, but we show both.
            denom_item = mat_cost_per_item + job_fee_per_item
            roi_item = (profit_per_item / float(denom_item)) if denom_item > 0 else None
            roi_item_percent = (float(roi_item) * 100.0) if roi_item is not None else None

            row = {
                # Produced item grain
                "type_id": prod_type_id,
                "type_name": prod_type_name,
                "category": prod_cat,
                "group": prod_grp,

                # Useful context
                "blueprint": bp.get("type_name"),
                "solar_system": (solar.get("name") if isinstance(solar, dict) else None),
                "solar_system_security": (solar.get("security_status") if isinstance(solar, dict) else None),

                # Job configuration
                "Runs": int(job_runs_i),
                "Units": int(prod_qty_total),
                "ME": bp.get("blueprint_material_efficiency_percent"),
                "TE": bp.get("blueprint_time_efficiency_percent"),

                # Per-item outputs
                "Mat. Cost / item": float(mat_cost_per_item),
                "Prod. Value / item": float(prod_value_per_item),
                "Profit / item": float(profit_per_item),
                "Est. Job Fee / item": float(job_fee_per_item),
                "ROI / item": float(roi_item_percent) if roi_item_percent is not None else None,

                # Totals
                "Total Mat. Cost": float(allocated_material_cost),
                "Total Prod. Value": float(allocated_product_value),
                "Total Profit": float(allocated_profit),
                "Total Job Fee": float(allocated_job_fee),
                "ROI / total": float(roi_total_percent) if roi_total_percent is not None else None,

                # Location should be last in the table
                "location": (loc.get("display_name") if isinstance(loc, dict) else None),

                # Internal (for consistency checks / possible later use)
                "_profit_total": float(profit_total),
                "_total_material_cost": float(total_material_cost),
                "_total_product_value": float(total_product_value),
                "_total_job_fee": float(est_fee_total_f),
            }

            table_rows.append(row)

    products_df = pd.DataFrame(table_rows)

    if selected_categories:
        products_df = products_df[products_df["category"].isin(selected_categories)]

    st.caption(f"{len(products_df)} product rows")

    # Keep the main table focused: hide debug/internal fields.
    hidden_cols = {
        "_profit_total",
        "_total_material_cost",
        "_total_product_value",
        "_total_job_fee",
    }
    display_df = products_df.drop(columns=[c for c in hidden_cols if c in products_df.columns], errors="ignore")

    # Hide category/group/blueprint fields from the table (requested).
    display_df = display_df.drop(
        columns=[c for c in ["category", "group", "blueprint"] if c in display_df.columns],
        errors="ignore",
    )

    # Ensure location is the last visible column.
    if "location" in display_df.columns:
        cols = [c for c in display_df.columns if c != "location"] + ["location"]
        display_df = display_df[cols]

    # Add item icon column right after type_id (if available).
    if "type_id" in display_df.columns:
        try:
            icon_series = display_df["type_id"].apply(lambda tid: _type_icon_url(tid, size=32))
            if "Icon" not in display_df.columns:
                if "type_name" in display_df.columns:
                    insert_at = max(0, int(list(display_df.columns).index("type_name")))
                else:
                    insert_at = min(1, len(display_df.columns))
                display_df.insert(insert_at, "Icon", icon_series)
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

    column_config = {}
    if "Icon" in display_df.columns:
        column_config["Icon"] = st.column_config.ImageColumn("Icon", width="small")
    if "Mat. Cost / item" in display_df.columns:
        column_config["Mat. Cost / item"] = st.column_config.NumberColumn("Mat. Cost / item", format="%.2f ISK")
    if "Prod. Value / item" in display_df.columns:
        column_config["Prod. Value / item"] = st.column_config.NumberColumn("Prod. Value / item", format="%.2f ISK")
    if "Profit / item" in display_df.columns:
        column_config["Profit / item"] = st.column_config.NumberColumn("Profit / item", format="%.2f ISK")
    if "Est. Job Fee / item" in display_df.columns:
        column_config["Est. Job Fee / item"] = st.column_config.NumberColumn(
            "Est. Job Fee / item",
            format="%.2f ISK",
            help="Estimated installation fee per produced item. For BPCs, this includes estimated copying overhead.",
        )
    if "ROI / item" in display_df.columns:
        column_config["ROI / item"] = st.column_config.NumberColumn(
            "ROI / item",
            format="%.2f%%",
            help="Per-item ROI (same as total ROI for linear costs).",
        )
    if "ROI / total" in display_df.columns:
        column_config["ROI / total"] = st.column_config.NumberColumn(
            "ROI / total",
            format="%.2f%%",
            help="Total ROI for this product row.",
        )
    for col in ["Total Mat. Cost", "Total Prod. Value", "Total Profit", "Total Job Fee"]:
        if col in display_df.columns:
            column_config[col] = st.column_config.NumberColumn(col, format="%.0f ISK")
    if "solar_system_security" in display_df.columns:
        column_config["solar_system_security"] = st.column_config.NumberColumn("Sec", format="%.2f")

    st.dataframe(display_df, width="stretch", hide_index=True, column_config=column_config)
    st.divider()

    if not filtered_blueprints or products_df.empty:
        return

    st.subheader("Product Details")

    # Select a produced item (product-centric workflow)
    prod_pairs = (
        products_df[["type_id", "type_name"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["type_name", "type_id"], ascending=[True, True])
    )
    if prod_pairs.empty:
        st.info("No producible products available with the current filters.")
        return

    prod_options = [(int(r["type_id"]), str(r["type_name"])) for _, r in prod_pairs.iterrows()]
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
    required_skills = full_bp_data.get("required_skills", []) or []

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
