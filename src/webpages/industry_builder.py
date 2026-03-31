import time
import streamlit as st
from typing import Any, cast

from utils.aggrid_formatters import js_eu_number_formatter, js_icon_cell_renderer
from utils.characters_api import (
    build_character_options,
    build_owned_blueprint_character_corporation_scope_options,
    build_owned_blueprint_character_scope_options,
    fetch_characters,
)
from utils.corporations_api import build_owned_blueprint_corporation_scope_options
from utils.formatters import format_duration
from utils.industry_builder_api import (
    clear_industry_builder_caches,
    fetch_job_manager_status,
    fetch_product_overview_refresh_status,
    fetch_solar_system_security_map,
    start_product_overview_refresh,
)
from utils.industry_builder_page import (
    default_character_id,
    ensure_overview_refresh_state,
    ensure_selection_state,
    ensure_meta_group_filter_state,
    ensure_toggle_state,
    fetch_industry_profiles_cached,
    overview_refresh_is_active,
    overview_refresh_view,
    persist_filter_preferences,
    poll_overview_refresh_job,
    resolve_profile_security_status,
    start_overview_refresh_job,
    clear_overview_refresh_job,
)
from utils.industry_builder_ui import (
    build_overview_grid_frame,
    build_debug_payload_preview,
    filter_overview_rows,
    meta_group_label,
    meta_group_toggle_key,
    ordered_meta_group_names,
    get_meta_group_name,
)
from utils.industry_profiles_api import build_industry_profile_options
from utils.session_state import ensure_valid_state_value
from utils.webpage_ui import render_job_status_panel, require_aggrid


def _rerun() -> None:
    st.rerun()


def _load_character_context() -> tuple[
    list[dict[str, Any]],
    dict[int, str],
    int,
    list[str],
    dict[str, str],
    str,
]:
    characters = fetch_characters()
    if not characters:
        raise RuntimeError("No character data found. Run main.py first.")

    character_options = build_character_options(characters)
    if not character_options:
        raise RuntimeError("No character data found. Run main.py first.")

    default_character_id_value = default_character_id(
        cast(list[dict[str, Any]], characters),
        character_options,
    )
    character_scope_options, character_scope_labels, default_character_scope = (
        build_owned_blueprint_character_scope_options(cast(list[dict[str, Any]], characters))
    )
    character_corp_scope_options, character_corp_scope_labels, default_character_corp_scope = (
        build_owned_blueprint_character_corporation_scope_options(cast(list[dict[str, Any]], characters))
    )
    corporation_scope_options, corporation_scope_labels = build_owned_blueprint_corporation_scope_options(
        cast(list[dict[str, Any]], characters)
    )
    owned_blueprint_scope_options = [
        *character_scope_options,
        *character_corp_scope_options,
        *corporation_scope_options,
        "all",
    ]
    owned_blueprint_scope_labels = {
        **character_scope_labels,
        **character_corp_scope_labels,
        **corporation_scope_labels,
        "all": "All (characters + corps)",
    }
    default_owned_blueprint_scope = default_character_corp_scope or default_character_scope or "all"
    return (
        cast(list[dict[str, Any]], characters),
        character_options,
        default_character_id_value,
        owned_blueprint_scope_options,
        owned_blueprint_scope_labels,
        default_owned_blueprint_scope,
    )


def _render_selector_section(
    *,
    character_options: dict[int, str],
    owned_blueprint_scope_options: list[str],
    owned_blueprint_scope_labels: dict[str, str],
) -> tuple[int, int, list[dict[str, Any]], int]:
    character_ids = list(character_options.keys())
    selector_col_left, selector_col_mid, selector_col_right = st.columns(3)
    with selector_col_left:
        st.selectbox(
            "Owned Blueprints",
            options=owned_blueprint_scope_options,
            format_func=lambda x: owned_blueprint_scope_labels.get(str(x), str(x)),
            key="industry_builder_owned_blueprints_scope",
        )
        st.caption(
            "Pick one character, one character plus its corporation, one corporation, or all characters and corporations."
        )

    with selector_col_mid:
        selected_character_id = st.selectbox(
            "Character Skills",
            options=character_ids,
            format_func=lambda x: character_options.get(int(x), str(x)),
            key="industry_builder_character_id",
        )

    industry_profiles = fetch_industry_profiles_cached(character_id=int(selected_character_id))
    industry_profile_options, industry_profile_labels, default_industry_profile_id = build_industry_profile_options(
        cast(list[dict[str, Any]], industry_profiles)
    )
    ensure_valid_state_value(
        "industry_builder_industry_profile_id",
        int(default_industry_profile_id),
        valid_values=industry_profile_options,
        coerce=int,
    )

    with selector_col_right:
        selected_industry_profile_id = st.selectbox(
            "Industry Profile",
            options=industry_profile_options,
            format_func=lambda x: industry_profile_labels.get(int(x), str(x)),
            key="industry_builder_industry_profile_id",
        )
        if len(industry_profiles) == 0:
            st.caption("No saved industry profiles for this character. The backend will continue without facility-specific modifiers.")
        else:
            st.caption("Applied only after Refresh Overview. Used for system cost indices, facility tax, and structure rig modifiers.")

    return int(selected_character_id), int(selected_industry_profile_id), industry_profiles, int(default_industry_profile_id)


def _render_filters_section(
    *,
    overview_rows: list[dict[str, Any]],
    reactions_allowed_for_profile: bool,
) -> set[str]:
    meta_group_names = ordered_meta_group_names({get_meta_group_name(row) for row in overview_rows})
    if not meta_group_names:
        return set()

    ensure_meta_group_filter_state(meta_group_names)

    filter_group_col, misc_group_col = st.columns(2)
    with filter_group_col:
        meta_group_container = st.container(border=True)
        meta_group_container.caption("Meta Group Filters")
        filter_columns = meta_group_container.columns(3)

    with misc_group_col:
        misc_container = st.container(border=True)
        misc_container.caption("Misc")
        misc_col_left, misc_col_right = misc_container.columns(2)
        with misc_col_left:
            st.checkbox(
                "Maximize BP runs",
                key="industry_builder_maximize_bp_runs_pending",
                help="Applied only after Refresh Overview. Uses the blueprint's max production limit as the number of manufacturing runs.",
            )
            st.checkbox(
                "Group identical BPCs",
                key="industry_builder_group_identical_bpcs",
                help="Applied only after Refresh Overview. When enabled, identical owned blueprint copies for the same product are shown as one aggregated product row. Disable to show one top-level product row per owned BPC.",
            )
            st.checkbox(
                "Build from BPC",
                key="industry_builder_build_from_bpc",
                help="Applied only after Refresh Overview. Prefer blueprint copies. If none exist, fallback to owned blueprint originals.",
            )
            st.checkbox(
                "I have a BPC/BPO",
                key="industry_builder_have_blueprint_source_only",
                help="Applied only after Refresh Overview. Returns only products where the backend identified a BPC or BPO source.",
            )
        with misc_col_right:
            st.checkbox(
                "I have the skills",
                key="industry_builder_have_skills_only",
                help="Show only products for which the selected character meets all manufacturing skill requirements.",
            )
            st.checkbox(
                "Include reactions",
                key="industry_builder_include_reactions",
                disabled=not reactions_allowed_for_profile,
                help=(
                    "Applied only after Refresh Overview. Includes recursive reaction planning for reaction-based materials."
                    if reactions_allowed_for_profile
                    else "Reactions are only available in low-sec or null-sec systems for the selected industry profile."
                ),
            )
            if not reactions_allowed_for_profile:
                st.caption("Reactions disabled: the selected industry profile is in high-sec.")

    column_groups = [
        {"Tech I", "Tech II", "Tech III"},
        {"Faction", "Storyline", "Officer"},
        {"Other"},
    ]
    enabled_meta_groups: set[str] = set()
    for meta_group_name in meta_group_names:
        toggle_key = meta_group_toggle_key(meta_group_name)
        label = meta_group_label(meta_group_name)
        if toggle_key not in st.session_state:
            st.session_state[toggle_key] = label == "Tech I"

        target_column_index = 2
        for index, group in enumerate(column_groups):
            if label in group:
                target_column_index = index
                break

        with filter_columns[target_column_index]:
            enabled = st.toggle(
                label,
                key=toggle_key,
            )
        if enabled:
            enabled_meta_groups.add(meta_group_name)

    persist_filter_preferences(meta_group_names)
    return enabled_meta_groups


def _render_job_manager_status(job_manager_status: dict[str, Any]) -> None:
    if not job_manager_status:
        return
    queue_counts = job_manager_status.get("queue_counts") or {}
    last_snapshot_at = job_manager_status.get("last_snapshot_at") or "Not built yet"
    st.caption(
        "Snapshot rows: {rows} | Last snapshot: {snapshot} | Queues -> MFG: {mfg}, React: {react}, Copy: {copy}, Invention: {inv}".format(
            rows=job_manager_status.get("snapshot_count", 0),
            snapshot=last_snapshot_at,
            mfg=queue_counts.get("manufacturing", 0),
            react=queue_counts.get("reaction", 0),
            copy=queue_counts.get("copying", 0),
            inv=queue_counts.get("invention", 0),
        )
        + " | ME Research: {me} | TE Research: {te}".format(
            me=queue_counts.get("research_material", 0),
            te=queue_counts.get("research_time", 0),
        )
    )


def _ensure_initial_overview_refresh_started(
    *,
    default_character_id_value: int,
    default_industry_profile_id: int,
    default_owned_blueprint_scope: str,
    reactions_allowed_for_profile: bool,
) -> bool:
    overview_rows = cast(list[dict[str, Any]], st.session_state.get("industry_builder_overview_rows") or [])
    if overview_rows or overview_refresh_is_active():
        return False

    clear_industry_builder_caches()
    fetch_industry_profiles_cached.clear()
    start_overview_refresh_job(
        default_character_id_value=default_character_id_value,
        default_industry_profile_id=default_industry_profile_id,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
        start_refresh_fn=start_product_overview_refresh,
    )
    return True


def _render_overview_grid(
    *,
    runtime: Any,
    filtered_overview_rows: list[dict[str, Any]],
) -> None:
    if not filtered_overview_rows:
        st.info("No overview rows available for the current selection.")
        return

    df, height, grid_state_key = build_overview_grid_frame(filtered_overview_rows)
    if df.empty:
        st.info("No overview rows available for the current selection.")
        return

    st.caption("Use the AgGrid chevrons in Step to expand or collapse the build tree.")
    icon_renderer = js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24)

    gb = runtime.grid_options_builder.from_dataframe(df)
    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=True,
        wrapText=False,
        autoHeight=False,
        wrapHeaderText=False,
        autoHeaderHeight=False,
        cellStyle={"whiteSpace": "nowrap", "lineHeight": "1.2"},
    )

    for col, width in [
        ("Icon", 72),
        ("Activity", 120),
        ("BPC Source", 160),
        ("BPO Source", 160),
        ("Meta Group", 130),
        ("Category", 140),
    ]:
        if col in df.columns:
            gb.configure_column(
                col,
                minWidth=width,
                wrapText=False,
                autoHeight=False,
                wrapHeaderText=False,
                autoHeaderHeight=False,
                cellStyle={"whiteSpace": "nowrap", "lineHeight": "1.2"},
            )

    if "Icon" in df.columns:
        gb.configure_column(
            "Icon",
            headerName="",
            width=72,
            cellRenderer=icon_renderer,
            suppressSizeToFit=True,
            sortable=False,
            filter=False,
        )

    if "Step" in df.columns:
        gb.configure_column("Step", hide=True)

    if "Type" in df.columns:
        gb.configure_column("Type", hide=True)

    for col in ["ID", "Qty", "Runs"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
                minWidth=105,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )
            if col == "ID":
                gb.configure_column(col, hide=True)

    for col in ["_path", "_parent_path", "_depth", "_sort_order", "_has_children"]:
        if col in df.columns:
            gb.configure_column(col, hide=True)

    for col in ["Material Cost", "Job Cost", "Total Cost"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                minWidth=130,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    if "Job Duration" in df.columns:
        gb.configure_column(
            "Job Duration",
            minWidth=140,
            wrapHeaderText=False,
            autoHeaderHeight=False,
        )

    if hasattr(gb, "configure_selection"):
        gb.configure_selection("single", use_checkbox=False)

    gb.configure_grid_options(
        suppressRowTransform=True,
        ensureDomOrder=True,
        tooltipShowDelay=0,
        rowHeight=32,
        headerHeight=36,
        animateRows=True,
        suppressCellFocus=False,
        rowSelection="single",
    )

    grid_options = gb.build()
    grid_options["treeData"] = True
    grid_options["enableRangeSelection"] = True
    grid_options["ensureDomOrder"] = True
    grid_options["copyHeadersToClipboard"] = True
    grid_options["suppressCopyRowsToClipboard"] = False
    grid_options["getDataPath"] = runtime.js_code(
        """
        function(data) {
            try {
                if (!data || data._path === null || data._path === undefined) return [];
                var path = String(data._path);
                if (!path) return [];
                return path.split('|||').filter(function(part) {
                    return part !== null && part !== undefined && String(part).length > 0;
                });
            } catch (e) {
                return [];
            }
        }
        """
    )
    grid_options["groupDefaultExpanded"] = 0
    grid_options["isGroupOpenByDefault"] = runtime.js_code(
        """
        function() {
            return false;
        }
        """
    )
    grid_options["autoGroupColumnDef"] = {
        "headerName": "Step",
        "pinned": "left",
        "minWidth": 320,
        "cellRendererParams": {
            "suppressCount": True,
            "innerRenderer": runtime.js_code(
                """
                function(params) {
                    if (!params || !params.data) return '';
                    return String(params.data.Step || '');
                }
                """
            ),
        },
    }

    runtime.aggrid_fn(
        df,
        gridOptions=grid_options,
        update_mode="NO_UPDATE",
        update_on=[],
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=True,
        key=f"industry_builder_products_overview_{grid_state_key}",
    )


def _render_debug_panel(filtered_overview_rows: list[dict[str, Any]]) -> None:
    debug_options: dict[str, str] = {}
    for row in filtered_overview_rows:
        overview_row_id = str(row.get("overview_row_id") or "")
        product_name = str(row.get("type_name") or row.get("type_id") or "")
        product_type_id = int(row.get("type_id") or 0)
        debug_options[overview_row_id] = f"{product_name} ({product_type_id})"

    if debug_options:
        selected_debug_blueprint_id = st.selectbox(
            "Debug blueprint payload",
            options=list(debug_options.keys()),
            format_func=lambda x: debug_options.get(str(x), str(x)),
            key="industry_builder_debug_blueprint_id",
        )
        selected_debug_payload = next(
            (
                row
                for row in filtered_overview_rows
                if str(row.get("overview_row_id") or "") == str(selected_debug_blueprint_id)
            ),
            None,
        )

        with st.expander("Raw data (for debugging)", expanded=False):
            st.write(build_debug_payload_preview(selected_debug_payload))
            if st.checkbox("Show full nested payload", key="industry_builder_show_full_debug_payload"):
                st.write(selected_debug_payload or {})
    else:
        with st.expander("Raw data (for debugging)", expanded=False):
            st.write({})


def render() -> None:
    st.subheader("Industry Builder")
    runtime = require_aggrid()

    try:
        (
            _characters,
            character_options,
            default_character_id_value,
            owned_blueprint_scope_options,
            owned_blueprint_scope_labels,
            default_owned_blueprint_scope,
        ) = _load_character_context()
    except Exception as e:
        st.error(str(e))
        return

    ensure_selection_state(
        character_options=character_options,
        default_character_id_value=default_character_id_value,
        owned_blueprint_scope_options=owned_blueprint_scope_options,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
    )
    ensure_toggle_state()
    ensure_overview_refresh_state()

    try:
        (
            selected_character_id,
            selected_industry_profile_id,
            industry_profiles,
            default_industry_profile_id,
        ) = _render_selector_section(
            character_options=character_options,
            owned_blueprint_scope_options=owned_blueprint_scope_options,
            owned_blueprint_scope_labels=owned_blueprint_scope_labels,
        )
    except Exception as e:
        st.error(f"Failed to load industry profiles: {e}")
        return

    solar_system_security_map: dict[int, float] = {}
    try:
        solar_system_security_map = fetch_solar_system_security_map()
    except Exception as e:
        st.warning(f"Failed to load solar system security status: {e}")

    selected_profile_security_status = resolve_profile_security_status(
        industry_profiles=industry_profiles,
        selected_industry_profile_id=int(selected_industry_profile_id),
        solar_system_security_map=solar_system_security_map,
    )
    reactions_allowed_for_profile = (
        selected_profile_security_status is None or selected_profile_security_status < 0.5
    )
    if not reactions_allowed_for_profile:
        st.session_state["industry_builder_include_reactions"] = False

    try:
        started_initial_refresh = _ensure_initial_overview_refresh_started(
            default_character_id_value=default_character_id_value,
            default_industry_profile_id=default_industry_profile_id,
            default_owned_blueprint_scope=default_owned_blueprint_scope,
            reactions_allowed_for_profile=reactions_allowed_for_profile,
        )
    except Exception as e:
        st.error(f"Failed to start industry product overview refresh: {e}")
        return

    if "industry_builder_job_manager_status" not in st.session_state:
        try:
            st.session_state["industry_builder_job_manager_status"] = fetch_job_manager_status()
        except Exception as e:
            st.warning(f"Failed to load industry job manager status: {e}")
            st.session_state["industry_builder_job_manager_status"] = {}

    if overview_refresh_is_active():
        try:
            poll_overview_refresh_job(
                fetch_status_fn=fetch_product_overview_refresh_status,
                fetch_job_manager_status_fn=fetch_job_manager_status,
            )
        except Exception as e:
            clear_overview_refresh_job(error_message=str(e))

    _render_job_manager_status(cast(dict[str, Any], st.session_state.get("industry_builder_job_manager_status") or {}))

    refresh_view = overview_refresh_view()
    if refresh_view.get("error_message"):
        st.error(str(refresh_view.get("error_message")))

    overview_rows = cast(list[dict[str, Any]], st.session_state.get("industry_builder_overview_rows") or [])
    if not overview_rows:
        if bool(refresh_view.get("is_active")):
            if started_initial_refresh:
                st.info("Preparing the initial product overview in the background.")
            render_job_status_panel(
                title="Preparing initial overview",
                is_running=True,
                progress_fraction=float(refresh_view.get("progress_fraction") or 0.0),
                progress_text=str(refresh_view.get("progress_label") or "Refreshing overview..."),
            )
            st.caption("The initial product overview is being prepared in the background. This page will update automatically when the snapshot is ready.")
            time.sleep(1.0)
            _rerun()

        st.info("No manufacturable product rows are available yet.")
        return

    st.caption(
        "Manufacturable product overview derived from the SDE blueprints and enriched with type metadata. "
        "Each product row contains a simplified manufacturing job payload with materials, skills, time, and production limits."
    )

    enabled_meta_groups = _render_filters_section(
        overview_rows=overview_rows,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
    )

    refresh_col_left, refresh_col_right = st.columns([6, 1])
    with refresh_col_left:
        st.caption("Backend-backed changes are applied only after clicking Refresh Overview.")
        if bool(refresh_view.get("is_active")):
            render_job_status_panel(
                title="Overview refresh",
                is_running=True,
                progress_fraction=float(refresh_view.get("progress_fraction") or 0.0),
                progress_text=str(refresh_view.get("progress_label") or "Refreshing overview..."),
            )
            st.caption("Refresh job is running in the background. The current snapshot stays visible until the backend job completes.")
    with refresh_col_right:
        if st.button(
            "Refresh Overview",
            key="industry_builder_refresh_overview",
            disabled=bool(refresh_view.get("is_active")),
        ):
            try:
                clear_industry_builder_caches()
                fetch_industry_profiles_cached.clear()
                start_overview_refresh_job(
                    default_character_id_value=default_character_id_value,
                    default_industry_profile_id=default_industry_profile_id,
                    default_owned_blueprint_scope=default_owned_blueprint_scope,
                    reactions_allowed_for_profile=reactions_allowed_for_profile,
                    start_refresh_fn=start_product_overview_refresh,
                )
            except Exception as e:
                st.error(f"Failed to refresh industry product overview: {e}")
                return
            _rerun()

    filtered_overview_rows = filter_overview_rows(
        overview_rows,
        tuple(sorted(enabled_meta_groups)),
        bool(st.session_state.get("industry_builder_have_skills_only", True)),
    )
    if not filtered_overview_rows:
        st.info("No manufacturable product rows match the current filters.")
        return

    _render_overview_grid(
        runtime=runtime,
        filtered_overview_rows=filtered_overview_rows,
    )
    _render_debug_panel(filtered_overview_rows)

    if bool(refresh_view.get("is_active")):
        _rerun()
