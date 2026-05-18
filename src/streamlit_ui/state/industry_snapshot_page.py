from __future__ import annotations

from dataclasses import dataclass
import time
from datetime import datetime, timezone
from typing import Any, cast

import streamlit as st

from streamlit_ui.api.characters import (
    build_character_options,
    build_owned_blueprint_character_corporation_scope_options,
    build_owned_blueprint_character_scope_options,
    fetch_characters,
    filter_industry_characters,
)
from streamlit_ui.api.corporations import build_owned_blueprint_corporation_scope_options
from streamlit_ui.api.industry_builder import (
    clear_industry_builder_caches,
    fetch_job_manager_status,
    fetch_product_overview_refresh_status,
    fetch_solar_system_security_map,
    start_product_overview_refresh,
)
from streamlit_ui.state.industry_builder_page import (
    default_character_id,
    ensure_meta_group_filter_state,
    ensure_overview_refresh_state,
    ensure_selection_state,
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
from streamlit_ui.state.industry_builder_ui import meta_group_label, meta_group_toggle_key, ordered_meta_group_names, get_meta_group_name
from streamlit_ui.api.industry_profiles import build_industry_profile_options
from streamlit_ui.state.session_state import ensure_valid_state_value
from streamlit_ui.components.webpage_ui import render_job_status_panel


@dataclass(frozen=True)
class SharedIndustrySnapshotContext:
    default_character_id_value: int
    default_industry_profile_id: int
    default_owned_blueprint_scope: str
    reactions_allowed_for_profile: bool
    overview_rows: list[dict[str, Any]]
    overview_meta: dict[str, Any]
    refresh_view: dict[str, Any]
    enabled_meta_groups: set[str]


_MARKET_HUB_OPTIONS = ["jita", "amarr", "dodixie", "rens", "hek"]
_MARKET_HUB_LABELS = {
    "jita": "Jita 4-4",
    "amarr": "Amarr VIII (Oris)",
    "dodixie": "Dodixie IX - Moon 20",
    "rens": "Rens VI - Moon 8",
    "hek": "Hek VIII - Moon 12",
}
_MARKET_ORDER_SIDE_OPTIONS = ["sell", "buy"]
_INPUT_ORDER_SIDE_LABELS = {
    "sell": "Buy from Sell Orders",
    "buy": "Buy with Buy Orders",
}
_OUTPUT_ORDER_SIDE_LABELS = {
    "sell": "Place Sell Orders",
    "buy": "Sell to Buy Orders",
}


def _rerun() -> None:
    st.rerun()


def _parse_iso_timestamp(value: Any) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_elapsed_seconds(value: float | None) -> str:
    if value is None:
        return "N/A"
    total_seconds = max(0, int(value))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _refresh_elapsed_seconds(refresh_view: dict[str, Any]) -> float | None:
    created_at = _parse_iso_timestamp(refresh_view.get("created_at"))
    if created_at is None:
        return None
    now = datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return max(0.0, (now - created_at).total_seconds())


def _refresh_stage_copy(stage: str) -> tuple[str, str]:
    stage_key = str(stage or "refresh").strip().lower()
    mapping = {
        "queued": ("Queued", "Your refresh request is waiting to start."),
        "startup": ("Starting", "The backend is preparing the refresh job and validating the request."),
        "blueprints": ("Loading Blueprints", "The latest blueprint snapshot is being loaded for this overview."),
        "context": ("Preparing Context", "Character settings, profile modifiers, and pricing context are being resolved."),
        "assets": ("Checking Assets", "Owned blueprints and available inventory are being matched to possible builds."),
        "rows": ("Building Products", "The backend is constructing manufacturable product rows and their base job trees."),
        "market_history": ("Loading Market History", "Regional historical volume is being loaded to estimate how actively items trade."),
        "liquidity": ("Loading Hub Liquidity", "Current hub buy and sell order depth is being loaded for market activity signals."),
        "profit": ("Calculating Profit", "Sale proceeds, fees, total costs, and profitability metrics are being computed."),
        "finalize": ("Finalizing", "Confidence signals and final payload details are being assembled for the page."),
        "completed": ("Completed", "The refreshed overview is ready and will be shown automatically."),
    }
    return mapping.get(stage_key, (stage_key.replace("_", " ").title() or "Refreshing", "The overview is being refreshed."))


def _refresh_step_items() -> list[tuple[int, str]]:
    return [
        (1, "Start refresh"),
        (2, "Load blueprint snapshot"),
        (3, "Prepare character and profile context"),
        (4, "Resolve owned assets and inventory"),
        (5, "Build manufacturable product rows"),
        (6, "Load regional market history"),
        (7, "Load hub liquidity signals"),
        (8, "Calculate costs and profitability"),
        (9, "Finalize overview payload"),
    ]


def render_refresh_in_progress(refresh_view: dict[str, Any]) -> None:
    progress_meta = cast(dict[str, Any], refresh_view.get("progress_meta") or {})
    elapsed_seconds = _refresh_elapsed_seconds(refresh_view)
    step = int(progress_meta.get("step") or 0)
    step_count = int(progress_meta.get("step_count") or 0)
    stage = str(progress_meta.get("stage") or "refresh")
    stage_title, stage_description = _refresh_stage_copy(stage)
    progress_fraction = float(refresh_view.get("progress_fraction") or 0.0)
    progress_label = str(refresh_view.get("progress_label") or "Refreshing overview...")

    st.markdown("### Refreshing Industry Builder")
    st.caption("The overview is being rebuilt in the background. The page will resume automatically when the refresh is finished.")

    summary_col_left, summary_col_mid, summary_col_right = st.columns(3)
    summary_col_left.metric("Elapsed", _format_elapsed_seconds(elapsed_seconds))
    current_step_label = f"{step}/{step_count}" if step_count > 0 and step > 0 else "Queued"
    summary_col_mid.metric("Current Step", current_step_label)
    summary_col_right.metric("Current Stage", stage_title)

    st.progress(int(max(0.0, min(1.0, progress_fraction)) * 100), text=progress_label)
    st.markdown(f"**Now happening:** {stage_description}")

    info_col, steps_col = st.columns([3, 2])
    with info_col:
        st.markdown("**What this refresh updates**")
        st.write("- Manufacturable product rows and job trees")
        st.write("- Material pricing and market history")
        st.write("- Hub liquidity, profitability, and confidence signals")
        st.write("- Any settings you changed before starting the refresh")
    with steps_col:
        st.markdown("**Refresh steps**")
        for item_step, item_label in _refresh_step_items():
            if step_count > 0 and item_step < step:
                prefix = "[done]"
            elif item_step == step:
                prefix = "[now]"
            else:
                prefix = "[next]"
            st.write(f"{prefix} {item_label}")

    st.caption("No additional overview content is rendered during the refresh to keep the page stable and reduce UI churn.")


def load_character_context() -> tuple[
    list[dict[str, Any]],
    dict[int, str],
    int,
    list[str],
    dict[str, str],
    str,
]:
    characters = filter_industry_characters(fetch_characters())
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


def render_selector_section(
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


def render_filters_section(
    *,
    overview_rows: list[dict[str, Any]],
    reactions_allowed_for_profile: bool,
) -> set[str]:
    meta_group_names = ordered_meta_group_names({get_meta_group_name(row) for row in overview_rows})
    if not meta_group_names:
        return set()

    ensure_meta_group_filter_state(meta_group_names)
    ensure_valid_state_value(
        "industry_builder_market_hub",
        "jita",
        valid_values=_MARKET_HUB_OPTIONS,
        coerce=str,
    )
    ensure_valid_state_value(
        "industry_builder_material_price_side",
        "sell",
        valid_values=_MARKET_ORDER_SIDE_OPTIONS,
        coerce=str,
    )
    ensure_valid_state_value(
        "industry_builder_product_price_side",
        "sell",
        valid_values=_MARKET_ORDER_SIDE_OPTIONS,
        coerce=str,
    )

    filter_group_col, misc_group_col, market_group_col, profit_group_col = st.columns(4)
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

    with market_group_col:
        market_container = st.container(border=True)
        market_container.caption("Market")
        market_container.selectbox(
            "Trade Hub",
            options=_MARKET_HUB_OPTIONS,
            format_func=lambda value: _MARKET_HUB_LABELS.get(str(value), str(value)),
            key="industry_builder_market_hub",
            help="Applied only after Refresh Overview. Uses the selected trade hub for both input and output pricing.",
        )
        market_container.selectbox(
            "Input Pricing",
            options=_MARKET_ORDER_SIDE_OPTIONS,
            format_func=lambda value: _INPUT_ORDER_SIDE_LABELS.get(str(value), str(value)),
            key="industry_builder_material_price_side",
            help="Choose whether required materials are valued from the hub's sell orders or buy orders.",
        )
        market_container.selectbox(
            "Output Pricing",
            options=_MARKET_ORDER_SIDE_OPTIONS,
            format_func=lambda value: _OUTPUT_ORDER_SIDE_LABELS.get(str(value), str(value)),
            key="industry_builder_product_price_side",
            help="Choose whether finished goods are valued as sell orders you place or direct sales into buy orders.",
        )
        market_container.caption("These market settings are backend-backed and take effect after Refresh Overview.")

    with profit_group_col:
        profit_container = st.container(border=True)
        profit_container.caption("Profit Filters")
        profit_container.checkbox(
            "Positive profit only",
            key="industry_builder_positive_profit_only",
        )
        profit_container.number_input(
            "Min Margin (%)",
            min_value=0.0,
            step=0.5,
            key="industry_builder_min_margin_pct",
        )
        profit_container.number_input(
            "Min ISK/Hour",
            min_value=0.0,
            step=100000.0,
            key="industry_builder_min_isk_per_hour",
        )
        profit_container.number_input(
            "Min Region Daily Volume",
            min_value=0,
            step=1,
            key="industry_builder_min_region_daily_volume",
            help="Most recent daily traded volume from ESI market history for the selected hub's region.",
        )
        profit_container.caption("These filters apply immediately to the current snapshot. Hub buy/sell liquidity stays a separate live orderbook signal.")

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


def render_job_manager_status(job_manager_status: dict[str, Any]) -> None:
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


def _format_age_minutes(value: Any) -> str:
    try:
        minutes = float(value or 0.0)
    except Exception:
        return "N/A"
    if minutes <= 0:
        return "0 min"
    if minutes >= 1440:
        return f"{minutes / 1440.0:.1f} d"
    if minutes >= 60:
        return f"{minutes / 60.0:.1f} h"
    return f"{minutes:.0f} min"


def render_pricing_batch_panel(pricing_batch: dict[str, Any]) -> None:
    if not pricing_batch:
        return

    with st.expander("Pricing provenance and freshness", expanded=False):
        st.caption(
            "Batch generated: {generated} | Hub: {hub} | Inputs: {inputs} | Outputs: {outputs}".format(
                generated=str(pricing_batch.get("generated_at") or "N/A"),
                hub=str(pricing_batch.get("market_hub_label") or pricing_batch.get("market_hub") or "N/A"),
                inputs=str(pricing_batch.get("material_price_side") or "N/A"),
                outputs=str(pricing_batch.get("product_price_side") or "N/A"),
            )
        )

        left, middle, right = st.columns(3)
        with left:
            st.markdown("**Batch settings**")
            st.write(
                {
                    "row_count": pricing_batch.get("row_count"),
                    "cache_ttl_seconds": pricing_batch.get("cache_ttl_seconds"),
                    "orderbook_depth": pricing_batch.get("orderbook_depth"),
                    "orderbook_smoothing": pricing_batch.get("orderbook_smoothing"),
                }
            )
        with middle:
            st.markdown("**Product pricing batch**")
            st.write(pricing_batch.get("product_pricing") or {})
        with right:
            st.markdown("**Material pricing batch**")
            st.write(pricing_batch.get("material_pricing") or {})

        confidence_col, activity_col = st.columns(2)
        with confidence_col:
            st.markdown("**Confidence distribution**")
            st.write(pricing_batch.get("confidence_distribution") or {})
        with activity_col:
            st.markdown("**Freshness / age**")
            st.write(
                {
                    "oldest_material_age": _format_age_minutes(pricing_batch.get("oldest_material_age_minutes")),
                    "oldest_product_age": _format_age_minutes(pricing_batch.get("oldest_product_age_minutes")),
                    "oldest_market_history_age": _format_age_minutes(pricing_batch.get("oldest_market_history_age_minutes")),
                }
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
    start_overview_refresh_job(
        default_character_id_value=default_character_id_value,
        default_industry_profile_id=default_industry_profile_id,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
        start_refresh_fn=start_product_overview_refresh,
    )
    return True


def prepare_shared_industry_snapshot_page(
    *,
    title: str,
    intro_caption: str,
    refresh_caption: str,
    refresh_button_label: str,
    refresh_button_key: str,
    no_rows_message: str,
    use_collapsed_advanced_snapshot_section: bool = False,
    context_heading: str | None = None,
    advanced_snapshot_heading: str | None = None,
) -> SharedIndustrySnapshotContext | None:
    st.subheader(title)
    ensure_overview_refresh_state()

    refresh_view = overview_refresh_view()
    if bool(refresh_view.get("is_active")):
        try:
            poll_overview_refresh_job(
                fetch_status_fn=fetch_product_overview_refresh_status,
                fetch_job_manager_status_fn=fetch_job_manager_status,
            )
        except Exception as exc:
            clear_overview_refresh_job(error_message=str(exc))
        refresh_view = overview_refresh_view()
        if bool(refresh_view.get("is_active")):
            render_refresh_in_progress(refresh_view)
            time.sleep(1.0)
            _rerun()
            return None

    (
        _characters,
        character_options,
        default_character_id_value,
        owned_blueprint_scope_options,
        owned_blueprint_scope_labels,
        default_owned_blueprint_scope,
    ) = load_character_context()

    ensure_selection_state(
        character_options=character_options,
        default_character_id_value=default_character_id_value,
        owned_blueprint_scope_options=owned_blueprint_scope_options,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
    )
    ensure_toggle_state()

    if context_heading:
        st.markdown(f"**{context_heading}**")

    (
        selected_character_id,
        selected_industry_profile_id,
        industry_profiles,
        default_industry_profile_id,
    ) = render_selector_section(
        character_options=character_options,
        owned_blueprint_scope_options=owned_blueprint_scope_options,
        owned_blueprint_scope_labels=owned_blueprint_scope_labels,
    )

    solar_system_security_map: dict[int, float] = {}
    try:
        solar_system_security_map = fetch_solar_system_security_map()
    except Exception as exc:
        st.warning(f"Failed to load solar system security status: {exc}")

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

    started_initial_refresh = _ensure_initial_overview_refresh_started(
        default_character_id_value=default_character_id_value,
        default_industry_profile_id=default_industry_profile_id,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
    )

    if "industry_builder_job_manager_status" not in st.session_state:
        try:
            st.session_state["industry_builder_job_manager_status"] = fetch_job_manager_status()
        except Exception as exc:
            st.warning(f"Failed to load industry job manager status: {exc}")
            st.session_state["industry_builder_job_manager_status"] = {}

    render_job_manager_status(cast(dict[str, Any], st.session_state.get("industry_builder_job_manager_status") or {}))

    refresh_view = overview_refresh_view()
    if refresh_view.get("error_message"):
        st.error(str(refresh_view.get("error_message")))

    overview_rows = cast(list[dict[str, Any]], st.session_state.get("industry_builder_overview_rows") or [])
    overview_meta = cast(dict[str, Any], st.session_state.get("industry_builder_overview_meta") or {})
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

        st.info(no_rows_message)
        return None

    st.caption(intro_caption)

    if use_collapsed_advanced_snapshot_section:
        summary_parts = [
            f"Hub: {str(st.session_state.get('industry_builder_market_hub') or 'jita').title()}",
            "Reactions: On" if bool(st.session_state.get("industry_builder_include_reactions", False)) else "Reactions: Off",
            "Build from BPC" if bool(st.session_state.get("industry_builder_build_from_bpc", True)) else "Build from BPO/SDE",
            "Profit filters active" if bool(
                st.session_state.get("industry_builder_positive_profit_only", False)
                or float(st.session_state.get("industry_builder_min_margin_pct", 0.0) or 0.0) > 0.0
                or float(st.session_state.get("industry_builder_min_isk_per_hour", 0.0) or 0.0) > 0.0
                or int(st.session_state.get("industry_builder_min_region_daily_volume", 0) or 0) > 0
            ) else "Profit filters default",
        ]
        st.caption("Advanced snapshot settings affect which products and prices the backend uses. Changes here take effect after refreshing the snapshot.")
        st.caption(" | ".join(summary_parts))
        with st.expander(advanced_snapshot_heading or "Advanced Snapshot Settings", expanded=False):
            render_pricing_batch_panel(cast(dict[str, Any], overview_meta.get("pricing_batch") or overview_meta))
            enabled_meta_groups = render_filters_section(
                overview_rows=overview_rows,
                reactions_allowed_for_profile=reactions_allowed_for_profile,
            )
    else:
        render_pricing_batch_panel(cast(dict[str, Any], overview_meta.get("pricing_batch") or overview_meta))
        enabled_meta_groups = render_filters_section(
            overview_rows=overview_rows,
            reactions_allowed_for_profile=reactions_allowed_for_profile,
        )

    refresh_col_left, refresh_col_right = st.columns([6, 1])
    with refresh_col_left:
        st.caption(refresh_caption)
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
            refresh_button_label,
            key=refresh_button_key,
            disabled=bool(refresh_view.get("is_active")),
        ):
            try:
                clear_industry_builder_caches()
                start_overview_refresh_job(
                    default_character_id_value=default_character_id_value,
                    default_industry_profile_id=default_industry_profile_id,
                    default_owned_blueprint_scope=default_owned_blueprint_scope,
                    reactions_allowed_for_profile=reactions_allowed_for_profile,
                    start_refresh_fn=start_product_overview_refresh,
                )
            except Exception as exc:
                st.error(f"Failed to refresh industry product overview: {exc}")
                return None
            _rerun()

    return SharedIndustrySnapshotContext(
        default_character_id_value=default_character_id_value,
        default_industry_profile_id=default_industry_profile_id,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
        overview_rows=overview_rows,
        overview_meta=overview_meta,
        refresh_view=refresh_view,
        enabled_meta_groups=enabled_meta_groups,
    )