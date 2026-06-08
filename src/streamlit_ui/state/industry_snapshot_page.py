from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, cast

import streamlit as st

from streamlit_ui.api.characters import (
    build_character_options,
    build_owned_blueprint_character_corporation_scope_options,
    build_owned_blueprint_character_scope_options,
    build_scope_refreshed_at_map,
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
    ensure_overview_refresh_state,
    ensure_selection_state,
    fetch_industry_profiles_cached,
    overview_refresh_is_active,
    overview_refresh_view,
    poll_overview_refresh_job,
    resolve_profile_security_status,
    start_overview_refresh_job,
    clear_overview_refresh_job,
)
from streamlit_ui.api.industry_profiles import build_industry_profile_options
from streamlit_ui.state.session_state import ensure_valid_state_value, ensure_state_defaults
from streamlit_ui.state.industry_builder_ui import get_meta_group_name, meta_group_label, meta_group_toggle_key, ordered_meta_group_names
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
    character_options: dict[int, str]
    owned_blueprint_scope_options: list[str]
    owned_blueprint_scope_labels: dict[str, str]
    scope_last_refreshed_at: dict[str, str | None]


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


def format_scope_refreshed_at(updated_at_iso: str | None) -> str:
    if not updated_at_iso:
        return "nooit ververst"
    dt = _parse_iso_timestamp(updated_at_iso)
    if dt is None:
        return "nooit ververst"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))
    if delta < 60:
        return "zojuist"
    if delta < 3600:
        return f"{delta // 60}m geleden"
    if delta < 86400:
        h, m = divmod(delta, 3600)
        return f"{h}h {m // 60}m geleden" if m >= 60 else f"{h}h geleden"
    return f"{delta // 86400}d geleden"


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


def render_refresh_in_progress(refresh_view: dict[str, Any]) -> None:
    progress_meta = cast(dict[str, Any], refresh_view.get("progress_meta") or {})
    elapsed_seconds = _refresh_elapsed_seconds(refresh_view)
    step = int(progress_meta.get("step") or 0)
    step_count = int(progress_meta.get("step_count") or 0)
    stage = str(progress_meta.get("stage") or "refresh")
    stage_title, stage_description = _refresh_stage_copy(stage)
    progress_fraction = float(refresh_view.get("progress_fraction") or 0.0)
    progress_pct = int(max(0.0, min(1.0, progress_fraction)) * 100)

    with st.container(border=True):
        title_col, elapsed_col = st.columns([6, 1])
        with title_col:
            st.markdown(f"**Refreshing** — {stage_title}")
            st.caption(stage_description)
        with elapsed_col:
            if elapsed_seconds is not None:
                st.caption(_format_elapsed_seconds(elapsed_seconds))
        st.progress(progress_pct)
        if step > 0 and step_count > 0:
            dots = " ".join(
                "●" if i < step else ("◉" if i == step else "○")
                for i in range(1, step_count + 1)
            )
            st.caption(dots)


def render_meta_group_filters(overview_rows: list[dict[str, Any]]) -> set[str]:
    # All possible meta groups that should be available
    all_meta_groups = {"Tech I", "Tech II", "Tech III", "Faction", "Storyline", "Other"}
    default_meta_groups = {"Tech I", "Tech II", "Faction", "Storyline", "Other"}

    # Initialize both pending (user selections) and applied (after refresh) states
    state_defaults = {}
    for name in all_meta_groups:
        toggle_key = meta_group_toggle_key(name)
        applied_key = f"{toggle_key}_applied"
        default_value = name in default_meta_groups
        state_defaults[toggle_key] = default_value
        state_defaults[applied_key] = default_value

    ensure_state_defaults(state_defaults)

    st.subheader("Meta Group Filters")
    filter_cols = st.columns(3)

    enabled_meta_groups: set[str] = set()
    for i, meta_group_name in enumerate(ordered_meta_group_names(all_meta_groups)):
        toggle_key = meta_group_toggle_key(meta_group_name)
        label = meta_group_label(meta_group_name)
        with filter_cols[i % 3]:
            enabled = st.toggle(label, key=toggle_key)
        if enabled:
            enabled_meta_groups.add(meta_group_name)

    return enabled_meta_groups


def render_misc_filters(reactions_allowed_for_profile: bool) -> dict[str, bool]:
    default_misc = {
        "industry_builder_maximize_bp_runs_pending": True,
        "industry_builder_group_identical_bpcs": False,
        "industry_builder_build_from_bpc": True,
        "industry_builder_have_blueprint_source_only": True,
        "industry_builder_have_skills_only": True,
        "industry_builder_include_reactions": False,
    }

    state_defaults = {}
    for key, default_value in default_misc.items():
        state_defaults[key] = default_value
        applied_key = f"{key.replace('_pending', '')}_applied" if "_pending" in key else f"{key}_applied"
        state_defaults[applied_key] = default_value

    ensure_state_defaults(state_defaults)

    st.subheader("Misc")
    misc_cols = st.columns(3)

    misc_filters = {
        "maximize_bp_runs": bool(st.session_state.get("industry_builder_maximize_bp_runs_pending", True)),
        "group_identical_bpcs": bool(st.session_state.get("industry_builder_group_identical_bpcs", True)),
        "build_from_bpc": bool(st.session_state.get("industry_builder_build_from_bpc", True)),
        "have_blueprint_source_only": bool(st.session_state.get("industry_builder_have_blueprint_source_only", True)),
        "have_skills_only": bool(st.session_state.get("industry_builder_have_skills_only", True)),
        "include_reactions": bool(st.session_state.get("industry_builder_include_reactions", False)),
    }

    with misc_cols[0]:
        st.toggle(
            "Maximize BP runs",
            key="industry_builder_maximize_bp_runs_pending",
            help="Uses the blueprint's max production limit as the number of manufacturing runs.",
        )
        st.toggle(
            "I have a BPC/BPO",
            key="industry_builder_have_blueprint_source_only",
            help="Returns only products where the backend identified a BPC or BPO source.",
        )

    with misc_cols[1]:
        st.toggle(
            "Group identical BPCs",
            key="industry_builder_group_identical_bpcs",
            help="Identical owned blueprint copies for the same product are shown as one aggregated row.",
        )
        st.toggle(
            "I have the skills",
            key="industry_builder_have_skills_only",
            help="Show only products for which the selected character meets all manufacturing skill requirements.",
        )

    with misc_cols[2]:
        st.toggle(
            "Build from BPC",
            key="industry_builder_build_from_bpc",
            help="Prefer blueprint copies. If none exist, fallback to owned blueprint originals.",
        )
        st.toggle(
            "Include reactions",
            key="industry_builder_include_reactions",
            disabled=not reactions_allowed_for_profile,
            help=(
                "Includes recursive reaction planning for reaction-based materials."
                if reactions_allowed_for_profile
                else "Reactions are only available in low-sec or null-sec systems for the selected industry profile."
            ),
        )

    return misc_filters


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
    render_about_fn: Callable | None = None,
    scope_last_refreshed_at: dict[str, str | None] | None = None,
) -> tuple[int, int, list[dict[str, Any]], int]:
    character_ids = list(character_options.keys())
    if render_about_fn:
        selector_col_left, selector_col_mid, selector_col_right, selector_col_about = st.columns([3, 3, 3, 1])
    else:
        selector_col_left, selector_col_mid, selector_col_right = st.columns(3)

    with selector_col_left:
        st.selectbox(
            "Owned Blueprints",
            options=owned_blueprint_scope_options,
            format_func=lambda x: owned_blueprint_scope_labels.get(str(x), str(x)),
            key="industry_builder_owned_blueprints_scope",
        )
        if scope_last_refreshed_at is not None:
            selected_scope = str(st.session_state.get("industry_builder_owned_blueprints_scope", ""))
            refreshed_at = scope_last_refreshed_at.get(selected_scope)
            st.caption(f"Data: {format_scope_refreshed_at(refreshed_at)}")

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
            st.caption("No saved industry profiles for this character.")

    if render_about_fn:
        with selector_col_about:
            st.write("")
            st.write("")
            render_about_fn()

    return int(selected_character_id), int(selected_industry_profile_id), industry_profiles, int(default_industry_profile_id)


def render_job_manager_status(job_manager_status: dict[str, Any]) -> None:
    if not job_manager_status:
        return
    queue_counts = job_manager_status.get("queue_counts") or {}
    last_snapshot_at = job_manager_status.get("last_snapshot_at") or "Not built yet"
    st.caption(
        "Snapshot rows: {rows} | Last snapshot: {snapshot} | Active Jobs -> MFG: {mfg}, React: {react}, Copy: {copy}, Invention: {inv}".format(
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
        return "just now"
    if minutes >= 1440:
        return f"{minutes / 1440.0:.1f} d"
    if minutes >= 60:
        return f"{minutes / 60.0:.1f} h"
    return f"{minutes:.0f} min"


def _format_iso_timestamp(iso_timestamp: Any) -> str:
    try:
        from datetime import datetime
        if isinstance(iso_timestamp, str):
            dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            local_dt = dt.astimezone()
            return local_dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(iso_timestamp)
    except Exception:
        return str(iso_timestamp)


def render_pricing_batch_panel(pricing_batch: dict[str, Any], job_manager_status: dict[str, Any] | None = None) -> None:
    if not pricing_batch:
        return

    with st.expander("Pricing provenance and freshness", expanded=False):
        # Combined header with job status and batch info
        header_parts = []

        if job_manager_status:
            queue_counts = job_manager_status.get("queue_counts") or {}
            snapshot_rows = job_manager_status.get("snapshot_count", 0)
            active_jobs = (
                f"Active Jobs: MFG {queue_counts.get('manufacturing', 0)} | "
                f"React {queue_counts.get('reaction', 0)} | "
                f"Copy {queue_counts.get('copying', 0)} | "
                f"Invention {queue_counts.get('invention', 0)} | "
                f"ME {queue_counts.get('research_material', 0)} | "
                f"TE {queue_counts.get('research_time', 0)}"
            )
            header_parts.append(f"Snapshot: {snapshot_rows} rows | {active_jobs}")

        header_parts.append(
            f"Generated: {_format_iso_timestamp(pricing_batch.get('generated_at'))} | "
            f"Hub: {str(pricing_batch.get('market_hub_label') or pricing_batch.get('market_hub') or 'N/A')} | "
            f"Inputs: {str(pricing_batch.get('material_price_side') or 'N/A')} | "
            f"Outputs: {str(pricing_batch.get('product_price_side') or 'N/A')}"
        )

        st.caption(" | ".join(header_parts))

        # Batch settings
        st.markdown("**Batch Configuration**")
        config_col1, config_col2, config_col3, config_col4 = st.columns(4)
        with config_col1:
            st.metric("Pricing rows", pricing_batch.get("row_count", 0))
        with config_col2:
            st.metric("Cache TTL", f"{pricing_batch.get('cache_ttl_seconds', 0)}s")
        with config_col3:
            st.metric("Orderbook depth", pricing_batch.get("orderbook_depth", 0))
        with config_col4:
            st.metric("Orderbook smoothing", pricing_batch.get("orderbook_smoothing", 0))

        # Pricing sources (filter out irrelevant fields)
        excluded_fields = {
            "cached_type_count", "live_type_count", "missing_type_count",
            "newest_age_minutes", "oldest_age_minutes",
            "newest_fetched_at", "oldest_fetched_at"
        }

        pricing_col1, pricing_col2 = st.columns(2)
        with pricing_col1:
            st.markdown("**Product Pricing Source**")
            product_pricing = pricing_batch.get("product_pricing") or {}
            if isinstance(product_pricing, dict) and product_pricing:
                has_relevant_data = False
                for source, count in product_pricing.items():
                    if source not in excluded_fields and count is not None:
                        st.caption(f"{source}: {count}")
                        has_relevant_data = True
                if not has_relevant_data:
                    st.caption("No product pricing data")
            else:
                st.caption("No product pricing data")

        with pricing_col2:
            st.markdown("**Material Pricing Source**")
            material_pricing = pricing_batch.get("material_pricing") or {}
            if isinstance(material_pricing, dict) and material_pricing:
                has_relevant_data = False
                for source, count in material_pricing.items():
                    if source not in excluded_fields and count is not None:
                        st.caption(f"{source}: {count}")
                        has_relevant_data = True
                if not has_relevant_data:
                    st.caption("No material pricing data")
            else:
                st.caption("No material pricing data")

        # Confidence and freshness
        confidence_col, freshness_col = st.columns(2)
        with confidence_col:
            st.markdown("**Pricing Confidence**")
            confidence = pricing_batch.get("confidence_distribution") or {}
            if isinstance(confidence, dict) and confidence:
                for level, count in confidence.items():
                    st.caption(f"{level}: {count}")
            else:
                st.caption("No confidence data")

        with freshness_col:
            st.markdown("**Data Freshness**")
            st.caption(f"Oldest material: {_format_age_minutes(pricing_batch.get('oldest_material_age_minutes'))}")
            st.caption(f"Oldest product: {_format_age_minutes(pricing_batch.get('oldest_product_age_minutes'))}")
            st.caption(f"Oldest market history: {_format_age_minutes(pricing_batch.get('oldest_market_history_age_minutes'))}")


@st.fragment(run_every=1)
def _refresh_status_fragment() -> None:
    if not overview_refresh_is_active():
        st.rerun()
        return
    try:
        poll_overview_refresh_job(
            fetch_status_fn=fetch_product_overview_refresh_status,
            fetch_job_manager_status_fn=fetch_job_manager_status,
        )
    except Exception as exc:
        clear_overview_refresh_job(error_message=str(exc))
    if overview_refresh_is_active():
        render_refresh_in_progress(overview_refresh_view())
    else:
        st.rerun()


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
    render_about_fn: Callable | None = None,
    render_refresh_button: bool = True,
    render_selector_section_ui: bool = True,
    render_status_panels: bool = True,
) -> SharedIndustrySnapshotContext | None:
    st.subheader(title)
    ensure_overview_refresh_state()

    refresh_view = overview_refresh_view()
    if bool(refresh_view.get("is_active")):
        _refresh_status_fragment()
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

    if context_heading:
        st.markdown(f"**{context_heading}**")

    if render_selector_section_ui:
        (
            selected_character_id,
            selected_industry_profile_id,
            industry_profiles,
            default_industry_profile_id,
        ) = render_selector_section(
            character_options=character_options,
            owned_blueprint_scope_options=owned_blueprint_scope_options,
            owned_blueprint_scope_labels=owned_blueprint_scope_labels,
            render_about_fn=render_about_fn,
            scope_last_refreshed_at=build_scope_refreshed_at_map(list(_characters)),
        )
    else:
        selected_character_id = int(st.session_state.get("industry_builder_character_id", default_character_id_value))
        industry_profiles = fetch_industry_profiles_cached(character_id=int(selected_character_id))
        industry_profile_options, industry_profile_labels, default_industry_profile_id = build_industry_profile_options(
            cast(list[dict[str, Any]], industry_profiles)
        )
        selected_industry_profile_id = int(st.session_state.get("industry_builder_industry_profile_id", default_industry_profile_id))

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

    if render_status_panels:
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
            _refresh_status_fragment()
            return None
        st.info(no_rows_message)
        return None

    if intro_caption:
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
        if render_status_panels:
            with st.expander(advanced_snapshot_heading or "Advanced Snapshot Settings", expanded=False):
                render_pricing_batch_panel(cast(dict[str, Any], overview_meta.get("pricing_batch") or overview_meta))
    else:
        if render_status_panels:
            render_pricing_batch_panel(cast(dict[str, Any], overview_meta.get("pricing_batch") or overview_meta))

    # Using hardcoded default meta groups for now
    enabled_meta_groups = {"Tech I", "Tech II", "Faction", "Storyline", "Other"}

    if render_refresh_button:
        refresh_col_left, refresh_col_right = st.columns([6, 1])
        with refresh_col_left:
            if refresh_caption:
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
                    st.session_state["industry_builder_overview_rows"] = []
                    st.session_state["industry_builder_overview_meta"] = {}
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
    else:
        if refresh_caption:
            st.caption(refresh_caption)
        if bool(refresh_view.get("is_active")):
            render_job_status_panel(
                title="Overview refresh",
                is_running=True,
                progress_fraction=float(refresh_view.get("progress_fraction") or 0.0),
                progress_text=str(refresh_view.get("progress_label") or "Refreshing overview..."),
            )
            st.caption("Refresh job is running in the background. The current snapshot stays visible until the backend job completes.")

    return SharedIndustrySnapshotContext(
        default_character_id_value=default_character_id_value,
        default_industry_profile_id=default_industry_profile_id,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
        overview_rows=overview_rows,
        overview_meta=overview_meta,
        refresh_view=refresh_view,
        enabled_meta_groups=enabled_meta_groups,
        character_options=character_options,
        owned_blueprint_scope_options=owned_blueprint_scope_options,
        owned_blueprint_scope_labels=owned_blueprint_scope_labels,
        scope_last_refreshed_at=build_scope_refreshed_at_map(list(_characters)),
    )